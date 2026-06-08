"""``@bv.expr`` — let someone write a normal Python function and turn it into a
beava "expression".

A beava expression is a small description of a calculation — for example
"amount times 2" — that the server runs later, once per event. It is *not* run
by Python here and now. Normally a user would build that description by hand;
``@bv.expr`` lets them write it as an ordinary-looking function instead.

The idea: when the function is decorated, we read its source text, make a fresh
copy of it, rewrite the parts that can't run on columns, fold the whole body
down to one expression, and hand back a stand-in function (a "wrapper"). Later,
when the user calls that stand-in with columns (each column stands for a field
in the event data), the folded copy runs and builds the expression description
for us.

What runs without rewriting: a column already knows how to record operations
like ``+`` and ``*`` on itself (see ``_col.py``), so plain arithmetic,
comparisons, method calls (``.lower()``), and builtins (``bv.log1p(...)``) build
their description just by running. What we DO rewrite are the few constructs
that would otherwise force Python to ask a column for a yes/no answer (which it
can't give): ``a if c else b``, ``and`` / ``or`` / ``not``, ``is None``, and
comparison chains like ``a < b < c`` (see ``_RewriteExpressions``).

On top of that we *fold the body into a single expression*: local variables are
copied in wherever they are read, and ``if`` / ``elif`` / ``else`` become nested
``if_else(...)`` calls (see ``_lower_stmts``). Anything beava can't express is
refused with a message that points at the user's own line (default-deny), and a
function that calls itself — directly or in a loop with others — is caught
rather than left to build forever.
"""
from __future__ import annotations

import ast
import copy
import functools
import inspect
import linecache
import sys
import textwrap
import threading
import types
from dataclasses import dataclass
from typing import Any, Callable, NoReturn, Optional

from beava._col import _coerce, _Expr, if_else
from beava._errors import RegistrationError

# The folded body calls this name wherever it had a ternary or an ``if``. We
# hand the real `if_else` helper to the function as a captured value under this
# name (see `_build_function`), so the call never depends on how the user
# imported beava and can't be hijacked by a stray argument. Two leading
# underscores make an accidental clash with a user's own parameter or variable
# essentially impossible.
_IF_ELSE_NAME = "__bv_if_else"

# The Python operators we can express. Everything else (``%``, ``**``, ``//``,
# ``<<`` …) has no equivalent on a column and is refused by default-deny.
_ALLOWED_BINOP = (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.BitAnd, ast.BitOr)
_ALLOWED_COMPARE = (ast.Lt, ast.Gt, ast.LtE, ast.GtE, ast.Eq, ast.NotEq)


def _is_none_literal(node: ast.AST) -> bool:
    """True if this node is the literal ``None``."""
    return isinstance(node, ast.Constant) and node.value is None


# ── decoration-time AST rewrites (constructs that can't run on columns) ───────


class _DesugarAugAssign(ast.NodeTransformer):
    """Rewrite ``x += 1`` into ``x = x + 1`` (and likewise for ``-= *= /= %=``).

    After this pass the rest of the code only ever sees plain ``x = ...``
    assignments, so nothing downstream has to special-case the ``+=`` forms.
    (``%=`` becomes ``x = x % ...``; ``%`` isn't supported, but by then it's an
    ordinary assignment that the default-deny floor rejects cleanly.)
    """

    def visit_AugAssign(self, node: ast.AugAssign) -> ast.Assign:
        self.generic_visit(node)
        # The same name appears on both sides; on the left it is being assigned
        # to, on the right it is being read. Copy it so the two don't share one
        # node, and flip the copy to a read.
        read_target = copy.deepcopy(node.target)
        read_target.ctx = ast.Load()
        return ast.Assign(
            targets=[node.target],
            value=ast.BinOp(left=read_target, op=node.op, right=node.value),
        )


class _RewriteExpressions(ast.NodeTransformer):
    """Turn the Python constructs that can't run on columns into ones that can.

    One method per shape:
      - ``a if c else b``                  → ``if_else(c, a, b)``
      - ``a and b`` / ``a or b``           → ``a & b`` / ``a | b``
      - ``not a``                          → ``~a``
      - ``x is None`` / ``x is not None``  → ``x.isnull()`` / ``~(x.isnull())``
      - ``a < b < c`` (a chain)            → ``(a < b) & (b < c)``

    Anything without a method here is left as-is, but its inner parts are still
    visited — so a ternary buried inside, say, an addition still gets rewritten.
    Constructs that survive this pass untouched (a stray ``is``, a ``%``, a
    unary ``-`` on a name) are refused later by the default-deny floor.
    """

    def visit_IfExp(self, node: ast.IfExp) -> ast.Call:
        # a if c else b  →  if_else(c, a, b). Plain Python would decide the
        # ternary by asking `c` for a yes/no, which a column can't give; turning
        # it into a call keeps both branches as data for the server to choose.
        self.generic_visit(node)
        return ast.Call(
            func=ast.Name(id=_IF_ELSE_NAME, ctx=ast.Load()),
            args=[node.test, node.body, node.orelse],
            keywords=[],
        )

    def visit_BoolOp(self, node: ast.BoolOp) -> ast.expr:
        # a and b and c  →  ((a & b) & c) ;  a or b  →  a | b. Python won't let us
        # hook `and`/`or` directly, but columns already support `&`/`|`, which
        # build the same wire output.
        self.generic_visit(node)
        op: ast.operator = ast.BitAnd() if isinstance(node.op, ast.And) else ast.BitOr()
        combined = node.values[0]
        for value in node.values[1:]:
            combined = ast.BinOp(left=combined, op=op, right=value)
        return combined

    def visit_UnaryOp(self, node: ast.UnaryOp) -> ast.expr:
        # not a  →  ~a. (We leave `+a` / `-a` alone here; the floor handles them.)
        self.generic_visit(node)
        if isinstance(node.op, ast.Not):
            return ast.UnaryOp(op=ast.Invert(), operand=node.operand)
        return node

    def visit_Compare(self, node: ast.Compare) -> ast.expr:
        # A chain like `a < b < c` is ONE node that holds both `<`s at once.
        # Split it into the AND of its neighbouring pairs and process that.
        if len(node.ops) > 1:
            operands = [node.left, *node.comparators]
            pairs = [
                ast.Compare(
                    # Copy each operand so the shared middle ones (`b` appears in
                    # both `a < b` and `b < c`) don't share a single node.
                    left=copy.deepcopy(operands[i]),
                    ops=[node.ops[i]],
                    comparators=[copy.deepcopy(operands[i + 1])],
                )
                for i in range(len(node.ops))
            ]
            chain: ast.expr = pairs[0]
            for pair in pairs[1:]:
                chain = ast.BinOp(left=chain, op=ast.BitAnd(), right=pair)
            return self.visit(chain)

        self.generic_visit(node)
        return self._rewrite_is_none(node)

    @staticmethod
    def _rewrite_is_none(node: ast.Compare) -> ast.expr:
        # `x is None` / `None is x`  → x.isnull() ; the `is not` forms add `~`.
        # We find the None side by position, so all four spellings work. Any
        # other use of `is` (e.g. `x is y`) is left for the floor to reject.
        op = node.ops[0]
        if not isinstance(op, (ast.Is, ast.IsNot)):
            return node
        left, right = node.left, node.comparators[0]
        if _is_none_literal(right):
            target = left
        elif _is_none_literal(left):
            target = right
        else:
            return node
        isnull = ast.Call(
            func=ast.Attribute(value=target, attr="isnull", ctx=ast.Load()),
            args=[],
            keywords=[],
        )
        if isinstance(op, ast.IsNot):
            return ast.UnaryOp(op=ast.Invert(), operand=isnull)
        return isnull


def _rewrite_body(func_def: ast.FunctionDef) -> ast.FunctionDef:
    """Run the two decoration-time AST passes over the function.

    First turn ``x += 1`` into ``x = x + 1``, then turn the constructs that
    can't run on columns (``a if c else b``, ``and`` / ``or`` / ``not``,
    ``is None``, comparison chains) into ones that can. The statement shape —
    assignments, ``if`` statements, ``return``s — is left for ``_lower_stmts``
    to fold into a single expression.
    """
    func_def = _DesugarAugAssign().visit(func_def)
    func_def = _RewriteExpressions().visit(func_def)
    ast.fix_missing_locations(func_def)
    return func_def


# ── folding the body into one expression (default-deny) ───────────────────────


@dataclass
class _Ctx:
    """The few facts the folding walk needs about the function being compiled."""

    fn: Callable[..., Any]
    src_file: str
    params: frozenset[str]
    fn_globals: dict[str, Any]
    # Every name assigned somewhere in the body. Used to tell a local that
    # hasn't been given a value yet (reject) from an outside name (a module,
    # a sibling helper) that is resolved when the body runs.
    assigned_locals: frozenset[str]


# The notebook mapping each local name to the folded expression it stands for.
_Env = dict[str, ast.expr]
# The folding walk returns one of two outcomes for a run of statements:
#   ("return", expr_node)  — the run ends by returning this expression
#   ("fall", env)          — the run reaches the end without returning; `env`
#                            is the variable notebook carried to the next code.
_Outcome = tuple[str, Any]


def _reject(code: str, node: ast.AST, ctx: _Ctx, what: str) -> NoReturn:
    """Refuse a construct with a message that points at the user's own line.

    Fetches the offending source line and underlines the exact column, like a
    normal Python traceback, then raises ``RegistrationError`` with the given
    code so callers can assert on it.
    """
    lineno = getattr(node, "lineno", 0)
    col = getattr(node, "col_offset", 0)
    line = linecache.getline(ctx.src_file, lineno).rstrip("\n") if lineno else ""
    pointer = ""
    if line:
        stripped = line.lstrip()
        indent = len(line) - len(stripped)
        caret = " " * max(col - indent, 0) + "^"
        pointer = f"\n    {stripped}\n    {caret}"
    message = (
        f"{what}\n"
        f"  File \"{ctx.src_file}\", line {lineno}, "
        f"in @bv.expr function {ctx.fn.__name__!r}{pointer}"
    )
    raise RegistrationError(code=code, message=message)


def _collect_params(func_def: ast.FunctionDef) -> frozenset[str]:
    a = func_def.args
    names = [arg.arg for arg in (*a.posonlyargs, *a.args, *a.kwonlyargs)]
    if a.vararg:
        names.append(a.vararg.arg)
    if a.kwarg:
        names.append(a.kwarg.arg)
    return frozenset(names)


def _collect_assigned(func_def: ast.FunctionDef) -> frozenset[str]:
    names = set()
    for node in ast.walk(func_def):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
    return frozenset(names)


def _resolve_name(node: ast.Name, env: _Env, ctx: _Ctx, *, is_call_head: bool) -> ast.expr:
    """Turn a name into the tree it stands for, or refuse it.

    - in the notebook (a local with a value) → copy that value in;
    - a function parameter → keep it (it becomes a column when the body runs);
    - a local that hasn't been given a value on this path → reject;
    - the head of a call (``bv`` in ``bv.log1p``, a sibling helper) → keep it,
      resolved when the body runs (this is what lets helpers call each other,
      even before the other one is defined);
    - any other outside name we can see in the module (a value, not a call) →
      keep it;
    - otherwise it is unknown → reject.
    """
    name = node.id
    if name in env:
        return copy.deepcopy(env[name])
    if name in ctx.params:
        return ast.Name(id=name, ctx=ast.Load())
    if name in ctx.assigned_locals:
        return _reject(
            "expr_unknown_name",
            node,
            ctx,
            f"the name {name!r} is read before it is given a value on this path",
        )
    if is_call_head:
        return ast.Name(id=name, ctx=ast.Load())
    if name in ctx.fn_globals:
        return ast.Name(id=name, ctx=ast.Load())
    return _reject(
        "expr_unknown_name",
        node,
        ctx,
        f"the name {name!r} is not defined",
    )


def _lower_callee(func: ast.expr, env: _Env, ctx: _Ctx) -> ast.expr:
    """Fold the thing being called (``bv.log1p``, ``url.lower``, ``if_else``).

    The leftmost name of a call may be an outside name resolved at run time, so
    we keep it as-is rather than demanding it already have a value.
    """
    if isinstance(func, ast.Name):
        return _resolve_name(func, env, ctx, is_call_head=True)
    if isinstance(func, ast.Attribute):
        return ast.Attribute(
            value=_lower_callee(func.value, env, ctx), attr=func.attr, ctx=ast.Load()
        )
    return _lower_expr(func, env, ctx)


def _lower_expr(node: ast.expr, env: _Env, ctx: _Ctx) -> ast.expr:
    """Fold one expression: copy local variables in, and refuse anything beava
    can't express. Returns a fresh AST node (the original is never mutated)."""
    if isinstance(node, ast.Constant):
        return ast.Constant(value=node.value)

    if isinstance(node, ast.Name):
        return _resolve_name(node, env, ctx, is_call_head=False)

    if isinstance(node, ast.BinOp):
        if not isinstance(node.op, _ALLOWED_BINOP):
            return _reject(
                "expr_unsupported_python_op",
                node,
                ctx,
                f"the operator {type(node.op).__name__} is not supported; "
                "@bv.expr allows + - * / and the words and/or",
            )
        return ast.BinOp(
            left=_lower_expr(node.left, env, ctx),
            op=node.op,
            right=_lower_expr(node.right, env, ctx),
        )

    if isinstance(node, ast.UnaryOp):
        # `~` is what `not` / `is not` became earlier — keep it.
        if isinstance(node.op, ast.Invert):
            return ast.UnaryOp(op=ast.Invert(), operand=_lower_expr(node.operand, env, ctx))
        # `-5` is stored as "negate the number 5"; fold it to the literal -5.
        if (
            isinstance(node.op, ast.USub)
            and isinstance(node.operand, ast.Constant)
            and isinstance(node.operand.value, (int, float))
            and not isinstance(node.operand.value, bool)
        ):
            return ast.Constant(value=-node.operand.value)
        return _reject(
            "expr_unsupported_python_op",
            node,
            ctx,
            "the only unary forms supported are `not` and a minus sign in front "
            "of a number; an expression has no negation operator",
        )

    if isinstance(node, ast.Compare):
        # Chains were already split into pairs; a stray multi-operator compare
        # would be a bug, so refuse it rather than mis-lower it.
        if len(node.ops) != 1:
            return _reject(
                "expr_unsupported_python_op", node, ctx, "unsupported comparison shape"
            )
        op = node.ops[0]
        if not isinstance(op, _ALLOWED_COMPARE):
            return _reject(
                "expr_unsupported_python_op",
                node,
                ctx,
                f"the comparison {type(op).__name__} is not supported "
                "(only `is None` / `is not None` use `is`)",
            )
        return ast.Compare(
            left=_lower_expr(node.left, env, ctx),
            ops=[op],
            comparators=[_lower_expr(node.comparators[0], env, ctx)],
        )

    if isinstance(node, ast.Call):
        return ast.Call(
            func=_lower_callee(node.func, env, ctx),
            args=[_lower_expr(arg, env, ctx) for arg in node.args],
            keywords=[
                ast.keyword(arg=kw.arg, value=_lower_expr(kw.value, env, ctx))
                for kw in node.keywords
            ],
        )

    if isinstance(node, ast.Attribute):
        return ast.Attribute(
            value=_lower_expr(node.value, env, ctx), attr=node.attr, ctx=ast.Load()
        )

    return _reject(
        "expr_unsupported_python_op",
        node,
        ctx,
        f"{type(node).__name__} is not supported in @bv.expr",
    )


def _if_else_node(cond: ast.expr, then_: ast.expr, else_: ast.expr) -> ast.Call:
    """Build a call to the captured ``if_else`` helper from already-folded parts."""
    return ast.Call(
        func=ast.Name(id=_IF_ELSE_NAME, ctx=ast.Load()),
        args=[cond, then_, else_],
        keywords=[],
    )


def _outcome_value(outcome: _Outcome, node: ast.AST, ctx: _Ctx) -> ast.expr:
    """Pull the value out of an outcome, or reject if the path never returned."""
    kind, payload = outcome
    if kind == "return":
        return payload
    return _reject(
        "expr_missing_return",
        node,
        ctx,
        "this path reaches the end of the function without returning a value",
    )


def _merge_envs(
    cond: ast.expr, base: _Env, then_env: _Env, else_env: _Env, node: ast.AST, ctx: _Ctx
) -> _Env:
    """Combine the two branch notebooks where the branches rejoin.

    For each name touched by a branch: if both branches give it the same value
    (an untouched outer variable), keep that; if they differ, the merged value
    is ``if_else(cond, then_value, else_value)``. A name set in only one branch
    with no value on the other path can't converge — reject it.
    """
    merged = dict(base)
    for name in sorted(set(then_env) | set(else_env)):
        in_then = name in then_env
        in_else = name in else_env
        if in_then and in_else:
            then_val, else_val = then_env[name], else_env[name]
            if then_val is else_val:
                merged[name] = then_val
            else:
                merged[name] = _if_else_node(
                    copy.deepcopy(cond),
                    copy.deepcopy(then_val),
                    copy.deepcopy(else_val),
                )
        else:
            return _reject(
                "expr_branch_local_binding",
                node,
                ctx,
                f"the name {name!r} is set in one branch of the `if` but not the "
                "other, so a path is left with no value for it",
            )
    return merged


def _lower_if(stmt: ast.If, rest: list[ast.stmt], env: _Env, ctx: _Ctx) -> _Outcome:
    """Fold an ``if`` (with whatever follows it) into one outcome.

    Each branch is folded against its own copy of the notebook. How they combine
    depends on whether a branch returns:
      - both return         → ``if_else(cond, then, else)``; code after is dead.
      - one returns         → the other branch carries on into the code after the
                              ``if``, and its result fills that side of the choice.
      - neither returns     → merge the notebooks and carry on once.
    """
    cond = _lower_expr(stmt.test, env, ctx)
    then_out = _lower_stmts(stmt.body, env, ctx)
    else_out = _lower_stmts(stmt.orelse, env, ctx)
    then_kind, then_payload = then_out
    else_kind, else_payload = else_out

    if then_kind == "return" and else_kind == "return":
        return ("return", _if_else_node(cond, then_payload, else_payload))

    if then_kind == "return":  # else fell through into the code after the `if`
        cont = _lower_stmts(rest, else_payload, ctx)
        return ("return", _if_else_node(cond, then_payload, _outcome_value(cont, stmt, ctx)))

    if else_kind == "return":  # then fell through into the code after the `if`
        cont = _lower_stmts(rest, then_payload, ctx)
        return ("return", _if_else_node(cond, _outcome_value(cont, stmt, ctx), else_payload))

    # Neither branch returned — merge their notebooks and carry on once.
    merged = _merge_envs(cond, env, then_payload, else_payload, stmt, ctx)
    return _lower_stmts(rest, merged, ctx)


def _lower_stmts(stmts: list[ast.stmt], env: _Env, ctx: _Ctx) -> _Outcome:
    """Fold a run of statements into one outcome, reading them in order.

    ``name = rhs`` records what ``name`` stands for; ``return e`` ends the run
    with a value; ``if`` hands off to ``_lower_if``. Anything else is refused.
    """
    env = dict(env)
    for index, stmt in enumerate(stmts):
        if isinstance(stmt, ast.Assign):
            if len(stmt.targets) != 1 or not isinstance(stmt.targets[0], ast.Name):
                return _reject(
                    "expr_bad_assign_target",
                    stmt,
                    ctx,
                    "only `name = value` assignments are supported "
                    "(no tuples, attributes, or subscripts on the left)",
                )
            env[stmt.targets[0].id] = _lower_expr(stmt.value, env, ctx)
        elif isinstance(stmt, ast.Return):
            if stmt.value is None:
                return _reject(
                    "expr_missing_return",
                    stmt,
                    ctx,
                    "a bare `return` gives no value; every path must return one",
                )
            return ("return", _lower_expr(stmt.value, env, ctx))
        elif isinstance(stmt, ast.If):
            return _lower_if(stmt, list(stmts[index + 1 :]), env, ctx)
        elif isinstance(stmt, ast.Pass):
            continue
        elif isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant):
            continue  # a docstring or stray literal — it has no effect
        else:
            return _reject(
                "expr_unsupported_python_op",
                stmt,
                ctx,
                f"{type(stmt).__name__} statements are not supported in @bv.expr",
            )
    return ("fall", env)


def _callee_head(func: ast.expr) -> Optional[str]:
    """The leftmost name of a call target (``bv`` in ``bv.log1p(x)``)."""
    while isinstance(func, ast.Attribute):
        func = func.value
    return func.id if isinstance(func, ast.Name) else None


def _scan_self_recursion(lowered: ast.expr, name: str, ctx: _Ctx) -> None:
    """Refuse a function that calls itself by name.

    Runs after folding, because a self-call hidden inside an ``if`` only becomes
    visible once the branches are built. Without this, the branches would build
    forever and crash with a raw ``RecursionError``.
    """
    for node in ast.walk(lowered):
        if isinstance(node, ast.Call) and _callee_head(node.func) == name:
            _reject(
                "expr_recursive_call",
                node,
                ctx,
                f"the function {name!r} calls itself; @bv.expr cannot express recursion",
            )


# ── runtime recursion guard (functions that call each other in a loop) ────────


@dataclass(frozen=True)
class _Frame:
    """One entry on the per-thread "currently building" stack."""

    key: Any  # identity of the @bv.expr function being built
    qname: str  # its name, for the cycle message
    file: str  # where it is defined
    called_from_file: str  # file of the code that called it
    called_from_line: int  # line of the code that called it


_build_stack = threading.local()


def _frames() -> list[_Frame]:
    frames = getattr(_build_stack, "frames", None)
    if frames is None:
        frames = []
        _build_stack.frames = frames
    return frames


def _format_cycle(cycle: list[_Frame]) -> str:
    """Describe a recursion loop, one block per hop, trimming long middles.

    ``cycle`` starts and ends with the same function. Each hop block names where
    one function calls the next. A loop longer than five hops keeps the first
    and last hop and replaces the middle with ``... (k more) ...`` so the
    closing edge stays visible.
    """
    names = " → ".join(frame.qname for frame in cycle)
    hops = []
    for current, nxt in zip(cycle, cycle[1:], strict=False):
        hops.append(
            f"  File \"{current.file}\", line {nxt.called_from_line}, "
            f"in {current.qname!r} → calls {nxt.qname!r}"
        )
    if len(hops) > 5:
        hops = [hops[0], f"  ... ({len(hops) - 2} more) ...", hops[-1]]
    return "recursive @bv.expr definition: " + names + "\n" + "\n".join(hops)


# ── compile + wrap ────────────────────────────────────────────────────────────


def _build_function(
    func_def: ast.FunctionDef, fn: Callable[..., Any], src_file: str
) -> Callable[..., Any]:
    """Compile the folded function and return a runnable function object.

    The body calls ``if_else`` by name (wherever it had a ternary or an ``if``),
    so that name has to be available when the body runs — without depending on
    the user's imports and without adding anything to the user's module. We do
    it by wrapping the function in a tiny outer function (``capture_if_else``)
    that takes ``if_else`` as an argument and returns the inner function.
    Running that outer function with the real helper makes the inner function
    "remember" it (a captured value). The inner function is still bound to the
    user's live module names, so it can also reach sibling ``@bv.expr`` helpers.
    """
    capture_def = ast.parse(f"def __bv_capture_if_else({_IF_ELSE_NAME}): pass").body[0]
    assert isinstance(capture_def, ast.FunctionDef)
    capture_def.body = [
        func_def,
        ast.Return(value=ast.Name(id=func_def.name, ctx=ast.Load())),
    ]

    module = ast.Module(body=[capture_def], type_ignores=[])
    ast.fix_missing_locations(module)
    module_code = compile(module, filename=src_file, mode="exec")
    capture_code = next(
        const for const in module_code.co_consts if isinstance(const, types.CodeType)
    )
    capture_if_else = types.FunctionType(capture_code, fn.__globals__)
    # Run it with the real helper; it returns the inner function with `if_else`
    # captured.
    return capture_if_else(if_else)


def expr(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Turn a plain Python function into something that builds a beava expression.

    This runs once, the moment ``@bv.expr`` is attached to a function. It reads
    the function's source, rewrites and folds the body into a single expression,
    refuses anything beava can't express (pointing at the user's own line), and
    hands back a stand-in function. Later, when the user calls that stand-in with
    columns (or plain numbers and strings), the folded body runs and returns the
    beava expression it builds.
    """
    # Make sure we were handed the right thing. @bv.expr needs a real function
    # whose source we can read.
    if inspect.isclass(fn):
        raise TypeError("@bv.expr can only decorate a function, not a class")
    if not inspect.isfunction(fn):
        raise TypeError(
            f"@bv.expr can only decorate a plain function; got {type(fn).__name__}"
        )
    if fn.__name__ == "<lambda>":
        raise TypeError("@bv.expr cannot decorate a lambda; use a `def`")

    try:
        lines, start_line = inspect.getsourcelines(fn)
    except OSError as exc:
        # The function has no source file we can read. This happens for
        # functions typed straight into an interactive prompt or built with exec().
        raise TypeError(
            f"@bv.expr needs {fn.__name__!r} defined in a source file; functions "
            "typed into a REPL or built with exec() have no source to rewrite"
        ) from exc

    src_file = inspect.getsourcefile(fn) or fn.__code__.co_filename
    tree = ast.parse(textwrap.dedent("".join(lines)))
    # The parsed copy starts at line 1, but the function really sits further down
    # its file. Anchoring on the first source line we were handed shifts every
    # node to its real line, so error messages point at the right place (and this
    # stays correct no matter how many decorator lines come before the `def`).
    ast.increment_lineno(tree, start_line - 1)

    func_def = next(
        (node for node in tree.body if isinstance(node, ast.FunctionDef)), None
    )
    if func_def is None:
        raise TypeError("@bv.expr can only decorate a plain function")

    func_def = _rewrite_body(func_def)
    ctx = _Ctx(
        fn=fn,
        src_file=src_file,
        params=_collect_params(func_def),
        fn_globals=fn.__globals__,
        assigned_locals=_collect_assigned(func_def),
    )

    # Fold the whole body down to one expression.
    outcome = _lower_stmts(func_def.body, {}, ctx)
    lowered = _outcome_value(outcome, func_def, ctx)
    _scan_self_recursion(lowered, fn.__name__, ctx)

    # Rebuild the function as `def name(same params): return <one expression>`,
    # reusing the original signature node so nothing about it is lost.
    func_def.decorator_list = []
    func_def.body = [ast.Return(value=lowered)]
    ast.fix_missing_locations(func_def)
    compiled = _build_function(func_def, fn, src_file)

    # A unique tag for this function, so the runtime loop-detector can tell it
    # apart from every other @bv.expr function (even one with the same name).
    key = object()
    qname = fn.__qualname__

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        building = any(isinstance(a, _Expr) for a in args) or any(
            isinstance(v, _Expr) for v in kwargs.values()
        )
        # Concrete door: called with only plain Python values → run the original
        # function and hand back whatever it returns. For a body written in plain
        # Python / stdlib this is a real number, so the same source can be unit
        # tested with ordinary values. NOTE: a body that calls `bv.*` (as the
        # fixtures here do) builds an expression even on this path — mapping
        # stdlib idioms onto beava nodes so those bodies stay native is deferred
        # (its own RFC). (No args at all still builds a tree, so a constant body
        # like `return 5` lands as a literal node.)
        if not building and (args or kwargs):
            return fn(*args, **kwargs)

        # Tree door: record this call on the per-thread stack so a loop of
        # functions calling each other (a → b → a) is caught on re-entry.
        caller = sys._getframe(1)
        frame = _Frame(
            key=key,
            qname=qname,
            file=src_file,
            called_from_file=caller.f_code.co_filename,
            called_from_line=caller.f_lineno,
        )
        frames = _frames()
        for position, existing in enumerate(frames):
            if existing.key is key:
                raise RegistrationError(
                    code="expr_recursive_call",
                    message=_format_cycle(frames[position:] + [frame]),
                )
        frames.append(frame)
        try:
            coerced_args = [_coerce(a) for a in args]
            coerced_kwargs = {k: _coerce(v) for k, v in kwargs.items()}
            # Coerce the result too, so a constant answer (`return None` → null,
            # `return 5` → literal 5) comes back as a proper node.
            return _coerce(compiled(*coerced_args, **coerced_kwargs))
        finally:
            frames.pop()

    return wrapper
