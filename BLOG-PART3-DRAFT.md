# The feature is the plan

_Learnings from building Beava, part 3. Part 1 covered why there is an open-source lane for a Python-decorator streaming engine. Part 2 covered the performance lessons that made the engine fast. This post is about what it unlocks: a world where data scientists and agents both submit compute plans directly against live state, without an ML platform team in between._

---

I want to make one specific prediction about the next two years.

Today, if you are a data scientist at a company with any kind of real-time feature infrastructure, you do not get to compute a feature. You write the feature as Python, you file a PR, you explain it to the ML platform team, you argue about resource usage, you argue about shard keys, you wait two weeks, and maybe it ships.

In two years, you will not file the PR. Neither will your agent.

You will write the feature. You will submit it to the engine. It will start computing. You will see the results streaming to you within seconds. If it is wrong, you will edit it and resubmit. The ML platform team will still exist, but they will not be the gatekeeper on "can I compute this feature." They will be the gatekeeper on "is this plan safe to run in the shared substrate."

The shift is not about AI. The shift is about recognizing that the compute plan, not the engine, is the thing that matters.

## The shape of the current problem

Here is the current state of play for a real-time ML feature at any non-trivial company.

1. Data scientist writes a feature in Python, probably in a notebook, against static data.
2. Hands it to the ML platform team for productionization.
3. Platform team translates it into whatever the streaming engine speaks. Flink SQL. PySpark. Beam. A DAG in some internal framework. This translation is where correctness bugs are born.
4. Review. Deploy. Backfill. Monitor.
5. Data scientist looks at the output. Notices something is off. Goes back to step 1.

The loop takes weeks. Sometimes months.

Chalk.ai noticed this and built the best current answer to it. Their Symbolic Python Interpreter transpiles Python resolvers to Velox so data scientists can write Python that runs as native vectorized code in production. It is clever engineering. They are winning that lane. But the answer still has the same shape: the engine owns the compute, the user submits code, the engine compiles and runs it.

This is not wrong. It is just not _enough_ for what is coming next.

What is coming next is agents.

An LLM agent that wants to answer "has this user's purchase pattern changed in the last hour" cannot file a PR. It cannot wait two weeks. It needs to express the feature as a plan, submit the plan, get a streaming answer back. Hours, not weeks. Minutes, not hours. And the agent is going to want to iterate: submit plan A, look at the result, submit plan B, submit plan C. If every iteration requires a human, the agent is useless.

The ML platform team between the data scientist and prod has always been a patience tax. The agent just cannot pay it.

## The primitive that actually matters

Here is the line I want to argue for:

**The feature is the plan, not the engine.**

The engine is infrastructure. The event log is infrastructure. The state backend is infrastructure. None of them are the primary artifact. The primary artifact is the _compute plan_: a serializable, replayable, forkable specification of "given this event log, here is the feature I want."

If the plan is the primary artifact, everything else follows.

- Plans can be submitted over HTTP. A user, an agent, a data scientist, all speak the same interface.
- Plans can be validated before they run. The engine can say "this join has a shard-key mismatch, fix the spec."
- Plans can be replayed. Submit plan v1 against the log from three months ago, get what the feature would have produced.
- Plans can be differentially executed. Plan v2 is a refinement of plan v1. Reuse v1's state where possible, recompute the delta.
- Plans can be forked. "Take this production plan, point it at a scoped slice of events on my laptop, let me iterate." (Beava already has this primitive; it is called `tally fork`.)

None of this requires the engine to be distributed. None of this requires a cluster. It requires a durable, replayable event log with event-time and LSN semantics, and it requires the plan to be a first-class artifact rather than compiled-in code.

Beava already has the event log. It is per-shard, LSN-tagged, deduplicated across replicas. It is the Phase 52-06 work. What Beava does not yet have, and what this post is really about, is the plan as a serializable artifact that a non-human submits.

## What `tally fork` already shows

Beava ships a CLI tool, `tally fork`, built during v0 of the project. A data scientist runs:

```bash
tally fork \
    --upstream https://prod.my-company.com:8080 \
    --streams transactions,users \
    --since "2026-04-01" \
    --data-dir ./scratch
```

`tally fork` streams a scoped slice of production events to a local replica. The data scientist runs a local Beava binary on their laptop against that slice. They register new feature definitions, see the outputs, edit, rerun. The production system is not touched. Their iteration loop goes from "ship to prod, wait, observe" to "run locally, observe, edit, rerun." Minutes per cycle instead of days.

This works today. It is already how we dogfood Beava internally. It is also the exact primitive an agent needs, minus the human calling the CLI.

The agent version looks like this:

```python
# agent pseudocode
scratch_fork = beava_client.create_fork(
    streams=["transactions", "users"],
    since="2026-04-01T00:00:00Z",
)

plan_v1 = beava_client.compile_plan(python_source="""
@bv.stream(shard_key="user_id")
class Transactions:
    amount: float
    user_id: str
    _event_time: int
    purchase_velocity = bv.count(window="1h") / bv.count(window="7d")
""")

result = scratch_fork.run(plan_v1, wait_for="current")
if result.looks_wrong():
    plan_v2 = refine(plan_v1, result)
    result = scratch_fork.run(plan_v2, wait_for="current")
```

The agent submits a plan. The fork runs it against the log. The agent looks at the result. The agent refines and resubmits. No human. No ticket. No two weeks.

What this requires on the engine side:

1. The plan must be _serializable_, not Python source that only this Python interpreter understands. Python source is the authoring surface. It compiles to an IR. The IR is what the engine runs.
2. The engine must _validate_ the plan before accepting it. Type checks. Shard-key agreement. Bounded resource use. The validation is the gate that replaces the human review.
3. The engine must _run_ the plan against a specified log slice, not against "production." Forks are first class.
4. The engine must _return_ the result as a stream. Not a batch. Live feature values, updating as more of the log replays.

Four requirements. All tractable. None of them need an AI breakthrough. What they need is the plan to be the first-class object, not the engine.

## The harder question: incremental recomputation

There is one capability that is load-bearing for this vision and genuinely hard to build: incremental recomputation when the plan changes.

Today, if you change a feature definition in a production streaming engine, you either:

1. Start from zero and wait for new events to accumulate, or
2. Run a full backfill from the event log, which on 100 GB of state takes a weekend.

Neither is acceptable for an agent iterating on plans. The agent cannot wait a weekend between submissions.

The answer that the database community has been cooking for a decade is _incremental view maintenance_: when the view definition changes, compute the delta between old and new output and apply only the delta. Materialize has built a lot of this on differential dataflow. RisingWave has a version. Epsio. Feldera. None of them are quite the same shape as what a streaming feature engine needs, but the math is the same math.

The Beava version of this is not written yet. The design I have in my head looks like:

1. Every plan that runs against the log leaves behind a checkpoint: "as of LSN X, this plan's per-key state was Y."
2. When plan v2 arrives and is structurally similar to plan v1 (e.g., adds an operator, removes a filter), the engine computes the diff between v1 and v2 and applies only the changed operators against the existing checkpoint. It does not recompute what did not change.
3. When v2 is structurally dissimilar (new shard key, new stream) the engine falls back to full replay.

This is not shipped. It is the hard bet. It is also the bet that makes the "agent submits a plan, gets a result in seconds" story work at scale. Without it, an agent iterating on plans on a 100 GB log is a weekend per iteration, same as the human.

## Compute plane, data plane

The deeper shift here is one that Snowflake already made for data warehouses and that streaming has not yet made: separating compute from storage.

Today's streaming engine: one process owns the compute graph _and_ the state it produces. Change the graph, you must rebuild the state. They are bonded together.

Tomorrow's streaming engine: storage is a _substrate_. A durable, replayable, event-time-and-LSN-tagged log, plus the current state it has produced, plus the ability to checkpoint the state against a plan version. Compute is _consumer_ of the substrate. Multiple plans can run against the same substrate, each producing their own outputs, each differentially recomputing when they change.

This separation is what makes agents viable. If every agent submission requires its own engine instance, you have a cost explosion. If every agent submission is a new consumer of a shared substrate, you have the same economics as running additional queries against the same warehouse: cheaper than anyone expects.

Beava is not yet architected this way. The Phase 53 fjall substrate is a step toward it (durable per-shard state, decoupled from the in-memory cache). The Phase 54 legacy-engine removal is another step (making the shard-dispatch path the sole hot path). The roadmap past that, v1.3 and beyond, is where the plan-as-artifact work lives.

## The hard parts, named plainly

I am not going to pretend this is a solved design. Things that are genuinely hard:

- **Plan validation at submit time.** The engine has to reject bad plans without running them. Type checking. Shard-key agreement on joins. Resource bounds. Some of this is static; some of it needs a pre-flight simulation against the log.
- **Multi-tenancy.** N agents submitting N plans means N concurrent plans running against the same substrate. Cost accounting per plan. Backpressure per plan so one bad plan cannot starve the others. Isolation so one plan cannot corrupt the substrate.
- **Incremental re-execution semantics.** What counts as "structurally similar" between plan v1 and v2. When is it safe to reuse state. When must we fall back to full replay.
- **Replayable randomness.** If the plan contains an LLM call, determinism is not free. Seed the RNG, cache the outputs, version the model weights, or accept that re-runs will diverge.
- **The UX problem.** A human submitting a plan needs feedback. An agent submitting a plan needs feedback. The feedback needs to be fast, structured, and replayable. "Why did my plan fail" cannot be a stack trace.

None of these are "impossible." All of them are "serious engineering." The bet is that getting them right produces a primitive that is worth more than any individual streaming engine, because it is the thing that makes human and agent collaboration on real-time state possible without a platform team in between.

## The bet

Here is the line I want to close on.

The primitive that matters is not the streaming engine. It is the substrate. A durable, replayable, forkable event log with incremental compute plans as first-class citizens. If we get the substrate right, the engine is a thin layer over it, one of several consumers. If we get the substrate wrong, we have built another Flink with better syntax.

I do not know exactly what real-time feature infrastructure looks like in 2028. I am pretty sure it looks more like a data scientist and their agent jointly submitting a compute plan against live state than it looks like a ticket queue and a Kafka cluster.

The engine we built in 2026 is a step toward that. Beava is single-binary, Python-decorator, thread-per-core, durable-by-default. The event log is the first-class primitive, and `tally fork` already proves that forking the log and iterating against a scoped slice works. The plan-as-artifact work is next.

If you think that bet is interesting, the project is open source. The short install is still:

```bash
pip install beava
beava serve &
```

Under 60 seconds to a correct feature value. That is part 1 of the series. Part 2 was why it is fast. This post is why it matters. The next thing I ship will be the plan IR and the first version of the fork-for-agents API.

If the bet is wrong, I end up with a fast OSS streaming engine. That is not the worst consolation prize. If the bet is right, we end up with the substrate for how humans and agents share real-time compute. That is worth building toward.

---

_DRAFT: ~2050 words. Open questions for the author: (1) Is the "agent submits a plan" framing strong enough, or does it need a concrete near-term user story (e.g., a fraud analyst + an LLM assistant iterating on a detection rule)? (2) The incremental recomputation section is honest about being unshipped. Stand behind that honesty, or pull it back? (3) Chalk reference: I cite them as prior art; is that the right move, or should this post be agnostic about named competitors?_
