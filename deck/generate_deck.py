"""
Streamlet — Vietnam AI Stars 2026 Pitch Deck Generator

Generates a 15-slide PDF pitch deck using reportlab.
Slide size: 13.333" x 7.5" (standard 16:9 widescreen, 960 x 540 pt).
"""

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import landscape
from reportlab.lib.colors import HexColor, Color, white, black
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# ── Page geometry ────────────────────────────────────────────────────────────
PAGE_W = 13.333 * 72   # 960 pt
PAGE_H = 7.5 * 72      # 540 pt
PAGE_SIZE = (PAGE_W, PAGE_H)

# ── Color palette ────────────────────────────────────────────────────────────
NAVY       = HexColor("#0B1E3F")   # primary dark
NAVY_SOFT  = HexColor("#1E3A5F")
INK        = HexColor("#0F172A")   # body text
SUBTLE     = HexColor("#64748B")   # muted text
RED        = HexColor("#DA251D")   # Vietnam flag red — accent
AMBER      = HexColor("#F59E0B")
CYAN       = HexColor("#0EA5E9")
MINT       = HexColor("#10B981")
BG         = HexColor("#FFFFFF")
BG_ALT     = HexColor("#F8FAFC")
LINE       = HexColor("#E2E8F0")
CODE_BG    = HexColor("#0F172A")
CODE_FG    = HexColor("#E2E8F0")
CODE_KW    = HexColor("#F472B6")
CODE_STR   = HexColor("#FCD34D")
CODE_FN    = HexColor("#60A5FA")
CODE_COM   = HexColor("#64748B")

# ── Fonts ────────────────────────────────────────────────────────────────────
F_BOLD = "Helvetica-Bold"
F_REG  = "Helvetica"
F_MONO = "Courier"
F_MONO_B = "Courier-Bold"

# ── Helpers ──────────────────────────────────────────────────────────────────
def draw_footer(c, slide_no, total):
    c.setFillColor(SUBTLE)
    c.setFont(F_REG, 9)
    c.drawString(40, 20, "Streamlet  ·  Vietnam AI Stars 2026")
    c.drawRightString(PAGE_W - 40, 20, f"{slide_no} / {total}")
    # thin red accent line at bottom-left
    c.setStrokeColor(RED)
    c.setLineWidth(2)
    c.line(40, 32, 100, 32)

def draw_title_bar(c, title, kicker=None):
    """Standard slide header: kicker + title."""
    if kicker:
        c.setFillColor(RED)
        c.setFont(F_BOLD, 10)
        c.drawString(40, PAGE_H - 52, kicker.upper())
    c.setFillColor(NAVY)
    c.setFont(F_BOLD, 28)
    c.drawString(40, PAGE_H - 86, title)
    c.setStrokeColor(LINE)
    c.setLineWidth(1)
    c.line(40, PAGE_H - 100, PAGE_W - 40, PAGE_H - 100)

def wrap_text(c, text, font, size, max_width):
    """Greedy word-wrap returning list of lines."""
    c.setFont(font, size)
    words = text.split()
    lines, cur = [], ""
    for w in words:
        probe = (cur + " " + w).strip()
        if c.stringWidth(probe, font, size) <= max_width:
            cur = probe
        else:
            if cur:
                lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    return lines

def draw_wrapped(c, text, x, y, font, size, max_width, color=INK, leading=None):
    c.setFillColor(color)
    lines = wrap_text(c, text, font, size, max_width)
    lh = leading or size * 1.25
    for i, line in enumerate(lines):
        c.setFont(font, size)
        c.drawString(x, y - i * lh, line)
    return y - len(lines) * lh

def draw_bullet(c, x, y, text, max_width, size=13, color=INK, bullet_color=RED):
    """Draw a bulleted line with a red square bullet."""
    # bullet square
    c.setFillColor(bullet_color)
    c.rect(x, y + 3, 6, 6, stroke=0, fill=1)
    # text
    return draw_wrapped(c, text, x + 16, y + size - 3, F_REG, size,
                        max_width - 16, color=color, leading=size * 1.4)

def draw_placeholder_badge(c, x, y, w, h, label, note):
    """A high-visibility yellow dashed box marking content that must be
    replaced with real data before submission."""
    c.saveState()
    c.setFillColor(HexColor("#FEF3C7"))      # soft amber fill
    c.setStrokeColor(HexColor("#F59E0B"))    # amber border
    c.setLineWidth(1.5)
    c.setDash(4, 3)
    c.roundRect(x, y, w, h, 6, stroke=1, fill=1)
    c.setDash()
    # label pill
    c.setFillColor(HexColor("#F59E0B"))
    c.roundRect(x + 10, y + h - 18, 120, 14, 7, stroke=0, fill=1)
    c.setFillColor(white); c.setFont(F_BOLD, 8)
    c.drawString(x + 18, y + h - 15, label)
    # note text
    c.setFillColor(HexColor("#78350F"))
    c.setFont(F_REG, 9)
    lines = wrap_text(c, note, F_REG, 9, w - 24)
    for i, line in enumerate(lines):
        c.drawString(x + 14, y + h - 34 - i * 11, line)
    c.restoreState()

def draw_placeholder_badge(c, x, y, w, h, label, note):
    """A high-visibility yellow dashed box marking content that must be
    replaced with real data before submission."""
    c.saveState()
    c.setFillColor(HexColor("#FEF3C7"))      # soft amber fill
    c.setStrokeColor(HexColor("#F59E0B"))    # amber border
    c.setLineWidth(1.5)
    c.setDash(4, 3)
    c.roundRect(x, y, w, h, 6, stroke=1, fill=1)
    c.setDash()
    # label pill
    c.setFillColor(HexColor("#F59E0B"))
    c.roundRect(x + 10, y + h - 18, 120, 14, 7, stroke=0, fill=1)
    c.setFillColor(white); c.setFont(F_BOLD, 8)
    c.drawString(x + 18, y + h - 15, label)
    # note text
    c.setFillColor(HexColor("#78350F"))
    c.setFont(F_REG, 9)
    lines = wrap_text(c, note, F_REG, 9, w - 24)
    for i, line in enumerate(lines):
        c.drawString(x + 14, y + h - 34 - i * 11, line)
    c.restoreState()

def draw_stat_card(c, x, y, w, h, number, label, accent=RED):
    c.setFillColor(BG_ALT)
    c.setStrokeColor(LINE)
    c.setLineWidth(1)
    c.roundRect(x, y, w, h, 8, stroke=1, fill=1)
    # accent bar
    c.setFillColor(accent)
    c.rect(x, y + h - 4, w, 4, stroke=0, fill=1)
    # number
    c.setFillColor(NAVY)
    c.setFont(F_BOLD, 32)
    c.drawString(x + 16, y + h - 54, number)
    # label
    c.setFillColor(SUBTLE)
    c.setFont(F_REG, 10)
    lines = wrap_text(c, label, F_REG, 10, w - 32)
    for i, line in enumerate(lines):
        c.drawString(x + 16, y + h - 74 - i * 13, line)

def draw_code_block(c, x, y, w, h, lines):
    """Draw a syntax-highlighted-ish code block.
    `lines` is a list of (style, text) tuples where style is
    'norm', 'kw', 'str', 'fn', 'com'."""
    c.setFillColor(CODE_BG)
    c.roundRect(x, y, w, h, 6, stroke=0, fill=1)
    # traffic-light dots for polish
    for i, col in enumerate([HexColor("#FF5F56"), HexColor("#FFBD2E"), HexColor("#27C93F")]):
        c.setFillColor(col)
        c.circle(x + 14 + i * 14, y + h - 14, 4.5, stroke=0, fill=1)
    # render lines
    c.setFont(F_MONO, 10)
    lh = 13
    tx, ty = x + 16, y + h - 34
    style_color = {
        'norm': CODE_FG, 'kw': CODE_KW, 'str': CODE_STR,
        'fn': CODE_FN, 'com': CODE_COM,
    }
    for line in lines:
        cx = tx
        for style, text in line:
            c.setFillColor(style_color.get(style, CODE_FG))
            font = F_MONO_B if style in ('kw', 'fn') else F_MONO
            c.setFont(font, 10)
            c.drawString(cx, ty, text)
            cx += c.stringWidth(text, font, 10)
        ty -= lh

# ─────────────────────────────────────────────────────────────────────────────
# SLIDES
# ─────────────────────────────────────────────────────────────────────────────

TOTAL_SLIDES = 15

def slide_01_title(c):
    # Full navy background
    c.setFillColor(NAVY)
    c.rect(0, 0, PAGE_W, PAGE_H, stroke=0, fill=1)
    # Red accent bar on the left
    c.setFillColor(RED)
    c.rect(0, 0, 12, PAGE_H, stroke=0, fill=1)
    # Kicker
    c.setFillColor(RED)
    c.setFont(F_BOLD, 12)
    c.drawString(60, PAGE_H - 80, "VIETNAM AI STARS  ·  2026")
    # Title
    c.setFillColor(white)
    c.setFont(F_BOLD, 72)
    c.drawString(60, PAGE_H - 180, "Streamlet")
    # Tagline
    c.setFillColor(HexColor("#E2E8F0"))
    c.setFont(F_REG, 22)
    c.drawString(60, PAGE_H - 230, "Zero-ops real-time features.")
    c.drawString(60, PAGE_H - 258, "Vibe-code your pipelines.")
    # Small descriptor
    c.setFillColor(HexColor("#94A3B8"))
    c.setFont(F_REG, 13)
    c.drawString(60, PAGE_H - 310,
                 "A single Rust binary. No Kafka, no Flink, no cluster, no platform team.")
    c.drawString(60, PAGE_H - 328,
                 "Scale = add memory. Author pipelines with the Claude skills that ship with it.")
    # Author block bottom
    c.setStrokeColor(RED)
    c.setLineWidth(2)
    c.line(60, 90, 120, 90)
    c.setFillColor(white)
    c.setFont(F_BOLD, 14)
    c.drawString(60, 62, "Hoang Phan")
    c.setFillColor(HexColor("#94A3B8"))
    c.setFont(F_REG, 11)
    c.drawString(60, 44, "Founder & Engineering Lead  ·  phan.minhhoang2606@gmail.com  ·  github.com/petrpan26")

def slide_02_opportunity(c):
    draw_title_bar(c, "Every startup wants real-time features. Nobody wants the Flink pager.", kicker="the opportunity")
    # Lead paragraph
    draw_wrapped(c,
        "Fraud detection, live personalization, AI agent memory — the features that define a modern "
        "product can't wait for a nightly batch. Every growth-stage founder knows they need real-time. "
        "What stops them isn't budget or ambition — it's the operational burden. Running Kafka, Flink, "
        "Zookeeper, and a feature store means hiring people who've done it before, and those people "
        "are scarce, expensive, and already working at FAANG.",
        40, PAGE_H - 140, F_REG, 13, PAGE_W - 80, color=INK, leading=18)

    # Stat cards row
    y = PAGE_H - 340
    card_w = (PAGE_W - 80 - 3 * 16) / 4
    card_h = 120
    stats = [
        ("~50,000", "Growth-stage startups globally that want real-time features", RED),
        ("< 2%", "That can staff a dedicated streaming-infra team today", CYAN),
        ("4+", "Systems to operate in the status-quo stack (Kafka, Flink, feature store, glue)", AMBER),
        ("24/7", "On-call rotation the day you turn Flink on in production", MINT),
    ]
    for i, (num, label, col) in enumerate(stats):
        draw_stat_card(c, 40 + i * (card_w + 16), y, card_w, card_h, num, label, accent=col)

    # Bottom takeaway
    c.setFillColor(NAVY)
    c.setFont(F_BOLD, 14)
    c.drawString(40, 90,
                 "Real-time features are table stakes. Running a streaming platform is still a FAANG-only skillset.")

def slide_03_problem(c):
    draw_title_bar(c, "The real cost of real-time isn't the license. It's the pager.", kicker="the problem")

    # Left side: the pain
    draw_wrapped(c,
        "To detect fraud, personalize a feed, or give an AI agent live context, you need features "
        "computed the instant an event lands — counts over the last hour, averages over the last day, "
        "distinct merchants in the last week. Every tool that does this was built for Uber and Netflix. "
        "Running them takes a team that most growth-stage startups can't staff:",
        40, PAGE_H - 140, F_REG, 12, PAGE_W / 2 - 60, color=INK, leading=17)

    # Stack box (bad) — ops burden, not cost
    bx, by, bw, bh = 40, 180, PAGE_W / 2 - 60, 160
    c.setFillColor(BG_ALT)
    c.setStrokeColor(LINE)
    c.roundRect(bx, by, bw, bh, 8, stroke=1, fill=1)
    c.setFillColor(RED)
    c.setFont(F_BOLD, 11)
    c.drawString(bx + 16, by + bh - 22, "THE STATUS-QUO STACK")
    rows = [
        ("Kafka + Zookeeper", "event transport", "brokers, rebalances"),
        ("Flink or Spark Streaming", "stateful compute", "checkpoints, state TTL"),
        ("Feature store (Feast/Tecton)", "serving layer", "online/offline sync"),
        ("ML platform engineer(s)", "keep it all alive", "24/7 on-call"),
    ]
    c.setFont(F_REG, 10)
    for i, (name, role, burden) in enumerate(rows):
        yy = by + bh - 48 - i * 22
        c.setFillColor(INK); c.setFont(F_BOLD, 11)
        c.drawString(bx + 16, yy, name)
        c.setFillColor(SUBTLE); c.setFont(F_REG, 10)
        c.drawString(bx + 170, yy, role)
        c.setFillColor(NAVY); c.setFont(F_BOLD, 10)
        c.drawRightString(bx + bw - 16, yy, burden)
    c.setStrokeColor(LINE)
    c.line(bx + 16, by + 38, bx + bw - 16, by + 38)
    c.setFillColor(RED); c.setFont(F_BOLD, 13)
    c.drawString(bx + 16, by + 18, "4 systems. 3 engineers. A full-time pager rotation.")

    # Right side: consequences
    rx = PAGE_W / 2 + 20
    c.setFillColor(NAVY); c.setFont(F_BOLD, 14)
    c.drawString(rx, PAGE_H - 140, "So what do growth-stage teams actually do?")
    y = PAGE_H - 170
    for text in [
        "Run nightly batch pipelines — features are hours stale. Fraud already happened.",
        "Bolt rules onto Postgres triggers and cron jobs — fragile, invisible, impossible to test.",
        "Try to hire a streaming-infra specialist — takes 6 months, loses them to FAANG in 12.",
        "Skip real-time entirely — leave revenue, safety, and retention on the table.",
    ]:
        y = draw_bullet(c, rx, y, text, PAGE_W - rx - 40, size=12) - 8

    # Bottom sting
    c.setFillColor(RED)
    c.rect(rx, 90, PAGE_W - rx - 40, 52, stroke=0, fill=1)
    c.setFillColor(white); c.setFont(F_BOLD, 13)
    c.drawString(rx + 16, 118, "The gap is not talent. It's the operational burden.")
    c.setFillColor(HexColor("#FECACA")); c.setFont(F_REG, 11)
    c.drawString(rx + 16, 100, "Every growth-stage founder has hit this wall. Nobody has a clean answer yet.")

def slide_04_solution(c):
    draw_title_bar(c, "Streamlet: one binary, zero cluster, features in microseconds.", kicker="the solution")

    # Big descriptor
    draw_wrapped(c,
        "Streamlet is a single Rust binary you run on one machine. It speaks a custom TCP protocol: "
        "push an event, receive the updated features synchronously in the same round-trip. "
        "All state lives in memory. Periodic snapshots to local disk handle crash recovery.",
        40, PAGE_H - 140, F_REG, 13, PAGE_W - 80, color=INK, leading=18)

    # Three pillars
    y = 220
    pw = (PAGE_W - 80 - 2 * 16) / 3
    ph = 140
    pillars = [
        ("Zero engineering ops",
         "No Kafka, Flink, Zookeeper, or Redis sidecar. No platform team. No pager. "
         "Scaling model is trivial: when you need more, add RAM.",
         RED),
        ("Dead-easy developer experience",
         "One Python class is a production pipeline. No DAGs, no operators, no topology. "
         "A backend engineer ships their first feature in 10 minutes, not 10 days.",
         CYAN),
        ("Vibe-codeable with Claude",
         "Streamlet ships with Claude skills that author pipelines from plain-English specs. "
         "\"Build me a fraud detector for card transactions\" → valid, running code.",
         AMBER),
    ]
    for i, (title, body, col) in enumerate(pillars):
        x = 40 + i * (pw + 16)
        c.setFillColor(BG_ALT)
        c.setStrokeColor(LINE)
        c.roundRect(x, y, pw, ph, 8, stroke=1, fill=1)
        c.setFillColor(col)
        c.rect(x, y + ph - 4, pw, 4, stroke=0, fill=1)
        c.setFillColor(NAVY)
        c.setFont(F_BOLD, 15)
        c.drawString(x + 16, y + ph - 30, title)
        draw_wrapped(c, body, x + 16, y + ph - 56, F_REG, 10.5, pw - 32,
                     color=INK, leading=14)

    # Bottom punchline
    c.setFillColor(NAVY); c.setFont(F_BOLD, 14)
    c.drawString(40, 140,
                 "Cost to run in production:")
    c.setFillColor(RED); c.setFont(F_BOLD, 18)
    c.drawString(230, 140, "$20 – $2,000 / month.  Scales from seed to Series C on the same binary.")
    c.setFillColor(SUBTLE); c.setFont(F_REG, 11)
    c.drawString(40, 118, "Up to 100× cheaper than Kafka+Flink+Feature-Store — and handles the same load up to 100K events/sec on one thread.")

def slide_05_architecture(c):
    draw_title_bar(c, "How it works — inside the binary", kicker="architecture")

    # Center an architecture diagram
    # Column 1: Client → TCP
    # Column 2: Engine pipeline
    # Column 3: State + snapshots

    # Draw boxes
    def box(x, y, w, h, title, sub, color, title_color=white, sub_color=None):
        c.setFillColor(color)
        c.roundRect(x, y, w, h, 6, stroke=0, fill=1)
        c.setFillColor(title_color)
        c.setFont(F_BOLD, 11)
        c.drawString(x + 10, y + h - 18, title)
        c.setFillColor(sub_color or HexColor("#CBD5E1"))
        c.setFont(F_REG, 9)
        lines = wrap_text(c, sub, F_REG, 9, w - 20)
        for i, line in enumerate(lines[:3]):
            c.drawString(x + 10, y + h - 34 - i * 11, line)

    # Client column (left)
    cx = 50
    c.setFillColor(SUBTLE); c.setFont(F_BOLD, 9)
    c.drawString(cx, PAGE_H - 130, "CLIENT")
    box(cx, PAGE_H - 220, 140, 70, "Python SDK",
        "Declarative @st.stream definitions. Thin TCP client.", NAVY_SOFT)
    box(cx, PAGE_H - 310, 140, 70, "Any TCP client",
        "Go, Node, Rust, C++, Java — custom binary protocol.", NAVY_SOFT)

    # Arrow
    c.setStrokeColor(RED); c.setLineWidth(2)
    c.line(195, PAGE_H - 220, 235, PAGE_H - 220)
    c.line(195, PAGE_H - 275, 235, PAGE_H - 275)
    # arrowheads
    for yy in [PAGE_H - 220, PAGE_H - 275]:
        c.line(235, yy, 228, yy + 4)
        c.line(235, yy, 228, yy - 4)

    # Streamlet server (center big box)
    sx, sy, sw, sh = 240, 140, 480, PAGE_H - 240
    c.setFillColor(BG_ALT)
    c.setStrokeColor(NAVY); c.setLineWidth(1.5)
    c.roundRect(sx, sy, sw, sh, 10, stroke=1, fill=1)
    c.setFillColor(NAVY); c.setFont(F_BOLD, 11)
    c.drawString(sx + 16, sy + sh - 20, "STREAMLET SERVER  ·  single Rust binary")

    # Inner stack
    iw = sw - 32
    ix = sx + 16
    iy = sy + sh - 52
    box(ix, iy - 50, iw, 50, "TCP protocol  ·  PUSH / GET / SET / MSET / REGISTER",
        "Length-prefixed binary frames over persistent connections.", CYAN, title_color=white)
    box(ix, iy - 110, iw, 50, "Pipeline Engine",
        "Operators (count, sum, avg, min, max, distinct_count, last). Expression evaluator. Cross-stream views. Lookups.", NAVY)
    box(ix, iy - 170, iw, 50, "In-memory State Store",
        "HashMap<EntityKey, FeatureMap>. Bucketed ring-buffer windows. HyperLogLog. TTL eviction.", NAVY_SOFT)
    box(ix, iy - 230, iw, 50, "Snapshot Persistence",
        "Periodic serde+bincode dump to disk. Cooperative yielding. Load on startup for recovery.", HexColor("#475569"))

    # Right column: HTTP mgmt + disk
    rx = 740
    c.setFillColor(SUBTLE); c.setFont(F_BOLD, 9)
    c.drawString(rx, PAGE_H - 130, "OUT OF BAND")
    box(rx, PAGE_H - 220, 180, 70, "HTTP management API",
        "Register pipelines, /metrics, /health, debug inspection.", NAVY_SOFT)
    box(rx, PAGE_H - 310, 180, 70, "Local disk snapshot",
        "Versioned binary file. 30s default cadence. ~5s recovery / 1M keys.", NAVY_SOFT)

    # Arrows from server out
    c.setStrokeColor(CYAN); c.setLineWidth(2)
    c.line(720, PAGE_H - 220, 735, PAGE_H - 220)
    c.line(720, PAGE_H - 275, 735, PAGE_H - 275)

def slide_06_sdk(c):
    draw_title_bar(c, "Developer experience: features as code", kicker="python sdk")

    # Left: code block
    code = [
        [("com", "# Declarative feature definitions. No DAGs.")],
        [("kw", "import"), ("norm", " streamlet "), ("kw", "as"), ("norm", " st")],
        [("norm", "")],
        [("fn", "@st.stream"), ("norm", "(key="), ("str", '"user_id"'), ("norm", ")")],
        [("kw", "class"), ("fn", " Transactions"), ("norm", ":")],
        [("norm", "    tx_count_1h      = st."), ("fn", "count"), ("norm", "(window="), ("str", '"1h"'), ("norm", ")")],
        [("norm", "    tx_sum_1h        = st."), ("fn", "sum"), ("norm", "("), ("str", '"amount"'), ("norm", ", window="), ("str", '"1h"'), ("norm", ")")],
        [("norm", "    unique_merchants = st."), ("fn", "distinct_count"), ("norm", "("), ("str", '"merchant_id"'), ("norm", ",")],
        [("norm", "                                          window="), ("str", '"24h"'), ("norm", ")")],
        [("norm", "    failed_30m       = st."), ("fn", "count"), ("norm", "(window="), ("str", '"30m"'), ("norm", ",")],
        [("norm", "                               where="), ("str", '"status == \'failed\'"'), ("norm", ")")],
        [("norm", "    failure_rate     = st."), ("fn", "derive"), ("norm", "("), ("str", '"failed_30m / tx_count_1h"'), ("norm", ")")],
        [("norm", "    velocity_spike   = st."), ("fn", "derive"), ("norm", "("), ("str", '"tx_count_1h / (tx_count_24h/24)"'), ("norm", ")")],
        [("norm", "")],
        [("com", "# One round-trip. Event in, features out.")],
        [("norm", "features = app."), ("fn", "push"), ("norm", "(Transactions, {")],
        [("norm", "    "), ("str", '"user_id"'), ("norm", ": "), ("str", '"u_9182"'), ("norm", ", "), ("str", '"amount"'), ("norm", ": "), ("str", "1_250_000"), ("norm", ",")],
        [("norm", "    "), ("str", '"merchant_id"'), ("norm", ": "), ("str", '"m_stripe_42"'), ("norm", ", "), ("str", '"status"'), ("norm", ": "), ("str", '"success"'), ("norm", "})")],
        [("norm", "")],
        [("norm", "features.velocity_spike   "), ("com", "# 4.7  -> fraud engine blocks the tx")],
        [("norm", "features.failure_rate     "), ("com", "# 0.12 -> elevated, flag for review")],
    ]
    draw_code_block(c, 40, 130, PAGE_W / 2 - 30, PAGE_H - 260, code)

    # Right: why this matters
    rx = PAGE_W / 2 + 10
    c.setFillColor(NAVY); c.setFont(F_BOLD, 16)
    c.drawString(rx, PAGE_H - 140, "Why this matters")
    y = PAGE_H - 170
    for text in [
        "Feature definitions live next to application code, not in a separate YAML DSL or UI.",
        "Windows, filters, derived expressions, and cross-stream lookups in the same declarative block.",
        "The Python SDK never touches the hot path — it serializes definitions once and the Rust server does the rest.",
        "Same API a backend engineer at a Series B fintech or a solo founder at a seed company can pick up in 10 minutes.",
    ]:
        y = draw_bullet(c, rx, y, text, PAGE_W - rx - 40, size=12) - 10

    # Demo placeholder — where a short GIF / screenshot of the SDK in action should go
    draw_placeholder_badge(
        c, rx, 130, PAGE_W - rx - 40, 90,
        "PLACEHOLDER — DEMO",
        "Drop a 10-second animated GIF or screenshot here: Python REPL running `app.push(...)` and "
        "printing the updated feature map returned in the same call. Prove the round-trip is real. "
        "Export-ready asset: `deck/assets/demo_push.gif` (1200×500 px).")

def slide_07_innovation(c):
    draw_title_bar(c, "What makes it fast — the core technical bets", kicker="innovation")

    items = [
        ("Single-threaded core",
         "Like Redis. One thread, zero locks, zero contention. Predictable latency under load. "
         "Key-partitioned multi-threading is a drop-in upgrade for v2 because keys are independent."),
        ("Bucketed sliding windows",
         "Every windowed operator uses a fixed ring buffer of buckets. O(window/bucket) memory per key — "
         "a few KB regardless of event rate. On read: sum the non-expired buckets in a single tight loop."),
        ("HyperLogLog for distinct count",
         "Unique-merchant, unique-IP and unique-device counts in 12KB per key, forever, at any cardinality. "
         "Well-understood error bounds (~1%). Bounded memory is what makes zero-ops possible."),
        ("Server-side expression evaluator",
         "`derive` and `where` expressions are parsed at register-time into an AST and evaluated in Rust "
         "at event-time. Python never runs on the hot path — the guarantee that matters for latency."),
        ("Snapshot-based durability (Redis RDB model)",
         "Periodic serialize-to-disk with cooperative yielding. No WAL, no LSM tree. "
         "Losing 30 seconds of state on crash is an acceptable trade for an order-of-magnitude simplicity win."),
        ("Cross-stream views and cross-key lookups",
         "`tx_count / login_count` across streams, or `lookup(merchant.chargebacks, on='merchant_id')` "
         "from a user stream. Views recompute on read — no pre-materialization needed."),
    ]

    cols = 2
    col_w = (PAGE_W - 80 - 20) / 2
    row_h = 95
    y_top = PAGE_H - 140
    for i, (title, body) in enumerate(items):
        col = i % cols
        row = i // cols
        x = 40 + col * (col_w + 20)
        y = y_top - row * row_h
        # number
        c.setFillColor(RED)
        c.circle(x + 12, y - 12, 12, stroke=0, fill=1)
        c.setFillColor(white); c.setFont(F_BOLD, 12)
        c.drawCentredString(x + 12, y - 16, str(i + 1))
        # title
        c.setFillColor(NAVY); c.setFont(F_BOLD, 12)
        c.drawString(x + 32, y - 10, title)
        # body
        draw_wrapped(c, body, x + 32, y - 28, F_REG, 10, col_w - 32,
                     color=INK, leading=13)

def slide_08_performance(c):
    draw_title_bar(c, "Performance targets — sized for one machine", kicker="benchmarks")

    # Top stat row
    stats = [
        ("100K+", "events / sec\nsustained on one thread", RED),
        ("< 100 µs", "PUSH latency\np99, single-event", CYAN),
        ("< 50 µs", "GET latency\np99", AMBER),
        ("< 5 KB", "memory per key\n(10 mixed features)", MINT),
    ]
    card_w = (PAGE_W - 80 - 3 * 16) / 4
    card_h = 130
    y = PAGE_H - 280
    for i, (num, label, col) in enumerate(stats):
        x = 40 + i * (card_w + 16)
        c.setFillColor(BG_ALT)
        c.setStrokeColor(LINE)
        c.roundRect(x, y, card_w, card_h, 8, stroke=1, fill=1)
        c.setFillColor(col)
        c.rect(x, y + card_h - 5, card_w, 5, stroke=0, fill=1)
        c.setFillColor(NAVY); c.setFont(F_BOLD, 34)
        c.drawString(x + 16, y + card_h - 58, num)
        c.setFillColor(INK); c.setFont(F_REG, 11)
        for j, line in enumerate(label.split("\n")):
            c.drawString(x + 16, y + card_h - 82 - j * 14, line)

    # Recovery stats
    y2 = PAGE_H - 330
    c.setFillColor(NAVY); c.setFont(F_BOLD, 13)
    c.drawString(40, y2 - 18, "Plus:")
    c.setFillColor(INK); c.setFont(F_REG, 12)
    c.drawString(80, y2 - 18, "< 1s snapshot write per 1M keys   ·   < 5s recovery from disk on startup   ·   single-binary deployment")

    # Reference box
    rx, ry, rw, rh = 40, 100, PAGE_W - 80, 140
    c.setFillColor(BG_ALT)
    c.setStrokeColor(LINE)
    c.roundRect(rx, ry, rw, rh, 8, stroke=1, fill=1)
    c.setFillColor(RED); c.setFont(F_BOLD, 11)
    c.drawString(rx + 16, ry + rh - 22, "HOW WE GET THERE  ·  design choices that make these numbers achievable")
    bullets = [
        "In-memory HashMap lookups (no disk I/O on the hot path) — same class as Redis.",
        "Fixed-size ring buffer per operator — event handling is a branch and an increment, not an allocation.",
        "Custom length-prefixed binary protocol over persistent TCP — no HTTP header overhead per event.",
        "Single-threaded event loop — zero lock contention, cache-friendly, latency is a flat line under load.",
    ]
    yb = ry + rh - 44
    for b in bullets:
        yb = draw_bullet(c, rx + 16, yb, b, rw - 32, size=10.5) - 4

    # Placeholder banner — these are design targets, not measured yet
    draw_placeholder_badge(
        c, 40, 54, PAGE_W - 80, 40,
        "PLACEHOLDER — BENCHMARKS",
        "Numbers above are design targets derived from comparable systems (Redis, Fennel, Materialize). "
        "Replace with measured p50/p99 from the Streamlet bench harness (`cargo bench`) before submission.")

def slide_09_usecases(c):
    draw_title_bar(c, "One binary, four high-revenue verticals", kicker="use cases")

    cases = [
        ("Fintech fraud & risk",
         "Velocity, device, merchant, and geo features for every card authorization or payment. "
         "Think Stripe Radar, but as a primitive every payments startup can afford.",
         "1 instance handles 50M txns/day"),
        ("E-commerce personalization",
         "Live session signals for Shopify Plus, Series B marketplaces, and DTC brands — time on "
         "category, cart abandonment, last-viewed product — all sub-ms.",
         "Feature store without the feature store"),
        ("AI agent memory & context",
         "Every LLM agent startup needs rolling per-user state: last N intents, entity frequency, "
         "tool-call history, anomaly scores — returned in a single GET call.",
         "The missing primitive for 2026 AI products"),
        ("Ad-tech & real-time bidding",
         "Per-user frequency capping, budget pacing, and audience features at bid-request latency "
         "(<10 ms) without a Flink cluster and a four-person SRE rotation.",
         "Sub-10ms bid-time feature serving"),
    ]

    col_w = (PAGE_W - 80 - 20) / 2
    row_h = 160
    y_top = PAGE_H - 140
    colors = [RED, CYAN, AMBER, MINT]
    for i, (title, body, tag) in enumerate(cases):
        col = i % 2
        row = i // 2
        x = 40 + col * (col_w + 20)
        y = y_top - row * row_h
        c.setFillColor(BG_ALT)
        c.setStrokeColor(LINE)
        c.roundRect(x, y - row_h + 20, col_w, row_h - 20, 8, stroke=1, fill=1)
        c.setFillColor(colors[i])
        c.rect(x, y, col_w, 6, stroke=0, fill=1)
        c.setFillColor(NAVY); c.setFont(F_BOLD, 15)
        c.drawString(x + 16, y - 22, title)
        draw_wrapped(c, body, x + 16, y - 44, F_REG, 11, col_w - 32,
                     color=INK, leading=15)
        # tag pill
        c.setFillColor(colors[i])
        tw = c.stringWidth(tag, F_BOLD, 9) + 20
        c.roundRect(x + 16, y - row_h + 32, tw, 18, 9, stroke=0, fill=1)
        c.setFillColor(white); c.setFont(F_BOLD, 9)
        c.drawString(x + 26, y - row_h + 37, tag)

def slide_10_impact(c):
    draw_title_bar(c, "3 engineers → 0. Two weeks to first pipeline → two minutes.", kicker="impact")

    # Comparison block
    # Status quo (left) vs Streamlet (right)
    lx = 40; rx = PAGE_W / 2 + 10
    bw = PAGE_W / 2 - 60

    # Left: status quo
    c.setFillColor(BG_ALT); c.setStrokeColor(LINE)
    c.roundRect(lx, 180, bw, 240, 10, stroke=1, fill=1)
    c.setFillColor(SUBTLE); c.setFont(F_BOLD, 10)
    c.drawString(lx + 16, 400, "STATUS QUO")
    c.setFillColor(NAVY); c.setFont(F_BOLD, 22)
    c.drawString(lx + 16, 368, "Kafka + Flink + Feature store")
    c.setFillColor(RED); c.setFont(F_BOLD, 26)
    c.drawString(lx + 16, 320, "3 engineers  ·  2 weeks")
    c.setFillColor(INK); c.setFont(F_REG, 11)
    for i, line in enumerate([
        "Streaming-infra specialist required",
        "24/7 on-call pager rotation",
        "Weeks to stand up the first feature",
        "Expertise is scarce and non-transferable",
    ]):
        c.drawString(lx + 16, 290 - i * 18, "·  " + line)

    # Right: Streamlet
    c.setFillColor(NAVY)
    c.roundRect(rx, 180, bw, 240, 10, stroke=0, fill=1)
    # red accent bar
    c.setFillColor(RED)
    c.rect(rx, 414, bw, 6, stroke=0, fill=1)
    c.setFillColor(HexColor("#FCA5A5")); c.setFont(F_BOLD, 10)
    c.drawString(rx + 16, 396, "WITH STREAMLET")
    c.setFillColor(white); c.setFont(F_BOLD, 22)
    c.drawString(rx + 16, 364, "One Rust binary")
    c.setFillColor(AMBER); c.setFont(F_BOLD, 26)
    c.drawString(rx + 16, 316, "0 engineers  ·  2 minutes")
    c.setFillColor(HexColor("#CBD5E1")); c.setFont(F_REG, 11)
    for i, line in enumerate([
        "Any backend engineer, no specialist hire",
        "No pager — restart = full recovery",
        "Pipeline authored by Claude from a prompt",
        "Scale by adding RAM, not headcount",
    ]):
        c.drawString(rx + 16, 286 - i * 18, "·  " + line)

    # Bottom impact band
    by = 60; bh = 100
    c.setFillColor(BG_ALT)
    c.setStrokeColor(LINE)
    c.roundRect(40, by, PAGE_W - 80, bh, 10, stroke=1, fill=1)
    c.setFillColor(NAVY); c.setFont(F_BOLD, 13)
    c.drawString(60, by + bh - 22, "If 5,000 growth-stage startups adopt Streamlet over 5 years:")
    c.setFillColor(INK); c.setFont(F_REG, 11.5)
    c.drawString(60, by + bh - 44,
        "·  ~15,000 engineer-years redirected from platform ops into product")
    c.drawString(60, by + bh - 60,
        "·  Real-time fraud, personalization, and AI-agent memory become standard, not luxury")
    c.drawString(60, by + bh - 76,
        "·  Vietnam becomes the origin of a globally-used developer primitive — our VAS contribution")

def slide_11_gtm(c):
    draw_title_bar(c, "Four-phase rollout: OSS-first, Vietnam beachhead, global target", kicker="go to market")

    phases = [
        ("Phase 1",
         "Open source launch",
         "MIT-licensed core on GitHub. Launch posts on Hacker News, /r/rust, /r/MachineLearning, "
         "Console DevTools, TLDR. Target: 2,000 GitHub stars, 20 contributors in Q2.",
         RED, "Q2 2026"),
        ("Phase 2",
         "Vietnam design partners",
         "3–5 Vietnamese fintech and AI startups run Streamlet in production in exchange for "
         "feedback and case studies. Leverages the founder's local network as a fast-feedback beachhead.",
         CYAN, "Q3 2026"),
        ("Phase 3",
         "Managed Cloud (global)",
         "Streamlet Cloud on AWS, GCP, and FPT Cloud. Fully managed, pay-per-usage. "
         "Primary revenue channel — targeting Series A/B startups anywhere in the world.",
         AMBER, "Q4 2026"),
        ("Phase 4",
         "Enterprise on-prem",
         "Air-gapped deployments for banks, insurers, and telcos globally. SOC2 / ISO 27001 track. "
         "$25–100K ACV annual support contracts with SLA.",
         MINT, "2027"),
    ]

    # Timeline: horizontal rail with 4 nodes
    rail_y = PAGE_H - 200
    c.setStrokeColor(LINE); c.setLineWidth(3)
    c.line(80, rail_y, PAGE_W - 80, rail_y)

    step_w = (PAGE_W - 160) / 3
    for i, (label, title, body, col, when) in enumerate(phases):
        cx = 80 + i * step_w
        # node
        c.setFillColor(col)
        c.circle(cx, rail_y, 12, stroke=0, fill=1)
        c.setFillColor(white); c.setFont(F_BOLD, 11)
        c.drawCentredString(cx, rail_y - 4, str(i + 1))
        # when pill above
        c.setFillColor(col); c.setFont(F_BOLD, 9)
        c.drawCentredString(cx, rail_y + 22, when)
        c.setFillColor(NAVY); c.setFont(F_BOLD, 11)
        c.drawCentredString(cx, rail_y + 40, label)

    # Below rail: cards
    cy = rail_y - 40
    ch = 130
    cw = step_w - 20
    for i, (label, title, body, col, when) in enumerate(phases):
        cx = 80 + i * step_w - cw / 2
        c.setFillColor(BG_ALT); c.setStrokeColor(LINE)
        c.roundRect(cx, cy - ch, cw, ch, 8, stroke=1, fill=1)
        c.setFillColor(col)
        c.rect(cx, cy - 4, cw, 4, stroke=0, fill=1)
        c.setFillColor(NAVY); c.setFont(F_BOLD, 12)
        c.drawString(cx + 12, cy - 24, title)
        draw_wrapped(c, body, cx + 12, cy - 42, F_REG, 9.5, cw - 24,
                     color=INK, leading=12)

def slide_12_status(c):
    draw_title_bar(c, "Project status — core engine is real", kicker="current state & roadmap")

    # Left: done
    c.setFillColor(BG_ALT); c.setStrokeColor(LINE)
    lw = PAGE_W / 2 - 60
    c.roundRect(40, 100, lw, PAGE_H - 260, 10, stroke=1, fill=1)
    c.setFillColor(MINT); c.setFont(F_BOLD, 11)
    c.drawString(60, PAGE_H - 170, "SHIPPED  ·  in the repo today")
    done = [
        ("Core state store", "HashMap<EntityKey, FeatureMap> with TTL eviction, live + static features."),
        ("Bucketed sliding windows", "Ring buffer implementation powering count, sum, avg, min, max."),
        ("HyperLogLog operator", "Distinct count with bounded 12KB memory per key."),
        ("Expression parser + evaluator", "Server-side AST for derive / where with arithmetic and booleans."),
        ("TCP protocol + server", "Length-prefixed binary frames. PUSH, GET, SET, MSET, REGISTER commands."),
        ("Snapshot persistence", "serde+bincode periodic dumps with cooperative yielding. Recovery on startup."),
        ("Cross-stream views & lookups", "derive() across streams, lookup() across keys."),
        ("Claude skills for pipeline authoring", "Shipped: Claude writes valid @st.stream pipelines from plain-English specs. Vibe-code real-time features."),
    ]
    y = PAGE_H - 200
    for title, body in done:
        c.setFillColor(MINT)
        c.rect(58, y + 2, 6, 6, stroke=0, fill=1)
        c.setFillColor(NAVY); c.setFont(F_BOLD, 10.5)
        c.drawString(72, y + 2, title)
        c.setFillColor(INK); c.setFont(F_REG, 10)
        y = draw_wrapped(c, body, 72, y - 12, F_REG, 10, lw - 50, leading=12) - 6

    # Right: next
    rx = PAGE_W / 2 + 10
    rw = PAGE_W / 2 - 50
    c.setFillColor(NAVY)
    c.roundRect(rx, 100, rw, PAGE_H - 260, 10, stroke=0, fill=1)
    c.setFillColor(HexColor("#FCA5A5")); c.setFont(F_BOLD, 11)
    c.drawString(rx + 20, PAGE_H - 170, "NEXT  ·  competition timeline")
    nxt = [
        ("Q2 2026", "Python SDK v1 + first design partner deploy", RED),
        ("Q2 2026", "Incremental snapshots (phase 9, in planning)", AMBER),
        ("Q3 2026", "Benchmark harness + public perf report", CYAN),
        ("Q3 2026", "Open source launch + Vietnamese docs", RED),
        ("Q4 2026", "Managed Cloud private beta on FPT Cloud", AMBER),
        ("Q4 2026", "10 production deployments milestone", MINT),
    ]
    y = PAGE_H - 210
    for when, title, col in nxt:
        c.setFillColor(col); c.setFont(F_BOLD, 9)
        c.drawString(rx + 20, y, when.upper())
        c.setFillColor(white); c.setFont(F_BOLD, 12)
        c.drawString(rx + 90, y, title)
        y -= 34

    # Bottom feasibility note
    c.setFillColor(BG_ALT); c.setStrokeColor(LINE)
    c.roundRect(40, 40, PAGE_W - 80, 50, 6, stroke=1, fill=1)
    c.setFillColor(RED); c.setFont(F_BOLD, 10)
    c.drawString(60, 68, "FEASIBILITY")
    c.setFillColor(INK); c.setFont(F_REG, 11)
    c.drawString(60, 50,
        "No external data needed (customers bring their own events). No ML model to train. "
        "Deploys on any Linux box. Founder has built the exact same class of system in production.")

def slide_13_team(c):
    draw_title_bar(c, "Built by someone who built this before", kicker="team")

    # Left: Hoang profile
    lx = 40; ly = 140; lw = 280; lh = PAGE_H - 260
    c.setFillColor(NAVY)
    c.roundRect(lx, ly, lw, lh, 10, stroke=0, fill=1)
    # Red bar
    c.setFillColor(RED)
    c.rect(lx, ly + lh - 6, lw, 6, stroke=0, fill=1)
    # Avatar placeholder
    c.setFillColor(HexColor("#1E3A5F"))
    c.circle(lx + lw / 2, ly + lh - 90, 44, stroke=0, fill=1)
    c.setFillColor(white); c.setFont(F_BOLD, 36)
    c.drawCentredString(lx + lw / 2, ly + lh - 102, "HP")
    # Name
    c.setFillColor(white); c.setFont(F_BOLD, 22)
    c.drawCentredString(lx + lw / 2, ly + lh - 170, "Hoang Phan")
    c.setFillColor(HexColor("#CBD5E1")); c.setFont(F_REG, 12)
    c.drawCentredString(lx + lw / 2, ly + lh - 190, "Founder & Engineering Lead")
    # Divider
    c.setStrokeColor(RED); c.setLineWidth(2)
    c.line(lx + 80, ly + lh - 210, lx + lw - 80, ly + lh - 210)
    # Contact
    c.setFillColor(HexColor("#94A3B8")); c.setFont(F_REG, 10)
    c.drawCentredString(lx + lw / 2, ly + lh - 230, "University of Waterloo, BCS")
    c.drawCentredString(lx + lw / 2, ly + lh - 245, "Minor in Finance")
    # Skills pills
    pills = ["Rust", "C++", "Python", "Distributed Systems", "Streaming", "RocksDB"]
    py = ly + 60
    px = lx + 20
    for p in pills:
        pw = c.stringWidth(p, F_BOLD, 9) + 16
        c.setFillColor(HexColor("#1E3A5F"))
        c.roundRect(px, py, pw, 16, 8, stroke=0, fill=1)
        c.setFillColor(white); c.setFont(F_BOLD, 9)
        c.drawString(px + 8, py + 4, p)
        px += pw + 6
        if px + 60 > lx + lw - 10:
            px = lx + 20
            py -= 22

    # Right: experience highlights
    rx = 340
    c.setFillColor(NAVY); c.setFont(F_BOLD, 14)
    c.drawString(rx, PAGE_H - 140, "Why this team, for this project")

    items = [
        ("Fennel AI  ·  acquired by Databricks (2024–2025)",
         "Distributed Systems Engineer on the exact same class of system Streamlet is. "
         "Re-architected streaming join and window operators for 10× runtime and memory efficiency. "
         "Designed approximate percentile data structures that landed 5 enterprise customers.",
         RED),
        ("Viggle AI  ·  Engineering Lead, Toronto (2025–present)",
         "Leading a 4-engineer team building real-time inference infrastructure. "
         "Reduced time-to-first-chunk from 8s to 2s, cut $1M+ in annual cloud spend, "
         "scaled the office from 1 to 8 engineers.",
         CYAN),
        ("Faire  ·  ML Platform Engineer (2022–2023)",
         "Built the self-serve deep-learning deployment path, cut Snowflake spend 3× "
         "via incremental transformations, built fraud-review product-quality signals that raised "
         "manual-review precision by 10%.",
         AMBER),
        ("Google  ·  SWE Intern (2021)   ·   APIO Silver Medal — 2nd place in Vietnam",
         "Ranked 35/164 across 30 countries at the Asia-Pacific Informatics Olympiad. "
         "Vietnamese-origin founder with a decade of systems and competitive programming foundation.",
         MINT),
    ]
    y = PAGE_H - 170
    for title, body, col in items:
        # dot
        c.setFillColor(col)
        c.rect(rx, y + 5, 4, 36, stroke=0, fill=1)
        c.setFillColor(NAVY); c.setFont(F_BOLD, 11.5)
        c.drawString(rx + 12, y + 28, title)
        draw_wrapped(c, body, rx + 12, y + 12, F_REG, 10, PAGE_W - rx - 60,
                     color=INK, leading=12)
        y -= 92

def slide_14_business(c):
    draw_title_bar(c, "Open-core: adoption first, revenue second", kicker="business model")

    # Three tiers
    tiers = [
        ("Open Source", "MIT-licensed core", "$0", [
            "Full server + Python SDK",
            "Single-binary deploy",
            "Community support (Discord, GitHub)",
            "For hobbyists, students, OSS projects"
        ], "Adoption engine", SUBTLE),
        ("Cloud", "Managed on AWS, GCP, FPT Cloud", "$99 – 2,000 / mo", [
            "Fully managed instance",
            "Automated snapshots + backups",
            "Dashboards and alerting",
            "For Series A–C startups globally"
        ], "Primary revenue", RED),
        ("Enterprise", "On-prem + support", "$25K – 100K / yr", [
            "Air-gapped deployment",
            "SLA-backed support contract",
            "SOC2 / ISO 27001 assistance",
            "For banks, insurers, telcos"
        ], "Highest ACV", NAVY),
    ]
    tw = (PAGE_W - 80 - 40) / 3
    th = 280
    ty = 130
    for i, (name, sub, price, feats, tag, col) in enumerate(tiers):
        tx = 40 + i * (tw + 20)
        c.setFillColor(BG_ALT); c.setStrokeColor(LINE)
        c.roundRect(tx, ty, tw, th, 10, stroke=1, fill=1)
        # accent
        c.setFillColor(col)
        c.rect(tx, ty + th - 6, tw, 6, stroke=0, fill=1)
        # tag pill top
        tag_w = c.stringWidth(tag, F_BOLD, 9) + 16
        c.setFillColor(col)
        c.roundRect(tx + tw - tag_w - 12, ty + th - 28, tag_w, 16, 8, stroke=0, fill=1)
        c.setFillColor(white); c.setFont(F_BOLD, 9)
        c.drawString(tx + tw - tag_w - 4, ty + th - 24, tag)
        # name
        c.setFillColor(NAVY); c.setFont(F_BOLD, 18)
        c.drawString(tx + 16, ty + th - 54, name)
        # sub
        c.setFillColor(SUBTLE); c.setFont(F_REG, 10)
        c.drawString(tx + 16, ty + th - 72, sub)
        # price
        c.setFillColor(col); c.setFont(F_BOLD, 20)
        c.drawString(tx + 16, ty + th - 108, price)
        # divider
        c.setStrokeColor(LINE)
        c.line(tx + 16, ty + th - 120, tx + tw - 16, ty + th - 120)
        # features
        y = ty + th - 140
        c.setFont(F_REG, 10.5)
        for f in feats:
            c.setFillColor(col)
            c.rect(tx + 16, y + 3, 5, 5, stroke=0, fill=1)
            c.setFillColor(INK)
            c.drawString(tx + 28, y, f)
            y -= 18

    # Bottom: comparable companies
    c.setFillColor(NAVY); c.setFont(F_BOLD, 12)
    c.drawString(40, 95, "Comparable open-core companies (same shape, different primitive):")
    c.setFillColor(INK); c.setFont(F_REG, 11)
    c.drawString(40, 75, "·  Redis Labs  →  $2B valuation   ·  ClickHouse  →  $2B valuation   ·  Confluent (Kafka)  →  public, $7B market cap")
    c.setFillColor(SUBTLE); c.setFont(F_REG, 10)
    c.drawString(40, 58, "Streamlet is to real-time features what Redis is to in-memory data.")

def slide_15_vision(c):
    # Full bleed navy
    c.setFillColor(NAVY)
    c.rect(0, 0, PAGE_W, PAGE_H, stroke=0, fill=1)
    c.setFillColor(RED)
    c.rect(0, 0, 12, PAGE_H, stroke=0, fill=1)

    # Kicker
    c.setFillColor(RED); c.setFont(F_BOLD, 12)
    c.drawString(60, PAGE_H - 80, "THE VISION")

    # Big vision statement
    c.setFillColor(white); c.setFont(F_BOLD, 42)
    c.drawString(60, PAGE_H - 150, "Zero-ops, AI-authored")
    c.drawString(60, PAGE_H - 200, "real-time infrastructure.")

    c.setFillColor(HexColor("#94A3B8")); c.setFont(F_REG, 16)
    c.drawString(60, PAGE_H - 240,
                 "Streamlet is the default real-time feature primitive for the next 50,000 growth-stage startups —")
    c.drawString(60, PAGE_H - 262,
                 "one binary to run, plain-English specs to author. Built out of Vietnam, used everywhere.")

    # Three asks
    c.setFillColor(RED); c.setFont(F_BOLD, 11)
    c.drawString(60, 240, "WHAT WE ARE ASKING FOR")

    asks = [
        ("Mentorship", "Guidance from VAS judges and mentors on taking a Vietnam-origin developer-tools startup to a global audience."),
        ("Design partners", "Warm introductions to 3–5 growth-stage fintech, e-commerce or AI startups — Vietnamese or global — willing to be first production users."),
        ("Seed capital", "A small pre-seed round to fund a second engineer and launch the managed-cloud beta."),
    ]
    ax = 60
    aw = (PAGE_W - 120 - 40) / 3
    ah = 140
    ay = 80
    colors = [RED, CYAN, AMBER]
    for i, (title, body) in enumerate(asks):
        x = ax + i * (aw + 20)
        c.setFillColor(HexColor("#1E3A5F"))
        c.roundRect(x, ay, aw, ah, 8, stroke=0, fill=1)
        c.setFillColor(colors[i])
        c.rect(x, ay + ah - 4, aw, 4, stroke=0, fill=1)
        c.setFillColor(white); c.setFont(F_BOLD, 14)
        c.drawString(x + 16, ay + ah - 30, title)
        draw_wrapped(c, body, x + 16, ay + ah - 52, F_REG, 11, aw - 32,
                     color=HexColor("#CBD5E1"), leading=14)

    # Thanks line bottom-right
    c.setFillColor(HexColor("#64748B")); c.setFont(F_REG, 10)
    c.drawRightString(PAGE_W - 40, 40, "Cảm ơn — Thank you.")
    c.drawRightString(PAGE_W - 40, 26, "phan.minhhoang2606@gmail.com  ·  github.com/petrpan26")

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
SLIDES = [
    ("title", slide_01_title, False),
    ("opportunity", slide_02_opportunity, True),
    ("problem", slide_03_problem, True),
    ("solution", slide_04_solution, True),
    ("architecture", slide_05_architecture, True),
    ("sdk", slide_06_sdk, True),
    ("innovation", slide_07_innovation, True),
    ("performance", slide_08_performance, True),
    ("use_cases", slide_09_usecases, True),
    ("impact", slide_10_impact, True),
    ("gtm", slide_11_gtm, True),
    ("status", slide_12_status, True),
    ("team", slide_13_team, True),
    ("business", slide_14_business, True),
    ("vision", slide_15_vision, False),
]

def build(path):
    c = canvas.Canvas(path, pagesize=PAGE_SIZE)
    c.setTitle("Streamlet — Vietnam AI Stars 2026")
    c.setAuthor("Hoang Phan")
    c.setSubject("Pitch deck for Vietnam AI Stars 2026 competition")

    for i, (name, fn, show_footer) in enumerate(SLIDES):
        fn(c)
        if show_footer:
            draw_footer(c, i + 1, len(SLIDES))
        c.showPage()

    c.save()
    print(f"Wrote {path}  ·  {len(SLIDES)} slides")


if __name__ == "__main__":
    import os
    out = os.path.join(os.path.dirname(__file__), "Streamlet_VAS2026_Deck.pdf")
    build(out)
