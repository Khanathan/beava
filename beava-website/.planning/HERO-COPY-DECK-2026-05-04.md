# Hero — copy & layout spec
# beava.dev landing page redesign · 2026-05-04

The hero is a two-column grid. Left = ~55% width (text), right = ~45% width
(live metrics panel). Stacks to single column on mobile.

Everything outside the hero stays as-is, except:
  • The cloud waitlist banner at the top of every page → DELETE
  • The PipelineShowcase code block further down the page → SWAP (see end of doc)


===============================================================================
LEFT COLUMN — top to bottom, in order
===============================================================================

────────────────────────────────────────────────────────────────────────────────
1.  STATUS PILL
────────────────────────────────────────────────────────────────────────────────
Copy:   v0.9.4 · Apache 2.0 · single binary
Style:  Existing pill (warm orange wash background, accent-colored dot,
        small sans, accent-colored text)


────────────────────────────────────────────────────────────────────────────────
2.  EYEBROW (sits above H1)
────────────────────────────────────────────────────────────────────────────────
Copy:   Dam good at streams.
Style:  Hand-drawn accent font (Gaegu), accent orange, ~22-28px, slight
        -2° rotation. Much smaller than today's huge headline.


────────────────────────────────────────────────────────────────────────────────
3.  H1 — primary headline
────────────────────────────────────────────────────────────────────────────────
Copy:   Real-time features without heavy infrastructure.
Style:  Serif (Alegreya), 600 weight, plain (NO rotation, NO hand-drawn font).
        Size: clamp(40px, 5vw, 64px). Tight line-height. Letter-spacing -0.02em.


────────────────────────────────────────────────────────────────────────────────
4.  EMPATHY LINE (sits between H1 and lede)
────────────────────────────────────────────────────────────────────────────────
Copy:   If your last 'simple' feature ate a sprint to deploy, you know why we
        built this.
Style:  Plain sans, fg2 color, ~16-18px, max-width ~560px. Slightly muted vs
        the H1 above it. No bold, no italic — reads as a quiet aside, not a
        pitch.


────────────────────────────────────────────────────────────────────────────────
5.  LEDE PARAGRAPH
────────────────────────────────────────────────────────────────────────────────
Copy:   Personalization, fraud rules, live dashboards — in hours, not quarters.
Style:  Italic serif, 20-24px, fg1 color, max-width ~560px.
Note:   The 3-step plan ("Push events. Maintain tables. Query by key.") moved
        out of the lede into its own strip beneath the install tabs (see #7).


────────────────────────────────────────────────────────────────────────────────
6.  INSTALL TABS  (existing component, with new eyebrow)
────────────────────────────────────────────────────────────────────────────────
Eyebrow above the tabs:
        Run it locally.
Style:  Small sans, fg2, ~14px, sits 8-12px above the tab row.

Tabs:   [brew]  [curl]  [docker]      ← brew is the default tab
Snippets per tab:
  brew    →   brew install beava
  curl    →   curl -fsSL beava.dev/install.sh | sh
  docker  →   docker run -p 6400:6400 beava/beava:latest

Caption below the tabs:
        ~14 MB · macOS, Linux, Windows · runs on 1 GB RAM · scales to one big box


────────────────────────────────────────────────────────────────────────────────
7.  3-STEP STRIP (sits beneath the install tabs caption)
────────────────────────────────────────────────────────────────────────────────
Copy:   Push events · Maintain tables · Query by key.
Style:  Single horizontal line. Plain sans, ~14-15px, fg2 color. Middot
        separators between the three verbs. Sits 16-20px below the install-tabs
        caption. Reads as a confident shorthand, not a tutorial — no numbers,
        no boxes.


────────────────────────────────────────────────────────────────────────────────
8.  SINGLE PRIMARY CTA
────────────────────────────────────────────────────────────────────────────────
Button: Read the guide →
Links:  /guide/
Style:  Primary button (accent orange fill, cream text, large)

REMOVED from hero:
  • "Star on GitHub" button       (kept only in the FinalCTA at page bottom)
  • "psst → click the beaver" hint  (no beaver to click anymore)


===============================================================================
RIGHT COLUMN — LiveMetrics panel
===============================================================================

Three cards stacked vertically. Each card: small uppercase label on top,
big number in the middle, tiny sparkline at the bottom (where applicable).

Card background: white, soft border, subtle shadow. Generous padding.
Cards refresh every ~5 seconds from the live beava query.


────────────────────────────────────────────────────────────────────────────────
CARD 1
────────────────────────────────────────────────────────────────────────────────
Label:      AVG TIME ON /docs/ · LAST HOUR
Number:     2m 14s              ← example; real value comes from beava
Sparkline:  60 data points, one per minute over the last hour


────────────────────────────────────────────────────────────────────────────────
CARD 2
────────────────────────────────────────────────────────────────────────────────
Label:      PAGES VIEWED · TODAY
Number:     1,247               ← example; real value comes from beava
Sparkline:  24 data points, one per hour over the last 24 hours


────────────────────────────────────────────────────────────────────────────────
CARD 3
────────────────────────────────────────────────────────────────────────────────
Label:     TOP PAGE · LAST HOUR
Number:    /docs/get-started/quickstart/   ← real path string from beava
Subline:   382 views                       ← optional, smaller, fg3
Sparkline: omit (the value is a string, not a number)


────────────────────────────────────────────────────────────────────────────────
CAPTION beneath all three cards
────────────────────────────────────────────────────────────────────────────────
Copy:   Three real beava queries. The pipeline is below.
Style:  Small italic sans, fg3, centered or left-aligned to match cards.


===============================================================================
ELSEWHERE ON THE PAGE — small adjustments tied to this redesign
===============================================================================

DELETE — Cloud waitlist banner at the top of every page
────────────────────────────────────────────────────────────────────────────────
The black bar reading:
  "beava cloud · managed from $50/mo · self-host is always free · join the
   waitlist"
Remove it entirely.


SWAP — PipelineShowcase section (further down the page)
────────────────────────────────────────────────────────────────────────────────
The section currently shows the FeedClick / Session / Global pipeline that
backed the deleted FeedBeaver mascot. Replace it with the SiteMetrics pipeline
that produces the three numbers in the hero panel. Hero ↔ code now match.

  Section eyebrow:  The homepage runs beava
  Section H2:       13 lines. That's the whole pipeline.
                    (was "18 lines.")
  Section lede:     Every page view on this site pushes a real event to this
                    pipeline. Every number in the hero panel is a real feature
                    query against it.
                    (was a FeedBeaver-specific paragraph)

  Code block:

      import beava as bv

      @bv.stream
      class PageView:
          session_id: str
          path: str
          dwell_ms: int   # set when the visitor leaves the page

      @bv.table(key="__global__")
      def SiteMetrics(e: PageView):
          return e.agg(
              avg_dwell_docs_1h = bv.avg(e.dwell_ms, window="1h",
                                         where="_event.path.startswith('/docs/')"),
              page_views_today  = bv.count(window="24h"),
              top_page_1h       = bv.top_k(e.path, k=1, window="1h"),
          )

      bv.App("0.0.0.0:6400").register(PageView, SiteMetrics).serve()

  Footnote (replaces the "no cookies" footnote that was FeedBeaver-specific):
      No tracking cookies, no fingerprinting. Anonymous session id per visit.


===============================================================================
ADJACENT TO THE HERO — Pillars section eyebrow
===============================================================================

The values line surfaces as the Pillars section eyebrow (the strip directly
below the hero). Today the eyebrow says "What's different". Replace with:

  Eyebrow:  Stream processing shouldn't require a platform team.
  Style:    Same eyebrow style used elsewhere — small uppercase sans, accent
            orange, letter-spacing 0.08em. Sits centered above the section H2.

The 4 pillar cards underneath remain unchanged:
  REPLACES REDIS + LUA / NO STREAMING STACK / CRASH-SAFE BY DEFAULT /
  APACHE 2.0, FOREVER


===============================================================================
WHAT STAYS UNCHANGED — for reference
===============================================================================

  • Nav (top of page, with search bar on docs only)
  • Pillars section — 4 cards: REPLACES REDIS + LUA / NO STREAMING STACK /
    CRASH-SAFE BY DEFAULT / APACHE 2.0, FOREVER
  • RecipeGrid section — 5 cards: Personalization / Fraud detection /
    Leaderboard / Rate limiting / Usage metering
  • GuideTeaser — Chapter 1 callout
  • TrustBand — 3 stats
  • FinalCTA — install tabs (repeat) + Star on GitHub + Discussions link
  • Footer

These are out of scope for this redesign. If we do another pass later, the
Pillars and RecipeGrid being back-to-back grids is the next thing to look at.


===============================================================================
MOBILE
===============================================================================

  • Two-column grid collapses to single column
  • LiveMetrics panel stacks below the text block
  • Cards shrink but stay legible
  • Sparklines can drop on cards 1 and 2 if vertical space gets tight
  • The H1 size scales down via clamp() — already specced
