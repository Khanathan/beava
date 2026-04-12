---
marp: true
size: 16:9
paginate: true
footer: 'Tally  ·  Vietnam AI Stars 2026'
theme: default
html: true
style: |
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=Instrument+Serif:ital@0;1&family=JetBrains+Mono:wght@400;500&display=swap');

  :root {
    --paper: #FAFAF5;
    --paper-alt: #F3EFE6;
    --ink: #0A0A0A;
    --ink-soft: #1F1F1F;
    --ink-muted: #6B6B6B;
    --ink-light: #9B9B9B;
    --line: #E8E4DC;
    --line-strong: #D4CFC5;
    --accent: #E63946;
    --accent-deep: #C8202F;
    --accent-soft: #FEE5E8;
    --code-bg: #0E0E10;
    --code-text: #F4F4F5;
  }

  section {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    font-size: 19px;
    background: var(--paper);
    color: var(--ink);
    padding: 56px 72px 80px;
    line-height: 1.5;
    letter-spacing: -0.005em;
    position: relative;
  }

  section h1 {
    font-family: 'Instrument Serif', Georgia, 'Times New Roman', serif;
    font-size: 52px;
    color: var(--ink);
    font-weight: 400;
    letter-spacing: -0.015em;
    margin: 0 0 14px;
    line-height: 1.08;
  }
  section h2 {
    font-family: 'Instrument Serif', Georgia, serif;
    font-size: 30px;
    color: var(--ink);
    font-weight: 400;
    letter-spacing: -0.005em;
    margin: 0 0 16px;
    line-height: 1.15;
  }
  section h3 {
    font-family: 'Inter', sans-serif;
    font-size: 14.5px;
    color: var(--ink);
    font-weight: 700;
    margin: 0 0 8px;
    letter-spacing: -0.003em;
  }
  section p { margin: 0 0 10px; font-size: 14.5px; color: var(--ink-soft); }
  section strong { color: var(--ink); font-weight: 700; }
  section em {
    font-family: 'Instrument Serif', Georgia, serif;
    font-style: italic;
    font-size: 1.05em;
    color: var(--ink);
  }
  section code {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.92em;
    background: var(--paper-alt);
    padding: 1px 5px;
    border-radius: 2px;
    color: var(--accent-deep);
  }

  section ul { padding-left: 0; list-style: none; margin: 8px 0; }
  section ul li {
    position: relative;
    padding-left: 20px;
    margin-bottom: 7px;
    font-size: 13.5px;
    line-height: 1.5;
    color: var(--ink-soft);
  }
  section ul li::before {
    content: '';
    position: absolute;
    left: 0; top: 9px;
    width: 10px; height: 1px;
    background: var(--accent);
  }

  section footer {
    color: var(--ink-muted);
    font-size: 10px;
    left: 72px;
    bottom: 26px;
    font-weight: 500;
    letter-spacing: 0.1em;
    text-transform: uppercase;
  }
  section::after {
    color: var(--ink-muted);
    font-size: 10px;
    right: 72px;
    bottom: 26px;
    font-weight: 500;
  }

  /* ── Tally mark motif on content slides ─────────── */
  section.content::before {
    content: '';
    position: absolute;
    right: 108px;
    bottom: 22px;
    width: 38px;
    height: 18px;
    background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 38 18'><line x1='3' y1='2' x2='3' y2='16' stroke='%23E63946' stroke-width='1.6' stroke-linecap='round'/><line x1='11' y1='2' x2='11' y2='16' stroke='%23E63946' stroke-width='1.6' stroke-linecap='round'/><line x1='19' y1='2' x2='19' y2='16' stroke='%23E63946' stroke-width='1.6' stroke-linecap='round'/><line x1='27' y1='2' x2='27' y2='16' stroke='%23E63946' stroke-width='1.6' stroke-linecap='round'/><line x1='0' y1='16' x2='34' y2='2' stroke='%23E63946' stroke-width='1.6' stroke-linecap='round'/></svg>");
    background-repeat: no-repeat;
    background-size: contain;
  }

  /* ── Kicker + divider ───────────────────────────── */
  .kicker {
    color: var(--accent);
    font-size: 10.5px;
    font-weight: 700;
    letter-spacing: 0.22em;
    text-transform: uppercase;
    margin-bottom: 12px;
    font-family: 'Inter', sans-serif;
  }
  .title-divider {
    height: 1px;
    background: var(--line);
    margin: 18px 0 26px;
  }

  /* ── Hero slide ─────────────────────────────────── */
  section.hero, section.vision {
    background: var(--paper);
    color: var(--ink);
    padding: 72px 88px 72px 100px;
    position: relative;
  }
  section.hero::before, section.vision::before {
    content: '';
    position: absolute;
    left: 0; top: 0; bottom: 0;
    width: 12px;
    background: var(--accent);
  }
  section.hero footer, section.hero::after,
  section.vision footer, section.vision::after { display: none; }

  .hero-title {
    font-family: 'Instrument Serif', Georgia, serif;
    font-size: 148px;
    font-weight: 400;
    color: var(--ink);
    letter-spacing: -0.035em;
    line-height: 0.92;
    margin: 14px 0 30px;
  }
  .hero-tagline {
    font-family: 'Instrument Serif', Georgia, serif;
    font-style: italic;
    color: var(--ink);
    font-size: 38px;
    font-weight: 400;
    letter-spacing: -0.01em;
    line-height: 1.18;
    max-width: 920px;
  }
  .hero-descriptor {
    color: var(--ink-muted);
    font-size: 16px;
    margin-top: 36px;
    line-height: 1.65;
    max-width: 780px;
    font-family: 'Inter', sans-serif;
  }
  .hero-author {
    position: absolute;
    left: 100px;
    bottom: 68px;
  }
  .hero-author .rule {
    width: 44px; height: 2px;
    background: var(--accent);
    margin-bottom: 14px;
  }
  .hero-author .name {
    color: var(--ink);
    font-size: 17px;
    font-weight: 700;
  }
  .hero-author .meta {
    color: var(--ink-muted);
    font-size: 11.5px;
    margin-top: 4px;
    letter-spacing: 0.01em;
  }
  .hero-tally {
    position: absolute;
    right: 100px;
    bottom: 68px;
    width: 60px;
    height: 26px;
    background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 38 18'><line x1='3' y1='2' x2='3' y2='16' stroke='%23E63946' stroke-width='1.6' stroke-linecap='round'/><line x1='11' y1='2' x2='11' y2='16' stroke='%23E63946' stroke-width='1.6' stroke-linecap='round'/><line x1='19' y1='2' x2='19' y2='16' stroke='%23E63946' stroke-width='1.6' stroke-linecap='round'/><line x1='27' y1='2' x2='27' y2='16' stroke='%23E63946' stroke-width='1.6' stroke-linecap='round'/><line x1='0' y1='16' x2='34' y2='2' stroke='%23E63946' stroke-width='1.6' stroke-linecap='round'/></svg>");
    background-repeat: no-repeat;
    background-size: contain;
  }

  /* ── Stats row ──────────────────────────────────── */
  .stats {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 14px;
    margin: 16px 0 20px;
  }
  .stat {
    background: var(--paper);
    border: 1px solid var(--line);
    border-top: 3px solid var(--accent);
    border-radius: 2px;
    padding: 22px 20px 20px;
  }
  .stat .num {
    font-family: 'Instrument Serif', Georgia, serif;
    font-size: 48px;
    font-weight: 400;
    color: var(--ink);
    letter-spacing: -0.025em;
    line-height: 1;
  }
  .stat .lab {
    color: var(--ink-muted);
    font-size: 11.5px;
    margin-top: 12px;
    line-height: 1.45;
  }

  .takeaway {
    font-family: 'Instrument Serif', Georgia, serif;
    color: var(--ink);
    font-size: 22px;
    font-style: italic;
    font-weight: 400;
    margin-top: 20px;
    letter-spacing: -0.003em;
    line-height: 1.3;
  }

  /* ── Two-column ─────────────────────────────────── */
  .two-col {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 36px;
    margin-top: 4px;
  }
  .two-col.code-heavy { grid-template-columns: 1.1fr 0.9fr; }

  /* ── Stack box ──────────────────────────────────── */
  .stack-box {
    background: var(--paper);
    border: 1px solid var(--line-strong);
    border-radius: 2px;
    padding: 20px 22px;
    margin-top: 10px;
  }
  .stack-box .label {
    color: var(--accent);
    font-size: 9.5px;
    font-weight: 700;
    letter-spacing: 0.18em;
    margin-bottom: 12px;
    text-transform: uppercase;
  }
  .stack-box .row {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    padding: 7px 0;
    border-bottom: 1px dashed var(--line);
    font-size: 12.5px;
  }
  .stack-box .row:last-of-type { border-bottom: none; }
  .stack-box .row .name { color: var(--ink); font-weight: 700; flex: 0 0 auto; }
  .stack-box .row .role {
    color: var(--ink-muted);
    font-size: 11.5px;
    flex: 1;
    margin: 0 12px;
    font-family: 'Inter', sans-serif;
    font-weight: 500;
  }
  .stack-box .row .burden { color: var(--ink); font-weight: 500; font-size: 11px; }
  .stack-box .sum {
    margin-top: 12px;
    padding-top: 12px;
    border-top: 1px solid var(--line);
    color: var(--accent);
    font-size: 13.5px;
    font-weight: 700;
    letter-spacing: 0.01em;
  }

  .sting {
    background: var(--paper);
    color: var(--ink);
    border-left: 3px solid var(--accent);
    padding: 14px 18px;
    margin-top: 18px;
    font-family: 'Instrument Serif', Georgia, serif;
    font-size: 19px;
    font-style: italic;
    font-weight: 400;
    line-height: 1.35;
  }

  /* ── Pillars (4 by default) ─────────────────────── */
  .pillars {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 14px;
    margin: 20px 0;
  }
  .pillar {
    background: var(--paper);
    border: 1px solid var(--line);
    border-top: 3px solid var(--accent);
    border-radius: 2px;
    padding: 20px 18px;
  }
  .pillar .idx {
    font-family: 'Instrument Serif', Georgia, serif;
    font-size: 22px;
    color: var(--accent);
    line-height: 1;
    margin-bottom: 10px;
  }
  .pillar h3 { font-size: 13.5px; margin-bottom: 8px; letter-spacing: -0.003em; }
  .pillar p { font-size: 12px; color: var(--ink-soft); line-height: 1.5; margin: 0; }

  .punchline {
    margin-top: 22px;
    font-size: 15px;
    color: var(--ink-soft);
    font-family: 'Inter', sans-serif;
    font-weight: 500;
    line-height: 1.55;
  }
  .punchline strong {
    color: var(--accent);
    font-weight: 700;
    font-size: 16px;
  }
  .punchline em {
    font-family: 'Instrument Serif', Georgia, serif;
    font-style: italic;
    color: var(--ink);
    font-size: 17px;
  }

  /* ── Architecture ───────────────────────────────── */
  .arch {
    display: grid;
    grid-template-columns: 180px 1fr 200px;
    gap: 18px;
    margin-top: 8px;
  }
  .arch .col-label {
    color: var(--accent);
    font-size: 9.5px;
    font-weight: 700;
    letter-spacing: 0.18em;
    margin-bottom: 10px;
    text-transform: uppercase;
  }
  .arch-box {
    background: var(--paper);
    border: 1px solid var(--line);
    color: var(--ink);
    border-radius: 2px;
    padding: 10px 12px;
    margin-bottom: 8px;
  }
  .arch-box .t { font-weight: 700; font-size: 11.5px; }
  .arch-box .d { font-size: 10px; color: var(--ink-muted); margin-top: 3px; line-height: 1.45; }
  .arch-center {
    background: var(--paper);
    border: 1.5px solid var(--ink);
    border-radius: 2px;
    padding: 14px 16px;
  }
  .arch-center .hdr {
    color: var(--ink);
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.15em;
    margin-bottom: 10px;
    text-transform: uppercase;
  }
  .arch-layer {
    border: 1px solid var(--line-strong);
    background: var(--paper);
    border-radius: 2px;
    padding: 8px 11px;
    margin-bottom: 6px;
  }
  .arch-layer .t { font-weight: 700; font-size: 11.5px; color: var(--ink); }
  .arch-layer .d { font-size: 10px; color: var(--ink-muted); margin-top: 2px; line-height: 1.4; }
  .arch-layer.highlight { border-left: 3px solid var(--accent); }
  .arch-note {
    margin-top: 14px;
    font-size: 12.5px;
    color: var(--ink-soft);
    font-family: 'Inter', sans-serif;
    font-weight: 500;
    text-align: center;
    line-height: 1.45;
  }
  .scaling-strip {
    margin: 14px 0 4px;
    padding: 12px 20px;
    background: var(--paper);
    border: 1px solid var(--line);
    border-left: 3px solid var(--accent);
    border-radius: 2px;
    display: flex;
    align-items: center;
    gap: 18px;
    flex-wrap: wrap;
  }
  .scaling-strip .ss-label {
    font-size: 9.5px;
    font-weight: 700;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: var(--accent);
    flex-shrink: 0;
  }
  .scaling-strip .ss-item {
    color: var(--ink-soft);
    font-size: 12.5px;
    font-weight: 500;
  }
  .scaling-strip .ss-item strong {
    color: var(--ink);
    font-weight: 700;
  }
  .scaling-strip .ss-sep {
    color: var(--line-strong);
    font-weight: 700;
  }
  .scaling-note {
    margin: 6px 0 10px;
    padding: 0 20px;
    font-size: 11.5px;
    color: var(--ink-muted);
    font-weight: 500;
    line-height: 1.5;
  }
  .scaling-note strong {
    color: var(--accent);
    font-weight: 700;
  }
  .flywheel {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 28px;
    margin: 18px 0 14px;
    position: relative;
  }
  .flywheel-step {
    background: var(--paper);
    border: 1px solid var(--line);
    border-top: 3px solid var(--accent);
    border-radius: 2px;
    padding: 20px 20px 18px;
    position: relative;
  }
  .flywheel-step .num {
    font-family: 'Instrument Serif', Georgia, serif;
    font-size: 32px;
    color: var(--accent);
    line-height: 1;
    margin-bottom: 12px;
  }
  .flywheel-step h3 {
    font-size: 13.5px;
    margin-bottom: 8px;
    line-height: 1.25;
  }
  .flywheel-step p {
    font-size: 11.5px;
    color: var(--ink-soft);
    line-height: 1.55;
    margin: 0;
  }
  .flywheel-step::after {
    content: '→';
    position: absolute;
    right: -22px;
    top: 50%;
    transform: translateY(-50%);
    font-size: 24px;
    color: var(--accent);
    font-weight: 700;
    line-height: 1;
  }
  .flywheel-step:last-child::after {
    content: '↻';
    font-size: 28px;
  }
  .flywheel-loop {
    margin-top: 14px;
    text-align: center;
    font-family: 'Inter', sans-serif;
    font-weight: 500;
    font-size: 14px;
    color: var(--ink-soft);
    line-height: 1.5;
  }
  .flywheel-loop em {
    font-family: 'Instrument Serif', Georgia, serif;
    font-style: italic;
    color: var(--ink);
    font-size: 16px;
  }

  /* ── Code blocks ────────────────────────────────── */
  /* Monokai theme — high-contrast, no dark blues anywhere */
  section pre {
    background: #272822 !important;
    border-radius: 5px;
    padding: 30px 26px 22px !important;
    font-family: 'JetBrains Mono', 'SF Mono', Menlo, monospace;
    font-size: 15px;
    line-height: 1.7;
    position: relative;
    border: 1px solid #3E3D32;
    box-shadow: 0 4px 12px rgba(15, 23, 42, 0.1);
  }
  section pre::before {
    content: '';
    position: absolute;
    top: 12px; left: 16px;
    width: 10px; height: 10px;
    background: #FF5F56;
    border-radius: 50%;
    box-shadow: 15px 0 0 #FFBD2E, 30px 0 0 #27C93F;
  }
  section pre code {
    color: #FFFFFF;
    font-family: inherit;
    display: block;
    background: transparent;
    padding: 14px 0 0;
    font-weight: 600;
  }
  section pre code span { font-family: inherit; }

  /* ── Moat 2x2 ───────────────────────────────────── */
  .moat {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 14px;
    margin-top: 10px;
  }
  .moat-item {
    background: var(--paper);
    border: 1px solid var(--line);
    border-top: 3px solid var(--accent);
    border-radius: 2px;
    padding: 18px 22px;
    display: flex;
    gap: 14px;
  }
  .moat-item .num {
    font-family: 'Instrument Serif', Georgia, serif;
    font-size: 30px;
    color: var(--accent);
    line-height: 1;
    flex-shrink: 0;
    margin-top: -2px;
  }
  .moat-item h3 { font-size: 13.5px; margin-bottom: 6px; }
  .moat-item p { font-size: 11.5px; color: var(--ink-soft); line-height: 1.5; margin: 0; }
  .moat-footer {
    margin-top: 18px;
    font-size: 14.5px;
    color: var(--ink-soft);
    font-family: 'Inter', sans-serif;
    font-weight: 500;
    text-align: center;
    line-height: 1.5;
  }
  .moat-footer em {
    font-family: 'Instrument Serif', Georgia, serif;
    font-style: italic;
    color: var(--ink);
    font-size: 16.5px;
  }

  /* ── Cost comparison ────────────────────────────── */
  .cost-compare {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
    margin: 16px 0 12px;
  }
  .cost-card {
    border: 1px solid var(--line);
    border-radius: 2px;
    padding: 16px 20px 14px;
    background: var(--paper);
  }
  .cost-card.status-quo { border-top: 3px solid var(--ink); }
  .cost-card.tally { border-top: 3px solid var(--accent); }
  .cost-card .label {
    font-size: 9.5px;
    font-weight: 700;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    color: var(--ink-muted);
    margin-bottom: 8px;
  }
  .cost-card.tally .label { color: var(--accent); }
  .cost-card ul { margin: 4px 0 6px; }
  .cost-card ul li { font-size: 11.5px; padding-left: 16px; margin-bottom: 4px; }
  .cost-card .total {
    margin-top: 8px;
    padding-top: 10px;
    border-top: 1px solid var(--line);
    font-family: 'Instrument Serif', Georgia, serif;
    font-size: 22px;
    color: var(--ink);
  }
  .cost-card.tally .total { color: var(--accent); }

  /* ── Use case cards ─────────────────────────────── */
  .cases {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 16px 22px;
    margin-top: 6px;
  }
  .case {
    background: var(--paper);
    border: 1px solid var(--line);
    border-left: 3px solid var(--accent);
    border-radius: 2px;
    padding: 16px 22px;
  }
  .case h3 { font-size: 14px; margin-bottom: 6px; }
  .case p { font-size: 12px; color: var(--ink-soft); line-height: 1.5; margin: 0 0 6px; }
  .case .case-stat {
    font-family: 'Instrument Serif', Georgia, serif;
    font-size: 34px;
    color: var(--accent);
    line-height: 1;
    margin: 4px 0 2px;
    letter-spacing: -0.02em;
  }
  .case .case-stat-label {
    font-size: 9.5px;
    color: var(--ink-muted);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 8px;
  }
  .case .tag {
    display: inline-block;
    margin-top: 2px;
    font-family: 'Inter', sans-serif;
    color: var(--accent);
    font-size: 12px;
    font-weight: 600;
  }

  /* ── Impact compare ─────────────────────────────── */
  .compare {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 18px;
    margin-top: 8px;
  }
  .compare .card {
    border: 1px solid var(--line);
    background: var(--paper);
    padding: 20px 22px;
    border-radius: 2px;
  }
  .compare .card.left { border-top: 3px solid var(--ink); }
  .compare .card.right { border-top: 3px solid var(--accent); }
  .compare .sublabel {
    font-size: 9.5px;
    font-weight: 700;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    color: var(--ink-muted);
    margin-bottom: 8px;
  }
  .compare .card.right .sublabel { color: var(--accent); }
  .compare .card-title {
    font-size: 14px;
    font-weight: 700;
    color: var(--ink);
    margin-bottom: 6px;
  }
  .compare .big {
    font-family: 'Instrument Serif', Georgia, serif;
    font-size: 26px;
    color: var(--ink);
    margin: 6px 0 10px;
    line-height: 1.15;
  }
  .compare .card.right .big { color: var(--accent); }
  .compare ul li { font-size: 12px; margin-bottom: 5px; }

  .proof {
    margin-top: 16px;
    padding: 14px 20px;
    background: var(--paper);
    border: 1px solid var(--line);
    border-left: 3px solid var(--accent);
    font-family: 'Inter', sans-serif;
    font-size: 14px;
    color: var(--ink);
    font-weight: 500;
    line-height: 1.6;
  }
  .proof strong {
    color: var(--accent);
    font-weight: 700;
  }

  /* ── Competition matrix ─────────────────────────── */
  .competition {
    display: grid;
    grid-template-columns: 1.35fr 1fr;
    gap: 22px;
    margin-top: 6px;
  }
  .comp-table {
    border: 1px solid var(--line);
    background: var(--paper);
    border-radius: 2px;
  }
  .comp-table .chead {
    display: grid;
    grid-template-columns: 1.6fr 0.85fr 0.85fr;
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--ink-muted);
    padding: 9px 14px;
    border-bottom: 1px solid var(--line-strong);
  }
  .comp-row {
    display: grid;
    grid-template-columns: 1.6fr 0.85fr 0.85fr;
    padding: 7px 14px;
    font-size: 12.5px;
    border-bottom: 1px dashed var(--line);
    align-items: center;
  }
  .comp-row:last-child { border-bottom: none; }
  .comp-row .cname { font-weight: 700; color: var(--ink); font-size: 12.5px; }
  .comp-row .ccell {
    color: var(--ink-soft);
    font-family: 'Inter', sans-serif;
    font-size: 12px;
    font-weight: 500;
  }
  .comp-row.us { background: var(--accent-soft); }
  .comp-row.us .cname { color: var(--accent-deep); }
  .comp-row.us .ccell { color: var(--accent-deep); font-weight: 600; }
  .comp-row.gone .cname { color: var(--ink-muted); }
  .comp-row.gone .ccell { color: var(--ink-muted); }

  .comp-body {
    font-size: 13.5px;
    color: var(--ink-soft);
    line-height: 1.55;
    font-weight: 500;
  }
  .comp-body p { margin-bottom: 10px; font-size: 13.5px; }
  .comp-body .tag {
    display: block;
    margin-top: 12px;
    padding-top: 10px;
    border-top: 1px solid var(--line);
    font-family: 'Inter', sans-serif;
    font-size: 13px;
    color: var(--accent);
    font-weight: 600;
    line-height: 1.5;
  }

  /* ── Timeline / GTM ─────────────────────────────── */
  .timeline {
    position: relative;
    margin: 14px 0 18px;
    padding: 0 40px;
  }
  .timeline .rail {
    position: absolute;
    top: 15px;
    left: 60px;
    right: 60px;
    height: 1px;
    background: var(--line-strong);
  }
  .nodes {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    position: relative;
  }
  .node { text-align: center; }
  .node .dot {
    width: 32px; height: 32px;
    border-radius: 50%;
    background: var(--paper);
    border: 1.5px solid var(--accent);
    color: var(--accent);
    font-family: 'Instrument Serif', Georgia, serif;
    font-size: 19px;
    line-height: 28px;
    margin: 0 auto 10px;
    position: relative;
    z-index: 2;
  }
  .node .when {
    font-size: 9.5px;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--ink-muted);
    font-weight: 700;
  }
  .node .phase {
    font-size: 10.5px;
    color: var(--ink);
    margin-top: 2px;
    font-weight: 600;
  }

  .phase-cards {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 14px;
    margin-top: 6px;
  }
  .phase-card {
    background: var(--paper);
    border: 1px solid var(--line);
    border-top: 3px solid var(--accent);
    border-radius: 2px;
    padding: 15px 16px;
  }
  .phase-card h3 { font-size: 12.5px; margin-bottom: 6px; }
  .phase-card p { font-size: 11px; color: var(--ink-soft); line-height: 1.5; margin: 0; }

  .gtm-tag {
    margin-top: 18px;
    font-family: 'Inter', sans-serif;
    font-size: 14.5px;
    color: var(--ink-soft);
    font-weight: 500;
    text-align: center;
    line-height: 1.5;
  }
  .gtm-tag strong {
    color: var(--accent);
    font-weight: 700;
  }

  /* ── Team ───────────────────────────────────────── */
  .team {
    display: grid;
    grid-template-columns: 0.75fr 1.65fr;
    gap: 28px;
    margin-top: 4px;
  }
  .profile .avatar {
    width: 88px; height: 88px;
    background: var(--paper);
    border: 1.5px solid var(--accent);
    color: var(--accent);
    font-family: 'Instrument Serif', Georgia, serif;
    font-size: 42px;
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 2px;
    margin-bottom: 14px;
  }
  .profile .name {
    font-family: 'Instrument Serif', Georgia, serif;
    font-size: 28px;
    color: var(--ink);
    line-height: 1.1;
    margin-bottom: 2px;
  }
  .profile .role {
    font-size: 11.5px;
    color: var(--ink-muted);
    font-weight: 500;
    letter-spacing: 0.02em;
  }
  .profile .rule {
    width: 42px; height: 2px;
    background: var(--accent);
    margin: 12px 0;
  }
  .profile .edu {
    font-size: 11.5px;
    color: var(--ink-soft);
    line-height: 1.5;
    margin-bottom: 12px;
  }
  .profile .pills { display: flex; flex-wrap: wrap; gap: 5px; margin-top: 6px; }
  .profile .pill {
    font-size: 9px;
    padding: 3px 8px;
    border: 1px solid var(--line-strong);
    color: var(--ink-muted);
    background: var(--paper);
    letter-spacing: 0.02em;
    font-weight: 500;
    border-radius: 2px;
  }

  .exp { display: flex; flex-direction: column; gap: 13px; }
  .exp-item { display: flex; gap: 14px; }
  .exp-item .bar {
    flex-shrink: 0;
    width: 2px;
    background: var(--accent);
    margin-top: 4px;
  }
  .exp-item h3 { font-size: 12.5px; margin-bottom: 4px; color: var(--ink); }
  .exp-item p { font-size: 11px; line-height: 1.5; color: var(--ink-soft); margin: 0; }
  .exp-item em {
    font-family: 'Inter', sans-serif;
    font-style: normal;
    font-weight: 700;
    color: var(--accent);
    font-size: 11px;
  }

  /* ── Tiers ──────────────────────────────────────── */
  .tiers {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 14px;
    margin-top: 6px;
  }
  .tier {
    background: var(--paper);
    border: 1px solid var(--line);
    border-radius: 2px;
    padding: 14px 18px 12px;
    position: relative;
  }
  .tier.primary { border-top: 3px solid var(--accent); }
  .tier.secondary { border-top: 3px solid var(--ink); }
  .tier .pill {
    display: inline-block;
    font-size: 8.5px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    padding: 2px 8px;
    border: 1px solid var(--line-strong);
    color: var(--ink-muted);
    margin-bottom: 6px;
    font-weight: 600;
    border-radius: 2px;
  }
  .tier .name {
    font-family: 'Instrument Serif', Georgia, serif;
    font-size: 20px;
    color: var(--ink);
    line-height: 1;
    margin-bottom: 3px;
  }
  .tier .sub {
    font-size: 10.5px;
    color: var(--ink-muted);
    font-style: italic;
    font-family: 'Instrument Serif', Georgia, serif;
    margin-bottom: 6px;
  }
  .tier .price {
    font-family: 'Instrument Serif', Georgia, serif;
    font-size: 20px;
    color: var(--accent);
    margin-bottom: 6px;
  }
  .tier hr { border: none; border-top: 1px solid var(--line); margin: 4px 0; }
  .tier ul { margin: 3px 0 0; }
  .tier ul li { font-size: 10px; margin-bottom: 3px; padding-left: 12px; line-height: 1.4; }

  .arr-table {
    margin-top: 10px;
    border: 1px solid var(--line);
    background: var(--paper);
    border-radius: 2px;
  }
  .arr-table .arr-head {
    display: grid;
    grid-template-columns: 0.7fr 1fr 2fr;
    padding: 8px 16px;
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: var(--ink-muted);
    border-bottom: 1px solid var(--line-strong);
  }
  .arr-row {
    display: grid;
    grid-template-columns: 0.7fr 1fr 2fr;
    padding: 8px 16px;
    font-size: 12.5px;
    border-bottom: 1px dashed var(--line);
    align-items: baseline;
  }
  .arr-row:last-child { border-bottom: none; }
  .arr-row .horizon { font-weight: 700; color: var(--ink); font-size: 12.5px; }
  .arr-row .target {
    font-family: 'Instrument Serif', Georgia, serif;
    font-size: 19px;
    color: var(--accent);
  }
  .arr-row .ref {
    font-size: 12.5px;
    color: var(--ink-soft);
    font-style: italic;
    font-family: 'Instrument Serif', Georgia, serif;
  }

  .business-tag {
    margin-top: 10px;
    font-family: 'Inter', sans-serif;
    font-size: 13px;
    color: var(--ink-soft);
    font-weight: 500;
    line-height: 1.5;
  }
  .business-tag strong {
    color: var(--accent);
    font-weight: 700;
  }

  /* ── Project status ─────────────────────────────── */
  .status-split {
    display: grid;
    grid-template-columns: 1.1fr 0.9fr;
    gap: 22px;
    margin-top: 4px;
  }
  .status-card {
    background: var(--paper);
    border: 1px solid var(--line);
    border-radius: 2px;
    padding: 18px 22px;
  }
  .status-card .head {
    font-size: 9.5px;
    font-weight: 700;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: var(--accent);
    margin-bottom: 12px;
  }
  .status-item { margin-bottom: 8px; }
  .status-item .t { font-weight: 700; font-size: 11.5px; color: var(--ink); }
  .status-item .d { font-size: 10.5px; color: var(--ink-muted); line-height: 1.45; }
  .next-row {
    display: flex;
    padding: 7px 0;
    font-size: 11.5px;
    border-bottom: 1px dashed var(--line);
  }
  .next-row:last-of-type { border-bottom: none; }
  .next-row .when {
    flex: 0 0 72px;
    color: var(--accent);
    font-weight: 700;
    font-size: 9.5px;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding-top: 2px;
  }
  .next-row .what { color: var(--ink); font-size: 11px; }

  /* ── Vision + asks ──────────────────────────────── */
  .vision-sub {
    font-family: 'Instrument Serif', Georgia, serif;
    font-style: italic;
    color: var(--ink-muted);
    font-size: 22px;
    max-width: 820px;
    margin: 16px 0 44px;
    line-height: 1.35;
  }
  .asks-head {
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.22em;
    text-transform: uppercase;
    color: var(--accent);
    margin-bottom: 14px;
  }
  .asks {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 18px;
  }
  .ask {
    border: 1px solid var(--line);
    background: var(--paper);
    border-top: 3px solid var(--accent);
    border-radius: 2px;
    padding: 18px 22px;
  }
  .ask h3 { font-size: 13.5px; margin-bottom: 8px; }
  .ask p { font-size: 11.5px; color: var(--ink-soft); line-height: 1.5; margin: 0; }
  .thanks {
    position: absolute;
    right: 100px;
    bottom: 68px;
    text-align: right;
  }
  .thanks .ty {
    font-family: 'Instrument Serif', Georgia, serif;
    font-size: 20px;
    font-style: italic;
    color: var(--ink);
    margin-bottom: 4px;
  }
  .thanks .meta {
    color: var(--ink-muted);
    font-size: 11px;
    letter-spacing: 0.02em;
  }
---

<!-- _class: hero -->
<!-- _paginate: false -->

<div class="kicker">Vietnam AI Stars  ·  2026</div>

<div class="hero-title">Tally</div>

<div class="hero-tagline">Zero-ops real-time features.<br/>A database, not a system.</div>

<div class="hero-descriptor">
Single node with snapshot failover. Install in 60 seconds. Author pipelines with Claude.<br/>
Batch is the default today. Streaming is the default in 2030.
</div>

<div class="hero-author">
  <div class="rule"></div>
  <div class="name">Hoang Phan</div>
  <div class="meta">Founder &amp; Engineering Lead  ·  phan.minhhoang2606@gmail.com  ·  github.com/petrpan26</div>
</div>

<div class="hero-tally"></div>

---

<!-- _class: content -->

<div class="kicker">Why real-time features matter</div>

# Zero-ops real-time features.

<div class="title-divider"></div>

<p style="font-size: 16.5px; color: var(--ink-soft); font-weight: 500; max-width: 1000px; margin: 0 0 22px; line-height: 1.55;">
Real-time features are the engine behind every modern ML product. The difference between catching a fraud transaction and refunding it. Between a feed that engages and one that dies. Between an agent that remembers you and one that feels broken every message. <strong style="color: var(--accent);">Today, only FAANG can build them. Tally makes them adoptable on day one.</strong>
</p>

<div class="cases">

<div class="case">
<h3>Fraud detection</h3>
<div class="case-stat">$6B+/yr</div>
<div class="case-stat-label">Stripe Radar fraud prevented (2024)</div>
<p>Sub-100ms scoring on every transaction — velocity, device fingerprint, merchant history, geo signals. Miss the window and the money is gone.</p>
<span class="tag">Global card fraud: $33B/yr market</span>
</div>

<div class="case">
<h3>Real-time personalization</h3>
<div class="case-stat">80%</div>
<div class="case-stat-label">of Netflix views come from recs</div>
<p>Rank items as the user scrolls using session signals, dwell time, recent clicks. TikTok's For You Page step-changed engagement over static recommenders.</p>
<span class="tag">70%+ of YouTube watch time from recs</span>
</div>

<div class="case">
<h3>LLM agent memory</h3>
<div class="case-stat">900M</div>
<div class="case-stat-label">weekly ChatGPT users (2026)</div>
<p>Per-turn context updates — intent history, tool-call state, user preferences. The layer between a coherent agent and one that forgets every message.</p>
<span class="tag">Every agent platform needs it</span>
</div>

<div class="case">
<h3>Real-time bidding</h3>
<div class="case-stat">$100B+</div>
<div class="case-stat-label">annual programmatic ad market</div>
<p>Sub-50ms bid decisions using audience features, frequency caps, budget pacing. Millions of bids per second, every one powered by real-time feature lookups.</p>
<span class="tag">The category that invented real-time features</span>
</div>

</div>

---

<!-- _class: content -->

<div class="kicker">Why now</div>

# Streaming-first is the 2030 default. The tools aren't ready for it.

<div class="title-divider"></div>

<div class="pillars">

<div class="pillar">
<div class="idx">01</div>
<h3>AI-native products can't tolerate batch latency</h3>
<p>Every agent, recommender, fraud detector, and LLM context builder shipping in 2026 needs state that's at most seconds stale. Nightly batch is architecturally disqualified.</p>
</div>

<div class="pillar">
<div class="idx">02</div>
<h3>Incremental beats full-table rewrites 3×</h3>
<p>I've personally taken a growth-stage marketplace's warehouse bill from $180K to $60K by moving transformations to incremental. Batch gets more expensive as data grows. Streaming stays flat.</p>
</div>

<div class="pillar">
<div class="idx">03</div>
<h3>The DevEx penalty for streaming collapsed</h3>
<p>Flink SQL used to be harder than dbt SQL. A <code>@tl.stream</code> Python class is now easier than a dbt model. The moment streaming is easier than batch, the default flips for every new workload.</p>
</div>

<div class="pillar">
<div class="idx">04</div>
<h3>Claude can author streaming pipelines</h3>
<p>Tally's SDK is small enough for an LLM to reason about and declarative enough that a pipeline is a data structure, not a DAG. Only possible because Tally was designed in the Claude Code era.</p>
</div>

</div>

<div class="takeaway">Every workload an AI-native team writes in 2026 defaults to streaming. The category is flipping. The incumbents weren't built for this moment.</div>

---

<!-- _class: content -->

<div class="kicker">The problem</div>

# Real-time isn't gated by budget. It's gated by engineering risk.

<div class="title-divider"></div>

<div class="two-col">
<div>

**I have shipped this system three times.**

At **Faire** I was the ML platform engineer. We deferred real-time features until the company crossed several hundred million in ARR. I built the internal version — a feature definition library that cut Snowflake spend from $180K to $60K and unlocked 3 production deep-learning launches with zero dedicated platform engineers.

At **Fennel AI** (acquired by Databricks, 2025) I re-architected the real-time engine. Five enterprise customers paid for the cluster-shaped version of this exact problem.

At **Viggle** I lead the team building real-time inference infra right now. Saved <strong>$1M/year</strong> in infrastructure spend in my first six months.

<div class="sting">
Three companies. Same pain. No one solved it for growth-stage teams. So I'm building it myself.
</div>

</div>
<div>

<div class="stack-box">
<div class="label">THE STATUS-QUO STACK</div>
<div class="row"><span class="name">Kafka + Zookeeper</span><span class="role">transport</span><span class="burden">brokers, rebalances</span></div>
<div class="row"><span class="name">Flink / Spark Streaming</span><span class="role">stateful compute</span><span class="burden">checkpoints, state TTL</span></div>
<div class="row"><span class="name">Feast / Tecton</span><span class="role">feature serving</span><span class="burden">online/offline sync</span></div>
<div class="row"><span class="name">Snowflake / BigQuery</span><span class="role">warehouse</span><span class="burden">training data prep</span></div>
<div class="row"><span class="name">dbt</span><span class="role">transformations</span><span class="burden">full-table rewrites</span></div>
<div class="row"><span class="name">Airflow / Dagster</span><span class="role">orchestration</span><span class="burden">glue code</span></div>
<div class="row"><span class="name">2–3 platform engineers</span><span class="role">keep it alive</span><span class="burden">24/7 on-call</span></div>
<div class="sum">6 systems. 3 engineers. $500K–$1M / year all-in.</div>
</div>

</div>
</div>

---

<!-- _class: content -->

<div class="kicker">The solution</div>

# Tally: one binary, one mental model, zero ops.

<div class="title-divider"></div>

<div class="pillars">
  <div class="pillar">
    <div class="idx">I</div>
    <h3>Runs like a database</h3>
    <p>Single-node Rust database with snapshot failover. No Kafka, Flink, or Zookeeper. Think Redis or Postgres, not a streaming system. Hot-standby replication on the roadmap.</p>
  </div>
  <div class="pillar">
    <div class="idx">II</div>
    <h3>One engineer's mental model</h3>
    <p>Python class in, pipeline out. No DAGs, no operator topology, no checkpoint coordinator. First feature in 10 minutes.</p>
  </div>
  <div class="pillar">
    <div class="idx">III</div>
    <h3>Controllable by design</h3>
    <p>Every feature in code. Every operator explains on a whiteboard. Snapshot on command. Inspect over HTTP. Restart for full recovery.</p>
  </div>
  <div class="pillar">
    <div class="idx">IV</div>
    <h3>Not an experiment</h3>
    <p>Predictable behavior. Bounded memory per key. Documented limits. Adopting Tally is closer to adding a Python library than standing up a data platform.</p>
  </div>
</div>

<div class="punchline">
<strong>100× cheaper</strong> than Kafka + Flink + feature store on the same workload. That's not why teams adopt it. Teams adopt it because <em>it isn't a bet</em>.
</div>

---

<!-- _class: content -->

<div class="kicker">Architecture</div>

# Single-node database. Four layers. Built like Redis.

<div class="title-divider"></div>

<div class="arch">

<div>
<div class="col-label">CLIENT</div>
<div class="arch-box"><div class="t">Python SDK</div><div class="d">Declarative <code>@tl.stream</code> definitions. Thin TCP client.</div></div>
<div class="arch-box"><div class="t">Any TCP client</div><div class="d">Go, Node, Rust, C++, Java via custom binary protocol.</div></div>
<div class="arch-box"><div class="t">Claude skills</div><div class="d">Author pipelines in prose. Generate realistic test data. Parity-test locally — dev box runs the same as prod.</div></div>
</div>

<div class="arch-center">
<div class="hdr">TALLY  ·  SINGLE-NODE DATABASE</div>
<div class="arch-layer"><div class="t">TCP protocol</div><div class="d">PUSH / GET / SET / MSET / REGISTER over persistent length-prefixed frames.</div></div>
<div class="arch-layer"><div class="t">Pipeline engine</div><div class="d">Operators (count, sum, avg, min, max, distinct_count, last). Expression AST. Cross-stream views. Lookups.</div></div>
<div class="arch-layer"><div class="t">In-memory state</div><div class="d">Everything stays in RAM. Sub-microsecond reads. Bounded memory per key.</div></div>
<div class="arch-layer highlight"><div class="t">Event log + snapshot persistence</div><div class="d">Shipped: periodic snapshots. Phase 9 (Q2 2026): incremental snapshots + durable event log for training-data replay.</div></div>
</div>

<div>
<div class="col-label">OUT OF BAND</div>
<div class="arch-box"><div class="t">Web debug UI</div><div class="d">Inspect any key's state. Watch live events. Step through operators visually.</div></div>
<div class="arch-box"><div class="t">HTTP management</div><div class="d">Register pipelines, /metrics, /health, debug inspection.</div></div>
<div class="arch-box"><div class="t">Local disk snapshot</div><div class="d">Versioned binary file. ~5s recovery per 1M keys.</div></div>
</div>

</div>

<div class="arch-note">No distributed consensus. No checkpoint coordinator. No rebalance protocol. Restart = full recovery in seconds.</div>

---

<!-- _class: content -->

<div class="kicker">Developer experience</div>

# A feature is a Python attribute. A pipeline is a class. Claude writes them.

<div class="title-divider"></div>

<div class="two-col code-heavy">
<div>

<pre><code><span style="color:#FF5F56;font-weight:700">import</span> tally <span style="color:#FF5F56;font-weight:700">as</span> tl

<span style="color:#FF5F56;font-weight:700">@tl.stream</span>(key=<span style="color:#FFA657;font-weight:700">"user_id"</span>)
<span style="color:#FF5F56;font-weight:700">class</span> Transactions:
    count_1h = tl.count(window=<span style="color:#FFA657;font-weight:700">"1h"</span>)
    sum_1h   = tl.sum(<span style="color:#FFA657;font-weight:700">"amount"</span>, window=<span style="color:#FFA657;font-weight:700">"1h"</span>)
    avg      = tl.derive(<span style="color:#FFA657;font-weight:700">"sum_1h / count_1h"</span>)

f = app.push(Transactions, {<span style="color:#FFA657;font-weight:700">"user_id"</span>: <span style="color:#FFA657;font-weight:700">"u_1"</span>, <span style="color:#FFA657;font-weight:700">"amount"</span>: <span style="color:#FFA657;font-weight:700">50</span>})
f.avg   <span style="color:#9DA5B4;font-style:italic"># 48.2</span></code></pre>

</div>
<div>

### Why this matters

- Feature definitions live next to application code — not a separate YAML DSL or UI.
- Same API a solo founder and a Series B ML team both pick up in 10 minutes.
- Web debug UI ships with Tally: inspect any key's state, watch live events, step through operators.
- Claude skills author pipelines AND generate realistic test data for parity testing. Dev box runs the same as prod because it's one node.

<div class="punchline" style="margin-top:22px;">
This is the abstraction incumbents <em>cannot retrofit</em>. Their SDKs are too large for an LLM to hold in context. Ours is small by design.
</div>

</div>
</div>

---

<!-- _class: content -->

<div class="kicker">The moat</div>

# Four locks that compound, not four features that copy.

<div class="title-divider"></div>

<div class="moat">

<div class="moat-item">
<div class="num">1</div>
<div>
<h3>Architectural lock-in</h3>
<p>Materialize, RisingWave, Flink, ksqlDB assume distributed state from day one. Shipping a single binary isn't a flag — it's a two-year rewrite of their state layer.</p>
</div>
</div>

<div class="moat-item">
<div class="num">2</div>
<div>
<h3>Price invisibility</h3>
<p>Databricks sales teams don't knock on seed-stage doors for a $500/mo deal. Tally at $0 OSS and $20–$5K/mo cloud is beneath every incumbent's go-to-market.</p>
</div>
</div>

<div class="moat-item">
<div class="num">3</div>
<div>
<h3>Features-first DX</h3>
<p>Python class in, running pipeline out. No SQL views, no DAG config, no feature store YAML. The only player close on this axis is Chalk — enterprise-only on Kubernetes.</p>
</div>
</div>

<div class="moat-item">
<div class="num">4</div>
<div>
<h3>AI-agent native by construction</h3>
<p>Claude skills author pipelines, generate test data, and debug live state. An MCP server exposes the running node to any AI agent. The SDK is small by design so an LLM can hold it all in context.</p>
</div>
</div>

</div>

<div class="moat-footer">Copying one lock doesn't threaten us. Copying all four means rebuilding the company.</div>

---

<!-- _class: content -->

<div class="kicker">Performance &amp; cost</div>

# Same throughput as a Flink cluster. Zero platform engineers.

<div class="title-divider"></div>

<div class="stats">
  <div class="stat"><div class="num">100K+</div><div class="lab">events/sec sustained on one thread</div></div>
  <div class="stat"><div class="num">&lt;100µs</div><div class="lab">PUSH latency (p99, single event)</div></div>
  <div class="stat"><div class="num">&lt;50µs</div><div class="lab">GET latency (p99)</div></div>
  <div class="stat"><div class="num">&lt;5KB</div><div class="lab">memory per key, 10 mixed features</div></div>
</div>

<div class="scaling-strip">
<span class="ss-label">SINGLE-NODE CAPACITY  ·  r6g MEMORY-OPTIMIZED</span>
<span class="ss-item"><strong>r6g.xlarge</strong> · 4c/32 GB → 5M entities · $150/mo</span>
<span class="ss-sep">·</span>
<span class="ss-item"><strong>r6g.4xlarge</strong> · 16c/128 GB → 20M entities · $600/mo</span>
<span class="ss-sep">·</span>
<span class="ss-item"><strong>r6g.16xlarge</strong> · 64c/512 GB → 80M entities · $2.4K/mo</span>
</div>

<div class="scaling-note">Throughput: <strong>100K evt/s</strong> on v1 (single-threaded, Redis-style). Key-partitioned multi-threading in v2 scales to <strong>500K+ evt/s</strong> on the same hardware — no code changes, just a config flag.</div>

<div class="cost-compare">
<div class="cost-card status-quo">
<div class="label">STATUS QUO  ·  KAFKA + FLINK + FEAST</div>
<ul>
<li>4 systems to operate</li>
<li>3 dedicated platform engineers</li>
<li>Cloud infrastructure: $15K–$40K / month</li>
<li>Salaries: $400K–$600K / year</li>
</ul>
<div class="total">~$450K–$1M / year</div>
</div>
<div class="cost-card tally">
<div class="label">WITH TALLY</div>
<ul>
<li>Single-node DB, 0 dedicated engineers</li>
<li>r6g memory-optimized: $150–$2,400 / month</li>
<li>Snapshot storage: cents / month</li>
<li>Platform team: $0</li>
</ul>
<div class="total">~$2K–$30K / year</div>
</div>
</div>

<div class="moat-footer">Same throughput. Same latency. Same feature set. Adoption risk: <em>a Python import</em>.</div>

---

<!-- _class: content -->

<div class="kicker">Landscape</div>

# The category consolidated upward in 2025.

<div class="title-divider"></div>

<div class="competition">

<div class="comp-table">
<div class="chead">
<div>Player</div><div>Single-node DB?</div><div>Self-serve price?</div>
</div>
<div class="comp-row">
<div class="cname">Kafka + Flink (Confluent)</div><div class="ccell">no — cluster</div><div class="ccell">enterprise</div>
</div>
<div class="comp-row">
<div class="cname">Materialize</div><div class="ccell">no — managed</div><div class="ccell">mid-market</div>
</div>
<div class="comp-row">
<div class="cname">RisingWave</div><div class="ccell">no — cluster</div><div class="ccell">enterprise sales</div>
</div>
<div class="comp-row">
<div class="cname">Tinybird</div><div class="ccell">managed only</div><div class="ccell">no low tier</div>
</div>
<div class="comp-row">
<div class="cname">Chalk</div><div class="ccell">no — Kubernetes</div><div class="ccell">contact sales</div>
</div>
<div class="comp-row">
<div class="cname">Feast</div><div class="ccell">BYO Kafka + Redis</div><div class="ccell">free, self-assembled</div>
</div>
<div class="comp-row gone">
<div class="cname">Tecton</div><div class="ccell">absorbed</div><div class="ccell">Databricks, Aug 2025</div>
</div>
<div class="comp-row gone">
<div class="cname">Fennel</div><div class="ccell">absorbed</div><div class="ccell">Databricks, Apr 2025</div>
</div>
<div class="comp-row gone">
<div class="cname">Neon</div><div class="ccell">absorbed</div><div class="ccell">Databricks $1B, 2025</div>
</div>
<div class="comp-row us">
<div class="cname">Tally</div><div class="ccell">yes — single node</div><div class="ccell">$0 – $5K/mo</div>
</div>
</div>

<div class="comp-body">
<p>Since 2023, Databricks has spent over <strong>$4B</strong> acquiring MosaicML ($1.3B, 2023), Tabular (~$2B, 2024), Fennel (Apr 2025), Neon ($1B, May 2025), and Tecton (Aug 2025). They rolled it into one enterprise platform called <em>Agent Bricks</em>.</p>
<p>The real-time feature store category effectively no longer exists as independent startups. Confluent owns managed Flink. Databricks owns the feature store layer. Neither sells to seed-stage founders.</p>
<p>The wedge stays open until an incumbent rewrites their cluster-shaped architecture into a single binary — a multi-year project none of them will start.</p>
<span class="tag">I was at Fennel through the Databricks acquisition. I saw this consolidation happen from inside the space.</span>
</div>

</div>

---

<!-- _class: content -->

<div class="kicker">Current state &amp; roadmap</div>

# Core engine is real. Phase 9 (incremental snapshots) substantially shipped.

<div class="title-divider"></div>

<div class="status-split">
<div class="status-card">
<div class="head">SHIPPED  ·  CORE STREAMING FUNCTIONALITY</div>
<div class="status-item"><div class="t">Real-time aggregations</div><div class="d">Count, sum, avg, min, max over sliding windows — updated on every event.</div></div>
<div class="status-item"><div class="t">Approximate distinct counts</div><div class="d">Unique merchants, IPs, devices at any cardinality with bounded memory.</div></div>
<div class="status-item"><div class="t">Derived expressions</div><div class="d">Compute features from other features: ratios, velocity spikes, composite scores.</div></div>
<div class="status-item"><div class="t">Cross-stream features</div><div class="d">Reference features from other streams. Lookup by alternate keys.</div></div>
<div class="status-item"><div class="t">One round-trip push</div><div class="d">Event in, updated features out in the same call. Sub-millisecond.</div></div>
<div class="status-item"><div class="t">Snapshot recovery</div><div class="d">Periodic state dumps. Restart equals full recovery in seconds.</div></div>
<div class="status-item"><div class="t">Incremental snapshots (Phase 9)</div><div class="d">Base + delta format v6. Dirty/deleted tracking. Durable event log. Recovery tested.</div></div>
<div class="status-item"><div class="t">Python SDK &amp; Claude skills</div><div class="d">Declarative @tl.stream classes. Claude authors pipelines from prose.</div></div>
</div>

<div class="status-card">
<div class="head">NEXT  ·  COMPETITION TIMELINE</div>
<div class="next-row"><span class="when">Q2 2026</span><span class="what">Design partner outreach: 5 ML leads from Fennel + Viggle network</span></div>
<div class="next-row"><span class="when">Q2 2026</span><span class="what">Benchmark harness + public performance report</span></div>
<div class="next-row"><span class="when">Q2 2026</span><span class="what">MCP server + web debug UI</span></div>
<div class="next-row"><span class="when">Q3 2026</span><span class="what">Open source launch on HN, Product Hunt, Vietnamese docs</span></div>
<div class="next-row"><span class="when">Q4 2026</span><span class="what">Tally Cloud private beta on FPT Cloud + AWS</span></div>
<div class="next-row"><span class="when">Q4 2026</span><span class="what">10 production deployments milestone</span></div>
</div>
</div>

<div class="moat-footer">No external data. No ML model to train. Deploys on any Linux box. Founder has built this class of system twice before.</div>

---

<!-- _class: content -->

<div class="kicker">Go to market</div>

# OSS → usage-based cloud → enterprise. The Vercel playbook.

<div class="title-divider"></div>

<div class="timeline">
<div class="rail"></div>
<div class="nodes">
  <div class="node"><div class="dot">1</div><div class="when">Q2 2026</div><div class="phase">Phase 1</div></div>
  <div class="node"><div class="dot">2</div><div class="when">Q3 2026</div><div class="phase">Phase 2</div></div>
  <div class="node"><div class="dot">3</div><div class="when">Q4 2026</div><div class="phase">Phase 3</div></div>
  <div class="node"><div class="dot">4</div><div class="when">2027+</div><div class="phase">Phase 4</div></div>
</div>
</div>

<div class="phase-cards">
  <div class="phase-card">
    <h3>OSS launch + design partners</h3>
    <p>MIT core on GitHub. Launch on Hacker News, Product Hunt, Claude Code community. Parallel: direct outreach to 5 named ML leads from Fennel and Viggle networks. Target: 5K stars, 3 signed design partners, 50 contributors.</p>
  </div>
  <div class="phase-card">
    <h3>Private cloud + replay</h3>
    <p>Self-hosted cloud deployment (run Tally Cloud in your own VPC) and durable event-log replay for training-data regeneration. The two features enterprise teams gate on.</p>
  </div>
  <div class="phase-card">
    <h3>Cloud private beta</h3>
    <p>Tally Cloud managed on AWS, GCP, and FPT Cloud. Usage-based pricing: pay per event ingested and key-hour stored. Same API as self-hosted. Zero migration.</p>
  </div>
  <div class="phase-card">
    <h3>Enterprise on-prem</h3>
    <p>Air-gapped deployments for banks, insurers, telcos. SOC2 / ISO 27001 track. $150K–$500K ACV with SLA-backed support contract.</p>
  </div>
</div>

<div class="gtm-tag">DevEx wins new workloads. New workloads compound into platform dependency. Platform dependency converts to revenue. Vercel (<strong>$9.3B</strong>), Supabase (<strong>$5B</strong>), PlanetScale (<strong>$1B+</strong>) all built this way.</div>

---

<!-- _class: content -->

<div class="kicker">Team</div>

# I've shipped this system three times. Now I'm building the version I wished existed.

<div class="title-divider"></div>

<div class="team">

<div class="profile">
<div class="avatar">HP</div>
<div class="name">Hoang Phan</div>
<div class="role">Founder &amp; Engineering Lead</div>
<div class="rule"></div>
<div class="edu">University of Waterloo<br/>BCS, Minor in Finance</div>
<div class="pills">
<span class="pill">Rust</span><span class="pill">C++</span><span class="pill">Python</span>
<span class="pill">Distributed systems</span><span class="pill">Streaming</span><span class="pill">RocksDB</span>
</div>
</div>

<div class="exp">

<div class="exp-item"><div class="bar"></div><div>
<h3>Viggle AI  ·  Engineering Lead, Toronto  ·  2025–present</h3>
<p>Leading the 4-engineer team building Viggle's real-time video inference platform. Cut time-to-first-chunk from 8s to 2s. Saved $1M/year in infrastructure. Scaled Toronto engineering from 1 to 8. Shipped the platform foundation underneath 7 signed customers.</p>
</div></div>

<div class="exp-item"><div class="bar"></div><div>
<h3>Fennel AI  ·  Distributed Systems Engineer  ·  2024–2025 (acquired by Databricks)</h3>
<p>Re-architected core streaming join and window operators for 10× runtime and memory efficiency. Designed percentile data structures that landed 5 enterprise customers including Upwork and Rippling. Co-owned infrastructure architecture with the CTO. <em>I built the class of system Tally is, at the company Databricks absorbed.</em></p>
</div></div>

<div class="exp-item"><div class="bar"></div><div>
<h3>Faire  ·  ML Platform Engineer  ·  2022–2023</h3>
<p>Shifted Faire from XGBoost-only to deep-learning-capable with 3 production launches and zero dedicated deployment engineers. Built the internal feature definition library Tally commercializes. Cut Snowflake spend from $180K to $60K via incremental transformations.</p>
</div></div>

<div class="exp-item"><div class="bar"></div><div>
<h3>Google SWE Intern, 2021  ·  APIO Silver Medal, 2nd in Vietnam</h3>
<p>Placed 35th of 164 across 30 countries at the Asia-Pacific Informatics Olympiad. A decade of systems and competitive programming foundation.</p>
</div></div>

</div>

</div>

---

<!-- _class: content -->

<div class="kicker">Business model &amp; market</div>

# Usage-based. The Supabase playbook, one layer up.

<div class="title-divider"></div>

<div class="tiers">
<div class="tier">
<span class="pill">ADOPTION ENGINE</span>
<div class="name">Open Source</div>
<div class="sub">MIT core, self-host</div>
<div class="price">$0</div>
<hr/>
<ul>
<li>Full server + Python SDK</li>
<li>Single-node deploy with failover</li>
<li>Community support, Discord + GitHub</li>
<li>For hobbyists, indie hackers, OSS projects</li>
</ul>
</div>

<div class="tier primary">
<span class="pill">PRIMARY REVENUE</span>
<div class="name">Cloud</div>
<div class="sub">Managed, usage-based</div>
<div class="price">$20 – $5K / mo</div>
<hr/>
<ul>
<li>Free tier for hobbyists</li>
<li>Pay per event ingested + key-hour stored</li>
<li>Automated snapshots, dashboards, alerts</li>
<li>Same API as self-hosted — zero migration</li>
</ul>
</div>

<div class="tier secondary">
<span class="pill">HIGHEST ACV</span>
<div class="name">Enterprise</div>
<div class="sub">On-prem + support</div>
<div class="price">$150K – $500K / yr</div>
<hr/>
<ul>
<li>Air-gapped deployment</li>
<li>SLA-backed support contract</li>
<li>SOC2 / ISO 27001 assistance</li>
<li>For banks, insurers, telcos</li>
</ul>
</div>
</div>

<div class="arr-table">
<div class="arr-head">
<div>Horizon</div><div>Target ARR</div><div>Real comparable at same stage</div>
</div>
<div class="arr-row">
<div class="horizon">Year 3</div><div class="target">$10–20M</div><div class="ref">Supabase at year 3 (~$10M ARR)</div>
</div>
<div class="arr-row">
<div class="horizon">Year 5</div><div class="target">$60–80M</div><div class="ref">Supabase at year 5  ·  $70M ARR  ·  $5B val  ·  250% YoY</div>
</div>
<div class="arr-row">
<div class="horizon">Year 10</div><div class="target">$800M–$1.5B</div><div class="ref">Vercel, MongoDB, dbt Labs class</div>
</div>
<div class="arr-row">
<div class="horizon">Year 15</div><div class="target">$3–5B</div><div class="ref">Snowflake / Databricks class  ·  $10B+ valuation</div>
</div>
</div>

<div class="business-tag">Supabase is $5B on $70M ARR today, targeting $10B. Tally is one layer up the stack. Databricks spent <strong>$4B+</strong> consolidating the enterprise tier of our category since 2023.</div>

---

<!-- _class: vision -->
<!-- _paginate: false -->

<div class="kicker">The vision</div>

# Zero-ops.<br/>Streaming-first.<br/>Made in Vietnam.

<div class="vision-sub">
Tally is the default streaming data primitive for the next generation of AI-native products. Every workload an AI-native team writes in 2026 defaults through it. Built in Vietnam. Used everywhere.
</div>

<div class="asks-head">WHAT WE ARE ASKING FOR</div>

<div class="asks">
<div class="ask">
<h3>Mentorship</h3>
<p>Guidance from VAS judges and mentors on taking a Vietnam-origin developer-tools startup to a global audience — especially on the OSS → managed-cloud → enterprise sequence.</p>
</div>
<div class="ask">
<h3>Design partners</h3>
<p>Warm intros to 3–5 growth-stage fintech, e-commerce, or AI startups — Vietnamese or global — willing to be first production users in exchange for direct engineering support and a case study.</p>
</div>
<div class="ask">
<h3>Seed capital</h3>
<p>$1M pre-seed. <strong>First hire: GTM co-founder.</strong> Second hire: senior Rust engineer. Funds 18 months runway, Tally Cloud beta on FPT Cloud + AWS, and 10 design-partner deployments. I know my blind spot is sales motion, not engineering.</p>
</div>
</div>

<div class="thanks">
<div class="ty">Cảm ơn. Thank you.</div>
<div class="meta">phan.minhhoang2606@gmail.com  ·  github.com/petrpan26</div>
</div>
