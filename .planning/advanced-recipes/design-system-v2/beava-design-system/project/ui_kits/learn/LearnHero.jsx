// ui_kits/learn/LearnHero.jsx
const LearnHero = () => (
  <section style={{ padding: '56px 24px 32px', background: 'var(--bg-alt)', borderBottom: '1px solid var(--border)' }}>
    <div style={{ maxWidth: 900, margin: '0 auto' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 18 }}>
        <a style={{ fontFamily: 'var(--font-sans)', fontSize: 13, color: 'var(--fg3)', textDecoration: 'none' }}>← Field guide</a>
        <span style={{ color: 'var(--border-strong)' }}>·</span>
        <span style={{ fontFamily: 'var(--font-accent)', fontWeight: 700, fontSize: 24, color: 'var(--accent)', lineHeight: 1, transform: 'rotate(-1deg)', display: 'inline-block' }}>chapter 03</span>
      </div>
      <h1 style={{ fontFamily: 'var(--font-serif)', fontWeight: 600, fontSize: 60, lineHeight: 1.05, letterSpacing: '-0.02em', color: 'var(--fg1)', margin: '0 0 20px', fontFeatureSettings: '"ss01" on, "ss02" on', textWrap: 'balance' }}>
        Detecting fraud with rolling counters
      </h1>
      <p style={{ fontFamily: 'var(--font-sans)', fontSize: 20, lineHeight: 1.55, color: 'var(--fg2)', margin: '0 0 28px', maxWidth: 680, textWrap: 'pretty' }}>
        I spent a weekend trying to detect credit-card fraud with nothing but a CSV of transactions and a Postgres instance. This is what I learned, where it broke, and how a single operator replaces about 180 lines of window-function SQL.
      </p>
      <div style={{ display: 'flex', alignItems: 'center', gap: 14, marginBottom: 10 }}>
        <div style={{ width: 40, height: 40, borderRadius: 999, background: 'var(--beava-orange-wash)', color: 'var(--accent)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontFamily: 'var(--font-sans)', fontWeight: 700, fontSize: 15 }}>SR</div>
        <div>
          <div style={{ fontFamily: 'var(--font-sans)', fontSize: 14, fontWeight: 600, color: 'var(--fg1)' }}>Sam Rosen</div>
          <div style={{ fontFamily: 'var(--font-sans)', fontSize: 13, color: 'var(--fg3)' }}>12 min read · April 22, 2026</div>
        </div>
      </div>
    </div>
  </section>
);
window.LearnHero = LearnHero;
