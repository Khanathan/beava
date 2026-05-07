// ui_kits/learn/ChapterGrid.jsx
const CHAPTERS = [
  { n: 1, title: 'Why streaming was hard', read: '8 min' },
  { n: 2, title: 'The five operators you actually need', read: '10 min' },
  { n: 3, title: 'Detecting fraud with rolling counters', read: '12 min', current: true },
  { n: 4, title: 'Building a leaderboard that never stalls', read: '9 min' },
  { n: 5, title: 'Rate limiting across a fleet', read: '11 min' },
  { n: 6, title: 'Last-N-seen: the sneaky useful primitive', read: '7 min' },
];

const ChapterGrid = () => (
  <section style={{ padding: '64px 24px', background: 'var(--bg-alt)', borderTop: '1px solid var(--border)' }}>
    <div style={{ maxWidth: 1000, margin: '0 auto' }}>
      <div style={{ fontFamily: 'var(--font-sans)', fontSize: 12, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em', color: 'var(--accent)', marginBottom: 8 }}>The field guide</div>
      <h2 style={{ fontFamily: 'var(--font-serif)', fontWeight: 600, fontSize: 36, lineHeight: 1.1, letterSpacing: '-0.02em', color: 'var(--fg1)', margin: '0 0 28px', fontFeatureSettings: '"ss01" on' }}>
        Other chapters
      </h2>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 14 }}>
        {CHAPTERS.filter(c => !c.current).map(c => (
          <a key={c.n} style={{ display: 'flex', gap: 16, padding: 20, background: '#fff', border: '1px solid var(--border)', borderRadius: 12, textDecoration: 'none', alignItems: 'center' }}>
            <div style={{ width: 46, height: 46, borderRadius: 10, background: 'var(--beava-paper)', color: 'var(--accent)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontFamily: 'var(--font-serif)', fontWeight: 700, fontSize: 22, flexShrink: 0 }}>
              {c.n}
            </div>
            <div style={{ flex: 1 }}>
              <div style={{ fontFamily: 'var(--font-serif)', fontSize: 18, fontWeight: 600, color: 'var(--fg1)', lineHeight: 1.25, fontFeatureSettings: '"ss01" on', marginBottom: 3 }}>{c.title}</div>
              <div style={{ fontFamily: 'var(--font-sans)', fontSize: 13, color: 'var(--fg3)' }}>Chapter {c.n} · {c.read}</div>
            </div>
            <div style={{ color: 'var(--accent)', fontSize: 18 }}>→</div>
          </a>
        ))}
      </div>
    </div>
  </section>
);
window.ChapterGrid = ChapterGrid;
