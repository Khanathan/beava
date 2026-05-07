// ui_kits/docs/DocsTOC.jsx
const TOC = [
  { id: 'what', label: 'What is a rolling counter?' },
  { id: 'define', label: 'Defining a counter', sub: true },
  { id: 'push', label: 'Pushing events', sub: true },
  { id: 'query', label: 'Querying', sub: true },
  { id: 'perf', label: 'Performance' },
  { id: 'caveats', label: 'Caveats' },
];

const DocsTOC = () => (
  <aside style={{ width: 200, flexShrink: 0, padding: '32px 14px', position: 'sticky', top: 60, alignSelf: 'flex-start' }}>
    <div style={{ fontFamily: 'var(--font-sans)', fontSize: 12, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em', color: 'var(--fg3)', marginBottom: 12 }}>On this page</div>
    <ul style={{ listStyle: 'none', padding: 0, margin: 0, display: 'flex', flexDirection: 'column', gap: 6 }}>
      {TOC.map((t,i) => (
        <li key={t.id}><a href={'#'+t.id} style={{
          display: 'block', padding: '3px 0', paddingLeft: t.sub ? 12 : 0,
          fontSize: 13.5, color: i===1 ? 'var(--accent)' : 'var(--fg3)',
          fontWeight: i===1 ? 600 : 400,
          textDecoration: 'none', fontFamily: 'var(--font-sans)',
          borderLeft: i===1 ? '2px solid var(--accent)' : '2px solid transparent',
          paddingLeft: t.sub ? 14 : 10, marginLeft: t.sub ? 0 : 0,
        }}>{t.label}</a></li>
      ))}
    </ul>
    <div style={{ marginTop: 28, padding: '12px 14px', background: 'var(--beava-paper)', border: '1px solid var(--border)', borderRadius: 10 }}>
      <div style={{ fontFamily: 'var(--font-sans)', fontSize: 12, fontWeight: 600, color: 'var(--fg2)', marginBottom: 4 }}>Edit this page</div>
      <a style={{ fontFamily: 'var(--font-sans)', fontSize: 12, color: 'var(--accent)', textDecoration: 'none' }}>GitHub →</a>
    </div>
  </aside>
);
window.DocsTOC = DocsTOC;
