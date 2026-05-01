// ui_kits/docs/DocsSidebar.jsx
const SECTIONS = [
  { title: 'Getting started', items: ['Install', 'Quick start', 'Configuration', 'Deploy'] },
  { title: 'Features', items: ['Rolling counters', 'Velocities', 'Leaderboards', 'Rate limits', 'Last-N-seen', 'Custom operators'] },
  { title: 'Operating', items: ['Storage', 'Replication', 'Backups', 'Monitoring'] },
  { title: 'Reference', items: ['HTTP API', 'CLI', 'Config schema', 'Client libraries'] },
];

const DocsSidebar = ({ active = 'Rolling counters' }) => (
  <aside style={{
    width: 260, flexShrink: 0, padding: '32px 0 32px 4px',
    position: 'sticky', top: 60, alignSelf: 'flex-start',
    maxHeight: 'calc(100vh - 60px)', overflowY: 'auto',
  }}>
    {SECTIONS.map(sec => (
      <div key={sec.title} style={{ marginBottom: 24 }}>
        <div style={{ fontFamily: 'var(--font-sans)', fontSize: 12, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em', color: 'var(--fg3)', padding: '0 14px', marginBottom: 8 }}>{sec.title}</div>
        <ul style={{ listStyle: 'none', padding: 0, margin: 0 }}>
          {sec.items.map(it => {
            const isActive = it === active;
            return (
              <li key={it}>
                <a style={{
                  display: 'block', padding: '6px 14px', fontSize: 14,
                  color: isActive ? 'var(--accent)' : 'var(--fg2)',
                  fontWeight: isActive ? 600 : 500,
                  background: isActive ? 'var(--beava-orange-wash)' : 'transparent',
                  borderLeft: isActive ? '2px solid var(--accent)' : '2px solid transparent',
                  textDecoration: 'none', fontFamily: 'var(--font-sans)',
                  borderRadius: '0 8px 8px 0',
                }}>{it}</a>
              </li>
            );
          })}
        </ul>
      </div>
    ))}
  </aside>
);
window.DocsSidebar = DocsSidebar;
