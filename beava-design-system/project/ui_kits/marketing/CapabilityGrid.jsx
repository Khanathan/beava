// ui_kits/marketing/CapabilityGrid.jsx
const CAPS = [
  { icon: 'chart', title: 'Rolling counters', desc: 'Sliding time windows over any event stream. Sub-ms queries, no tuning.' },
  { icon: 'clock', title: 'Velocities', desc: 'Rate-of-change over any window. Detect surges and drops without math.' },
  { icon: 'list', title: 'Leaderboards', desc: 'Top-N by any metric, always sorted, never stale. Free pagination.' },
  { icon: 'gauge', title: 'Rate limits', desc: 'Per-key token buckets. Distributed, durable, HTTP-native.' },
  { icon: 'zap', title: 'Last-N-seen', desc: 'Cheap recency queries: "did this user touch X in the last week?"' },
  { icon: 'lock', title: 'Single binary', desc: 'One Go binary. No ZooKeeper, no Kafka, no dedicated ops team.' },
];

const CapabilityGrid = () => (
  <section style={{ padding: '96px 24px', background: 'var(--bg-alt)', borderTop: '1px solid var(--border)', borderBottom: '1px solid var(--border)' }}>
    <div style={{ maxWidth: 1200, margin: '0 auto' }}>
      <div style={{ textAlign: 'center', marginBottom: 56, position: 'relative' }}>
        <Eyebrow>What beava does</Eyebrow>
        <h2 style={{ fontFamily: 'var(--font-serif)', fontWeight: 600, fontSize: 48, lineHeight: 1.1, letterSpacing: '-0.02em', color: 'var(--fg1)', margin: '10px 0 12px', fontFeatureSettings: '"ss01" on' }}>
          Purpose-built operators, not a framework
        </h2>
        <p style={{ fontFamily: 'var(--font-sans)', fontSize: 18, color: 'var(--fg2)', maxWidth: 620, margin: '0 auto 10px', lineHeight: 1.5 }}>
          Six primitives that cover 80% of real-time feature work. If you can describe it as a rolling window, beava probably already does it.
        </p>
        <div style={{ fontFamily: 'var(--font-accent)', fontWeight: 700, fontSize: 22, color: 'var(--accent)', lineHeight: 1, transform: 'rotate(-2deg)', display: 'inline-block', marginTop: 4 }}>
          six. that's the whole list.
        </div>
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 20 }}>
        {CAPS.map(c => (
          <div key={c.title} style={{
            background: '#fff', border: '1px solid var(--border)', borderRadius: 14, padding: 24,
            boxShadow: '0 1px 2px rgba(26,23,20,0.04), 0 2px 6px rgba(26,23,20,0.06)',
            transition: 'all 200ms cubic-bezier(0.22,1,0.36,1)',
          }}
          onMouseEnter={e => { e.currentTarget.style.transform = 'translateY(-2px)'; e.currentTarget.style.boxShadow = '0 2px 4px rgba(26,23,20,0.04), 0 8px 20px rgba(26,23,20,0.08)'; }}
          onMouseLeave={e => { e.currentTarget.style.transform = ''; e.currentTarget.style.boxShadow = '0 1px 2px rgba(26,23,20,0.04), 0 2px 6px rgba(26,23,20,0.06)'; }}
          >
            <div style={{ width: 40, height: 40, borderRadius: 10, background: 'var(--beava-orange-wash)', color: 'var(--accent)', display: 'flex', alignItems: 'center', justifyContent: 'center', marginBottom: 16 }}>
              <Icon name={c.icon} size={20}/>
            </div>
            <h4 style={{ fontFamily: 'var(--font-sans)', fontWeight: 600, fontSize: 17, margin: '0 0 6px', color: 'var(--fg1)' }}>{c.title}</h4>
            <p style={{ fontFamily: 'var(--font-sans)', fontSize: 14, lineHeight: 1.55, color: 'var(--fg3)', margin: 0 }}>{c.desc}</p>
          </div>
        ))}
      </div>
    </div>
  </section>
);
window.CapabilityGrid = CapabilityGrid;
