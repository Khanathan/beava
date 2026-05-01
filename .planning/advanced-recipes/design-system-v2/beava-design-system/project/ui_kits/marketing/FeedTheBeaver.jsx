// ui_kits/marketing/FeedTheBeaver.jsx
const FeedTheBeaver = () => {
  const [count, setCount] = React.useState(0);
  const [recent, setRecent] = React.useState([]);
  const [rate, setRate] = React.useState(0);
  const countRef = React.useRef(0);

  const feed = () => {
    countRef.current += 1;
    setCount(countRef.current);
    const t = Date.now();
    setRecent(prev => [...prev.slice(-11), t]);
  };

  React.useEffect(() => {
    const id = setInterval(() => {
      const now = Date.now();
      setRecent(prev => prev.filter(t => now - t < 10000));
      setRate(recent.filter(t => now - t < 10000).length);
    }, 500);
    return () => clearInterval(id);
  }, [recent]);

  return (
    <section style={{ padding: '96px 24px', background: 'var(--beava-orange-wash)' }}>
      <div style={{ maxWidth: 1100, margin: '0 auto', background: '#fff', border: '1px solid var(--border)', borderRadius: 20, padding: 40, boxShadow: '0 4px 8px rgba(26,23,20,0.06), 0 18px 40px rgba(26,23,20,0.10)', display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 40, alignItems: 'center' }}>
        <div>
          <Eyebrow>This page uses beava</Eyebrow>
          <h2 style={{ fontFamily: 'var(--font-serif)', fontWeight: 600, fontSize: 40, lineHeight: 1.1, letterSpacing: '-0.02em', color: 'var(--fg1)', margin: '10px 0 14px', fontFeatureSettings: '"ss01" on' }}>
            Feed the beaver
          </h2>
          <p style={{ fontFamily: 'var(--font-sans)', fontSize: 16, lineHeight: 1.6, color: 'var(--fg2)', margin: '0 0 24px' }}>
            Every click below is a real event pushed to a real beava instance. The counters below are real feature queries. It's turtles all the way down.
          </p>
          <div style={{ display: 'flex', alignItems: 'center', gap: 14 }}>
            <button onClick={feed} style={{
              background: 'var(--accent)', color: '#fff', border: 0, fontWeight: 600, fontSize: 16,
              padding: '14px 24px', borderRadius: 10, cursor: 'pointer',
              fontFamily: 'var(--font-sans)',
              boxShadow: '0 2px 4px rgba(26,23,20,0.08)',
              display: 'inline-flex', alignItems: 'center', gap: 10,
            }}>
              <Icon name="zap" size={16}/>
              Feed the beaver
            </button>
            <span style={{ fontFamily: 'var(--font-accent)', fontWeight: 700, fontSize: 20, color: 'var(--accent)', lineHeight: 1, transform: 'rotate(-3deg)', display: 'inline-block' }}>
              ← go on, click it
            </span>
          </div>
          <div style={{ marginTop: 14, fontFamily: 'var(--font-mono)', fontSize: 12, color: 'var(--fg3)' }}>
            POST /events/feed &middot; {count} sent this session
          </div>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <StatRow label="Total feeds (all-time)" value={count + 12847} hint="counter.total" />
          <StatRow label="Feeds in last 10s" value={rate} hint="rolling_counter(10s)" live />
          <StatRow label="Feeds in last 60s" value={recent.length} hint="rolling_counter(60s)" />
          <div style={{ display: 'flex', justifyContent: 'center', marginTop: 4 }}>
            <img src="../../assets/mascot-pose-3.png" width={120} height={120}/>
          </div>
        </div>
      </div>
    </section>
  );
};

const StatRow = ({ label, value, hint, live }) => (
  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '14px 18px', background: 'var(--beava-paper)', border: '1px solid var(--border)', borderRadius: 12 }}>
    <div>
      <div style={{ fontFamily: 'var(--font-sans)', fontSize: 13, color: 'var(--fg3)' }}>{label}</div>
      <div style={{ fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--accent)', marginTop: 2 }}>{hint}</div>
    </div>
    <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
      {live && <span style={{ fontSize: 11, fontFamily: 'var(--font-sans)', fontWeight: 600, color: 'var(--beava-success)', display: 'flex', alignItems: 'center', gap: 5 }}>
        <span style={{ width: 6, height: 6, borderRadius: 999, background: 'var(--beava-success)', boxShadow: '0 0 0 3px rgba(74,122,58,0.15)' }}/>LIVE
      </span>}
      <div style={{ fontFamily: 'var(--font-serif)', fontWeight: 600, fontSize: 28, color: 'var(--fg1)', letterSpacing: '-0.02em' }}>{value.toLocaleString()}</div>
    </div>
  </div>
);

window.FeedTheBeaver = FeedTheBeaver;
