// ui_kits/marketing/Hero.jsx
const Hero = () => {
  return (
    <section style={{ padding: '64px 24px 96px', position: 'relative' }}>
      <div style={{ maxWidth: 1200, margin: '0 auto', display: 'grid', gridTemplateColumns: '1.2fr 1fr', gap: 48, alignItems: 'center' }}>
        <div>
          <div style={{ display: 'inline-flex', alignItems: 'center', gap: 8, padding: '6px 12px', borderRadius: 999, background: 'var(--beava-orange-wash)', border: '1px solid #f1d8c2', color: 'var(--accent)', fontSize: 13, fontWeight: 600, marginBottom: 20 }}>
            <span style={{ width: 6, height: 6, background: 'var(--accent)', borderRadius: 999, boxShadow: '0 0 0 3px rgba(184,92,32,0.18)' }}></span>
            v0.9.4 · now with signed events
          </div>
          <h1 style={{ fontFamily: 'var(--font-accent)', fontWeight: 700, fontSize: 116, lineHeight: 0.95, letterSpacing: '-0.01em', color: 'var(--accent)', margin: '0 0 16px', transform: 'rotate(-1.5deg)', transformOrigin: 'left center', display: 'inline-block' }}>
            Dam good at streams.
          </h1>
          <p style={{ fontFamily: 'var(--font-serif)', fontWeight: 500, fontSize: 28, lineHeight: 1.2, color: 'var(--fg2)', margin: '0 0 28px', maxWidth: 560, textWrap: 'pretty' }}>
            Real-time features without the streaming priesthood.
          </p>
          <p style={{ fontFamily: 'var(--font-sans)', fontSize: 20, lineHeight: 1.55, color: 'var(--fg2)', margin: '0 0 28px', maxWidth: 560, textWrap: 'pretty' }}>
            beava is a single-binary feature server for rolling counters, velocities, last-N-seen, leaderboards, and rate limits. HTTP in, HTTP out. No Kafka to babysit.
          </p>
          <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
            <Button variant="primary" size="lg" icon={<Icon name="arrow" size={16}/>}>Get started</Button>
            <Button variant="secondary" size="lg">
              <Icon name="github" size={16}/>
              Star on GitHub <span style={{ color: 'var(--fg3)', fontWeight: 500, marginLeft: 4 }}>· 8.2k</span>
            </Button>
          </div>
          <div style={{ marginTop: 24, fontFamily: 'var(--font-mono)', fontSize: 13, color: 'var(--fg3)', display: 'flex', alignItems: 'center', gap: 14 }}>
            <span>$ <span style={{ color: 'var(--fg1)' }}>curl beava.dev/install | sh</span></span>
            <span style={{ fontFamily: 'var(--font-accent)', fontWeight: 700, fontSize: 18, color: 'var(--accent)', lineHeight: 1, transform: 'rotate(-2deg)', display: 'inline-block' }}>
              ← really, that's it
            </span>
          </div>
        </div>
        <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', position: 'relative' }}>
          <div style={{ position: 'absolute', inset: '10% 15%', background: 'var(--beava-orange-wash)', borderRadius: '50%', filter: 'blur(40px)', opacity: 0.6 }}/>
          <img src="../../assets/logo-mark.png" width={380} height={380} style={{ position: 'relative', filter: 'drop-shadow(0 12px 30px rgba(184,92,32,0.15))' }}/>
        </div>
      </div>
    </section>
  );
};
window.Hero = Hero;
