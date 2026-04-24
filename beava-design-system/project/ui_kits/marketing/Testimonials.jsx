// ui_kits/marketing/Testimonials.jsx
const QUOTES = [
  { quote: "We replaced our whole Flink setup with one Go binary. I haven't opened Grafana in three weeks, and that's the nicest thing I can say about any infra.", name: "Priya S.", title: "Staff eng, mid-size fintech" },
  { quote: "The HTTP-first thing sounds trivial until you realize you can just… call it from a Rails controller. Our feature store PR was 40 lines.", name: "Marcus W.", title: "CTO, seed-stage B2B" },
];

const Testimonials = () => (
  <section style={{ padding: '96px 24px' }}>
    <div style={{ maxWidth: 1100, margin: '0 auto' }}>
      <div style={{ textAlign: 'center', marginBottom: 48 }}>
        <Eyebrow>What people say</Eyebrow>
        <h2 style={{ fontFamily: 'var(--font-serif)', fontWeight: 600, fontSize: 40, lineHeight: 1.1, letterSpacing: '-0.02em', color: 'var(--fg1)', margin: '10px 0 0', fontFeatureSettings: '"ss01" on' }}>
          Quiet infra, loud relief
        </h2>
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20 }}>
        {QUOTES.map((q, i) => (
          <div key={i} style={{ background: '#fff', border: '1px solid var(--border)', borderRadius: 14, padding: 28, boxShadow: '0 1px 2px rgba(26,23,20,0.04), 0 2px 6px rgba(26,23,20,0.06)' }}>
            <div style={{ fontFamily: 'var(--font-serif)', fontSize: 36, color: 'var(--accent)', lineHeight: 0.5, marginBottom: 8 }}>&ldquo;</div>
            <p style={{ fontFamily: 'var(--font-sans)', fontSize: 17, lineHeight: 1.55, color: 'var(--fg1)', margin: '0 0 16px', textWrap: 'pretty' }}>{q.quote}</p>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, paddingTop: 14, borderTop: '1px solid var(--border)' }}>
              <div style={{ width: 32, height: 32, borderRadius: 999, background: 'var(--beava-orange-wash)', color: 'var(--accent)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontWeight: 700, fontSize: 13, fontFamily: 'var(--font-sans)' }}>{q.name[0]}</div>
              <div>
                <div style={{ fontFamily: 'var(--font-sans)', fontSize: 13, fontWeight: 600, color: 'var(--fg1)' }}>{q.name}</div>
                <div style={{ fontFamily: 'var(--font-sans)', fontSize: 12, color: 'var(--fg3)' }}>{q.title}</div>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  </section>
);
window.Testimonials = Testimonials;
