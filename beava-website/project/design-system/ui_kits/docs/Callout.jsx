// ui_kits/docs/Callout.jsx
const Callout = ({ kind = 'note', title, children }) => {
  const map = {
    note: { bg: 'var(--beava-info-wash)', border: '#cadae4', dot: 'var(--beava-info)', ic: 'i' },
    tip:  { bg: 'var(--beava-success-wash)', border: '#cfdcbd', dot: 'var(--beava-success)', ic: '✓' },
    warn: { bg: 'var(--beava-warn-wash)', border: '#ecd89a', dot: 'var(--beava-warn)', ic: '!' },
  }[kind];
  return (
    <div style={{ padding: '16px 18px', borderRadius: 12, background: map.bg, border: `1px solid ${map.border}`, display: 'flex', gap: 12, fontFamily: 'var(--font-sans)', fontSize: 14.5, lineHeight: 1.6, margin: '20px 0' }}>
      <span style={{ width: 22, height: 22, flexShrink: 0, borderRadius: 999, background: map.dot, color: '#fff', display: 'flex', alignItems: 'center', justifyContent: 'center', fontWeight: 700, fontSize: 12, fontFamily: 'var(--font-mono)', marginTop: 1 }}>{map.ic}</span>
      <div>
        {title && <strong style={{ display: 'block', color: 'var(--fg1)', marginBottom: 3 }}>{title}</strong>}
        <div style={{ color: 'var(--fg2)' }}>{children}</div>
      </div>
    </div>
  );
};
window.Callout = Callout;
