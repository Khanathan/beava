// js/docs/Card.jsx
// Card with optional eyebrow / icon. Variants: default, compact, iconed.
const Card = ({ title, desc, href = '#', kicker, kickerAccent, icon, compact, children }) => {
  const cls = ['bv-card'];
  if (compact) cls.push('compact');
  if (icon) cls.push('iconed');
  return (
    <a className={cls.join(' ')} href={href}>
      {icon ? <span className="bv-card-icon">{icon}</span> : null}
      {kicker ? <div className={'bv-card-kicker' + (kickerAccent ? ' accent' : '')}>{kicker}</div> : null}
      <div className="bv-card-title">{title}</div>
      {desc ? <div className="bv-card-desc">{desc}</div> : null}
      {children}
      <span className="bv-card-arrow">→</span>
    </a>
  );
};
const CardGrid = ({ children }) => <div className="bv-card-grid">{children}</div>;
Object.assign(window, { Card, CardGrid });
