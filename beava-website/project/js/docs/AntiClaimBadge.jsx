// js/docs/AntiClaimBadge.jsx
// "no Kafka", "no JVM" — typographic negation chips.
//   <AntiClaim>Kafka</AntiClaim>
//   <AntiClaim mono>JVM</AntiClaim>
//   <AntiClaimRow chips dotted? items={['JVM','Kafka','Flink']} />
const AntiClaim = ({ mono, children }) => (
  <span className="bv-anti">
    <span className="bv-anti-pre">no</span>
    <span className={'bv-anti-word' + (mono ? ' mono' : '')}>{children}</span>
  </span>
);
const AntiClaimRow = ({ items = [], dotted, chips, monoSet = new Set() }) => {
  const cls = ['bv-anti-row'];
  if (dotted) cls.push('dotted');
  if (chips) cls.push('chips');
  return (
    <div className={cls.join(' ')}>
      {items.map((it, i) => (
        <React.Fragment key={it}>
          <AntiClaim mono={monoSet.has && monoSet.has(it)}>{it}</AntiClaim>
          {dotted && i < items.length - 1 ? <span className="bv-sep">·</span> : null}
        </React.Fragment>
      ))}
    </div>
  );
};
Object.assign(window, { AntiClaim, AntiClaimRow });
