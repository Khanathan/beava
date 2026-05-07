// js/docs/StatGrid.jsx
// Three layouts:
//  <StatGrid lg> ... </StatGrid>      — 3-up large, perf hero
//  <StatGrid> ... </StatGrid>         — 4-up dense
//  <StatGrid loose> ... </StatGrid>   — 3-up cream cards, intro pages
// Children: <Stat label num unit? delta? deltaTone? />
const Stat = ({ label, num, unit, delta, deltaTone }) => (
  <div className="bv-stat">
    <div className="bv-stat-label">{label}</div>
    <div className="bv-stat-num">
      {num}
      {unit ? <span className="bv-stat-unit">{unit}</span> : null}
    </div>
    {delta ? <div className={'bv-stat-delta' + (deltaTone ? ' ' + deltaTone : '')}>{delta}</div> : null}
  </div>
);
const StatGrid = ({ lg, loose, children }) => {
  if (loose) return <div className="bv-stat-row">{children}</div>;
  return <div className={'bv-stat-grid' + (lg ? ' lg' : '')}>{children}</div>;
};
Object.assign(window, { Stat, StatGrid });
