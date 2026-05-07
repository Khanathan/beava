// js/docs/RFCRow.jsx
// Two layouts:
//   <RFCList items={[{id, title, desc, status, href}]} />        — community page
//   <RFCCards items={[...]} />                                    — intro page
// status: 'draft' | 'review' | 'accepted' | 'shipped' | undefined
const StatusBadge = ({ status, label }) => (
  <span className={'bv-rfc-status' + (status ? ' ' + status : '')}>{label || status || ''}</span>
);
const RFCList = ({ items = [] }) => (
  <div className="bv-rfc-list">
    {items.map(r => (
      <a key={r.id} className="bv-rfc-row" href={r.href || '#'}>
        <div className="bv-rfc-id">{r.id}</div>
        <div className="bv-rfc-body">
          <div className="bv-rfc-title">{r.title}</div>
          {r.desc ? <div className="bv-rfc-desc">{r.desc}</div> : null}
        </div>
        <StatusBadge status={r.status} label={r.statusLabel}/>
      </a>
    ))}
  </div>
);
const RFCCards = ({ items = [] }) => (
  <div className="bv-rfc-cards">
    {items.map(r => (
      <a key={r.id} className="bv-rfc-row" href={r.href || '#'}>
        <div className="bv-rfc-top">
          <span className="bv-rfc-id">{r.id}</span>
          <StatusBadge status={r.status} label={r.statusLabel}/>
        </div>
        <div className="bv-rfc-title">{r.title}</div>
        {r.desc ? <div className="bv-rfc-desc">{r.desc}</div> : null}
      </a>
    ))}
  </div>
);
Object.assign(window, { RFCList, RFCCards, StatusBadge });
