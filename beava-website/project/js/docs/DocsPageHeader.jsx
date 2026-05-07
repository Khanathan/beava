// js/docs/DocsPageHeader.jsx
// Breadcrumbs + eyebrow + h1 + lede + optional meta-strip pills.
//   <DocsPageHeader
//     crumbs={[{label:'Docs',href:'/docs/'},{label:'Operating',href:null},{label:'Performance',cur:true}]}
//     eyebrow="Operating"
//     title="Performance"
//     lede="Numbers, methodology, and the harness for reproducing them."
//     meta={[{kind:'live',label:'stable'},{label:'v0'},{text:'Last updated 2026-05-03'}]}
//   />
const Crumbs = ({ crumbs = [] }) => (
  <div className="bv-crumbs">
    {crumbs.map((c, i) => (
      <React.Fragment key={i}>
        {c.cur
          ? <span className="bv-crumb-cur">{c.label}</span>
          : (c.href ? <a href={c.href}>{c.label}</a> : <span>{c.label}</span>)}
        {i < crumbs.length - 1 ? <span className="bv-crumb-sep">/</span> : null}
      </React.Fragment>
    ))}
  </div>
);
const MetaStrip = ({ meta = [] }) => (
  <div className="bv-meta-strip">
    {meta.map((m, i) => (
      <React.Fragment key={i}>
        {m.kind || m.label
          ? <span className={'bv-pill' + (m.kind ? ' ' + m.kind : '')}>{m.kind === 'live' ? <span>● </span> : null}{m.label}</span>
          : null}
        {m.text ? <span>{m.text}</span> : null}
        {i < meta.length - 1 ? <span className="bv-meta-dot">·</span> : null}
      </React.Fragment>
    ))}
  </div>
);
const DocsPageHeader = ({ crumbs, eyebrow, title, lede, meta }) => (
  <header className="bv-page-header">
    {crumbs ? <Crumbs crumbs={crumbs}/> : null}
    {eyebrow ? <div className="bv-eyebrow">{eyebrow}</div> : null}
    <h1>{title}</h1>
    {lede ? <div className="bv-lede">{lede}</div> : null}
    {meta && meta.length ? <MetaStrip meta={meta}/> : null}
  </header>
);
Object.assign(window, { DocsPageHeader, Crumbs, MetaStrip });
