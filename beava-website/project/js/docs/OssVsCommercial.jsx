// js/docs/OssVsCommercial.jsx
// Two-column compare panel with hand-drawn "vs." between.
//   <OssVsCommercial
//     us={{ kicker:'Open source', title:'Self-host beava', blurb:'...', items:[...], link:{label,href}, price:'Free · forever' }}
//     them={{ kicker:'Coming later · 2026', title:'beava cloud', blurb:'...', items:[...], link:{label,href}, price:'From $0.10 / GB-hour · pay-as-you-go' }}
//   />
const Col = ({ side, kicker, title, blurb, items = [], link, price }) => (
  <div className={'bv-ovc-col ' + side}>
    {kicker ? <div className="bv-ovc-kicker">{kicker}</div> : null}
    {title ? <h3>{title}</h3> : null}
    {blurb ? <p>{blurb}</p> : null}
    <ul>{items.map((it, i) => <li key={i}>{it}</li>)}</ul>
    {link ? <a className="bv-ovc-link" href={link.href || '#'}>{link.label} →</a> : null}
    {price ? <div className="bv-ovc-price" dangerouslySetInnerHTML={{__html: price}}/> : null}
  </div>
);
const OssVsCommercial = ({ us, them }) => (
  <div className="bv-ovc">
    <Col side="us" {...us}/>
    <Col side="them" {...them}/>
  </div>
);
window.OssVsCommercial = OssVsCommercial;
