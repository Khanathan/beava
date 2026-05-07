// js/docs/CommitmentCard.jsx
// Used on /community/. 2-up grid of <CommitmentCard icon title body more href/>.
//   <CommitmentGrid>
//     <CommitmentCard icon={<svg.../>} title="..." body="..." more="Read →" href="..."/>
//   </CommitmentGrid>
const CommitmentCard = ({ icon, title, body, more, href = '#' }) => (
  <a className="bv-commit" href={href}>
    {icon ? <span className="bv-commit-ic">{icon}</span> : null}
    <h4>{title}</h4>
    <p>{body}</p>
    {more ? <span className="bv-commit-more">{more} →</span> : null}
  </a>
);
const CommitmentGrid = ({ children }) => <div className="bv-commitments">{children}</div>;
Object.assign(window, { CommitmentCard, CommitmentGrid });
