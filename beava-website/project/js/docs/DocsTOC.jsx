// js/docs/DocsTOC.jsx
// Right-rail table of contents. Generic — pages pass items.
// items: [{ id, label, sub? }]
const DocsTOC = ({ items = [], activeId, editHref = '#' }) => (
  <nav className="bv-toc" aria-label="On this page">
    <div className="bv-toc-kicker">On this page</div>
    <ul>
      {items.map(it => {
        const isActive = it.id === activeId;
        const cls = (it.sub ? 'sub' : '') + (isActive ? ' active' : '');
        return (
          <li key={it.id} className={cls.trim()}>
            <a href={'#' + it.id}>{it.label}</a>
          </li>
        );
      })}
    </ul>
    {editHref ? (
      <div className="bv-toc-foot">
        <span>Edit this page on</span>
        <a href={editHref}>GitHub →</a>
      </div>
    ) : null}
  </nav>
);
window.DocsTOC = DocsTOC;
