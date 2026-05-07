// js/docs/DocsShared.jsx
// Icons + a small Pager helper. All heading/paragraph styling lives in docs-kit.css —
// pages use raw <h1>/<h2>/<p> tags inside <main className="bv-content">.
const DIcon = ({ name, size = 16, stroke = 1.75 }) => {
  const paths = {
    search: <><circle cx="11" cy="11" r="7"/><path d="M21 21l-4.35-4.35"/></>,
    github: <path d="M12 2a10 10 0 0 0-3.16 19.49c.5.09.68-.22.68-.48v-1.7c-2.78.6-3.37-1.34-3.37-1.34-.46-1.16-1.11-1.47-1.11-1.47-.91-.62.07-.6.07-.6 1 .07 1.53 1.03 1.53 1.03.89 1.53 2.34 1.09 2.91.83.09-.65.35-1.09.63-1.34-2.22-.25-4.56-1.11-4.56-4.94 0-1.09.39-1.98 1.03-2.68-.1-.26-.45-1.27.1-2.65 0 0 .84-.27 2.75 1.02a9.56 9.56 0 0 1 5 0c1.91-1.29 2.75-1.02 2.75-1.02.55 1.38.2 2.39.1 2.65.64.7 1.03 1.59 1.03 2.68 0 3.84-2.34 4.68-4.57 4.93.36.31.68.92.68 1.85v2.75c0 .27.18.58.69.48A10 10 0 0 0 12 2z"/>,
    chevron: <path d="M6 9l6 6 6-6"/>,
    copy: <><rect x="9" y="9" width="12" height="12" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></>,
    check: <path d="M20 6L9 17l-5-5"/>,
    book: <><path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"/><path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z"/></>,
  };
  return <svg width={size} height={size} viewBox="0 0 24 24" fill={name === 'github' ? 'currentColor' : 'none'} stroke="currentColor" strokeWidth={stroke} strokeLinecap="round" strokeLinejoin="round">{paths[name]}</svg>;
};

// Pager — prev/next at the bottom of a doc page.
//   <Pager auto/>                                         — auto-resolve from FLAT_SIDEBAR
//   <Pager prev={{label,href}} next={{label,href}}/>      — explicit override
// Either side may be null/missing.
const Pager = ({ prev, next, auto }) => {
  if (auto && window.FLAT_SIDEBAR) {
    const path = window.location.pathname;
    const idx = window.FLAT_SIDEBAR.findIndex(it => it.href === path);
    if (idx >= 0) {
      prev = idx > 0 ? window.FLAT_SIDEBAR[idx - 1] : null;
      next = idx < window.FLAT_SIDEBAR.length - 1 ? window.FLAT_SIDEBAR[idx + 1] : null;
    }
  }
  return (
    <div className="bv-pager">
      {prev ? (
        <a className="prev" href={prev.href}>
          <div className="bv-pager-dir">← Previous</div>
          <div className="bv-pager-ttl">{prev.label}</div>
        </a>
      ) : <span className="bv-pager-spacer"/>}
      {next ? (
        <a className="next" href={next.href}>
          <div className="bv-pager-dir">Next →</div>
          <div className="bv-pager-ttl">{next.label}</div>
        </a>
      ) : <span className="bv-pager-spacer"/>}
    </div>
  );
};

// <Code lang="python">{`...`}</Code> — syntax-highlighted code block.
// Lazy-loads highlight.js (common bundle, ~50KB gz) on first mount.
// hljs token classes are themed via docs-kit.css to match the warm code palette.
const Code = ({ lang = 'python', children }) => {
  const ref = React.useRef(null);
  React.useEffect(() => {
    let cancelled = false;
    const apply = () => {
      if (cancelled || !ref.current || !window.hljs) return;
      // Strip any prior highlight (in case useEffect re-runs after children change).
      ref.current.removeAttribute('data-highlighted');
      ref.current.textContent = ref.current.textContent;
      window.hljs.highlightElement(ref.current);
    };
    if (window.hljs) { apply(); return; }
    if (!window.__hljsLoading) {
      window.__hljsLoading = new Promise((resolve) => {
        const s = document.createElement('script');
        s.src = 'https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.10.0/highlight.min.js';
        s.onload = () => resolve();
        s.onerror = () => { console.warn('highlight.js failed to load'); resolve(); };
        document.head.appendChild(s);
      });
    }
    window.__hljsLoading.then(apply);
    return () => { cancelled = true; };
  }, [children, lang]);
  return (
    <pre className="bv-code">
      <code ref={ref} className={`language-${lang}`}>{children}</code>
    </pre>
  );
};

Object.assign(window, { DIcon, Pager, Code });
