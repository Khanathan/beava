// ui_kits/docs/CodeBlock.jsx
const CodeBlock = ({ lang = 'toml', children }) => {
  const [copied, setCopied] = React.useState(false);
  const text = React.Children.toArray(children).map(c => typeof c === 'string' ? c : '').join('');
  const onCopy = () => { setCopied(true); setTimeout(() => setCopied(false), 1200); };
  return (
    <div style={{ position: 'relative', margin: '18px 0' }}>
      <div style={{ position: 'absolute', top: 10, right: 10, display: 'flex', alignItems: 'center', gap: 8 }}>
        <span style={{ fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--fg3)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>{lang}</span>
        <button onClick={onCopy} style={{ display: 'flex', alignItems: 'center', gap: 5, background: '#fff', border: '1px solid var(--border)', borderRadius: 6, padding: '4px 8px', cursor: 'pointer', color: 'var(--fg2)', fontFamily: 'var(--font-sans)', fontSize: 11 }}>
          <DIcon name={copied ? 'check' : 'copy'} size={12}/> {copied ? 'Copied' : 'Copy'}
        </button>
      </div>
      <pre style={{ fontFamily: 'var(--font-mono)', fontSize: 13.5, lineHeight: 1.65, background: 'var(--code-bg)', color: 'var(--code-fg)', padding: '18px 22px', paddingTop: 36, borderRadius: 10, border: '1px solid var(--border)', boxShadow: 'inset 0 1px 0 rgba(255,255,255,0.6)', margin: 0, overflowX: 'auto' }}>
        {children}
      </pre>
    </div>
  );
};

const K = ({ children }) => <span style={{ color: 'var(--code-keyword)' }}>{children}</span>;
const S = ({ children }) => <span style={{ color: 'var(--code-string)' }}>{children}</span>;
const C = ({ children }) => <span style={{ color: 'var(--code-comment)', fontStyle: 'italic' }}>{children}</span>;
const N = ({ children }) => <span style={{ color: 'var(--code-number)' }}>{children}</span>;
const F = ({ children }) => <span style={{ color: 'var(--code-fn)' }}>{children}</span>;
const T = ({ children }) => <span style={{ color: 'var(--code-type)' }}>{children}</span>;

Object.assign(window, { CodeBlock, K, S, C, N, F, T });
