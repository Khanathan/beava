// ui_kits/marketing/CodeShowcase.jsx
const CodeShowcase = () => {
  const pre = {
    fontFamily: 'var(--font-mono)', fontSize: 13.5, lineHeight: 1.7,
    background: 'var(--code-bg)', color: 'var(--code-fg)',
    padding: '20px 22px', borderRadius: 12,
    border: '1px solid var(--border)',
    boxShadow: 'inset 0 1px 0 rgba(255,255,255,0.6)',
    margin: 0, overflowX: 'auto',
  };
  const S = {
    kw: { color: 'var(--code-keyword)' },
    str: { color: 'var(--code-string)' },
    cmt: { color: 'var(--code-comment)', fontStyle: 'italic' },
    fn: { color: 'var(--code-fn)' },
    num: { color: 'var(--code-number)' },
    ty: { color: 'var(--code-type)' },
  };
  return (
    <section style={{ padding: '96px 24px' }}>
      <div style={{ maxWidth: 1200, margin: '0 auto' }}>
        <div style={{ textAlign: 'center', marginBottom: 48 }}>
          <Eyebrow>How it feels</Eyebrow>
          <h2 style={{ fontFamily: 'var(--font-serif)', fontWeight: 600, fontSize: 48, lineHeight: 1.1, letterSpacing: '-0.02em', color: 'var(--fg1)', margin: '10px 0 0', fontFeatureSettings: '"ss01" on' }}>
            Define it. Query it. Ship.
          </h2>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24 }}>
          <div>
            <div style={{ fontFamily: 'var(--font-sans)', fontSize: 14, fontWeight: 600, color: 'var(--fg2)', marginBottom: 10, display: 'flex', alignItems: 'center', gap: 8 }}>
              <span style={{ display: 'inline-block', width: 22, height: 22, borderRadius: 999, background: 'var(--accent)', color: '#fff', textAlign: 'center', lineHeight: '22px', fontSize: 12 }}>1</span>
              Declare a feature
            </div>
            <pre style={pre}>
<span style={S.cmt}># ~/beava.toml</span>{'\n'}
<span style={S.kw}>[features.clicks_60s]</span>{'\n'}
<span style={S.ty}>type</span>   = <span style={S.str}>"rolling_counter"</span>{'\n'}
<span style={S.ty}>window</span> = <span style={S.str}>"60s"</span>{'\n'}
<span style={S.ty}>key</span>    = <span style={S.str}>"user_id"</span>{'\n'}
<span style={S.ty}>source</span> = <span style={S.str}>"events.click"</span>
            </pre>
          </div>
          <div>
            <div style={{ fontFamily: 'var(--font-sans)', fontSize: 14, fontWeight: 600, color: 'var(--fg2)', marginBottom: 10, display: 'flex', alignItems: 'center', gap: 8 }}>
              <span style={{ display: 'inline-block', width: 22, height: 22, borderRadius: 999, background: 'var(--accent)', color: '#fff', textAlign: 'center', lineHeight: '22px', fontSize: 12 }}>2</span>
              Push events, query features
            </div>
            <pre style={pre}>
<span style={S.cmt}>// in your CRUD app, from anywhere</span>{'\n'}
<span style={S.kw}>await</span> beava.<span style={S.fn}>push</span>(<span style={S.str}>"click"</span>, &#123; user_id: <span style={S.num}>42</span> &#125;);{'\n'}
{'\n'}
<span style={S.kw}>const</span> n = <span style={S.kw}>await</span> beava.<span style={S.fn}>get</span>({'\n'}
{'  '}<span style={S.str}>"clicks_60s"</span>, &#123; user_id: <span style={S.num}>42</span> &#125;{'\n'}
);{'\n'}
<span style={S.cmt}>// =&gt; 7</span>
            </pre>
          </div>
        </div>
      </div>
    </section>
  );
};
window.CodeShowcase = CodeShowcase;
