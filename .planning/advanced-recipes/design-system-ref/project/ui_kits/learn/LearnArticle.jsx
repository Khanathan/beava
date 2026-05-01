// ui_kits/learn/LearnArticle.jsx
const LH2 = ({ children }) => <h2 style={{ fontFamily: 'var(--font-serif)', fontWeight: 600, fontSize: 34, lineHeight: 1.2, letterSpacing: '-0.02em', color: 'var(--fg1)', margin: '48px 0 14px', fontFeatureSettings: '"ss01" on' }}>{children}</h2>;
const LH3 = ({ children }) => <h3 style={{ fontFamily: 'var(--font-serif)', fontWeight: 600, fontSize: 24, lineHeight: 1.25, color: 'var(--fg1)', margin: '32px 0 10px' }}>{children}</h3>;
const LP = ({ children }) => <p style={{ fontFamily: 'var(--font-sans)', fontSize: 18, lineHeight: 1.75, color: 'var(--fg1)', margin: '0 0 20px', textWrap: 'pretty' }}>{children}</p>;
const LInline = ({ children }) => <code style={{ fontFamily: 'var(--font-mono)', fontSize: '0.9em', background: 'var(--bg-inset)', color: 'var(--code-fg)', padding: '0.12em 0.4em', borderRadius: 4, border: '1px solid var(--border-inset)' }}>{children}</code>;

const LearnArticle = () => (
  <article style={{ maxWidth: 720, margin: '0 auto', padding: '48px 24px 32px' }}>
    <LP>
      A friend at a small fintech was trying to answer a specific question: <strong>"in the last 5 minutes, has this card been used more than 10 times across more than 3 merchants?"</strong> If yes, flag. Simple enough to describe. Absolutely miserable to implement with a 2019-era stack.
    </LP>

    <LP>
      Her first attempt was a Postgres window query over a <LInline>transactions</LInline> table. It worked on 10k rows. It started to hurt at 100k. At 1M it stopped being online, and she gave up and moved to a batch job that ran every 5 minutes, which — you'll be shocked to learn — does not detect fraud in real time.
    </LP>

    <div style={{ margin: '32px -40px', background: 'var(--beava-paper)', border: '1px solid var(--border)', borderRadius: 14, padding: 32, position: 'relative' }}>
      <img src="../../assets/mascot-work-pose.svg" width={90} height={90} style={{ position: 'absolute', top: -20, right: 20, transform: 'rotate(8deg)' }}/>
      <div style={{ fontFamily: 'var(--font-sans)', fontSize: 12, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em', color: 'var(--accent)', marginBottom: 8 }}>What we're building</div>
      <div style={{ fontFamily: 'var(--font-serif)', fontSize: 22, lineHeight: 1.3, color: 'var(--fg1)', fontFeatureSettings: '"ss01" on' }}>
        Two rolling counters plus one threshold check. Total: 18 lines of code.
      </div>
    </div>

    <LH2>The naive SQL approach</LH2>
    <LP>
      If you've done any analytics work, the first thing you reach for is a window function. Something like:
    </LP>

    <CodeBlock lang="sql">
<K>SELECT</K> card_id,{'\n'}
{'  '}<F>COUNT</F>(*) <K>OVER</K> (<K>PARTITION BY</K> card_id{'\n'}
{'    '}<K>ORDER BY</K> ts <K>RANGE BETWEEN</K> <S>'5 minutes'</S> <K>PRECEDING AND CURRENT ROW</K>){'\n'}
<K>FROM</K> transactions{'\n'}
<K>WHERE</K> ts &gt; <F>NOW</F>() - <K>INTERVAL</K> <S>'5 minutes'</S>;
    </CodeBlock>

    <LP>
      This works. It also makes your DBA cry when traffic goes up. Window functions over time ranges are unfriendly to indexes, and you're asking Postgres to be a streaming system it wasn't born to be.
    </LP>

    <LH2>The beava version</LH2>
    <LP>Define two counters:</LP>

    <CodeBlock lang="toml">
<K>[features.card_tx_5m]</K>{'\n'}
<T>type</T>   = <S>"rolling_counter"</S>{'\n'}
<T>window</T> = <S>"5m"</S>{'\n'}
<T>key</T>    = <S>"card_id"</S>{'\n'}
<T>source</T> = <S>"events.tx"</S>{'\n'}
{'\n'}
<K>[features.card_merchants_5m]</K>{'\n'}
<T>type</T>   = <S>"cardinality"</S>{'\n'}
<T>window</T> = <S>"5m"</S>{'\n'}
<T>key</T>    = <S>"card_id"</S>{'\n'}
<T>field</T>  = <S>"merchant_id"</S>{'\n'}
<T>source</T> = <S>"events.tx"</S>
    </CodeBlock>

    <LP>In your transaction path:</LP>

    <CodeBlock lang="javascript">
<K>await</K> beava.<F>push</F>(<S>"tx"</S>, &#123; card_id, merchant_id, amount &#125;);{'\n'}
{'\n'}
<K>const</K> [txCount, merchantCount] = <K>await</K> beava.<F>getMany</F>([{'\n'}
{'  '}&#123; feature: <S>"card_tx_5m"</S>, key: &#123; card_id &#125; &#125;,{'\n'}
{'  '}&#123; feature: <S>"card_merchants_5m"</S>, key: &#123; card_id &#125; &#125;,{'\n'}
]);{'\n'}
{'\n'}
<K>if</K> (txCount &gt; <N>10</N> &amp;&amp; merchantCount &gt; <N>3</N>) flagForReview(cardId);
    </CodeBlock>

    <Callout kind="tip" title="Why this is fast">
      Both counters share the same event source, so we only push one event per transaction. beava fans it out internally. You can define ten more counters over <LInline>events.tx</LInline> without paying again on the write path.
    </Callout>

    <LH2>What broke, what didn't</LH2>
    <LP>
      Two things surprised me. First: cardinality queries under 1s windows are noticeably less accurate than beava's docs suggest — we saw ~2% undercounting at 500ms windows. (Fine for fraud; would not be fine for billing.) Second: the HTTP overhead is real if you call <LInline>push</LInline> synchronously from a hot path. Fire-and-forget via a local UDS queue removed the latency entirely.
    </LP>

    <Callout kind="warn" title="Don't block the hot path">
      If your transaction latency matters, batch pushes or use the local-UDS transport. The synchronous HTTP path is fine for moderate QPS, not for a payment processor's critical path.
    </Callout>

    <LH2>What I'd do differently</LH2>
    <LP>
      I'd start with a single-counter heuristic (<LInline>tx_count_5m &gt; 10</LInline>) in shadow mode, log false positives for a week, then layer on the merchant-cardinality check. Every time I've started with the complex version first, I've regretted it.
    </LP>

    <div style={{ marginTop: 48, paddingTop: 32, borderTop: '1px solid var(--border)', display: 'flex', gap: 14, alignItems: 'center' }}>
      <div style={{ width: 48, height: 48, borderRadius: 999, background: 'var(--beava-orange-wash)', color: 'var(--accent)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontFamily: 'var(--font-sans)', fontWeight: 700, fontSize: 18 }}>SR</div>
      <div style={{ flex: 1 }}>
        <div style={{ fontFamily: 'var(--font-sans)', fontSize: 15, fontWeight: 600, color: 'var(--fg1)' }}>Sam Rosen</div>
        <div style={{ fontFamily: 'var(--font-sans)', fontSize: 13, color: 'var(--fg3)' }}>Maintainer. Writes the field guide.</div>
      </div>
      <a style={{ fontFamily: 'var(--font-sans)', fontSize: 13, color: 'var(--accent)', textDecoration: 'none', padding: '8px 14px', border: '1px solid var(--border)', borderRadius: 8, background: '#fff' }}>Follow →</a>
    </div>
  </article>
);
window.LearnArticle = LearnArticle;
