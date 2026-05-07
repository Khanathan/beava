// ui_kits/marketing/Recipes.jsx
// Section 4 — five use-case cards. Each is a recipe link that lands in the
// guidebook. Locked decision — set of five: Personalization, Fraud,
// Leaderboard, Rate-limiting, Usage metering.

const RECIPES = [
  {
    cat: 'personalization',
    lines: '22 lines',
    title: '"Seen but didn\u2019t click" recency score',
    desc: 'Demote items a user has impressed and skipped, without batching features overnight.',
  },
  {
    cat: 'fraud',
    lines: '18 lines',
    title: 'Card-testing detection',
    desc: 'Two counters and a threshold \u2014 flag the attacker before the 11th decline.',
  },
  {
    cat: 'leaderboard',
    lines: '14 lines',
    title: 'Always-fresh top-N',
    desc: 'A leaderboard that updates incrementally and never stalls behind a batch job.',
  },
  {
    cat: 'rate limit',
    lines: '9 lines',
    title: 'Per-key sliding window',
    desc: 'The cheapest, most correct rate limit you can ship \u2014 one counter, one window.',
  },
  {
    cat: 'usage metering',
    lines: '16 lines',
    title: 'Per-customer monthly counters',
    desc: 'Bill on what they actually used, with counters that survive restarts and reset on schedule.',
  },
];

const Recipes = () => (
  <section style={{ padding: '88px 24px' }}>
    <div style={{ maxWidth: 1200, margin: '0 auto' }}>
      <div style={{ marginBottom: 40, display: 'flex', alignItems: 'flex-end', justifyContent: 'space-between', gap: 24, flexWrap: 'wrap' }}>
        <div>
          <Eyebrow>Recipes</Eyebrow>
          <h2 style={{
            fontFamily: 'var(--font-serif)', fontWeight: 600,
            fontSize: 40, lineHeight: 1.1, letterSpacing: '-0.02em',
            color: 'var(--fg1)', margin: '10px 0 0', textWrap: 'balance',
          }}>
            Pick the one that matches your day.
          </h2>
        </div>
        <a href="#" style={{
          fontFamily: 'var(--font-sans)', fontSize: 14, fontWeight: 500,
          color: 'var(--accent)', textDecoration: 'none',
          borderBottom: '1px solid color-mix(in oklab, var(--accent) 35%, transparent)',
          paddingBottom: 1,
        }}>
          See the full guidebook &rarr;
        </a>
      </div>

      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(3, 1fr)',
        gap: 16,
      }} className="beava-recipes-grid">
        {RECIPES.map(r => (
          <a key={r.title} href="#" style={{
            background: '#fff',
            border: '1px solid var(--border)',
            borderRadius: 14,
            padding: '22px 22px 20px',
            boxShadow: '0 1px 2px rgba(26,23,20,0.04), 0 2px 6px rgba(26,23,20,0.06)',
            display: 'flex', flexDirection: 'column', gap: 12,
            textDecoration: 'none',
            transition: 'all 200ms cubic-bezier(0.22,1,0.36,1)',
          }}
          onMouseEnter={e => {
            e.currentTarget.style.transform = 'translateY(-2px)';
            e.currentTarget.style.boxShadow = '0 2px 4px rgba(26,23,20,0.04), 0 8px 20px rgba(26,23,20,0.08)';
            e.currentTarget.style.borderColor = 'var(--border-strong)';
          }}
          onMouseLeave={e => {
            e.currentTarget.style.transform = '';
            e.currentTarget.style.boxShadow = '0 1px 2px rgba(26,23,20,0.04), 0 2px 6px rgba(26,23,20,0.06)';
            e.currentTarget.style.borderColor = 'var(--border)';
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8 }}>
              <span style={{
                fontFamily: 'var(--font-sans)', fontSize: 11, fontWeight: 600,
                textTransform: 'uppercase', letterSpacing: '0.08em',
                color: 'var(--accent)',
              }}>{r.cat}</span>
              <span style={{
                fontFamily: 'var(--font-mono)', fontSize: 11.5,
                color: 'var(--fg3)', whiteSpace: 'nowrap',
              }}>{r.lines}</span>
            </div>
            <h3 style={{
              fontFamily: 'var(--font-serif)', fontWeight: 600,
              fontSize: 21, lineHeight: 1.2, letterSpacing: '-0.01em',
              color: 'var(--fg1)', margin: 0, textWrap: 'balance',
            }}>{r.title}</h3>
            <p style={{
              fontFamily: 'var(--font-sans)', fontSize: 14.5, lineHeight: 1.55,
              color: 'var(--fg2)', margin: 0, textWrap: 'pretty', flex: 1,
            }}>{r.desc}</p>
            <div style={{
              fontFamily: 'var(--font-sans)', fontSize: 13, fontWeight: 500,
              color: 'var(--accent)', marginTop: 4,
            }}>Read the recipe &rarr;</div>
          </a>
        ))}
      </div>
    </div>

    <style>{`
      @media (max-width: 960px) {
        .beava-recipes-grid {
          grid-template-columns: repeat(2, 1fr) !important;
        }
      }
      @media (max-width: 560px) {
        .beava-recipes-grid {
          grid-template-columns: 1fr !important;
        }
      }
    `}</style>
  </section>
);
window.Recipes = Recipes;
