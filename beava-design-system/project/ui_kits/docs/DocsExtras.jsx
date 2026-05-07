// ui_kits/docs/DocsExtras.jsx
// Static (non-interactive) extras for the docs page.
// - DocsPageMeta: row of small chips under the H1 (stable / updated / read time / edit link)
// - DocsRelatedChips: "see also" chip row
// - DocsHelpCallout: mascot-led "stuck?" card
//
// All copy follows beava voice: warm, concrete, sentence case, no emoji.

// ---------- Page meta strip ----------------------------------------------
const DocsPageMeta = ({ stable = 'v0.9', updated = '3 days ago', readMins = 6 }) => {
  const chip = {
    display: 'inline-flex', alignItems: 'center', gap: 6,
    padding: '3px 10px', borderRadius: 999,
    background: 'var(--beava-paper)', border: '1px solid var(--border)',
    fontFamily: 'var(--font-sans)', fontSize: 12, color: 'var(--fg2)',
    lineHeight: 1.4,
  };
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 8,
      margin: '0 0 28px',
      flexWrap: 'wrap',
    }}>
      <span style={{ ...chip, color: 'var(--beava-success)', borderColor: '#d4ddc1', background: 'var(--beava-success-wash)' }}>
        <span style={{ width: 6, height: 6, borderRadius: 999, background: 'var(--beava-success)' }}/>
        stable since <span style={{ fontFamily: 'var(--font-mono)' }}>{stable}</span>
      </span>
      <span style={chip}>
        <DIcon name="hash" size={11} stroke={2}/> updated {updated}
      </span>
      <span style={chip}>
        <DIcon name="book" size={11} stroke={2}/> {readMins} min read
      </span>
      <span style={{ flex: 1 }}/>
      <a href="#" style={{
        ...chip, textDecoration: 'none',
        color: 'var(--accent)', borderColor: 'var(--border)',
      }}>
        <DIcon name="link" size={11} stroke={2}/>
        edit on github
      </a>
    </div>
  );
};

// ---------- Related features chips ---------------------------------------
const DocsRelatedChips = ({ items = [] }) => (
  <div style={{
    display: 'flex', alignItems: 'center', gap: 8,
    margin: '0 0 36px', flexWrap: 'wrap',
  }}>
    <span style={{
      fontFamily: 'var(--font-sans)', fontSize: 12, fontWeight: 600,
      color: 'var(--fg3)', textTransform: 'uppercase', letterSpacing: '0.08em',
      marginRight: 4,
    }}>see also</span>
    {items.map(it => (
      <a key={it.label} href="#" style={{
        display: 'inline-flex', alignItems: 'center', gap: 6,
        padding: '5px 11px', borderRadius: 999,
        background: '#fff', border: '1px solid var(--border)',
        color: 'var(--fg2)', textDecoration: 'none',
        fontFamily: 'var(--font-sans)', fontSize: 13, fontWeight: 500,
      }}>
        <span style={{ color: 'var(--fg3)' }}>›</span>
        {it.label}
      </a>
    ))}
  </div>
);

// ---------- Mascot-led help callout --------------------------------------
const DocsHelpCallout = () => (
  <div style={{
    margin: '40px 0 8px',
    padding: '18px 22px',
    background: 'var(--beava-paper)',
    border: '1px solid var(--border)',
    borderRadius: 14,
    display: 'flex', gap: 18, alignItems: 'center',
    position: 'relative', overflow: 'hidden',
  }}>
    <img
      src="../../assets/mascot-work-pose.svg"
      alt=""
      width={72}
      height={72}
      style={{ flexShrink: 0, alignSelf: 'flex-end', marginBottom: -6 }}
    />
    <div style={{ flex: 1, minWidth: 0 }}>
      <div style={{
        fontFamily: 'var(--font-accent)', fontSize: 18, fontWeight: 700,
        color: 'var(--accent)', transform: 'rotate(-2deg)',
        transformOrigin: 'left center', display: 'inline-block',
        marginBottom: 4,
      }}>Stuck on this one?</div>
      <div style={{
        fontFamily: 'var(--font-sans)', fontSize: 14, color: 'var(--fg2)',
        lineHeight: 1.55, marginBottom: 10, textWrap: 'pretty',
      }}>
        Half the answers in our docs started as someone asking &ldquo;is this a bug?&rdquo; out loud. Drop into Discussions or Discord — a maintainer is usually around.
      </div>
      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
        <a href="#" style={{
          display: 'inline-flex', alignItems: 'center', gap: 6,
          padding: '7px 14px', borderRadius: 8,
          background: 'var(--accent)', color: '#fff',
          border: '1px solid var(--accent)',
          fontFamily: 'var(--font-sans)', fontSize: 13, fontWeight: 600,
          textDecoration: 'none',
        }}>Ask on GitHub →</a>
        <a href="#" style={{
          display: 'inline-flex', alignItems: 'center', gap: 6,
          padding: '7px 14px', borderRadius: 8,
          background: '#fff', color: 'var(--fg1)',
          border: '1px solid var(--border)',
          fontFamily: 'var(--font-sans)', fontSize: 13, fontWeight: 500,
          textDecoration: 'none',
        }}>Discord</a>
      </div>
    </div>
  </div>
);

Object.assign(window, { DocsPageMeta, DocsRelatedChips, DocsHelpCallout });
