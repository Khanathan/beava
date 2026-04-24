// js/FeedBeaver.jsx — the clickable beaver engine.
// - Stages 0..7 pre-overflow; stage 8 = overflow + eat-the-page.
// - Real HTTP calls with simulated fallback shown as "demo mode" banner.

const FB_STAGES = [
  { maxClicks: 0,   scale: 1.00, mood: '😴 sleeping',      hint: 'feed me (click the beaver)',     weight: 5,   weightLabel: 'baby beaver',   deco: ['tree'] },
  { maxClicks: 1,   scale: 1.30, mood: '👀 curious',       hint: 'more please!',                    weight: 9,   weightLabel: 'a yearling',    deco: ['tree','acorn'] },
  { maxClicks: 2,   scale: 1.55, mood: '🙂 hungry',        hint: 'oh, yes',                         weight: 15,  weightLabel: 'normal adult',  deco: ['tree','acorn','leaf'] },
  { maxClicks: 4,   scale: 1.90, mood: '🍽️ eating',       hint: "don't stop",                       weight: 27,  weightLabel: 'a corgi',       deco: ['tree','acorn','leaf','flower'] },
  { maxClicks: 6,   scale: 2.30, mood: '💪 getting big',   hint: 'feeling POWERFUL',                weight: 46,  weightLabel: 'a labrador',    deco: ['tree','acorn','leaf','flower','squirrel'] },
  { maxClicks: 9,   scale: 2.70, mood: '🦾 a unit',        hint: 'you magnificent bastard',         weight: 78,  weightLabel: 'a grand piano', deco: ['tree','acorn','leaf','flower','squirrel','bird'] },
  { maxClicks: 12,  scale: 3.10, mood: '🏋️ chonker',       hint: '👀 is this safe?',                weight: 122, weightLabel: 'a small sedan', deco: ['tree','acorn','leaf','flower','squirrel','bird','mushroom'] },
  { maxClicks: 14,  scale: 3.60, mood: '⚠️ OH DEAR',       hint: "don't click again i'm warning you", weight: 196, weightLabel: 'a Mini Cooper', deco: ['tree','acorn','leaf','flower','squirrel','bird','mushroom'], warning: true },
];
// Stage 8: overflow fullscreen. clicks 15..24 eat the page in 9 bites.
const OVERFLOW_CLICK = 15;
const BITE_POSITIONS = [
  { x: -10, y: -10, w: 55, h: 48 },
  { x:  55, y: -10, w: 55, h: 48 },
  { x: -10, y:  62, w: 55, h: 55 },
  { x:  55, y:  60, w: 55, h: 55 },
  { x:  25, y:  -8, w: 55, h: 42 },
  { x:  -8, y:  30, w: 48, h: 50 },
  { x:  62, y:  30, w: 48, h: 50 },
  { x:  20, y:  58, w: 60, h: 45 },
  { x:  10, y:  10, w: 82, h: 82 }, // MEGA
];
const EAT_MESSAGES = [
  { big: '*CHOMP* — the page is gone.',          small: 'Keep clicking. Watch him chew.' },
  { big: '*CHOMP* — goodbye, footer.',            small: 'Mmf. *crunch*' },
  { big: '*CHOMP* — the stats, digested.',        small: '142 events per minute, now 0.' },
  { big: '*CHOMP* — load-bearing curls eaten.',   small: 'Those were important.' },
  { big: '*CHOMP* — eating the DOM now.',         small: 'Tastes like angle brackets.' },
  { big: '*CHOMP* — gulping the HTML.',           small: 'A balanced diet of tags.' },
  { big: '*CHOMP* — licking up the CSS.',         small: 'Flavor: calc() and transitions.' },
  { big: '*CHOMP* — swallowing the JavaScript.',  small: 'Hope nobody needed those closures.' },
  { big: 'Only crumbs remain.',                   small: '🪵' },
];

// Helpers -------------------------------------------------------
const randomPolygon = (amplitude = 11) => {
  // 14 points per side, jagged offset into the rect so edges look bitten.
  const pts = [];
  const N = 14;
  for (let i = 0; i <= N; i++) {
    const t = i / N;
    pts.push([t * 100, Math.random() * amplitude]);
  }
  for (let i = 1; i <= N; i++) {
    const t = i / N;
    pts.push([100 - Math.random() * amplitude, t * 100]);
  }
  for (let i = 1; i <= N; i++) {
    const t = i / N;
    pts.push([(1 - t) * 100, 100 - Math.random() * amplitude]);
  }
  for (let i = 1; i < N; i++) {
    const t = i / N;
    pts.push([Math.random() * amplitude, (1 - t) * 100]);
  }
  return 'polygon(' + pts.map(p => `${p[0].toFixed(1)}% ${p[1].toFixed(1)}%`).join(',') + ')';
};

// Session id — per spec: no cookies, no storage, regenerated every load.
const SESSION_ID = 'sess-' + Math.random().toString(36).slice(2, 10) + Math.random().toString(36).slice(2, 8);

// Backend client — tries real, falls back to simulated.
const useBeavaClient = (endpoint) => {
  const [demoMode, setDemoMode] = React.useState(false);
  const [global, setGlobal] = React.useState({
    total: 142857, rate_1m: 312, rate_1s: 7,
    p99_clickspeed: 847, visitors_1h: 287,
  });
  const failsRef = React.useRef(0);
  const simGlobalRef = React.useRef({
    total: 142857, rate_1m: 312, rate_1s: 7, recent: [],
    p99_clickspeed: 847, visitors_1h: 287,
  });
  // Last click time for this session — used to derive click_speed_ms
  const lastClickRef = React.useRef(0);

  const simTick = () => {
    // Simulated: drift the global numbers a bit so it feels alive.
    const s = simGlobalRef.current;
    const bump = Math.floor(Math.random() * 4);
    s.total += bump;
    s.recent.push(...Array(bump).fill(Date.now()));
    const now = Date.now();
    s.recent = s.recent.filter(t => now - t < 60000);
    s.rate_1m = Math.max(40, Math.round(180 + Math.sin(now / 8000) * 90 + Math.random() * 30));
    s.rate_1s = Math.max(1, Math.round(4 + Math.sin(now / 1200) * 3 + Math.random() * 2));
    // p99 think-time drifts between ~600–1200ms — reflects reader click cadence
    s.p99_clickspeed = Math.round(900 + Math.sin(now / 12000) * 250 + Math.random() * 80);
    // visitors in the last hour drifts gently around ~287
    s.visitors_1h = Math.max(240, Math.round(287 + Math.sin(now / 20000) * 40 + Math.random() * 10));
    return {
      total: s.total, rate_1m: s.rate_1m, rate_1s: s.rate_1s,
      p99_clickspeed: s.p99_clickspeed, visitors_1h: s.visitors_1h,
    };
  };

  const push = React.useCallback(async () => {
    const now = Date.now();
    // click_speed_ms: ms since this reader's previous click; 0 on the first click
    const click_speed_ms = lastClickRef.current === 0 ? 0 : now - lastClickRef.current;
    lastClickRef.current = now;
    if (!endpoint || demoMode) {
      simGlobalRef.current.total += 1;
      simGlobalRef.current.recent.push(now);
      // In sim mode, let the reader's clicks pull p99 slightly toward their cadence
      if (click_speed_ms > 0) {
        const s = simGlobalRef.current;
        s.p99_clickspeed = Math.round(s.p99_clickspeed * 0.92 + click_speed_ms * 0.08);
      }
      return;
    }
    try {
      const r = await fetch(`${endpoint}/events/FeedClick`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ session_id: SESSION_ID, click_speed_ms }),
      });
      if (!r.ok) throw new Error('status ' + r.status);
      failsRef.current = 0;
    } catch (e) {
      failsRef.current += 1;
      if (failsRef.current > 2) setDemoMode(true);
      simGlobalRef.current.total += 1;
    }
  }, [endpoint, demoMode]);

  React.useEffect(() => {
    let cancel = false;
    const poll = async () => {
      if (cancel) return;
      if (!endpoint || demoMode) { setGlobal(simTick()); return; }
      try {
        const r = await fetch(`${endpoint}/features/Global/__global__`);
        if (!r.ok) throw new Error('status ' + r.status);
        const data = await r.json();
        setGlobal({
          total: data.total ?? 0,
          rate_1m: data.rate_1m ?? 0,
          rate_1s: data.rate_1s ?? 0,
          p99_clickspeed: data.p99_clickspeed ?? 847,
          visitors_1h: data.visitors_1h ?? 287,
        });
        failsRef.current = 0;
      } catch (e) {
        failsRef.current += 1;
        if (failsRef.current > 2) setDemoMode(true);
        setGlobal(simTick());
      }
    };
    poll();
    const id = setInterval(poll, 500);
    return () => { cancel = true; clearInterval(id); };
  }, [endpoint, demoMode]);

  return { push, global, demoMode };
};

// Decorations near the beaver -----------------------------------
const FBDecorations = ({ unlocked }) => {
  // Each decoration is a small float near the beaver.
  const deco = {
    tree:     { emoji: '🌳', x: -170, y: 40,   rot: 0,  delay: 0 },
    acorn:    { emoji: '🌰', x: 180,  y: 60,   rot: -6, delay: 0.1 },
    leaf:     { emoji: '🍃', x: -140, y: -90,  rot: 12, delay: 0.2, float: true },
    flower:   { emoji: '🌸', x: 160,  y: -100, rot: 8,  delay: 0.3 },
    squirrel: { emoji: '🐿️', x: -210, y: -10,  rot: -4, delay: 0.4 },
    bird:     { emoji: '🐦', x: 220,  y: -60,  rot: -8, delay: 0.5, hover: true },
    mushroom: { emoji: '🍄', x: -100, y: 110,  rot: 0,  delay: 0.6 },
  };
  return (
    <div style={{ position: 'absolute', inset: 0, pointerEvents: 'none' }}>
      {Object.entries(deco).map(([key, d]) => {
        if (!unlocked.includes(key)) return null;
        return (
          <div key={key}
               className={'fb-deco ' + (d.float ? 'fb-deco-float ' : '') + (d.hover ? 'fb-deco-hover ' : '')}
               style={{
                 position: 'absolute', left: '50%', top: '50%',
                 transform: `translate(-50%,-50%) translate(${d.x}px, ${d.y}px) rotate(${d.rot}deg)`,
                 fontSize: 42, animationDelay: `${d.delay}s`,
                 filter: 'drop-shadow(0 4px 6px rgba(26,23,20,0.12))',
               }}>
            {d.emoji}
          </div>
        );
      })}
    </div>
  );
};

// Stats grid ----------------------------------------------------
const FBStats = ({ myFeeds, streak, stage, global }) => {
  const logs = Math.floor(myFeeds / 5);
  const weight = stage < 8 ? FB_STAGES[Math.min(stage, FB_STAGES.length - 1)].weight : '∞';
  const weightLabel = stage < 8 ? FB_STAGES[Math.min(stage, FB_STAGES.length - 1)].weightLabel : 'undefined behavior';

  const StatCard = ({ label, value, sub, hint, live }) => (
    <div style={{
      background: '#fff', border: '1px solid var(--border)', borderRadius: 14,
      padding: '16px 18px', display: 'flex', flexDirection: 'column', gap: 4,
      boxShadow: 'var(--shadow-xs)',
    }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
        <div style={{ fontFamily: 'var(--font-sans)', fontSize: 12, fontWeight: 600, color: 'var(--fg3)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>{label}</div>
        {live && <span style={{ fontSize: 10, fontFamily: 'var(--font-sans)', fontWeight: 700, color: 'var(--beava-success)', display: 'inline-flex', alignItems: 'center', gap: 4 }}>
          <span style={{ width: 6, height: 6, borderRadius: 999, background: 'var(--beava-success)', boxShadow: '0 0 0 3px rgba(74,122,58,0.15)' }}/>LIVE
        </span>}
      </div>
      <div style={{ fontFamily: 'var(--font-serif)', fontWeight: 600, fontSize: 30, lineHeight: 1.05, color: 'var(--fg1)', letterSpacing: '-0.02em' }}>
        {value}
      </div>
      {sub && <div style={{ fontFamily: 'var(--font-sans)', fontSize: 12, color: 'var(--fg3)' }}>{sub}</div>}
      {hint && <div style={{ fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--accent)', marginTop: 2 }}>{hint}</div>}
    </div>
  );

  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 12 }}>
      <StatCard label="You've fed"      value={myFeeds.toLocaleString()} sub="events this session" hint="Session.my_feeds"/>
      <StatCard label="Weight"          value={`${weight} kg`}           sub={weightLabel}          hint="computed: scale³ × 5"/>
      <StatCard label="Your streak"     value={streak}                   sub="clicks <2s apart"     hint="Session.my_streak"/>
      <StatCard label="🪵 Logs chewed"  value={logs}                     sub="one log per 5 feeds"  hint="floor(my_feeds / 5)"/>
      <StatCard label="Global (1m)"     value={global.rate_1m.toLocaleString()} sub="everyone, last minute" hint="Global.rate_1m" live/>
      <StatCard label="Rate / sec"      value={global.rate_1s.toLocaleString()} sub="rolling"              hint="Global.rate_1s" live/>
    </div>
  );
};

// Compact stat chip for inline hero strip --------------------
const CompactStat = ({ label, value, mono, live }) => (
  <div style={{ display: 'flex', alignItems: 'baseline', gap: 6, padding: '4px 14px' }}>
    <span style={{
      fontFamily: mono ? 'var(--font-mono)' : 'var(--font-serif)',
      fontWeight: mono ? 600 : 700,
      fontSize: mono ? 15 : 18,
      color: 'var(--fg1)',
      letterSpacing: mono ? 0 : '-0.01em',
      lineHeight: 1,
    }}>{value}</span>
    <span style={{ fontFamily: 'var(--font-sans)', fontSize: 11, fontWeight: 600, color: 'var(--fg3)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>
      {label}
    </span>
    {live && <span style={{ width: 5, height: 5, borderRadius: 999, background: 'var(--beava-success)', boxShadow: '0 0 0 2px rgba(74,122,58,0.18)', alignSelf: 'center' }}/>}
  </div>
);
const CompactStatDivider = () => (
  <span style={{ width: 1, alignSelf: 'stretch', background: 'var(--border)', margin: '4px 0' }}/>
);

// Fully-eaten modal — portaled to document.body so no parent filter touches it.
const FBEatenModal = () => {
  if (typeof document === 'undefined') return null;
  return ReactDOM.createPortal(
    <div style={{
      position: 'fixed', inset: 0,
      background: '#0a0604',
      zIndex: 2147483000,
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      padding: 24, pointerEvents: 'auto',
      animation: 'fb-fade-to-black 0.9s ease-out both',
    }}>
      <div style={{
        background: '#fdfaf4',
        border: '2px solid #b85c20',
        borderRadius: 20,
        padding: '40px 44px 36px',
        maxWidth: 520,
        width: '100%',
        textAlign: 'center',
        boxShadow: '0 30px 80px rgba(10,6,4,0.8), 0 0 0 6px rgba(184,92,32,0.22)',
        position: 'relative',
      }}>
        <img src="assets/logo-mark.png" width={96} height={96} style={{ display: 'block', margin: '0 auto 14px' }}/>
        <div style={{ fontFamily: 'var(--font-accent)', fontWeight: 700, fontSize: 'clamp(2rem, 4.5vw, 3rem)', color: 'var(--accent)', margin: '0 0 12px', letterSpacing: '-0.02em', transform: 'rotate(-1.5deg)', lineHeight: 0.95 }}>
          🦫 The beavers are in charge now.
        </div>
        <div style={{ fontFamily: 'var(--font-serif)', fontStyle: 'italic', fontSize: 17, lineHeight: 1.55, color: 'var(--fg2)', margin: '0 auto 10px', maxWidth: 440 }}>
          He ate your DOM. Then the internet. Then Belgium. <br/>Congress has been dissolved and replaced with a dam.
        </div>
        <div style={{ fontFamily: 'var(--font-sans)', fontSize: 13, color: 'var(--fg3)', margin: '0 auto 14px', maxWidth: 380 }}>
          Interim Supreme Chancellor: a 9kg rodent with a compliance problem.
        </div>
        <a
          href="https://parks.canada.ca/pn-np/nt/woodbuffalo/nature/beaver_gallery"
          target="_blank"
          rel="noopener noreferrer"
          style={{
            display: 'inline-flex', alignItems: 'center', gap: 8,
            fontFamily: 'var(--font-sans)', fontSize: 14, fontWeight: 600,
            color: 'var(--accent)',
            background: 'var(--beava-orange-wash)',
            border: '1px solid #f1d8c2',
            padding: '8px 14px', borderRadius: 999,
            textDecoration: 'none',
            margin: '0 auto 22px',
          }}>
          <span style={{ fontSize: 16 }}>🦫</span>
          <span>Meet your new overlords</span>
          <span aria-hidden="true">→</span>
        </a>
        <button
          ref={el => el && el.focus()}
          onClick={() => location.reload()}
          className="fb-refresh-pulse"
          style={{
            position: 'relative',
            background: 'var(--accent)', color: '#fff', border: 0, fontWeight: 800, fontSize: 18,
            padding: '16px 32px', borderRadius: 12, cursor: 'pointer', fontFamily: 'var(--font-sans)',
            display: 'inline-flex', alignItems: 'center', gap: 12,
            letterSpacing: '-0.01em',
          }}>
          <span style={{ fontSize: 22, lineHeight: 1 }}>↻</span>
          <span>Refresh &amp; pretend this didn't happen</span>
        </button>
        <div style={{ marginTop: 16, fontFamily: 'var(--font-mono)', fontSize: 12, color: 'var(--fg3)' }}>
          or <kbd style={{ fontFamily: 'var(--font-mono)', fontSize: 11, background: 'var(--bg-inset)', border: '1px solid var(--border)', padding: '2px 6px', borderRadius: 5, color: 'var(--fg2)' }}>⌘R</kbd> / <kbd style={{ fontFamily: 'var(--font-mono)', fontSize: 11, background: 'var(--bg-inset)', border: '1px solid var(--border)', padding: '2px 6px', borderRadius: 5, color: 'var(--fg2)' }}>Ctrl+R</kbd> to restore democracy
        </div>
      </div>
    </div>,
    document.body
  );
};

// The main widget -----------------------------------------------
//   default  — full boxed widget (standalone demo page)
//   floating — fixed, right-edge, always visible; follows user down the page.
//              Shows decorations, no stats grid (stats live in GlobalTicker).
const FeedBeaver = ({ endpoint = null, compact = false, unboxed = false, showDeco = true, mode = 'default', compactStats = false }) => {
  const floating = mode === 'floating';
  const [clicks, setClicks] = React.useState(0);
  const [streak, setStreak] = React.useState(0);
  const [bites, setBites] = React.useState([]);         // array of {idx, polygon}
  const [fullyEaten, setFullyEaten] = React.useState(false);
  const [chomping, setChomping] = React.useState(false);
  const [rateLimit, setRateLimit] = React.useState(false);
  const [msgKey, setMsgKey] = React.useState(0);
  const lastClickRef = React.useRef(0);
  const clickWindowRef = React.useRef([]);

  const { push, global, demoMode } = useBeavaClient(endpoint);

  const stageIdx = React.useMemo(() => {
    for (let i = 0; i < FB_STAGES.length; i++) if (clicks <= FB_STAGES[i].maxClicks) return i;
    return 8; // overflow
  }, [clicks]);
  const stage = stageIdx < 8 ? FB_STAGES[stageIdx] : null;
  const isOverflow = stageIdx === 8;
  const eatIdx = isOverflow ? Math.min(clicks - OVERFLOW_CLICK, EAT_MESSAGES.length - 1) : -1;

  // When fullyEaten is triggered, add body class
  React.useEffect(() => {
    if (fullyEaten) document.body.classList.add('fully-eaten');
    else document.body.classList.remove('fully-eaten');
  }, [fullyEaten]);

  // Overflow/eating mode — hide the nav and any sticky chrome so the beaver eats EVERYTHING
  React.useEffect(() => {
    if (isOverflow) document.body.classList.add('fb-overflowing');
    else document.body.classList.remove('fb-overflowing');
  }, [isOverflow]);

  const feed = React.useCallback(() => {
    // 20 clicks/sec rate limit
    const now = Date.now();
    clickWindowRef.current = clickWindowRef.current.filter(t => now - t < 1000);
    if (clickWindowRef.current.length >= 20) {
      setRateLimit(true);
      setTimeout(() => setRateLimit(false), 1400);
      return;
    }
    clickWindowRef.current.push(now);

    // streak: consecutive clicks <2s apart
    if (now - lastClickRef.current < 2000) setStreak(s => s + 1);
    else setStreak(1);
    lastClickRef.current = now;

    // chomp animation
    setChomping(true);
    setTimeout(() => setChomping(false), 260);

    // send real event
    push();

    setClicks(c => {
      const next = c + 1;
      // Overflow bite: each click 15..23 adds a bite, click 24 = final
      if (next >= OVERFLOW_CLICK && next < OVERFLOW_CLICK + BITE_POSITIONS.length) {
        const biteIndex = next - OVERFLOW_CLICK;
        setTimeout(() => {
          setBites(prev => [...prev, { ...BITE_POSITIONS[biteIndex], polygon: randomPolygon(7 + Math.random() * 8), idx: biteIndex }]);
          setMsgKey(k => k + 1);
        }, 200);
      }
      if (next === OVERFLOW_CLICK + BITE_POSITIONS.length) {
        // last bite — after 850ms, fully eaten
        setTimeout(() => setFullyEaten(true), 850);
      }
      return next;
    });
  }, [push]);

  // Auto-feed nudge — if a new visitor doesn't feed the beaver, show them.
  // Up to 3 auto-feeds total, then they're on their own.
  const autoFedRef = React.useRef(0);
  React.useEffect(() => {
    if (clicks >= 3 || autoFedRef.current >= 3 || fullyEaten) return;
    const delay = clicks === 0 ? 4500 : 3000;
    const t = setTimeout(() => {
      autoFedRef.current += 1;
      feed();
    }, delay);
    return () => clearTimeout(t);
  }, [clicks, feed, fullyEaten]);

  // Render beaver sizing
  const scale = isOverflow ? (window.innerWidth < 640 ? 4 : 5) : (stage?.scale ?? 1);
  const beaverSize = floating ? 200 : (compact ? 260 : 320);

  // Floating mode: a position:fixed cluster on the right edge. Decorations always on.
  if (floating) {
    return (
      <div className="fb-float-root" style={{
        position: 'fixed',
        right: 0,
        bottom: 0,
        width: 'min(42vw, 440px)',
        height: 'min(64vh, 560px)',
        pointerEvents: 'none',
        zIndex: 40,
      }}>
        {/* Inner anchor — everything centers on this point */}
        <div style={{ position: 'absolute', right: '50%', bottom: 40, transform: 'translateX(50%)', pointerEvents: 'none' }}>
          {/* Rate limit message */}
          {rateLimit && (
            <div style={{
              position: 'absolute', top: -40, left: '50%', transform: 'translateX(-50%)', zIndex: 60,
              background: '#fff', border: '1px solid var(--border)', borderRadius: 999,
              padding: '6px 14px', fontFamily: 'var(--font-sans)', fontSize: 13, fontWeight: 600, color: 'var(--fg2)',
              boxShadow: 'var(--shadow-sm)', whiteSpace: 'nowrap',
            }}>
              🐿️ Chill. The beaver needs a moment.
            </div>
          )}

          {/* Warning banner at stage 7 */}
          {stageIdx === 7 && !isOverflow && (
            <div className="fb-warn" style={{
              position: 'absolute', top: -70, left: '50%', transform: 'translateX(-50%)',
              background: 'var(--beava-danger)', color: '#fff',
              padding: '10px 22px', borderRadius: 999, zIndex: 55,
              fontFamily: 'var(--font-sans)', fontWeight: 700, fontSize: 13,
              letterSpacing: '0.06em', boxShadow: 'var(--shadow-md)',
              whiteSpace: 'nowrap', pointerEvents: 'auto',
            }}>
              ⚠️ BRACE YOURSELF ⚠️
            </div>
          )}

          {/* Decorations — always visible in floating mode, unlocked per stage */}
          {!isOverflow && stage && (
            <div style={{ position: 'absolute', left: '50%', top: '50%', width: 0, height: 0, pointerEvents: 'none' }}>
              <FBDecorations unlocked={stage.deco}/>
            </div>
          )}

          {/* Mood chip — follows him */}
          {!isOverflow && stage && (
            <div style={{
              position: 'absolute', right: -beaverSize * 0.55, top: -beaverSize * 0.4, transform: `translate(0, 0) scale(${Math.min(1.25, 0.85 + scale * 0.12)})`, transformOrigin: 'center',
              background: '#fff', border: '1px solid var(--border)',
              padding: '6px 12px', borderRadius: 999,
              fontFamily: 'var(--font-sans)', fontSize: 12, fontWeight: 600, color: 'var(--fg2)',
              boxShadow: 'var(--shadow-sm)',
              zIndex: 42, whiteSpace: 'nowrap',
              transition: 'transform 420ms cubic-bezier(0.34,1.56,0.64,1)',
            }}>
              {stage.mood}
            </div>
          )}

          {/* Beaver — freestanding, overflowing */}
          <button
            onClick={feed}
            aria-label="Feed the beaver"
            className={'fb-beaver ' + (chomping ? 'fb-chomp ' : '') + (stageIdx === 7 ? 'fb-vibrate ' : '') + (isOverflow ? 'fb-overflow ' : '')}
            style={{
              position: isOverflow ? 'fixed' : 'relative',
              inset: isOverflow ? 0 : undefined,
              margin: isOverflow ? 'auto' : undefined,
              border: 0, background: 'transparent', cursor: 'pointer',
              zIndex: isOverflow ? 600 : 50,
              padding: 0,
              pointerEvents: 'auto',
              width: isOverflow ? '100%' : beaverSize,
              height: isOverflow ? '100%' : beaverSize,
              transform: isOverflow ? `scale(1)` : `scale(${scale}) translateX(-50%)`,
              transformOrigin: 'center bottom',
              transition: 'transform 420ms cubic-bezier(0.34,1.56,0.64,1)',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              filter: `drop-shadow(0 ${10 + scale * 6}px ${14 + scale * 10}px rgba(26,23,20,${0.14 + scale * 0.02}))`,
            }}>
            <img src="assets/logo-mark.png" width={beaverSize} height={beaverSize}
                 style={{ display: 'block', width: isOverflow ? `min(90vw, 90vh)` : beaverSize, height: isOverflow ? `min(90vw, 90vh)` : beaverSize, pointerEvents: 'none' }}
                 draggable="false"/>
          </button>

          {/* Corner hint — pulsing, pointing up at beaver */}
          {!isOverflow && stage && (
            <div className="fb-hint" style={{
              position: 'absolute', left: '50%', bottom: -50, transform: 'translateX(-50%)',
              background: '#fff', border: '1px solid var(--border)', borderRadius: 999,
              padding: '8px 14px', boxShadow: 'var(--shadow-sm)',
              fontFamily: 'var(--font-sans)', fontSize: 13, color: 'var(--fg2)',
              display: 'inline-flex', alignItems: 'center', gap: 8, maxWidth: '80vw',
              whiteSpace: 'nowrap', pointerEvents: 'auto',
              zIndex: 40,
            }}>
              <span className="fb-arrow" style={{ fontFamily: 'var(--font-accent)', fontWeight: 700, color: 'var(--accent)', fontSize: 16, lineHeight: 1, display: 'inline-block' }}>↑</span>
              <span>{stage.hint}</span>
            </div>
          )}
        </div>

        {/* Bite-mark overlays (overflow only) */}
        {isOverflow && bites.map(b => (
          <div key={b.idx} className="fb-bite" style={{
            position: 'fixed',
            left: `${b.x}vw`, top: `${b.y}vh`,
            width: `${b.w}vw`, height: `${b.h}vh`,
            background: 'radial-gradient(ellipse at center, #0b0604 0%, #000 85%)',
            clipPath: b.polygon, WebkitClipPath: b.polygon,
            filter: 'drop-shadow(0 0 8px rgba(0,0,0,0.55))',
            zIndex: 650,
            pointerEvents: 'none',
          }}/>
        ))}

        {/* Eat-phase message */}
        {isOverflow && eatIdx >= 0 && (
          <div key={msgKey} className="fb-eat-msg" style={{
            position: 'fixed', left: '50%', bottom: '8vh', transform: 'translateX(-50%)',
            zIndex: 700, textAlign: 'center', maxWidth: '90vw', pointerEvents: 'none',
          }}>
            <div style={{
              fontFamily: 'var(--font-serif)', fontWeight: 800,
              fontSize: 'clamp(1.1rem, 3vw, 1.5rem)', color: '#fff',
              textShadow: '0 0 8px rgba(0,0,0,0.9), 0 0 20px rgba(0,0,0,0.85), 0 0 40px rgba(0,0,0,0.7)',
              margin: 0,
            }}>{EAT_MESSAGES[eatIdx].big}</div>
            <div style={{
              fontFamily: 'var(--font-sans)', fontSize: 14, fontWeight: 500, color: 'rgba(255,255,255,0.88)',
              textShadow: '0 0 8px rgba(0,0,0,0.9), 0 0 20px rgba(0,0,0,0.8)',
              marginTop: 6,
            }}>· {EAT_MESSAGES[eatIdx].small}</div>
          </div>
        )}

        {/* Fully-eaten modal — portaled to document.body, escapes all filters */}
        {fullyEaten && <FBEatenModal/>}
      </div>
    );
  }

  // Default/boxed mode below.

  return (
    <div className="fb-root" style={{ position: 'relative' }}>
      {/* Rate limit message */}
      {rateLimit && (
        <div style={{
          position: 'absolute', top: 12, right: 12, zIndex: 60,
          background: '#fff', border: '1px solid var(--border)', borderRadius: 999,
          padding: '6px 14px', fontFamily: 'var(--font-sans)', fontSize: 13, fontWeight: 600, color: 'var(--fg2)',
          boxShadow: 'var(--shadow-sm)',
        }}>
          🐿️ Chill. The beaver needs a moment.
        </div>
      )}

      {/* Warning banner at stage 7 */}
      {stageIdx === 7 && !isOverflow && (
        <div className="fb-warn" style={{
          position: 'absolute', top: 18, left: '50%', transform: 'translateX(-50%)',
          background: 'var(--beava-danger)', color: '#fff',
          padding: '10px 22px', borderRadius: 999, zIndex: 55,
          fontFamily: 'var(--font-sans)', fontWeight: 700, fontSize: 14,
          letterSpacing: '0.06em', boxShadow: 'var(--shadow-md)',
        }}>
          ⚠️ BRACE YOURSELF ⚠️
        </div>
      )}

      {/* Beaver + decorations stage */}
      <div className="fb-stage" style={{
        position: 'relative',
        height: compact ? (compactStats ? 340 : 360) : 480,
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        overflow: isOverflow ? 'visible' : (unboxed ? 'visible' : 'hidden'),
      }}>
        {/* Decorations — enabled in hero mode (showDeco) and standalone (non-unboxed) */}
        {!isOverflow && stage && (showDeco || !unboxed) && <FBDecorations unlocked={stage.deco}/>}

        {/* Beaver */}
        <button
          onClick={feed}
          aria-label="Feed the beaver"
          className={'fb-beaver ' + (chomping ? 'fb-chomp ' : '') + (stageIdx === 7 ? 'fb-vibrate ' : '') + (isOverflow ? 'fb-overflow ' : '')}
          style={{
            position: isOverflow ? 'fixed' : 'relative',
            inset: isOverflow ? 0 : undefined,
            margin: isOverflow ? 'auto' : undefined,
            border: 0, background: 'transparent', cursor: 'pointer',
            zIndex: isOverflow ? 600 : 10,
            padding: 0,
            width: isOverflow ? '100%' : beaverSize,
            height: isOverflow ? '100%' : beaverSize,
            transform: isOverflow ? `scale(1)` : `scale(${scale})`,
            transformOrigin: 'center',
            transition: 'transform 420ms cubic-bezier(0.34,1.56,0.64,1)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            filter: `drop-shadow(0 ${10 + scale * 6}px ${14 + scale * 10}px rgba(26,23,20,${0.14 + scale * 0.02}))`,
          }}>
          <img src="assets/logo-mark.png" width={beaverSize} height={beaverSize}
               style={{ display: 'block', width: isOverflow ? `min(90vw, 90vh)` : beaverSize, height: isOverflow ? `min(90vw, 90vh)` : beaverSize, pointerEvents: 'none' }}
               draggable="false"/>
        </button>

        {/* Corner hint pill (hidden in unboxed hero mode — the hero copy has its own nudge) */}
        {!isOverflow && stage && !unboxed && (
          <div className="fb-hint" style={{
            position: 'absolute', right: 16, bottom: 16,
            background: '#fff', border: '1px solid var(--border)', borderRadius: 999,
            padding: '10px 16px', boxShadow: 'var(--shadow-sm)',
            fontFamily: 'var(--font-sans)', fontSize: 14, color: 'var(--fg2)',
            display: 'inline-flex', alignItems: 'center', gap: 8, maxWidth: 'min(320px, 80vw)',
            whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
            zIndex: 40,
          }}>
            <span className="fb-arrow" style={{ fontFamily: 'var(--font-accent)', fontWeight: 700, color: 'var(--accent)', fontSize: 18, lineHeight: 1, display: 'inline-block' }}>↖</span>
            <span>{stage.hint}</span>
          </div>
        )}

        {/* Mood chip (hidden in unboxed hero mode) */}
        {!isOverflow && stage && !unboxed && (
          <div style={{
            position: 'absolute', left: 16, top: 16,
            background: 'var(--bg-inset)', border: '1px solid var(--border)',
            padding: '6px 14px', borderRadius: 999,
            fontFamily: 'var(--font-sans)', fontSize: 13, fontWeight: 600, color: 'var(--fg2)',
            zIndex: 40,
          }}>
            {stage.mood}
          </div>
        )}

        {/* Click counter inline chip (hidden in unboxed hero mode) */}
        {!isOverflow && !unboxed && (
          <div style={{
            position: 'absolute', right: 16, top: 16,
            background: 'var(--beava-orange-wash)', border: '1px solid #f1d8c2',
            padding: '6px 12px', borderRadius: 999,
            fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--accent)', fontWeight: 600,
            zIndex: 40,
          }}>
            POST /events/FeedClick · {clicks}
          </div>
        )}
      </div>

      {/* Bite-mark overlays (overflow only) */}
      {isOverflow && bites.map(b => (
        <div key={b.idx} className="fb-bite" style={{
          position: 'fixed',
          left: `${b.x}vw`, top: `${b.y}vh`,
          width: `${b.w}vw`, height: `${b.h}vh`,
          background: 'radial-gradient(ellipse at center, #0b0604 0%, #000 85%)',
          clipPath: b.polygon, WebkitClipPath: b.polygon,
          filter: 'drop-shadow(0 0 8px rgba(0,0,0,0.55))',
          zIndex: 650,
          pointerEvents: 'none',
        }}/>
      ))}

      {/* Eat-phase message */}
      {isOverflow && eatIdx >= 0 && (
        <div key={msgKey} className="fb-eat-msg" style={{
          position: 'fixed', left: '50%', bottom: '8vh', transform: 'translateX(-50%)',
          zIndex: 700, textAlign: 'center', maxWidth: '90vw', pointerEvents: 'none',
        }}>
          <div style={{
            fontFamily: 'var(--font-serif)', fontWeight: 800,
            fontSize: 'clamp(1.1rem, 3vw, 1.5rem)', color: '#fff',
            textShadow: '0 0 8px rgba(0,0,0,0.9), 0 0 20px rgba(0,0,0,0.85), 0 0 40px rgba(0,0,0,0.7)',
            margin: 0,
          }}>{EAT_MESSAGES[eatIdx].big}</div>
          <div style={{
            fontFamily: 'var(--font-sans)', fontSize: 14, fontWeight: 500, color: 'rgba(255,255,255,0.88)',
            textShadow: '0 0 8px rgba(0,0,0,0.9), 0 0 20px rgba(0,0,0,0.8)',
            marginTop: 6,
          }}>· {EAT_MESSAGES[eatIdx].small}</div>
        </div>
      )}

      {/* Fully-eaten overlay — modal card over semi-transparent veil so bite-marked page shows through */}
      {/* Fully-eaten modal — portaled to document.body, escapes all filters */}
      {fullyEaten && <FBEatenModal/>}

      {/* Stats — hidden during overflow */}
      {!isOverflow && !compact && (
        <div style={{ marginTop: 36 }}>
          <FBStats myFeeds={clicks} streak={streak} stage={stageIdx} global={global}/>
        </div>
      )}

      {/* Compact inline stats strip — for hero mode */}
      {!isOverflow && compact && compactStats && (
        <div style={{
          marginTop: -8,
          display: 'flex',
          justifyContent: 'center',
          flexWrap: 'wrap',
          gap: 0,
          fontFamily: 'var(--font-sans)',
          position: 'relative',
          zIndex: 5,
        }}>
          <CompactStat label="fed"            value={clicks.toLocaleString()}                          mono/>
          <CompactStatDivider/>
          <CompactStat label="streak"         value={streak}                                           mono/>
          <CompactStatDivider/>
          <CompactStat label="p99 clickspeed (1h)" value={`${global.p99_clickspeed.toLocaleString()}ms`}    mono live/>
        </div>
      )}
    </div>
  );
};

Object.assign(window, { FeedBeaver });
