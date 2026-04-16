const $ = id => document.getElementById(id);
const fmt = n => Number(n).toLocaleString();

// Phase 26-03: parse Prometheus text-format lines. Returns the first numeric
// value for the given metric name (optionally summed across label series).
// Tolerant of HELP/TYPE comments and of label sets Phase 25 may have added.
function sumMetric(text, name) {
  if (!text) return null;
  let total = 0;
  let saw = false;
  for (const line of text.split('\n')) {
    const t = line.trim();
    if (!t || t.startsWith('#')) continue;
    // Match `name <value>` or `name{labels} <value>` exactly.
    if (t === name || t.startsWith(name + ' ') || t.startsWith(name + '{')) {
      const v = parseFloat(t.split(/\s+/).pop());
      if (!Number.isNaN(v)) { total += v; saw = true; }
    }
  }
  return saw ? total : null;
}

async function poll() {
  try {
    const s = await fetch('/public/stats').then(r => r.json());
    $('events-total').textContent = fmt(s.events_total);
    $('current-eps').textContent = fmt(Math.round(s.current_eps));
    $('p99').textContent = fmt(Math.round(s.p99_push_us));
    $('uptime').textContent = s.uptime_seconds + 's';
    $('keys').textContent = fmt(s.keys_total);
    const e = await fetch('/public/recent-events?limit=20').then(r => r.json());
    $('events-list').innerHTML = (e.events || []).map(x => {
      const ts = new Date(x.ts).toISOString().slice(11, 19);
      return `<li><code>${ts}</code><b>${x.stream}</b><span>${x.key || '—'}</span></li>`;
    }).join('') || '<li><code>no events yet</code></li>';

    // Phase 24/25 observability: late-drop counter. Surfaced on the demo
    // so visitors can see watermark drops in real time if the stream drifts.
    // Missing / zero is fine — we hide the tile in that case.
    const drops = $('late-drops');
    if (drops) {
      try {
        const mtxt = await fetch('/metrics').then(r => r.text());
        const total = sumMetric(mtxt, 'beava_late_events_dropped_total');
        drops.textContent = total == null ? '–' : fmt(total);
      } catch (_) { /* best-effort */ }
    }
  } catch (err) { /* keep previous values on transient fetch failures */ }
}
$('lookup-form').addEventListener('submit', async ev => {
  ev.preventDefault();
  const key = $('key-input').value.trim();
  if (!key) return;
  const r = await fetch('/public/features/' + encodeURIComponent(key));
  const j = await r.json();
  $('key-result').textContent = JSON.stringify(j, null, 2);
});
poll();
setInterval(poll, 2000);
