const $ = id => document.getElementById(id);
const fmt = n => Number(n).toLocaleString();
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
