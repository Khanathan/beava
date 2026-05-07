// Page-view tracker — fires PageView events to the beava-website-beava
// instance via the /api/push proxy (Caddy injects the admin bearer).
// On load: timer starts. On hide/unload: send {path, dwell_ms} via
// sendBeacon (survives navigation; falls back to fetch + keepalive).
(function () {
  var startedAt = performance.now();
  var path = location.pathname || '/';
  var sent = false;

  function send() {
    if (sent) return;
    sent = true;
    var dwellMs = Math.round(performance.now() - startedAt);

    // Per-path payload (for PageView, keyed by path).
    var perPath = JSON.stringify({ path: path, dwell_ms: dwellMs });
    // Site-wide payload (for SiteMetrics, keyed by bucket="site" → one global row).
    var siteWide = JSON.stringify({ bucket: 'site', path: path, dwell_ms: dwellMs });

    function fire(url, body) {
      try {
        if (navigator.sendBeacon) {
          navigator.sendBeacon(url, new Blob([body], { type: 'application/json' }));
        } else {
          fetch(url, {
            method: 'POST',
            body: body,
            headers: { 'Content-Type': 'application/json' },
            keepalive: true,
          });
        }
      } catch (_) { /* drop on the floor; tracker must never block UX */ }
    }

    fire('/api/push/PageView', perPath);
    fire('/api/push/SiteMetrics', siteWide);
  }

  // pagehide is more reliable than unload on mobile/Safari.
  window.addEventListener('pagehide', send);
  window.addEventListener('beforeunload', send);
  // Also send when the tab goes hidden — covers users that switch tabs and never come back.
  document.addEventListener('visibilitychange', function () {
    if (document.visibilityState === 'hidden') send();
  });
})();
