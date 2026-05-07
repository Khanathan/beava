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
    var body = JSON.stringify({ path: path, dwell_ms: Math.round(performance.now() - startedAt) });
    try {
      if (navigator.sendBeacon) {
        navigator.sendBeacon('/api/push/PageView', new Blob([body], { type: 'application/json' }));
      } else {
        fetch('/api/push/PageView', {
          method: 'POST',
          body: body,
          headers: { 'Content-Type': 'application/json' },
          keepalive: true,
        });
      }
    } catch (_) { /* drop on the floor; tracker must never block UX */ }
  }

  // pagehide is more reliable than unload on mobile/Safari.
  window.addEventListener('pagehide', send);
  window.addEventListener('beforeunload', send);
  // Also send when the tab goes hidden — covers users that switch tabs and never come back.
  document.addEventListener('visibilitychange', function () {
    if (document.visibilityState === 'hidden') send();
  });
})();
