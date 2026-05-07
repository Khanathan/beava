// docs-sidebar-state.js — persist sidebar open/closed state + scroll position
// across navigation. Each <details class="side-section"> in the sidebar gets
// its expand state saved to localStorage on toggle, then restored on the
// next page load. Sections containing the active page stay open regardless
// of saved state (so the user always sees where they are).
(function () {
  var KEY_OPEN = 'beava-docs-sidebar:open';
  var KEY_SCROLL = 'beava-docs-sidebar:scrollTop';

  var sidebar = document.querySelector('.docs-sidebar');
  if (!sidebar) return;

  function loadOpen() {
    try {
      var raw = localStorage.getItem(KEY_OPEN);
      return raw ? JSON.parse(raw) : {};
    } catch (_) { return {}; }
  }

  function saveOpen(state) {
    try { localStorage.setItem(KEY_OPEN, JSON.stringify(state)); } catch (_) {}
  }

  // Apply saved state on load. Sections containing the active link override
  // saved-closed → open, so users see the section they just navigated into.
  var openState = loadOpen();
  var details = sidebar.querySelectorAll('details.side-section');
  details.forEach(function (d) {
    var summary = d.querySelector('summary');
    if (!summary) return;
    var title = summary.textContent.trim();
    var hasActive = !!d.querySelector('.side-link.active');
    if (hasActive) {
      d.open = true; // stay expanded when on a page in this group
      return;
    }
    if (Object.prototype.hasOwnProperty.call(openState, title)) {
      d.open = openState[title];
    }
  });

  // Save on toggle.
  details.forEach(function (d) {
    d.addEventListener('toggle', function () {
      var summary = d.querySelector('summary');
      if (!summary) return;
      var title = summary.textContent.trim();
      var state = loadOpen();
      state[title] = d.open;
      saveOpen(state);
    });
  });

  // Restore scroll position. Save it on every link click before navigation.
  try {
    var saved = parseInt(sessionStorage.getItem(KEY_SCROLL) || '0', 10);
    if (saved > 0) sidebar.scrollTop = saved;
  } catch (_) {}

  sidebar.addEventListener('click', function (e) {
    var link = e.target.closest('a.side-link');
    if (!link) return;
    try { sessionStorage.setItem(KEY_SCROLL, String(sidebar.scrollTop)); } catch (_) {}
  });
})();
