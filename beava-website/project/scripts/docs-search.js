// docs-search.js — Cmd-K + click-to-open search modal for the markdown-rendered
// docs pages. Mirrors the React SearchModal in js/_shared/SiteHeader.jsx so
// every surface (homepage, React docs, static docs) shares one search UI.
//
// Loads pagefind.js dynamically when first opened. No PagefindUI bundle.
(function () {
  var trigger = document.getElementById('search');
  if (!trigger) return;

  var pagefind = null;
  var pagefindError = null;
  var modal = null;
  var input = null;
  var resultsEl = null;
  var statusEl = null;
  var debounce = null;

  function ensureLoaded() {
    if (pagefind || pagefindError) return Promise.resolve();
    return import('/_pagefind/pagefind.js')
      .then(function (m) {
        pagefind = m;
        if (m.options) {
          try { m.options({ excerptLength: 24 }); } catch (_) {}
        }
      })
      .catch(function (err) {
        pagefindError = String(err && err.message || err);
      });
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;')
      .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  function renderResults(results) {
    if (!results.length) {
      resultsEl.innerHTML = '<div class="ds-empty">No matches. Try a different term, or browse <a href="/docs/">the docs index</a>.</div>';
      statusEl.textContent = '0 results';
      return;
    }
    var docIcon = '<svg class="ds-result-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>';
    var hashIcon = '<svg class="ds-sub-icon" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><line x1="4" y1="9" x2="20" y2="9"/><line x1="4" y1="15" x2="20" y2="15"/><line x1="10" y1="3" x2="8" y2="21"/><line x1="16" y1="3" x2="14" y2="21"/></svg>';
    resultsEl.innerHTML = results.map(function (r) {
      var subs = (r.subResults || []).filter(function (s) { return s.title; });
      var subHtml = subs.length === 0
        ? (r.excerpt ? '<div class="ds-result-excerpt">' + r.excerpt + '</div>' : '')
        : subs.map(function (sr) {
            return (
              '<a class="ds-sub" href="' + escapeHtml(sr.url) + '">' +
                '<div class="ds-sub-head">' + hashIcon +
                  '<span class="ds-sub-title">' + escapeHtml(sr.title) + '</span>' +
                '</div>' +
                (sr.excerpt ? '<div class="ds-sub-excerpt">' + sr.excerpt + '</div>' : '') +
              '</a>'
            );
          }).join('');
      return (
        '<div class="ds-group">' +
          '<a class="ds-result" href="' + escapeHtml(r.url) + '">' +
            '<div class="ds-result-row">' +
              docIcon +
              '<span class="ds-result-title">' + escapeHtml(r.title) + '</span>' +
              (r.section ? '<span class="ds-result-section">' + escapeHtml(r.section) + '</span>' : '') +
            '</div>' +
            '<span class="ds-result-url">' + escapeHtml(r.url) + '</span>' +
          '</a>' +
          subHtml +
        '</div>'
      );
    }).join('');
    statusEl.textContent = results.length + ' result' + (results.length === 1 ? '' : 's');
  }

  function runSearch(q) {
    q = (q || '').trim();
    if (!q) {
      resultsEl.innerHTML = '<div class="ds-empty">Type to search the docs.</div>';
      statusEl.textContent = '';
      return;
    }
    if (pagefindError) {
      resultsEl.innerHTML = '<div class="ds-empty">Search index unavailable: ' + escapeHtml(pagefindError) + '</div>';
      return;
    }
    if (!pagefind) {
      resultsEl.innerHTML = '<div class="ds-empty">Loading search…</div>';
      ensureLoaded().then(function () { runSearch(q); });
      return;
    }
    pagefind.search(q).then(function (s) {
      if (!s || !s.results) { renderResults([]); return; }
      Promise.all(s.results.slice(0, 10).map(function (r) { return r.data(); })).then(function (data) {
        renderResults(data.map(function (d) {
          return {
            url: d.url,
            title: (d.meta && d.meta.title) || d.url,
            section: (d.meta && d.meta.section) || '',
            excerpt: d.excerpt || '',
            subResults: (d.sub_results || []).slice(0, 4).map(function (sr) {
              return { title: sr.title || '', url: sr.url || d.url, excerpt: sr.excerpt || '' };
            }),
          };
        }));
      });
    });
  }

  function openModal() {
    if (modal) return;
    modal = document.createElement('div');
    modal.className = 'ds-modal';
    modal.innerHTML =
      '<div class="ds-overlay"></div>' +
      '<div class="ds-dialog" role="dialog" aria-label="Search the docs">' +
        '<div class="ds-input-row">' +
          '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="11" cy="11" r="7"/><path d="M21 21l-4.35-4.35"/></svg>' +
          '<input type="text" class="ds-input" placeholder="Search the docs..." autocomplete="off" spellcheck="false"/>' +
          '<span class="ds-kbd">esc</span>' +
        '</div>' +
        '<div class="ds-results"></div>' +
        '<div class="ds-foot">' +
          '<span><kbd class="ds-kbd">↵</kbd> open first match</span>' +
          '<span class="ds-status"></span>' +
        '</div>' +
      '</div>';
    document.body.appendChild(modal);
    input = modal.querySelector('.ds-input');
    resultsEl = modal.querySelector('.ds-results');
    statusEl = modal.querySelector('.ds-status');
    runSearch('');

    modal.querySelector('.ds-overlay').addEventListener('click', closeModal);
    input.addEventListener('input', function () {
      clearTimeout(debounce);
      debounce = setTimeout(function () { runSearch(input.value); }, 80);
    });
    input.addEventListener('keydown', function (e) {
      if (e.key === 'Enter') {
        var first = resultsEl.querySelector('.ds-result');
        if (first) window.location.href = first.getAttribute('href');
      }
    });

    setTimeout(function () { input.focus(); }, 30);
    document.documentElement.style.overflow = 'hidden';
    ensureLoaded();
  }

  function closeModal() {
    if (!modal) return;
    modal.remove();
    modal = null;
    input = null;
    resultsEl = null;
    statusEl = null;
    document.documentElement.style.overflow = '';
  }

  trigger.style.cursor = 'pointer';
  trigger.setAttribute('role', 'button');
  trigger.setAttribute('tabindex', '0');
  trigger.setAttribute('aria-label', 'Search the docs');
  trigger.addEventListener('click', openModal);
  trigger.addEventListener('keydown', function (e) {
    if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); openModal(); }
  });
  document.addEventListener('keydown', function (e) {
    var k = (e.key || '').toLowerCase();
    if ((e.metaKey || e.ctrlKey) && k === 'k') { e.preventDefault(); openModal(); return; }
    if (e.key === 'Escape') { closeModal(); }
  });
})();
