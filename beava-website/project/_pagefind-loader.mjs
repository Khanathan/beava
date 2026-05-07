// Bridges Pagefind into the page from a real `<script type="module">` context,
// where `import()` works regardless of how the calling code is loaded
// (Babel-standalone wraps `<script type="text/babel">` in a way that breaks
// dynamic import). SiteHeader.jsx injects a tag pointing here.
import('/_pagefind/pagefind.js')
  .then((m) => {
    window.__pagefind = m;
    window.dispatchEvent(new Event('pagefind-ready'));
  })
  .catch((err) => {
    window.__pagefindError = String(err && err.message || err);
    window.dispatchEvent(new CustomEvent('pagefind-failed', { detail: window.__pagefindError }));
  });
