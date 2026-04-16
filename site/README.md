# Beava landing page

Single-page static site for `beava.dev`. No build step, no JS frameworks, no
external runtime dependencies — just `index.html` + `styles.css` + assets.

## Layout

```
site/
├── index.html      # the page
├── styles.css      # vanilla CSS, ~7 KB
├── assets/
│   └── branding.png  # logo (also used as favicon)
└── README.md       # this file
```

## Local preview

```bash
cd site
python3 -m http.server 8000
# open http://localhost:8000
```

Or with any static server:

```bash
npx serve site/
```

## Deploy

### Option A — Vercel / Netlify / Cloudflare Pages (recommended)

Just point at `site/` as the publish directory. Zero config.

```bash
# Vercel
vercel deploy site/ --prod

# Netlify
netlify deploy --dir=site --prod

# Cloudflare Pages: Connect the repo, set build dir to `site`.
```

### Option B — GitHub Pages

```bash
git subtree push --prefix site origin gh-pages
# Settings → Pages → branch: gh-pages, dir: /
```

### Option C — Plain VPS + Caddy

```caddy
beava.dev {
    root * /var/www/beava-site
    try_files {path} {path}/index.html
    file_server
    encode gzip zstd
}
```

```bash
rsync -avz site/ user@beava.dev:/var/www/beava-site/
```

## Things to wire up before launch

1. **Hero GIF** — replace the `.video-placeholder` block with an autoplay
   muted loop video once the 12-sec recording lands. Suggested:
   ```html
   <video autoplay muted loop playsinline class="hero-gif" poster="/assets/hero-poster.jpg">
     <source src="/assets/fork-demo.mp4" type="video/mp4">
     <source src="/assets/fork-demo.webm" type="video/webm">
   </video>
   ```

2. **Cloud waitlist form** — the current form posts to a `mailto:`. Swap to
   ConvertKit / Mailchimp / Tally.so / Plain HTML form action when wired up.

3. **`/tutorial` page** — Colab notebook redirect (Phase 2).

4. **`/demo` page** — page embedding 90-sec + 6-min walkthrough videos.

5. **`/blog` page** — pull from `BLOG-POST-V6.md` once ready.

6. **OG image** — replace the favicon-as-OG with a 1200×630 image for social
   shares. Currently using `branding.png`.

7. **Analytics** — drop in Plausible / Fathom snippet in `<head>` if desired.
   Don't use GA — pretend you have taste.

## Accessibility + perf notes

- Single CSS file, ~7 KB. No JS. No external font loads.
- `prefers-reduced-motion` honoured.
- Color contrast meets WCAG AA throughout.
- All anchor links have `scroll-margin-top` set so they don't slide under the
  sticky header.
- Sticky sidebar collapses to inline at < 1024 px.

## Tweaking the design

The whole color system lives in `:root` at the top of `styles.css`. Change
`--accent` and the doors / link hovers / focus states all update.
