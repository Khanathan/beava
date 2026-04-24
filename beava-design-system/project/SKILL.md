---
name: beava-design
description: Use this skill to generate well-branded interfaces and assets for beava (beava.dev, an open-source single-binary feature server for stream processing), either for production or throwaway prototypes/mocks/etc. Contains essential design guidelines, colors, type, fonts, assets, and UI kit components for prototyping.
user-invocable: true
---

Read the `README.md` file within this skill, and explore the other available files.

If creating visual artifacts (slides, mocks, throwaway prototypes, etc.), copy assets out and create static HTML files for the user to view. If working on production code, you can copy assets and read the rules here to become an expert in designing with this brand.

If the user invokes this skill without any other guidance, ask them what they want to build or design, ask some questions, and act as an expert designer who outputs HTML artifacts _or_ production code, depending on the need.

## Quick starting points

- **Tokens:** always import `colors_and_type.css` first. Never invent colors — use the CSS variables.
- **Fonts:** Instrument Serif (display), Inter Tight (UI/body), JetBrains Mono (code), Gaegu (hand-drawn accent — use sparingly). All via Google Fonts.
- **Mascot:** `assets/logo-mark.png` for the default friendly pose; `assets/mascot-work-pose.svg` for docs/tutorials; `assets/mascot-mark-geometric.png` for favicons.
- **Voice:** warm, craft-oriented, self-aware, playful-at-the-edges. Sentence case everywhere. No emoji in product UI.
- **Reference UIs:** `ui_kits/marketing/`, `ui_kits/docs/`, `ui_kits/learn/` — copy components out of these rather than reinventing.
- **No stock photography, no 3D renders, no bluish-purple gradients, no glassmorphism.** Warm cream + orange + brown.
