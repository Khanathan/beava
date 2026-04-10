// Debug UI front-end (Phase 10 Plan 04).
//
// XSS posture (RESEARCH §Pitfall 4/7, Task 4 smoke test):
// every DOM write for a server-supplied or user-supplied string
// goes through textContent (via the `el({text:...})` helper) or
// d3's `.text()`.  The HTML-injection sinks banned by the Task 3
// acceptance-criteria grep are forbidden file-wide.

(function () {
  'use strict';

  // ---------- Section 1: state + helpers -----------------------------------

  const state = {
    paused: false,
    activeTab: 'topology',
    lastUpdate: null,
  };

  function el(tag, attrs, children) {
    const node = document.createElement(tag);
    if (attrs) {
      for (const k of Object.keys(attrs)) {
        if (k === 'class') node.className = attrs[k];
        else if (k === 'text') node.textContent = attrs[k];
        else node.setAttribute(k, String(attrs[k]));
      }
    }
    if (children) {
      for (const c of children) node.appendChild(c);
    }
    return node;
  }

  function clear(node) {
    while (node && node.firstChild) node.removeChild(node.firstChild);
  }

  function fmtNumber(n, decimals) {
    if (n == null || Number.isNaN(n)) return '—';
    const d = decimals == null ? 1 : decimals;
    if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(d) + 'M';
    if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(d) + 'k';
    return n.toFixed(d);
  }

  function fmtBytes(bytes) {
    if (!bytes) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB'];
    let i = 0;
    let v = bytes;
    while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
    return v.toFixed(1) + ' ' + units[i];
  }

  function fmtTime(d) {
    const p = (n) => String(n).padStart(2, '0');
    return p(d.getHours()) + ':' + p(d.getMinutes()) + ':' + p(d.getSeconds());
  }

  function updateLastUpdate() {
    state.lastUpdate = new Date();
    const span = document.getElementById('footer-update');
    if (span) span.textContent = fmtTime(state.lastUpdate);
  }

  function parseHtmxEvent(evt) {
    const xhr = evt && evt.detail && evt.detail.xhr;
    if (!xhr) return null;
    try { return JSON.parse(xhr.responseText); } catch (_) { return null; }
  }

  // ---------- Section 2: tabs + pause + footer chrome ----------------------

  function activateTab(name) {
    state.activeTab = name;
    document.querySelectorAll('.tab').forEach((t) => {
      const match = t.dataset.tab === name;
      t.setAttribute('aria-selected', match ? 'true' : 'false');
    });
    document.querySelectorAll('.tab-panel').forEach((p) => {
      if (p.id === 'panel-' + name) p.removeAttribute('hidden');
      else p.setAttribute('hidden', '');
    });
    if (name === 'topology') {
      renderTopology();
    }
  }

  function setPaused(paused) {
    state.paused = paused;
    const ctrl = document.querySelector('.poll-control');
    if (ctrl) ctrl.dataset.paused = String(paused);
    const btn = document.getElementById('pause-btn');
    if (btn) btn.textContent = paused ? 'Resume' : 'Pause';
    const label = document.querySelector('.poll-label');
    if (label) {
      if (paused) {
        const t = state.lastUpdate ? fmtTime(state.lastUpdate) : '—';
        label.textContent = 'Paused · last update ' + t;
      } else {
        label.textContent = 'Live · 1 Hz';
      }
    }
    const live = document.getElementById('poll-status');
    if (live) live.textContent = paused ? 'Polling paused' : 'Polling resumed';
    // Disable htmx polling on every container with an every-N trigger.
    document.querySelectorAll('[hx-trigger*="every"]').forEach((node) => {
      if (paused) node.setAttribute('data-hx-disable', 'true');
      else node.removeAttribute('data-hx-disable');
    });
  }

  async function loadHealth() {
    try {
      const res = await fetch('/health');
      if (!res.ok) return;
      const j = await res.json();
      const v = document.getElementById('footer-version');
      if (v) v.textContent = j && j.version ? 'v' + j.version : 'v—';
      const h = document.getElementById('footer-host');
      if (h) h.textContent = window.location.host;
    } catch (_) {
      // footer is decorative; swallow.
    }
  }

  // ---------- Section 3: topology renderer (dagre-d3 over fetch JSON) ------

  async function renderTopology() {
    const svgEl = document.getElementById('topology-svg');
    if (!svgEl) return;
    if (typeof dagreD3 === 'undefined' || typeof d3 === 'undefined') {
      setTimeout(renderTopology, 100);
      return;
    }
    try {
      const res = await fetch('/debug/topology');
      if (!res.ok) throw new Error('HTTP ' + res.status);
      const data = await res.json();
      drawTopology(data);
      updateLastUpdate();
    } catch (e) {
      drawTopologyError(String(e && e.message ? e.message : e));
    }
  }

  function drawTopology(data) {
    const svgEl = document.getElementById('topology-svg');
    if (!svgEl) return;
    const svg = d3.select('#topology-svg');
    svg.selectAll('*').remove();

    // Remove any empty/error state left over from a prior render.
    const card = document.querySelector('.topology-card');
    if (card) {
      card.querySelectorAll('.empty-state, .error-state').forEach((n) => n.remove());
      const canvas = card.querySelector('.topology-canvas');
      if (canvas) canvas.style.display = '';
    }

    if (!data || !data.nodes || data.nodes.length === 0) {
      drawTopologyEmpty();
      return;
    }

    const g = new dagreD3.graphlib.Graph()
      .setGraph({ rankdir: 'LR', marginx: 24, marginy: 24, nodesep: 24, ranksep: 48 })
      .setDefaultEdgeLabel(() => ({}));

    for (const node of data.nodes) {
      // dagre-d3 default labelType is 'text' — the label is rendered as
      // an SVG <text> element with text content escaping applied. We do
      // not opt into the HTML label variant anywhere; that would allow
      // XSS via stream/view names (T-10-02).
      g.setNode(node.name, {
        label: node.name,
        class: 'node ' + (node.kind === 'view' ? 'view' : 'stream'),
        rx: 12, ry: 12,
        paddingX: 16, paddingY: 12,
      });
    }
    for (const edge of (data.edges || [])) {
      g.setEdge(edge.from, edge.to, {
        arrowhead: 'vee',
        curve: d3.curveBasis,
      });
    }

    const render = new dagreD3.render();
    const inner = svg.append('g');
    render(inner, g);

    // Fit the drawn graph inside the svg viewport.
    const bbox = inner.node().getBBox();
    const svgW = svgEl.clientWidth || 800;
    const svgH = svgEl.clientHeight || 480;
    const scale = Math.min(
      (svgW - 24) / Math.max(bbox.width, 1),
      (svgH - 24) / Math.max(bbox.height, 1),
      1
    );
    const tx = (svgW - bbox.width * scale) / 2 - bbox.x * scale;
    const ty = (svgH - bbox.height * scale) / 2 - bbox.y * scale;
    inner.attr('transform', 'translate(' + tx + ',' + ty + ') scale(' + scale + ')');
  }

  function drawTopologyEmpty() {
    const card = document.querySelector('.topology-card');
    if (!card) return;
    const canvas = card.querySelector('.topology-canvas');
    if (canvas) canvas.style.display = 'none';
    if (card.querySelector('.empty-state')) return;
    const empty = el('div', { class: 'empty-state' }, [
      makeIconSvg('tally-mark'),
      el('h3', { text: 'No pipelines registered' }),
      el('p', { text: 'Use the Python SDK or REGISTER command to push a pipeline, then reload this page.' }),
    ]);
    card.appendChild(empty);
  }

  function drawTopologyError(msg) {
    const card = document.querySelector('.topology-card');
    if (!card) return;
    card.querySelectorAll('.error-state').forEach((n) => n.remove());
    const body = el('p', {});
    body.textContent = msg; // textContent — avoids any HTML injection sink.
    const btn = el('button', { class: 'btn btn-secondary', type: 'button', text: 'Retry' });
    btn.addEventListener('click', renderTopology);
    const err = el('div', { class: 'error-state' }, [
      makeIconSvg('alert-triangle'),
      el('h3', { text: 'Could not load topology' }),
      body,
      btn,
    ]);
    card.appendChild(err);
  }

  function makeIconSvg(name) {
    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('class', 'icon');
    svg.setAttribute('width', '48');
    svg.setAttribute('height', '48');
    svg.setAttribute('aria-hidden', 'true');
    const use = document.createElementNS('http://www.w3.org/2000/svg', 'use');
    use.setAttribute('href', '/static/icons.svg#' + name);
    svg.appendChild(use);
    return svg;
  }

  // ---------- Section 4: streams / entity / memory renderers ---------------

  function renderStreams(evt) {
    const data = parseHtmxEvent(evt);
    const container = document.getElementById('streams-list');
    if (!container) return;
    if (!data) return;
    updateLastUpdate();

    clear(container);

    const hdr = el('div', { class: 'streams-header' }, [
      el('div', { text: 'NAME' }),
      el('div', { text: 'KIND' }),
      el('div', { class: 'events-per-sec', text: 'EVENTS/SEC' }),
      el('div', { class: 'active-keys', text: 'ACTIVE KEYS' }),
      el('div', { text: 'STATUS' }),
      el('div', {}),
    ]);
    container.appendChild(hdr);

    if (!data.streams || data.streams.length === 0) {
      const empty = el('div', { class: 'empty-state' }, [
        makeIconSvg('tally-mark'),
        el('h3', { text: 'No streams yet' }),
        el('p', { text: 'Register a pipeline via the SDK to see streams appear here.' }),
      ]);
      container.appendChild(empty);
      return;
    }

    for (const s of data.streams) {
      const eps = typeof s.ewma_5s === 'number' ? s.ewma_5s : 0;
      const epsCell = el('div', { class: 'events-per-sec' + (eps === 0 ? ' zero' : ''), text: fmtNumber(eps) });
      const keysCell = el('div', { class: 'active-keys zero', text: '—' });
      const statusChip = el('span', { class: 'chip chip-ok', text: 'OK' });
      const row = el('div', { class: 'streams-row', role: 'button', tabindex: '0' }, [
        el('div', { class: 'name', text: s.name }),          // textContent — escapes XSS
        el('div', { text: 'stream' }),
        epsCell,
        keysCell,
        el('div', {}, [statusChip]),
        el('div', {}),
      ]);
      container.appendChild(row);
    }
  }

  function renderEntity(evt) {
    const container = document.getElementById('entity-result');
    if (!container) return;
    updateLastUpdate();
    clear(container);

    const xhr = evt && evt.detail && evt.detail.xhr;
    const rawKey = document.getElementById('entity-key');
    const typedKey = rawKey ? rawKey.value : '';
    const data = parseHtmxEvent(evt);

    // 404 or server-side error: data is either {error:...} or parses to
    // something without features. Treat both as "not found", echoing the
    // typed key verbatim via textContent (T-10-04 XSS mitigation).
    if (!xhr || xhr.status >= 400 || !data) {
      const keyForDisplay = (data && data.key) || typedKey || '';
      container.appendChild(el('div', { class: 'empty-state' }, [
        makeIconSvg('search'),
        el('h3', { text: 'No features for "' + keyForDisplay + '"' }),
        el('p', { text: 'This key has not received any events recently, or has been evicted by TTL.' }),
      ]));
      return;
    }

    // Real-world /debug/key/{key} returns computed_features (map) plus
    // live_operators and static_features. The plan interface doc showed
    // `features`, so accept either — Rule 1/3 deviation from the plan
    // spec to match the actual server contract in src/server/http.rs.
    const featureMap = data.computed_features || data.features || {};
    const featureKeys = Object.keys(featureMap);

    if (featureKeys.length === 0) {
      const keyForDisplay = data.key || typedKey || '';
      container.appendChild(el('div', { class: 'empty-state' }, [
        makeIconSvg('search'),
        el('h3', { text: 'No features for "' + keyForDisplay + '"' }),
        el('p', { text: 'This key has not received any events recently, or has been evicted by TTL.' }),
      ]));
      return;
    }

    // Header strip with the entity key and last-event timestamp.
    const lastEvent = data.last_event_at;
    const lastEventText = lastEvent != null
      ? 'last event: ' + new Date(lastEvent * 1000).toLocaleTimeString()
      : 'last event: —';
    const keyStrip = el('div', { class: 'entity-key-strip' }, [
      el('code', { class: 'entity-key', text: String(data.key || typedKey || '') }),
      el('span', { class: 'entity-last-event', text: lastEventText }),
    ]);
    container.appendChild(keyStrip);

    // Feature grid.
    const grid = el('div', { class: 'entity-feature-grid' });
    for (const name of featureKeys) {
      const value = featureMap[name];

      let source = '';
      let label = name;
      const dot = name.indexOf('.');
      if (dot > 0) {
        source = name.substring(0, dot);
        label = name.substring(dot + 1);
      }

      let valueClass = 'value';
      let valueText;
      if (value === true) { valueClass += ' bool-true'; valueText = 'true'; }
      else if (value === false) { valueClass += ' bool-false'; valueText = 'false'; }
      else if (value === null || value === undefined) { valueText = '—'; }
      else if (typeof value === 'number') { valueText = fmtNumber(value); }
      else { valueText = String(value); }

      const cell = el('div', { class: 'entity-feature-cell' }, [
        el('div', { class: 'label', text: label }),
        el('div', { class: valueClass, text: valueText }),
        el('div', { class: 'source', text: (source ? source + ' · live' : 'live') }),
      ]);
      grid.appendChild(cell);
    }
    container.appendChild(grid);
  }

  function renderMemory(evt) {
    const data = parseHtmxEvent(evt);
    if (!data) return;
    updateLastUpdate();

    const summary = document.getElementById('memory-summary');
    if (summary) {
      clear(summary);
      const stats = [
        { label: 'Total memory',    value: fmtBytes(data.estimated_bytes || 0) },
        { label: 'Active keys',     value: fmtNumber(data.entity_count || 0, 0) },
        { label: 'Streams tracked', value: fmtNumber(data.stream_count || 0, 0) },
      ];
      for (const s of stats) {
        summary.appendChild(el('div', { class: 'memory-stat' }, [
          el('div', { class: 'label', text: s.label }),
          el('div', { class: 'value', text: s.value }),
        ]));
      }
    }

    const bars = document.getElementById('memory-bars');
    if (!bars) return;
    clear(bars);

    const rows = (data.per_stream || []).slice()
      .sort((a, b) => (b.estimated_bytes || 0) - (a.estimated_bytes || 0));
    if (rows.length === 0) {
      bars.appendChild(el('div', { class: 'empty-state' }, [
        makeIconSvg('tally-mark'),
        el('h3', { text: 'No memory data yet' }),
        el('p', { text: 'Tally will report usage once a stream is registered.' }),
      ]));
      return;
    }

    const maxBytes = rows[0].estimated_bytes || 1;
    for (const r of rows) {
      const pct = maxBytes > 0
        ? Math.max(0, Math.min(100, ((r.estimated_bytes || 0) / maxBytes) * 100))
        : 0;
      const fill = el('div', { class: 'fill' });
      fill.style.width = pct.toFixed(1) + '%';
      const row = el('div', {
        class: 'memory-row' + (r.kind === 'view' ? ' view' : ''),
      }, [
        el('div', { class: 'name', text: r.name }),                                 // textContent
        el('div', { class: 'count', text: String(r.key_count || 0) + ' keys' }),
        el('div', { class: 'memory-bar' }, [fill]),
        el('div', { class: 'size', text: fmtBytes(r.estimated_bytes || 0) }),
      ]);
      bars.appendChild(row);
    }
  }

  // ---------- Section 5: DOMContentLoaded wire-up --------------------------

  document.addEventListener('DOMContentLoaded', () => {
    // Tab clicks
    document.querySelectorAll('.tab').forEach((t) => {
      t.addEventListener('click', (e) => {
        e.preventDefault();
        activateTab(t.dataset.tab);
      });
    });
    // Pause button
    const pauseBtn = document.getElementById('pause-btn');
    if (pauseBtn) pauseBtn.addEventListener('click', () => setPaused(!state.paused));
    // Global Space key toggles pause when no input is focused.
    document.addEventListener('keydown', (e) => {
      if (e.code !== 'Space') return;
      const active = document.activeElement;
      if (active && (active.tagName === 'INPUT' || active.tagName === 'TEXTAREA' || active.isContentEditable)) return;
      e.preventDefault();
      setPaused(!state.paused);
    });
    // Footer chrome
    loadHealth();
    const host = document.getElementById('footer-host');
    if (host) host.textContent = window.location.host;
    // Initial topology render (Topology is the default active tab).
    renderTopology();
  });

  window.app = {
    activateTab,
    setPaused,
    renderTopology,
    renderStreams,
    renderEntity,
    renderMemory,
  };
})();
