// Phase 10.1 Debug UI — interactive split-view behavior.
//
// Architecture:
//   1. Helpers (el, clear, svgEl, formatFps, formatBytes, formatFeatureValue,
//      fmtTime, makeTallyMarkIcon)
//   2. Module-scoped state
//   3. Selection controller (setSelected)
//   4. Drill-in panel renderers (5 sections for streams, 3 for views)
//   5. Topology loader (fetch + dagre-d3 render once + click bindings)
//   6. Edge label updater (1 Hz vanilla fetch + setInterval, pause-gated,
//      in-place tspan text updates — never re-renders the DAG)
//   7. Memory loader (2 Hz vanilla fetch + setInterval, pause-gated)
//   8. Pause controller (setPaused, updateFooterLastUpdate)
//   9. Entity lookup (stream-scoped filter)
//  10. Bootstrap (DOMContentLoaded wiring)
//
// XSS posture (Plan 10-04 + Phase 10.1): every DOM write for a server- or
// user-supplied string goes through textContent (via the el({text:...})
// helper) or d3 .text(). A regression test in tests/test_debug_ui.rs asserts
// that the banned HTML-parsing and code-execution sinks never appear in the
// served app.js bytes — including inside comments — so this file avoids
// writing those substrings even in documentation.

'use strict';

// ================================================================
// 1. Helpers
// ================================================================

// Minimal DOM helper — textContent-only, no HTML parsing.
function el(tag, attrs, children) {
  const node = document.createElement(tag);
  if (attrs) {
    for (const k of Object.keys(attrs)) {
      if (k === 'text') {
        node.textContent = attrs[k];
      } else if (k === 'class') {
        node.className = attrs[k];
      } else if (k.startsWith('on') && typeof attrs[k] === 'function') {
        node.addEventListener(k.slice(2).toLowerCase(), attrs[k]);
      } else {
        node.setAttribute(k, attrs[k]);
      }
    }
  }
  if (Array.isArray(children)) {
    for (const c of children) { if (c) node.appendChild(c); }
  } else if (children) {
    node.appendChild(children);
  }
  return node;
}

function clear(node) {
  while (node && node.firstChild) node.removeChild(node.firstChild);
}

// SVG elements require the SVG namespace — document.createElement produces
// useless HTMLUnknownElement nodes for 'svg', 'line', etc.
function svgEl(tag, attrs, children) {
  const node = document.createElementNS('http://www.w3.org/2000/svg', tag);
  if (attrs) {
    for (const k of Object.keys(attrs)) {
      if (k === 'text') {
        node.textContent = attrs[k];
      } else if (k === 'class') {
        node.setAttribute('class', attrs[k]);
      } else {
        node.setAttribute(k, attrs[k]);
      }
    }
  }
  if (Array.isArray(children)) {
    for (const c of children) { if (c) node.appendChild(c); }
  } else if (children) {
    node.appendChild(children);
  }
  return node;
}

// Format events/sec as a compact label: '—/s' · '0/s' · '0.4/s' · '12.3/s'
// · '450/s' · '1.2k/s' · '12k/s' · '1.2M/s'. RESEARCH Pattern 3.
function formatFps(n) {
  if (n == null || Number.isNaN(n)) return '—/s';
  if (n === 0) return '0/s';
  const abs = Math.abs(n);
  if (abs < 10) return n.toFixed(1) + '/s';
  if (abs < 1000) return Math.round(n) + '/s';
  if (abs < 1e6) {
    const k = n / 1000;
    return (k < 10 ? k.toFixed(1) : Math.round(k)) + 'k/s';
  }
  const m = n / 1e6;
  return (m < 10 ? m.toFixed(1) : Math.round(m)) + 'M/s';
}

function formatBytes(n) {
  if (n == null || Number.isNaN(n)) return '—';
  if (n === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB'];
  let i = 0;
  let v = n;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return (v < 10 ? v.toFixed(1) : Math.round(v)) + ' ' + units[i];
}

function formatFeatureValue(v) {
  if (v == null) return 'null';
  if (typeof v === 'boolean') return v ? 'true' : 'false';
  if (typeof v === 'number') {
    if (Number.isInteger(v)) return String(v);
    return v.toFixed(2);
  }
  if (typeof v === 'string') return v;
  // Objects/arrays: the JSON text goes through textContent, which is safe.
  try { return JSON.stringify(v); } catch (_) { return String(v); }
}

function fmtTime(d) {
  if (!d) return '—';
  const t = (d instanceof Date) ? d : new Date(d);
  const hh = String(t.getHours()).padStart(2, '0');
  const mm = String(t.getMinutes()).padStart(2, '0');
  const ss = String(t.getSeconds()).padStart(2, '0');
  return hh + ':' + mm + ':' + ss;
}

// Build the tally-mark icon for the empty state via createElementNS — every
// attribute is a static literal, no user input reaches SVG.
function makeTallyMarkIcon() {
  const svg = svgEl('svg', {
    width: '48', height: '48', viewBox: '0 0 24 24',
    fill: 'none', stroke: 'currentColor', 'stroke-width': '2',
    'stroke-linecap': 'round', 'aria-hidden': 'true'
  });
  const lines = [
    [4, 4, 4, 20], [9, 4, 9, 20], [14, 4, 14, 20], [19, 4, 19, 20],
    [2, 18, 21, 6]
  ];
  for (const coords of lines) {
    svg.appendChild(svgEl('line', {
      x1: coords[0], y1: coords[1], x2: coords[2], y2: coords[3]
    }));
  }
  return svg;
}

// ================================================================
// 2. Module-scoped state
// ================================================================

const state = {
  paused: false,
  selectedStream: null,
  topology: null,    // last /debug/topology response
  throughput: null,  // last /debug/throughput response
  memory: null,      // last /debug/memory response
  lastUpdate: null,  // Date of last successful tick
  health: null,
};

// ================================================================
// 3. Selection controller
// ================================================================

function findNode(name) {
  if (!state.topology || !state.topology.nodes) return null;
  for (const n of state.topology.nodes) {
    if (n.name === name) return n;
  }
  return null;
}

function setSelected(name) {
  state.selectedStream = name;

  // Update .selected / aria-pressed on every rendered g.node.
  const svg = d3.select('#topology-svg');
  svg.selectAll('g.node')
    .classed('selected', function (v) { return v === name; })
    .attr('aria-pressed', function (v) { return String(v === name); });

  renderDrillInPanel(name);
}

// ================================================================
// 4. Drill-in panel renderers
// ================================================================

function renderDrillInPanel(name) {
  const panel = document.getElementById('drill-in-panel');
  if (!panel) return;
  clear(panel);

  if (name == null) {
    panel.setAttribute('data-empty', 'true');
    panel.appendChild(el('p', {
      class: 'drill-in-placeholder',
      text: 'Select a stream to see details',
    }));
    return;
  }
  panel.removeAttribute('data-empty');

  const node = findNode(name);
  if (!node) {
    panel.setAttribute('data-empty', 'true');
    panel.appendChild(el('p', {
      class: 'drill-in-placeholder',
      text: 'Select a stream to see details',
    }));
    return;
  }

  renderStreamHeader(panel, node);
  if (node.kind === 'view') {
    // Views: State → Features; omit Memory and Throughput (no materialized
    // per-stream state for views).
    renderFeaturesSection(panel, node);
    renderEntityLookupSection(panel, node);
  } else {
    renderMemorySection(panel, node, state.memory);
    renderStateSection(panel, node);
    renderThroughputSection(panel, node, state.throughput);
    renderEntityLookupSection(panel, node);
  }
}

function renderStreamHeader(panel, node) {
  const section = el('section', { class: 'drill-in-section drill-in-header' });
  section.appendChild(el('h2', { class: 'drill-in-title', text: node.name }));

  const metaRow = el('div', { class: 'meta-row' });
  const badgeClass = node.kind === 'view' ? 'meta-badge meta-view' : 'meta-badge';
  metaRow.appendChild(el('span', {
    class: badgeClass,
    text: node.kind || 'stream',
  }));
  if (node.key_field) {
    metaRow.appendChild(el('span', { text: 'key: ' + node.key_field }));
  }
  if (node.depends_on && node.depends_on.length > 0) {
    metaRow.appendChild(el('span', {
      text: 'depends on: ' + node.depends_on.join(', '),
    }));
  }
  section.appendChild(metaRow);
  panel.appendChild(section);
}

function renderMemorySection(panel, node, memory) {
  // May be called from the 2 Hz tick to refresh this section ONLY, leaving
  // the entity input DOM node (and any rendered lookup results) untouched
  // (Pitfall 6 — granular re-render preserves user-visible state across
  // ticks). Mirrors the throughput pattern: if a <section
  // data-section="memory"> is already mounted, swap it in place; otherwise
  // append a new one during the initial drill-in panel render.
  const existing = panel.querySelector(
    '.drill-in-section[data-section="memory"]'
  );

  const section = el('section', {
    class: 'drill-in-section',
    'data-section': 'memory',
  });
  section.appendChild(el('h3', { class: 'section-title', text: 'Memory' }));

  let entry = null;
  if (memory && memory.per_stream) {
    for (const s of memory.per_stream) {
      if (s.name === node.name) { entry = s; break; }
    }
  }

  const row = el('div', { class: 'stat-row' });
  row.appendChild(el('div', { class: 'stat' }, [
    el('div', { class: 'stat-label', text: 'Active keys' }),
    el('div', {
      class: 'stat-value',
      text: entry ? String(entry.key_count != null ? entry.key_count : 0) : '—',
    }),
  ]));
  row.appendChild(el('div', { class: 'stat' }, [
    el('div', { class: 'stat-label', text: 'Bytes' }),
    el('div', {
      class: 'stat-value',
      text: entry ? formatBytes(entry.estimated_bytes || 0) : '—',
    }),
  ]));
  section.appendChild(row);

  if (existing) {
    existing.replaceWith(section);
  } else {
    panel.appendChild(section);
  }
}

function renderStateSection(panel, node) {
  const section = el('section', {
    class: 'drill-in-section',
    'data-section': 'state',
  });
  section.appendChild(el('h3', { class: 'section-title', text: 'State' }));

  const operators = node.operators || [];
  if (operators.length === 0) {
    section.appendChild(el('p', { class: 'muted', text: 'No operators registered.' }));
    panel.appendChild(section);
    return;
  }

  const list = el('ul', { class: 'operator-list' });
  for (const op of operators) {
    const parts = [];
    if (op.op) parts.push(op.op);
    if (op.window) parts.push(op.window);
    if (op.field) parts.push('field: ' + op.field);
    if (op.where) parts.push('where: ' + op.where);
    if (op.expr) parts.push('expr: ' + op.expr);
    const summary = parts.join(' · ');

    list.appendChild(el('li', { class: 'operator-item' }, [
      el('span', { class: 'operator-name', text: op.name || '' }),
      el('span', { class: 'operator-summary', text: summary }),
    ]));
  }
  section.appendChild(list);
  panel.appendChild(section);
}

// Views use Features instead of State. derive → expr; lookup → target + on.
function renderFeaturesSection(panel, node) {
  const section = el('section', {
    class: 'drill-in-section',
    'data-section': 'features',
  });
  section.appendChild(el('h3', { class: 'section-title', text: 'Features' }));

  const operators = node.operators || [];
  if (operators.length === 0) {
    section.appendChild(el('p', { class: 'muted', text: 'No features registered.' }));
    panel.appendChild(section);
    return;
  }

  const list = el('ul', { class: 'operator-list' });
  for (const op of operators) {
    const parts = [];
    if (op.op) parts.push(op.op);
    if (op.target) parts.push('target: ' + op.target);
    if (op.on) parts.push('on: ' + op.on);
    if (op.expr) parts.push('expr: ' + op.expr);
    const summary = parts.join(' · ');

    list.appendChild(el('li', { class: 'operator-item' }, [
      el('span', { class: 'operator-name', text: op.name || '' }),
      el('span', { class: 'operator-summary', text: summary }),
    ]));
  }
  section.appendChild(list);
  panel.appendChild(section);
}

function renderThroughputSection(panel, node, throughput) {
  // May be called from the 1 Hz tick to refresh this section ONLY, leaving
  // the entity input DOM node untouched (Pitfall 6 — granular re-render
  // preserves the user's typed text across ticks).
  const existing = panel.querySelector(
    '.drill-in-section[data-section="throughput"]'
  );

  const section = el('section', {
    class: 'drill-in-section',
    'data-section': 'throughput',
  });
  section.appendChild(el('h3', { class: 'section-title', text: 'Throughput' }));

  let entry = null;
  if (throughput && throughput.streams) {
    for (const s of throughput.streams) {
      if (s.name === node.name) { entry = s; break; }
    }
  }

  const row = el('div', { class: 'stat-row' });
  const cells = [['5s', 'ewma_5s'], ['1m', 'ewma_1m'], ['5m', 'ewma_5m']];
  for (const pair of cells) {
    const label = pair[0];
    const key = pair[1];
    const val = entry ? formatFps(entry[key]) : '—/s';
    row.appendChild(el('div', { class: 'stat' }, [
      el('div', { class: 'stat-label', text: label }),
      el('div', { class: 'stat-value', text: val }),
    ]));
  }
  section.appendChild(row);

  if (existing) {
    existing.replaceWith(section);
  } else {
    panel.appendChild(section);
  }
}

function renderEntityLookupSection(panel, node) {
  const section = el('section', {
    class: 'drill-in-section',
    'data-section': 'entity',
  });
  section.appendChild(el('h3', { class: 'section-title', text: 'Entity lookup' }));

  const form = el('form', { class: 'entity-form' });
  form.addEventListener('submit', function (e) {
    e.preventDefault();
    const input = form.querySelector('input[type="text"]');
    lookupEntityForSelectedStream(input ? input.value : '');
  });

  form.appendChild(el('label', { for: 'entity-key', text: 'Entity key' }));

  const row = el('div', { class: 'entity-input-row' });
  const input = el('input', {
    type: 'text',
    id: 'entity-key',
    placeholder: 'e.g. u_12345',
    autocomplete: 'off',
  });
  // Fresh state per selection — clear any previous value so the typed key
  // from a different stream doesn't bleed across selections.
  input.value = '';
  input.addEventListener('keydown', function (e) {
    if (e.key === 'Escape') {
      input.value = '';
      const result = document.getElementById('entity-result');
      if (result) {
        clear(result);
        result.appendChild(el('p', {
          class: 'entity-idle',
          text: 'Enter a key above to inspect its features.',
        }));
      }
      e.stopPropagation(); // don't also deselect the node
    }
  });
  row.appendChild(input);
  row.appendChild(el('button', {
    type: 'submit', class: 'btn btn-primary', text: 'Look up',
  }));
  form.appendChild(row);

  section.appendChild(form);

  const result = el('div', { id: 'entity-result', class: 'entity-result' });
  result.appendChild(el('p', {
    class: 'entity-idle',
    text: 'Enter a key above to inspect its features.',
  }));
  section.appendChild(result);

  // Suppress bubbling clicks inside the entity section so background-click
  // deselect doesn't fire when the user interacts with the form.
  section.addEventListener('click', function (e) { e.stopPropagation(); });

  panel.appendChild(section);
}

// ================================================================
// 5. Topology loader
// ================================================================

async function loadTopology() {
  let res;
  try {
    res = await fetch('/debug/topology');
  } catch (e) {
    showTopologyError('Network error: ' + String((e && e.message) || e));
    return;
  }
  if (!res.ok) {
    showTopologyError('HTTP ' + res.status);
    return;
  }

  let data;
  try {
    data = await res.json();
  } catch (_) {
    showTopologyError('Invalid JSON');
    return;
  }

  state.topology = data;

  const svg = d3.select('#topology-svg');
  svg.selectAll('*').remove();

  // Empty topology → centered overlay on the canvas (Pitfall 5 — never
  // display:none the canvas).
  if (!data.nodes || data.nodes.length === 0) {
    drawEmptyState();
    return;
  }

  // Remove any pre-existing empty-state overlay (e.g. from a prior empty
  // render before a REGISTER landed).
  const canvas = document.querySelector('.dag-canvas');
  if (canvas) {
    const old = canvas.querySelector('.empty-state');
    if (old) old.remove();
  }

  const g = new dagreD3.graphlib.Graph()
    .setGraph({ rankdir: 'LR', marginx: 24, marginy: 24, nodesep: 24, ranksep: 48 })
    .setDefaultEdgeLabel(function () { return {}; });

  for (const node of data.nodes) {
    g.setNode(node.name, {
      // dagre-d3 default label is rendered via addTextLabel → SVG <text>
      // with textContent escaping; the 'html' label variant is banned by
      // the forbidden-sink regression test.
      label: node.name,
      class: 'node ' + (node.kind === 'view' ? 'view' : 'stream'),
      rx: 6, ry: 6,
      paddingX: 16, paddingY: 12,
    });
  }

  for (const edge of (data.edges || [])) {
    g.setEdge(edge.from, edge.to, {
      // Right-padded placeholder so dagre-d3 allocates enough bbox width
      // for any realistic formatFps output — Pitfall 9.
      label: '    —/s    ',
      labelpos: 'c',
      arrowhead: 'vee',
      curve: d3.curveBasis,
      'class': 'edgePath edge-' + (edge.kind || 'cascade'),
    });
  }

  const inner = svg.append('g').attr('id', 'dag-root');
  const render = new dagreD3.render();
  render(inner, g); // <-- called exactly ONCE per topology load (Pattern 6)

  // Stamp each edge path with a data-kind attribute for CSS styling hooks.
  svg.selectAll('g.edgePath').each(function () {
    const datum = d3.select(this).datum();
    if (!datum) return;
    for (const te of (data.edges || [])) {
      if (te.from === datum.v && te.to === datum.w) {
        d3.select(this).attr('data-kind', te.kind || 'cascade');
        break;
      }
    }
  });

  // Fit-to-container — same pattern as Phase 10.
  const bbox = inner.node().getBBox();
  const svgNode = document.getElementById('topology-svg');
  const svgW = svgNode.clientWidth || 800;
  const svgH = svgNode.clientHeight || 480;
  const scale = Math.min(
    (svgW - 24) / Math.max(bbox.width, 1),
    (svgH - 24) / Math.max(bbox.height, 1),
    1
  );
  const tx = (svgW - bbox.width * scale) / 2 - bbox.x * scale;
  const ty = (svgH - bbox.height * scale) / 2 - bbox.y * scale;
  inner.attr(
    'transform',
    'translate(' + tx + ',' + ty + ') scale(' + scale + ')'
  );

  // Attach click handlers + a11y — RESEARCH Pattern 1.
  inner.selectAll('g.node').each(function (v) {
    const sel = d3.select(this);
    sel.style('cursor', 'pointer')
      .attr('tabindex', 0)
      .attr('role', 'button')
      .attr('aria-label', v)
      .attr('aria-pressed', 'false')
      .on('click', function (event) {
        event.stopPropagation(); // Pitfall 4 — don't bubble to background
        setSelected(state.selectedStream === v ? null : v);
      })
      .on('keydown', function (event) {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          setSelected(state.selectedStream === v ? null : v);
        }
      });
  });

  // Background click → deselect.
  svg.on('click', function () { setSelected(null); });

  // Kick off the 1 Hz edge-label updater now that the DAG is in the DOM.
  startEdgeLabelUpdater();
}

function drawEmptyState() {
  const canvas = document.querySelector('.dag-canvas');
  if (!canvas) return;
  const old = canvas.querySelector('.empty-state');
  if (old) old.remove();
  // Clear SVG contents but KEEP the <svg> element visible (Pitfall 5).
  d3.select('#topology-svg').selectAll('*').remove();

  const overlay = el('div', { class: 'empty-state' }, [
    makeTallyMarkIcon(),
    el('h2', { text: 'No pipelines registered' }),
    el('p', {
      text: 'Use the Python SDK or REGISTER command to push a pipeline, then reload this page.',
    }),
  ]);
  canvas.appendChild(overlay);
}

function showTopologyError(msg) {
  const canvas = document.querySelector('.dag-canvas');
  if (!canvas) return;
  const old = canvas.querySelector('.empty-state');
  if (old) old.remove();
  d3.select('#topology-svg').selectAll('*').remove();

  const overlay = el('div', { class: 'empty-state error-state' }, [
    el('h2', { text: 'Topology unavailable' }),
    el('p', { text: msg }),
  ]);
  canvas.appendChild(overlay);
}

// ================================================================
// 6. Edge label updater (1 Hz, pause-gated)
// ================================================================

let edgeUpdateHandle = null;

function updateEdgeLabels(throughputData) {
  if (!throughputData || !state.topology) return;
  const rateByName = Object.create(null);
  for (const s of (throughputData.streams || [])) {
    rateByName[s.name] = s.ewma_5s;
  }
  d3.select('#topology-svg').selectAll('g.edgeLabel').each(function () {
    const datum = d3.select(this).datum();
    if (!datum || !datum.w) return;
    const rate = rateByName[datum.w];
    const labelText = formatFps(rate);

    // Update the existing tspan in place — never invoke render() here.
    // Pattern 6: the DAG is rendered exactly once; subsequent ticks only
    // modify text content of existing SVG <tspan> nodes.
    const sel = d3.select(this);
    const tspan = sel.select('g.label').select('text').select('tspan');
    if (!tspan.empty()) {
      tspan.text(labelText);
    } else {
      // Fallback: some dagre-d3 label layouts place text one level up.
      const text = sel.select('text');
      if (!text.empty()) text.text(labelText);
    }
  });
}

async function tickEdgeUpdater() {
  if (state.paused) return; // pause gate — short-circuit BEFORE any fetch
  let res;
  try {
    res = await fetch('/debug/throughput');
  } catch (_) {
    return; // silent — next tick retries
  }
  if (!res.ok) return;
  let data;
  try {
    data = await res.json();
  } catch (_) {
    return;
  }
  if (state.paused) return; // late check: if user paused during the fetch
  state.throughput = data;
  updateEdgeLabels(data);

  // Granular re-render: if a stream is selected, update ONLY the Throughput
  // section — never the entity input (Pitfall 6).
  if (state.selectedStream) {
    const panel = document.getElementById('drill-in-panel');
    const node = findNode(state.selectedStream);
    // WR-02: selected stream vanished from topology (e.g. a future refresh
    // affordance reloaded /debug/topology and the previously-selected stream
    // was DELETEd). Reset to the explicit placeholder instead of silently
    // blanking the panel. Not a live bug today — topology is loaded exactly
    // once in Phase 10.1 — but removes a foot-gun for Phase 10.2's refresh
    // affordance and any test that intentionally swaps topologies.
    if (!node) {
      setSelected(null);
    } else if (panel && node.kind !== 'view') {
      renderThroughputSection(panel, node, data);
    }
  }

  state.lastUpdate = new Date();
  updateFooterLastUpdate();
}

function startEdgeLabelUpdater() {
  if (edgeUpdateHandle != null) return;
  tickEdgeUpdater();
  edgeUpdateHandle = setInterval(tickEdgeUpdater, 1000);
}

// ================================================================
// 7. Memory loader (2 Hz, pause-gated)
// ================================================================

let memoryIntervalHandle = null;

async function tickMemory() {
  if (state.paused) return;
  let res;
  try {
    res = await fetch('/debug/memory');
  } catch (_) {
    return;
  }
  if (!res.ok) return;
  let data;
  try {
    data = await res.json();
  } catch (_) {
    return;
  }
  if (state.paused) return;
  state.memory = data;

  // Granular re-render: if a stream is selected, update ONLY the Memory
  // section via `renderMemorySection` (which swaps the existing
  // [data-section="memory"] node in place). This matches the throughput
  // pattern and is WR-01's fix — never call `renderDrillInPanel` from
  // tickMemory, which would clobber the entity lookup input and any
  // rendered lookup results every 2 s (Pitfall 6).
  if (state.selectedStream) {
    const panel = document.getElementById('drill-in-panel');
    const node = findNode(state.selectedStream);
    // WR-02: selected stream vanished from topology. Reset to placeholder
    // instead of silently blanking the panel.
    if (!node) {
      setSelected(null);
    } else if (panel && node.kind !== 'view') {
      renderMemorySection(panel, node, data);
    }
  }
}

function startMemoryLoop() {
  if (memoryIntervalHandle != null) return;
  tickMemory();
  memoryIntervalHandle = setInterval(tickMemory, 2000);
}

// ================================================================
// 8. Pause controller
// ================================================================

function setPaused(paused) {
  state.paused = !!paused;

  const ctrl = document.querySelector('.poll-control');
  if (ctrl) ctrl.setAttribute('data-paused', String(state.paused));

  const btn = document.getElementById('pause-btn');
  if (btn) {
    btn.textContent = state.paused ? 'Resume' : 'Pause';
    btn.setAttribute('aria-pressed', String(state.paused));
  }

  const label = document.getElementById('poll-label');
  if (label) {
    if (state.paused) {
      label.textContent =
        'Paused · last update ' + (state.lastUpdate ? fmtTime(state.lastUpdate) : '—');
    } else {
      label.textContent = 'Live · 1 Hz';
    }
  }

  const live = document.getElementById('poll-status');
  if (live) live.textContent = state.paused ? 'Polling paused' : 'Polling resumed';
}

function updateFooterLastUpdate() {
  const n = document.getElementById('footer-update');
  if (n) n.textContent = fmtTime(state.lastUpdate);
}

// ================================================================
// 9. Entity lookup (stream-scoped filter)
// ================================================================

async function lookupEntityForSelectedStream(rawKey) {
  const result = document.getElementById('entity-result');
  if (!result) return;
  clear(result);

  const key = (rawKey || '').trim();
  const selectedStream = state.selectedStream;

  if (!key) {
    result.appendChild(el('p', {
      class: 'entity-idle',
      text: 'Enter a key above to inspect its features.',
    }));
    return;
  }
  if (!selectedStream) return;

  const url = '/debug/key/' + encodeURIComponent(key);
  let res;
  try {
    res = await fetch(url);
  } catch (e) {
    result.appendChild(el('div', { class: 'error-state' }, [
      el('h3', { text: 'Lookup failed' }),
      el('p', { text: 'Network error: ' + String((e && e.message) || e) }),
    ]));
    return;
  }

  if (res.status === 404) {
    result.appendChild(el('div', { class: 'empty-state-result' }, [
      el('h3', { text: 'No features for "' + key + '"' }),
      el('p', {
        text: 'This key has not received any events recently, or has been evicted by TTL.',
      }),
    ]));
    return;
  }
  if (!res.ok) {
    result.appendChild(el('div', { class: 'error-state' }, [
      el('h3', { text: 'Lookup failed' }),
      el('p', { text: 'Server returned HTTP ' + res.status }),
    ]));
    return;
  }

  let data;
  try {
    data = await res.json();
  } catch (_) {
    result.appendChild(el('div', { class: 'error-state' }, [
      el('h3', { text: 'Lookup failed' }),
      el('p', { text: 'Invalid JSON response' }),
    ]));
    return;
  }

  // `computed_features` is the authoritative field emitted by
  // StateStore::get_all_features (src/state/store.rs). It is a FLAT map of
  // {feature_name: value} with no stream prefix, because the state store
  // iterates every stream's operators and keys the result by the bare
  // operator name. To scope the result to the currently-selected stream we
  // build an allow-list from the topology node's per-stream `features`
  // array (http.rs:317) and filter the flat map against it. Per CR-01
  // (Phase 10.1 review), v1 callers register unique operator names per
  // stream, so collapse-on-collision is acceptable.
  const computed = (data && data.computed_features) || {};
  const node = findNode(selectedStream);
  const allowed = new Set((node && node.features) || []);
  const filtered = {};
  for (const name of Object.keys(computed)) {
    if (!allowed.has(name)) continue;
    filtered[name] = computed[name];
  }

  if (Object.keys(filtered).length === 0) {
    result.appendChild(el('div', { class: 'empty-state-result' }, [
      el('h3', { text: 'No features for "' + key + '"' }),
      el('p', {
        text: 'Stream "' + selectedStream + '" has not computed features for this key.',
      }),
    ]));
    return;
  }

  const grid = el('div', { class: 'entity-feature-grid' });
  for (const name of Object.keys(filtered)) {
    const value = filtered[name];
    grid.appendChild(el('div', { class: 'entity-feature-cell' }, [
      el('div', { class: 'label', text: name }),
      el('div', { class: 'value', text: formatFeatureValue(value) }),
    ]));
  }
  result.appendChild(grid);
}

// ================================================================
// 10. Bootstrap
// ================================================================

async function loadHealth() {
  try {
    const res = await fetch('/health');
    if (!res.ok) return;
    const data = await res.json();
    state.health = data;
    const v = document.getElementById('footer-version');
    if (v && data && data.version) v.textContent = 'v' + data.version;
    const h = document.getElementById('footer-host');
    if (h) h.textContent = location.host;
  } catch (_) { /* footer is decorative */ }
}

function wireGlobalKeyboard() {
  document.addEventListener('keydown', function (event) {
    const active = document.activeElement;
    const activeIsInput = active && (
      active.tagName === 'INPUT' ||
      active.tagName === 'TEXTAREA' ||
      active.isContentEditable
    );

    // Esc → deselect (but only if no input is focused — inputs use Esc to
    // clear their own value via the handler in renderEntityLookupSection)
    if (event.key === 'Escape') {
      if (activeIsInput) return;
      setSelected(null);
      return;
    }

    // Space → toggle pause (but only if no input is focused; the button
    // handles its own activation so skip when it's focused)
    if (event.key === ' ' || event.code === 'Space') {
      if (activeIsInput) return;
      if (active && active.id === 'pause-btn') return;
      event.preventDefault();
      setPaused(!state.paused);
    }
  });
}

function wirePauseButton() {
  const btn = document.getElementById('pause-btn');
  if (btn) btn.addEventListener('click', function () { setPaused(!state.paused); });
}

function init() {
  wireGlobalKeyboard();
  wirePauseButton();
  loadHealth();
  loadTopology();
  startMemoryLoop();
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}

// Expose a tiny debug surface for smoke testing in the browser console.
window.app = {
  state: state,
  setSelected: setSelected,
  setPaused: setPaused,
  loadTopology: loadTopology,
  lookupEntityForSelectedStream: lookupEntityForSelectedStream,
};
