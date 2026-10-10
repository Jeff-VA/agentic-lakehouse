/*
 * male.js: live charts for dashboards over the platform's views. One file, no build step.
 *
 *   <script type="module" src="/male.js"></script>
 *
 * Each chart tag names a view (config/views/<name>.sql, listed at /views) and the columns
 * to draw. Tags stay live: the page keeps one Server-Sent Events connection to /live, and a
 * tag redraws when its view's result changes. Labels and number formats come from the
 * ontology through the view's columns, so tags rarely need more than names.
 *
 * Tags (attributes in [brackets] are optional)
 *   <male-kpi view value [compare] [compare-label] [label]>
 *       One number from the first row, and its change against the same view with the
 *       `compare` parameters ("from_minutes_ago=120&to_minutes_ago=60").
 *   <male-bars view category value [highlight] [sort] [label]>
 *       A horizontal bar per row, in the view's order (sort="desc" or "asc" to reorder).
 *       highlight="first" | "max" | "min" | <a category> draws that bar in color, the rest gray.
 *   <male-lines view x y [series] [colors] [label] [height]>
 *       A line per `series` value over the time column `x`, with a crosshair, legend and table.
 *   <male-table view [columns] [label]>     The rows as a table; `columns` is a comma list.
 *   <male-text view>Worst: {isp_provider}</male-text>
 *       Text with values from the view's first row, formatted like the charts.
 *   <male-param name options [value] [kind] [label] [view]>
 *       Buttons (or kind="select") that set a page parameter. options="24=24 h,48=48 h" or
 *       "a,b,c"; with `view`, plain options that name its columns show their labels.
 *   <male-freshness [dataset]>  <male-gates>  <male-drift>
 *       The platform's status (from /status): how complete the historical tier is, the
 *       rollups' quality gates, and schema drift waiting for review.
 *   <male-live>      Whether the page is connected, and when data last arrived.
 *   <male-theme>     A light/dark toggle (also ?theme=dark in the URL).
 *
 * Parameters
 *   A page parameter, from the URL's query string or a <male-param>, fills every view
 *   parameter of the same name. A tag's `params` attribute ("hours=24&isp=cox") sets
 *   parameters for that tag alone. In any attribute, $name stands for the page parameter's
 *   value, e.g. y="$metric". Changing a parameter updates the URL, so a link reopens the
 *   same view of the dashboard.
 *
 * Colors follow the entity, not its rank: a series value keeps its color on every chart of
 * the page. Set the order with colors="comcast,verizon,..." on any tag; otherwise values
 * take the six slots in alphabetical order and further ones share a gray.
 *
 * Layout classes: male-grid (responsive columns), male-card, male-section, male-head.
 *
 * Custom code gets the same live data:
 *   import { subscribe } from "/male.js";
 *   subscribe("isp_now", { minutes: 30 }, (result, error) => { ... });  // returns an unsubscribe function
 */

const STYLE = `
:root {
  color-scheme: light;
  --page: #f9f9f7; --surface: #fcfcfb; --text-primary: #0b0b0b; --text-secondary: #52514e; --muted: #898781;
  --grid: #e1e0d9; --baseline: #c3c2b7; --border: rgba(11, 11, 11, 0.10); --hover: rgba(11, 11, 11, 0.04);
  --deemph: #bdbcb4; --other: #a3a29b;
  --series-1: #2a78d6; --series-2: #eb6834; --series-3: #1baf7a; --series-4: #eda100; --series-5: #e87ba4; --series-6: #008300;
  --good: #0ca30c; --warning: #fab219; --critical: #d03b3b; --good-text: #006300; --bad-text: #b42a2a;
}
@media (prefers-color-scheme: dark) {
  :root:where(:not([data-theme="light"])) {
    color-scheme: dark;
    --page: #0d0d0d; --surface: #1a1a19; --text-primary: #ffffff; --text-secondary: #c3c2b7; --muted: #898781;
    --grid: #2c2c2a; --baseline: #383835; --border: rgba(255, 255, 255, 0.10); --hover: rgba(255, 255, 255, 0.05);
    --deemph: #4d4c48; --other: #6b6a65;
    --series-1: #3987e5; --series-2: #d95926; --series-3: #199e70; --series-4: #c98500; --series-5: #d55181; --series-6: #008300;
    --good-text: #0ca30c; --bad-text: #f07a7a;
  }
}
:root[data-theme="dark"] {
  color-scheme: dark;
  --page: #0d0d0d; --surface: #1a1a19; --text-primary: #ffffff; --text-secondary: #c3c2b7; --muted: #898781;
  --grid: #2c2c2a; --baseline: #383835; --border: rgba(255, 255, 255, 0.10); --hover: rgba(255, 255, 255, 0.05);
  --deemph: #4d4c48; --other: #6b6a65;
  --series-1: #3987e5; --series-2: #d95926; --series-3: #199e70; --series-4: #c98500; --series-5: #d55181; --series-6: #008300;
  --good-text: #0ca30c; --bad-text: #f07a7a;
}
:where(body) { margin: 0; background: var(--page); color: var(--text-primary); font: 14px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif; }
:where(main) { display: block; max-width: 1180px; margin: 0 auto; padding: 24px 16px 48px; }
:where(h1) { font-size: 20px; font-weight: 600; margin: 0 0 4px; letter-spacing: -0.01em; }
:where(h2) { font-size: 15px; font-weight: 600; margin: 0; }
:where(code) { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }
:where(a) { color: var(--text-secondary); }
.male-section { margin-top: 28px; }
.male-head { display: flex; align-items: baseline; justify-content: space-between; gap: 12px; margin-bottom: 10px; flex-wrap: wrap; }
.male-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 12px; }
.male-card, male-kpi, male-bars, male-lines, male-table, male-freshness {
  display: block; background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 16px; min-width: 0;
}
male-gates, male-drift, male-text, male-live, male-param, male-theme { display: block; }
male-text, male-live, male-theme { display: inline; }
.male-caption, .male-foot { color: var(--text-secondary); font-size: 13px; }
.male-foot { display: flex; flex-wrap: wrap; align-items: center; gap: 4px 12px; margin-top: 8px; font-size: 12px; color: var(--muted); }
.male-foot .male-notes { flex-basis: 100%; }
.male-foot .male-spacer { flex: 1; }
.male-title { font-size: 13px; font-weight: 600; color: var(--text-secondary); margin: 0 0 8px; display: block; }
.male-muted { color: var(--muted); }
.male-empty { color: var(--text-secondary); font-size: 13px; padding: 8px 0; }
.male-error { color: var(--bad-text); font-size: 13px; padding: 4px 0; display: flex; gap: 8px; align-items: flex-start; }
male-kpi .male-value { font-size: 28px; font-weight: 600; margin-top: 2px; letter-spacing: -0.01em; font-variant-numeric: tabular-nums; }
male-kpi .male-label, male-freshness .male-label { color: var(--text-secondary); font-size: 13px; }
male-freshness .male-value { font-size: 22px; font-weight: 600; margin-top: 4px; }
.male-delta { font-size: 13px; font-weight: 600; margin-top: 2px; }
.male-delta.bad { color: var(--bad-text); } .male-delta.good { color: var(--good-text); } .male-delta.flat { color: var(--text-secondary); }
.male-delta .male-muted { font-weight: 400; }
male-param { display: inline-flex; align-items: center; gap: 6px; flex-wrap: wrap; }
male-param .male-label { color: var(--muted); font-size: 13px; }
.male-buttons { display: inline-flex; gap: 4px; flex-wrap: wrap; }
male-param button, male-theme button, .male-foot button, male-lines .male-legend button {
  font: inherit; font-size: 13px; color: var(--text-primary); background: var(--surface);
  border: 1px solid var(--border); border-radius: 6px; padding: 5px 10px; cursor: pointer;
}
.male-foot button { font-size: 12px; padding: 2px 8px; }
male-param button:hover, male-theme button:hover, .male-foot button:hover { background: var(--hover); }
male-param button[aria-pressed="true"], .male-foot button[aria-pressed="true"] { border-color: var(--text-secondary); font-weight: 600; }
male-param select { font: inherit; font-size: 13px; color: var(--text-primary); background: var(--surface); border: 1px solid var(--border); border-radius: 6px; padding: 5px 8px; }
:where([class^="male"], male-param, male-lines, male-bars, male-theme) :focus-visible { outline: 2px solid var(--series-1); outline-offset: 2px; }
.male-plot svg { display: block; overflow: visible; }
.male-plot svg text { fill: var(--text-secondary); font-size: 12px; }
.male-plot svg .axis { fill: var(--muted); font-variant-numeric: tabular-nums; }
.male-plot svg .name { fill: var(--text-primary); }
.male-plot svg .strong { fill: var(--text-primary); font-weight: 600; }
.male-plot .bar-row:focus { outline: none; }
.male-plot .bar-row:focus-visible rect.hit { stroke: var(--series-1); stroke-width: 2; }
male-lines .male-legend { display: flex; flex-wrap: wrap; gap: 6px 14px; margin: 0 0 8px; padding: 0; list-style: none; }
male-lines .male-legend button { border: none; background: none; padding: 2px 0; display: flex; align-items: center; gap: 6px; }
male-lines .male-legend button[aria-pressed="false"] { color: var(--muted); }
male-lines .male-legend button[aria-pressed="false"] .male-key { opacity: 0.3; }
.male-key { display: inline-block; width: 14px; height: 2px; border-radius: 1px; }
.male-table-wrap { max-height: 320px; overflow: auto; margin-top: 8px; }
.male-table-wrap table, male-table table { border-collapse: collapse; width: 100%; font-size: 13px; }
.male-table-wrap th, .male-table-wrap td, male-table th, male-table td { text-align: right; padding: 4px 8px; border-bottom: 1px solid var(--grid); font-variant-numeric: tabular-nums; }
.male-table-wrap th:first-child, .male-table-wrap td:first-child, male-table th:first-child, male-table td:first-child { text-align: left; }
.male-table-wrap th, male-table th { color: var(--text-secondary); font-weight: 600; position: sticky; top: 0; background: var(--surface); }
male-table .male-table-wrap { max-height: 420px; margin-top: 0; }
.male-status { display: inline-flex; align-items: center; gap: 6px; font-size: 13px; font-weight: 600; }
.male-icon { width: 16px; height: 16px; flex: none; }
.male-checks { list-style: none; padding: 0; margin: 8px 0 0; }
.male-checks li { display: flex; gap: 8px; align-items: flex-start; padding: 2px 0; font-size: 13px; }
.male-checks .what { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }
.male-item { padding: 12px 0; border-top: 1px solid var(--grid); }
.male-item:first-child { border-top: none; padding-top: 4px; }
.male-item-head { display: flex; justify-content: space-between; gap: 8px; flex-wrap: wrap; }
.male-item-name { font-weight: 600; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 13px; }
.male-item p { margin: 4px 0 0; }
male-live { color: var(--muted); font-size: 13px; }
male-live .male-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; vertical-align: 1px; background: var(--muted); }
male-live[data-state="live"] .male-dot { background: var(--good); }
male-live[data-state="reconnecting"] .male-dot { background: var(--warning); }
.male-stale { opacity: 0.55; transition: opacity 120ms; }
.male-tooltip {
  position: fixed; pointer-events: none; z-index: 10; min-width: 140px; max-width: 280px;
  background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
  box-shadow: 0 4px 16px rgba(0, 0, 0, 0.12); padding: 8px 10px; font-size: 12px; display: none; color: var(--text-primary);
}
.male-tooltip .title { color: var(--text-secondary); margin-bottom: 4px; }
.male-tooltip .row { display: flex; align-items: center; gap: 8px; }
.male-tooltip .row .v { font-weight: 600; font-variant-numeric: tabular-nums; min-width: 56px; }
.male-tooltip .row .n { color: var(--text-secondary); }
@media (prefers-reduced-motion: reduce) { .male-stale { transition: none; } }
`;

// ---------------------------------------------------------------- small helpers

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (value == null) continue;
    if (key === "class") node.className = value;
    else if (key === "text") node.textContent = value;
    else node.setAttribute(key, value);
  }
  for (const child of children) if (child != null) node.append(child);
  return node;
}

const SVG = "http://www.w3.org/2000/svg";
function svgEl(tag, attrs = {}, text) {
  const node = document.createElementNS(SVG, tag);
  for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, value);
  if (text != null) node.textContent = text;
  return node;
}

function icon(kind) {
  // Status colors always come with an icon and a label, never alone.
  const svg = svgEl("svg", { class: "male-icon", viewBox: "0 0 16 16", "aria-hidden": "true" });
  const fills = { good: "var(--good)", warn: "var(--warning)", bad: "var(--critical)", none: "var(--muted)" };
  svg.append(svgEl("circle", { cx: 8, cy: 8, r: 7, fill: fills[kind] }));
  const paths = { good: "M4.5 8.2l2.2 2.2 4.8-4.8", bad: "M5.2 5.2l5.6 5.6M10.8 5.2l-5.6 5.6", warn: "M8 4.2v4.6M8 11.2v.6", none: "M5 8h6" };
  svg.append(svgEl("path", { d: paths[kind], stroke: kind === "warn" ? "#0b0b0b" : "#ffffff", "stroke-width": 1.8, fill: "none", "stroke-linecap": "round", "stroke-linejoin": "round" }));
  return svg;
}

const MONTH_DAY = { month: "short", day: "numeric", timeZone: "UTC" };
function utcDay(t) { return new Date(t).toLocaleDateString("en-US", MONTH_DAY); }
function utcTime(t, seconds = false) { return new Date(t).toISOString().slice(11, seconds ? 19 : 16); }
function ago(iso) {
  const minutes = Math.round((Date.now() - new Date(iso)) / 60000);
  if (minutes < 1) return "just now";
  if (minutes < 120) return `${minutes} min ago`;
  const hours = Math.round(minutes / 60);
  return hours < 48 ? `${hours} h ago` : `${Math.round(hours / 24)} days ago`;
}
function signed(v, digits) { return (v > 0 ? "+" : v < 0 ? "−" : "±") + Math.abs(v).toFixed(digits); }

// ---------------------------------------------------------------- formats, from the view's columns

function plainNumber(v) {
  const size = Math.abs(v);
  return v.toLocaleString("en-US", { maximumFractionDigits: size >= 100 ? 0 : size >= 10 ? 1 : 2 });
}

function formatter(column) {
  const missing = "—";
  if (!column) return v => (v == null ? missing : String(v));
  if (column.type === "timestamp" || column.type === "date") {
    const daily = ["day", "week", "month"].includes(column.grain);
    return v => (v == null ? missing : daily ? utcDay(v) : `${utcDay(v)} ${utcTime(v)}`);
  }
  if (column.type === "boolean") return v => (v == null ? missing : v ? "Yes" : "No");
  if (column.type !== "number") return v => (v == null ? missing : String(v));
  const unit = column.unit ? ` ${column.unit}` : "";
  if (column.format === "percent") return v => (v == null ? missing : `${(v * 100).toFixed(1)}%`);
  if (column.format === "integer") return v => (v == null ? missing : `${Math.round(v).toLocaleString("en-US")}${unit}`);
  return v => (v == null ? missing : `${plainNumber(v)}${unit}`);
}

function axisFormatter(column, step) {
  if (column?.format === "percent") return v => `${(v * 100).toFixed(step * 100 < 1 ? 1 : 0)}%`;
  const digits = step >= 1 ? 0 : step >= 0.1 ? 1 : 2;
  return v => v.toLocaleString("en-US", { maximumFractionDigits: digits });
}

function titleOf(column) {
  if (!column) return "";
  return column.unit && column.format !== "percent" ? `${column.label} (${column.unit})` : column.label;
}

function niceTicks(max, count = 4) {
  if (!(max > 0)) return [0, 1];
  const raw = max / count;
  const magnitude = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 2.5, 5, 10].map(m => m * magnitude).find(s => s >= raw);
  const ticks = [];
  for (let v = 0; v <= max + step * 0.001; v += step) ticks.push(+v.toFixed(10));
  if (ticks[ticks.length - 1] < max) ticks.push(+(ticks[ticks.length - 1] + step).toFixed(10));
  return ticks;
}

// ---------------------------------------------------------------- colors: one entity, one color, page-wide

const SLOTS = 6;  // the validated categorical palette; more values share --other
const slots = new Map();
function registerColors(values) {
  for (const value of values) if (value !== "" && !slots.has(value) && slots.size < SLOTS) slots.set(value, slots.size + 1);
}
function colorOf(value) {
  registerColors([String(value)]);
  return slots.has(String(value)) ? `var(--series-${slots.get(String(value))})` : "var(--other)";
}

// ---------------------------------------------------------------- tooltip

let tooltip;
function showTip(x, y, title, rows) {
  tooltip ??= document.body.appendChild(el("div", { class: "male-tooltip", role: "tooltip" }));
  tooltip.replaceChildren(el("div", { class: "title", text: title }));
  for (const row of rows) {
    const key = el("span", { class: "male-key" });
    key.style.background = row.color ?? "transparent";
    tooltip.append(el("div", { class: "row" }, key, el("span", { class: "v", text: row.value }), el("span", { class: "n", text: row.name })));
  }
  tooltip.style.display = "block";
  const { width, height } = tooltip.getBoundingClientRect();
  tooltip.style.left = `${Math.min(x + 14, window.innerWidth - width - 8)}px`;
  tooltip.style.top = `${Math.max(8, Math.min(y + 14, window.innerHeight - height - 8))}px`;
}
function hideTip() { if (tooltip) tooltip.style.display = "none"; }

// ---------------------------------------------------------------- page parameters

const page = {
  values: new Map(),
  defaults: new Map(),
  get(name) { return this.values.get(name) ?? this.defaults.get(name) ?? null; },
  set(name, value) {
    if (value === this.defaults.get(name)) this.values.delete(name);
    else this.values.set(name, value);
    const url = new URL(location.href);
    url.searchParams.delete(name);
    if (this.values.has(name)) url.searchParams.set(name, value);
    history.replaceState(null, "", url);
    for (const tag of tags) tag.refresh();
  },
};
for (const [name, value] of new URLSearchParams(location.search)) if (name !== "theme") page.values.set(name, value);

function substitute(text) {
  return text.replace(/\$([a-z][a-z0-9_]*)/g, (_, name) => page.get(name) ?? "");
}

// ---------------------------------------------------------------- the view catalog

let catalogPromise;
function catalog() {
  catalogPromise ??= fetch("/views")
    .then(response => (response.ok ? response.json() : Promise.reject(new Error(`/views: HTTP ${response.status}`))))
    .then(body => new Map(body.views.map(view => [view.name, view])))
    .catch(error => { catalogPromise = null; throw error; });
  return catalogPromise;
}

function subscriptionKey(view, extra) {
  // The view's name, then its parameters as a query string: what /live takes as a `view` value.
  const params = new URLSearchParams();
  for (const param of view.params) {
    const value = page.get(param.name);
    if (value != null && value !== "") params.set(param.name, value);
  }
  for (const [key, value] of new URLSearchParams(extra ?? "")) params.set(key, value);
  params.sort();
  const query = params.toString();
  return query ? `${view.name}?${query}` : view.name;
}

// ---------------------------------------------------------------- the live connection

class Live {
  constructor() {
    this.subs = new Map();  // key -> { callbacks, result, error }
    this.source = null;
    this.state = "connecting";
    this.lastEvent = null;
    this.watchers = new Set();
  }

  subscribe(key, callback) {
    let sub = this.subs.get(key);
    if (!sub) {
      sub = { callbacks: new Set(), result: null, error: null };
      this.subs.set(key, sub);
      this.reconnectSoon();
    }
    sub.callbacks.add(callback);
    if (sub.result || sub.error) callback(sub);
    return () => {
      sub.callbacks.delete(callback);
      if (!sub.callbacks.size) {
        this.subs.delete(key);
        this.reconnectSoon();
      }
    };
  }

  reconnectSoon() {
    clearTimeout(this.timer);
    this.timer = setTimeout(() => this.connect(), 30);  // one reconnect for a burst of changes
  }

  connect() {
    this.source?.close();
    this.source = null;
    const keys = [...this.subs.keys()].filter(key => !this.subs.get(key).invalid);
    if (!keys.length) return this.setState(this.subs.size ? "offline" : "idle");
    const source = new EventSource(`/live?${keys.map(key => `view=${encodeURIComponent(key)}`).join("&")}`);
    this.source = source;
    source.onopen = () => this.setState("live");
    source.addEventListener("result", event => this.receive(JSON.parse(event.data), "result"));
    source.addEventListener("error", event => {
      if (event.data) return this.receive(JSON.parse(event.data), "error");  // a view failed to refresh
      if (source.readyState === EventSource.CLOSED) this.diagnose(keys);  // the server refused the request
      else this.setState("reconnecting");  // EventSource retries on its own
    });
  }

  receive(message, kind) {
    const sub = this.subs.get(message.subscription);
    this.lastEvent = new Date();
    this.setState("live");
    if (!sub) return;
    if (kind === "result") { sub.result = message.result; sub.error = null; }
    else sub.error = message.detail;
    for (const callback of sub.callbacks) callback(sub);
  }

  async diagnose(keys) {
    // A 400 or 404 closes the stream without saying which view caused it: ask each one.
    this.setState("offline");
    let found = false;
    for (const key of keys) {
      const [name, query] = key.split(/\?(.*)/s);
      const response = await fetch(`/views/${encodeURIComponent(name)}${query ? `?${query}` : ""}`).catch(() => null);
      if (response && (response.status === 400 || response.status === 404)) {
        const sub = this.subs.get(key);
        if (!sub) continue;
        sub.invalid = true;
        sub.error = (await response.json().catch(() => ({}))).detail ?? `HTTP ${response.status}`;
        for (const callback of sub.callbacks) callback(sub);
        found = true;
      }
    }
    setTimeout(() => this.connect(), found ? 0 : 5000);
  }

  setState(state) {
    this.state = state;
    for (const watcher of this.watchers) watcher();
  }
}
const live = new Live();

// ---------------------------------------------------------------- platform status (/status), polled

const status = {
  callbacks: new Set(),
  last: null,
  subscribe(callback) {
    this.callbacks.add(callback);
    if (this.last) callback(this.last);
    if (!this.timer) { this.poll(); this.timer = setInterval(() => this.poll(), 30000); }
    return () => {
      this.callbacks.delete(callback);
      if (!this.callbacks.size) { clearInterval(this.timer); this.timer = null; }
    };
  },
  async poll() {
    try {
      const response = await fetch("/status");
      this.last = response.ok ? { status: await response.json() } : { error: `/status: HTTP ${response.status}` };
    } catch (error) {
      this.last = { error: `/status: ${error.message}` };
    }
    for (const callback of this.callbacks) callback(this.last);
  },
};

// ---------------------------------------------------------------- tags that draw a view

const tags = new Set();

class ViewTag extends HTMLElement {
  connectedCallback() {
    if (this.getAttribute("colors")) registerColors(this.getAttribute("colors").split(",").map(s => s.trim()));
    tags.add(this);
    this.started = true;
    this.refresh();
  }

  disconnectedCallback() {
    tags.delete(this);
    this.started = false;
    this.leave();
  }

  attributeChangedCallback() {
    if (this.started) this.refresh();  // not while the element is being set up
  }

  attr(name, fallback = null) {
    const raw = this.getAttribute(name);
    return raw == null ? fallback : substitute(raw);
  }

  leave() {
    for (const off of this.offs ?? []) off();
    this.offs = [];
    this.keys = [];
  }

  // The parameter sets this tag watches; a KPI also watches its comparison.
  subscriptionsFor(view) {
    return [subscriptionKey(view, this.attr("params"))];
  }

  async refresh() {
    const name = this.attr("view");
    let views;
    try {
      views = await catalog();
    } catch (error) {
      return this.paint(null, `Couldn't load the view list: ${error.message}`);
    }
    const view = views.get(name);
    if (!view) {
      const known = [...views.keys()].join(", ") || "none yet";
      return this.paint(null, name ? `No view named "${name}". Views: ${known}.` : "Set the view attribute to a view's name.");
    }
    const keys = this.subscriptionsFor(view);
    this.view = view;
    if (keys.join("\n") === (this.keys ?? []).join("\n")) return this.draw();
    const previous = this.states;
    this.leave();
    this.keys = keys;
    this.states = keys.map(() => null);
    this.previous = previous;  // shown dimmed until the new parameters' data arrives
    this.painted = false;
    this.offs = keys.map((key, i) => live.subscribe(key, sub => {
      this.states[i] = { result: sub.result, error: sub.error };
      if (i === 0) this.previous = null;
      this.draw();
    }));
    this.draw();
  }

  draw() {
    const stale = !this.states?.[0] && this.previous?.[0];
    this.classList.toggle("male-stale", !!stale);
    const state = this.states?.[0] ?? (stale ? this.previous[0] : null);
    if (!state) return this.paint(null, null);
    if (stale) {
      const painted = this.painted;
      this.paint(state.result, state.error);
      this.painted = painted;  // the new data hasn't arrived yet
      return;
    }
    this.paint(state.result, state.error);
  }

  // Shared frame: the body a tag renders, then freshness, notes and errors underneath.
  paint(result, error) {
    hideTip();
    if (result || error) this.painted = true;
    const body = el("div", { class: "male-body" });
    if (!result) {
      body.append(error ? el("div", { class: "male-error", role: "alert" }, icon("bad"), el("span", { text: error })) : el("div", { class: "male-empty", text: "Loading…" }));
      this.replaceChildren(body);
      return;
    }
    if (!result.rows.length) body.append(el("div", { class: "male-empty", text: this.attr("empty", "No rows yet.") }));
    else this.render(body, result);
    const foot = this.foot(result, error);
    this.replaceChildren(...[this.head?.(result), body, foot].filter(Boolean));
  }

  foot(result, error) {
    const foot = el("div", { class: "male-foot" });
    if (result.coverage) foot.append(el("span", { text: `Live · as of ${utcTime(result.computed_at, true)} UTC` }));
    else if (result.data_through) foot.append(el("span", { text: `Complete through ${utcDay(result.data_through)} ${utcTime(result.data_through)} UTC` }));
    foot.append(el("span", { class: "male-spacer" }));
    if (this.tableToggle && result.rows.length) {
      const button = el("button", { type: "button", "aria-pressed": String(!!this.showTable), text: "Table" });
      button.addEventListener("click", () => { this.showTable = !this.showTable; this.draw(); });
      foot.append(button);
    }
    if (result.notes.length) foot.append(el("div", { class: "male-notes", text: result.notes.join(" ") }));
    if (error) foot.append(el("div", { class: "male-notes male-error", role: "alert" }, icon("warn"), el("span", { text: `Couldn't refresh: ${error}` })));
    return foot;
  }

  column(name) {
    return this.view?.columns.find(c => c.name === name);
  }
}

function table(columns, rows) {
  const formats = columns.map(formatter);
  return el("div", { class: "male-table-wrap" }, el("table", {},
    el("thead", {}, el("tr", {}, ...columns.map(c => el("th", { scope: "col", text: c.label })))),
    el("tbody", {}, ...rows.map(row => el("tr", {}, ...columns.map((c, i) => el("td", { text: formats[i](row[c.name]) }))))),
  ));
}

// <male-kpi view value [compare] [compare-label] [label]>
class KpiTag extends ViewTag {
  static observedAttributes = ["view", "value", "compare", "compare-label", "label", "params"];

  subscriptionsFor(view) {
    const keys = [subscriptionKey(view, this.attr("params"))];
    const compare = this.attr("compare");
    if (compare) {
      const params = new URLSearchParams(this.attr("params") ?? "");
      for (const [key, value] of new URLSearchParams(compare)) params.set(key, value);
      keys.push(subscriptionKey(view, params.toString()));
    }
    return keys;
  }

  paint(result, error) {
    if (result || error) this.painted = true;
    const column = this.column(this.attr("value"));
    const label = el("div", { class: "male-label", text: this.attr("label") ?? column?.label ?? this.attr("value") ?? "" });
    if (!result) {
      const body = error ? el("div", { class: "male-error", role: "alert" }, icon("bad"), el("span", { text: error })) : el("div", { class: "male-value male-muted", text: "…" });
      return this.replaceChildren(label, body);
    }
    if (!column) return this.replaceChildren(label, el("div", { class: "male-error", text: `View "${this.view.name}" has no column "${this.attr("value")}".` }));
    const value = result.rows[0]?.[column.name];
    const before = (this.states[1] ?? this.previous?.[1])?.result?.rows?.[0]?.[column.name];
    const parts = [label, el("div", { class: "male-value", text: formatter(column)(value) })];
    if (value != null && before != null) parts.push(this.delta(column, value, before));
    if (error) parts.push(el("div", { class: "male-foot male-error", role: "alert", text: `Couldn't refresh: ${error}` }));
    this.replaceChildren(...parts);
  }

  delta(column, value, before) {
    let text;
    if (column.format === "percent") text = `${signed((value - before) * 100, 1)} pt`;
    else if (column.unit) text = `${signed(value - before, column.format === "integer" ? 0 : 1)} ${column.unit}`;
    else text = before ? `${signed(((value - before) / Math.abs(before)) * 100, 0)}%` : "new";
    const kind = value === before || !column.better ? "flat" : (value > before) === (column.better === "higher") ? "good" : "bad";
    const words = { good: "better", bad: "worse", flat: "" }[kind];
    const delta = el("div", { class: `male-delta ${kind}`, text });
    const tail = [words && `(${words})`, this.attr("compare-label")].filter(Boolean).join(" ");
    if (tail) delta.append(el("span", { class: "male-muted", text: ` ${tail}` }));
    return delta;
  }
}

// <male-bars view category value [highlight] [sort] [label]>
class BarsTag extends ViewTag {
  static observedAttributes = ["view", "category", "value", "highlight", "sort", "label", "params"];
  tableToggle = true;

  head() {
    return el("span", { class: "male-title", text: this.attr("label") ?? titleOf(this.column(this.attr("value"))) });
  }

  render(body, result) {
    const category = this.column(this.attr("category"));
    const measure = this.column(this.attr("value"));
    if (!category || !measure) return body.append(el("div", { class: "male-error", text: `View "${this.view.name}" has no column "${!category ? this.attr("category") : this.attr("value")}".` }));
    let rows = [...result.rows];
    const sort = this.attr("sort");
    if (sort === "desc" || sort === "asc") rows.sort((a, b) => ((a[measure.name] ?? -Infinity) - (b[measure.name] ?? -Infinity)) * (sort === "asc" ? 1 : -1));
    const highlight = this.highlighted(rows, category, measure);
    if (this.showTable) return body.append(table(this.view.columns, rows));
    const format = formatter(measure);
    const width = Math.max(200, this.clientWidth - 32);
    const rowH = 28, barH = 14, labelW = Math.min(120, Math.max(60, 8 * Math.max(...rows.map(r => String(r[category.name]).length)))), valueW = 76;
    const plotW = Math.max(40, width - labelW - valueW);
    const max = Math.max(...rows.map(r => r[measure.name] ?? 0)) || 1;
    const svg = svgEl("svg", { width, height: rows.length * rowH + 4, role: "list", "aria-label": `${measure.label} by ${category.label}` });
    svg.append(svgEl("line", { x1: labelW, x2: labelW, y1: 0, y2: rows.length * rowH, stroke: "var(--baseline)", "stroke-width": 1 }));
    const numbers = this.view.columns.filter(c => c.type === "number");
    rows.forEach((row, i) => {
      const name = String(row[category.name]);
      const value = row[measure.name];
      const strong = highlight != null && name === highlight;
      const y = i * rowH + (rowH - barH) / 2;
      const g = svgEl("g", { class: "bar-row", tabindex: 0, role: "listitem", "aria-label": `${name}: ${format(value)}` });
      g.append(svgEl("rect", { class: "hit", x: 0, y: i * rowH, width, height: rowH, fill: "transparent" }));
      g.append(svgEl("text", { x: labelW - 8, y: y + barH / 2 + 4, "text-anchor": "end", class: strong ? "strong" : "name" }, name));
      if (value != null) {
        const x1 = Math.max(labelW + (value / max) * plotW, labelW + 1);
        const fill = highlight == null ? "var(--series-1)" : strong ? colorOf(name) : "var(--deemph)";
        g.append(svgEl("path", { d: roundedBar(labelW, y, x1, barH, 4), fill }));
        g.append(svgEl("text", { x: x1 + 6, y: y + barH / 2 + 4, class: strong ? "strong" : "" }, format(value)));
      }
      const show = (x, yy) => showTip(x, yy, name, numbers.map(c => ({ color: colorOf(name), name: c.label, value: formatter(c)(row[c.name]) })));
      g.addEventListener("pointermove", e => show(e.clientX, e.clientY));
      g.addEventListener("pointerleave", hideTip);
      g.addEventListener("focus", () => { const b = g.getBoundingClientRect(); show(b.right - 40, b.top); });
      g.addEventListener("blur", hideTip);
      svg.append(g);
    });
    body.append(el("div", { class: "male-plot" }, svg));
  }

  highlighted(rows, category, measure) {
    const mode = this.attr("highlight");
    if (!mode || mode === "none" || !rows.length) return null;
    const present = rows.filter(r => r[measure.name] != null);
    if (mode === "first") return String(rows[0][category.name]);
    if ((mode === "max" || mode === "min") && present.length) {
      const pick = present.reduce((a, b) => ((mode === "max" ? b[measure.name] > a[measure.name] : b[measure.name] < a[measure.name]) ? b : a));
      return String(pick[category.name]);
    }
    return mode;
  }
}

function roundedBar(x0, y, x1, h, r) {
  const w = x1 - x0;
  if (w <= 0) return "";
  r = Math.min(r, w, h / 2);
  return `M${x0},${y} H${x1 - r} Q${x1},${y} ${x1},${y + r} V${y + h - r} Q${x1},${y + h} ${x1 - r},${y + h} H${x0} Z`;
}

// <male-lines view x y [series] [colors] [label] [height]>
const GRAIN_MS = { minute: 60e3, hour: 3600e3, day: 86400e3, week: 7 * 86400e3 };

class LinesTag extends ViewTag {
  static observedAttributes = ["view", "x", "y", "series", "label", "params", "height", "colors"];
  tableToggle = true;
  hidden = new Set();

  head(result) {
    const title = el("span", { class: "male-title", text: this.attr("label") ?? titleOf(this.column(this.attr("y"))) });
    const series = this.column(this.attr("series"));
    if (!series || !result.rows.length) return title;
    const values = this.seriesValues(result.rows, series);
    const legend = el("ul", { class: "male-legend", "aria-label": series.label });
    for (const value of values) {
      const key = el("span", { class: "male-key" });
      key.style.background = colorOf(value);
      const button = el("button", { type: "button", "aria-pressed": String(!this.hidden.has(value)), title: `Show or hide ${value}` }, key, el("span", { text: value }));
      button.addEventListener("click", () => { this.hidden.has(value) ? this.hidden.delete(value) : this.hidden.add(value); this.draw(); });
      legend.append(el("li", {}, button));
    }
    return el("div", {}, title, legend);
  }

  seriesValues(rows, series) {
    const values = [...new Set(rows.map(r => String(r[series.name])))].sort();
    registerColors(values);  // alphabetical, after any colors="..." order
    return values.sort((a, b) => (slots.get(a) ?? 99) - (slots.get(b) ?? 99) || a.localeCompare(b));
  }

  render(body, result) {
    const x = this.column(this.attr("x")), y = this.column(this.attr("y")), series = this.column(this.attr("series"));
    const missing = [["x", x], ["y", y]].filter(([, c]) => !c).map(([a]) => this.attr(a));
    if (this.attr("series") && !series) missing.push(this.attr("series"));
    if (missing.length) return body.append(el("div", { class: "male-error", text: `View "${this.view.name}" has no column "${missing[0]}".` }));
    const rows = result.rows.filter(r => r[x.name] != null);
    const groups = series ? this.seriesValues(rows, series) : [""];
    const times = [...new Set(rows.map(r => Date.parse(r[x.name])))].sort((a, b) => a - b);
    if (this.showTable) return body.append(this.pivot(rows, x, y, series, groups, times));

    // A slot for every bucket from first to last, so a missing one breaks the line instead of bridging it.
    let step = GRAIN_MS[x.grain];
    if (!step) step = times.slice(1).reduce((min, t, i) => Math.min(min, t - times[i]), Infinity);
    const buckets = [];
    if (Number.isFinite(step) && step > 0 && (times.at(-1) - times[0]) / step <= 5000) for (let t = times[0]; t <= times.at(-1); t += step) buckets.push(t);
    else buckets.push(...times);
    const values = new Map(groups.map(g => [g, new Map()]));
    for (const r of rows) values.get(series ? String(r[series.name]) : "")?.set(Date.parse(r[x.name]), r[y.name]);
    const visible = groups.filter(g => !this.hidden.has(g));

    const width = Math.max(280, this.clientWidth - 32);
    const plotH = Number(this.attr("height", "240")), m = { left: 52, right: 16, top: 10, bottom: 28 };
    const height = plotH + m.top + m.bottom;
    const max = Math.max(0, ...visible.flatMap(g => [...values.get(g).values()].filter(v => v != null)));
    const ticks = niceTicks(max || 1);
    const top = ticks.at(-1);
    const axis = axisFormatter(y, ticks[1] - ticks[0]);
    const format = formatter(y);
    const pitch = buckets.length > 1 ? (width - m.left - m.right) / (buckets.length - 1) : 0;
    const px = i => m.left + i * pitch;
    const py = v => m.top + plotH - (v / top) * plotH;
    const color = g => (series ? colorOf(g) : "var(--series-1)");

    const svg = svgEl("svg", { width, height, role: "img", "aria-label": `${y.label}${series ? ` by ${series.label}` : ""}; the table view lists every value` });
    for (const t of ticks) {
      svg.append(svgEl("line", { x1: m.left, x2: width - m.right, y1: py(t), y2: py(t), stroke: t === 0 ? "var(--baseline)" : "var(--grid)", "stroke-width": 1 }));
      svg.append(svgEl("text", { x: m.left - 8, y: py(t) + 4, "text-anchor": "end", class: "axis" }, axis(t)));
    }
    const daily = step >= 86400e3;
    const every = Math.max(1, Math.ceil(64 / Math.max(pitch, 1)));
    buckets.forEach((t, i) => {
      if (i % every) return;
      const d = new Date(t);
      const label = daily || d.getUTCHours() + d.getUTCMinutes() === 0 || i === 0 ? utcDay(t) : utcTime(t);
      svg.append(svgEl("text", { x: px(i), y: height - 8, "text-anchor": "middle", class: "axis" }, label));
    });
    for (const g of visible) {
      let d = "", pen = false, last = null;
      buckets.forEach((t, i) => {
        const v = values.get(g).get(t);
        if (v == null) { pen = false; return; }
        d += `${pen ? "L" : "M"}${px(i).toFixed(1)},${py(v).toFixed(1)}`;
        pen = true;
        last = [px(i), py(v)];
      });
      svg.append(svgEl("path", { d, fill: "none", stroke: color(g), "stroke-width": 2, "stroke-linejoin": "round", "stroke-linecap": "round" }));
      if (last) svg.append(svgEl("circle", { cx: last[0], cy: last[1], r: 4, fill: color(g), stroke: "var(--surface)", "stroke-width": 2 }));
    }

    // Crosshair: snaps to the nearest bucket; arrow keys move it too.
    const cross = svgEl("line", { y1: m.top, y2: m.top + plotH, stroke: "var(--text-secondary)", "stroke-width": 1, visibility: "hidden" });
    const dots = svgEl("g", { visibility: "hidden" });
    const overlay = svgEl("rect", { x: m.left - pitch / 2, y: m.top, width: width - m.left - m.right + pitch, height: plotH, fill: "transparent", tabindex: 0, "aria-label": "Use the left and right arrow keys to read values" });
    svg.append(cross, dots, overlay);
    let index = buckets.length - 1;
    const point = (i, clientX, clientY) => {
      index = Math.max(0, Math.min(buckets.length - 1, i));
      cross.setAttribute("x1", px(index)); cross.setAttribute("x2", px(index)); cross.setAttribute("visibility", "visible");
      dots.replaceChildren(); dots.setAttribute("visibility", "visible");
      const entries = visible.map(g => ({ g, v: values.get(g).get(buckets[index]) })).filter(e => e.v != null).sort((a, b) => b.v - a.v);
      for (const e of entries) dots.append(svgEl("circle", { cx: px(index), cy: py(e.v), r: 4, fill: color(e.g), stroke: "var(--surface)", "stroke-width": 2 }));
      const when = daily ? utcDay(buckets[index]) : `${utcDay(buckets[index])} ${utcTime(buckets[index])} UTC`;
      showTip(clientX, clientY, when, entries.length ? entries.map(e => ({ color: color(e.g), name: e.g || y.label, value: format(e.v) })) : [{ name: "no data", value: "—" }]);
    };
    const leave = () => { cross.setAttribute("visibility", "hidden"); dots.setAttribute("visibility", "hidden"); hideTip(); };
    overlay.addEventListener("pointermove", e => { const box = svg.getBoundingClientRect(); point(Math.round((e.clientX - box.left - m.left) / (pitch || 1)), e.clientX, e.clientY); });
    overlay.addEventListener("pointerleave", leave);
    overlay.addEventListener("blur", leave);
    overlay.addEventListener("focus", () => { const b = svg.getBoundingClientRect(); point(index, b.left + px(index), b.top + m.top); });
    overlay.addEventListener("keydown", e => {
      if (e.key !== "ArrowLeft" && e.key !== "ArrowRight") return;
      e.preventDefault();
      const b = svg.getBoundingClientRect();
      const next = Math.max(0, Math.min(buckets.length - 1, index + (e.key === "ArrowRight" ? 1 : -1)));
      point(next, b.left + px(next), b.top + m.top);
    });
    body.append(el("div", { class: "male-plot" }, svg));
  }

  pivot(rows, x, y, series, groups, times) {
    const value = new Map(rows.map(r => [`${Date.parse(r[x.name])}|${series ? r[series.name] : ""}`, r[y.name]]));
    const format = formatter(y), when = formatter(x);
    const head = el("tr", {}, el("th", { scope: "col", text: `${x.label} (UTC)` }), ...groups.map(g => el("th", { scope: "col", text: g || y.label })));
    const body = [...times].reverse().map(t => el("tr", {}, el("td", { text: when(t) }), ...groups.map(g => el("td", { text: format(value.get(`${t}|${g}`)) }))));
    return el("div", { class: "male-table-wrap" }, el("table", {}, el("caption", { class: "male-muted", text: titleOf(y) }), el("thead", {}, head), el("tbody", {}, ...body)));
  }
}

// <male-table view [columns] [label]>
class TableTag extends ViewTag {
  static observedAttributes = ["view", "columns", "label", "params"];

  head() {
    const label = this.attr("label");
    return label ? el("span", { class: "male-title", text: label }) : null;
  }

  render(body, result) {
    const names = (this.attr("columns") ?? "").split(",").map(s => s.trim()).filter(Boolean);
    const columns = names.length ? names.map(name => this.column(name) ?? { name, label: name, type: "string" }) : this.view.columns;
    body.append(table(columns, result.rows));
  }
}

// <male-text view>Worst right now: {isp_provider}</male-text>
class TextTag extends ViewTag {
  static observedAttributes = ["view", "params"];

  connectedCallback() {
    this.template ??= this.innerHTML;
    super.connectedCallback();
  }

  paint(result, error) {
    if (result || error) this.painted = true;
    if (!result) {
      if (error) this.replaceChildren(el("span", { class: "male-error", text: `(${error})` }));
      else this.textContent = "";
      return;
    }
    const row = result.rows[0];
    if (!row) return (this.textContent = this.attr("empty", ""));
    const escape = s => s.replace(/[&<>"']/g, ch => `&#${ch.charCodeAt(0)};`);
    this.innerHTML = this.template.replace(/\{([a-z][a-z0-9_]*)\}/g, (match, name) => {
      const column = this.column(name);
      return column ? escape(formatter(column)(row[name])) : match;
    });
  }
}

// ---------------------------------------------------------------- controls

// <male-param name options [value] [kind] [label] [view]>
class ParamTag extends HTMLElement {
  connectedCallback() {
    tags.add(this);
    this.refresh();
  }

  disconnectedCallback() { tags.delete(this); }

  options() {
    return (this.getAttribute("options") ?? "").split(",").map(s => s.trim()).filter(Boolean).map(option => {
      const [value, label] = option.split("=");
      return { value: value.trim(), label: label?.trim() };
    });
  }

  async refresh() {
    const name = this.getAttribute("name");
    const options = this.options();
    const current = page.get(name);
    let columns = new Map();
    if (this.getAttribute("view")) {
      try { columns = new Map(((await catalog()).get(this.getAttribute("view"))?.columns ?? []).map(c => [c.name, c])); } catch { /* labels are optional */ }
    }
    const label = o => o.label ?? columns.get(o.value)?.label ?? o.value;
    const parts = [];
    if (this.getAttribute("label")) parts.push(el("span", { class: "male-label", id: `${name}-label`, text: this.getAttribute("label") }));
    if (this.getAttribute("kind") === "select") {
      const select = el("select", { "aria-labelledby": this.getAttribute("label") ? `${name}-label` : null, "aria-label": this.getAttribute("label") ? null : name });
      for (const o of options) select.append(el("option", { value: o.value, text: label(o) }));
      select.value = current;
      select.addEventListener("change", () => page.set(name, select.value));
      parts.push(select);
    } else {
      const group = el("span", { class: "male-buttons", role: "group", "aria-label": this.getAttribute("label") ?? name });
      for (const o of options) {
        const button = el("button", { type: "button", "aria-pressed": String(o.value === current), text: label(o) });
        button.addEventListener("click", () => page.set(name, o.value));
        group.append(button);
      }
      parts.push(group);
    }
    this.replaceChildren(...parts);
  }
}

// <male-theme>: light or dark, remembered; ?theme=dark|light in the URL wins.
function applyTheme(theme) { if (theme === "dark" || theme === "light") document.documentElement.setAttribute("data-theme", theme); }
let savedTheme = null;
try { savedTheme = localStorage.getItem("male-theme"); } catch { /* storage unavailable */ }
applyTheme(new URLSearchParams(location.search).get("theme") || savedTheme);

class ThemeTag extends HTMLElement {
  connectedCallback() {
    const button = el("button", { type: "button", "aria-label": "Switch between light and dark theme", text: "Theme" });
    button.addEventListener("click", () => {
      const root = document.documentElement;
      const dark = root.getAttribute("data-theme") === "dark" || (!root.hasAttribute("data-theme") && matchMedia("(prefers-color-scheme: dark)").matches);
      const next = dark ? "light" : "dark";
      applyTheme(next);
      try { localStorage.setItem("male-theme", next); } catch { /* storage unavailable */ }
    });
    this.replaceChildren(button);
  }
}

// <male-live>: connected or not, and when data last arrived.
class LiveTag extends HTMLElement {
  connectedCallback() {
    this.update = () => {
      const words = { live: "Live", connecting: "Connecting…", reconnecting: "Reconnecting…", offline: "Offline", idle: "Idle" }[live.state];
      this.dataset.state = live.state;
      const when = live.lastEvent ? ` · updated ${utcTime(live.lastEvent, true)} UTC` : "";
      this.replaceChildren(el("span", { class: "male-dot", "aria-hidden": "true" }), document.createTextNode(`${words}${when}`));
    };
    this.setAttribute("role", "status");
    live.watchers.add(this.update);
    this.update();
  }

  disconnectedCallback() { live.watchers.delete(this.update); }
}

// ---------------------------------------------------------------- platform status tags

class StatusTag extends HTMLElement {
  connectedCallback() { this.off = status.subscribe(state => this.paint(state)); }
  disconnectedCallback() { this.off?.(); }
  paint({ status: body, error }) {
    this.painted = true;
    if (!body) return this.replaceChildren(el("div", { class: "male-error", role: "alert" }, icon("bad"), el("span", { text: error })));
    this.render(body);
  }
}

// <male-freshness [dataset]>: how far the historical tier is complete.
class FreshnessTag extends StatusTag {
  render(body) {
    const name = this.getAttribute("dataset");
    const dataset = name ? body.datasets.find(d => d.name === name) : body.datasets[0];
    if (!dataset) return this.replaceChildren(el("div", { class: "male-empty", text: name ? `No dataset named "${name}".` : "No datasets." }));
    const through = dataset.historical_through;
    const lag = through ? (Date.now() - new Date(through)) / 60000 : Infinity;
    const kind = lag < 20 ? "good" : lag < 120 ? "warn" : "bad";
    const words = { good: "Historical tier is current", warn: "Flush is falling behind", bad: "Flush has stalled" }[kind];
    this.replaceChildren(
      el("div", { class: "male-label", text: this.getAttribute("label") ?? "Historical tier complete through" }),
      el("div", { class: "male-value", text: through ? `${utcTime(through)} UTC` : "—" }),
      el("div", { class: "male-status" }, icon(kind), el("span", { class: "male-muted", text: through ? `${words} · flushed ${ago(through)}` : "Nothing flushed yet" })),
    );
  }
}

// <male-gates>: each rollup's last quality gate.
class GatesTag extends StatusTag {
  render(body) {
    if (!body.rollups.length) return this.replaceChildren(el("div", { class: "male-empty", text: "No rollups are configured." }));
    this.replaceChildren(...body.rollups.map(rollup => {
      const audit = rollup.last_audit;
      const failed = audit ? audit.checks.filter(c => !c.passed) : [];
      const kind = !audit ? "none" : !audit.published ? "bad" : failed.length ? "warn" : "good";
      const words = { none: "Not run yet", bad: "Blocked", warn: `Published with ${failed.length} warning${failed.length > 1 ? "s" : ""}`, good: "Published" }[kind];
      const facts = [
        rollup.complete_through ? `complete through ${utcDay(rollup.complete_through)} ${utcTime(rollup.complete_through)} UTC` : "never published",
        audit ? `last checked ${ago(audit.at)}` : null,
        audit && !audit.published ? "the table keeps its last good contents" : null,
      ].filter(Boolean).join(" · ");
      const list = el("ul", { class: "male-checks" });
      for (const check of failed) {
        list.append(el("li", {}, icon(check.severity === "error" ? "bad" : "warn"), el("span", {}, el("span", { class: "what", text: check.check }), document.createTextNode(` — ${check.detail}`))));
      }
      const passed = audit ? audit.checks.length - failed.length : 0;
      if (passed) list.append(el("li", { class: "male-muted" }, icon("good"), el("span", { text: failed.length ? `${passed} other check${passed > 1 ? "s" : ""} passed` : `All ${passed} check${passed > 1 ? "s" : ""} passed` })));
      return el("div", { class: "male-item" },
        el("div", { class: "male-item-head" }, el("span", { class: "male-item-name", text: rollup.table }), el("span", { class: "male-status" }, icon(kind), el("span", { text: words }))),
        el("div", { class: "male-caption", text: facts }),
        list,
      );
    }));
  }
}

// <male-drift>: event fields the config doesn't describe, and the agent's proposals.
class DriftTag extends StatusTag {
  render(body) {
    const unmodeled = body.datasets.filter(d => d.unmodeled_columns.length);
    const proposals = body.drift_proposals;
    if (!unmodeled.length && !proposals.length) {
      return this.replaceChildren(el("div", { class: "male-empty", text: "The config describes every field in the data. When producers add event fields, the drift agent proposes config changes here." }));
    }
    const fields = unmodeled.map(d => el("div", { class: "male-item" },
      el("div", { class: "male-status" }, icon("warn"), el("span", { text: `${d.name}: ${d.unmodeled_columns.length} field${d.unmodeled_columns.length > 1 ? "s" : ""} in the data that the config doesn't describe` })),
      el("p", { class: "male-caption" }, el("code", { text: d.unmodeled_columns.join(", ") })),
      proposals.length ? null : el("p", { class: "male-caption", text: "The drift agent's hourly check proposes config changes for them (it needs ANTHROPIC_API_KEY in .env)." }),
    ));
    this.replaceChildren(...fields, ...proposals.map(p => el("div", { class: "male-item" },
      el("div", { class: "male-status" }, icon("warn"), el("span", { text: `${p.dataset}: proposed ${ago(p.created)}` })),
      el("p", { class: "male-caption", text: p.summary || "(no summary)" }),
      el("p", { class: "male-caption" }, document.createTextNode("Review, then: "), el("code", { text: `git apply proposals/${p.file} && python run.py setup` })),
    )));
  }
}

// ---------------------------------------------------------------- start

document.head.prepend(el("style", { id: "male-style", text: STYLE }));
// Before any tag draws: the page's parameter defaults, and the color order tags ask for.
for (const param of document.querySelectorAll("male-param[name]")) {
  const options = (param.getAttribute("options") ?? "").split(",").map(s => s.split("=")[0].trim()).filter(Boolean);
  page.defaults.set(param.getAttribute("name"), param.getAttribute("value") ?? options[0] ?? "");
}
for (const tag of document.querySelectorAll("[colors]")) registerColors(tag.getAttribute("colors").split(",").map(s => s.trim()));

customElements.define("male-kpi", KpiTag);
customElements.define("male-bars", BarsTag);
customElements.define("male-lines", LinesTag);
customElements.define("male-table", TableTag);
customElements.define("male-text", TextTag);
customElements.define("male-param", ParamTag);
customElements.define("male-theme", ThemeTag);
customElements.define("male-live", LiveTag);
customElements.define("male-freshness", FreshnessTag);
customElements.define("male-gates", GatesTag);
customElements.define("male-drift", DriftTag);

let resizeTimer;
window.addEventListener("resize", () => {
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(() => { for (const tag of tags) if (tag instanceof ViewTag) tag.draw(); }, 150);
});

/** Live results of a view for custom code: callback(result, error) now and on every change. */
export function subscribe(view, params, callback) {
  const query = new URLSearchParams(params ?? {});
  query.sort();
  return live.subscribe(query.toString() ? `${view}?${query}` : view, sub => callback(sub.result, sub.error));
}

/** True once every tag has drawn its data or an error: headless browsers wait for it. */
function settled() {
  for (const tag of tags) if (tag instanceof ViewTag && !tag.painted) return false;
  for (const tag of document.querySelectorAll("male-freshness, male-gates, male-drift")) if (!tag.painted) return false;
  return true;
}
globalThis.male = { page, live, settled, subscribe };

export { page };
