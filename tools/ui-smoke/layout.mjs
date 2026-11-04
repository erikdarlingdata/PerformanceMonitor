// The layout pass of the web UI smoke check (#7479).
//
// smoke.mjs checks each page at one window size and asks only whether it loaded and shows the seeded data. This pass loads every page
// again at three widths (600, 1280, 2560) and measures the page, so overflow, clipped text, charts with no height and a wide screen
// that the page does not use fail the job instead of reaching the app unseen. Every failure is reported, not only the first:
// page, width, check, selector and the numbers.
//
// What fails (the numbers are in the message; the thresholds are the constants below):
//   page-scrolls-sideways   documentElement.scrollWidth > clientWidth
//   content-past-edge       an element reaches past the right edge of #main, of a panel or of a card that holds it. An element inside
//                           a box that scrolls or clips (overflow-x other than visible: a grid's .table-wrap, a chart's SVG) is not
//                           measured against what is outside that box; the box itself is, so a grid scroll box that is wider than its
//                           panel still fails.
//   text-clipped            an element with overflow hidden whose text is wider than the box (scrollWidth > clientWidth) and that has no
//                           title (own or inherited) to read the rest. Cells of a grid are not measured: a grid cell clamps by design.
//   chart-size              a .chart whose plot is shorter than MIN_CHART_HEIGHT or narrower than MIN_CHART_WIDTH, or that sticks out of
//                           its panel
//   width-unused            at 2560 only: the page's content stops short of the right edge of #main by more than the share allowed
//
// A full-page PNG of every page at every width goes to <out>/layout/<page>@<width>.png (the workflow uploads <out>).

import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

export const WIDTHS = [600, 1280, 2560];
const HEIGHT = 800;
/* A chart whose plot is under 60 px tall is a line, not a chart; under 120 px wide, too narrow to read an axis. */
const MIN_CHART_HEIGHT = 60;
const MIN_CHART_WIDTH = 120;
/* At 2560 the content may use less than this share of the width #main has only when a page has too little to fill it. */
const MIN_FILL = 0.8;

/* Runs in the page. Returns { failures, fill, docHeight }. */
function probe(arg) {
  const { minChartHeight, minChartWidth, checkFill } = arg;
  const failures = [];
  const main = document.querySelector("#main");
  const de = document.documentElement;
  const cssPath = (el) => {
    const parts = [];
    let e = el;
    for (let i = 0; e && e !== document.body && i < 5; i++, e = e.parentElement) {
      let s = e.tagName.toLowerCase();
      if (e.id) {
        parts.unshift(s + "#" + e.id);
        break;
      }
      const dp = e.getAttribute("data-panel");
      if (dp) s += `[data-panel=${dp}]`;
      const cls = (typeof e.className === "string" ? e.className : "").split(/\s+/).filter(Boolean).slice(0, 2);
      if (cls.length) s += "." + cls.join(".");
      parts.unshift(s);
      if (dp) break;
    }
    return parts.join(" > ");
  };
  const r1 = (n) => Math.round(n * 10) / 10;
  const add = (check, el, detail) => failures.push({ check, selector: cssPath(el), detail });
  const visible = (el, rect) => rect.width > 0 && rect.height > 0 && getComputedStyle(el).visibility !== "hidden";

  if (de.scrollWidth > de.clientWidth) add("page-scrolls-sideways", de, `scrollWidth ${de.scrollWidth} > clientWidth ${de.clientWidth}`);
  if (!main) return { failures, fill: null, docHeight: de.scrollHeight };

  /* One walk of #main. `clipper` is the nearest ancestor that scrolls or clips sideways; the walk does not go inside one (its
     contents are its own business) unless it is a panel or card, whose contents are measured against that panel or card. */
  const isContainer = (el) => el === main || el.hasAttribute("data-panel") || el.classList.contains("card");
  const recs = [];
  const walk = (el, clipper) => {
    for (const child of el.children) {
      if (child.tagName === "SCRIPT" || child.tagName === "STYLE") continue;
      const cs = getComputedStyle(child);
      if (cs.display === "none" || cs.position === "fixed") continue;
      const rect = child.getBoundingClientRect();
      const clips = cs.overflowX !== "visible" || child instanceof SVGSVGElement;
      if (rect.width > 0 && rect.height > 0 && cs.visibility !== "hidden") recs.push({ el: child, rect, clipper, cs });
      if (clips && !isContainer(child)) continue;
      walk(child, clips ? child : clipper);
    }
  };
  walk(main, null);

  /* content-past-edge: against #main's content edge and against every panel and card. */
  const mainRect = main.getBoundingClientRect();
  const mainCs = getComputedStyle(main);
  const containers = [{ el: main, right: mainRect.right }].concat(
    [...main.querySelectorAll("[data-panel], .card")]
      .map((el) => ({ el, rect: el.getBoundingClientRect() }))
      .filter((c) => c.rect.width > 0 && c.rect.height > 0)
      .map((c) => ({ el: c.el, right: c.rect.right })),
  );
  const seen = new Set();
  for (const c of containers) {
    const over = recs.filter(
      (r) =>
        r.el !== c.el &&
        c.el.contains(r.el) &&
        r.rect.right > c.right + 1 &&
        /* a box that clips or scrolls inside the container hides what is past its edge: only the box itself is measured */
        (r.clipper === null || !c.el.contains(r.clipper) || r.clipper === c.el),
    );
    const set = new Set(over.map((r) => r.el));
    for (const r of over) {
      if (set.has(r.el.parentElement) && r.el.parentElement !== c.el) continue; /* report the outermost element only */
      const key = r.el;
      if (seen.has(key)) continue;
      seen.add(key);
      const what = c.el === main ? "#main" : cssPath(c.el);
      add("content-past-edge", r.el, `right ${r1(r.rect.right)} > ${what} right ${r1(c.right)} (+${r1(r.rect.right - c.right)} px), width ${r1(r.rect.width)}`);
    }
  }

  /* text-clipped: any clipping box in #main, except grid cells (they clamp by design and open on click). */
  for (const r of recs) {
    const el = r.el;
    if (el instanceof SVGElement) continue;
    if (r.cs.overflowX !== "hidden" && r.cs.overflowX !== "clip") continue;
    if (el.closest("table, .table-wrap")) continue;
    if (el.scrollWidth <= el.clientWidth + 1) continue;
    if (!(el.textContent || "").trim()) continue;
    if (el.closest("[title]")) continue;
    add("text-clipped", el, `scrollWidth ${el.scrollWidth} > clientWidth ${el.clientWidth}, text "${(el.textContent || "").trim().slice(0, 60)}"`);
  }

  /* chart-size */
  for (const chart of main.querySelectorAll(".chart")) {
    const rect = chart.getBoundingClientRect();
    if (rect.width === 0 && rect.height === 0) continue; /* in a hidden card */
    const plot = chart.querySelector(".chart-plot") || chart.querySelector("svg") || chart;
    const pr = plot.getBoundingClientRect();
    if (pr.height < minChartHeight || pr.width < minChartWidth) {
      add("chart-size", chart, `plot ${r1(pr.width)} x ${r1(pr.height)} px, wanted at least ${minChartWidth} x ${minChartHeight}`);
    }
    const panel = chart.closest("[data-panel], .card");
    if (panel) {
      const pb = panel.getBoundingClientRect();
      if (pr.right > pb.right + 1 || pr.left < pb.left - 1 || pr.bottom > pb.bottom + 1) {
        add("chart-size", chart, `plot ${r1(pr.left)}..${r1(pr.right)} x ..${r1(pr.bottom)} sticks out of its panel ${r1(pb.left)}..${r1(pb.right)} x ..${r1(pb.bottom)}`);
      }
    }
  }

  /* width-unused: how far right the page's content reaches, as a share of the width #main has. */
  let fill = null;
  {
    const padL = parseFloat(mainCs.paddingLeft || "0");
    const padR = parseFloat(mainCs.paddingRight || "0");
    const left = mainRect.left + padL;
    const avail = mainRect.width - padL - padR;
    let reach = left;
    let counted = 0;
    for (const r of recs) {
      /* Only the things that carry content count: a plain block wrapper always spans the row and says nothing. A page with
         nothing counted (an empty state, a form) has no fill to measure. */
      if (r.rect.right > reach && r.rect.width > 8 && r.el.matches(".card, .panel, .tile, .stat, .chart, .table-wrap")) {
        reach = Math.min(r.rect.right, left + avail);
        counted++;
      }
    }
    fill = avail > 0 && counted > 0 ? Math.round(((reach - left) / avail) * 1000) / 1000 : null;
  }
  return { failures, fill, docHeight: de.scrollHeight, checkFill };
}

export async function runLayout({ config, baseUrl, outDir, only, settle, browser, sessionCookie }) {
  const dir = join(outDir, "layout");
  mkdirSync(dir, { recursive: true });
  const started = Date.now();
  const failures = [];
  const exempted = [];
  const rows = [];
  for (const spec of config.pages) {
    if (only.length && !only.includes(spec.name)) continue;
    const context = await browser.newContext({ viewport: { width: WIDTHS[0], height: HEIGHT } });
    if (sessionCookie) await context.addCookies([{ name: "darling_web_session", value: sessionCookie, url: baseUrl }]);
    const page = await context.newPage();
    for (let i = 0; i < WIDTHS.length; i++) {
      const width = WIDTHS[i];
      const tag = `${spec.name}@${width}`;
      try {
        await page.setViewportSize({ width, height: HEIGHT });
        if (i === 0) await page.goto(baseUrl + "/" + spec.hash, { waitUntil: "domcontentloaded" });
        else await page.reload({ waitUntil: "domcontentloaded" });
        await settle(page);
        /* A chart draws at the width its box has once it is on screen: let one animation frame pass before measuring. */
        await page.evaluate(() => new Promise((res) => requestAnimationFrame(() => requestAnimationFrame(res))));
        const result = await page.evaluate(probe, { minChartHeight: MIN_CHART_HEIGHT, minChartWidth: MIN_CHART_WIDTH, checkFill: width >= 2000 });
        if (width >= 2000 && result.fill != null && result.fill < MIN_FILL) {
          result.failures.push({ check: "width-unused", selector: "#main", detail: `content reaches ${(result.fill * 100).toFixed(0)}% of the ${width}px page, wanted at least ${MIN_FILL * 100}%` });
        }
        for (const f of result.failures) {
          /* A page may exempt one check, with the reason in pages.json ("layoutExempt": [{ "check", "reason" }]); it is printed, never silent. */
          const ex = (spec.layoutExempt || []).find((e) => e.check === f.check);
          if (ex) exempted.push({ page: spec.name, width, ...f, reason: ex.reason });
          else failures.push({ page: spec.name, width, ...f });
        }
        rows.push({ page: spec.name, width, fill: result.fill, docHeight: result.docHeight, failures: result.failures.length });
        try {
          await page.screenshot({ path: join(dir, `${tag}.png`), fullPage: true });
        } catch (e) {
          failures.push({ page: spec.name, width, check: "screenshot", selector: "", detail: String(e.message || e).split("\n")[0] });
        }
      } catch (e) {
        failures.push({ page: spec.name, width, check: "did-not-settle", selector: "", detail: String(e && e.message ? e.message : e).split("\n")[0] });
      }
    }
    await context.close();
  }
  const seconds = Number(((Date.now() - started) / 1000).toFixed(1));
  writeFileSync(join(dir, "layout-report.json"), JSON.stringify({ widths: WIDTHS, seconds, rows, failures, exempted }, null, 2));
  return { failures, exempted, rows, seconds };
}
