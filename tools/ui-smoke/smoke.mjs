// Web UI smoke check for the Darling viewer pages and Web Admin (#5701).
//
// Loads every page in pages.json in a real Chrome (playwright-core, the browser already on the machine) against a
// running service seeded by seed.sql, and fails when a page shows a problem. A screenshot is kept of a page that failed, and the
// pages marked "screenshot" in pages.json are compared with their baselines (shots.mjs, #5744). Run it through run-ui-smoke.ps1, which starts the store and the service.
//
//   UI_SMOKE_URL      base address of the web host   (default http://127.0.0.1:5153)
//   UI_SMOKE_OUT      folder for report.json and failure screenshots   (default ./out)
//   UI_SMOKE_CHANNEL  the browser channel   (default chrome; msedge on a laptop without Chrome)
//   UI_SMOKE_PAGES    comma-separated page names to run   (default all)
//
// Per page (every failure is listed; any failure exits 1):
//   1. no browser console error, page error, failed request, or /api/* answer >= 400 (pages.json allowList excepted)
//   2. #main fills; the sidebar lists every seeded server; the route's nav link (or the server) is active
//   3. no .strip.error or [role=alert] anywhere in #main
//   4. named panels and grids have the state and row counts the seed guarantees
//   5. a panel the seed leaves empty shows its documented empty text (state "empty")
//   6. waits are for state (no loading strip left, network idle, the panel cards present), never a fixed sleep
import { chromium } from "playwright-core";
import { readFileSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { runShots } from "./shots.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const baseUrl = (process.env.UI_SMOKE_URL || "http://127.0.0.1:5153").replace(/\/+$/, "");
const outDir = process.env.UI_SMOKE_OUT || join(here, "out");
const channel = process.env.UI_SMOKE_CHANNEL || "chrome";
const only = (process.env.UI_SMOKE_PAGES || "").split(",").map((s) => s.trim()).filter(Boolean);
const config = JSON.parse(readFileSync(join(here, "pages.json"), "utf8"));
const SETTLE_MS = 30000;

mkdirSync(outDir, { recursive: true });

/* The allow-list is for a message that is known and harmless; each entry must name its issue. */
for (const entry of config.allowList || []) {
  if (!entry.match || !Number.isInteger(entry.issue)) {
    console.error("pages.json allowList entries need a 'match' and an integer 'issue'.");
    process.exit(2);
  }
}
const allowed = (text) => (config.allowList || []).some((e) => text.includes(e.match));

/* What one panel card shows, read in the page. */
function readPanels() {
  const main = document.querySelector("#main");
  const cards = [...main.querySelectorAll("[data-panel]")];
  return cards.map((card) => {
    const hidden = card.style.display === "none" || card.hidden;
    const body = card.querySelector(".panel-body") || card;
    let state = "data";
    if (hidden) state = "hidden";
    else if (body.querySelector(".strip.loading")) state = "loading";
    else if (body.querySelector(".strip.error, [role=alert]")) state = "error";
    else if (body.querySelector(".strip.empty") && !body.querySelector("[data-grid], svg, canvas, .stat, .stat-grid, table")) state = "empty";
    const grid = card.querySelector("[data-grid]");
    return {
      panel: card.getAttribute("data-panel"),
      title: card.getAttribute("data-title"),
      state,
      rows: grid ? grid.querySelectorAll("tbody tr").length : null,
      text: state === "empty" || state === "error" ? (body.textContent || "").trim().slice(0, 160) : "",
    };
  });
}

/* Settled: #main has content, nothing is loading, and that stays true for a short stable window. */
async function settle(page) {
  await page.waitForFunction(
    () => {
      const main = document.querySelector("#main");
      /* A panel that answered "not collected" and asked to be hidden (Instance CPU on a PostgreSQL server that is not Aurora,
         panels.js hideWhenNotCollected) keeps its loading strip inside a hidden card: not a panel still loading. */
      const loading = [...(main ? main.querySelectorAll(".strip.loading") : [])].filter((e) => {
        const card = e.closest(".panel.card");
        return !(e.closest("[hidden]") || (card && card.style.display === "none"));
      });
      return !!main && main.children.length > 0 && loading.length === 0;
    },
    null,
    { timeout: SETTLE_MS }
  );
  await page.waitForLoadState("networkidle", { timeout: SETTLE_MS });
  /* A tab builds its panels, then fills them: take the same answer twice in a row before believing it. */
  let last = "";
  for (let i = 0; i < 40; i++) {
    const now = JSON.stringify(await page.evaluate(readPanels));
    const loading = await page.evaluate(() => [...document.querySelectorAll("#main .strip.loading")].some((e) => {
      const card = e.closest(".panel.card");
      return !(e.closest("[hidden]") || (card && card.style.display === "none"));
    }));
    if (!loading && now === last) return;
    last = now;
    await page.waitForTimeout(250);
  }
}

async function checkPage(browser, spec) {
  const problems = [];
  const context = await browser.newContext({ viewport: { width: 1280, height: 800 } });
  /* The dashboard asks for its token on loopback too: run-ui-smoke.ps1 signs in once and passes the session cookie. */
  if (process.env.UI_SMOKE_SESSION_COOKIE) {
    await context.addCookies([{ name: "darling_web_session", value: process.env.UI_SMOKE_SESSION_COOKIE, url: baseUrl }]);
  }
  /* A trace of every page load; kept only for a page that fails (out/<page>.trace.zip, open with `npx playwright show-trace`). */
  await context.tracing.start({ screenshots: true, snapshots: true });
  const page = await context.newPage();
  let recording = true;
  const note = (text) => {
    if (recording && !allowed(text)) problems.push(text);
  };
  page.on("console", (m) => {
    if (m.type() === "error") note("console error: " + m.text());
  });
  page.on("pageerror", (e) => note("page error: " + e.message));
  page.on("requestfailed", (r) => note("request failed: " + r.url().replace(baseUrl, "") + " " + (r.failure()?.errorText || "")));
  page.on("response", (r) => {
    const url = new URL(r.url());
    /* Every answer >= 400, /api/* or not, named by method and path, so a console line that carries no URL can still be traced. */
    if (r.status() >= 400) note((url.pathname.startsWith("/api/") ? "api " : "asset ") + r.status() + ": " + r.request().method() + " " + url.pathname + url.search);
  });

  let panels = [];
  try {
    await page.goto(baseUrl + "/" + spec.hash, { waitUntil: "domcontentloaded" });
    await settle(page);
    panels = await page.evaluate(readPanels);

    /* 2: the shell. */
    for (const server of config.servers || []) {
      const n = await page.locator(`.server-item[data-display="${server}"], .server-item[data-server="${server}"]`).count();
      if (n === 0) problems.push(`sidebar does not list server ${server}`);
    }
    if (spec.nav) {
      const active = await page.locator(".nav a.active").evaluateAll((els) => els.map((e) => e.dataset.route));
      if (active.length !== 1 || active[0] !== spec.nav) problems.push(`active nav is [${active}], wanted ${spec.nav}`);
    }
    if (spec.server) {
      const n = await page.locator(`.server-item.active[data-display="${spec.server}"], .server-item.active[data-server="${spec.server}"]`).count();
      if (n !== 1) problems.push(`sidebar server ${spec.server} is not the active item`);
    }
    if (spec.heading) {
      const n = await page.locator("#main h2", { hasText: spec.heading }).count();
      if (n === 0) problems.push(`no heading "${spec.heading}"`);
    }

    /* 3: error strips, anywhere in #main. */
    const strips = await page.locator("#main .strip.error, #main [role=alert]").allTextContents();
    for (const t of strips) note("error strip: " + t.trim().slice(0, 200));
    for (const p of panels) {
      if (p.state === "loading") problems.push(`panel ${p.title} still loading`);
      if (p.state === "error") problems.push(`panel ${p.title} in error: ${p.text}`);
    }

    /* 4 and 5: what the seed guarantees. */
    for (const want of spec.expect?.panels || []) {
      const hit = panels.find((p) => (want.panel ? p.panel === want.panel : p.title === want.title));
      const label = want.panel || want.title;
      if (!hit) {
        problems.push(`panel ${label} not found`);
        continue;
      }
      if (want.state && hit.state !== want.state) problems.push(`panel ${label} is ${hit.state}, wanted ${want.state} ${hit.text}`.trim());
      if (want.minRows != null && (hit.rows ?? 0) < want.minRows) problems.push(`panel ${label} has ${hit.rows ?? 0} rows, wanted >= ${want.minRows}`);
    }
    /* #6291: the Overview's strips share one SVG in one panel card, so the card's state cannot say a strip has data. A strip the
       seed fills (CPU, Buffer pool) must draw something: a strip with no samples prints its reason in a .strip-none text. */
    for (const want of spec.expect?.strips || []) {
      const got = await page.locator(`#main svg.overview-strips g.strip[data-strip="${want.key}"]`).evaluateAll((els) =>
        els.map((e) => (e.querySelector(".strip-none") ? "empty" : "data")),
      );
      if (!got.length) problems.push(`strip ${want.key} not found`);
      else if (want.state && got[0] !== want.state) problems.push(`strip ${want.key} is ${got[0]}, wanted ${want.state}`);
    }
    if (spec.expect?.grids?.length) {
      const counts = await page.locator("#main [data-grid]").evaluateAll((els) => els.map((e) => e.querySelectorAll("tbody tr").length));
      for (const want of spec.expect.grids) {
        const got = counts[want.index ?? 0];
        if (got == null) problems.push(`grid #${want.index ?? 0} not found (page has ${counts.length})`);
        else {
          if (want.exactRows != null && got !== want.exactRows) problems.push(`grid #${want.index ?? 0} has ${got} rows, wanted ${want.exactRows}`);
          if (want.minRows != null && got < want.minRows) problems.push(`grid #${want.index ?? 0} has ${got} rows, wanted >= ${want.minRows}`);
        }
      }
    }
  } catch (e) {
    problems.push("did not settle: " + (e && e.message ? e.message.split("\n")[0] : String(e)));
  }

  recording = false;
  if (problems.length) {
    try {
      await page.screenshot({ path: join(outDir, `${spec.name}.png`), fullPage: true });
    } catch {
      /* a page that never drew has nothing to photograph */
    }
  }
  try {
    await context.tracing.stop(problems.length ? { path: join(outDir, `${spec.name}.trace.zip`) } : {});
  } catch {
    /* a trace that cannot be written must not hide the page's own result */
  }
  await context.close();
  return { problems, panels };
}

const browser = await chromium.launch({ channel, headless: true });
const results = [];
for (const spec of config.pages) {
  if (only.length && !only.includes(spec.name)) continue;
  const started = Date.now();
  let r = await checkPage(browser, spec);
  let flaked = false;
  if (r.problems.length) {
    /* One retry; a retry that passes is reported as a flake, never hidden. */
    const first = r.problems;
    const again = await checkPage(browser, spec);
    if (again.problems.length === 0) {
      flaked = true;
      console.log(`FLAKE ${spec.name}: failed once (${first.join("; ")}), passed on the retry`);
    }
    r = again.problems.length === 0 ? again : { problems: first, panels: r.panels };
  }
  const secs = ((Date.now() - started) / 1000).toFixed(1);
  results.push({ name: spec.name, hash: spec.hash, seconds: Number(secs), flaked, problems: r.problems, panels: r.panels });
  console.log(`${r.problems.length ? "FAIL" : "ok  "} ${spec.name} (${secs}s)`);
  for (const p of r.panels) console.log(`       panel ${String(p.title).padEnd(36)} ${p.state.padEnd(8)}${p.rows != null ? " rows=" + p.rows : ""}`);
  for (const p of r.problems) console.log(`       PROBLEM ${p}`);
}
await browser.close();

/* The walk's result lands first (report.json is { pages, screenshotProblems, screenshots }), so nothing the screenshot stage does can lose it. */
writeFileSync(join(outDir, "report.json"), JSON.stringify({ pages: results, screenshotProblems: [] }, null, 2));

/* Screenshot baselines (#5744): the pages marked "screenshot" in pages.json, in Playwright's own pinned Chromium. A throw out of the
   stage (Chromium not installed, an unwritable folder) is a screenshot problem like any other: it fails the run, it does not crash it. */
let shotProblems;
let shots = [];
try {
  ({ problems: shotProblems, shots } = await runShots({ config, baseUrl, outDir, only, settle }));
} catch (e) {
  shotProblems = [`screenshots: the stage threw: ${e && e.message ? e.message.split("\n")[0] : e}`];
}
for (const p of shotProblems) console.log(`       PROBLEM ${p}`);
/* The compare is advisory (#5931): a differing screenshot is in `shots` and printed as a NOTICE; only shotProblems fail the run. */
if ((process.env.UI_SMOKE_SHOTS || "compare").toLowerCase() === "compare" && (shots.length || shotProblems.length)) {
  const differ = shots.filter((x) => x.differing > 0).length;
  console.log(`screenshots: ${shots.length - differ} matched, ${differ} differ (advisory), ${shotProblems.length} problems`);
}

writeFileSync(join(outDir, "report.json"), JSON.stringify({ pages: results, screenshotProblems: shotProblems, screenshots: shots }, null, 2));
const failed = results.filter((r) => r.problems.length);
console.log(`\nui-smoke: ${results.length - failed.length}/${results.length} pages clean, ${results.filter((r) => r.flaked).length} flaked`);
if (results.length === 0) {
  console.log("ui-smoke: no page ran.");
  process.exit(1);
}
process.exit(failed.length || shotProblems.length ? 1 : 0);
