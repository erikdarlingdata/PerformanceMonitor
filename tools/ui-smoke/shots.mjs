// Screenshot baselines for the pages marked "screenshot" in pages.json (#5744, phase 2 of #5701).
//
// playwright-core has no toHaveScreenshot (that is @playwright/test's runner), so this does the same job in a few lines:
// page.screenshot() with animations off and the caret hidden, retaken until two shots in a row are identical (the stability
// rule toHaveScreenshot uses), then compared with pixelmatch against tools/ui-smoke/baselines/<page>.png.
//
//   UI_SMOKE_SHOTS      "compare" (default) | "update" (write new PNGs, compare nothing) | "off"
//   UI_SMOKE_ANCHOR     the seed's anchor, UTC, "YYYY-MM-DD HH:MM:SS" (run-ui-smoke.ps1 passes the one it gave seed.sql)
//   UI_SMOKE_BASELINES  folder of baselines   (default ./baselines)
//
// Why the page looks the same on every run. The seed dates every row from its anchor (the minute the seed ran), and the service
// reads "the last N hours" from its own clock, so left alone the axis labels and every timestamp move with the minute. The
// service must keep its real clock (as_of is refused past its reach, so a seed dated in the past would rot), so the BROWSER is
// moved instead: every /api answer has its timestamps shifted by (FIXED_ANCHOR - seed anchor), every windowed read is asked with
// as_of = seed anchor + 1 minute so the window never drifts with how long the walk took, and the page clock is pinned to
// FIXED_ANCHOR + 1 minute. The page then shows a store seeded at FIXED_ANCHOR, whatever minute the run really started.
import { chromium } from "playwright-core";
import { readFileSync, writeFileSync, mkdirSync, existsSync, copyFileSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { PNG } from "pngjs";
import pixelmatch from "pixelmatch";

export const FIXED_ANCHOR_MS = Date.UTC(2026, 2, 15, 12, 0, 0);
const AS_OF_AFTER_ANCHOR_MS = 60000;
/* Zero differing pixels, like toHaveScreenshot. Measured (#5744): unedited pages render 0 pixels apart run to run, and a one-label Fleet change is only 238 pixels (0.023%), which a 1% limit let through. */
const MAX_DIFF_PIXELS = 0;
const PIXEL_THRESHOLD = 0.1;
const MAX_SHOT_TRIES = 8;
/* How long to wait for every card to turn collection stale. The band opens 2 minutes after the seed anchor (the run log prints the wait it took). */
const STALE_WAIT_MS = 4 * 60 * 1000;
/* Screenshot-only: the status bar's database size is not the same on every run. Its text changes width when it crosses a digit count
   (99 MB to 100 MB), a mask hides the glyphs but not the box, and in a CI run (38437783077) the item was not there at all, because the
   size read had not finished. So the item and the separator before it are hidden: the bar is the same with or without it. */
const SHOT_STYLE = "#statusbar .sb-store-size, #statusbar .sb-sep:has(+ .sb-store-size) { display: none !important; }";
/* A timestamp farther than this from the seed anchor is not seed-relative (a build date, a plan's compile time): left alone. */
const SHIFT_REACH_MS = 60 * 24 * 3600 * 1000;
const TS = /(\d{4})-(\d{2})-(\d{2})([T ])(\d{2}):(\d{2}):(\d{2})(\.\d+)?(Z|[+-]\d{2}:?\d{2})?/g;

const pad = (n, w = 2) => String(n).padStart(w, "0");

/** Shift every ISO timestamp in `text` that lies near `anchorMs` by `deltaMs`, keeping its shape (T or space, fraction, offset). */
export function shiftTimestamps(text, anchorMs, deltaMs) {
  let shifted = 0;
  const out = text.replace(TS, (m, y, mo, d, sep, h, mi, s, frac, tz) => {
    let offMin = 0;
    if (tz && tz !== "Z") {
      const sign = tz[0] === "-" ? -1 : 1;
      const digits = tz.slice(1).replace(":", "");
      offMin = sign * (Number(digits.slice(0, 2)) * 60 + Number(digits.slice(2)));
    }
    const instant = Date.UTC(Number(y), Number(mo) - 1, Number(d), Number(h), Number(mi), Number(s)) - offMin * 60000;
    if (Math.abs(instant - anchorMs) > SHIFT_REACH_MS) return m;
    const local = new Date(instant + deltaMs + offMin * 60000);
    shifted++;
    return (
      `${local.getUTCFullYear()}-${pad(local.getUTCMonth() + 1)}-${pad(local.getUTCDate())}${sep}` +
      `${pad(local.getUTCHours())}:${pad(local.getUTCMinutes())}:${pad(local.getUTCSeconds())}${frac || ""}${tz || ""}`
    );
  });
  return { text: out, shifted };
}

/** "2026-03-15 12:00:00" (UTC) to epoch ms. */
export function parseAnchor(text) {
  const m = /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})/.exec(String(text || "").trim());
  if (!m) return NaN;
  return Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3]), Number(m[4]), Number(m[5]), Number(m[6]));
}

/* toHaveScreenshot's rule: shoot until two in a row are byte-identical, up to a bound, else say it never settled. */
async function stableShot(page, masks) {
  let last = null;
  for (let i = 0; i < MAX_SHOT_TRIES; i++) {
    const png = await page.screenshot({
      type: "png",
      mask: masks,
      maskColor: "#ff00ff",
      animations: "disabled",
      caret: "hide",
      scale: "css",
    });
    if (last && Buffer.compare(last, png) === 0) return png;
    last = png;
    await page.waitForTimeout(250);
  }
  throw new Error(`the page never settled: ${MAX_SHOT_TRIES} screenshots in a row, no two identical`);
}

async function shoot(browser, spec, ctx) {
  const { baseUrl, anchorMs, deltaMs, settle, config } = ctx;
  const context = await browser.newContext({
    viewport: { width: 1280, height: 800 },
    deviceScaleFactor: 1,
    colorScheme: "light",
    reducedMotion: "reduce",
    locale: "en-US",
    timezoneId: "UTC",
    bypassCSP: true,
  });
  /* The dashboard asks for its token on loopback too: run-ui-smoke.ps1 signs in once and passes the session cookie. */
  if (process.env.UI_SMOKE_SESSION_COOKIE) {
    await context.addCookies([{ name: "darling_web_session", value: process.env.UI_SMOKE_SESSION_COOKIE, url: baseUrl }]);
  }
  const page = await context.newPage();
  const asOf = new Date(anchorMs + AS_OF_AFTER_ANCHOR_MS).toISOString();
  let shiftedCount = 0;
  await context.route("**/api/**", async (route) => {
    const url = new URL(route.request().url());
    if (route.request().method() !== "GET") return route.continue();
    if (url.pathname.startsWith("/api/read/") && url.searchParams.has("hours") && !url.searchParams.has("as_of")) url.searchParams.set("as_of", asOf);
    const response = await route.fetch({ url: url.toString() });
    const type = response.headers()["content-type"] || "";
    if (!type.includes("json")) return route.fulfill({ response });
    const r = shiftTimestamps(await response.text(), anchorMs, deltaMs);
    shiftedCount += r.shifted;
    return route.fulfill({ response, body: r.text });
  });
  /* Pinned after the shift above has been agreed: a minute after the fixed anchor, as the real clock is a minute after the seed's. */
  await page.clock.setFixedTime(new Date(FIXED_ANCHOR_MS + AS_OF_AFTER_ANCHOR_MS));
  try {
    await page.goto(baseUrl + "/" + spec.hash, { waitUntil: "domcontentloaded" });
    /* A stylesheet, not an inline style: the status bar is re-mounted on a refresh. The page's CSP (style-src 'self') is why the context bypasses it. */
    await page.addStyleTag({ content: SHOT_STYLE });
    await settle(page);
    await page.evaluate(() => document.fonts.ready);
    const selectors = [...(config.screenshotDefaults?.mask || []), ...(spec.screenshot?.mask || [])];
    const masks = selectors.map((s) => page.locator(s));
    const png = await stableShot(page, masks);
    return { png, shifted: shiftedCount };
  } finally {
    await context.close();
  }
}

/** Run the screenshot pages. Returns { problems, shots }. `problems` (strings) fail the run: a page that would not shoot, no baseline, a
 *  size change. `shots` is one { name, differing, pct } per compared page and is advisory (#5931): a page that differs from its baseline
 *  prints a NOTICE and writes its baseline/actual/diff triple, but is not a problem. */
export async function runShots({ config, baseUrl, outDir, only, settle }) {
  const mode = (process.env.UI_SMOKE_SHOTS || "compare").toLowerCase();
  if (mode === "off") return { problems: [], shots: [] };
  const specs = config.pages.filter((p) => p.screenshot && (!only.length || only.includes(p.name)));
  if (specs.length === 0) return { problems: [], shots: [] };
  const anchorMs = parseAnchor(process.env.UI_SMOKE_ANCHOR);
  if (Number.isNaN(anchorMs)) return { problems: ["screenshots: UI_SMOKE_ANCHOR (the seed's anchor, UTC) is not set; run through run-ui-smoke.ps1"], shots: [] };
  const baselines = process.env.UI_SMOKE_BASELINES || join(dirname(fileURLToPath(import.meta.url)), "baselines");
  const newDir = join(outDir, "new-baselines");
  const failDir = join(outDir, "shot-failures");
  mkdirSync(newDir, { recursive: true });
  mkdirSync(failDir, { recursive: true });

  /* The seed's anchor is the minute it ran; the service answers as_of only up to its own clock, so wait out the minute. */
  const waitMs = anchorMs + AS_OF_AFTER_ANCHOR_MS + 2000 - Date.now();
  if (waitMs > 0) await new Promise((r) => setTimeout(r, waitMs));

  /* The service marks a server it cannot reach "collection stale" some minutes after it started, on its own clock: a page shot before
     that shows "Online", one shot after shows "Warning" (the red-proof run caught this, #5744). Wait until /api/fleet says every
     card is stale, so every run photographs the same state.
     "Stale" is a BAND, not a threshold: it opens 2 minutes after a server's last collection and closes at 30 (ServerHealthBands.cs,
     where the card turns Offline and is_online goes false). So the wait has two ways to fail, and each names its own cause: the cards
     are already past the band (the seed anchor aged past 30 minutes before the shots started), or they never reached it. The anchor
     is a minute old when this starts and the band opens at 2 minutes, so a real run is stale within a minute or two; the deadline is
     a few minutes, well inside the job's timeout-minutes. */
  const staleStarted = Date.now();
  const staleDeadline = staleStarted + STALE_WAIT_MS;
  let seen = "no answer from /api/fleet";
  for (;;) {
    try {
      /* The signed-in session, like the browser pages: a bare fetch is refused with 401 now that the dashboard asks for its token. */
      const cookie = process.env.UI_SMOKE_SESSION_COOKIE ? { Cookie: "darling_web_session=" + process.env.UI_SMOKE_SESSION_COOKIE } : {};
      const fleet = await (await fetch(baseUrl + "/api/fleet", { headers: cookie })).json();
      const cards = fleet.cards || [];
      const stale = cards.filter((c) => c.collection_stale === true).length;
      const offline = cards.filter((c) => c.is_online === false).length;
      seen = `${cards.length} cards, ${stale} stale, ${offline} offline`;
      if (cards.length > 0 && stale === cards.length) {
        console.log(`ok   fleet is collection stale after ${((Date.now() - staleStarted) / 1000).toFixed(0)}s of waiting (${seen})`);
        break;
      }
      if (offline > 0) {
        return { problems: [`screenshots: the fleet is already past collection stale (${seen}); the cards go Offline 30 minutes after the seed anchor, so the walk ran too long for the seed to stay stale. Shorten the walk or re-seed.`], shots: [] };
      }
    } catch {
      /* the service answers again on the next poll */
    }
    if (Date.now() > staleDeadline) {
      return { problems: [`screenshots: the fleet never reached collection stale in ${STALE_WAIT_MS / 60000} minutes (${seen}); the band opens 2 minutes after the seed anchor, so the service clock or the seed anchor is wrong, or /api/fleet stopped answering`], shots: [] };
    }
    await new Promise((r) => setTimeout(r, 5000));
  }

  /* Playwright's own Chromium, pinned by the playwright-core version (the workflow installs and caches it): the runner's Chrome
     updates with the image and would move pixels. */
  const browser = await chromium.launch({ headless: true });
  const problems = [];
  const shots = [];
  const ctx = { baseUrl, anchorMs, deltaMs: FIXED_ANCHOR_MS - anchorMs, settle, config };
  try {
    for (const spec of specs) {
      let r;
      try {
        r = await shoot(browser, spec, ctx);
      } catch (e) {
        problems.push(`screenshot ${spec.name}: ${e && e.message ? e.message.split("\n")[0] : e}`);
        console.log(`FAIL shot ${spec.name}: ${problems[problems.length - 1]}`);
        continue;
      }
      if (mode === "update") {
        writeFileSync(join(newDir, `${spec.name}.png`), r.png);
        console.log(`ok   shot ${spec.name}: new baseline written (${r.png.length} bytes, ${r.shifted} timestamps shifted)`);
        continue;
      }
      const basePath = join(baselines, `${spec.name}.png`);
      if (!existsSync(basePath)) {
        writeFileSync(join(failDir, `${spec.name}.actual.png`), r.png);
        problems.push(`screenshot ${spec.name}: no baseline at ${basePath}; dispatch the workflow with update_baselines and commit the PNGs`);
        console.log(`FAIL shot ${spec.name}: no baseline`);
        continue;
      }
      const base = PNG.sync.read(readFileSync(basePath));
      const actual = PNG.sync.read(r.png);
      writeFileSync(join(failDir, `${spec.name}.actual.png`), r.png);
      copyFileSync(basePath, join(failDir, `${spec.name}.baseline.png`));
      if (base.width !== actual.width || base.height !== actual.height) {
        problems.push(`screenshot ${spec.name}: size ${actual.width}x${actual.height}, baseline ${base.width}x${base.height}`);
        console.log(`FAIL shot ${spec.name}: size differs`);
        continue;
      }
      const diff = new PNG({ width: base.width, height: base.height });
      const differing = pixelmatch(base.data, actual.data, diff.data, base.width, base.height, { threshold: PIXEL_THRESHOLD });
      const ratio = differing / (base.width * base.height);
      shots.push({ name: spec.name, differing, pct: Number((ratio * 100).toFixed(3)) });
      if (differing > MAX_DIFF_PIXELS) {
        /* Advisory (#5931): reported with its images, never a problem, so a layout change on a page does not fail the job. */
        writeFileSync(join(failDir, `${spec.name}.diff.png`), PNG.sync.write(diff));
        console.log(`NOTICE screenshot ${spec.name}: ${differing} pixels differ (${(ratio * 100).toFixed(3)}%); baseline, actual and diff are in shot-failures`);
      } else {
        /* A pass keeps no triple: the failure folder holds only what failed. */
        for (const suffix of ["actual", "baseline"]) rmQuiet(join(failDir, `${spec.name}.${suffix}.png`));
        console.log(`ok   shot ${spec.name}: ${differing} pixels differ (${(ratio * 100).toFixed(3)}%), ${r.shifted} timestamps shifted`);
      }
    }
  } finally {
    await browser.close();
  }
  return { problems, shots };
}

function rmQuiet(p) {
  try {
    rmSync(p, { force: true });
  } catch {
    /* nothing to remove */
  }
}
