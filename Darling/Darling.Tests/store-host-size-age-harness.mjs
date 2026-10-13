/* Runs the store host card's "Store size" text (pages/sweeps.js storeSizeText, #5718) under Node and prints one line of JSON:
   the text the card shows for ONE cached payload at three render times, which is exactly what a 60 s poll tick does (it replays
   the cached payload through the same render path), plus the unavailable and no-as-of shapes.
       node store-host-size-age-harness.mjs <path to wwwroot/js>
   The whole js tree is copied into a scratch folder beside a package.json that marks it as an ES module (#5279). */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];
globalThis.document = { createElement: () => ({}), createTextNode: () => ({}) };

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "store-host-age-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const sweeps = await import(pathToFileURL(path.join(scratch, "pages", "sweeps.js")).href);

  const MIN = 60000;
  const base = Date.UTC(2026, 9, 12, 14, 0, 0);
  /* The server stamped the figure 10 minutes before it answered; the payload says so in both fields. */
  const payload = { size_bytes: 3 * 1024 * 1024 * 1024, size_as_of: "2026-10-12T14:00:00.123456", size_age_seconds: 600 };
  const out = {
    atFetch: sweeps.storeSizeText(payload, base + 10 * MIN),
    onePollLater: sweeps.storeSizeText(payload, base + 11 * MIN),
    anHourLater: sweeps.storeSizeText(payload, base + 70 * MIN),
    clockBehind: sweeps.storeSizeText(payload, base - 5 * MIN),
    noAsOfUsesServerAge: sweeps.storeSizeText({ size_bytes: payload.size_bytes, size_age_seconds: 600 }, base + 99 * MIN),
    noAge: sweeps.storeSizeText({ size_bytes: payload.size_bytes }, base),
    unavailable: sweeps.storeSizeText({ size_bytes: null, size_unavailable_reason: "timed out after 10 s" }, base),
  };
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
