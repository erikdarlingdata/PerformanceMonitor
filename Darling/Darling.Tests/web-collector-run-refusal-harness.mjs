/* Runs the shipped web viewer's collector-run request (pages/collector-run.js runCollectors) under Node against a stand-in
   fetch that refuses the POST, and prints the message the page would show. CollectorRunRefusalTextTests starts it as
       node web-collector-run-refusal-harness.mjs <path to wwwroot/js> <scenario>
   Scenarios: store_cannot_queue (403 with the store's sentence), read_only (403 from a read-only seat). */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenario] = process.argv.slice(-2);

const bodies = {
  store_cannot_queue: { error: "This store login cannot queue a collector run. Re-run provision-roles.sql. Nothing was queued.", code: "store_cannot_queue" },
  read_only: { error: "This account is read-only." },
};
if (!(scenario in bodies)) {
  console.error("unknown scenario " + scenario);
  process.exit(2);
}

const requests = [];
globalThis.fetch = async (url, init) => {
  requests.push({ url: String(url), method: init && init.method });
  const text = JSON.stringify(bodies[scenario]);
  return { status: 403, ok: false, headers: { get: () => "application/json" }, text: async () => text, json: async () => JSON.parse(text) };
};

/* The scripts are copied beside a package.json that marks them as modules, so Node never looks for one above a network share. */
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "collector-run-refusal-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const { runCollectors } = await import(pathToFileURL(path.join(scratch, "pages", "collector-run.js")).href);
  const result = await runCollectors("known", null);
  console.log(JSON.stringify({ ok: result.ok, message: result.message, requests }));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
