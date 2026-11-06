/* Runs the shipped getSession() (views-api.js, with the real util.js apiGet) under Node against a stand-in fetch, and prints the
   result of a scenario as one line of JSON (#7928). SessionRetryWebTests starts it as
       node session-retry-harness.mjs <path to wwwroot/js> <scenario>
   The js folder is copied into a scratch folder with a package.json that marks it as ES modules, then imported as the browser would. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenario] = process.argv.slice(-2);

const noop = () => {};
const domNode = () => ({
  style: {}, classList: { add: noop, remove: noop, toggle: noop, contains: () => false }, appendChild: noop, append: noop,
  setAttribute: noop, removeAttribute: noop, addEventListener: noop, removeEventListener: noop, querySelector: () => null,
  querySelectorAll: () => [], children: [], dataset: {},
});
globalThis.document = {
  createElement: domNode, createTextNode: domNode, getElementById: domNode, querySelector: () => null, querySelectorAll: () => [],
  addEventListener: noop, removeEventListener: noop, body: domNode(), documentElement: domNode(), cookie: "", hidden: false,
};
globalThis.window = globalThis;
globalThis.location = { hash: "", pathname: "/", search: "", href: "http://localhost/", origin: "http://localhost" };
const store = { getItem: () => null, setItem: noop, removeItem: noop };
globalThis.localStorage = store;
globalThis.sessionStorage = store;
globalThis.addEventListener = noop;
globalThis.removeEventListener = noop;

/* What /api/session answers, in order (the last one repeats); an Error is a transport failure, a number is an HTTP status with no body. */
let answers = [];
let calls = 0;
let release = null;
globalThis.fetch = async (url) => {
  if (url !== "/api/session") throw new Error("unexpected request " + url);
  calls++;
  if (release) await release;
  const next = answers.length > 1 ? answers.shift() : answers[0];
  if (next instanceof Error) throw next;
  if (typeof next === "number") return { ok: false, status: next, text: async () => "" };
  return { ok: true, status: 200, text: async () => JSON.stringify(next) };
};

const realNow = Date.now;
let clock = 1_000_000;
Date.now = () => clock;

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "session-retry-"));
const out = {};
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const api = await import(pathToFileURL(path.join(scratch, "views-api.js")).href);
  /* dev's views-api.js has no SESSION_RETRY_MS; a wait of a minute is past any wait the fix could choose below it. */
  const waitPastRetry = 60_000;

  if (scenario === "recovers") {
    /* The first read fails (a restart behind the front door), the next one after the wait is good. */
    answers = [new Error("connection refused"), { can_edit: true }];
    out.first = await api.getSession();
    out.peekAfterFailure = api.peekSession();
    clock += waitPastRetry;
    out.second = await api.getSession();
    out.peekAfterSuccess = api.peekSession();
    out.calls = calls;
  } else if (scenario === "failed-status-recovers") {
    answers = [503, { can_edit: false }];
    out.first = await api.getSession();
    clock += waitPastRetry;
    out.second = await api.getSession();
    out.calls = calls;
  } else if (scenario === "good-answer-kept") {
    /* A good answer is read once: a later failing service does not unseat it. */
    answers = [{ can_edit: true }, new Error("down")];
    out.first = await api.getSession();
    clock += 10 * waitPastRetry;
    out.second = await api.getSession();
    out.calls = calls;
  } else if (scenario === "no-storm") {
    /* A burst of callers while the service is down shares one read in flight, then one failed answer; none of them loops. */
    answers = [new Error("down")];
    let open;
    release = new Promise((r) => { open = r; });
    const burst = [api.getSession(), api.getSession(), api.getSession()];
    open();
    out.burst = await Promise.all(burst);
    release = null;
    out.callsAfterBurst = calls;
    for (let i = 0; i < 5; i++) await api.getSession();
    out.callsInsideWait = calls;
    clock += waitPastRetry;
    await api.getSession();
    await api.getSession();
    out.callsAfterWait = calls;
  } else {
    throw new Error("unknown scenario " + scenario);
  }
  console.log(JSON.stringify(out));
} finally {
  Date.now = realNow;
  fs.rmSync(scratch, { recursive: true, force: true });
}
process.exit(0);
