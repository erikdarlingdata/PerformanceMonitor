/* Runs the Slack bot token form's rules (wwwroot/js/pages/admin-slack-bot.js) under Node and prints one line of JSON.
   SlackBotTokenWebBehaviourTests starts it as
       node slack-bot-tokens-harness.mjs <path to wwwroot/js> <scenario>
   The module is imported from the shipped folder, so the real util.js runs; only the browser globals it expects at import are stand-ins.
   Scenarios: payload (what each state of the two boxes sends) and outcome (what each answer of the service says). */
process.env.TZ = "UTC";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenario] = process.argv.slice(-2);

/* The browser globals util.js reads when it loads; none of the form rules touch the DOM. */
const noop = () => {};
const storage = { getItem: () => null, setItem: noop, removeItem: noop };
const define = (name, value) => Object.defineProperty(globalThis, name, { value, configurable: true, writable: true });
define("window", globalThis);
define("location", { hash: "", href: "http://localhost/", pathname: "/", search: "", origin: "http://localhost" });
define("navigator", { userAgent: "node", language: "en-US", clipboard: { writeText: noop } });
define("localStorage", storage);
define("sessionStorage", storage);
define("document", {
  body: { appendChild: noop, removeChild: noop, classList: { add: noop, remove: noop, contains: () => false } },
  documentElement: { style: {}, classList: { add: noop, remove: noop, contains: () => false }, getAttribute: () => null, setAttribute: noop },
  createElement: () => ({ style: {}, classList: { add: noop, remove: noop, contains: () => false }, setAttribute: noop, appendChild: noop, addEventListener: noop }),
  createTextNode: (t) => ({ nodeValue: t }),
  addEventListener: noop,
  removeEventListener: noop,
  querySelector: () => null,
  querySelectorAll: () => [],
  getElementById: () => null,
  hidden: false,
  visibilityState: "visible",
});
globalThis.addEventListener = noop;
globalThis.removeEventListener = noop;
globalThis.matchMedia = () => ({ matches: false, addEventListener: noop, removeEventListener: noop });
globalThis.requestAnimationFrame = (f) => setTimeout(f, 0);
globalThis.fetch = async () => { throw new Error("the harness makes no request"); };

const mod = await import(pathToFileURL(path.join(jsDir, "pages", "admin-slack-bot.js")).href);

const TYPED_BOT = "xoxb-9-harness-bot-not-real";
const TYPED_APP = "xapp-1-harness-app-not-real";

let out;
if (scenario === "payload") {
  out = {
    typed: mod.slackBotPayload({ bot: "  " + TYPED_BOT + " ", app: TYPED_APP, clearBot: false, clearApp: false }),
    botOnly: mod.slackBotPayload({ bot: TYPED_BOT, app: "", clearBot: false, clearApp: false }),
    cleared: mod.slackBotPayload({ bot: "", app: "", clearBot: true, clearApp: true }),
    typedWins: mod.slackBotPayload({ bot: TYPED_BOT, app: "", clearBot: true, clearApp: false }),
    blank: mod.slackBotPayload({ bot: "", app: "  ", clearBot: false, clearApp: false }),
  };
} else if (scenario === "outcome") {
  out = {
    ok: mod.slackBotOutcome({ status: 200, body: { status: "updated", secrets: { slack_bot_token: { before: "not set", after: "set" }, slack_app_token: { before: "set", after: "not set" } } } }),
    unchanged: mod.slackBotOutcome({ status: 200, body: { status: "unchanged" } }),
    refused: mod.slackBotOutcome({ status: 403, body: { error: "This sign-in cannot change the Slack bot tokens." } }),
    bad: mod.slackBotOutcome({ status: 400, body: { error: "Unknown field: x." } }),
    network: mod.slackBotOutcome({ status: 0, body: null, message: "Network error." }),
    expired: mod.slackBotOutcome({ status: 401, body: null, expired: true }),
    /* An answer that somehow carried a value: the page says only the words it knows. */
    echo: mod.slackBotOutcome({ status: 200, body: { status: "updated", secrets: { slack_bot_token: { before: "not set", after: TYPED_BOT }, slack_app_token: { before: TYPED_APP, after: "set" } } } }),
  };
} else {
  out = { error: "unknown scenario " + scenario };
}
process.stdout.write(JSON.stringify(out) + "\n");
