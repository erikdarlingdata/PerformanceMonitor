/* Runs the web shell's real "Sign in again" code (the region of wwwroot/js/app.js from signInAgainTarget to the
   auto-refresh play/pause banner) and prints what the signed-out banner drew and what a click on its link did, as one
   line of JSON (#6278).
   SignInAgainBehaviourTests starts it as: node sign-in-again-harness.mjs <path to app.js> <scenario>
   app.js is the page's entry module, so only that region is evaluated, against stand-ins for the DOM helpers and
   for location (a recorder: assign and reload only note that they were called). */
import fs from "node:fs";
import vm from "node:vm";

const [appPath, scenario] = process.argv.slice(-2);
const full = fs.readFileSync(appPath, "utf8").replace(/\r\n/g, "\n");
const start = full.indexOf("function signInAgainTarget");
const end = full.indexOf("/* ─────────────────────────── auto-refresh play/pause");
if (start < 0 || end < start) throw new Error("app.js layout changed: sign-in-again region not found");
const source = full.slice(start, end);

const flat = (x) => (Array.isArray(x) ? x.flatMap(flat) : x == null ? [] : [x]);
const el = (tag, attrs, kids) => ({ tag, attrs: attrs || {}, kids: flat(kids) });
let drawn = null;
const calls = { assign: [], reload: 0 };
const context = vm.createContext({
  console, RegExp, String,
  el,
  mount: (parent, nodes) => { if (parent === "main") drawn = flat(nodes); },
  main: "main", serverList: "serverList", viewList: "viewList", statusbar: "statusbar",
  clearViewGroups: () => {},
  location: {
    pathname: "/", search: "?x=1", hash: "#/job-history",
    assign: (u) => calls.assign.push(u),
    reload: () => { calls.reload++; },
  },
});
vm.runInContext(source + "\nthis.showSignedOutState = showSignedOutState;", context);

const click = (login) => {
  context.showSignedOutState("session expired", login);
  const link = drawn[0].kids.find((k) => k && k.tag === "a");
  let prevented = false;
  link.attrs.onClick({ preventDefault: () => { prevented = true; } });
  return { href: link.attrs.href, text: link.attrs.text, prevented, assign: calls.assign, reload: calls.reload };
};

const logins = {
  server: "/", missing: undefined, nonString: 7, offSite: "https://off-site.example/x", protocolRelative: "//off-site.example/x",
  backslash: "/\\off-site.example", javascript: "javascript:alert(1)", sameOriginPath: "/auth/login?return=%2F",
};
if (!(scenario in logins)) throw new Error("unknown scenario " + scenario);
console.log(JSON.stringify(click(logins[scenario])));
