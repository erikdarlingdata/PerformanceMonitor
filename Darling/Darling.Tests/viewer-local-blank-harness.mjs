/* Runs the web dashboard's real per-browser state module (wwwroot/js/viewer-local.js) and answers, for each text in a list
   read from stdin, whether the page's blank-name rule and its name rule take it. WebDatabaseBlankRuleTests starts it as
       node viewer-local-blank-harness.mjs <path to viewer-local.js>
   and writes the list as JSON: an array of texts, each text an array of UTF-16 code units (numbers), so a lone surrogate
   travels intact. The answer is one JSON line: { blank: "0101...", name: "0110..." } with one character per text, "1" for
   yes. The module has no imports; its `export` keywords are dropped so it runs in a vm context, exactly as shipped. */
import fs from "node:fs";
import vm from "node:vm";

const path = process.argv[2];
const source = fs.readFileSync(path, "utf8").replace(/^export /gm, "");
const ctx = vm.createContext({ console, Date, localStorage: { getItem: () => null, setItem: () => {}, removeItem: () => {} } });
vm.runInContext(source + "\n;globalThis.api = { isBlankDatabaseName, isDatabaseName };", ctx);

const texts = JSON.parse(fs.readFileSync(0, "utf8")).map((units) => String.fromCharCode(...units));
const blank = texts.map((t) => (ctx.api.isBlankDatabaseName(t) ? "1" : "0")).join("");
const name = texts.map((t) => (ctx.api.isDatabaseName(t) ? "1" : "0")).join("");
console.log(JSON.stringify({ blank, name }));
