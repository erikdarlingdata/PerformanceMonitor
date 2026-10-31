/* Runs the shipped wwwroot/js/markdown.js on the markdown strings in HARNESS_INPUT (a JSON array) and prints
   {"out": [...]} as one line: for each input, the block nodes renderMarkdown built, as {tag, cls, text, href, kids}. WebMarkdownTests
   starts it as
       node web-markdown-harness.mjs <path to wwwroot/js>
   markdown.js imports only el() from util.js, which needs a browser, so that import line becomes the small stand-in below (a
   tree of plain objects for the DOM) and its `export` keywords are dropped. Everything else is markdown.js as shipped. */
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";

const jsDir = process.argv[process.argv.length - 1];
const source = fs.readFileSync(path.join(jsDir, "markdown.js"), "utf8")
  .replace(/^import .*;[ \t]*\r?$/gm, "")
  .replace(/^export /gm, "");
if (/^import /m.test(source)) {
  throw new Error("markdown.js layout changed: the harness cannot strip a multi-line import");
}

const flat = (x) => (Array.isArray(x) ? x.flatMap(flat) : x == null ? [] : [x]);
const el = (tag, attrs, kids) => ({ tag, cls: (attrs && attrs.class) || "", text: attrs && attrs.text, href: attrs && attrs.href, kids: flat(kids) });
const document = {
  createDocumentFragment: () => ({ tag: "#fragment", kids: [], appendChild(n) { this.kids.push(n); return n; } }),
  createTextNode: (text) => ({ tag: "#text", text, kids: [] }),
};

const context = vm.createContext({ document, el });
vm.runInContext(source + "\nthis.renderMarkdown = renderMarkdown;", context);

const inputs = JSON.parse(process.env.HARNESS_INPUT);
const plain = (n) => ({ tag: n.tag, cls: n.cls || "", text: n.text ?? null, href: n.href ?? null, kids: (n.kids || []).map(plain) });
console.log(JSON.stringify({ out: inputs.map((t) => context.renderMarkdown(t).kids.map(plain)) }));
