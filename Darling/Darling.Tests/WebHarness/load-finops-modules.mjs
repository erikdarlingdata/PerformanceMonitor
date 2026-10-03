const noop=()=>{}; const node=()=>({style:{},classList:{add:noop,remove:noop,toggle:noop},appendChild:noop,append:noop,setAttribute:noop,addEventListener:noop,children:[],dataset:{}});
globalThis.document={createElement:node,createElementNS:node,createTextNode:node,createDocumentFragment:node,getElementById:()=>null,querySelector:()=>null,querySelectorAll:()=>[],addEventListener:noop,body:node(),documentElement:node(),cookie:""};
globalThis.window=globalThis; globalThis.location={hash:"",pathname:"/",search:""}; globalThis.fetch=async()=>({ok:false,status:500,json:async()=>({}),text:async()=>""});
globalThis.localStorage={getItem:()=>null,setItem:noop}; globalThis.sessionStorage=globalThis.localStorage;
import { readdirSync } from "node:fs";
import { pathToFileURL } from "node:url";

/* Loads finops.js and every page under pages/finops/ the way the browser does, with a stubbed DOM and fetch. A
   ReferenceError while a module loads (a name used but not imported) or a missing export fails the run. */
const root = process.argv[2];
const files = [root + "/pages/finops.js", ...readdirSync(root + "/pages/finops").filter((f) => f.endsWith(".js")).sort().map((f) => root + "/pages/finops/" + f)];
for (const f of files) {
  const m = await import(pathToFileURL(f).href);
  const name = f.slice(root.length + 1);
  if (name.startsWith("pages/finops/")) {
    if (!m.tab || typeof m.tab.build !== "function" || !m.tab.id) throw new Error(name + " does not export tab = { id, build }");
  } else if (typeof m.renderFinops !== "function" || !Array.isArray(m.FINOPS_TABS)) {
    throw new Error(name + " does not export renderFinops and FINOPS_TABS");
  }
  console.log("loaded " + name);
}
