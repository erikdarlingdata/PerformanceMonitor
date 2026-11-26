/* #9789: module loader hooks for a Node process whose modules sit on a network share (a file URL with a host: file://server/share/...).
   Node decides whether a .js file is a module by reading the package.json files above it. Beside a share that read walks up to the
   server itself and fails ("Cannot read package config"), so importing the shipped wwwroot/js files from a share throws before a
   harness runs. The shipped files are all ES modules, so for a .js or .mjs file on a share these hooks answer "module" and read the
   source themselves. A file that is not there goes to Node's own resolver, which throws ERR_MODULE_NOT_FOUND as it does anywhere
   (plain-text.js tells that error from every other by its code). Every other URL goes to Node's own loader unchanged. */
import fs from "node:fs/promises";
import { fileURLToPath } from "node:url";

const isModuleOnShare = (url) => url.protocol === "file:" && url.hostname !== "" && /\.m?js$/.test(url.pathname);

const isFile = async (url) => {
  try {
    return (await fs.stat(fileURLToPath(url))).isFile();
  } catch {
    return false;
  }
};

export async function resolve(specifier, context, nextResolve) {
  if (context.parentURL && /^(\.\.?\/|\/|file:)/.test(specifier)) {
    const url = new URL(specifier, context.parentURL);
    if (isModuleOnShare(url) && (await isFile(url))) return { url: url.href, format: "module", shortCircuit: true };
  }
  return nextResolve(specifier, context);
}

export async function load(url, context, nextLoad) {
  const parsed = new URL(url);
  if (isModuleOnShare(parsed)) return { format: "module", source: await fs.readFile(fileURLToPath(parsed)), shortCircuit: true };
  return nextLoad(url, context);
}
