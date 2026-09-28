// Links (does NOT evaluate) every ES module under <wwwroot>/js with vm.SourceTextModule, starting from an
// entry module and then from every file. `node --check` parses one file alone, so it cannot see a module
// that imports a file that does not exist, a named export that is not there, or a duplicate declaration
// across an import; each of those blanks the dashboard at load. Linking reports them without running code.
// Usage: node --experimental-vm-modules .github/scripts/esm-link-check.mjs [wwwroot dir] [entry, default js/app.js]
// The wwwroot dir defaults to the Darling service wwwroot of this checkout.
// Exit: 0 all linked, 1 any failure, 2 when <wwwroot>/js does not exist.
import vm from 'node:vm';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(process.argv[2] || fileURLToPath(new URL('../../Darling/PerformanceMonitor.Darling.Service/wwwroot', import.meta.url)));
const entry = process.argv[3] || 'js/app.js';
const cache = new Map();
const context = vm.createContext({});

function load(file) {
  if (cache.has(file)) return cache.get(file);
  const src = fs.readFileSync(file, 'utf8');
  const m = new vm.SourceTextModule(src, { identifier: file, context });
  cache.set(file, m);
  return m;
}

async function linker(specifier, referencing) {
  if (!specifier.startsWith('.') && !specifier.startsWith('/')) {
    throw new Error(`bare import "${specifier}" in ${path.relative(root, referencing.identifier)}`);
  }
  const base = specifier.startsWith('/') ? root : path.dirname(referencing.identifier);
  const target = path.resolve(base, specifier.replace(/^\//, ''));
  if (!fs.existsSync(target)) {
    throw new Error(`missing module ${path.relative(root, target)} imported by ${path.relative(root, referencing.identifier)}`);
  }
  return load(target);
}

if (!fs.existsSync(path.join(root, 'js'))) {
  console.error(`FAIL: directory not found: ${path.join(root, 'js')}`);
  process.exit(2);
}

const files = [];
(function walk(d) {
  for (const e of fs.readdirSync(d, { withFileTypes: true })) {
    const p = path.join(d, e.name);
    if (e.isDirectory()) walk(p); else if (p.endsWith('.js')) files.push(p);
  }
})(path.join(root, 'js'));

let failures = 0;
for (const f of new Set([path.join(root, entry), ...files])) {
  try {
    const m = load(f);
    if (m.status === 'unlinked') await m.link(linker);
  } catch (e) {
    failures++;
    console.log(`FAIL ${path.relative(root, f)}: ${e.message}`);
  }
}
console.log(`linked ${cache.size} modules, ${failures} failure(s)`);
process.exit(failures ? 1 : 0);
