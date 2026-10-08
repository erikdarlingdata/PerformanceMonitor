/* Runs every case in Fixtures/time-range-cases.json against the web module wwwroot/js/time-range.js (#5562) and prints one
   line of JSON: { total, passed, failures: [ "<case name>: <what differed>" ] }. WebTimeRangeFixtureTests starts it as
       node time-range-harness.mjs <path to wwwroot/js> <path to time-range-cases.json>
   The module is pure (no DOM, no clock), so it is copied into a scratch folder beside a package.json that marks it as an ES
   module and imported there. Each case names its own IANA zone and its own now: the module takes both as arguments, so no
   process zone is set and the run does not depend on the machine's. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, fixturePath] = process.argv.slice(-2);

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "time-range-"));
let mod;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  mod = await import(pathToFileURL(path.join(scratch, "time-range.js")).href);
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
const failures = [];
let passed = 0;

for (const c of fixture.cases) {
  const problems = [];
  const r = mod.parseRange(c.input, Date.parse(c.now), c.zone);
  if (c.error) {
    if (r.ok) problems.push("expected error " + c.error + " but got " + r.echo);
    else if (r.errorCode !== c.error) problems.push("error code " + r.errorCode + " (" + r.error + "), expected " + c.error);
    else if (c.errorMessage && r.error !== c.errorMessage) problems.push("error message '" + r.error + "', expected '" + c.errorMessage + "'");
  } else if (!r.ok) {
    problems.push("refused with " + r.errorCode + " (" + r.error + ")");
  } else {
    if (r.range.startMs !== Date.parse(c.start)) problems.push("start " + new Date(r.range.startMs).toISOString() + ", expected " + c.start);
    if (r.range.endMs !== Date.parse(c.end)) problems.push("end " + new Date(r.range.endMs).toISOString() + ", expected " + c.end);
    if (r.range.live !== c.live) problems.push("live " + r.range.live + ", expected " + c.live);
    if (r.echo !== c.echo) problems.push("echo '" + r.echo + "', expected '" + c.echo + "'");
  }
  if (problems.length === 0) passed++;
  else failures.push(c.name + " [" + c.input + " @ " + c.zone + "]: " + problems.join("; "));
}

console.log(JSON.stringify({ total: fixture.cases.length, passed, minimumSpanMinutes: fixture.minimumSpanMinutes, failures }));
