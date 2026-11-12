/* #8069: runs the web UI smoke's stale-wait decision (tools/ui-smoke/fleet-stale.mjs, the judge shots.mjs applies to every /api/fleet
   answer before the screenshots) over fixture card lists, and prints as one line of JSON the verdict each one got. UiSmokeFleetStaleWaitTests
   starts it as
       node ui-smoke-fleet-stale-harness.mjs <path to tools/ui-smoke>
   The expected-Offline set comes from that folder's pages.json ("expectedOffline"), the same place shots.mjs reads it, so the
   test pins the config and the decision together. Each case may name its own set instead. */
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

const smokeDir = process.argv[process.argv.length - 1];
const pages = JSON.parse(fs.readFileSync(path.join(smokeDir, "pages.json"), "utf8"));
const configured = pages.expectedOffline || [];
const { judgeFleetStale } = await import(pathToFileURL(path.join(smokeDir, "fleet-stale.mjs")).href);

/* The four cards the smoke fleet shows, in the shape /api/fleet answers them: three servers the seed leaves with a collection at the
   anchor, which age through stale, and one the seed takes down (a stored failed connect, #7590), which reads Offline from the first poll. */
const card = (display_name, { stale = false, online = true } = {}) => ({ display_name, server_name: display_name + "-host", collection_stale: stale, is_online: online });
const DOWN = "ui-down-sql";
const LONG = "ui-long-name-sql-server";
const staleThree = () => [card("ui-sql", { stale: true }), card("ui-pg", { stale: true }), card(LONG, { stale: true })];
const freshThree = () => [card("ui-sql"), card("ui-pg"), card(LONG)];

const cases = {
  /* The steady state the stage waits for: every other card stale, the seeded-down card Offline. */
  seededDownOnly: { cards: [...staleThree(), card(DOWN, { online: false })] },
  /* Early polls: the band has not opened for the other three yet; the down card is already Offline, as it always is. */
  stillFresh: { cards: [...freshThree(), card(DOWN, { online: false })] },
  /* A card the seed did not take down reads Offline: the anchor aged past 30 minutes, or its server got no collection_log row. */
  otherOffline: { cards: [card("ui-sql", { online: false }), card("ui-pg", { stale: true }), card(LONG, { stale: true }), card(DOWN, { online: false })] },
  /* The seeded-down card reads Online: the seed's stored failed connect did not land. */
  downNotOffline: { cards: [...staleThree(), card(DOWN)] },
  /* The fleet does not list the seeded-down card yet. */
  downMissing: { cards: staleThree() },
  /* No server is seeded down: the decision as it was before there was one. */
  noneExpectedAllStale: { expectedOffline: [], cards: staleThree() },
  noneExpectedOneOffline: { expectedOffline: [], cards: [card("ui-sql", { online: false }), card("ui-pg", { stale: true })] },
};

const out = { expectedOffline: configured, cases: {} };
for (const [name, c] of Object.entries(cases)) {
  out.cases[name] = judgeFleetStale(c.cards, c.expectedOffline ?? configured);
}
process.stdout.write(JSON.stringify(out));
