// The screenshot stage's stale-wait decision (shots.mjs), on one /api/fleet answer. No dependencies, so a test can feed it fixture
// card lists under plain node (UiSmokeFleetStaleWaitTests).
//
// "Stale" is a BAND, not a threshold: it opens 2 minutes after a server's last collection and closes at 30 (ServerHealthBands.cs,
// where the card turns Offline and is_online goes false). One server in the smoke config is seeded as unreachable on purpose (#7590;
// pages.json names it under "expectedOffline"): its card reads Offline from the first poll and stays there, so it is judged on its own
// and never counted as the fleet having aged past the band (#8069). The wait then has three ways to fail, and each names its own cause.

/** Judge one fleet answer. `cards` is fleet.cards; `expectedOffline` holds the config names of the servers seed.sql takes down.
 *  Returns { verdict, seen, problem }: "ready" when every other card is collection stale and every expected-Offline card is listed and
 *  reads Offline; "problem" (with the text) when a card not seeded down reads Offline, or a seeded-down card does not; else "wait".
 *  `seen` is the one-line count for the log. */
export function judgeFleetStale(cards, expectedOffline = []) {
  const down = new Set(expectedOffline);
  const watched = cards.filter((c) => !down.has(c.display_name));
  const seededDown = cards.filter((c) => down.has(c.display_name));
  const stale = watched.filter((c) => c.collection_stale === true).length;
  const offline = watched.filter((c) => c.is_online === false).length;
  const downOnline = seededDown.filter((c) => c.is_online !== false).map((c) => c.display_name);
  const seen =
    down.size === 0
      ? `${cards.length} cards, ${stale} stale, ${offline} offline`
      : `${cards.length} cards, ${stale} stale, ${offline} offline, ${seededDown.length} of ${down.size} seeded down and ${seededDown.length - downOnline.length} Offline`;
  if (offline > 0) {
    return {
      verdict: "problem",
      seen,
      problem: `screenshots: a card not seeded down reads Offline (${seen}); a card reads Offline 30 minutes after the seed anchor (the run took too long: shorten it or re-seed) or from the first poll when seed.sql wrote its server no collection_log row.`,
    };
  }
  if (downOnline.length > 0) {
    return {
      verdict: "problem",
      seen,
      problem: `screenshots: ${downOnline.join(", ")} is seeded as unreachable (pages.json expectedOffline) but its card does not read Offline (${seen}); the stored failed connect seed.sql writes for it did not land.`,
    };
  }
  const everyDownListed = [...down].every((name) => cards.some((c) => c.display_name === name));
  if (watched.length > 0 && stale === watched.length && everyDownListed) return { verdict: "ready", seen };
  return { verdict: "wait", seen };
}
