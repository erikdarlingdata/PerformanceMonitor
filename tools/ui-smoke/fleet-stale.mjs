// The screenshot stage's stale-wait decision (shots.mjs), on one /api/fleet answer. No dependencies, so a test can feed it fixture
// card lists under plain node.
//
// "Stale" is a BAND, not a threshold: it opens 2 minutes after a server's last collection and closes at 30 (ServerHealthBands.cs,
// where the card turns Offline and is_online goes false). So the wait has two ways to fail, and each names its own cause: the cards
// are already past the band (the seed anchor aged past 30 minutes before the shots started), or they never reached it.

/** Judge one fleet answer. `cards` is fleet.cards. Returns { verdict, seen, problem }: verdict is "ready" when every card is
 *  collection stale, "problem" (with the text) when a card reads Offline, else "wait". `seen` is the one-line count for the log. */
export function judgeFleetStale(cards) {
  const stale = cards.filter((c) => c.collection_stale === true).length;
  const offline = cards.filter((c) => c.is_online === false).length;
  const seen = `${cards.length} cards, ${stale} stale, ${offline} offline`;
  if (cards.length > 0 && stale === cards.length) return { verdict: "ready", seen };
  if (offline > 0) {
    return {
      verdict: "problem",
      seen,
      problem: `screenshots: the fleet is already past collection stale (${seen}); a card reads Offline either 30 minutes after the seed anchor (the run took too long: shorten it or re-seed) or from the first poll when seed.sql wrote it no collection_log row (#7569).`,
    };
  }
  return { verdict: "wait", seen };
}
