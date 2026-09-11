using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #1154: the shared per-incident-fingerprint cooldown. Replaces the old per-(serverId, metricName)
/// throttle that silently dropped a genuinely distinct incident arriving inside the window. These
/// tests pin the send decision (send if ANY candidate key is fresh; stamp ALL on success), the
/// metric-level fallback for non-fingerprinted alerts, the per-fingerprint restart seed, and the
/// 2×-window eviction bound. Windows are large (minutes) so the real-clock math is deterministic
/// within a sub-second test run; the one eviction test uses a tiny window with a generous delay.
/// </summary>
public class IncidentCooldownTests
{
    private static readonly TimeSpan Window = TimeSpan.FromMinutes(15);

    private static AlertIncident Incident(string dedupKey) =>
        new(dedupKey, new[] { "db.dbo." + dedupKey });

    private static IReadOnlyList<AlertIncident> Incidents(params string[] keys) =>
        keys.Select(Incident).ToList();

    /// <summary>A cooldown with no restart seed (history returns null for every key).</summary>
    private static IncidentCooldown NoSeed() =>
        new(keyPrefix: "", seedLastSentUtc: (_, _, _) => Task.FromResult<DateTime?>(null));

    [Fact]
    public async Task DistinctFingerprints_BothFresh_SendsWithOneKeyPerFingerprint()
    {
        var cd = NoSeed();
        var d = await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("A", "B"), Window);

        Assert.True(d.ShouldSend);
        Assert.Equal(2, d.Keys.Count);
        Assert.Equal(2, d.Keys.Distinct().Count());
    }

    [Fact]
    public async Task SameFingerprintRepeat_WithinWindow_Suppressed()
    {
        var cd = NoSeed();

        var first = await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("A"), Window);
        Assert.True(first.ShouldSend);
        cd.Stamp(first);

        var second = await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("A"), Window);
        Assert.False(second.ShouldSend);   // #1091: a re-fire of the same fingerprint stays suppressed
    }

    [Fact]
    public async Task DistinctFingerprint_WithinWindowOfAnother_StillSends()
    {
        // gotqn's #1154 repro: A delivered, then a DIFFERENT incident B arrives in the window.
        var cd = NoSeed();

        var a = await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("A"), Window);
        cd.Stamp(a);

        var b = await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("B"), Window);
        Assert.True(b.ShouldSend);         // B is not throttled by A's unrelated cooldown
    }

    [Fact]
    public async Task SummaryWithMixedFreshness_SendsThenStampAllSuppressesEither()
    {
        var cd = NoSeed();

        var a = await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("A"), Window);
        cd.Stamp(a);

        // Summary {A (in cooldown), B (fresh)} -> send because B is fresh; stamp BOTH.
        var summary = await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("A", "B"), Window);
        Assert.True(summary.ShouldSend);
        cd.Stamp(summary);

        // Now A and B are both freshly stamped -> either one alone is suppressed.
        Assert.False((await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("A"), Window)).ShouldSend);
        Assert.False((await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("B"), Window)).ShouldSend);
    }

    [Fact]
    public async Task AllIncidentsInCooldown_Suppressed()
    {
        var cd = NoSeed();
        var first = await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("A", "B"), Window);
        cd.Stamp(first);

        var again = await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("A", "B"), Window);
        Assert.False(again.ShouldSend);
    }

    [Fact]
    public async Task NonFingerprinted_FallsBackToMetricKey_LikePre1154()
    {
        var cd = NoSeed();

        // No incidents -> single metric-level key.
        var cpu = await cd.EvaluateAsync("1", "High CPU", null, Window);
        Assert.True(cpu.ShouldSend);
        Assert.Single(cpu.Keys);
        cd.Stamp(cpu);

        // Same metric within window -> suppressed (today's behavior).
        Assert.False((await cd.EvaluateAsync("1", "High CPU", null, Window)).ShouldSend);
        // A different metric is its own key -> fresh.
        Assert.True((await cd.EvaluateAsync("1", "tempdb Space", new List<AlertIncident>(), Window)).ShouldSend);
        // A different server is its own key -> fresh.
        Assert.True((await cd.EvaluateAsync("2", "High CPU", null, Window)).ShouldSend);
    }

    [Fact]
    public async Task Seed_WithinWindow_Suppresses_OlderThanWindow_Sends()
    {
        // Seed returns "just now" for A and an old time for B.
        var cd = new IncidentCooldown("", (_, _, dedupKey) => Task.FromResult<DateTime?>(
            dedupKey == "A" ? DateTime.UtcNow
            : dedupKey == "B" ? DateTime.UtcNow - TimeSpan.FromMinutes(17)
            : null));

        Assert.False((await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("A"), Window)).ShouldSend); // seeded in-window
        Assert.True((await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("B"), Window)).ShouldSend);  // seeded but stale
        Assert.True((await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("C"), Window)).ShouldSend);  // never sent
    }

    [Fact]
    public async Task NullSeedDelegate_NeverSeeds_FirstTouchAlwaysFresh()
    {
        // Mirrors the webhook null-store path: no seeding, pure in-memory.
        var cd = new IncidentCooldown("webhook:", seedLastSentUtc: null);

        var d = await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("A"), Window);
        Assert.True(d.ShouldSend);
    }

    [Fact]
    public async Task FailedSend_NotStamped_DoesNotStartCooldown()
    {
        var cd = NoSeed();

        // Evaluate but do NOT stamp (simulating a failed send).
        var d = await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("A"), Window);
        Assert.True(d.ShouldSend);

        // Next evaluation still sends — a failed send must not consume the cooldown.
        Assert.True((await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("A"), Window)).ShouldSend);
    }

    /* ─────────────── #3313: the per-fingerprint verdicts, not only their reduction ─────────────── */

    /// <summary>
    /// #3313: the decision reports WHICH fingerprints were outside their own window, not only that one of
    /// them was. The "send if ANY is fresh" reduction above is the right answer to "does this post" and was
    /// the only answer available, so the channel builders rendered the whole incident set — including the
    /// ones still inside their own window, already delivered minutes earlier.
    ///
    /// <para>Reported as a member of the same decision rather than left to a second call: the cooldown
    /// evicts, seeds from history and stamps, so a render evaluating freshness for itself would not be
    /// asking the same question the send decision answered. Consumed by
    /// <c>IncidentDeliveryFilter.ForDelivery</c>; the render side is pinned in
    /// <c>Darling.Tests.IncidentDeliveryFilterTests</c>.</para>
    /// </summary>
    [Fact]
    public async Task Decision_NamesOnlyTheFingerprintsOutsideTheirOwnWindow()
    {
        var cd = NoSeed();

        var a = await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("A"), Window);
        cd.Stamp(a);
        Assert.Equal(new[] { "A" }, a.DeliverableDedupKeys);

        // Summary {A (in cooldown), B (fresh)}: posts because B is fresh, but only B is deliverable.
        var summary = await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("A", "B"), Window);
        Assert.True(summary.ShouldSend);
        Assert.Equal(2, summary.Keys.Count);           // both keys are still stamped on success (#1154)
        Assert.Equal(new[] { "B" }, summary.DeliverableDedupKeys);

        /* And with the fresh one FIRST. Both members are reductions over an ordered key list, so a
           last-one-wins or first-one-wins reduction is right on exactly one of the two orders — measured,
           not theoretical: writing this assertion is what caught a mutation of the ShouldSend reduction
           that the single-order form above passed. */
        var reversed = await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("B", "A"), Window);
        Assert.True(reversed.ShouldSend);
        Assert.Equal(new[] { "B" }, reversed.DeliverableDedupKeys);
    }

    /// <summary>
    /// The metric-level fallback reports null, not an empty list. Empty would read as "no incident is
    /// deliverable" and blank every CPU / memory / poison-wait / tempdb / failed-job card, plus the #2109 AG
    /// database alerts — none of which carries a fingerprint at all.
    /// </summary>
    [Fact]
    public async Task Decision_ReportsNoDeliverableSet_WhenThereAreNoFingerprints()
    {
        var cd = NoSeed();

        Assert.Null((await cd.EvaluateAsync("1", "High CPU", null, Window)).DeliverableDedupKeys);
        Assert.Null((await cd.EvaluateAsync("1", "High CPU", new List<AlertIncident>(), Window)).DeliverableDedupKeys);
    }

    /// <summary>
    /// A stamped-and-suppressed alert reports an EMPTY deliverable set, which is the state the null above
    /// has to stay distinguishable from. Unreachable through a send (ShouldSend is false, so nothing
    /// renders), and pinned for exactly that reason: it is the only pairing of the two members a renderer
    /// must never see, so nothing else would notice the two collapsing into one value.
    /// </summary>
    [Fact]
    public async Task Decision_ReportsAnEmptyDeliverableSet_WhenEveryFingerprintIsSuppressed()
    {
        var cd = NoSeed();

        var first = await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("A", "B"), Window);
        cd.Stamp(first);

        var again = await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("A", "B"), Window);
        Assert.False(again.ShouldSend);
        Assert.NotNull(again.DeliverableDedupKeys);
        Assert.Empty(again.DeliverableDedupKeys!);
    }

    [Fact]
    public async Task Eviction_DropsKeysPastTwiceWindow_KeepsDictBounded()
    {
        var window = TimeSpan.FromMilliseconds(50);
        var cd = NoSeed();

        var a = await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("A"), window);
        cd.Stamp(a);
        Assert.Equal(1, cd.TrackedKeyCount);

        await Task.Delay(250, TestContext.Current.CancellationToken); // > 2x window: A is now evictable

        var b = await cd.EvaluateAsync("1", "Deadlocks Detected", Incidents("B"), window);
        cd.Stamp(b);

        // A was pruned at the top of the second Evaluate, so only B remains (not 2).
        Assert.Equal(1, cd.TrackedKeyCount);
    }
}
