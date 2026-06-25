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
        Assert.True((await cd.EvaluateAsync("1", "TempDB Space", new List<AlertIncident>(), Window)).ShouldSend);
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
