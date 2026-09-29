using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4732: the collection service's housekeeping jobs (the Query Store backfill, archival, retention, the two cleanups
/// and analysis) decided whether they were due from the time elapsed since their last run, so a wall clock that stepped
/// backwards made the elapsed time negative and each job waited out the step. They now decide through
/// <see cref="CollectionBackgroundService.HousekeepingIsDue"/>: a last run ahead of the clock counts as due, and every
/// other case decides exactly as before.
/// </summary>
public sealed class HousekeepingClockStepTests
{
    private static readonly DateTime Now = new(2026, 9, 29, 12, 0, 0, DateTimeKind.Utc);

    private const string Backfill = "query store backfill";
    private const string Archive = "archive";
    private const string Retention = "retention";
    private const string FindingsCleanup = "findings cleanup";
    private const string DismissedAlertsCleanup = "dismissed alerts cleanup";
    private const string Analysis = "analysis";

    /// <summary>Each job's interval as the service declares it. Analysis reads a setting at the call, so it gets an hour here
    /// and the census below pins that its site passes that setting.</summary>
    private static TimeSpan IntervalOf(string job) => job switch
    {
        Backfill => CollectionBackgroundService.QueryStoreBackfillInterval,
        Archive => CollectionBackgroundService.ArchiveInterval,
        Retention => CollectionBackgroundService.RetentionInterval,
        FindingsCleanup => CollectionBackgroundService.FindingsCleanupInterval,
        DismissedAlertsCleanup => CollectionBackgroundService.DismissedAlertsCleanupInterval,
        Analysis => TimeSpan.FromMinutes(60),
        _ => throw new ArgumentOutOfRangeException(nameof(job)),
    };

    [Theory]
    [InlineData(Backfill)]
    [InlineData(Archive)]
    [InlineData(Retention)]
    [InlineData(FindingsCleanup)]
    [InlineData(DismissedAlertsCleanup)]
    [InlineData(Analysis)]
    public void ALastRunTenMinutesAhead_IsDue_AndOneThirtySecondsAgoIsNot(string job)
    {
        var interval = IntervalOf(job);
        Assert.True(interval > TimeSpan.FromSeconds(30));

        Assert.True(CollectionBackgroundService.HousekeepingIsDue(Now.AddMinutes(10), interval, Now), "a last run ahead of the clock must not wait out the step");
        Assert.False(CollectionBackgroundService.HousekeepingIsDue(Now.AddSeconds(-30), interval, Now), "a run 30 seconds ago is not due for a longer interval");
    }

    [Theory]
    [InlineData(Backfill)]
    [InlineData(Archive)]
    [InlineData(Retention)]
    [InlineData(FindingsCleanup)]
    [InlineData(DismissedAlertsCleanup)]
    [InlineData(Analysis)]
    public void ALastRunAtOrBeforeNow_DecidesExactlyAsTheElapsedTimeDid(string job)
    {
        var interval = IntervalOf(job);

        /* From "just ran" out to two intervals ago, and the never-ran stamp: the old test was now - last >= interval. */
        for (var seconds = 0; seconds <= 2 * (int)interval.TotalSeconds; seconds += 37)
        {
            var last = Now.AddSeconds(-seconds);
            Assert.Equal(Now - last >= interval, CollectionBackgroundService.HousekeepingIsDue(last, interval, Now));
        }

        Assert.False(CollectionBackgroundService.HousekeepingIsDue(Now, interval, Now));
        Assert.False(CollectionBackgroundService.HousekeepingIsDue(Now - interval + TimeSpan.FromTicks(1), interval, Now));
        Assert.True(CollectionBackgroundService.HousekeepingIsDue(Now - interval, interval, Now));
        Assert.True(CollectionBackgroundService.HousekeepingIsDue(DateTime.MinValue, interval, Now));
    }

    [Theory]
    [InlineData(Backfill)]
    [InlineData(Archive)]
    [InlineData(Retention)]
    [InlineData(FindingsCleanup)]
    [InlineData(DismissedAlertsCleanup)]
    [InlineData(Analysis)]
    public void AJob_AfterABackwardStep_RunsOnceAndThenWaitsItsIntervalAgain(string job)
    {
        var interval = IntervalOf(job);
        var ranAt = Now;
        var steppedBack = Now.AddMinutes(-10);

        Assert.True(CollectionBackgroundService.HousekeepingIsDue(ranAt, interval, steppedBack));

        /* The run stamps the clock's new reading, so the next run is a full interval away, not a run of catch-ups. */
        ranAt = steppedBack;
        Assert.False(CollectionBackgroundService.HousekeepingIsDue(ranAt, interval, steppedBack.AddSeconds(30)));
        Assert.True(CollectionBackgroundService.HousekeepingIsDue(ranAt, interval, steppedBack + interval));
    }

    [Fact]
    public void EveryJobInTheService_DecidesThroughTheSharedFunction_WithItsOwnIntervalAndStamp()
    {
        var source = ReadRepoFile("Lite/Services/CollectionBackgroundService.cs");

        Assert.Empty(Regex.Matches(source, @"DateTime\.UtcNow\s*-\s*_last\w+\s*[<>]=?").Select(m => m.Value));

        Assert.Equal(1, CountOf(source, "!HousekeepingIsDue(_lastQueryStoreBackfill, QueryStoreBackfillInterval, DateTime.UtcNow)"));
        Assert.Equal(1, CountOf(source, "HousekeepingIsDue(_lastArchiveTime, ArchiveInterval, DateTime.UtcNow)"));
        Assert.Equal(1, CountOf(source, "!HousekeepingIsDue(_lastRetentionTime, RetentionInterval, DateTime.UtcNow)"));
        Assert.Equal(1, CountOf(source, "!HousekeepingIsDue(_lastFindingsCleanupTime, FindingsCleanupInterval, DateTime.UtcNow)"));
        Assert.Equal(1, CountOf(source, "!HousekeepingIsDue(_lastDismissedAlertsCleanupTime, DismissedAlertsCleanupInterval, DateTime.UtcNow)"));
        Assert.Equal(1, CountOf(source, "!HousekeepingIsDue(_lastAnalysisTime, TimeSpan.FromMinutes(App.AnalysisIntervalMinutes), DateTime.UtcNow)"));

        /* One function, and it is the same rule the collector schedule applies to lastRun + interval. */
        Assert.Contains("CollectorCadence.IntervalElapsed(lastRunUtc, nowUtc, interval)", source, StringComparison.Ordinal);
    }

    [Fact]
    public void TheRetryAndRecheckThrottles_DecideThroughTheSameRule()
    {
        var remote = ReadRepoFile("Lite/Services/RemoteCollectorService.cs");
        Assert.DoesNotContain("DateTime.UtcNow - deniedAt <", remote, StringComparison.Ordinal);
        Assert.Contains("!CollectorCadence.IntervalElapsed(deniedAt, DateTime.UtcNow, AzureMasterRecheckInterval)", remote, StringComparison.Ordinal);

        var blockedProcess = ReadRepoFile("Lite/Services/RemoteCollectorService.BlockedProcessReport.cs");
        Assert.DoesNotContain("DateTime.UtcNow - gaveUpAtUtc <", blockedProcess, StringComparison.Ordinal);
        Assert.Contains("!CollectorCadence.IntervalElapsed(gaveUpAtUtc, DateTime.UtcNow, XeSessionRecreateRetryCooldown)", blockedProcess, StringComparison.Ordinal);
    }

    private static int CountOf(string text, string needle) =>
        Regex.Matches(text, Regex.Escape(needle)).Count;

    /* Locate the repo from this file — the DarlingLockTimeoutYieldTests idiom; no build-output copying. */
    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
