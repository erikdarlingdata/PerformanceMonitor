using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3941: the store's shared baseline tier, Lite's twin of Darling's. The scheduler and the Recommendations tab build a
/// fresh <see cref="AnalysisService"/> per pass, so the provider's bucket cache died with the pass and every pass
/// recomputed every 30-day baseline. These pin when a shared entry answers (hour, TTL, success only), that its answer is
/// the fresh one, and the wiring that hands the store's ONE tier to every production site.
///
/// <para>Non-parallel: the tier's liveness reads <see cref="BaselineProvider.CacheTtl"/>, a static the other baseline
/// classes set to a millisecond in their constructors to defeat caching. This class holds it at an hour for its run and
/// puts it back, which is only safe with nothing running beside it.</para>
/// </summary>
[Collection("lite-baseline-shared-tier")]
public sealed class SharedBaselineCacheTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -3941_02;

    /* A Wednesday hour whose 30-day window lies inside the seed, with four earlier Wednesdays in it. */
    private static readonly DateTime Hour = new(2026, 3, 18, 10, 0, 0);

    private readonly DuckDbInitializer _duckDb;
    private readonly TimeSpan _previousTtl;

    public SharedBaselineCacheTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _previousTtl = BaselineProvider.CacheTtl;
        BaselineProvider.CacheTtl = TimeSpan.FromHours(1);
    }

    public void Dispose() => BaselineProvider.CacheTtl = _previousTtl;

    private static BaselineProvider.CachedBaseline Entry(DateTime analysisHour, DateTime realTime) => new()
    {
        ComputedAt = analysisHour,
        RealTime = realTime,
        Buckets = new Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>(),
        Clock = LocalClockWindow.Utc(analysisHour),
    };

    [Fact]
    public void AnEntry_AnswersOnlyItsOwnHour_AndOnlyInsideTheTtl_AndTheSweepDropsTheDead()
    {
        var cache = new BaselineCache();
        var now = DateTime.UtcNow;
        var entry = Entry(Hour, now);
        cache.Put(1, "1:cpu", entry);

        Assert.True(cache.TryGet(1, "1:cpu", Hour, out var hit));
        Assert.Same(entry, hit);
        Assert.False(cache.TryGet(1, "1:cpu", Hour.AddHours(1), out _));
        Assert.False(cache.TryGet(2, "1:cpu", Hour, out _));
        Assert.False(cache.TryGet(1, "1:waits", Hour, out _));

        cache.Put(1, "1:io", Entry(Hour, now - BaselineProvider.CacheTtl - TimeSpan.FromSeconds(1)));
        Assert.False(cache.TryGet(1, "1:io", Hour, out _));

        cache.SweepIfDue(now);
        Assert.Equal(2, cache.Count);
        cache.SweepIfDue(now + BaselineProvider.CacheTtl / 3);
        Assert.Equal(1, cache.Count);

        cache.Invalidate(1);
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public void TheTier_IsOnePerStore()
    {
        Assert.Same(BaselineCache.For(_duckDb), BaselineCache.For(_duckDb));
        var other = new DuckDbInitializer(Path.Combine(Path.GetTempPath(), "LiteTests_3941_" + Guid.NewGuid().ToString("N")[..8], "other.duckdb"));
        Assert.NotSame(BaselineCache.For(_duckDb), BaselineCache.For(other));
    }

    [Fact]
    public async Task TwoInstantsInOneAnalysisHour_ComputeTheSameBaseline()
    {
        await SeedCpuAsync();

        /* Two PRIVATE providers, so no cache is involved: the window itself must end on the hour. Before #3941 the
           later instant's window held the ten rows between the hour and it, all in the looked-up bucket. */
        var early = await new BaselineProvider(_duckDb).GetBaselineAsync(ServerId, MetricNames.Cpu, Hour.AddMinutes(5));
        var late = await new BaselineProvider(_duckDb).GetBaselineAsync(ServerId, MetricNames.Cpu, Hour.AddMinutes(50));

        Assert.Equal(BaselineTier.Full, early.Tier);
        Assert.Equal(48L, early.SampleCount);   // four earlier Wednesdays × twelve rows in the 10h
        AssertSameBucket(early, late);
    }

    /// <summary>
    /// A second provider in the same hour takes the first one's compute — and that compute IS the fresh answer, so it
    /// changes nothing. Then the store changes under the tier: the shared entry keeps answering (it read nothing) while
    /// a private provider sees the change, which is the proof the hit did not touch the store.
    /// </summary>
    [Fact]
    public async Task ASharedEntry_IsTheFreshAnswer_AndReadsNothing()
    {
        await SeedCpuAsync();
        var shared = new BaselineCache();

        await new BaselineProvider(_duckDb, sharedCache: shared).GetBaselineAsync(ServerId, MetricNames.Cpu, Hour.AddMinutes(5));
        var fresh = await new BaselineProvider(_duckDb).GetBaselineAsync(ServerId, MetricNames.Cpu, Hour.AddMinutes(50));
        var hit = await new BaselineProvider(_duckDb, sharedCache: shared).GetBaselineAsync(ServerId, MetricNames.Cpu, Hour.AddMinutes(50));
        AssertSameBucket(fresh, hit);

        await InsertCpuAsync(Hour.AddDays(-7).AddMinutes(1), 99);

        var stillShared = await new BaselineProvider(_duckDb, sharedCache: shared).GetBaselineAsync(ServerId, MetricNames.Cpu, Hour.AddMinutes(50));
        var changed = await new BaselineProvider(_duckDb).GetBaselineAsync(ServerId, MetricNames.Cpu, Hour.AddMinutes(50));
        AssertSameBucket(fresh, stillShared);
        Assert.Equal(fresh.SampleCount + 1, changed.SampleCount);

        /* The next analysis hour is another window: computed, and it sees the new row. */
        var nextHour = await new BaselineProvider(_duckDb, sharedCache: shared).GetBaselineAsync(ServerId, MetricNames.Cpu, Hour.AddMinutes(65));
        Assert.True(nextHour.SampleCount > 0);
    }

    [Fact]
    public async Task ACompute_WithNoBuckets_IsNotShared()
    {
        await SeedCpuAsync();
        var shared = new BaselineCache();

        var none = await new BaselineProvider(_duckDb, sharedCache: shared).GetBaselineAsync(ServerId, "no_such_metric", Hour.AddMinutes(5));
        Assert.Equal(0L, none.SampleCount);
        Assert.Equal(0, shared.Count);

        await new BaselineProvider(_duckDb, sharedCache: shared).GetBaselineAsync(ServerId, MetricNames.Cpu, Hour.AddMinutes(5));
        Assert.Equal(1, shared.Count);
    }

    /// <summary>
    /// Every production site that builds a baseline provider hands it the store's tier — the scheduler's per-pass
    /// service, the MCP host's, the Recommendations tab's and the overview lanes' — or a site that forgot would keep a
    /// private cache and quietly reopen the gap. Comments are stripped, so the prose explaining the wiring may name it.
    /// </summary>
    [Fact]
    public void EveryProductionSite_HandsItsProviderTheStoresTier()
    {
        foreach (var file in new[]
                 {
                     @"Lite\Services\CollectionBackgroundService.cs",
                     @"Lite\Mcp\McpHostService.cs",
                     @"Lite\Controls\RecommendationsTab.xaml.cs",
                     @"Lite\Services\LocalDataService.Baselines.cs",
                 })
        {
            var code = StripComments(File.ReadAllText(Path.Combine(RepoRoot(), file)));
            Assert.True(code.Contains("BaselineCache.For(_duckDb)", StringComparison.Ordinal), $"{file} builds its analysis without the store's baseline tier");
        }

        /* The one AnalysisService built for cleanup alone reads no baseline, and is the only other construction site. */
        var scheduler = StripComments(File.ReadAllText(Path.Combine(RepoRoot(), @"Lite\Services\CollectionBackgroundService.cs")));
        Assert.Equal(2, Regex.Matches(scheduler, @"new AnalysisService\(").Count);
        Assert.Contains("new AnalysisService(_duckDb).CleanupAsync(", scheduler, StringComparison.Ordinal);
    }

    /* ───────────────────────── helpers ───────────────────────── */

    private static void AssertSameBucket(BaselineBucket expected, BaselineBucket actual)
    {
        Assert.Equal(expected.Tier, actual.Tier);
        Assert.Equal((expected.HourOfDay, expected.DayOfWeek), (actual.HourOfDay, actual.DayOfWeek));
        Assert.Equal(expected.SampleCount, actual.SampleCount);
        Assert.Equal(expected.DistinctDays, actual.DistinctDays);
        Assert.Equal(expected.Median, actual.Median);
        Assert.Equal(expected.Mad, actual.Mad);
        Assert.Equal(expected.Mean, actual.Mean, 9);
        Assert.Equal(expected.StdDev, actual.StdDev, 9);
    }

    /// <summary>31 days of five-minute CPU rows ending two hours after <see cref="Hour"/>, in one statement.</summary>
    private async Task SeedCpuAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var conn = _duckDb.CreateConnection();
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO cpu_utilization_stats
            (collection_id, collection_time, server_id, server_name, sample_time,
             sqlserver_cpu_utilization, other_process_cpu_utilization)
            SELECT -1000000 - n, $1 + to_minutes(n * 5), $2, 'TestServer', $1 + to_minutes(n * 5), 40 + (n % 7), 2
            FROM range(0, $3) AS t(n)";
        cmd.Parameters.Add(new DuckDBParameter { Value = Hour.AddDays(-31) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = (31 * 24 + 2) * 12L });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task InsertCpuAsync(DateTime at, int cpu)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var conn = _duckDb.CreateConnection();
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO cpu_utilization_stats
            (collection_id, collection_time, server_id, server_name, sample_time,
             sqlserver_cpu_utilization, other_process_cpu_utilization)
            VALUES (-2000001, $1, $2, 'TestServer', $1, $3, 2)";
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = cpu });
        await cmd.ExecuteNonQueryAsync();
    }

    private static string StripComments(string source) =>
        Regex.Replace(source, @"/\*.*?\*/|//[^\r\n]*", string.Empty, RegexOptions.Singleline);

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}

[CollectionDefinition("lite-baseline-shared-tier", DisableParallelization = true)]
public sealed class LiteBaselineSharedTierCollection
{
}
