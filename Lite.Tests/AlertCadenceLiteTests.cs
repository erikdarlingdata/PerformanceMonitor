/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/* #5642 lane L: Lite's half of the alert-cadence fix. Five classes, one per behaviour:
   the age line through the deliverer, the history store and its readers; the recent CPU sample list; the
   restart/reconnect stamp; the adapter's new sample times; and the pileup's own cadence. */

/// <summary>
/// The sample-age line reaches Lite's deliverer unchanged from the context the engine stamped at fire time
/// (#5642): appended once, never twice, and absent when the context carries no sample.
/// </summary>
public sealed class LiteSampleAgeDeliveryTests
{
    private static readonly DateTime Instant = new(2026, 7, 1, 15, 43, 29, DateTimeKind.Utc);

    private static AlertContext Stamped(string source = "CPU", int ageSeconds = 320) =>
        new() { NewestSample = new AlertNewestSample(source, Instant, TimeSpan.FromSeconds(ageSeconds)) };

    private static AlertOutcome Outcome(string metric, AlertContext? context, string? detail) =>
        new("1", "SQL01", metric, "97%", "90%", context, detail, 97, 90, false, null, "short");

    private static (LiteAlertDeliverer Deliverer, List<string?> Details) Build()
    {
        var details = new List<string?>();
        var deliverer = new LiteAlertDeliverer(
            (_, _, _, _, _) => { },
            (metricName, serverName, currentValue, thresholdValue, serverId, context, numCur, numThr, muted, detailText, mode, _) =>
            {
                details.Add(detailText);
                return Task.FromResult<AlertDelivery?>(null);
            },
            _ => null);
        return (deliverer, details);
    }

    [Fact]
    public async Task TheStampedAgeIsAppendedToTheDetailTextOnce()
    {
        var (deliverer, details) = Build();

        await deliverer.DeliverAndReportAsync(Outcome("High CPU", Stamped(), "Total CPU at 97%"), TestContext.Current.CancellationToken);

        var detail = Assert.Single(details);
        Assert.Equal("Total CPU at 97%\n\nNewest CPU sample: 15:43:29 UTC, 5m 20s old", detail);
    }

    [Fact]
    public async Task ATextThatAlreadyCarriesTheLineIsNotGivenASecondOne()
    {
        var (deliverer, details) = Build();
        var already = "Total CPU at 97%\n\nNewest CPU sample: 15:43:29 UTC, 5m 20s old";

        await deliverer.DeliverAndReportAsync(Outcome("High CPU", Stamped(), already), TestContext.Current.CancellationToken);

        var detail = Assert.Single(details);
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(detail!, "Newest CPU sample"));
    }

    [Fact]
    public async Task NoSampleMeansNoLineAndTheTextIsUntouched()
    {
        var (deliverer, details) = Build();

        await deliverer.DeliverAndReportAsync(Outcome("Failed Jobs", new AlertContext(), "Job failed"), TestContext.Current.CancellationToken);
        await deliverer.DeliverAndReportAsync(Outcome("Failed Jobs", null, null), TestContext.Current.CancellationToken);

        Assert.Equal("Job failed", details[0]);
        Assert.Null(details[1]);
    }

    [Fact]
    public async Task AnEmptyTextTakesTheLineAlone()
    {
        var (deliverer, details) = Build();

        await deliverer.DeliverAndReportAsync(Outcome("Low Disk", Stamped("volume", 90), null), TestContext.Current.CancellationToken);

        Assert.Equal("Newest volume sample: 15:43:29 UTC, 1m 30s old", Assert.Single(details));
    }

    [Fact]
    public void TheDelivererRoutesBothSendsThroughTheSharedLineHelper()
    {
        /* Source shape: the single send, the per-event split and the held digest row (#7086) all run the text through
           WithSampleAge, so a surface added later cannot skip it. */
        var source = File.ReadAllText(RepoFile("Lite/Services/LiteAlertDeliverer.cs"));
        Assert.Equal(3, System.Text.RegularExpressions.Regex.Matches(source, @"AlertDetailText\.WithSampleAge\(").Count);
    }

    internal static string RepoFile(string relative)
    {
        var dir = AppContext.BaseDirectory;
        while (dir != null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        return Path.Combine(dir ?? throw new InvalidOperationException("repo root not found"), relative.Replace('/', Path.DirectorySeparatorChar));
    }
}

/// <summary>
/// The history row keeps the age the engine computed at fire time and every Lite reader shows that value, read
/// off the persisted context (#5642): the store, the data service's row, the MCP read, the tab column and the
/// detail window. A row with no carrier gives no line.
/// </summary>
public sealed class LiteSampleAgeHistoryTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = 41;
    private readonly DuckDbInitializer _duckDb;

    public LiteSampleAgeHistoryTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private async Task RecordAsync(string metric, AlertContext? context, string? detail)
    {
        var store = new DuckDbAlertHistoryStore(_duckDb);
        await store.RecordAlertAsync(new AlertHistoryRecord(
            ServerId.ToString(), "SQL01", metric, "97%", "90%", 97, 90, AlertDelivery.NoChannelApplies(), false,
            detail, context is null ? null : AlertContextSerializer.Serialize(context)));
    }

    [Fact]
    public async Task TheRowReadsBackTheAgeStampedAtFireTime()
    {
        var instant = new DateTime(2026, 7, 1, 15, 43, 29, DateTimeKind.Utc);
        var context = new AlertContext { NewestSample = new AlertNewestSample("CPU", instant, TimeSpan.FromSeconds(320)) };
        await RecordAsync("High CPU", context, AlertDetailText.WithSampleAge("Total CPU at 97%", context));
        await RecordAsync("Failed Jobs", new AlertContext(), "Job failed");

        var rows = await new LocalDataService(_duckDb).GetAlertHistoryAsync(24, 50, ServerId);

        var cpu = Assert.Single(rows, r => r.MetricName == "High CPU");
        /* The 5m 20s was fixed when the alert fired; the row was recorded just now, and the hour of the stamp is
           years back, so a value worked out from the clock at read time could not equal it. */
        Assert.Equal(TimeSpan.FromSeconds(320), cpu.NewestSample!.Age);
        Assert.Equal(instant, cpu.NewestSample.InstantUtc);
        Assert.Equal("Newest CPU sample: 15:43:29 UTC, 5m 20s old", cpu.NewestSampleDisplay);
        Assert.Contains("Newest CPU sample: 15:43:29 UTC, 5m 20s old", cpu.DetailText);

        var jobs = Assert.Single(rows, r => r.MetricName == "Failed Jobs");
        Assert.Null(jobs.NewestSample);
        Assert.Equal("", jobs.NewestSampleDisplay);
        Assert.DoesNotContain("Newest", jobs.DetailText);
    }

    [Fact]
    public void ARowWithNoContextOrNoSampleHasNoLine()
    {
        Assert.Equal("", new AlertHistoryRow { ContextJson = null }.NewestSampleDisplay);
        Assert.Equal("", new AlertHistoryRow { ContextJson = "not json" }.NewestSampleDisplay);
        Assert.Equal("", new AlertHistoryRow { ContextJson = AlertContextSerializer.Serialize(new AlertContext()) }.NewestSampleDisplay);
    }

    [Fact]
    public void TheRowRereadsWhenItsContextChanges()
    {
        var row = new AlertHistoryRow { ContextJson = null };
        Assert.Equal("", row.NewestSampleDisplay);
        row.ContextJson = AlertContextSerializer.Serialize(new AlertContext
        {
            NewestSample = new AlertNewestSample("PVS", new DateTime(2026, 7, 1, 1, 2, 3, DateTimeKind.Utc), TimeSpan.FromSeconds(61))
        });
        Assert.Equal("Newest PVS sample: 01:02:03 UTC, 1m 1s old", row.NewestSampleDisplay);
    }

    [Fact]
    public async Task TheMcpReadCarriesTheStampedAgeOrNull()
    {
        var context = new AlertContext
        {
            NewestSample = new AlertNewestSample("CPU", new DateTime(2026, 7, 1, 15, 43, 29, DateTimeKind.Utc), TimeSpan.FromSeconds(320))
        };
        await RecordAsync("High CPU", context, AlertDetailText.WithSampleAge("Total CPU at 97%", context));
        await RecordAsync("Failed Jobs", new AlertContext(), "Job failed");

        var json = await McpAlertTools.GetAlertHistory(new LocalDataService(_duckDb), 24, 50);

        using var doc = JsonDocument.Parse(json);
        var alerts = doc.RootElement.GetProperty("alerts").EnumerateArray().ToList();
        var cpu = alerts.Single(a => a.GetProperty("metric_name").GetString() == "High CPU");
        var sample = cpu.GetProperty("newest_sample");
        Assert.Equal("CPU", sample.GetProperty("source").GetString());
        Assert.Equal(320, sample.GetProperty("age_seconds").GetDouble());
        Assert.Contains("Newest CPU sample", cpu.GetProperty("detail_text").GetString());
        var jobs = alerts.Single(a => a.GetProperty("metric_name").GetString() == "Failed Jobs");
        Assert.Equal(JsonValueKind.Null, jobs.GetProperty("newest_sample").ValueKind);
    }

    /// <summary>#5654: Lite has no external alert intake, but its history rows carry Darling's external_source key
    /// (always null) so both products return one row shape.</summary>
    [Fact]
    public async Task TheMcpHistoryRowCarriesTheExternalSourceKey_AlwaysNull()
    {
        await RecordAsync("High CPU", new AlertContext(), "Total CPU at 97%");

        var json = await McpAlertTools.GetAlertHistory(new LocalDataService(_duckDb), 24, 50);

        using var doc = JsonDocument.Parse(json);
        var row = doc.RootElement.GetProperty("alerts").EnumerateArray().Single();
        Assert.Equal(JsonValueKind.Null, row.GetProperty("external_source").ValueKind);
    }

    [Fact]
    public void TheTabAndTheDetailWindowShowTheRowsPersistedLine()
    {
        var tab = File.ReadAllText(LiteSampleAgeDeliveryTests.RepoFile("Lite/Controls/AlertsHistoryTab.xaml"));
        Assert.Contains("Binding=\"{Binding NewestSampleDisplay}\"", tab);
        /* The column carries a filter button like its siblings: the grid's filter manager finds the column's value by the
           button's Tag, read as a property name of the row type, so the Tag must name a public string property. */
        Assert.Matches(@"Tag=""NewestSampleDisplay""\s+Click=""FilterButton_Click""[\s\S]{0,200}Text=""Newest Sample""", tab);
        var filterProperty = typeof(AlertHistoryRow).GetProperty("NewestSampleDisplay");
        Assert.NotNull(filterProperty);
        Assert.Equal(typeof(string), filterProperty!.PropertyType);

        var window = File.ReadAllText(LiteSampleAgeDeliveryTests.RepoFile("Lite/Windows/AlertDetailWindow.xaml.cs"));
        /* The flat-text fallback already ends in the line the engine stamped, so the window must hand it that text as the
           prose (the shared helper then returns null and the line prints once, #5642 review); and the row is decided after
           the detail branches, once DetailTextBox visibility is known. */
        Assert.Contains("AlertDetailText.SampleAgeLineFromJson(", window);
        Assert.Contains("DetailTextBox.Visibility == Visibility.Visible ? item.DetailText : null", window);
        Assert.True(window.IndexOf("SampleAgeLineFromJson(", StringComparison.Ordinal) > window.IndexOf("TryBuildDetailViews(item.ContextJson", StringComparison.Ordinal));
        Assert.DoesNotContain("item.NewestSampleDisplay", window);
        Assert.DoesNotContain("DateTime.UtcNow", window);

        /* The row reads the carrier; it never subtracts from the clock. */
        var rowSource = File.ReadAllText(LiteSampleAgeDeliveryTests.RepoFile("Lite/Services/LocalDataService.AlertHistory.cs"));
        var rowClass = rowSource[rowSource.IndexOf("public class AlertHistoryRow", StringComparison.Ordinal)..];
        Assert.DoesNotContain("DateTime.UtcNow", rowClass);
        /* One property off the persisted JSON, not a rehydrate of the whole context per grid row (#5642 review). */
        Assert.Contains("AlertContextSerializer.TryReadNewestSample(ContextJson)", rowClass);
        Assert.DoesNotContain("TryDeserialize(", rowClass);
    }
}

/// <summary>
/// The recent CPU sample list the engine's persistence gate judges one at a time (#5642): oldest first, inside
/// 15 minutes, at most 32, the newest kept when there are more.
/// </summary>
public sealed class LiteRecentCpuSampleListTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = 52;
    private static readonly DateTime Now = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
    private readonly DuckDbInitializer _duckDb;

    public LiteRecentCpuSampleListTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private async Task SeedAsync(IEnumerable<(DateTime? Utc, int Sql)> samples)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        var id = -7_000_000L;
        foreach (var (utc, sql) in samples)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name, sample_time, sample_time_utc,
     sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, 'S52', $2, $4, $5, 3)";
            cmd.Parameters.Add(new DuckDBParameter { Value = id-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = (utc ?? Now).AddHours(-5) });
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)utc ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = sql });
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task TheListIsOldestFirstAndInsideFifteenMinutes()
    {
        await SeedAsync(new (DateTime?, int)[]
        {
            (Now.AddMinutes(-1), 95), (Now.AddMinutes(-20), 99), (Now.AddMinutes(-9), 91), (Now.AddMinutes(-5), 93),
            (null, 88), /* a row from before v63 has no UTC instant */
        });

        var list = await new LocalDataService(_duckDb).GetRecentCpuSamplesAsync(ServerId, Now);

        Assert.Equal(new[] { 91.0, 93.0, 95.0 }, list.Select(s => s.SqlCpuPercent!.Value).ToArray());
        Assert.Equal(new[] { Now.AddMinutes(-9), Now.AddMinutes(-5), Now.AddMinutes(-1) }, list.Select(s => s.SampleTimeUtc).ToArray());
        Assert.All(list, s => Assert.Equal(DateTimeKind.Utc, s.SampleTimeUtc.Kind));
        Assert.Equal(98.0, list[^1].TotalCpuPercent); /* SQL 95 + other 3 */
    }

    [Fact]
    public async Task MoreThanThirtyTwoKeepTheNewestThirtyTwo()
    {
        /* 40 samples, 20 seconds apart, newest 5 seconds before now: all inside 15 minutes. */
        await SeedAsync(Enumerable.Range(0, 40).Select(i => ((DateTime?)Now.AddSeconds(-5 - 20 * (39 - i)), 50 + i)));

        var list = await new LocalDataService(_duckDb).GetRecentCpuSamplesAsync(ServerId, Now);

        Assert.Equal(LocalDataService.AlertCpuSampleLimit, list.Count);
        Assert.Equal(32, list.Count);
        Assert.Equal(89.0, list[^1].SqlCpuPercent); /* the newest survives the bound */
        Assert.Equal(58.0, list[0].SqlCpuPercent);  /* the eight oldest were dropped */
        Assert.True(list.Zip(list.Skip(1), (a, b) => a.SampleTimeUtc < b.SampleTimeUtc).All(x => x));
    }

    [Fact]
    public void TheBoundsAreFifteenMinutesAndThirtyTwo()
    {
        Assert.Equal(TimeSpan.FromMinutes(15), LocalDataService.AlertCpuSampleLookback);
        Assert.Equal(32, LocalDataService.AlertCpuSampleLimit);
    }
}

/// <summary>
/// <see cref="WatchingSinceTracker"/> gives the engine a new "watching since" on an app restart and on a
/// reconnect, and keeps it steady while the server stays online (#5642).
/// </summary>
public sealed class WatchingSinceTrackerTests
{
    private static readonly DateTime T0 = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void ARepeatedOnlineReadingKeepsTheFirstValue()
    {
        var tracker = new WatchingSinceTracker();
        tracker.Observe("a", true, T0);
        tracker.Observe("a", true, T0.AddMinutes(5)); /* the connection tick and the alert sweep both call it */

        Assert.Equal(T0, tracker.Get("a"));
    }

    [Fact]
    public void AReconnectGivesANewValue()
    {
        var tracker = new WatchingSinceTracker();
        tracker.Observe("a", true, T0);
        tracker.Observe("a", false, T0.AddMinutes(1));
        Assert.Null(tracker.Get("a"));

        tracker.Observe("a", true, T0.AddMinutes(2));

        Assert.Equal(T0.AddMinutes(2), tracker.Get("a"));
    }

    [Fact]
    public void AnAppRestartGivesANewValue()
    {
        var before = new WatchingSinceTracker();
        before.Observe("a", true, T0);

        var after = new WatchingSinceTracker(); /* a restart builds a fresh tracker */
        Assert.Null(after.Get("a"));
        after.Observe("a", true, T0.AddHours(3));

        Assert.Equal(T0.AddHours(3), after.Get("a"));
        Assert.NotEqual(before.Get("a"), after.Get("a"));
    }

    [Fact]
    public void ServersAreIndependentAndForgetClearsOne()
    {
        var tracker = new WatchingSinceTracker();
        tracker.Observe("a", true, T0);
        tracker.Observe("b", true, T0.AddMinutes(1));
        tracker.Observe("a", false, T0.AddMinutes(2));

        Assert.Null(tracker.Get("a"));
        Assert.Equal(T0.AddMinutes(1), tracker.Get("b"));
        tracker.Forget("b");
        Assert.Null(tracker.Get("b"));
        Assert.Null(tracker.Get("never-seen"));
    }

    [Fact]
    public void GetOrAppStart_IsTheConnectionsValueWhenThereIsOne_ElseTheAppStart_NeverNull()
    {
        var tracker = new WatchingSinceTracker(T0);
        Assert.Equal(T0, tracker.AppStartUtc);

        // Never observed, and offline: the app start.
        Assert.Equal(T0, tracker.GetOrAppStart("never-seen"));
        tracker.Observe("a", false, T0.AddMinutes(1));
        Assert.Equal(T0, tracker.GetOrAppStart("a"));

        // Online: the connection's own value, which a repeated reading does not move.
        tracker.Observe("a", true, T0.AddMinutes(2));
        tracker.Observe("a", true, T0.AddMinutes(3));
        Assert.Equal(T0.AddMinutes(2), tracker.GetOrAppStart("a"));

        // Dropped again: back to the app start, not null.
        tracker.Observe("a", false, T0.AddMinutes(4));
        Assert.Equal(T0, tracker.GetOrAppStart("a"));
        tracker.Forget("a");
        Assert.Equal(T0, tracker.GetOrAppStart("a"));
    }

    [Fact]
    public void TheSnapshotIsBuiltFromTheTrackersValue()
    {
        var source = File.ReadAllText(LiteSampleAgeDeliveryTests.RepoFile("Lite/MainWindow.AlertEngine.cs"));
        Assert.Contains("_watchingSince.GetOrAppStart(", source);
        Assert.Contains("WatchingSinceUtc = watchingSinceUtc", source);
        Assert.Contains("RecentCpuSamples = recentCpuSamples", source);
    }

    [Fact]
    public void TheSnapshotNeverCarriesANullWatchingSince_ItFallsBackToTheAppStart()
    {
        // The shared engine collapses an unknown watching-since to the single newest sample, so a null would cost
        // Lite the batch gate for that sweep. Lite always has an answer: the connection's value, else the app start.
        var source = File.ReadAllText(LiteSampleAgeDeliveryTests.RepoFile("Lite/MainWindow.AlertEngine.cs"));
        Assert.DoesNotContain("DateTime? watchingSinceUtc = null", source);
        Assert.Contains("_watchingSince.GetOrAppStart(", source);
    }
}

/// <summary>
/// The adapter fills the newest-sample time for the six alert types whose DTO had none (#5642), from the
/// collection time each read already uses.
/// </summary>
public sealed class LiteAdapterNewestSampleTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = 63;
    private readonly DuckDbInitializer _duckDb;

    public LiteAdapterNewestSampleTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    [Fact]
    public async Task VolumesCarryTheirSnapshotTime()
    {
        var snapshot = DateTime.UtcNow.AddMinutes(-2);
        snapshot = new DateTime(snapshot.Ticks - snapshot.Ticks % TimeSpan.TicksPerSecond, DateTimeKind.Unspecified);
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc,
     file_name, physical_name, total_size_mb, used_size_mb, volume_mount_point, volume_total_mb, volume_free_mb)
VALUES (-6300001, $1, $2, 'S63', 'db1', 5, 1, 'ROWS', 'f', 'D:\f.mdf', 100, 50, 'D:\', 1000, 40)";
            cmd.Parameters.Add(new DuckDBParameter { Value = snapshot });
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var rows = await new LiteAlertReadAdapter(new LocalDataService(_duckDb)).GetVolumeFreeSpaceAsync(ServerId.ToString());

        var volume = Assert.Single(rows);
        Assert.Equal(DateTime.SpecifyKind(snapshot, DateTimeKind.Utc), volume.NewestSampleUtc);
    }

    [Theory]
    [InlineData("Lite/Services/LiteAlertReadAdapter.cs", "row.NewestSampleUtc = DateTime.SpecifyKind(row.CollectionTime")]          /* deadlocks */
    [InlineData("Lite/Services/LiteAlertReadAdapter.cs", "job.NewestSampleUtc = DateTime.SpecifyKind(snapshotTime.Value")]         /* anomalous jobs */
    [InlineData("Lite/Services/LocalDataService.VolumeSpace.cs", "MAX(collection_time) AS newest_sample")]                         /* volumes */
    [InlineData("Lite/Services/LocalDataService.WaitStats.cs", "r.collection_time AS newest_sample")]                              /* long-running queries */
    [InlineData("Lite/Services/LocalDataService.FinOps.Pvs.cs", "NewestSampleUtc = reader.IsDBNull(8)")]                           /* PVS */
    [InlineData("Lite/Services/LocalDataService.DatabaseStates.cs", "NewestSampleUtc = reader.IsDBNull(4)")]                       /* database state */
    public void EachOfTheSixReadsFillsTheTime(string file, string expected)
    {
        Assert.Contains(expected, File.ReadAllText(LiteSampleAgeDeliveryTests.RepoFile(file)));
    }
}

/// <summary>
/// The same-statement-pileup check runs on its own cadence, beside the collection cycle (#5642): a pinned
/// delay and a fake pass stand in for the clock, and nothing waits for real time.
/// </summary>
public sealed class LitePileupCadenceTests
{
    /// <summary>A delay that returns when the test releases it, so the test decides when each wait ends.</summary>
    private sealed class ManualClock
    {
        private TaskCompletionSource _tick = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public List<TimeSpan> Waits { get; } = new();

        public Task Delay(TimeSpan wait, CancellationToken token)
        {
            Waits.Add(wait);
            var tick = _tick;
            return tick.Task.WaitAsync(token);
        }

        public void Release()
        {
            var tick = _tick;
            _tick = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            tick.SetResult();
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!condition())
        {
            await Task.Delay(1, timeout.Token);
        }
    }

    [Fact]
    public async Task ThePassRunsOnItsOwnCadenceWhileACollectionCycleIsStillRunning()
    {
        /* The slow collection cycle is a task that never completes in this test. The pileup loop has no
           reference to it, so it must still tick and pass. */
        var slowCycle = new TaskCompletionSource();
        var clock = new ManualClock();
        var passes = 0;
        using var stop = new CancellationTokenSource();

        var loop = CollectionBackgroundService.RunPileupCadenceAsync(
            CollectionBackgroundService.PileupInterval, () => true,
            _ => { Interlocked.Increment(ref passes); return Task.CompletedTask; },
            clock.Delay, _ => { }, stop.Token);

        for (var i = 1; i <= 3; i++)
        {
            await WaitUntilAsync(() => clock.Waits.Count == i);
            clock.Release();
            await WaitUntilAsync(() => Volatile.Read(ref passes) == i);
        }

        Assert.False(slowCycle.Task.IsCompleted);
        Assert.All(clock.Waits, w => Assert.Equal(TimeSpan.FromSeconds(30), w));

        await stop.CancelAsync();
        await loop;
    }

    [Fact]
    public async Task OnePassRunsAtATime()
    {
        var gate = new SemaphoreSlim(1, 1);
        var release = new TaskCompletionSource();
        var entered = 0;
        var concurrent = 0;
        var maxConcurrent = 0;

        async Task Body()
        {
            Interlocked.Increment(ref entered);
            var now = Interlocked.Increment(ref concurrent);
            maxConcurrent = Math.Max(maxConcurrent, now);
            await release.Task;
            Interlocked.Decrement(ref concurrent);
        }

        var first = CollectionBackgroundService.RunExclusiveAsync(gate, Body);
        await WaitUntilAsync(() => Volatile.Read(ref entered) == 1);

        var second = await CollectionBackgroundService.RunExclusiveAsync(gate, Body);
        Assert.False(second);
        Assert.Equal(1, entered);

        release.SetResult();
        Assert.True(await first);
        Assert.Equal(1, maxConcurrent);

        /* the gate is free again afterwards */
        Assert.True(await CollectionBackgroundService.RunExclusiveAsync(gate, () => Task.CompletedTask));
    }

    [Fact]
    public async Task ThePassIsAwaitedBeforeTheNextWaitSoPassesNeverOverlap()
    {
        var clock = new ManualClock();
        var release = new TaskCompletionSource();
        var passes = 0;
        using var stop = new CancellationTokenSource();

        var loop = CollectionBackgroundService.RunPileupCadenceAsync(
            TimeSpan.FromSeconds(30), () => true,
            async _ => { Interlocked.Increment(ref passes); await release.Task; },
            clock.Delay, _ => { }, stop.Token);

        await WaitUntilAsync(() => clock.Waits.Count == 1);
        clock.Release();
        await WaitUntilAsync(() => Volatile.Read(ref passes) == 1);
        await Task.Delay(50); /* a pass still running: no second wait may start */
        Assert.Single(clock.Waits);

        release.SetResult();
        await WaitUntilAsync(() => clock.Waits.Count == 2);
        await stop.CancelAsync();
        await loop;
    }

    [Fact]
    public async Task ThePausedAndDatabaseDownGatesSkipThePass()
    {
        var clock = new ManualClock();
        var allowed = false;
        var passes = 0;
        using var stop = new CancellationTokenSource();

        var loop = CollectionBackgroundService.RunPileupCadenceAsync(
            TimeSpan.FromSeconds(30), () => allowed,
            _ => { Interlocked.Increment(ref passes); return Task.CompletedTask; },
            clock.Delay, _ => { }, stop.Token);

        await WaitUntilAsync(() => clock.Waits.Count == 1);
        clock.Release();
        await WaitUntilAsync(() => clock.Waits.Count == 2); /* ticked, gated, waiting again */
        Assert.Equal(0, passes);

        allowed = true;
        clock.Release();
        await WaitUntilAsync(() => Volatile.Read(ref passes) == 1);

        await stop.CancelAsync();
        await loop;
    }

    [Fact]
    public async Task AFaultedPassIsReportedAndTheLoopKeepsGoing()
    {
        var clock = new ManualClock();
        var faults = new List<string>();
        var passes = 0;
        using var stop = new CancellationTokenSource();

        var loop = CollectionBackgroundService.RunPileupCadenceAsync(
            TimeSpan.FromSeconds(30), () => true,
            _ =>
            {
                if (Interlocked.Increment(ref passes) == 1)
                {
                    throw new InvalidOperationException("boom");
                }

                return Task.CompletedTask;
            },
            clock.Delay, ex => faults.Add(ex.Message), stop.Token);

        await WaitUntilAsync(() => clock.Waits.Count == 1);
        clock.Release();
        await WaitUntilAsync(() => clock.Waits.Count == 2);
        clock.Release();
        await WaitUntilAsync(() => Volatile.Read(ref passes) == 2);

        Assert.Equal(new[] { "boom" }, faults);

        await stop.CancelAsync();
        await loop;
    }

    [Fact]
    public async Task ShutdownStopsTheLoopDuringTheWait()
    {
        var clock = new ManualClock();
        var passes = 0;
        using var stop = new CancellationTokenSource();

        var loop = CollectionBackgroundService.RunPileupCadenceAsync(
            TimeSpan.FromSeconds(30), () => true,
            _ => { Interlocked.Increment(ref passes); return Task.CompletedTask; },
            clock.Delay, _ => { }, stop.Token);

        await WaitUntilAsync(() => clock.Waits.Count == 1);
        await stop.CancelAsync();
        await loop.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        Assert.Equal(0, passes);
    }

    [Fact]
    public async Task ShutdownDuringAPassPassesTheTokenAndLeavesQuietly()
    {
        var clock = new ManualClock();
        var faults = 0;
        using var stop = new CancellationTokenSource();
        var inPass = new TaskCompletionSource();

        var loop = CollectionBackgroundService.RunPileupCadenceAsync(
            TimeSpan.FromSeconds(30), () => true,
            async token => { inPass.SetResult(); await Task.Delay(Timeout.Infinite, token); },
            clock.Delay, _ => Interlocked.Increment(ref faults), stop.Token);

        await WaitUntilAsync(() => clock.Waits.Count == 1);
        clock.Release();
        await inPass.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        await stop.CancelAsync();
        await loop.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        Assert.Equal(0, faults); /* a cancelled pass is a stop, not a fault */
    }

    [Fact]
    public void TheCollectionCycleNoLongerRunsThePileupAndTheLoopIsStartedBeforeIt()
    {
        var source = File.ReadAllText(LiteSampleAgeDeliveryTests.RepoFile("Lite/Services/CollectionBackgroundService.cs"));

        Assert.Equal(TimeSpan.FromSeconds(30), CollectionBackgroundService.PileupInterval);
        /* RunPileupSweepAsync is reached only through the gate inside the loop's pass, not from the cycle body. */
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(source, @"RunPileupSweepAsync\(token\)"));
        Assert.DoesNotContain("await RunPileupSweepAsync(", source);
        var loopStart = source.IndexOf("var pileupLoop = RunPileupCadenceAsync(", StringComparison.Ordinal);
        var cycle = source.IndexOf("while (!stoppingToken.IsCancellationRequested)", loopStart, StringComparison.Ordinal);
        Assert.True(loopStart > 0 && cycle > loopStart);
        Assert.Contains("!IsPaused && !_collectorService.LocalDatabaseIsDown()", source);
        Assert.Contains("await pileupLoop;", source);
    }
}
