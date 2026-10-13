using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5643: a collection cycle that runs late skips the grid slots it missed, and used to say nothing. The loop now counts
/// the skipped slots, logs one Warning naming the slowest steps (or the wait that overran), and writes one
/// <c>collection_log</c> row per enabled server. The count is driven here by the production wait
/// (<see cref="CollectionBackgroundService.WaitForNextCycleAsync"/>) over a fake clock and a fake delay, the way
/// <see cref="CollectionCycleResumeTests"/> drives it.
/// </summary>
public sealed class CollectionCycleSkipTests : IDisposable
{
    private static readonly DateTime T0 = new(2026, 10, 12, 10, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Interval = TimeSpan.FromMinutes(1);

    private readonly string _dir = Directory.CreateTempSubdirectory("pm-lite-skip-tests-").FullName;
    private readonly List<DuckDbInitializer> _initializers = [];

    public void Dispose()
    {
        foreach (var initializer in _initializers)
        {
            initializer.Dispose();
        }

        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    /// <summary>
    /// One loop pass from slot <paramref name="start"/>: the cycle's work takes <paramref name="cycleTook"/>, the wall clock
    /// steps by <paramref name="stepDuringCycle"/> before the wait begins, and the wait's delay comes back
    /// <paramref name="oversleepDuringWait"/> late (negative: the clock stepped back while it ran).
    /// </summary>
    private static async Task<(DateTime Next, int Skipped)> PassAsync(DateTime start, TimeSpan cycleTook,
        TimeSpan stepDuringCycle = default, TimeSpan oversleepDuringWait = default)
    {
        var now = start + cycleTook + stepDuringCycle;
        var next = await CollectionBackgroundService.WaitForNextCycleAsync(start, Interval, () => now,
            (wait, _) =>
            {
                now += wait + oversleepDuringWait;
                return Task.CompletedTask;
            },
            CancellationToken.None);
        return (next, CollectionCycleSkip.CountSkippedSlots(start, next, Interval));
    }

    [Theory]
    [InlineData(5, 0)]      // an ordinary cycle
    [InlineData(59, 0)]     // finishes just inside the interval
    [InlineData(61, 1)]     // 1 second over: the slot at +60 s was missed
    [InlineData(130, 2)]    // slots at +60 s and +120 s were missed
    [InlineData(312, 5)]    // 5 minutes 12 s: slots +60 s through +300 s
    public async Task ACycleThatRunsLong_SkipsTheWholeIntervalsItMissed(int cycleSeconds, int expectedSkipped)
    {
        var (_, skipped) = await PassAsync(T0, TimeSpan.FromSeconds(cycleSeconds));

        Assert.Equal(expectedSkipped, skipped);
    }

    [Fact]
    public async Task ACycleThatEndsAHairBeforeItsSlotOrAfterIt_SkipsNothing()
    {
        // The wait returns a second after its slot.
        var (_, late) = await PassAsync(T0, TimeSpan.FromSeconds(5), oversleepDuringWait: TimeSpan.FromSeconds(1));
        // The wait returns before its slot (an early delay): the stamp keeps its grid slot.
        var (_, early) = await PassAsync(T0, TimeSpan.FromSeconds(5), oversleepDuringWait: TimeSpan.FromMilliseconds(-300));

        Assert.Equal(0, late);
        Assert.Equal(0, early);
    }

    [Fact]
    public async Task AWallClockThatStepsBackwards_IsNotASkippedSlot()
    {
        /* #4732: the step re-anchors the grid at the clock's reading, which lands BEFORE the slot that should have come
           next. Counting that as a skip (or a negative count) would write a SKIPPED row for every clock correction. */
        var (stepDuringCycle, _) = await PassAsync(T0, TimeSpan.FromSeconds(5), stepDuringCycle: TimeSpan.FromMinutes(-10));
        var (stepDuringWait, _) = await PassAsync(T0, TimeSpan.FromSeconds(5), oversleepDuringWait: TimeSpan.FromMinutes(-10));

        Assert.Equal(0, CollectionCycleSkip.CountSkippedSlots(T0, stepDuringCycle, Interval));
        Assert.Equal(0, CollectionCycleSkip.CountSkippedSlots(T0, stepDuringWait, Interval));
        Assert.Equal(0, CollectionCycleSkip.CountSkippedSlots(T0, T0 - TimeSpan.FromDays(1), Interval));
    }

    [Fact]
    public async Task AWaitThatOverran_CountsTheSlotsOfTheSleep_WhileTheCycleWasShort()
    {
        /* A 47-minute sleep inside the wait: the cycle took 5 s, the wait ran 47 minutes past its slot. */
        var (next, skipped) = await PassAsync(T0, TimeSpan.FromSeconds(5), oversleepDuringWait: TimeSpan.FromMinutes(47));

        Assert.Equal(47, skipped);
        Assert.Equal(T0 + TimeSpan.FromMinutes(1) + TimeSpan.FromMinutes(47), next);
    }

    [Fact]
    public void AnIntervalThatIsNotPositive_CountsNothing()
    {
        Assert.Equal(0, CollectionCycleSkip.CountSkippedSlots(T0, T0.AddHours(1), TimeSpan.Zero));
    }

    // ── the report's text ────────────────────────────────────────────────────────────────────────────────

    private static List<CycleStep> Steps() =>
    [
        new("connection_check", 400),
        new("collectors", 61_000),
        new("archival", 240_000),
        new("query_store_backfill", 0),
        new("retention", 9_000),
        new("analysis", 2_000),
    ];

    [Fact]
    public void ALongCycle_NamesItsTotalTheSlotsAndItsThreeSlowestSteps()
    {
        var report = CollectionCycleSkip.Build(4, TimeSpan.FromSeconds(312), TimeSpan.FromSeconds(1), Steps(), Interval);

        Assert.NotNull(report);
        Assert.Equal(
            "Collection cycle took 312s, so 4 collection slot(s) were skipped. Slowest steps: archival 240s, collectors 61s, retention 9s.",
            report!.Message);
        Assert.Equal(4, report.SlotsSkipped);
        Assert.Equal(312_000, report.CycleMs);
        Assert.Equal("archival", report.SlowestStep);
        Assert.Equal(240_000, report.SlowestStepMs);
    }

    [Fact]
    public void AStepNameWithUnderscores_IsReadableInTheLine_AndKeepsItsNameInTheRow()
    {
        var steps = new List<CycleStep> { new("query_store_backfill", 90_000), new("collectors", 1_500) };

        var report = CollectionCycleSkip.Build(1, TimeSpan.FromSeconds(95), TimeSpan.Zero, steps, Interval);

        Assert.Contains("Slowest steps: query store backfill 90s, collectors 1.5s.", report!.Message);
        Assert.Equal("query_store_backfill", report.SlowestStep);
    }

    [Fact]
    public void AShortCycleWithAnOverrunWait_SaysTheWaitOverran_AndNamesTheWaitAsTheSlowestStep()
    {
        var report = CollectionCycleSkip.Build(46, TimeSpan.FromSeconds(5), TimeSpan.FromMinutes(47), Steps(), Interval);

        Assert.NotNull(report);
        Assert.Equal(
            "Collection wait overran: it lasted 2820s against a 60s interval, so 46 collection slot(s) were skipped. "
            + "The cycle itself took 5s; the computer may have slept or hibernated, or the clock stepped forward.",
            report!.Message);
        Assert.Equal("wait", report.SlowestStep);
        Assert.Equal(2_820_000, report.SlowestStepMs);
        Assert.Equal(5_000, report.CycleMs);
    }

    [Fact]
    public void NoSkippedSlots_IsNoReport()
    {
        Assert.Null(CollectionCycleSkip.Build(0, TimeSpan.FromSeconds(300), TimeSpan.Zero, Steps(), Interval));
    }

    [Fact]
    public void TheReportText_CarriesNoEpochLabel_SoTheBaselineMarkerReadNeverPicksItUp()
    {
        /* BaselineDiscontinuity.MarkerRowsSql selects every collection_log row whose note carries an epoch label. */
        var report = CollectionCycleSkip.Build(3, TimeSpan.FromSeconds(200), TimeSpan.Zero, Steps(), Interval)!;
        var overran = CollectionCycleSkip.Build(3, TimeSpan.FromSeconds(2), TimeSpan.FromMinutes(5), Steps(), Interval)!;

        foreach (var message in new[] { report.Message, overran.Message })
        {
            Assert.DoesNotContain("=", message);
        }
    }

    // ── the log line and the rows ────────────────────────────────────────────────────────────────────────

    private sealed class CapturingLogger<T>(List<(LogLevel Level, string Message)> sink) : ILogger<T>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => sink.Add((logLevel, formatter(state, exception)));
    }

    private sealed record Fixture(
        DuckDbInitializer DuckDb,
        CollectionBackgroundService Loop,
        ServerConnection Enabled1,
        ServerConnection Enabled2,
        ServerConnection Disabled,
        List<(LogLevel Level, string Message)> Log);

    private async Task<Fixture> OpenAsync(bool dropLogTable = false)
    {
        var duckDb = new DuckDbInitializer(Path.Combine(_dir, "pm.duckdb"));
        _initializers.Add(duckDb);
        await duckDb.InitializeFromTemplateAsync();

        var servers = new ServerManager(_dir);
        var one = new ServerConnection { ServerName = "skip-one", DisplayName = "skip-one" };
        var two = new ServerConnection { ServerName = "skip-two", DisplayName = "skip-two" };
        var off = new ServerConnection { ServerName = "skip-off", DisplayName = "skip-off", IsEnabled = false };
        servers.AddServer(one);
        servers.AddServer(two);
        servers.AddServer(off);

        if (dropLogTable)
        {
            using var readLock = duckDb.AcquireReadLock();
            using var connection = duckDb.CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var drop = connection.CreateCommand();
            drop.CommandText = "DROP TABLE collection_log";
            await drop.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var collector = new RemoteCollectorService(duckDb, servers, new ScheduleManager(_dir));
        var log = new List<(LogLevel, string)>();
        var loop = new CollectionBackgroundService(collector, duckDb, serverManager: servers,
            logger: new CapturingLogger<CollectionBackgroundService>(log));
        return new Fixture(duckDb, loop, one, two, off, log);
    }

    private sealed record LogRow(string Collector, string Status, DateTime Time, int? DurationMs, int? Rows, int? SqlMs,
        int? DuckMs, int? Items, string? SlowestItem, int? SlowestItemMs, string? Message, string ServerName);

    private static async Task<Dictionary<int, LogRow>> RowsAsync(DuckDbInitializer duckDb)
    {
        var rows = new Dictionary<int, LogRow>();
        using var readLock = duckDb.AcquireReadLock();
        using var connection = duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = @"SELECT server_id, collector_name, status, collection_time, duration_ms, rows_collected,
            sql_duration_ms, duckdb_duration_ms, fanout_item_count, slowest_item, slowest_item_ms, error_message, server_name
            FROM collection_log";
        using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            int? Int(int i) => reader.IsDBNull(i) ? null : Convert.ToInt32(reader.GetValue(i));
            rows[reader.GetInt32(0)] = new LogRow(reader.GetString(1), reader.GetString(2), reader.GetDateTime(3), Int(4), Int(5),
                Int(6), Int(7), Int(8), reader.IsDBNull(9) ? null : reader.GetString(9), Int(10),
                reader.IsDBNull(11) ? null : reader.GetString(11), reader.GetString(12));
        }

        return rows;
    }

    [Fact]
    public async Task ASkippedCycle_LogsOneWarning_AndWritesOneRowPerEnabledServer_WithTheAgreedShape()
    {
        var f = await OpenAsync();
        var previous = T0;
        var next = T0 + TimeSpan.FromMinutes(6);

        await f.Loop.ReportSkippedSlotsAsync(previous, next, TimeSpan.FromSeconds(312), TimeSpan.FromSeconds(48), Steps(),
            CancellationToken.None);

        var expected = CollectionCycleSkip.Build(5, TimeSpan.FromSeconds(312), TimeSpan.FromSeconds(48), Steps(), Interval)!;
        var warning = Assert.Single(f.Log);
        Assert.Equal(LogLevel.Warning, warning.Level);
        Assert.Equal(expected.Message, warning.Message);

        var rows = await RowsAsync(f.DuckDb);
        Assert.Equal(2, rows.Count);
        Assert.False(rows.ContainsKey(RemoteCollectorService.GetServerId(f.Disabled)));
        foreach (var server in new[] { f.Enabled1, f.Enabled2 })
        {
            var row = rows[RemoteCollectorService.GetServerId(server)];
            Assert.Equal("collection_cycle", row.Collector);
            Assert.Equal("SKIPPED", row.Status);
            Assert.Equal(previous, row.Time);
            Assert.Equal(312_000, row.DurationMs);
            Assert.Equal(5, row.Items);
            Assert.Equal("archival", row.SlowestItem);
            Assert.Equal(240_000, row.SlowestItemMs);
            Assert.Equal(expected.Message, row.Message);
            Assert.Equal(0, row.Rows);
            Assert.Null(row.SqlMs);
            Assert.Null(row.DuckMs);
            Assert.Equal(server.DisplayName, row.ServerName);
        }
    }

    [Fact]
    public async Task AWaitThatOverran_WritesTheWaitAsTheSlowestItem()
    {
        var f = await OpenAsync();

        await f.Loop.ReportSkippedSlotsAsync(T0, T0 + TimeSpan.FromMinutes(48), TimeSpan.FromSeconds(5),
            TimeSpan.FromMinutes(47), Steps(), CancellationToken.None);

        var row = (await RowsAsync(f.DuckDb))[RemoteCollectorService.GetServerId(f.Enabled1)];
        Assert.Equal("wait", row.SlowestItem);
        Assert.Equal(2_820_000, row.SlowestItemMs);
        Assert.Equal(5_000, row.DurationMs);
        Assert.Equal(47, row.Items);
        Assert.StartsWith("Collection wait overran", row.Message);
    }

    [Fact]
    public async Task ACycleOnTime_AndABackwardClockStep_LogNothingAndWriteNothing()
    {
        var f = await OpenAsync();

        await f.Loop.ReportSkippedSlotsAsync(T0, T0 + Interval, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(55), Steps(), CancellationToken.None);
        await f.Loop.ReportSkippedSlotsAsync(T0, T0 - TimeSpan.FromMinutes(10), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(55), Steps(), CancellationToken.None);

        Assert.Empty(f.Log);
        Assert.Empty(await RowsAsync(f.DuckDb));
    }

    [Fact]
    public async Task AFailedRowWrite_IsLoggedAndNeverLeavesTheLoop()
    {
        var f = await OpenAsync(dropLogTable: true);

        var ex = await Record.ExceptionAsync(() => f.Loop.ReportSkippedSlotsAsync(T0, T0 + TimeSpan.FromMinutes(6),
            TimeSpan.FromSeconds(312), TimeSpan.Zero, Steps(), CancellationToken.None));

        Assert.Null(ex);
        Assert.Single(f.Log);
    }

    // ── the loop is wired to it ──────────────────────────────────────────────────────────────────────────

    private static string LoopSource([CallerFilePath] string thisFile = "")
        => File.ReadAllText(Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Services", "CollectionBackgroundService.cs")));

    [Fact]
    public void TheLoop_TimesEveryAwaitedStep_AndReportsAfterTheWait()
    {
        var source = LoopSource();
        var body = source[source.IndexOf("protected override async Task ExecuteAsync", StringComparison.Ordinal)..];
        body = body[..body.IndexOf("The pileup loop ends on the same token", StringComparison.Ordinal)];

        var lastIndex = -1;
        foreach (var step in new[] { "connection_check", "collectors", "archival", "query_store_backfill", "retention",
                     "findings_cleanup", "dismissed_alerts_cleanup", "analysis" })
        {
            var index = body.IndexOf($"Lap(\"{step}\")", StringComparison.Ordinal);
            Assert.True(index > lastIndex, $"step {step} is not timed in loop order");
            lastIndex = index;
        }

        var wait = body.IndexOf("await WaitForNextCycleAsync(", StringComparison.Ordinal);
        var report = body.IndexOf("await ReportSkippedSlotsAsync(previousCycleStart, cycleStart", StringComparison.Ordinal);
        Assert.True(wait > 0 && report > wait, "the skipped-slot report must follow the wait that returns the new stamp");
        /* A step added to the loop without a Lap falls out of the report: every awaited housekeeping call is followed by one. */
        Assert.Equal(8, System.Text.RegularExpressions.Regex.Matches(body, @"Lap[(]""").Count);
        Assert.Equal(6, System.Text.RegularExpressions.Regex.Matches(body, @"await Run\w+IfDueAsync\([^)]*\);\s*Lap\(").Count);
    }
}
