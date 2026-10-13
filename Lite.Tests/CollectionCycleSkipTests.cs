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
using PerformanceMonitorLite.Mcp;
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
/* Collection app-logger-statics: AFailedRowWrite_... reads AppLogger's process-wide buffer, which the other classes in the
   collection also drain. */
[Collection("app-logger-statics")]
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
    public void ALongCycleFollowedByAnOverrunWait_PrintsBothCauses_AndDoesNotBlameTheCycleAlone()
    {
        /* A 65 s cycle (4 s over the interval), then the computer slept: the wait lasted 2 h. The slot count is the whole gap. */
        var report = CollectionCycleSkip.Build(124, TimeSpan.FromSeconds(65), TimeSpan.FromHours(2), Steps(), Interval);

        Assert.NotNull(report);
        Assert.Equal(
            "Collection cycle took 65s and the wait overran, so 124 collection slot(s) were skipped across the two. "
            + "Slowest steps: archival 240s, collectors 61s, retention 9s. "
            + "The wait lasted 7200s against a 60s interval; the computer may have slept or hibernated, or the clock stepped forward.",
            report!.Message);
        Assert.Equal(124, report.SlotsSkipped);
        Assert.Equal(65_000, report.CycleMs);
        /* The row's slowest item is whichever is longer: here the wait. */
        Assert.Equal("wait", report.SlowestStep);
        Assert.Equal(7_200_000, report.SlowestStepMs);
        Assert.DoesNotContain("so 124 collection slot(s) were skipped. ", report.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ALongCycleWhoseWaitRanOnlyItsShortSlack_IsTheCyclesCauseAlone()
    {
        /* A 130 s cycle then a wait that only used up its own slack (0 s left after the cycle): no overrun, so one cause. */
        var report = CollectionCycleSkip.Build(2, TimeSpan.FromSeconds(130), TimeSpan.FromSeconds(30), Steps(), Interval);

        Assert.StartsWith("Collection cycle took 130s, so 2 collection slot(s) were skipped.", report!.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("overran", report.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheGapSentence_NamesBothEndsOfTheGap_WhenTheStampsAreGiven()
    {
        var late = T0 + TimeSpan.FromHours(25);
        var report = CollectionCycleSkip.Build(1499, TimeSpan.FromSeconds(5), TimeSpan.FromHours(25), Steps(), Interval, T0, late);

        Assert.EndsWith(" The gap ran from 2026-10-12 10:00:00 UTC to 2026-10-13 11:00:00 UTC.", report!.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("=", report.Message);
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

        var expected = CollectionCycleSkip.Build(5, TimeSpan.FromSeconds(312), TimeSpan.FromSeconds(48), Steps(), Interval, previous, next)!;
        Assert.Contains("2026-10-12 10:00:00 UTC to 2026-10-12 10:06:00 UTC", expected.Message, StringComparison.Ordinal);
        var warning = Assert.Single(f.Log);
        Assert.Equal(LogLevel.Warning, warning.Level);
        Assert.Equal(expected.Message, warning.Message);

        var rows = await RowsAsync(f.DuckDb);
        Assert.Equal(2, rows.Count);
        Assert.False(rows.ContainsKey(RemoteCollectorService.GetServerId(f.Disabled)));
        foreach (var server in new[] { f.Enabled1, f.Enabled2 })
        {
            var row = rows[RemoteCollectorService.GetServerId(server)];
            Assert.Equal(CollectionCycleSkip.CollectorName, row.Collector);
            Assert.Equal("SKIPPED", row.Status);
            /* The late cycle's own start (the stamp the wait returned), not the previous cycle's: the message names that end. */
            Assert.Equal(next, row.Time);
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
    public async Task ATwentyFiveHourGapThatEndedAMinuteAgo_ShowsInTheMcpBlockAndTheCollectionHealthLine()
    {
        /* The row is stamped at the late cycle's own start, so a gap longer than the 24-hour window is still inside it for
           24 hours after it ends. Stamped at the previous cycle (25 hours back) it would read count_24h 0 from the start. */
        var f = await OpenAsync();
        var lateStamp = DateTime.UtcNow.AddMinutes(-1);
        var previous = lateStamp.AddHours(-25);

        await f.Loop.ReportSkippedSlotsAsync(previous, lateStamp, TimeSpan.FromSeconds(5), TimeSpan.FromHours(25), Steps(),
            CancellationToken.None);

        var summary = await new LocalDataService(f.DuckDb)
            .GetSkippedCycleSummaryAsync(RemoteCollectorService.GetServerId(f.Enabled1), cancellationToken: TestContext.Current.CancellationToken);
        var block = System.Text.Json.JsonSerializer.SerializeToElement(McpHealthTools.SkippedCyclesBlock(summary));

        Assert.Equal(1, block.GetProperty("count_24h").GetInt64());
        Assert.Equal(1, summary.Count);
        Assert.StartsWith("Skipped cycles in 24 h: 1 late cycle(s), 1,499 slot(s) skipped.", summary.SummaryLine(), StringComparison.Ordinal);
        Assert.Contains("The gap ran from", summary.LastMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ASkippedCycle_NamesTheHousekeepingStepsThatOverlappedTheGap_InTheLogAndTheRow()
    {
        var f = await OpenAsync();
        /* #5731: archival ran 4 minutes inside the gap; a step from the day before did not overlap it. */
        f.Loop.HousekeepingLedger.Record("archival", T0 + TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(4));
        f.Loop.HousekeepingLedger.Record("retention", T0 - TimeSpan.FromHours(20), TimeSpan.FromMinutes(2));

        await f.Loop.ReportSkippedSlotsAsync(T0, T0 + TimeSpan.FromMinutes(6), TimeSpan.FromSeconds(312), TimeSpan.FromSeconds(48),
            Steps(), CancellationToken.None);

        var warning = Assert.Single(f.Log);
        Assert.Contains(" Housekeeping in that window: archival 240s.", warning.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("retention 120s", warning.Message, StringComparison.Ordinal);
        var row = (await RowsAsync(f.DuckDb))[RemoteCollectorService.GetServerId(f.Enabled1)];
        Assert.Equal(warning.Message, row.Message);
        Assert.Equal("archival", row.SlowestItem); /* slowest_item is still the cycle's own slowest step */
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
        AppLogger.DrainBufferedLines();

        var ex = await Record.ExceptionAsync(() => f.Loop.ReportSkippedSlotsAsync(T0, T0 + TimeSpan.FromMinutes(6),
            TimeSpan.FromSeconds(312), TimeSpan.Zero, Steps(), CancellationToken.None));

        Assert.Null(ex);
        /* The loop's Warning is written before the row, on every skip; it is not the failure. The write failure goes to
           AppLogger from RemoteCollectorService.WriteCollectionLogRowAsync, once per enabled server, read here from
           AppLogger's own buffer (the seam its other pins use). */
        Assert.Single(f.Log);
        var failures = AppLogger.DrainBufferedLines()
            .Where(l => l.Contains("[ERROR]", StringComparison.Ordinal)
                        && l.Contains("[Collector]", StringComparison.Ordinal)
                        && l.Contains("COLLECTION LOGGING FAILED", StringComparison.Ordinal))
            .ToList();
        Assert.Equal(2, failures.Count);
    }

    // ── the collector name has one home ──────────────────────────────────────────────────────────────────

    [Fact]
    public void NoFileUnderLite_CopiesTheCollectorNameAsASingleQuotedSqlLiteral()
    {
        /* The nine reader predicates interpolate CollectionCycleSkip.CollectorName; a rename must move writer and readers
           together. The census has no exceptions: doc comments write the name in <c> tags, never in single quotes. */
        var literal = "'" + CollectionCycleSkip.CollectorName + "'";
        var lite = Path.GetFullPath(Path.Combine(RepoRootFromThisFile(), "Lite"));
        var offenders = Directory.EnumerateFiles(lite, "*", SearchOption.AllDirectories)
            .Where(f => !f.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                        && !f.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                        && !string.Equals(Path.GetFileName(f), "CollectionCycleSkip.cs", StringComparison.Ordinal)
                        && (f.EndsWith(".cs", StringComparison.Ordinal) || f.EndsWith(".xaml", StringComparison.Ordinal)
                            || f.EndsWith(".sql", StringComparison.Ordinal)))
            .Where(f => File.ReadAllText(f).Contains(literal, StringComparison.Ordinal))
            .Select(f => Path.GetRelativePath(lite, f))
            .ToList();

        Assert.Empty(offenders);
    }

    [Fact]
    public void TheNinePredicates_InterpolateTheCollectorName()
    {
        var services = Path.Combine(RepoRootFromThisFile(), "Lite", "Services");
        var count = Directory.EnumerateFiles(services, "LocalDataService.*.cs")
            .Sum(f => System.Text.RegularExpressions.Regex.Matches(File.ReadAllText(f),
                @"collector_name <> '\{CollectionCycleSkip\.CollectorName\}'").Count);

        Assert.Equal(9, count);
    }

    [Fact]
    public void TheDailySummaryAndCollectionHealthSql_CarryTheCollectorNamesValue_NotTheBracedTemplate()
    {
        /* #5644 merge guard: a literal that holds the {CollectionCycleSkip.CollectorName} hole but lost its $ prefix still
           compiles, and the daily summary then compares collector_name against the template text and counts skipped
           cycles as runs again. Read the built SQL, not the source. DailySummaryRangeSql is private, so it is read by
           reflection; CollectionHealthSql is internal. */
        var daily = (string)typeof(LocalDataService)
            .GetField("DailySummaryRangeSql", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .GetValue(null)!;

        foreach (var sql in new[] { daily, LocalDataService.CollectionHealthSql })
        {
            Assert.Contains("collector_name <> '" + CollectionCycleSkip.CollectorName + "'", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("{CollectionCycleSkip", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void EveryPredicateHole_SitsInAnInterpolatedLiteral()
    {
        /* The twin of the pin above for the readers whose SQL is composed inside a method (Overview, the collection-health
           lookups, runs, config changes, the runtime precondition): walk each file's string literals and require that every
           literal holding the hole starts with $. */
        var services = Path.Combine(RepoRootFromThisFile(), "Lite", "Services");
        const string hole = "{CollectionCycleSkip.CollectorName}";
        var holes = 0;
        var bare = new List<string>();
        foreach (var file in Directory.EnumerateFiles(services, "LocalDataService.*.cs"))
        {
            var s = File.ReadAllText(file);
            var i = 0;
            while (i < s.Length)
            {
                if (string.CompareOrdinal(s, i, "//", 0, 2) == 0) { var e = s.IndexOf('\n', i); i = e < 0 ? s.Length : e; }
                else if (string.CompareOrdinal(s, i, "/*", 0, 2) == 0) { i = s.IndexOf("*/", i + 2, StringComparison.Ordinal) + 2; }
                else if (s[i] == '\'') { i++; while (s[i] != '\'') i += s[i] == '\\' ? 2 : 1; i++; }
                else
                {
                    var start = i;
                    while (i < s.Length && (s[i] == '$' || s[i] == '@')) i++;
                    if (i < s.Length && s[i] == '"')
                    {
                        var prefix = s[start..i];
                        var verbatim = prefix.Contains('@', StringComparison.Ordinal);
                        var j = i + 1;
                        while (true)
                        {
                            if (prefix.Contains('$', StringComparison.Ordinal) && s[j] == '{')
                            {
                                /* An interpolation hole may hold its own quotes (string.Join(", ", ids)); skip it whole. */
                                if (s[j + 1] == '{') { j += 2; continue; }
                                var depth = 0;
                                do
                                {
                                    if (s[j] == '{') depth++;
                                    else if (s[j] == '}') depth--;
                                    else if (s[j] == '"') { j++; while (s[j] != '"') j += s[j] == '\\' ? 2 : 1; }
                                    j++;
                                } while (depth > 0);
                            }
                            else if (verbatim && string.CompareOrdinal(s, j, "\"\"", 0, 2) == 0) j += 2;
                            else if (!verbatim && s[j] == '\\') j += 2;
                            else if (s[j] == '"') break;
                            else j++;
                        }
                        var body = s[i..j];
                        var at = body.IndexOf(hole, StringComparison.Ordinal);
                        if (at >= 0)
                        {
                            holes++;
                            if (!prefix.Contains('$', StringComparison.Ordinal))
                                bare.Add(Path.GetFileName(file) + ":" + (s.Take(i + at).Count(c => c == '\n') + 1));
                        }
                        i = j + 1;
                    }
                    else if (i == start) i++;
                }
            }
        }

        Assert.Empty(bare);
        Assert.True(holes >= 8, "expected the nine predicates to sit in at least eight literals, saw " + holes);
    }

    private static string RepoRootFromThisFile([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));

    // ── the loop is wired to it ──────────────────────────────────────────────────────────────────────────

    private static string LoopSource([CallerFilePath] string thisFile = "")
        => File.ReadAllText(Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Services", "CollectionBackgroundService.cs")));

    [Fact]
    public void TheLoop_TimesEveryAwaitedStep_AndReportsAfterTheWait()
    {
        var source = LoopSource();
        var body = source[source.IndexOf("protected override async Task ExecuteAsync", StringComparison.Ordinal)..];
        body = body[..body.IndexOf("The pileup loop ends on the same token", StringComparison.Ordinal)];

        /* #5731: the six housekeeping awaits (and their Laps) moved to the housekeeping loops; what is left is timed. */
        var lastIndex = -1;
        foreach (var step in new[] { "connection_check", "collectors" })
        {
            var index = body.IndexOf($"Lap(\"{step}\")", StringComparison.Ordinal);
            Assert.True(index > lastIndex, $"step {step} is not timed in loop order");
            lastIndex = index;
        }

        var wait = body.IndexOf("await WaitForNextCycleAsync(", StringComparison.Ordinal);
        var report = body.IndexOf("await ReportSkippedSlotsAsync(previousCycleStart, cycleStart", StringComparison.Ordinal);
        Assert.True(wait > 0 && report > wait, "the skipped-slot report must follow the wait that returns the new stamp");
        /* A step added to the loop without a Lap falls out of the report. Housekeeping is not one of them any more: no
           Run*IfDueAsync is awaited from the collection loop, and the loops that run them are started before it (#5731). */
        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(body, @"Lap[(]""").Count);
        Assert.DoesNotMatch(@"await Run\w+IfDueAsync\(", body);
        var housekeeping = body.IndexOf("StartHousekeepingLoops(", StringComparison.Ordinal);
        var cycle = body.IndexOf("while (!stoppingToken.IsCancellationRequested)", StringComparison.Ordinal);
        Assert.True(housekeeping > 0 && cycle > housekeeping, "the housekeeping loops must start before the collection loop");
    }
}
