/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5813: drives the host's REAL per-database Query Store loop (the Azure SQL Database arm, the one a fake target can
/// reach end to end) with one database whose runtime-stats read never answers, the stand-in for a WAITFOR. The pass is
/// cut at the read budget, that database's watermark does not move, the sibling database is written in the same pass,
/// the timed-out database is the run's slowest item under its marker, and the next pass that can read it resumes from
/// the same watermark with every interval stored exactly once. The timed-out warning is logged once per database per
/// UTC day. The enumerated arm (every engine but Azure SQL Database) is driven through the same fake, which lists the
/// databases and reads each one's <c>[db].sys.sp_executesql</c> statement. The same harness reaches the plan-XML fetch (its gate is the
/// provider's engine since #5813, a fact the fake states), so the plan fetch's time budget is driven here too.
///
/// <para><b>#1776 own-store</b> - every test here creates and drops its own scratch database through
/// <see cref="ScratchPostgres"/>, so none is in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class QueryStoreReadBudgetLiveTests
{
    private const int LiveServerId = -581300;
    private const string Alpha = "alpha";
    private const string Zeta = "zeta";
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(1);
    /* #7130: the two-database test names the held database as the slowest item, which holds only while the sibling's
       read and write cost less than the budget. 1 s was too tight for a busy runner, so that test alone runs at 5 s. */
    private static readonly TimeSpan SiblingBudget = TimeSpan.FromSeconds(5);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static ServerRuntime MakeServer(bool azure = true) => new()
    {
        Config = new MonitoredServer { Name = "t", Host = "h" },
        ConnectionString = "Server=azure;Database=master",
        Target = new CollectorTargetInfo { IsAzureSqlDb = azure, Engine = CollectorTargetEngine.SqlServer },
        StorageName = "h",
        ServerId = LiveServerId,
    };

    private static Task<DateTime?> StoreAsync(DarlingCollectorRunner runner, string database, CancellationToken token) =>
        runner.GetLastCollectedTimeForDatabaseAsync(
            LiveServerId, "query_store_stats", "last_execution_time", "database_name", database, token,
            WatermarkPolicy.ReadFloor(DateTime.UtcNow));

    /// <summary>The target the fakes read: rows per database, and which database's read never answers.</summary>
    private sealed class Target
    {
        public Dictionary<string, List<object[]>> Rows { get; } = new(StringComparer.Ordinal);
        public string? SlowOn { get; set; }
        public int SlowReads;

        /// <summary>The databases the enumerated arm's list query returns (null on the Azure arm, which lists by override).</summary>
        public List<string>? Enumerated { get; set; }

        /// <summary>
        /// #5813 plan fetch: how long the statement takes per requested plan before it ships its first row, the stand-in
        /// for the real statement's decompression of every requested plan into a temp table.
        /// </summary>
        public TimeSpan DecompressPerPlan { get; set; }

        /// <summary>#6196: as <see cref="HoldTextUntilCut"/>, for the plan fetch.</summary>
        public bool HoldPlanUntilCut { get; set; }

        /// <summary>#5813 plan fetch: how long each plan row then takes to arrive.</summary>
        public TimeSpan ShipPerPlan { get; set; }

        /// <summary>Every plan id the plan fetch shipped, in order, across passes.</summary>
        public List<long> ShippedPlans { get; } = [];

        /// <summary>How many ids each plan-fetch statement asked for, in order, across passes.</summary>
        public List<(string Database, int Width)> RequestedWidths { get; } = [];

        /// <summary>#6102 text fetch: how long the statement takes per requested id before it ships its first row, the stand-in
        /// for the real statement's materialising every requested text into a temp table.</summary>
        public TimeSpan MaterialisePerText { get; set; }

        /// <summary>#6196 text fetch: when set, the statement ships nothing until its token is cancelled, so only the time
        /// budget can end it. A timed delay longer than the budget is not certain to be cut: a runner that stalls past the
        /// budget lets the delay finish first and the fetch then receives rows.</summary>
        public bool HoldTextUntilCut { get; set; }

        /// <summary>#6102 text fetch: how long each text row then takes to arrive.</summary>
        public TimeSpan ShipPerText { get; set; }

        /// <summary>Every query id the text fetch shipped, in order, across passes.</summary>
        public List<long> ShippedTexts { get; } = [];

        /// <summary>How many ids each text-fetch statement asked for, in order, across passes.</summary>
        public List<(string Database, int Width)> RequestedTextWidths { get; } = [];

        public List<int> TextWidthsFor(string database)
        {
            lock (RequestedTextWidths)
            {
                return RequestedTextWidths.Where(w => w.Database == database).Select(static w => w.Width).ToList();
            }
        }

        public List<int> WidthsFor(string database)
        {
            lock (RequestedWidths)
            {
                return RequestedWidths.Where(w => w.Database == database).Select(static w => w.Width).ToList();
            }
        }
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<(LogLevel Level, string Text)> Lines { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (Lines)
            {
                Lines.Add((logLevel, formatter(state, exception)));
            }
        }

        /// <summary>The plan fetch's once-a-day budget line for <paramref name="database"/>, at Information.</summary>
        public int PlanFetchBudgetLines(string database)
        {
            lock (Lines)
            {
                return Lines.Count(l => l.Level == LogLevel.Information && l.Text.StartsWith("query_store plan fetch on", StringComparison.Ordinal)
                    && l.Text.Contains(" budget: ", StringComparison.Ordinal)
                    && l.Text.Contains("[" + database + "]", StringComparison.Ordinal));
            }
        }

        /// <summary>The text fetch's once-a-day budget line for <paramref name="database"/>, at Information (#6102).</summary>
        public int TextFetchBudgetLines(string database)
        {
            lock (Lines)
            {
                return Lines.Count(l => l.Level == LogLevel.Information && l.Text.StartsWith("query_store text fetch on", StringComparison.Ordinal)
                    && l.Text.Contains(" budget: ", StringComparison.Ordinal)
                    && l.Text.Contains("[" + database + "]", StringComparison.Ordinal));
            }
        }

        /// <summary>The #2776 backoff's line: the cap narrowed after <paramref name="failures"/> consecutive failure(s).</summary>
        public int NarrowedAfter(string database, int failures)
        {
            lock (Lines)
            {
                return Lines.Count(l => l.Text.Contains("width narrowed", StringComparison.Ordinal)
                    && l.Text.Contains("after " + failures + " consecutive failure", StringComparison.Ordinal)
                    && l.Text.Contains("[" + database + "]", StringComparison.Ordinal));
            }
        }

        public int TimedOutWarnings(string database)
        {
            lock (Lines)
            {
                return Lines.Count(l => l.Level == LogLevel.Warning && l.Text.Contains("timed out:", StringComparison.Ordinal)
                    && l.Text.Contains("[" + database + "]", StringComparison.Ordinal));
            }
        }
    }

    private sealed class Rig : IAsyncDisposable
    {
        public ScratchPostgres Scratch = null!;
        public NpgsqlDataSource Source = null!;
        public CapturingLogger Log = new();
        public DarlingCollectorRunner Runner = null!;

        public static async Task<Rig> OpenAsync(CancellationToken ct, TimeSpan? budget = null)
        {
            var rig = new Rig();
            rig.Scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
            await using (var migrate = new NpgsqlConnection(rig.Scratch.ConnectionString))
            {
                await migrate.OpenAsync(ct);
                await PgMigrations.MigrateAsync(migrate, ct);
            }

            rig.Source = new NpgsqlDataSourceBuilder(rig.Scratch.ConnectionString).Build();
            rig.Runner = new DarlingCollectorRunner(rig.Source, new CollectorDeltaCalculator(), rig.Log)
            {
                QueryStoreReadBudgetSpan = budget ?? Budget,
            };
            return rig;
        }

        public async ValueTask DisposeAsync()
        {
            await Source.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    /// <summary>One runtime-stats row for interval <paramref name="id"/>, last executed at <paramref name="lastUtc"/>.</summary>
    private static object[] Row(long id, DateTime lastUtc)
    {
        var stamp = new DateTimeOffset(lastUtc, TimeSpan.Zero);
        var row = new object[56];
        Array.Fill(row, DBNull.Value);
        row[0] = id;
        row[1] = id;
        row[2] = "Regular";
        row[3] = stamp;
        row[4] = stamp;
        row[7] = "0x" + id.ToString("X8", System.Globalization.CultureInfo.InvariantCulture);
        row[8] = 1L;
        row[51] = "0x" + id.ToString("X8", System.Globalization.CultureInfo.InvariantCulture);
        row[53] = id;
        return row;
    }

    private static Task<CollectorRunResult> PassAsync(Rig rig, Target target, CancellationToken token, params string[] databases)
    {
        var provider = new FakeTargetProvider(target);
        rig.Runner.TargetProviderOverrideForTests = _ => provider;
        var listed = databases.Length == 0 ? new List<string> { Alpha, Zeta } : databases.ToList();
        rig.Runner.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(listed);
        return rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer(), token);
    }

    /// <summary>The newest collection_log row the test wrote for query_store: status, slowest item, its ms, item count.</summary>
    private static async Task<(string Status, string? SlowestItem, int? SlowestItemMs, int? ItemCount)> NewestLogRowAsync(
        Rig rig, CancellationToken token)
    {
        await using var connection = new NpgsqlConnection(rig.Scratch.ConnectionString);
        await connection.OpenAsync(token);
        await using var command = new NpgsqlCommand(
            "SELECT status, slowest_item, slowest_item_ms, fanout_item_count FROM collection_log "
            + "WHERE server_id = @s AND collector_name = 'query_store' ORDER BY collection_time DESC, log_id DESC LIMIT 1",
            connection);
        command.Parameters.AddWithValue("s", LiveServerId);
        await using var reader = await command.ExecuteReaderAsync(token);
        Assert.True(await reader.ReadAsync(token), "the run wrote a collection_log row");
        return (reader.GetString(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetInt32(2),
            reader.IsDBNull(3) ? null : reader.GetInt32(3));
    }

    /// <summary>Each stored interval of <paramref name="database"/> and how many raw rows hold it.</summary>
    private static async Task<Dictionary<long, long>> StoredAsync(Rig rig, string database, CancellationToken token)
    {
        await using var connection = new NpgsqlConnection(rig.Scratch.ConnectionString);
        await connection.OpenAsync(token);
        await using var command = new NpgsqlCommand(
            "SELECT runtime_stats_interval_id, count(*) FROM collect.query_store_stats WHERE server_id = @s AND database_name = @d GROUP BY 1",
            connection);
        command.Parameters.AddWithValue("s", LiveServerId);
        command.Parameters.AddWithValue("d", database);
        var result = new Dictionary<long, long>();
        await using var reader = await command.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
        {
            result[reader.GetInt64(0)] = reader.GetInt64(1);
        }

        return result;
    }

    [Fact]
    public async Task ASlowRead_IsCutAtTheBudget_KeepsItsWatermark_AndTheNextPassStoresEveryIntervalOnce()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #5813 read-budget pin.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token, SiblingBudget);

        /* Whole seconds, so the store's microseconds compare exactly. Both inside the 15-minute floor the #2111 shrink
           narrows to after two failures, so the resumed cutoff is the stored watermark itself, not a shrunk floor. */
        var now = DateTime.UtcNow;
        now = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);
        var first = now.AddMinutes(-8);
        var second = now.AddMinutes(-4);

        var target = new Target();
        target.Rows[Alpha] = [Row(1, first)];
        target.Rows[Zeta] = [Row(11, first)];

        /* Pass 1: both databases answer. */
        await PassAsync(rig, target, token);
        Assert.Equal(first, await StoreAsync(rig.Runner, Alpha, token));

        /* A new interval on each database; alpha's read now never answers. */
        target.Rows[Alpha].Add(Row(2, second));
        target.Rows[Zeta].Add(Row(12, second));
        target.SlowOn = Alpha;

        /* Pass 2: the stand-in never answers, so only a budget can end the read. The marker and the budget-sized
           slowest_item_ms below say it was THIS budget, not the 500 s command timeout or the 10-minute item budget
           (either of those would name no timed-out item), so no clock is needed. */
        var cut = await PassAsync(rig, target, token);
        Assert.Equal(1, target.SlowReads);

        /* The sibling is written in the same pass; alpha ships nothing and its watermark stays. */
        Assert.Equal(new Dictionary<long, long> { [11] = 1, [12] = 1 }, await StoredAsync(rig, Zeta, token));
        Assert.Equal(new Dictionary<long, long> { [1] = 1 }, await StoredAsync(rig, Alpha, token));
        Assert.Equal(first, await StoreAsync(rig.Runner, Alpha, token));

        /* slowest_item names the timed-out database with its marker, and slowest_item_ms is the budget. */
        Assert.NotNull(cut.Fanout);
        Assert.Equal(Alpha, cut.Fanout!.Value.SlowestItem);
        Assert.Equal((int)SiblingBudget.TotalMilliseconds, cut.Fanout.Value.SlowestItemMs);
        Assert.Equal(2, cut.Fanout.Value.ItemCount);

        /* #5813 review: the timed-out database's SQL slice is in the run's blended SQL time, as the enumerated driver's
           finally puts it there; before the fix the catch arm never added it and SqlMs held only zeta's instant read. The
           floor is half the budget, not the budget: the budget is a CancelAfter timer, which fires on the system timer's
           tick, so at Windows' default 15.6 ms tick a 1,000 ms budget can cut the read a few milliseconds short of 1,000
           by the slice's stopwatch (Lite's twin read 999 on CI, at its 1 s budget). Load only lengthens the slice, so half the budget
           separates the two shapes with no clock in the test. */
        Assert.True(cut.SqlMs >= (long)(SiblingBudget.TotalMilliseconds / 2),$"SqlMs {cut.SqlMs} holds the timed-out slice");
        Assert.Contains("timed out", cut.HostNote ?? string.Empty, StringComparison.Ordinal);
        Assert.Equal(1, rig.Log.TimedOutWarnings(Alpha));

        /* Pass 3, the same UTC day: cut again, and not named again. */
        await PassAsync(rig, target, token);
        Assert.Equal(2, target.SlowReads);
        Assert.Equal(1, rig.Log.TimedOutWarnings(Alpha));
        Assert.Equal(first, await StoreAsync(rig.Runner, Alpha, token));

        /* Pass 4: alpha answers, and resumes from the watermark it kept. Every interval is stored exactly once. */
        target.SlowOn = null;
        var resumed = await PassAsync(rig, target, token);
        Assert.Equal(new Dictionary<long, long> { [1] = 1, [2] = 1 }, await StoredAsync(rig, Alpha, token));
        Assert.Equal(new Dictionary<long, long> { [11] = 1, [12] = 1 }, await StoredAsync(rig, Zeta, token));
        Assert.Equal(second, await StoreAsync(rig.Runner, Alpha, token));
        Assert.DoesNotContain(QueryStoreReadBudget.TimedOutMarker, resumed.Fanout?.SlowestItem ?? string.Empty, StringComparison.Ordinal);

        /* Pass 5, the next UTC day: a database that is still timing out is named once more at the day boundary. */
        rig.Runner.QueryStoreReadTimeoutClockForTests = () => DateTime.UtcNow.AddDays(1);
        target.SlowOn = Alpha;
        await PassAsync(rig, target, token);
        Assert.Equal(2, rig.Log.TimedOutWarnings(Alpha));
        Assert.Equal(0, rig.Log.TimedOutWarnings(Zeta));
    }

    /// <summary>
    /// The one-database server, the shape of the field's slowest servers: its only database times out, so every
    /// database in the pass failed and the run is an ERROR (the #3754 rule; a run that wrote nothing is never SUCCESS).
    /// The rethrown timeout carries the pass's fan-out, and the row the worker's ERROR arm writes from it names the
    /// database with its marker and the budget, where it used to hold NULL. The watermark stays and the next pass resumes.
    /// </summary>
    [Fact]
    public async Task OneDatabaseServer_ATimedOutRead_IsAnErrorRowThatNamesTheDatabase_AndTheNextPassResumes()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #5813 read-budget pin.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);

        var now = DateTime.UtcNow;
        now = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);
        var first = now.AddMinutes(-8);
        var second = now.AddMinutes(-4);

        var target = new Target();
        target.Rows[Alpha] = [Row(1, first)];
        await PassAsync(rig, target, token, Alpha);
        Assert.Equal(first, await StoreAsync(rig.Runner, Alpha, token));

        target.Rows[Alpha].Add(Row(2, second));
        target.SlowOn = Alpha;

        /* The only database times out: the runner surfaces the timeout itself, with the fan-out on it. */
        var timeout = await Assert.ThrowsAsync<QueryStoreReadTimeoutException>(() => PassAsync(rig, target, token, Alpha));
        Assert.Equal(1, target.SlowReads);
        Assert.Equal(Alpha, timeout.Database);
        var carried = CollectorFaultFanout.For(timeout);
        Assert.NotNull(carried);
        Assert.Equal(Alpha, carried!.Value.SlowestItem);
        Assert.Equal((int)Budget.TotalMilliseconds, carried.Value.SlowestItemMs);
        Assert.Equal(1, carried.Value.ItemCount);

        /* The same pass through the worker: its general ERROR arm reads the fan-out back off the exception into the row. */
        var config = new MonitoredServer { Name = "t", Host = "h" };
        var loop = new DarlingWorker.ServerLoopState { Config = config, Runtime = MakeServer() };
        var worker = new DarlingWorker(
            NullLogger<DarlingWorker>.Instance, NullLoggerFactory.Instance, new McpRuntimeState(), new WebRuntimeState(),
            new MonitoredServerRegistryState(), new CollectorRuntimeState(), new WebTlsCertificateState(), new BaselineCache(),
            new ReadLatencyAccumulator())
        {
            StoreForTests = rig.Source,
        };
        var effective = StoreConfigProvider.ResolveSchedule(QueryStoreCollector.Instance.Name, LiveServerId, worker.ScheduleOverridesForTest);
        Assert.False(await worker.RunOnLoadAsync(loop, rig.Runner, QueryStoreCollector.Instance.Name, LiveServerId, effective, token));
        Assert.Equal(2, target.SlowReads);
        var row = await NewestLogRowAsync(rig, token);
        Assert.Equal("ERROR", row.Status);
        Assert.Equal(Alpha, row.SlowestItem);
        Assert.Equal((int)Budget.TotalMilliseconds, row.SlowestItemMs);
        Assert.Equal(1, row.ItemCount);

        /* Nothing shipped, so the watermark stayed; the next good pass reads from it and stores interval 2 once. */
        Assert.Equal(new Dictionary<long, long> { [1] = 1 }, await StoredAsync(rig, Alpha, token));
        Assert.Equal(first, await StoreAsync(rig.Runner, Alpha, token));
        target.SlowOn = null;
        await PassAsync(rig, target, token, Alpha);
        Assert.Equal(new Dictionary<long, long> { [1] = 1, [2] = 1 }, await StoredAsync(rig, Alpha, token));
        Assert.Equal(second, await StoreAsync(rig.Runner, Alpha, token));
    }

    /// <summary>
    /// The enumerated arm (every engine but Azure SQL Database): query_store lists the databases, then reads each on the
    /// same connection. The only database's read never answers, so it is cut at the budget, logged as timed out, and the
    /// rethrown timeout carries the fan-out that names it with the budget. Without the budget the read would wait out the
    /// guard and surface as a cancellation instead.
    /// </summary>
    [Fact]
    public async Task TheEnumeratedArm_CutsATimedOutRead_AndCarriesItsFanoutToTheRethrow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #5813 read-budget pin.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);

        var target = new Target { SlowOn = Alpha, Enumerated = [Alpha] };
        var provider = new FakeTargetProvider(target);
        rig.Runner.TargetProviderOverrideForTests = _ => provider;
        using var guard = CancellationTokenSource.CreateLinkedTokenSource(token);
        guard.CancelAfter(TimeSpan.FromSeconds(20));

        var timeout = await Assert.ThrowsAsync<QueryStoreReadTimeoutException>(
            () => rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer(azure: false), guard.Token));
        Assert.Equal(1, target.SlowReads);
        Assert.Equal(Alpha, timeout.Database);
        var carried = CollectorFaultFanout.For(timeout);
        Assert.NotNull(carried);
        Assert.Equal(Alpha, carried!.Value.SlowestItem);
        Assert.Equal((int)Budget.TotalMilliseconds, carried.Value.SlowestItemMs);
        Assert.Equal(1, carried.Value.ItemCount);
        Assert.Equal(1, rig.Log.TimedOutWarnings(Alpha));
    }

    /// <summary>
    /// #5813 review: <c>collection_log.slowest_item</c> is an identity column. The Query Store clutter reader groups by it
    /// as the database name and checks it against the server's current databases, so a timed-out database is stored under
    /// its plain name: one clutter row that counts its timed-out runs, never a phantom name omitted as dropped. The words
    /// "(timed out)" are rendered by <c>get_collection_health</c> from the budget-sized cost. Two runs are written through
    /// the real writer (the fan-out <see cref="QueryStoreReadBudget.ObserveTimeout"/> fills, then
    /// <see cref="DarlingObservability.LogCollectionAsync"/>): one where alpha timed out at the production budget, one
    /// where alpha completed and was still the slowest.
    /// </summary>
    [Fact]
    public async Task ATimedOutDatabase_IsOneClutterRowUnderItsOwnName_AndCollectionHealthStillSaysItTimedOut()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #5813 slowest-item identity test.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);
        var server = MakeServer();

        var timedOut = new FanoutCostAccumulator();
        timedOut.Observe(Zeta, 40);
        Assert.True(QueryStoreReadBudget.ObserveTimeout(
            new QueryStoreReadTimeoutException(Alpha, QueryStoreReadBudget.Budget, null), timedOut));
        var completed = new FanoutCostAccumulator();
        completed.Observe(Zeta, 40);
        completed.Observe(Alpha, 500);

        await DarlingObservability.LogCollectionAsync(
            rig.Source, server, QueryStoreCollector.Instance.Name, "SUCCESS", 1, 31_000, 30_050, null,
            fanout: timedOut.Result, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: null, rig.Log, token);
        await DarlingObservability.LogCollectionAsync(
            rig.Source, server, QueryStoreCollector.Instance.Name, "SUCCESS", 2, 700, 560, null,
            fanout: completed.Result, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: null, rig.Log, token);

        /* The server's current databases, snapshotted after both runs: alpha and zeta, nothing else. */
        await using (var connection = new NpgsqlConnection(rig.Scratch.ConnectionString))
        {
            await connection.OpenAsync(token);
            var at = DateTime.SpecifyKind(DateTime.UtcNow.AddSeconds(1), DateTimeKind.Unspecified);
            var id = 1;
            foreach (var database in new[] { Alpha, Zeta })
            {
                await using var states = new NpgsqlCommand(
                    "INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) "
                    + "VALUES ($1, $2, $3, $4, $5, $6, 'ONLINE', FALSE)", connection);
                states.Parameters.AddWithValue(CollectionIdGenerator.Next());
                states.Parameters.AddWithValue(at);
                states.Parameters.AddWithValue(LiveServerId);
                states.Parameters.AddWithValue(server.StorageName);
                states.Parameters.AddWithValue(database);
                states.Parameters.AddWithValue(id++);
                await states.ExecuteNonQueryAsync(token);
            }

            await using var register = new NpgsqlCommand(
                "INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) "
                + "VALUES ($1, $2, $2, TRUE, 15, $3, $3)", connection);
            register.Parameters.AddWithValue(LiveServerId);
            register.Parameters.AddWithValue(server.StorageName);
            register.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
            await register.ExecuteNonQueryAsync(token);
        }

        /* The clutter reader: one alpha row holding both runs, and nothing omitted as a database the server no longer has. */
        var omitted = new List<string>();
        var cost = await DarlingQueryStoreClutterReader.GetReadCostAsync(
            rig.Source, [LiveServerId], DateTime.UtcNow.AddHours(-1), DateTime.UtcNow.AddHours(1),
            omittedDatabases: omitted, cancellationToken: token);
        Assert.Empty(omitted);
        var alpha = Assert.Single(cost);
        Assert.Equal(Alpha, alpha.DatabaseName);
        Assert.Equal(2, alpha.RunsSlowest);
        Assert.Equal(2, alpha.RunsObserved);
        Assert.Equal(30_000, alpha.SlowestItemMsP95);

        /* get_collection_health's fan-out names the dearest run's database and says it timed out. */
        var json = await PerformanceMonitor.Darling.Service.Mcp.DarlingMcpDataTools.GetCollectionHealth(
            rig.Source, server.StorageName, full_detail: true, cancellationToken: token);
        using var document = JsonDocument.Parse(json);
        var queryStore = document.RootElement.GetProperty("collectors").EnumerateArray()
            .Single(r => r.GetProperty("collector").GetString() == QueryStoreCollector.Instance.Name);
        var fanout = queryStore.GetProperty("fanout");
        Assert.Equal(Alpha + QueryStoreReadBudget.TimedOutMarker, fanout.GetProperty("slowest").GetString());
        Assert.Equal(30_000, fanout.GetProperty("slowest_ms").GetInt32());
    }

    /// <summary>
    /// #5813 (a) and (d): the plan fetch's time budget, the field's real spike (91 s of a 97 s run was the plan fetch).
    /// Six plans at 400 ms each against a 1 s budget, with no measured time per plan yet: the first pass asks for all
    /// six, is cut at the budget, stores the plans it received and leaves the rest in the backlog; later passes ask for
    /// the predicted width and fetch the rest, every plan exactly once. The budget line is logged once that UTC day and
    /// again the next.
    /// </summary>
    [Fact]
    public async Task ASlowPlanFetch_IsCutAtItsBudget_StoresWhatItReceived_AndLaterPassesFetchTheRestOnce()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #5813 plan-fetch budget pin.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);
        rig.Runner.QueryStorePlanFetchBudgetSpan = TimeSpan.FromSeconds(1);

        var now = WholeSecondNow();
        var target = new Target { ShipPerPlan = TimeSpan.FromMilliseconds(400) };
        target.Rows[Alpha] = Enumerable.Range(1, 6).Select(id => Row(id, now.AddMinutes(-8))).ToList();

        await PassAsync(rig, target, token, Alpha);

        /* The intervals are all written; the plan fetch asked for all six, was cut, and stored what it received. */
        Assert.Equal(6, (await StoredAsync(rig, Alpha, token)).Count);
        Assert.Equal([6], target.WidthsFor(Alpha));
        var afterFirst = await StoredPlansAsync(rig, Alpha, token);
        Assert.InRange(afterFirst.Count, 1, 5);
        Assert.Equal(afterFirst, target.ShippedPlans.Order().ToList());
        Assert.NotNull(rig.Runner.PlanFetchMsPerPlanForTests(LiveServerId, Alpha, QueryStoreCollector.Instance.Name));
        Assert.Equal(1, rig.Log.PlanFetchBudgetLines(Alpha));

        /* Later passes read no new interval and fetch the owed plans from the backlog, a predicted few at a time. */
        for (var pass = 0; pass < 10 && (await StoredPlansAsync(rig, Alpha, token)).Count < 6; pass++)
        {
            await PassAsync(rig, target, token, Alpha);
        }

        Assert.Equal([1L, 2L, 3L, 4L, 5L, 6L], await StoredPlansAsync(rig, Alpha, token));
        Assert.Equal(target.ShippedPlans.Count, target.ShippedPlans.Distinct().Count());
        Assert.All(target.WidthsFor(Alpha).Skip(1), w => Assert.InRange(w, 1, 2));
        Assert.Equal(6, (await StoredAsync(rig, Alpha, token)).Count);
        Assert.Equal(1, rig.Log.PlanFetchBudgetLines(Alpha));

        /* (d) The next UTC day, four new plans at the same pace: the pass is held to its budget again and says so once. */
        rig.Runner.QueryStoreReadTimeoutClockForTests = () => DateTime.UtcNow.AddDays(1);
        target.Rows[Alpha].AddRange(Enumerable.Range(7, 4).Select(id => Row(id, now.AddMinutes(-4))));
        await PassAsync(rig, target, token, Alpha);
        Assert.Equal(2, rig.Log.PlanFetchBudgetLines(Alpha));
    }

    /// <summary>
    /// #5813 (b): the binding catch. The fetch statement decompresses every requested plan before it ships the first row,
    /// so a cut usually receives nothing. Eight plans held until the 2 s budget cuts the statement: the first
    /// pass is cut empty, which counts as a fetch failure (the #2776 backoff narrows the next pass) and leaves a time per
    /// plan that asks for at most half as many next pass, which then fits; every plan is fetched exactly once.
    /// </summary>
    [Fact]
    public async Task ACutThatReceivedNothing_IsAFetchFailure_AndTheNextPassAsksForAtMostHalf()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #5813 plan-fetch budget pin.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);
        rig.Runner.QueryStorePlanFetchBudgetSpan = TimeSpan.FromSeconds(2);

        var now = WholeSecondNow();
        /* #6196: held until the budget cuts it, as the text twin below; later passes decompress at a speed that fits. */
        var target = new Target { HoldPlanUntilCut = true };
        target.Rows[Alpha] = Enumerable.Range(1, 8).Select(id => Row(id, now.AddMinutes(-8))).ToList();

        await PassAsync(rig, target, token, Alpha);
        Assert.Equal([8], target.WidthsFor(Alpha));
        Assert.Empty(target.ShippedPlans);

        /* #5813 review: the empty cut is a backlog, not a drain. Catch-up stays set, so the next pass's candidate cap
           keeps the first-contact floor instead of trusting a backlog-biased average. */
        var estimate = rig.Runner.PlanSizeEstimateForTests(LiveServerId, Alpha, QueryStoreCollector.Instance.Name);
        Assert.NotNull(estimate);
        Assert.True(estimate!.Value.CatchUpInProgress, "a pass the time budget cut with nothing received keeps catch-up in progress");
        Assert.Empty(await StoredPlansAsync(rig, Alpha, token));
        /* The time per plan it recorded asks for at most half the cut width (4 of 8) at the full budget. Not
           `>= 2 x 2000 / 8`: the cut chunk runs under the time LEFT, which is under the budget by whatever the pass did
           before it, and the cancellation timer can fire a clock tick early, so the chunk's measured share can land a
           few milliseconds under 2000 / 8. That bound held on a margin of 4 to 14 ms and failed on CI once it ran out. */
        var cutMsPerPlan = rig.Runner.PlanFetchMsPerPlanForTests(LiveServerId, Alpha, QueryStoreCollector.Instance.Name);
        Assert.True(
            QueryStoreReadBudget.PlanFetchWidth(TimeSpan.FromSeconds(2), cutMsPerPlan, 400, firstChunk: true) <= 8 / 2,
            $"a cut-empty pass of 8 plans recorded {cutMsPerPlan} ms a plan, which asks for more than half as many next pass");
        Assert.Equal(1, rig.Log.PlanFetchBudgetLines(Alpha));

        target.HoldPlanUntilCut = false;
        target.DecompressPerPlan = TimeSpan.FromMilliseconds(100);
        await PassAsync(rig, target, token, Alpha);
        var widths = target.WidthsFor(Alpha);
        Assert.InRange(widths[1], 1, 4);
        Assert.Equal(1, rig.Log.NarrowedAfter(Alpha, 1));
        Assert.NotEmpty(await StoredPlansAsync(rig, Alpha, token));

        for (var pass = 0; pass < 10 && (await StoredPlansAsync(rig, Alpha, token)).Count < 8; pass++)
        {
            await PassAsync(rig, target, token, Alpha);
        }

        Assert.Equal(Enumerable.Range(1, 8).Select(static id => (long)id).ToList(), await StoredPlansAsync(rig, Alpha, token));
        Assert.Equal(8, target.ShippedPlans.Count);
        Assert.Equal(0, rig.Log.NarrowedAfter(Alpha, 2));
    }

    /// <summary>
    /// #5813 (c): the predicted width's two clamps on the real fetch. A database slower per plan than the whole budget
    /// still asks for one plan each pass, never none. A fast one's measured time per plan predicts the full 400 ids per
    /// statement, so the time budget leaves its passes exactly as they were: every statement asks for the candidate cap
    /// the byte budget sized, and no pass is held to the budget.
    /// </summary>
    [Fact]
    public async Task ThePredictedWidth_NeverFallsBelowOnePlan_NorRisesAboveTheStatementWidth()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #5813 plan-fetch budget pin.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);
        rig.Runner.QueryStorePlanFetchBudgetSpan = TimeSpan.FromSeconds(1);

        var now = WholeSecondNow();
        var slow = new Target { ShipPerPlan = TimeSpan.FromMilliseconds(1500) };
        slow.Rows[Alpha] = Enumerable.Range(1, 3).Select(id => Row(id, now.AddMinutes(-8))).ToList();
        for (var pass = 0; pass < 3; pass++)
        {
            await PassAsync(rig, slow, token, Alpha);
        }

        /* 3 unmeasured, cut empty; then 2 x 1000 / 3 ms a plan asks for 1; cut empty again, 2000 ms a plan would ask for
           none, and the floor keeps it at 1. */
        Assert.Equal([3, 1, 1], slow.WidthsFor(Alpha));
        Assert.Equal(0, QueryStoreReadBudget.PlanFetchWidth(
            TimeSpan.FromSeconds(1), rig.Runner.PlanFetchMsPerPlanForTests(LiveServerId, Alpha, QueryStoreCollector.Instance.Name), 400, firstChunk: false));

        var fast = new Target();
        fast.Rows[Zeta] = Enumerable.Range(1, 600).Select(id => Row(id, now.AddMinutes(-8))).ToList();
        for (var pass = 0; pass < 12 && (await StoredPlansAsync(rig, Zeta, token)).Count < 600; pass++)
        {
            await PassAsync(rig, fast, token, Zeta);
        }

        Assert.True(600 == (await StoredPlansAsync(rig, Zeta, token)).Count, "widths " + string.Join(",", fast.WidthsFor(Zeta)));
        var fastWidths = fast.WidthsFor(Zeta);
        Assert.All(fastWidths.SkipLast(1), w => Assert.Equal(fastWidths[0], w));
        Assert.InRange(fastWidths[0], QueryStorePlanXmlState.MinCandidatePlans, 400);
        Assert.Equal(400, QueryStoreReadBudget.PlanFetchWidth(
            TimeSpan.FromSeconds(1), rig.Runner.PlanFetchMsPerPlanForTests(LiveServerId, Zeta, QueryStoreCollector.Instance.Name), 400, firstChunk: true));
        Assert.Equal(0, rig.Log.PlanFetchBudgetLines(Zeta));
    }

    /// <summary>
    /// #6102 (a): the text fetch's time budget, the twin of the plan fetch's, on the same fake. Six texts at 400 ms each
    /// against a 1 s budget, with no measured time per id yet: the first pass asks for all six, is cut at the budget,
    /// stores the texts it received and leaves the rest in the text backlog; later passes ask for the predicted width
    /// and fetch the rest from that backlog first, every text exactly once. The budget line is logged once that UTC day
    /// and again the next.
    /// </summary>
    [Fact]
    public async Task ASlowTextFetch_IsCutAtItsBudget_StoresWhatItReceived_AndLaterPassesFetchTheRestOnce()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6102 text-fetch budget pin.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);
        rig.Runner.QueryStoreTextFetchBudgetSpan = TimeSpan.FromSeconds(1);

        var now = WholeSecondNow();
        var target = new Target { ShipPerText = TimeSpan.FromMilliseconds(400) };
        target.Rows[Alpha] = Enumerable.Range(1, 6).Select(id => Row(id, now.AddMinutes(-8))).ToList();

        await PassAsync(rig, target, token, Alpha);

        /* The intervals are all written; the text fetch asked for all six, was cut, and stored what it received. */
        Assert.Equal(6, (await StoredAsync(rig, Alpha, token)).Count);
        Assert.Equal([6], target.TextWidthsFor(Alpha));
        var afterFirst = await StoredTextsAsync(rig, Alpha, token);
        Assert.InRange(afterFirst.Count, 1, 5);
        Assert.Equal(afterFirst, target.ShippedTexts.Order().ToList());
        Assert.NotNull(rig.Runner.TextFetchMsPerTextForTests(LiveServerId, Alpha, QueryStoreCollector.Instance.Name));
        Assert.Equal(1, rig.Log.TextFetchBudgetLines(Alpha));
        /* The plan side is its own budget: the instant plan fetch was neither cut nor held. */
        Assert.Equal(0, rig.Log.PlanFetchBudgetLines(Alpha));

        /* Later passes read no new interval and fetch the owed texts from the backlog, a predicted few at a time. */
        for (var pass = 0; pass < 10 && (await StoredTextsAsync(rig, Alpha, token)).Count < 6; pass++)
        {
            await PassAsync(rig, target, token, Alpha);
        }

        Assert.Equal([1L, 2L, 3L, 4L, 5L, 6L], await StoredTextsAsync(rig, Alpha, token));
        Assert.Equal(target.ShippedTexts.Count, target.ShippedTexts.Distinct().Count());
        Assert.All(target.TextWidthsFor(Alpha).Skip(1), w => Assert.InRange(w, 1, 2));
        Assert.Equal(6, (await StoredAsync(rig, Alpha, token)).Count);
        Assert.Equal(1, rig.Log.TextFetchBudgetLines(Alpha));

        /* The next UTC day, four new queries at the same pace: the pass is held to its budget again and says so once. */
        rig.Runner.QueryStoreReadTimeoutClockForTests = () => DateTime.UtcNow.AddDays(1);
        target.Rows[Alpha].AddRange(Enumerable.Range(7, 4).Select(id => Row(id, now.AddMinutes(-4))));
        await PassAsync(rig, target, token, Alpha);
        Assert.Equal(2, rig.Log.TextFetchBudgetLines(Alpha));
    }

    /// <summary>
    /// #6102 (b): the binding catch, as on the plan side. The text statement materialises every requested text before it
    /// ships the first row, so a cut usually receives nothing. Eight texts held until the 2 s budget cuts the statement: the first
    /// pass is cut empty, which counts as a text-fetch failure and leaves a time per id that asks for at most half as many
    /// next pass, which then fits; a pass that receives texts clears the failure; every text is fetched exactly once.
    /// </summary>
    [Fact]
    public async Task ATextCutThatReceivedNothing_IsAFetchFailure_AndTheNextPassAsksForAtMostHalf()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6102 text-fetch budget pin.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);
        rig.Runner.QueryStoreTextFetchBudgetSpan = TimeSpan.FromSeconds(2);
        var collector = QueryStoreCollector.Instance.Name;

        var now = WholeSecondNow();
        /* #6196: the first pass's statement is held until the budget cuts it, so it receives nothing on any runner. A timed
           delay (8 x 400 ms against 2 s) is cut only if the runner is not stalled past the budget; when it was, the
           delay finished first and the fetch received rows. Later passes materialise at a speed that fits the budget. */
        var target = new Target { HoldTextUntilCut = true };
        target.Rows[Alpha] = Enumerable.Range(1, 8).Select(id => Row(id, now.AddMinutes(-8))).ToList();

        await PassAsync(rig, target, token, Alpha);
        Assert.Equal([8], target.TextWidthsFor(Alpha));
        Assert.Empty(target.ShippedTexts);
        Assert.Empty(await StoredTextsAsync(rig, Alpha, token));
        Assert.Equal(1, rig.Runner.TextFetchFailuresForTests(LiveServerId, Alpha, collector));

        /* Not `>= 2 x 2000 / 8`: the cut runs under the time LEFT and the timer can fire a tick early (see the plan twin). */
        var cutMsPerText = rig.Runner.TextFetchMsPerTextForTests(LiveServerId, Alpha, collector);
        Assert.True(
            QueryStoreReadBudget.TextFetchWidth(TimeSpan.FromSeconds(2), cutMsPerText, 1000, firstChunk: true) <= 8 / 2,
            $"a cut-empty pass of 8 texts recorded {cutMsPerText} ms an id, which asks for more than half as many next pass");
        Assert.Equal(1, rig.Log.TextFetchBudgetLines(Alpha));

        target.HoldTextUntilCut = false;
        target.MaterialisePerText = TimeSpan.FromMilliseconds(100);
        await PassAsync(rig, target, token, Alpha);
        Assert.InRange(target.TextWidthsFor(Alpha)[1], 1, 4);
        Assert.NotEmpty(await StoredTextsAsync(rig, Alpha, token));
        Assert.Equal(0, rig.Runner.TextFetchFailuresForTests(LiveServerId, Alpha, collector));

        for (var pass = 0; pass < 10 && (await StoredTextsAsync(rig, Alpha, token)).Count < 8; pass++)
        {
            await PassAsync(rig, target, token, Alpha);
        }

        Assert.Equal(Enumerable.Range(1, 8).Select(static id => (long)id).ToList(), await StoredTextsAsync(rig, Alpha, token));
        Assert.Equal(8, target.ShippedTexts.Count);
        Assert.Equal(0, rig.Runner.TextFetchFailuresForTests(LiveServerId, Alpha, collector));
    }

    /// <summary>
    /// #6102 (c): the predicted width's two clamps on the real text fetch. A database slower per id than the whole budget
    /// still asks for one id each pass, never none. A fast one's measured time per id predicts the full 1000 ids per
    /// statement, so the budget leaves its passes exactly as they were: one statement for the whole backlog, no pass held
    /// to the budget.
    /// </summary>
    [Fact]
    public async Task ThePredictedTextWidth_NeverFallsBelowOneId_NorRisesAboveTheStatementWidth()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6102 text-fetch budget pin.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);
        rig.Runner.QueryStoreTextFetchBudgetSpan = TimeSpan.FromSeconds(1);
        var collector = QueryStoreCollector.Instance.Name;

        var now = WholeSecondNow();
        var slow = new Target { ShipPerText = TimeSpan.FromMilliseconds(1500) };
        slow.Rows[Alpha] = Enumerable.Range(1, 3).Select(id => Row(id, now.AddMinutes(-8))).ToList();
        for (var pass = 0; pass < 3; pass++)
        {
            await PassAsync(rig, slow, token, Alpha);
        }

        /* 3 unmeasured, cut empty; then 2 x 1000 / 3 ms an id asks for 1; cut empty again, 2000 ms an id would ask for
           none, and the floor keeps it at 1. */
        Assert.Equal([3, 1, 1], slow.TextWidthsFor(Alpha));
        Assert.Equal(0, QueryStoreReadBudget.TextFetchWidth(
            TimeSpan.FromSeconds(1), rig.Runner.TextFetchMsPerTextForTests(LiveServerId, Alpha, collector), 1000, firstChunk: false));

        var fast = new Target();
        fast.Rows[Zeta] = Enumerable.Range(1, 600).Select(id => Row(id, now.AddMinutes(-8))).ToList();
        for (var pass = 0; pass < 12 && (await StoredTextsAsync(rig, Zeta, token)).Count < 600; pass++)
        {
            await PassAsync(rig, fast, token, Zeta);
        }

        Assert.True(600 == (await StoredTextsAsync(rig, Zeta, token)).Count, "widths " + string.Join(",", fast.TextWidthsFor(Zeta)));
        Assert.Equal([600], fast.TextWidthsFor(Zeta));
        Assert.Equal(1000, QueryStoreReadBudget.TextFetchWidth(
            TimeSpan.FromSeconds(1), rig.Runner.TextFetchMsPerTextForTests(LiveServerId, Zeta, collector), 1000, firstChunk: true));
        Assert.Equal(0, rig.Log.TextFetchBudgetLines(Zeta));
    }

    /// <summary>The query ids the store holds statement text for in <paramref name="database"/>, ascending.</summary>
    private static async Task<List<long>> StoredTextsAsync(Rig rig, string database, CancellationToken token)
    {
        await using var connection = new NpgsqlConnection(rig.Scratch.ConnectionString);
        await connection.OpenAsync(token);
        await using var command = new NpgsqlCommand(
            "SELECT query_id FROM collect.query_store_text WHERE server_id = @s AND database_name = @d ORDER BY query_id",
            connection);
        command.Parameters.AddWithValue("s", LiveServerId);
        command.Parameters.AddWithValue("d", database);
        var result = new List<long>();
        await using var reader = await command.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
        {
            result.Add(reader.GetInt64(0));
        }

        return result;
    }

    private static DateTime WholeSecondNow()
    {
        var now = DateTime.UtcNow;
        return new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);
    }

    /// <summary>The plan ids the store holds for <paramref name="database"/>, ascending.</summary>
    private static async Task<List<long>> StoredPlansAsync(Rig rig, string database, CancellationToken token)
    {
        await using var connection = new NpgsqlConnection(rig.Scratch.ConnectionString);
        await connection.OpenAsync(token);
        await using var command = new NpgsqlCommand(
            "SELECT plan_id FROM collect.query_store_plan_map WHERE server_id = @s AND database_name = @d ORDER BY plan_id",
            connection);
        command.Parameters.AddWithValue("s", LiveServerId);
        command.Parameters.AddWithValue("d", database);
        var result = new List<long>();
        await using var reader = await command.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
        {
            result.Add(reader.GetInt64(0));
        }

        return result;
    }

    private sealed class FakeTargetProvider(Target target) : ITargetProvider
    {
        public CollectorTargetEngine Engine => CollectorTargetEngine.SqlServer;

        public DbConnection CreateConnection(string connectionString) => new FakeConnection(connectionString);

        public DbCommand CreateCommand(CollectorQuery query, DbConnection connection, int commandTimeoutSeconds) =>
            new FakeCommand((FakeConnection)connection, target, query);

        public CollectorTargetFault Classify(Exception exception, bool yieldsOnLockTimeout) => CollectorTargetFault.Unclassified;

        public string WithDatabase(string connectionString, string databaseName) => "db=" + databaseName;

        public (string ConnectionString, CollectorQuery Query) BuildDatabaseListPlan(
            string connectionString, IReadOnlyList<string>? excludedDatabases, IReadOnlyList<string>? databaseScope) =>
            throw new NotSupportedException();
    }

    private sealed class FakeConnection(string connectionString) : DbConnection
    {
        private string _cs = connectionString;
        public string DatabaseName => _cs.StartsWith("db=", StringComparison.Ordinal) ? _cs[3..] : "master";

        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ConnectionString { get => _cs; set => _cs = value ?? string.Empty; }
        public override string Database => string.Empty;
        public override string DataSource => string.Empty;
        public override string ServerVersion => string.Empty;
        public override ConnectionState State => ConnectionState.Open;
        public override void ChangeDatabase(string databaseName) { }
        public override void Close() { }
        public override void Open() { }
        public override Task OpenAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
        protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
    }

    private sealed class FakeCommand(FakeConnection owner, Target target, CollectorQuery query) : DbCommand
    {
        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string CommandText { get; set; } = string.Empty;
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }
        protected override DbConnection? DbConnection { get; set; }
        protected override DbParameterCollection DbParameterCollection => throw new NotSupportedException();
        protected override DbTransaction? DbTransaction { get; set; }
        public override void Cancel() { }
        public override int ExecuteNonQuery() => throw new NotSupportedException();
        public override object? ExecuteScalar() => throw new NotSupportedException();
        public override void Prepare() { }
        protected override DbParameter CreateDbParameter() => throw new NotSupportedException();
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new NotSupportedException();

        /* The runtime-stats read is the one command that carries @cutoff_time. For the slow database it is the
           stand-in for a WAITFOR: it waits until the token is cancelled, so only the budget can end it. Every read
           ships the database's rows past the cutoff, as the real `> @cutoff_time` predicate does, so a row the store
           already holds is offered again only if the watermark went backwards. */
        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            var cutoff = query.Parameters?.FirstOrDefault(static p => p.Name == "@cutoff_time")?.Value as DateTime?;
            if (cutoff is null && query.Text.Contains("FROM sys.query_store_plan AS qsp", StringComparison.Ordinal))
            {
                /* #5813: the plan-XML fetch by ids. Like the real statement it spends DecompressPerPlan on every
                   requested plan before the first row, then ships the plans in ascending plan_id order. */
                var ids = System.Text.RegularExpressions.Regex.Match(query.Text, @"plan_id IN \(([0-9, ]+)\)").Groups[1].Value
                    .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
                    .Select(static id => long.Parse(id, System.Globalization.CultureInfo.InvariantCulture))
                    .Order()
                    .ToArray();
                lock (target.RequestedWidths)
                {
                    target.RequestedWidths.Add((owner.DatabaseName, ids.Length));
                }

                if (target.HoldPlanUntilCut)
                {
                    await Task.Delay(Timeout.Infinite, cancellationToken);
                }

                if (target.DecompressPerPlan > TimeSpan.Zero)
                {
                    await Task.Delay(target.DecompressPerPlan * ids.Length, cancellationToken);
                }

                return new PlanReader(target, ids);
            }

            if (cutoff is null && query.Text.Contains("FROM sys.query_store_query AS qsq", StringComparison.Ordinal))
            {
                /* #6102: the statement-text fetch by ids. Like the real statement it spends MaterialisePerText on every
                   requested id before the first row, then ships the texts in ascending query_id order. */
                var ids = System.Text.RegularExpressions.Regex.Match(query.Text, @"query_id IN \(([0-9, ]+)\)").Groups[1].Value
                    .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
                    .Select(static id => long.Parse(id, System.Globalization.CultureInfo.InvariantCulture))
                    .Order()
                    .ToArray();
                lock (target.RequestedTextWidths)
                {
                    target.RequestedTextWidths.Add((owner.DatabaseName, ids.Length));
                }

                if (target.HoldTextUntilCut)
                {
                    await Task.Delay(Timeout.Infinite, cancellationToken);
                }

                if (target.MaterialisePerText > TimeSpan.Zero)
                {
                    await Task.Delay(target.MaterialisePerText * ids.Length, cancellationToken);
                }

                return new PlanReader(target, ids, text: true);
            }

            if (cutoff is null && target.Enumerated is { } listed && query.Text.Contains("FROM sys.databases AS d", StringComparison.Ordinal))
            {
                return new Reader(listed.Select(static d => new object[] { d }).ToArray());
            }

            if (cutoff is null)
            {
                return new Reader(Array.Empty<object[]>());
            }

            /* The enumerated arm reads every database on the master connection through [db].sys.sp_executesql. */
            var item = System.Text.RegularExpressions.Regex.Match(query.Text, @"EXECUTE \[([^\]]+)\]\.sys\.sp_executesql");
            var database = item.Success ? item.Groups[1].Value : owner.DatabaseName;
            if (database == target.SlowOn)
            {
                Interlocked.Increment(ref target.SlowReads);
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }

            var floor = DateTime.SpecifyKind(cutoff.Value, DateTimeKind.Utc);
            var rows = target.Rows.TryGetValue(database, out var all) ? all : new List<object[]>();
            return new Reader(rows.Where(r => ((DateTimeOffset)r[4]).UtcDateTime > floor).ToArray());
        }
    }

    /// <summary>The plan fetch's rows (plan_id, plan hash, plan XML); each takes <see cref="Target.ShipPerPlan"/> to arrive.</summary>
    private sealed class PlanReader(Target target, long[] ids, bool text = false) : DbDataReader
    {
        private int _row = -1;

        public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            if (_row + 1 >= ids.Length)
            {
                return false;
            }

            var shipPer = text ? target.ShipPerText : target.ShipPerPlan;
            if (shipPer > TimeSpan.Zero)
            {
                await Task.Delay(shipPer, cancellationToken);
            }

            _row++;
            var shipped = text ? target.ShippedTexts : target.ShippedPlans;
            lock (shipped)
            {
                shipped.Add(ids[_row]);
            }

            return true;
        }

        public override bool Read() => throw new NotSupportedException();
        public override bool NextResult() => false;
        public override long GetInt64(int o) => ids[_row];
        public override string GetString(int o) => o == 1
            ? "0x" + ids[_row].ToString("X8", System.Globalization.CultureInfo.InvariantCulture)
            : text
                ? "SELECT " + ids[_row].ToString(System.Globalization.CultureInfo.InvariantCulture)
                : "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.0\" />";
        public override bool IsDBNull(int o) => false;
        public override object GetValue(int o) => o == 0 ? ids[_row] : GetString(o);
        public override int FieldCount => 3;
        public override bool HasRows => ids.Length > 0;
        public override bool IsClosed => false;
        public override int Depth => 0;
        public override int RecordsAffected => -1;
        public override object this[int o] => GetValue(o);
        public override object this[string n] => throw new NotSupportedException();
        public override bool GetBoolean(int o) => throw new NotSupportedException();
        public override byte GetByte(int o) => throw new NotSupportedException();
        public override long GetBytes(int o, long d, byte[]? b, int bo, int l) => throw new NotSupportedException();
        public override char GetChar(int o) => throw new NotSupportedException();
        public override long GetChars(int o, long d, char[]? b, int bo, int l) => throw new NotSupportedException();
        public override string GetDataTypeName(int o) => throw new NotSupportedException();
        public override DateTime GetDateTime(int o) => throw new NotSupportedException();
        public override decimal GetDecimal(int o) => throw new NotSupportedException();
        public override double GetDouble(int o) => throw new NotSupportedException();
        public override IEnumerator GetEnumerator() => throw new NotSupportedException();
        public override Type GetFieldType(int o) => throw new NotSupportedException();
        public override float GetFloat(int o) => throw new NotSupportedException();
        public override Guid GetGuid(int o) => throw new NotSupportedException();
        public override short GetInt16(int o) => throw new NotSupportedException();
        public override int GetInt32(int o) => throw new NotSupportedException();
        public override string GetName(int o) => throw new NotSupportedException();
        public override int GetOrdinal(string n) => throw new NotSupportedException();
        public override int GetValues(object[] v) => throw new NotSupportedException();
    }

    private sealed class Reader(object[][] rows) : DbDataReader
    {
        private int _row = -1;
        private object[] Cur => rows[_row];

        public override bool Read() => ++_row < rows.Length;
        public override bool NextResult() => false;
        public override string GetString(int o) => (string)Cur[o];
        public override long GetInt64(int o) => (long)Cur[o];
        public override DateTime GetDateTime(int o) => (DateTime)Cur[o];
        public override bool GetBoolean(int o) => (bool)Cur[o];
        public override bool IsDBNull(int o) => Cur[o] is DBNull;
        public override object GetValue(int o) => Cur[o];
        public override int FieldCount => rows.Length == 0 ? 0 : rows[0].Length;
        public override bool HasRows => rows.Length > 0;
        public override bool IsClosed => false;
        public override int Depth => 0;
        public override int RecordsAffected => -1;
        public override object this[int o] => Cur[o];
        public override object this[string n] => throw new NotSupportedException();
        public override byte GetByte(int o) => throw new NotSupportedException();
        public override long GetBytes(int o, long d, byte[]? b, int bo, int l) => throw new NotSupportedException();
        public override char GetChar(int o) => throw new NotSupportedException();
        public override long GetChars(int o, long d, char[]? b, int bo, int l) => throw new NotSupportedException();
        public override string GetDataTypeName(int o) => throw new NotSupportedException();
        public override decimal GetDecimal(int o) => throw new NotSupportedException();
        public override double GetDouble(int o) => throw new NotSupportedException();
        public override IEnumerator GetEnumerator() => throw new NotSupportedException();
        public override Type GetFieldType(int o) => throw new NotSupportedException();
        public override float GetFloat(int o) => throw new NotSupportedException();
        public override Guid GetGuid(int o) => throw new NotSupportedException();
        public override short GetInt16(int o) => throw new NotSupportedException();
        public override int GetInt32(int o) => throw new NotSupportedException();
        public override string GetName(int o) => throw new NotSupportedException();
        public override int GetOrdinal(string n) => throw new NotSupportedException();
        public override int GetValues(object[] v) => throw new NotSupportedException();
    }
}
