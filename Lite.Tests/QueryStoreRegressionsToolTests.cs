/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's get_query_store_regressions (#2484), the twin of Darling's.
///
/// <para>Two things are pinned here that no reader-level test would see. The four empty branches - only one
/// of which is good news - and the DEDUP, which is correctness rather than performance on this read: the
/// baseline arm is unbounded while the recent arm is a short window, so the two sides being compared have
/// systematically different re-collection density per interval, and losing the dedup moves the averages and
/// the 25% gate for reasons that have nothing to do with the query.</para>
/// </summary>
public sealed class QueryStoreRegressionsToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "RegressionsSrv";
    private const string Db = "AppDb";

    /* Lite DERIVES its server id from the storage name; a hardcoded one would seed rows the tool looks
       straight past and pass the never-collected assertion for the wrong reason. */
    private readonly int _serverId;

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public QueryStoreRegressionsToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-regressions-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = ServerName,
            IsEnabled = true,
        };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    [Fact]
    public async Task FourKindsOfNothing_AreFourDifferentAnswers()
    {
        var service = new LocalDataService(_duckDb);
        var baseNow = Truncate(DateTime.UtcNow);

        /* 1. nothing ever collected. */
        var never = Root(await McpQueryTools.GetQueryStoreRegressions(service, _serverManager, ServerName, 24));
        Assert.Equal("unavailable", never.GetProperty("status").GetString());
        var neverText = never.GetProperty("message").GetString()!;
        Assert.Contains("EVER", neverText, StringComparison.Ordinal);
        Assert.Contains("Query Store may be OFF", neverText, StringComparison.Ordinal);

        /*
            2. a baseline exists but nothing landed in the window. Seeded FIRST, and the ordering is
            load-bearing: once a row exists 30 minutes ago no window a caller can legally ask for excludes
            it, so this branch is unreachable after the recent row is planted. The four states are walked
            by moving hours_back over one fixed set of rows rather than by deleting rows between
            assertions.
        */
        await SeedAsync(baseNow.AddHours(-40), executions: 100, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 2);

        var noRecent = Root(await McpQueryTools.GetQueryStoreRegressions(service, _serverManager, ServerName, 2));
        Assert.Equal("empty", noRecent.GetProperty("status").GetString());
        var noRecentText = noRecent.GetProperty("message").GetString()!;
        Assert.Contains("Widen hours_back", noRecentText, StringComparison.Ordinal);
        Assert.DoesNotContain("EVER", noRecentText, StringComparison.Ordinal);

        /* 3. both sides collected, nothing regressed: the ONE good-news answer. The store must hold the whole span the
           read compares (#4966): the probe checks from the baseline's start, 7 days before the window, so a row older
           than that (outside both windows, another query) shows the span covered. Without it the window is cut, and
           the all-clear rightly gives way to the cut sentence. */
        await SeedAsync(baseNow.AddDays(-10), executions: 100, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 3, queryId: 99);
        await SeedAsync(baseNow.AddMinutes(-30), executions: 100, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 1);

        var clear = Root(await McpQueryTools.GetQueryStoreRegressions(service, _serverManager, ServerName, 24));
        Assert.Equal("empty", clear.GetProperty("status").GetString());
        var clearText = clear.GetProperty("message").GetString()!;
        Assert.Contains("all-clear", clearText, StringComparison.Ordinal);
        Assert.DoesNotContain("EVER", clearText, StringComparison.Ordinal);
        Assert.DoesNotContain("Widen", clearText, StringComparison.Ordinal);

        /*
            4. recent rows but NO baseline - the branch this read exists to get right, reached from the
            SAME rows by widening the window until every one of them falls inside it. There is then nothing
            to compare against, and answering "no regressions" would be a confident wrong answer rather
            than a missing one.
        */
        var noBaseline = Root(await McpQueryTools.GetQueryStoreRegressions(service, _serverManager, ServerName, 48));
        Assert.Equal("unavailable", noBaseline.GetProperty("status").GetString());
        var noBaselineText = noBaseline.GetProperty("message").GetString()!;
        Assert.Contains("no baseline", noBaselineText, StringComparison.Ordinal);
        Assert.Contains("NOT a clean bill of health", noBaselineText, StringComparison.Ordinal);

        /* The baseline is a FIXED 7-day lookback now, not "everything before the window" - so the message
           names the lookback rather than telling the caller to shorten hours_back, which no longer helps
           once the whole lookback itself has nothing in it. */
        Assert.Contains("baseline lookback yet", noBaselineText, StringComparison.Ordinal);
        Assert.DoesNotContain("Widen", noBaselineText, StringComparison.Ordinal);

        /* Four sentences, not one sentence four times. */
        var messages = new[] { neverText, noBaselineText, noRecentText, clearText };
        Assert.Equal(4, messages.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public async Task ARealRegression_IsRanked_AndSurvivesTheDedup()
    {
        var service = new LocalDataService(_duckDb);
        var baseNow = Truncate(DateTime.UtcNow);

        /*
            Query 7 doubles its CPU and quadruples its duration between the baseline and the window. The
            baseline interval is ALSO re-collected with a higher cumulative count, which is what the dedup
            has to survive: un-deduped, the baseline exec_count would be 140 (50 + 90) rather than 90 and
            the baseline averages would be an avg-of-avgs over two snapshots of one interval.
        */
        await SeedAsync(baseNow.AddHours(-40), 50, 1000, 1000, intervalId: 3, queryId: 7);
        await SeedAsync(baseNow.AddHours(-39), 90, 1000, 1000, intervalId: 3, queryId: 7);
        /* #6781: enough recent runs that the rise adds at least 1 s of CPU (QueryStoreRegressionRules.CostMinAddedCpuMs). */
        await SeedAsync(baseNow.AddMinutes(-30), 500, 4000, 4000, intervalId: 4, queryId: 7);

        var hit = Root(await McpQueryTools.GetQueryStoreRegressions(service, _serverManager, ServerName, 24));
        Assert.Equal(ServerName, hit.GetProperty("server").GetString());
        Assert.Equal(1, hit.GetProperty("regression_count").GetInt32());
        Assert.False(hit.GetProperty("truncated").GetBoolean());

        var row = hit.GetProperty("regressions")[0];
        Assert.Equal(7, row.GetProperty("query_id").GetInt64());
        Assert.Equal(Db, row.GetProperty("database_name").GetString());

        /* 1 ms -> 4 ms is +300%, the CRITICAL band (> 100%). */
        Assert.Equal("CRITICAL", row.GetProperty("severity").GetString());
        Assert.Equal(1.0, row.GetProperty("baseline_duration_ms").GetDouble(), 6);
        Assert.Equal(4.0, row.GetProperty("recent_duration_ms").GetDouble(), 6);
        Assert.Equal(300.0, row.GetProperty("duration_regression_percent").GetDouble(), 6);

        /* The ranking key: 3 ms per execution across the 500 recent executions. */
        Assert.Equal(1500.0, row.GetProperty("additional_duration_ms").GetDouble(), 6);

        /* 90, not 140. This is the dedup, and it is the assertion the whole read leans on. */
        Assert.Equal(90, row.GetProperty("baseline_exec_count").GetInt64());
        Assert.Equal(500, row.GetProperty("recent_exec_count").GetInt64());
    }

    /// <summary>
    /// #6750: a server whose first row comes after the 168-hour window's start has an empty baseline and a cut recent window, and the note
    /// says so with the three shared sentences, not with the one for a baseline that holds part of its span. A 24-hour read keeps the old
    /// sentence.
    /// </summary>
    [Fact]
    public async Task AServerWhoseFirstRowIsAfterTheWindowStart_SaysTheBaselineIsEmpty_AndTheRecentWindowIsCut()
    {
        var service = new LocalDataService(_duckDb);
        var baseNow = Truncate(DateTime.UtcNow);

        await SeedAsync(baseNow.AddHours(-30), 3, 1000, 1000, intervalId: 3, queryId: 7, executionType: "Aborted");
        await SeedAsync(baseNow.AddHours(-10), 6, 1000, 1000, intervalId: 4, queryId: 7, executionType: "Aborted");

        var hit = Root(await McpQueryTools.GetQueryStoreRegressions(service, _serverManager, ServerName, 168));
        Assert.True(hit.GetProperty("window_truncated").GetBoolean());
        var note = hit.GetProperty("truncation_note").GetString()!;
        Assert.EndsWith(
            PerformanceMonitor.Common.QueryStoreRegressionRules.EmptyBaselineTail + " " + PerformanceMonitor.Common.QueryStoreRegressionRules.RecentCutTail + " " + PerformanceMonitor.Common.QueryStoreRegressionRules.RecentHoursHeldTail,
            note, StringComparison.Ordinal);
        Assert.DoesNotContain(PerformanceMonitor.Common.QueryStoreRegressionRules.BaselineWindowTail, note, StringComparison.Ordinal);
        Assert.Equal("timeouts", Assert.Single(hit.GetProperty("regressions").EnumerateArray()).GetProperty("regression_reason").GetString());

        /* A 24-hour read of the same store: 3 timeouts in the 6 hours of baseline it holds (0.5 an hour) against 36 in the 24-hour window
           (1.5 an hour). It must be a rise, or the answer is the empty envelope, which carries the note under hints, not at the top. */
        await SeedAsync(baseNow.AddHours(-2), 30, 1000, 1000, intervalId: 5, queryId: 7, executionType: "Aborted");
        var day = Root(await McpQueryTools.GetQueryStoreRegressions(service, _serverManager, ServerName, 24));
        Assert.EndsWith(PerformanceMonitor.Common.QueryStoreRegressionRules.BaselineWindowTail, day.GetProperty("truncation_note").GetString()!, StringComparison.Ordinal);
    }

    /// <summary>
    /// #6750: the per-hour rates divide by the hours the store HOLDS, not by the 7 days and the window it was asked for. The store's
    /// first row is 30 hours back, so a 24-hour read holds 6 hours of baseline. The steady query runs 2 timeouts an hour in both
    /// (12 in the 6 hours held, 48 in the 24 hours), so it is not a rise: over the 168 hours asked for its baseline rate reads 0.07 an
    /// hour and it would be flagged. The rising query's baseline rate is its count over the hours held.
    /// </summary>
    [Fact]
    public async Task AStoreHoldingPartOfTheBaseline_DividesTheRatesByTheHoursHeld()
    {
        var service = new LocalDataService(_duckDb);
        var baseNow = Truncate(DateTime.UtcNow);

        await SeedAsync(baseNow.AddHours(-30), 12, 1000, 1000, intervalId: 3, queryId: 8, executionType: "Aborted");
        await SeedAsync(baseNow.AddHours(-3), 48, 1000, 1000, intervalId: 4, queryId: 8, executionType: "Aborted");
        await SeedAsync(baseNow.AddHours(-30), 6, 1000, 1000, intervalId: 5, queryId: 7, executionType: "Aborted");
        await SeedAsync(baseNow.AddHours(-3), 120, 1000, 1000, intervalId: 6, queryId: 7, executionType: "Aborted");

        var hit = Root(await McpQueryTools.GetQueryStoreRegressions(service, _serverManager, ServerName, 24));
        var row = Assert.Single(hit.GetProperty("regressions").EnumerateArray());
        Assert.Equal(7, row.GetProperty("query_id").GetInt64());
        Assert.Equal("timeouts", row.GetProperty("regression_reason").GetString());

        /* 6 runs over the 6 hours held (the 30 hours back to the window's start), not over 168. */
        Assert.Equal(1.0, row.GetProperty("baseline_aborted_per_hour").GetDouble(), 1);
        Assert.Equal(5.0, row.GetProperty("recent_aborted_per_hour").GetDouble(), 1);
    }

    /// <summary>
    /// #6654: with no database named, a regression in a database the server dropped (an earlier database_states snapshot lists it,
    /// the newest does not) is left out before the cap and named in databases_left_out; naming the database reads it as asked.
    /// </summary>
    [Fact]
    public async Task ADroppedDatabasesRegression_IsLeftOut_AndNamed_UntilTheDatabaseIsNamed()
    {
        var service = new LocalDataService(_duckDb);
        var baseNow = Truncate(DateTime.UtcNow);
        const string Gone = "GoneDb";

        await SeedAsync(baseNow.AddDays(-10), 100, 1000, 1000, intervalId: 9, queryId: 99);
        foreach (var (queryId, database) in new[] { (20L, Db), (30L, Gone) })
        {
            await SeedAsync(baseNow.AddHours(-40), 100, 1000, 1000, intervalId: 1, queryId: queryId, database: database);
            /* #6781: 600 recent runs at +2 ms add 1.2 s of CPU, over the 1 s floor. */
            await SeedAsync(baseNow.AddMinutes(-30), 600, 3000, 3000, intervalId: 2, queryId: queryId, database: database);
        }

        var connection = await SeedConnectionAsync();
        foreach (var (age, names) in new[] { (TimeSpan.FromHours(1), new[] { Db, Gone }), (TimeSpan.FromMinutes(5), new[] { Db }) })
        {
            var at = DateTime.UtcNow - age;
            var n = 1;
            foreach (var name in names)
            {
                using var cmd = connection.CreateCommand();
                cmd.CommandText = "INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES "
                    + $"({_nextId++}, TIMESTAMP '{at:yyyy-MM-dd HH:mm:ss}', {_serverId}, '{ServerName}', '{name}', {n++}, 'ONLINE', false)";
                await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            }
        }

        var all = Root(await McpQueryTools.GetQueryStoreRegressions(service, _serverManager, ServerName, 24));
        Assert.Equal([20L], all.GetProperty("regressions").EnumerateArray().Select(r => r.GetProperty("query_id").GetInt64()).ToArray());
        Assert.Equal([Gone], all.GetProperty("databases_left_out").EnumerateArray().Select(x => x.GetString()).ToArray());

        var named = Root(await McpQueryTools.GetQueryStoreRegressions(service, _serverManager, ServerName, 24, Gone));
        Assert.Equal([30L], named.GetProperty("regressions").EnumerateArray().Select(r => r.GetProperty("query_id").GetInt64()).ToArray());
        Assert.True(named.GetProperty("databases_left_out").ValueKind == JsonValueKind.Null);
    }

    [Fact]
    public async Task AnOutOfRangeCap_IsRefused_NotSilentlyClamped()
    {
        var service = new LocalDataService(_duckDb);
        await SeedAsync(Truncate(DateTime.UtcNow).AddMinutes(-30), 100, 1000, 1000, intervalId: 1);

        var tooBig = await McpQueryTools.GetQueryStoreRegressions(service, _serverManager, ServerName, 24, null, 5000);
        Assert.Contains("exceeds maximum of", tooBig, StringComparison.Ordinal);
        Assert.Contains("1000", tooBig, StringComparison.Ordinal);
    }

    /// <summary>
    /// #6877: an as_of replay whose 24 hour window and 7 day baseline both end before the stored rows start (the only rows are 3 days
    /// back, the replay is 6 days back), on a server whose collector was storing rows 20 days back. The answer names where the stored
    /// data starts and says there is nothing to compare, instead of "no sample stored yet" quoting the server now.
    /// </summary>
    [Fact]
    public async Task AReplayWindowEndingBeforeTheStoredRows_OnAServerCollectedBeforeIt_NamesWhereTheDataStarts()
    {
        var service = new LocalDataService(_duckDb);
        var baseNow = Truncate(DateTime.UtcNow);
        await SeedAsync(baseNow.AddDays(-3), 100, 1000, 1000, intervalId: 1);
        await SeedCollectionLogAsync(baseNow.AddDays(-20), baseNow);

        var root = Root(await McpQueryTools.GetQueryStoreRegressions(service, _serverManager, ServerName, 24, as_of: ReplayAsOf(baseNow, 6)));

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        Assert.Contains("This window (", message, StringComparison.Ordinal);
        Assert.Contains($"is older than this server's raw Query Store data, which starts {baseNow.AddDays(-3).ToString("MMM d", System.Globalization.CultureInfo.InvariantCulture)}, so there is nothing to compare", message, StringComparison.Ordinal);
        Assert.DoesNotContain("no Query Store sample has been stored yet", message, StringComparison.Ordinal);
        var hints = root.GetProperty("hints");
        Assert.True(hints.GetProperty("window_truncated").GetBoolean());
        Assert.NotEqual(JsonValueKind.Null, hints.GetProperty("effective_start").ValueKind);
    }

    /// <summary>#6877: a server whose first productive run is after the replay window keeps the first-interval answers, labelled as the server now.</summary>
    [Fact]
    public async Task AReplayWindowEndingBeforeTheStoredRows_OnAServerAddedAfterIt_DoesNotClaimTheDataAgedOut()
    {
        var service = new LocalDataService(_duckDb);
        var baseNow = Truncate(DateTime.UtcNow);
        await SeedAsync(baseNow.AddDays(-3), 100, 1000, 1000, intervalId: 1);
        await SeedCollectionLogAsync(baseNow.AddDays(-2), baseNow);

        var root = Root(await McpQueryTools.GetQueryStoreRegressions(service, _serverManager, ServerName, 24, as_of: ReplayAsOf(baseNow, 6)));

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        Assert.DoesNotContain("This window (", message, StringComparison.Ordinal);
        Assert.DoesNotContain("which starts", message, StringComparison.Ordinal);
        Assert.DoesNotContain("aged out", message, StringComparison.Ordinal);
    }

    private static string ReplayAsOf(DateTime now, int daysBack) =>
        now.AddDays(-daysBack).ToString("yyyy-MM-ddTHH:mm:ssZ", System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>One successful query_store run a day from <paramref name="from"/> to <paramref name="to"/>.</summary>
    private async Task SeedCollectionLogAsync(DateTime from, DateTime to)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        for (var t = from; t <= to; t = t.AddDays(1))
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status, rows_collected)
VALUES ($1, $2, $3, 'query_store', $4, 'SUCCESS', 5)";
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
            cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = t });
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    private static DateTime Truncate(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedAsync(
        DateTime collectionTime, long executions, long avgDurationUs, long avgCpuUs, long intervalId, long queryId = 1, string database = Db, string executionType = "Regular")
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads,
     runtime_stats_interval_id, query_text, last_execution_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryId });
        cmd.Parameters.Add(new DuckDBParameter { Value = 9L });
        cmd.Parameters.Add(new DuckDBParameter { Value = executionType });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions });
        cmd.Parameters.Add(new DuckDBParameter { Value = avgDurationUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = avgCpuUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = 100L });
        cmd.Parameters.Add(new DuckDBParameter { Value = intervalId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT * FROM dbo.Widgets" });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        await cmd.ExecuteNonQueryAsync();
    }
}
