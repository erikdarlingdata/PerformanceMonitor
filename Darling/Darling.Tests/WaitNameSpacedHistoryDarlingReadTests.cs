/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Darling reads of wait history stored under two spellings. Before #4884 four wait names reached the store with
/// the trailing space <c>sys.dm_os_wait_stats</c> reports on SQL Server 2022 and 2025 (<c>EDC_DOPP_LOCK </c>); from
/// #4884 on they are trimmed at collection (<c>EDC_DOPP_LOCK</c>). A store upgraded across that change holds both.
/// Every read that groups by the name has to merge them into one row under the clean name, and every read that looks
/// a name up has to find both. These run each changed read against a real store with both spellings seeded.
/// </summary>
[Collection("live-postgres")]
public sealed class WaitNameSpacedHistoryLivePostgresTests
{
    private const string ServerName = "wait-name-spaced-history-e2e";
    private const int ServerId = -488417;

    private const string Clean = "EDC_DOPP_LOCK";
    private const string Spaced = "EDC_DOPP_LOCK ";

    /* Heavier than either spelling of EDC_DOPP_LOCK on its own (300 each) and lighter than the two together (600),
       so a ranking or a "top wait" only comes out right when the read sums both spellings. Lands in the same
       FinOps category ('Other') as EDC_DOPP_LOCK. */
    private const string Rival = "PREEMPTIVE_OS_WRITEFILE";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* ─────────────────────────── viewer ─────────────────────────── */

    [Fact]
    public Task ViewerWaitPicker_ANameStoredBothWays_IsOneCleanNameRankedByTheSum_AgainstDevPostgres() => RunAsync(async (connection, _, cs, ct) =>
    {
        var (start, end) = await SeedSplitTotalsAsync(connection, ct);

        await using var viewer = new ViewerDataService(cs);
        var types = await viewer.GetDistinctWaitTypesAsync(ServerId, start, end, cancellationToken: ct);

        Assert.Equal(new[] { Clean, Rival }, types);
    });

    [Fact]
    public Task ViewerWaitTrends_ByTheCleanName_AreOneSeriesThatIncludesTheSpacedHistory_AgainstDevPostgres() => RunAsync(async (connection, _, cs, ct) =>
    {
        /* m0 is minute-aligned, so with the 1-minute buckets a 12-minute window gets, t2 and t3 share the m0+5 bucket. */
        var m0 = MinuteAligned(DateTime.UtcNow.AddMinutes(-30));
        var t1 = m0.AddSeconds(10);
        var t2 = m0.AddMinutes(5).AddSeconds(10);
        var t3 = m0.AddMinutes(5).AddSeconds(40);
        var t4 = m0.AddMinutes(10).AddSeconds(10);

        /* t1..t3 carry no stored interval (rows written before the interval column), so their rate comes from the
           previous collection of the SAME wait: t2's from t1 (300 s), t3's from t2 (30 s) across the spelling
           change. t4 stores a measured 270 s. */
        await InsertWaitStatAsync(connection, ct, t1, Spaced, deltaWaitMs: 100, deltaTasks: 1, sampleIntervalSeconds: null);
        await InsertWaitStatAsync(connection, ct, t2, Spaced, deltaWaitMs: 300, deltaTasks: 3, sampleIntervalSeconds: null);
        await InsertWaitStatAsync(connection, ct, t3, Clean, deltaWaitMs: 60, deltaTasks: 1, sampleIntervalSeconds: null);
        await InsertWaitStatAsync(connection, ct, t4, Clean, deltaWaitMs: 2700, deltaTasks: 9, sampleIntervalSeconds: 270);

        await using var viewer = new ViewerDataService(cs);
        var trends = await viewer.GetWaitStatsTrendsByTypesAsync(ServerId, new List<string> { Clean }, m0.AddMinutes(-1), m0.AddMinutes(11), ct);

        var series = Assert.Single(trends);
        Assert.Equal(Clean, series.Key);

        /* t1 has no earlier collection, so it is not a point. The m0+5 bucket is t2 and t3 together: 360 ms over
           330 s. The m0+10 bucket is t4 alone: 2700 ms over 270 s. */
        Assert.Equal(new[] { m0.AddMinutes(5), m0.AddMinutes(10) }, series.Value.Select(p => p.CollectionTime).ToArray());
        Assert.Equal(360.0 / 330.0, series.Value[0].WaitTimeMsPerSecond, precision: 6);
        Assert.Equal(90.0, series.Value[0].AvgMsPerWait, precision: 6);
        Assert.Equal(10.0, series.Value[1].WaitTimeMsPerSecond, precision: 6);
    });

    [Fact]
    public Task ViewerCurrentWaitsTrend_ANameStoredBothWays_IsOneRowWithTheSummedDuration_AgainstDevPostgres() => RunAsync(async (connection, _, cs, ct) =>
    {
        var t = MinuteAligned(DateTime.UtcNow.AddMinutes(-20)).AddSeconds(10);
        await InsertWaitingTaskAsync(connection, ct, t, Spaced, waitDurationMs: 400);
        await InsertWaitingTaskAsync(connection, ct, t, Clean, waitDurationMs: 600);
        await InsertWaitingTaskAsync(connection, ct, t.AddMinutes(3), Spaced, waitDurationMs: 50);

        await using var viewer = new ViewerDataService(cs);
        var points = await viewer.GetWaitingTaskTrendAsync(ServerId, t.AddMinutes(-5), t.AddMinutes(5), ct);

        Assert.Equal(
            new[] { (Clean, 1000L), (Clean, 50L) },
            points.Select(p => (p.WaitType, p.TotalWaitMs)).ToArray());
    });

    [Fact]
    public Task ViewerWaitCategorySummary_TopWait_SumsBothSpellingsUnderTheCleanName_AgainstDevPostgres() => RunAsync(async (connection, _, cs, ct) =>
    {
        await SeedSplitTotalsAsync(connection, ct);

        await using var viewer = new ViewerDataService(cs);
        var rows = await viewer.GetWaitCategorySummaryAsync(ServerId, hoursBack: 2, ct);

        var other = Assert.Single(rows, r => r.Category == "Other");
        Assert.Equal(Clean, other.TopWaitType);
        Assert.Equal(600L, other.TopWaitTimeMs);
        Assert.Equal(1100L, other.TotalWaitTimeMs);
    });

    [Fact]
    public Task ViewerQueryDrillDown_ByTheCleanName_FindsASnapshotStoredWithTheSpace_AgainstDevPostgres() => RunAsync(async (connection, _, cs, ct) =>
    {
        var t = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-20));
        await InsertQuerySnapshotAsync(connection, ct, t, sessionId: 71, Spaced);
        await InsertQuerySnapshotAsync(connection, ct, t.AddMinutes(2), sessionId: 72, Clean);

        await using var viewer = new ViewerDataService(cs);
        var rows = await viewer.GetQuerySnapshotsByWaitTypeAsync(ServerId, Clean, t.AddMinutes(-5), t.AddMinutes(5), cancellationToken: ct);

        Assert.Equal(new[] { 72, 71 }, rows.Select(r => r.SessionId).ToArray());
    });

    /* ─────────────────────────── MCP and web reads ─────────────────────────── */

    [Fact]
    public Task McpWaitStats_ANameStoredBothWays_IsOneRowWithTheSummedValues_AgainstDevPostgres() => RunAsync(async (connection, postgres, _, ct) =>
    {
        var (start, end) = await SeedSplitTotalsAsync(connection, ct);

        var rows = await DarlingDataReader.GetWaitStatsAsync(postgres, ServerId, start, end, 10, ct);

        Assert.Equal(
            new[] { (Clean, 5L, 600L, 50L), (Rival, 5L, 500L, 50L) },
            rows.Select(r => (r.WaitType, r.TotalWaitingTasks, r.TotalWaitTimeMs, r.TotalSignalWaitTimeMs)).ToArray());
    });

    [Fact]
    public Task McpWaitTypes_ANameStoredBothWays_IsOneCleanNameRankedByTheSum_AgainstDevPostgres() => RunAsync(async (connection, postgres, _, ct) =>
    {
        var (start, end) = await SeedSplitTotalsAsync(connection, ct);

        var types = await DarlingDataReader.GetDistinctWaitTypesAsync(postgres, ServerId, start, end, ct);

        Assert.Equal(new[] { Clean, Rival }, types);
    });

    [Fact]
    public Task McpWaitTrend_ByTheCleanName_IncludesTheSpacedHistory_AgainstDevPostgres() => RunAsync(async (connection, postgres, _, ct) =>
    {
        var m0 = MinuteAligned(DateTime.UtcNow.AddMinutes(-30));
        var t1 = m0.AddSeconds(10);
        var t2 = m0.AddMinutes(5).AddSeconds(10);
        var t3 = m0.AddMinutes(10).AddSeconds(10);
        await InsertWaitStatAsync(connection, ct, t1, Spaced, deltaWaitMs: 100, deltaTasks: 1, sampleIntervalSeconds: null);
        await InsertWaitStatAsync(connection, ct, t2, Spaced, deltaWaitMs: 300, deltaTasks: 3, sampleIntervalSeconds: null);
        await InsertWaitStatAsync(connection, ct, t3, Clean, deltaWaitMs: 600, deltaTasks: 6, sampleIntervalSeconds: 300);

        var points = await DarlingDataReader.GetWaitTrendAsync(postgres, ServerId, Clean, m0.AddMinutes(-1), m0.AddMinutes(11), ct);
        Assert.Equal(new[] { (t2, 1.0), (t3, 2.0) }, points.Select(p => (p.CollectionTime, p.WaitTimeMsPerSecond)).ToArray());

        var buckets = await DarlingDataReader.GetWaitBucketsAsync(postgres, ServerId, Clean, m0.AddMinutes(-1), m0.AddMinutes(11), 1, ct);
        Assert.Equal(
            new[] { (m0.AddMinutes(5), 1.0), (m0.AddMinutes(10), 2.0) },
            buckets.Select(b => (b.BucketStart, b.WaitTimeMsPerSecond)).ToArray());
    });

    [Fact]
    public Task McpCurrentWaitsTrend_ANameStoredBothWays_IsOneRowWithTheSummedDuration_AgainstDevPostgres() => RunAsync(async (connection, postgres, _, ct) =>
    {
        var t = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-20));
        await InsertWaitingTaskAsync(connection, ct, t, Spaced, waitDurationMs: 400);
        await InsertWaitingTaskAsync(connection, ct, t, Clean, waitDurationMs: 600);
        await InsertWaitingTaskAsync(connection, ct, t.AddMinutes(3), Spaced, waitDurationMs: 50);

        var rows = await DarlingDataReader.GetWaitingTaskTrendAsync(postgres, ServerId, t.AddMinutes(-5), t.AddMinutes(5), ct);

        Assert.Equal(
            new[] { (t, Clean, 1000L), (t.AddMinutes(3), Clean, 50L) },
            rows.Select(r => (r.CollectionTime, r.WaitType, r.TotalWaitMs)).ToArray());
    });

    /* ─────────────────────────── daily summary, analysis ─────────────────────────── */

    [Fact]
    public Task DailySummary_TopWait_SumsBothSpellingsUnderTheCleanName_AgainstDevPostgres() => RunAsync(async (connection, _, _, ct) =>
    {
        var day = DarlingMcpTestData.Naive(DateTime.UtcNow.Date.AddDays(-2));
        await InsertWaitStatAsync(connection, ct, day.AddHours(1), Spaced, deltaWaitMs: 300, deltaTasks: 3, sampleIntervalSeconds: 60);
        await InsertWaitStatAsync(connection, ct, day.AddHours(2), Clean, deltaWaitMs: 300, deltaTasks: 2, sampleIntervalSeconds: 60);
        await InsertWaitStatAsync(connection, ct, day.AddHours(1), Rival, deltaWaitMs: 500, deltaTasks: 5, sampleIntervalSeconds: 60);

        await using var read = new NpgsqlCommand(DailySummarySql.RangeSql, connection);
        read.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = day });
        read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = day.AddDays(1) });
        read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = EventWindowFloor.For(day) });

        string? topWait = null;
        await using (var reader = await read.ExecuteReaderAsync(ct))
        {
            var dayOrd = reader.GetOrdinal("day");
            var topOrd = reader.GetOrdinal("top_wait_type");
            while (await reader.ReadAsync(ct))
            {
                if (reader.GetDateTime(dayOrd) == day)
                {
                    topWait = reader.IsDBNull(topOrd) ? null : reader.GetString(topOrd);
                }
            }
        }

        Assert.Equal(Clean, topWait);
    });

    [Fact]
    public Task AnalysisWaitFacts_ANameStoredBothWays_IsOneRowWithTheSummedValues_AgainstDevPostgres() => RunAsync(async (connection, _, _, ct) =>
    {
        var (start, end) = await SeedSplitTotalsAsync(connection, ct);

        var rows = await ReadRowsAsync(connection, PgFactCollector.WaitStatsSql, ct, start, end);

        Assert.Equal(
            new[] { $"{Clean}|5|600|50", $"{Rival}|5|500|50" },
            rows);
    });

    [Fact]
    public Task AnomalyContributors_ANameStoredBothWays_IsOneRowWithTheSummedValue_AgainstDevPostgres() => RunAsync(async (connection, _, _, ct) =>
    {
        var (start, end) = await SeedSplitTotalsAsync(connection, ct);

        var rows = await ReadRowsAsync(connection, PgAnomalyDetector.WaitContribWindowSql, ct, start, end);

        Assert.Equal(new[] { $"{Clean}|600", $"{Rival}|500" }, rows);
    });

    /* ─────────────────────────── Custom Views ─────────────────────────── */

    [Fact]
    public Task CustomViewGroupedByWaitType_ANameStoredBothWays_IsOneRowWithTheSummedValue_AgainstDevPostgres() => RunAsync(async (connection, _, _, ct) =>
    {
        var (start, end) = await SeedSplitTotalsAsync(connection, ct);

        var rows = await RunPanelAsync(connection,
            "{\"source\":\"wait_stats\",\"measure\":\"wait_time_delta_ms\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"wait_type\"],\"viz\":\"bar\"}",
            start, end, ct);

        Assert.Equal(new[] { $"{Clean}|600", $"{Rival}|500" }, rows);
    });

    [Theory]
    [InlineData("eq", 600.0)]
    [InlineData("like", 600.0)]
    [InlineData("neq", 500.0)]
    [InlineData("gt", 500.0)]
    [InlineData("lte", 600.0)]
    public Task CustomViewFilteredByTheCleanName_MatchesBothSpellings_AgainstDevPostgres(string op, double expected) => RunAsync(async (connection, _, _, ct) =>
    {
        var (start, end) = await SeedSplitTotalsAsync(connection, ct);

        var rows = await RunPanelAsync(connection,
            "{\"source\":\"wait_stats\",\"measure\":\"wait_time_delta_ms\",\"aggregate\":\"sum\",\"viz\":\"stat\","
                + "\"filters\":[{\"dimension\":\"wait_type\",\"op\":\"" + op + "\",\"value\":\"" + Clean + "\"}]}",
            start, end, ct);

        Assert.Equal(new[] { expected.ToString(System.Globalization.CultureInfo.InvariantCulture) }, rows);
    });

    /* ─────────────────────────── plumbing ─────────────────────────── */

    /// <summary>EDC_DOPP_LOCK 300 ms under each spelling (one collection each) and the rival 500 ms, all inside the
    /// last hour. Returns a window around them.</summary>
    private static async Task<(DateTime Start, DateTime End)> SeedSplitTotalsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var t1 = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-40));
        var t2 = t1.AddMinutes(10);
        await InsertWaitStatAsync(connection, ct, t1, Spaced, deltaWaitMs: 300, deltaTasks: 3, sampleIntervalSeconds: 60, deltaSignalMs: 30);
        await InsertWaitStatAsync(connection, ct, t2, Clean, deltaWaitMs: 300, deltaTasks: 2, sampleIntervalSeconds: 60, deltaSignalMs: 20);
        await InsertWaitStatAsync(connection, ct, t1, Rival, deltaWaitMs: 500, deltaTasks: 5, sampleIntervalSeconds: 60, deltaSignalMs: 50);
        return (t1.AddMinutes(-5), t2.AddMinutes(5));
    }

    private static async Task<string[]> RunPanelAsync(NpgsqlConnection connection, string panelJson, DateTime start, DateTime end, CancellationToken ct)
    {
        var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(panelJson)!, []);
        Assert.True(parseError is null, parseError);

        var (compiled, compileError) = ComposeCompiler.Compile(
            plan!,
            new ComposeRunContext([ServerName], start, end, ComposeRunContext.NoVariables, RollupAvailability.All, end, RollupCoverage.Unknown));
        Assert.True(compileError is null, compileError);

        await using var command = new NpgsqlCommand(compiled!.Sql, connection);
        foreach (var parameter in compiled.Parameters)
        {
            command.Parameters.Add(parameter);
        }

        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var cells = new List<string>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                cells.Add(reader.IsDBNull(i) ? "NULL" : Convert.ToString(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture)!);
            }

            rows.Add(string.Join("|", cells));
        }

        return rows.ToArray();
    }

    /// <summary>Runs a windowed read bound as $1 server_id, $2/$3 window and returns each row as its cells joined by '|'.</summary>
    private static async Task<string[]> ReadRowsAsync(NpgsqlConnection connection, string sql, CancellationToken ct, DateTime start, DateTime end)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DarlingMcpTestData.Naive(start) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DarlingMcpTestData.Naive(end) });

        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var cells = new List<string>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                cells.Add(Convert.ToString(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture)!);
            }

            rows.Add(string.Join("|", cells));
        }

        return rows.ToArray();
    }

    private static DateTime MinuteAligned(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    private static Task InsertWaitStatAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, string waitType,
        long deltaWaitMs, long deltaTasks, int? sampleIntervalSeconds, long deltaSignalMs = 0) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, 0, 0, 0, $6, $7, $8, $9::integer)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), ServerId, ServerName, waitType,
            deltaTasks, deltaWaitMs, deltaSignalMs, sampleIntervalSeconds);

    private static Task InsertWaitingTaskAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, string waitType, long waitDurationMs) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.waiting_tasks
    (collection_id, collection_time, server_id, server_name, session_id, wait_type,
     wait_duration_ms, blocking_session_id, resource_description, database_name)
VALUES ($1, $2, $3, $4, 55, $5, $6, 0, NULL, 'AppDb')",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), ServerId, ServerName, waitType, waitDurationMs);

    private static Task InsertQuerySnapshotAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, int sessionId, string waitType) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.query_snapshots
    (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text,
     status, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms)
VALUES ($1, $2, $3, $4, $5, 'AppDb', 'SELECT 1', 'suspended', $6, 1000, 10, 2000)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), ServerId, ServerName, sessionId, waitType);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.wait_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.waiting_tasks WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.query_snapshots WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.servers WHERE server_id = $1", ServerId);
    }

    private static Task RegisterServerAsync(NpgsqlConnection connection, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 16, $3, $3)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE",
            ServerId, ServerName, DarlingMcpTestData.Naive(DateTime.UtcNow));

    private static async Task RunAsync(Func<NpgsqlConnection, NpgsqlDataSource, string, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live spaced-wait-name read tests.");

        var ct = TestContext.Current.CancellationToken;

        await using (var migration = new NpgsqlConnection(cs))
        {
            await migration.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migration, ct);
        }

        /* The seed and every read run on sessions pinned to the product's search path. A pooled session that ran the
           migration on a fresh database keeps the path it opened with, so the shared connection string alone would
           not resolve the bare relation names the product's SQL uses. */
        var pinned = new NpgsqlConnectionStringBuilder(cs) { SearchPath = "collect,config" }.ConnectionString;
        await using var postgres = NpgsqlDataSource.Create(pinned);
        await using var connection = await postgres.OpenConnectionAsync(ct);
        await DeleteRowsAsync(connection, ct);
        await RegisterServerAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await body(connection, postgres, pinned, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }
}
