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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The analysis reads of blocked-process reports, deadlocks and the PostgreSQL deadlock capture window on when the
/// EVENT happened, not when the collector got to it, so an analysis fact counts the same events the grids show.
/// Three rows per table: (a) happened two hours BEFORE the window and was collected inside it, (b) happened and
/// was collected inside it, (c) happened inside it and was collected 30 minutes AFTER it. b and c count; a does not.
/// For <c>pg_deadlocks</c> a fourth row (d) has no <c>occurred_at</c> and is collected inside the window, so it
/// counts through the <c>collection_time</c> fallback. Gated on DARLING_TEST_PG; every read runs through the
/// product's own collector, drill-down or reader.
/// </summary>
/* #1776 own-store: each fact plants its rows under dedicated server ids and deletes them in cleanup. */
[Collection("live-postgres")]
public sealed class EventTimeAnalysisReadsLiveTests
{
    private const string SqlServerName = "darling-event-time-analysis-sql";
    private const string PgServerName = "darling-event-time-analysis-pg";
    private static readonly int SqlServerId = ServerIdHelper.GetDeterministicHashCode(SqlServerName);
    private static readonly int PgServerId = ServerIdHelper.GetDeterministicHashCode(PgServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string Graph(string db) =>
        $"<deadlock><process-list><process id=\"p0\" currentdbname=\"{db}\" /></process-list></deadlock>";

    /// <summary>The window ends two hours ago so a row collected 30 minutes after it is still in the past.</summary>
    private static (DateTime Start, DateTime End) Window(TimeSpan length)
    {
        var now = DateTime.UtcNow;
        var end = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerMinute), DateTimeKind.Unspecified).AddHours(-2);
        return (end - length, end);
    }

    /// <summary>(event time, collection time) for the three SQL Server shapes, repeated a, b and c times. The counts differ
    /// so a read that windows on collection time (a + b) cannot equal one that windows on the event (b + c) by accident.</summary>
    private static IEnumerable<(string Shape, DateTime Event, DateTime Collected)> Shapes(DateTime start, DateTime end, int a, int b, int c)
    {
        for (var i = 0; i < a; i++) yield return ("a", start.AddHours(-2).AddMinutes(i), start.AddMinutes(30 + i));
        for (var i = 0; i < b; i++) yield return ("b", start.AddHours(1).AddMinutes(i), start.AddHours(1).AddMinutes(i + 1));
        for (var i = 0; i < c; i++) yield return ("c", end.AddMinutes(-30 + i), end.AddMinutes(30 + i));
    }

    private static async Task<T> WithStoreAsync<T>(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task<T>> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var result = await body(connection, postgres, ct);
            bodySucceeded = true;
            return result;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private sealed record SqlRun(List<Fact> Facts, List<Fact> Anomalies, JsonElement Deadlocks, JsonElement Chains);

    /// <summary>Plants the a, b and c rows in both tables and runs the fact collector, the
    /// anomaly detector and the drill-down against a four-hour window.</summary>
    private static Task<SqlRun> RunSqlServerAsync(int a, int b, int c, IReadOnlyList<string>? separate, bool drillDown) =>
        WithStoreAsync(async (connection, postgres, ct) =>
        {
            await Exec(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 16, now()::timestamp, now()::timestamp)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ct, SqlServerId, SqlServerName);

            var (start, end) = Window(TimeSpan.FromHours(4));
            for (var m = 0; m <= 240; m += 15)
                await Exec(connection, "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms) VALUES ($1,$2,$3,$4,'CXPACKET',1,10,0)",
                    ct, CollectionIdGenerator.Next(), start.AddMinutes(m), SqlServerId, SqlServerName);

            var spid = 70;
            foreach (var (_, eventTime, collected) in Shapes(start, end, a, b, c))
            {
                await Exec(connection, "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, database_name) VALUES ($1,$2,$3,$4,$5,12000,60,$6,'suspended','HS')",
                    ct, CollectionIdGenerator.Next(), collected, SqlServerId, SqlServerName, eventTime, spid++);
                await Exec(connection, "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml) VALUES ($1,$2,$3,$4,$5,$6)",
                    ct, CollectionIdGenerator.Next(), collected, SqlServerId, SqlServerName, eventTime, Graph("HS"));
            }

            var context = new AnalysisContext
            {
                ServerId = SqlServerId, ServerName = SqlServerName, TimeRangeStart = start, TimeRangeEnd = end,
                ServerUtcOffset = TimeSpan.Zero, SeparatelyMonitoredDatabases = separate, CancellationToken = ct
            };
            var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);
            var spikes = await new PgAnomalyDetector(postgres, new PgBaselineProvider(postgres)).DetectAnomaliesAsync(context);

            JsonElement deadlocks = default, chains = default;
            if (drillDown)
            {
                var finding = new AnalysisFinding
                {
                    RootFactKey = "DEADLOCKS", StoryPath = "DEADLOCKS", PathKeys = ["DEADLOCKS", "BLOCKING_EVENTS"], Severity = 1.0
                };
                await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);
                deadlocks = JsonSerializer.SerializeToElement(finding.DrillDown!["top_deadlocks"]);
                chains = JsonSerializer.SerializeToElement(finding.DrillDown!["top_blocking_chains"]);
            }
            return new SqlRun(facts, spikes, deadlocks, chains);
        });

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BlockingAndDeadlockFacts_CountEventsInTheWindow_NotRowsCollectedInIt(bool azureMasterForm)
    {
        /* b + c = 2 + 4; collection-time windowing would read a + b = 3. */
        var run = await RunSqlServerAsync(1, 2, 4, azureMasterForm ? new[] { "GP" } : null, false);
        Assert.Equal(6.0, Assert.Single(run.Facts, f => f.Key == "BLOCKING_EVENTS").Metadata["event_count"]);
        Assert.Equal(6.0, Assert.Single(run.Facts, f => f.Key == "DEADLOCKS").Metadata["deadlock_count"]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnomalyDetectorCurrentWindowCounts_CountEventsInTheWindow_NotRowsCollectedInIt(bool azureMasterForm)
    {
        /* b + c = 6 in each table; collection-time windowing reads a + b = 3, which is under the blocking bar (5)
           and fires the deadlock spike at the wrong count. No baseline, so a spike fires on the count alone. */
        var run = await RunSqlServerAsync(1, 2, 4, azureMasterForm ? new[] { "GP" } : null, false);
        Assert.Equal(6.0, Assert.Single(run.Anomalies, f => f.Key == "ANOMALY_BLOCKING_SPIKE").Value);
        Assert.Equal(6.0, Assert.Single(run.Anomalies, f => f.Key == "ANOMALY_DEADLOCK_SPIKE").Value);
    }

    [Fact]
    public async Task DrillDownTopDeadlocksAndTopChains_ListEventsInTheWindow_NotRowsCollectedInIt()
    {
        var run = await RunSqlServerAsync(1, 1, 1, null, true);
        var (start, end) = Window(TimeSpan.FromHours(4));
        /* The drill-down reports the event's own time; a (two hours before the window) must not be among them. */
        var deadlockTimes = run.Deadlocks.EnumerateArray().Select(d => DateTime.Parse(d.GetProperty("deadlock_time").GetString()!, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind)).ToList();
        Assert.Equal(2, deadlockTimes.Count);
        Assert.All(deadlockTimes, t => Assert.InRange(t, start, end));
        Assert.Equal(2, run.Chains.GetArrayLength());
    }

    private sealed record PgRun(int ExemplarCount, int ExemplarRows, int ReaderRows);

    [Fact]
    public async Task PgDeadlockCapture_CountsByOccurrenceWithTheCollectionTimeFallback_InFactDrillDownAndReader()
    {
        var run = await WithStoreAsync(async (connection, postgres, ct) =>
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, PgServerId, PgServerName, MonitoredEngineKind.Postgres, 18, ct);
            var (start, end) = Window(TimeSpan.FromHours(1));

            /* The span gate and the live counter series, as the exemplar e2e plants them. */
            await PlantDatabaseStatsAsync(connection, end.AddHours(-25), 1000, 0, ct);
            for (var m = -1; m <= 60; m++)
            {
                var mm = Math.Max(m, 0);
                await PlantDatabaseStatsAsync(connection, start.AddMinutes(m), 1000 + 100L * mm, mm / 10, ct);
            }

            /* b + c (twice) + d = 4; collection-time windowing would read a + b + d = 3. */
            await PlantDeadlockAsync(connection, "a", start.AddHours(-2), start.AddMinutes(10), ct);
            await PlantDeadlockAsync(connection, "b", start.AddMinutes(20), start.AddMinutes(21), ct);
            await PlantDeadlockAsync(connection, "c1", start.AddMinutes(40), end.AddMinutes(30), ct);
            await PlantDeadlockAsync(connection, "c2", start.AddMinutes(41), end.AddMinutes(31), ct);
            await PlantDeadlockAsync(connection, "d", null, start.AddMinutes(30), ct);

            var context = new AnalysisContext
            {
                ServerId = PgServerId, ServerName = PgServerName, TimeRangeStart = start, TimeRangeEnd = end,
                ServerUtcOffset = TimeSpan.Zero, CancellationToken = ct
            };
            var facts = await new PgTargetFactCollector(postgres).CollectFactsAsync(context);
            var rate = Assert.Single(facts, f => f.Key == PgTargetFactKeys.DeadlockRate);
            var count = (int)rate.Metadata[PgTargetScorer.DeadlockExemplarCountKey];

            /* The drill-down reads without the fact's stamp, so its total is its own row count. */
            var finding = new AnalysisFinding
            {
                RootFactKey = PgTargetFactKeys.DeadlockRate, StoryPath = PgTargetFactKeys.DeadlockRate,
                PathKeys = [PgTargetFactKeys.DeadlockRate], Severity = 1.0
            };
            await new PgTargetDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);
            var section = JsonSerializer.SerializeToElement(finding.DrillDown![PgTargetDrillDownCollector.DeadlockExemplarsSection]);

            var readerRows = await DarlingPgDeadlockReader.GetDeadlocksAsync(postgres, PgServerId, start, end, 50, ct);
            return new PgRun(count, section.GetProperty("log_captured").GetInt32(), readerRows.Count);
        });

        Assert.Equal(4, run.ExemplarCount);
        Assert.Equal(4, run.ExemplarRows);
        Assert.Equal(4, run.ReaderRows);
    }

    private static async Task Exec(NpgsqlConnection c, string sql, CancellationToken ct, params object[] p)
    {
        using var cmd = new NpgsqlCommand(sql, c);
        foreach (var v in p) cmd.Parameters.AddWithValue(v);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantDatabaseStatsAsync(NpgsqlConnection connection, DateTime at, long xactCommit, long deadlocks, CancellationToken ct) =>
        await Exec(connection, @"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
VALUES ($1, $2, $3, $4, 'appdb', $5, 10, 100, 9000, 0, 0, $6, NULL)", ct,
            CollectionIdGenerator.Next(), at, PgServerId, PgServerName, xactCommit, deadlocks);

    private static async Task PlantDeadlockAsync(NpgsqlConnection connection, string hash, DateTime? occurredAt, DateTime collected, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_deadlocks
    (collection_id, collection_time, server_id, server_name, occurred_at, victim_pid, participant_count, deadlock_hash, lock_modes, resources, victim_statement, graph_text)
VALUES ($1, $2, $3, $4, $5, 4242, 2, $6, 'ShareLock', 'relation orders', 'UPDATE orders SET status = $1', 'graph')", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(collected);
        command.Parameters.AddWithValue(PgServerId);
        command.Parameters.AddWithValue(PgServerName);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)occurredAt ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp });
        command.Parameters.AddWithValue("hash-" + hash);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id = {SqlServerId}; " +
            $"DELETE FROM blocked_process_reports WHERE server_id = {SqlServerId}; " +
            $"DELETE FROM deadlocks WHERE server_id = {SqlServerId}; " +
            $"DELETE FROM pg_database_stats WHERE server_id = {PgServerId}; " +
            $"DELETE FROM pg_deadlocks WHERE server_id = {PgServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id IN ({SqlServerId}, {PgServerId}); " +
            $"DELETE FROM servers WHERE server_id IN ({SqlServerId}, {PgServerId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
