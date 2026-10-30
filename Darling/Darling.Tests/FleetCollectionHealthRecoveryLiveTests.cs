// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5666 (with #5646): the fleet roll-ups band a recovered collector the way the per-server Collection Health read
/// does. The fleet statements are plain aggregates and cannot see a collector's newest runs, so after the read each
/// roll-up asks ONE follow-up statement (<see cref="CollectorTrailingRuns.FleetStreakSql"/>) for just the rows whose
/// band the streak could change, and runs nothing when no row needs it.
/// </summary>
// #1776 own-store: the live tests mint their own scratch database through ScratchPostgres.
[Collection("live-postgres")]
public sealed class FleetCollectionHealthRecoveryLiveTests
{
    private const int ServerId = -5666001;
    private const string ServerName = "fleet-health-recovery";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheFleetCard_AndTheViewerFleetRead_BandARecoveredCollector_AsTheServerTabDoes_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fleet collection-health recovery test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_monitored_servers (server_id, name, host, is_enabled) VALUES ($1, $2, $2, TRUE)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ServerId, ServerName);

            var start = DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Utc).AddMinutes(-150);

            /* recovered: 4 errors then 10 successes (28% over the line), the newest 3 in a row succeeded. */
            for (var i = 0; i < 14; i++)
            {
                await SeedAsync(connection, ct, "recovered", 1000 + i, start.AddMinutes(i * 5), i < 4 ? "ERROR" : "SUCCESS");
            }

            /* alternating: 12 runs, the newest a success, never 3 in a row. */
            for (var i = 0; i < 12; i++)
            {
                await SeedAsync(connection, ct, "alternating", 2000 + i, start.AddMinutes(i * 5), i % 2 == 0 ? "ERROR" : "SUCCESS");
            }

            /* a clean collector: no failures, so no row for the follow-up statement. */
            for (var i = 0; i < 12; i++)
            {
                await SeedAsync(connection, ct, "steady", 3000 + i, start.AddMinutes(i * 5), "SUCCESS");
            }

            var windowStart = DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-7));
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            /* The server tab's bands. */
            var perServer = (await DarlingDataReader.GetCollectionHealthAsync(postgres, ServerId, windowStart, ct))
                .ToDictionary(r => r.CollectorName, r => r.HealthStatus);
            Assert.Equal(CollectorHealthClassifier.Healthy, perServer["recovered"]);
            Assert.Equal(CollectorHealthClassifier.Warning, perServer["alternating"]);
            Assert.Equal(CollectorHealthClassifier.Healthy, perServer["steady"]);

            /* The fleet card's counts: recovered + steady are HEALTHY, alternating is the one WARNING. */
            var read = typeof(DarlingFleetReader).GetMethod("ReadFailingCollectorCountsAsync", BindingFlags.NonPublic | BindingFlags.Static)
                ?? throw new InvalidOperationException("ReadFailingCollectorCountsAsync not found");
            var counts = await (Task<Dictionary<int, DarlingFleetReader.CollectorCounts>>)read.Invoke(null, [postgres, DateTime.UtcNow, ct])!;
            var card = counts[ServerId];
            Assert.Equal(perServer.Values.Count(b => b == CollectorHealthClassifier.Healthy), card.Healthy);
            Assert.Equal(3, card.Total);

            /* The viewer's fleet read (the Overview cards and the status bar). */
            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var fleet = (await viewer.GetFleetCollectionHealthByServerAsync(ct))[ServerId]
                .ToDictionary(r => r.CollectorName, r => r.HealthStatus);
            Assert.Equal(perServer, fleet);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task TheFollowUpStatement_ReadsThreeRunsPerPair_ThroughAnIndex_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fleet streak plan test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        if (scratch.TimescaleEnabled)
        {
            /* V23 made collection_log a hypertable with a compression policy (compress_after 1 day), and every
               seeded chunk is older than that. TimescaleDB's scheduler runs the policy on its own clock, so a run
               landing between the seed and the plan compressed a chunk, and a compressed chunk is read through its
               own "compressed_server_id__ts_meta_v2_first_col_idx" instead of idx_collection_log_watermark (dev run
               40987085936, #6944). The plan is not the thing under test there, so no policy job runs here. */
            await DarlingMcpTestData.ExecAsync(connection, ct, "SELECT _timescaledb_functions.stop_background_workers()");
        }
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            /* A store big enough for the planner to choose: 6 collectors x 2,000 runs, one minute apart, the newest
               at 12:00 UTC yesterday (#6370). Under TimescaleDB the raw chunks are one UTC day, so runs ending at
               the real now() split 2,000 minutes into three chunks whose sizes follow the hour the test runs. In
               the first ~2.5 hours after midnight UTC the newest chunk holds only 60 to 140 runs per collector, and
               the planner reads that small chunk through idx_collection_log_time (22 entries for 4 rows, an
               amount bounded by the chunk) while the large chunks use idx_collection_log_watermark: the statement
               is fine, the test's seed was the moving part (CI run 39703284604, shard 5, ~01:20Z). 12:00 gives
               the same two large chunks (720 and 1,280 runs per collector) at any hour, so the plan is the
               one the statement is written for: every chunk reads through the watermark index. */
            var newest = LiveClock.Now().Date.AddDays(-1).AddHours(12);
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message,
     rows_collected, sql_duration_ms, duckdb_duration_ms)
SELECT -5666000000 - (c * 100000 + g), $1, $2, 'c' || c,
       $3 - make_interval(mins => g), 100,
       CASE WHEN g % 7 = 0 THEN 'ERROR' ELSE 'SUCCESS' END, NULL, 5, 80, 20
FROM generate_series(1, 6) c, generate_series(1, 2000) g", ServerId, ServerName, newest);
            await DarlingMcpTestData.ExecAsync(connection, ct, "ANALYZE collection_log");
            if (scratch.TimescaleEnabled)
            {
                await using var compressed = new NpgsqlCommand(
                    "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'collection_log' AND is_compressed", connection);
                Assert.Equal(0L, (long)(await compressed.ExecuteScalarAsync(ct))!);
            }

            var windowStart = DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-7));
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            var pairs = Enumerable.Range(1, 6).Select(c => (ServerId, "c" + c)).ToList();
            var streaks = await CollectorTrailingRuns.ReadAsync(postgres, pairs, windowStart, 30, ct);
            Assert.Equal(6, streaks.Count);
            /* g = 1, 2, ... minutes ago; the newest run is g = 1 and the first error is g = 7. */
            Assert.All(streaks.Values, v => Assert.Equal(3, v));

            await using var explain = postgres.CreateCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + CollectorTrailingRuns.FleetStreakSql);
            explain.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = pairs.Select(p => p.ServerId).ToArray() });
            explain.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = pairs.Select(p => p.Item2).ToArray() });
            explain.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = windowStart });
            var plan = ((string)(await explain.ExecuteScalarAsync(ct))!).Replace("\": \"", "\":\"");

            Assert.Contains("Index", plan, StringComparison.Ordinal);
            var indexes = System.Text.RegularExpressions.Regex.Matches(plan, "\"Index Name\":\"([^\"]+)\"")
                /* Under TimescaleDB (#6317) the plan reads each chunk's own copy of the index, named
                   "_hyper_<h>_<c>_chunk_<index>" and ChunkAppend-ed in time order: strip that prefix to
                   name the index the statement was written for. */
                .Select(m => System.Text.RegularExpressions.Regex.Replace(m.Groups[1].Value, "^_hyper_[0-9]+_[0-9]+_chunk_", ""))
                .Distinct().ToList();
            Console.WriteLine("PLAN5666 indexes=" + string.Join(",", indexes));
            Assert.NotEmpty(indexes);
            Assert.All(indexes, i => Assert.StartsWith("idx_collection_log_", i, StringComparison.Ordinal));
            /* The index that carries the lookup's own key and order, (server_id, collector_name, collection_time
               DESC): the prefix above would pass idx_collection_log_time just as well. */
            Assert.Contains("idx_collection_log_watermark", indexes);
            Assert.DoesNotContain("idx_collection_log_time", indexes);
            Assert.DoesNotContain("\"Node Type\":\"Seq Scan\"", plan, StringComparison.Ordinal);
            /* No sort of the window. The index is keyed on collection_time but not log_id, so the planner does
               choose an INCREMENTAL Sort for the tie-break, which sorts only the rows that share one
               collection_time: every sort node in the plan must be that kind, presorted on the index's order,
               and sit under the Limit. (The exact "Sort" assert above cannot see an Incremental Sort.) */
            var sortNodes = System.Text.RegularExpressions.Regex.Matches(plan, "\"Node Type\":\"([^\"]*Sort[^\"]*)\"")
                .Select(m => m.Groups[1].Value).ToList();
            Console.WriteLine("PLAN5666 sortNodes=" + string.Join(",", sortNodes));
            Assert.All(sortNodes, n => Assert.Equal("Incremental Sort", n));
            if (sortNodes.Count > 0)
                Assert.Contains("\"Presorted Key\"", plan, StringComparison.Ordinal);
            Assert.Contains("\"Node Type\":\"Limit\"", plan, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task NoStatementRuns_WhenNoRowNeedsTheStreak()
    {
        /* A data source pointed at a host that does not exist: any statement would throw. */
        await using var unreachable = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Timeout=1;Command Timeout=1;Username=x;Database=x");
        var ct = TestContext.Current.CancellationToken;

        var healthy = new CollectorHealth
        {
            CollectorName = "steady", TotalRuns = 40, SuccessCount = 40, ErrorCount = 0,
            LastSuccessTime = DateTime.UtcNow, LastRunTime = DateTime.UtcNow
        };
        Assert.False(healthy.RecoveryCouldChangeBand);
        await DarlingFleetReader.ApplyTrailingSuccessRunsAsync(unreachable, [(1, healthy)], DateTime.UtcNow.AddDays(-7), ct);

        var viewerHealthy = new CollectorHealthRow
        {
            CollectorName = "steady", TotalRuns = 40, SuccessCount = 40, ErrorCount = 0,
            LastSuccessTime = DateTime.UtcNow, LastRunTime = DateTime.UtcNow
        };
        Assert.False(viewerHealthy.RecoveryCouldChangeBand);
        await ViewerDataService.ApplyTrailingSuccessRunsAsync(
            unreachable, new Dictionary<int, List<CollectorHealthRow>> { [1] = [viewerHealthy] }, DateTime.UtcNow.AddDays(-7), ct);

        /* A row that fails past the line is the one the streak could change. */
        var recovering = new CollectorHealth
        {
            CollectorName = "recovered", TotalRuns = 14, SuccessCount = 10, ErrorCount = 4,
            LastSuccessTime = DateTime.UtcNow, LastRunTime = DateTime.UtcNow
        };
        Assert.True(recovering.RecoveryCouldChangeBand);
        await Assert.ThrowsAnyAsync<Exception>(() =>
            DarlingFleetReader.ApplyTrailingSuccessRunsAsync(unreachable, [(1, recovering)], DateTime.UtcNow.AddDays(-7), ct));
    }

    private static Task SeedAsync(NpgsqlConnection c, CancellationToken ct, string collector, long logId, DateTime atUtc, string status) =>
        DarlingMcpTestData.ExecAsync(c, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message,
     rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, 100, $6, $7, 5, 80, 20)",
            logId, ServerId, ServerName, collector, DarlingMcpTestData.Naive(atUtc), status,
            status == "ERROR" ? "boom" : (object?)null);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
