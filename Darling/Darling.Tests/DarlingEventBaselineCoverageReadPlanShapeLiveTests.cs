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
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4731: the plan shape of the blocking baseline's coverage read (the <c>logged</c> CTE over <c>collection_log</c>),
/// over a store whose log has compressed chunks. Two properties keep that read cheap:
///
/// <para>(a) A server whose event source holds nothing in the window never reads the log. The <c>slots</c> CTE gates the
/// log arm with <c>WHERE EXISTS (SELECT 1 FROM events)</c>, an uncorrelated test the planner runs once as an InitPlan
/// and applies as a one-time filter, so every log scan under it shows <c>Actual Loops: 0</c> ("never executed"). The
/// same statement without the gate is the control: its log scans do run.</para>
///
/// <para>(b) A server with events reads only its own segment of a compressed chunk. The log is compressed segmented by
/// <c>server_id</c>, so the <c>server_id = $1</c> condition has to reach the scan of the compressed chunk itself (the
/// <c>compress_hyper_*</c> relation), not be applied to rows already decompressed for every server.</para>
///
/// <para>Mints its own scratch database: other classes' leftover chunks change the chunk list a plan-shape assertion reads.
/// Unlike the model this follows, the plan is read as JSON, off <c>EXPLAIN (ANALYZE, FORMAT JSON)</c>, because (a) is a
/// statement about which nodes ran.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingEventBaselineCoverageReadPlanShapeLiveTests
{
    private const int QuietServerId = -473101;
    private const int BusyServerId = -473102;
    private const string ServerName = "EVENT-COVERAGE-PLAN-SRV";
    private const string Collector = "blocked_process_report";

    /// <summary>Well below any id the generator hands out, so a seeded row can never collide with a real one.</summary>
    private const long LogIdBase = -473_100_000_000L;

    private static readonly DateTime AnalysisTime = new(2026, 3, 4, 14, 0, 0, DateTimeKind.Unspecified);

    /// <summary>Inside the window, on the busy server only.</summary>
    private static readonly DateTime EventTime = new(2026, 2, 8, 9, 10, 0, DateTimeKind.Unspecified);

    [Fact]
    public async Task TheBlockingArm_NeverReadsTheLogForAServerWithoutEvents_AndKeepsTheServerFilterOnTheCompressedScan()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live coverage read plan test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        // #1776 own-store: a scratch database, so no other class's chunks shape the plan under test.
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connectionString = scratch.ConnectionString;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(connectionString, ct);
        Assert.SkipUnless(timescaleEnabled, "TimescaleDB is not available on this cluster; the compressed-chunk plan shape needs it.");
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        /* No background policy job reshapes the chunks under the plan assertion. */
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);
        await DeleteLiveRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await SeedLogAsync(connection, QuietServerId, ct);
            await SeedLogAsync(connection, BusyServerId, ct);
            await SeedEventAsync(connection, BusyServerId, ct);
            await CompressOldChunksAsync(connection, ct);

            await using (var count = new NpgsqlCommand(
                "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'collection_log' AND is_compressed", connection))
            {
                var compressedChunks = Convert.ToInt64(await count.ExecuteScalarAsync(ct));
                Assert.True(compressedChunks >= 1, "the seed produced no compressed collection_log chunk, so the plan would prove nothing");
            }

            var sql = PgBaselineProvider.GetBaselineQuery(MetricNames.Blocking)!;

            /* (a) No events in the window: the log scans are in the plan and none of them ran. */
            var quiet = await ExplainAsync(connection, sql, QuietServerId, ct);
            var quietScans = LogScans(quiet);
            Assert.True(quietScans.Count > 0, $"the plan has no collection_log scan at all:\n{quiet.GetRawText()}");
            Assert.True(
                quietScans.All(scan => Loops(scan) == 0),
                $"a server with no events in the window read collection_log; the gate is no longer a one-time filter:\n{quiet.GetRawText()}");

            /* The control: without the gate the same statement reads the log for that server, so the zero above is the gate's doing. */
            const string Gate = "WHERE EXISTS (SELECT 1 FROM events)";
            Assert.Contains(Gate, sql, StringComparison.Ordinal);
            var ungated = LogScans(await ExplainAsync(connection, sql.Replace(Gate, string.Empty, StringComparison.Ordinal), QuietServerId, ct));
            Assert.True(ungated.Any(scan => Loops(scan) > 0), "without the gate the log scan should have run for the server with no events");

            /* (b) Events in the window: the log is read, and the compressed chunks are read through this server's segment only. */
            var busy = await ExplainAsync(connection, sql, BusyServerId, ct);
            var compressedScans = LogScans(busy)
                .Where(scan => scan.GetProperty("Relation Name").GetString()!.StartsWith("compress_hyper_", StringComparison.Ordinal))
                .ToList();
            Assert.True(compressedScans.Count > 0, $"the plan for a server with events reads no compressed chunk:\n{busy.GetRawText()}");
            Assert.True(compressedScans.All(scan => Loops(scan) > 0), $"a compressed scan did not run:\n{busy.GetRawText()}");
            Assert.True(
                compressedScans.All(scan => Conditions(scan).Contains("server_id", StringComparison.Ordinal)),
                $"the server_id condition does not reach the compressed chunk's own scan:\n{busy.GetRawText()}");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteLiveRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>Every node of the plan that scans the log's hypertable or one of its chunks (compressed or not), outside the
    /// <c>events</c> CTE's own subtree, which reads the event source.</summary>
    private static List<JsonElement> LogScans(JsonElement plan)
    {
        var found = new List<JsonElement>();

        void Visit(JsonElement node, bool inEvents)
        {
            var events = inEvents || (node.TryGetProperty("Subplan Name", out var name) && name.GetString() == "CTE events");
            if (!events && node.TryGetProperty("Relation Name", out var relation) && IsLogRelation(relation.GetString()!))
            {
                found.Add(node);
            }

            if (node.TryGetProperty("Plans", out var children))
            {
                foreach (var child in children.EnumerateArray())
                {
                    Visit(child, events);
                }
            }
        }

        Visit(plan, false);
        return found;
    }

    private static bool IsLogRelation(string relation) =>
        relation == "collection_log"
        || relation.StartsWith("_hyper_", StringComparison.Ordinal)
        || relation.StartsWith("compress_hyper_", StringComparison.Ordinal);

    /// <summary>How many times the node ran: 0 is "never executed".</summary>
    private static double Loops(JsonElement node) => node.GetProperty("Actual Loops").GetDouble();

    /// <summary>The node's own conditions and its descendants' (a bitmap scan carries its index condition one node down).</summary>
    private static string Conditions(JsonElement node)
    {
        var text = new List<string>();

        void Collect(JsonElement current)
        {
            foreach (var property in new[] { "Index Cond", "Filter", "Recheck Cond" })
            {
                if (current.TryGetProperty(property, out var value))
                {
                    text.Add(value.GetString() ?? string.Empty);
                }
            }

            if (current.TryGetProperty("Plans", out var children))
            {
                foreach (var child in children.EnumerateArray())
                {
                    Collect(child);
                }
            }
        }

        Collect(node);
        return string.Join(" | ", text);
    }

    /// <summary>The statement as the provider runs it: the six bound parameters of an unkeyed arm, over the day-grain window.</summary>
    private static async Task<JsonElement> ExplainAsync(NpgsqlConnection connection, string sql, int serverId, CancellationToken ct)
    {
        var windowEnd = PgBaselineProvider.RoundedDay(AnalysisTime);
        var windowStart = windowEnd.AddDays(-BaselineMath.BaselineWindowDays);
        var clock = LocalClockWindow.Utc(windowEnd);

        using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + sql, connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(clock.TransitionAtUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(clock.OffsetBeforeMinutes);
        command.Parameters.AddWithValue(clock.OffsetAfterMinutes);

        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        using var document = JsonDocument.Parse(json);
        return document.RootElement[0].GetProperty("Plan").Clone();
    }

    /// <summary>Five weeks of SUCCESS runs every 15 minutes ending just before the analysis hour, so every slot of the window is covered.</summary>
    private static async Task SeedLogAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status, rows_collected)
              SELECT $1 - row_number() OVER (), $2, $3, $4, t, 'SUCCESS', 0
              FROM generate_series($5::timestamp, $6::timestamp, INTERVAL '15 minutes') AS g(t)", connection);
        command.Parameters.AddWithValue(LogIdBase + serverId * 1_000_000L);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(Collector);
        command.Parameters.AddWithValue(AnalysisTime.AddDays(-35));
        command.Parameters.AddWithValue(AnalysisTime.AddMinutes(-15));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task SeedEventAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time) VALUES ($1, $2, $3, $4, $5)",
            connection);
        command.Parameters.AddWithValue(LogIdBase);
        command.Parameters.AddWithValue(EventTime);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(EventTime);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Compresses every chunk that closed more than a day ago. Best-effort per chunk, as the model does: the
    /// caller counts the compressed chunks afterwards and fails the test if the seed compressed none.</summary>
    private static async Task CompressOldChunksAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var chunkList = new NpgsqlCommand(
            "SELECT show_chunks('collection_log', older_than => INTERVAL '1 day')::text", connection);
        await using var reader = await chunkList.ExecuteReaderAsync(ct);
        var chunks = new List<string>();
        while (await reader.ReadAsync(ct))
        {
            chunks.Add(reader.GetString(0));
        }

        await reader.DisposeAsync();

        foreach (var chunk in chunks)
        {
            try
            {
                using var compress = new NpgsqlCommand($"SELECT compress_chunk('{chunk}', if_not_compressed => true)", connection);
                await compress.ExecuteNonQueryAsync(ct);
            }
            catch (PostgresException)
            {
                /* Not eligible yet: the caller's count of compressed chunks is the check. */
            }
        }
    }

    private static async Task DeleteLiveRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collection_log WHERE server_id IN ({QuietServerId}, {BusyServerId}); " +
            $"DELETE FROM blocked_process_reports WHERE server_id IN ({QuietServerId}, {BusyServerId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
