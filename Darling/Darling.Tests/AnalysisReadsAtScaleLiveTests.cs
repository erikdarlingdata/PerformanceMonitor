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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and never touches another test's
   rows, so it is deliberately NOT [Collection("live-postgres")]. The scratch database is dropped on disposal, which is
   the cleanup. */

/// <summary>
/// #8570: two statements of the analysis fact reads that grew with the size of the monitored server and not with the
/// window. The database-size probe compared every file of the probed snapshots with every file of the newest one (2.1
/// seconds at 4,000 files), and the sp_configure trace read walked every default-trace event the server had kept. Each
/// test runs the shipped statement and its pre-#8570 form over the same rows: the answer must be equal, and the plan
/// must have the new shape (no pairwise join, a fraction of the blocks).
/// </summary>
public sealed class AnalysisReadsAtScaleLiveTests
{
    /// <summary>The database-size probe as shipped before #8570 (the dropped_out column's NOT EXISTS over <c>cur</c>). An oracle, never run by the product.</summary>
    private const string PairwiseDatabaseSizeNewestSql = @"
WITH newest AS (
    SELECT collection_time
    FROM file_io_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   size_mb > 0
    ORDER BY collection_time DESC
    LIMIT 1
),
cur AS (
    SELECT DISTINCT ON (database_name, file_name) database_name, file_name, size_mb
    FROM file_io_stats
    WHERE server_id = $1
    AND   collection_time = (SELECT collection_time FROM newest)
    AND   size_mb > 0
    ORDER BY database_name, file_name
),
edge AS (
    (
        SELECT collection_time
        FROM file_io_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time < (SELECT collection_time FROM newest)
        AND   size_mb > 0
        ORDER BY collection_time DESC
        LIMIT 1
    )
    UNION
    (
        SELECT collection_time
        FROM file_io_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time < (SELECT collection_time FROM newest)
        AND   size_mb > 0
        ORDER BY collection_time ASC
        LIMIT 1
    )
)
SELECT
    (SELECT SUM(size_mb) FROM cur) AS total_size_mb,
    EXISTS (
        SELECT 1
        FROM file_io_stats AS f
        WHERE f.server_id = $1
        AND   f.collection_time IN (SELECT collection_time FROM edge)
        AND   f.size_mb > 0
        AND   NOT EXISTS (
            SELECT 1
            FROM cur AS c
            WHERE c.database_name = f.database_name
            AND   c.file_name = f.file_name
        )
    ) AS dropped_out";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private static async Task<(ScratchPostgres Scratch, NpgsqlConnection Connection)> OpenAsync(CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return (scratch, connection);
    }

    private static async Task ExecAsync(NpgsqlConnection c, string sql, CancellationToken ct, params object[] parameters)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        foreach (var p in parameters)
        {
            cmd.Parameters.AddWithValue(p is DateTime dt ? Naive(dt) : p);
        }

        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<JsonDocument> ExplainAsync(NpgsqlConnection c, string sql, object[] parameters, CancellationToken ct)
    {
        await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, c);
        foreach (var p in parameters)
        {
            explain.Parameters.AddWithValue(p is DateTime dt ? Naive(dt) : p);
        }

        return JsonDocument.Parse((string)(await explain.ExecuteScalarAsync(ct))!);
    }

    private static long Blocks(JsonDocument explain)
    {
        var plan = explain.RootElement[0].GetProperty("Plan");
        return plan.GetProperty("Shared Hit Blocks").GetInt64()
             + (plan.TryGetProperty("Shared Read Blocks", out var read) ? read.GetInt64() : 0);
    }

    /// <summary>True when any node of the plan is a nested loop of the given join type.</summary>
    private static bool HasNestedLoop(JsonElement node, string joinType)
    {
        if (node.TryGetProperty("Node Type", out var type) && type.GetString() == "Nested Loop"
            && node.TryGetProperty("Join Type", out var join) && join.GetString() == joinType)
        {
            return true;
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                if (HasNestedLoop(child, joinType))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static async Task<(double? Total, bool DroppedOut)> RunProbeAsync(NpgsqlConnection c, string sql, object[] parameters, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        foreach (var p in parameters)
        {
            cmd.Parameters.AddWithValue(p is DateTime dt ? Naive(dt) : p);
        }

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.IsDBNull(0) ? null : Convert.ToDouble(reader.GetValue(0)), reader.GetBoolean(1));
    }

    [Fact]
    public async Task TheDatabaseSizeProbe_HashesTheFileKeysInsteadOfComparingEveryPair_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live database-size probe test.");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, c) = await OpenAsync(ct);
        await using var scratchLease = scratch;
        await using var connectionLease = c;

        const int serverId = -857_001;
        const int files = 1_500;
        var end = LiveClock.AnchoredEnd();

        /* Three snapshots of one server: the newest, the one before it, and the oldest of the 24-hour lookback; every file
           in each of them, so the probe finds nothing dropped out. */
        await ExecAsync(c, @"
INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, size_mb)
SELECT 857_000_000_000 + s.k * 100_000 + f, s.t, $1, 'scale', 'Db' || (f / 2), 'File' || f, 'ROWS', 10 + f
FROM (VALUES (1, $2::timestamp - interval '1 minute'), (2, $2::timestamp - interval '2 minutes'), (3, $2::timestamp - interval '23 hours 50 minutes')) AS s (k, t)
CROSS JOIN generate_series(0, $3 - 1) AS f", ct, serverId, end, files);
        await ExecAsync(c, "ANALYZE file_io_stats", ct);

        object[] parameters = [serverId, end.AddHours(-24), end];
        var shipped = await RunProbeAsync(c, PgFactCollector.DatabaseSizeNewestSql, parameters, ct);
        var oracle = await RunProbeAsync(c, PairwiseDatabaseSizeNewestSql, parameters, ct);
        Assert.Equal(oracle, shipped);
        Assert.False(shipped.DroppedOut);

        /* A file that only the oldest snapshot holds: dropped out, in both. */
        await ExecAsync(c, @"
INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, size_mb)
VALUES (857_900_000_001, $2, $1, 'scale', 'GoneDb', 'GoneFile', 'ROWS', 5)", ct, serverId, end.AddMinutes(-(23 * 60 + 50)));
        await ExecAsync(c, "ANALYZE file_io_stats", ct);
        var shippedGone = await RunProbeAsync(c, PgFactCollector.DatabaseSizeNewestSql, parameters, ct);
        var oracleGone = await RunProbeAsync(c, PairwiseDatabaseSizeNewestSql, parameters, ct);
        Assert.Equal(oracleGone, shippedGone);
        Assert.True(shippedGone.DroppedOut);

        /* The shape: the keys are hashed once. The pairwise form is a nested loop anti join with one comparison per pair of files. */
        using var newPlan = await ExplainAsync(c, PgFactCollector.DatabaseSizeNewestSql, parameters, ct);
        using var oldPlan = await ExplainAsync(c, PairwiseDatabaseSizeNewestSql, parameters, ct);
        Assert.True(HasNestedLoop(oldPlan.RootElement[0].GetProperty("Plan"), "Anti"),
            "the pre-#8570 probe is expected to plan a nested loop anti join at this size; if it no longer does, the plant has stopped showing the defect: " + oldPlan.RootElement);
        Assert.False(HasNestedLoop(newPlan.RootElement[0].GetProperty("Plan"), "Anti"),
            "the probe compares the probed files with the newest snapshot's pair by pair: " + newPlan.RootElement);
    }

    [Fact]
    public async Task TheReconfigureTraceRead_ReadsTheSpansEvents_NotTheServersWholeTraceHistory_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live reconfigure trace test.");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, c) = await OpenAsync(ct);
        await using var scratchLease = scratch;
        await using var connectionLease = c;

        const int serverId = -857_002;
        var end = LiveClock.AnchoredEnd();
        var start = end.AddHours(-4);

        /* Thirty days of default-trace events for this server and another, collected a minute after they happened, one in
           two hundred an sp_configure line (msg 15457). */
        await ExecAsync(c, @"
INSERT INTO default_trace_events (default_trace_event_id, collection_time, server_id, server_name, event_time, event_name, error_number, text_data)
SELECT 857_000_000_000 + n, t, $1 + (n % 2), 'scale', t - interval '1 minute', 'ErrorLog',
       CASE WHEN n % 200 = 0 THEN 15457 ELSE 18453 END, 'row ' || n
FROM (SELECT n, $2::timestamp - (n % 43200) * interval '1 minute' AS t FROM generate_series(1, 90000) AS n) AS s", ct, serverId, end);
        /* Two lines the bound must keep: one collected nine hours before its event time (a monitored server's clock running
           fast), one collected two hours after it (a collector that ran late). */
        await ExecAsync(c, @"
INSERT INTO default_trace_events (default_trace_event_id, collection_time, server_id, server_name, event_time, event_name, error_number, text_data)
VALUES (857_900_000_001, $2::timestamp - interval '12 hours', $1, 'scale', $2::timestamp - interval '3 hours', 'ErrorLog', 15457, 'fast clock'),
       (857_900_000_002, $2::timestamp, $1, 'scale', $2::timestamp - interval '2 hours', 'ErrorLog', 15457, 'late collector')", ct, serverId, end);
        await ExecAsync(c, "ANALYZE default_trace_events", ct);

        /* The pre-#8570 read: the shipped text with the redundant collection_time bound taken out. */
        var oldSql = Regex.Replace(DarlingAnalysisService.ReconfigureTraceLinesForAttributionSql,
            @"AND\s+dte\.collection_time >= \$2 - interval '1 day'", string.Empty);
        Assert.NotEqual(DarlingAnalysisService.ReconfigureTraceLinesForAttributionSql, oldSql);

        object[] parameters = [serverId, start, end];
        var newRows = await ReadLinesAsync(c, DarlingAnalysisService.ReconfigureTraceLinesForAttributionSql, parameters, ct);
        var oldRows = await ReadLinesAsync(c, oldSql, parameters, ct);
        Assert.Equal(oldRows.Order(StringComparer.Ordinal), newRows.Order(StringComparer.Ordinal));
        Assert.Contains("fast clock", newRows);
        Assert.Contains("late collector", newRows);
        Assert.True(newRows.Count >= 4, "the plant should put several sp_configure lines in the span: " + string.Join(", ", newRows));

        using var newPlan = await ExplainAsync(c, DarlingAnalysisService.ReconfigureTraceLinesForAttributionSql, parameters, ct);
        using var oldPlan = await ExplainAsync(c, oldSql, parameters, ct);
        var newBlocks = Blocks(newPlan);
        var oldBlocks = Blocks(oldPlan);
        Assert.True(newBlocks * 4 <= oldBlocks,
            $"the read should touch a quarter of the blocks the whole-history read does or less: old={oldBlocks} new={newBlocks}");
    }

    private static async Task<List<string>> ReadLinesAsync(NpgsqlConnection c, string sql, object[] parameters, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        foreach (var p in parameters)
        {
            cmd.Parameters.AddWithValue(p is DateTime dt ? Naive(dt) : p);
        }

        var rows = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(reader.GetString(1));
        }

        return rows;
    }
}
