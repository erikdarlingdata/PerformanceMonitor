/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8570 LIVE: the web's collapsed Plan Corrections read (<see cref="DarlingPlanCorrectionReader.PlanCorrectionsCollapsedSql"/>) ranks on the
/// narrow key columns and fetches <c>query_text</c> and the other wide columns for the page's rows only. Before, three window functions ran over
/// every capture row of the window with the wide columns carried through the sort, and on a compressed week every row's wide text was
/// decompressed to be thrown away.
/// <para>The first test holds the new statement to the old one's answer on a seeded store, ties included. The second holds the temp blocks its
/// sorts write on a compressed window to a fraction of the old statement's.</para>
/// </summary>
/* #1776 own-store: every test here boots its own database through ScratchPostgres and works entirely inside
   it, so it cannot race the shared store's live collection and is left out of [Collection("live-postgres")]
   deliberately (the next sweep should not "fix" that). */
public sealed class PlanCorrectionNarrowRankLiveTests
{
    private const int ServerId = 8570;

    /// <summary>The statement as it ran before #8570, kept here as the oracle the new one must agree with. The wide columns ride through the rank.</summary>
    private const string OldCollapsedSql = @"
WITH captured AS (
    SELECT
        collection_time, database_name, query_text, recommendation_state, recommendation_state_reason, recommendation_reason, score,
        estimated_gain_seconds, query_id, regressed_plan_id, last_good_plan_id, last_good_plan_forcing_type, last_good_plan_is_forced,
        last_good_plan_force_failure_reason, regressed_plan_execution_count, regressed_plan_cpu_time_average_ms, last_good_plan_execution_count,
        last_good_plan_cpu_time_average_ms, valid_since, last_refresh, execute_action_initiated_by, execute_action_initiated_time,
        revert_action_initiated_by, revert_action_initiated_time,
        ROW_NUMBER() OVER (PARTITION BY database_name, recommendation_name, query_id ORDER BY collection_time DESC, score DESC NULLS LAST, regressed_plan_id DESC NULLS LAST, last_refresh DESC NULLS LAST) AS newest_rank,
        MIN(collection_time) OVER (PARTITION BY database_name, recommendation_name, query_id) AS first_seen,
        COUNT(*) OVER (PARTITION BY database_name, recommendation_name, query_id) AS capture_count
    FROM plan_correction
    WHERE server_id = $1
    AND   collection_time > $2
    AND   collection_time <= $3
    AND   recommendation_name IS NOT NULL
    AND   ($5::text[] IS NULL OR database_name = ANY($5))
)
SELECT
    collection_time, database_name, query_text, recommendation_state, recommendation_state_reason, recommendation_reason, score,
    estimated_gain_seconds, query_id, regressed_plan_id, last_good_plan_id, last_good_plan_forcing_type, last_good_plan_is_forced,
    last_good_plan_force_failure_reason, regressed_plan_execution_count, regressed_plan_cpu_time_average_ms, last_good_plan_execution_count,
    last_good_plan_cpu_time_average_ms, valid_since, last_refresh, execute_action_initiated_by, execute_action_initiated_time,
    revert_action_initiated_by, revert_action_initiated_time, first_seen, capture_count
FROM captured
WHERE newest_rank = 1
ORDER BY collection_time DESC, score DESC, database_name, query_id
LIMIT $4";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task NarrowRank_ReturnsTheOldStatementsRows_TiesAndNullsIncluded_AtEveryCap()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8570 plan correction test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var t = new DateTime(2026, 6, 20, 12, 0, 0, DateTimeKind.Unspecified);

        /* R1 on query 1: three captures. The newest instant holds three rows that tie on score (90): the regressed plan id decides (9), and two
           rows tie on that too, so the last refresh decides. */
        await PlantAsync(connection, 1, t.AddMinutes(-10), "a", "R1", 1, "old", 50, 3, t.AddMinutes(-11), ct);
        await PlantAsync(connection, 2, t.AddMinutes(-5), "a", "R1", 1, "mid", 70, 3, t.AddMinutes(-6), ct);
        await PlantAsync(connection, 3, t, "a", "R1", 1, "tie-plan-5", 90, 5, t.AddMinutes(-1), ct);
        await PlantAsync(connection, 4, t, "a", "R1", 1, "tie-plan-9-early", 90, 9, t.AddMinutes(-3), ct);
        await PlantAsync(connection, 5, t, "a", "R1", 1, "tie-plan-9-late", 90, 9, t.AddMinutes(-2), ct);

        /* Three more recommendations that tie R1's newest row on collection_time and score, so the order and the cap boundary run through the
           database_name, query_id tie-break. */
        await PlantAsync(connection, 6, t, "a", "R2", 2, "tie-b", 90, 4, t.AddMinutes(-4), ct);
        await PlantAsync(connection, 7, t, "b", "R3", 3, "tie-c", 90, 4, t.AddMinutes(-4), ct);
        await PlantAsync(connection, 8, t, "b", "R3", 4, "tie-d", 90, 4, t.AddMinutes(-4), ct);

        /* A NULL query id, a NULL score that loses to a real one at the same instant (NULLS LAST), and a NULL database name. */
        await PlantAsync(connection, 9, t.AddMinutes(-2), "c", "R4", null, "null-score", null, 1, t.AddMinutes(-5), ct);
        await PlantAsync(connection, 10, t.AddMinutes(-2), "c", "R4", null, "real-score", 10, 1, t.AddMinutes(-5), ct);
        await PlantAsync(connection, 11, t.AddMinutes(-7), null, "R5", 5, "no-database", 30, 1, t.AddMinutes(-8), ct);
        await PlantAsync(connection, 12, t.AddMinutes(-9), null, "R5", 5, "no-database-older", 35, 1, t.AddMinutes(-9), ct);

        /* The same capture stored twice: one row on the page, counted twice. */
        await PlantAsync(connection, 13, t.AddMinutes(-3), "d", "R6", 6, "dup", 20, 2, t.AddMinutes(-4), ct);
        await PlantAsync(connection, 14, t.AddMinutes(-3), "d", "R6", 6, "dup", 20, 2, t.AddMinutes(-4), ct);

        /* A capture exactly at the window's start is outside it (the half-open window of #9140), whichever statement reads. */
        await PlantAsync(connection, 16, t.AddHours(-1), "a", "R7", 7, "at-the-start", 99, 1, null, ct);

        /* A row with no recommendation, outside the read. */
        await PlantAsync(connection, 15, t, "a", null, null, "enablement-only", null, null, null, ct);

        var start = t.AddHours(-1);
        var end = t.AddHours(1);
        var filters = new string[]?[] { null, ["a", "b"], ["c", "d"] };

        foreach (var filter in filters)
        {
            for (var cap = 1; cap <= 9; cap++)
            {
                var expected = await RunAsync(connection, OldCollapsedSql, start, end, cap, filter, ct);
                var actual = await RunAsync(connection, DarlingPlanCorrectionReader.PlanCorrectionsCollapsedSql, start, end, cap, filter, ct);

                Assert.Equal(expected, actual);
            }
        }

        var all = await RunAsync(connection, DarlingPlanCorrectionReader.PlanCorrectionsCollapsedSql, start, end, 50, null, ct);
        Assert.Equal(7, all.Count);
    }

    [Fact]
    public async Task NarrowRank_SortsInMemoryWhereTheOldStatementSpilledToDiskOnACompressedWindow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8570 plan correction buffer test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled,
            "The live #8570 plan correction buffer test needs TimescaleDB: the history it measures is a compressed hypertable's chunks.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql("plan_correction"), ct);

        /* 60 recommendations captured every 5 minutes for 30 hours, each capture carrying about 3 KB of query text. */
        var newest = new DateTime(2026, 6, 20, 12, 0, 0, DateTimeKind.Unspecified);
        await ExecAsync(connection,
            @"INSERT INTO plan_correction (collection_id, collection_time, server_id, server_name, database_name, recommendation_name, recommendation_state,
                                           query_id, query_text, score, regressed_plan_id, last_refresh)
              SELECT (q::bigint * 1000000) + m, $1 - make_interval(mins => m * 5), $2, 'plan-correction-buffers', 'db' || (q % 3), 'PR_' || q, 'Active',
                     q, repeat(md5(q::text || m::text), 100), 50 + (m % 40), q, $1 - make_interval(mins => m * 5)
              FROM generate_series(1, 60) AS q, generate_series(0, 359) AS m",
            ct, newest, ServerId);
        await ExecAsync(connection,
            "SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('plan_correction') AS c", ct);
        await ExecAsync(connection, "ANALYZE plan_correction", ct);

        var start = newest.AddHours(-31);
        var end = newest.AddHours(1);

        var oldSpill = await SpilledBlocksAsync(connection, OldCollapsedSql, start, end, ct);
        var newSpill = await SpilledBlocksAsync(connection, DarlingPlanCorrectionReader.PlanCorrectionsCollapsedSql, start, end, ct);

        Assert.True(oldSpill > 0,
            "The old statement no longer spills its sort to disk on this seed, so the seed is too small to tell the two statements apart.");
        Assert.True(newSpill * 4 < oldSpill,
            $"The narrow rank wrote {newSpill} temp blocks sorting a compressed plan_correction window against the old statement's {oldSpill}; ranking on the key columns and fetching the wide ones for the page's rows keeps the sorts in memory.");
    }

    /// <summary>The temp blocks a statement writes, from EXPLAIN (ANALYZE, BUFFERS): the sorts that did not fit in work_mem and spilled to disk.
    /// Unlike the shared buffer count, which also holds the catalog pages a backend reads to rebuild a cache entry, this figure moves only with the
    /// width and count of the rows the statement sorts. The shared count of the two statements is close on a compressed window (the batches are
    /// read either way), so it does not tell them apart.</summary>
    private static async Task<long> SpilledBlocksAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, CancellationToken ct)
    {
        await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, connection);
        AddParameters(explain, start, end, 50, null);
        using var document = System.Text.Json.JsonDocument.Parse(Convert.ToString(await explain.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!);
        var root = document.RootElement[0].GetProperty("Plan");
        return root.TryGetProperty("Temp Written Blocks", out var written) ? written.GetInt64() : 0;
    }

    private static async Task<List<string>> RunAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, int cap, string[]? databases, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        AddParameters(command, start, end, cap, databases);

        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        var values = new object[reader.FieldCount];
        while (await reader.ReadAsync(ct))
        {
            reader.GetValues(values);
            rows.Add(string.Join("|", values.Select(v => v is DBNull ? "~" : Convert.ToString(v, CultureInfo.InvariantCulture))));
        }

        return rows;
    }

    private static void AddParameters(NpgsqlCommand command, DateTime start, DateTime end, int cap, string[]? databases)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = start, NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = end, NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = cap });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)databases ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text });
    }

    private static async Task PlantAsync(
        NpgsqlConnection connection, long collectionId, DateTime collectionTime, string? database, string? recommendation, long? queryId, string state,
        int? score, long? regressedPlanId, DateTime? lastRefresh, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO plan_correction
                (collection_id, collection_time, server_id, server_name, database_name, recommendation_name, recommendation_state,
                 query_id, query_text, score, regressed_plan_id, last_refresh, estimated_gain_seconds)
            VALUES ($1, $2, $3, 'plan-correction-ties', $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """, connection);
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = collectionId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = collectionTime, NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)database ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)recommendation ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter { Value = state, NpgsqlDbType = NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)queryId ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Bigint });
        command.Parameters.Add(new NpgsqlParameter { Value = "select " + state, NpgsqlDbType = NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)score ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Integer });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)regressedPlanId ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Bigint });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)lastRefresh ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter { Value = 1.0, NpgsqlDbType = NpgsqlDbType.Double });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] values)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var value in values)
        {
            command.Parameters.Add(value is DateTime when
                ? new NpgsqlParameter { Value = when, NpgsqlDbType = NpgsqlDbType.Timestamp }
                : new NpgsqlParameter { Value = value });
        }

        await command.ExecuteNonQueryAsync(ct);
    }
}
