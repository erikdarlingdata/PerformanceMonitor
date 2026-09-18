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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3540 (V128), the read half of the completion, proven against LIVE Postgres (gated on
/// <c>DARLING_TEST_PG</c>) through the real readers: a collection every row of which the collector stored with
/// <c>sample_interval_seconds = 0</c> — the calculator's "no delta knowable" marker, in practice a restart —
/// is NOT a point on the procedure duration trend or the per-statement PostgreSQL trend. It used to be a
/// confident 0.00 ms/sec and 0.00 calls/sec at exactly the moment nothing was knowable.
///
/// <para>Three states per collection, read distinctly: a MEASURED interval (MAX over the collection's rows)
/// divides the summed deltas and wins over the LAG derivation; the marker (every row 0) yields no point; and
/// NULL — every row collected before V128 — falls back to the LAG over collection_time those reads always
/// used. The procedure history grid shows the stored interval itself, the marker's 0 included, because a
/// displayed interval is not a rate. Lite's twin of these claims runs in-process on DuckDB
/// (<c>DeltaFamilyUnknowableRowReadTests</c>).</para>
///
/// <para>Every expected value below was worked by hand from the rows and reproduced against PG18 +
/// TimescaleDB 2.28.1 before this was written.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DeltaFamilyIntervalCompletionLivePostgresTests
{
    private const int ServerId = -128128;
    private const string ServerName = "delta-interval-completion-e2e";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// The procedure duration trend, both copies (the viewer's read and the MCP's SQL, which the pin in
    /// DarlingMcpTrendToolsTests proves are one string): t1/t2 pre-V128 (NULL) — t1 no prior, no point; t2 the
    /// LAG's 300 s. t3 a restart — every row 0 — absent. t4 a steady pass with a readmitted plan (its row 0)
    /// beside a measured 120 s row: MAX 120 wins over the LAG's 300, and the readmitted plan adds 0.
    /// </summary>
    [Fact]
    public async Task ProcedureDurationTrend_DropsTheUnknowableCollection_PrefersTheStoredInterval_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live procedure-trend test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var viewer = new ViewerDataService(cs!);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            var t1 = Naive(TruncateToSeconds(DateTime.UtcNow.AddHours(-2)));
            var t2 = t1.AddMinutes(5);
            var t3 = t2.AddMinutes(5);
            var t4 = t3.AddMinutes(5);

            await ProcedureAsync(connection, t1, "usp_A", deltaExecutions: 5, deltaElapsedUs: 100_000, interval: null, ct);
            await ProcedureAsync(connection, t2, "usp_A", 30, 600_000, null, ct);
            await ProcedureAsync(connection, t3, "usp_A", 0, 0, 0, ct);
            await ProcedureAsync(connection, t4, "usp_A", 24, 1_200_000, 120, ct);
            await ProcedureAsync(connection, t4, "usp_New", 0, 0, 0, ct);

            var points = await viewer.GetProcedureDurationTrendAsync(ServerId, t1.AddMinutes(-1), t4.AddMinutes(1), cancellationToken: ct);
            Assert.Equal(new[] { t2, t4 }, points.Select(p => p.CollectionTime).ToArray());
            Assert.Equal(2.0, points[0].Value, precision: 6);      /* 600 ms / LAG 300 s */
            Assert.Equal(0, points[0].ExecutionCount);             /* 30 / 300 = 0.1 executions/sec, truncated to long as always */
            Assert.Equal(10.0, points[1].Value, precision: 6);     /* 1200 ms / STORED 120 s, not the LAG's 4.0 */

            /* The MCP copy, run as the tool would run it on the raw tier. */
            await using (var command = postgres.CreateCommand(DarlingTrendReader.ProcedureDurationTrendSql))
            {
                command.Parameters.AddWithValue(ServerId);
                command.Parameters.AddWithValue(t1.AddMinutes(-1));
                command.Parameters.AddWithValue(t4.AddMinutes(1));
                var mcp = new List<(DateTime At, double? Rate, double? Executions)>();
                await using var reader = await command.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    mcp.Add((reader.GetDateTime(0),
                        reader.IsDBNull(1) ? null : Convert.ToDouble(reader.GetValue(1)),
                        reader.IsDBNull(2) ? null : Convert.ToDouble(reader.GetValue(2))));
                }

                /* The SQL returns all four collections; t1 and t3 carry NULL rates, which the shared reader drops. */
                Assert.Equal(new[] { t1, t2, t3, t4 }, mcp.Select(m => m.At).ToArray());
                Assert.Null(mcp[0].Rate);
                Assert.Equal(2.0, mcp[1].Rate!.Value, precision: 6);
                Assert.Null(mcp[2].Rate);
                Assert.Null(mcp[2].Executions);
                Assert.Equal(10.0, mcp[3].Rate!.Value, precision: 6);
                Assert.Equal(0.2, mcp[3].Executions!.Value, precision: 6);
            }

            /* The history grid: the stored interval as stored, the marker's 0 included; LAG only for NULL. */
            var history = await viewer.GetProcedureStatsHistoryAsync(ServerId, "AppDb", "dbo", "usp_A", t1.AddMinutes(-1), t4.AddMinutes(1), ct);
            Assert.Equal(new[] { t1, t2, t3, t4 }, history.Select(h => h.CollectionTime).ToArray());
            Assert.Null(history[0].SampleIntervalSeconds);
            Assert.Equal(300, history[1].SampleIntervalSeconds);
            Assert.Equal(0, history[2].SampleIntervalSeconds);
            Assert.Equal(120, history[3].SampleIntervalSeconds);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The per-statement PostgreSQL trend (<see cref="DarlingPgTrendReader.QueryDurationTrendSql"/>): the same
    /// four collections for one queryid across two (dbid, userid, toplevel) entries. t3 is a
    /// <c>pg_stat_statements_reset()</c> — both entries' rows store 0 — and is absent; at t4 one entry is a
    /// first sighting (0) beside the other's measured 120 s, so MAX is 120 and calls_per_second is the measured
    /// entry's 24 calls over 120 s, not over the LAG's 300.
    /// </summary>
    [Fact]
    public async Task PgQueryDurationTrend_DropsTheUnknowableSnapshot_PrefersTheStoredInterval_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live pg-statement-trend test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            var t1 = Naive(TruncateToSeconds(DateTime.UtcNow.AddHours(-2)));
            var t2 = t1.AddMinutes(5);
            var t3 = t2.AddMinutes(5);
            var t4 = t3.AddMinutes(5);
            const long QueryId = 4242;

            await PgStatementAsync(connection, t1, QueryId, userId: 10, deltaCalls: 5, deltaMs: 100, interval: null, ct);
            await PgStatementAsync(connection, t2, QueryId, 10, 30, 600, null, ct);
            await PgStatementAsync(connection, t3, QueryId, 10, 0, 0, 0, ct);
            await PgStatementAsync(connection, t3, QueryId, 11, 0, 0, 0, ct);
            await PgStatementAsync(connection, t4, QueryId, 10, 24, 1200, 120, ct);
            await PgStatementAsync(connection, t4, QueryId, 11, 0, 0, 0, ct);

            var points = await DarlingPgTrendReader.GetQueryDurationTrendAsync(postgres, ServerId, QueryId, t1.AddMinutes(-1), t4.AddMinutes(1), ct);

            Assert.Equal(new[] { t2, t4 }, points.Select(p => p.CollectionTimeUtc).ToArray());
            Assert.Equal(30, points[0].Calls);
            Assert.Equal(0.1, points[0].CallsPerSecond, precision: 6);   /* 30 / LAG 300 s */
            Assert.Equal(20.0, points[0].MeanExecMs, precision: 6);      /* 600 / 30 */
            Assert.Equal(24, points[1].Calls);
            Assert.Equal(0.2, points[1].CallsPerSecond, precision: 6);   /* 24 / STORED 120 s, not the LAG's 0.08 */
            Assert.Equal(50.0, points[1].MeanExecMs, precision: 6);      /* 1200 / 24 */

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* ---- seeding ---------------------------------------------------------------------------------------- */

    private static DateTime Naive(DateTime t) => DateTime.SpecifyKind(t, DateTimeKind.Unspecified);

    private static DateTime TruncateToSeconds(DateTime value) =>
        new(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond), value.Kind);

    private static async Task ProcedureAsync(NpgsqlConnection connection, DateTime t, string objectName, long deltaExecutions, long deltaElapsedUs, int? interval, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(
            "INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type, " +
            "execution_count, total_worker_time, total_elapsed_time, total_logical_reads, total_physical_reads, total_logical_writes, " +
            "delta_execution_count, delta_worker_time, delta_elapsed_time, sample_interval_seconds) " +
            "VALUES (1, $1, $2, $3, 'AppDb', 'dbo', $4, 'PROCEDURE', 0, 0, 0, 0, 0, 0, $5, 0, $6, $7)", connection);
        cmd.Parameters.AddWithValue(t);
        cmd.Parameters.AddWithValue(ServerId);
        cmd.Parameters.AddWithValue(ServerName);
        cmd.Parameters.AddWithValue(objectName);
        cmd.Parameters.AddWithValue(deltaExecutions);
        cmd.Parameters.AddWithValue(deltaElapsedUs);
        cmd.Parameters.AddWithValue(interval.HasValue ? interval.Value : DBNull.Value);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task PgStatementAsync(NpgsqlConnection connection, DateTime t, long queryId, long userId, long deltaCalls, long deltaMs, int? interval, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(
            "INSERT INTO pg_statement_stats (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel, " +
            "calls, total_exec_time_ms, rows_returned, delta_calls, delta_total_exec_time_ms, delta_rows, sample_interval_seconds) " +
            "VALUES (1, $1, $2, $3, $4, 16384, $5, true, 0, 0, 0, $6, $7, 0, $8)", connection);
        cmd.Parameters.AddWithValue(t);
        cmd.Parameters.AddWithValue(ServerId);
        cmd.Parameters.AddWithValue(ServerName);
        cmd.Parameters.AddWithValue(queryId);
        cmd.Parameters.AddWithValue(userId);
        cmd.Parameters.AddWithValue(deltaCalls);
        cmd.Parameters.AddWithValue(deltaMs);
        cmd.Parameters.AddWithValue(interval.HasValue ? interval.Value : DBNull.Value);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM procedure_stats WHERE server_id = {ServerId}; DELETE FROM pg_statement_stats WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
