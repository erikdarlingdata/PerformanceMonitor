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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The wide pass of the top-procedures read (#9280) on a real PostgreSQL store. Pass 2 fetched the winners' rows by joining the winners to
/// every raw row of the window, which reads the whole window a second time to keep a handful of procedures. It now joins on plain equalities
/// on the database and object plus the procedure's full name, the expression <c>idx_procedure_stats_server_db_name_time</c> keys on (#8570),
/// so the planner seeks each winner's rows. Two behaviours are held here: the answer is the old answer (NULL database, NULL object, NULL
/// schema and two object types under one name all survive), and the read touches far fewer buffers when the window holds many procedures.
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so cross-test row churn cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class TopProceduresWidePassLiveTests
{
    private const int ServerId = -9280401;
    private const int Procedures = 800;
    private const int SamplesPerProcedure = 36;
    private const int Top = 9;

    /// <summary>Pass 2 as it was before #9280, kept here only as the reference the new pass must agree with.</summary>
    private const string OldWideSql = """
        SELECT
            database_name,
            schema_name,
            object_name,
            object_type,
            MAX(sql_handle) AS sql_handle,
            MAX(plan_handle) AS plan_handle,
            CAST(SUM(delta_execution_count) AS bigint) AS total_executions,
            CAST(SUM(delta_worker_time) AS bigint) AS total_cpu_us,
            CAST(SUM(delta_elapsed_time) AS bigint) AS total_elapsed_us,
            CAST(SUM(delta_logical_reads) AS bigint) AS total_reads,
            CAST(SUM(delta_logical_writes) AS bigint) AS total_writes,
            CAST(SUM(delta_physical_reads) AS bigint) AS total_physical_reads,
            CAST(SUM(delta_spills) AS bigint) AS total_spills,
            MIN(min_worker_time) AS min_worker_time,
            MAX(max_worker_time) AS max_worker_time,
            MIN(min_elapsed_time) AS min_elapsed_time,
            MAX(max_elapsed_time) AS max_elapsed_time,
            MAX(last_execution_time) AS last_execution_time,
            MAX(cached_time) AS cached_time,
            MIN(min_logical_reads) AS min_logical_reads,
            MAX(max_logical_reads) AS max_logical_reads,
            MIN(min_physical_reads) AS min_physical_reads,
            MAX(max_physical_reads) AS max_physical_reads,
            MIN(min_logical_writes) AS min_logical_writes,
            MAX(max_logical_writes) AS max_logical_writes,
            MIN(min_spills) AS min_spills,
            MAX(max_spills) AS max_spills
        FROM procedure_stats
        JOIN winners AS w
            ON  COALESCE(object_name, '') = COALESCE(w.win_object_name, '')
            AND database_name IS NOT DISTINCT FROM w.win_database_name
            AND schema_name IS NOT DISTINCT FROM w.win_schema_name
            AND object_name IS NOT DISTINCT FROM w.win_object_name
            AND object_type IS NOT DISTINCT FROM w.win_object_type
        WHERE server_id = $1
        AND   collection_time > $2
        AND   collection_time <= $3
        AND   ($5::text[] IS NULL OR database_name = ANY($5))
        AND   ($6::text[] IS NULL OR database_name IS NULL OR database_name <> ALL($6))
        AND   sample_interval_seconds IS DISTINCT FROM 0
        GROUP BY database_name, schema_name, object_name, object_type
        ORDER BY MAX(w.rank_metric) DESC NULLS LAST, MAX(w.rank_cpu) DESC NULLS LAST, database_name, schema_name, object_name, object_type
        """;

    private static string OldStatement => TopRankings.Apply(DarlingDataReader.TopProceduresRawWinnersSql, TopRanking.Cpu, hourly: false) + "\n" + OldWideSql;

    private static string NewStatement => TopRankings.Apply(DarlingDataReader.TopProceduresSql, TopRanking.Cpu, hourly: false);

    [Fact]
    public async Task TheWidePass_AnswersTheOldRows_AndSeeksTheWinnersRowsThroughTheNameIndex()
    {
        await RunAsync(async (connection, ct, start, end) =>
        {
            var oldRows = await ReadAsync(connection, OldStatement, start, end, ct);
            var newRows = await ReadAsync(connection, NewStatement, start, end, ct);

            /* The special winners are all in the top: a NULL database, a NULL object, a NULL schema and one name under two object types. */
            Assert.Equal(Top, oldRows.Count);
            Assert.Contains(oldRows, r => r.StartsWith("|dbo|PNULLDB|", StringComparison.Ordinal));
            Assert.Contains(oldRows, r => r.StartsWith("db1|dbo||", StringComparison.Ordinal));
            Assert.Contains(oldRows, r => r.StartsWith("db1||PNOSCHEMA|", StringComparison.Ordinal));
            Assert.Equal(2, oldRows.Count(r => r.StartsWith("db1|dbo|PTWOTYPES|", StringComparison.Ordinal)));
            Assert.Equal(oldRows, newRows);

            var oldPlan = await PlanAsync(connection, OldStatement, start, end, ct);
            var newPlan = await PlanAsync(connection, NewStatement, start, end, ct);

            /* On a hypertable a chunk's index name carries the index's name as its tail. */
            Assert.DoesNotContain(oldPlan.IndexNames, n => n.EndsWith(ProcedureStatsNameIndex.IndexName, StringComparison.Ordinal));
            Assert.Contains(newPlan.IndexNames, n => n.EndsWith(ProcedureStatsNameIndex.IndexName, StringComparison.Ordinal));
            Assert.True(newPlan.Buffers * 4 < oldPlan.Buffers * 3, $"new read {newPlan.Buffers} buffers, old read {oldPlan.Buffers}");
        });
    }

    private static async Task<List<string>> ReadAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, CancellationToken ct)
    {
        using var command = Bind(connection, sql, start, end);
        var rows = new List<string>();
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var cells = new string[reader.FieldCount];
            for (var i = 0; i < cells.Length; i++)
            {
                cells[i] = reader.IsDBNull(i) ? string.Empty : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture)!;
            }

            rows.Add(string.Join('|', cells));
        }

        return rows;
    }

    /// <summary>The shared buffers the statement touched, hits plus reads, from the root node of a warm second run, and the indexes its plan scanned.</summary>
    private static async Task<(long Buffers, HashSet<string> IndexNames)> PlanAsync(
        NpgsqlConnection connection, string sql, DateTime start, DateTime end, CancellationToken ct)
    {
        long total = 0;
        var names = new HashSet<string>(StringComparer.Ordinal);
        for (var run = 0; run < 2; run++)
        {
            using var command = Bind(connection, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, start, end);
            var json = Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!;
            using var document = JsonDocument.Parse(json);
            var plan = document.RootElement[0].GetProperty("Plan");
            total = plan.GetProperty("Shared Hit Blocks").GetInt64() + plan.GetProperty("Shared Read Blocks").GetInt64();
            names.Clear();
            Collect(plan, names);
        }

        return (total, names);
    }

    private static void Collect(JsonElement node, HashSet<string> names)
    {
        if (node.TryGetProperty("Index Name", out var name))
        {
            names.Add(name.GetString()!);
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                Collect(child, names);
            }
        }
    }

    private static NpgsqlCommand Bind(NpgsqlConnection connection, string sql, DateTime start, DateTime end)
    {
        var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = start });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = end });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Top });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
        return command;
    }

    private static async Task SeedAsync(NpgsqlConnection connection, DateTime start, DateTime end, CancellationToken ct)
    {
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, "wide-pass-9280", ct);

        /* Ordinary procedures: small CPU, so the special ones below are the top. 5 minute samples across the window. */
        await using (var ordinary = new NpgsqlCommand(
            $@"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
                                            object_type, sql_handle, plan_handle, delta_execution_count, delta_worker_time, delta_elapsed_time,
                                            delta_logical_reads, delta_spills, min_worker_time, max_worker_time, sample_interval_seconds)
               SELECT {ServerId}::bigint * 1000000 + row_number() OVER (), $1::timestamp + make_interval(mins => 5 * s), {ServerId}, 'wide-pass-9280',
                      'db' || (n % 3), 'dbo', 'P' || lpad(n::text, 5, '0'), 'P', 'sh' || n, 'ph' || n,
                      1 + (n + s) % 9, 100 + (n * 37 + s * 11) % 5000, 200 + (n * 53 + s * 7) % 9000,
                      n % 50, n % 2, n % 97, 2 * (n % 97), 300
               FROM generate_series(0, {Procedures - 1}) AS n
               CROSS JOIN generate_series(1, {SamplesPerProcedure}) AS s", connection) { CommandTimeout = 120 })
        {
            ordinary.Parameters.AddWithValue(NpgsqlDbType.Timestamp, start);
            await ordinary.ExecuteNonQueryAsync(ct);
        }

        /* The top: each with enormous CPU. The database, the object, the schema and the object type is NULL on one each. */
        var specials = new (string? Database, string? Schema, string? Object, string Type, long Cpu)[]
        {
            (null, "dbo", "PNULLDB", "P", 900_000_000),
            ("db1", "dbo", null, "P", 800_000_000),
            ("db1", null, "PNOSCHEMA", "P", 700_000_000),
            ("db1", "dbo", "PTWOTYPES", "P", 600_000_000),
            ("db1", "dbo", "PTWOTYPES", "PC", 500_000_000),
            ("db2", "dbo", "PBIG1", "P", 400_000_000),
            ("db2", "dbo", "PBIG2", "P", 300_000_000),
            ("db2", "dbo", "PBIG3", "P", 200_000_000),
            ("db0", "dbo", "PBIG4", "P", 100_000_000),
        };
        for (var i = 0; i < specials.Length; i++)
        {
            var (database, schema, objectName, type, cpu) = specials[i];
            for (var s = 1; s <= SamplesPerProcedure; s++)
            {
                await using var command = new NpgsqlCommand(
                    @"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
                                                   object_type, delta_execution_count, delta_worker_time, delta_elapsed_time, sample_interval_seconds)
                      VALUES ($1, $2, $3, 'wide-pass-9280', $4, $5, $6, $7, 5, $8, $8, 300)", connection);
                command.Parameters.AddWithValue(NpgsqlDbType.Bigint, ServerId * 1_000_000L - 100_000L - (i * 100L) - s);
                command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, start.AddMinutes(5 * s));
                command.Parameters.AddWithValue(NpgsqlDbType.Integer, ServerId);
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)database ?? DBNull.Value });
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)schema ?? DBNull.Value });
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)objectName ?? DBNull.Value });
                command.Parameters.AddWithValue(NpgsqlDbType.Text, type);
                command.Parameters.AddWithValue(NpgsqlDbType.Bigint, cpu / SamplesPerProcedure);
                await command.ExecuteNonQueryAsync(ct);
            }
        }

        await using var index = new NpgsqlCommand(ProcedureStatsNameIndex.CreateSql, connection) { CommandTimeout = 120 };
        await index.ExecuteNonQueryAsync(ct);
        await using var analyze = new NpgsqlCommand("ANALYZE procedure_stats", connection) { CommandTimeout = 120 };
        await analyze.ExecuteNonQueryAsync(ct);
    }

    private static async Task RunAsync(Func<NpgsqlConnection, CancellationToken, DateTime, DateTime, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live top-procedures wide-pass test.");

        var ct = TestContext.Current.CancellationToken;
        var end = LiveClock.AnchoredEnd();
        var start = end.AddHours(-3);

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteTestRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, start, end, ct);
            await body(connection, ct, start, end);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand($"DELETE FROM procedure_stats WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId}", connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
