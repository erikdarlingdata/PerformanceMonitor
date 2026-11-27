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
using PerformanceMonitor.Darling.Service.Reports;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only
   to CREATE and DROP its own database through ScratchPostgres, then works entirely inside it.

   #8570: the Daily Health Report's failed-jobs read keeps only step 0 failures, and on an uncompressed chunk each server's time range
   fetched thousands of heap rows to find them. V302 adds a partial index over exactly those rows. The answer must not change, and the
   read must use the index on an uncompressed chunk. */
public sealed class JobHistoryFailedIndexLiveTests
{
    private const string IndexName = "idx_job_history_job_failed";
    private const int ServerCount = 8;

    /// <summary>Eight servers, 20,000 outcome and step rows each over the last 44 hours. About one row in 390 is a step 0 failure (the rows
    /// the index holds), one in five of the step rows fails too (rows the index must leave out), and the rest are successes.</summary>
    private const string Seed = @"
INSERT INTO collect.job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, job_enabled, category_name,
    step_id, step_name, run_status, run_status_desc, run_datetime, run_duration_seconds, retries_attempted, message)
SELECT (s * 1000000 + g)::bigint, $1::timestamp - g * interval '8 seconds', s, 'srv' || s, (s * 1000000 + g)::bigint, 'job' || (g % 7), 'job' || (g % 7), true, 'cat',
       g % 4, 'step', CASE WHEN g % 4 = 0 AND g % 388 = 0 THEN 0 WHEN g % 4 <> 0 AND g % 5 = 0 THEN 0 ELSE 1 END,
       'desc', $1::timestamp - g * interval '8 seconds', 5, 0, 'message ' || g
FROM generate_series(1, 8) AS s, generate_series(1, 20000) AS g
ORDER BY g DESC, s;";

    private static async Task<List<string>> RowsAsync(NpgsqlConnection connection, DateTime end, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var command = Command(connection, DailyHealthReportBuilder.PeriodFailedJobsSql, end);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(string.Join("|", Enumerable.Range(0, reader.FieldCount)
                .Select(i => reader.IsDBNull(i) ? "<null>" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture))));
        }

        return rows;
    }

    private static NpgsqlCommand Command(NpgsqlConnection connection, string sql, DateTime end)
    {
        var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        var ids = Enumerable.Range(1, ServerCount).ToArray();
        command.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = ids });
        command.Parameters.Add(new NpgsqlParameter<DateTime[]> { TypedValue = ids.Select(_ => end.AddHours(-48)).ToArray() });
        command.Parameters.Add(new NpgsqlParameter<DateTime[]> { TypedValue = ids.Select(_ => end).ToArray() });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = end.AddHours(-49) });
        return command;
    }

    private static bool PlanUsesIndex(JsonElement plan)
    {
        if (plan.TryGetProperty("Index Name", out var name) && (name.GetString() ?? string.Empty).EndsWith(IndexName, StringComparison.Ordinal))
        {
            return true;
        }

        if (plan.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                if (PlanUsesIndex(child))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static async Task<bool> ReadUsesIndexAsync(NpgsqlConnection connection, DateTime end, CancellationToken ct)
    {
        await using var explain = Command(connection, "EXPLAIN (FORMAT JSON) " + DailyHealthReportBuilder.PeriodFailedJobsSql, end);
        using var document = JsonDocument.Parse(Convert.ToString(await explain.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!);
        return PlanUsesIndex(document.RootElement[0].GetProperty("Plan"));
    }


    [Fact]
    public async Task FailedJobsRead_UsesTheFailedOutcomeIndex_OnAnUncompressedChunk_AndReturnsTheSameRows()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8570 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            /* The migration made a partial index on job_history: the failed outcomes only. */
            await using (var catalog = new NpgsqlCommand(
                "SELECT i.indisvalid, i.indpred IS NOT NULL FROM pg_index AS i JOIN pg_class AS c ON c.oid = i.indexrelid JOIN pg_namespace AS n ON n.oid = c.relnamespace WHERE n.nspname = 'collect' AND c.relname = $1", connection))
            {
                catalog.Parameters.AddWithValue(IndexName);
                await using var reader = await catalog.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct), "V302 did not create the failed outcome index");
                Assert.True(reader.GetBoolean(0));
                Assert.True(reader.GetBoolean(1), "the failed outcome index is not partial");
            }

            await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            var end = LiveClock.AnchoredEnd();
            await ExecAsync(connection, Seed, ct, end);
            await ExecAsync(connection, "ANALYZE collect.job_history", ct);

            await using (var chunks = new NpgsqlCommand(
                "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = 'job_history' AND NOT is_compressed", connection))
            {
                Assert.True((long)(await chunks.ExecuteScalarAsync(ct))! >= 2, "the seed must sit in uncompressed chunks");
            }

            var withIndex = await RowsAsync(connection, end, ct);
            Assert.True(await ReadUsesIndexAsync(connection, end, ct), "the failed jobs read did not use the failed outcome index");
            Assert.NotEmpty(withIndex);
            Assert.All(withIndex, r => Assert.StartsWith("message", r.Split('|')[4], StringComparison.Ordinal));

            /* The answer is the answer without the index. */
            await ExecAsync(connection, "DROP INDEX collect.idx_job_history_job_failed", ct);
            var withoutIndex = await RowsAsync(connection, end, ct);
            Assert.False(await ReadUsesIndexAsync(connection, end, ct));
            Assert.Equal(withoutIndex, withIndex);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 600 };
        foreach (var value in parameters)
        {
            command.Parameters.AddWithValue(value);
        }

        await command.ExecuteNonQueryAsync(ct);
    }
}
