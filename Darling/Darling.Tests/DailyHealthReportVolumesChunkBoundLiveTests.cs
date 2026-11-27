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
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Reports;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only
   to CREATE and DROP its own database through ScratchPostgres, then works entirely inside it.

   #8570: the Daily Health Report's period volume read bounded its newest-snapshot CTE to the period but not the outer read of
   database_size_stats, so the join read every chunk of the table. The outer read now carries the same bounds. The rows must not
   change, and a chunk that ends before the period must not be read. */
public sealed class DailyHealthReportVolumesChunkBoundLiveTests
{
    private const int ServerCount = 3;
    private const int DaysSeeded = 4;

    /// <summary>The statement as it read before #8570: the outer read had no period bounds.</summary>
    private static readonly string UnboundedSql = @"
WITH newest AS (
    SELECT server_id, MAX(collection_time) AS collection_time
    FROM database_size_stats
    WHERE collection_time >= $1
    AND   collection_time <= $2
    GROUP BY server_id
)
SELECT
    d.server_id,
    " + VolumeKey.Sql + @" AS volume_mount_point,
    MAX(d.volume_total_mb) AS volume_total_mb,
    MIN(d.volume_free_mb) AS volume_free_mb,
    MAX(d.collection_time) AS newest_collection_time
FROM database_size_stats AS d
JOIN newest AS n ON n.server_id = d.server_id AND n.collection_time = d.collection_time
WHERE " + VolumeKey.Sql + @" IS NOT NULL
AND   d.volume_total_mb > 0
GROUP BY d.server_id, " + VolumeKey.Sql + @"";

    /// <summary>One snapshot per server per hour for four days over 6 hour chunks, two volumes per server, so the table spans many chunks.
    /// Volume free space changes every hour, so each snapshot's rows are distinct.</summary>
    private const string Seed = @"
INSERT INTO database_size_stats(collection_id, collection_time, server_id, server_name, database_name, file_id, file_type_desc, file_name,
    physical_name, total_size_mb, is_percent_growth, growth_pct, volume_mount_point, volume_total_mb, volume_free_mb)
SELECT -(900000000 + k * 1000 + s * 10 + v), $1::timestamp - k * interval '1 hour', s, 'srv' || s, 'db', v, 'ROWS', 'f' || v,
       '/data/f' || v || '.mdf', 100, false, 0, CASE WHEN v = 1 THEN 'C:\' ELSE 'D:\' END, 1000000, 400000 + k
FROM generate_series(0, 95) AS k, generate_series(1, 3) AS s, generate_series(1, 2) AS v
ORDER BY k DESC, s, v;
ANALYZE collect.database_size_stats;";

    private static async Task<List<string>> RowsAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var command = Command(connection, sql, start, end);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(string.Join("|", Enumerable.Range(0, reader.FieldCount)
                .Select(i => reader.IsDBNull(i) ? "<null>" : Convert.ToString(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture))));
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    private static NpgsqlCommand Command(NpgsqlConnection connection, string sql, DateTime start, DateTime end)
    {
        var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = start });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = end });
        return command;
    }

    [Fact]
    public async Task PeriodVolumes_ReturnTheSameRows_AndReadNoChunkThatEndsBeforeThePeriod()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8570 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await ExecAsync(connection, "SELECT set_chunk_time_interval('collect.database_size_stats', interval '6 hours')", ct);
        var end = LiveClock.AnchoredEnd();
        await ExecAsync(connection, Seed.Split("ANALYZE")[0], ct, end);
        await ExecAsync(connection, "ANALYZE collect.database_size_stats", ct);

        var bodySucceeded = false;
        try
        {
            var start = end.AddHours(-4);

            /* Rows seeded after the period's end are inside the table too: the bounds must keep them out of the answer. */
            await ExecAsync(connection,
                "INSERT INTO database_size_stats(collection_id, collection_time, server_id, server_name, database_name, file_id, file_type_desc, file_name, physical_name, total_size_mb, is_percent_growth, growth_pct, volume_mount_point, volume_total_mb, volume_free_mb) "
                + "VALUES (-899999999, $1, 1, 'srv1', 'db', 1, 'ROWS', 'f1', '/data/f1.mdf', 100, false, 0, 'E:\\', 1000000, 1)", ct, end.AddHours(3));

            var bounded = await RowsAsync(connection, DailyHealthReportBuilder.PeriodVolumesSql, start, end, ct);
            var unbounded = await RowsAsync(connection, UnboundedSql, start, end, ct);
            Assert.Equal(ServerCount * 2, bounded.Count);
            Assert.Equal(unbounded, bounded);
            Assert.DoesNotContain(bounded, r => r.Contains("E:", StringComparison.Ordinal));

            /* The chunks that end before the period: the plan must not read a row from any of them. */
            var outside = new HashSet<string>(StringComparer.Ordinal);
            await using (var chunkNames = new NpgsqlCommand(
                "SELECT cl.relname FROM show_chunks('collect.database_size_stats', older_than => $1::timestamp) AS ch JOIN pg_class AS cl ON cl.oid = ch", connection))
            {
                chunkNames.Parameters.AddWithValue(start);
                await using var names = await chunkNames.ExecuteReaderAsync(ct);
                while (await names.ReadAsync(ct))
                {
                    outside.Add(names.GetString(0));
                }
            }

            Assert.True(outside.Count >= 12, $"the store must hold many chunks that end before the period, it holds {outside.Count}");

            await using var explain = Command(connection, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + DailyHealthReportBuilder.PeriodVolumesSql, start, end);
            var outsideNodes = ChunkNodesInPlan(await explain.ExecuteScalarAsync(ct), outside);
            Assert.True(outsideNodes == 0, $"the period volume plan holds {outsideNodes} scans of chunks that end before the period starts");

            /* The control: the old statement does read them, so the plan assertion above can fail. */
            await using var oldExplain = Command(connection, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + UnboundedSql, start, end);
            var oldOutsideNodes = ChunkNodesInPlan(await oldExplain.ExecuteScalarAsync(ct), outside);
            Assert.True(oldOutsideNodes > 0, "the unbounded statement was expected to plan scans of chunks outside the period");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>The plan's scan nodes over <paramref name="chunks"/>, counted whether or not they ran: a chunk the plan holds is a
    /// chunk the statement can read, and a nested loop that never reached one still carries it.</summary>
    private static int ChunkNodesInPlan(object? explainJson, ISet<string> chunks)
    {
        using var document = System.Text.Json.JsonDocument.Parse(Convert.ToString(explainJson, System.Globalization.CultureInfo.InvariantCulture)!);
        return CountChunkNodes(document.RootElement[0].GetProperty("Plan"), chunks);
    }

    private static int CountChunkNodes(System.Text.Json.JsonElement plan, ISet<string> chunks)
    {
        var count = plan.TryGetProperty("Relation Name", out var name) && chunks.Contains(name.GetString() ?? string.Empty) ? 1 : 0;
        if (plan.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                count += CountChunkNodes(child, chunks);
            }
        }

        return count;
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
