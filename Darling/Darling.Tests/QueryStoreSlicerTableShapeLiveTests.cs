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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8549: the Query Store slicer's interval-table statement used to group every binned row by (bucket, query_id) with five sums and then
/// group that by bucket, so on a 30-day window the sort or hash carried all five sums over millions of groups and wrote hundreds of
/// megabytes of temp files. It now adds the bucket totals straight off the binned rows and counts distinct queries in a separate step over
/// (bucket, query_id) pairs alone. The test keeps the previous statement as a reference, seeds one table of 343,000 rows (rows with no
/// query_id, rows with no interval start that are placed by collection time, an hour holding only rows with no query_id, rows on and just past
/// the window's end), and checks that the two statements return the same lines (the seed's values are whole numbers, so the sums are exact in
/// any order) and that, at a work_mem of 8 MB with no parallel workers, the new statement writes at most a third of the temp blocks the
/// previous one wrote.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The test reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class QueryStoreSlicerTableShapeLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly DateTime WindowStart = new(2026, 9, 15, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowEnd = new(2026, 10, 15, 0, 0, 0, DateTimeKind.Unspecified);

    /// <summary>The statement as it read before #8549's change: per (bucket, query_id) sums first, then per bucket.</summary>
    private static readonly string PreviousSlicerTableSql = $$"""
        WITH binned AS (
            SELECT
                query_id,
                date_bin(CAST($5 AS integer) * INTERVAL '1 minute', COALESCE(interval_start_time_utc, collection_time), {{TrendBucketSql.OriginSql}}) AS bucket,
                CAST(avg_cpu_time_us AS double precision) * execution_count AS cpu,
                CAST(avg_duration_us AS double precision) * execution_count AS dur,
                CAST(avg_logical_io_reads AS double precision) * execution_count AS reads,
                CAST(avg_logical_io_writes AS double precision) * execution_count AS writes,
                CAST(avg_physical_io_reads AS double precision) * execution_count AS preads
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   COALESCE(interval_start_time_utc, collection_time) >= $2
            AND   COALESCE(interval_start_time_utc, collection_time) <= $3
            AND   first_execution_time >= $2 - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
            AND   first_execution_time <= $3 + {{QueryStoreIntervalWide.IntervalStartFirstExecMarginSql}}
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ),
        per_query AS (
            SELECT bucket, query_id, SUM(cpu) AS cpu, SUM(dur) AS dur, SUM(reads) AS reads, SUM(writes) AS writes, SUM(preads) AS preads
            FROM binned
            GROUP BY bucket, query_id
        )
        SELECT
            bucket,
            COUNT(query_id) AS query_count,
            COALESCE(SUM(cpu), 0) / 1000.0 AS total_cpu_ms,
            COALESCE(SUM(dur), 0) / 1000.0 AS total_duration_ms,
            COALESCE(SUM(reads), 0) AS total_reads,
            COALESCE(SUM(writes), 0) AS total_writes,
            COALESCE(SUM(preads), 0) AS total_physical_reads
        FROM per_query
        GROUP BY bucket
        ORDER BY bucket
        """;

    private const string SeedSql = """
        CREATE TABLE public.query_store_interval_wide (
            server_id integer NOT NULL, database_name text, query_id bigint,
            interval_start_time_utc timestamp, collection_time timestamp NOT NULL, first_execution_time timestamp,
            execution_count bigint, avg_cpu_time_us bigint, avg_duration_us bigint,
            avg_logical_io_reads bigint, avg_logical_io_writes bigint, avg_physical_io_reads bigint);

        -- 720 hourly intervals of 477 queries, except the hour at 2026-09-20 05:00 that gets only rows with no query_id below.
        -- Every 97th query has no query_id and every 53rd has no interval start (placed by collection time, 3 minutes later in the same hour).
        INSERT INTO public.query_store_interval_wide
        SELECT 1, 'db' || (q % 3), CASE WHEN q % 97 = 0 THEN NULL ELSE q END,
               CASE WHEN q % 53 = 0 THEN NULL ELSE ts END, ts + interval '3 minutes', ts - interval '1 hour',
               1 + (q % 7), 100 + (q % 13), 900 + (q % 31), 10 + (q % 5), q % 3, q % 2
        FROM generate_series(timestamp '2026-09-15 00:00', timestamp '2026-10-15 00:00', interval '1 hour') AS ts
        CROSS JOIN generate_series(1, 477) AS q
        WHERE ts <> timestamp '2026-09-20 05:00';

        -- A query seen again in the same hour (another plan), so one query_id has two rows in a bucket.
        INSERT INTO public.query_store_interval_wide
        SELECT 1, 'db1', q, ts, ts + interval '3 minutes', ts - interval '1 hour', 2, 50, 60, 7, 1, 1
        FROM generate_series(timestamp '2026-09-15 00:00', timestamp '2026-10-15 00:00', interval '1 hour') AS ts
        CROSS JOIN generate_series(1, 5) AS q
        WHERE ts <> timestamp '2026-09-20 05:00';

        -- The hour with no query_id at all: counted as a bucket with zero queries, its work still summed.
        INSERT INTO public.query_store_interval_wide VALUES
            (1, 'db1', NULL, timestamp '2026-09-20 05:00', timestamp '2026-09-20 05:03', timestamp '2026-09-20 04:00', 4, 11, 12, 13, 14, 15),
            (1, 'db1', NULL, NULL, timestamp '2026-09-20 05:30', timestamp '2026-09-20 04:00', 3, 21, 22, 23, 24, 25);

        -- On the window's end, a whole microsecond past it (left out), and on its start.
        INSERT INTO public.query_store_interval_wide VALUES
            (1, 'db1', 90001, timestamp '2026-10-15 00:00', timestamp '2026-10-15 00:03', timestamp '2026-10-14 23:00', 5, 1, 2, 3, 4, 5),
            (1, 'db1', 90002, timestamp '2026-10-15 00:00:00.000001', timestamp '2026-10-15 00:03', timestamp '2026-10-14 23:00', 5, 1, 2, 3, 4, 5),
            (1, 'db1', 90003, timestamp '2026-09-15 00:00', timestamp '2026-09-15 00:03', timestamp '2026-09-14 23:00', 6, 1, 2, 3, 4, 5);

        -- Another server's rows, which the read never sees.
        INSERT INTO public.query_store_interval_wide
        SELECT 2, 'db1', q, ts, ts, ts, 9, 9, 9, 9, 9, 9
        FROM generate_series(timestamp '2026-09-15 00:00', timestamp '2026-09-16 00:00', interval '1 hour') AS ts
        CROSS JOIN generate_series(1, 10) AS q;

        CREATE INDEX ON public.query_store_interval_wide (server_id, (COALESCE(interval_start_time_utc, collection_time)));
        ANALYZE public.query_store_interval_wide;
        """;

    private static NpgsqlCommand Command(NpgsqlConnection connection, string sql)
    {
        var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 1 });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = WindowStart });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = WindowEnd });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 60 });
        return command;
    }

    private static async Task<List<string>> LinesAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var lines = new List<string>();
        await using var command = Command(connection, sql);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            lines.Add(string.Create(CultureInfo.InvariantCulture,
                $"{reader.GetDateTime(0):O}|{reader.GetInt64(1)}|{reader.GetDouble(2):R}|{reader.GetDouble(3):R}|{reader.GetDouble(4):R}|{reader.GetDouble(5):R}|{reader.GetDouble(6):R}"));
        }

        return lines;
    }

    /// <summary>The top plan node's temp blocks written, from EXPLAIN (ANALYZE, BUFFERS), which counts the whole statement.</summary>
    private static async Task<long> TempBlocksWrittenAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var transaction = await connection.BeginTransactionAsync(ct);
        foreach (var setting in new[] { "SET LOCAL work_mem = '8MB'", "SET LOCAL max_parallel_workers_per_gather = 0" })
        {
            await using var set = new NpgsqlCommand(setting, connection, transaction);
            await set.ExecuteNonQueryAsync(ct);
        }

        await using var command = Command(connection, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql);
        command.Transaction = transaction;
        var json = (string?)await command.ExecuteScalarAsync(ct);
        await transaction.RollbackAsync(ct);
        using var document = JsonDocument.Parse(json!);
        return document.RootElement[0].GetProperty("Plan").GetProperty("Temp Written Blocks").GetInt64();
    }

    [Fact]
    public async Task TheSlicerTableStatement_ReturnsThePreviousLines_AndWritesAThirdOfTheTempBlocks()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #8549 slicer shape live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await using (var seed = new NpgsqlCommand(SeedSql, connection) { CommandTimeout = 120 })
            {
                await seed.ExecuteNonQueryAsync(ct);
            }

            var previous = await LinesAsync(connection, PreviousSlicerTableSql, ct);
            var current = await LinesAsync(connection, ViewerDataService.QueryStoreSlicerTableSql, ct);

            Assert.Equal(721, previous.Count);
            Assert.Equal(previous, current);

            /* The hour with no query_id: a bar with no queries and the work of its two rows (4 x 11 + 3 x 21 microseconds, in milliseconds). */
            var noIdHour = Assert.Single(current, l => l.StartsWith("2026-09-20T05:00:00", StringComparison.Ordinal));
            Assert.StartsWith("2026-09-20T05:00:00.0000000|0|0.107|", noIdHour, StringComparison.Ordinal);
            /* The last bar: the 464 queries with an interval start on the window's end and the one row there, but not the row a microsecond past it,
               and not the 9 rows with no interval start (placed by collection time, 3 minutes past the end). */
            Assert.StartsWith("2026-10-15T00:00:00.0000000|465|", current[^1], StringComparison.Ordinal);

            var previousTemp = await TempBlocksWrittenAsync(connection, PreviousSlicerTableSql, ct);
            var currentTemp = await TempBlocksWrittenAsync(connection, ViewerDataService.QueryStoreSlicerTableSql, ct);
            Assert.True(previousTemp > 0, "the reference statement wrote no temp blocks at 8 MB, so this seed no longer shows the spill");
            Assert.True(
                currentTemp * 3 <= previousTemp,
                $"the slicer table statement wrote {currentTemp} temp blocks against the previous statement's {previousTemp}; the bucket sums are carrying the (bucket, query_id) grouping again");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}
