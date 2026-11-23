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
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.QueryStoreIntervalRetentionTestKit;

namespace Darling.Tests;

/// <summary>
/// #8549: the Query Store by Duration grid fetches the rows of the first <c>top + 5</c> kept keys and asks for more only when the text
/// trim leaves the page short. Fetching all 250 kept keys read 250 keys' heap pages (one Bitmap Heap Scan per key and day leaf) for a page
/// of 50. The tests run on a store with a day leaf for each of 30 days and the background indexes, and check that the page is the rows of a
/// fetch over every kept key (the old fetch, kept here as the oracle), on keys with rows in many leaves, a NULL replica role and query hash,
/// and a NULL database and query id, with WAITFOR statements at the top of the ranking that the tail trims; and that the plan of the first
/// round reads a fraction of the data pages a fetch over every kept key reads.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The class reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class QueryStoreGridKeyFetchLiveTests
{
    private static readonly string[] Metrics = (
        "avg_duration_us min_duration_us max_duration_us avg_cpu_time_us min_cpu_time_us max_cpu_time_us avg_logical_io_reads min_logical_io_reads max_logical_io_reads "
        + "avg_logical_io_writes min_logical_io_writes max_logical_io_writes avg_physical_io_reads min_physical_io_reads max_physical_io_reads avg_clr_time_us min_clr_time_us max_clr_time_us "
        + "min_dop max_dop avg_query_max_used_memory min_query_max_used_memory max_query_max_used_memory avg_rowcount min_rowcount max_rowcount "
        + "avg_num_physical_io_reads min_num_physical_io_reads max_num_physical_io_reads avg_log_bytes_used min_log_bytes_used max_log_bytes_used "
        + "avg_tempdb_space_used min_tempdb_space_used max_tempdb_space_used").Split(' ');

    private const int WaitforKeys = 12;

    /// <summary>
    /// A migrated store with the background indexes and a day leaf for each of the 30 days before <see cref="Now"/> (the retention horizon
    /// makes only the last ten itself), and <paramref name="keys"/> keys plus (when <paramref name="nullKey"/> is set) one NULL-identity key, each with a row on every other day. Keys 1 to
    /// 300 are the heavy ones, ranked by their number (key 1 first); key 0 has NULL database and query id and ranks as key 20 does. Every
    /// 50th key has replica role 'Primary', the rest NULL; every 7th key has a NULL query hash. When <paramref name="waitfor"/> is set the first
    /// <see cref="WaitforKeys"/> keys carry WAITFOR text.
    /// </summary>
    private static async Task<NpgsqlConnection> OpenSeededStoreAsync(ScratchPostgres scratch, int keys, bool waitfor, bool nullKey, CancellationToken ct)
    {
        var logger = new ListLogger();
        var connection = await OpenStoreAsync(scratch, ct);
        await PromoteAsync(connection, Wide, Now.AddDays(-36), logger, ct);
        await QueryStoreBackgroundIndexesLiveTests.EnsureAllAsync(connection, ct);
        await QueryStoreIntervalPartitions.CreateAheadAsync(connection, Wide, Now, logger, ct);
        for (var d = 30; d >= 11; d--)
        {
            await ExecAsync(connection, QueryStoreIntervalPartitions.CreateDaySql(Wide, Now.Date.AddDays(-d)), ct);
        }

        var metricList = string.Join(", ", Metrics);
        var metricValues = string.Join(", ", Metrics.Select((m, i) => m.StartsWith("avg_duration", StringComparison.Ordinal)
            ? "CASE WHEN r.g <= 300 THEN 5000000 + (301 - CASE WHEN r.g = 0 THEN 20 ELSE r.g END) * 100000 + r.n ELSE 100 + (r.g % 997) END"
            : m.StartsWith("avg", StringComparison.Ordinal) ? $"((r.g * 7919 + r.n * 31 + {i}) % 9000 + 1)::bigint"
            : m.StartsWith("min", StringComparison.Ordinal) ? $"((r.g + {i}) % 50)::bigint" : $"((r.g + {i}) % 5000 + 100)::bigint"));
        var day0 = Now.Date.AddDays(-29);
        await ExecAsync(connection, $@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time, query_hash,
 execution_count, {metricList}, plan_type, is_forced_plan, query_plan_hash, replica_role, runtime_stats_interval_id, interval_start_time_utc)
SELECT t + interval '30 minutes', 1, CASE WHEN r.g = 0 THEN NULL ELSE 'db' || (r.g % 2) END, CASE WHEN r.g = 0 THEN NULL ELSE r.g END,
       CASE WHEN r.g = 0 THEN 7 ELSE r.g END, 'Regular', t, t, CASE WHEN r.g % 7 = 0 THEN NULL ELSE '0x' || md5(r.g::text) END,
       CASE WHEN r.g <= 300 THEN 100 ELSE 1 + (r.g * 13 + r.n) % 400 END, {metricValues}, 'Query', false, '0x' || md5((r.g + 1)::text),
       CASE WHEN r.g > 0 AND r.g % 50 = 0 THEN 'Primary' END, r.g * 100 + r.n, date_trunc('hour', t)
FROM (SELECT g, n, timestamp '{day0:yyyy-MM-dd}' + (n * interval '1 day') + (((g * 37 + n * 101) % 86000) * interval '1 second') AS t
      FROM generate_series({(nullKey ? 0 : 1)}, {keys}) AS g CROSS JOIN generate_series(0, 29) AS n
      WHERE (g * 31 + n * 17) % 2 = 0 ORDER BY random()) AS r;
{(waitfor ? $@"INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, last_seen)
SELECT 1, 'db' || (g % 2), g, 'WAITFOR DELAY ''00:00:30''', timestamp '{Now:yyyy-MM-dd}' FROM generate_series(1, {WaitforKeys}) AS g;" : "")}
ANALYZE collect.query_store_interval_wide;", ct);
        return connection;
    }

    /// <summary>Every simple property of a row, by name, as one string, so two pages compare cell by cell.</summary>
    private static string Snapshot(ViewerQueryStoreRow row) => string.Join("|", typeof(ViewerQueryStoreRow).GetProperties()
        .Where(p => p.CanRead && p.GetIndexParameters().Length == 0 && (p.PropertyType.IsPrimitive || p.PropertyType == typeof(string) || p.PropertyType == typeof(DateTime)
            || p.PropertyType == typeof(DateTime?) || p.PropertyType == typeof(decimal) || Nullable.GetUnderlyingType(p.PropertyType)?.IsPrimitive == true))
        .Select(p => p.Name + "=" + Convert.ToString(p.GetValue(row), System.Globalization.CultureInfo.InvariantCulture)));

    private static readonly DateTime ReadStart = DateTime.SpecifyKind(Now.AddDays(-30), DateTimeKind.Unspecified);

    private static void AddShared(NpgsqlCommand command, int top)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 1 });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = ReadStart });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = top });
        command.Parameters.Add(ViewerDataService.DatabaseFilterParameter(null));
    }

    /// <summary>The keys statement as the grid runs it, in the grid's own order, at the final candidate limit.</summary>
    private static async Task<ViewerDataService.TableReadKeys> ReadKeysAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int top, CancellationToken ct)
    {
        var keys = new ViewerDataService.TableReadKeys();
        await using var command = new NpgsqlCommand(ViewerDataService.QueryStoreTopTableKeysSql, connection, transaction) { CommandTimeout = 300 };
        AddShared(command, top);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TopFill.FinalCandidates(top) });
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            keys.Add(reader);
        }

        return keys;
    }

    /// <summary>The per-key fetch's plan settings, as <c>SetPerKeyFetchPlanAsync</c> sets them.</summary>
    private static Task PerKeyFetchPlanAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken ct) =>
        new NpgsqlCommand("SET LOCAL parallel_setup_cost TO DEFAULT; SET LOCAL parallel_tuple_cost TO DEFAULT; SET LOCAL enable_indexscan TO off", connection, transaction)
            .ExecuteNonQueryAsync(ct);

    [Fact]
    public async Task TheGridFetch_ReturnsTheRowsOfAFetchOverEveryKeptKey_AndAsksForMoreKeysOnlyWhenTheTrimLeavesThePageShort()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenSeededStoreAsync(scratch, keys: 600, waitfor: true, nullKey: true, ct);

            /* top 50: the first round's 55 keys hold 12 WAITFOR keys, so 43 rows is short and the next round asks for 220 keys, which fills the page.
               top 1: the first round's 6 keys are all WAITFOR keys (an empty page), the next round's 24 fill it. */
            foreach (var (top, expectedRounds) in new[] { (50, new[] { 55, 220 }), (1, new[] { 6, 24 }) })
            {
                await using var transaction = await connection.BeginTransactionAsync(ct);
                var keys = await ReadKeysAsync(connection, transaction, top, ct);
                Assert.Equal(TopFill.FinalCandidates(top), keys.Count);
                await PerKeyFetchPlanAsync(connection, transaction, ct);

                var (rows, keysPerRound) = await ViewerDataService.ReadTablePageByKeysAsync(
                    connection, transaction, ViewerDataService.QueryStoreTopTableByKeysSql, 1, ReadStart, null, top, null, keys, ct);

                /* The oracle: the fetch the grid ran before #8549, the rows of every kept key in one statement. */
                List<ViewerQueryStoreRow> expected;
                await using (var oracle = new NpgsqlCommand(ViewerDataService.QueryStoreTopTableByKeysSql, connection, transaction) { CommandTimeout = 300 })
                {
                    AddShared(oracle, top);
                    keys.AddParameters(oracle);
                    keys.AddFilterParameters(oracle);
                    await using var reader = await oracle.ExecuteReaderAsync(ct);
                    (expected, _) = await TopFill.ReadPageAsync(reader, ViewerDataService.QueryStorePageOrdinal, ViewerDataService.ReadQueryStoreTopRow, ct);
                }

                Assert.Equal(expectedRounds, keysPerRound);
                Assert.Equal(top, expected.Count);
                Assert.Equal(expected.Select(Snapshot), rows.Select(Snapshot));
                Assert.DoesNotContain(rows, r => r.QueryId is >= 1 and <= WaitforKeys && r.DatabaseName.StartsWith("db", StringComparison.Ordinal));

                if (top == 50)
                {
                    /* The seed does stress the match: the NULL-identity key and a Primary key are on the page, and so is a key with a NULL query hash. */
                    Assert.Contains(rows, r => r.QueryId == 0 && r.DatabaseName == "");
                    Assert.Contains(rows, r => r.QueryId == 50);
                    Assert.Contains(rows, r => r.QueryId == 14 && r.QueryHash == "");
                }

                await transaction.RollbackAsync(ct);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheGridFetch_WhenThePageFillsInTheFirstRound_ReadsAFractionOfTheDataPagesAFetchOverEveryKeptKeyReads()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenSeededStoreAsync(scratch, keys: 20000, waitfor: false, nullKey: false, ct);
            const int top = 50;
            await using var transaction = await connection.BeginTransactionAsync(ct);
            var keys = await ReadKeysAsync(connection, transaction, top, ct);
            Assert.Equal(TopFill.FinalCandidates(top), keys.Count);
            await PerKeyFetchPlanAsync(connection, transaction, ct);

            var (rows, keysPerRound) = await ViewerDataService.ReadTablePageByKeysAsync(
                connection, transaction, ViewerDataService.QueryStoreTopTableByKeysSql, 1, ReadStart, null, top, null, keys, ct);
            Assert.Equal(top, rows.Count);
            var firstRound = Assert.Single(keysPerRound);
            Assert.True(firstRound < keys.Count, $"the page filled from {firstRound} of {keys.Count} kept keys");

            async Task<long> PagesAsync(ViewerDataService.TableReadKeys fetched)
            {
                await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + ViewerDataService.QueryStoreTopTableByKeysSql, connection, transaction) { CommandTimeout = 300 };
                AddShared(explain, top);
                fetched.AddParameters(explain);
                fetched.AddFilterParameters(explain);
                return ExplainBuffers.DataPages(await explain.ExecuteScalarAsync(ct));
            }

            var allKeys = await PagesAsync(keys);
            var roundOne = await PagesAsync(keys.First(firstRound));
            Assert.True(roundOne * 2 < allKeys, $"the first round read {roundOne} data pages against {allKeys} for every kept key");

            await transaction.RollbackAsync(ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
