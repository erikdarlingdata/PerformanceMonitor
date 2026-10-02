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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4952's two background indexes on a real store whose <c>collect.query_store_stats</c> is a compressed hypertable:
/// both build valid (the partial one per chunk, a compressed chunk included), a second ensure changes nothing, an
/// INVALID leftover is dropped and rebuilt, the Query Store top reads return identical rows and the probe the same
/// answer with the indexes absent and present (a NULL-interval row included), the probe plans on the partial index
/// once it exists, and the writer's update stays HOT with the btree present.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it, so it cannot race
   live collection. */
public sealed class QueryStoreBackgroundIndexesLiveTests
{
    private const string Raw = "collect.query_store_stats";
    private const string Wide = "collect.query_store_interval_wide";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<NpgsqlConnection> OpenStoreAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB must be available for the #4952 live tests");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await TimescaleSupport.ApplyCompressionPolicyAsync(connection, null, ct);
        return connection;
    }

    private static Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct) =>
        QueryStoreIntervalWideBrinIndexLiveTests.ExecAsync(connection, sql, ct);

    private static Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct) =>
        QueryStoreIntervalWideBrinIndexLiveTests.ScalarAsync(connection, sql, ct);

    private static async Task EnsureAllAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await QueryStoreBackgroundIndexes.EnsureAsync(connection, QueryStoreBackgroundIndexes.WideServerFirstExec, NullLogger.Instance, ct);
        await QueryStoreBackgroundIndexes.EnsureAsync(connection, QueryStoreBackgroundIndexes.LegacyProbe, NullLogger.Instance, ct);
    }

    /* Three UTC days of 10-minute passes for servers 1 and 2 (60 queries a pass), the oldest chunk compressed, and one
       legacy raw row (NULL interval_start_time_utc) for server 2 six hours ago, so the probe's true answer is covered
       beside its false one. The wide table gets one hourly interval per query. */
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, @"
CREATE TEMP TABLE seed_passes AS
SELECT g, (date_trunc('day', timezone('UTC', now())) - interval '2 days') + g * interval '10 minutes' AS t
FROM generate_series(0, (extract(epoch FROM (timezone('UTC', now()) - (date_trunc('day', timezone('UTC', now())) - interval '2 days'))) / 600)::int) g;

INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc,
     first_execution_time, last_execution_time, query_text, execution_count, avg_duration_us, interval_start_time_utc)
SELECT row_number() OVER (ORDER BY p.t, s, q), p.t, s, 'srv' || s, 'db', q, q, 'Regular',
       date_trunc('hour', p.t), p.t, 'select 1', 10 + (p.g % 6), 1000 + q, date_trunc('hour', p.t)
FROM seed_passes p CROSS JOIN generate_series(1, 2) s CROSS JOIN generate_series(1, 60) q
ORDER BY p.t, s, q;

INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc,
     first_execution_time, last_execution_time, query_text, execution_count, avg_duration_us, interval_start_time_utc)
VALUES (-1, timezone('UTC', now()) - interval '6 hours', 2, 'srv2', 'db', 1, 1, 'Regular',
        timezone('UTC', now()) - interval '7 hours', timezone('UTC', now()) - interval '6 hours', 'select 1', 5, 1000, NULL);

INSERT INTO collect.query_store_interval_wide
    (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time,
     last_execution_time, query_text, execution_count, avg_duration_us, runtime_stats_interval_id, interval_start_time_utc)
SELECT LEAST(timezone('UTC', now()), h + interval '1 hour'), s, 'db', q, q, 'Regular', h, LEAST(timezone('UTC', now()), h + interval '1 hour'),
       'select 1', 10 + q, 1000 + q, extract(epoch FROM h)::bigint / 3600, h
FROM generate_series(date_trunc('hour', timezone('UTC', now())) - interval '60 hours', date_trunc('hour', timezone('UTC', now())), interval '1 hour') h
CROSS JOIN generate_series(1, 2) s CROSS JOIN generate_series(1, 60) q;

INSERT INTO collect.query_store_interval_wide
    (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time,
     last_execution_time, query_text, execution_count, avg_duration_us, runtime_stats_interval_id, interval_start_time_utc)
VALUES (timezone('UTC', now()) - interval '6 hours', 2, 'db', 1, 1, 'Regular', timezone('UTC', now()) - interval '7 hours',
        timezone('UTC', now()) - interval '6 hours', 'select 1', 5, 1000, -1, NULL);

SELECT count(compress_chunk(c, if_not_compressed => true))
FROM show_chunks('collect.query_store_stats', older_than => date_trunc('day', timezone('UTC', now())) - interval '1 day') c;
ANALYZE collect.query_store_stats;
ANALYZE collect.query_store_interval_wide;", ct);
    }

    private static NpgsqlParameter Ts(DateTime value) => new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(value, DateTimeKind.Unspecified) };

    private static NpgsqlParameter Text() => new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value };

    private static NpgsqlParameter NullTs() => new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DBNull.Value };

    /* Every row of a result, stringified (a timestamp round-trips to the tick, a NULL reads as NULL) and sorted, so a
       comparison between two runs is exact and order-free. */
    private static async Task<List<string>> RowsAsync(NpgsqlCommand command, CancellationToken ct)
    {
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = new List<string>();
        while (await reader.ReadAsync(ct))
        {
            var values = new object[reader.FieldCount];
            reader.GetValues(values);
            rows.Add(string.Join("|", values.Select(v => v switch
            {
                DBNull => "NULL",
                DateTime stamp => stamp.ToString("O", CultureInfo.InvariantCulture),
                _ => Convert.ToString(v, CultureInfo.InvariantCulture),
            })));
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    /* Every read the two indexes speed up, as the product binds each one, for both servers over a 24 h and a 60 h
       window ending at <paramref name="end"/>, which the caller fixes so two runs read the same windows: the
       legacy-row probe, the MCP table read, the web viewer's table read (once with a closed window end and once with
       the open end a preset window binds as NULL) and the duration trend's table read. Server 2 holds the NULL-start
       rows, so the probe answers true for it and the table reads and the trend's second arm return its legacy row. */
    private static async Task<List<string>> ReadAllAsync(NpgsqlConnection connection, DateTime end, CancellationToken ct)
    {
        var results = new List<string>();
        foreach (var serverId in new[] { 1, 2 })
        {
            foreach (var hours in new[] { 24, 60 })
            {
                var start = end.AddHours(-hours);

                await using (var probe = new NpgsqlCommand(QueryStoreIntervalWide.HasLegacyRowSql, connection))
                {
                    probe.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
                    probe.Parameters.Add(Ts(start));
                    probe.Parameters.Add(Ts(end));
                    results.Add($"probe|server {serverId}|{hours}h|{await probe.ExecuteScalarAsync(ct)}");
                }

                await using (var top = new NpgsqlCommand(DarlingDataReader.QueryStoreTopTableSql, connection))
                {
                    top.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
                    top.Parameters.Add(Ts(start));
                    top.Parameters.Add(Ts(end));
                    top.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 500 });
                    top.Parameters.Add(Text());
                    top.Parameters.Add(Text());
                    top.Parameters.Add(Text());
                    var rows = await RowsAsync(top, ct);
                    results.Add($"mcp top|server {serverId}|{hours}h|{rows.Count} rows");
                    results.AddRange(rows.Select(r => $"  {r}"));
                }

                foreach (var openEnd in new[] { false, true })
                {
                    await using var viewerTop = new NpgsqlCommand(ViewerDataService.QueryStoreTopTableSql, connection);
                    viewerTop.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
                    viewerTop.Parameters.Add(Ts(start));
                    viewerTop.Parameters.Add(openEnd ? NullTs() : Ts(end));
                    viewerTop.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 500 });
                    viewerTop.Parameters.Add(ViewerDataService.DatabaseFilterParameter(null));
                    var rows = await RowsAsync(viewerTop, ct);
                    results.Add($"viewer top|server {serverId}|{hours}h|{(openEnd ? "open end" : "closed end")}|{rows.Count} rows");
                    results.AddRange(rows.Select(r => $"  {r}"));
                }

                await using (var trend = new NpgsqlCommand(ViewerDataService.QueryStoreDurationTrendTableSql, connection))
                {
                    trend.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
                    trend.Parameters.Add(Ts(start));
                    trend.Parameters.Add(Ts(end));
                    trend.Parameters.Add(Ts(end));
                    trend.Parameters.Add(ViewerDataService.DatabaseFilterParameter(null));
                    var rows = await RowsAsync(trend, ct);
                    results.Add($"trend|server {serverId}|{hours}h|{rows.Count} rows");
                    results.AddRange(rows.Select(r => $"  {r}"));
                }
            }
        }

        return results;
    }

    [Fact]
    public async Task TheReads_AreIdenticalWithTheIndexesAbsentAndPresent_AndTheProbePlansOnThePartialIndex()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4952 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await SeedAsync(connection, ct);

        Assert.Null(await ScalarAsync(connection, $"SELECT to_regclass('{QueryStoreBackgroundIndexes.LegacyProbeIndexName}')::text", ct) is string a ? a : null);
        Assert.Null(await ScalarAsync(connection, $"SELECT to_regclass('{QueryStoreBackgroundIndexes.WideServerFirstExecIndexName}')::text", ct) is string b ? b : null);
        var end = DateTime.UtcNow;
        var absent = await ReadAllAsync(connection, end, ct);

        /* Non-vacuous: the probe is false for server 1 and true for server 2 (its legacy row), and the top read returns rows. */
        Assert.Contains("probe|server 1|24h|False", absent);
        Assert.Contains("probe|server 2|24h|True", absent);
        Assert.Contains("probe|server 2|60h|True", absent);
        Assert.DoesNotContain("mcp top|server 1|24h|0 rows", absent);
        Assert.DoesNotContain("mcp top|server 2|60h|0 rows", absent);
        Assert.DoesNotContain("viewer top|server 1|24h|closed end|0 rows", absent);
        Assert.DoesNotContain("viewer top|server 2|60h|open end|0 rows", absent);
        Assert.DoesNotContain("trend|server 1|24h|0 rows", absent);
        Assert.DoesNotContain("trend|server 2|60h|0 rows", absent);

        /* The NULL-start row of server 2 is in what the trend read returns (its second arm), so that arm is compared too. */
        var legacyPoint = ((DateTime)(await ScalarAsync(connection, $"SELECT collection_time FROM {Wide} WHERE interval_start_time_utc IS NULL", ct))!).ToString("O", CultureInfo.InvariantCulture);
        Assert.Contains(absent, line => line.TrimStart().StartsWith(legacyPoint, StringComparison.Ordinal));

        var planBefore = (string)(await ScalarAsync(connection, ProbePlanSql(), ct))!;
        Assert.DoesNotContain("ix_query_store_stats_legacy_server_time", planBefore);

        await EnsureAllAsync(connection, ct);
        await ExecAsync(connection, $"ANALYZE {Raw}", ct);
        await ExecAsync(connection, $"ANALYZE {Wide}", ct);
        var present = await ReadAllAsync(connection, end, ct);

        Assert.Equal(absent, present);

        var planAfter = (string)(await ScalarAsync(connection, ProbePlanSql(), ct))!;
        Assert.Contains("ix_query_store_stats_legacy_server_time", planAfter);
    }

    /* The probe for server 1 over the last 24 h, as a literal statement so EXPLAIN needs no parameters. */
    private static string ProbePlanSql()
    {
        var end = DateTime.UtcNow;
        string Literal(DateTime value) => string.Create(CultureInfo.InvariantCulture, $"timestamp '{value:yyyy-MM-dd HH:mm:ss}'");
        return $@"EXPLAIN (FORMAT JSON) SELECT EXISTS (SELECT 1 FROM {Raw} AS s WHERE s.server_id = 1
AND s.collection_time >= {Literal(end.AddHours(-24))} AND s.collection_time <= {Literal(end)} AND s.interval_start_time_utc IS NULL)";
    }

    [Fact]
    public async Task Ensure_BuildsBothIndexesValid_PerChunkIncludingACompressedChunk_AndASecondEnsureChangesNothing()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4952 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await SeedAsync(connection, ct);
        await EnsureAllAsync(connection, ct);

        foreach (var name in new[] { QueryStoreBackgroundIndexes.LegacyProbeIndexName, QueryStoreBackgroundIndexes.WideServerFirstExecIndexName })
        {
            Assert.True((bool)(await ScalarAsync(connection, $"SELECT indisvalid FROM pg_index WHERE indexrelid = '{name}'::regclass", ct))!, name);
        }

        var probeDefinition = (string)(await ScalarAsync(connection, $"SELECT pg_get_indexdef('{QueryStoreBackgroundIndexes.LegacyProbeIndexName}'::regclass)", ct))!;
        Assert.Contains("(server_id, collection_time)", probeDefinition);
        Assert.Contains("WHERE (interval_start_time_utc IS NULL)", probeDefinition);
        var wideDefinition = (string)(await ScalarAsync(connection, $"SELECT pg_get_indexdef('{QueryStoreBackgroundIndexes.WideServerFirstExecIndexName}'::regclass)", ct))!;
        Assert.Contains("USING btree (server_id, first_execution_time)", wideDefinition);

        /* Every chunk has the partial index and it is valid, the compressed chunk (an empty shell) included. */
        await using (var chunks = new NpgsqlCommand(@"
SELECT count(*), count(*) FILTER (WHERE c.is_compressed), count(*) FILTER (WHERE ci.indisvalid)
FROM timescaledb_information.chunks c
LEFT JOIN LATERAL (
    SELECT i.indisvalid FROM pg_index i
    WHERE i.indrelid = format('%I.%I', c.chunk_schema, c.chunk_name)::regclass AND i.indpred IS NOT NULL
      AND pg_get_indexdef(i.indexrelid) LIKE '%interval_start_time_utc IS NULL%'
    LIMIT 1) ci ON true
WHERE c.hypertable_schema = 'collect' AND c.hypertable_name = 'query_store_stats'", connection))
        await using (var reader = await chunks.ExecuteReaderAsync(ct))
        {
            Assert.True(await reader.ReadAsync(ct));
            Assert.True(reader.GetInt64(0) >= 3, "three days of data make at least three chunks");
            Assert.True(reader.GetInt64(1) >= 1, "the oldest chunk is compressed");
            Assert.Equal(reader.GetInt64(0), reader.GetInt64(2));
        }

        var probeOid = await ScalarAsync(connection, $"SELECT '{QueryStoreBackgroundIndexes.LegacyProbeIndexName}'::regclass::oid", ct);
        var wideOid = await ScalarAsync(connection, $"SELECT '{QueryStoreBackgroundIndexes.WideServerFirstExecIndexName}'::regclass::oid", ct);
        await EnsureAllAsync(connection, ct);
        Assert.Equal(probeOid, await ScalarAsync(connection, $"SELECT '{QueryStoreBackgroundIndexes.LegacyProbeIndexName}'::regclass::oid", ct));
        Assert.Equal(wideOid, await ScalarAsync(connection, $"SELECT '{QueryStoreBackgroundIndexes.WideServerFirstExecIndexName}'::regclass::oid", ct));
    }

    [Fact]
    public async Task ThePartialIndex_IsDeferredWhileTheNewestChunkIsAboveTheLimit_AndBuiltOnceItIsAtIt()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4952 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await SeedAsync(connection, ct);

        /* The newest chunk is the one with the latest range end; its heap is the main fork's size. */
        var newestBytes = (long)(await ScalarAsync(connection, @"
SELECT pg_relation_size(format('%I.%I', chunk_schema, chunk_name)::regclass)
FROM timescaledb_information.chunks
WHERE hypertable_schema = 'collect' AND hypertable_name = 'query_store_stats'
ORDER BY range_end DESC
LIMIT 1", ct))!;
        Assert.True(newestBytes > 0, "the newest seeded chunk holds today's rows");

        /* One byte under the chunk's real size: deferred, nothing built, a retry asked for, one Information line. */
        var oneByteTooSmall = QueryStoreBackgroundIndexes.LegacyProbe with { MaxNewestChunkBytes = newestBytes - 1 };
        var logger = new CapturingTestLogger();
        Assert.Equal(
            QueryStoreBackgroundIndexes.EnsureOutcome.RetryLater,
            await QueryStoreBackgroundIndexes.EnsureAsync(connection, oneByteTooSmall, logger, ct));
        Assert.Null(await ScalarAsync(connection, $"SELECT to_regclass('{QueryStoreBackgroundIndexes.LegacyProbeIndexName}')::text", ct) is string built ? built : null);
        Assert.Equal(1, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Information));
        Assert.Contains("newest chunk", logger.Joined, StringComparison.Ordinal);

        /* The same deferral on a retry is quiet: still no index, still a retry asked for, no second Information line. */
        Assert.Equal(
            QueryStoreBackgroundIndexes.EnsureOutcome.RetryLater,
            await QueryStoreBackgroundIndexes.EnsureAsync(connection, oneByteTooSmall, logger, ct, isRetry: true));
        Assert.Equal(1, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Information));

        /* A deferred attempt leaves no half-built index behind and takes no per-chunk index. */
        Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM pg_indexes WHERE indexname LIKE '%legacy_server_time%'", ct));

        /* Exactly the chunk's size: at the limit builds, valid. */
        var atTheLimit = QueryStoreBackgroundIndexes.LegacyProbe with { MaxNewestChunkBytes = newestBytes };
        Assert.Equal(
            QueryStoreBackgroundIndexes.EnsureOutcome.Settled,
            await QueryStoreBackgroundIndexes.EnsureAsync(connection, atTheLimit, NullLogger.Instance, ct));
        Assert.True((bool)(await ScalarAsync(connection, $"SELECT indisvalid FROM pg_index WHERE indexrelid = '{QueryStoreBackgroundIndexes.LegacyProbeIndexName}'::regclass", ct))!);

        /* Built and valid: a later ensure with a limit it would fail (a retry after the build) changes nothing. */
        Assert.Equal(
            QueryStoreBackgroundIndexes.EnsureOutcome.Settled,
            await QueryStoreBackgroundIndexes.EnsureAsync(connection, oneByteTooSmall, NullLogger.Instance, ct, isRetry: true));
    }

    [Fact]
    public async Task AnInvalidLeftover_IsDroppedAndRebuilt_ForBothIndexes()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4952 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await EnsureAllAsync(connection, ct);

        var oldOids = new Dictionary<string, object?>();
        foreach (var name in new[] { QueryStoreBackgroundIndexes.LegacyProbeIndexName, QueryStoreBackgroundIndexes.WideServerFirstExecIndexName })
        {
            oldOids[name] = await ScalarAsync(connection, $"SELECT '{name}'::regclass::oid", ct);

            /* An interrupted build leaves exactly this catalog state; the test roles are superuser, so flipping
               indisvalid reproduces it deterministically. */
            await ExecAsync(connection, $"UPDATE pg_index SET indisvalid = false WHERE indexrelid = '{name}'::regclass", ct);
            Assert.False((bool)(await ScalarAsync(connection, $"SELECT indisvalid FROM pg_index WHERE indexrelid = '{name}'::regclass", ct))!);
        }

        await EnsureAllAsync(connection, ct);

        foreach (var name in new[] { QueryStoreBackgroundIndexes.LegacyProbeIndexName, QueryStoreBackgroundIndexes.WideServerFirstExecIndexName })
        {
            Assert.True((bool)(await ScalarAsync(connection, $"SELECT indisvalid FROM pg_index WHERE indexrelid = '{name}'::regclass", ct))!, name);
            Assert.NotEqual(oldOids[name], await ScalarAsync(connection, $"SELECT '{name}'::regclass::oid", ct));
        }
    }

    [Fact]
    public async Task TheUpsertsWholeSetList_StaysHotWithTheWideBtreePresent_AndNotWithABtreeOnASetColumn()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4952 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await SeedAsync(connection, ct);
        await EnsureAllAsync(connection, ct);

        /* The claim is about the wide btree being present, so prove it is: with the ensure taken out the update below
           stays heap-only trivially, and this test passed without the index until it asserted it. */
        Assert.True((bool)(await ScalarAsync(connection, $"SELECT indisvalid FROM pg_index WHERE indexrelid = '{QueryStoreBackgroundIndexes.WideServerFirstExecIndexName}'::regclass", ct))!);

        /* Exactly the columns the writer's ON CONFLICT ... DO UPDATE sets, each given a changed value by its type. */
        var setColumns = QueryStoreIntervalWideBrinIndexLiveTests.UpsertSetColumns();
        var types = new Dictionary<string, string>(StringComparer.Ordinal);
        await using (var typeCommand = new NpgsqlCommand(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = 'query_store_interval_wide'", connection))
        await using (var reader = await typeCommand.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                types[reader.GetString(0)] = reader.GetString(1);
            }
        }

        var assignments = setColumns.OrderBy(c => c, StringComparer.Ordinal).Select(column => types[column] switch
        {
            "timestamp without time zone" => $"{column} = {column} + interval '1 minute'",
            "boolean" => $"{column} = NOT COALESCE({column}, false)",
            "text" => $"{column} = COALESCE({column}, '') || 'x'",
            _ => $"{column} = COALESCE({column}, 0) + 1",
        });
        var setList = string.Join(", ", assignments);

        var (updated, hot) = await QueryStoreIntervalWideBrinIndexLiveTests.UpdateAndReadHotAsync(connection, setList, "server_id = 1 AND query_id = 1", ct);
        Assert.True(updated >= 20, $"the update must touch rows; touched {updated}");
        Assert.Equal(updated, hot);

        /* The control: a btree on a SET column makes the same update non-HOT, so the counters can fail. */
        await ExecAsync(connection, $"CREATE INDEX ix_btree_probe ON {Wide} (execution_count)", ct);
        var (controlUpdated, controlHot) = await QueryStoreIntervalWideBrinIndexLiveTests.UpdateAndReadHotAsync(connection, setList, "server_id = 1 AND query_id = 1", ct);
        Assert.Equal(updated, controlUpdated);
        Assert.Equal(0, controlHot);
    }
}
