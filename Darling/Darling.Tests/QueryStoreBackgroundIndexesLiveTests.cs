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
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The background Query Store indexes on a real store whose <c>collect.query_store_stats</c> is a hypertable: the
/// BRIN and #4952's btree both build valid, a second ensure changes nothing, an INVALID leftover is dropped and
/// rebuilt, the Query Store top reads and the duration trend's table read return identical rows with the indexes
/// absent and present (a NULL-start row included), the writer's update stays HOT with both present, and a spec aimed
/// at the hypertable is skipped with a warning and builds nothing.
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
        return connection;
    }

    private static Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct) =>
        QueryStoreIntervalWideBrinIndexLiveTests.ExecAsync(connection, sql, ct);

    private static Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct) =>
        QueryStoreIntervalWideBrinIndexLiveTests.ScalarAsync(connection, sql, ct);

    /* Both background indexes, in the order the worker's delayed task ensures them: the BRIN, then the btree. Shared with
       the HOT census in QueryStoreIntervalWideBrinIndexLiveTests, which must see every spec in All. */
    internal static async Task EnsureAllAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var spec in QueryStoreBackgroundIndexes.All)
        {
            await QueryStoreBackgroundIndexes.EnsureAsync(connection, spec, Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, ct);
        }
    }

    private static async Task<bool> IndexExistsAsync(NpgsqlConnection connection, string indexName, CancellationToken ct) =>
        (bool)(await ScalarAsync(connection, $"SELECT to_regclass('{indexName}') IS NOT NULL", ct))!;

    private static async Task<bool> IndexIsValidAsync(NpgsqlConnection connection, string indexName, CancellationToken ct) =>
        (bool)(await ScalarAsync(connection, $"SELECT indisvalid FROM pg_index WHERE indexrelid = '{indexName}'::regclass", ct))!;

    /* One hourly interval per query for servers 1 and 2 (60 queries each) in the wide table, up to the current hour,
       and one NULL-start row (runtime_stats_interval_id -1, the legacy shape) for server 2 six hours ago, so the
       duration trend's second arm returns a row beside its first. */
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, @"
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

    /* Every read the indexes speed up, as the product binds each one, for both servers over a 24 h and a 60 h window
       ending at <paramref name="end"/>, which the caller fixes so two runs read the same windows: the MCP table read,
       the web viewer's table read (once with a closed window end and once with the open end a preset window binds as
       NULL) and the duration trend's table read. Server 2 holds the NULL-start row, so the trend read's second arm
       returns it. */
    private static async Task<List<string>> ReadAllAsync(NpgsqlConnection connection, DateTime end, CancellationToken ct)
    {
        var results = new List<string>();
        foreach (var serverId in new[] { 1, 2 })
        {
            foreach (var hours in new[] { 24, 60 })
            {
                var start = end.AddHours(-hours);

                await using (var top = new NpgsqlCommand(DarlingDataReader.QueryStoreTopTableSql, connection))
                {
                    top.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
                    top.Parameters.Add(Ts(start));
                    top.Parameters.Add(Ts(end));
                    top.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 500 });
                    top.Parameters.Add(Text());
                    top.Parameters.Add(Text());
                    top.Parameters.Add(Text());
                    top.Parameters.Add(new NpgsqlParameter<int> { TypedValue = PerformanceMonitor.Darling.Storage.TopFill.FirstCandidates(500) });  /* #5313: the round's candidate limit, bound last */
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
                    viewerTop.Parameters.Add(new NpgsqlParameter<int> { TypedValue = PerformanceMonitor.Darling.Storage.TopFill.FirstCandidates(500) });  /* #5313: the round's candidate limit, bound last */
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
    public async Task TheReads_AreIdenticalWithTheIndexesAbsentAndPresent()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4952 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await SeedAsync(connection, ct);

        Assert.False(await IndexExistsAsync(connection, QueryStoreIntervalWideBrinIndex.IndexName, ct));
        Assert.False(await IndexExistsAsync(connection, QueryStoreBackgroundIndexes.WideServerFirstExecIndexName, ct));
        var end = DateTime.UtcNow;
        var absent = await ReadAllAsync(connection, end, ct);

        /* Non-vacuous: every read returns rows. */
        Assert.DoesNotContain("mcp top|server 1|24h|0 rows", absent);
        Assert.DoesNotContain("mcp top|server 2|60h|0 rows", absent);
        Assert.DoesNotContain("viewer top|server 1|24h|closed end|0 rows", absent);
        Assert.DoesNotContain("viewer top|server 2|60h|open end|0 rows", absent);
        Assert.DoesNotContain("trend|server 1|24h|0 rows", absent);
        Assert.DoesNotContain("trend|server 2|60h|0 rows", absent);

        /* The NULL-start row of server 2 is in what the trend read returns (its second arm), so that arm is compared too. */
        var legacyPoint = ((DateTime)(await ScalarAsync(connection, $"SELECT collection_time FROM {Wide} WHERE interval_start_time_utc IS NULL", ct))!).ToString("O", CultureInfo.InvariantCulture);
        Assert.Contains(absent, line => line.TrimStart().StartsWith(legacyPoint, StringComparison.Ordinal));

        await EnsureAllAsync(connection, ct);
        await ExecAsync(connection, $"ANALYZE {Wide}", ct);
        var present = await ReadAllAsync(connection, end, ct);

        Assert.Equal(absent, present);
    }

    [Fact]
    public async Task Ensure_BuildsTheBtreeAndTheBrinValid_AndASecondEnsureChangesNothing()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4952 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await SeedAsync(connection, ct);
        await EnsureAllAsync(connection, ct);

        var names = new[] { QueryStoreIntervalWideBrinIndex.IndexName, QueryStoreBackgroundIndexes.WideServerFirstExecIndexName };
        foreach (var name in names)
        {
            Assert.True(await IndexIsValidAsync(connection, name, ct), name);
        }

        var wideDefinition = (string)(await ScalarAsync(connection, $"SELECT pg_get_indexdef('{QueryStoreBackgroundIndexes.WideServerFirstExecIndexName}'::regclass)", ct))!;
        Assert.Contains("USING btree (server_id, first_execution_time)", wideDefinition);
        var brinDefinition = (string)(await ScalarAsync(connection, $"SELECT pg_get_indexdef('{QueryStoreIntervalWideBrinIndex.IndexName}'::regclass)", ct))!;
        Assert.Contains("USING brin (collection_time)", brinDefinition);

        var oids = new List<object?>();
        foreach (var name in names)
        {
            oids.Add(await ScalarAsync(connection, $"SELECT '{name}'::regclass::oid", ct));
        }

        await EnsureAllAsync(connection, ct);
        for (var i = 0; i < names.Length; i++)
        {
            Assert.Equal(oids[i], await ScalarAsync(connection, $"SELECT '{names[i]}'::regclass::oid", ct));
        }
    }

    /* The Custom Views fleet loop resolves each server's read source in turn, and for a window of 12 hours or more on this
       plain table each resolve runs PlainTableFloorSql: MIN(first_execution_time) for one server. With the wide btree the
       planner answers it through its min/max path, a Limit over an ordered scan of that btree under an index condition on
       server_id, instead of a pass over the table. The older single-column first_execution_time index also supports a
       min/max path (it walks entries of every server until one matches), so a plan that merely shows a Limit over an
       index scan would pass without the wide btree: the pin names the index. */
    [Fact]
    public async Task ThePerServerTableFloorRead_IsAnsweredByTheWideBtree_NotByAScan()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4952 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await SeedAsync(connection, ct);
        await EnsureAllAsync(connection, ct);

        /* VACUUM as well as ANALYZE, as a store's autovacuum does: it sets the visibility map, which lets the wide btree
           (it holds both columns the statement touches) answer with an index-only scan. On a table whose pages are not yet
           marked all-visible the planner prices that scan's heap fetches at random cost and takes the older index instead,
           whose order follows the heap. */
        await ExecAsync(connection, "VACUUM (ANALYZE) collect.query_store_interval_wide", ct);

        /* The premise: two servers, and a table of many pages, so a pass over the table is a real alternative. */
        Assert.Equal(2L, await ScalarAsync(connection, "SELECT count(DISTINCT server_id) FROM collect.query_store_interval_wide", ct));
        var pages = (int)(await ScalarAsync(connection, "SELECT relpages FROM pg_class WHERE oid = 'collect.query_store_interval_wide'::regclass", ct))!;
        Assert.True(pages >= 20, $"the table spans {pages} pages; the pin needs a table whose pass would cost more than an index probe");

        var plan = await ExplainPlainTableFloorAsync(connection, 1, ct);

        var wideName = QueryStoreBackgroundIndexes.WideServerFirstExecIndexName[(QueryStoreBackgroundIndexes.WideServerFirstExecIndexName.IndexOf('.', StringComparison.Ordinal) + 1)..];
        Assert.DoesNotContain("Seq Scan", plan.NodeTypes);
        Assert.Contains("Limit", plan.NodeTypes);
        Assert.Equal(new[] { wideName }, plan.IndexScansUnderLimit.Select(scan => scan.Index).Distinct().ToArray());
        Assert.All(plan.IndexScansUnderLimit, scan => Assert.Contains("server_id", scan.Cond, StringComparison.Ordinal));
    }

    /* EXPLAIN of the product's own statement, its server bound as the product binds it: an integer parameter. */
    private static async Task<(HashSet<string> NodeTypes, List<(string Index, string Cond)> IndexScansUnderLimit)> ExplainPlainTableFloorAsync(
        NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        await using var explain = new NpgsqlCommand("EXPLAIN (FORMAT JSON) " + QueryStoreIntervalWide.PlainTableFloorSql, connection);
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        var json = (string)(await explain.ExecuteScalarAsync(ct))!;

        var nodeTypes = new HashSet<string>(StringComparer.Ordinal);
        var underLimit = new List<(string Index, string Cond)>();

        void Walk(JsonElement element, bool limited)
        {
            if (element.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in element.EnumerateArray())
                {
                    Walk(item, limited);
                }

                return;
            }

            if (element.ValueKind != JsonValueKind.Object)
            {
                return;
            }

            var isLimit = false;
            if (element.TryGetProperty("Node Type", out var type))
            {
                nodeTypes.Add(type.GetString()!);
                isLimit = type.GetString() == "Limit";
            }

            if (limited && element.TryGetProperty("Index Name", out var index))
            {
                underLimit.Add((index.GetString()!, element.TryGetProperty("Index Cond", out var cond) ? cond.GetString()! : string.Empty));
            }

            foreach (var property in element.EnumerateObject())
            {
                Walk(property.Value, limited || isLimit);
            }
        }

        using var document = JsonDocument.Parse(json);
        Walk(document.RootElement, false);
        return (nodeTypes, underLimit);
    }

    [Fact]
    public async Task ASpecAimedAtTheRawHypertable_IsSkippedWithAWarning_AndBuildsNothing()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4952 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);

        /* The premise: the store holds the raw table as a hypertable, which refuses CREATE INDEX CONCURRENTLY and whose
           per-chunk build is not offered, so an index there is skipped, never built. */
        Assert.True((bool)(await ScalarAsync(connection, "SELECT EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_schema = 'collect' AND hypertable_name = 'query_store_stats')", ct))!);

        var aimedAtTheHypertable = new QueryStoreBackgroundIndexes.IndexSpec(
            "collect.ix_query_store_stats_skip_probe",
            Raw,
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_query_store_stats_skip_probe ON collect.query_store_stats (server_id, collection_time);",
            "DROP INDEX CONCURRENTLY IF EXISTS collect.ix_query_store_stats_skip_probe;");
        var logger = new CapturingTestLogger();
        await QueryStoreBackgroundIndexes.EnsureAsync(connection, aimedAtTheHypertable, logger, ct);

        Assert.Equal(1, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
        Assert.Contains("is a hypertable", logger.Joined, StringComparison.Ordinal);
        Assert.False(await IndexExistsAsync(connection, aimedAtTheHypertable.IndexName, ct));
        Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM pg_indexes WHERE indexname LIKE '%skip_probe%'", ct));

        /* The real set builds nothing on the raw hypertable either: its index list is the same before and after. */
        const string rawIndexCount = @"
SELECT count(*)
FROM pg_index i
WHERE i.indrelid = 'collect.query_store_stats'::regclass
   OR i.indrelid IN (SELECT format('%I.%I', c.chunk_schema, c.chunk_name)::regclass
                     FROM timescaledb_information.chunks c
                     WHERE c.hypertable_schema = 'collect' AND c.hypertable_name = 'query_store_stats')";
        var before = await ScalarAsync(connection, rawIndexCount, ct);
        await EnsureAllAsync(connection, ct);
        Assert.Equal(before, await ScalarAsync(connection, rawIndexCount, ct));
    }

    [Fact]
    public async Task AnInvalidLeftover_IsDroppedAndRebuilt_ForTheBtreeAndTheBrin()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4952 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await EnsureAllAsync(connection, ct);

        var names = new[] { QueryStoreIntervalWideBrinIndex.IndexName, QueryStoreBackgroundIndexes.WideServerFirstExecIndexName };
        var oldOids = new Dictionary<string, object?>();
        foreach (var name in names)
        {
            oldOids[name] = await ScalarAsync(connection, $"SELECT '{name}'::regclass::oid", ct);

            /* An interrupted build leaves exactly this catalog state; the test roles are superuser, so flipping
               indisvalid reproduces it deterministically. */
            await ExecAsync(connection, $"UPDATE pg_index SET indisvalid = false WHERE indexrelid = '{name}'::regclass", ct);
            Assert.False(await IndexIsValidAsync(connection, name, ct));
        }

        await EnsureAllAsync(connection, ct);

        foreach (var name in names)
        {
            Assert.True(await IndexIsValidAsync(connection, name, ct), name);
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
        Assert.True(await IndexIsValidAsync(connection, QueryStoreBackgroundIndexes.WideServerFirstExecIndexName, ct));

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
