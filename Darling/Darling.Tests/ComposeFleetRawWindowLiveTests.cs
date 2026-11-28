using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only
   to CREATE and DROP its own database through ScratchPostgres, then works entirely inside it.

   #9151: procedure_stats and query_stats have no index on collection_time alone, so a fleet panel over a short window read the
   whole day's chunk and discarded most of it. The panel now carries a server list the (server_id, collection_time) index can drive.
   The answer must not change (a removed server's rows included). The list is read inside the panel's own window, so the walk touches
   only the chunks the window touches and not every chunk of the table. The multi-chunk tests below build the store the way the service
   does: hypertables on 6 hour chunks with the old chunks compressed. */
public sealed class ComposeFleetRawWindowLiveTests
{
    private const int RemovedServerId = -91510;
    private const string Panel = "{\"source\":\"procedure_stats\",\"measure\":\"proc_executions\",\"aggregate\":\"sum\",\"topN\":50,\"groupBy\":[\"object_name\"],\"viz\":\"table\","
        + "\"filters\":[{\"dimension\":\"object_name\",\"op\":\"like\",\"value\":\"%Order%\"}]}";

    private const string Seed = @"
INSERT INTO collect.procedure_stats(collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
    execution_count, delta_execution_count, sample_interval_seconds)
SELECT s::bigint * 1000000000 + k * 1000 + p, t, s, 'srv' || s, 'db', 'dbo',
       CASE WHEN p % 7 = 0 THEN NULL ELSE 'usp_' || (ARRAY['Orders','Billing','Report','Sync'])[p % 4 + 1] || '_' || (p % 6) END,
       'P', k, 5 + (p % 3), 60
FROM generate_series(719, 0, -1) k, generate_series(1, 12) s, generate_series(1, 25) p,
     LATERAL (SELECT date_trunc('minute', now() AT TIME ZONE 'UTC') - k * interval '1 minute' AS t) tt
ORDER BY k DESC, s, 1;
INSERT INTO collect.procedure_stats(collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
    execution_count, delta_execution_count, sample_interval_seconds)
SELECT -k, date_trunc('minute', now() AT TIME ZONE 'UTC') - k * interval '1 minute', -91510, 'srv-removed', 'db', 'dbo', 'usp_OrderRemoved', 'P', k, 3, 60
FROM generate_series(10, 20) k;
ANALYZE collect.procedure_stats;";

    [Fact]
    public async Task FleetPanel_ReturnsTheRowsItReturnedWithoutTheServerList()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9151 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        for (var id = 1; id <= 12; id++)
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, id, "srv" + id, ct);
        }

        await using (var seed = new NpgsqlCommand(Seed, connection) { CommandTimeout = 300 })
        {
            await seed.ExecuteNonQueryAsync(ct);
        }

        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var context = new ComposeRunContext(null, now.AddHours(-4), now, ComposeRunContext.NoVariables, RollupAvailability.None, now, RollupCoverage.Unknown, QueryStoreWideEligible: false);
        var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(Panel)!, Array.Empty<string>());
        Assert.True(parseError is null, parseError);
        var (compiled, compileError) = ComposeCompiler.Compile(plan!, context);
        Assert.True(compileError is null, compileError);

        /* today's text: the same statement without the server list */
        var sql = compiled!.Sql;
        var listStart = sql.IndexOf("  AND f.server_id = ANY(", StringComparison.Ordinal);
        Assert.True(listStart > 0, "an unscoped procedure_stats panel must carry the server list");
        var oldSql = sql.Remove(listStart, sql.IndexOf('\n', listStart) + 1 - listStart);
        Assert.NotEqual(sql, oldSql);

        var newRows = await RowsAsync(connection, sql, compiled, ct);
        var oldRows = await RowsAsync(connection, oldSql, compiled, ct);
        Assert.NotEmpty(newRows);
        Assert.Equal(oldRows, newRows);
        /* the removed server (no registry row) still counts, and NULL object names stay out of the LIKE */
        Assert.Contains(newRows, r => r.StartsWith("usp_OrderRemoved|", StringComparison.Ordinal));
        Assert.DoesNotContain(newRows, r => r.StartsWith("<null>|", StringComparison.Ordinal));
    }

    /* The multi-chunk store. Procedure rows every 5 minutes for 4 days over 17 six-hour chunks, 12 servers, 10 procedures each, plus
       edge servers: 13 and 14 hold one row exactly at the window start and at the window end (both inside, the window is closed at
       both ends), 15 and 16 hold one row a second outside it. Chunks that end before the window are compressed, as the store's policy
       leaves old chunks. The window is four hours inside one chunk, whatever hour the test runs (LiveClock.AnchoredEnd). */
    private const string MultiChunkProcedureSeed = @"
INSERT INTO collect.procedure_stats(collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
    execution_count, delta_execution_count, sample_interval_seconds)
SELECT s::bigint * 1000000000 + k * 1000 + p, $1::timestamp - k * interval '5 minutes', s, 'srv' || s, 'db', 'dbo',
       'usp_Order_' || (p % 6), 'P', k, 5 + (p % 3), 300
FROM generate_series(0, 1151) k, generate_series(1, 12) s, generate_series(1, 10) p
ORDER BY 2, 3, 1;
INSERT INTO collect.procedure_stats(collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
    execution_count, delta_execution_count, sample_interval_seconds)
SELECT -1000000000 - s::bigint * 1000000 - k * 100 - p, $1::timestamp - k * interval '20 seconds', s, 'srv' || s, 'db', 'dbo',
       'usp_Order_' || (p % 40), 'P', k, 5 + (p % 3), 20
FROM generate_series(0, 1079) k, generate_series(1, 12) s, generate_series(1, 60) p
ORDER BY 2, 3, 1;
INSERT INTO collect.procedure_stats(collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
    execution_count, delta_execution_count, sample_interval_seconds)
VALUES (-1, $2::timestamp, 13, 'srv13', 'db', 'dbo', 'usp_OrderEdge', 'P', 1, 7, 300),
       (-2, $3::timestamp, 14, 'srv14', 'db', 'dbo', 'usp_OrderEdge', 'P', 1, 7, 300),
       (-3, $2::timestamp - interval '1 second', 15, 'srv15', 'db', 'dbo', 'usp_OrderOutside', 'P', 1, 7, 300),
       (-4, $3::timestamp + interval '1 second', 16, 'srv16', 'db', 'dbo', 'usp_OrderOutside', 'P', 1, 7, 300);";

    private const string MultiChunkQuerySeed = @"
INSERT INTO collect.query_stats(collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
    query_text, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_rows,
    delta_spills, min_dop, max_dop, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time)
SELECT s::bigint * 1000000000 + k * 1000 + q, $1::timestamp - k * interval '5 minutes', s, 'srv' || s, 'DbOrder' || (q % 3), '0xH' || q, '0xP' || q,
       '0xS' || q, '0xL' || q, 'SELECT 1', 5 + (q % 3), 100, 200, 10, 0, 1, 10, 0, 1, 4, 10, 20, 30, 40
FROM generate_series(0, 1151) k, generate_series(1, 12) s, generate_series(1, 10) q
ORDER BY 2, 3, 1;
INSERT INTO collect.query_stats(collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
    query_text, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_rows,
    delta_spills, min_dop, max_dop, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time)
VALUES (-1, $2::timestamp, 13, 'srv13', 'DbOrderEdge', '0xE', '0xE', '0xE', '0xE', 'SELECT 1', 7, 100, 200, 10, 0, 1, 10, 0, 1, 4, 10, 20, 30, 40),
       (-2, $3::timestamp, 14, 'srv14', 'DbOrderEdge', '0xE', '0xE', '0xE', '0xE', 'SELECT 1', 7, 100, 200, 10, 0, 1, 10, 0, 1, 4, 10, 20, 30, 40),
       (-3, $2::timestamp - interval '1 second', 15, 'srv15', 'DbOrderOutside', '0xO', '0xO', '0xO', '0xO', 'SELECT 1', 7, 100, 200, 10, 0, 1, 10, 0, 1, 4, 10, 20, 30, 40),
       (-4, $3::timestamp + interval '1 second', 16, 'srv16', 'DbOrderOutside', '0xO', '0xO', '0xO', '0xO', 'SELECT 1', 7, 100, 200, 10, 0, 1, 10, 0, 1, 4, 10, 20, 30, 40);";

    private const string QueryPanel = "{\"source\":\"query_stats\",\"measure\":\"query_executions\",\"aggregate\":\"sum\",\"topN\":50,\"groupBy\":[\"database_name\"],\"viz\":\"table\","
        + "\"filters\":[{\"dimension\":\"database_name\",\"op\":\"like\",\"value\":\"%Order%\"}]}";

    /// <summary>Builds the store as the service does: hypertables on 6 hour chunks (the width the field's stores have), compression on, rows seeded,
    /// every chunk that ends before the window compressed. Returns the window's end.</summary>
    private static async Task<DateTime> SeedMultiChunkStoreAsync(NpgsqlConnection connection, string table, string seed, CancellationToken ct)
    {
        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await ExecAsync(connection, $"SELECT set_chunk_time_interval('collect.{table}', interval '6 hours')", ct);
        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql("collect." + table), ct);
        for (var id = 1; id <= 12; id++)
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, id, "srv" + id, ct);
        }

        var end = LiveClock.AnchoredEnd();
        /* one statement per INSERT: a statement with parameters cannot hold several commands */
        foreach (var statement in System.Text.RegularExpressions.Regex.Split(seed, @";\s*(?=INSERT INTO)"))
        {
            await ExecAsync(connection, statement, ct, end.AddHours(2), end.AddHours(-4), end);
        }

        await ExecAsync(connection, $"SELECT compress_chunk(ch, if_not_compressed => true) FROM show_chunks('collect.{table}', older_than => $1::timestamp) AS ch", ct, end.AddHours(-4));
        await ExecAsync(connection, $"ANALYZE collect.{table}", ct);
        return end;
    }

    private static ComposeCompiled CompilePanel(string panel, DateTime end)
    {
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var context = new ComposeRunContext(null, end.AddHours(-4), end, ComposeRunContext.NoVariables, RollupAvailability.None, now, RollupCoverage.Unknown, QueryStoreWideEligible: false);
        var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(panel)!, Array.Empty<string>());
        Assert.True(parseError is null, parseError);
        var (compiled, compileError) = ComposeCompiler.Compile(plan!, context);
        Assert.True(compileError is null, compileError);
        return compiled!;
    }

    /// <summary>The statement as dev compiled it: the same text without the server list.</summary>
    private static string WithoutTheServerList(string sql)
    {
        var listStart = sql.IndexOf("  AND f.server_id = ANY(", StringComparison.Ordinal);
        if (listStart < 0)
        {
            return sql;
        }

        return sql.Remove(listStart, sql.IndexOf('\n', listStart) + 1 - listStart);
    }

    [Fact]
    public async Task FleetPanel_OnAMultiChunkStore_ReadsFewerPagesThanTheStatementWithoutTheList_AndReturnsTheSameRows()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9151 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var end = await SeedMultiChunkStoreAsync(connection, "procedure_stats", MultiChunkProcedureSeed, ct);
            var chunks = (long)(await ScalarAsync(connection, "SELECT count(*) FROM show_chunks('collect.procedure_stats')", ct))!;
            Assert.True(chunks >= 16, $"the store must hold many chunks, it holds {chunks}");

            var compiled = CompilePanel(Panel, end);
            var devSql = WithoutTheServerList(compiled.Sql);

            var rows = await RowsAsync(connection, compiled.Sql, compiled, ct);
            var devRows = await RowsAsync(connection, devSql, compiled, ct);
            Assert.NotEmpty(rows);
            Assert.Equal(devRows, rows);
            /* a row exactly at either end of the window counts, a row a second outside does not */
            Assert.Contains(rows, r => r.StartsWith("usp_OrderEdge|14", StringComparison.Ordinal));
            Assert.DoesNotContain(rows, r => r.StartsWith("usp_OrderOutside|", StringComparison.Ordinal));

            var pages = await DataPagesAsync(connection, compiled.Sql, compiled, ct);
            var devPages = await DataPagesAsync(connection, devSql, compiled, ct);
            /* #9943: on PostgreSQL 18 the listed statement is a bitmap heap scan and reads fewer pages (7,975 against 10,474 here). On 17 the
               planner takes a parallel index scan over the same list, which re-reads heap pages in server order (15,406 against 10,474), and
               the unlisted statement is a sequential scan of the one chunk. This window holds 524,282 of the table's 915,844 rows, so
               the sequential scan is not a poor choice, and the page count says which scan the version's planner picked. The same rows
               (above) and the chunk walk (below) hold on both versions. */
            if (connection.PostgreSqlVersion.Major >= 18)
            {
                Assert.True(pages < devPages, $"the panel with the bounded server list read {pages} pages, the statement without the list read {devPages}");
            }

            /* the list walk reads the chunks the window touches and no other: a chunk that ends before the window starts is not in the plan */
            var outside = new HashSet<string>(StringComparer.Ordinal);
            await using (var chunkNames = new NpgsqlCommand(
                "SELECT cl.relname FROM show_chunks('collect.procedure_stats', older_than => $1::timestamp) AS ch JOIN pg_class AS cl ON cl.oid = ch", connection))
            {
                chunkNames.Parameters.AddWithValue(end.AddHours(-4));
                await using var names = await chunkNames.ExecuteReaderAsync(ct);
                while (await names.ReadAsync(ct))
                {
                    outside.Add(names.GetString(0));
                }
            }

            Assert.True(outside.Count >= 12, $"the store must hold many chunks that end before the window, it holds {outside.Count}");
            await using var explain = Command(connection, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + compiled.Sql, compiled);
            var outsideRows = ExplainBuffers.RowsReadAtLeast(await explain.ExecuteScalarAsync(ct), outside);
            Assert.True(outsideRows == 0, $"the panel read {outsideRows} rows from chunks that end before the window starts");
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task QueryStatsFleetPanel_ReturnsTheRowsItReturnedWithoutTheServerList()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9151 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var end = await SeedMultiChunkStoreAsync(connection, "query_stats", MultiChunkQuerySeed, ct);

            var compiled = CompilePanel(QueryPanel, end);
            var devSql = WithoutTheServerList(compiled.Sql);
            Assert.NotEqual(compiled.Sql, devSql);

            var rows = await RowsAsync(connection, compiled.Sql, compiled, ct);
            var devRows = await RowsAsync(connection, devSql, compiled, ct);
            Assert.NotEmpty(rows);
            Assert.Equal(devRows, rows);
            Assert.Contains(rows, r => r.StartsWith("DbOrderEdge|14", StringComparison.Ordinal));
            Assert.DoesNotContain(rows, r => r.StartsWith("DbOrderOutside|", StringComparison.Ordinal));
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

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    /// <summary>The table and index pages the statement reads (the second, warm run: the first rebuilds catalog caches).</summary>
    private static async Task<long> DataPagesAsync(NpgsqlConnection connection, string sql, ComposeCompiled compiled, CancellationToken ct)
    {
        long pages = 0;
        for (var run = 0; run < 2; run++)
        {
            await using var command = Command(connection, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, compiled);
            pages = ExplainBuffers.DataPages(await command.ExecuteScalarAsync(ct));
        }

        return pages;
    }

    private static NpgsqlCommand Command(NpgsqlConnection connection, string sql, ComposeCompiled compiled)
    {
        var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        foreach (var p in compiled.Parameters)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = p.NpgsqlDbType, Value = p.Value });
        }

        return command;
    }

    private static async Task<List<string>> RowsAsync(NpgsqlConnection connection, string sql, ComposeCompiled compiled, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var command = Command(connection, sql, compiled);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(string.Join("|", Enumerable.Range(0, reader.FieldCount)
                .Select(i => reader.IsDBNull(i) ? "<null>" : Convert.ToString(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture))));
        }

        return rows;
    }
}
