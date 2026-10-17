/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5984 (wave 2 of #5971): the Darling query reads carry the query tag and filter by it. The tests seed
/// <c>config.query_tag_map</c> directly (the builder is #5985) and read through the MCP tools: a mapped row carries one
/// <c>tag</c> (its path name), an unmapped row carries none, the filter matches the tag and every tag beneath it before the
/// top-N cut, <c>untagged</c> keeps the unmapped rows, and an unknown name answers empty and says so. One read of each kind:
/// query stats, procedure stats, Query Store, active-query snapshots and long-query completions. Map keys match the rows' key text
/// exactly (no case folding at read time), so the lookup can use the map's primary key.
/// </summary>
[Collection("live-postgres")]
public sealed class QueryTagReadsLiveTests
{
    internal const string ServerName = "darling-query-tag-reads-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "TagDb";
    private const string Hog = "0xTAGHOG";
    private const string Ord = "0xTAGORD";
    private const string Bat = "0xTAGBAT";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    internal static async Task RunAsync(Func<NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live query-tag read tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            using (var connection = new NpgsqlConnection(cs))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
                await CleanupAsync(connection, ct);
                await SeedAsync(connection, ct);
            }

            await body(postgres, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await CleanupAsync(cleanup, cleanupCt));
        }
    }

    private static async Task CleanupAsync(NpgsqlConnection c, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(c, ct,
            $@"DELETE FROM query_stats WHERE server_id = {ServerId}; DELETE FROM procedure_stats WHERE server_id = {ServerId};
               DELETE FROM query_store_stats WHERE server_id = {ServerId}; DELETE FROM query_snapshots WHERE server_id = {ServerId};
               DELETE FROM long_query_completions WHERE server_id = {ServerId}; DELETE FROM config.query_tag_map WHERE server_id = {ServerId};
               DELETE FROM config.query_tags WHERE name IN ('TagReadsProd', 'TagReadsOrders', 'TagReadsBatch'); DELETE FROM servers WHERE server_id = {ServerId}");

    /// <summary>Tags: TagReadsProd with child TagReadsOrders, and TagReadsBatch. Map: Ord to Orders, Bat to Batch, Hog unmapped.</summary>
    private static async Task SeedAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await DarlingMcpTestData.RegisterServerAsync(c, ServerId, ServerName, ct);
        await DarlingMcpTestData.ExecAsync(c, ct, "INSERT INTO config.query_tags (name) VALUES ('TagReadsProd')");
        await DarlingMcpTestData.ExecAsync(c, ct, "INSERT INTO config.query_tags (name, parent_id) SELECT 'TagReadsOrders', id FROM config.query_tags WHERE name = 'TagReadsProd'");
        await DarlingMcpTestData.ExecAsync(c, ct, "INSERT INTO config.query_tags (name) VALUES ('TagReadsBatch')");
        foreach (var (kind, key, tag) in new[]
        {
            ("hash", Ord, "TagReadsOrders"), ("hash", Bat, "TagReadsBatch"), ("object", $"{Db}.dbo.usp_TagOrders", "TagReadsOrders"),
        })
        {
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source)
                  SELECT $1, $2, $3, id, 'rule' FROM config.query_tags WHERE name = $4",
                ServerId, kind, key, tag);
        }

        var at = DarlingMcpTestData.Naive(LiveClock.Now().AddMinutes(-10));
        foreach (var (hash, cpu) in new[] { (Hog, 9_000_000L), (Ord, 5_000_000L), (Bat, 1_000_000L) })
        {
            var text = "SELECT " + hash;
            var digest = System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(text));
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                                           query_text, query_text_digest, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, min_dop, max_dop)
                  VALUES ($1,$2,$3,$4,$5,$6,'0xPLAN','0xSQLH','0xPLANH',$7,$8,10,$9,$9,100,1,1)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, Db, hash, text, digest, cpu);
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, query_hash, query_plan_hash,
                                                 query_text, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, avg_rowcount, min_dop, max_dop, last_execution_time)
                  VALUES ($1,$2,$3,$4,$5,$6,$6,'Regular',$7,'0xPLAN',$8,10,$9,4000,800,0,40,250,1,4,$2)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, Db, (long)Math.Abs(hash.GetHashCode() % 100000), hash, text, cpu / 10);
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status, query_hash, cpu_time_ms, total_elapsed_time_ms)
                  VALUES ($1,$2,$3,$4,$5,$6,$7,'running',$8,$9,$9)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, 50 + Math.Abs(hash.GetHashCode() % 40), Db, text, hash, cpu / 1000);
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, duration_microseconds, statement_text, query_hash)
                  VALUES ($1,$2,$3,$4,$2,'rpc_completed',$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, Db, cpu, text, hash);
        }

        foreach (var (name, cpu) in new[] { ("usp_TagPlain", 8_000_000L), ("usp_TagOrders", 2_000_000L) })
        {
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
                                               delta_execution_count, delta_worker_time, delta_elapsed_time)
                  VALUES ($1,$2,$3,$4,$5,'dbo',$6,'SQL_STORED_PROCEDURE',10,$7,$7)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, Db, name, cpu);
        }
    }

    private static JsonElement Rows(string json, string member)
    {
        var root = JsonDocument.Parse(json).RootElement.Clone();
        return root.GetProperty(member);
    }

    private static string?[] Tags(JsonElement rows) =>
        rows.EnumerateArray().Select(r => r.TryGetProperty("tag", out var t) ? t.GetString() : null).ToArray();

    [Fact]
    public Task TopQueries_ARowCarriesItsTagPath_AndAnUnmappedRowCarriesNoTagMember() => RunAsync(async (pg, ct) =>
    {
        var rows = Rows(await DarlingMcpDataTools.GetTopQueriesByCpu(pg, ServerName, 1, 10, Db, cancellationToken: ct), "queries");
        Assert.Equal([Hog, Ord, Bat], rows.EnumerateArray().Select(r => r.GetProperty("query_hash").GetString()).ToArray());
        Assert.Equal([null, "TagReadsProd / TagReadsOrders", "TagReadsBatch"], Tags(rows));
        Assert.False(rows[0].TryGetProperty("tag", out _));
    });

    [Fact]
    public Task TopQueries_ATagFilter_MatchesTheTagAndItsDescendants_AnyCase() => RunAsync(async (pg, ct) =>
    {
        foreach (var tag in new[] { "TagReadsProd", "tagreadsprod", "TagReadsProd / TagReadsOrders", "TagReadsOrders" })
        {
            var rows = Rows(await DarlingMcpDataTools.GetTopQueriesByCpu(pg, ServerName, 1, 10, Db, tag: tag, cancellationToken: ct), "queries");
            Assert.Equal([Ord], rows.EnumerateArray().Select(r => r.GetProperty("query_hash").GetString()).ToArray());
        }
    });

    [Fact]
    public Task TopQueries_TheFilterAppliesBeforeTheCut_SoATaggedQueryOutsideTheUnfilteredTopStillComesBack() => RunAsync(async (pg, ct) =>
    {
        var unfiltered = Rows(await DarlingMcpDataTools.GetTopQueriesByCpu(pg, ServerName, 1, 1, Db, cancellationToken: ct), "queries");
        Assert.Equal(Hog, Assert.Single(unfiltered.EnumerateArray()).GetProperty("query_hash").GetString());
        var filtered = Rows(await DarlingMcpDataTools.GetTopQueriesByCpu(pg, ServerName, 1, 1, Db, tag: "TagReadsBatch", cancellationToken: ct), "queries");
        Assert.Equal(Bat, Assert.Single(filtered.EnumerateArray()).GetProperty("query_hash").GetString());
    });

    [Fact]
    public Task TopQueries_Untagged_KeepsOnlyTheUnmappedRows() => RunAsync(async (pg, ct) =>
    {
        var rows = Rows(await DarlingMcpDataTools.GetTopQueriesByCpu(pg, ServerName, 1, 10, Db, tag: "untagged", cancellationToken: ct), "queries");
        Assert.Equal([Hog], rows.EnumerateArray().Select(r => r.GetProperty("query_hash").GetString()).ToArray());
    });

    [Fact]
    public Task AnUnknownTag_AnswersEmptyAndSaysSo_OnEveryRead() => RunAsync(async (pg, ct) =>
    {
        foreach (var json in new[]
        {
            await DarlingMcpDataTools.GetTopQueriesByCpu(pg, ServerName, 1, 10, Db, tag: "NoSuchTag", cancellationToken: ct),
            await DarlingMcpDataTools.GetTopProceduresByCpu(pg, ServerName, 1, 10, Db, tag: "NoSuchTag", cancellationToken: ct),
            await DarlingMcpDataTools.GetQueryStoreTop(pg, ServerName, 1, 10, Db, tag: "NoSuchTag", cancellationToken: ct),
            await DarlingMcpSessionTools.GetActiveQueries(pg, ServerName, 1, tag: "NoSuchTag", cancellationToken: ct),
            await DarlingMcpLongQueryTools.GetLongQueryCompletions(pg, ServerName, 1, tag: "NoSuchTag", cancellationToken: ct),
        })
        {
            var root = JsonDocument.Parse(json).RootElement;
            Assert.Equal("empty", root.GetProperty("status").GetString());
            Assert.Contains("NoSuchTag", root.GetProperty("message").GetString(), StringComparison.Ordinal);
        }
    });

    [Fact]
    public Task TopProcedures_CarryTheObjectTag_AndFilterByIt() => RunAsync(async (pg, ct) =>
    {
        var all = Rows(await DarlingMcpDataTools.GetTopProceduresByCpu(pg, ServerName, 1, 10, Db, cancellationToken: ct), "procedures");
        Assert.Equal([null, "TagReadsProd / TagReadsOrders"], Tags(all));
        var filtered = Rows(await DarlingMcpDataTools.GetTopProceduresByCpu(pg, ServerName, 1, 1, Db, tag: "TagReadsProd", cancellationToken: ct), "procedures");
        Assert.Equal("dbo.usp_TagOrders", Assert.Single(filtered.EnumerateArray()).GetProperty("full_name").GetString());
    });

    [Fact]
    public Task ObjectKeys_MatchExactly_ARowDifferingOnlyInCaseIsNotTagged() => RunAsync(async (pg, ct) =>
    {
        /* #5984: reads join the map by exact key text. The map key is TagDb.dbo.usp_TagOrders; a row whose name differs only in case is
           a different key, and case folding belongs to the map build (#5985), never to the read. */
        await using (var connection = await pg.OpenConnectionAsync(ct))
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
                                               delta_execution_count, delta_worker_time, delta_elapsed_time)
                  VALUES ($1,$2,$3,$4,$5,'dbo','usp_tagorders','SQL_STORED_PROCEDURE',10,1000000,1000000)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(LiveClock.Now().AddMinutes(-10)), ServerId, ServerName, Db);
        }

        var all = Rows(await DarlingMcpDataTools.GetTopProceduresByCpu(pg, ServerName, 1, 10, Db, cancellationToken: ct), "procedures");
        var byName = all.EnumerateArray().ToDictionary(r => r.GetProperty("full_name").GetString()!, r => r.TryGetProperty("tag", out var t) ? t.GetString() : null);
        Assert.Equal("TagReadsProd / TagReadsOrders", byName["dbo.usp_TagOrders"]);
        Assert.Null(byName["dbo.usp_tagorders"]);

        var filtered = Rows(await DarlingMcpDataTools.GetTopProceduresByCpu(pg, ServerName, 1, 10, Db, tag: "TagReadsProd", cancellationToken: ct), "procedures");
        Assert.Equal("dbo.usp_TagOrders", Assert.Single(filtered.EnumerateArray()).GetProperty("full_name").GetString());
        var untagged = Rows(await DarlingMcpDataTools.GetTopProceduresByCpu(pg, ServerName, 1, 10, Db, tag: "untagged", cancellationToken: ct), "procedures");
        Assert.Equal(["dbo.usp_TagPlain", "dbo.usp_tagorders"], untagged.EnumerateArray().Select(r => r.GetProperty("full_name").GetString()).OrderBy(n => n, StringComparer.Ordinal).ToArray());
    });

    [Fact]
    public Task TagLookup_ProbesTheMapsPrimaryKey_NotAScanOfTheServersKeys() => RunAsync(async (pg, ct) =>
    {
        /* #5984: a function on the key (lower(key)) cannot use the primary key (server_id, key_kind, key), and a server's hash map can hold
           hundreds of thousands of rows. Seed a few hundred more rows, then EXPLAIN the shipped lookup with sequential scans off: only a
           statement whose key predicate is a plain comparison can answer from the primary key. */
        await using var connection = await pg.OpenConnectionAsync(ct);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source)
              SELECT $1, 'hash', '0xFILL' || g, id, 'rule' FROM config.query_tags, generate_series(1, 400) AS g WHERE name = 'TagReadsBatch'",
            ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "ANALYZE config.query_tag_map");
        await DarlingMcpTestData.ExecAsync(connection, ct, "SET enable_seqscan = off");

        var plan = new System.Text.StringBuilder();
        await using (var explain = new NpgsqlCommand("EXPLAIN (COSTS OFF) " + QueryTagReadJoin.LookupSql, connection))
        {
            explain.Parameters.Add(new NpgsqlParameter { Value = ServerId, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
            explain.Parameters.Add(new NpgsqlParameter { Value = "hash", NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            explain.Parameters.Add(new NpgsqlParameter { Value = new[] { Ord, Bat }, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text });
            await using var reader = await explain.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                plan.AppendLine(reader.GetString(0));
            }
        }

        var text = plan.ToString();
        /* The planner may take the primary key or idx_query_tag_map_key (key_kind, key); either answers from the key columns. */
        Assert.True(text.Contains("query_tag_map_pkey", StringComparison.Ordinal) || text.Contains("idx_query_tag_map_key", StringComparison.Ordinal), text);
        Assert.Matches("Index (Only )?Scan|Bitmap Index Scan", text);
        Assert.DoesNotContain("Seq Scan", text, StringComparison.Ordinal);
        Assert.DoesNotContain("lower(", text, StringComparison.Ordinal);
    });

    [Fact]
    public Task QueryStoreTop_CarriesTheHashTag_AndFiltersByIt() => RunAsync(async (pg, ct) =>
    {
        var all = Rows(await DarlingMcpDataTools.GetQueryStoreTop(pg, ServerName, 1, 10, Db, cancellationToken: ct), "queries");
        Assert.Equal(3, all.GetArrayLength());
        Assert.Equal(["TagReadsProd / TagReadsOrders", "TagReadsBatch"], Tags(all).Where(t => t is not null).ToArray());
        var filtered = Rows(await DarlingMcpDataTools.GetQueryStoreTop(pg, ServerName, 1, 10, Db, tag: "TagReadsProd", cancellationToken: ct), "queries");
        Assert.Equal(Ord, Assert.Single(filtered.EnumerateArray()).GetProperty("query_hash").GetString());
    });

    [Fact]
    public Task ActiveQueries_CarryTheHashTag_AndFilterByIt() => RunAsync(async (pg, ct) =>
    {
        var all = Rows(await DarlingMcpSessionTools.GetActiveQueries(pg, ServerName, 1, cancellationToken: ct), "queries");
        Assert.Equal(["TagReadsProd / TagReadsOrders", "TagReadsBatch"], Tags(all).Where(t => t is not null).ToArray());
        var filtered = Rows(await DarlingMcpSessionTools.GetActiveQueries(pg, ServerName, 1, tag: "TagReadsProd", cancellationToken: ct), "queries");
        Assert.Equal(Ord, Assert.Single(filtered.EnumerateArray()).GetProperty("query_hash").GetString());
    });

    [Fact]
    public Task LongQueryCompletions_CarryTheHashTag_AndFilterByIt() => RunAsync(async (pg, ct) =>
    {
        var all = Rows(await DarlingMcpLongQueryTools.GetLongQueryCompletions(pg, ServerName, 1, cancellationToken: ct), "completions");
        Assert.Equal(["TagReadsProd / TagReadsOrders", "TagReadsBatch"], Tags(all).Where(t => t is not null).ToArray());
        var filtered = Rows(await DarlingMcpLongQueryTools.GetLongQueryCompletions(pg, ServerName, 1, tag: "TagReadsProd", cancellationToken: ct), "completions");
        Assert.Equal(Ord, Assert.Single(filtered.EnumerateArray()).GetProperty("query_hash").GetString());
    });

    /* ─── #5984 round 1: the answer that carries rows names the tag that cut its population ─── */

    [Fact]
    public Task TopQueries_FilterApplied_NamesTheTag_AndStaysNullWithoutOne() => RunAsync(async (pg, ct) =>
    {
        var plain = JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(pg, ServerName, 1, 10, Db, cancellationToken: ct)).RootElement;
        Assert.Equal(JsonValueKind.Null, plain.GetProperty("filter_applied").ValueKind);

        var tagged = JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(pg, ServerName, 1, 10, Db, tag: "tagreadsbatch", cancellationToken: ct)).RootElement;
        Assert.Contains("'tagreadsbatch'", tagged.GetProperty("filter_applied").GetString(), StringComparison.Ordinal);

        var untagged = JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(pg, ServerName, 1, 10, Db, tag: "untagged", cancellationToken: ct)).RootElement;
        Assert.Contains("'untagged'", untagged.GetProperty("filter_applied").GetString(), StringComparison.Ordinal);
    });

    [Fact]
    public Task TopProcedures_NameTheTag_OnlyWhenOneWasSent() => RunAsync(async (pg, ct) =>
    {
        var plain = JsonDocument.Parse(await DarlingMcpDataTools.GetTopProceduresByCpu(pg, ServerName, 1, 10, Db, cancellationToken: ct)).RootElement;
        Assert.False(plain.TryGetProperty("tag_filter", out _));
        var tagged = JsonDocument.Parse(await DarlingMcpDataTools.GetTopProceduresByCpu(pg, ServerName, 1, 10, Db, tag: "TagReadsProd", cancellationToken: ct)).RootElement;
        Assert.Equal("TagReadsProd", tagged.GetProperty("tag_filter").GetString());
        var untagged = JsonDocument.Parse(await DarlingMcpDataTools.GetTopProceduresByCpu(pg, ServerName, 1, 10, Db, tag: "untagged", cancellationToken: ct)).RootElement;
        Assert.Equal("untagged", untagged.GetProperty("tag_filter").GetString());
    });

    [Fact]
    public Task QueryStoreTop_NamesTheTag_OnlyWhenOneWasSent() => RunAsync(async (pg, ct) =>
    {
        var plain = JsonDocument.Parse(await DarlingMcpDataTools.GetQueryStoreTop(pg, ServerName, 1, 10, Db, cancellationToken: ct)).RootElement;
        Assert.False(plain.TryGetProperty("tag_filter", out _));
        var tagged = JsonDocument.Parse(await DarlingMcpDataTools.GetQueryStoreTop(pg, ServerName, 1, 10, Db, tag: "TagReadsProd", cancellationToken: ct)).RootElement;
        Assert.Equal("TagReadsProd", tagged.GetProperty("tag_filter").GetString());
    });

    [Fact]
    public Task ActiveQueries_FiltersApplied_NamesTheTag_AndIsUnchangedWithoutOne() => RunAsync(async (pg, ct) =>
    {
        var plain = JsonDocument.Parse(await DarlingMcpSessionTools.GetActiveQueries(pg, ServerName, 1, cancellationToken: ct)).RootElement;
        Assert.Equal(["database_name", "blocking_only", "wait_type"], plain.GetProperty("filters_applied").EnumerateObject().Select(p => p.Name).ToArray());
        var tagged = JsonDocument.Parse(await DarlingMcpSessionTools.GetActiveQueries(pg, ServerName, 1, tag: "TagReadsProd", cancellationToken: ct)).RootElement;
        Assert.Equal("TagReadsProd", tagged.GetProperty("filters_applied").GetProperty("tag").GetString());
        var untagged = JsonDocument.Parse(await DarlingMcpSessionTools.GetActiveQueries(pg, ServerName, 1, tag: "untagged", cancellationToken: ct)).RootElement;
        Assert.Equal("untagged", untagged.GetProperty("filters_applied").GetProperty("tag").GetString());
    });

    [Fact]
    public Task LongQueryCompletions_NameTheTag_BesideTheDatabaseEcho_OnlyWhenOneWasSent() => RunAsync(async (pg, ct) =>
    {
        var plain = JsonDocument.Parse(await DarlingMcpLongQueryTools.GetLongQueryCompletions(pg, ServerName, 1, cancellationToken: ct)).RootElement;
        Assert.False(plain.TryGetProperty("tag_filter", out _));
        var tagged = JsonDocument.Parse(await DarlingMcpLongQueryTools.GetLongQueryCompletions(pg, ServerName, 1, tag: "TagReadsProd", cancellationToken: ct)).RootElement;
        Assert.Equal("TagReadsProd", tagged.GetProperty("tag_filter").GetString());
    });

    /// <summary>
    /// #5984: the tag predicate runs inside the host-object rollup (both passes), and a rolled procedure row carries a null
    /// query_hash, so the filter must act on each STATEMENT before the sum: a procedure with one tagged and one untagged
    /// statement counts only the matching statement's figures under the tag and under untagged, and an ad-hoc row (host NULL)
    /// still comes back on its own.
    /// </summary>
    [Fact]
    public Task TopQueries_HostObjectRollup_CountsOnlyTheStatementsTheTagSelects() => RunAsync(async (pg, ct) =>
    {
        const string host = "dbo.usp_TagRollup";
        const string tagged = "0xTAGPROCA";
        const string plain = "0xTAGPROCB";
        var at = DarlingMcpTestData.Naive(LiveClock.Now().AddMinutes(-10));
        using (var c = new NpgsqlConnection(ConnectionString))
        {
            await c.OpenAsync(ct);
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source)
                  SELECT $1, 'hash', $2, id, 'rule' FROM config.query_tags WHERE name = 'TagReadsBatch'",
                ServerId, tagged);
            foreach (var (hash, executions, cpu) in new[] { (tagged, 3L, 3_000_000L), (plain, 7L, 7_000_000L) })
            {
                var text = "SELECT " + hash;
                var digest = System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(text));
                await DarlingMcpTestData.ExecAsync(c, ct,
                    @"INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                                               query_text, query_text_digest, host_object_name, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, min_dop, max_dop)
                      VALUES ($1,$2,$3,$4,$5,$6,'0xPLAN','0xSQLH','0xPLANH',$7,$8,$9,$10,$11,$11,100,1,1)",
                    CollectionIdGenerator.Next(), at, ServerId, ServerName, Db, hash, text, digest, host, executions, cpu);
            }
        }

        static JsonElement ProcRow(JsonElement rows) =>
            rows.EnumerateArray().Single(r => r.GetProperty("host_object").ValueKind == JsonValueKind.String);

        var onTag = Rows(await DarlingMcpDataTools.GetTopQueriesByCpu(pg, ServerName, 1, 10, Db, tag: "TagReadsBatch", group_by: "host_object", cancellationToken: ct), "queries");
        Assert.Equal(2, onTag.GetArrayLength());
        var taggedProc = ProcRow(onTag);
        Assert.Equal(host, taggedProc.GetProperty("host_object").GetString());
        Assert.Equal(1, taggedProc.GetProperty("distinct_query_hashes").GetInt32());
        Assert.Equal(3L, taggedProc.GetProperty("execution_count").GetInt64());
        Assert.Equal(3000.0, taggedProc.GetProperty("total_cpu_ms").GetDouble(), 3);
        var adHoc = onTag.EnumerateArray().Single(r => r.GetProperty("host_object").ValueKind == JsonValueKind.Null);
        Assert.Equal(Bat, adHoc.GetProperty("query_hash").GetString());

        var untagged = Rows(await DarlingMcpDataTools.GetTopQueriesByCpu(pg, ServerName, 1, 10, Db, tag: "untagged", group_by: "host_object", cancellationToken: ct), "queries");
        var plainProc = ProcRow(untagged);
        Assert.Equal(host, plainProc.GetProperty("host_object").GetString());
        Assert.Equal(1, plainProc.GetProperty("distinct_query_hashes").GetInt32());
        Assert.Equal(7L, plainProc.GetProperty("execution_count").GetInt64());
        Assert.Equal(7000.0, plainProc.GetProperty("total_cpu_ms").GetDouble(), 3);
        Assert.Equal(Hog, untagged.EnumerateArray().Single(r => r.GetProperty("host_object").ValueKind == JsonValueKind.Null).GetProperty("query_hash").GetString());
    });
}
