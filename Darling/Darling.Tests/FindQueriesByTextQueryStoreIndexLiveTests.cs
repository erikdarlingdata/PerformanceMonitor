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
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8570 on a real store: once the opt-in trigram index on the Query Store text is ready, the one-server Query Store source of
/// <c>find_queries_by_text</c> reads it for a pattern that is rare on the server, keeps the key-order walk for a common one, and answers
/// with the same rows as without the index: the same matches in the same order, the same cap and the same excerpts. A store an earlier
/// build indexed on the text alone answers the same through the index keyed by server, which applies the server inside the index.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test creates its own scratch database through ScratchPostgres and
   works entirely inside it, so it cannot race live collection. The index is built on the test's own tracker and the tool is handed it,
   so the process-wide tracker is never read. */
public sealed class FindQueriesByTextQueryStoreIndexLiveTests
{
    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the #8570 live tests.";

    private const string ServerName = "qx-a";

    private const int ServerId = 1;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly DateTime Now = new(2026, 11, 17, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>8 servers of 2,500 texts of about 300 characters, each server's rows in 8 databases, inserted interleaved by server the way
    /// collection writes them. The <c>ORDER BY</c> fixes that order: a cross join's row order is the planner's choice, and a server whose
    /// rows sit together on a few pages walks its key in a handful of buffers, which is not the store the buffer test measures. Per server: <c>Col_12 FROM</c> matches 3 texts (one each in db_1, db_4 and db_6), <c>Customer_12</c> matches
    /// 111, <c>dbo.Orders_</c> matches every text, and the no-match pattern none. A full-table ANALYZE, so the plans are the same every run.</summary>
    private static async Task SeedAsync(NpgsqlConnection c, CancellationToken ct)
    {
        foreach (var (id, name) in new[] { (ServerId, ServerName), (2, "qx-b") })
        {
            await DarlingMcpTestData.RegisterServerAsync(c, id, name, ct);
        }

        await DarlingMcpTestData.ExecAsync(c, ct, @"
INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, query_hash, last_seen)
SELECT s, 'db_' || (q % 8), q,
       '(@p' || (q % 50) || ' int)SELECT t.' || md5(q::text || 'a') || ', t.Col_' || (q % 997) || ' FROM dbo.Orders_' || (q % 5000)
       || ' AS t JOIN dbo.Customer_' || (q % 3001) || ' AS c ON c.Id = t.CustomerId WHERE t.' || md5(q::text || 'b')
       || ' = @p' || (q % 50) || ' AND c.Region = N''' || md5(q::text || 'c') || ''' ORDER BY t.Created DESC /* ' || md5(s::text || q::text) || ' */',
       '0x' || upper(substr(md5(q::text), 1, 16)),
       timestamp '2026-11-01' + (q % 1000) * interval '1 minute'
FROM generate_series(1, 2500) AS q, generate_series(1, 8) AS s
ORDER BY q, s");
        await DarlingMcpTestData.ExecAsync(c, ct, "ANALYZE collect.query_store_text");
    }

    private static async Task<(ScratchPostgres Scratch, NpgsqlDataSource Data, NpgsqlConnection Admin)> OpenAsync(CancellationToken ct)
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipReason);
        var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var admin = new NpgsqlConnection(scratch.ConnectionString);
        await admin.OpenAsync(ct);
        await SeedAsync(admin, ct);
        return (scratch, NpgsqlDataSource.Create(scratch.ConnectionString), admin);
    }

    private static async Task<QueryTextTrigramIndexTracker> BuildAsync(NpgsqlDataSource data, NpgsqlConnection admin, CancellationToken ct)
    {
        var tracker = new QueryTextTrigramIndexTracker();
        tracker.Configure(true);
        await QueryStoreTextTrigramIndex.RunPassAsync(data, true, tracker, Now, NullLogger.Instance, ct);
        await using (var connection = await data.OpenConnectionAsync(ct))
        {
            Assert.Equal(TrigramIndexStatus.Ready, (await QueryStoreTextTrigramIndex.ReadHealthAsync(connection, tracker, ct)).Status);
        }

        await DarlingMcpTestData.ExecAsync(admin, ct, "ANALYZE collect.query_store_text");
        return tracker;
    }

    /// <summary>The Query Store source of the reply, as one string: status, returned, truncated and every row as the reply carries it.</summary>
    private static async Task<string> QueryStoreSourceAsync(
        NpgsqlDataSource data, string pattern, string? database, int limit, QueryTextTrigramIndexTracker queryStoreTextTracker, CancellationToken ct)
    {
        var planCacheTracker = new QueryTextTrigramIndexTracker();
        planCacheTracker.Configure(false);
        var reply = await DarlingMcpFindQueriesByTextTools.FindQueriesByText(
            data, pattern, ServerName, database, limit, McpDataDetail.Full,
            DarlingMcpFindQueriesByTextTools.SourceBudget.Default, DarlingMcpFindQueriesByTextTools.SourceBudget.Default, ct,
            planCacheTracker, queryStoreTextTracker);
        using var document = JsonDocument.Parse(reply);
        var source = document.RootElement.GetProperty("sources").GetProperty("query_store");
        var status = source.GetProperty("status").GetString();
        Assert.True(status is "ok" or "empty", reply);
        return string.Join(
            "\n",
            status,
            source.GetProperty("returned").GetInt32(),
            source.GetProperty("truncated").GetBoolean(),
            source.GetProperty("rows").GetRawText());
    }

    /// <summary>Two Query Store sources are the same answer: the same status, count and cap, and the same rows in the same order with the same
    /// excerpts. The one difference allowed is the withheld marker in place of a row's excerpt, on either side: the seeded texts hold nothing
    /// the statement filter names, so a marker there is the filter's fail-closed answer to a match that ran past its timeout on a loaded
    /// machine, which the index does not change.</summary>
    private static void AssertSameAnswer(string before, string after, string label)
    {
        var at = before.IndexOf('[', StringComparison.Ordinal);
        var afterAt = after.IndexOf('[', StringComparison.Ordinal);
        var rowsBefore = JsonNode.Parse(before[at..])!.AsArray();
        var rowsAfter = JsonNode.Parse(after[afterAt..])!.AsArray();
        if (rowsBefore.Count == rowsAfter.Count)
        {
            for (var i = 0; i < rowsBefore.Count; i++)
            {
                if (!Withheld(rowsBefore[i]) && !Withheld(rowsAfter[i])) continue;
                foreach (var row in new[] { rowsBefore[i]!, rowsAfter[i]! })
                {
                    row["excerpt"] = SensitiveStatements.PlaceholderText;
                    row["text_length"] = null;
                }
            }
        }

        Assert.True(
            before[..at] == after[..afterAt] && rowsBefore.ToJsonString() == rowsAfter.ToJsonString(),
            $"{label}:\nwithout the index\n{before}\nwith it\n{after}");

        static bool Withheld(JsonNode? row) =>
            row?["excerpt"] is JsonValue excerpt && excerpt.GetValue<string>() == SensitiveStatements.PlaceholderText;
    }

    private static (string Database, long QueryId)[] Keys(string source)
    {
        using var document = JsonDocument.Parse(source[source.IndexOf('[', StringComparison.Ordinal)..]);
        return document.RootElement.EnumerateArray()
            .Select(r => (r.GetProperty("database_name").GetString()!, r.GetProperty("query_id").GetInt64()))
            .ToArray();
    }

    /// <summary>The patterns and caps, by what they test: none on the server, a rare one in three databases, a cap on a pattern the
    /// index plan reads in full and sorts (the walk stops at the cap), the same cap inside one database, a cap on a pattern every text
    /// holds, and the excerpt of a match deep in the text.</summary>
    private static readonly (string Pattern, string? Database, int Limit)[] Cases =
    [
        ("zz_no_such_table_zz", null, 20),
        ("Col_12 FROM", null, 20),
        ("Customer_12", null, 5),
        ("Customer_12", "db_3", 3),
        ("dbo.Orders_", null, 4),
        ("c.Region = N'", null, 2),
    ];

    [Fact]
    public async Task TheReadyIndex_AnswersWithTheSameRows_TheSameOrder_TheSameCapAndTheSameExcerpts()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, data, admin) = await OpenAsync(ct);
        await using var _s = scratch;
        await using var _d = data;
        await using var _a = admin;

        var off = new QueryTextTrigramIndexTracker();
        off.Configure(false);
        var before = new List<string>();
        foreach (var (pattern, database, limit) in Cases)
        {
            before.Add(await QueryStoreSourceAsync(data, pattern, database, limit, off, ct));
        }

        var ready = await BuildAsync(data, admin, ct);

        for (var i = 0; i < Cases.Length; i++)
        {
            var (pattern, database, limit) = Cases[i];
            var after = await QueryStoreSourceAsync(data, pattern, database, limit, ready, ct);
            AssertSameAnswer(before[i], after, $"'{pattern}' in {database ?? "every database"}, limit {limit}");
        }

        /* What the rows are, so the comparison is not two empty answers. */
        Assert.StartsWith("empty\n0\nFalse", before[0], StringComparison.Ordinal);
        Assert.Equal(new[] { ("db_1", 1009L), ("db_4", 12L), ("db_6", 2006L) }, Keys(before[1]));
        Assert.Equal(new[] { ("db_0", 120L), ("db_0", 128L), ("db_0", 1200L), ("db_0", 1208L), ("db_0", 1216L) }, Keys(before[2]));
        Assert.StartsWith("ok\n5\nTrue", before[2], StringComparison.Ordinal);
        Assert.StartsWith("ok\n3\nTrue", before[3], StringComparison.Ordinal);
        Assert.True(Keys(before[3]).All(k => k.Database == "db_3"));
        Assert.StartsWith("ok\n4\nTrue", before[4], StringComparison.Ordinal);
        Assert.Equal(new[] { ("db_0", 8L), ("db_0", 16L) }, Keys(before[5]));
    }

    private static async Task<JsonElement> ExplainAsync(NpgsqlConnection connection, string pattern, int limit, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + DarlingMcpFindQueriesByTextTools.QueryStoreSql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DarlingMcpFindQueriesByTextTools.EscapeLike(pattern) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = limit + 1 });
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        using var document = JsonDocument.Parse(json);
        return document.RootElement[0].GetProperty("Plan").Clone();
    }

    private static IEnumerable<string> IndexNames(JsonElement node)
    {
        if (node.TryGetProperty("Index Name", out var name))
        {
            yield return name.GetString()!;
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                foreach (var inner in IndexNames(child))
                {
                    yield return inner;
                }
            }
        }
    }

    private static IEnumerable<JsonElement> Nodes(JsonElement node)
    {
        yield return node;
        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                foreach (var inner in Nodes(child))
                {
                    yield return inner;
                }
            }
        }
    }

    private static double RowsRemovedByFilter(JsonElement plan) =>
        Nodes(plan).Sum(n => n.TryGetProperty("Rows Removed by Filter", out var removed) ? removed.GetDouble() : 0);

    private static string IndexCondition(JsonElement plan, string index) =>
        Nodes(plan).First(n => n.TryGetProperty("Index Name", out var name) && name.GetString() == index).GetProperty("Index Cond").GetString()!;

    private static long Buffers(JsonElement plan) =>
        plan.GetProperty("Shared Hit Blocks").GetInt64() + plan.GetProperty("Shared Read Blocks").GetInt64();

    /// <summary>The statement the tool runs, with the parameters it binds: without the index a pattern the server does not hold walks every
    /// one of the server's rows; with it the planner reads the trigram index and touches a fraction of the pages, for that pattern and for a
    /// rare one. A pattern every text holds still walks the key and stops at the cap.</summary>
    [Fact]
    public async Task TheReadyIndex_ServesARarePattern_AndACommonOneKeepsTheKeyWalk()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, data, admin) = await OpenAsync(ct);
        await using var _s = scratch;
        await using var _d = data;
        await using var _a = admin;

        var noMatchBefore = await ExplainAsync(admin, "zz_no_such_table_zz", 20, ct);
        var rareBefore = await ExplainAsync(admin, "Col_12 FROM", 20, ct);
        Assert.DoesNotContain(QueryStoreTextTrigramIndex.IndexBareName, IndexNames(noMatchBefore));

        await BuildAsync(data, admin, ct);

        var noMatchAfter = await ExplainAsync(admin, "zz_no_such_table_zz", 20, ct);
        var rareAfter = await ExplainAsync(admin, "Col_12 FROM", 20, ct);
        var common = await ExplainAsync(admin, "dbo.Orders_", 20, ct);

        Assert.Contains(QueryStoreTextTrigramIndex.IndexBareName, IndexNames(noMatchAfter));
        Assert.Contains(QueryStoreTextTrigramIndex.IndexBareName, IndexNames(rareAfter));
        Assert.True(Buffers(noMatchAfter) * 4 < Buffers(noMatchBefore), $"no match: {Buffers(noMatchBefore)} buffers without the index, {Buffers(noMatchAfter)} with it");
        Assert.True(Buffers(rareAfter) * 4 < Buffers(rareBefore), $"rare: {Buffers(rareBefore)} buffers without the index, {Buffers(rareAfter)} with it");

        Assert.DoesNotContain(QueryStoreTextTrigramIndex.IndexBareName, IndexNames(common));
        Assert.Contains("query_store_text_pkey", IndexNames(common));
        Assert.Equal(21.0, common.GetProperty("Actual Rows").GetDouble());
    }

    /// <summary>A store an earlier build indexed on the text alone, then this build's pass: every case answers with the same rows as through
    /// the replaced index, and the one-server plan reads the index keyed by server with the server in its condition, so the other servers'
    /// matches never reach the table (through the replaced index they are read, rechecked and then filtered out).</summary>
    [Fact]
    public async Task TheServerKeyedIndex_AnswersAsTheIndexOnTheTextAloneDid_AndAppliesTheServerInsideTheIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, data, admin) = await OpenAsync(ct);
        await using var _s = scratch;
        await using var _d = data;
        await using var _a = admin;

        await QueryStoreTextTrigramIndexLiveTests.CreateReplacedIndexAsync(admin, ct);
        await DarlingMcpTestData.ExecAsync(admin, ct, "ANALYZE collect.query_store_text");
        const string replaced = "ix_query_store_text_trgm";
        var off = new QueryTextTrigramIndexTracker();
        off.Configure(false);
        var before = new List<string>();
        foreach (var (pattern, database, limit) in Cases)
        {
            before.Add(await QueryStoreSourceAsync(data, pattern, database, limit, off, ct));
        }

        var rareBefore = await ExplainAsync(admin, "Col_12 FROM", 20, ct);
        Assert.Contains(replaced, IndexNames(rareBefore));
        Assert.DoesNotContain("server_id", IndexCondition(rareBefore, replaced), StringComparison.Ordinal);
        Assert.True(RowsRemovedByFilter(rareBefore) > 0, "the index on the text alone hands back the other servers' matches for the filter");

        var ready = await BuildAsync(data, admin, ct);
        await using (var gone = new NpgsqlCommand($"SELECT to_regclass('{QueryStoreTextTrigramIndex.ReplacedIndexName}') IS NULL", admin))
        {
            Assert.True((bool)(await gone.ExecuteScalarAsync(ct))!);
        }

        for (var i = 0; i < Cases.Length; i++)
        {
            var (pattern, database, limit) = Cases[i];
            var after = await QueryStoreSourceAsync(data, pattern, database, limit, ready, ct);
            AssertSameAnswer(before[i], after, $"'{pattern}' in {database ?? "every database"}, limit {limit}");
        }

        Assert.Equal(new[] { ("db_1", 1009L), ("db_4", 12L), ("db_6", 2006L) }, Keys(before[1]));
        foreach (var pattern in new[] { "Col_12 FROM", "zz_no_such_table_zz" })
        {
            var plan = await ExplainAsync(admin, pattern, 20, ct);
            Assert.Contains(QueryStoreTextTrigramIndex.IndexBareName, IndexNames(plan));
            Assert.Contains("server_id", IndexCondition(plan, QueryStoreTextTrigramIndex.IndexBareName), StringComparison.Ordinal);
            Assert.Equal(0.0, RowsRemovedByFilter(plan));
        }
    }
}
