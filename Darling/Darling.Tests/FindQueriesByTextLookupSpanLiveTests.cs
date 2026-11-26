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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Each test creates its own scratch database through ScratchPostgres and
   works entirely inside it, so it cannot race live collection. */
/// <summary>
/// #9219: the all-servers window join of <c>find_queries_by_text</c> tests the pattern on the texts of the newest spans' own samples, not on every text
/// in the store. A budget cannot model the difference without a sleep (the cost of a whole-table text match grows with the store, and a test store
/// is small), so the first test counts the rows the statement reads from the text table, which does not depend on timing.
/// </summary>
public sealed class FindQueriesByTextLookupSpanLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the find_queries_by_text lookup-span live tests.";

    private const int FillerTexts = 6000;

    /// <summary>6,000 texts name dbo.Common and no sample carries them (the pattern is common: past the probe's cap). Server lk-a ran four statements in
    /// the newest span (L1 and L2 name it, L3 does not, L4 has no digest), L2 again with another text 90 minutes ago and L6 ninety-five minutes ago (the
    /// second span); the newest sighting of the L2 key is the first span's. Server lk-b ran L1's statement three days ago.</summary>
    private static async Task<ScratchPostgres> OpenScratchAsync(CancellationToken ct)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        try
        {
            await using var c = new NpgsqlConnection(scratch.ConnectionString);
            await c.OpenAsync(ct);
            await PgMigrations.MigrateAsync(c, ct);
            await DarlingMcpTestData.RegisterServerAsync(c, 1, "lk-a", ct);
            await DarlingMcpTestData.RegisterServerAsync(c, 2, "lk-b", ct);
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
                  SELECT sha256(('filler' || i)::text::bytea), 'SELECT ' || i || ' FROM dbo.Common', timezone('UTC', now())
                  FROM generate_series(1, " + FillerTexts + ") AS i");
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
                  VALUES (sha256('l1'::text::bytea), 'SELECT * FROM dbo.Common WHERE Id = @p', timezone('UTC', now())),
                         (sha256('l2'::text::bytea), 'SELECT COUNT(*) FROM dbo.Common', timezone('UTC', now())),
                         (sha256('l3'::text::bytea), 'SELECT 1 FROM dbo.Other', timezone('UTC', now())),
                         (sha256('l5'::text::bytea), 'DELETE FROM dbo.Common', timezone('UTC', now())),
                         (sha256('l6'::text::bytea), 'TRUNCATE TABLE dbo.Common', timezone('UTC', now()))");

            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            async Task Stat(int server, string? database, string hash, string? text, TimeSpan age) =>
                await DarlingMcpTestData.ExecAsync(c, ct,
                    @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text_digest)
                      VALUES ($1,$2,$3,$4,$5,$6,CASE WHEN $7::text IS NULL THEN NULL ELSE sha256($7::text::bytea) END)",
                    CollectionIdGenerator.Next(), now - age, server, server == 1 ? "lk-a" : "lk-b", (object?)database ?? DBNull.Value, hash, (object?)text ?? DBNull.Value);

            await Stat(1, "Books", "0xL1", "l1", TimeSpan.FromMinutes(20));
            await Stat(1, "Books", "0xL2", "l2", TimeSpan.FromMinutes(30));
            await Stat(1, "Books", "0xL3", "l3", TimeSpan.FromMinutes(25));
            await Stat(1, "Books", "0xL4", null, TimeSpan.FromMinutes(15));
            await Stat(1, "Books", "0xL2", "l5", TimeSpan.FromMinutes(90));
            await Stat(1, null, "0xL6", "l6", TimeSpan.FromMinutes(95));
            await Stat(2, "Books", "0xL1", "l1", TimeSpan.FromDays(3));

            await DarlingMcpTestData.ExecAsync(c, ct, "CREATE EXTENSION IF NOT EXISTS pg_trgm");
            await DarlingMcpTestData.ExecAsync(c, ct,
                "CREATE INDEX IF NOT EXISTS " + FindQueriesByTextIndexScopeLiveTests.IndexShortName + " ON collect.query_text_dim USING gin (query_text gin_trgm_ops)");
            foreach (var table in new[] { "collect.query_text_dim", "query_stats" })
            {
                await DarlingMcpTestData.ExecAsync(c, ct, "ANALYZE " + table);
            }

            return scratch;
        }
        catch
        {
            await scratch.DisposeAsync();
            throw;
        }
    }

    private static QueryTextTrigramIndexTracker ReadyTracker()
    {
        var tracker = new QueryTextTrigramIndexTracker();
        tracker.Configure(true);
        return tracker;
    }

    /// <summary>The text-table rows a plan examined: each scan of <c>query_text_dim</c> counts the rows it returned, the rows it threw away and the
    /// rows its index recheck threw away, times its loops.</summary>
    internal static long TextRowsRead(JsonElement node)
    {
        long total = 0;
        if (node.TryGetProperty("Relation Name", out var relation) && relation.GetString() == "query_text_dim")
        {
            var loops = node.GetProperty("Actual Loops").GetInt64();
            long perLoop = 0;
            foreach (var name in new[] { "Actual Rows", "Rows Removed by Filter", "Rows Removed by Index Recheck" })
            {
                if (node.TryGetProperty(name, out var value))
                {
                    perLoop += (long)Math.Ceiling(value.GetDouble());
                }
            }

            total += perLoop * Math.Max(loops, 1);
        }

        if (node.TryGetProperty("Plans", out var plans))
        {
            foreach (var child in plans.EnumerateArray())
            {
                total += TextRowsRead(child);
            }
        }

        return total;
    }

    /// <summary>A common pattern's first span reads the texts of that span's own samples, not the text table: on a store whose text table holds 6,000
    /// texts that all name the pattern and whose first span carries four of them, the statement the tool runs for the newest span examines a few dozen
    /// text rows at most. A statement that matched the whole table in the span would examine all 6,000.</summary>
    [Fact]
    public async Task TheNewestSpansStatement_ExaminesTheTextsOfItsOwnSamples_NotTheTextTable()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await OpenScratchAsync(ct);
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var span = DarlingMcpFindQueriesByTextTools.RawTierSpans(now - TimescaleSupport.RawRetentionSpan, now)[0];

        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using var c = await data.OpenConnectionAsync(ct);
        await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + DarlingMcpFindQueriesByTextTools.PlanCacheAllServersLookupSpanSql, c);
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DarlingMcpFindQueriesByTextTools.EscapeLike("dbo.Common") });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = 21 });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = span.Floor });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = span.Ceiling });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = Array.Empty<int>() });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = Array.Empty<string>() });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = Array.Empty<string>() });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = 21 * DarlingMcpFindQueriesByTextTools.AllServersLookupMatchFactor });
        var plan = JsonDocument.Parse((string)(await explain.ExecuteScalarAsync(ct))!).RootElement[0].GetProperty("Plan");

        var read = TextRowsRead(plan);
        Assert.True(read < 100, $"the newest span's statement examined {read} text rows on a store with {FillerTexts} texts that name the pattern");
    }

    /// <summary>A finished all-servers search over a common pattern lists what the whole-window join lists, in its order, when the newest span and
    /// the spans after it both hold rows: a key whose newest sighting is in the first span is not listed again from the second, a key seen only in
    /// the second span is, a statement with no digest or a text that does not name the pattern never is, and the same query_hash on the other
    /// server is its own row. A span reads three samples a part here, so the first ends at the samples of half an hour ago and the search reads
    /// the matched texts and goes on: a seed of six samples is otherwise read whole by the first statement, with nothing for a second span.</summary>
    [Fact]
    public async Task AFinishedSearch_ListsWhatTheWholeWindowJoinLists_WithRowsInTheLookupSpansAndTheLaterOnes()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);

        var reply = await DarlingMcpFindQueriesByTextTools.FindQueriesByText(
            data, "dbo.Common", null, null, 20, McpDataDetail.Full,
            DarlingMcpFindQueriesByTextTools.SourceBudget.Default,
            new DarlingMcpFindQueriesByTextTools.SourceBudget(DarlingMcpFindQueriesByTextTools.BudgetMilliseconds) { SpanRows = 3 }, ct, ReadyTracker());
        var planCache = JsonDocument.Parse(reply).RootElement.GetProperty("sources").GetProperty("plan_cache");

        Assert.Equal("ok", planCache.GetProperty("status").GetString());
        Assert.Equal("join", planCache.GetProperty("sample_read").GetString());
        Assert.True(planCache.GetProperty("searched").GetProperty("complete").GetBoolean());
        var keys = planCache.GetProperty("rows").EnumerateArray()
            .Select(r => r.GetProperty("server").GetString() + "@" + r.GetProperty("query_hash").GetString() + "@" + r.GetProperty("database_name").GetString())
            .ToArray();
        Assert.Equal(await WholeWindowJoinKeysAsync(data, "dbo.Common", ct), keys);
        Assert.Equal(new[] { "lk-a@0xL1@Books", "lk-a@0xL2@Books", "lk-a@0xL6@", "lk-b@0xL1@Books" }, keys);
    }

    /// <summary>The whole-window join run directly with the tool's own parameters, its rows as <c>server@query_hash@database</c> in its order.</summary>
    private static async Task<string[]> WholeWindowJoinKeysAsync(NpgsqlDataSource data, string pattern, CancellationToken ct)
    {
        await using var c = await data.OpenConnectionAsync(ct);
        await using var join = new NpgsqlCommand(
            "SELECT s.server_name, j.query_hash, j.database_name FROM (" + DarlingMcpFindQueriesByTextTools.PlanCacheAllServersSql.TrimEnd() +
            ") AS j JOIN collect.servers AS s ON s.server_id = j.server_id ORDER BY j.last_collected DESC, j.server_id, j.query_hash, j.database_name", c);
        join.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DarlingMcpFindQueriesByTextTools.EscapeLike(pattern) });
        join.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = 21 });
        join.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Timestamp,
            Value = DateTime.SpecifyKind(DateTime.UtcNow - TimescaleSupport.RawRetentionSpan, DateTimeKind.Unspecified),
        });
        var rows = new List<string>();
        await using var reader = await join.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(reader.GetString(0) + "@" + reader.GetString(1) + "@" + (reader.IsDBNull(2) ? string.Empty : reader.GetString(2)));
        }

        return rows.ToArray();
    }
}
