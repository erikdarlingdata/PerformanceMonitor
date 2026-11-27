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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Each test creates its own scratch database through ScratchPostgres and
   works entirely inside it, so it cannot race live collection. */
/// <summary>
/// #9219: the all-servers window join of <c>find_queries_by_text</c> probes the digests of the newest span in the order of their newest sample and
/// stops as soon as the page is full, and the newest spans are cut by a count of sample rows, so the work of the first statement does not depend on
/// how many samples an hour holds. The budget is the statement-count clock the other classes use, never a sleep.
/// </summary>
public sealed class FindQueriesByTextNewestFirstLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the find_queries_by_text newest-first live tests.";

    /// <summary>Sample rows, one digest each, five seconds apart going back from the newest.</summary>
    private const int Samples = 600;

    private const int SpanRows = 100;

    /// <summary>The texts the search can match: every sample's text names dbo.Common, and 1,100 more texts that no sample carries name both patterns, so
    /// each is common and the search joins the window (more texts than the probe takes). Only samples 50, 120 and 250 name dbo.Rare.</summary>
    private static async Task<(ScratchPostgres Scratch, DateTime Newest)> OpenScratchAsync(CancellationToken ct)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        try
        {
            await using var c = new NpgsqlConnection(scratch.ConnectionString);
            await c.OpenAsync(ct);
            await PgMigrations.MigrateAsync(c, ct);
            await DarlingMcpTestData.RegisterServerAsync(c, 1, "nf-a", ct);
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
                  SELECT sha256(('filler' || i)::text::bytea), 'SELECT ' || i || ' FROM dbo.Common JOIN dbo.Rare', timezone('UTC', now())
                  FROM generate_series(1, 1100) AS i");
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
                  SELECT sha256(('s' || i)::text::bytea),
                         'SELECT ' || i || ' FROM dbo.Common' || CASE WHEN i IN (50, 120, 250) THEN ' JOIN dbo.Rare' ELSE '' END,
                         timezone('UTC', now())
                  FROM generate_series(0, " + (Samples - 1) + ") AS i");

            var newest = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow) - TimeSpan.FromSeconds(5);
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text_digest)
                  SELECT $2::bigint + i, $1::timestamp - (i * interval '5 seconds'), 1, 'nf-a', 'Books', '0xH' || lpad(i::text, 4, '0'),
                         sha256(('s' || i)::text::bytea)
                  FROM generate_series(0, " + (Samples - 1) + ") AS i", newest, CollectionIdGenerator.Next());

            await DarlingMcpTestData.ExecAsync(c, ct, "CREATE EXTENSION IF NOT EXISTS pg_trgm");
            await DarlingMcpTestData.ExecAsync(c, ct,
                "CREATE INDEX IF NOT EXISTS " + FindQueriesByTextIndexScopeLiveTests.IndexShortName + " ON collect.query_text_dim USING gin (query_text gin_trgm_ops)");
            foreach (var table in new[] { "collect.query_text_dim", "query_stats" })
            {
                await DarlingMcpTestData.ExecAsync(c, ct, "ANALYZE " + table);
            }

            return (scratch, newest);
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

    /// <summary>A budget that reads as spent once <paramref name="spentAfter"/> statements have started, with a row cap small enough that the store's
    /// 600 samples are six newest spans.</summary>
    private static DarlingMcpFindQueriesByTextTools.SourceBudget Budget(int? spentAfter, Action<int>? counted = null, int spanRows = SpanRows)
    {
        var started = 0;
        return new DarlingMcpFindQueriesByTextTools.SourceBudget(DarlingMcpFindQueriesByTextTools.BudgetMilliseconds)
        {
            SpanRows = spanRows,
            ElapsedClock = () => spentAfter is { } n && started >= n
                ? TimeSpan.FromMilliseconds(DarlingMcpFindQueriesByTextTools.BudgetMilliseconds + 1)
                : TimeSpan.Zero,
            BeforeStatement = (_, _, _) => { started++; counted?.Invoke(started); return Task.CompletedTask; },
        };
    }

    private static async Task<JsonElement> PlanCacheAsync(
        string connectionString, string pattern, int limit, DarlingMcpFindQueriesByTextTools.SourceBudget budget, CancellationToken ct)
    {
        await using var data = NpgsqlDataSource.Create(connectionString);
        var reply = await DarlingMcpFindQueriesByTextTools.FindQueriesByText(
            data, pattern, null, null, limit, McpDataDetail.Full, DarlingMcpFindQueriesByTextTools.SourceBudget.Default, budget, ct, ReadyTracker());
        return JsonDocument.Parse(reply).RootElement.GetProperty("sources").GetProperty("plan_cache").Clone();
    }

    private static string[] Hashes(JsonElement planCache) =>
        planCache.GetProperty("rows").EnumerateArray().Select(r => r.GetProperty("query_hash").GetString()!).ToArray();

    private static string Hash(int sample) => "0xH" + sample.ToString("0000", CultureInfo.InvariantCulture);

    /// <summary>A common pattern answers from the newest digests: three rows, the three newest samples, in order, in one statement, and the search is
    /// complete because the page filled. A page too big for a first span of 30 samples stops after it with the span's rows and the span it covered.</summary>
    [Fact]
    public async Task ACommonPattern_AnswersWithTheNewestRows_ThroughTheFirstSpanOnly()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, newest) = await OpenScratchAsync(ct);
        await using var _ = scratch;
        var statements = 0;

        var planCache = await PlanCacheAsync(scratch.ConnectionString, "dbo.Common", 3, Budget(null, n => statements = n), ct);

        Assert.Equal("ok", planCache.GetProperty("status").GetString());
        Assert.Equal("lookup", planCache.GetProperty("sample_read").GetString());
        Assert.Equal([Hash(0), Hash(1), Hash(2)], Hashes(planCache));
        Assert.True(planCache.GetProperty("truncated").GetBoolean());
        Assert.True(planCache.GetProperty("searched").GetProperty("complete").GetBoolean());
        Assert.Equal(1, statements);

        /* A page the first span cannot fill (the most a page holds is 50 rows, so the span is cut at 30 sample rows here), with the budget spent
           after that statement: the 30 digests of the span come back and the reply covers the span (to the 30th sample), not the hour. */
        const int smallSpan = 30;
        var stopped = await PlanCacheAsync(scratch.ConnectionString, "dbo.Common", DarlingMcpFindQueriesByTextTools.MaxLimit, Budget(1, spanRows: smallSpan), ct);
        Assert.Equal("stopped", stopped.GetProperty("status").GetString());
        Assert.Equal(Enumerable.Range(0, smallSpan).Select(Hash).ToArray(), Hashes(stopped));
        Assert.Equal(newest - TimeSpan.FromSeconds((smallSpan - 1) * 5), stopped.GetProperty("searched").GetProperty("searched_from").GetDateTime().ToUniversalTime());
    }

    /// <summary>The early stop is in the plan: the statement for a span of 100 digests that all name the pattern, for a page of four, reads the text
    /// table for a few dozen texts, not for all 100 (a statement that tested every digest of the span reads 100).</summary>
    [Fact]
    public async Task TheFirstSpansStatement_StopsProbingTextsOnceThePageIsFull()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, _) = await OpenScratchAsync(ct);
        await using var _ = scratch;
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using var c = await data.OpenConnectionAsync(ct);
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

        await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + DarlingMcpFindQueriesByTextTools.PlanCacheAllServersLookupSpanSql, c);
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DarlingMcpFindQueriesByTextTools.EscapeLike("dbo.Common") });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = 4 });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = now - TimeSpan.FromSeconds(SpanRows * 5) });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = now.AddDays(1) });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = Array.Empty<int>() });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = Array.Empty<string>() });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = Array.Empty<string>() });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = 4 * DarlingMcpFindQueriesByTextTools.AllServersLookupMatchFactor });
        var plan = JsonDocument.Parse((string)(await explain.ExecuteScalarAsync(ct))!).RootElement[0].GetProperty("Plan");

        var read = FindQueriesByTextLookupSpanLiveTests.TextRowsRead(plan);
        Assert.True(read < 60, $"the statement probed {read} texts for a page of four on a span of {SpanRows} digests that all match");
    }

    /// <summary>A pattern few of the newest texts name, with a budget that runs out after two statements: the rows found in the two spans come back,
    /// and the span named is the one those two statements covered (from the sample 200 rows back), not the hour a time-cut span would have claimed.</summary>
    [Fact]
    public async Task ARarePattern_ThatStopsAfterTwoSpans_ReturnsTheNewestMatches_AndTheSpanItCovered()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, newest) = await OpenScratchAsync(ct);
        await using var _ = scratch;
        var statements = 0;

        var planCache = await PlanCacheAsync(scratch.ConnectionString, "dbo.Rare", 20, Budget(2, n => statements = n), ct);

        Assert.Equal("stopped", planCache.GetProperty("status").GetString());
        Assert.Equal([Hash(50), Hash(120)], Hashes(planCache));
        Assert.Equal(2, statements);
        var searched = planCache.GetProperty("searched");
        Assert.False(searched.GetProperty("complete").GetBoolean());
        var coveredFrom = newest - TimeSpan.FromSeconds((2 * SpanRows - 1) * 5);
        Assert.Equal(coveredFrom, searched.GetProperty("searched_from").GetDateTime().ToUniversalTime());
    }

    /// <summary>A rare pattern with room in the budget finishes with the rows of the whole-window join, in its order, over spans cut by a row count
    /// and a last one that takes what is left of the window.</summary>
    [Fact]
    public async Task AFinishedSearch_ListsWhatTheWholeWindowJoinLists()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, _) = await OpenScratchAsync(ct);
        await using var _ = scratch;
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);

        var planCache = await PlanCacheAsync(scratch.ConnectionString, "dbo.Rare", 20, Budget(null), ct);

        Assert.Equal("ok", planCache.GetProperty("status").GetString());
        Assert.True(planCache.GetProperty("searched").GetProperty("complete").GetBoolean());
        Assert.Equal([Hash(50), Hash(120), Hash(250)], Hashes(planCache));
        await using var c = await data.OpenConnectionAsync(ct);
        await using var join = new NpgsqlCommand(DarlingMcpFindQueriesByTextTools.PlanCacheAllServersSql, c);
        join.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DarlingMcpFindQueriesByTextTools.EscapeLike("dbo.Rare") });
        join.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = 21 });
        join.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Timestamp,
            Value = DateTime.SpecifyKind(DateTime.UtcNow - TimescaleSupport.RawRetentionSpan, DateTimeKind.Unspecified),
        });
        var expected = new List<string>();
        await using var reader = await join.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            expected.Add(reader.GetString(1));
        }

        Assert.Equal(expected, Hashes(planCache));
    }

    /// <summary>One query_hash with more matching digests than the first match limit holds (a statement with many literal variants), all sampled at
    /// the newest time, then 300 other matching keys: a page of three. The first statement's limit is spent on the one key, so the page is short
    /// and the statement says it was capped. The rerun keeps its limit and excludes that key, so it probes a few dozen texts more and fills the page,
    /// and the search never reads the span's 340 sightings. A rerun with no limit probes every sighting that is left.</summary>
    [Fact]
    public async Task AKeyWithManyDigests_DoesNotSendTheRerunToProbeTheWholeSpan()
    {
        var ct = TestContext.Current.CancellationToken;
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        const int limit = 3;
        var bigDigests = DarlingMcpFindQueriesByTextTools.AllServersLookupMatchFactor * (limit + 1) + 8;
        const int others = 300;
        var newest = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow) - TimeSpan.FromSeconds(5);
        await using (var c = new NpgsqlConnection(scratch.ConnectionString))
        {
            await c.OpenAsync(ct);
            await PgMigrations.MigrateAsync(c, ct);
            await DarlingMcpTestData.RegisterServerAsync(c, 1, "nf-a", ct);
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
                  SELECT sha256(('filler' || i)::text::bytea), 'SELECT ' || i || ' FROM dbo.Common', timezone('UTC', now())
                  FROM generate_series(1, 1100) AS i");
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
                  SELECT sha256(('b' || i)::text::bytea), 'SELECT ' || i || ' FROM dbo.Common WHERE x = ' || i, timezone('UTC', now())
                  FROM generate_series(1, " + bigDigests + @") AS i");
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
                  SELECT sha256(('o' || i)::text::bytea), 'SELECT ' || i || ' FROM dbo.Common', timezone('UTC', now())
                  FROM generate_series(1, " + others + ") AS i");
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text_digest)
                  SELECT $2::bigint + i, $1::timestamp, 1, 'nf-a', 'Books', '0xA0000', sha256(('b' || i)::text::bytea)
                  FROM generate_series(1, " + bigDigests + @") AS i", newest, CollectionIdGenerator.Next());
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text_digest)
                  SELECT $2::bigint + i, $1::timestamp - (i * interval '5 seconds'), 1, 'nf-a', 'Books', '0xH' || lpad(i::text, 4, '0'),
                         sha256(('o' || i)::text::bytea)
                  FROM generate_series(1, " + others + ") AS i", newest, CollectionIdGenerator.Next() + 1_000_000);
            await DarlingMcpTestData.ExecAsync(c, ct, "CREATE EXTENSION IF NOT EXISTS pg_trgm");
            await DarlingMcpTestData.ExecAsync(c, ct,
                "CREATE INDEX IF NOT EXISTS " + FindQueriesByTextIndexScopeLiveTests.IndexShortName + " ON collect.query_text_dim USING gin (query_text gin_trgm_ops)");
            foreach (var table in new[] { "collect.query_text_dim", "query_stats" })
            {
                await DarlingMcpTestData.ExecAsync(c, ct, "ANALYZE " + table);
            }
        }

        /* One pooled connection, so the text table's scan counts the tool's backend adds are flushed by the flush this test asks for on the same one. */
        await using var data = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { MaxPoolSize = 1 }.ConnectionString);

        async Task<long> TextScansAsync()
        {
            await using (var flush = data.CreateCommand("SELECT pg_stat_force_next_flush()"))
            {
                await flush.ExecuteScalarAsync(ct);
            }

            await using var read = data.CreateCommand(
                "SELECT coalesce(seq_scan, 0) + coalesce(idx_scan, 0) FROM pg_stat_user_tables WHERE relid = 'collect.query_text_dim'::regclass");
            return (long)(await read.ExecuteScalarAsync(ct))!;
        }

        var before = await TextScansAsync();
        var budget = new DarlingMcpFindQueriesByTextTools.SourceBudget(DarlingMcpFindQueriesByTextTools.BudgetMilliseconds) { SpanRows = 5000 };
        var reply = await DarlingMcpFindQueriesByTextTools.FindQueriesByText(
            data, "dbo.Common", null, null, limit, McpDataDetail.Full, DarlingMcpFindQueriesByTextTools.SourceBudget.Default, budget, ct, ReadyTracker());
        var scans = await TextScansAsync() - before;
        var planCache = JsonDocument.Parse(reply).RootElement.GetProperty("sources").GetProperty("plan_cache");

        Assert.Equal("ok", planCache.GetProperty("status").GetString());
        Assert.Equal("lookup", planCache.GetProperty("sample_read").GetString());
        Assert.Equal(["0xA0000", "0xH0001", "0xH0002"], Hashes(planCache));
        Assert.True(planCache.GetProperty("truncated").GetBoolean());
        Assert.True(planCache.GetProperty("searched").GetProperty("complete").GetBoolean());
        var sightings = bigDigests + others;
        Assert.True(scans < sightings / 2, $"the search scanned the text table {scans} times for a page of {limit} over a span of {sightings} sightings");
    }
}
