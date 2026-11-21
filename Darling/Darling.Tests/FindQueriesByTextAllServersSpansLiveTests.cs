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
/// #8570: the all-servers plan-cache probe of <c>find_queries_by_text</c> searches the raw tier newest first, in the spans the one-server
/// search uses, inside its one budget, so a budget that runs out hands back the newest matches and says how much of the window was searched,
/// and every server, query_hash and database is listed once, with its newest sighting. The budget is spent through the source's test clock, so
/// where it runs out is a fixed point, not a race: the clock reads zero until the first span's statement has run and past the budget after it.
/// The window join past the digest cap stays one statement, and its stopped message says so without a span count.
/// </summary>
public sealed class FindQueriesByTextAllServersSpansLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the find_queries_by_text all-servers span live tests.";

    private const string LedgerHash = "0xAA01";
    private const string CountHash = "0xBB02";
    private const string OtherHash = "0xDD04";

    /// <summary>Two texts that name dbo.Ledger and one that does not, on two servers. The ledger statement ran on span-a half an hour ago, two hours
    /// ago and three days ago, and on span-b, with the same query_hash and database, three days and an hour ago; the count statement ran on
    /// span-a three days and a half hour ago; the other statement ran on span-a half an hour ago. Only span-a's ledger and other statements sit in
    /// the newest span.</summary>
    private static async Task<(ScratchPostgres Scratch, DateTime NewestSample)> OpenScratchAsync(CancellationToken ct)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, 1, "span-a", ct);
        await DarlingMcpTestData.RegisterServerAsync(c, 2, "span-b", ct);
        await DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
              VALUES (sha256('ledger'::text::bytea), 'SELECT * FROM dbo.Ledger WHERE LedgerId = @p', timezone('UTC', now())),
                     (sha256('count'::text::bytea), 'SELECT COUNT(*) FROM dbo.Ledger', timezone('UTC', now())),
                     (sha256('other'::text::bytea), 'SELECT 1 FROM dbo.Other', timezone('UTC', now()))");

        var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
        async Task Stat(int server, string name, string hash, string text, TimeSpan age) =>
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text_digest)
                  VALUES ($1,$2,$3,$4,'Books',$5,sha256($6::text::bytea))",
                CollectionIdGenerator.Next(), now - age, server, name, hash, text);

        await Stat(1, "span-a", LedgerHash, "ledger", TimeSpan.FromMinutes(30));
        await Stat(1, "span-a", LedgerHash, "ledger", TimeSpan.FromHours(2));
        await Stat(1, "span-a", LedgerHash, "ledger", TimeSpan.FromDays(3));
        await Stat(2, "span-b", LedgerHash, "ledger", TimeSpan.FromDays(3) + TimeSpan.FromHours(1));
        await Stat(1, "span-a", CountHash, "count", TimeSpan.FromDays(3) + TimeSpan.FromMinutes(30));
        await Stat(1, "span-a", OtherHash, "other", TimeSpan.FromMinutes(30));

        await DarlingMcpTestData.ExecAsync(c, ct, "CREATE EXTENSION IF NOT EXISTS pg_trgm");
        await DarlingMcpTestData.ExecAsync(c, ct,
            "CREATE INDEX IF NOT EXISTS " + FindQueriesByTextIndexScopeLiveTests.IndexShortName + " ON collect.query_text_dim USING gin (query_text gin_trgm_ops)");
        return (scratch, now - TimeSpan.FromMinutes(30));
    }

    private static QueryTextTrigramIndexTracker ReadyTracker()
    {
        var tracker = new QueryTextTrigramIndexTracker();
        tracker.Configure(true);
        return tracker;
    }

    private static async Task<JsonElement> PlanCacheAsync(
        string connectionString, string pattern, DarlingMcpFindQueriesByTextTools.SourceBudget planCache, int limit, CancellationToken ct)
    {
        await using var data = NpgsqlDataSource.Create(connectionString);
        var reply = await DarlingMcpFindQueriesByTextTools.FindQueriesByText(
            data, pattern, null, null, limit, McpDataDetail.Full,
            DarlingMcpFindQueriesByTextTools.SourceBudget.Default, planCache, ct, ReadyTracker());
        return JsonDocument.Parse(reply).RootElement.GetProperty("sources").GetProperty("plan_cache");
    }

    private static string[] Keys(JsonElement planCache) =>
        planCache.GetProperty("rows").EnumerateArray()
            .Select(r => r.GetProperty("server").GetString() + "@" + r.GetProperty("query_hash").GetString() + "@" + r.GetProperty("database_name").GetString())
            .ToArray();

    /// <summary>How many statements an uncut search runs: one per span of the raw window.</summary>
    private static int SpanCount()
    {
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        return DarlingMcpFindQueriesByTextTools.RawTierSpans(now - TimescaleSupport.RawRetentionSpan, now).Count;
    }

    /// <summary>#9219: the newest span a search reads first, a small slice of the window, not a sixteenth of it.</summary>
    private static double FirstSpanSeconds() =>
        (TimescaleSupport.RawRetentionSpan / DarlingMcpFindQueriesByTextTools.RawTierSpanParts).TotalSeconds;

    private static string Stamp(DateTime naiveUtc) => DateTime.SpecifyKind(naiveUtc, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);

    /// <summary>The whole budget, counting the statements the source runs.</summary>
    private static DarlingMcpFindQueriesByTextTools.SourceBudget Counting(Action counted) =>
        new(DarlingMcpFindQueriesByTextTools.BudgetMilliseconds)
        {
            BeforeStatement = (_, _, _) => { counted(); return Task.CompletedTask; },
        };

    [Fact]
    public async Task ABudgetThatRunsOutAfterTheNewestSpan_KeepsThatSpansRows_AndSaysHowMuchWasSearched()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, newestSample) = await OpenScratchAsync(ct);
        await using var _ = scratch;

        var statements = 0;
        var budget = new DarlingMcpFindQueriesByTextTools.SourceBudget(DarlingMcpFindQueriesByTextTools.BudgetMilliseconds)
        {
            ElapsedClock = () => statements >= 1 ? TimeSpan.FromMilliseconds(DarlingMcpFindQueriesByTextTools.BudgetMilliseconds + 1) : TimeSpan.Zero,
            BeforeStatement = (_, _, _) => { statements++; return Task.CompletedTask; },
        };

        var planCache = await PlanCacheAsync(scratch.ConnectionString, "dbo.Ledger", budget, 20, ct);

        Assert.Equal("stopped", planCache.GetProperty("status").GetString());
        Assert.Equal("probe", planCache.GetProperty("sample_read").GetString());
        Assert.Equal(2, planCache.GetProperty("matched_texts").GetInt32());
        Assert.Equal(["span-a@" + LedgerHash + "@Books"], Keys(planCache));
        Assert.Equal(Stamp(newestSample), planCache.GetProperty("rows")[0].GetProperty("last_collected_at").GetString());
        Assert.False(planCache.GetProperty("truncated").GetBoolean());

        var floor = DateTime.UtcNow - TimescaleSupport.RawRetentionSpan;
        var searched = DarlingMcpFindQueriesByTextTools.DescribeSearched(1, floor, floor + TimescaleSupport.RawRetentionSpan);
        var days = ((int)TimescaleSupport.RawRetentionSpan.TotalDays).ToString(CultureInfo.InvariantCulture);
        var message = planCache.GetProperty("message").GetString()!;
        Assert.Contains($"after the newest {searched} of the {days}-day raw tier had been searched, newest first", message, StringComparison.Ordinal);
        Assert.Contains("the matches in the part searched and not a complete answer", message, StringComparison.Ordinal);
        Assert.Equal(1, statements);

        /* #9150: the reply names the servers and the part of the window it read: the newest span, ending now. */
        var searchedWindow = planCache.GetProperty("searched");
        Assert.Equal("every enabled server", searchedWindow.GetProperty("servers").GetString());
        Assert.False(searchedWindow.GetProperty("complete").GetBoolean());
        var from = DateTime.Parse(searchedWindow.GetProperty("searched_from").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        var to = DateTime.Parse(searchedWindow.GetProperty("searched_to").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        Assert.InRange((to - from).TotalSeconds, FirstSpanSeconds() - 1, FirstSpanSeconds() + 1);
        Assert.Equal(searchedWindow.GetProperty("window_to").GetString(), searchedWindow.GetProperty("searched_to").GetString());
    }

    [Fact]
    public async Task ABudgetThatRunsOutBeforeAnySpan_IsStopped_AndSaysNothingWasSearched()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, _) = await OpenScratchAsync(ct);
        await using var _s = scratch;

        var budget = new DarlingMcpFindQueriesByTextTools.SourceBudget(DarlingMcpFindQueriesByTextTools.BudgetMilliseconds)
        {
            ElapsedClock = () => TimeSpan.FromMilliseconds(DarlingMcpFindQueriesByTextTools.BudgetMilliseconds + 1),
        };

        var planCache = await PlanCacheAsync(scratch.ConnectionString, "dbo.Ledger", budget, 20, ct);

        Assert.Equal("stopped", planCache.GetProperty("status").GetString());
        Assert.Equal(0, planCache.GetProperty("returned").GetInt32());
        var message = planCache.GetProperty("message").GetString()!;
        Assert.Contains("before the newest span of the", message, StringComparison.Ordinal);
        Assert.Contains("(none)", message, StringComparison.Ordinal);
    }

    /// <summary>The ledger statement on span-a is found in the newest span and is not listed again by the span that holds its three-day-old
    /// sighting; the same query_hash and database on span-b is its own row; the other statement lists nowhere. Every span runs, each one told
    /// the server ids found before it, and the rows are the window join's rows in its order.</summary>
    [Fact]
    public async Task TheWholeBudget_ListsEachServerHashAndDatabaseOnce_WithItsNewestSighting_AsTheJoinDoes()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, newestSample) = await OpenScratchAsync(ct);
        await using var _ = scratch;

        var carried = new List<int[]>();
        var budget = new DarlingMcpFindQueriesByTextTools.SourceBudget(DarlingMcpFindQueriesByTextTools.BudgetMilliseconds)
        {
            StatementBound = (_, parameters) =>
            {
                var ids = parameters.Cast<NpgsqlParameter>().Single(p => p.NpgsqlDbType == (NpgsqlDbType.Array | NpgsqlDbType.Integer));
                carried.Add((int[])ids.Value!);
            },
        };

        var planCache = await PlanCacheAsync(scratch.ConnectionString, "dbo.Ledger", budget, 20, ct);

        Assert.Equal("ok", planCache.GetProperty("status").GetString());
        Assert.Equal("probe", planCache.GetProperty("sample_read").GetString());
        string[] expected = ["span-a@" + LedgerHash + "@Books", "span-a@" + CountHash + "@Books", "span-b@" + LedgerHash + "@Books"];
        Assert.Equal(expected, Keys(planCache));
        Assert.Equal(Stamp(newestSample), planCache.GetProperty("rows")[0].GetProperty("last_collected_at").GetString());
        Assert.Equal(Stamp(newestSample - TimeSpan.FromDays(3) - TimeSpan.FromMinutes(30)), planCache.GetProperty("rows")[2].GetProperty("last_collected_at").GetString());
        Assert.False(planCache.GetProperty("truncated").GetBoolean());

        Assert.Equal(SpanCount(), carried.Count);
        Assert.Empty(carried[0]);
        Assert.Equal(new[] { 1 }, carried[1]);
        Assert.Equal(new[] { 1, 1, 2 }, carried[^1].Order());

        Assert.Equal(expected, await JoinKeysAsync(scratch.ConnectionString, "dbo.Ledger", 20, ct));
    }

    [Fact]
    public async Task ALimitBelowTheMatches_IsTruncated_KeepsTheNewestRow_AndStopsAtTheSpanThatFilledIt()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, _) = await OpenScratchAsync(ct);
        await using var _s = scratch;

        var statements = 0;
        var budget = Counting(() => statements++);

        var planCache = await PlanCacheAsync(scratch.ConnectionString, "dbo.Ledger", budget, 1, ct);

        Assert.Equal("ok", planCache.GetProperty("status").GetString());
        Assert.True(planCache.GetProperty("truncated").GetBoolean());
        Assert.Equal(["span-a@" + LedgerHash + "@Books"], Keys(planCache));
        Assert.InRange(statements, 2, SpanCount() - 1);
    }

    /// <summary>#9150: a page that fills in the newest span stops the search there (one statement), and the reply says the page is the whole-window
    /// answer: <c>complete</c> is true and <c>searched_from</c> is the window's start, because spans run newest first and the older ones could not
    /// change a full page. <c>truncated</c> says more matches exist. A count statement on span-b twenty minutes ago makes the newest span hold two rows.</summary>
    [Fact]
    public async Task APageThatFillsInTheNewestSpan_IsTheWholeWindowAnswer_AndIsTruncated()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, _) = await OpenScratchAsync(ct);
        await using var _s = scratch;
        await using (var c = new NpgsqlConnection(scratch.ConnectionString))
        {
            await c.OpenAsync(ct);
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text_digest)
                  VALUES ($1,$2,2,'span-b','Books',$3,sha256('count'::text::bytea))",
                CollectionIdGenerator.Next(), DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow) - TimeSpan.FromMinutes(20), CountHash);
        }

        var statements = 0;
        var planCache = await PlanCacheAsync(scratch.ConnectionString, "dbo.Ledger", Counting(() => statements++), 1, ct);

        Assert.Equal("ok", planCache.GetProperty("status").GetString());
        Assert.True(planCache.GetProperty("truncated").GetBoolean());
        Assert.Equal(1, statements);
        var searchedWindow = planCache.GetProperty("searched");
        Assert.True(searchedWindow.GetProperty("complete").GetBoolean());
        Assert.Equal(searchedWindow.GetProperty("window_from").GetString(), searchedWindow.GetProperty("searched_from").GetString());
        Assert.Equal(searchedWindow.GetProperty("window_to").GetString(), searchedWindow.GetProperty("searched_to").GetString());
    }

    private const string CommonHash = "0xC001";
    private const string SecondCommonHash = "0xC002";
    private const string NullDatabaseHash = "0xC003";

    /// <summary>A pattern more texts contain than the probe takes (1,100 filler texts name dbo.Common), so the search joins the window in spans. Server
    /// span-a ran the first statement half an hour ago and two and a half days ago, and the third statement, with no database, half an hour ago and three days
    /// ago; span-b ran the first statement, with the same query_hash and database, two and a half days ago; span-a ran the second statement three and
    /// a half days ago; a statement that does not name dbo.Common ran half an hour ago. Only the first and third span-a statements sit in the newest span.</summary>
    private static async Task<(ScratchPostgres Scratch, DateTime NewestSample)> OpenCommonScratchAsync(CancellationToken ct)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, 1, "span-a", ct);
        await DarlingMcpTestData.RegisterServerAsync(c, 2, "span-b", ct);
        await DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
              SELECT sha256(('filler' || i)::text::bytea), 'SELECT ' || i || ' FROM dbo.Common', timezone('UTC', now())
              FROM generate_series(1, 1100) AS i");
        await DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
              VALUES (sha256('first'::text::bytea), 'SELECT * FROM dbo.Common WHERE Id = @p', timezone('UTC', now())),
                     (sha256('second'::text::bytea), 'SELECT COUNT(*) FROM dbo.Common', timezone('UTC', now())),
                     (sha256('third'::text::bytea), 'UPDATE dbo.Common SET X = 1', timezone('UTC', now())),
                     (sha256('other'::text::bytea), 'SELECT 1 FROM dbo.Other', timezone('UTC', now()))");

        var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
        async Task Stat(int server, string name, string? database, string hash, string text, TimeSpan age) =>
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text_digest)
                  VALUES ($1,$2,$3,$4,$5,$6,sha256($7::text::bytea))",
                CollectionIdGenerator.Next(), now - age, server, name, (object?)database ?? DBNull.Value, hash, text);

        await Stat(1, "span-a", "Books", CommonHash, "first", TimeSpan.FromMinutes(30));
        await Stat(1, "span-a", "Books", CommonHash, "first", TimeSpan.FromDays(2.5));
        await Stat(2, "span-b", "Books", CommonHash, "first", TimeSpan.FromDays(2.5));
        await Stat(1, "span-a", "Books", SecondCommonHash, "second", TimeSpan.FromDays(3.5));
        await Stat(1, "span-a", null, NullDatabaseHash, "third", TimeSpan.FromMinutes(30));
        await Stat(1, "span-a", null, NullDatabaseHash, "third", TimeSpan.FromDays(3));
        await Stat(1, "span-a", "Books", OtherHash, "other", TimeSpan.FromMinutes(30));

        await DarlingMcpTestData.ExecAsync(c, ct, "CREATE EXTENSION IF NOT EXISTS pg_trgm");
        await DarlingMcpTestData.ExecAsync(c, ct,
            "CREATE INDEX IF NOT EXISTS " + FindQueriesByTextIndexScopeLiveTests.IndexShortName + " ON collect.query_text_dim USING gin (query_text gin_trgm_ops)");
        return (scratch, now - TimeSpan.FromMinutes(30));
    }

    /// <summary>#9150: a pattern more texts contain than the probe takes is joined to the window in spans, newest first. An uncut search runs every
    /// span and lists what the whole-window join lists, in its order: each server, query_hash and database once, at its newest sighting, a database
    /// that is NULL included (a span does not list it again), and the same query_hash on the other server as its own row.</summary>
    [Fact]
    public async Task ACommonPattern_JoinsTheWindowInSpans_AndListsWhatTheWholeWindowJoinLists()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, newestSample) = await OpenCommonScratchAsync(ct);
        await using var _ = scratch;

        var statements = 0;
        var planCache = await PlanCacheAsync(scratch.ConnectionString, "dbo.Common", Counting(() => statements++), 20, ct);

        Assert.Equal("ok", planCache.GetProperty("status").GetString());
        Assert.Equal("join", planCache.GetProperty("sample_read").GetString());
        Assert.Equal(JsonValueKind.Null, planCache.GetProperty("matched_texts").ValueKind);
        Assert.Equal(SpanCount(), statements);
        string[] expected =
        [
            "span-a@" + CommonHash + "@Books", "span-a@" + NullDatabaseHash + "@",
            "span-b@" + CommonHash + "@Books", "span-a@" + SecondCommonHash + "@Books",
        ];
        Assert.Equal(expected, Keys(planCache));
        Assert.Equal(await JoinKeysAsync(scratch.ConnectionString, "dbo.Common", 20, ct), Keys(planCache));
        Assert.Equal(Stamp(newestSample), planCache.GetProperty("rows")[0].GetProperty("last_collected_at").GetString());
        Assert.True(planCache.GetProperty("searched").GetProperty("complete").GetBoolean());
    }

    /// <summary>#9150: a budget that runs out after the newest span of the join keeps that span's rows, and <c>searched</c> names that span. Before this
    /// the join was one statement, and a cut in it returned no rows and said none of the window had been read.</summary>
    [Fact]
    public async Task ACommonPatternWhoseJoinIsCutAfterTheNewestSpan_KeepsThatSpansRows_AndSaysHowMuchWasSearched()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, newestSample) = await OpenCommonScratchAsync(ct);
        await using var _ = scratch;

        var statements = 0;
        var budget = new DarlingMcpFindQueriesByTextTools.SourceBudget(DarlingMcpFindQueriesByTextTools.BudgetMilliseconds)
        {
            ElapsedClock = () => statements >= 1 ? TimeSpan.FromMilliseconds(DarlingMcpFindQueriesByTextTools.BudgetMilliseconds + 1) : TimeSpan.Zero,
            BeforeStatement = (_, _, _) => { statements++; return Task.CompletedTask; },
        };

        var planCache = await PlanCacheAsync(scratch.ConnectionString, "dbo.Common", budget, 20, ct);

        Assert.Equal("stopped", planCache.GetProperty("status").GetString());
        Assert.Equal("join", planCache.GetProperty("sample_read").GetString());
        Assert.Equal(["span-a@" + CommonHash + "@Books", "span-a@" + NullDatabaseHash + "@"], Keys(planCache));
        Assert.Equal(Stamp(newestSample), planCache.GetProperty("rows")[0].GetProperty("last_collected_at").GetString());
        Assert.Equal(1, statements);

        var searchedWindow = planCache.GetProperty("searched");
        Assert.False(searchedWindow.GetProperty("complete").GetBoolean());
        var from = DateTime.Parse(searchedWindow.GetProperty("searched_from").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        var to = DateTime.Parse(searchedWindow.GetProperty("searched_to").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        Assert.InRange((to - from).TotalSeconds, FirstSpanSeconds() - 1, FirstSpanSeconds() + 1);
        Assert.Equal(searchedWindow.GetProperty("window_to").GetString(), searchedWindow.GetProperty("searched_to").GetString());
    }

    /// <summary>A budget gone before the join's first span is stopped, and the reply names no part of the window as searched.</summary>
    [Fact]
    public async Task ACommonPatternWhoseBudgetIsGoneBeforeTheFirstSpan_IsStopped_AndSaysNothingWasSearched()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, _) = await OpenCommonScratchAsync(ct);
        await using var _s = scratch;

        var spent = new DarlingMcpFindQueriesByTextTools.SourceBudget(DarlingMcpFindQueriesByTextTools.BudgetMilliseconds)
        {
            ElapsedClock = () => TimeSpan.FromMilliseconds(DarlingMcpFindQueriesByTextTools.BudgetMilliseconds + 1),
        };
        var stopped = await PlanCacheAsync(scratch.ConnectionString, "dbo.Common", spent, 20, ct);

        Assert.Equal("stopped", stopped.GetProperty("status").GetString());
        Assert.Equal("join", stopped.GetProperty("sample_read").GetString());
        Assert.Equal(0, stopped.GetProperty("returned").GetInt32());
        var searchedWindow = stopped.GetProperty("searched");
        Assert.False(searchedWindow.GetProperty("complete").GetBoolean());
        Assert.Equal(JsonValueKind.Null, searchedWindow.GetProperty("searched_from").ValueKind);
        Assert.Equal(JsonValueKind.Null, searchedWindow.GetProperty("searched_to").ValueKind);
        Assert.NotEqual(JsonValueKind.Null, searchedWindow.GetProperty("window_from").ValueKind);
        Assert.True(searchedWindow.GetProperty("server_count").GetInt32() > 0);
    }

    /// <summary>The window join run directly with the tool's own parameters, its rows in its order as <c>server@query_hash@database</c>, the row
    /// past the page dropped as the tool drops it.</summary>
    private static async Task<string[]> JoinKeysAsync(string connectionString, string pattern, int limit, CancellationToken ct)
    {
        await using var data = NpgsqlDataSource.Create(connectionString);
        await using var c = await data.OpenConnectionAsync(ct);
        var names = new Dictionary<int, string>();
        await using (var servers = new NpgsqlCommand("SELECT server_id, COALESCE(display_name, server_name) FROM collect.servers", c))
        await using (var reader = await servers.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                names[reader.GetInt32(0)] = reader.GetString(1);
            }
        }

        await using var join = new NpgsqlCommand(DarlingMcpFindQueriesByTextTools.PlanCacheAllServersSql, c);
        join.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DarlingMcpFindQueriesByTextTools.EscapeLike(pattern) });
        join.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = limit + 1 });
        join.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Timestamp,
            Value = DateTime.SpecifyKind(DateTime.UtcNow - TimescaleSupport.RawRetentionSpan, DateTimeKind.Unspecified),
        });
        var rows = new List<string>();
        await using (var reader = await join.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                rows.Add(names[reader.GetInt32(0)] + "@" + reader.GetString(1) + "@" + (reader.IsDBNull(2) ? string.Empty : reader.GetString(2)));
            }
        }

        if (rows.Count > limit) rows.RemoveAt(rows.Count - 1);
        return rows.ToArray();
    }
}
