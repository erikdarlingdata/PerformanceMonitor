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

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Each test creates its own scratch database through ScratchPostgres and
   works entirely inside it, so it cannot race live collection. */
/// <summary>
/// The plan-cache source of <c>find_queries_by_text</c> on one server searches the raw tier newest first, in bounded spans, inside its
/// one budget, so a source whose budget runs out hands back the matches it had already found and says how far it got, instead of
/// nothing. The budget is spent through the source's test clock, so where it runs out is a fixed point, not a race: the clock reads
/// zero until the first span's statement has run and the whole budget after it.
/// </summary>
public sealed class FindQueriesByTextBudgetKeepsRowsLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the find_queries_by_text budget live tests.";

    private const string NewestHash = "0xAA01";
    private const string OlderHash = "0xBB02";
    private const string UnmatchedHash = "0xDD04";

    /// <summary>Two texts that both name dbo.Ledger, and one that does not. The newest statement ran in Books an hour ago, two hours ago
    /// and three days ago, and in Archive, with the same text, three days and an hour ago; the older statement ran in Books three days ago
    /// only; the unmatched statement ran an hour ago and three days ago. All sit inside the raw tier's window; only the newest and the
    /// unmatched statements sit in its newest hours, so a text first judged there is seen again, matched or not, in a later span.</summary>
    private static async Task<(ScratchPostgres Scratch, DateTime NewestSample)> OpenScratchAsync(CancellationToken ct)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, 1, "ledger", ct);
        await DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
              VALUES (sha256('newest'::text::bytea), 'SELECT * FROM dbo.Ledger WHERE LedgerId = @p', timezone('UTC', now())),
                     (sha256('older'::text::bytea), 'SELECT COUNT(*) FROM dbo.Ledger', timezone('UTC', now())),
                     (sha256('unmatched'::text::bytea), 'SELECT 1 FROM dbo.Other', timezone('UTC', now()))");

        var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
        async Task Stat(string hash, string text, TimeSpan age, string database = "Books") =>
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text_digest)
                  VALUES ($1,$2,1,'ledger',$5,$3,sha256($4::text::bytea))",
                CollectionIdGenerator.Next(), now - age, hash, text, database);

        await Stat(NewestHash, "newest", TimeSpan.FromHours(1));
        await Stat(NewestHash, "newest", TimeSpan.FromHours(2));
        await Stat(NewestHash, "newest", TimeSpan.FromDays(3));
        await Stat(NewestHash, "newest", TimeSpan.FromDays(3) + TimeSpan.FromHours(1), "Archive");
        await Stat(OlderHash, "older", TimeSpan.FromDays(3));
        await Stat(UnmatchedHash, "unmatched", TimeSpan.FromHours(1));
        await Stat(UnmatchedHash, "unmatched", TimeSpan.FromDays(3));
        return (scratch, now - TimeSpan.FromHours(1));
    }

    private static async Task<JsonElement> PlanCacheAsync(ScratchPostgres scratch, DarlingMcpFindQueriesByTextTools.SourceBudget planCache, int limit, CancellationToken ct)
    {
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var reply = await DarlingMcpFindQueriesByTextTools.FindQueriesByText(
            data, "dbo.Ledger", "ledger", null, limit, McpDataDetail.Full,
            DarlingMcpFindQueriesByTextTools.SourceBudget.Default, planCache, ct);
        return JsonDocument.Parse(reply).RootElement.GetProperty("sources").GetProperty("plan_cache");
    }

    private static string[] Hashes(JsonElement planCache) =>
        planCache.GetProperty("rows").EnumerateArray().Select(r => r.GetProperty("query_hash").GetString()!).ToArray();

    private static (string Hash, string Database)[] Keys(JsonElement planCache) =>
        planCache.GetProperty("rows").EnumerateArray().Select(r => (r.GetProperty("query_hash").GetString()!, r.GetProperty("database_name").GetString()!)).ToArray();

    [Fact]
    public async Task ABudgetThatRunsOutAfterTheNewestSpan_ReturnsTheRowsFoundThere_AndSaysItStopped()
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

        var planCache = await PlanCacheAsync(scratch, budget, 20, ct);

        Assert.Equal("stopped", planCache.GetProperty("status").GetString());
        Assert.Equal(1, planCache.GetProperty("returned").GetInt32());
        Assert.Equal([NewestHash], Hashes(planCache));
        Assert.Equal(DateTime.SpecifyKind(newestSample, DateTimeKind.Utc).ToString("o"), planCache.GetProperty("rows")[0].GetProperty("last_collected_at").GetString());
        Assert.False(planCache.GetProperty("truncated").GetBoolean());
        var message = planCache.GetProperty("message").GetString()!;
        Assert.Contains("not a complete answer", message, StringComparison.Ordinal);
        Assert.DoesNotContain("(none)", message, StringComparison.Ordinal);
        Assert.Equal(1, statements);
    }

    [Fact]
    public async Task ABudgetThatRunsOutBeforeAnySpan_IsStopped_NotEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, _) = await OpenScratchAsync(ct);
        await using var _s = scratch;

        var budget = new DarlingMcpFindQueriesByTextTools.SourceBudget(DarlingMcpFindQueriesByTextTools.BudgetMilliseconds)
        {
            ElapsedClock = () => TimeSpan.FromMilliseconds(DarlingMcpFindQueriesByTextTools.BudgetMilliseconds + 1),
        };

        var planCache = await PlanCacheAsync(scratch, budget, 20, ct);

        Assert.Equal("stopped", planCache.GetProperty("status").GetString());
        Assert.Equal(0, planCache.GetProperty("returned").GetInt32());
        Assert.Contains("(none)", planCache.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    /// <summary>The newest statement's text is judged in the newest span, where it ran in Books, and the same text's run in Archive sits
    /// spans later: it lists there too, once per database, with that database's own newest sighting. The unmatched text, also judged in
    /// the newest span and seen again later, lists nowhere.</summary>
    [Fact]
    public async Task TheWholeBudget_ListsEveryMatchNewestFirst_OncePerHashAndDatabase()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, newestSample) = await OpenScratchAsync(ct);
        await using var _ = scratch;

        var planCache = await PlanCacheAsync(scratch, DarlingMcpFindQueriesByTextTools.SourceBudget.Default, 20, ct);

        Assert.Equal("ok", planCache.GetProperty("status").GetString());
        Assert.Equal([(NewestHash, "Books"), (OlderHash, "Books"), (NewestHash, "Archive")], Keys(planCache));
        Assert.Equal(DateTime.SpecifyKind(newestSample, DateTimeKind.Utc).ToString("o"), planCache.GetProperty("rows")[0].GetProperty("last_collected_at").GetString());
        Assert.Equal(DateTime.SpecifyKind(newestSample - TimeSpan.FromDays(3), DateTimeKind.Utc).ToString("o"), planCache.GetProperty("rows")[2].GetProperty("last_collected_at").GetString());
        Assert.False(planCache.GetProperty("truncated").GetBoolean());
    }

    [Fact]
    public async Task ALimitBelowTheMatches_IsTruncated_AndKeepsTheNewestRow()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, _) = await OpenScratchAsync(ct);
        await using var _s = scratch;

        var planCache = await PlanCacheAsync(scratch, DarlingMcpFindQueriesByTextTools.SourceBudget.Default, 1, ct);

        Assert.Equal("ok", planCache.GetProperty("status").GetString());
        Assert.True(planCache.GetProperty("truncated").GetBoolean());
        Assert.Equal([NewestHash], Hashes(planCache));
    }
}
