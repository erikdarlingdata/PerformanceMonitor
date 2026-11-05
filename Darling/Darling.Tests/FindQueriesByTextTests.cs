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

/* #1776 own-store: each live test seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// #7610: <c>find_queries_by_text</c> searches statement text on one server, in Query Store text and in the raw tier's plan-cache text.
/// The refusals need no store; the rest are gated on DARLING_TEST_PG.
/// </summary>
public sealed class FindQueriesByTextTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the find_queries_by_text live tests.";

    private static JsonElement Parse(string reply)
    {
        var root = JsonDocument.Parse(reply).RootElement;
        Assert.True(root.TryGetProperty("sources", out _), reply);
        return root;
    }

    private static Task<string> Run(NpgsqlDataSource? postgres, string? pattern, string? server, string? database = null, int limit = 20,
        McpDataDetail detail = McpDataDetail.Full, DarlingMcpFindQueriesByTextTools.SourceBudget? queryStore = null, DarlingMcpFindQueriesByTextTools.SourceBudget? planCache = null) =>
        DarlingMcpFindQueriesByTextTools.FindQueriesByText(
            postgres!, pattern, server, database, limit, detail,
            queryStore ?? DarlingMcpFindQueriesByTextTools.SourceBudget.Default, planCache ?? DarlingMcpFindQueriesByTextTools.SourceBudget.Default,
            TestContext.Current.CancellationToken);

    [Theory]
    [InlineData(McpDataDetail.MaskLiterals, "maskLiterals")]
    [InlineData(McpDataDetail.MetadataOnly, "metadataOnly")]
    public async Task AtALevelThatHidesText_TheToolRefusesAndNamesTheLevel(McpDataDetail detail, string word)
    {
        var reply = JsonDocument.Parse(await Run(null, "dbo.Orders", "srv", detail: detail)).RootElement;
        Assert.Equal("invalid", reply.GetProperty("status").GetString());
        Assert.Contains(word, reply.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("ab", "srv", "pattern")]
    [InlineData("   ", "srv", "pattern")]
    [InlineData("dbo.Orders", "", "server_name")]
    [InlineData("dbo.Orders", null, "server_name")]
    public async Task ShortPatternOrMissingServer_IsRefused(string pattern, string? server, string parameter)
    {
        var reply = JsonDocument.Parse(await Run(null, pattern, server)).RootElement;
        Assert.Equal("invalid", reply.GetProperty("status").GetString());
        Assert.Equal(parameter, reply.GetProperty("hints").GetProperty("parameter").GetString());
    }

    [Fact]
    public async Task LimitOutsideTheRange_IsRefused()
    {
        foreach (var limit in new[] { 0, 51 })
        {
            var reply = JsonDocument.Parse(await Run(null, "dbo.Orders", "srv", limit: limit)).RootElement;
            Assert.Equal("limit", reply.GetProperty("hints").GetProperty("parameter").GetString());
        }
    }

    [Fact]
    public void EscapeLike_EscapesTheBackslashThePercentAndTheUnderscore() =>
        Assert.Equal(@"a\\b\%c\_d", DarlingMcpFindQueriesByTextTools.EscapeLike(@"a\b%c_d"));

    private static async Task SeedAsync(NpgsqlConnection c, CancellationToken ct)
    {
        foreach (var (id, name) in new[] { (1, "fq-a"), (2, "fq-b") })
        {
            await DarlingMcpTestData.RegisterServerAsync(c, id, name, ct);
        }

        var now = DarlingMcpTestData.Naive(DateTime.UtcNow);
        async Task QueryStore(int server, string database, long id, string text, string? hash = null) =>
            await DarlingMcpTestData.ExecAsync(c, ct,
                "INSERT INTO query_store_text (server_id, database_name, query_id, query_sql_text, query_hash, last_seen) VALUES ($1,$2,$3,$4,$5,$6)",
                server, database, id, text, hash, now);

        await QueryStore(1, "Sales", 1, "SELECT * FROM dbo.Orders WHERE OrderId = 1", "0xAAAA");
        await QueryStore(1, "Sales", 2, "select * from DBO.ORDERS o join dbo.Customers c on c.id = o.cid");
        await QueryStore(1, "Hr", 3, "SELECT * FROM dbo.Orders");
        await QueryStore(1, "Sales", 4, "SELECT 50% off, a_b FROM dbo.Promo");
        await QueryStore(1, "Sales", 5, "SELECT 505 off, aXb FROM dbo.Promo");
        await QueryStore(2, "Sales", 6, "SELECT * FROM dbo.Orders /* other server */");
        await QueryStore(1, "Sales", 7, new string('x', 400) + " NEEDLE_TEXT " + new string('y', 3000));

        /* The plan cache: text lives in query_text_dim behind a digest. Two samples of one hash, so the read must return the hash once. */
        async Task Dim(byte[] digest, string text) =>
            await DarlingMcpTestData.ExecAsync(c, ct, "INSERT INTO query_text_dim (digest, query_text, last_seen) VALUES ($1,$2,$3)", digest, text, now);
        async Task Stat(int server, string name, string database, string hash, byte[] digest, TimeSpan age) =>
            await DarlingMcpTestData.ExecAsync(c, ct,
                "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text_digest) VALUES ($1,$2,$3,$4,$5,$6,$7)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(DateTime.UtcNow - age), server, name, database, hash, digest);

        var d1 = new byte[] { 1, 1, 1, 1 };
        var d2 = new byte[] { 2, 2, 2, 2 };
        var d3 = new byte[] { 3, 3, 3, 3 };
        await Dim(d1, "SELECT * FROM dbo.Orders WHERE OrderId = @p");
        await Dim(d2, "UPDATE dbo.Widgets SET n = 1");
        await Dim(d3, "SELECT 1 FROM dbo.Orders");
        await Stat(1, "fq-a", "Sales", "0xAAAA", d1, TimeSpan.FromHours(3));
        await Stat(1, "fq-a", "Sales", "0xAAAA", d1, TimeSpan.FromHours(1));
        await Stat(1, "fq-a", "Sales", "0xBBBB", d2, TimeSpan.FromHours(1));
        await Stat(2, "fq-b", "Sales", "0xCCCC", d3, TimeSpan.FromHours(1));
    }

    private static async Task<(ScratchPostgres Scratch, NpgsqlDataSource Source)> OpenAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var c = new NpgsqlConnection(scratch.ConnectionString))
        {
            await c.OpenAsync(ct);
            await PgMigrations.MigrateAsync(c, ct);
            await SeedAsync(c, ct);
        }

        return (scratch, NpgsqlDataSource.Create(scratch.ConnectionString));
    }

    [Fact]
    public async Task SearchesOneServer_CaseBlind_WithAnExcerptAndEachSourcesHorizon()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await OpenAsync(ct);
        await using var _s = scratch;
        await using var _d = source;

        var reply = Parse(await Run(source, "dbo.orders", "fq-a"));
        var qs = reply.GetProperty("sources").GetProperty("query_store");
        Assert.Equal("ok", qs.GetProperty("status").GetString());
        Assert.Equal(new long[] { 3, 1, 2 }, qs.GetProperty("rows").EnumerateArray().Select(r => r.GetProperty("query_id").GetInt64()).ToArray());
        Assert.DoesNotContain("other server", qs.GetRawText(), StringComparison.Ordinal);
        Assert.Contains("query_store_text", qs.GetProperty("horizon").GetString(), StringComparison.Ordinal);
        Assert.Equal("0xAAAA", qs.GetProperty("rows").EnumerateArray().First(r => r.GetProperty("query_id").GetInt64() == 1).GetProperty("query_hash").GetString());

        var pc = reply.GetProperty("sources").GetProperty("plan_cache");
        Assert.Equal(new[] { "0xAAAA" }, pc.GetProperty("rows").EnumerateArray().Select(r => r.GetProperty("query_hash").GetString()).ToArray());
        Assert.Contains("4 days", pc.GetProperty("horizon").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task DatabaseFilter_NarrowsBothSources_AndTheCapSaysTruncated()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await OpenAsync(ct);
        await using var _s = scratch;
        await using var _d = source;

        var hr = Parse(await Run(source, "dbo.Orders", "fq-a", "Hr")).GetProperty("sources");
        Assert.Equal(new long[] { 3 }, hr.GetProperty("query_store").GetProperty("rows").EnumerateArray().Select(r => r.GetProperty("query_id").GetInt64()).ToArray());
        Assert.Equal("empty", hr.GetProperty("plan_cache").GetProperty("status").GetString());

        var capped = Parse(await Run(source, "dbo.Orders", "fq-a", limit: 2)).GetProperty("sources").GetProperty("query_store");
        Assert.Equal(2, capped.GetProperty("returned").GetInt32());
        Assert.True(capped.GetProperty("truncated").GetBoolean());
    }

    [Fact]
    public async Task PercentAndUnderscore_MatchOnlyThemselves()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await OpenAsync(ct);
        await using var _s = scratch;
        await using var _d = source;

        var percent = Parse(await Run(source, "50% off", "fq-a")).GetProperty("sources").GetProperty("query_store");
        Assert.Equal(new long[] { 4 }, percent.GetProperty("rows").EnumerateArray().Select(r => r.GetProperty("query_id").GetInt64()).ToArray());

        var underscore = Parse(await Run(source, "a_b", "fq-a")).GetProperty("sources").GetProperty("query_store");
        Assert.Equal(new long[] { 4 }, underscore.GetProperty("rows").EnumerateArray().Select(r => r.GetProperty("query_id").GetInt64()).ToArray());
    }

    [Fact]
    public async Task TheExcerpt_IsAboutThreeHundredCharactersAroundTheFirstMatch_NeverTheWholeText()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await OpenAsync(ct);
        await using var _s = scratch;
        await using var _d = source;

        var row = Parse(await Run(source, "needle_text", "fq-a")).GetProperty("sources").GetProperty("query_store")
            .GetProperty("rows").EnumerateArray().Single();
        var excerpt = row.GetProperty("excerpt").GetString()!;
        Assert.Equal(DarlingMcpFindQueriesByTextTools.ExcerptLength, excerpt.Length);
        Assert.Contains("NEEDLE_TEXT", excerpt, StringComparison.Ordinal);
        Assert.Equal(DarlingMcpFindQueriesByTextTools.ExcerptLead, excerpt.IndexOf("NEEDLE_TEXT", StringComparison.Ordinal));
        Assert.Equal(400 + " NEEDLE_TEXT ".Length + 3000, row.GetProperty("text_length").GetInt32());
    }

    [Fact]
    public async Task ASourceThatRunsOutOfTime_IsStopped_AndTheOtherSourceStillAnswers()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await OpenAsync(ct);
        await using var _s = scratch;
        await using var _d = source;

        var slow = new DarlingMcpFindQueriesByTextTools.SourceBudget(100, TestDelaySeconds: 2);
        var reply = Parse(await Run(source, "dbo.Orders", "fq-a", queryStore: slow)).GetProperty("sources");
        var qs = reply.GetProperty("query_store");
        Assert.Equal("stopped", qs.GetProperty("status").GetString());
        Assert.Contains("database_name", qs.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Equal(0, qs.GetProperty("returned").GetInt32());
        Assert.Equal("ok", reply.GetProperty("plan_cache").GetProperty("status").GetString());
        Assert.Single(reply.GetProperty("plan_cache").GetProperty("rows").EnumerateArray());
    }
}
