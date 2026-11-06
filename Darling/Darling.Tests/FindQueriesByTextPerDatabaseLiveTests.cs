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
   works entirely inside it, so it cannot race live collection. The all-servers test builds the trigram index on its own tracker, never
   the process-wide one. */
/// <summary>
/// #7905: the plan-cache source of <c>find_queries_by_text</c> keeps one row per server, query_hash AND database. A statement that runs in
/// several databases (per-tenant databases of one schema) lists once per database, each with the <c>database_name</c> that
/// <c>get_query_trend</c> takes, and a statement of one database still lists once.
/// </summary>
public sealed class FindQueriesByTextPerDatabaseLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the #7905 live tests.";

    /// <summary>Two texts that both name dbo.Orders. The shared statement (hash 0xAB01) ran in TenantA twice and in TenantB once; the
    /// single statement (0xCD02) ran in TenantA only.</summary>
    private static async Task<ScratchPostgres> OpenScratchAsync(CancellationToken ct)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, 1, "tenants", ct);
        await DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
              VALUES (sha256('shared'::text::bytea), 'SELECT * FROM dbo.Orders WHERE OrderId = @p', timezone('UTC', now())),
                     (sha256('single'::text::bytea), 'SELECT * FROM dbo.Orders WHERE CustomerId = @c', timezone('UTC', now()))");

        async Task Stat(string database, string hash, string text, TimeSpan age) =>
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text_digest)
                  VALUES ($1,$2,1,'tenants',$3,$4,sha256($5::text::bytea))",
                CollectionIdGenerator.Next(), LiveClock.AnchoredEnd() - age, database, hash, text);

        await Stat("TenantA", "0xAB01", "shared", TimeSpan.FromHours(3));
        await Stat("TenantA", "0xAB01", "shared", TimeSpan.FromHours(2));
        await Stat("TenantB", "0xAB01", "shared", TimeSpan.FromHours(1));
        await Stat("TenantA", "0xCD02", "single", TimeSpan.FromHours(1));
        return scratch;
    }

    private static string[] HashAtDatabase(JsonElement planCache) =>
        planCache.GetProperty("rows").EnumerateArray()
            .Select(r => r.GetProperty("query_hash").GetString() + "@" + r.GetProperty("database_name").GetString())
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToArray();

    [Fact]
    public async Task OneServer_AStatementSharedByTwoDatabases_ListsOncePerDatabase()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);

        var reply = JsonDocument.Parse(await DarlingMcpFindQueriesByTextTools.FindQueriesByText(
            data, "dbo.Orders", "tenants", null, 20, McpDataDetail.Full,
            DarlingMcpFindQueriesByTextTools.SourceBudget.Default, DarlingMcpFindQueriesByTextTools.SourceBudget.Default, ct)).RootElement;

        var planCache = reply.GetProperty("sources").GetProperty("plan_cache");
        Assert.Equal("ok", planCache.GetProperty("status").GetString());
        Assert.Equal(["0xAB01@TenantA", "0xAB01@TenantB", "0xCD02@TenantA"], HashAtDatabase(planCache));
        Assert.Equal(3, planCache.GetProperty("returned").GetInt32());
        Assert.Contains("query_hash and database", planCache.GetProperty("horizon").GetString(), StringComparison.Ordinal);
    }

    /// <summary>The all-servers scope, served by the trigram index, keys its rows the same way.</summary>
    [Fact]
    public async Task AllServers_AStatementSharedByTwoDatabases_ListsOncePerDatabase()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var tracker = await FindQueriesByTextIndexScopeLiveTests.BuildIndexAsync(data, ct);

        var reply = JsonDocument.Parse(await DarlingMcpFindQueriesByTextTools.FindQueriesByText(
            data, "dbo.Orders", null, null, 20, McpDataDetail.Full,
            DarlingMcpFindQueriesByTextTools.SourceBudget.Default, DarlingMcpFindQueriesByTextTools.SourceBudget.Default, ct, tracker)).RootElement;

        Assert.Equal("all_servers", reply.GetProperty("scope").GetString());
        var planCache = reply.GetProperty("sources").GetProperty("plan_cache");
        Assert.Equal("ok", planCache.GetProperty("status").GetString());
        Assert.Equal(["0xAB01@TenantA", "0xAB01@TenantB", "0xCD02@TenantA"], HashAtDatabase(planCache));
        Assert.Contains("query_hash and database", planCache.GetProperty("horizon").GetString(), StringComparison.Ordinal);
    }
}
