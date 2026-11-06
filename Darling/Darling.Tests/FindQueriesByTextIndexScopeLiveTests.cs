/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test creates its own scratch database through ScratchPostgres and
   works entirely inside it, so it cannot race live collection. */
/// <summary>
/// #7674 part B on a real store: the one-server scope keeps the plain path when the trigram index exists (the planner cannot use the index for
/// it), and a search that names no server is refused unless the index is ready, in a sentence that names the option.
/// </summary>
public sealed class FindQueriesByTextIndexScopeLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the #7674 part B live tests.";

    private static readonly DateTime Now = new(2026, 11, 6, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>A few thousand distinct texts, so a rare pattern is selective enough for the planner to want the index, and a few samples that
    /// point at some of them on two servers.</summary>
    internal static async Task SeedAsync(NpgsqlConnection c, CancellationToken ct)
    {
        foreach (var (id, name) in new[] { (1, "ix-a"), (2, "ix-b") })
        {
            await DarlingMcpTestData.RegisterServerAsync(c, id, name, ct);
        }

        await DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
              SELECT sha256(i::text::bytea), 'SELECT * FROM dbo.Orders_' || i || ' WHERE OrderId = @p', timezone('UTC', now())
              FROM generate_series(1, 3000) AS i");

        async Task Stat(int server, string name, string database, string hash, int text, TimeSpan age) =>
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text_digest)
                  VALUES ($1,$2,$3,$4,$5,$6,sha256($7::text::bytea))",
                CollectionIdGenerator.Next(), LiveClock.AnchoredEnd() - age, server, name, database, hash, text.ToString(System.Globalization.CultureInfo.InvariantCulture));

        await Stat(1, "ix-a", "Sales", "0xA001", 77, TimeSpan.FromHours(2));
        await Stat(1, "ix-a", "Sales", "0xA001", 77, TimeSpan.FromHours(1));
        await Stat(2, "ix-b", "Hr", "0xB002", 77, TimeSpan.FromHours(1));
        await Stat(2, "ix-b", "Hr", "0xB003", 1234, TimeSpan.FromHours(1));

        /* The ids come last, from the same generator, so none repeats one used above. */
        /* Server ix-a has a sample for almost every text. With only a handful of samples the planner reads the text by primary key, one digest at a time,
           and never considers the trigram index, so the plan checks below could not tell a statement the index can serve from one it cannot. */
        await DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text_digest)
              SELECT $1::bigint + i, $2::timestamp, 1, 'ix-a', 'Sales', '0xS' || i, sha256(i::text::bytea)
              FROM generate_series(1, 3000) AS i
              WHERE i NOT IN (77, 1234)",
            CollectionIdGenerator.Next(), LiveClock.AnchoredEnd() - TimeSpan.FromHours(3));
    }

    internal static async Task<ScratchPostgres> OpenScratchAsync(CancellationToken ct)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);
        await SeedAsync(c, ct);
        await using var count = new NpgsqlCommand("SELECT count(*) FROM collect.query_text_dim", c);
        Assert.Equal(3000L, Convert.ToInt64(await count.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture));
        return scratch;
    }

    internal static async Task<QueryTextTrigramIndexTracker> BuildIndexAsync(NpgsqlDataSource data, CancellationToken ct)
    {
        var tracker = new QueryTextTrigramIndexTracker();
        tracker.Configure(true);
        await QueryTextTrigramIndex.RunPassAsync(data, true, tracker, Now, NullLogger.Instance, ct);
        await using var c = await data.OpenConnectionAsync(ct);
        var state = await QueryTextTrigramIndex.ReadStateAsync(c, ct);
        Assert.True(state.IndexValid, "the fixture's index was not built, so the plan check below would prove nothing");
        return tracker;
    }

    /// <summary>The planner's plan for one of the tool's statements with sequential scans turned off, so that any index that can serve the
    /// predicate is the cheaper way in and shows up in the plan. Returns the plan as JSON text.</summary>
    internal static async Task<string> ExplainAsync(NpgsqlDataSource data, string sql, string? database, string pattern, bool withServer, CancellationToken ct)
    {
        await using var c = await data.OpenConnectionAsync(ct);
        await using (var set = new NpgsqlCommand("SET enable_seqscan = off", c))
        {
            await set.ExecuteNonQueryAsync(ct);
        }

        /* Both tables, so the planner knows server ix-a has about three thousand samples: without that it assumes a handful and reads the text by key. */
        foreach (var table in new[] { "collect.query_text_dim", "query_stats" })
        {
            await using var analyze = new NpgsqlCommand("ANALYZE " + table, c);
            await analyze.ExecuteNonQueryAsync(ct);
        }

        await using var explain = new NpgsqlCommand("EXPLAIN (FORMAT JSON) " + sql, c);
        var floor = LiveClock.Now() - TimescaleSupport.RawRetentionSpan;
        if (withServer)
        {
            explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = 1 });
            explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)database ?? DBNull.Value });
        }

        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DarlingMcpFindQueriesByTextTools.EscapeLike(pattern) });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = 21 });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = floor });
        return Convert.ToString(await explain.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture)!;
    }

    internal const string IndexShortName = "ix_query_text_dim_trgm";

    [Theory]
    [InlineData(null)]
    [InlineData("Sales")]
    public async Task OneServerScope_DoesNotUseTheTrigramIndex_WhenItExists(string? database)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        await BuildIndexAsync(data, ct);

        var plan = await ExplainAsync(data, DarlingMcpFindQueriesByTextTools.PlanCacheSql, database, "orders_77 where", withServer: true, ct);

        Assert.Contains("query_text_dim", plan, StringComparison.Ordinal);
        Assert.DoesNotContain(IndexShortName, plan, StringComparison.Ordinal);
    }

    /// <summary>A search that names no server is the all-servers scope. Without a ready index (the option is off here: the process-wide tracker was never
    /// configured) it is refused, and the sentence names the option and the state, never a result.</summary>
    [Fact]
    public async Task NoServerNamed_WithoutAReadyIndex_IsRefusedAndNamesTheOption()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);

        var reply = JsonDocument.Parse(await DarlingMcpFindQueriesByTextTools.FindQueriesByText(
            data, "orders_77 where", null, null, 20, McpDataDetail.Full,
            DarlingMcpFindQueriesByTextTools.SourceBudget.Default, DarlingMcpFindQueriesByTextTools.SourceBudget.Default, ct)).RootElement;

        Assert.Equal("invalid", reply.GetProperty("status").GetString());
        Assert.False(reply.TryGetProperty("sources", out _), "an all-servers search answered with no ready index");
        var message = reply.GetProperty("message").GetString();
        Assert.Contains("queryTextTrigramIndexEnabled", message, StringComparison.Ordinal);
        Assert.Contains("off", message, StringComparison.Ordinal);
    }

    /// <summary>The block is on the reply of a store that has no self-metrics yet (status empty) and on the full reply: the index state does not
    /// depend on the self-metrics.</summary>
    [Fact]
    public async Task GetStoreMetrics_CarriesTheIndexBlock_OnTheEmptyReplyAndOnTheFullReply()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);

        using (var empty = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(data, cancellationToken: ct)))
        {
            Assert.Equal("empty", empty.RootElement.GetProperty("status").GetString());
            var block = empty.RootElement.GetProperty("query_text_index");
            Assert.Equal("Off", block.GetProperty("status").GetString());
            Assert.False(block.GetProperty("exists").GetBoolean());
        }

        await using (var c = new NpgsqlConnection(scratch.ConnectionString))
        {
            await c.OpenAsync(ct);
            await DarlingMcpTestData.ExecAsync(c, ct,
                "INSERT INTO collect.store_metrics (metric_time, object_name, object_kind, total_bytes, enabled_server_count) VALUES (timezone('UTC', now()) - interval '5 minutes', 'darling', $1, 1000000, 1)",
                StoreSelfMetrics.StoreObjectKind);
        }

        using var full = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(data, cancellationToken: ct));
        Assert.Equal("summary", full.RootElement.GetProperty("view").GetString());
        var fullBlock = full.RootElement.GetProperty("query_text_index");
        Assert.Equal("Off", fullBlock.GetProperty("status").GetString());
        Assert.False(fullBlock.GetProperty("exists").GetBoolean());
    }
}
