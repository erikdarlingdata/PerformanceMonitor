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
using System.Reflection;
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
/// #7849: the all-servers scope of <c>find_queries_by_text</c> finds where the matched texts ran by reading the matched digests first and binding
/// them to the sample read (the probe), up to a cap; past the cap the raw window is joined to the matches, as it is for every pattern without
/// this. The trigram option's pass also builds an index on <c>query_stats (query_text_digest, collection_time)</c> that the probe reaches on an
/// uncompressed chunk, and drops it with the trigram index; the search is served with or without it. The index and the probe statement are named
/// here as literals and reached by reflection, so that the tests say what the store must hold and what the reply must say, and build on a tree
/// that has neither.
/// </summary>
public sealed class FindQueriesByTextSampleIndexLiveTests
{
    private const string SampleIndexShortName = "ix_query_stats_text_digest";
    private const string SampleIndexName = "collect." + SampleIndexShortName;

    private static readonly DateTime Now = new(2026, 11, 6, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>The probe statement: the all-servers sample read that takes the matched digests as an array.</summary>
    private static string ProbeSql()
    {
        var field = typeof(DarlingMcpFindQueriesByTextTools).GetField("PlanCacheAllServersProbeSql", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);
        Assert.True(field is not null, "find_queries_by_text has no probe statement for the all-servers sample read: the samples are joined to the matched texts for every pattern");
        return (string)field!.GetRawConstantValue()!;
    }

    /// <summary>How many matched texts the probe takes before the sample read falls back to the join.</summary>
    private static int ProbeDigestCap()
    {
        var field = typeof(DarlingMcpFindQueriesByTextTools).GetField("ProbeDigestCap", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);
        Assert.True(field is not null, "find_queries_by_text has no cap on the matched texts the probe takes");
        return (int)field!.GetRawConstantValue()!;
    }

    private static Task<string> RunTool(NpgsqlDataSource data, string pattern, QueryTextTrigramIndexTracker tracker, int limit, CancellationToken ct) =>
        DarlingMcpFindQueriesByTextTools.FindQueriesByText(
            data, pattern, null, null, limit, McpDataDetail.Full,
            DarlingMcpFindQueriesByTextTools.SourceBudget.Default, DarlingMcpFindQueriesByTextTools.SourceBudget.Default, ct, tracker);

    private static async Task<bool?> SampleIndexValidAsync(NpgsqlDataSource data, CancellationToken ct)
    {
        await using var c = await data.OpenConnectionAsync(ct);
        await using var q = new NpgsqlCommand("SELECT i.indisvalid FROM pg_index AS i WHERE i.indexrelid = to_regclass('" + SampleIndexName + "')", c);
        var value = await q.ExecuteScalarAsync(ct);
        return value is bool valid ? valid : null;
    }

    /// <summary>The trigram index as an operator's hand-made one, with no pass run: the store then holds the text index and nothing on the samples,
    /// which is where a store stands between the trigram build and the sample build.</summary>
    private static async Task CreateTrigramIndexByHandAsync(NpgsqlDataSource data, CancellationToken ct)
    {
        await using var c = await data.OpenConnectionAsync(ct);
        await DarlingMcpTestData.ExecAsync(c, ct, "CREATE EXTENSION IF NOT EXISTS pg_trgm");
        await DarlingMcpTestData.ExecAsync(c, ct,
            "CREATE INDEX IF NOT EXISTS " + FindQueriesByTextIndexScopeLiveTests.IndexShortName + " ON collect.query_text_dim USING gin (query_text gin_trgm_ops)");
    }

    private static async Task<byte[][]> DigestsOfAsync(NpgsqlDataSource data, string pattern, CancellationToken ct)
    {
        await using var c = await data.OpenConnectionAsync(ct);
        await using var q = new NpgsqlCommand("SELECT digest FROM collect.query_text_dim WHERE query_text ILIKE '%' || $1 || '%' ESCAPE '\\'", c);
        q.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DarlingMcpFindQueriesByTextTools.EscapeLike(pattern) });
        var digests = new List<byte[]>();
        await using var reader = await q.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            digests.Add((byte[])reader[0]);
        }

        return digests.ToArray();
    }

    /// <summary>The planner's plan for the probe with these digests bound, with sequential scans off so that any index that can serve the digest
    /// read is the cheaper way in and shows up in the plan. Returns the plan as JSON text.</summary>
    private static async Task<string> ExplainProbeAsync(NpgsqlDataSource data, byte[][] digests, CancellationToken ct)
    {
        await using var c = await data.OpenConnectionAsync(ct);
        await DarlingMcpTestData.ExecAsync(c, ct, "SET enable_seqscan = off");
        foreach (var table in new[] { "collect.query_text_dim", "query_stats" })
        {
            await DarlingMcpTestData.ExecAsync(c, ct, "ANALYZE " + table);
        }

        await using var explain = new NpgsqlCommand("EXPLAIN (FORMAT JSON) " + ProbeSql(), c);
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bytea, Value = digests });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = 21 });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Floor() });
        return Convert.ToString(await explain.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!;
    }

    private static DateTime Floor() => DateTime.SpecifyKind(DateTime.UtcNow - TimescaleSupport.RawRetentionSpan, DateTimeKind.Unspecified);

    /// <summary>The rows of the tool's plan-cache source, in reply order, as <c>server@query_hash@database</c>.</summary>
    private static string[] Keys(JsonElement planCache) =>
        planCache.GetProperty("rows").EnumerateArray()
            .Select(r => r.GetProperty("server").GetString() + "@" + r.GetProperty("query_hash").GetString() + "@" + r.GetProperty("database_name").GetString())
            .ToArray();

    /// <summary>Today's read: the window join run directly with the tool's own parameters, its rows in its order as <c>server@query_hash@database</c>,
    /// the row past the page dropped as the tool drops it.</summary>
    private static async Task<string[]> TodaysReadAsync(NpgsqlDataSource data, string pattern, int limit, CancellationToken ct)
    {
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
        join.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Floor() });
        var rows = new List<string>();
        await using (var reader = await join.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                rows.Add(names[reader.GetInt32(0)] + "@" + reader.GetString(1) + "@" + reader.GetString(2));
            }
        }

        if (rows.Count > limit) rows.RemoveAt(rows.Count - 1);
        return rows.ToArray();
    }

    private static void AssertProbe(JsonElement planCache, int matched)
    {
        Assert.Equal("ok", planCache.GetProperty("status").GetString());
        Assert.True(planCache.TryGetProperty("sample_read", out var sampleRead), "the plan_cache source does not say how its samples were read (sample_read)");
        Assert.Equal("probe", sampleRead.GetString());
        Assert.Equal(matched, planCache.GetProperty("matched_texts").GetInt32());
    }

    private static void AssertJoin(JsonElement planCache)
    {
        Assert.Equal("ok", planCache.GetProperty("status").GetString());
        Assert.True(planCache.TryGetProperty("sample_read", out var sampleRead), "the plan_cache source does not say how its samples were read (sample_read)");
        Assert.Equal("join", sampleRead.GetString());
        Assert.Equal(JsonValueKind.Null, planCache.GetProperty("matched_texts").ValueKind);
    }

    /// <summary>One pass with the option on leaves the sample index valid; the probe's plan reaches it (with sequential scans off, so any index that
    /// can serve the digest read is the way in); and the tool's rows for a pattern matching one text are the probe's and the same rows today's join lists.</summary>
    [Fact]
    public async Task WithTheSampleIndex_TheProbeReachesIt_AndListsTodaysRows()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FindQueriesByTextIndexScopeLiveTests.OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var tracker = await FindQueriesByTextIndexScopeLiveTests.BuildIndexAsync(data, ct);

        Assert.True(await SampleIndexValidAsync(data, ct), "the pass with the option on did not leave a valid " + SampleIndexName + " on query_stats");

        var digests = await DigestsOfAsync(data, "orders_77 where", ct);
        Assert.Single(digests);
        var plan = await ExplainProbeAsync(data, digests, ct);
        Assert.Contains(SampleIndexShortName, plan, StringComparison.Ordinal);

        var reply = JsonDocument.Parse(await RunTool(data, "orders_77 where", tracker, 20, ct)).RootElement;
        Assert.Equal("all_servers", reply.GetProperty("scope").GetString());
        var planCache = reply.GetProperty("sources").GetProperty("plan_cache");
        AssertProbe(planCache, 1);
        var keys = Keys(planCache);
        Assert.Equal(new[] { "ix-a", "ix-b" }, keys.Select(k => k.Split('@')[0]).OrderBy(x => x, StringComparer.Ordinal).ToArray());
        Assert.Equal(await TodaysReadAsync(data, "orders_77 where", 20, ct), keys);
    }

    /// <summary>With the trigram index alone (no sample index), the state is Ready, the probe's plan reaches no sample index, and the tool still
    /// answers through the probe with the same rows today's join lists: the index is an accelerator, not a gate.</summary>
    [Fact]
    public async Task WithoutTheSampleIndex_TheProbeStillServes_AndListsTodaysRows()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FindQueriesByTextIndexScopeLiveTests.OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        await CreateTrigramIndexByHandAsync(data, ct);
        var tracker = new QueryTextTrigramIndexTracker();
        tracker.Configure(true);
        Assert.Null(await SampleIndexValidAsync(data, ct));

        await using (var c = await data.OpenConnectionAsync(ct))
        {
            var health = await QueryTextTrigramIndex.ReadHealthAsync(c, tracker, ct);
            Assert.Equal(TrigramIndexStatus.Ready, health.Status);
        }

        var plan = await ExplainProbeAsync(data, await DigestsOfAsync(data, "orders_77 where", ct), ct);
        Assert.DoesNotContain(SampleIndexShortName, plan, StringComparison.Ordinal);
        Assert.Contains("query_stats", plan, StringComparison.Ordinal);

        var reply = JsonDocument.Parse(await RunTool(data, "orders_77 where", tracker, 20, ct)).RootElement;
        var planCache = reply.GetProperty("sources").GetProperty("plan_cache");
        AssertProbe(planCache, 1);
        Assert.Equal(await TodaysReadAsync(data, "orders_77 where", 20, ct), Keys(planCache));
    }

    /// <summary>A pattern every seeded text contains matches far more texts than the cap, so the sample read is the join, and the page it lists is
    /// today's page in today's order.</summary>
    [Fact]
    public async Task PastTheCap_TheSampleReadIsTheJoin_AndListsTodaysRows()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FindQueriesByTextIndexScopeLiveTests.OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        await CreateTrigramIndexByHandAsync(data, ct);
        var tracker = new QueryTextTrigramIndexTracker();
        tracker.Configure(true);
        Assert.True((await DigestsOfAsync(data, "dbo.Orders_", ct)).Length > ProbeDigestCap(), "the fixture's pattern must match more texts than the cap");

        var reply = JsonDocument.Parse(await RunTool(data, "dbo.Orders_", tracker, DarlingMcpFindQueriesByTextTools.MaxLimit, ct)).RootElement;

        var planCache = reply.GetProperty("sources").GetProperty("plan_cache");
        AssertJoin(planCache);
        Assert.True(planCache.GetProperty("truncated").GetBoolean());
        Assert.Equal(DarlingMcpFindQueriesByTextTools.MaxLimit, planCache.GetProperty("returned").GetInt32());
        Assert.Equal(await TodaysReadAsync(data, "dbo.Orders_", DarlingMcpFindQueriesByTextTools.MaxLimit, ct), Keys(planCache));
    }

    /// <summary>The cap is inclusive: exactly the cap's worth of matched texts is probed, one more is joined, and both list the same rows.</summary>
    [Fact]
    public async Task AtTheCap_TheSamplesAreProbed_OnePastIt_TheyAreJoined()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FindQueriesByTextIndexScopeLiveTests.OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        await CreateTrigramIndexByHandAsync(data, ct);
        var tracker = new QueryTextTrigramIndexTracker();
        tracker.Configure(true);
        var cap = ProbeDigestCap();

        await using (var c = await data.OpenConnectionAsync(ct))
        {
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
                  SELECT sha256(('boundary_' || i)::text::bytea), 'SELECT * FROM dbo.Boundary_' || i || ' WHERE Id = @p', timezone('UTC', now())
                  FROM generate_series(1, $1) AS i",
                cap);
            foreach (var (hash, text) in new[] { ("0xC001", "boundary_1"), ("0xC002", "boundary_2") })
            {
                await DarlingMcpTestData.ExecAsync(c, ct,
                    @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text_digest)
                      VALUES ($1, $2, 2, 'ix-b', 'Hr', $3, sha256($4::text::bytea))",
                    CollectionIdGenerator.Next(), LiveClock.AnchoredEnd() - TimeSpan.FromHours(1), hash, text);
            }
        }

        var atCap = JsonDocument.Parse(await RunTool(data, "dbo.Boundary_", tracker, 20, ct)).RootElement.GetProperty("sources").GetProperty("plan_cache");
        AssertProbe(atCap, cap);
        var expected = await TodaysReadAsync(data, "dbo.Boundary_", 20, ct);
        Assert.Equal(2, expected.Length);
        Assert.Equal(expected, Keys(atCap));

        await using (var c = await data.OpenConnectionAsync(ct))
        {
            await DarlingMcpTestData.ExecAsync(c, ct,
                "INSERT INTO collect.query_text_dim (digest, query_text, last_seen) VALUES (sha256('boundary_more'::text::bytea), 'SELECT * FROM dbo.Boundary_more WHERE Id = @p', timezone('UTC', now()))");
        }

        var pastCap = JsonDocument.Parse(await RunTool(data, "dbo.Boundary_", tracker, 20, ct)).RootElement.GetProperty("sources").GetProperty("plan_cache");
        AssertJoin(pastCap);
        Assert.Equal(expected, Keys(pastCap));
    }

    /// <summary>The option off drops the sample index with the trigram index: a store that holds one from an earlier run is left with neither.</summary>
    [Fact]
    public async Task APassWithTheOptionOff_DropsTheSampleIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FindQueriesByTextIndexScopeLiveTests.OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using (var c = await data.OpenConnectionAsync(ct))
        {
            await DarlingMcpTestData.ExecAsync(c, ct,
                "CREATE INDEX " + SampleIndexShortName + " ON collect.query_stats (query_text_digest, collection_time DESC) WHERE query_text_digest IS NOT NULL");
        }

        Assert.True(await SampleIndexValidAsync(data, ct), "the fixture's index was not created, so the drop below would prove nothing");
        var tracker = new QueryTextTrigramIndexTracker();
        tracker.Configure(false);

        await QueryTextTrigramIndex.RunPassAsync(data, false, tracker, Now, NullLogger.Instance, ct);

        Assert.Null(await SampleIndexValidAsync(data, ct));
    }

    /// <summary>The store health read names the sample index beside the trigram index, so an operator watching the build sees both.</summary>
    [Fact]
    public async Task GetStoreMetrics_QueryTextIndexBlock_NamesTheSampleIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FindQueriesByTextIndexScopeLiveTests.OpenScratchAsync(ct);
        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var reply = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(data, cancellationToken: ct));

        var block = reply.RootElement.GetProperty("query_text_index");
        Assert.True(block.TryGetProperty("sample_index_name", out var name), "query_text_index does not name the sample index");
        Assert.Equal(SampleIndexName, name.GetString());
        Assert.True(block.TryGetProperty("sample_index_exists", out var exists), "query_text_index does not say whether the sample index exists");
        Assert.False(exists.GetBoolean());
    }

    /// <summary>On a TimescaleDB store <c>query_stats</c> is a hypertable, which refuses <c>CREATE INDEX CONCURRENTLY</c> and <c>DROP INDEX CONCURRENTLY</c>.
    /// The pass builds the sample index there too (valid on the parent, one chunk at a time), the probe reaches it, and the option off drops it.</summary>
    [Fact]
    public async Task OnAHypertable_ThePassBuildsTheSampleIndex_TheProbeReachesIt_AndTheOptionOffDropsIt()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #7849 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var c = new NpgsqlConnection(scratch.ConnectionString))
        {
            await c.OpenAsync(ct);
            await PgMigrations.MigrateAsync(c, ct);
            Assert.SkipUnless(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB is not available; the per-chunk build needs a hypertable.");
            await TimescaleSupport.ConvertToHypertablesAsync(c, null, ct);
            await DarlingMcpTestData.ExecAsync(c, ct, "SELECT _timescaledb_functions.stop_background_workers()");
            await FindQueriesByTextIndexScopeLiveTests.SeedAsync(c, ct);
        }

        await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
        var tracker = await FindQueriesByTextIndexScopeLiveTests.BuildIndexAsync(data, ct);

        Assert.True(await SampleIndexValidAsync(data, ct), "the pass with the option on did not leave a valid " + SampleIndexName + " on the hypertable");
        var plan = await ExplainProbeAsync(data, await DigestsOfAsync(data, "orders_77 where", ct), ct);
        Assert.Contains(SampleIndexShortName, plan, StringComparison.Ordinal);
        var planCache = JsonDocument.Parse(await RunTool(data, "orders_77 where", tracker, 20, ct)).RootElement.GetProperty("sources").GetProperty("plan_cache");
        AssertProbe(planCache, 1);
        Assert.Equal(await TodaysReadAsync(data, "orders_77 where", 20, ct), Keys(planCache));

        tracker.Configure(false);
        await QueryTextTrigramIndex.RunPassAsync(data, false, tracker, Now.AddHours(1), NullLogger.Instance, ct);

        Assert.Null(await SampleIndexValidAsync(data, ct));
    }
}
