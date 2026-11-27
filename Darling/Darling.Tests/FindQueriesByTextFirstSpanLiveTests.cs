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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Each test creates its own scratch database through ScratchPostgres and
   works entirely inside it, so it cannot race live collection. */
/// <summary>
/// #9219: <c>find_queries_by_text</c> never stops with nothing searched when the newest slice of the raw tier can be read inside the budget. The
/// first span of a search is a small slice of the newest data and the spans widen after it, so a store whose whole window cannot be read in the
/// budget still answers with the newest matches and the span they cover. A slow span is modelled without a sleep: the budget's statement hook
/// takes an advisory lock that a second connection holds, for a span wider than the test allows, so that statement waits on the lock until
/// the source's own timeout cancels it, as a statement that cannot finish in the budget is cancelled. The spans' widths come from the tool:
/// its own schedule for the one-server search and the probe, and the span each statement binds for the all-servers search, whose lookup
/// spans are sized by a row count (#9605). So the test needs no knowledge of how wide any span is, only that the window cannot be read whole.
/// </summary>
public sealed class FindQueriesByTextFirstSpanLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the find_queries_by_text first-span live tests.";

    /// <summary>The widest span the modelled store can read inside the budget. The window is days long, so the whole of it never fits.</summary>
    private static readonly TimeSpan ReadableSpan = TimeSpan.FromHours(2);

    /// <summary>The source's budget in the modelled runs: a span the store cannot read waits out all of it, so it is short, and a span it can read
    /// takes milliseconds, so it is far from the edge.</summary>
    private const int ModelledBudgetMilliseconds = 2500;

    private const long LockKey = 921900;

    /// <summary>The samples an all-servers lookup span reads for each of its parts in the modelled runs. The seed holds eleven samples, fewer
    /// than one span reads by default, and a window read whole by the first statement has no later span to be slow on. At three a part the
    /// newest span ends at the samples of 30 minutes ago and the second at those of 90 minutes ago, both inside <see cref="ReadableSpan"/>, and
    /// the third takes the rest of the window, which is not.</summary>
    private const int ModelledSpanRows = 3;

    private const string CommonPattern = "dbo.Common";
    private const string RarePattern = "dbo.Rare";

    /// <summary>One server, "first-a", runs statements that name dbo.Rare 20, 30 and 40 minutes ago, 90 minutes ago, five hours ago and three days
    /// ago (R1 to R6), and statements that name dbo.Common (more texts than the probe takes, because 1,100 filler texts name it too) 30 minutes ago
    /// on it (C1), 40 minutes ago on "first-b" (C2), 90 minutes ago (C3), five hours ago (C4) and three days ago (C5). Every sample is in the
    /// Books database.</summary>
    private static async Task<ScratchPostgres> OpenScratchAsync(CancellationToken ct)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        try
        {
            await using var c = new NpgsqlConnection(scratch.ConnectionString);
            await c.OpenAsync(ct);
            await PgMigrations.MigrateAsync(c, ct);
            await DarlingMcpTestData.RegisterServerAsync(c, 1, "first-a", ct);
            await DarlingMcpTestData.RegisterServerAsync(c, 2, "first-b", ct);
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
                  SELECT sha256(('filler' || i)::text::bytea), 'SELECT ' || i || ' FROM dbo.Common', timezone('UTC', now())
                  FROM generate_series(1, 1100) AS i");
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
                  SELECT sha256(t::bytea), CASE WHEN t LIKE 'r%' THEN 'SELECT * FROM dbo.Rare WHERE k = ' ELSE 'SELECT * FROM dbo.Common WHERE k = ' END || t, timezone('UTC', now())
                  FROM unnest(ARRAY['r1','r2','r3','r4','r5','r6','c1','c2','c3','c4','c5']) AS t");

            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            async Task Stat(int server, string text, TimeSpan age) =>
                await DarlingMcpTestData.ExecAsync(c, ct,
                    @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text_digest)
                      VALUES ($1,$2,$3,$4,'Books',$5,sha256($6::text::bytea))",
                    CollectionIdGenerator.Next(), now - age, server, server == 1 ? "first-a" : "first-b", "0x" + text.ToUpperInvariant(), text);

            await Stat(1, "r1", TimeSpan.FromMinutes(20));
            await Stat(1, "r2", TimeSpan.FromMinutes(30));
            await Stat(1, "r3", TimeSpan.FromMinutes(40));
            await Stat(1, "r4", TimeSpan.FromMinutes(90));
            await Stat(1, "r5", TimeSpan.FromHours(5));
            await Stat(1, "r6", TimeSpan.FromDays(3));
            await Stat(1, "c1", TimeSpan.FromMinutes(30));
            await Stat(2, "c2", TimeSpan.FromMinutes(40));
            await Stat(1, "c3", TimeSpan.FromMinutes(90));
            await Stat(1, "c4", TimeSpan.FromHours(5));
            await Stat(1, "c5", TimeSpan.FromDays(3));

            await DarlingMcpTestData.ExecAsync(c, ct, "CREATE EXTENSION IF NOT EXISTS pg_trgm");
            await DarlingMcpTestData.ExecAsync(c, ct,
                "CREATE INDEX IF NOT EXISTS " + FindQueriesByTextIndexScopeLiveTests.IndexShortName + " ON collect.query_text_dim USING gin (query_text gin_trgm_ops)");
            await DarlingMcpTestData.ExecAsync(c, ct, "ANALYZE collect.query_text_dim");
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

    /// <summary>A second connection holding the advisory lock a modelled slow span waits on, for as long as the test runs.</summary>
    private sealed class SlowStore : IAsyncDisposable
    {
        private readonly NpgsqlConnection _holder;

        private SlowStore(NpgsqlConnection holder) => _holder = holder;

        public static async Task<SlowStore> OpenAsync(string connectionString, CancellationToken ct)
        {
            var holder = new NpgsqlConnection(connectionString);
            await holder.OpenAsync(ct);
            await DarlingMcpTestData.ExecAsync(holder, ct, "SELECT pg_advisory_lock(" + LockKey.ToString(CultureInfo.InvariantCulture) + ")");
            return new SlowStore(holder);
        }

        /// <summary>The source's budget for a store that reads a span of up to <see cref="ReadableSpan"/> and cannot read a wider one: before the
        /// statement of a wider span the hook waits on the lock the holder has, so the source's timeout cancels it.</summary>
        public static DarlingMcpFindQueriesByTextTools.SourceBudget Budget(List<TimeSpan>? widths = null)
        {
            var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            var spans = DarlingMcpFindQueriesByTextTools.RawTierSpans(now - TimescaleSupport.RawRetentionSpan, now);
            var step = 0;
            return new DarlingMcpFindQueriesByTextTools.SourceBudget(ModelledBudgetMilliseconds)
            {
                BeforeStatement = async (connection, transaction, token) =>
                {
                    var span = spans[Math.Min(step++, spans.Count - 1)];
                    var width = (span.Ceiling > now ? now : span.Ceiling) - span.Floor;
                    widths?.Add(width);
                    if (width > ReadableSpan)
                    {
                        await using var wait = new NpgsqlCommand("SELECT pg_advisory_xact_lock(" + LockKey.ToString(CultureInfo.InvariantCulture) + ")", connection, transaction);
                        await wait.ExecuteNonQueryAsync(token);
                    }
                },
            };
        }

        /// <summary>The all-servers search's budget for the same store. A lookup span is sized by a row count, so the span a statement reads is
        /// the one it binds (its third and fourth parameters, the floor and the ceiling), not one of a fixed schedule: the hook, which runs once
        /// the statement is bound, waits on the lock when that span is wider than <see cref="ReadableSpan"/>. A span reads
        /// <see cref="ModelledSpanRows"/> samples a part, so the seed is several spans, as a large store is.</summary>
        public static DarlingMcpFindQueriesByTextTools.SourceBudget AllServersBudget(List<TimeSpan>? widths = null)
        {
            var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            var width = TimeSpan.Zero;
            return new DarlingMcpFindQueriesByTextTools.SourceBudget(ModelledBudgetMilliseconds)
            {
                SpanRows = ModelledSpanRows,
                StatementBound = (_, parameters) =>
                {
                    var floor = (DateTime)parameters[2].Value!;
                    var ceiling = (DateTime)parameters[3].Value!;
                    width = (ceiling > now ? now : ceiling) - floor;
                },
                BeforeStatement = async (connection, transaction, token) =>
                {
                    widths?.Add(width);
                    if (width > ReadableSpan)
                    {
                        await using var wait = new NpgsqlCommand("SELECT pg_advisory_xact_lock(" + LockKey.ToString(CultureInfo.InvariantCulture) + ")", connection, transaction);
                        await wait.ExecuteNonQueryAsync(token);
                    }
                },
            };
        }

        public async ValueTask DisposeAsync() => await _holder.DisposeAsync();
    }

    private static async Task<JsonElement> AllServersAsync(
        string connectionString, string pattern, DarlingMcpFindQueriesByTextTools.SourceBudget planCache, int limit, CancellationToken ct)
    {
        await using var data = NpgsqlDataSource.Create(connectionString);
        var reply = await DarlingMcpFindQueriesByTextTools.FindQueriesByText(
            data, pattern, null, null, limit, McpDataDetail.Full, DarlingMcpFindQueriesByTextTools.SourceBudget.Default, planCache, ct, ReadyTracker());
        return JsonDocument.Parse(reply).RootElement.GetProperty("sources").GetProperty("plan_cache");
    }

    private static async Task<JsonElement> OneServerAsync(
        string connectionString, string pattern, DarlingMcpFindQueriesByTextTools.SourceBudget planCache, int limit, bool indexReady, CancellationToken ct)
    {
        await using var data = NpgsqlDataSource.Create(connectionString);
        var reply = await DarlingMcpFindQueriesByTextTools.FindQueriesByText(
            data, pattern, "first-a", null, limit, McpDataDetail.Full, DarlingMcpFindQueriesByTextTools.SourceBudget.Default, planCache, ct,
            indexReady ? ReadyTracker() : new QueryTextTrigramIndexTracker());
        return JsonDocument.Parse(reply).RootElement.GetProperty("sources").GetProperty("plan_cache");
    }

    private static string[] AllServersKeys(JsonElement planCache) =>
        planCache.GetProperty("rows").EnumerateArray().Select(r => r.GetProperty("server").GetString() + "@" + r.GetProperty("query_hash").GetString()).ToArray();

    private static string[] Hashes(JsonElement planCache) =>
        planCache.GetProperty("rows").EnumerateArray().Select(r => r.GetProperty("query_hash").GetString()!).ToArray();

    private static DateTime Instant(JsonElement searched, string name) =>
        DateTime.Parse(searched.GetProperty(name).GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);

    /// <summary>A stopped search names a span it did search: from a time inside the window to the window's end, and no wider than the spans the
    /// store could read.</summary>
    private static void AssertNamesTheSpanItSearched(JsonElement planCache)
    {
        var searched = planCache.GetProperty("searched");
        Assert.False(searched.GetProperty("complete").GetBoolean());
        Assert.NotEqual(JsonValueKind.Null, searched.GetProperty("searched_from").ValueKind);
        Assert.Equal(searched.GetProperty("window_to").GetString(), searched.GetProperty("searched_to").GetString());
        var covered = Instant(searched, "searched_to") - Instant(searched, "searched_from");
        Assert.InRange(covered.TotalMinutes, 1, TimeSpan.FromHours(5).TotalMinutes);
    }

    /// <summary>All servers, a pattern more texts contain than the probe takes: lookup spans from end to end. The store reads a span of up to two
    /// hours and not a wider one, so the search reads the newest two spans (the samples back to 30 minutes ago, then those back to 90 minutes
    /// ago) and stops at the third, which is the rest of the window. It returns the newest matches and names what it covered, not nothing: a
    /// search whose first statement read the whole window would have waited on the lock at once.</summary>
    [Fact]
    public async Task AllServers_ACommonPattern_OnAStoreThatCannotReadTheWholeWindow_ReturnsTheNewestMatches_AndSaysWhatItSearched()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await OpenScratchAsync(ct);
        await using var store = await SlowStore.OpenAsync(scratch.ConnectionString, ct);

        var planCache = await AllServersAsync(scratch.ConnectionString, CommonPattern, SlowStore.AllServersBudget(), 20, ct);

        Assert.Equal("stopped", planCache.GetProperty("status").GetString());
        Assert.Equal("join", planCache.GetProperty("sample_read").GetString());
        Assert.Equal(["first-a@0xC1", "first-b@0xC2", "first-a@0xC3"], AllServersKeys(planCache));
        AssertNamesTheSpanItSearched(planCache);
    }

    /// <summary>The same store, a pattern few texts contain: the newest lookup span, then the digest probe, which reads the rest of the window
    /// in the fixed spans and stops at the first one wider than the store can read.</summary>
    [Fact]
    public async Task AllServers_ARarePattern_OnAStoreThatCannotReadTheWholeWindow_ReturnsTheNewestMatches_AndSaysWhatItSearched()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await OpenScratchAsync(ct);
        await using var store = await SlowStore.OpenAsync(scratch.ConnectionString, ct);

        var planCache = await AllServersAsync(scratch.ConnectionString, RarePattern, SlowStore.AllServersBudget(), 20, ct);

        Assert.Equal("stopped", planCache.GetProperty("status").GetString());
        Assert.Equal("probe", planCache.GetProperty("sample_read").GetString());
        Assert.Equal(["first-a@0xR1", "first-a@0xR2", "first-a@0xR3", "first-a@0xR4"], AllServersKeys(planCache));
        AssertNamesTheSpanItSearched(planCache);
    }

    /// <summary>One server, no database, a rare pattern, at each limit. Limits 1, 2 and 3 fill their page in the first two spans, so they answer
    /// <c>ok</c>; the default limit does not fill it, so it stops at the span the store cannot read, with the four matches the spans before it
    /// held. None of them stops with nothing searched, with the plan-cache index ready or off (an off index reads in spans too).</summary>
    [Theory]
    [InlineData(true, 1)]
    [InlineData(true, 2)]
    [InlineData(true, 3)]
    [InlineData(true, DarlingMcpFindQueriesByTextTools.DefaultLimit)]
    [InlineData(false, 1)]
    [InlineData(false, 2)]
    [InlineData(false, 3)]
    [InlineData(false, DarlingMcpFindQueriesByTextTools.DefaultLimit)]
    public async Task OneServer_ARarePattern_WithNoDatabase_OnAStoreThatCannotReadTheWholeWindow_NeverStopsWithNothingSearched(bool indexReady, int limit)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await OpenScratchAsync(ct);
        await using var store = await SlowStore.OpenAsync(scratch.ConnectionString, ct);

        var planCache = await OneServerAsync(scratch.ConnectionString, RarePattern, SlowStore.Budget(), limit, indexReady, ct);

        string[] newest = ["0xR1", "0xR2", "0xR3", "0xR4"];
        Assert.NotEqual(JsonValueKind.Null, planCache.GetProperty("searched").GetProperty("searched_from").ValueKind);
        Assert.Equal(newest.Take(limit).ToArray(), Hashes(planCache));
        if (limit < DarlingMcpFindQueriesByTextTools.DefaultLimit)
        {
            Assert.Equal("ok", planCache.GetProperty("status").GetString());
            Assert.True(planCache.GetProperty("searched").GetProperty("complete").GetBoolean());
            Assert.True(planCache.GetProperty("truncated").GetBoolean());
        }
        else
        {
            Assert.Equal("stopped", planCache.GetProperty("status").GetString());
            AssertNamesTheSpanItSearched(planCache);
            Assert.Equal(indexReady ? "probe" : null, planCache.TryGetProperty("sample_read", out var read) ? read.GetString() : null);
        }
    }

    /// <summary>A search that finishes says so: <c>complete</c> and the whole window, in a store that reads every span, for both scopes.</summary>
    [Fact]
    public async Task AFinishedSearch_IsComplete_AndNamesTheWholeWindow()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await OpenScratchAsync(ct);
        var roomy = new DarlingMcpFindQueriesByTextTools.SourceBudget(60_000);

        foreach (var planCache in new[]
        {
            await AllServersAsync(scratch.ConnectionString, CommonPattern, roomy, 20, ct),
            await OneServerAsync(scratch.ConnectionString, RarePattern, roomy, 20, true, ct),
            await OneServerAsync(scratch.ConnectionString, RarePattern, roomy, 20, false, ct),
        })
        {
            Assert.Equal("ok", planCache.GetProperty("status").GetString());
            var searched = planCache.GetProperty("searched");
            Assert.True(searched.GetProperty("complete").GetBoolean());
            Assert.Equal(searched.GetProperty("window_from").GetString(), searched.GetProperty("searched_from").GetString());
            Assert.Equal(searched.GetProperty("window_to").GetString(), searched.GetProperty("searched_to").GetString());
        }
    }

    /// <summary>A pattern that is not there answers <c>empty</c> and complete, not <c>stopped</c>, on a store that cannot read the window whole:
    /// no text matches, so no sample statement runs and nothing is left to read.</summary>
    [Fact]
    public async Task APatternThatIsNotThere_IsEmptyAndComplete_NotStopped()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await OpenScratchAsync(ct);
        await using var store = await SlowStore.OpenAsync(scratch.ConnectionString, ct);

        foreach (var planCache in new[]
        {
            await AllServersAsync(scratch.ConnectionString, "dbo.NotThere", SlowStore.AllServersBudget(), 20, ct),
            await OneServerAsync(scratch.ConnectionString, "dbo.NotThere", SlowStore.Budget(), 20, true, ct),
        })
        {
            Assert.Equal("empty", planCache.GetProperty("status").GetString());
            Assert.True(planCache.GetProperty("searched").GetProperty("complete").GetBoolean());
        }
    }

    /// <summary>The first statement of every search is a small slice of the newest data and no span after it is narrower, so a store that is slow
    /// on any part of the window is slow on the newest slice last. An all-servers span is sized by rows, and it reads no fewer than the one
    /// before it, so on this seed, whose samples thin out with age, each is at least as wide in time as the one before it.</summary>
    [Fact]
    public async Task TheFirstSpanOfASearch_IsTheNewestSlice_AndTheSpansWidenAfterIt()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await OpenScratchAsync(ct);
        await using var store = await SlowStore.OpenAsync(scratch.ConnectionString, ct);

        var widths = new List<TimeSpan>();
        _ = await AllServersAsync(scratch.ConnectionString, CommonPattern, SlowStore.AllServersBudget(widths), 20, ct);

        Assert.True(widths.Count >= 3, "the store reads the newest spans, so the hook ran for each");
        Assert.True(widths[0] <= TimescaleSupport.RawRetentionSpan / 90, "the first statement reads a small slice of the window");
        for (var i = 1; i < widths.Count; i++)
        {
            Assert.True(widths[i] >= widths[i - 1], "a span after the first is no narrower than the one before it");
        }
    }
}
