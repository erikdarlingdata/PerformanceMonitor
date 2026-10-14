/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Protocol;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>#5506 part 3: the read cache itself: storing, the TTL on a fake clock, the 1 MB and 64 MB limits, and the
/// single flight that runs identical misses once (D6).</summary>
public sealed class ReadResultCacheTests
{
    [Fact]
    public void AStoredAnswer_IsServed_UntilItsTtlRunsOut()
    {
        var clock = new TimerTimeProvider();
        using var cache = new ReadResultCache(clock);
        var ttl = ReadResultCache.TtlOf(ReadWindowBand.OneDay);
        Assert.Equal(TimeSpan.FromSeconds(30), ttl);

        Assert.True(cache.TryStore("k", "{\"rows\":[1]}", ttl));
        clock.Advance(TimeSpan.FromSeconds(29));
        Assert.True(cache.TryGet("k", out var text));
        Assert.Equal("{\"rows\":[1]}", text);

        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.False(cache.TryGet("k", out _));
    }

    [Theory]
    [InlineData("none", 15)]
    [InlineData("1h", 15)]
    [InlineData("24h", 30)]
    [InlineData("7d", 60)]
    [InlineData("7d+", 120)]
    public void TheTtl_FollowsTheBand(string band, int seconds) =>
        Assert.Equal(TimeSpan.FromSeconds(seconds), ReadResultCache.TtlOf(band));

    [Fact]
    public void AnAnswerOverOneMegabyte_IsNeverStored()
    {
        using var cache = new ReadResultCache(new TimerTimeProvider());
        var big = "[\"" + new string('x', ReadResultCache.MaxAnswerBytes) + "\"]";
        Assert.False(cache.TryStore("big", big, TimeSpan.FromSeconds(60)));
        Assert.False(cache.TryGet("big", out _));

        /* Counted in UTF-8: half a million two-byte characters is over the limit too. */
        var wide = "[\"" + new string('é', ReadResultCache.MaxAnswerBytes / 2) + "\"]";
        Assert.False(cache.TryStore("wide", wide, TimeSpan.FromSeconds(60)));
    }

    [Fact]
    public void TheSizeCap_EvictsOlderEntries_SoTheNewestFits()
    {
        /* A 64 KB cap stands in for the 64 MB one: each entry is about 20 KB. */
        using var cache = new ReadResultCache(new TimerTimeProvider(), sizeLimitBytes: 64 * 1024);
        var text = "[\"" + new string('x', 10_000) + "\"]";
        for (var i = 0; i < 12; i++)
        {
            Assert.True(cache.TryStore("k" + i, text, TimeSpan.FromSeconds(60)));
        }

        Assert.True(cache.Count <= 3);
        Assert.True(cache.TryGet("k11", out _));
        Assert.False(cache.TryGet("k0", out _));
        Assert.Equal(64L * 1024 * 1024, ReadResultCache.DefaultSizeLimitBytes);
    }

    [Fact]
    public async Task TwoIdenticalMisses_RunOneRead_AndTheAnswerIsStored()
    {
        using var cache = new ReadResultCache(new TimerTimeProvider());
        var hold = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var runs = 0;
        Task<string> Read(CancellationToken token)
        {
            Interlocked.Increment(ref runs);
            return hold.Task;
        }

        var first = cache.Join("k", TimeSpan.FromSeconds(15), Read, t => t);
        var second = cache.Join("k", TimeSpan.FromSeconds(15), Read, t => t);
        Assert.True(first.IsLead);
        Assert.False(second.IsLead);

        hold.SetResult("[1]");
        Assert.Equal("[1]", await first.WaitAsync(CancellationToken.None));
        Assert.Equal("[1]", await second.WaitAsync(CancellationToken.None));
        Assert.Equal(1, runs);
        Assert.True(cache.TryGet("k", out var stored));
        Assert.Equal("[1]", stored);
    }

    [Fact]
    public async Task ACallerThatLeaves_DoesNotFailTheOthers()
    {
        using var cache = new ReadResultCache(new TimerTimeProvider());
        var hold = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        CancellationToken readToken = default;
        Task<string> Read(CancellationToken token)
        {
            readToken = token;
            return hold.Task;
        }

        var lead = cache.Join("k", TimeSpan.FromSeconds(15), Read, t => t);
        var joined = cache.Join("k", TimeSpan.FromSeconds(15), Read, t => t);
        var third = cache.Join("k", TimeSpan.FromSeconds(15), Read, t => t);
        using var leaves = new CancellationTokenSource();
        var leaving = joined.WaitAsync(leaves.Token);
        leaves.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => leaving);
        Assert.False(readToken.IsCancellationRequested);

        hold.SetResult("[2]");
        Assert.Equal("[2]", await lead.WaitAsync(CancellationToken.None));
        Assert.Equal("[2]", await third.WaitAsync(CancellationToken.None));
    }

    [Fact]
    public async Task WhenEveryCallerLeaves_TheSharedReadIsCancelled()
    {
        using var cache = new ReadResultCache(new TimerTimeProvider());
        Task<string> Read(CancellationToken token) => Task.Delay(Timeout.Infinite, token).ContinueWith(_ => "never", TaskScheduler.Default);
        var lead = cache.Join("k", TimeSpan.FromSeconds(15), async token =>
        {
            await Task.Delay(Timeout.Infinite, token);
            return "never";
        }, t => t);
        var joined = cache.Join("k", TimeSpan.FromSeconds(15), Read, t => t);
        using var a = new CancellationTokenSource();
        using var b = new CancellationTokenSource();
        var leadWait = lead.WaitAsync(a.Token);
        var joinedWait = joined.WaitAsync(b.Token);
        b.Cancel();
        a.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => joinedWait);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => leadWait);

        /* The cancelled flight is gone: the next identical call starts a new read. */
        var next = cache.Join("k", TimeSpan.FromSeconds(15), _ => Task.FromResult("[3]"), t => t);
        Assert.True(next.IsLead);
        Assert.Equal("[3]", await next.WaitAsync(CancellationToken.None));
    }

    [Fact]
    public async Task AFailedRead_ReachesEveryCaller_AndIsNotStored()
    {
        using var cache = new ReadResultCache(new TimerTimeProvider());
        var hold = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var lead = cache.Join("k", TimeSpan.FromSeconds(15), _ => hold.Task, t => t);
        var joined = cache.Join("k", TimeSpan.FromSeconds(15), _ => hold.Task, t => t);
        hold.SetException(new InvalidOperationException("store down"));

        await Assert.ThrowsAsync<InvalidOperationException>(() => lead.WaitAsync(CancellationToken.None));
        await Assert.ThrowsAsync<InvalidOperationException>(() => joined.WaitAsync(CancellationToken.None));
        Assert.False(cache.TryGet("k", out _));
    }
}

/// <summary>#5506 part 3: the cache keys. Argument order, defaults and number forms make one key; the write generation,
/// the TTL bucket, the GCF switch and the compose surface make different ones.</summary>
public sealed class ReadCacheKeyTests
{
    private static Dictionary<string, JsonElement> Args(string json) =>
        JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(json)!;

    private static McpToolParameterTypes Types()
    {
        var types = new McpToolParameterTypes();
        types.Register("probe", typeof(ReadCacheKeyTests).GetMethod(nameof(Probe), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, null);
        return types;
    }

    private static string Probe(string? server_name = null, int hours_back = 24, string? as_of = null) => string.Empty;

    [Fact]
    public void ArgumentOrder_Defaults_AndNumberForms_MakeOneKey()
    {
        using var cache = new ReadResultCache(new TimerTimeProvider());
        var types = Types();
        var ttl = TimeSpan.FromSeconds(30);
        string Key(string json) => ReadCacheKey.ForMcp("probe", Args(json), types, false, 0, cache, ttl);

        var expected = Key("{\"server_name\":\"a\",\"hours_back\":24}");
        Assert.Equal(expected, Key("{\"hours_back\":24,\"server_name\":\"a\"}"));
        Assert.Equal(expected, Key("{\"hours_back\":24.0,\"server_name\":\"a\"}"));
        Assert.Equal(expected, Key("{\"server_name\":\"a\"}"));
        Assert.Equal(expected, Key("{\"server_name\":\"a\",\"as_of\":null}"));
        Assert.NotEqual(expected, Key("{\"server_name\":\"a\",\"hours_back\":25}"));
        Assert.NotEqual(expected, Key("{\"server_name\":\"b\"}"));
    }

    [Fact]
    public void TheGeneration_TheBucket_TheAnchor_AndGcf_MakeDifferentKeys()
    {
        var clock = new TimerTimeProvider();
        using var cache = new ReadResultCache(clock);
        var ttl = TimeSpan.FromSeconds(15);
        var args = Args("{\"hours_back\":24}");
        var key = ReadCacheKey.ForMcp("probe", args, null, false, 0, cache, ttl);

        Assert.NotEqual(key, ReadCacheKey.ForMcp("probe", args, null, false, 1, cache, ttl));
        Assert.NotEqual(key, ReadCacheKey.ForMcp("probe", args, null, true, 0, cache, ttl));
        Assert.NotEqual(key, ReadCacheKey.ForMcp("probe", Args("{\"hours_back\":24,\"as_of\":\"2026-10-01T00:00:00Z\"}"), null, false, 0, cache, ttl));

        clock.Advance(TimeSpan.FromSeconds(15));
        Assert.NotEqual(key, ReadCacheKey.ForMcp("probe", args, null, false, 0, cache, ttl));

        /* An anchored key does not move with the bucket. */
        var anchored = Args("{\"as_of\":\"2026-10-01T00:00:00Z\"}");
        var before = ReadCacheKey.ForMcp("probe", anchored, null, false, 0, cache, ttl);
        clock.Advance(TimeSpan.FromSeconds(60));
        Assert.Equal(before, ReadCacheKey.ForMcp("probe", anchored, null, false, 0, cache, ttl));
    }

    [Fact]
    public void WebKeys_AreSortedButKeepTheValuesAsSent_AndNeverMatchMcpKeys()
    {
        using var cache = new ReadResultCache(new TimerTimeProvider());
        var ttl = TimeSpan.FromSeconds(30);
        var ab = ReadCacheKey.ForWeb("probe", [new("server", "a"), new("hours", "24")], 0, cache, ttl);
        Assert.Equal(ab, ReadCacheKey.ForWeb("probe", [new("hours", "24"), new("server", "a")], 0, cache, ttl));
        Assert.NotEqual(ab, ReadCacheKey.ForWeb("probe", [new("hours", "24.0"), new("server", "a")], 0, cache, ttl));
        Assert.NotEqual(ab, ReadCacheKey.ForMcp("probe", Args("{\"server\":\"a\",\"hours\":24}"), null, false, 0, cache, ttl));
    }

    [Fact]
    public void ComposeKeys_DifferByTheSurfaceThatRanThem()
    {
        using var cache = new ReadResultCache(new TimerTimeProvider());
        var ttl = TimeSpan.FromSeconds(30);
        const string body = "{\"source\":\"waits\"}";
        Assert.NotEqual(
            ReadCacheKey.ForCompose(ReadSurface.Web, "waits", body, 0, cache, ttl),
            ReadCacheKey.ForCompose(ReadSurface.Mcp, "waits", body, 0, cache, ttl));
    }
}

/// <summary>#5506 part 3: what is never cached, and what moves the write generation.</summary>
public sealed class ReadCacheInvalidationTests
{
    [Theory]
    [InlineData("get_read_latency")]
    [InlineData("get_slow_reads")]
    [InlineData("get_collection_health")]
    [InlineData("get_collection_log")]
    [InlineData("get_store_metrics")]
    [InlineData("get_store_log")]
    [InlineData("list_servers")]
    [InlineData("analyze_server")]
    [InlineData("create_server_tag")]
    public void HealthReads_Writes_AndAnalyzeServer_AreNeverCached(string route)
    {
        Assert.False(ReadResultCache.IsCacheableRoute(route));

        /* Even when the classification calls the pair heavy. */
        var admission = new ReadAdmission(TimeProvider.System, ReadAdmission.DefaultWaitCap, null, 2);
        admission.SetTable(new Dictionary<(ReadSurface, string, string), long> { [(ReadSurface.Mcp, route, ReadWindowBand.None)] = 9_000 });
        Assert.False(admission.IsCached(ReadSurface.Mcp, route, ReadWindowBand.None));
    }

    [Fact]
    public void OnlyHeavyPairs_AreCached()
    {
        var admission = new ReadAdmission(TimeProvider.System, ReadAdmission.DefaultWaitCap, null, 0);
        admission.SetTable(new Dictionary<(ReadSurface, string, string), long>
        {
            [(ReadSurface.Mcp, "get_wait_stats", ReadWindowBand.OneDay)] = 2_500,
            [(ReadSurface.Mcp, "get_cpu_utilization", ReadWindowBand.OneDay)] = 300,
        });

        /* With admission off (limit 0) a heavy answer is still cached. */
        Assert.True(admission.IsCached(ReadSurface.Mcp, "get_wait_stats", ReadWindowBand.OneDay));
        Assert.False(admission.IsCached(ReadSurface.Mcp, "get_cpu_utilization", ReadWindowBand.OneDay));
        Assert.False(admission.IsCached(ReadSurface.Web, "get_wait_stats", ReadWindowBand.OneDay));
        Assert.True(admission.IsCached(ReadSurface.Web, "get_wait_stats", ReadWindowBand.SevenDays));
    }

    [Theory]
    [InlineData("{\"rows\":[1]}", true)]
    [InlineData("[1,2]", true)]
    [InlineData("{\"status\":\"ok\",\"rows\":[]}", true)]
    [InlineData("{\"status\":\"empty\",\"message\":\"none\"}", false)]
    [InlineData("{\"status\":\"error\",\"message\":\"Error during x: y\"}", false)]
    [InlineData("{\"status\":\"busy\",\"message\":\"wait\"}", false)]
    [InlineData("Error during x: y", false)]
    [InlineData("", false)]
    public void OnlyOkAnswers_AreStored(string text, bool cacheable) =>
        Assert.Equal(cacheable, ReadResultCache.IsCacheableText(text, gcf: false));

    [Fact]
    public void AnAnswerTheStatementSweepWithheldFrom_IsNeverStored()
    {
        /* The sweep writes the same marker for a value it stopped judging at its time budget, so any withheld value may
           be a budget stop that a later call judges in full. */
        var placeholder = JsonSerializer.Serialize(new { rows = new[] { new { query_text = SensitiveStatements.PlaceholderText } } });
        var plan = JsonSerializer.Serialize(new { plan = SensitiveStatements.WithheldPlanSentence });
        Assert.False(ReadResultCache.IsCacheableText(placeholder, gcf: false));
        Assert.False(ReadResultCache.IsCacheableText(plan, gcf: false));
    }

    /// <summary>Round 2, L9: the withheld-marker rule holds with GCF on too: a GCF wire of an answer the statement sweep
    /// withheld a value from is never stored, though the same wire without the marker is.</summary>
    [Fact]
    public void AGcfWire_TheStatementSweepWithheldFrom_IsNeverStored()
    {
        var clean = GcfOutput.TryEncode(JsonSerializer.Serialize(new { rows = new[] { new { query_text = "select 1" } } }));
        var placeholder = GcfOutput.TryEncode(JsonSerializer.Serialize(new { rows = new[] { new { query_text = SensitiveStatements.PlaceholderText } } }));
        var plan = GcfOutput.TryEncode(JsonSerializer.Serialize(new { plan = SensitiveStatements.WithheldPlanSentence }));
        Assert.NotNull(clean);
        Assert.NotNull(placeholder);
        Assert.NotNull(plan);
        Assert.False(clean!.TrimStart().StartsWith('{'), "the encoded answer is still JSON, so this would not test the GCF arm");
        Assert.True(ReadResultCache.IsCacheableText(clean, gcf: true));
        Assert.False(ReadResultCache.IsCacheableText(placeholder!, gcf: true));
        Assert.False(ReadResultCache.IsCacheableText(plan!, gcf: true));
    }

    /// <summary>Round 2, L7: an answer over 1 MB is refused before it is parsed, and so is one under 1 MB in characters
    /// whose UTF-8 form is over it.</summary>
    [Fact]
    public void AnAnswerOver1MB_IsNotCacheable_ThoughItIsAnOkJsonObject()
    {
        var large = "{\"rows\":[\"" + new string('a', ReadResultCache.MaxAnswerBytes) + "\"]}";
        Assert.False(ReadResultCache.IsCacheableText(large, gcf: false));
        var wide = "{\"rows\":[\"" + new string('\u00e9', ReadResultCache.MaxAnswerBytes / 2 + 1) + "\"]}";
        Assert.True(wide.Length < ReadResultCache.MaxAnswerBytes);
        Assert.False(ReadResultCache.IsCacheableText(wide, gcf: false));
        Assert.True(ReadResultCache.IsCacheableText("{\"rows\":[\"" + new string('a', 1000) + "\"]}", gcf: false));
    }

    /// <summary>Round 2, L4: a value that holds a comma and the same name given twice are two keys (the handler reads the
    /// first of a repeated name), and so are names and values that hold the key's own separators.</summary>
    [Fact]
    public void WebKeys_EscapeNamesAndValues_SoACommaValueIsNotTwoValues()
    {
        using var cache = new ReadResultCache(new TimerTimeProvider());
        var ttl = TimeSpan.FromSeconds(30);
        var commaValue = ReadCacheKey.ForWeb("probe", [new("database", "x,y")], 0, cache, ttl);
        var twoValues = ReadCacheKey.ForWeb("probe", [new("database", "x"), new("database", "y")], 0, cache, ttl);
        Assert.NotEqual(commaValue, twoValues);
        Assert.Equal(twoValues, ReadCacheKey.ForWeb("probe", [new("database", "x"), new("database", "y")], 0, cache, ttl));
        Assert.NotEqual(
            ReadCacheKey.ForWeb("probe", [new("a", "1=b"), new("c", "2")], 0, cache, ttl),
            ReadCacheKey.ForWeb("probe", [new("a", "1"), new("b", "2"), new("c", "2")], 0, cache, ttl));
        Assert.NotEqual(
            ReadCacheKey.ForWeb("probe", [new("a", "1\u001fb=2")], 0, cache, ttl),
            ReadCacheKey.ForWeb("probe", [new("a", "1"), new("b", "2")], 0, cache, ttl));
    }

    /// <summary>Round 2, L1: a caller that arrives after a flight's last caller has left, before its read is cancelled,
    /// starts a read of its own and gets its answer, instead of joining the closing flight and getting a cancellation it
    /// never asked for.</summary>
    [Fact(Timeout = 60_000)]
    public async Task ACallerArrivingAsTheLastCallerLeaves_StartsItsOwnRead()
    {
        using var cache = new ReadResultCache(new TimerTimeProvider());
        var ttl = TimeSpan.FromSeconds(30);
        var runs = 0;
        ReadFlight<string>? arrived = null;
        cache.BeforeFlightCancel = () =>
        {
            cache.BeforeFlightCancel = null;
            arrived = cache.Join("k", ttl, _ =>
            {
                Interlocked.Increment(ref runs);
                return Task.FromResult("second");
            }, _ => null);
        };

        var first = cache.Join("k", ttl, async token =>
        {
            Interlocked.Increment(ref runs);
            await Task.Delay(Timeout.Infinite, token);
            return "first";
        }, _ => null);
        Assert.True(first.IsLead);

        /* The only caller leaves: its leave closes the flight, the seam arrives in the gap, then the read is cancelled. */
        using var leaves = new CancellationTokenSource();
        await leaves.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first.WaitAsync(leaves.Token));

        Assert.NotNull(arrived);
        Assert.True(arrived!.Value.IsLead, "the caller that arrived in the gap joined the closing flight");
        Assert.Equal("second", await arrived.Value.WaitAsync(TestContext.Current.CancellationToken)
            .WaitAsync(TimeSpan.FromSeconds(20), TestContext.Current.CancellationToken));
        Assert.Equal(2, runs);
    }

    /// <summary>Round 2, M1: a joined caller records only what it got itself.</summary>
    [Theory]
    [InlineData(ReadOutcome.Timeout, ReadOutcome.CacheHit)]
    [InlineData(ReadOutcome.Error, ReadOutcome.CacheHit)]
    [InlineData(ReadOutcome.FallbackRaw, ReadOutcome.CacheHit)]
    [InlineData(ReadOutcome.GateFailed, ReadOutcome.CacheHit)]
    [InlineData(ReadOutcome.Ok, ReadOutcome.CacheHit)]
    [InlineData(ReadOutcome.Busy, ReadOutcome.Busy)]
    [InlineData(ReadOutcome.Cancelled, ReadOutcome.Cancelled)]
    public void AJoinedCaller_RecordsItsOwnOutcome_NeverTheSharedRun(ReadOutcome received, ReadOutcome recorded)
    {
        Assert.Equal(recorded, ReadResultCache.JoinedOutcome(received, CancellationToken.None));
        Assert.Equal(ReadOutcome.Cancelled, ReadResultCache.JoinedOutcome(received, new CancellationToken(canceled: true)));
    }

    [Fact]
    public void AnErrorResult_OrOneWithStructuredContent_IsNotCacheable()
    {
        var ok = new CallToolResult { Content = [new TextContentBlock { Text = "{\"rows\":[1]}" }] };
        Assert.Equal("{\"rows\":[1]}", McpToolLatencyFilter.CacheableText(ok, gcf: false));

        var flagged = new CallToolResult { IsError = true, Content = [new TextContentBlock { Text = "{\"rows\":[1]}" }] };
        Assert.Null(McpToolLatencyFilter.CacheableText(flagged, gcf: false));

        var two = new CallToolResult { Content = [new TextContentBlock { Text = "[1]" }, new TextContentBlock { Text = "[2]" }] };
        Assert.Null(McpToolLatencyFilter.CacheableText(two, gcf: false));
    }

    [Fact]
    public void ASuccessfulWrite_OrUnanchoredAnalyzeServer_MovesTheGeneration()
    {
        var ok = new CallToolResult { Content = [new TextContentBlock { Text = "{\"status\":\"ok\"}" }] };
        var failed = new CallToolResult { Content = [new TextContentBlock { Text = McpHelpers.FormatError("create_server_tag", new InvalidOperationException("no")) }] };
        var none = new Dictionary<string, JsonElement>();
        var anchored = new Dictionary<string, JsonElement> { ["as_of"] = JsonSerializer.SerializeToElement("2026-10-01T00:00:00Z") };

        Assert.True(McpToolLatencyFilter.MovesGeneration("create_server_tag", none, ok));
        Assert.False(McpToolLatencyFilter.MovesGeneration("create_server_tag", none, failed));
        Assert.True(McpToolLatencyFilter.MovesGeneration("analyze_server", none, ok));
        Assert.True(McpToolLatencyFilter.MovesGeneration("analyze_server", null, ok));
        Assert.False(McpToolLatencyFilter.MovesGeneration("analyze_server", anchored, ok));
        Assert.False(McpToolLatencyFilter.MovesGeneration("get_wait_stats", none, ok));
    }

    [Theory]
    [InlineData("POST", "/api/mute-rules", 200, true)]
    [InlineData("PUT", "/api/views/7", 204, true)]
    [InlineData("DELETE", "/api/servers/3", 200, true)]
    [InlineData("POST", "/api/mute-rules", 400, false)]
    [InlineData("GET", "/api/read/get_wait_stats", 200, false)]
    [InlineData("POST", "/api/compose/run", 200, false)]
    [InlineData("POST", "/api/alerts/validate", 200, false)]
    [InlineData("POST", "/api/alerts/test", 200, false)]
    [InlineData("POST", "/login", 200, false)]
    public void AWebWrite_IsA2xxNonGetApiAnswer_ExceptTheReadOnlyPosts(string method, string path, int status, bool moves) =>
        Assert.Equal(moves, ReadResultCache.IsWebWrite(method, path, status));

    [Fact]
    public void BumpingTheGeneration_MakesEarlierEntriesUnreachable()
    {
        using var cache = new ReadResultCache(new TimerTimeProvider());
        var ttl = TimeSpan.FromSeconds(60);
        var args = new Dictionary<string, JsonElement>();
        var before = ReadCacheKey.ForMcp("probe", args, null, false, cache.Generation, cache, ttl);
        Assert.True(cache.TryStore(before, "[1]", ttl));

        cache.BumpGeneration();
        var after = ReadCacheKey.ForMcp("probe", args, null, false, cache.Generation, cache, ttl);
        Assert.NotEqual(before, after);
        Assert.False(cache.TryGet(after, out _));
    }
}
