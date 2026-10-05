/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5158: the host's side of the <c>procedure_stats</c> deferred plan fetch, without a monitored server: the
/// identity key, the knob, the shadow comparison, the on-mode reuse pass (cache, cap, age limit, cadence gate,
/// oversized plans) and the collector's identity-column shapes. The end-to-end behaviour against a real server is
/// in <see cref="ProcedureStatsDeferredPlanFetchLiveTests"/>.
/// </summary>
public sealed class ProcedureStatsPlanReuseTests
{
    private static readonly DateTime s_now = new(2026, 10, 5, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime s_cached = new(2026, 10, 5, 8, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime s_compile = new(2026, 10, 5, 9, 0, 0, DateTimeKind.Utc);

    private static ProcedureStatsCollector.Row RowFor(
        int handle, string? plan = null, long? bytes = null, long count = 3, long generation = 7,
        DateTime? cached = null, DateTime? compile = null, string db = "db1") =>
        default(ProcedureStatsCollector.Row) with
        {
            DatabaseName = db,
            SchemaName = "dbo",
            ObjectName = "p" + handle.ToString(CultureInfo.InvariantCulture),
            ObjectType = "P",
            CachedTime = cached ?? s_cached,
            SqlHandle = "0x03" + handle.ToString("X2", CultureInfo.InvariantCulture),
            PlanHandle = "0x05" + handle.ToString("X2", CultureInfo.InvariantCulture),
            QueryPlanXml = plan,
            QueryPlanXmlBytes = bytes ?? plan?.Length,
            PlanStatementCount = count,
            PlanLastStatementCompile = compile ?? s_compile,
            PlanGenerationSum = generation,
        };

    private static PlanDigestCache<ProcedureStatsPlanKey> ConfirmedCache(
        IEnumerable<ProcedureStatsCollector.Row> rows, long ordinal = 1)
    {
        var cache = new PlanDigestCache<ProcedureStatsPlanKey>();
        var keys = new List<ProcedureStatsPlanKey>();
        foreach (var row in rows)
        {
            var key = ProcedureStatsPlanKey.TryCreate(1, row)!.Value;
            cache.AddPending(key, row.QueryPlanXml is null ? null : ProcedureStatsPlanReuse.DigestOf(row.QueryPlanXml), row.QueryPlanXmlBytes, s_now, ordinal);
            keys.Add(key);
        }

        cache.ConfirmPending(keys, s_now);
        return cache;
    }

    private static ProcedureStatsPlanReuse.PlanFetch FetchOf(
        Dictionary<string, (string? Plan, long? Bytes)> plansByHandleHex, List<int>? calls = null) =>
        (handles, _) =>
        {
            calls?.Add(handles.Count);
            var result = new Dictionary<int, (string? PlanXml, long? Bytes)>();
            for (var i = 0; i < handles.Count; i++)
            {
                if (plansByHandleHex.TryGetValue("0x" + Convert.ToHexString(handles[i]), out var found))
                {
                    result[i] = found;
                }
            }

            return Task.FromResult(result);
        };

    // ---- the key -------------------------------------------------------------------------------

    [Fact]
    public void TheKey_IsBuiltFromAFullyFingerprintedRow_AndEqualRowsMakeEqualKeys()
    {
        var a = ProcedureStatsPlanKey.TryCreate(1, RowFor(1, "<a/>"));
        var b = ProcedureStatsPlanKey.TryCreate(1, RowFor(1, "<other/>"));
        Assert.NotNull(a);
        Assert.Equal(a, b); /* the plan text is not part of the identity */
        Assert.Equal(a!.Value.GetHashCode(), b!.Value.GetHashCode());
    }

    public static IEnumerable<object[]> OneFieldDiffers() => new[]
    {
        new object[] { "server", 2, RowFor(1, "<a/>") },
        new object[] { "handle", 1, RowFor(2, "<a/>") },
        new object[] { "database", 1, RowFor(1, "<a/>", db: "db2") },
        new object[] { "cached_time", 1, RowFor(1, "<a/>", cached: s_cached.AddMinutes(1)) },
        new object[] { "statement_count", 1, RowFor(1, "<a/>", count: 4) },
        new object[] { "last_compile", 1, RowFor(1, "<a/>", compile: s_compile.AddSeconds(1)) },
        new object[] { "generation_sum", 1, RowFor(1, "<a/>", generation: 8) },
    };

    [Theory]
    [MemberData(nameof(OneFieldDiffers))]
    public void EachIdentityField_ThatDiffers_IsAMiss(string field, int serverId, ProcedureStatsCollector.Row other)
    {
        var cache = ConfirmedCache(new[] { RowFor(1, "<a/>") });
        var key = ProcedureStatsPlanKey.TryCreate(serverId, other)!.Value;

        Assert.False(cache.TryGet(key, s_now, out _), field + " must change the identity");
    }

    [Fact]
    public void ACountOfZero_OrNoFingerprint_OrABadHandle_HasNoKey()
    {
        Assert.Null(ProcedureStatsPlanKey.TryCreate(1, RowFor(1, "<a/>", count: 0)));
        Assert.Null(ProcedureStatsPlanKey.TryCreate(1, RowFor(1, "<a/>") with { PlanStatementCount = null }));
        Assert.Null(ProcedureStatsPlanKey.TryCreate(1, RowFor(1, "<a/>") with { PlanHandle = "0x05Z" }));
        Assert.Null(ProcedureStatsPlanKey.TryCreate(1, RowFor(1, "<a/>") with { PlanHandle = null }));
    }

    [Fact]
    public void TheKeyText_NamesTheHandleAndTheFingerprint_AndNothingElse()
    {
        var text = ProcedureStatsPlanKey.TryCreate(1, RowFor(1, "<a/>"))!.Value.ToString();

        Assert.Contains("plan_handle 0x0501", text, StringComparison.Ordinal);
        Assert.Contains("statements 3", text, StringComparison.Ordinal);
        Assert.Contains("generation_sum 7", text, StringComparison.Ordinal);
        Assert.DoesNotContain("db1", text, StringComparison.Ordinal);
        Assert.DoesNotContain("p1", text, StringComparison.Ordinal);
    }

    // ---- the knob ------------------------------------------------------------------------------

    [Theory]
    [InlineData(null, true, ProcedureStatsPlanFetchMode.Off)]
    [InlineData("", true, ProcedureStatsPlanFetchMode.Off)]
    [InlineData("off", true, ProcedureStatsPlanFetchMode.Off)]
    [InlineData("shadow", true, ProcedureStatsPlanFetchMode.Shadow)]
    [InlineData(" Shadow ", true, ProcedureStatsPlanFetchMode.Shadow)]
    [InlineData("ON", true, ProcedureStatsPlanFetchMode.On)]
    [InlineData("true", false, ProcedureStatsPlanFetchMode.Off)]
    [InlineData("yes", false, ProcedureStatsPlanFetchMode.Off)]
    public void TheKnobParses_OffShadowOn_AndAnythingElseIsOff(string? value, bool recognized, ProcedureStatsPlanFetchMode expected)
    {
        Assert.Equal(recognized, ProcedureStatsPlanFetchModes.TryParse(value, out var mode));
        Assert.Equal(expected, mode);
    }

    [Fact]
    public void TheShippedKnobValue_IsOff()
    {
        Assert.Equal("off", new DarlingConfig().ProcedureStatsDeferredPlanFetch);
        Assert.Equal("off", ProcedureStatsPlanFetchModes.DefaultValue);
    }

    private static (DarlingCollectorRunner Runner, CapturingTestLogger Log) RunnerWith(string? knob, bool capturePlans = true)
    {
        var log = new CapturingTestLogger();
        var runner = new DarlingCollectorRunner(
            NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Password=x;Database=x"), new CollectorDeltaCalculator(), log,
            capturePlans: () => capturePlans, procedureStatsDeferredPlanFetch: () => knob);
        return (runner, log);
    }

    [Theory]
    [InlineData("off", ProcedureStatsPlanFetchMode.Off)]
    [InlineData("shadow", ProcedureStatsPlanFetchMode.Shadow)]
    [InlineData("on", ProcedureStatsPlanFetchMode.On)]
    public void TheRunner_ReadsTheKnob_ForProcedureStatsOnly(string knob, ProcedureStatsPlanFetchMode expected)
    {
        var (runner, _) = RunnerWith(knob);
        var target = new CollectorTargetInfo();

        Assert.Equal(expected, runner.ProcedureStatsPlanFetchModeFor("procedure_stats", target));
        Assert.Equal(ProcedureStatsPlanFetchMode.Off, runner.ProcedureStatsPlanFetchModeFor("query_stats", target));
        Assert.Equal(ProcedureStatsPlanFetchMode.Off, runner.ProcedureStatsPlanFetchModeFor("query_store", target));
    }

    [Theory]
    [InlineData("shadow")]
    [InlineData("on")]
    public void AzureSqlDatabase_IsForcedOff_WhateverTheKnobSays(string knob)
    {
        var (runner, _) = RunnerWith(knob);

        Assert.Equal(
            ProcedureStatsPlanFetchMode.Off,
            runner.ProcedureStatsPlanFetchModeFor("procedure_stats", new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.False(runner.ShouldDeferPlanFetchFor("procedure_stats", true, new CollectorTargetInfo { IsAzureSqlDb = true }));
    }

    [Fact]
    public void WithPlanCaptureOff_TheKnobMeansNothing()
    {
        var (runner, _) = RunnerWith("on", capturePlans: false);

        Assert.Equal(ProcedureStatsPlanFetchMode.Off, runner.ProcedureStatsPlanFetchModeFor("procedure_stats", new CollectorTargetInfo()));
    }

    [Fact]
    public void AnUnknownKnobValue_WarnsOnce_AndMeansOff()
    {
        var (runner, log) = RunnerWith("maybe");
        var target = new CollectorTargetInfo();

        Assert.Equal(ProcedureStatsPlanFetchMode.Off, runner.ProcedureStatsPlanFetchModeFor("procedure_stats", target));
        Assert.Equal(ProcedureStatsPlanFetchMode.Off, runner.ProcedureStatsPlanFetchModeFor("procedure_stats", target));

        Assert.Equal(1, log.CountAtLevel(LogLevel.Warning));
        Assert.Contains("maybe", log.Joined, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("off", true, false)]
    [InlineData("shadow", true, false)]   /* shadow renders inline: it never defers the fetch */
    [InlineData("on", true, true)]
    [InlineData("on", false, false)]      /* a gated cycle captures nothing, so nothing is deferred */
    public void OnlyOnMode_DefersTheFetch_OnACaptureCycle(string knob, bool capture, bool expected)
    {
        var (runner, _) = RunnerWith(knob);

        Assert.Equal(expected, runner.ShouldDeferPlanFetchFor("procedure_stats", capture, new CollectorTargetInfo()));
    }

    // ---- shadow --------------------------------------------------------------------------------

    [Fact]
    public void Shadow_OnAnUnchangedModule_CountsAWouldHit_AndNoFalseHit()
    {
        var cache = ConfirmedCache(new[] { RowFor(1, "<plan one/>") });
        var rows = new List<ProcedureStatsCollector.Row> { RowFor(1, "<plan one/>") };

        var outcome = ProcedureStatsPlanReuse.ApplyShadow(1, cache, rows, 2, s_now, _ => Assert.Fail("no false hit expected"));

        Assert.Equal(1, outcome.WouldHit);
        Assert.Equal(0, outcome.FalseHit);
        Assert.Equal(0, outcome.Miss);
        Assert.Equal(1, outcome.Rendered);
    }

    [Fact]
    public void Shadow_WhenTheCachedPlanDiffersFromTheRender_CountsAFalseHit_AndReportsTheKey()
    {
        var cache = ConfirmedCache(new[] { RowFor(1, "<stale plan/>") });
        var rows = new List<ProcedureStatsCollector.Row> { RowFor(1, "<current plan/>") };
        var reported = new List<ProcedureStatsPlanKey>();

        var outcome = ProcedureStatsPlanReuse.ApplyShadow(1, cache, rows, 2, s_now, reported.Add);

        Assert.Equal(1, outcome.WouldHit);
        Assert.Equal(1, outcome.FalseHit);
        Assert.Equal(ProcedureStatsPlanKey.TryCreate(1, rows[0])!.Value, Assert.Single(reported));

        /* shadow warms the cache from the inline render: once it is confirmed, the same render is a true hit */
        cache.ConfirmPending(outcome.Pending, s_now);
        var again = ProcedureStatsPlanReuse.ApplyShadow(1, cache, rows, 3, s_now, _ => Assert.Fail("healed"));
        Assert.Equal(1, again.WouldHit);
        Assert.Equal(0, again.FalseHit);
    }

    [Fact]
    public void Shadow_ACold_Miss_WarmsTheCache_AndLeavesEveryRowUntouched()
    {
        var cache = new PlanDigestCache<ProcedureStatsPlanKey>();
        var rows = new List<ProcedureStatsCollector.Row>
        {
            RowFor(1, "<plan one/>"),
            RowFor(2, "<plan two/>", count: 0),   /* no statements visible: never cached */
            RowFor(3, null),                       /* aged out: no plan, no size */
        };
        var before = rows.ToList();

        var outcome = ProcedureStatsPlanReuse.ApplyShadow(1, cache, rows, 1, s_now, null);

        Assert.Equal(before, rows); /* byte-identical to off: the pass never writes a row */
        Assert.Equal(0, outcome.WouldHit);
        Assert.Equal(3, outcome.Miss);
        Assert.Equal(2, outcome.Rendered);
        Assert.Single(outcome.Pending);
        Assert.Equal(1, cache.Count);
    }

    [Fact]
    public void Shadow_AnOverCapIdentity_IsAHit_WithoutAFalseHit_WhenTheSizeMatches()
    {
        var over = QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes + 5L;
        var cache = ConfirmedCache(new[] { RowFor(1, null, over) });
        var rows = new List<ProcedureStatsCollector.Row> { RowFor(1, null, over) };

        var outcome = ProcedureStatsPlanReuse.ApplyShadow(1, cache, rows, 2, s_now, _ => Assert.Fail("same size"));

        Assert.Equal(1, outcome.WouldHit);
        Assert.Equal(0, outcome.FalseHit);

        var grew = ProcedureStatsPlanReuse.ApplyShadow(
            1, cache, new List<ProcedureStatsCollector.Row> { RowFor(1, null, over + 100) }, 2, s_now, null);
        Assert.Equal(1, grew.FalseHit);
    }

    // ---- on ------------------------------------------------------------------------------------

    [Fact]
    public async Task On_AHit_CarriesTheCachedDigestAndSize_AndRendersNothing()
    {
        var cached = RowFor(1, "<plan one/>");
        var cache = ConfirmedCache(new[] { cached });
        var rows = new List<ProcedureStatsCollector.Row> { RowFor(1) };
        var calls = new List<int>();

        var outcome = await ProcedureStatsPlanReuse.ApplyOnAsync(
            1, cache, rows, captureCycle: true, 2, s_now, FetchOf(new(), calls), CancellationToken.None);

        Assert.Equal(1, outcome.Hit);
        Assert.Empty(calls);
        Assert.Null(rows[0].QueryPlanXml);
        Assert.Equal(ProcedureStatsPlanReuse.DigestOf("<plan one/>"), rows[0].KnownPlanDigest);
        Assert.Equal("<plan one/>".Length, rows[0].QueryPlanXmlBytes);
    }

    [Fact]
    public async Task On_AMiss_IsRenderedInOneFetch_AndCachedAsPendingUntilACommitConfirmsIt()
    {
        var cache = new PlanDigestCache<ProcedureStatsPlanKey>();
        var rows = new List<ProcedureStatsCollector.Row> { RowFor(1), RowFor(2) };
        var plans = new Dictionary<string, (string?, long?)> { ["0x0501"] = ("<one/>", 6), ["0x0502"] = ("<two/>", 6) };
        var calls = new List<int>();

        var outcome = await ProcedureStatsPlanReuse.ApplyOnAsync(
            1, cache, rows, true, 1, s_now, FetchOf(plans!, calls), CancellationToken.None);

        Assert.Equal(new[] { 2 }, calls);
        Assert.Equal(2, outcome.Rendered);
        Assert.Equal(12, outcome.RenderedBytes);
        Assert.Equal("<one/>", rows[0].QueryPlanXml);
        Assert.Equal("<two/>", rows[1].QueryPlanXml);
        Assert.Equal(2, outcome.Pending.Count);

        /* pending is not proof: a lookup before the commit still misses */
        Assert.False(cache.TryGet(outcome.Pending[0], s_now, out _));
        cache.ConfirmPending(outcome.Pending, s_now);
        Assert.True(cache.TryGet(outcome.Pending[0], s_now, out _));
    }

    [Fact]
    public async Task On_AFailedCommit_LeavesNothingConfirmed_SoTheNextRunRendersAgain()
    {
        var cache = new PlanDigestCache<ProcedureStatsPlanKey>();
        var plans = new Dictionary<string, (string?, long?)> { ["0x0501"] = ("<one/>", 6) };

        var first = await ProcedureStatsPlanReuse.ApplyOnAsync(
            1, cache, new List<ProcedureStatsCollector.Row> { RowFor(1) }, true, 1, s_now, FetchOf(plans!), CancellationToken.None);
        cache.DiscardPending(first.Pending);

        var calls = new List<int>();
        var second = await ProcedureStatsPlanReuse.ApplyOnAsync(
            1, cache, new List<ProcedureStatsCollector.Row> { RowFor(1) }, true, 2, s_now, FetchOf(plans!, calls), CancellationToken.None);

        Assert.Equal(0, second.Hit);
        Assert.Equal(new[] { 1 }, calls);
    }

    [Fact]
    public async Task On_ARowWithACountOfZero_IsRenderedButNeverCached_AndNeverHits()
    {
        var cache = new PlanDigestCache<ProcedureStatsPlanKey>();
        var plans = new Dictionary<string, (string?, long?)> { ["0x0501"] = ("<one/>", 6) };
        var rows = new List<ProcedureStatsCollector.Row> { RowFor(1, count: 0) };

        var outcome = await ProcedureStatsPlanReuse.ApplyOnAsync(
            1, cache, rows, true, 1, s_now, FetchOf(plans!), CancellationToken.None);

        Assert.Equal(1, outcome.Rendered);
        Assert.Equal("<one/>", rows[0].QueryPlanXml);
        Assert.Empty(outcome.Pending);
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public async Task On_AnEntryOlderThanFourCaptureCycles_IsAMiss_AndAnEntryAtFourIsAHit()
    {
        var cache = ConfirmedCache(new[] { RowFor(1, "<one/>") }, ordinal: 10);
        var plans = new Dictionary<string, (string?, long?)> { ["0x0501"] = ("<one/>", 6) };

        var atLimit = await ProcedureStatsPlanReuse.ApplyOnAsync(
            1, cache, new List<ProcedureStatsCollector.Row> { RowFor(1) }, true, 10 + ProcedureStatsPlanReuse.TtlCaptureCycles, s_now,
            FetchOf(plans!), CancellationToken.None);
        Assert.Equal(1, atLimit.Hit);

        var calls = new List<int>();
        var past = await ProcedureStatsPlanReuse.ApplyOnAsync(
            1, cache, new List<ProcedureStatsCollector.Row> { RowFor(1) }, true, 10 + ProcedureStatsPlanReuse.TtlCaptureCycles + 1, s_now,
            FetchOf(plans!, calls), CancellationToken.None);
        Assert.Equal(0, past.Hit);
        Assert.Equal(1, past.Miss);
        Assert.Equal(new[] { 1 }, calls);
        Assert.Equal(4, ProcedureStatsPlanReuse.TtlCaptureCycles);
    }

    [Fact]
    public async Task On_AtMost150MissesAreRendered_InRowOrder_AndTheRestShipNoPlan()
    {
        var cache = new PlanDigestCache<ProcedureStatsPlanKey>();
        var rows = Enumerable.Range(0, 160).Select(i => RowFor(i)).ToList();
        var plans = Enumerable.Range(0, 160).ToDictionary(
            i => "0x05" + i.ToString("X2", CultureInfo.InvariantCulture), i => ((string?)("<p" + i + "/>"), (long?)5));
        var calls = new List<int>();

        var outcome = await ProcedureStatsPlanReuse.ApplyOnAsync(
            1, cache, rows, true, 1, s_now, FetchOf(plans!, calls), CancellationToken.None);

        Assert.Equal(new[] { 150 }, calls);
        Assert.Equal(150, outcome.Rendered);
        Assert.Equal(10, outcome.OverCap);
        Assert.Equal(160, outcome.Miss);
        Assert.NotNull(rows[149].QueryPlanXml);
        Assert.Null(rows[150].QueryPlanXml);
        Assert.Null(rows[159].QueryPlanXml);
        Assert.Equal(150, outcome.Pending.Count);
    }

    [Fact]
    public async Task OnAGatedCycle_AHitCarriesItsDigest_AndAMissIssuesNoFetchAndShipsNoPlan()
    {
        var cache = ConfirmedCache(new[] { RowFor(1, "<one/>") });
        var rows = new List<ProcedureStatsCollector.Row> { RowFor(1), RowFor(2) };
        var calls = new List<int>();
        var plans = new Dictionary<string, (string?, long?)> { ["0x0502"] = ("<two/>", 6) };

        var outcome = await ProcedureStatsPlanReuse.ApplyOnAsync(
            1, cache, rows, captureCycle: false, 2, s_now, FetchOf(plans!, calls), CancellationToken.None);

        Assert.Empty(calls);
        Assert.Equal(1, outcome.Hit);
        Assert.Equal(1, outcome.Miss);
        Assert.NotNull(rows[0].KnownPlanDigest);
        Assert.Null(rows[1].QueryPlanXml);
        Assert.Null(rows[1].KnownPlanDigest);
        Assert.Equal(0, outcome.Rendered);
        Assert.Empty(outcome.Pending);
    }

    [Fact]
    public async Task On_AnOverCapIdentity_IsCachedWithNoDigestAndItsBytes_AndStillYieldsAnObservationWithoutARender()
    {
        var over = QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes + 1L;
        var cache = new PlanDigestCache<ProcedureStatsPlanKey>();
        var plans = new Dictionary<string, (string?, long?)> { ["0x0501"] = (null, over) };

        var first = await ProcedureStatsPlanReuse.ApplyOnAsync(
            1, cache, new List<ProcedureStatsCollector.Row> { RowFor(1) }, true, 1, s_now, FetchOf(plans!), CancellationToken.None);
        cache.ConfirmPending(first.Pending, s_now);

        var calls = new List<int>();
        var rows = new List<ProcedureStatsCollector.Row> { RowFor(1) };
        var second = await ProcedureStatsPlanReuse.ApplyOnAsync(
            1, cache, rows, true, 2, s_now, FetchOf(plans!, calls), CancellationToken.None);

        Assert.Empty(calls);
        Assert.Equal(1, second.Hit);
        Assert.Null(rows[0].KnownPlanDigest);
        Assert.Equal(over, rows[0].QueryPlanXmlBytes);
        var observation = ProcedureStatsCollector.Instance.DescribeOversizedPlan(rows[0]);
        Assert.NotNull(observation);
        Assert.Equal(over, observation!.Value.ObservedBytes);
    }

    [Fact]
    public async Task On_APlanThatAgedOutBetweenTheQueries_ShipsNoPlan_AndIsNotCached()
    {
        var cache = new PlanDigestCache<ProcedureStatsPlanKey>();
        var rows = new List<ProcedureStatsCollector.Row> { RowFor(1) };

        var outcome = await ProcedureStatsPlanReuse.ApplyOnAsync(
            1, cache, rows, true, 1, s_now, FetchOf(new()), CancellationToken.None);

        Assert.Equal(0, outcome.Rendered);
        Assert.Null(rows[0].QueryPlanXml);
        Assert.Empty(outcome.Pending);
    }

    [Fact]
    public async Task On_AFailedFetch_IsReportedNotThrown_AndCachesNothing()
    {
        var cache = new PlanDigestCache<ProcedureStatsPlanKey>();
        var rows = new List<ProcedureStatsCollector.Row> { RowFor(1) };

        var outcome = await ProcedureStatsPlanReuse.ApplyOnAsync(
            1, cache, rows, true, 1, s_now, (_, _) => throw new InvalidOperationException("boom"), CancellationToken.None);

        Assert.IsType<InvalidOperationException>(outcome.FetchFailure);
        Assert.Empty(outcome.Pending);
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public async Task On_AStop_PropagatesRatherThanBecomingAFetchFailure()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ProcedureStatsPlanReuse.ApplyOnAsync(
            1, new PlanDigestCache<ProcedureStatsPlanKey>(), new List<ProcedureStatsCollector.Row> { RowFor(1) }, true, 1, s_now,
            (_, token) => throw new OperationCanceledException(token), cts.Token));
    }

    // ---- the collector's shapes ----------------------------------------------------------------

    private static CollectorContext Context(bool capture, bool identity, bool defer = false, bool azure = false) => new()
    {
        ServerId = 42,
        ServerName = "test-server",
        CollectionTime = s_now,
        Deltas = new NoDeltas(),
        Target = new CollectorTargetInfo { IsAzureSqlDb = azure },
        CapturePlanXml = capture,
        DeferPlanXmlFetch = defer,
        PlanIdentityColumns = identity,
    };

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void TheShadowQuery_IsTheInlineQuery_PlusOnlyTheIdentityFragments(bool azure)
    {
        var inline = ProcedureStatsCollector.Instance.BuildQuery(Context(true, false, azure: azure)).Text;
        var shadow = ProcedureStatsCollector.Instance.BuildQuery(Context(true, true, azure: azure)).Text;
        var deferred = ProcedureStatsCollector.Instance.BuildQuery(Context(true, true, defer: true, azure: azure)).Text;

        Assert.NotEqual(inline, shadow);
        Assert.Contains("dm_exec_text_query_plan", shadow, StringComparison.Ordinal);
        Assert.Contains("plan_statement_count", shadow, StringComparison.Ordinal);
        Assert.Contains("dm_exec_query_stats", shadow, StringComparison.Ordinal);
        /* the plan columns come first, so their ordinals (27, 28) are those of the inline query */
        Assert.True(
            shadow.IndexOf("query_plan_xml_bytes", StringComparison.Ordinal) < shadow.IndexOf("plan_statement_count", StringComparison.Ordinal));
        /* the shadow text is the inline text plus the two identity fragments and nothing else */
        string Fragment(string name) => (string)typeof(ProcedureStatsCollector)
            .GetField(name, System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!.GetRawConstantValue()!;
        Assert.Equal(
            inline,
            shadow.Replace(Fragment("PlanIdentitySelectFragment"), "", StringComparison.Ordinal)
                .Replace(Fragment("PlanIdentityApplyFragment"), "", StringComparison.Ordinal));
        Assert.DoesNotContain("dm_exec_text_query_plan", deferred, StringComparison.Ordinal);
    }

    [Fact]
    public void AGatedOnCycle_ShipsTheNoPlanForm_PlusTheIdentity_AndNoPlanRender()
    {
        var sql = ProcedureStatsCollector.Instance.BuildQuery(Context(capture: false, identity: true)).Text;

        Assert.DoesNotContain("dm_exec_text_query_plan", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("query_plan_xml", sql, StringComparison.Ordinal);
        Assert.Contains("plan_generation_sum", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void WithoutTheSwitch_TheQuery_IsUnchanged()
    {
        var plain = ProcedureStatsCollector.Instance.BuildQuery(Context(true, false)).Text;

        Assert.DoesNotContain("plan_statement_count", plain, StringComparison.Ordinal);
        Assert.Equal(plain, ProcedureStatsCollector.Instance.BuildQuery(Context(true, false, defer: false)).Text);
    }

    [Theory]
    [InlineData(true, 29)]    /* shadow: after the inline plan columns at 27 and 28 */
    [InlineData(false, 27)]   /* a gated cycle: identity alone */
    public async Task ReadAsync_FindsTheIdentity_WhereTheQueryPutIt(bool capture, int firstIdentityOrdinal)
    {
        using var table = new DataTable();
        for (var i = 0; i < 27; i++)
        {
            table.Columns.Add("c" + i.ToString(CultureInfo.InvariantCulture), i switch
            {
                4 or 5 => typeof(DateTime),
                0 or 1 or 2 or 3 or 25 or 26 => typeof(string),
                _ => typeof(long),
            });
        }

        if (capture)
        {
            table.Columns.Add("query_plan_xml", typeof(string));
            table.Columns.Add("query_plan_xml_bytes", typeof(long));
        }

        table.Columns.Add("plan_statement_count", typeof(long));
        table.Columns.Add("plan_last_statement_compile", typeof(DateTime));
        table.Columns.Add("plan_generation_sum", typeof(long));

        var values = new object[table.Columns.Count];
        for (var i = 0; i < 27; i++)
        {
            values[i] = table.Columns[i].DataType == typeof(long) && i is 6 or 7 or 8 or 9 or 10 or 11 or 22 or 23 or 24 ? 1L : DBNull.Value;
        }

        if (capture)
        {
            values[27] = "<plan/>";
            values[28] = 7L;
        }

        values[firstIdentityOrdinal] = 4L;
        values[firstIdentityOrdinal + 1] = s_compile;
        values[firstIdentityOrdinal + 2] = 11L;
        table.Rows.Add(values);

        await using var reader = table.CreateDataReader();
        var rows = await ProcedureStatsCollector.Instance.ReadAsync(reader, Context(capture, identity: true), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(4L, row.PlanStatementCount);
        Assert.Equal(s_compile, row.PlanLastStatementCompile);
        Assert.Equal(11L, row.PlanGenerationSum);
        Assert.Equal(capture ? "<plan/>" : null, row.QueryPlanXml);
    }

    private sealed class NoDeltas : ICollectorDeltaCalculator
    {
        public long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
            DateTime? collectionTime = null, int maxGapSeconds = 0) => currentValue;

        public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
            out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        {
            intervalSeconds = 0;
            return currentValue;
        }
    }
}
