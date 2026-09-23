/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653: the viewer's Performance Trends tab takes the route the MCP trio took in #3590 — raw for a window
/// raw can serve, the hourly rollup otherwise, and the series says which. The routing and the hourly SQL live
/// in Storage (<see cref="DurationTrendRouting"/>) where both apps can call them; the MCP reader keeps its own
/// named members, and what this class pins is that those members ARE the Storage definitions.
///
/// <para><b>Equality became identity.</b> #3666 moved the definitions and pinned the MCP reader's copies EQUAL
/// to them over compiled values — a pin that fails only after the two have drifted. The #3653 follow-up made
/// every one of those members an ALIAS (a const bound to the Storage const, a static readonly bound to the
/// builder's output, an expression-bodied delegation for the pure functions), so there is one definition and
/// nothing left to drift. Two consequences for this class: the compiled-value comparisons that survive below
/// are kept where they still pin something a source alias does not (the alias's ARGUMENT — that the MCP text
/// is the builder's output WITHOUT the viewer's filter; the literal against the rollup's declared bucket), and
/// the ones that had become a function compared with itself (the 1,680-cell resolver census, the coverage
/// tuple loop) are gone, replaced by <see cref="McpTrendReaderMembers_AreAliasesOfTheStorageDefinitions"/>,
/// which reads the declarations and fails the moment any of them is restated.</para>
///
/// <para>The same PR gave the fourth chart — the Query Store duration trend, whose routing is #2736's
/// materialization watermark rather than this ladder — its own disclosure, in the same title idiom, off the
/// floor the route already carried; those pins are here too, beside the siblings'.</para>
/// </summary>
public sealed class ViewerTrendRoutingPortTests
{
    private static readonly DateTime Now = new(2026, 8, 19, 12, 0, 0, DateTimeKind.Utc);

    private static string Lf(string s) => s.Replace("\r\n", "\n", StringComparison.Ordinal);

    /// <summary>The MCP reader's hourly SQL is the Storage builder's output, byte for byte (line endings aside).
    /// Since #3653 that is true by alias, and since #3897 the builder is the BUCKETED one
    /// (<see cref="DurationTrendRouting.BuildBucketedHourlyTrendSql"/>, which gathers the rollup's hours into the
    /// tool's width); what this still pins is the alias's argument — the legacy view, no database filter — which a
    /// source pin on the declaration names but only a value comparison proves the builder honours.</summary>
    [Fact]
    public void McpHourlySql_IsTheStorageBuilder_WithoutTheDatabaseFilter()
    {
        Assert.Equal(Lf(DurationTrendRouting.BuildBucketedHourlyTrendSql(TimescaleSupport.QueryStatsHourlyView)), Lf(DarlingTrendReader.QueryDurationTrendHourlySql));
        Assert.Equal(Lf(DurationTrendRouting.BuildBucketedHourlyTrendSql(TimescaleSupport.ProcedureStatsHourlyView)), Lf(DarlingTrendReader.ProcedureDurationTrendHourlySql));
        Assert.DoesNotContain("$4::text[]", DarlingTrendReader.QueryDurationTrendHourlySql, StringComparison.Ordinal);
        Assert.DoesNotContain("$4::text[]", DarlingTrendReader.ProcedureDurationTrendHourlySql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The viewer's hourly SQL reads the rollup rows the MCP's buckets are built from, plus exactly one line —
    /// the #1319 database filter, the same guarded <c>$4::text[]</c> shape its raw reads carry. Until #3897 the
    /// two were one statement; now the tool gathers the hours into its own width (the chart plots every hour), so
    /// what must stay one is the READ of the rollup: the same relation, the same window and the same per-hour
    /// grouping, the viewer's plus its filter.
    /// </summary>
    [Theory]
    [InlineData(nameof(ViewerDataService.QueryDurationTrendHourlySql))]
    [InlineData(nameof(ViewerDataService.ProcedureDurationTrendHourlySql))]
    public void ViewerHourlySql_IsTheMcpText_PlusTheDatabaseFilterLine(string name)
    {
        var (viewer, mcp) = name switch
        {
            nameof(ViewerDataService.QueryDurationTrendHourlySql) => (ViewerDataService.QueryDurationTrendHourlySql, DarlingTrendReader.QueryDurationTrendHourlySql),
            _ => (ViewerDataService.ProcedureDurationTrendHourlySql, DarlingTrendReader.ProcedureDurationTrendHourlySql),
        };

        var viewerLines = Lf(viewer).Split('\n');
        var filterLines = viewerLines.Where(l => l.Contains("$4::text[]", StringComparison.Ordinal)).ToArray();
        var filter = Assert.Single(filterLines);
        Assert.Equal("AND   ($4::text[] IS NULL OR database_name = ANY($4))", filter.Trim());

        /* The rollup read, FROM through GROUP BY bucket: the viewer's minus its filter line IS the MCP's hourly CTE. */
        static string[] RollupRead(IEnumerable<string> lines) => lines
            .Select(l => l.Trim())
            .SkipWhile(l => !l.StartsWith("FROM ", StringComparison.Ordinal))
            .TakeWhile(l => l != "GROUP BY bucket")
            .Append("GROUP BY bucket")
            .ToArray();
        Assert.Equal(
            RollupRead(viewerLines.Where(l => !l.Contains("$4::text[]", StringComparison.Ordinal))),
            RollupRead(Lf(mcp).Split('\n')));
        Assert.Contains("GROUP BY bucket", Lf(mcp), StringComparison.Ordinal);

        /* The filter sits inside the WHERE, before the GROUP BY — a filter after the aggregate would be a HAVING
           on a column the rollup groups by, which parses and silently filters nothing. */
        Assert.True(Array.IndexOf(viewerLines, filter) < Array.FindIndex(viewerLines, l => l.StartsWith("GROUP BY", StringComparison.Ordinal)));
        Assert.Contains("$4", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain("$5", viewer, StringComparison.Ordinal);
    }

    /// <summary>The constants the decision and the disclosure rest on are one number each, read by both —
    /// by alias since #3653 (see <see cref="McpTrendReaderMembers_AreAliasesOfTheStorageDefinitions"/>); the
    /// last assertion, the literal against the rollup's declared bucket width, is the one that pins a fact no
    /// alias can.</summary>
    [Fact]
    public void RoutingConstants_AreSharedNotRestated()
    {
        Assert.Equal(DurationTrendRouting.RawTierMargin, DarlingTrendReader.RawTierMargin);
        Assert.Equal(DurationTrendRouting.TruncationSlack, DarlingTrendReader.TruncationSlack);
        Assert.Equal(DurationTrendRouting.HourlyBucketSecondsSql, DarlingTrendReader.HourlyBucketSecondsSql);
        Assert.Equal(
            TimescaleSupport.HourlyBucket.TotalSeconds,
            double.Parse(DurationTrendRouting.HourlyBucketSecondsSql, System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// The decision is the one the defect needed: a 7-day window on a built store leaves raw; a 1-day window
    /// stays; a store without the rollup stays raw at any depth. Pinned on the Storage resolver directly —
    /// the MCP reader's <c>ResolveTier</c> / <c>ShouldUseRawTier</c> ARE this resolver since #3653 (the
    /// 1,680-cell census #3666 ran between the two here compared a function with itself once the aliases
    /// landed and was retired; <see cref="McpTrendReaderMembers_AreAliasesOfTheStorageDefinitions"/> pins the
    /// delegation, DarlingQueryTrendTieringTests walks the ladder's table).
    /// </summary>
    [Fact]
    public void ResolveTier_TheDecisionTheDefectNeeded()
    {
        Assert.Equal(RetentionTier.Hourly, DurationTrendRouting.ResolveTier(Now.AddHours(-168), Now, hourlyAvailable: true, TierCoverage.Unknown));
        Assert.Equal(RetentionTier.Raw, DurationTrendRouting.ResolveTier(Now.AddHours(-24), Now, hourlyAvailable: true, TierCoverage.Unknown));
        Assert.Equal(RetentionTier.Raw, DurationTrendRouting.ResolveTier(Now.AddHours(-168), Now, hourlyAvailable: false, TierCoverage.Unknown));
        Assert.True(DurationTrendRouting.ShouldUseRawTier(Now.AddHours(-24), Now));
        Assert.False(DurationTrendRouting.ShouldUseRawTier(Now.AddHours(-168), Now));
    }

    /// <summary>
    /// Identity, not equality (#3653): every member of <c>DarlingTrendReader</c> that #3666 pinned EQUAL to a
    /// <see cref="DurationTrendRouting"/> member is declared AS that member — so the two cannot drift because
    /// there are not two. Read off the source because that is the only place an alias is visible: a const
    /// alias compiles to the same literal a restatement would, a static readonly bound to a builder call is a
    /// fresh string each time (no reference to compare), and a delegating method body is indistinguishable
    /// from a copied one by output — which is exactly why the old equality pins could not tell the two apart.
    /// The negative half is the one that bites: the file must carry NONE of the definitions it used to.
    /// </summary>
    [Fact]
    public void McpTrendReaderMembers_AreAliasesOfTheStorageDefinitions()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingTrendReader.cs");

        Assert.Contains("public const string HourlyBucketSecondsSql = DurationTrendRouting.HourlyBucketSecondsSql;", source, StringComparison.Ordinal);
        Assert.Contains("public static readonly string QueryDurationTrendHourlySql =\n        DurationTrendRouting.BuildBucketedHourlyTrendSql(TimescaleSupport.QueryStatsHourlyView);", Lf(source), StringComparison.Ordinal);
        Assert.Contains("public static readonly string ProcedureDurationTrendHourlySql =\n        DurationTrendRouting.BuildBucketedHourlyTrendSql(TimescaleSupport.ProcedureStatsHourlyView);", Lf(source), StringComparison.Ordinal);
        Assert.Contains("public static readonly TimeSpan RawTierMargin = DurationTrendRouting.RawTierMargin;", source, StringComparison.Ordinal);
        Assert.Contains("public static readonly TimeSpan TruncationSlack = DurationTrendRouting.TruncationSlack;", source, StringComparison.Ordinal);
        Assert.Contains("public static bool ShouldUseRawTier(DateTime startUtc, DateTime nowUtc) =>\n        DurationTrendRouting.ShouldUseRawTier(startUtc, nowUtc);", Lf(source), StringComparison.Ordinal);
        Assert.Contains("public static RetentionTier ResolveTier(DateTime startUtc, DateTime nowUtc, bool hourlyAvailable, TierCoverage coverage) =>\n        DurationTrendRouting.ResolveTier(startUtc, nowUtc, hourlyAvailable, coverage);", Lf(source), StringComparison.Ordinal);
        Assert.Contains("public static (DateTime EffectiveStartUtc, bool Truncated) DescribeCoverage(DateTime? firstPointUtc, DateTime startUtc) =>\n        DurationTrendRouting.DescribeCoverage(firstPointUtc, startUtc);", Lf(source), StringComparison.Ordinal);

        /* The tier word too: the route record and the query-history payload spell it through SourceWord. */
        Assert.Equal(2, CountOf(source, "DurationTrendRouting.SourceWord("));
        Assert.DoesNotContain("? \"raw\" : \"hourly\"", source, StringComparison.Ordinal);

        /* None of the retired definitions survive as text — a restatement beside an alias is drift with a
           head start. */
        Assert.DoesNotContain("\"3600.0\"", source, StringComparison.Ordinal);
        Assert.DoesNotContain("SUM(elapsed_time_sum)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("FROM {TimescaleSupport.QueryStatsHourlyView}", source, StringComparison.Ordinal);
        Assert.DoesNotContain("FROM {TimescaleSupport.ProcedureStatsHourlyView}", source, StringComparison.Ordinal);
        Assert.DoesNotContain("TimeSpan.FromHours(1)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("TimeSpan.FromMinutes(90)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("TimescaleSupport.RawRetentionSpan + RawTierMargin", source, StringComparison.Ordinal);
        Assert.DoesNotContain("TierCoverage.ReachesFurtherBack(", source, StringComparison.Ordinal);
        Assert.DoesNotContain("first > startUtc + TruncationSlack", source, StringComparison.Ordinal);

        /* And the values still land where the tools read them (the alias is public, the names unchanged). */
        Assert.Equal(DurationTrendRouting.RawTierMargin, DarlingTrendReader.RawTierMargin);
        Assert.Equal(DurationTrendRouting.TruncationSlack, DarlingTrendReader.TruncationSlack);
        Assert.Equal(DurationTrendRouting.HourlyBucketSecondsSql, DarlingTrendReader.HourlyBucketSecondsSql);
    }

    /// <summary>The coverage description and the tier word are the MCP payload's, not a viewer restatement.</summary>
    [Fact]
    public void DescribeCoverage_AndSourceWord_MatchTheMcpPayload()
    {
        var start = Now.AddHours(-24);

        /* The coverage rule at its edges (the MCP reader's DescribeCoverage IS this one since #3653). */
        Assert.Equal((start, false), DurationTrendRouting.DescribeCoverage(null, start));
        Assert.Equal((start.AddMinutes(90), false), DurationTrendRouting.DescribeCoverage(start.AddMinutes(90), start));
        Assert.Equal((start.AddMinutes(91), true), DurationTrendRouting.DescribeCoverage(start.AddMinutes(91), start));

        var rawRoute = DarlingTrendReader.ResolveQueryDurationTrendRoute(Now.AddHours(-2), RollupAvailability.All, RollupCoverage.Unknown, Now);
        var hourlyRoute = DarlingTrendReader.ResolveQueryDurationTrendRoute(Now.AddHours(-168), RollupAvailability.All, RollupCoverage.Unknown, Now);
        Assert.Equal(RetentionTier.Raw, rawRoute.Tier);
        Assert.Equal(RetentionTier.Hourly, hourlyRoute.Tier);
        Assert.Equal(rawRoute.Source, DurationTrendRouting.SourceWord(RetentionTier.Raw));
        Assert.Equal(hourlyRoute.Source, DurationTrendRouting.SourceWord(RetentionTier.Hourly));
        Assert.Equal("raw", new QueryTrendSeries(new List<QueryTrendPoint>(), RetentionTier.Raw, start, false).Source);
        Assert.Equal("hourly", new QueryTrendSeries(new List<QueryTrendPoint>(), RetentionTier.Hourly, start, false).Source);

        /* The Query Store chart's word is the Query Store payload's (#3653): rollup+raw / raw, one definition. */
        var rawOnly = QueryStoreTrendRouting.QueryStoreTrendRoute.RawOnly;
        var routed = QueryStoreTrendRouting.Resolve(rollupExists: true, oldestBucketUtc: Now.AddDays(-3), newestBucketUtc: Now.AddHours(-2));
        Assert.Equal("raw", QueryStoreTrendRouting.SourceWord(rawOnly));
        Assert.Equal("rollup+raw", QueryStoreTrendRouting.SourceWord(routed));
        Assert.Equal("raw", new QueryStoreTrendSeries(new List<QueryTrendPoint>(), rawOnly, start, null).Source);
        Assert.Equal("rollup+raw", new QueryStoreTrendSeries(new List<QueryTrendPoint>(), routed, start, null).Source);
    }

    /// <summary>
    /// The viewer's routed reads call the Storage resolver — not <see cref="RetentionTierRouter"/> (the
    /// built-in tabs' one-day-margin ladder) and not a private re-derivation — and probe the GRAIN's own
    /// availability flag, so a store with the query rollup but not the procedure one routes each trend by
    /// its own rollup.
    /// </summary>
    [Fact]
    public void ViewerTrendReads_RouteThroughTheSharedResolver()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.QueryTrends.cs");
        var partial = source[source.IndexOf("public sealed partial class ViewerDataService", StringComparison.Ordinal)..];

        /* Two resolver calls: the shared body the two duration trends go through, and the execution-count read
           (which routes on the query grain by itself). The Query Store read is NOT a third — its route is
           #2736's watermark, not this ladder. Three coverage descriptions: the same two sites, plus the Query
           Store read (#3653), which names its effective start by the same shared rule so the chart and the
           MCP payload agree on the instant even though its unserved-head rule is its own. */
        Assert.Equal(2, CountOf(partial, "DurationTrendRouting.ResolveTier("));
        Assert.Equal(3, CountOf(partial, "DurationTrendRouting.DescribeCoverage("));
        Assert.DoesNotContain("RetentionTierRouter.", partial, StringComparison.Ordinal);
        Assert.Contains("static rollups => rollups.QueryGrainHourly", partial, StringComparison.Ordinal);
        Assert.Contains("static rollups => rollups.ProcedureGrainHourly", partial, StringComparison.Ordinal);
        Assert.Contains("TimescaleSupport.ProcedureStatsHourlyView, TimescaleSupport.ProcedureStatsDailyView", partial, StringComparison.Ordinal);

        /* The hourly texts are the Storage builder's with the viewer's filter, not a copied literal. */
        Assert.Contains("DurationTrendRouting.QueryDurationTrendHourlySql(withDatabaseFilter: true)", partial, StringComparison.Ordinal);
        Assert.Contains("DurationTrendRouting.ProcedureDurationTrendHourlySql(withDatabaseFilter: true)", partial, StringComparison.Ordinal);
        Assert.DoesNotContain("FROM query_stats_hourly", partial, StringComparison.Ordinal);
        Assert.DoesNotContain("FROM procedure_stats_hourly", partial, StringComparison.Ordinal);
    }

    /// <summary>
    /// The execution-count chart routes with its duration sibling and, on the hourly tier, plots the SAME
    /// statement's <c>executions_per_second</c> column (ordinal 2) rather than a fourth SQL text.
    /// </summary>
    [Fact]
    public void ExecutionCountTrend_RoutesWithItsSibling_AndReadsTheSharedRollupColumn()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.QueryTrends.cs");
        var method = source[source.IndexOf("public async Task<QueryTrendSeries> GetExecutionCountTrendAsync(", StringComparison.Ordinal)..];
        method = method[..method.IndexOf("return new QueryTrendSeries(", StringComparison.Ordinal)];

        Assert.Contains("DurationTrendRouting.ResolveTier(", method, StringComparison.Ordinal);
        Assert.Contains("rollups.QueryGrainHourly", method, StringComparison.Ordinal);
        Assert.Contains("ReadTrendPointsAsync(ExecutionCountTrendSql, serverId, startUtc, endUtc, databaseNames, valueOrdinal: 1, executionsOrdinal: null", method, StringComparison.Ordinal);
        Assert.Contains("ReadTrendPointsAsync(QueryDurationTrendHourlySql, serverId, startUtc, endUtc, databaseNames, valueOrdinal: 2, executionsOrdinal: null", method, StringComparison.Ordinal);

        /* Ordinal 2 of the shared hourly text IS executions_per_second. */
        var projection = Lf(ViewerDataService.QueryDurationTrendHourlySql).Split('\n').Where(l => l.Contains(" AS ", StringComparison.Ordinal)).Select(l => l.Trim()).ToArray();
        Assert.Equal(3, projection.Length);
        Assert.EndsWith("AS collection_time,", projection[0], StringComparison.Ordinal);
        Assert.EndsWith("AS elapsed_ms_per_second,", projection[1], StringComparison.Ordinal);
        Assert.EndsWith("AS executions_per_second", projection[2], StringComparison.Ordinal);
    }

    /// <summary>
    /// The chart says what it served, in the payload's vocabulary: the tier word always, the first served point
    /// only when the tier did not hold the window's head — the one case where the axis and the data disagree.
    /// The raw, un-truncated case is deliberately terse: it is the common one, and a long banner on every chart
    /// would teach the eye to skip the one that matters.
    /// </summary>
    [Fact]
    public void DescribeTrendCoverage_NamesTheTier_AndTheHeadOnlyWhenTruncated()
    {
        var start = new DateTime(2026, 8, 12, 12, 0, 0, DateTimeKind.Unspecified);

        var raw = ViewerServerTab.DescribeTrendCoverage(new QueryTrendSeries(new List<QueryTrendPoint>(), RetentionTier.Raw, start, false));
        Assert.Equal("Source: raw (one point per collection)", raw);

        var hourly = ViewerServerTab.DescribeTrendCoverage(new QueryTrendSeries(new List<QueryTrendPoint>(), RetentionTier.Hourly, start, false));
        Assert.Equal("Source: hourly rollup (one point per hour)", hourly);
        Assert.DoesNotContain("data begins", hourly, StringComparison.Ordinal);

        var head = start.AddDays(3);
        var truncated = ViewerServerTab.DescribeTrendCoverage(new QueryTrendSeries(new List<QueryTrendPoint>(), RetentionTier.Hourly, head, true));
        Assert.StartsWith("Source: hourly rollup (one point per hour) — data begins ", truncated, StringComparison.Ordinal);
        Assert.Contains(ViewerTimeHelper.ForDisplay(head).ToString("yyyy-MM-dd HH:mm", System.Globalization.CultureInfo.InvariantCulture), truncated, StringComparison.Ordinal);
        Assert.EndsWith("the store no longer holds the rest of this window at this tier", truncated, StringComparison.Ordinal);

        /* The word the chart leads with is the word the tool publishes. */
        foreach (var tier in new[] { RetentionTier.Raw, RetentionTier.Hourly })
        {
            var text = ViewerServerTab.DescribeTrendCoverage(new QueryTrendSeries(new List<QueryTrendPoint>(), tier, start, false));
            Assert.StartsWith("Source: " + DurationTrendRouting.SourceWord(tier), text, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The fourth chart's title (#3653), in the siblings' idiom through the siblings' composer: the payload's
    /// route word always, what a point IS on that route (the grain changes at #2736's watermark, so the
    /// rollup+raw parenthetical names the seam), and the "data begins" clause ONLY when the head went
    /// unserved — the rollup's measured floor above the requested start — with this route's own reason and
    /// remedy. A late first point on a route that reached the start is NOT disclosed: that is a quiet server,
    /// and the sibling wording ("the store no longer holds") would be false here.
    /// </summary>
    [Fact]
    public void DescribeQueryStoreTrendCoverage_NamesTheRoute_AndTheHeadOnlyWhenUnserved()
    {
        var start = new DateTime(2026, 8, 12, 12, 0, 0, DateTimeKind.Unspecified);
        var rawFrom = new DateTime(2026, 8, 19, 10, 0, 0, DateTimeKind.Unspecified);
        var rawOnly = QueryStoreTrendRouting.QueryStoreTrendRoute.RawOnly;
        var routed = new QueryStoreTrendRouting.QueryStoreTrendRoute(UseRollup: true, RawStartUtc: rawFrom, RollupFloorUtc: start.AddDays(-30));

        var raw = ViewerServerTab.DescribeQueryStoreTrendCoverage(new QueryStoreTrendSeries(new List<QueryTrendPoint>(), rawOnly, start, null));
        Assert.Equal("Source: raw (one point per Query Store interval)", raw);

        var served = ViewerServerTab.DescribeQueryStoreTrendCoverage(new QueryStoreTrendSeries(new List<QueryTrendPoint>(), routed, start, null));
        var rawFromText = ViewerTimeHelper.ForDisplay(rawFrom).ToString("yyyy-MM-dd HH:mm", System.Globalization.CultureInfo.InvariantCulture);
        Assert.Equal($"Source: rollup+raw (one point per hour before {rawFromText}, one per Query Store interval from it)", served);
        Assert.DoesNotContain("data begins", served, StringComparison.Ordinal);

        /* A late head on a route that reached the start: quiet, not truncated — no clause. */
        var quietHead = ViewerServerTab.DescribeQueryStoreTrendCoverage(new QueryStoreTrendSeries(new List<QueryTrendPoint>(), routed, start.AddDays(3), null));
        Assert.DoesNotContain("data begins", quietHead, StringComparison.Ordinal);

        /* The floor above the start: the head was not served, and the title says from where and why. */
        var head = start.AddDays(3);
        var unserved = ViewerServerTab.DescribeQueryStoreTrendCoverage(new QueryStoreTrendSeries(new List<QueryTrendPoint>(), routed, head, UnservedBeforeUtc: head));
        Assert.StartsWith($"Source: rollup+raw (one point per hour before {rawFromText}, one per Query Store interval from it) — data begins ", unserved, StringComparison.Ordinal);
        Assert.Contains(ViewerTimeHelper.ForDisplay(head).ToString("yyyy-MM-dd HH:mm", System.Globalization.CultureInfo.InvariantCulture), unserved, StringComparison.Ordinal);
        Assert.EndsWith("; the corrected Query Store rollup has not materialized the rest of this window (--backfill-rollups reaches it)", unserved, StringComparison.Ordinal);
        Assert.DoesNotContain("no longer holds", unserved, StringComparison.Ordinal);

        /* Same sentence shape as the siblings — "Source: {served} — data begins yyyy-MM-dd HH:mm; {why}" — because
           both go through one composer; the two slots differ, the frame does not. */
        var sibling = ViewerServerTab.DescribeTrendCoverage(new QueryTrendSeries(new List<QueryTrendPoint>(), RetentionTier.Hourly, head, true));
        var frame = new System.Text.RegularExpressions.Regex(@"^Source: [^—]+ — data begins \d{4}-\d{2}-\d{2} \d{2}:\d{2}; [^;]+$");
        Assert.Matches(frame, sibling);
        Assert.Matches(frame, unserved);
        Assert.Matches(new System.Text.RegularExpressions.Regex(@"^Source: [^—;]+$"), served);

        /* The word the chart leads with is the word the tool publishes. */
        Assert.StartsWith("Source: " + QueryStoreTrendRouting.SourceWord(routed), served, StringComparison.Ordinal);
        Assert.StartsWith("Source: " + QueryStoreTrendRouting.SourceWord(rawOnly), raw, StringComparison.Ordinal);
    }

    /// <summary>
    /// The unserved-head rule is ONE definition the MCP tool's <c>routing.unserved_before</c> and the viewer's
    /// title both read (#3653): the rollup's floor when it sits above the requested start, null when the route
    /// reached the start, null on the raw-only route by construction (raw is complete wherever the rollup has
    /// not armed its purge). No slack: a measured floor is not an ambiguous late head.
    /// </summary>
    [Fact]
    public void QueryStoreUnservedBefore_IsOneRule_TheToolAndTheChartRead()
    {
        var start = Now.AddDays(-7);
        var floorAbove = new QueryStoreTrendRouting.QueryStoreTrendRoute(true, Now.AddHours(-1), start.AddDays(3));
        var floorAt = new QueryStoreTrendRouting.QueryStoreTrendRoute(true, Now.AddHours(-1), start);
        var floorBelow = new QueryStoreTrendRouting.QueryStoreTrendRoute(true, Now.AddHours(-1), start.AddDays(-10));
        var floorSliver = new QueryStoreTrendRouting.QueryStoreTrendRoute(true, Now.AddHours(-1), start.AddMinutes(1));
        var floorUnknown = new QueryStoreTrendRouting.QueryStoreTrendRoute(true, Now.AddHours(-1), null);

        Assert.Equal(start.AddDays(3), QueryStoreTrendRouting.UnservedBefore(floorAbove, start));
        Assert.Null(QueryStoreTrendRouting.UnservedBefore(floorAt, start));
        Assert.Null(QueryStoreTrendRouting.UnservedBefore(floorBelow, start));
        Assert.Equal(start.AddMinutes(1), QueryStoreTrendRouting.UnservedBefore(floorSliver, start));
        Assert.Null(QueryStoreTrendRouting.UnservedBefore(floorUnknown, start));
        Assert.Null(QueryStoreTrendRouting.UnservedBefore(QueryStoreTrendRouting.QueryStoreTrendRoute.RawOnly, start));
        Assert.Null(QueryStoreTrendRouting.UnservedBefore(new QueryStoreTrendRouting.QueryStoreTrendRoute(false, default, start.AddDays(3)), start));

        /* The series carries it as the chart's flag. */
        Assert.True(new QueryStoreTrendSeries(new List<QueryTrendPoint>(), floorAbove, start.AddDays(3), start.AddDays(3)).HeadUnserved);
        Assert.False(new QueryStoreTrendSeries(new List<QueryTrendPoint>(), floorBelow, start, null).HeadUnserved);

        /* Both consumers call the Storage rule and the Storage word; neither restates them. */
        var tool = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpTrendTools.cs");
        Assert.Contains("QueryStoreTrendRouting.UnservedBefore(route, windowStartUtc) is DateTime floor", tool, StringComparison.Ordinal);
        Assert.Equal(2, CountOf(tool, "QueryStoreTrendRouting.SourceWord(route)"));
        Assert.DoesNotContain("\"rollup+raw\"", tool, StringComparison.Ordinal);
        Assert.DoesNotContain("route.RollupFloorUtc is DateTime floor && floor > windowStartUtc", tool, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.QueryTrends.cs");
        Assert.Contains("QueryStoreTrendRouting.UnservedBefore(route, startUtc)", viewer, StringComparison.Ordinal);
        Assert.Contains("QueryStoreTrendRouting.SourceWord(Route)", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain("\"rollup+raw\"", viewer, StringComparison.Ordinal);
        Assert.Contains("public async Task<QueryStoreTrendSeries> GetQueryStoreDurationTrendAsync(", viewer, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every chart update takes the series and shows its coverage before drawing — the disclosure is not
    /// optional per chart, and since #3653 that includes the Query Store chart #3666 left out.
    /// <c>chart.Reset()</c> inside <c>ClearChart</c> clears a previous title, so a stale truncation note
    /// cannot survive a reload that came back empty.
    /// </summary>
    [Fact]
    public void EveryRoutedTrendChart_ShowsItsCoverage()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.QueryTrends.cs");
        foreach (var chart in new[] { "QueryDurationTrendChart", "ProcDurationTrendChart", "QueryStoreDurationTrendChart", "ExecutionCountTrendChart" })
        {
            Assert.Contains($"ShowTrendCoverage({chart}, series);", source, StringComparison.Ordinal);
        }

        Assert.Contains("private void UpdateQueryDurationTrendChart(QueryTrendSeries series", source, StringComparison.Ordinal);
        Assert.Contains("private void UpdateProcDurationTrendChart(QueryTrendSeries series", source, StringComparison.Ordinal);
        Assert.Contains("private void UpdateQueryStoreDurationTrendChart(QueryStoreTrendSeries series", source, StringComparison.Ordinal);
        Assert.Contains("private void UpdateExecutionCountTrendChart(QueryTrendSeries series", source, StringComparison.Ordinal);

        /* Both title arms go through the one composer, so the sentence shape cannot fork. */
        Assert.Equal(2, CountOf(source, "=> ComposeTrendCoverage("));
        Assert.Equal(1, CountOf(source, "private static string ComposeTrendCoverage("));
    }

    /// <summary>
    /// #3653's third series: the FinOps PVS top-5 trend coerced a NULL <c>persistent_version_store_size_mb</c>
    /// (an unmeasured pass) to 0 MB on both SKUs — a cliff drawn into a series that has none. The Darling
    /// viewer's reader, which has no payload consumer, skips the row; Lite's reader is shared with
    /// <c>get_pvs_trend</c>, so it carries the point as null (a collection happened; its size is unknown) and
    /// the chart leaves it out. Pinned on both sources from here because the Lite suite's CI filter does not
    /// reach Darling paths.
    /// </summary>
    [Fact]
    public void PvsTrend_BothSkus_NeverPlotAnUnmeasuredPointAsZero()
    {
        var darling = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.FinOps.Pvs.cs");
        var method = darling[darling.IndexOf("GetPvsTrendAsync(", StringComparison.Ordinal)..];
        method = method[..method.IndexOf("return items;", StringComparison.Ordinal)];
        Assert.Contains("if (reader.IsDBNull(2))", method, StringComparison.Ordinal);
        Assert.Contains("continue;", method, StringComparison.Ordinal);
        Assert.DoesNotContain("IsDBNull(2) ? 0", method, StringComparison.Ordinal);

        var lite = RepoFile.ReadRepoFile("Lite", "Services", "LocalDataService.FinOps.Pvs.cs");
        Assert.Contains("public sealed record PvsTrendPoint(string DatabaseName, DateTime CollectionTime, double? PvsSizeMb, double? PctOfDatabase);", lite, StringComparison.Ordinal);
        var liteTrend = lite[lite.IndexOf("GetPvsTrendAsync(", StringComparison.Ordinal)..];
        liteTrend = liteTrend[..liteTrend.IndexOf("return items;", StringComparison.Ordinal)];
        Assert.Contains("reader.IsDBNull(2) ? null : ToDouble(reader.GetValue(2)),", liteTrend, StringComparison.Ordinal);
        /* Scoped to the trend read: the latest-snapshot grid one method up reads a DIFFERENT ordinal-2
           (database_data_size_mb, a denominator) and its 0 is that grid's own #1951 contract. */
        Assert.DoesNotContain("IsDBNull(2) ? 0", liteTrend, StringComparison.Ordinal);

        var chart = RepoFile.ReadRepoFile("Lite", "Controls", "FinOpsTab.xaml.cs");
        Assert.Contains("trend.Where(t => t.PvsSizeMb.HasValue)", chart, StringComparison.Ordinal);

        var tool = RepoFile.ReadRepoFile("Lite", "Mcp", "McpPvsTools.cs");
        Assert.Contains("pvs_measured = p.PvsSizeMb.HasValue,", tool, StringComparison.Ordinal);
    }

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var i = text.IndexOf(needle, StringComparison.Ordinal); i >= 0; i = text.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }
}

/// <summary>
/// #3653 on a live TimescaleDB store: the viewer's routed query-duration trend, asked for a window whose start
/// the raw tier no longer serves, reads the hourly rollup — honouring the database filter — and says so; asked
/// for a recent window it reads raw and says that. Mints its own scratch database (the RollupBackfillLiveTests
/// pattern) so it can plant history and build the rollup without touching anything shared — and is therefore
/// deliberately NOT in the <c>live-postgres</c> collection (the #1776 own-store argument: it cannot race the
/// shared store, and it creates continuous aggregates the shared fixture must never inherit). First executed
/// in CI; the seed's shape is the sibling test's, and the routing assertions are the pure ones above run
/// against the real relation.
/// </summary>
public sealed class ViewerTrendRoutingLivePostgresTests
{
    private const int ServerId = -936536;

    [Fact]
    public async Task RoutedQueryDurationTrend_ReadsTheRollupPastTheRawHorizon_WithTheDatabaseFilter()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #3653 routing test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "the dev fixture is expected to have TimescaleDB installed");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        /* Naive UTC throughout. Hour-aligned so a bucket and a collection coincide exactly. */
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var nowHour = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified);
        var oldest = nowHour.AddDays(-8);

        /* Two databases, one row each per hour: A does 2,000 ms of elapsed / 10 executions an hour, B 6,000 / 30.
           Per hourly bucket: unfiltered 8,000 ms / 40 execs; filtered to A, 2,000 / 10. */
        await using (var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count)
SELECT
    (extract(epoch FROM g)::bigint * 100 + d.n),
    g,
    $3,
    'route-e2e',
    d.name,
    decode(md5('q' || d.n::text), 'hex'),
    decode(md5('h' || d.n::text), 'hex'),
    1000,
    d.n * 2000000,
    d.n * 10
FROM generate_series($1::timestamp, $2::timestamp, INTERVAL '1 hour') AS g
CROSS JOIN (VALUES (1, 'DbA'), (3, 'DbB')) AS d(n, name)", connection))
        {
            insert.Parameters.AddWithValue(oldest);
            insert.Parameters.AddWithValue(nowHour);
            insert.Parameters.AddWithValue(ServerId);
            await insert.ExecuteNonQueryAsync(ct);
        }

        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        await RollupBackfill.RunSliceAsync(
            connection, TimescaleSupport.QueryStatsHourlyView, oldest.Date.AddDays(-1), nowHour.Date.AddDays(1),
            new RefreshDisclosure(message => Assert.Fail($"the backfill refresh degraded unexpectedly: {message}")), ct);

        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        /* A 7-day window: its start is 3 days past the raw margin, so the ladder leaves raw. */
        var start = nowHour.AddDays(-7);
        var end = nowHour;
        var routed = await viewer.GetQueryDurationTrendAsync(ServerId, start, end, nowUtc: now, cancellationToken: ct);

        Assert.Equal(RetentionTier.Hourly, routed.Tier);
        Assert.Equal("hourly", routed.Source);
        Assert.NotEmpty(routed.Points);
        Assert.Equal(start, routed.EffectiveStartUtc);
        Assert.False(routed.Truncated);
        /* Every point is an hour bucket, and the bucket-width denominator gives the exact planted rate:
           8,000 ms / 3,600 s and 40 / 3,600 (truncated to long: 0). No unrated first point on this tier. */
        Assert.All(routed.Points, p =>
        {
            Assert.Equal(0, p.CollectionTime.Minute);
            Assert.Equal(8000.0 / 3600.0, p.Value, 6);
            Assert.Equal(0, p.ExecutionCount);
        });
        Assert.Equal(7 * 24 + 1, routed.Points.Count);

        /* The database filter survives the routing (#1319 on the rollup). */
        var filtered = await viewer.GetQueryDurationTrendAsync(ServerId, start, end, new[] { "DbA" }, nowUtc: now, cancellationToken: ct);
        Assert.Equal(RetentionTier.Hourly, filtered.Tier);
        Assert.Equal(routed.Points.Count, filtered.Points.Count);
        Assert.All(filtered.Points, p => Assert.Equal(2000.0 / 3600.0, p.Value, 6));

        /* The execution-count chart rides the same statement's second column. */
        var executions = await viewer.GetExecutionCountTrendAsync(ServerId, start, end, nowUtc: now, cancellationToken: ct);
        Assert.Equal(RetentionTier.Hourly, executions.Tier);
        Assert.Equal(routed.Points.Count, executions.Points.Count);
        Assert.All(executions.Points, p => Assert.Equal(40.0 / 3600.0, p.Value, 6));

        /* A recent window stays raw, drops its unrated first collection, and says raw. */
        var recent = await viewer.GetQueryDurationTrendAsync(ServerId, nowHour.AddHours(-6), end, nowUtc: now, cancellationToken: ct);
        Assert.Equal(RetentionTier.Raw, recent.Tier);
        Assert.Equal("raw", recent.Source);
        Assert.Equal(6, recent.Points.Count); /* seven collections in the window; the first has no rate */
        Assert.Equal(nowHour.AddHours(-5), recent.Points[0].CollectionTime);
        Assert.Equal(nowHour.AddHours(-5), recent.EffectiveStartUtc);
        Assert.False(recent.Truncated); /* one cadence late is inside the 90-minute slack */
        Assert.All(recent.Points, p => Assert.Equal(8000.0 / 3600.0, p.Value, 6));

        /* Truncation is disclosed when the tier's floor sits inside the window: a window reaching a day past the
           planted history opens at the first bucket the rollup holds, more than the slack after the start. */
        var deep = await viewer.GetQueryDurationTrendAsync(ServerId, oldest.AddDays(-1), end, nowUtc: now, cancellationToken: ct);
        Assert.Equal(RetentionTier.Hourly, deep.Tier);
        Assert.Equal(oldest, deep.EffectiveStartUtc);
        Assert.True(deep.Truncated);
    }
}
