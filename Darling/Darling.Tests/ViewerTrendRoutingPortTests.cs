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
/// raw can serve, the hourly rollup otherwise, and the series says which. The routing and the hourly SQL now
/// live in Storage (<see cref="DurationTrendRouting"/>) where both apps can call them; the MCP reader keeps its
/// own named members, and what this class pins is that those members ARE the Storage definitions — same text,
/// same constants, same decision for every input — so the two apps cannot drift about which relation answers a
/// seven-day chart or about what the served series covers. The pin is deliberately over the compiled values,
/// not over source text: a change to either side that is not made to both fails here.
/// </summary>
public sealed class ViewerTrendRoutingPortTests
{
    private static readonly DateTime Now = new(2026, 8, 19, 12, 0, 0, DateTimeKind.Utc);

    private static string Lf(string s) => s.Replace("\r\n", "\n", StringComparison.Ordinal);

    /// <summary>The MCP reader's hourly SQL is the Storage builder's output, byte for byte (line endings aside).</summary>
    [Fact]
    public void McpHourlySql_IsTheStorageBuilder_WithoutTheDatabaseFilter()
    {
        Assert.Equal(Lf(DurationTrendRouting.QueryDurationTrendHourlySql(withDatabaseFilter: false)), Lf(DarlingTrendReader.QueryDurationTrendHourlySql));
        Assert.Equal(Lf(DurationTrendRouting.ProcedureDurationTrendHourlySql(withDatabaseFilter: false)), Lf(DarlingTrendReader.ProcedureDurationTrendHourlySql));
    }

    /// <summary>
    /// The viewer's hourly SQL is the MCP's plus exactly one line — the #1319 database filter, the same guarded
    /// <c>$4::text[]</c> shape its raw reads carry — so the desktop chart and the tool run one statement.
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
        Assert.Equal(Lf(mcp), string.Join('\n', viewerLines.Where(l => !l.Contains("$4::text[]", StringComparison.Ordinal))));

        /* The filter sits inside the WHERE, before the GROUP BY — a filter after the aggregate would be a HAVING
           on a column the rollup groups by, which parses and silently filters nothing. */
        Assert.True(Array.IndexOf(viewerLines, filter) < Array.FindIndex(viewerLines, l => l.StartsWith("GROUP BY", StringComparison.Ordinal)));
        Assert.Contains("$4", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain("$5", viewer, StringComparison.Ordinal);
    }

    /// <summary>The constants the decision and the disclosure rest on are one number each, read by both.</summary>
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
    /// The census: for every hours_back the tools accept, across the availability and coverage shapes the
    /// ladder distinguishes (fully built / no rollup / rollup floor above the start with raw measured deeper /
    /// with raw measured shallower / unmeasured), the viewer's resolver and the MCP's return the same tier. The
    /// grid straddles the raw margin on purpose: the boundary hours are where a re-derivation would diverge.
    /// </summary>
    [Fact]
    public void ResolveTier_ViewerAndMcp_AgreeOnEveryInput()
    {
        var coverages = new[]
        {
            TierCoverage.Unknown,
            new TierCoverage(HourlyFloorUtc: Now.AddDays(-2), DailyFloorUtc: null, RawOldestUtc: Now.AddDays(-9)),
            new TierCoverage(HourlyFloorUtc: Now.AddDays(-2), DailyFloorUtc: null, RawOldestUtc: Now.AddDays(-1)),
            new TierCoverage(HourlyFloorUtc: Now.AddDays(-30), DailyFloorUtc: null, RawOldestUtc: Now.AddDays(-3)),
            new TierCoverage(HourlyFloorUtc: null, DailyFloorUtc: null, RawOldestUtc: Now.AddDays(-9)),
        };

        var compared = 0;
        for (var hoursBack = 1; hoursBack <= McpHelpers.MaxHoursBack; hoursBack++)
        {
            var start = Now.AddHours(-hoursBack);
            foreach (var hourlyAvailable in new[] { true, false })
            foreach (var coverage in coverages)
            {
                Assert.Equal(
                    DarlingTrendReader.ResolveTier(start, Now, hourlyAvailable, coverage),
                    DurationTrendRouting.ResolveTier(start, Now, hourlyAvailable, coverage));
                Assert.Equal(DarlingTrendReader.ShouldUseRawTier(start, Now), DurationTrendRouting.ShouldUseRawTier(start, Now));
                compared++;
            }
        }

        Assert.True(compared >= McpHelpers.MaxHoursBack * 2 * coverages.Length);

        /* And the decision is the one the defect needed: a 7-day window on a built store leaves raw. */
        Assert.Equal(RetentionTier.Hourly, DurationTrendRouting.ResolveTier(Now.AddHours(-168), Now, hourlyAvailable: true, TierCoverage.Unknown));
        Assert.Equal(RetentionTier.Raw, DurationTrendRouting.ResolveTier(Now.AddHours(-24), Now, hourlyAvailable: true, TierCoverage.Unknown));
        Assert.Equal(RetentionTier.Raw, DurationTrendRouting.ResolveTier(Now.AddHours(-168), Now, hourlyAvailable: false, TierCoverage.Unknown));
    }

    /// <summary>The coverage description and the tier word are the MCP payload's, not a viewer restatement.</summary>
    [Fact]
    public void DescribeCoverage_AndSourceWord_MatchTheMcpPayload()
    {
        var start = Now.AddHours(-24);
        foreach (var first in new DateTime?[] { null, start, start.AddMinutes(90), start.AddMinutes(91), start.AddHours(4) })
        {
            Assert.Equal(DarlingTrendReader.DescribeCoverage(first, start), DurationTrendRouting.DescribeCoverage(first, start));
        }

        Assert.Equal((start.AddMinutes(91), true), DurationTrendRouting.DescribeCoverage(start.AddMinutes(91), start));

        var rawRoute = DarlingTrendReader.ResolveQueryDurationTrendRoute(Now.AddHours(-2), RollupAvailability.All, RollupCoverage.Unknown, Now);
        var hourlyRoute = DarlingTrendReader.ResolveQueryDurationTrendRoute(Now.AddHours(-168), RollupAvailability.All, RollupCoverage.Unknown, Now);
        Assert.Equal(RetentionTier.Raw, rawRoute.Tier);
        Assert.Equal(RetentionTier.Hourly, hourlyRoute.Tier);
        Assert.Equal(rawRoute.Source, DurationTrendRouting.SourceWord(RetentionTier.Raw));
        Assert.Equal(hourlyRoute.Source, DurationTrendRouting.SourceWord(RetentionTier.Hourly));
        Assert.Equal("raw", new QueryTrendSeries(new List<QueryTrendPoint>(), RetentionTier.Raw, start, false).Source);
        Assert.Equal("hourly", new QueryTrendSeries(new List<QueryTrendPoint>(), RetentionTier.Hourly, start, false).Source);
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
    /// Every chart update takes the series and shows its coverage before drawing — the disclosure is not
    /// optional per chart. <c>chart.Reset()</c> inside <c>ClearChart</c> clears a previous title, so a stale
    /// truncation note cannot survive a reload that came back empty.
    /// </summary>
    [Fact]
    public void EveryRoutedTrendChart_ShowsItsCoverage()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.QueryTrends.cs");
        foreach (var chart in new[] { "QueryDurationTrendChart", "ProcDurationTrendChart", "ExecutionCountTrendChart" })
        {
            Assert.Contains($"ShowTrendCoverage({chart}, series);", source, StringComparison.Ordinal);
        }

        Assert.Contains("private void UpdateQueryDurationTrendChart(QueryTrendSeries series", source, StringComparison.Ordinal);
        Assert.Contains("private void UpdateProcDurationTrendChart(QueryTrendSeries series", source, StringComparison.Ordinal);
        Assert.Contains("private void UpdateExecutionCountTrendChart(QueryTrendSeries series", source, StringComparison.Ordinal);
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
        Assert.Contains("reader.IsDBNull(2) ? null : ToDouble(reader.GetValue(2)),", lite, StringComparison.Ordinal);
        Assert.DoesNotContain("IsDBNull(2) ? 0", lite, StringComparison.Ordinal);

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
