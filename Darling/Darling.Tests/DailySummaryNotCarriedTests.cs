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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 A6, the last clause: a day the rollup tier SKIPPED below its ceiling prints <c>unique_queries = NULL</c>
/// and is named in <c>days_missing[]</c>, where it printed 0 beside that day's real wait, CPU and deadlock numbers.
///
/// <para><b>The two cases the item told apart.</b> A rollup-tier calendar answers its query count from a
/// continuous aggregate. A day inside the aggregate's materialized span for which this server has no bucket used
/// to fall out of the <c>queries</c> LEFT JOIN and be COALESCEd to 0 — the same 0 a day with no query activity
/// prints. They are different facts: the second is measured, the first is a day the tier never materialized
/// (the pre-outage tail <see cref="RetentionTierRouter"/>'s essay derives; repaired at the next start by
/// <see cref="TimescaleSupport.RepairMaterializationHolesAsync"/> since #3731, so the case is rarer, not gone —
/// between the post-resume refresh that moves the ceiling and that start, and past the per-start cap, the
/// calendar still reads the skipped day). Measurement contract rule 1 (<c>MeasurementContractCensusTests</c>):
/// what was not measured is NULL, never 0. The daily calendar is now an instance of it.</para>
///
/// <para><b>The witness is the hole scan's definition, reused, not a second one.</b> The scan calls a bucket a
/// hole when the materialization holds no row for it AND the source holds an admitted row in it. The routed
/// <c>queries</c> CTE's third member asks the same two questions per server at day grain, with the source, its
/// time column and its filter read off <see cref="TimescaleSupport.MaterializationHoleTargets"/> — the repair's
/// own target list. So a server that genuinely ran nothing that day (no source row) is NOT named: the
/// disclosure cannot claim a hole where the raw table was simply empty. The live test below plants exactly
/// that control beside the hole.</para>
///
/// <para><b>Lite is untouched.</b> It has no rollup tier: its calendar reads raw DuckDB, where every day inside
/// retention is the source itself. The shared model's <c>PerformanceCalendarDay.UniqueQueries</c> became
/// <c>long?</c> and the shared <see cref="DailyHealthBandCalculator.BuildKeyMetricsLine"/> takes <c>long?</c>;
/// Lite's callers hand a <c>long</c> and compile unchanged — a no-op field widening, stated here.</para>
/// </summary>
public sealed class DailySummaryNotCarriedTests
{
    private static readonly (RetentionTier Tier, string Relation)[] RoutedForms =
    {
        (RetentionTier.Hourly, TimescaleSupport.QueryStatsHourlyView),
        (RetentionTier.Hourly, TimescaleSupport.QueryStatsIntervalHourlyView),
        (RetentionTier.Daily, TimescaleSupport.QueryStatsDailyView),
    };

    /// <summary>
    /// The outer select passes the NULL through and the presence arm reads the count. On raw both spellings are
    /// the old ones by another name (a COUNT is never NULL), which is what lets one outer text serve every tier;
    /// the old <c>COALESCE(q.c, 0)</c> is gone because it is precisely the fold that turned "not carried" into 0.
    /// </summary>
    [Fact]
    public void TheOuterSelect_PassesTheNullThrough_AndPresenceReadsTheCount()
    {
        var sql = DailySummarySql.RangeSql;
        Assert.Contains("CASE WHEN q.d IS NULL THEN 0 ELSE q.c END AS unique_queries", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("COALESCE(q.c, 0)", sql, StringComparison.Ordinal);
        Assert.Contains("(CASE WHEN q.c IS NULL THEN 0 ELSE 1 END)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("(CASE WHEN q.d IS NULL THEN 0 ELSE 1 END)", sql, StringComparison.Ordinal);

        /* Raw has no not-carried member: raw is the source itself, and a NULL count is impossible there. */
        Assert.DoesNotContain("NULL::bigint AS c", sql, StringComparison.Ordinal);
        Assert.Equal(sql, DailySummarySql.RangeSqlFor(RetentionTier.Raw));
    }

    /// <summary>
    /// Every routed form carries the not-carried member with the hole scan's two probes, per server, at day
    /// grain, against the relation and the SOURCE the repair's target list names for it — including the
    /// aggregate's own WHERE on the source probe, which is what keeps a restart-only day from reading as a hole
    /// on the interval-honest successor.
    /// </summary>
    [Fact]
    public void EveryRoutedForm_AsksTheHoleScansTwoProbes_PerServer_AtDayGrain()
    {
        foreach (var (tier, relation) in RoutedForms)
        {
            var sql = DailySummarySql.RangeSqlFor(tier, relation);
            var target = TimescaleSupport.MaterializationHoleTargets.Single(t => t.View == relation);
            var filter = TimescaleSupport.MaterializationHoleSourceFilterFor(target.CreateSql);

            Assert.Contains("SELECT b.d, NULL::bigint AS c", sql, StringComparison.Ordinal);
            Assert.Contains("FROM generate_series(date_trunc('day', $2::timestamp), date_trunc('day', $3::timestamp), INTERVAL '1 day') AS b(d)", sql, StringComparison.Ordinal);
            Assert.Contains("WHERE b.d < $3", sql, StringComparison.Ordinal);
            Assert.Contains("AND b.d < COALESCE((SELECT last_day + INTERVAL '1 day' FROM queries_ceiling), $2)", sql, StringComparison.Ordinal);

            /* Probe one: no rollup row for THIS server that day. */
            var lf = sql.Replace("\r\n", "\n", StringComparison.Ordinal);
            Assert.Contains(
                $"NOT EXISTS (\n        SELECT 1 FROM collect.{relation} AS r\n        WHERE r.server_id = $1 AND r.bucket >= b.d AND r.bucket < b.d + INTERVAL '1 day')",
                lf, StringComparison.Ordinal);

            /* Probe two: an admitted source row for THIS server that day — the source and its time column are
               the repair's, and the filter is the aggregate's own WHERE, verbatim. */
            var sourceProbe = $"SELECT 1 FROM collect.{target.Source} AS s\n        WHERE s.server_id = $1 AND s.{target.SourceTimeColumn} >= b.d AND s.{target.SourceTimeColumn} < b.d + INTERVAL '1 day'"
                + (filter.Length == 0 ? ")" : "\n          AND " + filter + ")");
            Assert.Contains(sourceProbe, lf, StringComparison.Ordinal);

            /* The three members, in order: rollup half, raw tail, not-carried. */
            Assert.Equal(2, sql.Split("UNION ALL").Length - 1);
            var rollupAt = sql.IndexOf("SELECT date_trunc('day', bucket) AS d, COUNT(DISTINCT query_hash) AS c", StringComparison.Ordinal);
            var rawAt = sql.IndexOf("FROM v_query_stats", StringComparison.Ordinal);
            var notCarriedAt = sql.IndexOf("SELECT b.d, NULL::bigint AS c", StringComparison.Ordinal);
            Assert.True(rollupAt > 0 && rawAt > rollupAt && notCarriedAt > rawAt);
        }

        /* The daily's source is the legacy hourly it is hierarchical from (no filter); the successor's is raw
           with its restart predicate; the legacy hourly's is raw with none. Stated as the values, so a change to
           the registry moves this test rather than silently moving the probe. */
        Assert.Equal((TimescaleSupport.QueryStatsHourlyView, "bucket", string.Empty), SourceOf(TimescaleSupport.QueryStatsDailyView));
        Assert.Equal(("query_stats", "collection_time", string.Empty), SourceOf(TimescaleSupport.QueryStatsHourlyView));
        Assert.Equal(("query_stats", "collection_time", "sample_interval_seconds IS DISTINCT FROM 0"), SourceOf(TimescaleSupport.QueryStatsIntervalHourlyView));
    }

    private static (string Source, string TimeColumn, string Filter) SourceOf(string view)
    {
        var target = TimescaleSupport.MaterializationHoleTargets.Single(t => t.View == view);
        return (target.Source, target.SourceTimeColumn, TimescaleSupport.MaterializationHoleSourceFilterFor(target.CreateSql));
    }

    /// <summary>
    /// A rollup bucket holding zero distinct hashes cannot exist: every query rollup GROUPs BY <c>query_hash</c>,
    /// so a bucket exists only where a source row did and carries at least one hash. That is what lets the
    /// not-carried row be told apart by <c>c IS NULL</c> alone — "carried with a count of 0" is not a state the
    /// rollup half can produce, so NULL needs no flag column beside it. Pinned on the CREATE texts, because the
    /// moment a query rollup stopped grouping by hash this reasoning would be false and the NULL ambiguous.
    /// </summary>
    [Fact]
    public void ABucketWithZeroDistinctHashes_CannotExist_SoNullNeedsNoFlag()
    {
        foreach (var create in new[]
        {
            TimescaleSupport.CreateQueryStatsHourlySql,
            TimescaleSupport.CreateQueryStatsIntervalHourlySql,
            TimescaleSupport.CreateQueryStatsDailySql,
        })
        {
            Assert.Contains("GROUP BY server_id, server_name, database_name, query_hash", create, StringComparison.Ordinal);
        }

        /* And the rollup half counts that hash, so a rollup row's count is >= 1 by construction. */
        Assert.Contains("SELECT date_trunc('day', bucket) AS d, COUNT(DISTINCT query_hash) AS c", DailySummarySql.RangeSqlFor(RetentionTier.Daily), StringComparison.Ordinal);
    }

    /// <summary>The routed form refuses a relation the registry does not know before it reaches the store.</summary>
    [Fact]
    public void RangeSqlFor_RefusesAnUnregisteredRelation()
    {
        var ex = Assert.Throws<ArgumentException>(() => DailySummarySql.RangeSqlFor(RetentionTier.Hourly, "some_future_rollup"));
        Assert.Contains("MaterializationHoleTargets", ex.Message, StringComparison.Ordinal);
        Assert.Throws<ArgumentException>(() => DailySummarySql.RangeSqlFor(RetentionTier.Hourly, " "));
    }

    /// <summary>
    /// The MCP reader's row keeps the NULL and the range result derives <c>days_missing</c> from the rows — so the
    /// list and the NULLs cannot disagree — and the band does NOT move: it never read the count
    /// (<see cref="DailyHealthSignals"/> has no member for it), so a not-carried day bands on its other signals
    /// exactly as the same day with a measured count would. "Not carried" is not "quiet" and it is not
    /// "No Data" either; it is one column's disclosure.
    /// </summary>
    [Fact]
    public void TheReaderRow_KeepsTheNull_DerivesDaysMissing_AndTheBandDoesNotReadTheCount()
    {
        var day = new DateTime(2026, 9, 10, 0, 0, 0, DateTimeKind.Unspecified);
        var carried = new DarlingHealthReader.DailySummaryReadRow(day, 12m, "CXPACKET", 87, 0, 0, 0, 0, 0, 0, 0, 0, HasData: true) { CollectionRuns = 288 };
        var notCarried = carried with { UniqueQueries = null };
        var quiet = carried with { UniqueQueries = 0 };

        Assert.Null(notCarried.UniqueQueries);
        Assert.Equal(0L, quiet.UniqueQueries);
        Assert.Equal(carried.HealthBand, notCarried.HealthBand);
        Assert.Equal(DailyHealthBand.Healthy, notCarried.HealthBand);
        Assert.True(notCarried.ToSignals().HasData);
        Assert.DoesNotContain(typeof(DailyHealthSignals).GetProperties(), p => p.Name.Contains("Quer", StringComparison.Ordinal));

        var range = new DarlingHealthReader.DailySummaryRangeReadResult(
            new List<DarlingHealthReader.DailySummaryReadRow> { carried, notCarried with { SummaryDate = day.AddDays(1) }, quiet with { SummaryDate = day.AddDays(2) } },
            day.AddDays(-30), 30);
        Assert.Equal(new[] { day.AddDays(1) }, range.DaysMissing);

        var none = new DarlingHealthReader.DailySummaryRangeReadResult(new List<DarlingHealthReader.DailySummaryReadRow> { carried, quiet }, day.AddDays(-30), 30);
        Assert.Empty(none.DaysMissing);
    }

    /// <summary>
    /// Both readers keep the NULL at ordinal 3 (the MCP reader and the viewer's calendar make the identical read
    /// off the identical statement, #1661's whole point), both MCP tools emit the one key name, the web calendar
    /// renders the null in words and the list in one line, and the desktop key-metrics line does not print 0.
    /// Source pins, because the viewer and the page cannot be executed here.
    /// </summary>
    [Fact]
    public void EveryConsumer_KeepsTheNull_AndSpellsTheDisclosureOneWay()
    {
        var mcpReader = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingHealthReader.cs");
        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.DailySummary.cs");
        const string KeepTheNull = "reader.IsDBNull(3) ? null : Convert.ToInt64(reader.GetValue(3))";
        Assert.Contains(KeepTheNull, mcpReader, StringComparison.Ordinal);
        Assert.Contains(KeepTheNull, viewer, StringComparison.Ordinal);
        Assert.DoesNotContain("reader.IsDBNull(3) ? 0L", mcpReader, StringComparison.Ordinal);
        Assert.DoesNotContain("reader.IsDBNull(3) ? 0L", viewer, StringComparison.Ordinal);
        Assert.Contains("public long? UniqueQueries { get; set; }", viewer, StringComparison.Ordinal);

        var tools = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpHealthTools.cs");
        Assert.Equal(2, tools.Split("days_missing = ").Length - 1);
        Assert.Contains("days_missing = range.DaysMissing.Select(day => day.ToString(\"yyyy-MM-dd\"))", tools, StringComparison.Ordinal);
        Assert.Contains("days_missing = row.UniqueQueries is null ? new[] { row.SummaryDate.ToString(\"yyyy-MM-dd\") } : Array.Empty<string>()", tools, StringComparison.Ordinal);

        var page = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");
        Assert.Contains("export function dailyCalendarPanel(server)", page, StringComparison.Ordinal);
        Assert.Contains("res.data.days_missing", page, StringComparison.Ordinal);
        Assert.Contains("row.unique_queries == null", page, StringComparison.Ordinal);
        Assert.Contains("[\"not materialized\"]", page, StringComparison.Ordinal);
        Assert.Contains("dailyCalendarPanel(server),", page, StringComparison.Ordinal);
        /* The descriptor it replaced is gone: one fetch of the one read, as before. */
        Assert.DoesNotContain("\"Daily Health Calendar\",\n        \"get_daily_summary_range\"", page.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);

        /* The shared model and the shared line: Lite hands a long, Darling may hand a null. */
        var calendarDay = RepoFile.ReadRepoFile("PerformanceMonitor.Ui", "PerformanceCalendarDay.cs");
        Assert.Contains("public long? UniqueQueries { get; init; }", calendarDay, StringComparison.Ordinal);
        var lite = RepoFile.ReadRepoFile("Lite", "Services", "LocalDataService.DailySummary.cs");
        Assert.Contains("public long UniqueQueries { get; set; }", lite, StringComparison.Ordinal);
        Assert.Contains("UniqueQueries = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3)),", lite, StringComparison.Ordinal);
    }
}

/// <summary>
/// The skipped day, planted for real on TimescaleDB (#3653 A6): a rollup with one day skipped BELOW its ceiling
/// reads NULL on the routed calendar and appears in <c>days_missing</c>; a day the server genuinely had no rows
/// for is absent, not NULL (the raw table was simply empty — no hole is claimed); a day past the ceiling reads
/// from raw as before; the tier whose rollup carried every day reads no NULL at all; a day holding only a
/// restart row is a hole for the legacy hourly (which admits the row) and not for the interval-honest successor
/// (whose WHERE the probe carries). Then the one thing the disclosure exists to point at:
/// <see cref="TimescaleSupport.RepairMaterializationHolesAsync"/> closes exactly that day, and the same read
/// prints its count.
///
/// <para><b>Two servers, two clusters of days, one store.</b> The RECENT server's days sit ten to six days back:
/// inside the hourly source's 90-day scan horizon, so the repair reaches them, and below every refresh policy's
/// window (the daily's three days, the hourly's one), so a background policy run landing mid-test cannot
/// materialize a day this test asserts is skipped. The OLD server's days sit a hundred days back: past
/// <see cref="RetentionTierRouter.HourlyMaxAge"/>, so a window over them routes to the DAILY tier by AGE through
/// the reader, the router, the MCP tool and the wire — the production path a calendar month takes — and past the
/// repair's scan horizon, which is the point: the disclosure does not depend on the repair reaching the day, only
/// on the source still holding it.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DailySummaryNotCarriedLiveTests
{
    private const int RecentServerId = -936538;
    private const string RecentServerName = "not-carried-recent";
    private const int OldServerId = -936539;
    private const string OldServerName = "not-carried-old";

    [Fact]
    public async Task ASkippedDayBelowTheCeiling_ReadsNullAndIsNamed_AnEmptyDayIsAbsent_AndTheRepairClosesIt_AgainstDevPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live not-carried calendar test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled,
            "The live not-carried calendar test needs TimescaleDB: a rollup tier exists only as a continuous aggregate.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        await RegisterServerAsync(connection, RecentServerId, RecentServerName, ct);
        await RegisterServerAsync(connection, OldServerId, OldServerName, ct);

        var hourly = TimescaleSupport.QueryStatsHourlyView;
        var successor = TimescaleSupport.QueryStatsIntervalHourlyView;
        var daily = TimescaleSupport.QueryStatsDailyView;

        /* One layout, planted twice. Five whole UTC days, D0 the oldest, window [D0, D5). D0: 3 hashes. D1:
           NOTHING — the control, a day the server genuinely had no rows for. D2: 5 hashes, the day the daily
           rollup will SKIP. D3: 7 hashes. D4: 2 hashes, raw only — past the rollup's ceiling, the #3698 tail.
           The hourly is refreshed over D0..D3; the daily over D0 and over D3 and never over D2. */
        var recent0 = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-10), DateTimeKind.Unspecified);
        var old0 = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-100), DateTimeKind.Unspecified);
        Assert.True(DateTime.UtcNow - old0 > RetentionTierRouter.HourlyMaxAge, "the old cluster must route to the daily tier by age");
        Assert.True(DateTime.UtcNow - recent0.AddDays(5) > TimescaleSupport.DailyRefreshStartSpan, "the recent cluster must sit below the daily policy's window");

        foreach (var (serverId, serverName, d0) in new[] { (RecentServerId, RecentServerName, recent0), (OldServerId, OldServerName, old0) })
        {
            await PlantDayAsync(connection, serverId, serverName, d0, hashes: 3, ct);
            await PlantDayAsync(connection, serverId, serverName, d0.AddDays(2), hashes: 5, ct);
            await PlantDayAsync(connection, serverId, serverName, d0.AddDays(3), hashes: 7, ct);
            await PlantDayAsync(connection, serverId, serverName, d0.AddDays(4), hashes: 2, ct);
            await RefreshAsync(connection, hourly, d0, d0.AddDays(4), ct);
            await RefreshAsync(connection, daily, d0, d0.AddDays(1), ct);
            await RefreshAsync(connection, daily, d0.AddDays(3), d0.AddDays(4), ct);
            Assert.Equal(new[] { d0, d0.AddDays(3) }, await BucketDaysAsync(connection, daily, serverId, ct));
            Assert.Equal(new[] { d0, d0.AddDays(2), d0.AddDays(3) }, await BucketDaysAsync(connection, hourly, serverId, ct));
        }

        DateTime R(int n) => recent0.AddDays(n);
        DateTime O(int n) => old0.AddDays(n);

        /* THE DEFECT'S DAY, READ AT THE DAILY TIER: D2 is a row (its source, the hourly, proves it was collected)
           with a NULL count; D1 is no row at all; D0 and D3 carry their counts from the rollup; D4 reads from raw. */
        var dailyTier = await ReadCalendarAsync(connection, DailySummarySql.RangeSqlFor(RetentionTier.Daily), RecentServerId, R(0), R(5), ct);
        Assert.Equal(new[] { R(0), R(2), R(3), R(4) }, dailyTier.Select(r => r.Day).ToArray());
        Assert.Equal(new long?[] { 3L, null, 7L, 2L }, dailyTier.Select(r => r.UniqueQueries).ToArray());
        /* The not-carried day is not a source that holds the day: presence excludes it. Nothing else is planted,
           so a carried day reads exactly one source (its queries row) and the skipped day reads none. */
        Assert.Equal(new[] { 1, 0, 1, 1 }, dailyTier.Select(r => r.SignalSourcesPresent).ToArray());

        /* THE HOURLY TIER CARRIED EVERY DAY: no NULL anywhere, D4 from raw, D1 absent. */
        var hourlyTier = await ReadCalendarAsync(connection, DailySummarySql.RangeSqlFor(RetentionTier.Hourly, hourly), RecentServerId, R(0), R(5), ct);
        Assert.Equal(new[] { R(0), R(2), R(3), R(4) }, hourlyTier.Select(r => r.Day).ToArray());
        Assert.Equal(new long?[] { 3L, 5L, 7L, 2L }, hourlyTier.Select(r => r.UniqueQueries).ToArray());

        /* THE CONTROL ON THE SOURCE FILTER: D1 gets ONE restart row (interval 0). Neither hourly is refreshed over
           it; the successor is refreshed over D0 and D3 so D1 and D2 sit below its ceiling too. For the legacy
           hourly, which admits the row, D1 is now a hole and reads NULL; for the successor, whose WHERE the probe
           carries, D1 holds no admitted row and stays absent — while D2, which it never materialized, is NULL. */
        await PlantRestartRowAsync(connection, RecentServerId, RecentServerName, R(1).AddHours(6), ct);
        await RefreshAsync(connection, successor, R(0), R(1), ct);
        await RefreshAsync(connection, successor, R(3), R(4), ct);
        var legacyTier = await ReadCalendarAsync(connection, DailySummarySql.RangeSqlFor(RetentionTier.Hourly, hourly), RecentServerId, R(0), R(5), ct);
        Assert.Equal(new[] { R(0), R(1), R(2), R(3), R(4) }, legacyTier.Select(r => r.Day).ToArray());
        Assert.Null(legacyTier[1].UniqueQueries);
        var successorTier = await ReadCalendarAsync(connection, DailySummarySql.RangeSqlFor(RetentionTier.Hourly, successor), RecentServerId, R(0), R(5), ct);
        Assert.Equal(new[] { R(0), R(2), R(3), R(4) }, successorTier.Select(r => r.Day).ToArray());
        Assert.Equal(new long?[] { 3L, null, 7L, 2L }, successorTier.Select(r => r.UniqueQueries).ToArray());

        /* THROUGH THE READER, THE ROUTER AND THE TOOL, on the OLD server: the window starts a hundred days back,
           past HourlyMaxAge, and the daily's floor covers it, so the reader routes DAILY; its rows carry the null,
           its DaysMissing names D2, and the wire spells both — unique_queries: null and days_missing[]. */
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var range = await DarlingHealthReader.GetDailySummaryRangeAsync(postgres, OldServerId, O(0), O(5), cancellationToken: ct);
        Assert.Equal(new[] { O(0), O(2), O(3), O(4) }, range.Rows.Select(r => r.SummaryDate).ToArray());
        Assert.Equal(new long?[] { 3L, null, 7L, 2L }, range.Rows.Select(r => r.UniqueQueries).ToArray());
        Assert.Equal(new[] { O(2) }, range.DaysMissing);

        var wire = await DarlingMcpHealthTools.GetDailySummaryRange(postgres, OldServerName, days_back: 5, as_of: O(4).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
        using (var doc = JsonDocument.Parse(wire))
        {
            var root = doc.RootElement;
            Assert.False(root.TryGetProperty("status", out _), wire);
            Assert.Equal(new[] { O(2).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) }, root.GetProperty("days_missing").EnumerateArray().Select(e => e.GetString()).ToArray());
            var days = root.GetProperty("days").EnumerateArray().ToArray();
            Assert.Equal(4, days.Length);
            Assert.Equal(O(2).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), days[1].GetProperty("summary_date").GetString());
            Assert.Equal(JsonValueKind.Null, days[1].GetProperty("unique_queries").ValueKind);
            Assert.Equal(3, days[0].GetProperty("unique_queries").GetInt32());
            Assert.Equal(2, days[3].GetProperty("unique_queries").GetInt32());
        }

        /* And the recent server's window through the same reader: ten days back routes HOURLY by age (inside
           HourlyMaxAge, past RawMaxAge), and the supply rule keeps the LEGACY — the successor's oldest bucket is
           the 01:00 collection, an hour past the window's midnight start, so it does not reach as far as the
           legacy does and "loses nothing" fails by that hour (PrefersSuccessor's own bar; asserted through the
           probe the reader uses, not assumed). So the reader carries the legacy's answer: the restart-only D1 is
           the legacy's hole and reads NULL, D2 is carried, and days_missing names D1 — the hole scan's rule
           surfacing on the wire for a day whose only row is one the successor would have refused. */
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(postgres, await TimescaleSupport.DetectRollupsAsync(postgres, ct), ct);
        Assert.Equal(hourly, coverage.HourlyRelationFor(hourly, R(0)));
        var recentRange = await DarlingHealthReader.GetDailySummaryRangeAsync(postgres, RecentServerId, R(0), R(5), cancellationToken: ct);
        Assert.Equal(new[] { R(0), R(1), R(2), R(3), R(4) }, recentRange.Rows.Select(r => r.SummaryDate).ToArray());
        Assert.Equal(new long?[] { 3L, null, 5L, 7L, 2L }, recentRange.Rows.Select(r => r.UniqueQueries).ToArray());
        Assert.Equal(new[] { R(1) }, recentRange.DaysMissing);
        var recentWire = await DarlingMcpHealthTools.GetDailySummaryRange(postgres, RecentServerName, days_back: 5, as_of: R(4).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
        using (var doc = JsonDocument.Parse(recentWire))
        {
            Assert.Equal(new[] { R(1).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) }, doc.RootElement.GetProperty("days_missing").EnumerateArray().Select(e => e.GetString()).ToArray());
        }

        var single = await DarlingMcpHealthTools.GetDailySummary(postgres, OldServerName, O(2).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
        using (var doc = JsonDocument.Parse(single))
        {
            /* A hundred days back is before the store's retention horizon and no other signal holds the day, so
               the single-day tool answers its purged envelope — the horizon's own vocabulary, which wins — rather
               than a band. What this leg pins is that the routed read behind it produced the row and did not
               throw on the NULL. */
            var root = doc.RootElement;
            Assert.True(root.TryGetProperty("status", out var status) && status.GetString() == "unavailable", single);
            Assert.Equal("purged", root.GetProperty("hints").GetProperty("data_state").GetString());
        }

        /* THE REPAIR: the start-up pass scans the daily from its floor (the recent D0 is inside the hourly source's
           90-day horizon) and finds exactly the recent D2 — its source, the hourly, holds the day and the daily
           never materialized it — and closes it. The old cluster is past the scan horizon and stands (the
           disclosure above did not depend on the repair). The same calendar read then prints 5 where it printed
           NULL, D2 counts as a source again, and the recent server's DaysMissing at the daily tier is empty. */
        var log = new CapturingTestLogger();
        var summary = await TimescaleSupport.RepairMaterializationHolesAsync(connection, log, DateTime.UtcNow, ct);
        Assert.Equal(0, summary.Failures);
        Assert.Contains($"{daily} had 1 bucket(s) in [{R(2):O}, {R(3):O})", log.Joined, StringComparison.Ordinal);

        var repaired = await ReadCalendarAsync(connection, DailySummarySql.RangeSqlFor(RetentionTier.Daily), RecentServerId, R(0), R(5), ct);
        Assert.Equal(new[] { R(0), R(2), R(3), R(4) }, repaired.Select(r => r.Day).ToArray());
        Assert.Equal(new long?[] { 3L, 5L, 7L, 2L }, repaired.Select(r => r.UniqueQueries).ToArray());
        Assert.Equal(new[] { 1, 1, 1, 1 }, repaired.Select(r => r.SignalSourcesPresent).ToArray());

        var oldAfter = await DarlingHealthReader.GetDailySummaryRangeAsync(postgres, OldServerId, O(0), O(5), cancellationToken: ct);
        Assert.Equal(new[] { O(2) }, oldAfter.DaysMissing);
    }

    private sealed record CalendarRow(DateTime Day, long? UniqueQueries, int SignalSourcesPresent);

    private static async Task<List<CalendarRow>> ReadCalendarAsync(NpgsqlConnection connection, string sql, int serverId, DateTime from, DateTime to, CancellationToken ct)
    {
        var rows = new List<CalendarRow>();
        await using var read = new NpgsqlCommand(sql, connection);
        read.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = from });
        read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = to });
        await using var reader = await read.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new CalendarRow(
                reader.GetDateTime(0),
                reader.IsDBNull(3) ? null : Convert.ToInt64(reader.GetValue(3), CultureInfo.InvariantCulture),
                Convert.ToInt32(reader.GetValue(13), CultureInfo.InvariantCulture)));
        }

        return rows;
    }

    private static async Task PlantDayAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime day, int hashes, CancellationToken ct)
    {
        /* Three collections spread over the day, every hash in each, so the day has hourly buckets the daily can
           roll up and a raw tail the raw half can count — the same COUNT(DISTINCT query_hash) on every tier. */
        foreach (var hour in new[] { 1, 9, 17 })
        {
            for (var hash = 0; hash < hashes; hash++)
            {
                await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'CalendarDb', $5, $6, 1000, 1000, 10, 1200)", connection);
                insert.Parameters.AddWithValue((long)(Math.Abs(serverId) * 1_000_000L + day.DayOfYear * 10_000 + hour * 100 + hash));
                insert.Parameters.AddWithValue(day.AddHours(hour));
                insert.Parameters.AddWithValue(serverId);
                insert.Parameters.AddWithValue(serverName);
                insert.Parameters.AddWithValue($"0xHASH{hash:D4}");
                insert.Parameters.AddWithValue($"0xHANDLE{hash:D4}");
                await insert.ExecuteNonQueryAsync(ct);
            }
        }
    }

    private static async Task PlantRestartRowAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES (777777, $1, $2, $3, 'CalendarDb', '0xRESTART', '0xRESTARTHANDLE', 0, 0, 0, 0)", connection);
        insert.Parameters.AddWithValue(at);
        insert.Parameters.AddWithValue(serverId);
        insert.Parameters.AddWithValue(serverName);
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        /* The plain, unforced form a policy runs — the CALL cannot be inside a transaction. */
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }

    private static async Task<DateTime[]> BucketDaysAsync(NpgsqlConnection connection, string view, int serverId, CancellationToken ct)
    {
        var days = new List<DateTime>();
        await using var read = new NpgsqlCommand($"SELECT DISTINCT date_trunc('day', bucket) FROM collect.{view} WHERE server_id = $1 ORDER BY 1", connection);
        read.Parameters.AddWithValue(serverId);
        await using var reader = await read.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            days.Add(reader.GetDateTime(0));
        }

        return days.ToArray();
    }

    private static async Task RegisterServerAsync(NpgsqlConnection connection, int serverId, string serverName, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, 15, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_major_version = 15;", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }
}
