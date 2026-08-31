/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>Which physical relation a composed panel reads: the raw collector table, or one of its
/// continuous-aggregate rollups.</summary>
public enum ComposeSourceTier
{
    /// <summary>The raw per-sweep collector table (<c>collect.&lt;table&gt;</c>).</summary>
    Raw,

    /// <summary>The hourly continuous aggregate (<c>collect.&lt;table&gt;_hourly</c>).</summary>
    Hourly,

    /// <summary>The daily continuous aggregate (<c>collect.&lt;table&gt;_daily</c>).</summary>
    Daily,
}

/// <summary>
/// The routing decision for one panel: which tier to read and the relation/time-column that implies. The raw tier
/// carries no relation (the compiler keeps its existing <c>SourceTable</c> + prefix-time-column path); a CAGG tier
/// names the rollup view, whose time column is always the <c>bucket</c> the CAGG produced.
/// </summary>
public sealed record ComposeRoute(ComposeSourceTier Tier, string? CaggRelation)
{
    /// <summary>The raw route — the compiler's unchanged behaviour.</summary>
    public static readonly ComposeRoute Raw = new(ComposeSourceTier.Raw, null);

    public bool IsCagg => Tier != ComposeSourceTier.Raw;

    /// <summary>Every CAGG's time dimension is the <c>time_bucket(...) AS bucket</c> column.</summary>
    public const string CaggTimeColumn = "bucket";
}

/// <summary>One raw table's continuous-aggregate coverage: its hourly (and optional daily) rollup view names and
/// the dimensions those rollups are grouped by — the set a panel's group-by/filter dimensions must be a subset of
/// to route (the universal <c>server</c> dimension always qualifies, since server_id/server_name lead every CAGG's
/// GROUP BY).
///
/// <para><b><c>LegacyHourlyView</c>/<c>LegacyDailyView</c> are the SUPERSEDED pair, and only query_store_stats has
/// one (#1849).</b> Its original rollups materialized <c>sum(execution_count)</c> from un-deduped raw rows, so they
/// bake in the cumulative-snapshot double-count — measured at up to 496x for a single Query Store interval. The
/// corrected rollups above replace them, but the old pair CANNOT be dropped and rebuilt: a continuous aggregate's
/// columns cannot be ALTERed, and with retention active a rebuild would re-materialize from 4 days of raw and
/// permanently destroy the retained hourly and indefinite daily history (#1759/#1793). So the corrected pair starts
/// empty and deepens from deploy, the old pair keeps everything it already holds, and reads prefer the corrected
/// one wherever it reaches. <b>Past that boundary a window is still served INFLATED numbers</b> — a visible step
/// where the two meet, unavoidable while the old history is the only history, and shrinking every day as the
/// corrected rollups deepen and the old rows age out.</para>
///
/// <para><b><c>DayGrainDailyView</c> is a BETTER daily, not a deeper one (#1869), and only query_store_stats
/// has one.</b> <c>DailyView</c> dedups each Query Store interval within a collection HOUR, so an interval
/// straddling an hour boundary is counted once per hour it was collected in — measured at 1.97x. The
/// day-grain daily dedups across the whole DAY and counts it once. It is a NEW aggregate that starts empty,
/// so it wins only where it has actually materialized the window; older windows keep falling to
/// <c>DailyView</c>, and older ones still to <c>LegacyDailyView</c>. Three dailies, best-first, each step the
/// same comparative rule.</para></summary>
public sealed record ComposeCaggInfo(
    string RawTable,
    string HourlyView,
    string? DailyView,
    IReadOnlySet<string> Dimensions,
    string? LegacyHourlyView = null,
    string? LegacyDailyView = null,
    string? DayGrainDailyView = null)
{
    public bool Covers(string dimensionName) =>
        string.Equals(dimensionName, MeasureCatalog.ServerDimensionName, StringComparison.Ordinal)
        || Dimensions.Contains(dimensionName);

    /// <summary>Is there a superseded pair to fall back to for windows the primary pair has not materialized?</summary>
    public bool HasLegacyPair => LegacyHourlyView is not null;
}

/// <summary>
/// The catalog of which raw tables have CAGGs, keyed by raw table name — the composer-dimension shape after the
/// CAGG reshape (query_store_stats regrouped to module_name/query_hash; procedure_stats carrying schema_name).
/// Dimensions here are the CAGGs' GROUP BY columns (minus the universal server prefix); a panel routes only when
/// its dimensions are covered (see <see cref="ComposeCaggInfo.Covers"/>). query_stats' <c>object_name</c> is
/// covered via the module_map join (the CAGG carries sql_handle; the compiler joins collect.module_map for it) —
/// it stays a ViaModuleJoin dimension so the RAW path still uses the live #1568 procedure_stats CTE.
/// All three rolled tables (query_stats / procedure_stats / query_store_stats) now carry both an hourly and a
/// daily CAGG, so every one tiers raw → hourly → daily.
/// </summary>
public static class ComposeCaggCatalog
{
    private static readonly Dictionary<string, ComposeCaggInfo> s_byTable = new(StringComparer.Ordinal)
    {
        /* object_name is coverable via the module_map join (the query_stats CAGG carries sql_handle) — the
           compiler joins collect.module_map for it on a CAGG route. It stays a ViaModuleJoin dimension, so the raw
           path still uses the live #1568 procedure_stats CTE. statement (#2737) rides the same join plus the
           CAGG's own query_hash fallback column, so it is coverable for exactly the same reason. */
        ["query_stats"] = new(
            "query_stats", "query_stats_hourly", "query_stats_daily",
            new HashSet<string>(StringComparer.Ordinal) { "database_name", "query_hash", "object_name", "statement" }),

        ["procedure_stats"] = new(
            "procedure_stats", "procedure_stats_hourly", "procedure_stats_daily",
            new HashSet<string>(StringComparer.Ordinal) { "database_name", "schema_name", "object_name" }),

        /* The CORRECTED pair is primary (#1849); the original inflated pair is the documented fallback for
           windows the corrected rollups have not materialized. Same dimensions and the same column NAMES on
           both, so ComposeCaggValueMapper and the compiled SQL are identical either way — only which relation
           is named changes. */
        ["query_store_stats"] = new(
            "query_store_stats", "query_store_stats_corrected_hourly", "query_store_stats_corrected_daily",
            new HashSet<string>(StringComparer.Ordinal) { "database_name", "module_name", "query_hash" },
            LegacyHourlyView: "query_store_stats_hourly",
            LegacyDailyView: "query_store_stats_daily",
            DayGrainDailyView: "query_store_stats_daygrain_daily"),
    };

    /// <summary>The CAGG coverage for <paramref name="rawTable"/>, or null if it has no continuous aggregate.</summary>
    public static ComposeCaggInfo? For(string rawTable) =>
        s_byTable.TryGetValue(rawTable, out var info) ? info : null;
}

/// <summary>
/// Chooses the physical source tier for a composed panel by the AGE of the window's oldest point, never by the
/// display grain (an explicit bucket, or the Ranked/Scalar modes that resolve no grain at all, would otherwise
/// route wrong). Retention drops chunks by ACTUAL now, so the oldest point's age is measured from a caller-supplied
/// <c>nowUtc</c> (deterministic for tests), not from the window's end — a purely historical window ("30 to 25 days
/// ago") must reach the tier that still retains it or the query returns empty rows.
///
/// <para>Route thresholds sit a margin BELOW each retention horizon (raw kept 4d → route ≤3d; hourly kept 90d →
/// route ≤89d), so a drop lagging the boundary (1-day chunk granularity + the 3-day CAGG refresh) can never leave
/// the chosen tier missing the oldest chunk. The margin is pinned as a test invariant against the retention
/// constants. A whole window routes to the single tier its OLDEST point needs — uniform coarsening, no cross-tier
/// union (a future optimization); real-time aggregation on the CAGG stitches the still-filling recent edge.</para>
/// </summary>
public static class ComposeSourceRouter
{
    /// <summary>Raw is chosen only for windows whose oldest point is within this age — a day inside the 4-day raw
    /// retention, so raw never routes to an about-to-drop chunk. Aliases the shared definition (#1661); the
    /// viewer's built-in tabs route off the same value.</summary>
    public static readonly TimeSpan RawRouteMaxAge = RetentionTierRouter.RawMaxAge;

    /// <summary>The hourly CAGG is chosen up to this age — a day inside the 90-day (#1937) hourly retention; older windows
    /// fall to the daily CAGG (or stay on the hourly, capped, when no daily CAGG exists yet). Aliases the shared
    /// definition (#1661).</summary>
    public static readonly TimeSpan HourlyRouteMaxAge = RetentionTierRouter.HourlyMaxAge;

    /// <summary>
    /// The tier for <paramref name="plan"/> given the window's oldest point (<paramref name="windowStartUtc"/>)
    /// relative to <paramref name="nowUtc"/>, gated by which rollups the store actually HAS
    /// (<paramref name="rollups"/>, #1665 — the composer's arm of the #1664 availability guard: the CAGGs are
    /// runtime TimescaleDB setup, so a plain-PostgreSQL store has none and routing there by age alone compiled
    /// SQL against missing relations). Falls through to <see cref="ComposeRoute.Raw"/> — the compiler's
    /// unchanged path — whenever the table has no CAGG, a grouped/filtered dimension is outside the CAGG's coverage
    /// (object_name IS covered on query_stats, via the module_map join), the window is recent enough that raw
    /// still holds it, or the age-resolved tier's view is absent (per-table degrade: daily missing → hourly,
    /// hourly missing → raw — the same shared ladder the viewer's built-in tabs use).
    ///
    /// <para><paramref name="coverage"/> adds the #1759 gate: a rollup that EXISTS may still not have
    /// materialized the window (created <c>WITH NO DATA</c> over pre-existing history), and the router drops
    /// to a tier measured to reach further back rather than serving an empty result off a rollup while raw
    /// still holds the rows. Pass <see cref="RollupCoverage.Unknown"/> to keep the pure age + availability
    /// decision.</para>
    /// </summary>
    public static ComposeRoute Resolve(
        PanelPlan plan, DateTime nowUtc, DateTime windowStartUtc, RollupAvailability rollups, RollupCoverage coverage)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(coverage);

        var cagg = ComposeCaggCatalog.For(plan.Measure.SourceTable);
        if (cagg is null)
        {
            return ComposeRoute.Raw;
        }

        /* No blanket module-join gate: object_name (the only ViaModuleJoin dimension) is now coverable on the
           query_stats CAGG via the module_map join (the CAGG carries sql_handle), and is in that CAGG's coverage
           set below. The dimension-coverage loop is the sole gate — a dimension no CAGG can serve (directly or via
           the map) still falls to raw. */

        /* Every grouped/filtered dimension must be served by the CAGG (a GROUP BY column, or object_name via the
           module_map join), else the rollup can't reproduce it. */
        foreach (var dimension in plan.GroupBy)
        {
            if (!cagg.Covers(dimension.Name))
            {
                return ComposeRoute.Raw;
            }
        }

        foreach (var filter in plan.Filters)
        {
            if (!cagg.Covers(filter.Dimension.Name))
            {
                return ComposeRoute.Raw;
            }
        }

        /* The age decision AND the availability degrade are shared with the viewer's built-in tabs
           (#1661/#1664) — one ladder in RetentionTierRouter, so the two readers can never drift into
           disagreeing about which tier still retains (or exists for) a window. The coverage gates above stay
           composer-specific; the flags are per-TABLE (#1665), because a failure-isolated ensure sweep can
           build one table's pair and not another's. A catalog entry with no daily view feeds
           dailyAvailable=false through the same ladder — identical to the old null-DailyView fallback. */
        var primary = PreferDayGrainDaily(
            cagg,
            ResolveFamily(cagg.HourlyView, cagg.DailyView, nowUtc, windowStartUtc, rollups, coverage),
            windowStartUtc, rollups, coverage);

        if (!cagg.HasLegacyPair)
        {
            return primary.Route;
        }

        /* ── the corrected/legacy boundary (#1849) ────────────────────────────────────────────────────────
           The corrected Query Store rollups are NEW objects that start empty and deepen from deploy, beside
           an old pair that already holds up to the full hourly horizon (90d) and unbounded daily of INFLATED history.
           The rule is: use the corrected pair wherever it has actually materialized the window, and only
           reach for the superseded pair where it measurably reaches further back.

           WHAT THIS MEANS FOR A READER, stated because it is a real product behaviour and not a detail: a
           window inside the corrected coverage reads deduped numbers; a window past it reads the old
           inflated ones, and there is a VISIBLE STEP where the two meet. That step is not new — it exists
           today between raw (deduped since #1853) and the CAGGs, and this change moves it strictly outward
           and shrinks it every day. It disappears once the corrected rollups are backfilled past the old
           pair's floor, which --backfill-rollups does.

           The comparison is the same COMPARATIVE primitive the coverage ladder uses, never an absolute one:
           the legacy pair wins only when its floor is measurably deeper, so on a healthy store — where the
           corrected pair covers everything asked for — this never fires and never silently prefers the
           inflated numbers. */
        /* AVAILABILITY IS NOT COVERAGE, and deciding them in one step is wrong. A store whose service predates
           #1849 has NO corrected rollups, so there is nothing to compare floors against — every floor is null
           and the comparative rule below (correctly) refuses to move on no evidence, which would strand every
           Query Store window on raw and throw away the legacy pair that was serving them yesterday. Existence
           is therefore settled FIRST, and settling it restores exactly the pre-#1849 routing. */
        var correctedExists = rollups.Has(cagg.HourlyView)
            || (cagg.DailyView is not null && rollups.Has(cagg.DailyView));

        if (!correctedExists)
        {
            return ResolveFamily(cagg.LegacyHourlyView!, cagg.LegacyDailyView, nowUtc, windowStartUtc, rollups, coverage).Route;
        }

        if (primary.Route.IsCagg && TierCoverage.Covers(primary.FloorUtc, windowStartUtc))
        {
            return primary.Route;
        }

        var legacy = ResolveFamily(cagg.LegacyHourlyView!, cagg.LegacyDailyView, nowUtc, windowStartUtc, rollups, coverage);

        return legacy.Route.IsCagg && TierCoverage.ReachesFurtherBack(legacy.FloorUtc, primary.FloorUtc)
            ? legacy.Route
            : primary.Route;
    }

    /// <summary>
    /// #1869: at the DAILY tier only, swap in the day-grain daily where it can answer the window.
    ///
    /// <para><b>Why the daily tier only.</b> The hour-straddle residual this removes is IRREDUCIBLE at the
    /// hourly grain — an interval genuinely collected across two hours has to appear in both — so there is no
    /// better hourly to prefer, and an hourly-age window is left exactly as #1849 routed it. What made the
    /// daily worth a fourth near-raw-cardinality aggregate is that the daily tier is kept INDEFINITELY: its
    /// numbers are the ones that persist and get compared year over year.</para>
    ///
    /// <para><b>The candidate has to prove itself, which is the reverse of the corrected/legacy step in
    /// <see cref="Resolve"/> and deliberately so.</b> There the incumbent was the inflated pair and the
    /// newcomer was simply better;
    /// here both relations are corrected and the newcomer is merely MORE correct, while being younger and
    /// therefore shallower. So it wins where it covers the window, or where it measurably reaches further
    /// back, and otherwise the corrected daily keeps the window — trading a bounded ~2x over-count for
    /// coverage the newer view does not have, the same trade the legacy step makes one notch further down.
    /// With no coverage measured at all (a failed probe) nothing moves: <see cref="TierCoverage"/>'s rule is
    /// that null is no evidence, and no evidence never wins.</para>
    /// </summary>
    private static (ComposeRoute Route, DateTime? FloorUtc) PreferDayGrainDaily(
        ComposeCaggInfo cagg, (ComposeRoute Route, DateTime? FloorUtc) resolved,
        DateTime windowStartUtc, RollupAvailability rollups, RollupCoverage coverage)
    {
        if (cagg.DayGrainDailyView is null
            || resolved.Route.Tier != ComposeSourceTier.Daily
            || !rollups.Has(cagg.DayGrainDailyView))
        {
            return resolved;
        }

        var dayGrain = (Route: new ComposeRoute(ComposeSourceTier.Daily, cagg.DayGrainDailyView),
                        FloorUtc: coverage.FloorOf(cagg.DayGrainDailyView));

        if (TierCoverage.Covers(dayGrain.FloorUtc, windowStartUtc))
        {
            return dayGrain;
        }

        return TierCoverage.ReachesFurtherBack(dayGrain.FloorUtc, resolved.FloorUtc) ? dayGrain : resolved;
    }

    /// <summary>
    /// One rollup family's route AND the materialized floor the chosen tier actually reaches — the pair the
    /// corrected/legacy comparison above needs. A raw outcome carries a null floor, which
    /// <see cref="TierCoverage.ReachesFurtherBack"/> reads as "no evidence", so an absent or empty family can
    /// never beat one holding something.
    /// </summary>
    private static (ComposeRoute Route, DateTime? FloorUtc) ResolveFamily(
        string hourlyView, string? dailyView, DateTime nowUtc, DateTime windowStartUtc,
        RollupAvailability rollups, RollupCoverage coverage)
    {
        /* coverage.For(...) is called INLINE in the Resolve argument list, and again below for the floors,
           rather than hoisted into a local used twice. That is deliberate: RollupCoverageRoutingTests scans
           the source for RetentionTierRouter.Resolve call sites and requires each to look coverage up in its
           own argument list — a guard against the #1759 shape where a caller routes on no evidence. A local
           satisfies the compiler and defeats the scan, so the duplicate lookup (two dictionary probes) buys
           a guard that stays honest. Do not "simplify" it back. */
        var tier = RetentionTierRouter.Resolve(
            nowUtc, windowStartUtc,
            hourlyAvailable: rollups.Has(hourlyView),
            dailyAvailable: dailyView is not null && rollups.Has(dailyView),
            coverage: coverage.For(hourlyView, dailyView));

        var tierCoverage = coverage.For(hourlyView, dailyView);

        return tier switch
        {
            RetentionTier.Raw => (ComposeRoute.Raw, null),
            RetentionTier.Hourly => (new ComposeRoute(ComposeSourceTier.Hourly, hourlyView), tierCoverage.HourlyFloorUtc),
            _ => (new ComposeRoute(ComposeSourceTier.Daily, dailyView!), tierCoverage.DailyFloorUtc),
        };
    }
}
