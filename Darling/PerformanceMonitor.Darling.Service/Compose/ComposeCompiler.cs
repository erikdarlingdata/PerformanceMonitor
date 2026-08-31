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
using System.Text;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>The server scope + window + variable bindings a <see cref="PanelPlan"/> compiles against. The
/// window is naive UTC (Kind=Unspecified is stamped when bound, matching every other Darling read).
/// <see cref="NowUtc"/> is the actual wall-clock now, DECOUPLED from <see cref="EndUtc"/> (#1606): the
/// window binds $1/$2 and the bucket ceiling, but retention-tier routing and the partial-window notice
/// measure AGE from now — an absolute (zoomed/historical) window can end long before now.
/// <see cref="Servers"/> is the resolved <c>$server</c> scope (Erik's Decision 1 fleet axis): null/empty =
/// the WHOLE FLEET (no server predicate); one or many server names = a bound <c>server_name = ANY($n)</c>
/// filter, never interpolated (the compile-run endpoint resolves the $server variable to this). Group-by-server
/// and a per-panel server filter are separate and flow through the normal dimension path.</summary>
/// <see cref="Coverage"/> is the #1759 companion to <see cref="Rollups"/>: existence is not enough, because a
/// rollup created over pre-existing history serves only what it materialized, so the router also needs each
/// tier's measured floor to avoid answering an old window with silence.
public sealed record ComposeRunContext(
    IReadOnlyList<string>? Servers,
    DateTime StartUtc,
    DateTime EndUtc,
    IReadOnlyDictionary<string, string?> Variables,
    RollupAvailability Rollups,
    DateTime NowUtc,
    RollupCoverage Coverage)
{
    public static readonly IReadOnlyDictionary<string, string?> NoVariables =
        new Dictionary<string, string?>(StringComparer.Ordinal);
}

/// <summary>The compiled SQL + its bound parameters (in <c>$1..$n</c> order), and the source route the
/// compiler actually took (<see cref="ComposeRoute.Raw"/> for annotation queries, whose event tables have no
/// rollups) — the runner reads the route to tell the user when a raw/hourly fallback cannot retain the whole
/// requested window on a retention-active store (#1665).</summary>
public sealed record ComposeCompiled(string Sql, IReadOnlyList<NpgsqlParameter> Parameters, ComposeRoute Route);

/// <summary>
/// Compiles a validated <see cref="PanelPlan"/> into a parameterized Postgres query. IRON RULES, all
/// upheld by construction because every identifier comes from <see cref="MeasureCatalog"/> (never the
/// caller):
/// <list type="bullet">
/// <item>Table/column/aggregate/time-column identifiers are catalog constants, emitted schema-qualified
/// <c>collect.&lt;table&gt;</c> — the composed query can NEVER name a <c>config</c> table or an
/// off-catalog column, and never relies on search_path.</item>
/// <item>Every VALUE is a bound parameter: <c>$1</c>/<c>$2</c> the naive-UTC window, then (when the run is
/// scoped to specific servers) a bound <c>server_name = ANY($n)</c>, then filter values as <c>= ANY($n)</c>
/// (with the array bound), <c>LIKE $n</c>, threshold <c>$n</c>, and <c>LIMIT $n</c> for topN.</item>
/// <item>Aggregation is archetype-gated (SUM on the delta of a cumulative, on the column of a delta; AVG/
/// MIN/MAX on the gauge/per-event column; <c>percentile_cont</c> only on per-event); a ratio is
/// <c>SUM(a)::float / NULLIF(SUM(b), 0)</c>.</item>
/// <item>The #1568 <c>object_name</c> module join is bounded by the same window (the DoS fix over the
/// viewer's currently-unbounded stitch).</item>
/// </list>
/// The compiler assumes its input is a <see cref="PanelPlan"/> that <see cref="ComposeSpec.TryParsePanel"/>
/// already validated; the only failure it can still surface is the window×resolution ceiling (which needs
/// the run window, so it cannot be checked at write time).
/// </summary>
public static class ComposeCompiler
{
    /// <summary>The prefix time column per source (the collector's <c>PrefixTimeColumnName</c> — usually
    /// <c>collection_time</c>), resolved from the catalog so the compiler buckets/windows on the SAME
    /// column the pin test asserts each measure's time column is.</summary>
    private static readonly Dictionary<string, string> s_timeColumnByTable =
        CollectorCatalog.All.ToDictionary(c => c.TargetTable, c => c.PrefixTimeColumnName, StringComparer.Ordinal);

    /// <summary>The fact-table alias every composed query uses, so fact columns qualify unambiguously
    /// against the <c>m</c> module-join alias.</summary>
    private const char FactAlias = 'f';

    private const char ModuleAlias = 'm';

    /// <summary>The one source table whose RAW rows need a per-interval dedup before aggregation (#1841).</summary>
    private const string QueryStoreTable = "query_store_stats";

    /// <summary>
    /// The relation the panel aggregates: the routed CAGG, or the raw source table — except
    /// <c>query_store_stats</c> on the RAW route, which is wrapped in a per-interval dedup first (#1841).
    ///
    /// <para>Its rows are CUMULATIVE per-Query-Store-interval snapshots and the collector re-fetches the OPEN
    /// interval every cycle, so <c>qs_executions</c> (SUM) and the weighted <c>qs_avg_*</c> ratios would count
    /// one interval's work once per collection. The dedup keeps the LATEST snapshot per interval — the same
    /// ROW_NUMBER convention the analysis collectors and both apps' Query Store readers use.</para>
    ///
    /// <para><c>server_id</c> is in the partition because a composed panel spans the fleet, not one server —
    /// and <c>server_name</c> is there too, which is NOT redundant: Postgres can push a qual through a
    /// subquery containing a window function only when the qual's columns appear in EVERY window's
    /// PARTITION BY, and the panel's server scope is expressed as <c>server_name = ANY(...)</c> in the outer
    /// WHERE. Without it a fleet store would rank every server's rows before narrowing to the panel's. It is
    /// 1:1 with <c>server_id</c>, so it only ever makes the partition finer, never over-collapses. The
    /// grouped/filtered dimensions (<c>database_name</c>) push down for the same reason.</para>
    ///
    /// <para>The window is pushed INSIDE so "latest" means latest within the panel's window (the outer WHERE
    /// re-applies it harmlessly). <c>SELECT *</c> keeps every dimension and filter column available, which a
    /// compiler that may reference any catalog column needs; the extra <c>qs_rn</c> column is never selected
    /// or grouped. The cost is that unreferenced columns ride through the window sort — acceptable here
    /// because the raw route only ever covers the raw retention tier and the result is row-capped, and the
    /// alternative is hardcoding a column list a new measure would silently outgrow. Composed SQL still names
    /// only catalog identifiers, and every value is one of the two already-bound window parameters.</para>
    ///
    /// <para><b>The CAGG route is repaired since #1849, but only where the corrected rollups reach.</b> The
    /// original <c>query_store_stats_hourly</c>/<c>_daily</c> materialized their sums from UN-DEDUPED raw
    /// rows, and no read-side edit can undo that — the duplicates are gone once materialized and the CAGG
    /// output carries no interval identity to key on. #1849 therefore added a corrected family
    /// (<c>query_store_stats_interval_hourly</c> dedups at interval grain;
    /// <c>query_store_stats_corrected_hourly</c>/<c>_daily</c> collapse it to these composer dims) and
    /// <see cref="ComposeSourceRouter"/> prefers it. The old pair is KEPT — a CAGG cannot be reshaped in
    /// place, and rebuilding would destroy the entire retained hourly tier and all daily history (#1759/#1793) — so a
    /// window older than the corrected rollups have materialized still reads INFLATED numbers, with a
    /// visible step at that boundary. <c>--backfill-rollups</c> is what moves the boundary; retention aging
    /// out the old rows is what eventually removes it.</para>
    ///
    /// <para><b>A SECOND, permanent understatement sits underneath all of that, from before #1907.</b> Query
    /// Store returns the flushed and the still-in-memory slice of one interval as two ADDITIVE rows, and
    /// builds before #1907 stored both; every rollup materialized from them therefore carries ONE SLICE of
    /// each split interval rather than the sum. On the live evidence that was 8 executions where 94 was true.
    /// <c>--collapse-legacy-slices</c> (#1912) repairs the stored rows and re-materializes what they fed, but
    /// only as far back as RAW still reaches — a few days — because a rollup cannot be rebuilt from raw that
    /// retention has already dropped, and re-materializing a range raw no longer covers DESTROYS the rollup
    /// there rather than correcting it. So Query Store numbers older than the raw window at the moment that
    /// verb was run are understated PERMANENTLY, and the daily tiers keep them indefinitely. That is a
    /// disclosure, not a to-do: no ordering of these operations reaches it, which is why the release notes say
    /// so plainly and why the verb is worth running promptly after upgrading, while raw still covers the
    /// period the collector was getting wrong. Nothing about it is visible in a panel — the numbers are simply
    /// low — which is precisely why it is written down here beside the boundary it shares.</para>
    ///
    /// <para><b>At the DAILY tier there is one more rung since #1869.</b> The corrected daily dedups each
    /// interval within a collection HOUR, so an interval straddling an hour boundary lands in it about twice
    /// (measured 1.97x); <c>query_store_stats_daygrain_daily</c> dedups across the whole DAY and counts it
    /// once. It is a newer, shallower aggregate, so the router prefers it only where it has materialized the
    /// window — three dailies, best-first, each rung the same comparative rule. The hourly tier has no such
    /// rung because the residual is irreducible there.</para>
    ///
    /// <para>The dedup partition below is the SAME key the corrected L1 groups on, and it has to stay that
    /// way — raw and rollup answering one panel with different notions of "an interval" is the class of
    /// defect #1784 records. L1 additionally groups <c>module_name</c>/<c>query_hash</c> because it projects
    /// them; both are functionally dependent on <c>query_id</c>, so they add no groups.</para>
    /// </summary>
    private static string BuildFactRelation(
        string sourceTable, ComposeRoute route, string timeColumn, string startParam, string endParam)
    {
        if (route.IsCagg)
        {
            return $"{PgSchemaGenerator.CollectSchema}.{route.CaggRelation!}";
        }

        if (!string.Equals(sourceTable, QueryStoreTable, StringComparison.Ordinal))
        {
            return $"{PgSchemaGenerator.CollectSchema}.{sourceTable}";
        }

        return "(SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY server_id, server_name, database_name, "
            + $"query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role ORDER BY {timeColumn} DESC, execution_count DESC) AS qs_rn "
            + $"FROM {PgSchemaGenerator.CollectSchema}.{QueryStoreTable} "
            + $"WHERE {timeColumn} >= {startParam} AND {timeColumn} <= {endParam}) AS qs_ranked WHERE qs_rn = 1)";
    }

    /// <summary>
    /// Compiles <paramref name="plan"/> against <paramref name="context"/>. Returns the parameterized SQL,
    /// or a caller-facing error for the one runtime-only check (the window×resolution bucket ceiling).
    /// </summary>
    public static (ComposeCompiled? Compiled, string? Error) Compile(PanelPlan plan, ComposeRunContext context)
    {
        if (plan is null)
        {
            throw new ArgumentNullException(nameof(plan));
        }

        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        /* Source routing: read a CAGG rollup instead of raw when the window's oldest point is past the raw
           horizon (ComposeSourceRouter). Age is measured from NowUtc, NOT EndUtc (#1606): an absolute zoomed
           window can end well in the past, and retention drops by actual wall-clock now — a purely historical
           window must reach the tier that still retains it. Fall back to raw when a value expression can't be
           remapped to the CAGG columns (CanRemap; the overlay AND-gate below). */
        var route = ComposeSourceRouter.Resolve(plan, context.NowUtc, context.StartUtc, context.Rollups, context.Coverage);
        if (route.IsCagg
            && (!ComposeCaggValueMapper.CanRemap(plan.Measure, plan.Aggregate)
                || (plan.Overlay is ComposeOverlay o && !ComposeCaggValueMapper.CanRemap(o.Measure, o.Aggregate))))
        {
            /* One route serves BOTH value expressions — if either can't remap to the rollup columns, the whole
               panel reads raw (#1606: the overlay AND-gate). */
            route = ComposeRoute.Raw;
        }

        /* Auto resolves to a concrete grain from the window before anything downstream (ceiling + date_trunc);
           a non-Auto bucket passes through unchanged, so existing panels are byte-for-byte identical. */
        var effectiveBucket = plan.TimeBucket;
        if (plan.Mode == PanelMode.TimeSeries)
        {
            var windowSeconds = (context.EndUtc - context.StartUtc).TotalSeconds;
            effectiveBucket = MeasureCatalog.ResolveBucket(plan.TimeBucket, windowSeconds);
            /* A CAGG can't render finer than its own bucket — clamp the display grain up to the tier's grain
               (hourly -> at least hour, daily -> at least day) so date_trunc re-aggregates, never under-reads. */
            if (route.IsCagg)
            {
                effectiveBucket = CoarserBucket(
                    effectiveBucket, route.Tier == ComposeSourceTier.Daily ? ComposeTimeBucket.Day : ComposeTimeBucket.Hour);
            }
            var bucketSeconds = MeasureCatalog.BucketSeconds(effectiveBucket);
            if (bucketSeconds > 0)
            {
                var buckets = Math.Ceiling(windowSeconds / bucketSeconds);
                if (buckets > ComposeLimits.MaxBuckets)
                {
                    return (null,
                        $"the window and '{MeasureCatalog.WireName(effectiveBucket)}' bucket would produce {buckets:0} points " +
                        $"(max {ComposeLimits.MaxBuckets}); choose a coarser bucket or a shorter window.");
                }
            }
        }

        var p = new ParamList();
        /* $1 start, $2 end — the naive-UTC window prelude. The server scope is OPTIONAL/multi: null/empty
           $server => the whole fleet (no predicate); one/many servers => a bound server_name = ANY($n), never
           interpolated (the compile-run endpoint resolves the $server variable to this). */
        var startParam = p.AddTimestamp(context.StartUtc);
        var endParam = p.AddTimestamp(context.EndUtc);
        var hasServerScope = context.Servers is { Count: > 0 };
        var serverScopeParam = hasServerScope ? p.AddTextArray(context.Servers!) : null;

        var timeColumn = route.IsCagg ? ComposeRoute.CaggTimeColumn : s_timeColumnByTable[plan.Measure.SourceTable];
        var sql = new StringBuilder();

        /* The #1568 module CTE (window-bounded from procedure_stats) — only on the RAW path. A CAGG route joins the
           retained module_map instead (procedure_stats raw is dropped at 4d, so the CTE can't cover old windows). */
        if (plan.UsesModuleJoin && !route.IsCagg)
        {
            /* Window-bounded AND scoped to the same server set — partitioned by (server_name, sql_handle) so a
               handle reused across servers attributes per server, not globally. */
            sql.Append("WITH ").Append(ModuleAlias).Append(" AS (\n");
            sql.Append("    SELECT server_name, sql_handle, object_name, schema_name, database_name\n");
            sql.Append("    FROM (\n");
            sql.Append("        SELECT server_name, sql_handle, object_name, schema_name, database_name,\n");
            sql.Append("               ROW_NUMBER() OVER (PARTITION BY server_name, sql_handle ORDER BY collection_time DESC) AS rn\n");
            sql.Append("        FROM ").Append(PgSchemaGenerator.CollectSchema).Append(".procedure_stats\n");
            sql.Append("        WHERE collection_time >= ").Append(startParam).Append('\n');
            sql.Append("          AND collection_time <= ").Append(endParam).Append('\n');
            if (hasServerScope)
            {
                sql.Append("          AND server_name = ANY(").Append(serverScopeParam).Append(")\n");
            }

            sql.Append("          AND sql_handle IS NOT NULL\n");
            sql.Append("          AND sql_handle <> ''\n");
            sql.Append("    ) ranked_modules\n");
            sql.Append("    WHERE rn = 1\n");
            sql.Append(")\n");
        }

        /* Where the CTE (if any) ends and the real statement begins. A capped time-series query is wrapped
           in a subquery below, and the wrapper has to open HERE so the WITH stays at the top level rather
           than being swallowed into the subquery. */
        var bodyStart = sql.Length;

        /* SELECT list + the matching GROUP BY expressions. */
        var selectExprs = new List<string>();
        var groupExprs = new List<string>();

        if (plan.Mode == PanelMode.TimeSeries)
        {
            var bucketExpr = $"date_trunc('{MeasureCatalog.DateTruncField(effectiveBucket)}', {FactAlias}.{timeColumn})";
            selectExprs.Add(bucketExpr + " AS bucket");
            groupExprs.Add(bucketExpr);
        }

        foreach (var dim in plan.GroupBy)
        {
            var expr = ColumnRef(dim);
            selectExprs.Add(expr + " AS " + dim.Name);
            groupExprs.Add(expr);
        }

        selectExprs.Add(BuildValueExpr(plan.Measure, plan.Aggregate, plan.Unit, route) + " AS value");
        if (plan.Overlay is ComposeOverlay overlay)
        {
            /* The second measure (#1606): one more select expression over the SAME fact rows — never a join,
               never a parameter, never a second query. Same route as the primary (the AND-gate above). */
            selectExprs.Add(BuildValueExpr(overlay.Measure, overlay.Aggregate, overlay.Unit, route) + " AS value2");
        }

        sql.Append("SELECT ").Append(string.Join(", ", selectExprs)).Append('\n');
        sql.Append("FROM ").Append(BuildFactRelation(plan.Measure.SourceTable, route, timeColumn, startParam, endParam))
            .Append(" AS ").Append(FactAlias).Append('\n');

        if (plan.UsesModuleJoin)
        {
            /* Raw joins the window-bounded CTE (m); a CAGG route joins the retained collect.module_map directly
               (its raw procedure_stats is gone at 4d, but the CAGG carries sql_handle). Same alias + join keys, so
               object_name resolves as m.object_name either way. */
            if (route.IsCagg)
            {
                sql.Append("LEFT JOIN ").Append(PgSchemaGenerator.CollectSchema).Append(".module_map AS ").Append(ModuleAlias)
                    .Append(" ON ").Append(ModuleAlias).Append(".sql_handle = ").Append(FactAlias).Append(".sql_handle AND ")
                    .Append(ModuleAlias).Append(".server_name = ").Append(FactAlias).Append(".server_name\n");
            }
            else
            {
                sql.Append("LEFT JOIN ").Append(ModuleAlias).Append(" ON ").Append(ModuleAlias)
                    .Append(".sql_handle = ").Append(FactAlias).Append(".sql_handle AND ").Append(ModuleAlias)
                    .Append(".server_name = ").Append(FactAlias).Append(".server_name\n");
            }
        }

        sql.Append("WHERE ").Append(FactAlias).Append('.').Append(timeColumn).Append(" >= ").Append(startParam).Append('\n');
        sql.Append("  AND ").Append(FactAlias).Append('.').Append(timeColumn).Append(" <= ").Append(endParam).Append('\n');
        if (hasServerScope)
        {
            sql.Append("  AND ").Append(FactAlias).Append(".server_name = ANY(").Append(serverScopeParam).Append(")\n");
        }

        foreach (var filter in plan.Filters)
        {
            sql.Append("  AND ").Append(BuildFilterClause(filter, context, p)).Append('\n');
        }

        if (groupExprs.Count > 0)
        {
            sql.Append("GROUP BY ").Append(string.Join(", ", groupExprs)).Append('\n');
        }

        switch (plan.Mode)
        {
            case PanelMode.TimeSeries:
                /* The cap keeps the NEWEST buckets, not the oldest (#1687). A plain
                   "ORDER BY bucket LIMIT n" silently returns the EARLIEST n rows, so a grouped
                   minute-grain panel over 24h rendered its first ~87 minutes as though that were the
                   whole window — numbers right, window wrong, and nothing on screen said so. Recent
                   data is the point of a monitoring chart, so the cap is applied DESC inside a
                   subquery and the survivors re-sorted ascending for the renderer, which consumes
                   buckets in order.

                   No parameter-ordering hazard: this LIMIT is a literal (only the Ranked arm binds a
                   LIMIT parameter), and the wrapper only brackets text whose parameters were already
                   appended in the same order, so $n positions are untouched. */
                sql.Insert(bodyStart, "SELECT * FROM (\n");
                sql.Append("ORDER BY bucket DESC\n");
                sql.Append("LIMIT ").Append(ComposeLimits.HardRowCap).Append('\n');
                sql.Append(") AS capped\n");
                sql.Append("ORDER BY bucket");
                break;
            case PanelMode.Ranked:
                sql.Append("ORDER BY value DESC\n");
                sql.Append("LIMIT ").Append(p.AddInt(plan.TopN));
                break;
            default: /* Scalar — a single aggregate row. */
                sql.Append("LIMIT 1");
                break;
        }

        return (new ComposeCompiled(sql.ToString(), p.Parameters, route), null);
    }

    /// <summary>
    /// Compiles the panel's event-annotation overlays (design D5) into one bounded parameterized query per
    /// requested source — the SAME discipline as <see cref="Compile"/>: identifiers come ONLY from the annotation
    /// catalog (schema-qualified <c>collect.&lt;table&gt;</c>, never a caller string); the window and the (optional)
    /// server scope are the SAME bound parameters the measure query uses (never interpolated); and each query is
    /// capped at <see cref="ComposeLimits.MaxAnnotationEvents"/> and ordered by event time. Returns one
    /// <c>(sourceKey, compiled)</c> pair per requested source (empty when the panel requests none). Annotations
    /// never change the measure query — the endpoint runs these separately and returns their events alongside it.
    /// </summary>
    public static IReadOnlyList<(string Source, ComposeCompiled Compiled)> CompileAnnotations(PanelPlan plan, ComposeRunContext context)
    {
        if (plan is null)
        {
            throw new ArgumentNullException(nameof(plan));
        }

        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        if (plan.Annotations.Count == 0)
        {
            return Array.Empty<(string, ComposeCompiled)>();
        }

        var results = new List<(string, ComposeCompiled)>(plan.Annotations.Count);
        foreach (var source in plan.Annotations)
        {
            results.Add((source.Key, CompileAnnotation(source, context)));
        }

        return results;
    }

    /// <summary>Compiles one annotation source into its bounded, catalog-only, schema-qualified event query:
    /// <c>SELECT f.&lt;timeCol&gt; AS ts, f.&lt;labelCol&gt; AS label FROM collect.&lt;table&gt; AS f WHERE
    /// f.&lt;timeCol&gt; BETWEEN $1 AND $2 [AND f.server_name = ANY($3)] ORDER BY ts LIMIT
    /// &lt;MaxAnnotationEvents&gt;</c>. Every identifier is a catalog constant; every value is bound.</summary>
    private static ComposeCompiled CompileAnnotation(ComposeAnnotationSource source, ComposeRunContext context)
    {
        var p = new ParamList();
        var startParam = p.AddTimestamp(context.StartUtc);
        var endParam = p.AddTimestamp(context.EndUtc);
        var hasServerScope = context.Servers is { Count: > 0 };
        var serverScopeParam = hasServerScope ? p.AddTextArray(context.Servers!) : null;

        var sql = new StringBuilder();
        sql.Append("SELECT ").Append(FactAlias).Append('.').Append(source.TimeColumn).Append(" AS ts, ")
            .Append(FactAlias).Append('.').Append(source.LabelColumn).Append(" AS label\n");
        sql.Append("FROM ").Append(PgSchemaGenerator.CollectSchema).Append('.').Append(source.SourceTable)
            .Append(" AS ").Append(FactAlias).Append('\n');
        sql.Append("WHERE ").Append(FactAlias).Append('.').Append(source.TimeColumn).Append(" >= ").Append(startParam).Append('\n');
        sql.Append("  AND ").Append(FactAlias).Append('.').Append(source.TimeColumn).Append(" <= ").Append(endParam).Append('\n');
        if (hasServerScope)
        {
            sql.Append("  AND ").Append(FactAlias).Append(".server_name = ANY(").Append(serverScopeParam).Append(")\n");
        }

        sql.Append("ORDER BY ts\n");
        sql.Append("LIMIT ").Append(ComposeLimits.MaxAnnotationEvents);

        return new ComposeCompiled(sql.ToString(), p.Parameters, ComposeRoute.Raw);
    }

    /// <summary>The qualified reference for a dimension column: <c>m.</c> for a module-join dimension,
    /// <c>f.</c> for a fact column.</summary>
    private static string ColumnRef(ComposeDimension dimension) =>
        (dimension.ViaModuleJoin ? ModuleAlias : FactAlias) + "." + dimension.Column;

    /// <summary>The coarser of two buckets (None &lt; Minute &lt; Hour &lt; Day) — clamps a display grain up to a
    /// CAGG tier's own grain, since a rollup can never be rendered finer than it was materialized.</summary>
    private static ComposeTimeBucket CoarserBucket(ComposeTimeBucket a, ComposeTimeBucket b) =>
        (ComposeTimeBucket)Math.Max((int)a, (int)b);

    /// <summary>Builds the <c>value</c> expression: the archetype/kind-gated aggregate, cast to double, then
    /// scaled by the requested unit's conversion factor (a compile-time family constant). Parameterized on
    /// (measure, aggregate, unit) rather than the whole plan (#1606) so the overlay's <c>value2</c> compiles
    /// through the SAME expression builder — the two can never drift. Binds ZERO parameters (the percentile
    /// and unit factors are literals), so a second call cannot shift the $n order.</summary>
    private static string BuildValueExpr(ComposeMeasure measure, ComposeAggregate aggregate, string unit, ComposeRoute route)
    {
        /* A CAGG route reads the pre-aggregated rollup columns instead of the raw delta (the route gate guarantees
           the measure + aggregate is remappable — CanRemap); unit-scaled exactly like the raw paths below. */
        if (route.IsCagg)
        {
            return ApplyUnitConversion(
                ComposeCaggValueMapper.BuildCaggNativeExpr(measure, aggregate), measure.UnitFamily, measure.NativeUnit, unit);
        }

        if (measure.Kind == MeasureKind.Ratio)
        {
            string native;
            if (measure.RatioMode == MeasureRatioMode.Weighted)
            {
                /* Weighted mode (a pre-aggregated per-interval average weighted by its execution count): the
                   execution-weighted mean SUM(value * weight) / NULLIF(SUM(weight), 0) across intervals — the
                   correct average of averages (design §2), not a plain avg-of-avgs. Both operands are raw source
                   columns, not numerator/denominator measures. */
                native = $"(CAST(SUM({FactAlias}.{measure.WeightedValueColumn} * {FactAlias}.{measure.WeightColumn}) AS double precision) " +
                         $"/ NULLIF(SUM({FactAlias}.{measure.WeightColumn}), 0))";
            }
            else if (measure.RatioMode == MeasureRatioMode.WeightedSum)
            {
                /* WeightedSum (#2732): the Weighted numerator with no denominator — SUM(value * weight) is the
                   window TOTAL, because avg * execution_count is each interval's total consumption. No NULLIF:
                   there is no division, and SUM over zero rows is already NULL. */
                native = $"CAST(SUM({FactAlias}.{measure.WeightedValueColumn} * {FactAlias}.{measure.WeightColumn}) AS double precision)";
            }
            else
            {
                var numerator = MeasureCatalog.Measure(measure.NumeratorKey)!;
                var denominator = MeasureCatalog.Measure(measure.DenominatorKey)!;
                /* Sum mode (cumulative/delta operands): SUM(delta) / NULLIF(SUM(delta), 0). Avg mode (gauge operands,
                   whose AggregationColumn is null — a gauge is never summable): AVG(col) / NULLIF(AVG(col), 0). */
                native = measure.RatioMode == MeasureRatioMode.Avg
                    ? $"(CAST(AVG({FactAlias}.{numerator.Column}) AS double precision) " +
                      $"/ NULLIF(AVG({FactAlias}.{denominator.Column}), 0))"
                    : $"(CAST(SUM({FactAlias}.{numerator.AggregationColumn}) AS double precision) " +
                      $"/ NULLIF(SUM({FactAlias}.{denominator.AggregationColumn}), 0))";
            }

            return ApplyUnitConversion(native, measure.UnitFamily, measure.NativeUnit, unit);
        }

        /* COUNT is a plain, unitless row count. */
        if (aggregate == ComposeAggregate.Count)
        {
            return "CAST(COUNT(*) AS double precision)";
        }

        /* The aggregated column: the delta for a cumulative counter, the column itself otherwise. */
        var aggColumn = measure.Archetype == MeasureArchetype.Cumulative ? measure.DeltaColumn! : measure.Column!;
        var qualified = FactAlias + "." + aggColumn;

        var nativeExpr = aggregate switch
        {
            ComposeAggregate.Sum => $"CAST(SUM({qualified}) AS double precision)",
            ComposeAggregate.Avg => $"CAST(AVG({qualified}) AS double precision)",
            ComposeAggregate.Min => $"CAST(MIN({qualified}) AS double precision)",
            ComposeAggregate.Max => $"CAST(MAX({qualified}) AS double precision)",
            ComposeAggregate.PercentileCont =>
                $"percentile_cont({FormatDouble(ComposeLimits.DefaultPercentile)}) WITHIN GROUP (ORDER BY {qualified})",
            _ => throw new InvalidOperationException($"Unhandled aggregate {aggregate}"),
        };

        return ApplyUnitConversion(nativeExpr, measure.UnitFamily, measure.NativeUnit, unit);
    }

    /// <summary>Scales <paramref name="expr"/> (already a double) from <paramref name="nativeUnit"/> to
    /// <paramref name="requestedUnit"/> within <paramref name="familyName"/>: <c>value * factorNative /
    /// factorRequested</c>. A no-op when the units match. Factors are compile-time family constants.</summary>
    private static string ApplyUnitConversion(string expr, string familyName, string nativeUnit, string requestedUnit)
    {
        var family = MeasureCatalog.Family(familyName);
        var from = family?.Unit(nativeUnit);
        var to = family?.Unit(requestedUnit);
        if (from is null || to is null || from.BaseFactor == to.BaseFactor)
        {
            return expr;
        }

        return $"({expr}) * {FormatDouble(from.BaseFactor)} / {FormatDouble(to.BaseFactor)}";
    }

    /// <summary>Builds one filter's SQL predicate, binding every value as a parameter.</summary>
    private static string BuildFilterClause(ComposeFilter filter, ComposeRunContext context, ParamList p)
    {
        var column = ColumnRef(filter.Dimension);
        var values = ResolveValues(filter.Value, context);

        switch (filter.Op)
        {
            case ComposeFilterOp.Eq:
                return $"{column} = ANY({p.AddTextArray(values)})";
            case ComposeFilterOp.Neq:
                return $"{column} <> ALL({p.AddTextArray(values)})";
            case ComposeFilterOp.Like:
                return $"{column} LIKE {p.AddText(First(values))}";
            case ComposeFilterOp.Gt:
                return $"{column} > {p.AddText(First(values))}";
            case ComposeFilterOp.Gte:
                return $"{column} >= {p.AddText(First(values))}";
            case ComposeFilterOp.Lt:
                return $"{column} < {p.AddText(First(values))}";
            case ComposeFilterOp.Lte:
                return $"{column} <= {p.AddText(First(values))}";
            default:
                throw new InvalidOperationException($"Unhandled filter op {filter.Op}");
        }
    }

    /// <summary>Resolves a filter value to concrete strings: a literal set as-is, or a declared variable's
    /// run value (its request binding or default; a missing binding resolves to an empty string).</summary>
    private static IReadOnlyList<string> ResolveValues(ComposeFilterValue value, ComposeRunContext context)
    {
        if (value.VariableRef is not null)
        {
            var resolved = context.Variables.TryGetValue(value.VariableRef, out var v) ? v : null;
            return new[] { resolved ?? "" };
        }

        return value.Literals ?? Array.Empty<string>();
    }

    private static string First(IReadOnlyList<string> values) => values.Count > 0 ? values[0] : "";

    /// <summary>Formats an (always integral) factor / the percentile as a double literal so Postgres never
    /// does integer division (e.g. <c>1048576.0</c>, <c>0.95</c>).</summary>
    private static string FormatDouble(double value)
    {
        if (value == Math.Floor(value) && !double.IsInfinity(value))
        {
            return ((long)value).ToString(CultureInfo.InvariantCulture) + ".0";
        }

        return value.ToString("0.0###############", CultureInfo.InvariantCulture);
    }

    /// <summary>Accumulates bound parameters and hands back their <c>$n</c> placeholders in order.</summary>
    private sealed class ParamList
    {
        private readonly List<NpgsqlParameter> _parameters = new();

        public IReadOnlyList<NpgsqlParameter> Parameters => _parameters;

        public string AddInt(int value)
        {
            _parameters.Add(new NpgsqlParameter<int> { TypedValue = value });
            return Placeholder();
        }

        public string AddTimestamp(DateTime value)
        {
            _parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(value, DateTimeKind.Unspecified) });
            return Placeholder();
        }

        public string AddText(string value)
        {
            _parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = value ?? "" });
            return Placeholder();
        }

        public string AddTextArray(IReadOnlyList<string> values)
        {
            _parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = values.ToArray() });
            return Placeholder();
        }

        private string Placeholder() => "$" + _parameters.Count.ToString(CultureInfo.InvariantCulture);
    }
}
