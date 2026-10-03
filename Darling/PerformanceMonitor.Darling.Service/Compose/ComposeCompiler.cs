/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Analysis.Baselines;
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
    RollupCoverage Coverage,
    bool QueryStoreWideEligible = false,
    DateTime? QueryStoreWideStart = null)
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
/// viewer's currently-unbounded stitch), and its NULL misses are folded at <see cref="ColumnRef"/>
/// (#2737) so ad-hoc rows are labeled and filterable rather than a null-named blob that
/// <c>&lt;&gt; ALL</c> silently drops.</item>
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

    /// <summary>The residual series label a <see cref="PanelMode.RankedTimeSeries"/> panel emits when the
    /// spec sets <c>includeOther</c> (#2734). Parenthesized-lowercase matches the product's sentinel family
    /// (<see cref="MeasureCatalog.AdHocLabel"/>, "(unknown)", "(none)") — distinct from every member of it —
    /// and is a value no engine-constant dimension (wait types, clerk types, lock modes, event names, hex
    /// hashes) can ever carry. An identifier dimension COULD in principle hold a database/object literally
    /// named "(other)" — that member's own series would then merge with the residual, a visible labeling
    /// ambiguity whose per-bucket arithmetic still sums to the window total — accepted over inventing an
    /// escaping scheme for one pathological name.</summary>
    public const string OtherSeriesLabel = "(other)";

    /// <summary>The rank CTE's name in a <see cref="PanelMode.RankedTimeSeries"/> statement (#2734).</summary>
    private const string RankCte = "topn";

    /// <summary>
    /// The relation the panel aggregates: the routed CAGG, or the raw source table — except
    /// <c>query_store_stats</c> on the RAW route, which is wrapped in a per-interval dedup first (#1841).
    ///
    /// <para>Its rows are CUMULATIVE per-Query-Store-interval snapshots and the collector re-fetches the OPEN
    /// interval every cycle, so <c>qs_executions</c> (SUM) and the weighted <c>qs_avg_*</c> ratios would count
    /// one interval's work once per collection. The dedup keeps the LATEST snapshot per interval — the same
    /// ROW_NUMBER convention the analysis collectors and both apps' Query Store readers use. When the panel
    /// filters on a dimension of this table, the dedupe ranks only the partitions that hold a matching row
    /// (<see cref="QueryStorePartitionRestriction"/>); the filter itself stays outside, so a mid-window rename
    /// cannot change which snapshot survives.</para>
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
        string sourceTable, ComposeRoute route, string timeColumn, string startParam, string endParam, ComposeRunContext context, string? wideStartParam = null,
        IReadOnlyList<string>? dimensionFilters = null, string? serverScopeSql = null, bool restrictDedupe = false)
    {
        if (route.IsCagg)
        {
            /* #3653 A6: CaggFromClause is the FROM-clause item (decision 2) — either
               "collect.<relation> AS f" unchanged, or RollupCoverage.StitchedRelationSql's stitched form.
               AppendFactBody appends " AS f" itself for the raw/QueryStore branches below, but a CAGG
               FROM-clause item is already a complete, aliased relation, so BuildFactRelation returns it
               whole and AppendFactBody's own " AS f" suffix applies to it as a no-op repeat of the SAME
               alias the clause already carries — see the guard in DarlingComposeTests pinning that shape. */
            return route.CaggFromClause ?? $"{PgSchemaGenerator.CollectSchema}.{route.CaggRelation!}";
        }

        if (!string.Equals(sourceTable, QueryStoreTable, StringComparison.Ordinal))
        {
            return $"{PgSchemaGenerator.CollectSchema}.{sourceTable}";
        }

        /* #4605: query_store_interval_wide (V145) already holds the latest snapshot per
           interval, every outcome — exactly what the raw ROW_NUMBER dedupe below computes — so an eligible
           run reads it directly instead of re-sorting every raw snapshot in the window. Eligibility
           (QueryStoreIntervalWide.UseTable plus clause 6, per server in scope) is decided by the runner
           BEFORE compiling (ComposeRunContext.QueryStoreWideEligible), never here: the compiler stays pure
           and never opens a connection. The table has no server_name column, so the relation joins the
           registry (collect.servers, server_id PRIMARY KEY / server_name NOT NULL — 1:1) to restore it,
           the same column every downstream WHERE/GROUP BY/partition on this fact body reads.

           This route deliberately carries no first_execution_time floor (#4605). With a BRIN index on
           collection_time and random_page_cost 1.1, the floor made the planner read the window plus 26 h of rows
           through idx_query_store_interval_wide_first_exec instead of that index, about 4x slower. Fleet-wide
           windows of 12 h or more rely on that index and that setting; without them this read scans the table. */
        if (context.QueryStoreWideEligible)
        {
            return $"(SELECT w.*, s.server_name FROM {PgSchemaGenerator.CollectSchema}.query_store_interval_wide AS w "
                + $"JOIN {PgSchemaGenerator.CollectSchema}.servers AS s ON s.server_id = w.server_id "
                + $"WHERE w.{timeColumn} >= {wideStartParam ?? startParam} AND w.{timeColumn} <= {endParam})";
        }

        return "(SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY " + QueryStorePartitionColumns
            + $" ORDER BY {timeColumn} DESC, execution_count DESC) AS qs_rn "
            + $"FROM {PgSchemaGenerator.CollectSchema}.{QueryStoreTable} "
            + $"WHERE {timeColumn} >= {startParam} AND {timeColumn} <= {endParam}"
            + QueryStorePartitionRestriction(timeColumn, startParam, endParam, dimensionFilters, serverScopeSql, restrictDedupe)
            + ") AS qs_ranked WHERE qs_rn = 1)";
    }

    /* The ONE partition key: the dedupe's PARTITION BY text and the restriction's join keys both come from it. */
    private static readonly string[] s_queryStorePartitionKey =
    {
        "server_id", "server_name", "database_name", "query_id", "plan_id", "runtime_stats_interval_id", "first_execution_time", "execution_type_desc", "replica_role",
    };

    private static readonly string QueryStorePartitionColumns = string.Join(", ", s_queryStorePartitionKey);

    /// <summary>
    /// The raw dedupe's input restriction when the panel carries dimension filters (#4605): only the partitions
    /// holding at least one in-window row that matches those filters are ranked. Every kept partition keeps ALL
    /// of its window rows, so <c>qs_rn = 1</c> picks the same survivor as the unrestricted dedupe, and the outer
    /// filter still decides which survivors count. A partition with no matching row cannot yield a matching
    /// survivor. Moving the predicate itself inside the dedupe would NOT be exact: a module renamed mid-window
    /// leaves one partition with rows under two names, and the filter would change which row survives.
    ///
    /// <para>The restriction is a hashable NULL-safe semi-join: <c>EXISTS</c> over a <c>DISTINCT</c> key subquery,
    /// joined on <c>coalesce(col, sentinel)</c> equality for every key column (the hash keys) plus an
    /// <c>IS NOT DISTINCT FROM</c> residual. <c>PARTITION BY</c> groups NULLs together and a plain <c>IN</c> does
    /// not match them, but <c>IS NOT DISTINCT FROM</c> alone cannot be hashed. <c>replica_role</c> is NULL on every
    /// standalone server, so on such a store the NULL-safe comparison covers every row and must stay hashable (a
    /// first shape, <c>IN … OR (null AND correlated EXISTS)</c>, timed out there). The residual keeps the result
    /// exact even when a real value equals a sentinel (<c>''</c>, <c>-1</c>, <c>-infinity</c>).</para>
    ///
    /// <para>Only filters on the fact's own columns reach here; a module-joined dimension is not a column of this
    /// table. The restriction is added only when at least one filter is on a NON-partition column
    /// (<c>module_name</c>, <c>query_hash</c>): a filter on <c>server_name</c> or <c>database_name</c> is a
    /// partition column and Postgres already pushes it through the window subquery, so restricting on it would add
    /// a second scan, a DISTINCT and a semi-join for no change in the ranked rows. With no such filter the text is
    /// empty and the dedupe is unchanged. When it is added, the inner WHERE still carries every pushable filter.</para>
    /// </summary>
    private static string QueryStorePartitionRestriction(
        string timeColumn, string startParam, string endParam, IReadOnlyList<string>? dimensionFilters, string? serverScopeSql, bool restrictDedupe)
    {
        if (!restrictDedupe || dimensionFilters is not { Count: > 0 })
        {
            return string.Empty;
        }

        var table = $"{PgSchemaGenerator.CollectSchema}.{QueryStoreTable}";
        var window = $"{FactAlias}.{timeColumn} >= {startParam} AND {FactAlias}.{timeColumn} <= {endParam}"
            + (serverScopeSql is null ? string.Empty : " AND " + serverScopeSql)
            + string.Concat(dimensionFilters.Select(c => " AND " + c));
        var keys = s_queryStorePartitionKey;
        string Sentinel(string c) => c switch
        {
            "server_id" => throw new InvalidOperationException("server_id is never NULL and is compared directly; it has no sentinel."),
            "query_id" or "plan_id" or "runtime_stats_interval_id" => "-1",
            "first_execution_time" => "'-infinity'::timestamp",
            _ => "''",
        };
        var hashKey = string.Concat(keys.Select(c => c == "server_id"
            ? $" AND k.server_id = {QueryStoreTable}.server_id"
            : $" AND coalesce(k.{c}, {Sentinel(c)}) = coalesce({QueryStoreTable}.{c}, {Sentinel(c)})"));
        var residual = string.Concat(keys.Where(c => c != "server_id").Select(c => $" AND k.{c} IS NOT DISTINCT FROM {QueryStoreTable}.{c}"));
        return $" AND EXISTS (SELECT 1 FROM (SELECT DISTINCT {string.Join(", ", keys.Select(c => FactAlias + "." + c))}"
            + $" FROM {table} AS {FactAlias} WHERE {window}) AS k WHERE true{hashKey}{residual})";
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
        if (plan.Mode is PanelMode.TimeSeries or PanelMode.RankedTimeSeries)
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

        /* #4689: below raw's floor the interval table is exact only from the runner's common start
           (ComposeRunContext.QueryStoreWideStart, the latest per-server read start), so an eligible Query Store
           read binds the later of that and the window start, in the same collection_time column the window
           uses. Bound once, so the rank CTE and the outer query share it. */
        var wideStartParam = context.QueryStoreWideEligible
            && context.QueryStoreWideStart is DateTime wideStart && wideStart > context.StartUtc
            && string.Equals(plan.Measure.SourceTable, QueryStoreTable, StringComparison.Ordinal)
                ? p.AddTimestamp(wideStart)
                : null;

        /* Filter predicates are built — and their values BOUND — once, in filter order, so the parameter
           order is identical for every mode (window, scope, filters, then topN). RankedTimeSeries (#2734)
           reuses the same clause TEXT in both its rank CTE and its series query, which reuses the same $n
           placeholders rather than double-binding each value. */
        var filterClauses = new List<string>(plan.Filters.Count);
        var pushableFilterClauses = new List<string>();
        var restrictDedupe = false;
        foreach (var filter in plan.Filters)
        {
            var clause = BuildFilterClause(filter, context, p);
            filterClauses.Add(clause);
            if (!filter.Dimension.ViaModuleJoin
                && string.Equals(filter.Dimension.SourceTable, QueryStoreTable, StringComparison.Ordinal))
            {
                pushableFilterClauses.Add(clause);
                restrictDedupe |= !s_queryStorePartitionKey.Contains(filter.Dimension.Column, StringComparer.Ordinal);
            }
        }

        var timeColumn = route.IsCagg ? ComposeRoute.CaggTimeColumn : s_timeColumnByTable[plan.Measure.SourceTable];
        var sql = new StringBuilder();

        /* The fact FROM + (optional) module join + WHERE window/scope/filters — one emitter because the
           RankedTimeSeries rank CTE and the outer query must aggregate the SAME fact rows; two hand-kept
           copies would drift into ranking one population and charting another. `indent` nests the text
           inside the CTE without changing the outer query's byte-for-byte shape. */
        void AppendFactBody(string indent)
        {
            sql.Append(indent).Append("FROM ").Append(BuildFactRelation(plan.Measure.SourceTable, route, timeColumn, startParam, endParam, context, wideStartParam, pushableFilterClauses, hasServerScope ? $"{FactAlias}.server_name = ANY({serverScopeParam})" : null, restrictDedupe));

            /* #3653 A6: a CAGG route's FROM-clause item (route.CaggFromClause) is already a complete, aliased
               relation — "collect.<x> AS f" or a stitched "(... UNION ALL ...) AS f" — so it must NOT get a
               second " AS f" appended here (that would be a syntax error). The raw/QueryStore branches in
               BuildFactRelation still return a bare relation, so they still need the alias appended below. */
            if (!route.IsCagg || route.CaggFromClause is null)
            {
                sql.Append(" AS ").Append(FactAlias);
            }

            sql.Append('\n');

            if (plan.UsesModuleJoin)
            {
                /* Raw joins the window-bounded CTE (m); a CAGG route joins the retained collect.module_map directly
                   (its raw procedure_stats is gone at 4d, but the CAGG carries sql_handle). Same alias + join keys, so
                   object_name resolves as m.object_name either way. */
                if (route.IsCagg)
                {
                    sql.Append(indent).Append("LEFT JOIN ").Append(PgSchemaGenerator.CollectSchema).Append(".module_map AS ").Append(ModuleAlias)
                        .Append(" ON ").Append(ModuleAlias).Append(".sql_handle = ").Append(FactAlias).Append(".sql_handle AND ")
                        .Append(ModuleAlias).Append(".server_name = ").Append(FactAlias).Append(".server_name\n");
                }
                else
                {
                    sql.Append(indent).Append("LEFT JOIN ").Append(ModuleAlias).Append(" ON ").Append(ModuleAlias)
                        .Append(".sql_handle = ").Append(FactAlias).Append(".sql_handle AND ").Append(ModuleAlias)
                        .Append(".server_name = ").Append(FactAlias).Append(".server_name\n");
                }
            }

            /* A rollup bucket is stamped at its START, so a CAGG route's window end is EXCLUSIVE: with the end
               exactly on a bucket start, `bucket <= end` would also take the whole hour (or, on the daily tier,
               the whole day) that begins there, which lies after the window. A raw route stamps a sample when it
               was taken, so a sample at the end still counts and it keeps `<=`. */
            var endOperator = route.IsCagg ? " < " : " <= ";
            sql.Append(indent).Append("WHERE ").Append(FactAlias).Append('.').Append(timeColumn).Append(" >= ").Append(startParam).Append('\n');
            sql.Append(indent).Append("  AND ").Append(FactAlias).Append('.').Append(timeColumn).Append(endOperator).Append(endParam).Append('\n');
            if (hasServerScope)
            {
                sql.Append(indent).Append("  AND ").Append(FactAlias).Append(".server_name = ANY(").Append(serverScopeParam).Append(")\n");
            }

            foreach (var clause in filterClauses)
            {
                sql.Append(indent).Append("  AND ").Append(clause).Append('\n');
            }
        }

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

        /* The #2734 rank pass: RankedTimeSeries prepends a CTE that IS the Ranked query minus the time
           column — the same fact rows, filters, and value expression, grouped by the dims alone, ordered
           by the window-total aggregate, LIMIT topN. The outer query then buckets ONLY those members.
           Ranking by the WINDOW TOTAL is the decided semantic (#2734 option 1): membership is stable
           across the window, so the chart reads as N lines. Per-bucket re-ranking is a non-goal — see the
           PanelMode doc. */
        if (plan.Mode == PanelMode.RankedTimeSeries)
        {
            sql.Append(sql.Length == 0 ? "WITH " : ", ").Append(RankCte).Append(" AS (\n");
            var rankSelects = new List<string>();
            foreach (var dim in plan.GroupBy)
            {
                rankSelects.Add(GroupRef(dim) + " AS " + dim.Name);
            }

            rankSelects.Add(BuildValueExpr(plan.Measure, plan.Aggregate, plan.Unit, route) + " AS value");
            sql.Append("    SELECT ").Append(string.Join(", ", rankSelects)).Append('\n');
            AppendFactBody("    ");
            sql.Append("    GROUP BY ").Append(string.Join(", ", plan.GroupBy.Select(GroupRef))).Append('\n');
            /* NULLS LAST, because Postgres's DESC default is NULLS FIRST: a group whose aggregate is NULL
               (every in-window row's delta column NULL — a counter's first-ever collection, say) would
               otherwise outrank every REAL winner and silently occupy a series slot. Worse here than in
               the plain Ranked arm below (where the NULL row is at least visible): this ordering decides
               MEMBERSHIP for the whole chart. */
            sql.Append("    ORDER BY value DESC NULLS LAST\n");
            sql.Append("    LIMIT ").Append(p.AddInt(plan.TopN)).Append('\n');
            sql.Append(")\n");
        }

        /* Where the CTEs (if any) end and the real statement begins. A capped time-series query is wrapped
           in a subquery below, and the wrapper has to open HERE so the WITH stays at the top level rather
           than being swallowed into the subquery. */
        var bodyStart = sql.Length;

        /* Whether one fact row's group key is in the rank CTE. IS NOT DISTINCT FROM, not '=': a NULL
           dimension value is a real, rankable group (GROUP BY collects it), and '=' would knock it out of
           its own series the moment it won a top-N slot. The CTE is referenced more than once under
           includeOther, so Postgres materializes it — the rank runs once, the probes hit <= topN rows. */
        string? memberOfTopN = null;
        if (plan.Mode == PanelMode.RankedTimeSeries)
        {
            var comparisons = plan.GroupBy.Select(d => $"t.{d.Name} IS NOT DISTINCT FROM {GroupRef(d)}");
            memberOfTopN = $"EXISTS (SELECT 1 FROM {RankCte} AS t WHERE {string.Join(" AND ", comparisons)})";
        }

        /* SELECT list + the matching GROUP BY expressions. */
        var selectExprs = new List<string>();
        var groupExprs = new List<string>();

        if (plan.Mode is PanelMode.TimeSeries or PanelMode.RankedTimeSeries)
        {
            var bucketExpr = $"date_trunc('{MeasureCatalog.DateTruncField(effectiveBucket)}', {FactAlias}.{timeColumn})";
            selectExprs.Add(bucketExpr + " AS bucket");
            groupExprs.Add(bucketExpr);
        }

        foreach (var dim in plan.GroupBy)
        {
            /* includeOther (#2734): the residual fold. Every non-top-N row keeps contributing, relabeled
               into the one "(other)" series (all its dim columns take the label), so the chart's buckets
               still sum to the window total. Without it, non-members are filtered out below instead. */
            var expr = plan.Mode == PanelMode.RankedTimeSeries && plan.IncludeOther
                ? $"CASE WHEN {memberOfTopN} THEN {GroupRef(dim)} ELSE '{OtherSeriesLabel}' END"
                : GroupRef(dim);
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
        AppendFactBody(string.Empty);

        if (plan.Mode == PanelMode.RankedTimeSeries && !plan.IncludeOther)
        {
            /* No residual requested: non-top-N rows are filtered out entirely (the chart under-reports the
               window total by exactly what they did — the includeOther fold is the honest-total option). */
            sql.Append("  AND ").Append(memberOfTopN).Append('\n');
        }

        if (groupExprs.Count > 0)
        {
            sql.Append("GROUP BY ").Append(string.Join(", ", groupExprs)).Append('\n');
        }

        switch (plan.Mode)
        {
            case PanelMode.TimeSeries:
            case PanelMode.RankedTimeSeries:
                /* The cap keeps the NEWEST buckets, not the oldest (#1687). A plain
                   "ORDER BY bucket LIMIT n" silently returns the EARLIEST n rows, so a grouped
                   minute-grain panel over 24h rendered its first ~87 minutes as though that were the
                   whole window — numbers right, window wrong, and nothing on screen said so. Recent
                   data is the point of a monitoring chart, so the cap is applied DESC inside a
                   subquery and the survivors re-sorted ascending for the renderer, which consumes
                   buckets in order.

                   No parameter-ordering hazard: this LIMIT is a literal (only the Ranked arm and the
                   RankedTimeSeries rank CTE bind a LIMIT parameter, and the CTE's is appended before the
                   wrapper exists), and the wrapper only brackets text whose parameters were already
                   appended in the same order, so $n positions are untouched. */
                sql.Insert(bodyStart, "SELECT * FROM (\n");
                sql.Append("ORDER BY bucket DESC\n");
                sql.Append("LIMIT ").Append(ComposeLimits.HardRowCap).Append('\n');
                sql.Append(") AS capped\n");
                sql.Append("ORDER BY bucket");
                break;
            case PanelMode.Ranked:
                /* NULLS LAST for the same reason as the rank CTE above: DESC's default NULLS FIRST put a
                   NULL-aggregate group at the TOP of the ranking, ahead of every real value — and under
                   the LIMIT it evicted a legitimate member. */
                sql.Append("ORDER BY value DESC NULLS LAST\n");
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
    ///
    /// <para><paramref name="serverClocks"/> is what <see cref="ReadServerClocksAsync"/> returned for
    /// <see cref="CompileServerClockRead"/>, keyed by server name. Only a
    /// <see cref="AnnotationClockFrame.ServerLocal"/> source uses it; pass <see cref="NoServerClocks"/> when the
    /// panel has none. A server missing from it reads as UTC.</para>
    /// </summary>
    public static IReadOnlyList<(string Source, ComposeCompiled Compiled)> CompileAnnotations(
        PanelPlan plan, ComposeRunContext context, IReadOnlyDictionary<string, ServerClock> serverClocks)
    {
        if (plan is null)
        {
            throw new ArgumentNullException(nameof(plan));
        }

        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        if (serverClocks is null)
        {
            throw new ArgumentNullException(nameof(serverClocks));
        }

        if (plan.Annotations.Count == 0)
        {
            return Array.Empty<(string, ComposeCompiled)>();
        }

        var results = new List<(string, ComposeCompiled)>(plan.Annotations.Count);
        foreach (var source in plan.Annotations)
        {
            results.Add((source.Key, CompileAnnotation(source, context, serverClocks)));
        }

        return results;
    }

    /// <summary>No server clock at all: every server-local marker reads as UTC. For a panel with no
    /// server-local annotation source, which needs no clock read.</summary>
    public static readonly IReadOnlyDictionary<string, ServerClock> NoServerClocks =
        new Dictionary<string, ServerClock>(StringComparer.Ordinal);

    /// <summary>
    /// The read that runs before a panel's <see cref="AnnotationClockFrame.ServerLocal"/> annotation query
    /// (#4821): each server's newest <c>server_properties</c> row that has an offset, with the
    /// <c>time_zone_id</c> from that SAME row, the row <c>DarlingServerClockReader</c> reads for one server.
    ///
    /// <para><c>server_properties</c> is indexed <c>(server_id, collection_time)</c> and NOT on
    /// <c>server_name</c>, so the <c>DISTINCT ON</c> sort has no index to ride. When the panel names its
    /// servers, the same list the annotation query filters on bounds it, so the sort covers the requested
    /// servers instead of the whole fleet's retained history. A fleet-wide panel names none and reads every
    /// server, as the old in-query join did.</para>
    /// </summary>
    public static ComposeCompiled CompileServerClockRead(ComposeRunContext context)
    {
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        var p = new ParamList();
        var sql = new StringBuilder();
        sql.Append("SELECT DISTINCT ON (server_name) server_name, time_zone_id, utc_offset_minutes\n");
        sql.Append("FROM ").Append(PgSchemaGenerator.CollectSchema).Append(".server_properties\n");
        sql.Append("WHERE utc_offset_minutes IS NOT NULL\n");
        if (context.Servers is { Count: > 0 })
        {
            sql.Append("AND   server_name = ANY(").Append(p.AddTextArray(context.Servers)).Append(")\n");
        }

        sql.Append("ORDER BY server_name, collection_time DESC");

        return new ComposeCompiled(sql.ToString(), p.Parameters, ComposeRoute.Raw);
    }

    /// <summary>
    /// One <see cref="ServerClock"/> per server from the rows of <see cref="CompileServerClockRead"/>, through
    /// <see cref="ServerClock.Resolve"/>: the zone where the server reports one (SQL Server 2022 and later) and
    /// it resolves on this machine, else the fixed offset. Servers with the same zone and offset share one
    /// clock, so <see cref="ServerLocalRanges"/> works each distinct clock out once.
    /// </summary>
    public static async Task<IReadOnlyDictionary<string, ServerClock>> ReadServerClocksAsync(
        DbDataReader reader, CancellationToken cancellationToken)
    {
        if (reader is null)
        {
            throw new ArgumentNullException(nameof(reader));
        }

        var clocks = new Dictionary<string, ServerClock>(StringComparer.Ordinal);
        var shared = new Dictionary<(string?, int), ServerClock>();
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.IsDBNull(0) || reader.IsDBNull(2))
            {
                continue;
            }

            var key = (reader.IsDBNull(1) ? null : reader.GetString(1), reader.GetInt32(2));
            if (!shared.TryGetValue(key, out var clock))
            {
                clock = ServerClock.Resolve(key.Item1, key.Item2);
                shared[key] = clock;
            }

            clocks[reader.GetString(0)] = clock;
        }

        return clocks;
    }

    /// <summary>One stretch of a server's local time over which <see cref="ServerClock.ToUtc"/> subtracts one
    /// offset: <c>[LocalFrom, LocalTo)</c>.</summary>
    internal readonly record struct ServerLocalRange(string ServerName, DateTime LocalFrom, DateTime LocalTo, int UtcOffsetMinutes);

    /// <summary>
    /// Every server's clock as stretches of its local time, each with the ONE offset
    /// <see cref="ServerClock.ToUtc"/> subtracts across it (#4821), in server-name order. A zone gets a new
    /// stretch at each daylight saving change; a fixed offset gets one stretch.
    ///
    /// <para>Each stretch boundary is where <see cref="ServerClock.ToUtc"/> itself changes offset, found by
    /// stepping a day at a time and halving to the tick, so the stretches reproduce it exactly, including the
    /// repeated hour (first occurrence) and the skipped hour (read forward by the gap). Stepping a day at a
    /// time would miss two changes less than a day apart; no zone has them.</para>
    ///
    /// <para>The stretches cover <c>[start - 1 day, end + 1 day)</c> of local time. No zone is more than 14
    /// hours from UTC, so that holds every local time that can land in the window; a row outside it matches
    /// no stretch and falls back to UTC, which leaves it outside the window as well.</para>
    /// </summary>
    internal static IReadOnlyList<ServerLocalRange> ServerLocalRanges(
        IReadOnlyDictionary<string, ServerClock> serverClocks, DateTime startUtc, DateTime endUtc)
    {
        var from = ShiftDays(startUtc, -1);
        var to = ShiftDays(endUtc, 1);
        var byClock = new Dictionary<ServerClock, List<(DateTime From, DateTime To, int Offset)>>();
        var ranges = new List<ServerLocalRange>();
        foreach (var (server, clock) in serverClocks.OrderBy(c => c.Key, StringComparer.Ordinal))
        {
            if (!byClock.TryGetValue(clock, out var stretches))
            {
                stretches = Stretches(clock, from, to);
                byClock[clock] = stretches;
            }

            foreach (var (stretchFrom, stretchTo, offset) in stretches)
            {
                ranges.Add(new ServerLocalRange(server, stretchFrom, stretchTo, offset));
            }
        }

        return ranges;
    }

    private static List<(DateTime From, DateTime To, int Offset)> Stretches(ServerClock clock, DateTime from, DateTime to)
    {
        var stretches = new List<(DateTime, DateTime, int)>();
        var stretchFrom = from;
        var offset = LocalOffsetMinutes(clock, from);
        var probe = from;
        while (probe < to)
        {
            var next = to - probe > TimeSpan.FromDays(1) ? probe.AddDays(1) : to;
            if (LocalOffsetMinutes(clock, next) == offset)
            {
                probe = next;
                continue;
            }

            /* The first tick in (probe, next] on the new offset. */
            long before = probe.Ticks, after = next.Ticks;
            while (after - before > 1)
            {
                var middle = before + (after - before) / 2;
                if (LocalOffsetMinutes(clock, new DateTime(middle, DateTimeKind.Unspecified)) == offset)
                {
                    before = middle;
                }
                else
                {
                    after = middle;
                }
            }

            var change = new DateTime(after, DateTimeKind.Unspecified);
            stretches.Add((stretchFrom, change, offset));
            stretchFrom = change;
            offset = LocalOffsetMinutes(clock, change);
            probe = change;
        }

        if (stretchFrom < to)
        {
            stretches.Add((stretchFrom, to, offset));
        }

        return stretches;
    }

    /// <summary>The offset <see cref="ServerClock.ToUtc"/> subtracts from this local time, in minutes.</summary>
    private static int LocalOffsetMinutes(ServerClock clock, DateTime local) =>
        (int)((local.Ticks - clock.ToUtc(local).Ticks) / TimeSpan.TicksPerMinute);

    /// <summary>A naive time moved by whole days, held at the ends of the calendar instead of throwing.</summary>
    private static DateTime ShiftDays(DateTime value, int days) =>
        new(Math.Clamp(value.Ticks + (days * TimeSpan.TicksPerDay), DateTime.MinValue.Ticks, DateTime.MaxValue.Ticks), DateTimeKind.Unspecified);

    /// <summary>The <see cref="ServerLocalRanges"/> stretches as bound arrays, joined in for a
    /// <see cref="AnnotationClockFrame.ServerLocal"/> source on the row's server AND its own local time, so
    /// each row takes the offset in force on its own date. Every value is bound and every identifier is a
    /// compiler constant. The stretches do not overlap, so a row joins at most one and no row is repeated.</summary>
    private static string ServerLocalRangeJoin(ParamList p, string timeColumn, IReadOnlyList<ServerLocalRange> ranges)
    {
        var servers = p.AddTextArray(ranges.Select(r => r.ServerName).ToArray());
        var localFrom = p.AddTimestampArray(ranges.Select(r => r.LocalFrom).ToArray());
        var localTo = p.AddTimestampArray(ranges.Select(r => r.LocalTo).ToArray());
        var offsets = p.AddIntArray(ranges.Select(r => r.UtcOffsetMinutes).ToArray());

        return "LEFT JOIN unnest(" + servers + "::text[], " + localFrom + "::timestamp[], " + localTo + "::timestamp[], "
            + offsets + "::integer[]) AS o(server_name, local_from, local_to, utc_offset_minutes)\n"
            + "        ON  o.server_name = " + FactAlias + ".server_name\n"
            + "        AND " + FactAlias + "." + timeColumn + " >= o.local_from\n"
            + "        AND " + FactAlias + "." + timeColumn + " < o.local_to\n";
    }

    /// <summary>Compiles one annotation source into its bounded, catalog-only, schema-qualified event query:
    /// <c>SELECT &lt;ts&gt; AS ts, f.&lt;labelCol&gt; AS label FROM collect.&lt;table&gt; AS f WHERE
    /// &lt;ts&gt; BETWEEN $1 AND $2 [AND f.server_name = ANY($3)] ORDER BY ts LIMIT
    /// &lt;MaxAnnotationEvents&gt;</c>. Every identifier is a catalog constant; every value is bound.
    ///
    /// <para><c>&lt;ts&gt;</c> is the bare <c>f.&lt;timeCol&gt;</c> for a UTC source and the de-skewed
    /// <c>f.&lt;timeCol&gt; - make_interval(mins =&gt; COALESCE(o.utc_offset_minutes, 0))</c> for a
    /// <see cref="AnnotationClockFrame.ServerLocal"/> one, and the SAME expression both returns and bounds —
    /// the measure query it decorates buckets on naive-UTC <c>collection_time</c>, so a server-local marker
    /// would be selected from the wrong slice and drawn at the wrong x-position. <c>o</c> is
    /// <see cref="ServerLocalRangeJoin"/>: the offset in force at the row's own local time (#4821), where it
    /// used to be the server's one newest offset, which put a marker from before the last daylight saving
    /// change an hour off. <c>LEFT JOIN</c> with <c>COALESCE(..., 0)</c> keeps a server whose
    /// <c>server_properties</c> has not been collected yet: treating its clock as UTC is the same fallback the
    /// reader-side de-skews take, and it beats dropping the server's markers with no explanation.</para></summary>
    private static ComposeCompiled CompileAnnotation(
        ComposeAnnotationSource source, ComposeRunContext context, IReadOnlyDictionary<string, ServerClock> serverClocks)
    {
        var p = new ParamList();
        var startParam = p.AddTimestamp(context.StartUtc);
        var endParam = p.AddTimestamp(context.EndUtc);
        var hasServerScope = context.Servers is { Count: > 0 };
        var serverScopeParam = hasServerScope ? p.AddTextArray(context.Servers!) : null;
        var serverLocal = source.Frame == AnnotationClockFrame.ServerLocal;

        var ts = serverLocal
            ? $"{FactAlias}.{source.TimeColumn} - make_interval(mins => COALESCE(o.utc_offset_minutes, 0))"
            : $"{FactAlias}.{source.TimeColumn}";

        var sql = new StringBuilder();
        sql.Append("SELECT ").Append(ts).Append(" AS ts, ")
            .Append(FactAlias).Append('.').Append(source.LabelColumn).Append(" AS label\n");
        sql.Append("FROM ").Append(PgSchemaGenerator.CollectSchema).Append('.').Append(source.SourceTable)
            .Append(" AS ").Append(FactAlias).Append('\n');
        if (serverLocal)
        {
            var ranges = ServerLocalRanges(serverClocks, context.StartUtc, context.EndUtc);
            sql.Append("      ").Append(ServerLocalRangeJoin(p, source.TimeColumn, ranges));
        }

        sql.Append("WHERE ").Append(ts).Append(" >= ").Append(startParam).Append('\n');
        sql.Append("  AND ").Append(ts).Append(" <= ").Append(endParam).Append('\n');
        if (hasServerScope)
        {
            sql.Append("  AND ").Append(FactAlias).Append(".server_name = ANY(").Append(serverScopeParam).Append(")\n");
        }

        sql.Append("ORDER BY ts\n");
        sql.Append("LIMIT ").Append(ComposeLimits.MaxAnnotationEvents);

        return new ComposeCompiled(sql.ToString(), p.Parameters, ComposeRoute.Raw);
    }

    /// <summary>The qualified reference for a dimension column: <c>f.</c> for a fact column; for a
    /// module-join dimension, the <c>m.</c> column with its NULL misses FOLDED (#2737) — never the bare
    /// joined column.
    ///
    /// <para>The module LEFT JOIN misses every ad-hoc statement, and a bare <c>m.object_name</c> made
    /// those NULLs the dimension's value: <c>GROUP BY</c> collapsed all ad-hoc SQL into one null-named
    /// row, <c>neq</c> (<c>&lt;&gt; ALL</c>) silently excluded the whole ad-hoc population, and nothing
    /// could filter TO it. Folding here — the ONE place both the grouped expression and every filter
    /// compile through — gives the dimension a never-NULL value with ordinary text semantics for every
    /// operator: <c>neq 'X'</c> now INCLUDES ad-hoc rows ("everything except X" means the rest of the
    /// workload, not "every other procedure"), <c>eq/neq AdHocLabel</c> select/exclude the bucket
    /// explicitly, and LIKE patterns match the label like any other value. The fallback is the
    /// dimension's own <c>FallbackColumn</c> when declared (<c>statement</c> → per-hash identity), else
    /// the AdHocLabel sentinel — a compile-time catalog constant emitted as a literal, never a caller
    /// string.</para></summary>
    private static string ColumnRef(ComposeDimension dimension)
    {
        if (!dimension.ViaModuleJoin)
        {
            return FactAlias + "." + dimension.Column;
        }

        var fallback = dimension.FallbackColumn is not null
            ? FactAlias + "." + dimension.FallbackColumn
            : "'" + MeasureCatalog.AdHocLabel + "'";
        return $"COALESCE({ModuleAlias}.{dimension.Column}, {fallback})";
    }

    /// <summary>The expression a dimension GROUPS on: <see cref="ColumnRef"/>, trimmed for a
    /// <see cref="ComposeDimension.TrailingSpaceHistory"/> dimension so both stored spellings of a wait name are
    /// one group under the clean name. Filters never use it: they keep the column bare and widen the value
    /// instead (<see cref="BuildFilterClause"/>).</summary>
    private static string GroupRef(ComposeDimension dimension) =>
        dimension.TrailingSpaceHistory ? $"rtrim({ColumnRef(dimension)})" : ColumnRef(dimension);

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
        var measured = MeasuredDeltaFilter(measure);

        var nativeExpr = aggregate switch
        {
            ComposeAggregate.Sum => $"CAST(SUM({qualified}){measured} AS double precision)",
            ComposeAggregate.Avg => $"CAST(AVG({qualified}){measured} AS double precision)",
            ComposeAggregate.Min => $"CAST(MIN({qualified}){measured} AS double precision)",
            ComposeAggregate.Max => $"CAST(MAX({qualified}){measured} AS double precision)",
            ComposeAggregate.PercentileCont =>
                $"percentile_cont({FormatDouble(ComposeLimits.DefaultPercentile)}) WITHIN GROUP (ORDER BY {qualified})",
            _ => throw new InvalidOperationException($"Unhandled aggregate {aggregate}"),
        };

        return ApplyUnitConversion(nativeExpr, measure.UnitFamily, measure.NativeUnit, unit);
    }

    /// <summary>
    /// The predicate every row's <c>sample_interval_seconds</c> must pass before its delta joins an aggregate
    /// (#3653, the Compose Cumulative-archetype item; #2234 / #3540 for the contract it enforces). Emitted as
    /// the aggregate's own <c>FILTER (WHERE …)</c> clause — parameter-free, so it cannot shift the $n order the
    /// filter clauses bind in.
    /// </summary>
    private const string MeasuredDeltaPredicate = "sample_interval_seconds IS DISTINCT FROM 0";

    /// <summary>
    /// The <c>FILTER (WHERE f.sample_interval_seconds IS DISTINCT FROM 0)</c> clause a raw-tier aggregate over a
    /// per-interval DELTA column carries, or the empty string when the measure is not one.
    ///
    /// <para><b>Why a row filter at all.</b> A delta-family collector stores, beside every row's <c>delta_*</c>
    /// columns, the interval those deltas were measured over — and stores <c>0</c> for a row it could NOT
    /// difference (first sighting, counter reset, a gap past the delta policy; in practice a service restart),
    /// with a <c>0</c> delta beside it. That pair is a marker meaning "nothing knowable", not a measurement of
    /// nothing (the #2234 contract: 0 is the unknowable marker, NULL a pre-upgrade row, <c>n</c> measured).
    /// <c>SUM(delta)</c> is indifferent to it (adding 0), but <c>AVG(delta)</c> averaged the marker in as a
    /// measured zero — a per-sample mean dragged toward 0 by every restart in the window — and <c>MIN(delta)</c>
    /// read 0 at exactly the sample that measured nothing, every time a restart sat inside the window. The
    /// rest of the read layer already excludes the marker (<c>NULLIF(sample_interval_seconds, 0)</c> in the
    /// rate reads, <c>IS DISTINCT FROM 0</c> in the file-IO and PG trend aggregates); since V128 all ten
    /// delta families carry the column, so the Compose compiler can say it once for every delta aggregate.</para>
    ///
    /// <para><b>Which measures.</b> Any scalar measure whose aggregated column is a per-interval delta on a
    /// source that stores the interval: the <see cref="MeasureArchetype.Cumulative"/> measures (their
    /// <c>DeltaColumn</c> is the delta) and the <see cref="MeasureArchetype.Delta"/> measures that read a
    /// <c>delta_*</c> column as a first-class measure on the SAME tables (<c>wait_time_delta_ms</c> compiles to
    /// the very column <c>wait_time_ms</c> does — filtering one and not the other would have two names for one
    /// column disagree about one row). "Stores the interval" is <see cref="CollectorDeltaCalculator.IsDeltaFamily"/>
    /// — the collector set the delta calculator stamps, which <c>DeltaFamilyIntervalColumnTests</c> pins carries
    /// <c>sample_interval_seconds</c> in both directions — rather than a second list here that could drift from
    /// it. Query Store's <c>qs_executions</c> is a Delta measure on a table OUTSIDE that set (a per-interval
    /// snapshot with no interval column), so it compiles unfiltered, as it must: naming an absent column fails
    /// at parse time. Gauges, PerEvent columns, ratios and <c>COUNT(*)</c> are untouched — a gauge read at a
    /// restart pass is a real reading, and a row is a row.</para>
    ///
    /// <para><b>Why the aggregate's FILTER and not the statement's WHERE.</b> One compiled statement aggregates
    /// its primary measure and its overlay (#1606) over the SAME fact rows, and a Cumulative primary can sit
    /// beside a Gauge overlay on one table (<c>memory_grant_stats</c>: <c>grant_timeouts</c> beside
    /// <c>grant_waiters</c>). A WHERE would drop the restart row from BOTH — the gauge's valid reading with the
    /// delta's marker — and a Gauge primary beside a Cumulative overlay would filter neither. The FILTER clause
    /// is scoped to the one aggregate whose column the marker poisons, and the RankedTimeSeries rank CTE
    /// inherits it through the same expression builder, so membership is ranked over measured rows only.</para>
    ///
    /// <para><b>Raw tier only.</b> A CAGG route reads pre-aggregated <c>_sum</c>/<c>_min</c>/<c>_max</c> columns
    /// through <see cref="ComposeCaggValueMapper"/>; the row is gone by then, and excluding the marker from a
    /// continuous aggregate is the aggregate definition's job (#3653's A6 item: a new aggregate with the
    /// predicate baked in, because a CAGG cannot be altered in place).</para>
    /// </summary>
    private static string MeasuredDeltaFilter(ComposeMeasure measure)
    {
        var aggregatesADelta = measure.Archetype switch
        {
            MeasureArchetype.Cumulative => true,
            MeasureArchetype.Delta => true,
            _ => false,
        };

        return aggregatesADelta && CollectorDeltaCalculator.IsDeltaFamily(measure.SourceTable)
            ? $" FILTER (WHERE {FactAlias}.{MeasuredDeltaPredicate})"
            : "";
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

    /// <summary>Builds one filter's SQL predicate, binding every value as a parameter.
    ///
    /// <para>On a <see cref="ComposeDimension.TrailingSpaceHistory"/> dimension the predicate answers as if it
    /// compared the clean name, with the column kept bare: <c>eq</c>/<c>neq</c> also list each value plus one
    /// space, <c>like</c> also tries the pattern plus one space, <c>gt</c> leaves out the spaced spelling of the
    /// bound, and <c>lte</c> takes it in. <c>gte</c> and <c>lt</c> need nothing, because a name with one
    /// trailing space sorts directly after its clean form.</para></summary>
    private static string BuildFilterClause(ComposeFilter filter, ComposeRunContext context, ParamList p)
    {
        var column = ColumnRef(filter.Dimension);
        var values = ResolveValues(filter.Value, context);
        var spaced = filter.Dimension.TrailingSpaceHistory;

        switch (filter.Op)
        {
            case ComposeFilterOp.Eq:
                return $"{column} = ANY({p.AddTextArray(spaced ? WithTrailingSpace(values) : values)})";
            case ComposeFilterOp.Neq:
                return $"{column} <> ALL({p.AddTextArray(spaced ? WithTrailingSpace(values) : values)})";
            case ComposeFilterOp.Like:
            {
                var pattern = p.AddText(First(values));
                return spaced
                    ? $"({column} LIKE {pattern} OR {column} LIKE {pattern} || ' ')"
                    : $"{column} LIKE {pattern}";
            }
            case ComposeFilterOp.Gt:
            {
                var bound = p.AddText(First(values));
                return spaced
                    ? $"({column} > {bound} AND {column} <> {bound} || ' ')"
                    : $"{column} > {bound}";
            }
            case ComposeFilterOp.Gte:
                return $"{column} >= {p.AddText(First(values))}";
            case ComposeFilterOp.Lt:
                return $"{column} < {p.AddText(First(values))}";
            case ComposeFilterOp.Lte:
            {
                var bound = p.AddText(First(values));
                return spaced
                    ? $"({column} <= {bound} OR {column} = {bound} || ' ')"
                    : $"{column} <= {bound}";
            }
            default:
                throw new InvalidOperationException($"Unhandled filter op {filter.Op}");
        }
    }

    /// <summary>Each value as given, then each value with one trailing space: the two spellings a wait name can
    /// be stored under (<see cref="ComposeDimension.TrailingSpaceHistory"/>).</summary>
    private static string[] WithTrailingSpace(IReadOnlyList<string> values) =>
        values.Concat(values.Select(v => v + " ")).ToArray();

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

        public string AddTimestampArray(IReadOnlyList<DateTime> values)
        {
            _parameters.Add(new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp,
                Value = values.Select(v => DateTime.SpecifyKind(v, DateTimeKind.Unspecified)).ToArray(),
            });
            return Placeholder();
        }

        public string AddIntArray(IReadOnlyList<int> values)
        {
            _parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = values.ToArray() });
            return Placeholder();
        }

        private string Placeholder() => "$" + _parameters.Count.ToString(CultureInfo.InvariantCulture);
    }
}
