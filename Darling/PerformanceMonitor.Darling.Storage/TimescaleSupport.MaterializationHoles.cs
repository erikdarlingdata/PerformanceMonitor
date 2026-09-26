/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// THE MATERIALIZATION-HOLE REPAIR (#3653, Q10 — item 9's last clause): at service start, find every bucket
/// range INSIDE a continuous aggregate's materialized span where the source holds rows and the aggregate holds
/// none, and close each with ONE targeted <c>refresh_continuous_aggregate</c> over exactly its bounds.
///
/// <para><b>The hole, read honestly (the <see cref="RetentionTierRouter"/> essay, #3698).</b> A refresh policy
/// re-materializes <c>[now - start_offset, now - end_offset]</c> and nothing else. When the service is down
/// longer than <see cref="HourlyRefreshStartOffset"/>, the first refresh after it resumes opens at
/// <c>now - start_offset</c>, the watermark jumps past everything before that, and the raw collected BETWEEN
/// the last pre-outage refresh's window end and the moment collection stopped — at most <c>end_offset</c> plus
/// one <see cref="HourlyRefreshScheduleInterval"/> of collections, about two hours — is never materialized.
/// The outage itself has no rows, so the aggregate is correctly empty there; what goes missing is the tail. On
/// a <c>materialized_only</c> rollup that tail reads EMPTY under a floor that says it is covered; on a real-time
/// baseline aggregate the tail sits below the watermark and is served by neither branch. Nothing else in the
/// product can see it: the startup backfill and <c>--backfill-rollups</c> both measure the FLOOR, and a floor
/// cannot see a hole above itself. Once the source's retention passes the tail the hole is permanent.</para>
///
/// <para><b>Ruled (Q10): a targeted refresh of the hole, not a wider <c>start_offset</c>.</b> Widening the
/// policy's window would make every hourly refresh forever re-scan the extra span — the cost #3012 measured at
/// 118–175% of cadence when the window was three days — to cover a shape that happens once per outage. A
/// refresh over exactly the hole's bounds costs the hole's buckets once, and only on the start that finds them.</para>
///
/// <para><b>Detection is by shape, not by ledger.</b> The store keeps no collection-run ledger cheap enough to
/// read the outage from, so the scan asks the two relations directly: for every bucket from the aggregate's
/// materialized floor (or the source's horizon, whichever is later) up to its last materialized bucket, does the
/// materialization hold a row for it, and does the source hold a row that the aggregate's own <c>WHERE</c>
/// would admit? Both are index probes — every hypertable carries its time index and every materialization
/// its <c>bucket</c> index — so the scan is a few hundred probes per aggregate whatever the tables weigh. The
/// scan's SQL has to fence them to keep them that way (#3933, <see cref="MaterializationHoleScanSql"/>): written
/// bare, the planner joined each one over its whole relation.
/// The aggregate's WHERE is applied to the source probe (<see cref="MaterializationHoleSourceFilterFor"/>) so a
/// bucket whose every source row the aggregate rejects — a restart hour on an interval-honest successor — is
/// not read as a hole and refreshed on every start for nothing. Buckets below the floor are the backfill's
/// business and are never touched here; buckets past the last materialized one are the live edge the policy
/// owns.</para>
///
/// <para><b>The seam case (#4186), lowered to a CONTIGUOUS DOWNWARD FILL by #4301's ruling.</b> The three
/// interval-honest successors (<c>SupersededHourlyRollups</c>) can each have an un-materialized span BELOW
/// their own floor: an outage that outlasts <see cref="HourlyRefreshStartOffset"/> before the successor's
/// first refresh leaves raw rows between the frozen legacy's last bucket and the successor's floor that
/// neither side ever materialized (the outage shape worked through above) — and, separately, an outage that
/// predates this store's freeze can leave a hole INSIDE the legacy's own already-materialized span that
/// nothing ever re-scans, because the six frozen views are excluded from <see cref="MaterializationHoleTargets"/>.
/// Both sit BELOW the successor's floor, so the ordinary "floor up to ceiling" scan never reaches them — a
/// hole, by this pass's own definition, has to be inside the materialized span. For a successor found through
/// <c>LegacyOf</c>, <see cref="MaterializationHoleScanWindows"/> adds a seam window down to RAW's own filtered
/// floor — not merely the legacy's last bucket — whenever that reaches further back than the successor's floor
/// already does — UNCLAMPED by the horizon that still bounds the ordinary window, because an outage longer than
/// the horizon's own span is exactly the shape that needs repairing, not a shape to skip (an earlier cut folded
/// the seam into that same horizon clamp, and a seam older than the horizon was silently never scanned — see
/// <see cref="MaterializationHoleScanWindows"/>'s own doc for that history). Filling all the way to raw's floor
/// rather than stopping at the legacy's last bucket is the point of the #4301 ruling: <c>RollupCoverage.StitchedRelationSql</c>
/// splits its read at the successor's own floor, so that floor has to be contiguous with everything raw still
/// admits for the stitch to read every row exactly once. The seam window then reads as an ordinary hole and
/// the existing machinery repairs it: bounded per start, NEWEST FIRST (the successor's own invariant — see the
/// H1 note below), filter-aware — a span wider than one start's cap (<see cref="MaterializationHoleRepairCapBuckets"/>)
/// takes more than one start to close in full, but every start makes progress on it, walking downward from the
/// successor's floor toward raw's. Once repaired, the successor's floor covers the seam on its own and
/// <see cref="RetentionArmSafetySql"/>'s seam probe — which exists because this stitch is NOT gap-free by
/// construction — finds nothing there and releases the raw purge gate automatically, with no manual step,
/// bounded only by that same per-start repair cap.</para>
///
/// <para><b>Bounded, and the bound is stated.</b> Per aggregate per start, at most one refresh policy window's
/// worth of buckets (<see cref="MaterializationHoleRepairCapBuckets"/>: 24 hourly, 3 daily) is refreshed,
/// OLDEST FIRST — the oldest hole is the one the source's retention is about to make permanent. Anything past
/// the cap is logged with its bounds and left for the next start. Every hole repaired is one INFORMATION line
/// with its bounds, its bucket count, its duration, which refresh closed it and what the re-scan found
/// afterwards; failure is isolated per aggregate, the #1775 shape. The pass does NOT write its own
/// end-of-pass summary: it RETURNS its tally (<see cref="MaterializationHoleRepairSummary"/>) and the
/// worker writes the ONE summary line per start, unconditionally (#3756). The first cut wrote a summary
/// here only when something happened and a Debug line otherwise, so the first store to carry the scan
/// produced a log in which a start that found nothing, a start whose scan threw before its first probe and
/// a start that never reached the scan read identically; the line that proves the scan RAN belongs to the
/// caller that knows what a start is and owns the other two outcomes (a connection that would not open, a
/// shutdown that cut it short).</para>
///
/// <para><b>Plain refresh first, FORCED only on a measured remainder — the backfill's own escalation shape
/// (<see cref="RollupBackfill.RunSliceAsync"/> then <see cref="RollupBackfill.RepairAsync"/>), and the order was
/// settled on the rig rather than argued.</b> The first cut of this pass went straight to the forced refresh on
/// the reasoning that the tail's rows were inserted ABOVE the invalidation threshold their day's refresh had set
/// and so were never logged, leaving a plain refresh nothing to do. Measured on TimescaleDB 2.28.1, that is
/// FALSE for the outage shape: when a refresh advances the threshold past a region it did not cover, the engine
/// records that region as invalid, and a plain refresh over the tail materializes it. So the plain form runs
/// first — it exists on every TimescaleDB version, and on the outage shape it is the whole repair. The re-scan
/// then decides: a hole that survives a plain refresh has no invalidation behind it (a materialization written
/// and lost, a refresh cut short after it consumed its entries — the trap <see cref="RollupBackfill.RefreshSliceSql"/>
/// documents), and only that hole gets the forced refresh, which is the one case <c>force</c> is for and the
/// one case a pre-2.18 store cannot repair (its 42883 is caught per aggregate and reported). Both paths are
/// exercised live, each on the shape that needs it. Every refresh carries the backfill's 55P03 retry for a
/// policy run landing on the same aggregate and its <see cref="RefreshDisclosure"/> for the <c>options</c>
/// parameter.</para>
///
/// <para><b>Dependency order, because a hole propagates down the tier.</b> A hierarchical daily reads its
/// hourly, so an hourly hole is a daily hole too, and the daily's own refresh window (three days) is no wider
/// against an outage than the hourly's. The targets are walked in <see cref="RollupBackfill.Targets"/> order —
/// every raw-sourced rollup before the rollups that read it — followed by the baseline aggregates, so a daily's
/// scan runs after its hourly has been repaired and sees the rows it needs.</para>
///
/// <para><b>Launched, not awaited.</b> The scan itself is cheap and starts the moment the ensure sweep has
/// created every aggregate; the repairs are bounded but a full cap on the heaviest aggregate is a policy run's
/// worth of work, which is minutes on the largest store. So the worker launches this the way it launches the
/// baseline backfill (#1757) — its own connection, concurrent with the rest of startup, drained at shutdown —
/// rather than holding a restarted service dark for it. Ordering against the retention policies (i.e. running
/// this scan before or after <see cref="EnsureRetentionPoliciesAsync"/> on the SAME start) is not load-bearing
/// under #4299's design (variant d′): the raw purge no longer runs on its own schedule at all — the three raw
/// jobs stay permanently unscheduled, and the only thing that ever triggers a purge is the service's own
/// hourly Periodic pass, gated on a repair already finished under the CURRENT
/// <c>pg_postmaster_start_time()</c> (<see cref="RetentionArmSafetySql"/> states the gate in full). Since that
/// trigger cannot fire before the NEXT hourly tick, it can never race this start's own hole scan no matter
/// which of the two the Startup pass launches first. Two things still bypass the service's own gate, named
/// here rather than treated as a leak: the FIRST start after the upgrade, which still runs whichever raw job
/// the OLD scheduled-based code had already armed, once (Low L2 — 3.8.0 parity for that one run only, before
/// this store has converged to the never-scheduled shape); and a DBA's own <c>alter_job</c>/<c>run_job</c>
/// against a raw job, which always executes immediately like any other job in the catalog and is reverted by
/// the very next hourly converge. Neither exception changes what bounds the ordinary race: a hole is
/// repairable for exactly as long as its source holds the rows.</para>
/// </summary>
public static partial class TimescaleSupport
{
    /// <summary>One aggregate the hole scan walks: the view, the relation its CREATE selects FROM, that
    /// relation's time column, the bucket width, and the CREATE text the source filter is read from.</summary>
    public readonly record struct MaterializationHoleTarget(
        string View, string Source, string SourceTimeColumn, TimeSpan BucketWidth, string CreateSql);

    /// <summary>
    /// The aggregates the repair walks, in dependency order: <see cref="RollupBackfill.Targets"/> (every
    /// raw-sourced rollup before the rollups that read it) followed by <see cref="BaselineAggregates"/>, each
    /// paired with its shipped CREATE so the source filter and the group key are read from the definition rather
    /// than restated. Derived, never hand-listed — an aggregate registered for creation is scanned for holes the
    /// same moment.
    /// </summary>
    public static IReadOnlyList<MaterializationHoleTarget> MaterializationHoleTargets =>
        RollupBackfill.Targets
            .Select(t => new MaterializationHoleTarget(
                t.View, t.Source, t.SourceTimeColumn, t.BucketWidth,
                HourlyAggregates.Concat(DailyAggregates).Single(a => string.Equals(a.View, t.View, StringComparison.Ordinal)).CreateSql))
            .Concat(BaselineAggregates.Select(a => new MaterializationHoleTarget(
                a.View, SourceTableFor(a.View), "collection_time", HourlyBucket, a.CreateSql)))
            .ToArray();

    /// <summary>
    /// #3653 A6, lane LC-a4: the same per-relation descriptor as <see cref="MaterializationHoleTargets"/>, but
    /// over EVERY member of <see cref="RollupViews"/> — including the six the freeze (LC) took out of
    /// <see cref="RollupBackfill.Targets"/>, and so out of <see cref="MaterializationHoleTargets"/> too. The
    /// daily summary's not-carried probe (<c>DailySummarySql.QueriesCteForCagg</c> and
    /// <c>QueriesCteForStitchedCagg</c>) still names a frozen legacy rollup long after LC stops it advancing —
    /// under <c>RollupCoverage.Unknown</c>, and below a successor's stitch floor — so it needs a lookup that
    /// still knows one, while the repair walk (<see cref="RepairMaterializationHolesAsync"/>) must never see a
    /// frozen view among ITS targets: refreshing one is the one thing the freeze forbids. Kept as a SEPARATE
    /// list rather than folded into <see cref="MaterializationHoleTargets"/> so that list's membership and
    /// dependency order — a repair-walk invariant — stay exactly as the freeze left them. Its CREATE text comes
    /// from <see cref="HourlyAggregates"/>, <see cref="DailyAggregates"/> OR <see cref="FrozenRollupAggregates"/>
    /// — together the three hold exactly one entry per <see cref="RollupViews"/> member, frozen or not, so the
    /// lookup below cannot go ambiguous or come up empty for any relation this file knows by name.
    /// </summary>
    public static IReadOnlyList<MaterializationHoleTarget> RollupCoverageProbeTargets =>
        RollupViews
            .Select(r => new MaterializationHoleTarget(
                r.View, r.Source, r.SourceTimeColumn, r.BucketWidth,
                HourlyAggregates.Concat(DailyAggregates).Concat(FrozenRollupAggregates)
                    .Single(a => string.Equals(a.View, r.View, StringComparison.Ordinal)).CreateSql))
            .Concat(BaselineAggregates.Select(a => new MaterializationHoleTarget(
                a.View, SourceTableFor(a.View), "collection_time", HourlyBucket, a.CreateSql)))
            .ToArray();

    /// <summary>
    /// How many buckets one aggregate may have repaired per start: its own refresh policy's window in buckets —
    /// <see cref="HourlyRefreshStartSpan"/> over <see cref="HourlyBucket"/> (24) for an hourly aggregate,
    /// <see cref="DailyRefreshStartSpan"/> over <see cref="DailyBucket"/> (3) for a daily. So a start never does
    /// more re-materialization for one aggregate than one ordinary policy run would, which is the cost every
    /// hour already pays; a hole wider than that is repaired across as many starts as it takes, oldest first,
    /// and each start says what it left.
    /// </summary>
    public static int MaterializationHoleRepairCapBuckets(TimeSpan bucketWidth)
    {
        if (bucketWidth <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(bucketWidth), bucketWidth, "a bucket has a positive width");
        }

        var window = bucketWidth >= DailyBucket ? DailyRefreshStartSpan : HourlyRefreshStartSpan;
        return Math.Max(1, (int)(window.Ticks / bucketWidth.Ticks));
    }

    /// <summary>
    /// How far back from the aggregate's last materialized bucket the scan reaches: as far as the SOURCE still
    /// holds rows, because a hole whose source rows have been purged cannot be repaired and a probe over
    /// purged chunks is wasted. Read from <see cref="RetentionPolicies"/> for a source that has a policy there
    /// (the three rolled raw tables and every rollup that is itself a source); the collector-purged raw
    /// tables the baseline aggregates read have no store-side policy, so they take
    /// <see cref="BaselineRetentionSpan"/>, the longest horizon a baseline-tier read has ever reached.
    /// </summary>
    public static TimeSpan MaterializationHoleScanSpanFor(string source)
    {
        foreach (var (relation, dropAfter, _, _) in RetentionPolicies)
        {
            if (!string.Equals(relation, source, StringComparison.Ordinal))
            {
                continue;
            }

            if (string.Equals(dropAfter, RawRetentionInterval, StringComparison.Ordinal)) return RawRetentionSpan;
            if (string.Equals(dropAfter, HourlyRetentionInterval, StringComparison.Ordinal)) return HourlyRetentionSpan;
            if (string.Equals(dropAfter, IntervalRetentionInterval, StringComparison.Ordinal)) return IntervalRetentionSpan;
            if (string.Equals(dropAfter, IntervalDailyRetentionInterval, StringComparison.Ordinal)) return IntervalDailyRetentionSpan;
            if (string.Equals(dropAfter, BaselineRetentionInterval, StringComparison.Ordinal)) return BaselineRetentionSpan;

            throw new InvalidOperationException(
                $"{relation}'s retention horizon '{dropAfter}' has no TimeSpan twin the hole scan knows — add the pair here when a new tier is added");
        }

        return BaselineRetentionSpan;
    }

    /// <summary>
    /// The aggregate's own row filter, as text to append to the source probe — the <c>WHERE</c> clause of its
    /// CREATE between <c>WHERE</c> and <c>GROUP BY</c>, or empty for a definition with none. Applied so the scan
    /// asks "does the source hold a row this aggregate would produce output from", not merely "does the source
    /// hold a row": an hour in which every collection was a restart holds rows an interval-honest successor
    /// rejects, and reading it as a hole would refresh it on every start and report a repair that repaired
    /// nothing. The clause is taken verbatim, so a predicate the aggregate applies the probe applies.
    /// </summary>
    public static string MaterializationHoleSourceFilterFor(string createSql)
    {
        ArgumentNullException.ThrowIfNull(createSql);

        var match = Regex.Match(createSql, @"\bWHERE\b(.*?)\bGROUP BY\b", RegexOptions.Singleline);
        if (!match.Success)
        {
            return string.Empty;
        }

        var clause = string.Join(" ", match.Groups[1].Value.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
        return clause.Length == 0 ? string.Empty : clause;
    }

    /// <summary>
    /// The scan, for one aggregate: every bucket in <c>[$1, $2]</c> at <c>$3</c> width that the materialization
    /// hypertable holds NO row for and the source holds at least one row for that the aggregate's own filter
    /// admits. Two correlated <c>EXISTS</c> probes per bucket, each an index range on a relation whose time index
    /// TimescaleDB creates by default (the source's <c>&lt;time&gt;_idx</c>, the materialization's
    /// <c>bucket_idx</c>), so cost follows the number of buckets scanned and not the size of either table.
    /// <paramref name="materialization"/> is the aggregate's materialization hypertable
    /// (<see cref="ResolveMaterializationAsync"/>), read directly so a real-time aggregate's un-materialized
    /// tail does not count as covered. Ordered oldest first, the order the cap consumes.
    ///
    /// <para><b>The probes stay probes because the SQL says so (#3933).</b> Written as a bare <c>NOT EXISTS</c>
    /// and <c>EXISTS</c>, PostgreSQL pulls both up into joins, and neither join can push the per-bucket bound into
    /// its scan. The planner made the materialization side a merge anti-join that read the WHOLE materialization
    /// hypertable (every server, its whole retention, decompressing every compressed chunk to sort it by bucket),
    /// and the source side a semi-join over a <c>Materialize</c> of the WHOLE source table. On a healthy store
    /// the second never runs, because no bucket survives the first. After an outage, which is the case this pass
    /// exists for, every outage bucket survives, holds no source row, and reads the entire materialized source
    /// through once; the first one builds it, spilling past <c>work_mem</c>. That is the #3905 defect in
    /// <see cref="DailySummarySql"/>'s not-carried probes without the <c>server_id</c> that bounded it there to
    /// one server's rows. So each probe carries <c>OFFSET 0</c>, which PostgreSQL will not pull up into a join
    /// (<c>simplify_EXISTS_query</c>: "OFFSET 0 ... traditionally is used as an optimization fence"), and runs as
    /// a SubPlan once per bucket with the chunk it needs picked at run time. The candidate buckets are fenced the
    /// same way, so the source is probed only for the buckets the materialization probe found empty. Measured
    /// with the old text as the oracle: the same buckets, in the same order, on every target.</para>
    /// </summary>
    public static string MaterializationHoleScanSql(MaterializationHoleTarget target, (string Schema, string Name) materialization)
    {
        var filter = MaterializationHoleSourceFilterFor(target.CreateSql);
        var sourceFilter = filter.Length == 0 ? string.Empty : $"\n    AND   {filter}";

        return $@"
SELECT c.bucket
FROM (
    SELECT b.bucket
    FROM generate_series($1::timestamp, $2::timestamp, $3::interval) AS b(bucket)
    WHERE NOT EXISTS (SELECT 1 FROM {QuoteIdentifier(materialization.Schema)}.{QuoteIdentifier(materialization.Name)} AS m WHERE m.bucket = b.bucket OFFSET 0)
    OFFSET 0
) AS c
WHERE EXISTS (
    SELECT 1 FROM collect.{target.Source} AS s
    WHERE s.{target.SourceTimeColumn} >= c.bucket
    AND   s.{target.SourceTimeColumn} < c.bucket + $3::interval{sourceFilter}
    OFFSET 0)
ORDER BY c.bucket";
    }

    /// <summary>
    /// ONE hole definition for a frozen-legacy/successor pair (#4301), used by
    /// <see cref="TimescaleSupport.RetentionArmSafetySql"/> (the gate): a bucket in
    /// <c>[<paramref name="fromExpr"/>, <paramref name="toExpr"/>]</c> is a hole when raw admits at least one
    /// row in <c>[bucket, bucket + width)</c> AND neither the legacy nor the successor has materialized that
    /// bucket. <paramref name="fromExpr"/>/<paramref name="toExpr"/> are SQL expressions (a literal, a
    /// parameter placeholder, a correlated subquery) so each caller supplies its own bounds in its own idiom.
    /// OFFSET 0 fenced for the same #3933 reason <see cref="MaterializationHoleScanSql"/> is: written bare, the
    /// planner pulls the per-bucket EXISTS probes up into joins that scan the whole relation instead of
    /// probing one bucket's worth. A bucket below raw's own current floor can hold no admitted row, so it can
    /// never be a hole under this definition and never holds the purge — a gap left below the floor by an
    /// earlier version's purge is invisible here by construction, not merely undetected (see this member's own
    /// callers for what that means for the gate).
    ///
    /// <para><b>The repair walk (#4301, the ruling lane) does NOT share this definition.</b> An earlier cut of
    /// #4301 planned a walk branch that probed the legacy-or-successor union the same way the gate does; the
    /// ruling replaced it with a plain successor-only fill down to raw's own filtered floor
    /// (<see cref="RepairMaterializationHolesAsync"/>'s seam-floor block), so the walk's own hole definition
    /// stays <see cref="MaterializationHoleScanSql"/> (successor-only) throughout — every non-empty hour below
    /// the successor's floor simply becomes a successor bucket. The LIST-form twin this method used to have
    /// (<c>LegacySuccessorHoleScanSql</c>) had no other caller once that ruling landed and was removed with it.
    /// </para>
    /// </summary>
    public static string LegacySuccessorHoleExistsSql(
        string relation, string sourceTimeColumn, string sourceFilter, string legacy, string successor,
        string fromExpr, string toExpr, string bucketWidthLiteral)
        => $"EXISTS ({LegacySuccessorHoleBodySql(relation, sourceTimeColumn, sourceFilter, legacy, successor, fromExpr, toExpr, bucketWidthLiteral)})";

    /// <summary>
    /// The body <see cref="LegacySuccessorHoleExistsSql"/> wraps in <c>EXISTS(...)</c> — kept as its own
    /// method (#4301, H2) so a future second caller can share it without duplicating the buckets clause;
    /// today <see cref="LegacySuccessorHoleExistsSql"/> is its only caller.
    /// </summary>
    private static string LegacySuccessorHoleBodySql(
        string relation, string sourceTimeColumn, string sourceFilter, string legacy, string successor,
        string fromExpr, string toExpr, string bucketWidthLiteral)
    {
        ArgumentNullException.ThrowIfNull(relation);
        ArgumentNullException.ThrowIfNull(sourceTimeColumn);
        ArgumentNullException.ThrowIfNull(sourceFilter);
        ArgumentNullException.ThrowIfNull(legacy);
        ArgumentNullException.ThrowIfNull(successor);
        ArgumentNullException.ThrowIfNull(fromExpr);
        ArgumentNullException.ThrowIfNull(toExpr);
        ArgumentNullException.ThrowIfNull(bucketWidthLiteral);

        var filterClause = sourceFilter.Length == 0 ? string.Empty : $"\n        AND   {sourceFilter}";

        return $@"
    SELECT hb.bucket
    FROM generate_series({fromExpr}, {toExpr}, {bucketWidthLiteral}) AS hb(bucket)
    WHERE NOT EXISTS (SELECT 1 FROM collect.{legacy} AS hl WHERE hl.bucket = hb.bucket OFFSET 0)
    AND   NOT EXISTS (SELECT 1 FROM collect.{successor} AS hs WHERE hs.bucket = hb.bucket OFFSET 0)
    AND   EXISTS (
              SELECT 1 FROM collect.{relation} AS hr
              WHERE hr.{sourceTimeColumn} >= hb.bucket
              AND   hr.{sourceTimeColumn} < hb.bucket + {bucketWidthLiteral}{filterClause}
              OFFSET 0)
    OFFSET 0";
    }

    /// <summary>The materialized span of one aggregate — its oldest and newest bucket — read off the
    /// materialization hypertable, both index-endpoint lookups. NULLs for an aggregate that has never
    /// materialized, which is the backfill's case and not this pass's.</summary>
    public static string MaterializationSpanSql((string Schema, string Name) materialization)
        => $"SELECT min(bucket), max(bucket) FROM {QuoteIdentifier(materialization.Schema)}.{QuoteIdentifier(materialization.Name)}";

    /// <summary>
    /// Runs of consecutive hole buckets folded into half-open ranges <c>[Start, End)</c>, oldest first — one
    /// refresh call per run rather than per bucket. Pure. A bucket that is not exactly one
    /// <paramref name="bucketWidth"/> after the previous one starts a new range, so two holes with a covered
    /// bucket between them stay two refreshes and the covered bucket is not re-materialized.
    /// </summary>
    public static IReadOnlyList<(DateTime Start, DateTime End)> MergeContiguousBuckets(IEnumerable<DateTime> buckets, TimeSpan bucketWidth)
    {
        ArgumentNullException.ThrowIfNull(buckets);
        if (bucketWidth <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(bucketWidth), bucketWidth, "a bucket has a positive width");
        }

        var ranges = new List<(DateTime Start, DateTime End)>();
        foreach (var bucket in buckets.OrderBy(b => b))
        {
            if (ranges.Count > 0 && ranges[^1].End == bucket)
            {
                ranges[^1] = (ranges[^1].Start, bucket + bucketWidth);
                continue;
            }

            ranges.Add((bucket, bucket + bucketWidth));
        }

        return ranges;
    }

    /// <summary>
    /// The cap applied to the merged ranges: up to <paramref name="capBuckets"/> buckets in total are repaired
    /// this start, the rest are reported and left. A range that straddles the cap is split at it, so the budget
    /// is spent exactly and the remainder is a well-formed range for the next start. Pure, so the tests can walk
    /// it.
    ///
    /// <para><b>Direction (#4186 round-3 H1).</b> Oldest-first (<paramref name="newestFirst"/> <c>false</c>,
    /// the default) takes the OLDEST ranges, splitting a straddler at its NEWER edge — safe for the ordinary
    /// window, where a repair sits strictly above the floor and can never move it. Newest-first takes the
    /// ranges closest to the END of the ordering, splitting a straddler at its OLDER edge so the kept portion
    /// stays adjacent to whatever is already materialized — the seam window's own requirement, since there the
    /// floor IS a bare <c>min(bucket)</c> with no contiguity check behind it, and only a gapless top-down
    /// descent keeps every unrepaired row inside the gate's probe window.</para>
    /// </summary>
    public static (IReadOnlyList<(DateTime Start, DateTime End)> Repair, IReadOnlyList<(DateTime Start, DateTime End)> Deferred) CapMaterializationHoleRepairs(
        IReadOnlyList<(DateTime Start, DateTime End)> ranges, int capBuckets, TimeSpan bucketWidth, bool newestFirst = false)
    {
        ArgumentNullException.ThrowIfNull(ranges);
        if (capBuckets <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capBuckets), capBuckets, "the cap is at least one bucket");
        }

        var repair = new List<(DateTime Start, DateTime End)>();
        var deferred = new List<(DateTime Start, DateTime End)>();
        var remaining = capBuckets;

        var ordered = newestFirst ? ranges.OrderByDescending(r => r.Start) : ranges.OrderBy(r => r.Start);
        foreach (var range in ordered)
        {
            var width = (int)((range.End - range.Start).Ticks / bucketWidth.Ticks);
            if (remaining <= 0)
            {
                deferred.Add(range);
                continue;
            }

            if (width <= remaining)
            {
                repair.Add(range);
                remaining -= width;
                continue;
            }

            if (newestFirst)
            {
                var newestSplit = range.End - TimeSpan.FromTicks(bucketWidth.Ticks * remaining);
                repair.Add((newestSplit, range.End));
                deferred.Add((range.Start, newestSplit));
                remaining = 0;
                continue;
            }

            var split = range.Start + TimeSpan.FromTicks(bucketWidth.Ticks * remaining);
            repair.Add((range.Start, split));
            deferred.Add((split, range.End));
            remaining = 0;
        }

        return (repair, deferred);
    }

    /// <summary>
    /// The scan window(s) for one aggregate this start, given its own <paramref name="floor"/> and
    /// <paramref name="ceiling"/>, the horizon <see cref="MaterializationHoleScanSpanFor"/> computes for its
    /// source, and the seam bound (<paramref name="seamFloor"/>, equal to <paramref name="floor"/> when the
    /// aggregate has no frozen legacy or raw's own filtered floor does not reach back past the successor's
    /// floor). Pure, so the tests can walk it.
    ///
    /// <para><b>Two windows, not one (#4186 follow-up).</b> The seam fix's first cut folded the seam into the
    /// SAME <c>max(_, horizon)</c> the ordinary scan already clamps to — <c>from = max(seamFloor, horizon)</c>
    /// — which reads right for an outage shorter than the horizon (<see cref="MaterializationHoleScanSpanFor"/>:
    /// 4 days of raw retention for <c>query_stats</c>/<c>procedure_stats</c>) and silently drops the rest of the
    /// seam for a longer one: a store stopped more than 4 days before its first start on this version has its
    /// seam tail clamped away every single start, the gate's seam probe (<see cref="RetentionArmSafetySql"/>)
    /// keeps finding the un-repaired rows, and the raw purge never releases on its own. The horizon exists to
    /// skip source rows the raw retention has already purged — scanning past it wastes a probe on a bucket that
    /// cannot be repaired. A seam is not that case: the outage that opened it left the source with no rows
    /// there at all (not purged, empty), and probing an empty span costs one cheap index range per bucket
    /// whatever its age. So the seam gets its OWN window, <c>[seamFloor, floor)</c>, scanned in full however far
    /// below the horizon it reaches, while the ordinary window stays exactly <c>[max(floor, horizon), ceiling]</c>
    /// — the successor's own span below the horizon is the source retention's business, not this repair's, and
    /// widening it was never the fix.</para>
    ///
    /// <para><b>The seam now reaches raw's own filtered floor, not merely the legacy's last bucket (#4301,
    /// per the ruling "fill the successor CONTIGUOUSLY DOWNWARD").</b> An earlier cut of this method took a
    /// separate, oldest-first-walked third window for a hole strictly INSIDE the frozen legacy's own span —
    /// dead code once the ruling landed: the walk fills every hole below the successor's floor down to raw's
    /// floor in ONE newest-first descent (the seam window itself, now with its lower bound moved), because
    /// contiguity from the successor's floor upward is the property <c>RollupCoverage.StitchedRelationSql</c>
    /// needs, and a bucket does not care which side of the legacy's last bucket it happened to sit on.</para>
    /// </summary>
    public static IReadOnlyList<(DateTime From, DateTime To)> MaterializationHoleScanWindows(
        DateTime floor, DateTime ceiling, DateTime horizon, DateTime seamFloor, TimeSpan bucketWidth)
    {
        if (bucketWidth <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(bucketWidth), bucketWidth, "a bucket has a positive width");
        }

        var windows = new List<(DateTime From, DateTime To)>();

        if (seamFloor < floor)
        {
            var seamTo = floor - bucketWidth;
            if (seamFloor <= seamTo)
            {
                windows.Add((seamFloor, seamTo));
            }
        }

        var ordinaryFrom = floor > horizon ? floor : horizon;
        if (ordinaryFrom <= ceiling)
        {
            windows.Add((ordinaryFrom, ceiling));
        }

        return windows;
    }

    /// <summary>
    /// What one start's pass did — the whole tally the worker's one unconditional summary line reports (#3756)
    /// and the live test asserts. Every count the line names is carried here rather than re-derived by the
    /// caller, so the line cannot say something the pass did not measure.
    ///
    /// <para><see cref="AggregatesScanned"/> is the aggregates the scan actually probed ("walked");
    /// <see cref="AggregatesSkipped"/> the ones it had a reason not to (not a continuous aggregate on this
    /// store, never materialized, or — absent a seam — a span entirely below the source's horizon; a seam
    /// window is never skipped for that reason). <see cref="HolesFound"/>
    /// counts the contiguous hole RANGES the scan saw across every aggregate, BEFORE the cap, and
    /// <see cref="BucketsFound"/> the buckets those ranges span — so <c>BucketsFound == BucketsRepaired +
    /// BucketsDeferred</c> always, while <c>HolesRepaired + HolesDeferred</c> exceeds <c>HolesFound</c> by one
    /// for every range the cap split (the straddle <see cref="CapMaterializationHoleRepairs"/> describes: one
    /// hole as the scan saw it, two lines as the repair reported it). Buckets are the unambiguous currency;
    /// the range counts are how the per-hole lines are numbered. <see cref="HolesRemaining"/> is buckets still
    /// reading as holes after both refresh paths; <see cref="Failures"/> the aggregates whose scan or repair
    /// threw and was isolated; <see cref="HolesForced"/> the holes the plain refresh left standing and the
    /// forced one had to close. <see cref="Elapsed"/> is the pass's own wall clock from entry to return —
    /// the detect, every probe, every refresh — and not the caller's connection open.</para>
    /// </summary>
    public sealed record MaterializationHoleRepairSummary(
        int AggregatesScanned, int AggregatesSkipped, int HolesFound, int BucketsFound, int HolesRepaired, int BucketsRepaired, int HolesDeferred, int BucketsDeferred, int HolesRemaining, int Failures, int HolesForced, TimeSpan Elapsed);

    /// <summary>
    /// THE PASS: scan every registered continuous aggregate for materialization holes and close each with one
    /// forced, targeted refresh, oldest first, up to the per-aggregate cap. <paramref name="utcNow"/> is the
    /// service clock, the scan's horizon anchor; bound as a parameter, never written as <c>now()</c> in store
    /// SQL (the <see cref="BaselineBackfillProbeSql(string, string)"/> zone reasoning). Returns what it did,
    /// as the complete tally — and ONLY returns it: the one summary line per start is the caller's (#3756;
    /// the type summary says why), so a pass that finds nothing returns zeros rather than falling silent.
    /// Failure-isolated per aggregate; a store without the extension, or an aggregate that is a plain fallback
    /// view, is skipped with a Debug line. See the type summary for the design.
    /// </summary>
    public static async Task<MaterializationHoleRepairSummary> RepairMaterializationHolesAsync(
        NpgsqlConnection connection, ILogger? logger, DateTime utcNow, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        /* #3756: the pass times itself from here, so the summary's Elapsed is the scan's own cost — detect,
           probes, refreshes — and not whatever the caller did to get a connection. */
        var passClock = Stopwatch.StartNew();

        var scanned = 0;
        var skipped = 0;
        var holesFound = 0;
        var bucketsFound = 0;
        var holesRepaired = 0;
        var bucketsRepaired = 0;
        var holesDeferred = 0;
        var bucketsDeferred = 0;
        var holesRemaining = 0;
        var failures = 0;

        var holesForced = 0;

        if (!await DetectAsync(connection, cancellationToken))
        {
            logger?.LogDebug("Materialization-hole repair (#3653): no TimescaleDB on this store, nothing to scan.");
            return new MaterializationHoleRepairSummary(0, MaterializationHoleTargets.Count, 0, 0, 0, 0, 0, 0, 0, 0, 0, passClock.Elapsed);
        }

        var disclosure = new RefreshDisclosure(message => logger?.LogWarning(
            "Materialization-hole repair (#3653): {Message}", message));

        foreach (var target in MaterializationHoleTargets)
        {
            try
            {
                var materialization = await ResolveMaterializationAsync(connection, target.View, cancellationToken);
                if (materialization is null)
                {
                    skipped++;
                    logger?.LogDebug("Materialization-hole repair (#3653): {View} is not a continuous aggregate on this store — skipped.", target.View);
                    continue;
                }

                DateTime? floor;
                DateTime? ceiling;
                using (var span = new NpgsqlCommand(MaterializationSpanSql(materialization.Value), connection) { CommandTimeout = SetupTimeoutSeconds })
                {
                    await using var reader = await span.ExecuteReaderAsync(cancellationToken);
                    await reader.ReadAsync(cancellationToken);
                    floor = reader.IsDBNull(0) ? null : reader.GetDateTime(0);
                    ceiling = reader.IsDBNull(1) ? null : reader.GetDateTime(1);
                }

                if (floor is null || ceiling is null)
                {
                    skipped++;
                    logger?.LogDebug("Materialization-hole repair (#3653): {View} has materialized nothing yet — the backfill's case, not a hole.", target.View);
                    continue;
                }

                /* #4186 seam fix, lowered by #4301's ruling ("fill the successor CONTIGUOUSLY DOWNWARD"):
                   a successor whose legacy is frozen (LegacyOf, non-null only for the three
                   SupersededHourlyRollups successors) can hold an un-materialized span BELOW its own floor —
                   not only the seam an outage opens between the legacy's last bucket and the successor's
                   first refresh, but any hole INSIDE the legacy's own frozen span from an outage that predates
                   this store's freeze (see this class's doc, and RetentionArmSafetySql's, for the full shape).
                   A hole is defined as a gap INSIDE the materialized span, so scanning from the successor's own
                   floor never reaches either one. Extend the lower bound down to raw's own filtered floor —
                   the successor's admitted source floor, the SAME bound RetentionArmSafetySql's stitch probes
                   from — whenever that reaches further back than the successor's own floor; below it raw
                   admits no row, so no hole can exist there and the walk has nothing left to fill (this is the
                   contiguous-downward fill: RollupCoverage.StitchedRelationSql splits its read at the
                   successor's floor, so every row below it must already be a successor bucket once this
                   converges). min() is a no-op once the successor's floor overtakes raw's floor on its own, so
                   this converges to plain floor scanning as the successor accumulates history. A raw table
                   with nothing admitted (min is NULL) leaves the floor untouched. */
                var seamFloor = floor.Value;
                var legacy = LegacyOf(target.View);
                if (legacy is not null)
                {
                    var sourceFilter = MaterializationHoleSourceFilterFor(target.CreateSql);
                    var sourceWhere = sourceFilter.Length == 0 ? string.Empty : $" WHERE {sourceFilter}";
                    using var rawFilteredFloor = new NpgsqlCommand($"SELECT min({target.SourceTimeColumn}) FROM collect.{target.Source}{sourceWhere}", connection) { CommandTimeout = SetupTimeoutSeconds };
                    if (await rawFilteredFloor.ExecuteScalarAsync(cancellationToken) is DateTime rawFloor)
                    {
                        var seamBound = AlignDown(rawFloor, target.BucketWidth);
                        if (seamBound < seamFloor)
                        {
                            seamFloor = seamBound;
                        }
                    }
                }

                /* #4299 L4: for a raw-sourced target the old time horizon (utcNow - MaterializationHoleScanSpanFor)
                   assumed raw purges on ITS OWN schedule, so nothing older than that span could still be sitting
                   in raw unrepaired. Variant (d') stops scheduling the three raw jobs at all — raw now purges
                   only when the service's own trigger fires — so raw can hold rows far older than that span
                   while the trigger has not yet run, and the old clamp would leave a hole below it unscanned
                   indefinitely. For raw-sourced targets the lower bound is instead the RAW FLOOR actually still
                   present (min(SourceTimeColumn) in the source table itself), so the scan reaches every bucket
                   raw genuinely still holds; a raw table with nothing in it yet (fresh install) falls back to the
                   old time horizon, which is harmless there since there is nothing to scan either way. */
                var horizon = IsRawSourced(target.Source)
                    ? await RawFloorHorizonAsync(connection, target, utcNow, cancellationToken)
                    : AlignDown(utcNow - MaterializationHoleScanSpanFor(target.Source), target.BucketWidth);
                var windows = MaterializationHoleScanWindows(floor.Value, ceiling.Value, horizon, seamFloor, target.BucketWidth);
                if (windows.Count == 0)
                {
                    skipped++;
                    continue;
                }

                scanned++;

                /* #4186 round-3 H1: the seam window's holes are scanned, capped and walked SEPARATELY from
                   the ordinary window's, never merged into one oldest-first pass. windows[0] is the seam
                   window whenever one exists — MaterializationHoleScanWindows always emits it first, and it
                   exists exactly when seamFloor < floor (both bucket-aligned, so that comparison alone
                   decides it; see that method for why). An interior (ordinary) repair can never move the
                   successor's floor (s.mn) — it only fills a gap strictly above an already-materialized
                   bucket — so oldest-first there is exactly as safe as it always was. A SEAM repair is
                   different: s.mn is a bare min(bucket) with no contiguity check behind it, so materializing
                   the OLDEST seam buckets first (the bug) can drop the floor straight to the seam's bottom
                   while newer seam buckets in between are still holes, and RetentionArmSafetySql's probe —
                   bounded above by s.mn — stops looking there. Walking the seam from the TOP (the buckets
                   next to s.mn) and stopping at the first range that fails keeps the descent gapless: the
                   floor only ever retreats into ground this pass already covered, so every unrepaired seam
                   row stays inside the probe window — the property RollupBackfill.cs:39-44 states for its own
                   newest-first slices. */
                var isSeamWindow = seamFloor < floor.Value;
                var seamWindow = isSeamWindow ? windows[0] : ((DateTime From, DateTime To)?)null;
                var ordinaryWindows = isSeamWindow ? windows.Skip(1) : windows;

                var seamHoles = seamWindow is { } sw
                    ? await ScanHolesAsync(connection, target, materialization.Value, sw.From, sw.To, cancellationToken)
                    : new List<DateTime>();

                var ordinaryHoles = new List<DateTime>();
                foreach (var (from, to) in ordinaryWindows)
                {
                    ordinaryHoles.AddRange(await ScanHolesAsync(connection, target, materialization.Value, from, to, cancellationToken));
                }

                if (seamHoles.Count == 0 && ordinaryHoles.Count == 0)
                {
                    continue;
                }

                var seamRanges = MergeContiguousBuckets(seamHoles, target.BucketWidth);
                var ordinaryRanges = MergeContiguousBuckets(ordinaryHoles, target.BucketWidth);

                /* #3756: found is counted as the scan saw it — contiguous ranges and the buckets they span — BEFORE
                   the cap decides what this start repairs and what it leaves, so the summary can say "found"
                   independently of "repaired" and "deferred" (the record's summary states the arithmetic). */
                holesFound += seamRanges.Count + ordinaryRanges.Count;
                bucketsFound += seamHoles.Count + ordinaryHoles.Count;

                /* One shared cap per aggregate, same total as before this fix — the seam spends from it FIRST,
                   newest end down, and whatever it leaves is what the ordinary window (oldest-first, as
                   always) has this start. That keeps "a start never re-materializes more for one aggregate
                   than an ordinary policy run does" true with the seam in the mix, not only without it. */
                var cap = MaterializationHoleRepairCapBuckets(target.BucketWidth);
                var (seamRepair, seamDeferred) = CapMaterializationHoleRepairs(seamRanges, cap, target.BucketWidth, newestFirst: true);
                var seamBucketsTaken = seamRepair.Sum(r => (int)((r.End - r.Start).Ticks / target.BucketWidth.Ticks));
                var ordinaryCap = cap - seamBucketsTaken;

                IReadOnlyList<(DateTime Start, DateTime End)> ordinaryRepair;
                IReadOnlyList<(DateTime Start, DateTime End)> ordinaryDeferred;
                if (ordinaryCap > 0)
                {
                    (ordinaryRepair, ordinaryDeferred) = CapMaterializationHoleRepairs(ordinaryRanges, ordinaryCap, target.BucketWidth);
                }
                else
                {
                    ordinaryRepair = Array.Empty<(DateTime Start, DateTime End)>();
                    ordinaryDeferred = ordinaryRanges;
                }

                /* One range's plain-then-forced repair, shared by the seam and ordinary walks below. Returns
                   the holes still standing in [start, lastBucket] after both attempts. */
                async Task<int> RepairRangeAsync(DateTime start, DateTime end)
                {
                    var buckets = (int)((end - start).Ticks / target.BucketWidth.Ticks);
                    var lastBucket = end - target.BucketWidth;
                    var stopwatch = Stopwatch.StartNew();

                    /* Plain first: on the outage shape this IS the repair (measured on 2.28.1 — see the type
                       summary), and it runs on every TimescaleDB version. */
                    await RollupBackfill.RunSliceAsync(connection, target.View, start, end, disclosure, cancellationToken);
                    var remaining = (await ScanHolesAsync(connection, target, materialization.Value, start, lastBucket, cancellationToken)).Count;
                    var forced = false;

                    if (remaining > 0)
                    {
                        /* A hole the plain refresh left has no invalidation behind it; the forced refresh
                           re-batches every bucket in the range regardless. Escalation on a MEASURED remainder,
                           the backfill's rule, never speculative. */
                        forced = true;
                        holesForced++;
                        await RollupBackfill.RepairAsync(connection, target.View, start, end, disclosure, cancellationToken);
                        remaining = (await ScanHolesAsync(connection, target, materialization.Value, start, lastBucket, cancellationToken)).Count;
                    }

                    stopwatch.Stop();
                    holesRepaired++;
                    bucketsRepaired += buckets;
                    holesRemaining += remaining;

                    if (remaining == 0)
                    {
                        logger?.LogInformation(
                            "Materialization-hole repair (#3653): {View} had {Buckets} bucket(s) in [{Start}, {End}) that the source held rows for and the aggregate had never materialized — {Path} over exactly those bounds closed it in {Seconds:F1}s.",
                            target.View, buckets, start.ToString("O", CultureInfo.InvariantCulture), end.ToString("O", CultureInfo.InvariantCulture),
                            forced ? "a plain refresh left it standing (no invalidation behind it) and one forced refresh" : "one refresh",
                            stopwatch.Elapsed.TotalSeconds);
                    }
                    else
                    {
                        logger?.LogWarning(
                            "Materialization-hole repair (#3653): {View} still shows {Remaining} of {Buckets} bucket(s) in [{Start}, {End}) as holes after a plain and a forced refresh ({Seconds:F1}s) — the source has rows there that the refresh produced no output for; the aggregate's own filter and the scan's copy of it may have diverged, or the refresh was cut short. Re-judged on the next start.",
                            target.View, remaining, buckets, start.ToString("O", CultureInfo.InvariantCulture), end.ToString("O", CultureInfo.InvariantCulture), stopwatch.Elapsed.TotalSeconds);
                    }

                    return remaining;
                }

                /* #4186 round-3 H1: newest seam range first; stop at the first one that throws (propagates to
                   this target's own catch below, exactly the risk the ordinary window always carried) or
                   leaves buckets standing (remaining > 0) — do NOT go on to an older seam range once either
                   happens, or the floor could advance past a still-open hole the same way the bug did. */
                foreach (var (start, end) in seamRepair)
                {
                    var remaining = await RepairRangeAsync(start, end);
                    if (remaining > 0)
                    {
                        break;
                    }
                }

                /* Ordinary window: unchanged from before this fix. An interior repair cannot move the floor,
                   so a remainder here only means "re-judged on the next start" — it never risks the gate. */
                foreach (var (start, end) in ordinaryRepair)
                {
                    await RepairRangeAsync(start, end);
                }

                foreach (var (start, end) in seamDeferred.Concat(ordinaryDeferred))
                {
                    var buckets = (int)((end - start).Ticks / target.BucketWidth.Ticks);
                    holesDeferred++;
                    bucketsDeferred += buckets;
                    logger?.LogInformation(
                        "Materialization-hole repair (#3653): {View} has a further {Buckets} bucket(s) of hole in [{Start}, {End}) left for the next start — this start's cap for it is {Cap} bucket(s), one refresh policy window, so a start never re-materializes more for one aggregate than an ordinary policy run does.",
                        target.View, buckets, start.ToString("O", CultureInfo.InvariantCulture), end.ToString("O", CultureInfo.InvariantCulture), MaterializationHoleRepairCapBuckets(target.BucketWidth));
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failures++;
                logger?.LogWarning(
                    "Materialization-hole repair (#3653): could not scan or repair {View} this start — its holes, if any, stand until the next start retries: {Message}",
                    target.View, ex.Message);
            }
        }

        /* #3756: no summary line here — the tally goes back to the caller, whose one INFORMATION line per start
           is written whatever the counts are. The conditional summary this replaced (Information when something
           happened, Debug otherwise) is the exact shape the issue names: at the level a production log is read,
           a zero-hole start wrote nothing, and nothing is also what a start that never reached the scan writes. */
        passClock.Stop();
        return new MaterializationHoleRepairSummary(
            scanned, skipped, holesFound, bucketsFound, holesRepaired, bucketsRepaired, holesDeferred, bucketsDeferred, holesRemaining, failures, holesForced, passClock.Elapsed);
    }

    /// <summary>
    /// #4299 L4: is <paramref name="source"/> one of the three raw tables named in <see cref="RawTierCoverage"/>
    /// (the ones a service-triggered purge drops, never on a schedule of their own)? Used to pick the hole
    /// scan's lower bound: a raw-sourced target needs the RAW FLOOR itself, not the old time-based horizon (see
    /// <see cref="RawFloorHorizonAsync"/>'s doc for why the time horizon stopped being safe under variant (d')).
    /// </summary>
    public static bool IsRawSourced(string source)
    {
        foreach (var (relation, _, _) in RawTierCoverage)
        {
            if (string.Equals(relation, source, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// #4299 L4: the hole scan's lower bound for a raw-sourced target, aligned down to a bucket boundary — the
    /// oldest row <paramref name="target"/>'s source table (one of <see cref="RawTierCoverage"/>'s three) still
    /// holds, or <c>utcNow - MaterializationHoleScanSpanFor(target.Source)</c> when the source is empty (nothing
    /// to scan below either bound in that case, so the fallback is harmless).
    ///
    /// <para><b>Why the time horizon stopped being safe.</b> Before #4299, every raw table purged on its OWN
    /// scheduled retention job, so nothing older than <see cref="MaterializationHoleScanSpanFor"/>'s span could
    /// still be sitting in raw — scanning further back than that was wasted probes over rows already gone.
    /// Variant (d') UNSCHEDULES the three raw jobs (<c>scheduled</c> stays permanently false) and moves the
    /// purge onto a service-triggered <c>CALL run_job(id)</c>, gated on this very repair having found no hole in
    /// the range about to be dropped (see <see cref="HoleFreeThroughAsync"/>). Between the moment raw ages past
    /// that old span and the moment the trigger's gate is satisfied, raw legitimately holds rows older than the
    /// old horizon — an outage-lengthened startup, a store that has never yet passed the gate, or simply a
    /// service that has not reached an hourly Periodic pass yet. Clamping the scan to the old time horizon in
    /// that window would leave a real hole below it unscanned and unrepaired for as long as the purge stays
    /// held, which is now indefinite rather than bounded by the old schedule. Reading the raw floor directly
    /// removes the assumption: the scan reaches exactly as far back as raw still has rows to lose.</para>
    /// </summary>
    public static async Task<DateTime> RawFloorHorizonAsync(
        NpgsqlConnection connection, MaterializationHoleTarget target, DateTime utcNow, CancellationToken cancellationToken)
    {
        using var floorCommand = new NpgsqlCommand(
            $"SELECT min({target.SourceTimeColumn}) FROM collect.{target.Source}", connection) { CommandTimeout = SetupTimeoutSeconds };
        var rawFloor = await floorCommand.ExecuteScalarAsync(cancellationToken);
        return rawFloor is DateTime raw
            ? AlignDown(raw, target.BucketWidth)
            : AlignDown(utcNow - MaterializationHoleScanSpanFor(target.Source), target.BucketWidth);
    }

    /// <summary>
    /// #4299 L4: the raw purge's own trigger gate, checked as a FRESH scan of the EXACT range the purge is about
    /// to drop — never a reuse of a repair pass's stale tally, because the repair and the trigger can run in
    /// different passes and a range clean when the repair last looked can have grown a hole since (a plain
    /// refresh that regressed, a collection gap the repair pass never saw). Returns <c>true</c> only when EVERY
    /// bucket in <c>[dropFrom, dropTo)</c> is covered by the materialization AND not one of <paramref
    /// name="deferredRanges"/> — a range this same pass's <see cref="CapMaterializationHoleRepairs"/> capped out
    /// of and left for the next start counts as a hole for gating purposes even though the scan itself would
    /// currently read it clean once repaired, because "repaired" here means "already closed", not "queued".
    /// <paramref name="deferredRanges"/> is the union of a target's seam-deferred and ordinary-deferred ranges
    /// from the SAME pass that produced the fresh <paramref name="materialization"/> reads this call scans
    /// against — passing a stale deferred list from an earlier pass defeats the point exactly as reusing a
    /// stale "finished" flag would.
    /// </summary>
    public static async Task<bool> HoleFreeThroughAsync(
        NpgsqlConnection connection, MaterializationHoleTarget target, (string Schema, string Name) materialization,
        DateTime dropFrom, DateTime dropTo, IReadOnlyList<(DateTime Start, DateTime End)> deferredRanges,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(deferredRanges);
        if (dropTo <= dropFrom)
        {
            return true;
        }

        foreach (var deferred in deferredRanges)
        {
            if (deferred.Start < dropTo && deferred.End > dropFrom)
            {
                return false;
            }
        }

        var holes = await ScanHolesAsync(connection, target, materialization, dropFrom, dropTo - target.BucketWidth, cancellationToken);
        return holes.Count == 0;
    }

    /// <summary>The hole buckets of one aggregate over <c>[from, to]</c> inclusive, oldest first.</summary>
    private static async Task<List<DateTime>> ScanHolesAsync(
        NpgsqlConnection connection, MaterializationHoleTarget target, (string Schema, string Name) materialization,
        DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        var holes = new List<DateTime>();
        using var scan = new NpgsqlCommand(MaterializationHoleScanSql(target, materialization), connection) { CommandTimeout = SetupTimeoutSeconds };
        scan.Parameters.AddWithValue(DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
        scan.Parameters.AddWithValue(DateTime.SpecifyKind(to, DateTimeKind.Unspecified));
        scan.Parameters.AddWithValue(target.BucketWidth);
        await using var reader = await scan.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            holes.Add(reader.GetDateTime(0));
        }

        return holes;
    }

    /// <summary>An instant aligned DOWN to a bucket boundary from the epoch — how <c>time_bucket</c> aligns for
    /// the widths in use (an hour, a day), so a scan horizon lands on a bucket the series can produce.</summary>
    public static DateTime AlignDown(DateTime instant, TimeSpan bucketWidth)
    {
        if (bucketWidth <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(bucketWidth), bucketWidth, "a bucket has a positive width");
        }

        return new DateTime(instant.Ticks - (instant.Ticks % bucketWidth.Ticks), DateTimeKind.Unspecified);
    }
}
