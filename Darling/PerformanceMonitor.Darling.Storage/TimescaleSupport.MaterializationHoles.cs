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
/// its <c>bucket</c> index — so the scan is a few hundred probes per aggregate whatever the tables weigh.
/// The aggregate's WHERE is applied to the source probe (<see cref="MaterializationHoleSourceFilterFor"/>) so a
/// bucket whose every source row the aggregate rejects — a restart hour on an interval-honest successor — is
/// not read as a hole and refreshed on every start for nothing. Buckets below the floor are the backfill's
/// business and are never touched here; buckets past the last materialized one are the live edge the policy
/// owns.</para>
///
/// <para><b>Bounded, and the bound is stated.</b> Per aggregate per start, at most one refresh policy window's
/// worth of buckets (<see cref="MaterializationHoleRepairCapBuckets"/>: 24 hourly, 3 daily) is refreshed,
/// OLDEST FIRST — the oldest hole is the one the source's retention is about to make permanent. Anything past
/// the cap is logged with its bounds and left for the next start. Every hole repaired is one INFORMATION line
/// with its bounds, its bucket count, its duration, which refresh closed it and what the re-scan found
/// afterwards; failure is isolated per aggregate, the #1775 shape.</para>
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
/// rather than holding a restarted service dark for it. Ordering against the retention policies is not
/// load-bearing: a raw purge is a background job already on its own schedule on every upgraded store, so
/// running the scan before <see cref="EnsureRetentionPoliciesAsync"/> re-arms it changes nothing about the
/// race; what bounds the race is that a hole is repairable for exactly as long as its source holds the rows.</para>
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
    /// </summary>
    public static string MaterializationHoleScanSql(MaterializationHoleTarget target, (string Schema, string Name) materialization)
    {
        var filter = MaterializationHoleSourceFilterFor(target.CreateSql);
        var sourceFilter = filter.Length == 0 ? string.Empty : $"\n    AND   {filter}";

        return $@"
SELECT b.bucket
FROM generate_series($1::timestamp, $2::timestamp, $3::interval) AS b(bucket)
WHERE NOT EXISTS (SELECT 1 FROM {QuoteIdentifier(materialization.Schema)}.{QuoteIdentifier(materialization.Name)} AS m WHERE m.bucket = b.bucket)
AND   EXISTS (
    SELECT 1 FROM collect.{target.Source} AS s
    WHERE s.{target.SourceTimeColumn} >= b.bucket
    AND   s.{target.SourceTimeColumn} < b.bucket + $3::interval{sourceFilter})
ORDER BY b.bucket";
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
    /// The cap applied to the merged ranges: the OLDEST ranges up to <paramref name="capBuckets"/> buckets in
    /// total are repaired this start, the rest are reported and left. A range that straddles the cap is split
    /// at it, so the budget is spent exactly and the remainder is a well-formed range for the next start.
    /// Pure, so the tests can walk it.
    /// </summary>
    public static (IReadOnlyList<(DateTime Start, DateTime End)> Repair, IReadOnlyList<(DateTime Start, DateTime End)> Deferred) CapMaterializationHoleRepairs(
        IReadOnlyList<(DateTime Start, DateTime End)> ranges, int capBuckets, TimeSpan bucketWidth)
    {
        ArgumentNullException.ThrowIfNull(ranges);
        if (capBuckets <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capBuckets), capBuckets, "the cap is at least one bucket");
        }

        var repair = new List<(DateTime Start, DateTime End)>();
        var deferred = new List<(DateTime Start, DateTime End)>();
        var remaining = capBuckets;

        foreach (var range in ranges.OrderBy(r => r.Start))
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

            var split = range.Start + TimeSpan.FromTicks(bucketWidth.Ticks * remaining);
            repair.Add((range.Start, split));
            deferred.Add((split, range.End));
            remaining = 0;
        }

        return (repair, deferred);
    }

    /// <summary>What one start's pass did, for the summary line and the live test. <see cref="HolesForced"/>
    /// counts the holes the plain refresh left standing and the forced one had to close.</summary>
    public sealed record MaterializationHoleRepairSummary(
        int AggregatesScanned, int AggregatesSkipped, int HolesRepaired, int BucketsRepaired, int HolesDeferred, int BucketsDeferred, int HolesRemaining, int Failures, int HolesForced);

    /// <summary>
    /// THE PASS: scan every registered continuous aggregate for materialization holes and close each with one
    /// forced, targeted refresh, oldest first, up to the per-aggregate cap. <paramref name="utcNow"/> is the
    /// service clock, the scan's horizon anchor; bound as a parameter, never written as <c>now()</c> in store
    /// SQL (the <see cref="BaselineBackfillProbeSql(string, string)"/> zone reasoning). Returns what it did.
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

        var scanned = 0;
        var skipped = 0;
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
            return new MaterializationHoleRepairSummary(0, MaterializationHoleTargets.Count, 0, 0, 0, 0, 0, 0, 0);
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

                var horizon = AlignDown(utcNow - MaterializationHoleScanSpanFor(target.Source), target.BucketWidth);
                var from = floor.Value > horizon ? floor.Value : horizon;
                var to = ceiling.Value;
                if (from > to)
                {
                    skipped++;
                    continue;
                }

                scanned++;
                var holes = await ScanHolesAsync(connection, target, materialization.Value, from, to, cancellationToken);
                if (holes.Count == 0)
                {
                    continue;
                }

                var ranges = MergeContiguousBuckets(holes, target.BucketWidth);
                var (repair, deferred) = CapMaterializationHoleRepairs(ranges, MaterializationHoleRepairCapBuckets(target.BucketWidth), target.BucketWidth);

                foreach (var (start, end) in repair)
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
                }

                foreach (var (start, end) in deferred)
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

        var summary = new MaterializationHoleRepairSummary(scanned, skipped, holesRepaired, bucketsRepaired, holesDeferred, bucketsDeferred, holesRemaining, failures, holesForced);
        if (holesRepaired > 0 || holesDeferred > 0 || failures > 0)
        {
            logger?.LogInformation(
                "Materialization-hole repair (#3653): {Scanned} aggregate(s) scanned, {Skipped} skipped (never materialized, or not a continuous aggregate here), {HolesRepaired} hole(s) / {BucketsRepaired} bucket(s) repaired ({Forced} needed the forced refresh), {HolesDeferred} hole(s) / {BucketsDeferred} bucket(s) deferred to the next start, {Remaining} bucket(s) still reading as holes after repair, {Failures} aggregate(s) failed.",
                scanned, skipped, holesRepaired, bucketsRepaired, holesForced, holesDeferred, bucketsDeferred, holesRemaining, failures);
        }
        else
        {
            logger?.LogDebug(
                "Materialization-hole repair (#3653): {Scanned} aggregate(s) scanned, {Skipped} skipped, no holes.",
                scanned, skipped);
        }

        return summary;
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
