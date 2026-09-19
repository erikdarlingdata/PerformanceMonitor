/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// <c>pg_bloat</c> — table and index bloat TRENDS (filled by lane 13 of #3691, design §3.4): <c>PG_BLOAT_TREND</c>
/// over the hourly <c>pg_table_bloat_stats</c> samples, <c>PG_INDEX_BLOAT_TREND</c> over the daily
/// <c>pg_index_bloat</c> samples. A trend, never a point estimate — the bar is on GROWTH of the estimate across
/// a fourteen-day lookback, and a fact over so sparse a series states its own sample count in metadata.
///
/// <para><b>Why never a spot percentage — the measured reason.</b> The fleet calibration pass of 2026-09-19
/// read <c>bloat_pct_estimate</c> across 50 Aurora PostgreSQL clusters over 14 days: the per-server median
/// sits at 12.9 %, but the fleet p75 is 71 %, p90 86 %, max 99.8 % — driven by small heaps (the median heap
/// is 75 MB), where the column-width arithmetic the estimate rests on is noise against a handful of pages.
/// Index <c>est_bloat_pct</c> reads p50 46 %, p99 88 % with a MEDIAN index size of 16 kB, and most indexes
/// are skipped outright. A percentage graded without a size floor would page on most of the fleet, and a
/// spot percentage graded at all would be #2542's 88.59 %-against-0.50 % trap made into a finding. So: a
/// heap or index below the size floor is never graded; a percentage is spoken only as growth of the BYTES
/// estimate against its own earlier value; and the line the growth must cross is stated in bytes as well.</para>
///
/// <para><b>Lineage.</b> The growth line (25 % AND 256 MiB), the size floors (64 MiB) and the lookback
/// (14 days) are <b>measured</b>: fleet p90 of worst-table 14-day growth over 14 days × 50 Aurora PostgreSQL
/// clusters of the dogfood fleet, 2026-09-19 (p99-table growth median 5.6 MB / 6.3 %; fleet p90 874 MB /
/// 120 %). Growth of a bytes estimate is engine-neutral by construction, so the Aurora population stands in
/// for stock PostgreSQL here — stated, not assumed. The CRITICAL line (1 GiB) sits inside the measured EMPTY
/// interval (fleet max 6.8 GB on one table; nothing else approached it), so it is measured as a bar nothing
/// routine reaches. A fact graded through these carries <c>threshold_lineage = 1</c>. The one chosen number,
/// the three-sample minimum for a daily-cadence trend, is <b>unmeasured</b>, and an index fact held back by
/// it carries <c>threshold_lineage = 0</c> — the flag names whichever bar decided.</para>
///
/// <para><b>Metadata contract with the collector</b> (<c>PgTargetFactCollector.Bloat.cs</c>): the keys below
/// are the whole interface between the read and the grade, and the advice partial reads the same names. A
/// missing key reads as 0 through <c>GetValueOrDefault</c>, which for every gate here means "does not fire".
/// The top-3 objects ride as <c>growth_bytes_&lt;schema.table&gt;</c> / <c>growth_pct_&lt;schema.table&gt;</c> /
/// <c>dead_tuples_&lt;schema.table&gt;</c> — the name in the KEY, the way the wait profile carries
/// <c>contrib_Type:event</c>, because <see cref="Fact.Metadata"/> is doubles-only.</para>
/// </summary>
public static partial class PgTargetScorer
{
    /* ── shared by both bloat facts ── */

    /// <summary>
    /// Days the trend reads back from the window END, regardless of the pass window. The analysis window is
    /// hours; bloat moves in days — a four-hour window has at most four hourly bloat samples and zero daily
    /// index samples, so the fact reads a LONGER span than the pass, the way the baseline provider reads 30
    /// days behind its window, and stamps <see cref="BloatLookbackDaysKey"/> so the reader knows which span
    /// the growth is over. Lineage: <b>measured</b> — fourteen days is the calibration pass's own window
    /// (over 14 days × 50 Aurora PostgreSQL clusters of the dogfood fleet, 2026-09-19), so the growth line
    /// below and the span it was measured over agree; both bloat tables keep 90 days by default.
    /// </summary>
    public const int BloatLookbackDays = 14;

    /// <summary>
    /// Growth of the bloat-bytes estimate at which the trend is a finding: the fleet p90 of worst-table
    /// 14-day growth. Lineage: <b>measured</b> — fleet p90 of worst-table 14-day growth over 14 days × 50
    /// Aurora PostgreSQL clusters of the dogfood fleet, 2026-09-19 (fleet p90 874 MB / 120 %; p99-table growth
    /// median 5.6 MB / 6.3 %). Both this AND <see cref="BloatGrowthConcerningFraction"/> must hold: the bytes
    /// line keeps a small table's doubling out, the fraction keeps a huge table's routine churn out.
    /// </summary>
    public const long BloatGrowthConcerningBytes = 256L * 1024 * 1024;

    /// <summary>
    /// Growth as a fraction of the EARLIER estimate that must accompany the bytes line. Lineage:
    /// <b>measured</b> — the same read as <see cref="BloatGrowthConcerningBytes"/> (fleet p90 of worst-table
    /// 14-day growth, 120 %, over 14 days × 50 Aurora PostgreSQL clusters of the dogfood fleet, 2026-09-19); a
    /// quarter is well under the p90 so the bytes line is the one that discriminates. An earlier estimate of
    /// ZERO satisfies this arm trivially (a quarter of nothing is nothing) — the bytes line alone decides for
    /// a table that had no bloat at the start of the lookback, and the fact says the percentage is not
    /// computable rather than carrying an infinity.
    /// </summary>
    public const double BloatGrowthConcerningFraction = 0.25;

    /// <summary>
    /// Growth at which the trend grades 1.0. Lineage: <b>measured</b> (empty interval) — over 14 days × 50
    /// Aurora PostgreSQL clusters of the dogfood fleet, 2026-09-19, the fleet MAX worst-table growth was 6.8 GB
    /// on one table and no other approached a gigabyte; the bar sits inside the interval nothing routine
    /// reaches, which is what a critical bar is for.
    /// </summary>
    public const long BloatGrowthCriticalBytes = 1024L * 1024 * 1024;

    /// <summary>
    /// Heap size (table) / index size (index) at the window end below which NO percentage is spoken and no
    /// grade is made. Lineage: <b>measured</b> — over 14 days × 50 Aurora PostgreSQL clusters of the dogfood
    /// fleet, 2026-09-19, the median heap was 75 MB and the median estimable index 16 kB, and it is exactly
    /// those small objects that carry the 71–99.8 % spot estimates; below 64 MiB the column-width arithmetic
    /// is noise against a few thousand pages. The collector binds this into both reads.
    /// </summary>
    public const long BloatSizeFloorBytes = 64L * 1024 * 1024;

    /// <summary>
    /// Samples an INDEX needs inside the lookback before its trend is graded. The index collector is daily, so
    /// the lookback holds at most fourteen; two points are a line and three is the first count with a middle
    /// to disagree with the ends. Lineage: <b>unmeasured</b> — chosen, not measured; calibrate against
    /// <c>pg_index_bloat</c> before the next release. An index fact held back by this gate carries
    /// <c>threshold_lineage = 0</c> and <see cref="BloatUnavailableReasonKey"/> = <see cref="BloatReasonInsufficientSamples"/>.
    /// The hourly table read has no such gate: two samples a week apart is a real trend, and the fact
    /// states its sample count either way.
    /// </summary>
    public const int IndexBloatMinimumSamples = 3;

    /// <summary>How many objects the fact names (the worst in <see cref="Fact.ObjectName"/>, all of them by
    /// name in metadata). Three, because the advice leads with one and mentions the others; the read's own
    /// bound, not a bar.</summary>
    public const int BloatTopObjects = 3;

    /* metadata: the graded object */

    /// <summary>Metadata key: growth of the bloat-bytes estimate (table) or reclaimable-bytes estimate (index)
    /// across the lookback, latest minus earliest sample, for the worst object. The fact's Value.</summary>
    public const string BloatGrowthBytesKey = "growth_bytes";
    /// <summary>Metadata key: that growth as a percentage of the earlier estimate. ABSENT when the earlier
    /// estimate was zero; <see cref="BloatGrowthPctComputableKey"/> carries the branch.</summary>
    public const string BloatGrowthPctKey = "growth_pct";
    public const string BloatGrowthPctComputableKey = "growth_pct_computable";
    /// <summary>Metadata key: the estimate at the EARLIEST sample inside the lookback.</summary>
    public const string BloatEarlierBytesKey = "earlier_estimate_bytes";
    /// <summary>Metadata key: the estimate at the LATEST sample.</summary>
    public const string BloatLatestBytesKey = "latest_estimate_bytes";
    /// <summary>Metadata key: the object's MEASURED size at the latest sample — <c>heap_bytes</c> for a table,
    /// <c>index_bytes</c> for an index; the size floor is applied to it.</summary>
    public const string BloatObjectBytesKey = "object_bytes";
    /// <summary>Metadata key: the object's spot estimate percentage at the latest sample — carried so the
    /// reader can see what was NOT graded, never used by the grade.</summary>
    public const string BloatSpotPctKey = "spot_pct_estimate";
    public const string BloatDeadTuplesKey = "dead_tuples";
    public const string BloatLiveTuplesKey = "live_tuples";
    /// <summary>Metadata key: samples of the worst object inside the lookback (after the availability
    /// filter). The fact's own coverage statement — it never borrows the one-minute coverage fraction.</summary>
    public const string BloatSamplesKey = "samples";
    /// <summary>Metadata key: hours between the worst object's earliest and latest usable sample.</summary>
    public const string BloatSpanHoursKey = "span_hours";
    public const string BloatLookbackDaysKey = "lookback_days";
    /// <summary>Metadata key: distinct collection times any row in the lookback carried, for the whole
    /// server — how many times the collector looked, not how many the object was usable.</summary>
    public const string BloatSamplesInLookbackKey = "samples_in_lookback";

    /* metadata: the population */

    /// <summary>Metadata key: distinct objects seen in the lookback, usable or not.</summary>
    public const string BloatObjectsSeenKey = "objects_seen";
    /// <summary>Metadata key: distinct objects with at least one usable estimate in the lookback (table:
    /// <c>estimate_unavailable = false</c>; index: <c>skipped_reason IS NULL</c>).</summary>
    public const string BloatObjectsEstimableKey = "objects_estimable";
    /// <summary>Metadata key: objects that passed the size floor (and, for indexes, the sample minimum) and were
    /// therefore graded.</summary>
    public const string BloatObjectsConsideredKey = "objects_considered";
    /// <summary>Metadata key: how many considered objects crossed the growth line — the fact names the worst.</summary>
    public const string BloatObjectsOverLineKey = "objects_over_line";
    /// <summary>Metadata key (index fact): share of indexes seen whose LATEST row was skipped, 0–1. "A majority
    /// skipped" is what the advice says when this is at or past one half.</summary>
    public const string IndexBloatSkippedShareKey = "skipped_share";
    /// <summary>Metadata key (index fact): how many DISTINCT skip reasons the skipped indexes carried — counts,
    /// never the prose; <c>get_pg_index_bloat</c> shows the reasons.</summary>
    public const string IndexBloatSkipReasonsKey = "skip_reasons";
    /// <summary>Metadata key (index fact): 1 when any row in the lookback reported the pgstattuple extension
    /// installed in its database — whether the exact-measurement route the skip reasons point at is open.</summary>
    public const string IndexBloatPgstattupleAvailableKey = "pgstattuple_available";

    /* metadata: availability */

    /// <summary>Metadata key: 1 when the fact carries a graded object, 0 when it is an <c>unavailable</c> /
    /// <c>insufficient</c> statement and <see cref="BloatUnavailableReasonKey"/> says why.</summary>
    public const string BloatEstimateAvailableKey = "estimate_available";
    /// <summary>Metadata key: why no object was graded, one of the <c>BloatReason*</c> codes. Absent when graded.</summary>
    public const string BloatUnavailableReasonKey = "unavailable_reason";
    /// <summary>Table: every row in the lookback said <c>estimate_unavailable</c> — the monitoring role cannot
    /// see <c>pg_stats</c> (the #2542 grant), or nothing was analysed.</summary>
    public const int BloatReasonEstimateUnavailable = 1;
    /// <summary>Index: every index in the lookback was skipped and pgstattuple is not installed anywhere the
    /// collector looked — neither the estimate nor the exact route is open.</summary>
    public const int BloatReasonPgstattupleUnavailable = 2;
    /// <summary>Index: every index was skipped, but pgstattuple IS installed — the exact route is open.</summary>
    public const int BloatReasonAllSkipped = 3;
    /// <summary>Objects were estimable but none passed the size floor, so no percentage may be spoken.</summary>
    public const int BloatReasonBelowSizeFloor = 4;
    /// <summary>Index: estimable indexes over the floor exist, but none has <see cref="IndexBloatMinimumSamples"/>
    /// in the lookback yet.</summary>
    public const int BloatReasonInsufficientSamples = 5;

    /* metadata: the named objects (name in the key) */

    /// <summary>Metadata key prefix: <c>growth_bytes_&lt;schema.table&gt;</c> (table fact) or
    /// <c>growth_bytes_&lt;schema.table.index&gt;</c> (index fact), value = growth bytes, for each of the
    /// top <see cref="BloatTopObjects"/>. The name lives in the key because metadata is doubles.</summary>
    public const string BloatNamedGrowthBytesPrefix = "growth_bytes_";
    /// <summary>Metadata key prefix: <c>growth_pct_&lt;name&gt;</c>; absent for an object whose earlier estimate was zero.</summary>
    public const string BloatNamedGrowthPctPrefix = "growth_pct_";
    /// <summary>Metadata key prefix (table fact): <c>dead_tuples_&lt;name&gt;</c> at the latest sample.</summary>
    public const string BloatNamedDeadTuplesPrefix = "dead_tuples_";

    /// <summary>
    /// Layer-1 base severity for the bloat family. One arm per key; an unknown key under this source is 0.
    /// </summary>
    private static partial double ScoreBloatFact(Fact fact) => fact.Key switch
    {
        PgTargetFactKeys.BloatTrend => ScoreBloatTrend(fact, isIndex: false),
        PgTargetFactKeys.IndexBloatTrend => ScoreBloatTrend(fact, isIndex: true),
        _ => 0.0,
    };

    /// <summary>
    /// The trend grade, shared by both facts: nothing without a graded object; nothing under the size floor
    /// (re-applied here so a fact built by hand cannot bypass the collector's bind); nothing under the growth
    /// line on EITHER arm; then the shared ramp from 0.5 at the bytes line to 1.0 at the critical line. The
    /// fraction arm reads the earlier estimate from metadata, so an earlier zero passes it (a quarter of
    /// nothing) and the bytes line decides alone — which is the honest reading of "grew from no bloat to
    /// 300 MB of it". Stamps <c>threshold_lineage = 1</c> on a graded fact (every bar here is measured) and
    /// <c>0</c> on an index fact held back by the chosen sample minimum.
    /// </summary>
    private static double ScoreBloatTrend(Fact fact, bool isIndex)
    {
        if (fact.Metadata.GetValueOrDefault(BloatEstimateAvailableKey) < 1)
        {
            /* unmeasured: IndexBloatMinimumSamples is the one chosen gate; when it is what withheld the grade the
               flag says so. Every other withholding reason is a measured floor or an absence of data. */
            if (isIndex && (int)fact.Metadata.GetValueOrDefault(BloatUnavailableReasonKey) == BloatReasonInsufficientSamples)
                fact.Metadata["threshold_lineage"] = 0;
            return 0.0;
        }

        var growth = fact.Metadata.GetValueOrDefault(BloatGrowthBytesKey, fact.Value);
        var earlier = fact.Metadata.GetValueOrDefault(BloatEarlierBytesKey);
        var objectBytes = fact.Metadata.GetValueOrDefault(BloatObjectBytesKey);

        /* measured: BloatSizeFloorBytes — below it the estimate is arithmetic noise (fleet read of 2026-09-19). */
        if (objectBytes < BloatSizeFloorBytes)
            return 0.0;

        /* unmeasured: IndexBloatMinimumSamples — the daily series needs three points before it is a trend. */
        if (isIndex && fact.Metadata.GetValueOrDefault(BloatSamplesKey) < IndexBloatMinimumSamples)
        {
            fact.Metadata["threshold_lineage"] = 0;
            return 0.0;
        }

        /* measured: fleet p90 of worst-table 14-day growth over 14 days × 50 Aurora PostgreSQL clusters of the
           dogfood fleet, 2026-09-19 — both arms of the line, bytes AND fraction of the earlier estimate. */
        if (growth < BloatGrowthConcerningBytes || growth < earlier * BloatGrowthConcerningFraction)
        {
            fact.Metadata["threshold_lineage"] = 1;
            return 0.0;
        }

        /* measured: the ramp's ends are the concerning line above and the critical line inside the measured
           empty interval (fleet max 6.8 GB, 2026-09-19). */
        fact.Metadata["threshold_lineage"] = 1;
        return FactScorer.ApplyThresholdFormula(growth, BloatGrowthConcerningBytes, BloatGrowthCriticalBytes);
    }

    /// <summary>
    /// The object names a bloat fact carries — the worst in <see cref="Fact.ObjectName"/> and every named one
    /// in the <see cref="BloatNamedGrowthBytesPrefix"/> keys — for the same-object co-fire predicates. Public
    /// because the graph and the advice read the same set. An index fact's names are
    /// <c>schema.table.index</c>; <paramref name="parentTables"/> returns their <c>schema.table</c> prefix
    /// instead so an index can be matched to its table's fact.
    /// </summary>
    public static HashSet<string> BloatNamedObjects(Fact fact, bool parentTables = false)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        if (!string.IsNullOrEmpty(fact.ObjectName))
            names.Add(fact.ObjectName);
        foreach (var key in fact.Metadata.Keys)
        {
            if (key.StartsWith(BloatNamedGrowthBytesPrefix, StringComparison.Ordinal) && key.Length > BloatNamedGrowthBytesPrefix.Length)
                names.Add(key[BloatNamedGrowthBytesPrefix.Length..]);
        }

        if (!parentTables)
            return names;

        var parents = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in names)
        {
            /* schema.table.index → schema.table; a name without three parts is kept as it is. */
            var lastDot = name.LastIndexOf('.');
            var firstDot = name.IndexOf('.');
            parents.Add(lastDot > firstDot && firstDot >= 0 ? name[..lastDot] : name);
        }
        return parents;
    }

    /// <summary>
    /// Whether the backlog fact and the bloat fact name the same table. The backlog fact carries ONE table
    /// (its <see cref="Fact.ObjectName"/>); the bloat fact carries up to three by name. When the backlog fact
    /// has no name (an older collector, a hand-built fact) the intersection cannot be tested and the answer is
    /// co-presence — stated in the amplifier / edge description so the reader knows which it was.
    /// </summary>
    public static bool BloatAndBacklogShareATable(Fact bloat, Fact backlog) =>
        string.IsNullOrEmpty(backlog.ObjectName) || BloatNamedObjects(bloat).Contains(backlog.ObjectName);

    /// <summary>
    /// Layer-2 amplifiers for the bloat family — the §3.4 chain made numeric: bloat is the DAMAGE whose cause
    /// is the §3.1 backlog (dead tuples autovacuum did not reclaim are where the growth comes from), and a
    /// held horizon is what makes them unreclaimable. Boost values are <b>unmeasured</b> — chosen, not
    /// measured; calibrate against the dogfood PostgreSQL fleet's co-fire rates before the next release.
    /// Every predicate reads the sibling's <see cref="Fact.BaseSeverity"/>, never <see cref="Fact.Severity"/>
    /// (the vacuum family's emission-order lesson). No config co-fire: <c>fillfactor</c> is per-object and
    /// there is no server knob whose value makes a table bloat.
    /// </summary>
    private static partial List<AmplifierDefinition> BloatAmplifiers(string key) => key switch
    {
        PgTargetFactKeys.BloatTrend => BloatTrendAmplifiers(),
        PgTargetFactKeys.IndexBloatTrend => IndexBloatTrendAmplifiers(),
        _ => [],
    };

    private static List<AmplifierDefinition> BloatTrendAmplifiers() =>
    [
        new()
        {
            Description = "PG_AUTOVACUUM_BACKLOG co-fired on a table this trend names — the dead tuples autovacuum is not clearing are where the growth is coming from",
            /* unmeasured: chosen, not measured — calibrate against the dogfood PostgreSQL fleet before the next release. */
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.BloatTrend, out var bloat)
                && facts.TryGetValue(PgTargetFactKeys.AutovacuumBacklog, out var backlog)
                && backlog.BaseSeverity > 0
                && BloatAndBacklogShareATable(bloat, backlog),
        },
        new()
        {
            Description = "PG_XMIN_HOLD co-fired — a held xmin horizon makes dead tuples unremovable, so the space they hold cannot be reused and the heap grows around them",
            /* unmeasured: chosen, not measured — calibrate against the dogfood PostgreSQL fleet before the next release. */
            Boost = 0.2,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.XminHold, out var hold) && hold.BaseSeverity > 0,
        },
    ];

    private static List<AmplifierDefinition> IndexBloatTrendAmplifiers() =>
    [
        new()
        {
            Description = "PG_BLOAT_TREND co-fired on this index's table — the heap and its index are growing together, which is churn, not a one-off",
            /* unmeasured: chosen, not measured — calibrate against the dogfood PostgreSQL fleet before the next release. */
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.IndexBloatTrend, out var index)
                && facts.TryGetValue(PgTargetFactKeys.BloatTrend, out var table)
                && table.BaseSeverity > 0
                && BloatNamedObjects(index, parentTables: true).Overlaps(BloatNamedObjects(table)),
        },
    ];
}
