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
/// <c>pg_growth</c> — object growth (filled by lane 38 of #3691, design v2 "object growth / contention anomalies",
/// the growth half): <c>PG_DATABASE_GROWTH</c> over the hourly <c>pg_database_size_stats</c> samples (V136). A trend,
/// never a spot size — the bloat family's lesson applied to the one quantity everyone already has a screen for: a
/// database's size is a fact about the past, and the only thing an operator can act on is how fast it is changing.
///
/// <para><b>The shape.</b> Per database, the latest <c>size_bytes</c> inside a fourteen-day lookback ending at the
/// analysis window against the earliest sample inside it — growth in bytes, as a fraction of where it started, per
/// day, and the straight-line days-to-double the slope implies. The instance total (<c>total_bytes</c>, denormalised
/// onto every row by the collector and NULL on every row when any database could not be sized) gets the same
/// arithmetic where it exists. Databases the monitoring role may not size (NULL <c>size_bytes</c>) are EXCLUDED and
/// COUNTED (<see cref="GrowthDatabasesUnsizedKey"/>) — never read as zero growth; a database with fewer than three
/// samples in the lookback is not a trend and is not graded; a fact with nothing to grade says which gate withheld it.</para>
///
/// <para><b>Lineage — every bar here is unmeasured, and the family says so.</b> The table is one day old at this
/// family's birth (V136, 2026-09-20); the fleet calibration of 2026-09-19 ran before it existed and there is no
/// growth distribution to cite. So the growth line (10 % AND 1 GiB), the critical line (25 % AND 10 GiB), the
/// lookback (14 days), the three-sample minimum and the co-fire boosts are all <b>chosen, not measured — calibrate
/// against <c>pg_database_size_stats</c> once it holds 14 d before the next release</b>, and every fact this
/// family emits carries <c>threshold_lineage = 0</c> so <c>get_analysis_facts</c> shows it. No bar is a SQL Server
/// constant reused by value (the SQL Server engine has no growth trend fact at all).</para>
///
/// <para><b>Why two arms on each line.</b> Bytes alone would name a 2 TB warehouse's routine week; a fraction alone
/// would name a 200 MB sandbox doubling. Both must hold, and the grade is the WEAKER arm's ramp — 0.5 when both sit
/// at their lines, 1.0 only when both reach their critical values — so a 10 GiB / 12 % growth reads as a warning
/// with a large number in it, not as critical.</para>
///
/// <para><b>Metadata contract with the collector</b> (<c>PgTargetFactCollector.Growth.cs</c>): the keys below are
/// the whole interface between the read and the grade; the advice partial reads the same names. A missing key reads
/// as 0 through <c>GetValueOrDefault</c>, which for every gate here means "does not fire". The top databases ride as
/// <c>growth_bytes_&lt;database&gt;</c> / <c>growth_pct_&lt;database&gt;</c> — the name in the KEY, the bloat family's
/// shape, because <see cref="Fact.Metadata"/> is doubles-only.</para>
/// </summary>
public static partial class PgTargetScorer
{
    /// <summary>
    /// Days the trend reads back from the window END, regardless of the pass window: the analysis window is hours
    /// and a database grows in days, so the fact reads a LONGER span than the pass (the bloat family's shape) and
    /// stamps <see cref="GrowthLookbackDaysKey"/>. Lineage: <b>unmeasured</b> — chosen, not measured: fourteen days
    /// matches the bloat family's lookback so the two trends' spans agree and the growth → bloat edge compares like
    /// with like; calibrate against <c>pg_database_size_stats</c> once it holds 14 d before the next release.
    /// </summary>
    public const int GrowthLookbackDays = 14;

    /// <summary>
    /// Growth in bytes across the lookback at which the trend is a finding (with <see cref="GrowthConcerningFraction"/>).
    /// Lineage: <b>unmeasured</b> — chosen, not measured: the table is one day old; a gibibyte in two weeks is the
    /// smallest growth an operator is likely to plan storage around. Calibrate against <c>pg_database_size_stats</c>
    /// once it holds 14 d before the next release.
    /// </summary>
    public const long GrowthConcerningBytes = 1024L * 1024 * 1024;

    /// <summary>
    /// Growth as a fraction of the EARLIEST size that must accompany <see cref="GrowthConcerningBytes"/>. Lineage:
    /// <b>unmeasured</b> — chosen, not measured; ten per cent in two weeks is a doubling in about five months, the
    /// slope at which "when does the disk fill" becomes a planning question. Calibrate against
    /// <c>pg_database_size_stats</c> once it holds 14 d before the next release. An earliest size of ZERO satisfies
    /// this arm trivially (a tenth of nothing is nothing) — the bytes arm decides alone and the fact says the
    /// percentage is not computable rather than carrying an infinity.
    /// </summary>
    public const double GrowthConcerningFraction = 0.10;

    /// <summary>
    /// Growth in bytes at which the trend grades 1.0 (with <see cref="GrowthCriticalFraction"/>). Lineage:
    /// <b>unmeasured</b> — chosen, not measured; calibrate against <c>pg_database_size_stats</c> once it holds 14 d
    /// before the next release.
    /// </summary>
    public const long GrowthCriticalBytes = 10L * 1024 * 1024 * 1024;

    /// <summary>
    /// Growth as a fraction of the earliest size at which the trend grades 1.0 (with <see cref="GrowthCriticalBytes"/>):
    /// a quarter in two weeks doubles in about two months. Lineage: <b>unmeasured</b> — chosen, not measured; calibrate
    /// against <c>pg_database_size_stats</c> once it holds 14 d before the next release.
    /// </summary>
    public const double GrowthCriticalFraction = 0.25;

    /// <summary>
    /// Samples a database (or the instance total) needs inside the lookback before its trend is graded: two points
    /// are a line and three is the first count with a middle to disagree with the ends — the bloat family's index
    /// minimum, for the same reason. Lineage: <b>unmeasured</b> — chosen, not measured; calibrate against
    /// <c>pg_database_size_stats</c> once it holds 14 d before the next release. A fact withheld by this gate carries
    /// <see cref="GrowthUnavailableReasonKey"/> = <see cref="GrowthReasonInsufficientSamples"/>.
    /// </summary>
    public const int GrowthMinimumSamples = 3;

    /// <summary>How many databases the fact names (the worst in <see cref="Fact.ObjectName"/>, all of them by name
    /// in metadata). Three, because the advice leads with one and mentions the others; the read's own bound, not a bar.</summary>
    public const int GrowthTopDatabases = 3;

    /// <summary>Boost when the bloat trend fired on a database this fact names — growth that is bloat is a different
    /// remedy than growth that is data. Lineage: <b>unmeasured</b> — chosen, not measured; calibrate against the
    /// dogfood PostgreSQL fleet's co-fire rates before the next release.</summary>
    public const double GrowthBloatCoFireBoost = 0.3;

    /// <summary>Boost when the growth-rate anomaly fired in the window — the fortnight's trend is also this hour's
    /// unusual rate, so the growth is happening NOW rather than having happened. Lineage: <b>unmeasured</b> — chosen,
    /// not measured; calibrate against the dogfood PostgreSQL fleet's co-fire rates before the next release.</summary>
    public const double GrowthAnomalyCoFireBoost = 0.2;

    /* metadata: the population */

    public const string GrowthLookbackDaysKey = "lookback_days";
    /// <summary>Metadata key: distinct collection times any row in the lookback carried — how many times the collector
    /// looked, for the whole server.</summary>
    public const string GrowthSamplesInLookbackKey = "samples_in_lookback";
    /// <summary>Metadata key: distinct databases seen in the lookback, sized or not.</summary>
    public const string GrowthDatabasesSeenKey = "databases_seen";
    /// <summary>Metadata key: distinct databases with NO sized sample in the lookback — the monitoring role may not
    /// size them (no CONNECT and not <c>pg_read_all_stats</c>), so their growth is UNKNOWN, never zero. When this is
    /// above zero the instance total is NULL on every row too, and the fact says so.</summary>
    public const string GrowthDatabasesUnsizedKey = "databases_unsized";
    /// <summary>Metadata key: sized databases with at least <see cref="GrowthMinimumSamples"/> samples — the graded set.</summary>
    public const string GrowthDatabasesConsideredKey = "databases_considered";
    /// <summary>Metadata key: how many considered databases crossed the growth line — the fact names the worst.</summary>
    public const string GrowthDatabasesOverLineKey = "databases_over_line";

    /* metadata: availability */

    /// <summary>Metadata key: 1 when the fact carries a graded trend (a database or the instance total with enough
    /// samples), 0 when it is a withheld statement and <see cref="GrowthUnavailableReasonKey"/> says why.</summary>
    public const string GrowthAvailableKey = "growth_available";
    public const string GrowthUnavailableReasonKey = "unavailable_reason";
    /// <summary>Databases were sized but none (nor the total) has <see cref="GrowthMinimumSamples"/> samples yet.</summary>
    public const int GrowthReasonInsufficientSamples = 1;
    /// <summary>Every database seen was unsized on every row — the role can size nothing here.</summary>
    public const int GrowthReasonAllUnsized = 2;

    /* metadata: the worst database (the fact's subject) */

    /// <summary>Metadata key: latest minus earliest <c>size_bytes</c> across the lookback for the worst database. The fact's Value.</summary>
    public const string GrowthBytesKey = "growth_bytes";
    public const string GrowthFirstBytesKey = "first_bytes";
    public const string GrowthLatestBytesKey = "latest_bytes";
    /// <summary>Metadata key: growth as a percentage of the earliest size. ABSENT when the earliest size was zero;
    /// <see cref="GrowthPctComputableKey"/> carries the branch.</summary>
    public const string GrowthPctKey = "growth_pct";
    public const string GrowthPctComputableKey = "growth_pct_computable";
    /// <summary>Metadata key: the worst database's samples inside the lookback.</summary>
    public const string GrowthSamplesKey = "samples";
    /// <summary>Metadata key: hours between the worst database's earliest and latest sample.</summary>
    public const string GrowthSpanHoursKey = "span_hours";
    /// <summary>Metadata key: growth bytes over the span, per day — the slope.</summary>
    public const string GrowthBytesPerDayKey = "bytes_per_day";
    /// <summary>Metadata key: growth percentage over the span, per day.</summary>
    public const string GrowthPctPerDayKey = "pct_per_day";
    /// <summary>Metadata key: at the lookback's slope, days until the database is twice its latest size — a
    /// STRAIGHT-LINE extrapolation (latest bytes over bytes per day), stated as such. Absent when the slope is not
    /// positive.</summary>
    public const string GrowthDaysToDoubleKey = "days_to_double";

    /* metadata: the instance total */

    /// <summary>Metadata key: 1 when <c>total_bytes</c> had at least <see cref="GrowthMinimumSamples"/> non-NULL
    /// samples in the lookback; 0 when the instance total could not be trended (an unsized database NULLs it on every
    /// row — a sum over the databases this role can see is NOT the instance total, and none is made).</summary>
    public const string GrowthTotalAvailableKey = "total_available";
    public const string GrowthTotalBytesKey = "total_growth_bytes";
    public const string GrowthTotalFirstBytesKey = "total_first_bytes";
    public const string GrowthTotalLatestBytesKey = "total_latest_bytes";
    public const string GrowthTotalPctKey = "total_growth_pct";
    public const string GrowthTotalSamplesKey = "total_samples";
    public const string GrowthTotalSpanHoursKey = "total_span_hours";
    public const string GrowthTotalBytesPerDayKey = "total_bytes_per_day";
    public const string GrowthTotalDaysToDoubleKey = "total_days_to_double";

    /// <summary>Metadata key: which subject produced the grade — <see cref="GrowthSubjectDatabase"/> when a named
    /// database's arms decided, <see cref="GrowthSubjectInstance"/> when the instance total's did, 0 when nothing graded.</summary>
    public const string GrowthGradedSubjectKey = "graded_subject";
    public const int GrowthSubjectDatabase = 1;
    public const int GrowthSubjectInstance = 2;

    /* metadata: the named databases (name in the key) */

    /// <summary>Metadata key prefix: <c>growth_bytes_&lt;database&gt;</c>, value = growth bytes, for each of the top
    /// <see cref="GrowthTopDatabases"/>. The name lives in the key because metadata is doubles.</summary>
    public const string GrowthNamedBytesPrefix = "growth_bytes_";
    /// <summary>Metadata key prefix: <c>growth_pct_&lt;database&gt;</c>; absent for a database whose earliest size was zero.</summary>
    public const string GrowthNamedPctPrefix = "growth_pct_";

    /// <summary>Layer-1 base severity for the growth family. One arm per key; an unknown key under this source is 0.</summary>
    private static partial double ScoreGrowthFact(Fact fact) => fact.Key switch
    {
        PgTargetFactKeys.DatabaseGrowth => ScoreDatabaseGrowth(fact),
        _ => 0.0,
    };

    /// <summary>
    /// The trend grade: nothing without a graded subject; otherwise the BEST of the named databases' and the instance
    /// total's two-arm grades (<see cref="GrowthGrade"/>), with <see cref="GrowthGradedSubjectKey"/> stamped so the
    /// advice can say which it was. Every exit stamps <c>threshold_lineage = 0</c>: every bar in this family is chosen.
    /// The fraction arm reads the earliest size from metadata, so an earliest zero passes it (a tenth of nothing) and
    /// the bytes line decides alone — the honest reading of a database created inside the lookback.
    /// </summary>
    private static double ScoreDatabaseGrowth(Fact fact)
    {
        /* unmeasured: every bar in this family (see the class summary) — the flag is the family's, not one gate's. */
        fact.Metadata["threshold_lineage"] = 0;

        if (fact.Metadata.GetValueOrDefault(GrowthAvailableKey) < 1)
            return 0.0;

        var best = 0.0;
        var subject = 0;

        foreach (var name in GrowthNamedDatabases(fact))
        {
            var bytes = fact.Metadata.GetValueOrDefault(GrowthNamedBytesPrefix + name);
            var pct = fact.Metadata.TryGetValue(GrowthNamedPctPrefix + name, out var p) ? p : (double?)null;
            var grade = GrowthGrade(bytes, pct);
            if (grade > best)
            {
                best = grade;
                subject = GrowthSubjectDatabase;
            }
        }

        if (fact.Metadata.GetValueOrDefault(GrowthTotalAvailableKey) >= 1)
        {
            var totalPct = fact.Metadata.TryGetValue(GrowthTotalPctKey, out var tp) ? tp : (double?)null;
            var grade = GrowthGrade(fact.Metadata.GetValueOrDefault(GrowthTotalBytesKey), totalPct);
            if (grade > best)
            {
                best = grade;
                subject = GrowthSubjectInstance;
            }
        }

        fact.Metadata[GrowthGradedSubjectKey] = subject;
        return best;
    }

    /// <summary>
    /// The two-arm grade for one subject: 0 under EITHER line; otherwise the weaker of the two ramps — bytes from
    /// <see cref="GrowthConcerningBytes"/> to <see cref="GrowthCriticalBytes"/>, fraction from
    /// <see cref="GrowthConcerningFraction"/> to <see cref="GrowthCriticalFraction"/> — so both must reach their
    /// critical value for 1.0. A null <paramref name="pct"/> (earliest size zero) passes the fraction arm at 1.0 and the
    /// bytes ramp decides alone. Public because the collector-side count of databases over the line and the advice's
    /// "crossed / did not cross" sentence rest on the same predicate.
    /// </summary>
    public static double GrowthGrade(double growthBytes, double? pct)
    {
        /* unmeasured: GrowthConcerningBytes / GrowthConcerningFraction — chosen, not measured (see the constants). */
        if (growthBytes < GrowthConcerningBytes)
            return 0.0;
        if (pct is { } fraction && fraction < 100.0 * GrowthConcerningFraction)
            return 0.0;

        /* unmeasured: the ramps' ends are the chosen concerning and critical lines above. */
        var bytesGrade = FactScorer.ApplyThresholdFormula(growthBytes, GrowthConcerningBytes, GrowthCriticalBytes);
        var pctGrade = pct is { } f
            ? FactScorer.ApplyThresholdFormula(f, 100.0 * GrowthConcerningFraction, 100.0 * GrowthCriticalFraction)
            : 1.0;
        return Math.Min(bytesGrade, pctGrade);
    }

    /// <summary>
    /// The database names a growth fact carries — the worst in <see cref="Fact.ObjectName"/> and every named one in
    /// the <see cref="GrowthNamedBytesPrefix"/> keys — for the same-database co-fire predicates. Public because the
    /// graph and the advice read the same set.
    /// </summary>
    public static HashSet<string> GrowthNamedDatabases(Fact fact)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        if (!string.IsNullOrEmpty(fact.ObjectName))
            names.Add(fact.ObjectName);
        foreach (var key in fact.Metadata.Keys)
        {
            if (key.StartsWith(GrowthNamedBytesPrefix, StringComparison.Ordinal) && key.Length > GrowthNamedBytesPrefix.Length)
                names.Add(key[GrowthNamedBytesPrefix.Length..]);
        }
        return names;
    }

    /// <summary>
    /// Whether the bloat trend fact and the growth fact name the same DATABASE: the bloat fact carries its worst
    /// table's database in <see cref="Fact.DatabaseName"/>; the growth fact carries up to three databases by name.
    /// When the bloat fact carries no database name the intersection cannot be tested and the answer is co-presence —
    /// stated in the amplifier / edge description. The single definition the edge AND the amplifier use.
    /// </summary>
    public static bool GrowthAndBloatShareADatabase(Fact growth, Fact bloat) =>
        string.IsNullOrEmpty(bloat.DatabaseName) || GrowthNamedDatabases(growth).Contains(bloat.DatabaseName);

    /// <summary>
    /// Layer-2 amplifiers for the growth family. Two co-fires, both reading the sibling's <see cref="Fact.BaseSeverity"/>
    /// (never <see cref="Fact.Severity"/> — the vacuum family's emission-order lesson): the bloat trend on the same
    /// database (the growth is dead space, and the remedy is the bloat card's), and the growth-rate anomaly (the
    /// fortnight's slope is also this window's unusual rate — the anomaly is a context fact at base 0 when it does not
    /// fire and cannot open an edge, lane 15's pattern, so the co-fire is read here). No config co-fire: no server
    /// knob makes a database grow.
    /// </summary>
    private static partial List<AmplifierDefinition> GrowthAmplifiers(string key) => key switch
    {
        PgTargetFactKeys.DatabaseGrowth =>
        [
            new()
            {
                Description = "PG_BLOAT_TREND co-fired on a database this trend names — part of the growth is dead space autovacuum has not reclaimed, and the bloat card's levers come before any storage decision",
                /* unmeasured: GrowthBloatCoFireBoost (see the constant). */
                Boost = GrowthBloatCoFireBoost,
                Predicate = facts => facts.TryGetValue(PgTargetFactKeys.DatabaseGrowth, out var growth)
                    && facts.TryGetValue(PgTargetFactKeys.BloatTrend, out var bloat)
                    && bloat.BaseSeverity > 0
                    && GrowthAndBloatShareADatabase(growth, bloat),
            },
            new()
            {
                Description = "ANOMALY_PG_DATABASE_GROWTH co-fired — the instance's growth rate in this window is unusual against its own hour-of-week routine, so the fortnight's slope is happening now, not a step that already happened",
                /* unmeasured: GrowthAnomalyCoFireBoost (see the constant). */
                Boost = GrowthAnomalyCoFireBoost,
                Predicate = facts => facts.TryGetValue(PgTargetFactKeys.AnomalyDatabaseGrowth, out var anomaly) && anomaly.BaseSeverity > 0,
            },
        ],
        _ => [],
    };
}
