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

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Advice for the object-growth family (filled by lane 38 of #3691). Value-stated from the trend fact — the
/// database, its size at both ends of the lookback, the growth in bytes and (where computable) as a fraction of
/// where it started, the slope per day, the straight-line days-to-double, the sample count and span, the instance
/// total beside it — and the levers in prose with the counter-objective of each: the bloat question FIRST (growth
/// that is dead space has the bloat card's remedies, and a storage decision made on it buys disk for nothing),
/// then retention / archival, then partitioning by time.
///
/// <para><b>What this card cannot say, and says it cannot.</b> Disk free space is NOT collected for a PostgreSQL
/// target (the #3691 checklist's disk-free line is open), so no sentence here is "the disk fills on Tuesday" —
/// days-to-double is a statement about the DATABASE at the lookback's slope, extrapolated in a straight line, and
/// the card names the volume's headroom as the figure the operator must bring. A slope is not a forecast: a
/// one-off load inside the lookback reads as the same bytes as a steady drift, and the anomaly beside this card is
/// what tells the two apart.</para>
///
/// <para>Every bar the card quotes is chosen, not measured (the table is a day old) and the investigation says so;
/// the <c>withheld</c> shapes — nothing sized, too few samples — get their own card, because a fact the role could
/// not measure must never read as "no growth".</para>
/// </summary>
public static partial class PgTargetAdvice
{
    /* filled by lane 38 of #3691. */

    private static partial AdviceBlock? ComposeGrowth(string key, IReadOnlyDictionary<string, Fact> factsByKey) => key switch
    {
        PgTargetFactKeys.DatabaseGrowth => ComposeDatabaseGrowth(factsByKey),
        _ => null,
    };

    /* ── PG_DATABASE_GROWTH ── */

    private static readonly AdviceBlock s_databaseGrowthStatic = new(
        Headline: "A database has grown past the line across the fourteen-day lookback",
        Investigation:
            "pg_database_size_stats samples every database's pg_database_size() hourly, and the instance total beside it. " +
            "This finding is a TREND, never a spot size: the latest size against the earliest sample inside a fourteen-day " +
            "lookback ending at the analysis window, graded only when the growth is both at least 1 GB and at least 10 % of " +
            "where the database started (25 % and 10 GB is the critical line). Both lines are chosen, not measured — the " +
            "table is new and no fleet distribution exists yet — and the fact carries threshold_lineage = 0 to say so. A " +
            "database the monitoring role may not size is excluded and counted, never read as zero growth; the instance " +
            "total is NULL whenever one such database exists, because a sum over the databases this role can see is not " +
            "the instance total.",
        Remediation:
            "Ask the bloat question first: if PG_BLOAT_TREND fired on the same database the growth is dead space and the " +
            "remedy is autovacuum's, not storage. For growth that is data, retention or archival of the oldest rows " +
            "(counter-objective: the rows are gone from the live database) and partitioning by time so old partitions can " +
            "be detached or dropped in O(1) (counter-objective: a migration of the table and every query's plan). Disk " +
            "free space is not collected for a PostgreSQL target — bring the volume's headroom to the days-to-double figure " +
            "yourself before deciding anything.");

    private static AdviceBlock ComposeDatabaseGrowth(IReadOnlyDictionary<string, Fact> facts)
    {
        if (!facts.TryGetValue(PgTargetFactKeys.DatabaseGrowth, out var f))
            return s_databaseGrowthStatic;

        var m = f.Metadata;
        var lookback = m.GetValueOrDefault(PgTargetScorer.GrowthLookbackDaysKey, PgTargetScorer.GrowthLookbackDays);
        var seen = m.GetValueOrDefault(PgTargetScorer.GrowthDatabasesSeenKey);
        var unsized = m.GetValueOrDefault(PgTargetScorer.GrowthDatabasesUnsizedKey);
        var considered = m.GetValueOrDefault(PgTargetScorer.GrowthDatabasesConsideredKey);
        var overLine = m.GetValueOrDefault(PgTargetScorer.GrowthDatabasesOverLineKey);
        var samplesInLookback = m.GetValueOrDefault(PgTargetScorer.GrowthSamplesInLookbackKey);

        if (m.GetValueOrDefault(PgTargetScorer.GrowthAvailableKey) < 1)
            return ComposeGrowthUnavailable((int)m.GetValueOrDefault(PgTargetScorer.GrowthUnavailableReasonKey), lookback, seen, unsized, samplesInLookback);

        var db = string.IsNullOrEmpty(f.ObjectName) ? "A database" : f.ObjectName;
        var growth = m.GetValueOrDefault(PgTargetScorer.GrowthBytesKey, f.Value);
        var first = m.GetValueOrDefault(PgTargetScorer.GrowthFirstBytesKey);
        var latest = m.GetValueOrDefault(PgTargetScorer.GrowthLatestBytesKey);
        var pctComputable = m.GetValueOrDefault(PgTargetScorer.GrowthPctComputableKey) >= 1;
        var pct = m.GetValueOrDefault(PgTargetScorer.GrowthPctKey);
        var samples = m.GetValueOrDefault(PgTargetScorer.GrowthSamplesKey);
        var spanHours = m.GetValueOrDefault(PgTargetScorer.GrowthSpanHoursKey);
        var perDay = m.GetValueOrDefault(PgTargetScorer.GrowthBytesPerDayKey);
        var pctPerDay = m.GetValueOrDefault(PgTargetScorer.GrowthPctPerDayKey);
        var doubling = m.TryGetValue(PgTargetScorer.GrowthDaysToDoubleKey, out var d) ? d : (double?)null;
        var totalAvailable = m.GetValueOrDefault(PgTargetScorer.GrowthTotalAvailableKey) >= 1;
        var subject = (int)m.GetValueOrDefault(PgTargetScorer.GrowthGradedSubjectKey);
        var graded = f.BaseSeverity > 0;
        var bloat = facts.TryGetValue(PgTargetFactKeys.BloatTrend, out var b) && b.Severity > 0 ? b : null;
        var bloatSameDb = bloat is not null && PgTargetScorer.GrowthAndBloatShareADatabase(f, bloat);
        var anomaly = facts.TryGetValue(PgTargetFactKeys.AnomalyDatabaseGrowth, out var a) && a.Severity > 0;

        var pctText = pctComputable ? $" ({Pct(pct)})" : " (from an earliest size of zero — no percentage to state)";
        var doublingText = doubling is { } dd && dd > 0 ? $"; at that slope it doubles in ~{FmtDays(dd)}" : string.Empty;
        var headline = graded
            ? subject == PgTargetScorer.GrowthSubjectInstance && totalAvailable
                ? $"The instance grew {FmtBytes(m.GetValueOrDefault(PgTargetScorer.GrowthTotalBytesKey))}{TotalPctText(m)} in {FmtDays(lookback)} — {FmtBytes(m.GetValueOrDefault(PgTargetScorer.GrowthTotalBytesPerDayKey))}/day, led by {db} at {FmtBytes(growth)}"
                : $"{db} grew {FmtBytes(growth)}{pctText} in {FmtDays(lookback)} — {FmtBytes(perDay)}/day{doublingText}"
            : $"{db} is the fastest-growing database at {FmtBytes(growth)}{pctText} over {FmtDays(lookback)} — under the line";

        var inv = new StringBuilder();
        inv.Append($"{db} measured {FmtBytes(first)} at its earliest sample inside the {lookback:0}-day lookback ending at the analysis window and {FmtBytes(latest)} at its latest ({samples:0} hourly samples, {FmtHours(spanHours)} apart at the ends): growth of {FmtBytes(growth)}{pctText}, {FmtBytes(perDay)} a day");
        if (pctComputable)
            inv.Append($" ({Pct(pctPerDay)} a day)");
        inv.Append(". ");
        if (doubling is { } dbl && dbl > 0)
            inv.Append($"At that slope the database is twice its current size in about {FmtDays(dbl)} — a straight-line extrapolation of the lookback's slope, not a forecast: a one-off load inside the fortnight reads as the same bytes as a steady drift. ");

        if (totalAvailable)
        {
            var tGrowth = m.GetValueOrDefault(PgTargetScorer.GrowthTotalBytesKey);
            var tFirst = m.GetValueOrDefault(PgTargetScorer.GrowthTotalFirstBytesKey);
            var tLatest = m.GetValueOrDefault(PgTargetScorer.GrowthTotalLatestBytesKey);
            var tPerDay = m.GetValueOrDefault(PgTargetScorer.GrowthTotalBytesPerDayKey);
            inv.Append($"The instance total moved from {FmtBytes(tFirst)} to {FmtBytes(tLatest)} over the same lookback: {FmtBytes(tGrowth)}{TotalPctText(m)}, {FmtBytes(tPerDay)} a day");
            if (m.TryGetValue(PgTargetScorer.GrowthTotalDaysToDoubleKey, out var tdd) && tdd > 0)
                inv.Append($", doubling in about {FmtDays(tdd)} on the same straight line");
            inv.Append(". ");
        }
        else if (unsized > 0)
        {
            inv.Append($"The instance total could not be trended: {unsized:0} database{(unsized == 1 ? string.Empty : "s")} could not be sized by the monitoring role (no CONNECT privilege and not a member of pg_read_all_stats — on a managed service, typically the vendor's own database), the collector writes the total as NULL on every row in that case, and a sum over the databases this role can see is not the instance total. Their growth is unknown, not zero. ");
        }

        inv.Append($"The line is chosen, not measured — growth of at least {FmtBytes(PgTargetScorer.GrowthConcerningBytes)} AND at least {Pct(100.0 * PgTargetScorer.GrowthConcerningFraction)} of the earliest size; {FmtBytes(PgTargetScorer.GrowthCriticalBytes)} AND {Pct(100.0 * PgTargetScorer.GrowthCriticalFraction)} is the critical line, and the grade is the weaker arm's — the table behind this family is new and no fleet distribution has been read for it (threshold_lineage = 0 on this fact). ");
        inv.Append(graded
            ? subject == PgTargetScorer.GrowthSubjectInstance
                ? "The instance total crossed it. "
                : "This database crossed it. "
            : "Nothing crossed it; the fact is context, not a finding. ");

        var others = NamedObjects(m, PgTargetScorer.GrowthNamedBytesPrefix).Where(n => n.Name != f.ObjectName).ToList();
        if (others.Count > 0)
        {
            inv.Append("Next by growth: ");
            inv.Append(string.Join("; ", others.Select(o =>
            {
                var oPct = m.TryGetValue(PgTargetScorer.GrowthNamedPctPrefix + o.Name, out var p) ? $", {Pct(p)}" : string.Empty;
                return $"{o.Name} ({FmtBytes(o.Value)}{oPct})";
            })));
            inv.Append(". ");
        }
        inv.Append($"Population: {seen:0} database{(seen == 1 ? string.Empty : "s")} seen in the lookback, {unsized:0} unsized, {considered:0} with at least {PgTargetScorer.GrowthMinimumSamples} samples, {overLine:0} over the line. ");

        if (bloatSameDb)
            inv.Append($"PG_BLOAT_TREND co-fired on {bloat!.ObjectName ?? "a table"} in {bloat.DatabaseName ?? db}: part of this growth is dead space autovacuum has not reclaimed — the bloat card's levers come before any storage decision. ");
        else if (bloat is not null)
            inv.Append($"PG_BLOAT_TREND fired in {bloat.DatabaseName ?? "another database"} — not one this trend names; the two findings do not share a cause on the evidence here. ");
        if (anomaly)
            inv.Append("ANOMALY_PG_DATABASE_GROWTH co-fired: the instance's growth rate in this window is unusual against its own hour-of-week routine, so the fortnight's slope is happening now rather than being a step that already happened. ");

        var rem = new StringBuilder();
        rem.Append("Ask the bloat question first. ");
        if (bloatSameDb)
            rem.Append($"Work the PG_BLOAT_TREND card for {bloat!.ObjectName ?? "the named table"} before buying storage for this: reclaimed dead space is growth that reverses. ");
        else
            rem.Append($"get_pg_table_bloat for {db} shows whether the bytes are live rows or dead ones; growth that is bloat has autovacuum's remedies, and a storage decision made on it buys disk for nothing. ");
        rem.Append($"For growth that is data: retention or archival of the oldest rows in the largest tables (get_pg_table_bloat ranks {db}'s tables by measured heap size) is the lever that shrinks the slope — counter-objective: the rows leave the live database, and a DELETE of that size is its own vacuum and WAL event, so the archive is a batch job, not a statement. ");
        rem.Append("Partitioning the largest time-keyed tables by range lets old partitions be detached or dropped in O(1) instead of deleted row by row — counter-objective: a migration of the table under load and a plan change for every query that reads it. ");
        rem.Append($"Disk free space is NOT collected for a PostgreSQL target, so this card cannot say when the volume fills: bring the volume's headroom to the {FmtBytes(perDay)}/day figure yourself. ");
        rem.Append("Whatever the decision, watch the same fact over the next lookback — a slope that flattens after an archive was data; one that does not is a workload that grew.");

        return new AdviceBlock(headline, inv.ToString().TrimEnd(), rem.ToString().TrimEnd());
    }

    private static string TotalPctText(Dictionary<string, double> m) =>
        m.TryGetValue(PgTargetScorer.GrowthTotalPctKey, out var tp) ? $" ({Pct(tp)})" : string.Empty;

    /* ── the withheld shapes ── */

    /// <summary>The card for a fact with nothing graded: which honesty gate withheld it, in the operator's terms.
    /// This is NOT "no growth".</summary>
    private static AdviceBlock ComposeGrowthUnavailable(int reason, double lookback, double seen, double unsized, double samplesInLookback)
    {
        var inv = new StringBuilder();
        var rem = new StringBuilder();
        inv.Append($"{seen:0} database{(seen == 1 ? string.Empty : "s")} seen in the {lookback:0}-day lookback ending at the analysis window, {unsized:0} unsized, {samplesInLookback:0} distinct collection times. This is a statement about what could be measured, not a growth figure — a withheld trend never reads as zero growth. ");

        string headline;
        switch (reason)
        {
            case PgTargetScorer.GrowthReasonAllUnsized:
                headline = "No database's size could be read in the lookback — the monitoring role may size none of them";
                inv.Append("Every row carried size_bytes NULL: pg_database_size() requires CONNECT on the database or membership in pg_read_all_stats, and the collector writes NULL rather than failing where the role has neither. ");
                rem.Append("Grant the monitoring role pg_read_all_stats (or CONNECT on each database it should size); the hourly collector's next samples then carry sizes and the trend begins from there. Counter-objective: the role can read every database's statistics. ");
                break;
            default:
                headline = $"Database growth has sized databases, but none has {PgTargetScorer.GrowthMinimumSamples} hourly samples in the lookback yet";
                inv.Append($"A trend needs {PgTargetScorer.GrowthMinimumSamples} samples before it is graded (a chosen minimum, not a measured one — threshold_lineage = 0 on this fact). The size collector is hourly, so this is a store younger than three hours of PostgreSQL collection, or a target the collector has only just reached. ");
                rem.Append("Nothing to do but wait for the collector: the fact grades itself once three hourly samples exist for a sized database. ");
                break;
        }

        rem.Append("The trend is the finding; a spot size — which every screen already shows — is not.");
        return new AdviceBlock(headline, inv.ToString().TrimEnd(), rem.ToString().TrimEnd());
    }

    /* ── ANOMALY_PG_DATABASE_GROWTH ── */

    private static readonly AdviceBlock s_databaseGrowthAnomalyStatic = new(
        Headline: "The instance's growth rate is unusual for this time of week",
        Investigation:
            "The instance total in pg_database_size_stats (total_bytes, one value per hourly collection, NULL wherever a " +
            "database could not be sized) is differenced between consecutive samples and rated per day; the window's peak " +
            "and mean rate are compared with this server's own 30-day hour-of-week baseline of the same series. The " +
            "collector is hourly, so the baseline holds about four samples per hour-of-week bucket and thirty per hour — " +
            "it will sit at the hour-only (or flat) tier for months, and the magnitude floor (256 MB a day) carries most of " +
            "the grade until then. Both bars are chosen, not measured; the fact carries threshold_lineage = 0.",
        Remediation:
            "A growth-rate deviation on its own is context: read PG_DATABASE_GROWTH for which database is growing and by how " +
            "much over the fortnight, and get_pg_table_bloat for whether the new bytes are live rows or dead ones. A rate " +
            "spike with no fortnight trend behind it is a load, a restore or a rebuild — find the job; a spike on top of a " +
            "trend is the trend accelerating.");

    private static partial AdviceBlock? ComposeGrowthAnomaly(IReadOnlyDictionary<string, Fact> factsByKey)
    {
        var fallback = s_databaseGrowthAnomalyStatic;
        if (!factsByKey.TryGetValue(PgTargetFactKeys.AnomalyDatabaseGrowth, out var fact))
            return fallback;

        var block = ComposeDeviation(fact, fallback, "Instance growth", "peak_bytes_per_day", v => FmtBytes(v) + " a day");
        if (ReferenceEquals(block, fallback)) return block;

        var sb = new StringBuilder(240);
        var mean = KnobMeta(fact, "mean_bytes_per_day");
        if (mean is > 0)
            sb.Append(" The window's mean rate was ").Append(FmtBytes(mean.Value)).Append(" a day, over ")
              .Append(KnobNum(KnobMeta(fact, "window_samples") ?? 0)).Append(" hourly sample").Append((KnobMeta(fact, "window_samples") ?? 0) == 1 ? string.Empty : "s").Append('.');
        var tier = KnobMeta(fact, "baseline_tier");
        sb.Append(" The series is hourly, so this baseline is thin by construction (about four samples per hour-of-week bucket over 30 days) and will lean on its magnitude floor of ")
          .Append(FmtBytes(Baselines.AnomalyThresholds.PgDatabaseGrowthFloorBytesPerDay)).Append(" a day for months");
        if (tier is { } t)
            sb.Append(" — it graded at tier ").Append(t.ToString("0", CultureInfo.InvariantCulture)).Append(" (0 = full hour-of-week, 1 = hour only, 2 = flat)");
        sb.Append('.');

        var trend = KnobFact(factsByKey, PgTargetFactKeys.DatabaseGrowth);
        if (trend is not null && (KnobMeta(trend, PgTargetScorer.GrowthAvailableKey) ?? 0) >= 1)
        {
            var db = string.IsNullOrEmpty(trend.ObjectName) ? "the fastest-growing database" : trend.ObjectName;
            sb.Append(" PG_DATABASE_GROWTH names ").Append(db).Append(" at ").Append(FmtBytes(KnobMeta(trend, PgTargetScorer.GrowthBytesKey) ?? trend.Value))
              .Append(" over the fortnight").Append(trend.BaseSeverity > 0 ? " — over the line, so this rate is a trend accelerating." : " — under the line, so this rate is a load, not yet a trend.");
        }

        return block with { Investigation = block.Investigation + sb.ToString().TrimEnd() };
    }

    private static string FmtDays(double days) => days switch
    {
        < 1 => $"{days * 24:0.#} hours",
        < 60 => $"{days:0} days",
        < 730 => $"{days / 30.4375:0.#} months",
        _ => $"{days / 365.25:0.#} years",
    };
}
