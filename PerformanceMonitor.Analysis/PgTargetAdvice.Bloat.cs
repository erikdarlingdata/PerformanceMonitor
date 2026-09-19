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
using System.Text;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Advice for table and index bloat trends (filled by lane 13 of #3691, design §3.4). Value-stated from the
/// trend facts — the object, its measured size, the estimate at both ends of the lookback, the growth in
/// bytes and (where computable) as a percentage of the earlier estimate, the sample count and span — and
/// never a <c>CREATE INDEX</c> or <c>REINDEX</c> DDL statement (D8); the levers are named in prose with the
/// counter-objective of each.
///
/// <para><b>The sentence this card exists to prevent</b> is the one that gets someone to run <c>VACUUM
/// (FULL)</c> on a Tuesday because a screen said 86 %. The fleet read behind this family
/// (<c>PgTargetScorer.Bloat.cs</c>) found the spot percentage sitting at 71–99.8 % on most of the measured
/// population's small heaps while their 14-day growth was zero bytes; so the investigation says, in so many
/// words, that the TREND is the finding and the percentage is not, and the remediation orders the levers
/// from "let autovacuum reclaim it" (cheap, online, reversible) through <c>pg_repack</c> (online, an
/// extension and a full copy's worth of I/O) to <c>VACUUM (FULL)</c> (an ACCESS EXCLUSIVE lock for the
/// duration and double the table's space) — last, and only when the space must come back now. The
/// "unavailable" shapes get their own card: a fact withheld by a grant or a size floor must never read as
/// "no bloat".</para>
/// </summary>
public static partial class PgTargetAdvice
{
    /* filled by lane 13 of #3691. */

    private static partial AdviceBlock? ComposeBloat(string key, IReadOnlyDictionary<string, Fact> factsByKey) => key switch
    {
        PgTargetFactKeys.BloatTrend => ComposeBloatTrend(factsByKey),
        PgTargetFactKeys.IndexBloatTrend => ComposeIndexBloatTrend(factsByKey),
        _ => null,
    };

    /* ── PG_BLOAT_TREND ── */

    private static readonly AdviceBlock s_bloatTrendStatic = new(
        Headline: "A table's estimated bloat has grown past the measured line across the fourteen-day lookback",
        Investigation:
            "PostgreSQL's per-table bloat figure in pg_table_bloat_stats is an ESTIMATE from column-width statistics, " +
            "sampled hourly; on the measured population its spot percentage sits at 71–99.8 % for most small heaps " +
            "while their fourteen-day growth is zero bytes — the percentage is arithmetic noise below a few thousand " +
            "pages. So this finding is a TREND: the estimate at the latest sample minus the estimate at the earliest " +
            "usable sample inside a fourteen-day lookback, graded only for tables of at least 64 MiB, and only when " +
            "the growth is both at least 256 MiB and at least a quarter of the earlier estimate (the fleet p90 of " +
            "worst-table growth). Rows whose estimate_unavailable flag is set are excluded outright: the estimator " +
            "reported 88.59 % against a true 0.50 % when the monitoring role could not see pg_stats.",
        Remediation:
            "The trend is the finding; the percentage is not — do not run VACUUM (FULL) because of a percentage. " +
            "First find out why autovacuum is not reclaiming the space (PG_AUTOVACUUM_BACKLOG on the same table, " +
            "PG_XMIN_HOLD pinning the dead tuples): a lower per-table autovacuum_vacuum_scale_factor makes vacuum " +
            "visit sooner at the cost of more vacuum work. If the space must come back, pg_repack rebuilds the " +
            "table online at the cost of installing an extension and a full copy's worth of I/O and disk while it " +
            "runs; VACUUM (FULL) is last — it takes an ACCESS EXCLUSIVE lock for the whole rewrite and needs the " +
            "table's size again in free space.");

    private static AdviceBlock ComposeBloatTrend(IReadOnlyDictionary<string, Fact> facts)
    {
        if (!facts.TryGetValue(PgTargetFactKeys.BloatTrend, out var f))
            return s_bloatTrendStatic;

        var m = f.Metadata;
        var lookback = m.GetValueOrDefault(PgTargetScorer.BloatLookbackDaysKey, PgTargetScorer.BloatLookbackDays);
        var seen = m.GetValueOrDefault(PgTargetScorer.BloatObjectsSeenKey);
        var estimable = m.GetValueOrDefault(PgTargetScorer.BloatObjectsEstimableKey);
        var considered = m.GetValueOrDefault(PgTargetScorer.BloatObjectsConsideredKey);
        var overLine = m.GetValueOrDefault(PgTargetScorer.BloatObjectsOverLineKey);

        if (m.GetValueOrDefault(PgTargetScorer.BloatEstimateAvailableKey) < 1)
            return ComposeBloatUnavailable("table", "tables", (int)m.GetValueOrDefault(PgTargetScorer.BloatUnavailableReasonKey), lookback, seen, estimable, m);

        var table = string.IsNullOrEmpty(f.ObjectName) ? "A table" : f.ObjectName;
        var db = string.IsNullOrEmpty(f.DatabaseName) ? string.Empty : $" in {f.DatabaseName}";
        var growth = m.GetValueOrDefault(PgTargetScorer.BloatGrowthBytesKey, f.Value);
        var pctComputable = m.GetValueOrDefault(PgTargetScorer.BloatGrowthPctComputableKey) >= 1;
        var pct = m.GetValueOrDefault(PgTargetScorer.BloatGrowthPctKey);
        var earlier = m.GetValueOrDefault(PgTargetScorer.BloatEarlierBytesKey);
        var latest = m.GetValueOrDefault(PgTargetScorer.BloatLatestBytesKey);
        var heap = m.GetValueOrDefault(PgTargetScorer.BloatObjectBytesKey);
        var samples = m.GetValueOrDefault(PgTargetScorer.BloatSamplesKey);
        var spanHours = m.GetValueOrDefault(PgTargetScorer.BloatSpanHoursKey);
        var backlog = facts.TryGetValue(PgTargetFactKeys.AutovacuumBacklog, out var b) && b.Severity > 0 ? b : null;
        var backlogSameTable = backlog is not null && PgTargetScorer.BloatAndBacklogShareATable(f, backlog);
        var xminHeld = facts.TryGetValue(PgTargetFactKeys.XminHold, out var xmin) && xmin.Severity > 0;
        var graded = f.BaseSeverity > 0;

        var pctText = pctComputable ? $" ({Pct(pct)} of the earlier estimate)" : " (from an earlier estimate of zero — no percentage to state)";
        var headline = graded
            ? $"{table}{db} grew {FmtBytes(growth)} of estimated bloat{pctText} across {samples:0} hourly samples spanning {FmtHours(spanHours)}"
            : $"{table}{db} is the fastest-growing estimable table at {FmtBytes(growth)}{pctText} across {samples:0} hourly samples — under the measured line";

        var inv = new StringBuilder();
        inv.Append($"The bloat-bytes estimate for this table moved from {FmtBytes(earlier)} to {FmtBytes(latest)} between its earliest and latest usable samples inside the {lookback:0}-day lookback ending at the analysis window ({samples:0} hourly samples, {FmtHours(spanHours)} apart at the ends). ");
        inv.Append($"The heap measures {FmtBytes(heap)} (pg_relation_size, measured — not estimated)");
        if (m.TryGetValue(PgTargetScorer.BloatDeadTuplesKey, out var dead))
        {
            inv.Append($", and carries {Fmt(dead)} dead tuples");
            if (m.TryGetValue(PgTargetScorer.BloatLiveTuplesKey, out var live) && live > 0)
                inv.Append($" against {Fmt(live)} live ({Pct(100.0 * dead / (dead + live))} of the tuple count)");
            inv.Append(" at the latest sample");
        }
        inv.Append(". ");

        if (m.TryGetValue(PgTargetScorer.BloatSpotPctKey, out var spot))
            inv.Append($"The spot estimate reads {Pct(spot)} and was NOT graded: on the measured population the spot percentage sits at 71–99.8% for most small heaps while their fourteen-day growth is zero bytes, so the percentage alone is arithmetic noise and the growth in bytes is what this finding rests on. ");

        inv.Append($"The line is measured — growth of at least {FmtBytes(PgTargetScorer.BloatGrowthConcerningBytes)} AND at least {Pct(100.0 * PgTargetScorer.BloatGrowthConcerningFraction)} of the earlier estimate, the fleet p90 of worst-table fourteen-day growth; {FmtBytes(PgTargetScorer.BloatGrowthCriticalBytes)} is the critical line, which nothing routine on the measured population reached. ");
        inv.Append(graded
            ? "This table crossed it. "
            : "This table did not cross it; the fact is context, not a finding. ");

        var others = NamedObjects(m, PgTargetScorer.BloatNamedGrowthBytesPrefix).Where(n => n.Name != f.ObjectName).ToList();
        if (others.Count > 0)
        {
            inv.Append("Next by growth: ");
            inv.Append(string.Join("; ", others.Select(o =>
            {
                var oPct = m.TryGetValue(PgTargetScorer.BloatNamedGrowthPctPrefix + o.Name, out var p) ? $", {Pct(p)}" : string.Empty;
                var oDead = m.TryGetValue(PgTargetScorer.BloatNamedDeadTuplesPrefix + o.Name, out var d) ? $", {Fmt(d)} dead tuples now" : string.Empty;
                return $"{o.Name} ({FmtBytes(o.Value)}{oPct}{oDead})";
            })));
            inv.Append(". ");
        }
        inv.Append($"Population: {seen:0} table{(seen == 1 ? string.Empty : "s")} seen in the lookback, {estimable:0} with a usable estimate, {considered:0} at or over the {FmtBytes(PgTargetScorer.BloatSizeFloorBytes)} heap floor, {overLine:0} over the line. ");

        if (backlogSameTable)
            inv.Append($"PG_AUTOVACUUM_BACKLOG co-fired on {backlog!.ObjectName ?? "this table"}: the dead tuples autovacuum is not clearing are where this growth is coming from — the backlog is the cause, this trend is the damage. ");
        else if (backlog is not null)
            inv.Append($"PG_AUTOVACUUM_BACKLOG fired on {backlog.ObjectName} — a different table; the two findings do not share a cause on the evidence here. ");
        if (xminHeld)
            inv.Append("PG_XMIN_HOLD co-fired: with the xmin horizon held back, dead tuples newer than it cannot be removed by any vacuum, so their space is not reused and the heap grows around them. ");

        var rem = new StringBuilder();
        rem.Append("The trend is the finding; the percentage is not — nothing here is a reason to run VACUUM (FULL) on a Tuesday. ");
        if (xminHeld)
            rem.Append("Resolve PG_XMIN_HOLD first: while the horizon is pinned, no VACUUM can reclaim these tuples and any rewrite below only moves them. ");
        if (backlogSameTable)
            rem.Append($"Work the PG_AUTOVACUUM_BACKLOG card for {backlog!.ObjectName ?? table} first — until autovacuum keeps up on this table, reclaimed space fills again. ");
        else
            rem.Append($"First find out why autovacuum is not reclaiming the space on {table}: get_pg_autovacuum_health shows its own trigger line and whether it is being reached; a lower per-table autovacuum_vacuum_scale_factor (ALTER TABLE {table} SET (autovacuum_vacuum_scale_factor = …), using the table's own figures from that read) makes vacuum visit sooner. Counter-objective: more vacuum work on this table — more read and write I/O while it runs. ");
        rem.Append($"If {FmtBytes(growth)} must come back now: pg_repack rebuilds the table online, holding only brief locks at the start and end. Counter-objective: the extension has to be installed, and the rebuild costs a full copy's worth of I/O plus the table's size again in disk while it runs. ");
        rem.Append("VACUUM (FULL) is the last lever, not the first: it takes an ACCESS EXCLUSIVE lock on the table for the whole rewrite — every read and write on it waits — and needs the table's size again in free space; it is a maintenance-window decision made on this growth figure and the dead-tuple counts, never on the spot percentage. ");
        rem.Append("Whatever reclaims the space, this trend re-grows unless the vacuum side is fixed; watch the same fact over the next lookback.");

        return new AdviceBlock(headline, inv.ToString().TrimEnd(), rem.ToString().TrimEnd());
    }

    /* ── PG_INDEX_BLOAT_TREND ── */

    private static readonly AdviceBlock s_indexBloatTrendStatic = new(
        Headline: "An index's estimated reclaimable space has grown past the measured line across the fourteen-day lookback",
        Investigation:
            "PostgreSQL's per-index bloat figure in pg_index_bloat is an ESTIMATE from catalog statistics, sampled " +
            "daily and suppressed — with a stated reason — wherever the statistics cannot support it (a partial " +
            "index, a never-analysed parent, a name-typed key, a deduplicated btree denser than the model). On the " +
            "measured population the spot percentage reads 46 % at the median with a MEDIAN index size of 16 kB, so " +
            "this finding is a TREND on est_reclaimable_bytes (the figure the reads rank on — bytes, never a " +
            "percentage): latest minus earliest estimate inside a fourteen-day lookback, for indexes of at least " +
            "64 MiB with at least three daily samples, graded only when the growth is at least 256 MiB and at least a " +
            "quarter of the earlier estimate.",
        Remediation:
            "Check whether the index is read at all (get_pg_index_usage) before spending anything on it — an " +
            "unused index's bloat is a reason to drop it, not to rebuild it. For an index that is used, rebuilding " +
            "it concurrently (REINDEX CONCURRENTLY, PostgreSQL 12+) reclaims the space online; counter-objective: a " +
            "second copy of the index on disk while it builds, the I/O to build it, and a transaction that holds the " +
            "xmin horizon for the duration. If its table's bloat is growing too (PG_BLOAT_TREND), fix the vacuum side " +
            "first or the index re-bloats.");

    private static AdviceBlock ComposeIndexBloatTrend(IReadOnlyDictionary<string, Fact> facts)
    {
        if (!facts.TryGetValue(PgTargetFactKeys.IndexBloatTrend, out var f))
            return s_indexBloatTrendStatic;

        var m = f.Metadata;
        var lookback = m.GetValueOrDefault(PgTargetScorer.BloatLookbackDaysKey, PgTargetScorer.BloatLookbackDays);
        var seen = m.GetValueOrDefault(PgTargetScorer.BloatObjectsSeenKey);
        var estimable = m.GetValueOrDefault(PgTargetScorer.BloatObjectsEstimableKey);
        var considered = m.GetValueOrDefault(PgTargetScorer.BloatObjectsConsideredKey);
        var overLine = m.GetValueOrDefault(PgTargetScorer.BloatObjectsOverLineKey);

        if (m.GetValueOrDefault(PgTargetScorer.BloatEstimateAvailableKey) < 1)
            return ComposeBloatUnavailable("index", "indexes", (int)m.GetValueOrDefault(PgTargetScorer.BloatUnavailableReasonKey), lookback, seen, estimable, m);

        var index = string.IsNullOrEmpty(f.ObjectName) ? "An index" : f.ObjectName;
        var db = string.IsNullOrEmpty(f.DatabaseName) ? string.Empty : $" in {f.DatabaseName}";
        var growth = m.GetValueOrDefault(PgTargetScorer.BloatGrowthBytesKey, f.Value);
        var pctComputable = m.GetValueOrDefault(PgTargetScorer.BloatGrowthPctComputableKey) >= 1;
        var pct = m.GetValueOrDefault(PgTargetScorer.BloatGrowthPctKey);
        var earlier = m.GetValueOrDefault(PgTargetScorer.BloatEarlierBytesKey);
        var latest = m.GetValueOrDefault(PgTargetScorer.BloatLatestBytesKey);
        var size = m.GetValueOrDefault(PgTargetScorer.BloatObjectBytesKey);
        var samples = m.GetValueOrDefault(PgTargetScorer.BloatSamplesKey);
        var spanHours = m.GetValueOrDefault(PgTargetScorer.BloatSpanHoursKey);
        var graded = f.BaseSeverity > 0;
        var tableTrend = facts.TryGetValue(PgTargetFactKeys.BloatTrend, out var t) && t.Severity > 0
            && PgTargetScorer.BloatNamedObjects(f, parentTables: true).Overlaps(PgTargetScorer.BloatNamedObjects(t));

        var pctText = pctComputable ? $" ({Pct(pct)} of the earlier estimate)" : " (from an earlier estimate of zero — no percentage to state)";
        var headline = graded
            ? $"{index}{db} grew {FmtBytes(growth)} of estimated reclaimable space{pctText} across {samples:0} daily samples spanning {FmtHours(spanHours)}"
            : $"{index}{db} is the fastest-growing estimable index at {FmtBytes(growth)}{pctText} across {samples:0} daily samples — under the measured line";

        var inv = new StringBuilder();
        inv.Append($"The reclaimable-bytes estimate for this index moved from {FmtBytes(earlier)} to {FmtBytes(latest)} between its earliest and latest estimated samples inside the {lookback:0}-day lookback ending at the analysis window ({samples:0} daily samples, {FmtHours(spanHours)} apart at the ends). ");
        inv.Append($"The index measures {FmtBytes(size)} (relpages × block size — measured, not estimated). ");
        if (m.TryGetValue(PgTargetScorer.BloatSpotPctKey, out var spot))
            inv.Append($"The spot estimate reads {Pct(spot)} and was NOT graded: on the measured population the index spot percentage reads 46% at the median on indexes whose median size is 16 kB, so it is noise without a size floor and a trend. ");
        inv.Append($"The line is measured — growth of at least {FmtBytes(PgTargetScorer.BloatGrowthConcerningBytes)} AND at least {Pct(100.0 * PgTargetScorer.BloatGrowthConcerningFraction)} of the earlier estimate (the fleet p90 of worst-table fourteen-day growth, the engine-neutral quantity shared with PG_BLOAT_TREND); an index needs {PgTargetScorer.IndexBloatMinimumSamples} daily samples before its trend is graded at all (a chosen minimum, not a measured one). ");
        inv.Append(graded ? "This index crossed it. " : "This index did not cross it; the fact is context, not a finding. ");

        var others = NamedObjects(m, PgTargetScorer.BloatNamedGrowthBytesPrefix).Where(n => n.Name != f.ObjectName).ToList();
        if (others.Count > 0)
        {
            inv.Append("Next by growth: ");
            inv.Append(string.Join("; ", others.Select(o =>
            {
                var oPct = m.TryGetValue(PgTargetScorer.BloatNamedGrowthPctPrefix + o.Name, out var p) ? $", {Pct(p)}" : string.Empty;
                return $"{o.Name} ({FmtBytes(o.Value)}{oPct})";
            })));
            inv.Append(". ");
        }
        inv.Append($"Population: {seen:0} index{(seen == 1 ? string.Empty : "es")} seen in the lookback, {estimable:0} with a usable estimate, {considered:0} at or over the {FmtBytes(PgTargetScorer.BloatSizeFloorBytes)} floor with enough samples, {overLine:0} over the line. ");
        AppendSkippedShare(inv, m);

        if (tableTrend)
            inv.Append("PG_BLOAT_TREND co-fired on this index's table: heap and index are growing together, which is churn on the table — the index will re-bloat after a rebuild until the table's vacuum side is fixed. ");

        var rem = new StringBuilder();
        rem.Append("The trend is the finding; the percentage is not. ");
        rem.Append($"Check whether {index} is read at all first — get_pg_index_usage shows its scan counts and whether a constraint or replica identity depends on it; an unused index's bloat is a reason to drop it, not to rebuild it. ");
        if (tableTrend)
            rem.Append("Work the PG_BLOAT_TREND card for the table first: a rebuilt index on a table whose dead tuples autovacuum is not clearing grows back. ");
        rem.Append($"For an index that is used and must give back {FmtBytes(growth)}: rebuilding it concurrently (REINDEX CONCURRENTLY, PostgreSQL 12+) reclaims the space online. Counter-objective: a second copy of the index on disk while it builds, the I/O to build it, and a transaction that holds the xmin horizon for the duration (PG_XMIN_HOLD will see it). ");
        rem.Append("A non-concurrent rebuild holds a lock that blocks writes to the table for the duration and is a maintenance-window decision. Watch the same fact over the next lookback — a rebuilt index that re-grows at this rate is a table-churn problem wearing an index's name.");

        return new AdviceBlock(headline, inv.ToString().TrimEnd(), rem.ToString().TrimEnd());
    }

    /* ── the withheld shapes ── */

    /// <summary>
    /// The card for a fact with no graded object: which honesty gate withheld it, in the operator's terms,
    /// and what opens it. Shared by both facts because the reasons overlap (size floor) and the sentence that
    /// matters is the same — this is NOT "no bloat".
    /// </summary>
    private static AdviceBlock ComposeBloatUnavailable(string noun, string plural, int reason, double lookback, double seen, double estimable, Dictionary<string, double> m)
    {
        var inv = new StringBuilder();
        var rem = new StringBuilder();
        var what = noun == "table" ? "bloat" : "reclaimable-space";
        inv.Append($"{seen:0} {(seen == 1 ? noun : plural)} seen in the {lookback:0}-day lookback ending at the analysis window; {estimable:0} with a usable estimate. This is a statement about what could be measured, not a bloat figure — a withheld estimate never reads as zero bloat. ");

        string headline;
        switch (reason)
        {
            case PgTargetScorer.BloatReasonEstimateUnavailable:
                headline = $"No table's {what} estimate could be used in the lookback — estimate_unavailable on every row";
                inv.Append("Every row carried estimate_unavailable = true: the estimator had no basis, most commonly because the monitoring role lacks SELECT on the tables, so pg_stats filters every column out (measured against a pg_monitor-only role the estimator did not fail — it reported 88.59% bloat for a table whose true figure is 0.50%); or because the tables have never been analysed. ");
                rem.Append("Grant the monitoring role pg_read_all_data (or SELECT on the monitored tables) so pg_stats is visible to it, and make sure the tables have been ANALYZEd; the hourly collector's next samples then carry a usable estimate and the trend begins from there. Counter-objective: the monitoring role can read table data. ");
                break;
            case PgTargetScorer.BloatReasonPgstattupleUnavailable:
                headline = "No index's bloat estimate could be used in the lookback, and pgstattuple is not installed for an exact measurement";
                inv.Append("Every index seen was skipped with a stated reason (get_pg_index_bloat shows each — a partial index, a never-analysed parent, a name-typed key, column widths hidden from the monitoring role, or a deduplicated btree denser than the model predicts), and no database the collector looked at has the pgstattuple extension, so the exact route the skip reasons point at (pgstatindex) is closed too. ");
                AppendSkippedShare(inv, m);
                rem.Append("Read the reasons in get_pg_index_bloat — an ANALYZE of the parent or a grant on pg_stats opens the estimate for most of them. For an exact answer on a specific index, install the pgstattuple extension in that database and run pgstatindex on it; counter-objective: pgstatindex reads every page of the index. ");
                break;
            case PgTargetScorer.BloatReasonAllSkipped:
                headline = "No index's bloat estimate could be used in the lookback — every index was skipped with a stated reason";
                inv.Append("Every index seen was skipped with a stated reason (get_pg_index_bloat shows each). pgstattuple IS installed, so pgstatindex gives an exact answer for any specific index. ");
                AppendSkippedShare(inv, m);
                rem.Append("Read the reasons in get_pg_index_bloat — an ANALYZE of the parent or a grant on pg_stats opens the estimate for most of them; for a specific index, pgstatindex measures it exactly at the cost of reading every page. ");
                break;
            case PgTargetScorer.BloatReasonInsufficientSamples:
                headline = $"Index bloat has estimable indexes over the size floor, but none has {PgTargetScorer.IndexBloatMinimumSamples} daily samples in the lookback yet";
                inv.Append($"The index collector is daily; a trend needs {PgTargetScorer.IndexBloatMinimumSamples} samples before it is graded (a chosen minimum, not a measured one — threshold_lineage = 0 on this fact). {m.GetValueOrDefault(PgTargetScorer.BloatSamplesInLookbackKey):0} distinct collection times were seen in the lookback. ");
                rem.Append("Nothing to do but wait for the collector: the fact grades itself once three daily samples exist for an index over the floor. ");
                break;
            default:
                headline = $"No {noun} at or over the {FmtBytes(PgTargetScorer.BloatSizeFloorBytes)} size floor has a usable {what} estimate in the lookback";
                inv.Append($"Estimable {plural} exist, but none is at least {FmtBytes(PgTargetScorer.BloatSizeFloorBytes)} at the latest sample (or has two usable samples to difference). Below the floor the column-width arithmetic the estimate rests on is noise against a few thousand pages — on the measured population the small heaps and 16 kB indexes are exactly where the 71–99.8% spot figures live — so no percentage is spoken for them by design. ");
                rem.Append("Nothing to do: small objects are not graded for bloat here, and their spot percentages in get_pg_table_bloat / get_pg_index_bloat should be read with that in mind. ");
                break;
        }

        rem.Append("The trend is the finding; a spot percentage — when one becomes available — is not.");
        return new AdviceBlock(headline, inv.ToString().TrimEnd(), rem.ToString().TrimEnd());
    }

    private static void AppendSkippedShare(StringBuilder inv, Dictionary<string, double> m)
    {
        if (!m.TryGetValue(PgTargetScorer.IndexBloatSkippedShareKey, out var share) || share <= 0)
            return;
        var reasons = m.GetValueOrDefault(PgTargetScorer.IndexBloatSkipReasonsKey);
        var pgstattuple = m.GetValueOrDefault(PgTargetScorer.IndexBloatPgstattupleAvailableKey) >= 1;
        inv.Append($"{Pct(100.0 * share)} of the indexes seen were skipped at their latest sample{(share >= 0.5 ? " — a majority" : string.Empty)}, across {reasons:0} distinct reason{(reasons == 1 ? string.Empty : "s")} (counts only; get_pg_index_bloat shows the reasons); pgstattuple is {(pgstattuple ? "installed, so pgstatindex can measure any of them exactly" : "not installed, so the exact route is closed until it is")}. ");
    }

    /// <summary>The named objects a bloat fact carries under <paramref name="prefix"/>, largest value first.</summary>
    private static List<(string Name, double Value)> NamedObjects(Dictionary<string, double> m, string prefix) =>
        m.Where(kv => kv.Key.StartsWith(prefix, StringComparison.Ordinal) && kv.Key.Length > prefix.Length)
            .Select(kv => (Name: kv.Key[prefix.Length..], Value: kv.Value))
            .OrderByDescending(o => o.Value)
            .ThenBy(o => o.Name, StringComparer.Ordinal)
            .ToList();
}
