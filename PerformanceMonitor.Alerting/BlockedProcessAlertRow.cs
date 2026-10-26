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

namespace PerformanceMonitor.Alerting;

/// <summary>
/// One blocked-process event as the alert engine consumes it (Phase-5 slice B) — the superset of
/// fields the shared <see cref="AlertContextBuilders.BuildBlockingContext"/> builder, the
/// <c>BlockingIncidentGrouper</c> projection, the alert loop's excluded-database count filter, and
/// the XE→DMV fallback merge (<see cref="BlockedProcessReportMerge"/>) actually touch. Lite's
/// grid row (<c>BlockedProcessReportRow</c>) derives from this so its store reads flow into the
/// shared builders without any mapping copy; the Darling adapter materializes this type directly.
/// Store-specific display members (formatted local times, grid badges) stay on the derived types.
/// </summary>
public class BlockedProcessAlertRow
{
    /// <summary>Source tag for rows captured by the blocked-process-report XE session.</summary>
    public const string XeReportSource = "blocked-process-report";

    /// <summary>Source tag for rows from the always-on DMV blocking-snapshot fallback.</summary>
    public const string DmvSnapshotSource = "DMV snapshot";

    public DateTime? EventTime { get; set; }
    public string DatabaseName { get; set; } = "";
    public int BlockedSpid { get; set; }
    public int BlockingSpid { get; set; }
    public long WaitTimeMs { get; set; }
    public string LockMode { get; set; } = "";
    public string BlockedSqlText { get; set; } = "";
    public string BlockingSqlText { get; set; } = "";
    public string BlockedProcessReportXml { get; set; } = "";
    public string ContentiousObject { get; set; } = "";

    /// <summary>
    /// Where this row came from: the blocked-process-report XE ("blocked-process-report") or the
    /// always-on DMV snapshot fallback ("DMV snapshot"). Surfaced as a grid badge so a row captured
    /// when the blocked-process threshold was unset (e.g. AWS RDS) is distinguishable.
    /// </summary>
    public string Source { get; set; } = XeReportSource;

    public bool HasReportXml => !string.IsNullOrEmpty(BlockedProcessReportXml);

    /// <summary>
    /// The head blocker's session facts (login time, last request end, open transaction count, transaction log bytes
    /// used) exist only on a DMV-snapshot row: the report XML carries none of them. When the merge drops a DMV row because a
    /// report row covers the same pair in the same minute, it calls this on the report row so the facts are not lost with
    /// the dropped row. A field the report row already has is kept. The base row type has none of the fields, so it does
    /// nothing; each row type that stores them overrides.
    /// </summary>
    public virtual void FillHeadEvidenceFrom(BlockedProcessAlertRow dmvRow) { }
}

/// <summary>
/// The XE→DMV blocking fallback merge — moved verbatim from the tail of Lite's
/// <c>LocalDataService.AppendDmvBlockedProcessGridRowsAsync</c> so the Lite and Darling
/// <see cref="IAlertReadAdapter"/> implementations reproduce EXACTLY the same semantics:
/// keep all blocked-process-report rows; append a DMV-snapshot row only when no BPR row covers
/// the same (blocked, blocker) SPID pair within the same minute (BPR preferred — richer report
/// XML); then re-cap to the newest <paramref name="cap"/> rows. OrderByDescending is stable, so
/// BPR rows (added first) win ties over DMV rows at the same event time.
/// </summary>
public static class BlockedProcessReportMerge
{
    /// <summary>Lite's grid LIMIT — both stores fetch and re-cap at 200 rows.</summary>
    public const int DefaultCap = 200;

    public static void AppendDmvFallbackRows<TRow>(List<TRow> items, List<TRow> dmvItems, int cap = DefaultCap)
        where TRow : BlockedProcessAlertRow
    {
        if (items is null) throw new ArgumentNullException(nameof(items));
        if (dmvItems is null || dmvItems.Count == 0) return;

        // Dedup: keep all BPR rows; add a DMV row only if no BPR row covers the same (blocked, blocker)
        // SPID within the same minute. BPR is preferred (richer report XML), but the report XML has no head-blocker
        // session facts, so a dropped DMV row hands its facts to the report rows it lost to.
        var seen = new HashSet<(int, int, long)>();
        var reportRows = new Dictionary<(int, int, long), List<TRow>>();
        foreach (var b in items)
            if (b.EventTime.HasValue)
            {
                var reportKey = (b.BlockedSpid, b.BlockingSpid, b.EventTime.Value.Ticks / TimeSpan.TicksPerMinute);
                seen.Add(reportKey);
                if (!reportRows.TryGetValue(reportKey, out var sameKey))
                    reportRows[reportKey] = sameKey = new List<TRow>();
                sameKey.Add(b);
            }

        foreach (var d in dmvItems)
        {
            var key = (d.BlockedSpid, d.BlockingSpid, (d.EventTime?.Ticks ?? 0) / TimeSpan.TicksPerMinute);
            if (seen.Add(key))
                items.Add(d);
            else if (reportRows.TryGetValue(key, out var covering))
                foreach (var report in covering)
                    report.FillHeadEvidenceFrom(d);
        }

        // Re-cap to the grid's LIMIT, newest first. OrderByDescending is stable, so BPR rows (added
        // first) win ties over DMV rows at the same event time — matching the Dashboard grid.
        var merged = items.OrderByDescending(i => i.EventTime ?? DateTime.MinValue).Take(cap).ToList();
        items.Clear();
        items.AddRange(merged);
    }
}
