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
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// The pure alert-context builders (Phase-5 slices A + B) both apps' alert engines call, plus the
/// small pure helpers they share (<see cref="ContextToDetailText"/>, <see cref="TruncateText"/>,
/// <see cref="GetBreachedVolumes"/>, <see cref="FormatLowDiskThreshold"/>). Moved verbatim from the
/// line-identical private copies in Lite's and the Dashboard's <c>MainWindow.AlertEngine.cs</c> so
/// the rendered alert detail (and the #1140 dedup fingerprints) can no longer drift between the
/// apps — and so the headless Darling alert engine renders the same alerts from the same rows.
/// <para>
/// The ONE reconciled difference at extraction time: <see cref="BuildLongRunningQueryContext"/>
/// adopts the Dashboard's version, which renders a ("Program", ProgramName) detail item Lite's
/// copy lacked — so Lite's long-running-query alerts gain the Program field.
/// </para>
/// <para>
/// Slice B lifted the last two builders — <see cref="BuildBlockingContext"/> and
/// <see cref="BuildDeadlockContext"/> — out of Lite's async wrappers (Lite's grouped rendering is
/// canonical per the Phase-5 review): the fetch moved behind <see cref="IAlertReadAdapter"/> and
/// the bodies are otherwise verbatim, with Lite's fields/settings (server name, excluded
/// databases) as parameters. The Dashboard's async blocking/deadlock builders deliberately remain
/// app-side — its rendering diverged and convergence is a separately-planned migration.
/// </para>
/// </summary>
public static class AlertContextBuilders
{
    /// <summary>
    /// The blocking-alert context from the store's blocked-process rows (XE + DMV-fallback merged —
    /// see <see cref="IAlertReadAdapter.GetRecentBlockedProcessReportsAsync"/>). Body verbatim from
    /// Lite's pre-slice-B <c>BuildBlockingContextAsync</c> minus the fetch: excluded databases drop
    /// their rows (no-database rows always pass); samples of the same chain collapse into one group
    /// (#1140/#1141) with true occurrence count + wait range; capped at 10 groups with a "+N more"
    /// trailer; the first row with report XML becomes the attachment. Null when nothing renders.
    /// </summary>
    /// <param name="decorateIncidents">
    /// #2216: optional hook applied to the grouped incidents BEFORE they are rendered, so a caller that
    /// keeps per-fingerprint history (the engine, via <see cref="IncidentOccurrenceAccumulator"/>) can
    /// attach each incident's monotonic total. It has to run here rather than on the finished context
    /// because the renderer projects the incidents into detail items in the same pass — decorating
    /// afterwards would leave the rendered facts describing the undecorated values. Null for callers with
    /// no such history, which is every path that built this context before #2216.
    /// </param>
    public static AlertContext? BuildBlockingContext(
        string serverName, IReadOnlyList<BlockedProcessAlertRow>? events, IReadOnlyList<string> excludedDatabases,
        Func<IReadOnlyList<AlertIncident>, IReadOnlyList<AlertIncident>>? decorateIncidents = null)
    {
        if (events == null || events.Count == 0) return null;

        var filtered = BlockingWaitRows(events, excludedDatabases);
        if (filtered.Count == 0) return null;

        var groups = GroupBlocking(serverName, filtered);

        const int maxGroups = 10;
        var shown = groups.Take(maxGroups).ToList();

        var context = new AlertContext();
        foreach (var g in shown)
        {
            var item = new AlertDetailItem
            {
                Heading = g.OccurrenceCount > 1 ? $"Blocking chain (x{g.OccurrenceCount})" : "Blocking chain",
                Fields = new()
            };
            if (!string.IsNullOrEmpty(g.Database))
                item.Fields.Add(("Database", g.Database));
            if (!string.IsNullOrEmpty(g.BlockedQuery))
                item.Fields.Add(("Blocked Query", TruncateStatement(g.BlockedQuery)));
            if (!string.IsNullOrEmpty(g.BlockingQuery))
                item.Fields.Add(("Blocking Query", TruncateStatement(g.BlockingQuery)));
            item.Fields.Add(("Wait Range", g.Incident.WaitRange ?? g.MaxWaitMs.ToString()));
            context.Details.Add(item);
        }

        /* Surface the true total instead of silently dropping (gotqn's report). */
        if (groups.Count > maxGroups)
        {
            context.Details.Add(new AlertDetailItem
            {
                Heading = $"+{groups.Count - maxGroups} more distinct blocking {(groups.Count - maxGroups == 1 ? "incident" : "incidents")}",
                Fields = new()
            });
        }

        /* The alert-level attachment, for the ONE card an unfiltered Summary send produces: it lists every
           incident, so the first report in the window belongs to something it describes. Each incident also
           carries its own (#3330, in GroupBlocking) for the two paths that render a SUBSET — the per-event
           splitter and #3313's delivery filter — where "the first in the window" is another incident's. */
        var firstXml = filtered.FirstOrDefault(e => e.HasReportXml)?.BlockedProcessReportXml;
        if (!string.IsNullOrEmpty(firstXml))
        {
            context.AttachmentXml = firstXml;
            context.AttachmentFileName = AlertIncidentAttachment.BlockedProcessReportFileName;
        }

        AlertIncidentRenderer.Apply(context, Decorate(shown.Select(g => g.Incident).ToList(), decorateIncidents));

        return context.Details.Count == 0 ? null : context;
    }

    /// <summary>
    /// The deadlock-alert context from the store's deadlock rows. Deadlocks whose processes ALL ran in
    /// excluded databases are dropped (<see cref="IsDeadlockExcluded"/>); the first graph XML becomes
    /// the attachment; ALL deadlocks in the window feed the #1140 involved-object fingerprint grouping.
    /// Null when nothing survives.
    ///
    /// <para>#2108 reshaped what displays: each fingerprint incident is now a SELF-CONTAINED unit — its
    /// own Database (#2109), Victim SQL, Processes, Dedup Key, Involved Objects, Occurrences — rendered
    /// via <see cref="AlertIncidentRenderer.BuildItem"/> with the forensic fields INCLUDED, and the old
    /// standalone "Deadlock Victim" items are kept only for deadlocks the fingerprint cannot see
    /// (no parseable objects). Before, the victim fields and the fingerprint metadata lived in separate
    /// items — on a multi-incident card there was no way to tell which victim belonged to which
    /// fingerprint, and the two lists even disagreed on membership (victims = first 3 raw events,
    /// incidents = all fingerprints).</para>
    /// </summary>
    /// <param name="decorateIncidents">
    /// #2216: see <see cref="BuildBlockingContext"/> — the same pre-render hook, for the same reason. This
    /// builder renders each incident itself rather than through
    /// <see cref="AlertIncidentRenderer.Apply"/> (#2108's self-contained cards), so the hook has to sit
    /// ahead of that loop too.
    /// </param>
    public static AlertContext? BuildDeadlockContext(
        string serverName, IReadOnlyList<DeadlockAlertRow>? deadlocks, IReadOnlyList<string> excludedDatabases,
        Func<IReadOnlyList<AlertIncident>, IReadOnlyList<AlertIncident>>? decorateIncidents = null)
    {
        if (deadlocks == null || deadlocks.Count == 0) return null;

        var filtered = FilterDeadlocks(deadlocks, excludedDatabases);
        if (filtered.Count == 0) return null;

        var context = new AlertContext();
        /* The alert-level attachment, for the ONE card an unfiltered Summary send produces — see the same
           note in BuildBlockingContext. Each incident carries its own graph too (#3330, in
           GroupParsedDeadlocks). */
        var firstGraph = filtered.FirstOrDefault(d => d.HasDeadlockXml)?.DeadlockGraphXml;
        if (!string.IsNullOrEmpty(firstGraph))
        {
            context.AttachmentXml = firstGraph;
            context.AttachmentFileName = AlertIncidentAttachment.DeadlockGraphFileName;
        }

        /* One parse pass per deadlock: the fingerprint's object set and the discrete Database fact's
           database set (#2109) both come off the graph. */
        var parsed = ParseDeadlocks(filtered);

        /* Deadlocks the fingerprint cannot see (no parseable objects) would vanish entirely under the
           incident-only rendering, so they keep the standalone victim item — the #1140 rule that "the
           builder still displays them", now scoped to exactly the events that need it. */
        foreach (var p in parsed.Where(p => p.Objects.Count == 0).Take(3))
        {
            var item = new AlertDetailItem
            {
                Heading = "Deadlock Victim",
                Fields = new()
            };

            if (p.Databases.Count > 0)
                item.Fields.Add(("Database", string.Join(", ", p.Databases)));
            if (!string.IsNullOrEmpty(p.Row.VictimSqlText))
                item.Fields.Add(("Victim SQL", TruncateStatement(p.Row.VictimSqlText)));
            if (!string.IsNullOrEmpty(p.Row.ProcessSummary))
                item.Fields.Add(("Processes", p.Row.ProcessSummary));
            /* #3442: the same per-party facts the fingerprinted item carries. Both render paths take
               them from the one parse in ParseDeadlocks, because the two lists disagreeing on what a
               deadlock's parties were is the #2108 defect in a narrower place. */
            foreach (var party in p.Parties)
                item.Fields.Add(party);

            context.Details.Add(item);
        }

        /* #1140: fingerprint each deadlock by its sorted involved-object set, across ALL deadlocks in
           the window, grouped so recurrences over the same objects collapse to one incident with a
           count. Each incident renders self-contained (#2108): heading + its representative's forensic
           fields + the dedup metadata, one item per incident. */
        var groups = GroupParsedDeadlocks(serverName, parsed);
        var incidents = Decorate(groups.Select(g => g.Incident).ToList(), decorateIncidents);
        if (incidents.Count > 0)
        {
            context.Incidents = new List<AlertIncident>(incidents);
            for (int n = 0; n < incidents.Count; n++)
            {
                var heading = incidents.Count == 1 ? "Deadlock" : $"Deadlock {n + 1} of {incidents.Count}";
                context.Details.Add(AlertIncidentRenderer.BuildItem(incidents[n], heading, includeDetailFields: true));
            }
        }

        return context;
    }

    /// <summary>
    /// #2216: the fingerprinted incidents for a set of blocked-process rows — the SAME grouping
    /// <see cref="BuildBlockingContext"/> renders, exposed so the alert engine can observe them on every
    /// sweep rather than only on the sweeps that deliver an alert.
    ///
    /// <para>It has to be the same grouping, not a parallel implementation: the engine's occurrence state is
    /// keyed by fingerprint, so a filter or identity rule that drifted between the counting path and the
    /// rendering path would silently key them differently and every delivered incident would look like a
    /// first contact. Both paths share <c>FilterBlocking</c> and <c>GroupBlocking</c> for that reason.</para>
    ///
    /// <para>Uncapped, unlike the rendered list. The render cap is a display budget; a fingerprint outside
    /// the top 10 still has a live incident, and dropping it from the observation would reset its total the
    /// next time it surfaced.</para>
    /// </summary>
    public static IReadOnlyList<AlertIncident> BlockingIncidents(
        string serverName, IReadOnlyList<BlockedProcessAlertRow>? events, IReadOnlyList<string> excludedDatabases)
    {
        if (events == null || events.Count == 0) return Array.Empty<AlertIncident>();

        var filtered = BlockingWaitRows(events, excludedDatabases);
        if (filtered.Count == 0) return Array.Empty<AlertIncident>();

        return GroupBlocking(serverName, filtered).Select(g => g.Incident).ToList();
    }

    /// <summary>
    /// #2216: the deadlock twin of <see cref="BlockingIncidents"/> — same grouping
    /// <see cref="BuildDeadlockContext"/> uses, for the same reason.
    /// </summary>
    public static IReadOnlyList<AlertIncident> DeadlockIncidents(
        string serverName, IReadOnlyList<DeadlockAlertRow>? deadlocks, IReadOnlyList<string> excludedDatabases)
    {
        if (deadlocks == null || deadlocks.Count == 0) return Array.Empty<AlertIncident>();

        var filtered = FilterDeadlocks(deadlocks, excludedDatabases);
        if (filtered.Count == 0) return Array.Empty<AlertIncident>();

        return GroupDeadlocks(serverName, filtered).Select(g => g.Incident).ToList();
    }

    /// <summary>
    /// #2362: the observation lists for the five remaining fingerprinted alerts, mirroring
    /// <see cref="BlockingIncidents"/> and <see cref="DeadlockIncidents"/>.
    ///
    /// <para><b>Uncapped, and that is the whole point.</b> Each context builder renders a capped subset
    /// (3 for long-running queries and anomalous jobs, 5 for the rest) because a card with fifty entries
    /// helps nobody. The render cap is a display budget; a fingerprint outside it still has a live incident,
    /// and observing only the displayed subset would reset the total of anything that fell out of the top N —
    /// a subtler version of the undercount #2216 exists to fix, reintroduced by the fix for it.</para>
    ///
    /// <para>Each is a pure function of a list, so the same builder serves both callers: the check passes the
    /// FULL list to observe, the context builder passes its capped <c>shown</c> to render. One grouping rule,
    /// two inputs, no way for the two to disagree about what a fingerprint is.</para>
    /// </summary>
    public static IReadOnlyList<AlertIncident> LongRunningQueryIncidents(
        string serverName, IReadOnlyList<LongRunningQueryInfo>? queries)
    {
        if (queries is null || queries.Count == 0) return Array.Empty<AlertIncident>();

        /* #1140: dedup key = query_hash (stable across literals/plans). Null hash -> no incident.
           #5711: an all-zero hash is "the engine did not say" (BACKUP, DBCC and some maintenance requests carry
           0x0000000000000000), not a query, and keyed on as-is it folded every such request in one database into
           one incident. The original rule extends to it: a query whose hash is unknown (NULL, empty or all-zero,
           QueryHashIdentity.IsUnknown) emits no incident, whatever its text. A known hash keys exactly as before. */
        return queries
            .Where(q => !QueryHashIdentity.IsUnknown(q.QueryHash))
            .Select(q => AlertFingerprint.ForKey(serverName, AlertFingerprint.Query, q.QueryHash!,
                string.IsNullOrEmpty(q.DatabaseName) ? Array.Empty<string>() : new[] { q.DatabaseName },
                database: q.DatabaseName))
            .Where(i => i is not null).Select(i => i!).ToList();
    }

    /// <inheritdoc cref="LongRunningQueryIncidents"/>
    public static IReadOnlyList<AlertIncident> VolumeFreeSpaceIncidents(
        string serverName, IReadOnlyList<VolumeFreeSpaceInfo>? volumes)
    {
        if (volumes is null || volumes.Count == 0) return Array.Empty<AlertIncident>();

        /* #1140: dedup key per volume (the drive/mount point). */
        return volumes
            .Select(v => AlertFingerprint.ForKey(serverName, AlertFingerprint.Disk, v.MountPoint, new[] { v.MountPoint }))
            .Where(i => i is not null).Select(i => i!).ToList();
    }

    /// <inheritdoc cref="LongRunningQueryIncidents"/>
    public static IReadOnlyList<AlertIncident> PvsPressureIncidents(
        string serverName, IReadOnlyList<PvsPressureInfo>? databases)
    {
        if (databases is null || databases.Count == 0) return Array.Empty<AlertIncident>();

        return databases
            .Select(d => AlertFingerprint.ForKey(serverName, AlertFingerprint.Database, d.DatabaseName, new[] { d.DatabaseName },
                database: d.DatabaseName))
            .Where(i => i is not null).Select(i => i!).ToList();
    }

    /// <inheritdoc cref="LongRunningQueryIncidents"/>
    public static IReadOnlyList<AlertIncident> AnomalousJobIncidents(
        string serverName, IReadOnlyList<AnomalousJobInfo>? jobs)
    {
        if (jobs is null || jobs.Count == 0) return Array.Empty<AlertIncident>();

        /* #1140: dedup key per job (job name, scoped to the instance via serverName). */
        return jobs
            .Select(j => AlertFingerprint.ForKey(serverName, AlertFingerprint.Job, j.JobName, new[] { j.JobName }))
            .Where(i => i is not null).Select(i => i!).ToList();
    }

    /// <inheritdoc cref="LongRunningQueryIncidents"/>
    public static IReadOnlyList<AlertIncident> FailedJobIncidents(
        string serverName, IReadOnlyList<FailedJobInfo>? jobs)
    {
        if (jobs is null || jobs.Count == 0) return Array.Empty<AlertIncident>();

        return jobs
            .Select(j => AlertFingerprint.ForKey(serverName, AlertFingerprint.Job, j.JobName, new[] { j.JobName }))
            .Where(i => i is not null).Select(i => i!).ToList();
    }

    /// <summary>
    /// #2349: the file-growth observation list. UNCAPPED, like every other <c>*Incidents</c> builder and for
    /// the reason #2362 established — the card renders a capped subset, and observing only what is displayed
    /// resets the total of any file that fell out of the top N.
    ///
    /// <para>Fingerprinted on the FILE, not the database: a database with eight tempdb data files that all grow
    /// together is eight files and one problem, but a log file that runs away while its data files sit still is
    /// a different incident from its neighbours, and collapsing them on database name would merge the two.</para>
    /// </summary>
    public static IReadOnlyList<AlertIncident> FileGrowthIncidents(
        string serverName, IReadOnlyList<DatabaseFileGrowthInfo>? files)
    {
        if (files is null || files.Count == 0) return Array.Empty<AlertIncident>();

        return files
            .Select(f => AlertFingerprint.ForKey(
                serverName, AlertFingerprint.Disk, $"{f.DatabaseName}|{f.FileName}",
                new[] { $"{f.DatabaseName}.{f.FileName}" },
                database: f.DatabaseName))
            .Where(i => i is not null).Select(i => i!).ToList();
    }

    /// <summary>
    /// The unit phrase every surface that shows the file-growth rise threshold uses (#3539 A8c). Both Settings
    /// windows, the alert body's threshold line and the card spell the knob's unit with THIS string (the MCP tool
    /// descriptions, which are prose, write it out as "megabytes per HOUR"), and a census test holds each of
    /// them to it — the knob meant "per lookback" from the day it shipped because nothing held the surfaces to
    /// one phrase.
    /// </summary>
    public const string FileGrowthRiseUnit = "MB/hr";

    /// <summary>
    /// The rise bar, in megabytes over the lookback window, for a threshold expressed in MB per HOUR (#3539 A8c).
    ///
    /// <para><b>What the knob means.</b> <see cref="IAlertEngineSettings.FileGrowthRiseMb"/> is a RATE — megabytes
    /// per hour — and <see cref="IAlertEngineSettings.FileGrowthLookbackMinutes"/> is the window that rate is
    /// averaged over. Before this, the stored number was compared against the raw growth inside the window, so
    /// the same 10,240 meant "10 GB in five minutes" on a store whose operator had shortened the lookback and
    /// "10 GB in a day" on one who had lengthened it: two knobs, one of which silently rescaled the other by up
    /// to 288×. Now 10,240 is 10 GB/hr everywhere; a 5-minute window asks for 853 MB inside it, a 24-hour
    /// window for 240 GB, and both are the same sustained rate.</para>
    ///
    /// <para><b>Why the bar is scaled to the CONFIGURED window rather than the rate read off the MEASURED one.</b>
    /// <see cref="DatabaseFileGrowthInfo.GrowthMbPerHour"/> divides by the width the samples actually span,
    /// which on a server that started collecting five minutes ago — or just came out of a collection gap — is
    /// five minutes: one 1 GB autogrowth in that span reads as 12 GB/hr, fires the default bar, and resolves at
    /// the next sample when the span widens. Holding the growth to <c>rate × configured window</c> instead counts
    /// unobserved time as no growth, the same conservative reading a single-sample window already gets ("no rise
    /// observed", not "the whole file appeared"). It also makes the change byte-identical for every store on the
    /// shipped 60-minute lookback: <c>rate × 60 / 60</c> is the number that was always compared.</para>
    /// </summary>
    public static double FileGrowthRiseBarMb(int riseMbPerHour, int lookbackMinutes) =>
        /* Product first, one division: the product of two ints is exact in a double for any value the knobs'
           clamps allow (the write bound is int.MaxValue on the rate, so it is widened before multiplying), and
           dividing once keeps "rate × 60 / 60" equal to the rate to the last bit on the shipped lookback. */
        (double)riseMbPerHour * Math.Max(1, lookbackMinutes) / 60.0;

    /// <summary>
    /// #2349: the files breaching either gate, worst first. Both gates are applied HERE rather than in the
    /// engine so the render path, the observation path and the decision can never disagree about which files
    /// are involved.
    ///
    /// <para>The rise gate compares the growth inside the window against <see cref="FileGrowthRiseBarMb"/> — the
    /// MB-per-hour threshold scaled to the window it is averaged over (#3539 A8c), so the same rate gives the
    /// same verdict whatever the lookback is set to.</para>
    ///
    /// <para>Ordered by how much of its volume the file occupies, because that is the one number that says how
    /// close this is to becoming a <c>Volume Free Space</c> page — a 40 GB rise on a 4 TB volume is less urgent
    /// than a 10 GB file that is now 80% of a small one.</para>
    /// </summary>
    public static List<DatabaseFileGrowthInfo> GetBreachedFiles(
        IReadOnlyList<DatabaseFileGrowthInfo>? files, int riseMbPerHour, int volumePercent, int lookbackMinutes) =>
        GetBreachedFiles(files, _ => (riseMbPerHour, volumePercent), lookbackMinutes);

    /// <summary>#5910: <see cref="GetBreachedFiles(IReadOnlyList{DatabaseFileGrowthInfo}, int, int, int)"/> with each
    /// file held to its own database's rise and level bars, which the engine resolves through
    /// <see cref="IAlertThresholdOverrides"/>. The lookback stays one per server: it shapes the read.</summary>
    public static List<DatabaseFileGrowthInfo> GetBreachedFiles(
        IReadOnlyList<DatabaseFileGrowthInfo>? files,
        Func<DatabaseFileGrowthInfo, (int RiseMbPerHour, int VolumePercent)> barsFor,
        int lookbackMinutes)
    {
        if (files is null || files.Count == 0) return new List<DatabaseFileGrowthInfo>();

        var breached = files
            .Where(f =>
            {
                var (riseMbPerHour, volumePercent) = barsFor(f);
                return BreachesRiseGate(f, riseMbPerHour, lookbackMinutes) || BreachesLevelGate(f, volumePercent);
            })
            .OrderByDescending(f => f.VolumePercent)
            .ThenByDescending(f => f.GrowthMb)
            .ToList();

        return breached;
    }

    /// <summary>The RISE gate on its own: grew at least <see cref="FileGrowthRiseBarMb"/> — the MB-per-hour
    /// threshold scaled to the configured window (#3539 A8c) — inside the lookback window. Zero disables it.
    /// Split out of <see cref="GetBreachedFiles"/> at #3636 because the engine's once-per-observation guard
    /// applies to THIS gate only, and it has to ask the same question the breach list asked rather than a
    /// re-typed copy of it.</summary>
    public static bool BreachesRiseGate(DatabaseFileGrowthInfo f, int riseMbPerHour, int lookbackMinutes) =>
        riseMbPerHour > 0 && f.GrowthMb >= FileGrowthRiseBarMb(riseMbPerHour, lookbackMinutes);

    /// <summary>The LEVEL gate on its own: the file is at least <paramref name="volumePercent"/> of its volume.
    /// Zero disables it; a file with no volume stats (Azure SQL DB) is never level-gated. A standing level that
    /// re-fires on the cooldown by design — the #3636 guard never consults it.</summary>
    public static bool BreachesLevelGate(DatabaseFileGrowthInfo f, int volumePercent) =>
        volumePercent > 0 && f.VolumeTotalMb > 0 && f.VolumePercent >= volumePercent;

    /// <summary>#2349: the alert card. Renders the top few by the same order <see cref="GetBreachedFiles"/>
    /// produced, and names the fields an operator needs to act without opening the Viewer — including
    /// <c>is_percent_growth</c>, which surfaces a percent-autogrowth misconfiguration for free.</summary>
    public static AlertContext? BuildFileGrowthContext(
        string serverName, List<DatabaseFileGrowthInfo> files,
        Func<IReadOnlyList<AlertIncident>, IReadOnlyList<AlertIncident>>? decorateIncidents = null)
    {
        if (files.Count == 0) return null;

        var context = new AlertContext();
        var shown = files.GetRange(0, Math.Min(5, files.Count));
        foreach (var f in shown)
        {
            var fields = new List<(string, string)>
            {
                /* #2109: the database as a discrete fact, not only in the heading. */
                ("Database", f.DatabaseName),
                ("File", f.FileName),
                ("Physical Name", f.PhysicalName),
                ("Size", $"{f.TotalSizeGb:F1} GB"),
                /* The rate here is over the MEASURED span (what the samples actually show); the threshold line
                   on the alert says what bar it was held to and over what window. Same unit phrase as the
                   threshold, so the two numbers read as comparable (#3539 A8c). */
                ("Growth", $"{f.GrowthGb:F1} GB in {f.GrowthWindowMinutes:F0} min ({f.GrowthMbPerHour:F0} {FileGrowthRiseUnit})"),
                ("Volume", string.IsNullOrEmpty(f.VolumeMountPoint) ? "(unknown)" : f.VolumeMountPoint),
                ("Volume Free", f.VolumeFreeMb is double freeMb ? $"{freeMb / 1024.0:F1} GB" : "n/a"),
                ("File % of Volume", f.VolumeTotalMb is double volTotal && volTotal > 0 ? $"{f.VolumePercent:F0}%" : "n/a"),
                /* A percent autogrowth on a large file is its own finding: each growth is bigger than the last,
                   which is exactly how a file gets away from someone. WS3 knows about the pattern and does not
                   alert on it. */
                ("Autogrowth", f.IsPercentGrowth
                    ? $"{f.GrowthPct:F0}% (percent growth)"
                    : f.AutoGrowthMb is double mb ? $"{mb:F0} MB" : "(unknown)"),
            };

            if (f.MaxSizeMb is double max)
            {
                fields.Add(("Max Size", max < 0 ? "Unlimited" : $"{max / 1024.0:F1} GB"));
            }

            context.Details.Add(new AlertDetailItem
            {
                Heading = $"{f.DatabaseName}.{f.FileName} — {f.TotalSizeGb:F1} GB ({(f.VolumeTotalMb is double headTotal && headTotal > 0 ? $"{f.VolumePercent:F0}% of {(string.IsNullOrEmpty(f.VolumeMountPoint) ? "(unknown)" : f.VolumeMountPoint)}" : "volume unknown")})",
                Fields = fields
            });
        }

        AlertIncidentRenderer.Apply(context, Decorate(FileGrowthIncidents(serverName, shown).ToList(), decorateIncidents));
        return context;
    }

    /// <summary>
    /// #5644 W1/W2: THE set of blocking events a blocking alert is made of, so the alert's value and its detail
    /// cannot disagree. Excluded databases drop their rows, the reports of one wait collapse to one row carrying
    /// the longest wait time (<see cref="BlockingWaits"/>), and the count is XE-preferred as Lite's overview has
    /// always been: the blocked-process-report waits when there are any, else the DMV-snapshot waits. "When there
    /// are any" is judged on the rows BEFORE the excluded-database filter, so XE reports that exist only in
    /// excluded databases yield an empty set rather than a fall back to DMV waits. The alert
    /// value is this set's <c>Count</c>; the card's "Blocking chain (xN)" groups are drawn from the same list, so
    /// the chain counts add up to the value.
    /// </summary>
    public static IReadOnlyList<BlockedProcessAlertRow> BlockingWaitRows(
        IReadOnlyList<BlockedProcessAlertRow>? events, IReadOnlyList<string> excludedDatabases)
    {
        if (events == null || events.Count == 0) return Array.Empty<BlockedProcessAlertRow>();

        /* #6645: a parallel query's self-wait report (the same non-zero spid on both sides) is not a blocking
           report, so a window whose XE reports are all self-waits has no XE reports and falls back to the DMV
           waits, as the SQL read surfaces do (BlockingWaits.NotSelfWaitSql). Spid 0 is a report that named no
           session and still counts. */
        var hasXeReports = events.Any(r => r.Source == BlockedProcessAlertRow.XeReportSource
            && !(r.BlockedSpid == r.BlockingSpid && r.BlockedSpid != 0));

        var waits = BlockingWaits.Collapse(
            FilterBlocking(events, excludedDatabases),
            r => r.Source ?? "",
            r => r.BlockedSpid,
            r => r.BlockingSpid,
            r => r.EventTime,
            r => r.WaitTimeMs);

        /* XE-or-DMV is decided on the UNFILTERED rows, as Lite's overview and the fleet tile do: if blocked-process
           reports exist at all, they are the source, and when every one sits in an excluded database the answer
           is empty. Deciding after the filter would fall back to the DMV snapshots there and fire an alert the
           excluded-database rule means to suppress. */
        return hasXeReports
            ? waits.Where(r => r.Source == BlockedProcessAlertRow.XeReportSource).ToList()
            : waits;
    }

    /* Excluded databases drop their rows; rows with no database always pass. Shared by the render path and
       #2216's observation path so the two can never disagree about which rows exist. */
    private static IReadOnlyList<BlockedProcessAlertRow> FilterBlocking(
        IReadOnlyList<BlockedProcessAlertRow> events, IReadOnlyList<string> excludedDatabases)
    {
        if (excludedDatabases is not { Count: > 0 })
        {
            return events;
        }

        return events
            .Where(e => string.IsNullOrEmpty(e.DatabaseName) ||
                !excludedDatabases.Any(ex =>
                    string.Equals(ex, e.DatabaseName, StringComparison.OrdinalIgnoreCase)))
            .ToList();
    }

    /* A deadlock whose processes ALL ran in excluded databases is dropped. Shared, as above. */
    private static IReadOnlyList<DeadlockAlertRow> FilterDeadlocks(
        IReadOnlyList<DeadlockAlertRow> deadlocks, IReadOnlyList<string> excludedDatabases)
    {
        if (excludedDatabases is not { Count: > 0 })
        {
            return deadlocks;
        }

        return deadlocks.Where(d => !IsDeadlockExcluded(d, excludedDatabases)).ToList();
    }

    /* #1140/#1141: collapse samples of the same chain into one group (true occurrence count + wait range)
       instead of listing it once per sample, and attach the dedup fingerprint. Identity is the resolved
       contentious object (collected server-side, §5.3), falling back to database + literal-stripped query
       pair only when the object did not resolve. */
    private static List<BlockingIncidentGrouper.BlockingGroup> GroupBlocking(
        string serverName, IReadOnlyList<BlockedProcessAlertRow> filtered) =>
        BlockingIncidentGrouper.Group(
            serverName,
            filtered.Select(e => new BlockingIncidentGrouper.BlockedEvent(
                e.DatabaseName, e.ContentiousObject, e.BlockedSqlText, e.BlockingSqlText, e.WaitTimeMs, e.LockMode,
                /* #3330: the row's own report travels with it, so the group can attach the one belonging to
                   its own fingerprint. HasReportXml is false for every DMV-snapshot row, which has no
                   report — null here, and an incident grouped only from those gets no attachment. */
                e.HasReportXml
                    ? new AlertIncidentAttachment(
                        e.BlockedProcessReportXml, AlertIncidentAttachment.BlockedProcessReportFileName)
                    : null)),
            /* #5320: the card's statement cut judges the whole statement first. Identity reads the raw text,
               so a withheld statement does not change which incident a sample belongs to. */
            statement => TruncateStatement(statement));

    /* The graph parse, shared by the render path and #2216's observation path. The fingerprint's object
       set, the #2109 Database fact and #3442's per-party facts all come off the same pass, so parsing once
       per deadlock is the point. */
    private static List<(DeadlockAlertRow Row, IReadOnlyList<string> Objects, IReadOnlyList<string> Databases,
        IReadOnlyList<(string Label, string Value)> Parties)>
        ParseDeadlocks(IReadOnlyList<DeadlockAlertRow> filtered) =>
        filtered
            .Select(d => (Row: d,
                Objects: DeadlockObjectExtractor.FromGraphXml(d.DeadlockGraphXml),
                Databases: DeadlockObjectExtractor.DatabasesFromGraphXml(d.DeadlockGraphXml),
                Parties: d.PartyFacts))
            .ToList();

    /* #1140: fingerprint each deadlock by its sorted involved-object set, across ALL deadlocks in the window,
       grouped so recurrences over the same objects collapse to one incident with a count. */
    private static List<DeadlockIncidentGrouper.DeadlockGroup> GroupParsedDeadlocks(
        string serverName,
        List<(DeadlockAlertRow Row, IReadOnlyList<string> Objects, IReadOnlyList<string> Databases,
            IReadOnlyList<(string Label, string Value)> Parties)> parsed) =>
        DeadlockIncidentGrouper.Group(
            serverName,
            parsed.Select(p => new DeadlockIncidentGrouper.DeadlockEvent(
                p.Objects,
                DeadlockDetailFields(p.Databases, p.Row.VictimSqlText, p.Row.ProcessSummary, p.Parties),
                /* #3330: the deadlock's own graph travels with it, so each incident attaches the graph for
                   the deadlock its card actually describes. */
                p.Row.HasDeadlockXml
                    ? new AlertIncidentAttachment(
                        p.Row.DeadlockGraphXml, AlertIncidentAttachment.DeadlockGraphFileName)
                    : null)));

    private static List<DeadlockIncidentGrouper.DeadlockGroup> GroupDeadlocks(
        string serverName, IReadOnlyList<DeadlockAlertRow> filtered) =>
        GroupParsedDeadlocks(serverName, ParseDeadlocks(filtered));

    /* #2216: runs the caller's incident decorator, with the no-decorator and no-incident cases short-
       circuited. A decorator that returned a different NUMBER of incidents would silently change what the
       alert renders — dropped incidents, or a "+N more" trailer that no longer matches the items below it —
       so a mismatched result is discarded in favour of the originals. The accumulator's contract is
       same-order-same-count; this makes a breach of it inert rather than invisible. */
    private static IReadOnlyList<AlertIncident> Decorate(
        List<AlertIncident> incidents,
        Func<IReadOnlyList<AlertIncident>, IReadOnlyList<AlertIncident>>? decorate)
    {
        if (decorate is null || incidents.Count == 0)
        {
            return incidents;
        }

        var decorated = decorate(incidents);
        return decorated is not null && decorated.Count == incidents.Count ? decorated : incidents;
    }

    /* #1141/#2109: forensic detail carried on a deadlock incident — the representative event's
       databases, victim SQL, process summary, and #3442's per-party facts. Since #2108 these render on
       the incident's own summary item too, not just per-event cards.

       The parties are appended AFTER the three existing facts rather than interleaved with them. Every
       consumer that re-reads this body by label takes the FIRST line matching a prefix it knows —
       AlertMuteContext.PopulateFromDetailText's Database / Victim SQL pre-fill most directly — so a new
       fact ahead of one of those would change which value a consumer resolves without changing any
       consumer. Behind them, it cannot. */
    private static List<AlertIncidentField>? DeadlockDetailFields(
        IReadOnlyList<string> databases, string? victimSql, string? processes,
        IReadOnlyList<(string Label, string Value)> parties)
    {
        var f = new List<AlertIncidentField>();
        if (databases.Count > 0) f.Add(new AlertIncidentField("Database", string.Join(", ", databases)));
        if (!string.IsNullOrWhiteSpace(victimSql)) f.Add(new AlertIncidentField("Victim SQL", TruncateStatement(victimSql!)));
        if (!string.IsNullOrWhiteSpace(processes)) f.Add(new AlertIncidentField("Processes", processes!));
        foreach (var (label, value) in parties) f.Add(new AlertIncidentField(label, value));
        return f.Count > 0 ? f : null;
    }

    /// <summary>
    /// True when EVERY process in the deadlock graph ran in an excluded database (case-insensitive
    /// on the graph's <c>currentdbname</c>) — a deadlock touching any non-excluded database still
    /// alerts. Unparseable or database-less graphs are never excluded. Public because the alert
    /// loop's count filter uses it too, not just <see cref="BuildDeadlockContext"/>.
    /// </summary>
    public static bool IsDeadlockExcluded(DeadlockAlertRow row, IReadOnlyList<string> excludedDatabases)
    {
        return PerformanceMonitor.Common.DeadlockGraphDatabases.AllIn(row.DeadlockGraphXml, excludedDatabases);
    }
    public static AlertContext? BuildPoisonWaitContext(List<PoisonWaitDelta> triggeredWaits)
    {
        if (triggeredWaits.Count == 0) return null;

        var context = new AlertContext();
        foreach (var w in triggeredWaits)
        {
            context.Details.Add(new AlertDetailItem
            {
                Heading = w.WaitType,
                Fields = new()
                {
                    ("Avg ms/wait", $"{w.AvgMsPerWait:F1}"),
                    ("Delta wait ms", $"{w.DeltaMs:N0}"),
                    ("Delta tasks", $"{w.DeltaTasks:N0}")
                }
            });
        }
        return context;
    }

    /// <summary>Requests shown on a Runnable Queue card (#5641): the same render budget as the Long-Running Query
    /// card, so one card never lists more sessions than the other.</summary>
    public const int RunnableQueueRequestDisplayCap = LongRunningQueryDisplayCap;

    /// <summary>How far back the CPU samples on the engine's snapshot reach, in minutes: Lite's
    /// <c>LocalDataService.AlertCpuSampleLookback</c> and Darling's <c>DarlingWorker.RecentCpuLookback</c>, both 15. The
    /// Runnable Queue card's CPU window never claims more (a test pins the three to each other).</summary>
    public const int RunnableQueueCpuSampleLookbackMinutes = 15;

    /// <summary>
    /// The Runnable Queue card's CPU item (#5641): CPU percent for the same minutes the queue average covers.
    /// A queue of runnable tasks is a CPU symptom, so the first question on the page is whether the cores are
    /// actually pinned. Built from the CPU samples the engine already holds on its snapshot (no read of its own):
    /// the samples at or after <c>nowUtc - windowMinutes</c> and not after <c>nowUtc</c>, reported as an average and
    /// a peak with the sample count. When the window holds no sample the item falls back to the snapshot's single
    /// newest reading and says so in its heading, because a number from another minute must not read as this
    /// window's. Returns null when there is no CPU reading at all (the card then simply has no CPU item).
    /// <para>Round 2 of #5709: the window is clamped to <see cref="RunnableQueueCpuSampleLookbackMinutes"/> (15), the
    /// lookback both stores read the CPU samples with, because the queue's span reaches 20 minutes at the Low-Impact
    /// preset and up to 60 at a slower cadence while the samples never reach back that far. The heading names the
    /// window the samples actually cover, so a longer queue span cannot claim a longer CPU window than the data
    /// holds.</para>
    /// </summary>
    public static AlertDetailItem? BuildRunnableQueueCpuItem(
        IReadOnlyList<AlertCpuSample>? recentSamples,
        double? latestSqlCpuPercent,
        double? latestTotalCpuPercent,
        DateTime nowUtc,
        int windowMinutes)
    {
        windowMinutes = Math.Min(windowMinutes, RunnableQueueCpuSampleLookbackMinutes);
        var floor = nowUtc - TimeSpan.FromMinutes(windowMinutes);
        var inWindow = (recentSamples ?? Array.Empty<AlertCpuSample>())
            .Where(s => s.SampleTimeUtc >= floor && s.SampleTimeUtc <= nowUtc)
            .ToList();

        var sql = inWindow.Where(s => s.SqlCpuPercent.HasValue).Select(s => s.SqlCpuPercent!.Value).ToList();
        var total = inWindow.Where(s => s.TotalCpuPercent.HasValue).Select(s => s.TotalCpuPercent!.Value).ToList();

        if (sql.Count > 0 || total.Count > 0)
        {
            var fields = new List<(string Label, string Value)>();
            if (sql.Count > 0)
            {
                fields.Add(("SQL Server CPU", string.Create(CultureInfo.InvariantCulture,
                    $"{sql.Average():F0}% average, {sql.Max():F0}% peak")));
            }
            if (total.Count > 0)
            {
                fields.Add(("Total CPU", string.Create(CultureInfo.InvariantCulture,
                    $"{total.Average():F0}% average, {total.Max():F0}% peak")));
            }
            fields.Add(("CPU samples", string.Create(CultureInfo.InvariantCulture, $"{inWindow.Count}")));
            return new AlertDetailItem
            {
                Heading = string.Create(CultureInfo.InvariantCulture, $"CPU over the last {windowMinutes} minutes"),
                Fields = fields
            };
        }

        if (latestSqlCpuPercent.HasValue || latestTotalCpuPercent.HasValue)
        {
            var fields = new List<(string Label, string Value)>();
            if (latestSqlCpuPercent.HasValue)
            {
                fields.Add(("SQL Server CPU", string.Create(CultureInfo.InvariantCulture, $"{latestSqlCpuPercent.Value:F0}%")));
            }
            if (latestTotalCpuPercent.HasValue)
            {
                fields.Add(("Total CPU", string.Create(CultureInfo.InvariantCulture, $"{latestTotalCpuPercent.Value:F0}%")));
            }
            return new AlertDetailItem { Heading = "CPU (newest sample, none in the window)", Fields = fields };
        }

        return null;
    }

    /// <summary>
    /// The Client Timeouts card's detail items (#5640): the reading (rate, exact count, minutes covered, and the estimate line only
    /// when the mute proportion changed the count), then the top statements, databases and client applications. Those three lists
    /// cover the samples inside the finding's covered span (round 2 of #5714, L6); when the span kept no samples they are the
    /// lookback's, the headings say "in the last N minutes", and the database and application lines carry the label "Recent ..." so
    /// the mute dialogs pre-fill nothing from an hour-old list.
    /// </summary>
    public static IReadOnlyList<AlertDetailItem> BuildClientTimeoutItems(
        AttentionEvaluator.Finding finding, AttentionWindow window)
    {
        var items = new List<AlertDetailItem>();

        /* 1. The reading: the exact rate and count, the span, and (only when the mute proportion changed the count) the estimate line. */
        var reading = new AlertDetailItem { Heading = "Client timeouts", Fields = new() };
        reading.Fields.Add(("Rate", string.Create(CultureInfo.InvariantCulture, $"{finding.TotalCount / finding.CoveredMinutes:N1} per minute")));
        reading.Fields.Add(("Count", string.Create(CultureInfo.InvariantCulture, $"{finding.TotalCount:N0} exact, from the session's event counter")));
        reading.Fields.Add(("Minutes Covered", string.Create(CultureInfo.InvariantCulture, $"{finding.CoveredMinutes:N0} ({finding.RunCount} collector run{(finding.RunCount == 1 ? "" : "s")})")));
        if (finding.Estimated)
        {
            reading.Fields.Add(("Estimate", string.Create(CultureInfo.InvariantCulture,
                $"estimated from {finding.SampledEvents:N0} sampled events: {finding.JudgedCount / finding.CoveredMinutes:N1} per minute is not muted")));
        }

        if (finding.ExcludedSamples > 0)
        {
            reading.Fields.Add(("Excluded", string.Create(CultureInfo.InvariantCulture,
                $"{finding.ExcludedSamples:N0} of {finding.SampledEvents:N0} sampled events came from SSMS IntelliSense and VS Code mssql metadata connections cancelling their own metadata queries; they are left out of the rate judged")));
        }

        if (finding.Bars is not null)
        {
            reading.Fields.Add(("Bar", AttentionEvaluator.BarSentence(finding)));
        }

        items.Add(reading);

        /* 2. The top statements (the statement filter judges each sample text before it is cut), the sampled events a statement
           appears in, not the count of timeouts. */
        var lookback = window.TopsFromLookback;
        var lookbackNote = string.Create(CultureInfo.InvariantCulture, $" in the last {AttentionEvaluator.ReadLookbackMinutes} minutes");
        var rank = 0;
        foreach (var statement in window.TopStatements.Take(AttentionTopListCap))
        {
            rank++;
            var item = new AlertDetailItem
            {
                Heading = string.Create(CultureInfo.InvariantCulture, $"Top statement {rank}: {statement.Samples:N0} sampled event{(statement.Samples == 1 ? "" : "s")}{(lookback ? lookbackNote : "")}"),
                Fields = new() { ("Fingerprint", statement.Name) },
            };
            if (!string.IsNullOrEmpty(statement.SampleText))
            {
                item.Fields.Add(("Statement", TruncateStatement(statement.SampleText)));
            }

            items.Add(item);
        }

        /* 3. Databases and client applications, the top first. The FIRST "Database" and "Client Application" lines are what the
           mute dialogs pre-fill (AlertMuteContext.PopulateFromDetailText), so those two labels are used for the top entry only,
           and only for a list that covers the span: a lookback list wears "Recent ..." labels, which the pre-fill ignores. */
        AddTopList(items, lookback ? $"Top databases (sampled events{lookbackNote})" : "Top databases (sampled events)",
            lookback ? "Recent Database" : "Database", window.TopDatabases);
        AddTopList(items, lookback ? $"Top client applications (sampled events{lookbackNote})" : "Top client applications (sampled events)",
            lookback ? "Recent Client Application" : "Client Application", window.TopApplications);

        /* 4. #6553: the client hosts, so the card says WHO was timing out without a second read. Every line wears the same label,
           "Client Host": the dataDetail masker classes a field by its label, and a numbered label ("Client Host 2") is not a host
           to it. The mute dialogs read no host, so this list pre-fills nothing. */
        AddTopHostList(items, lookback ? $"Top client hosts (sampled events{lookbackNote})" : "Top client hosts (sampled events)", window.Hosts);
        return items;
    }

    /// <summary>How many statements, databases and applications the Client Timeouts card lists.</summary>
    public const int AttentionTopListCap = 5;

    private static void AddTopHostList(List<AlertDetailItem> items, string heading, IReadOnlyList<AttentionTopItem> hosts)
    {
        if (hosts.Count == 0)
        {
            return;
        }

        var item = new AlertDetailItem { Heading = heading, Fields = new() };
        for (var i = 0; i < Math.Min(hosts.Count, AttentionTopListCap); i++)
        {
            item.Fields.Add(("Client Host", hosts[i].Name));
            item.Fields.Add(("Sampled Events", hosts[i].Samples.ToString("N0", CultureInfo.InvariantCulture)));
        }

        items.Add(item);
    }

    private static void AddTopList(List<AlertDetailItem> items, string heading, string label, IReadOnlyList<AttentionTopItem> top)
    {
        if (top.Count == 0)
        {
            return;
        }

        var item = new AlertDetailItem { Heading = heading, Fields = new() };
        for (var i = 0; i < Math.Min(top.Count, AttentionTopListCap); i++)
        {
            item.Fields.Add((i == 0 ? label : $"{label} {i + 1}", top[i].Name));
            item.Fields.Add(("Sampled Events", top[i].Samples.ToString("N0", CultureInfo.InvariantCulture)));
        }

        items.Add(item);
    }

    /// <summary>
    /// The Runnable Queue card's top-request items (#5641): the heaviest requests in the newest query snapshot,
    /// ranked by CPU time (then elapsed time) because the alert is about CPU queueing. Takes the rows an
    /// unfiltered, threshold-zero <c>GetLongRunningQueriesAsync</c> read returned, which is exactly the surface the
    /// High CPU card's maintenance probe reads; shows at most <see cref="RunnableQueueRequestDisplayCap"/> and
    /// states the omission as a final item rather than cutting silently. Fields are the Long-Running Query
    /// card's session fields, so a reader sees the same facts under both alerts. Returns an empty list when the
    /// snapshot holds no request. <paramref name="readHitCap"/> says the read returned a full page, so the count of
    /// omitted requests is a floor and the trailer reads "at least N more requests" ("1 more request" for one).
    /// </summary>
    public static IReadOnlyList<AlertDetailItem> BuildRunnableQueueRequestItems(IReadOnlyList<LongRunningQueryInfo> sessions, bool readHitCap = false)
    {
        if (sessions.Count == 0) return Array.Empty<AlertDetailItem>();

        var ranked = sessions
            .OrderByDescending(q => q.CpuTimeMs)
            .ThenByDescending(q => q.ElapsedSeconds)
            .ThenBy(q => q.SessionId)
            .ToList();
        var items = new List<AlertDetailItem>();
        foreach (var q in ranked.Take(RunnableQueueRequestDisplayCap))
        {
            var item = new AlertDetailItem
            {
                Heading = string.Create(CultureInfo.InvariantCulture,
                    $"Top request: session #{q.SessionId} - {q.CpuTimeMs:N0} ms CPU"),
                Fields = new()
            };
            if (!string.IsNullOrEmpty(q.DatabaseName))
                item.Fields.Add(("Database", q.DatabaseName));
            if (!string.IsNullOrEmpty(q.ProgramName))
                item.Fields.Add(("Program", q.ProgramName));
            if (!string.IsNullOrEmpty(q.QueryText))
                item.Fields.Add(("Query", TruncateStatement(q.QueryText)));
            item.Fields.Add(("Elapsed", FormatDuration(q.ElapsedSeconds)));
            item.Fields.Add(("CPU Time", $"{q.CpuTimeMs:N0} ms"));
            item.Fields.Add(("Reads", $"{q.Reads:N0}"));
            item.Fields.Add(("Writes", $"{q.Writes:N0}"));
            if (!string.IsNullOrEmpty(q.WaitType))
                item.Fields.Add(("Wait Type", q.WaitType));
            if (q.BlockingSessionId.HasValue && q.BlockingSessionId.Value > 0)
                item.Fields.Add(("Blocked By", $"Session #{q.BlockingSessionId.Value}"));
            items.Add(item);
        }

        if (ranked.Count > RunnableQueueRequestDisplayCap)
        {
            items.Add(new AlertDetailItem
            {
                Heading = "More requests",
                Fields = new()
                {
                    ("Not shown", string.Create(CultureInfo.InvariantCulture,
                        $"{(readHitCap ? "at least " : "")}{ranked.Count - RunnableQueueRequestDisplayCap} more {(ranked.Count - RunnableQueueRequestDisplayCap == 1 ? "request" : "requests")} in the newest query snapshot"))
                }
            });
        }
        return items;
    }

    /// <summary>
    /// The Long-Running Query card's render budget: sessions shown per card. A display cap, NOT an
    /// observation cap (#2362 keeps the fingerprint observation list uncapped) — named because two
    /// call sites have to agree on it: <see cref="BuildLongRunningQueryContext"/> renders this many,
    /// and the engine resolves Agent-job names (#3497) for exactly the same subset, so a name is never
    /// fetched for a session the card will not show.
    /// </summary>
    public const int LongRunningQueryDisplayCap = 3;

    /// <summary>
    /// <paramref name="agentJobNames"/> is #3497's annotation input — <b>annotation, never
    /// suppression</b>: the same sessions render, the same incidents are fingerprinted, every card
    /// still fires; a session whose <c>program_name</c> carries the SQLAgent job-step form merely
    /// gains one field naming the job, because "that is the maintenance job, running as scheduled,
    /// merely long" should not have to be reconstructed from the statement shape and the hour, twelve
    /// times a night. NULL (the default, and every pre-#3497 caller) renders the card byte-identically
    /// to before — no resolution was attempted, so nothing is claimed. NON-null says a resolution ran:
    /// a parsed Agent session whose key the map lacks (msdb denied, lookup failed, job deleted) renders
    /// the UNRESOLVED form, still stating the fact the parse alone establishes and carrying the raw
    /// job-id marker so the operator can match it against the Program field's hex by eye. The
    /// annotation states what IS, never a verdict.
    /// </summary>
    public static AlertContext? BuildLongRunningQueryContext(
        string serverName, List<LongRunningQueryInfo> queries,
        Func<IReadOnlyList<AlertIncident>, IReadOnlyList<AlertIncident>>? decorateIncidents = null,
        IReadOnlyDictionary<AgentJobStepKey, AgentJobStepNames>? agentJobNames = null)
    {
        if (queries.Count == 0) return null;

        var context = new AlertContext();
        var shown = queries.GetRange(0, Math.Min(LongRunningQueryDisplayCap, queries.Count));
        foreach (var q in shown)
        {
            var item = new AlertDetailItem
            {
                Heading = $"Session #{q.SessionId} — {q.ElapsedSeconds / 60}m {q.ElapsedSeconds % 60}s",
                Fields = new()
            };

            if (!string.IsNullOrEmpty(q.DatabaseName))
                item.Fields.Add(("Database", q.DatabaseName));
            if (!string.IsNullOrEmpty(q.ProgramName))
                item.Fields.Add(("Program", q.ProgramName));
            /* #3497: directly under Program, so the raw form and the resolved name read as one fact.
               Fields never enter AlertFingerprint.ForKey — the dedup key hashes (server, type,
               query_hash) only — so the annotation is fingerprint-inert by construction: a card that
               re-fires with a different elapsed or a freshly resolved name folds into the same
               incident it always did. Pinned in the owning suites rather than merely stated. */
            if (agentJobNames is not null && AgentJobStepQuery.TryParseProgramName(q.ProgramName, out var jobKey))
            {
                item.Fields.Add(("Running under Agent job",
                    agentJobNames.TryGetValue(jobKey, out var jobNames)
                        ? jobNames.StepName is { Length: > 0 }
                            ? $"{jobNames.JobName}, step {jobKey.StepId} ({jobNames.StepName})"
                            : $"{jobNames.JobName}, step {jobKey.StepId}"
                        : $"(name unresolved) Job 0x{AgentJobStepQuery.ToProgramNameHex(jobKey.JobId)}, step {jobKey.StepId}"));
            }
            if (!string.IsNullOrEmpty(q.QueryText))
                item.Fields.Add(("Query", TruncateStatement(q.QueryText)));
            item.Fields.Add(("CPU Time", $"{q.CpuTimeMs:N0} ms"));
            item.Fields.Add(("Reads", $"{q.Reads:N0}"));
            item.Fields.Add(("Writes", $"{q.Writes:N0}"));
            if (!string.IsNullOrEmpty(q.WaitType))
                item.Fields.Add(("Wait Type", q.WaitType));
            if (q.BlockingSessionId.HasValue && q.BlockingSessionId.Value > 0)
                item.Fields.Add(("Blocked By", $"Session #{q.BlockingSessionId.Value}"));

            context.Details.Add(item);
        }

        /* #1140: dedup key = query_hash (stable across literals/plans). Null hash -> no incident. */
        AlertIncidentRenderer.Apply(context, Decorate(LongRunningQueryIncidents(serverName, shown).ToList(), decorateIncidents));
        return context;
    }

    /// <summary>The <c>Excluded Count</c> label on the Long-Running Query card's knob item (#3653 A5, Q5) — the
    /// one field a reader of the card or of <c>get_alert_history</c>'s <c>context_json</c> looks up to see the
    /// opt-out knob working. A constant so the engine, the tests and any reader spell it once.</summary>
    public const string LongRunningQueryExcludedCountLabel = "Excluded Count";

    /// <summary>The knob item's per-arm split (#3653 A5, Q5 addendum): sessions the <c>program_name</c> PREFIX arm
    /// removed. A session matching both arms is counted here and not under the login arm, so the two labels sum
    /// to <see cref="LongRunningQueryExcludedCountLabel"/>.</summary>
    public const string LongRunningQueryExcludedByProgramPrefixLabel = "Excluded By Program Prefix";

    /// <summary>The knob item's per-arm split: sessions the exact <c>login_name</c> arm removed and the program
    /// arm did not.</summary>
    public const string LongRunningQueryExcludedByLoginLabel = "Excluded By Login";

    /// <summary>
    /// The Long-Running Query card's OPT-OUT KNOB item (#3653 A5, ruling Q5): how many over-threshold sessions
    /// the <see cref="LongRunningQueryExclusions"/> knob removed from this evaluation, split by the arm that
    /// removed them, and the entries that did it. Appended by the engine after the session items, and only when
    /// the knob is set — it is set by default (the seeded job-step prefix and the two NT AUTHORITY logins), so
    /// the item is absent only for an operator who cleared both lists, to whom "Excluded Count: 0" on every
    /// card would be a line about nothing.
    ///
    /// <para>Why the count is on the card at all: the knob's only effect is a page NOT arriving, and a setting
    /// whose effect is an absence is one an operator cannot verify from the outside. The count is the knob's
    /// receipt — "I removed 4 sessions before deciding this" — which is also how an entry that is too broad
    /// shows itself (a count that equals the whole snapshot). The SPLIT says which default did the work: a
    /// fleet whose login arm removes seventy sessions and whose prefix arm removes none has learned something
    /// about its background. The counts are SESSIONS, not snapshot rows, and a session matching both arms is
    /// counted once, under the program prefix. The entries are listed so the card is self-describing to
    /// whoever reads it in six months without the Settings window open.</para>
    /// </summary>
    /// <param name="exclusions">The knob as the engine applied it (normalised).</param>
    /// <param name="excludedByProgramPrefix">The read's count of sessions the program-prefix arm removed (including any that also matched a login).</param>
    /// <param name="excludedByLogin">The read's count of sessions the login arm removed and the program arm did not.</param>
    public static AlertDetailItem BuildLongRunningQueryExclusionItem(LongRunningQueryExclusions exclusions, int excludedByProgramPrefix, int excludedByLogin)
    {
        if (exclusions is null) throw new ArgumentNullException(nameof(exclusions));

        var excludedCount = excludedByProgramPrefix + excludedByLogin;
        var fields = new List<(string Label, string Value)>
        {
            (LongRunningQueryExcludedCountLabel, excludedCount.ToString(CultureInfo.InvariantCulture)),
            (LongRunningQueryExcludedByProgramPrefixLabel, excludedByProgramPrefix.ToString(CultureInfo.InvariantCulture)),
            (LongRunningQueryExcludedByLoginLabel, excludedByLogin.ToString(CultureInfo.InvariantCulture))
        };
        if (exclusions.ProgramNamePrefixes.Count > 0)
        {
            fields.Add(("Excluded Program Prefixes", string.Join(", ", exclusions.ProgramNamePrefixes)));
        }

        if (exclusions.Logins.Count > 0)
        {
            fields.Add(("Excluded Logins", string.Join(", ", exclusions.Logins)));
        }

        return new AlertDetailItem
        {
            Heading = excludedCount == 1
                ? "1 session over the threshold was excluded by the opt-out knob"
                : $"{excludedCount} sessions over the threshold were excluded by the opt-out knob",
            Fields = fields
        };
    }

    /// <summary>The <c>Excluded By Database</c> label on the Long-Running Query card's excluded-databases item
    /// (#3742) — the one field a reader of the card or of <c>get_alert_history</c>'s <c>context_json</c> looks up
    /// to see how many over-threshold sessions the shared <c>excludedDatabases</c> list removed ahead of the page.
    /// A constant so the engine, the tests and any reader spell it once, beside the knob's three.</summary>
    public const string LongRunningQueryExcludedByDatabaseLabel = "Excluded By Database";

    /// <summary>
    /// The Long-Running Query card's EXCLUDED-DATABASES item (#3742): how many over-threshold sessions the shared
    /// <c>excludedDatabases</c> list removed from this evaluation ahead of the row cap, and the databases that
    /// did it. Appended by the engine after the sessions and after the knob's own item, and only when the list is
    /// SET — it is empty by default on both SKUs, so a fresh install's card is byte-identical to the pre-#3742
    /// one; an operator who has named a database gets the receipt on every card, including a "0", exactly as the
    /// knob's item reads 0/0/0 on a sweep where it removed nothing.
    ///
    /// <para>Why this is its own item and not three more fields on the knob's: the two settings are different
    /// instruments with different owners. The knob names programs and principals, ships seeded, and is the
    /// Long-Running Query alert's alone; <c>excludedDatabases</c> is the blunt, empty-by-default list the
    /// blocking and deadlock arms also honour. The knob item's heading says "excluded by the opt-out knob" and
    /// its <c>Excluded Count</c> has always been the knob's two arms; folding a database count into it would
    /// make that heading false and that count ambiguous. The count here is SESSIONS the database list removed
    /// that neither knob arm had already removed (the read counts the database arm last), so this item's number
    /// and the knob item's sum to the sessions the page does not show.</para>
    ///
    /// <para>Why the count is on the card at all is #3742's whole finding: before it, the list was applied after
    /// the cap, so an excluded reporting database whose ETL held the five longest sessions consumed the page and
    /// the alert came back short or empty — and NOTHING on the card said so. Now the rows are removed ahead of
    /// the cap AND the card states how many, so a page of two with "Excluded By Database: 6" beside it is a
    /// page an operator can read. ANNOTATION, NEVER SUPPRESSION, like the knob's item: the fire is decided
    /// before this exists and it can only add a line.</para>
    /// </summary>
    /// <param name="excludedDatabases">The shared list as the engine holds it (the settings value, not re-normalised —
    /// the card lists what the operator typed).</param>
    /// <param name="excludedByDatabase">The read's count of sessions the database list removed and neither knob arm did.</param>
    public static AlertDetailItem BuildLongRunningQueryExcludedDatabasesItem(IReadOnlyList<string> excludedDatabases, int excludedByDatabase)
    {
        if (excludedDatabases is null) throw new ArgumentNullException(nameof(excludedDatabases));

        var fields = new List<(string Label, string Value)>
        {
            (LongRunningQueryExcludedByDatabaseLabel, excludedByDatabase.ToString(CultureInfo.InvariantCulture)),
            ("Excluded Databases", string.Join(", ", excludedDatabases))
        };

        return new AlertDetailItem
        {
            Heading = excludedByDatabase == 1
                ? "1 session over the threshold was in an excluded database"
                : $"{excludedByDatabase} sessions over the threshold were in excluded databases",
            Fields = fields
        };
    }

    /* ---------------- High CPU: the active-maintenance annotation (#3495) ---------------- */

    /// <summary>
    /// How many maintenance sessions the High CPU card names before stating an omission instead of
    /// growing without bound — the same budget the Long-Running Query card gives its own sessions
    /// (<see cref="BuildLongRunningQueryContext"/> shows three), because both caps answer the same
    /// question: how many lines help a reader before they stop reading. Never a silent cut: sessions
    /// past the cap are counted on a stated-omission line, the #3494 discipline.
    /// </summary>
    public const int ActiveMaintenanceMaxLines = 3;

    /// <summary>
    /// The statement heads that read as system maintenance on a CPU card (#3495's BACKUP DATABASE /
    /// RESTORE, plus ALTER INDEX — the issue's "plausibly, same shape": an online rebuild burns the SQL
    /// CPU share exactly the way backup compression does). Matched against the TRIMMED head of the
    /// captured statement text, case-insensitively, and deliberately NOT as a contains-anywhere search:
    /// a head match can miss a maintenance statement buried mid-batch, which costs the card its
    /// annotation and nothing else, while a contains match can NAME maintenance on a card where none
    /// runs (someone's dynamic-SQL builder mentioning BACKUP DATABASE in a literal) — and an annotation
    /// whose whole value is being trustworthy must fail toward silence.
    /// </summary>
    public static readonly IReadOnlyList<string> MaintenanceStatementHeads = new[]
    {
        "BACKUP DATABASE",
        "BACKUP LOG",
        "RESTORE DATABASE",
        "RESTORE LOG",
        "ALTER INDEX"
    };

    /// <summary>
    /// The matched maintenance head for one session's statement text, normalized to the canonical
    /// uppercase spelling for the card line — or null when the session is not a maintenance shape.
    /// </summary>
    public static string? TryGetMaintenanceStatementHead(string? queryText)
    {
        if (string.IsNullOrWhiteSpace(queryText)) return null;

        var head = queryText.TrimStart();
        foreach (var candidate in MaintenanceStatementHeads)
        {
            if (head.StartsWith(candidate, StringComparison.OrdinalIgnoreCase))
                return candidate;
        }

        return null;
    }

    /// <summary>
    /// #3495's one-line form, appended to the High CPU card's detail text when the fire-time
    /// active-session snapshot holds system maintenance:
    /// <c>Active maintenance: BACKUP DATABASE (RdsAdminService), 17m 3s elapsed, ASYNC_IO_COMPLETION</c>
    /// — program name, elapsed, wait, straight off the session row. One line per concurrent maintenance
    /// session in the input's own order (the read returns elapsed DESC, so the longest-running — the
    /// likeliest pin — leads), capped at <see cref="ActiveMaintenanceMaxLines"/> with the omission
    /// stated.
    ///
    /// <para><b>Annotation, never suppression</b> — the #3495 contract, spelled here because this is
    /// the function that could most easily drift into judging: the tiers stay where they are, the page
    /// still fires, and the line states what IS (session kind, program, elapsed, wait) and never a
    /// verdict. A backup pinning CPU at 04:30 with the store idle is routine; the same pin at 14:30
    /// under checkout load is a capacity finding with a named cause — that judgment belongs to the
    /// reader, and suppressing or down-tiering CPU alerts during backups would blind exactly the case
    /// where the overlap matters.</para>
    ///
    /// <para>Returns "" when no maintenance session is present, which is the regression pin: the
    /// caller string-appends this, so an empty answer leaves the card BYTE-identical to today.</para>
    /// </summary>
    public static string BuildActiveMaintenanceDetail(IReadOnlyList<LongRunningQueryInfo> activeSessions)
    {
        if (activeSessions.Count == 0) return "";

        var sb = new StringBuilder();
        int named = 0;
        int omitted = 0;
        foreach (var session in activeSessions)
        {
            var head = TryGetMaintenanceStatementHead(session.QueryText);
            if (head is null) continue;

            if (named == ActiveMaintenanceMaxLines)
            {
                omitted++;
                continue;
            }

            named++;
            sb.Append("\n  Active maintenance: ").Append(head);
            if (!string.IsNullOrEmpty(session.ProgramName))
                sb.Append(" (").Append(session.ProgramName).Append(')');
            sb.Append(", ").Append(FormatDuration(session.ElapsedSeconds)).Append(" elapsed");
            if (!string.IsNullOrEmpty(session.WaitType))
                sb.Append(", ").Append(session.WaitType);
        }

        /* The #3494 discipline: whole lines that fit, then a stated omission — never a silent cut. */
        if (omitted > 0)
            sb.Append("\n  Active maintenance: ").Append(omitted).Append(omitted == 1 ? " more maintenance session not shown" : " more maintenance sessions not shown");

        return sb.ToString();
    }

    /* Returns the volumes whose free space is under the configured % or GB threshold (a 0 threshold
       disables that dimension), worst (lowest free %) first, so the alert names the tightest volume. */
    public static List<VolumeFreeSpaceInfo> GetBreachedVolumes(List<VolumeFreeSpaceInfo> volumes, double thresholdPercent, double thresholdGb)
    {
        double pct = thresholdPercent;
        double gb = thresholdGb;
        return volumes
            .Where(v => (pct > 0 && v.FreePercent < pct) || (gb > 0 && v.FreeGb < gb))
            .OrderBy(v => v.FreePercent)
            .ToList();
    }

    public static string FormatLowDiskThreshold(double thresholdPercent, double thresholdGb)
    {
        var parts = new List<string>();
        if (thresholdPercent > 0) parts.Add($"{thresholdPercent}%");
        if (thresholdGb > 0) parts.Add($"{thresholdGb} GB");
        return parts.Count > 0 ? string.Join(" / ", parts) : "—";
    }

    public static AlertContext? BuildVolumeFreeSpaceContext(
        string serverName, List<VolumeFreeSpaceInfo> volumes,
        Func<IReadOnlyList<AlertIncident>, IReadOnlyList<AlertIncident>>? decorateIncidents = null)
    {
        if (volumes.Count == 0) return null;

        var context = new AlertContext();
        var shown = volumes.GetRange(0, Math.Min(5, volumes.Count));
        foreach (var v in shown)
        {
            context.Details.Add(new AlertDetailItem
            {
                Heading = $"{v.MountPoint} — {v.FreePercent:F0}% Free",
                Fields = new()
                {
                    ("Free Space", $"{v.FreeGb:F1} GB"),
                    ("Total Size", $"{v.TotalMb / 1024.0:F1} GB"),
                    ("Used", $"{(v.TotalMb - v.FreeMb) / 1024.0:F1} GB")
                }
            });
        }

        /* #1140: dedup key per volume (the drive/mount point). */
        AlertIncidentRenderer.Apply(context, Decorate(VolumeFreeSpaceIncidents(serverName, shown).ToList(), decorateIncidents));
        return context;
    }

    /* Returns the ADR databases whose PVS breaches BOTH gates (#1984) — percent of database at/over
       the threshold AND size at/over the floor (a 0 floor removes that qualifier) — worst (highest
       PVS %) first, so the alert names the most-consumed database. AND, not the volume pair's OR:
       percent is the trigger and the floor only keeps small databases from paging anyone. A
       thresholdPercent of 0 disables the check at the caller. */
    public static List<PvsPressureInfo> GetBreachedPvsDatabases(List<PvsPressureInfo> databases, double thresholdPercent, double floorGb) =>
        GetBreachedPvsDatabases(databases, _ => (thresholdPercent, floorGb));

    /* #5910: the same gates with each database held to its own pair, which the engine resolves through
       IAlertThresholdOverrides. A database whose percent resolves to 0 never breaches. */
    public static List<PvsPressureInfo> GetBreachedPvsDatabases(
        List<PvsPressureInfo> databases, Func<PvsPressureInfo, (double ThresholdPercent, double FloorGb)> barsFor)
    {
        return databases
            .Where(d =>
            {
                var (thresholdPercent, floorGb) = barsFor(d);
                return thresholdPercent > 0
                    && d.PvsPercent >= thresholdPercent
                    && (floorGb <= 0 || d.PvsGb >= floorGb);
            })
            .OrderByDescending(d => d.PvsPercent)
            .ToList();
    }

    public static string FormatPvsThreshold(double thresholdPercent, double floorGb)
    {
        return floorGb > 0
            ? $"{thresholdPercent}% of database and ≥ {floorGb} GB"
            : $"{thresholdPercent}% of database";
    }

    public static AlertContext? BuildPvsPressureContext(
        string serverName, List<PvsPressureInfo> databases,
        Func<IReadOnlyList<AlertIncident>, IReadOnlyList<AlertIncident>>? decorateIncidents = null)
    {
        if (databases.Count == 0) return null;

        var context = new AlertContext();
        var shown = databases.GetRange(0, Math.Min(5, databases.Count));
        foreach (var d in shown)
        {
            var fields = new List<(string, string)>
            {
                /* #2109: the database as a discrete fact, not only in the heading — downstream
                   automation routes on the fact name, and headings are display prose. */
                ("Database", d.DatabaseName),
                ("PVS Size (off-row)", $"{d.PvsGb:F1} GB"),
                ("Database Data Size", $"{d.DatabaseDataSizeMb / 1024.0:F1} GB"),
                ("Aborted Transactions", d.CurrentAbortedTransactionCount.ToString()),
                /* MS's shape for "cleanup is ongoing": a cleaner start time with no end time. */
                ("Aborted Cleanup", d.AbortedCleanupOngoing ? "Ongoing" : "Idle")
            };
            /* The input to MS's "old aborted transaction is preventing cleanup" read — shown as the
               gap itself, never a verdict (the same reasoning as the FinOps grids). */
            if (d.AbortedTransactionLag is long lag)
            {
                fields.Add(("Aborted/Active Lag", lag.ToString()));
            }
            context.Details.Add(new AlertDetailItem
            {
                Heading = $"{d.DatabaseName} — PVS {d.PvsPercent:F0}% of database",
                Fields = fields
            });
        }

        AlertIncidentRenderer.Apply(context, Decorate(PvsPressureIncidents(serverName, shown).ToList(), decorateIncidents));
        return context;
    }

    public static AlertContext? BuildTempDbSpaceContext(TempDbSpaceInfo tempDb)
    {
        var context = new AlertContext();
        context.Details.Add(new AlertDetailItem
        {
            Heading = $"tempdb — {tempDb.ReservedPercent:F0}% Reserved",
            Fields = new()
            {
                ("Total Reserved", $"{tempDb.TotalReservedMb:F0} MB"),
                ("Unallocated", $"{tempDb.UnallocatedMb:F0} MB"),
                /* #2515: the percentage above is against the CEILING where there is one and against the
                   current allocation where there is not, and Total Reserved + Unallocated only ever shows
                   the allocation — so without this the reader cannot tell which denominator produced the
                   number they are being paged about. Three states, reported as three different words:
                   a cap, no cap at all, and a snapshot taken before the ceiling was collected. */
                ("Max Size", tempDb.MaxSizeMb switch
                {
                    > 0 => $"{tempDb.MaxSizeMb:F0} MB",
                    < 0 => "Unlimited",
                    _ => "Unknown"
                }),
                ("User Objects", $"{tempDb.UserObjectReservedMb:F0} MB"),
                ("Internal Objects", $"{tempDb.InternalObjectReservedMb:F0} MB"),
                ("Version Store", $"{tempDb.VersionStoreReservedMb:F0} MB"),
                ("Top Consumer", tempDb.TopConsumerSessionId > 0
                    ? $"Session #{tempDb.TopConsumerSessionId} ({tempDb.TopConsumerMb:F0} MB)"
                    : "None")
            }
        });
        return context;
    }

    /// <summary>The <c>Fired By</c> label on the Blocking Wait Time gate item (#3653 A5) — the one field a
    /// renderer or an MCP reader of <c>get_alert_history</c>'s <c>context_json</c> looks up to learn which arm
    /// admitted the delivery. A constant so the engine, the tests and any reader spell it once.</summary>
    public const string BlockingWaitFiredByLabel = "Fired By";

    /// <summary>
    /// The Blocking Wait Time fire's GATE item (#3653 A5, ruling Q4): the arm that admitted this delivery and
    /// the numbers it was judged on, prepended by the engine ahead of the blocked-process detail. The
    /// blocked-process rows are the count gate's evidence and may be absent altogether for a DMV-only
    /// episode; this item is the wait gate's own evidence and is present on every fire from that arm.
    ///
    /// <para><c>Fired By</c> carries the machine token (<c>AlertEngine.BlockingWaitFiredBySingleSnapshot</c>
    /// / <c>BlockingWaitFiredByConsecutive</c>) rather than prose, because its reader is as likely to be a
    /// tool as a person: a Slack card renders it as-is and still reads, and <c>get_alert_history</c> hands it
    /// through <c>context_json</c> untouched. The single-snapshot bar is stated beside the configured one so a
    /// reader can see WHY a one-snapshot page was admitted without knowing the multiplier — the same reason
    /// the tempdb item states its denominator.</para>
    /// </summary>
    /// <param name="current">The fresh snapshot the fire was judged on.</param>
    /// <param name="thresholdSeconds">The configured <c>BlockingWaitSecondsThreshold</c>.</param>
    /// <param name="firedBy">The arm token — see the engine's two <c>BlockingWaitFiredBy*</c> members.</param>
    public static AlertDetailItem BuildBlockingWaitGateItem(CurrentBlockingWaitResult current, int thresholdSeconds, string firedBy)
    {
        if (current is null) throw new ArgumentNullException(nameof(current));

        return new AlertDetailItem
        {
            Heading = $"Blocking Wait Time — {current.TotalWaitSeconds:F0}s across {current.BlockedSessionCount} blocked {(current.BlockedSessionCount == 1 ? "session" : "sessions")}",
            Fields = new()
            {
                (BlockingWaitFiredByLabel, firedBy),
                ("Total Blocked Wait", $"{current.TotalWaitSeconds:F0}s"),
                ("Threshold", $"{thresholdSeconds}s"),
                ("Single-Snapshot Bar", $"{thresholdSeconds * AlertEngine.BlockingWaitSingleSnapshotMultiplier}s ({AlertEngine.BlockingWaitSingleSnapshotMultiplier}× threshold; below it, {AlertEngine.BlockingWaitBreachSamples} consecutive collections)"),
                /* The snapshot's collection_time is stored naive-UTC (both adapters compare it against
                   DateTime.UtcNow for freshness), so it renders through the UTC marker — #3422's rule that
                   every timestamp an alert body carries declares its clock. */
                ("Snapshot", AlertTimestamp.Utc(current.SnapshotTime))
            }
        };
    }

    public static AlertContext? BuildAnomalousJobContext(
        string serverName, List<AnomalousJobInfo> jobs,
        Func<IReadOnlyList<AlertIncident>, IReadOnlyList<AlertIncident>>? decorateIncidents = null)
    {
        if (jobs.Count == 0) return null;

        var context = new AlertContext();
        var shown = jobs.GetRange(0, Math.Min(3, jobs.Count));
        foreach (var j in shown)
        {
            context.Details.Add(new AlertDetailItem
            {
                Heading = j.JobName,
                Fields = new()
                {
                    ("Current Duration", FormatDuration(j.CurrentDurationSeconds)),
                    ("Avg Duration", FormatDuration(j.AvgDurationSeconds)),
                    ("P95 Duration", FormatDuration(j.P95DurationSeconds)),
                    ("% of Average", j.PercentOfAverage.HasValue ? $"{j.PercentOfAverage:F0}%" : "N/A"),
                    /* The run start is the monitored server's own clock; the alert time beside it, and
                       "Incident Since" below it, are UTC. Rendered through AlertTimestamp so the value
                       states which. */
                    ("Started", AlertTimestamp.ForServerInstant(j.StartTime, j.UtcOffsetMinutes))
                }
            });
        }

        /* #1140: dedup key per job (job name, scoped to the instance via serverName). */
        AlertIncidentRenderer.Apply(context, Decorate(AnomalousJobIncidents(serverName, shown).ToList(), decorateIncidents));
        return context;
    }

    /// <summary>
    /// The failed-Agent-job context: one item per failure in the lookback window, capped at five.
    /// </summary>
    /// <param name="windowEndUtc">
    /// The instant the window was read, in UTC, and <paramref name="lookbackMinutes"/> its length — together
    /// the <see cref="FailureWindowHeading"/> item.
    /// <para><b>Why the body states its own window.</b> The window is longer than the interval between
    /// firings, so consecutive alerts overlap and a failure appears in every body whose window contains it.
    /// That is what a window report does, and it is NOT narrowed to "new since the last alert": the only
    /// discriminator available is the engine's failed-job watermark, which holds the newest RUN START it has
    /// reported — and a job whose run started before that instant and failed after it is new, not repeated,
    /// so filtering on it would drop a failure nobody has been told about. Losing a repeat costs a reader a
    /// second look; losing a first report costs them the page. So the repeat stays and the window is
    /// declared, which is what lets a reader tell "this failed again" from "this is the same failure in a
    /// later window". <c>sysjobhistory.instance_id</c> is the append cursor that would make a cursored body
    /// exact; the watermark is not it.</para>
    /// <para>Null omits the item, for a caller with no clock — the window is a fact about the read, not
    /// about the rows, so it is stated only when the reader actually supplied it.</para>
    /// </param>
    /// <param name="lookbackMinutes">The window's length in minutes, as configured.</param>
    public static AlertContext? BuildFailedJobContext(
        string serverName, List<FailedJobInfo> jobs,
        Func<IReadOnlyList<AlertIncident>, IReadOnlyList<AlertIncident>>? decorateIncidents = null,
        DateTime? windowEndUtc = null, int lookbackMinutes = 0)
    {
        if (jobs.Count == 0) return null;

        var context = new AlertContext();
        var shown = jobs.GetRange(0, Math.Min(5, jobs.Count));
        foreach (var j in shown)
        {
            var item = new AlertDetailItem { Heading = j.JobName, Fields = new() };
            item.Fields.Add(("Job", j.JobName));
            item.Fields.Add(("Failed At", j.RunDateTimeFormatted));
            if (j.StepId > 0 && !string.IsNullOrEmpty(j.StepName))
                item.Fields.Add(("Step", $"{j.StepId} — {j.StepName}"));
            if (!string.IsNullOrEmpty(j.Message))
                item.Fields.Add(("Message", TruncateText(j.Message, 300)));
            context.Details.Add(item);
        }

        if (windowEndUtc is DateTime endUtc && lookbackMinutes > 0)
        {
            context.Details.Add(new AlertDetailItem
            {
                Heading = FailureWindowHeading,
                Fields = new()
                {
                    (FailureWindowFromLabel, AlertTimestamp.Utc(endUtc.AddMinutes(-lookbackMinutes))),
                    (FailureWindowToLabel, AlertTimestamp.Utc(endUtc))
                }
            });
        }

        /* #1140: dedup key per job (job name, scoped to the instance via serverName) — mirrors
           BuildAnomalousJobContext so two distinct failed jobs are distinct incidents under the
           #1154 per-fingerprint cooldown instead of coalescing on the metric key. */
        AlertIncidentRenderer.Apply(context, Decorate(FailedJobIncidents(serverName, shown).ToList(), decorateIncidents));
        return context;
    }

    /// <summary>Heading of the failed-job body's window item. Declared, not spelled inline, because a
    /// heading is what a reader keys on and the tests assert it.</summary>
    public const string FailureWindowHeading = "Failure Window";

    /// <summary>Label of the window's start fact. A fact name is a consumer API — see
    /// <see cref="AlertIncidentRenderer"/>.</summary>
    public const string FailureWindowFromLabel = "From";

    /// <summary>Label of the window's end fact.</summary>
    public const string FailureWindowToLabel = "To";

    /// <summary>
    /// Flattens an <see cref="AlertContext"/> into the plain-text detail block persisted in alert
    /// history and rendered in plain-text notification bodies. Null when there is nothing to render.
    /// <para>The implementation lives in <see cref="AlertDetailText.Flatten"/>, in the Notifications
    /// project, because the delivery channels there decide whether an alert's prose detail adds anything
    /// over its structured context by comparing against this exact text — and that project cannot
    /// reference this one. Kept as the name every fire site already calls.</para>
    /// </summary>
    public static string? ContextToDetailText(AlertContext? context) =>
        AlertDetailText.Flatten(context);

    /// <summary>
    /// Collapses newlines to spaces, trims, and truncates to <paramref name="maxLength"/> with a
    /// trailing ellipsis — the single-line preview treatment for query text / job messages.
    /// </summary>
    public static string TruncateText(string text, int maxLength = 300)
    {
        if (string.IsNullOrEmpty(text)) return "";
        text = text.Replace('\r', ' ').Replace('\n', ' ').Trim();
        return text.Length <= maxLength ? text : text.Substring(0, maxLength) + "...";
    }

    /// <summary>
    /// <see cref="TruncateText"/> for statement text (#5320): the statement is judged WHOLE by
    /// <see cref="SensitiveStatements.Text"/> and the result is cut, never the other way round. A judge that sees
    /// an already-cut statement misses a value whose naming text (a <c>CREATE LOGIN</c> head, a
    /// <c>PASSWORD =</c> keyword) lies past the cut. A withheld statement reads as
    /// <see cref="SensitiveStatements.PlaceholderText"/>, which is shorter than any cut.
    /// </summary>
    public static string TruncateStatement(string text, int maxLength = 300)
    {
        var judged = SensitiveStatements.Text(text) ?? "";
        return TruncateText(judged, maxLength);
    }

    private static string FormatDuration(long seconds)
    {
        if (seconds < 60) return $"{seconds}s";
        if (seconds < 3600) return $"{seconds / 60}m {seconds % 60}s";
        return $"{seconds / 3600}h {(seconds % 3600) / 60}m";
    }
}
