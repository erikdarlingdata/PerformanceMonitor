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

        var filtered = FilterBlocking(events, excludedDatabases);
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
                item.Fields.Add(("Blocked Query", TruncateText(g.BlockedQuery)));
            if (!string.IsNullOrEmpty(g.BlockingQuery))
                item.Fields.Add(("Blocking Query", TruncateText(g.BlockingQuery)));
            item.Fields.Add(("Wait Range", g.Incident.WaitRange ?? g.MaxWaitMs.ToString()));
            context.Details.Add(item);
        }

        /* Surface the true total instead of silently dropping (gotqn's report). */
        if (groups.Count > maxGroups)
        {
            context.Details.Add(new AlertDetailItem
            {
                Heading = $"+{groups.Count - maxGroups} more distinct blocking incident(s)",
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
                item.Fields.Add(("Victim SQL", TruncateText(p.Row.VictimSqlText)));
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

        var filtered = FilterBlocking(events, excludedDatabases);
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

        /* #1140: dedup key = query_hash (stable across literals/plans). Null hash -> no incident. */
        return queries
            .Select(q => AlertFingerprint.ForKey(serverName, AlertFingerprint.Query, q.QueryHash ?? "",
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
    /// #2349: the files breaching either gate, worst first. Both gates are applied HERE rather than in the
    /// engine so the render path, the observation path and the decision can never disagree about which files
    /// are involved.
    ///
    /// <para>Ordered by how much of its volume the file occupies, because that is the one number that says how
    /// close this is to becoming a <c>Volume Free Space</c> page — a 40 GB rise on a 4 TB volume is less urgent
    /// than a 10 GB file that is now 80% of a small one.</para>
    /// </summary>
    public static List<DatabaseFileGrowthInfo> GetBreachedFiles(
        IReadOnlyList<DatabaseFileGrowthInfo>? files, int riseMb, int volumePercent)
    {
        if (files is null || files.Count == 0) return new List<DatabaseFileGrowthInfo>();

        var breached = files
            .Where(f =>
                (riseMb > 0 && f.GrowthMb >= riseMb)
                || (volumePercent > 0 && f.VolumeTotalMb > 0 && f.VolumePercent >= volumePercent))
            .OrderByDescending(f => f.VolumePercent)
            .ThenByDescending(f => f.GrowthMb)
            .ToList();

        return breached;
    }

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
                ("Growth", $"{f.GrowthGb:F1} GB in {f.GrowthWindowMinutes:F0} min ({f.GrowthMbPerHour:F0} MB/hr)"),
                ("Volume", string.IsNullOrEmpty(f.VolumeMountPoint) ? "(unknown)" : f.VolumeMountPoint),
                ("Volume Free", $"{f.VolumeFreeMb / 1024.0:F1} GB"),
                ("File % of Volume", $"{f.VolumePercent:F0}%"),
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
                Heading = $"{f.DatabaseName}.{f.FileName} — {f.TotalSizeGb:F1} GB ({f.VolumePercent:F0}% of {f.VolumeMountPoint})",
                Fields = fields
            });
        }

        AlertIncidentRenderer.Apply(context, Decorate(FileGrowthIncidents(serverName, shown).ToList(), decorateIncidents));
        return context;
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
                    : null)));

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
        if (!string.IsNullOrWhiteSpace(victimSql)) f.Add(new AlertIncidentField("Victim SQL", TruncateText(victimSql)));
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
        if (string.IsNullOrEmpty(row.DeadlockGraphXml)) return false;
        try
        {
            var doc = System.Xml.Linq.XElement.Parse(row.DeadlockGraphXml);
            var dbNames = doc.Descendants("process")
                .Select(p => p.Attribute("currentdbname")?.Value)
                .Where(n => !string.IsNullOrEmpty(n))
                .Cast<string>()
                .ToList();
            if (dbNames.Count == 0) return false;
            return dbNames.All(db => excludedDatabases.Any(e =>
                string.Equals(e, db, StringComparison.OrdinalIgnoreCase)));
        }
        catch { return false; }
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
                item.Fields.Add(("Query", TruncateText(q.QueryText)));
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
            sb.Append("\n  Active maintenance: ").Append(omitted).Append(" more maintenance session(s) not shown");

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
    public static List<PvsPressureInfo> GetBreachedPvsDatabases(List<PvsPressureInfo> databases, double thresholdPercent, double floorGb)
    {
        return databases
            .Where(d => thresholdPercent > 0
                && d.PvsPercent >= thresholdPercent
                && (floorGb <= 0 || d.PvsGb >= floorGb))
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

    private static string FormatDuration(long seconds)
    {
        if (seconds < 60) return $"{seconds}s";
        if (seconds < 3600) return $"{seconds / 60}m {seconds % 60}s";
        return $"{seconds / 3600}h {(seconds % 3600) / 60}m";
    }
}
