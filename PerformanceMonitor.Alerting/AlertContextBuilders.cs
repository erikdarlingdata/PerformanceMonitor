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

        var firstXml = filtered.FirstOrDefault(e => e.HasReportXml)?.BlockedProcessReportXml;
        if (!string.IsNullOrEmpty(firstXml))
        {
            context.AttachmentXml = firstXml;
            context.AttachmentFileName = "blocked_process_report.xml";
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
        var firstGraph = filtered.FirstOrDefault(d => d.HasDeadlockXml)?.DeadlockGraphXml;
        if (!string.IsNullOrEmpty(firstGraph))
        {
            context.AttachmentXml = firstGraph;
            context.AttachmentFileName = "deadlock_graph.xml";
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
                e.DatabaseName, e.ContentiousObject, e.BlockedSqlText, e.BlockingSqlText, e.WaitTimeMs, e.LockMode)));

    /* The graph parse, shared by the render path and #2216's observation path. Both the fingerprint's object
       set and the #2109 Database fact come off the same pass, so parsing once per deadlock is the point. */
    private static List<(DeadlockAlertRow Row, IReadOnlyList<string> Objects, IReadOnlyList<string> Databases)>
        ParseDeadlocks(IReadOnlyList<DeadlockAlertRow> filtered) =>
        filtered
            .Select(d => (Row: d,
                Objects: DeadlockObjectExtractor.FromGraphXml(d.DeadlockGraphXml),
                Databases: DeadlockObjectExtractor.DatabasesFromGraphXml(d.DeadlockGraphXml)))
            .ToList();

    /* #1140: fingerprint each deadlock by its sorted involved-object set, across ALL deadlocks in the window,
       grouped so recurrences over the same objects collapse to one incident with a count. */
    private static List<DeadlockIncidentGrouper.DeadlockGroup> GroupParsedDeadlocks(
        string serverName,
        List<(DeadlockAlertRow Row, IReadOnlyList<string> Objects, IReadOnlyList<string> Databases)> parsed) =>
        DeadlockIncidentGrouper.Group(
            serverName,
            parsed.Select(p => new DeadlockIncidentGrouper.DeadlockEvent(
                p.Objects,
                DeadlockDetailFields(p.Databases, p.Row.VictimSqlText, p.Row.ProcessSummary))));

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
       databases, victim SQL, and process summary. Since #2108 these render on the incident's own
       summary item too, not just per-event cards. */
    private static List<AlertIncidentField>? DeadlockDetailFields(
        IReadOnlyList<string> databases, string? victimSql, string? processes)
    {
        var f = new List<AlertIncidentField>();
        if (databases.Count > 0) f.Add(new AlertIncidentField("Database", string.Join(", ", databases)));
        if (!string.IsNullOrWhiteSpace(victimSql)) f.Add(new AlertIncidentField("Victim SQL", TruncateText(victimSql)));
        if (!string.IsNullOrWhiteSpace(processes)) f.Add(new AlertIncidentField("Processes", processes!));
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

    public static AlertContext? BuildLongRunningQueryContext(
        string serverName, List<LongRunningQueryInfo> queries,
        Func<IReadOnlyList<AlertIncident>, IReadOnlyList<AlertIncident>>? decorateIncidents = null)
    {
        if (queries.Count == 0) return null;

        var context = new AlertContext();
        var shown = queries.GetRange(0, Math.Min(3, queries.Count));
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
            Heading = $"tempdb — {tempDb.UsedPercent:F0}% Used",
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
                    ("Started", j.StartTime.ToString("yyyy-MM-dd HH:mm:ss"))
                }
            });
        }

        /* #1140: dedup key per job (job name, scoped to the instance via serverName). */
        AlertIncidentRenderer.Apply(context, Decorate(AnomalousJobIncidents(serverName, shown).ToList(), decorateIncidents));
        return context;
    }

    public static AlertContext? BuildFailedJobContext(
        string serverName, List<FailedJobInfo> jobs,
        Func<IReadOnlyList<AlertIncident>, IReadOnlyList<AlertIncident>>? decorateIncidents = null)
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

        /* #1140: dedup key per job (job name, scoped to the instance via serverName) — mirrors
           BuildAnomalousJobContext so two distinct failed jobs are distinct incidents under the
           #1154 per-fingerprint cooldown instead of coalescing on the metric key. */
        AlertIncidentRenderer.Apply(context, Decorate(FailedJobIncidents(serverName, shown).ToList(), decorateIncidents));
        return context;
    }

    /// <summary>
    /// Flattens an <see cref="AlertContext"/> into the plain-text detail block persisted in alert
    /// history and rendered in plain-text notification bodies. Null when there is nothing to render.
    /// </summary>
    public static string? ContextToDetailText(AlertContext? context)
    {
        if (context == null || context.Details.Count == 0) return null;
        var sb = new System.Text.StringBuilder();
        foreach (var detail in context.Details)
        {
            if (sb.Length > 0) sb.AppendLine();
            sb.AppendLine(detail.Heading);
            foreach (var (label, value) in detail.Fields)
                sb.AppendLine($"  {label}: {value}");
        }
        return sb.ToString().TrimEnd();
    }

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
