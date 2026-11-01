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
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Alerting;

/// <summary>One <c>count</c> row of <c>attention_events</c>: the exact number of attentions the event_counter saw since the
/// prior count row (#5640). The collector writes one only for a run whose delta is above zero.</summary>
/// <param name="CollectionTime">The run's <c>collection_time</c> (naive UTC, microseconds kept).</param>
/// <param name="Count">The row's <c>attention_count</c>.</param>
public sealed record AttentionCountRow(DateTime CollectionTime, long Count);

/// <summary>Sampled attention events (the ring buffer's kept events, one <c>sample</c> row each) of one run, for one
/// (database, client application). A sample is detail, never the total (#5640).</summary>
/// <param name="CollectionTime">The collection time of the run that kept the samples.</param>
/// <param name="DatabaseName">The event's database, or null when the row carries none.</param>
/// <param name="ClientAppName">The event's client application, or null when the row carries none.</param>
/// <param name="Samples">How many sample rows this group holds.</param>
/// <param name="ShortSamples">The part of <paramref name="Samples"/> with a recorded duration under <see cref="AttentionWindowSql.MinInterruptDurationUs"/> (#7061).</param>
/// <param name="AzdataMetadataSamples">The part of <paramref name="Samples"/> that is Azure Data Studio metadata work and not already short (#7061).</param>
public sealed record AttentionSampleGroup(
    DateTime CollectionTime, string? DatabaseName, string? ClientAppName, long Samples, long ShortSamples = 0, long AzdataMetadataSamples = 0);

/// <summary>One entry of a top-5 list in the read: a statement fingerprint, a database or a client application, ranked by
/// its sampled event count over the range the read covers (the alert card's covered span, or the whole lookback when the span kept
/// no samples; see <see cref="AttentionWindow.TopsFromLookback"/>).</summary>
/// <param name="Name">The fingerprint, database name or application name. Rows that carry none are not listed.</param>
/// <param name="Samples">Sampled events carrying it in the lookback.</param>
/// <param name="SampleText">For a statement fingerprint, one sample's statement text (already through the statement filter);
/// null for the other two lists.</param>
public sealed record AttentionTopItem(string Name, long Samples, string? SampleText = null);

/// <summary>
/// The Client Timeouts alert's whole read (#5640), as plain data with no judgment (the adapter contract,
/// <see cref="IAlertReadAdapter.GetAttentionWindowAsync"/>).
/// </summary>
/// <param name="RunTimes">Every SUCCESSFUL attention collector run in the lookback, from the collection log, newest first. A
/// quiet run writes no row to <c>attention_events</c>, so the log is the only proof the collector ran.</param>
/// <param name="Counts">The <c>count</c> rows in the lookback, each with its own time.</param>
/// <param name="SampleGroups">The <c>sample</c> rows in the lookback grouped by (run, database, application).</param>
/// <param name="TopStatements">The top 5 statement fingerprints by sampled events, each with one sample text.</param>
/// <param name="TopDatabases">The top 5 databases by sampled events.</param>
/// <param name="TopApplications">The top 5 client applications by sampled events.</param>
/// <param name="TopsFromLookback">False (the default): the three top lists cover the samples inside the covered span
/// (<see cref="AttentionEvaluator.SpanBoundary"/> onward), the alert card's reading. True: the span kept no samples, so the lists
/// cover the whole lookback and the card says so in its headings (round 2 of #5714, L6).</param>
/// <param name="TopHosts">The top 5 client hosts by sampled events (#6553), over the same range as the other top lists. Null on a window
/// built without them; read it through <see cref="AttentionWindow.Hosts"/>.</param>
public sealed record AttentionWindow(
    IReadOnlyList<DateTime> RunTimes,
    IReadOnlyList<AttentionCountRow> Counts,
    IReadOnlyList<AttentionSampleGroup> SampleGroups,
    IReadOnlyList<AttentionTopItem> TopStatements,
    IReadOnlyList<AttentionTopItem> TopDatabases,
    IReadOnlyList<AttentionTopItem> TopApplications,
    bool TopsFromLookback = false,
    IReadOnlyList<AttentionTopItem>? TopHosts = null)
{
    /// <summary>The top client hosts (#6553), empty when the window carries none.</summary>
    public IReadOnlyList<AttentionTopItem> Hosts => TopHosts ?? Array.Empty<AttentionTopItem>();

    /// <summary>No runs, no rows: what a read of a server that never ran the collector returns.</summary>
    public static AttentionWindow Empty { get; } = new(
        Array.Empty<DateTime>(), Array.Empty<AttentionCountRow>(), Array.Empty<AttentionSampleGroup>(),
        Array.Empty<AttentionTopItem>(), Array.Empty<AttentionTopItem>(), Array.Empty<AttentionTopItem>());
}

/// <summary>Sampled events of one (database, client application) inside the covered span.</summary>
public sealed record AttentionGroup(string? DatabaseName, string? ClientAppName, long Samples, long ShortSamples = 0, long AzdataMetadataSamples = 0);

/// <summary>
/// What <see cref="AttentionEvaluator.Read"/> took from the window: the covered span, the exact count in it and the
/// sampled events by group.
/// </summary>
/// <param name="TotalCount">The exact number of attentions in the span (the sum of the count rows of the taken runs).</param>
/// <param name="CoveredMinutes">Minutes from the run BEFORE the oldest taken run to the newest run (at least 1).</param>
/// <param name="RunCount">How many successful runs the span takes.</param>
/// <param name="NewestRunTime">The newest successful run (naive UTC): the fresh-sample marker.</param>
/// <param name="Groups">Sampled events in the span by (database, application); empty when the span kept no samples.</param>
public sealed record AttentionReading(
    long TotalCount,
    double CoveredMinutes,
    int RunCount,
    DateTime NewestRunTime,
    IReadOnlyList<AttentionGroup> Groups)
{
    /// <summary>All sampled events in the span.</summary>
    public long SampleTotal => Groups.Sum(g => g.Samples);

    /// <summary>Sampled events in the span from the applications <see cref="AttentionEvaluator.IsExcludedApp"/> leaves out: SSMS IntelliSense (#6552) and the VS Code mssql extension's metadata connections (#6856).</summary>
    public long ExcludedSamples => Groups.Where(g => AttentionEvaluator.IsExcludedApp(g.ClientAppName)).Sum(g => g.Samples);

    /// <summary>Sampled events in the span from SSMS IntelliSense, the part of <see cref="ExcludedSamples"/> that names that tool (#6856).</summary>
    public long ExcludedSsmsSamples => Groups.Where(g => AttentionEvaluator.IsSsmsIntelliSense(g.ClientAppName)).Sum(g => g.Samples);

    /// <summary>Sampled events in the span from the VS Code mssql extension's metadata connections, the other part of <see cref="ExcludedSamples"/> (#6856).</summary>
    public long ExcludedVsCodeSamples => Groups.Where(g => AttentionEvaluator.IsVsCodeMssqlMetadata(g.ClientAppName)).Sum(g => g.Samples);

    /// <summary>Sampled events in the span under the duration floor (#7061), from applications that are not already left out by name. A sampled event with no
    /// recorded duration is never counted here.</summary>
    public long ShortSamples => Groups.Where(g => !AttentionEvaluator.IsExcludedApp(g.ClientAppName)).Sum(g => g.ShortSamples);

    /// <summary>Sampled events in the span that are Azure Data Studio metadata work (#7061), not already counted in <see cref="ShortSamples"/>.</summary>
    public long AzdataMetadataSamples => Groups.Where(g => !AttentionEvaluator.IsExcludedApp(g.ClientAppName)).Sum(g => g.AzdataMetadataSamples);

    /// <summary>The distinct <c>client_app_name</c> strings the rule left out of the span, exactly as stored, most sampled events first (#6856).</summary>
    public IReadOnlyList<string> ExcludedApps => Groups
        .Where(g => g.Samples > 0 && AttentionEvaluator.IsExcludedApp(g.ClientAppName))
        .GroupBy(g => g.ClientAppName!, StringComparer.Ordinal)
        .OrderByDescending(x => x.Sum(g => g.Samples))
        .ThenBy(x => x.Key, StringComparer.Ordinal)
        .Select(x => x.Key)
        .ToList();
}

/// <summary>
/// Grades the Client Timeouts alert (#5640): client attentions (a client cancelled or timed out a query) per minute, from the
/// <c>attention_events</c> rows the attention collector writes.
///
/// <para><b>The count model.</b> The collector writes one <c>count</c> row per run, only when the session's event_counter
/// moved, holding the exact number of attentions since the prior run. The other rows are SAMPLES, one per kept ring-buffer
/// event, with database, application and statement; they are never the total. A quiet run writes nothing, so "no rows" never
/// means the collector ran: freshness and the minutes covered come from the collection log's successful runs.</para>
///
/// <para><b>The rate rule.</b> Take the successful runs newest first, and add runs until the span from the run BEFORE the
/// oldest taken run to the newest run reaches <see cref="WindowMinutes"/> (3) minutes, or the history runs out. The rate is
/// the sum of the taken runs' count rows divided by that span in minutes (at least 1). That is exact: each count row covers
/// the time since the run before it, so the taken rows cover exactly the span, and a failed run inside it only widens the
/// span of the next success, whose counter delta spans both. A storm at three times the bar still fires on the first run after
/// it starts, as the same averaging over three minutes does for #5641.</para>
///
/// <para><b>Not observed.</b> With no successful run within max(10 minutes, 4 times the median of the newest 5 gaps between
/// runs), capped at the lookback, or with fewer than two runs (no span), the alert neither fires nor clears. The window
/// rule is <see cref="RunnableQueueEvaluator.FallbackWindowOfTimes(IReadOnlyList{DateTime})"/>: one definition for both alerts.
/// A standing alert unevaluated for <see cref="StaleIncidentMinutes"/> is resolved by the engine, with the reason stated.</para>
///
/// <para><b>Known edge, deliberate.</b> The first run after a service restart stages the counter and writes no count row, so
/// the span before it is counted as quiet. That under-counts, which is the safe direction for a paging alert; nothing more
/// is built for it.</para>
///
/// <para><b>Mute is proportional.</b> A mute rule matches a (database, application) group, but the exact count has no groups,
/// only the samples do. The judged count is the exact count times the unmuted share of the sampled events in the span.
/// With no samples in the span the full count is judged. When the full count crosses a bar and the judged count does not, the
/// fire is recorded Muted. When the proportion changed the count, the detail says it is an estimate from N sampled
/// events.</para>
///
/// <para><b>The bars (#5736).</b> A field read of two stores found no fixed bar that fits: each server's own normal rate (the p99.9 of
/// its 3-minute average over a week) ran from 0 to over 900 attentions a minute. So the bars are relative. Warning is
/// max(WarningFloor, WarningMultiple x baseline) and Critical is max(CriticalFloor, CriticalMultiple x baseline), the baseline being
/// the server's own p99.9 of its 3-minute average attention rate over the trailing <see cref="AttentionBaselineMath.LookbackDays"/> days
/// (<see cref="AttentionBaselineMath"/>). The floors are the settings the alert shipped with, Warning 10 and Critical 60 per minute, and
/// the multiples default to 3 and 10. A server with under a day of capture has no baseline and is graded on the floors alone, and the alert
/// says so. ON by default (V216 on Darling).</para>
/// </summary>
public static class AttentionEvaluator
{
    /// <summary>The metric name on the alert row, the mute rule, the cooldown and the history grids. Declared once, in
    /// <see cref="AlertFamily.ClientTimeoutsMetric"/>, because the assemblies below this one read it too.</summary>
    public const string MetricName = AlertFamily.ClientTimeoutsMetric;

    /// <summary>The span the rate is averaged over, in minutes.</summary>
    public const int WindowMinutes = 3;

    /// <summary>The client application SSMS IntelliSense runs its metadata queries under (#6552). It cancels them itself when the user keeps
    /// typing, so each one is a cancel the client asked for, not a timeout an application user saw.</summary>
    public const string IntelliSenseClientApp = "Microsoft SQL Server Management Studio - Transact-SQL IntelliSense";

    /// <summary>
    /// True for a sampled event whose client application is SSMS IntelliSense (#6552) or one of the VS Code mssql extension's metadata
    /// connections (#6856): the alert judges the count without it, and <c>get_client_timeouts</c> marks it excluded. The filter lives at
    /// READ time (here and in <see cref="ClientTimeoutsRead"/>); the collector still stores every event, so a mute rule, the sampled-event
    /// list and the history keep seeing them. SSMS matches "Microsoft SQL Server Management Studio" followed by anything and ending in
    /// "IntelliSense", so a renamed SSMS build still matches. VS Code matches a closed list, see <see cref="IsVsCodeMssqlMetadata"/>.
    /// Two more cuts are NOT by application name, because they are made on the sampled events inside the shared read (<see cref="AttentionWindowSql.SampleGroups"/>) and
    /// reach the evaluator as counts on the group (#7061): events under the 1 ms duration floor (<see cref="AttentionWindowSql.MinInterruptDurationUs"/>), and Azure Data Studio's
    /// metadata statements (<see cref="AttentionWindowSql.AzdataMetadataPredicate"/>). Azure Data Studio's query editor connects as plain <c>azdata</c> too, so <c>azdata</c> is not excluded by name.
    /// </summary>
    public static bool IsExcludedApp(string? clientAppName) =>
        IsSsmsIntelliSense(clientAppName) || IsVsCodeMssqlMetadata(clientAppName);

    /// <summary>True for the SSMS IntelliSense application name (#6552).</summary>
    public static bool IsSsmsIntelliSense(string? clientAppName) =>
        clientAppName is not null
        && clientAppName.StartsWith(AttentionWindowSql.SsmsIntelliSensePrefix, StringComparison.OrdinalIgnoreCase)
        && clientAppName.TrimEnd(' ').EndsWith(AttentionWindowSql.SsmsIntelliSenseSuffix, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// The tool-naming part of every sentence that says where the left-out cancels came from (#6856): the alert summary, the alert context
    /// line and the <c>get_client_timeouts</c> note all call this, so a tool added to the rule is named in one place. Names only the tools
    /// whose flag is set, so an SSMS-only server is not told about VS Code. With neither flag set there is nothing to name, so it throws. The one exception is the <c>get_client_timeouts</c> note, which
    /// describes the rule in general rather than one window, so it passes both flags.
    /// </summary>
    public static string ExcludedToolNames(bool ssmsIntelliSense, bool vsCodeMetadata) =>
        (ssmsIntelliSense, vsCodeMetadata) switch
        {
            (true, true) => "SSMS IntelliSense and VS Code mssql metadata connections",
            (false, true) => "VS Code mssql metadata connections",
            (true, false) => "SSMS IntelliSense",
            /* No caller reaches this: each guards on left-out samples above zero, and those come from one of the two tools. A tool added to the rule
               must get its own arm here rather than be named as SSMS. */
            (false, false) => throw new ArgumentException("At least one excluded tool must be set.", nameof(ssmsIntelliSense)),
        };

    /// <summary>True for one of the VS Code mssql extension's metadata application names (#6856). The closed list lives in <see cref="AttentionWindowSql.VsCodeMetadataApps"/>, with the reasons, so the baseline's SQL rule reads the same names.</summary>
    public static bool IsVsCodeMssqlMetadata(string? clientAppName)
    {
        if (clientAppName is null)
        {
            return false;
        }

        var name = clientAppName.Trim(' ');
        foreach (var known in AttentionWindowSql.VsCodeMetadataApps)
        {
            if (string.Equals(name, known, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>How far back the read looks, in minutes. Equal to <see cref="RunnableQueueEvaluator.ReadLookbackMinutes"/>
    /// (a test pins it): the shared freshness rule caps its window there.</summary>
    public const int ReadLookbackMinutes = 60;

    /// <summary>How far past the newest successful run a row may sit and still belong to the span, in seconds. The collection
    /// log stamps a run when it starts and the rows carry the time the runner took a moment later; a row further out than this
    /// belongs to a run the log has not recorded yet (the log row is written after the data rows), so it is left for the next
    /// evaluation instead of counted in a span that does not cover it.</summary>
    public const int RowSkewSeconds = 30;

    /// <summary>A standing alert with no evaluation for this many minutes is resolved by the engine ("no fresh collector
    /// runs"), so a collector that was disabled, slowed or broken mid-incident cannot hold it open forever.</summary>
    public const int StaleIncidentMinutes = 30;

    /// <summary>Shipped Warning floor (#5736): attentions per minute. The relative Warning bar never sits below it.</summary>
    public const int DefaultWarningPerMinute = 10;

    /// <summary>Shipped Critical floor (#5736): attentions per minute. The relative Critical bar never sits below it.</summary>
    public const int DefaultCriticalPerMinute = 60;

    /// <summary>The effective bar for one tier (#5736): the floor, or the multiple times the baseline rounded up to a whole attention a minute
    /// when that is higher. With no baseline it is the floor.</summary>
    public static int EffectiveBar(int floor, int multiple, double? baseline) =>
        baseline is { } b && b > 0 && !double.IsNaN(b) && !double.IsInfinity(b)
            ? (int)Math.Min(int.MaxValue, Math.Max(floor, Math.Ceiling(multiple * b)))
            : floor;

    /// <summary>True when the exact rate of <paramref name="reading"/> reaches the Warning floor, the cheapest test of whether any bar
    /// can be crossed: a relative bar is never below its floor, so a rate under the floor fires nothing and the baseline need not be read.</summary>
    public static bool ReachesFloor(AttentionReading? reading, int warningFloor)
    {
        if (reading is null)
        {
            return false;
        }

        var (warning, _) = ClampBars(warningFloor, warningFloor);
        return Math.Round(reading.TotalCount / reading.CoveredMinutes, 2, MidpointRounding.AwayFromZero) >= warning;
    }

    /// <summary>The clamp both stores apply on read: the Warning bar is at least 1 and the Critical bar is never below it.
    /// The same rule as the Runnable Queue pair.</summary>
    public static (int Warning, int Critical) ClampBars(int warning, int critical) =>
        RunnableQueueEvaluator.ClampBars(warning, critical);

    /// <summary>
    /// Applies the window and rate rules to <paramref name="window"/> at evaluation time <paramref name="now"/> (naive UTC,
    /// the clock of the collector's times). Null is "not observed" (see the class remarks).
    /// </summary>
    public static AttentionReading? Read(AttentionWindow? window, DateTime now)
    {
        if (window is null || window.RunTimes.Count == 0)
        {
            return null;
        }

        var runs = window.RunTimes.Distinct().OrderByDescending(t => t).ToList();

        /* The freshness rule is #5709's, applied to the collector's runs: the newest run must be inside
           max(10 minutes, 4 x the median of the newest 5 run gaps), capped at the lookback. */
        if (runs[0] < now.AddMinutes(-RunnableQueueEvaluator.FallbackWindowOfTimes(runs)))
        {
            return null;
        }

        /* The span needs a run BEFORE the oldest taken run, so one run alone is no observation. */
        if (runs.Count < 2)
        {
            return null;
        }

        return Take(window, runs, OldestTaken(runs));
    }

    /// <summary>The index of the oldest run <see cref="Read"/> takes: runs are added newest first until the span from the run before
    /// the oldest taken run to the newest run reaches <see cref="WindowMinutes"/>, or the history runs out. Needs at least two runs.</summary>
    private static int OldestTaken(List<DateTime> runs)
    {
        var newest = runs[0];
        var oldestTaken = 0;
        for (var k = 0; k < runs.Count - 1; k++)
        {
            oldestTaken = k;
            if ((newest - runs[k + 1]).TotalMinutes >= WindowMinutes)
            {
                break;
            }
        }

        return oldestTaken;
    }

    /// <summary>
    /// The instant after which a row belongs to the span <see cref="Read"/> judges (round 2 of #5714, L6): the midpoint between the
    /// base run and the oldest taken run. The alert reads' adapters start the card's top lists here, so the top statements,
    /// databases and applications cover the alert's own few minutes and not the whole lookback. Null when fewer than two runs
    /// make no span. Freshness is not asked: a stale window is "not observed" and never reaches a card.
    /// </summary>
    public static DateTime? SpanBoundary(IReadOnlyList<DateTime> runTimes)
    {
        var runs = runTimes.Distinct().OrderByDescending(t => t).ToList();
        return runs.Count < 2 ? null : Boundary(runs, OldestTaken(runs));
    }

    /// <summary>
    /// Where the alert read's three top lists start (round 2 of #5714, L6), and whether they are the lookback's. The span's boundary
    /// when the span kept sampled events; otherwise (no span, or a span with no samples) the whole lookback, flagged so the card
    /// says its real range. Both alert adapters call this once, after the runs and sample groups are read.
    /// </summary>
    public static (DateTime Floor, bool FromLookback) TopListFloor(
        IReadOnlyList<DateTime> runTimes, IReadOnlyList<AttentionSampleGroup> sampleGroups, DateTime lookbackFloor)
    {
        /* Final round of #5714 (L2): "the span kept samples" is asked with Take's own cut, > boundary AND <= newest + RowSkewSeconds,
           so a row a collector run committed past the newest logged run (its SUCCESS log row not yet written) neither lists on the
           card nor turns a sample-less span into a plain "Database" / "Client Application" list the count never saw. */
        var ceiling = TopListCeiling(runTimes);
        if (SpanBoundary(runTimes) is { } boundary
            && sampleGroups.Any(g => g.CollectionTime > boundary && g.CollectionTime <= ceiling))
        {
            return (boundary, false);
        }

        return (lookbackFloor, true);
    }

    /// <summary>
    /// The row ceiling of the alert read's three top lists (final round of #5714, L2): the newest successful run plus
    /// <see cref="RowSkewSeconds"/>, the very ceiling <see cref="Take"/> cuts the count and the sample groups at (and
    /// <see cref="RowSpan"/> hands the MCP read). Both the span lists and the lookback fallback stop here, so the card never lists a
    /// row the judged count could not have held. With no run at all the window is "not observed" and never reaches a card, so
    /// there is nothing to cut at: <see cref="DateTime.MaxValue"/>.
    /// </summary>
    public static DateTime TopListCeiling(IReadOnlyList<DateTime> runTimes) =>
        runTimes.Count == 0 ? DateTime.MaxValue : runTimes.Max().AddSeconds(RowSkewSeconds);

    private static DateTime Boundary(List<DateTime> runs, int oldestTaken)
    {
        var baseRun = runs[oldestTaken + 1];
        return baseRun + ((runs[oldestTaken] - baseRun) / 2);
    }

    /// <summary>
    /// The read surface's reading (get_client_timeouts, #5640): the SAME freshness and span rules as <see cref="Read"/>, but the span
    /// takes EVERY successful run in the window (from the oldest run in it to the newest) instead of stopping at
    /// <see cref="WindowMinutes"/>, so the rate is the exact count over the whole range asked for. Null when the evaluator would say
    /// "not observed" (no fresh run, or fewer than two runs). The first run in the window is the BASE, not a taken run: its count row
    /// covers the time before the window.
    /// </summary>
    public static AttentionReading? ReadSpan(AttentionWindow? window, DateTime now)
    {
        if (window is null || window.RunTimes.Count == 0)
        {
            return null;
        }

        var runs = window.RunTimes.Distinct().OrderByDescending(t => t).ToList();
        if (runs[0] < now.AddMinutes(-RunnableQueueEvaluator.FallbackWindowOfTimes(runs)) || runs.Count < 2)
        {
            return null;
        }

        return Take(window, runs, runs.Count - 2);
    }

    /// <summary>
    /// Which rows belong to the span of <paramref name="runTimes"/> (the successful runs of one range, any order, duplicates allowed):
    /// the rows with <c>collection_time &gt; Boundary</c> and <c>collection_time &lt;= Ceiling</c>, the rule <see cref="Take"/> cuts the
    /// counts and the sample groups with. Null when fewer than two runs exist (no base run, so no span). The read surface (#5640,
    /// get_client_timeouts) binds these two times into the top lists and the sampled events, so they cover exactly the rows
    /// <see cref="ReadSpan"/> counts and never the base run's own samples from before the span or anything past the newest run.
    /// </summary>
    public static (DateTime Boundary, DateTime Ceiling)? RowSpan(IEnumerable<DateTime> runTimes)
    {
        var runs = runTimes.Distinct().OrderByDescending(t => t).ToList();
        if (runs.Count < 2)
        {
            return null;
        }

        return (Boundary(runs, runs.Count - 2), runs[0].AddSeconds(RowSkewSeconds));
    }

    private static AttentionReading Take(AttentionWindow window, List<DateTime> runs, int oldestTaken)
    {
        var newest = runs[0];
        var baseRun = runs[oldestTaken + 1];
        var covered = Math.Max(1d, (newest - baseRun).TotalMinutes);

        /* A row belongs to the span when it sits after the midpoint between the base run and the oldest taken run. The
           collection log stamps a run when it starts and the rows carry the time the runner took a moment later, so the
           two clocks differ by seconds; the midpoint keeps the base run's own row out and the oldest taken run's row in
           for any cadence. */
        var boundary = Boundary(runs, oldestTaken);

        var ceiling = newest.AddSeconds(RowSkewSeconds);

        var total = window.Counts.Where(c => c.CollectionTime > boundary && c.CollectionTime <= ceiling).Sum(c => c.Count);
        var groups = window.SampleGroups
            .Where(g => g.CollectionTime > boundary && g.CollectionTime <= ceiling)
            .GroupBy(g => (g.DatabaseName, g.ClientAppName))
            .Select(g => new AttentionGroup(g.Key.DatabaseName, g.Key.ClientAppName, g.Sum(x => x.Samples), g.Sum(x => x.ShortSamples), g.Sum(x => x.AzdataMetadataSamples)))
            .ToList();

        return new AttentionReading(total, covered, oldestTaken + 1, newest, groups);
    }

    /// <summary>
    /// Grades the reading against the two bars. Null when no reading, or when the EXACT count's rate is under the Warning
    /// bar. A rate is compared at two decimals, so a rate sitting exactly at a bar is not lost to floating point.
    /// <paramref name="isGroupMuted"/> is asked once per sampled (database, application) group, and only when the exact
    /// rate crossed a bar.
    /// </summary>
    public static Finding? Evaluate(
        AttentionReading? reading, int warningPerMinute, int criticalPerMinute, Func<string?, string?, bool>? isGroupMuted = null,
        AttentionRelativeBars? relative = null)
    {
        if (reading is null)
        {
            return null;
        }

        var (warningFloor, criticalFloor) = ClampBars(warningPerMinute, criticalPerMinute);
        var (warningMultiple, criticalMultiple) = relative is null
            ? (0, 0)
            : AttentionBaselineMath.ClampMultiples(relative.WarningMultiple, relative.CriticalMultiple);
        var warning = relative is null ? warningFloor : EffectiveBar(warningFloor, warningMultiple, relative.Baseline);
        var critical = relative is null ? criticalFloor : Math.Max(warning, EffectiveBar(criticalFloor, criticalMultiple, relative.Baseline));
        var bars = relative is null ? null : new BarBasis(relative.Baseline, warningFloor, criticalFloor, warningMultiple, criticalMultiple);
        var fullRate = Math.Round(reading.TotalCount / reading.CoveredMinutes, 2, MidpointRounding.AwayFromZero);
        if (!Grade(fullRate, warning, critical, out var fullSeverity, out var fullBar))
        {
            return null;
        }

        var sampleTotal = reading.SampleTotal;
        var excludedSamples = reading.ExcludedSamples;
        var excludedSsms = reading.ExcludedSsmsSamples;
        var excludedVsCode = reading.ExcludedVsCodeSamples;
        var shortSamples = reading.ShortSamples;
        var azdataMetadata = reading.AzdataMetadataSamples;
        var judgedCount = (double)reading.TotalCount;
        var estimated = false;
        if (sampleTotal == 0 && isGroupMuted is not null && isGroupMuted(null, null))
        {
            /* Round 2 of #5714 (M1): a span that kept no samples is judged at the full count, and the server-wide mute still
               applies. A rule with a database or app pattern never matches a null database and app, so only a rule on the
               server and metric alone is asked here and can mute. The finding is Muted at the full count's grade. */
            return new Finding(fullSeverity, fullRate, fullBar, reading.TotalCount, judgedCount, reading.CoveredMinutes,
                reading.RunCount, sampleTotal, estimated, Muted: true, reading.NewestRunTime, bars, excludedSamples, excludedSsms, excludedVsCode, shortSamples, azdataMetadata);
        }

        if (sampleTotal > 0 && (isGroupMuted is not null || excludedSamples > 0 || shortSamples > 0 || azdataMetadata > 0))
        {
            /* #6552, #6856: the own cancels of SSMS IntelliSense and the VS Code mssql metadata connections are left out the way a muted group is, by the same proportion of the sampled events.
               #7061: so are the sampled events under the duration floor and Azure Data Studio's metadata statements. The exact count cannot be filtered by duration, so it
               scales by the share of sampled events that pass. A sample with no recorded duration passes. */
            var unmuted = reading.Groups
                .Where(g => !IsExcludedApp(g.ClientAppName) && (isGroupMuted is null || !isGroupMuted(g.DatabaseName, g.ClientAppName)))
                .Sum(g => g.Samples - g.ShortSamples - g.AzdataMetadataSamples);
            if (unmuted < sampleTotal)
            {
                judgedCount = reading.TotalCount * (double)unmuted / sampleTotal;
                estimated = true;
            }
        }

        var judgedRate = Math.Round(judgedCount / reading.CoveredMinutes, 2, MidpointRounding.AwayFromZero);
        if (Grade(judgedRate, warning, critical, out var judgedSeverity, out var judgedBar))
        {
            return new Finding(judgedSeverity, judgedRate, judgedBar, reading.TotalCount, judgedCount, reading.CoveredMinutes,
                reading.RunCount, sampleTotal, estimated, Muted: false, reading.NewestRunTime, bars, excludedSamples, excludedSsms, excludedVsCode, shortSamples, azdataMetadata);
        }

        /* The exact count crossed a bar, the unmuted share did not: the fire is recorded Muted at the exact count's grade. */
        return new Finding(fullSeverity, fullRate, fullBar, reading.TotalCount, judgedCount, reading.CoveredMinutes,
            reading.RunCount, sampleTotal, estimated, Muted: true, reading.NewestRunTime, bars, excludedSamples, excludedSsms, excludedVsCode, shortSamples, azdataMetadata);
    }

    private static bool Grade(double rate, int warning, int critical, out AlertSeverityLevel severity, out int bar)
    {
        if (rate >= critical)
        {
            severity = AlertSeverityLevel.Critical;
            bar = critical;
            return true;
        }

        if (rate >= warning)
        {
            severity = AlertSeverityLevel.Warning;
            bar = warning;
            return true;
        }

        severity = AlertSeverityLevel.Warning;
        bar = 0;
        return false;
    }

    /// <summary>
    /// The summary sentence: "412 client timeouts in the last 3 minutes (137.3 per minute)." and, when the mute proportion
    /// was applied, "Estimated from 20 sampled events: 31.2 per minute is unmuted."
    /// </summary>
    public static string Summary(Finding finding)
    {
        var minutes = Math.Max(1, (int)Math.Round(finding.CoveredMinutes, MidpointRounding.AwayFromZero));
        var text = string.Create(CultureInfo.InvariantCulture,
            $"{finding.TotalCount:N0} client timeout{(finding.TotalCount == 1 ? "" : "s")} in the last {minutes} minute{(minutes == 1 ? "" : "s")} ({finding.TotalCount / finding.CoveredMinutes:N1} per minute).");
        if (finding.Estimated)
        {
            text += string.Create(CultureInfo.InvariantCulture,
                $" Estimated from {finding.SampledEvents:N0} sampled events: {finding.JudgedCount / finding.CoveredMinutes:N1} per minute is not muted.");
        }

        if (finding.ExcludedSamples > 0)
        {
            text += string.Create(CultureInfo.InvariantCulture,
                $" {finding.ExcludedSamples:N0} of the {finding.SampledEvents:N0} sampled events came from {ExcludedToolNames(finding.ExcludedSsmsSamples > 0, finding.ExcludedVsCodeSamples > 0)} cancelling their own queries and are left out of the rate judged.");
        }

        if (finding.ShortSamples > 0)
        {
            text += string.Create(CultureInfo.InvariantCulture,
                $" {finding.ShortSamples:N0} of the {finding.SampledEvents:N0} sampled events lasted under {AttentionWindowSql.MinInterruptDurationUs / 1000} ms (a cancel on a command that had already finished) and are left out of the rate judged.");
        }

        if (finding.AzdataMetadataSamples > 0)
        {
            text += string.Create(CultureInfo.InvariantCulture,
                $" {finding.AzdataMetadataSamples:N0} of the {finding.SampledEvents:N0} sampled events were Azure Data Studio metadata queries and are left out of the rate judged.");
        }

        if (finding.Bars is not null)
        {
            text += " " + BarSentence(finding);
        }

        return text;
    }

    /// <summary>
    /// The sentence that names the baseline, the multiple and the bar that was crossed (#5736): "Baseline for this server: 150.0 per minute (p99.9 of
    /// its 3-minute average over the last 7 days). Warning is 3 x baseline = 450 per minute." When the multiple of the baseline is under the floor,
    /// it says the floor applies; with no baseline it says the server has under a day of capture and the floor alone is the bar.
    /// </summary>
    public static string BarSentence(Finding finding)
    {
        if (finding.Bars is not { } bars)
        {
            return "";
        }

        var tier = finding.Severity == AlertSeverityLevel.Critical ? "Critical" : "Warning";
        var floor = finding.Severity == AlertSeverityLevel.Critical ? bars.CriticalFloor : bars.WarningFloor;
        var multiple = finding.Severity == AlertSeverityLevel.Critical ? bars.CriticalMultiple : bars.WarningMultiple;
        if (bars.Baseline is null)
        {
            return string.Create(CultureInfo.InvariantCulture,
                $"This server has under {AttentionBaselineMath.MinimumCaptureDays} day of capture and no baseline yet, so the {tier} bar is the floor, {floor} per minute.");
        }

        var baseline = bars.Baseline.Value;
        if (baseline <= 0)
        {
            return string.Create(CultureInfo.InvariantCulture,
                $"Baseline for this server: 0.0 per minute (p99.9 of its 3-minute average over the last {AttentionBaselineMath.LookbackDays} days), so the {tier} bar is the floor, {floor} per minute.");
        }

        var scaled = multiple * baseline;
        var lead = string.Create(CultureInfo.InvariantCulture,
            $"Baseline for this server: {baseline:N1} per minute (p99.9 of its 3-minute average over the last {AttentionBaselineMath.LookbackDays} days).");
        return scaled >= floor
            ? string.Create(CultureInfo.InvariantCulture, $"{lead} {tier} is {multiple} x baseline = {finding.BarCrossed} per minute.")
            : string.Create(CultureInfo.InvariantCulture, $"{lead} {multiple} x baseline is {scaled:N1}, under the floor, so the {tier} bar is the floor, {floor} per minute.");
    }

    /// <summary>The numbers the bars of one grading came from (#5736).</summary>
    /// <param name="Baseline">The server's baseline, or null when it has under a day of capture.</param>
    /// <param name="WarningFloor">The Warning floor setting.</param>
    /// <param name="CriticalFloor">The Critical floor setting.</param>
    /// <param name="WarningMultiple">The Warning multiple.</param>
    /// <param name="CriticalMultiple">The Critical multiple.</param>
    public sealed record BarBasis(double? Baseline, int WarningFloor, int CriticalFloor, int WarningMultiple, int CriticalMultiple);

    /// <summary>One reading over a bar.</summary>
    /// <param name="Severity">Warning, or Critical when the rate reached the Critical bar.</param>
    /// <param name="RatePerMinute">The rate the grade rests on, two decimals: the unmuted (judged) rate, or the exact rate for a Muted finding.</param>
    /// <param name="BarCrossed">The bar of <paramref name="Severity"/>: the threshold the alert row stores.</param>
    /// <param name="TotalCount">The exact attentions in the span.</param>
    /// <param name="JudgedCount">The count after the mute proportion (equal to <paramref name="TotalCount"/> when nothing was muted).</param>
    /// <param name="CoveredMinutes">The span, in minutes.</param>
    /// <param name="RunCount">Successful runs the span takes.</param>
    /// <param name="SampledEvents">Sampled events in the span.</param>
    /// <param name="Estimated">True when the mute proportion changed the count: the judged count is an estimate from the samples.</param>
    /// <param name="Muted">True when the exact count crossed a bar and the judged count did not: record the fire as Muted.</param>
    /// <param name="NewestRunTime">The newest run's time (naive UTC).</param>
    /// <param name="Bars">Where the bars came from (#5736), null when the grading used fixed bars only.</param>
    /// <param name="ExcludedSamples">Sampled events in the span from SSMS IntelliSense and the VS Code mssql metadata connections (<see cref="IsExcludedApp"/>), left out of the judged count (#6552, #6856).</param>
    /// <param name="ExcludedSsmsSamples">The part of <paramref name="ExcludedSamples"/> from SSMS IntelliSense (#6856).</param>
    /// <param name="ExcludedVsCodeSamples">The part of <paramref name="ExcludedSamples"/> from the VS Code mssql extension's metadata connections (#6856).</param>
    /// <param name="ShortSamples">Sampled events in the span under the 1 ms duration floor (<see cref="AttentionWindowSql.MinInterruptDurationUs"/>), left out of the judged count (#7061).</param>
    /// <param name="AzdataMetadataSamples">Sampled events in the span that are Azure Data Studio metadata queries (see <see cref="AttentionWindowSql.AzdataMetadataPredicate"/>), left out of the judged count (#7061).</param>
    public sealed record Finding(
        AlertSeverityLevel Severity,
        double RatePerMinute,
        int BarCrossed,
        long TotalCount,
        double JudgedCount,
        double CoveredMinutes,
        int RunCount,
        long SampledEvents,
        bool Estimated,
        bool Muted,
        DateTime NewestRunTime,
        BarBasis? Bars = null,
        long ExcludedSamples = 0,
        long ExcludedSsmsSamples = 0,
        long ExcludedVsCodeSamples = 0,
        long ShortSamples = 0,
        long AzdataMetadataSamples = 0)
    {
        /// <summary>The summary sentence for the alert.</summary>
        public string Summary => AttentionEvaluator.Summary(this);

        /// <summary>The "current value" text the alert row shows.</summary>
        public string CurrentValueText =>
            string.Create(CultureInfo.InvariantCulture, $"{RatePerMinute:N1} per minute");

        /// <summary>The "threshold" text the alert row shows.</summary>
        public string ThresholdText =>
            string.Create(CultureInfo.InvariantCulture, $"{BarCrossed} per minute");
    }
}
