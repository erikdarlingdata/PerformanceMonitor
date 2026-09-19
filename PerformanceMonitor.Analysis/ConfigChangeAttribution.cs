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

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The <c>CONFIG_CHANGED</c> analysis finding (#3653 A10, ruling Q2): when a server configuration setting
/// changed inside the pass window, compare the four hours before the change with the four hours after it
/// and SAY what moved — in the server's own dispersion units where a baseline exists, on the scorer's
/// ladder otherwise — or say that nothing did. Shared by both SKUs so Lite and Darling attribute the same
/// change the same way; each service reads its own store's <c>server_config</c> snapshots, diffs them
/// through the shared <c>ConfigChangeDiff</c>, calls its own <c>ComparePeriodsAsync</c>, and hands the
/// results here. This assembly stays a leaf (no project references — <c>PerformanceMonitor.Common</c> would
/// bring the MCP SDK and the credential package with it), so the diff's records are mapped onto
/// <see cref="SettingChange"/> at the call site rather than consumed here.
///
/// <para><b>The gap this closes.</b> Every prior reference to configuration changes in the product was a
/// history READ (<c>get_server_config_changes</c>, the Configuration Changes tabs): the analysis engine
/// scored the CURRENT value of seven settings and never once asked whether a change had a consequence.
/// <c>ForcePlanSelfReview</c> already proves the verify-after shape in-repo for forced plans; this is the
/// same shape for <c>sp_configure</c>. The engine review called it "the single biggest credibility upgrade
/// available", because an operator who changed MAXDOP at 14:02 and sees CPU fall at 14:10 today has to
/// join two tools by hand to say so.</para>
///
/// <para><b>There is no change table.</b> The issue text speaks of <c>server_config_changes</c> rows; no such
/// table exists on either SKU. <c>server_config</c> is an APPEND-ONLY snapshot table that the config
/// collector fills ON CONNECT (<c>CollectorScheduleDefaults["server_config"]</c> is frequency 0, both
/// SKUs), and a "change" is what <c>ConfigChangeDiff.DiffServerConfigChanges</c> derives from two
/// consecutive snapshots. Two consequences are load-bearing and both are disclosed on the fact:</para>
/// <list type="bullet">
///   <item><description>The change TIME is the capture that first OBSERVED the new value, not the moment
///   <c>RECONFIGURE</c> ran. The real change landed somewhere between the previous capture and this one —
///   <see cref="MetaObservationGapHours"/> is that span, and when it exceeds the before-window the "before"
///   half may already include the new value. The finding says "first observed at", never "changed at".</description></item>
///   <item><description>Every setting that changed between the same two captures shares ONE change time,
///   so they share ONE compare — the data cannot attribute an outcome to one of two settings that were
///   observed together, and this class does not pretend to. One fact per change EVENT (capture), naming
///   every setting that moved at it.</description></item>
/// </list>
///
/// <para><b>Why one fact, keyed exactly <see cref="FactKey"/>.</b> The engine is one-fact-per-key end to
/// end: <see cref="FactCollectionExtensions.ToFactLookup"/> keeps the first fact per key,
/// <c>InferenceEngine.BuildStories</c> marks a key consumed after its first story, and
/// <c>FactAdvice.PopulateStoryText</c> composes from the same lookup. Two same-key facts would silently
/// lose the second. So when several change events fall inside one pass window — two reconnects with
/// changes between them inside four hours, which the connect cadence makes rare — the MOST RECENT event is
/// the fact's subject and the earlier ones are counted in <see cref="MetaEarlierEventsInWindow"/>; their
/// own ±4 h compares belonged to the passes that ran while they were the most recent. (A per-setting key
/// suffix, <c>BAD_ACTOR_&lt;hash&gt;</c>-style, was considered and rejected for slice one: the ruling names
/// the key, and the shared compare makes per-setting facts carry identical numbers.)</para>
///
/// <para><b>Why it never meets the scorer.</b> <c>FactScorer.ScoreConfigFact</c> returns 0 for a
/// <c>"config"</c> key it does not know, and <c>ScoreAll</c> zeroes the severity of every base-0 fact — a
/// fact appended BEFORE scoring would be dropped from the working set. The services therefore append this
/// fact AFTER <c>_scorer.ScoreAll(facts)</c> with <see cref="Fact.BaseSeverity"/> and
/// <see cref="Fact.Severity"/> already set to <see cref="InformationSeverity"/>, and
/// <c>InferenceEngine.ConfigAdvisoryRootKeys</c> lists the key so a sub-0.5 fact roots its own story. The
/// scorer file is untouched by design (#3691's lane owns it).</para>
///
/// <para><b>Rooted at Information, which attributes and does not accuse.</b> Both SKUs' readers band a
/// finding's severity identically (<c>LiteRecommendationsReader.SeverityBand</c>,
/// <c>ViewerDataService.SeverityBand</c>): <c>&gt;= 1.5</c> CRITICAL, <c>&gt;= 0.75</c> WARNING, else INFO.
/// <see cref="InformationSeverity"/> sits inside INFO and BELOW every standing-misconfiguration advisory
/// (the 0.4 <c>CONFIG_*</c> base, the 0.3 <c>FILE_AUTOGROWTH_PERCENT</c> base), so a change that had no
/// effect sorts under a setting that is wrong today; above 0 so it roots, persists, recurs and mutes like
/// any other finding. <c>AnalysisNotificationService</c> filters <c>Severity &gt;= AnalysisNotifySeverity</c>
/// before grouping, so this finding never e-mails — intended.</para>
///
/// <para><b>What it does not do.</b> It is not a causal test: one window against one window cannot show
/// that a change caused anything (the compare tool's own description says so, citing the OtterTune field
/// study's 4× DB-time variance on an unchanged configuration), and the remediation prose repeats it. It does
/// not fold into an incident — a configuration change is context for whatever else the pass found, not a
/// symptom of it — and <c>AnomalyIncidentReconciler</c> is not touched. It does not cover
/// <c>database_config</c> or <c>trace_flags</c> yet (slice two). It does not reach
/// <c>get_analysis_facts</c>, which runs the fenced <c>CollectAndScoreFactsAsync</c> and not the pass.</para>
/// </summary>
public static class ConfigChangeAttribution
{
    /// <summary>The fact key and the finding's root key. Named by the Q2 ruling.</summary>
    public const string FactKey = "CONFIG_CHANGED";

    /// <summary>The existing <c>"config"</c> source — <c>FactScorer.KnownSources</c> is a census of every
    /// <c>Source = "..."</c> literal in the three analysis assemblies, and a new spelling would fail it.</summary>
    public const string FactSource = "config";

    /// <summary>
    /// The Information root. 0.25: inside the readers' INFO band (below 0.75), below the 0.4 config-advisory
    /// and 0.3 autogrowth bases so an attribution never outranks a standing misconfiguration, above 0 so the
    /// story roots and the mute filter (<c>story.Severity &lt;= 0</c> is skipped) keeps it. Not a measured
    /// number — a position in an ordering, stated so the ordering is the thing reviewed.
    /// </summary>
    public const double InformationSeverity = 0.25;

    /// <summary>Hours on each side of the observed change. The ruling's ±4 h, and the pass's own window length.</summary>
    public const int CompareWindowHours = 4;

    /// <summary>
    /// Joins the changed setting names into <see cref="Fact.ObjectName"/> — the one string slot a fact
    /// has, since <see cref="Fact.Metadata"/> is doubles-only by contract. No <c>sys.configurations</c>
    /// name contains a semicolon, so the composer can split on it.
    /// </summary>
    public const string SettingSeparator = "; ";

    /// <summary>
    /// Per-key metadata beyond this many moved rows is dropped and counted in <see cref="MetaMovedKeysOmitted"/>.
    /// The banding orders rows worst-first, so what survives is the head of the verdict list.
    /// </summary>
    public const int MaxMovedKeysInMetadata = 12;

    /* ── Metadata keys (doubles). Shared with the composer and the tests so the spelling lives once. ── */

    /// <summary>How many settings changed at this event (also the fact's Value).</summary>
    public const string MetaChangedSettings = "changed_settings";
    /// <summary>The observed change time as Unix seconds — a DateTime cannot ride in a double map.</summary>
    public const string MetaChangeTimeUnix = "change_time_unix";
    /// <summary>Hours from the previous config capture to the one that observed the change: the span the
    /// real change landed in.</summary>
    public const string MetaObservationGapHours = "observation_gap_hours";
    /// <summary>Nominal hours in the before half (always <see cref="CompareWindowHours"/>).</summary>
    public const string MetaBeforeHours = "before_hours";
    /// <summary>Hours of the after half that exist yet: <c>min(4, observedThrough − changeTime)</c>.</summary>
    public const string MetaAfterHoursObserved = "after_hours_observed";
    /// <summary>1 when the after half was clamped to the pass end — the change is still less than 4 h old.</summary>
    public const string MetaAfterWindowClamped = "after_window_clamped";
    /// <summary>The collector's observed fraction of the before half (<see cref="WindowCoverage.Fraction"/>).</summary>
    public const string MetaBeforeCoverageFraction = "before_coverage_fraction";
    /// <summary>The collector's observed fraction of the after half as it exists so far.</summary>
    public const string MetaAfterCoverageFraction = "after_coverage_fraction";
    /// <summary>Other change events inside the pass window that this fact does NOT compare (see the class remarks).</summary>
    public const string MetaEarlierEventsInWindow = "earlier_change_events_in_window";
    /// <summary>1 when the compare could not run (both collections threw); the fact still records the change.</summary>
    public const string MetaCompareUnavailable = "compare_unavailable";
    /// <summary>Keys the compare banded (present on either side).</summary>
    public const string MetaComparedKeys = "compared_keys";
    public const string MetaWorse = "worse";
    public const string MetaBetter = "better";
    public const string MetaStable = "stable";
    /// <summary>Moved rows beyond <see cref="MaxMovedKeysInMetadata"/> that carry no per-key entry.</summary>
    public const string MetaMovedKeysOmitted = "moved_keys_omitted";

    /// <summary>Per-setting old/new values. <c>|</c> separates the setting from the field; no setting name contains it.</summary>
    public static string OldInUseKey(string setting) => $"{setting}|old_value_in_use";
    public static string NewInUseKey(string setting) => $"{setting}|new_value_in_use";
    public static string OldConfiguredKey(string setting) => $"{setting}|old_value_configured";
    public static string NewConfiguredKey(string setting) => $"{setting}|new_value_configured";
    /// <summary>1 when the setting is non-dynamic and its configured value has not yet reached in-use
    /// (<c>ConfigChangeDiff.ServerConfigChange.RequiresRestart</c>) — the change has not happened to the
    /// engine yet, and nothing SHOULD have moved.</summary>
    public static string RequiresRestartKey(string setting) => $"{setting}|requires_restart";

    /// <summary>Per-moved-key verdict: +1 worse, −1 better. Only non-stable rows get entries.</summary>
    public static string StatusKey(string factKey) => $"{factKey}|status";
    /// <summary>The value delta in the bucket's robust sigma — present only for baseline-banded rows.</summary>
    public static string DeltaSigmaKey(string factKey) => $"{factKey}|delta_sigma";
    /// <summary>|after − before| over the larger side, 0..1 (1 for a one-sided row).</summary>
    public static string RelativeMoveKey(string factKey) => $"{factKey}|relative_move";
    /// <summary>after − before in the key's own unit — present only when both sides had the key.</summary>
    public static string ValueDeltaKey(string factKey) => $"{factKey}|value_delta";

    private const string StatusSuffix = "|status";

    /// <summary>
    /// One setting's move between two consecutive captures — <c>ConfigChangeDiff.ServerConfigChange</c>
    /// without the change time (which is the event's) and without the display strings. Mapped at the
    /// service, one line of LINQ, so this assembly need not reference the diff's home.
    /// <see cref="RequiresRestart"/> is the diff's derivation (non-dynamic AND configured ≠ in-use): the
    /// configured value moved but the engine is still running the old one.
    /// </summary>
    public sealed record SettingChange(
        string Name,
        long? OldValueConfigured,
        long? NewValueConfigured,
        long? OldValueInUse,
        long? NewValueInUse,
        bool RequiresRestart)
    {
        /// <summary>True when the value the engine actually runs on moved — the case a compare can speak to.</summary>
        public bool InUseMoved => OldValueInUse != NewValueInUse;
    }

    /// <summary>
    /// One config capture at which one or more settings were first seen with a new value.
    /// <see cref="PreviousCaptureTime"/> is the capture before it — the other edge of the span the real
    /// change landed in.
    /// </summary>
    public sealed record ChangeEvent(
        DateTime ChangeTime,
        DateTime PreviousCaptureTime,
        IReadOnlyList<SettingChange> Changes)
    {
        public TimeSpan ObservationGap => ChangeTime - PreviousCaptureTime;
    }

    /// <summary>The two halves of the compare, in the order <c>ComparePeriodsAsync</c> takes them.</summary>
    public sealed record CompareWindows(
        DateTime BeforeStart, DateTime BeforeEnd, DateTime AfterStart, DateTime AfterEnd, bool AfterClamped)
    {
        public double AfterHoursObserved => Math.Max(0, (AfterEnd - AfterStart).TotalHours);
    }

    /// <summary>
    /// Groups already-diffed changes (each tagged with the capture time that observed it) into change
    /// events, NEWEST FIRST. The caller runs <c>ConfigChangeDiff.DiffServerConfigChanges(snapshots,
    /// windowStart, windowEnd)</c> — so the window rule is the diff's, the same one every history reader
    /// applies — and its snapshot read must include the last capture BEFORE the window (the diff baseline)
    /// or a change on the first in-window capture is invisible.
    ///
    /// <para><paramref name="captureTimes"/> are every capture the snapshots held (duplicates fine): the
    /// previous capture time is derived from them rather than carried on the change record, because every
    /// configuration row is present in every capture, so the capture immediately before the event's is
    /// the same for every setting in it.</para>
    /// </summary>
    public static List<ChangeEvent> GroupIntoEvents(
        IEnumerable<(DateTime ChangeTime, SettingChange Change)> changes, IEnumerable<DateTime> captureTimes)
    {
        ArgumentNullException.ThrowIfNull(changes);
        ArgumentNullException.ThrowIfNull(captureTimes);

        var changeList = changes.ToList();
        if (changeList.Count == 0)
            return [];

        var captures = captureTimes.Distinct().OrderBy(t => t).ToList();

        return changeList
            .GroupBy(c => c.ChangeTime)
            .Select(g =>
            {
                var previous = captures.LastOrDefault(t => t < g.Key);
                /* A change record exists only because a previous capture existed, so this arm cannot fire on
                   the diff's own output; it guards a caller that hands a change list from elsewhere. A gap of
                   zero then reads as "unknown", and the composer says nothing about the span. */
                if (previous == default)
                    previous = g.Key;
                return new ChangeEvent(
                    g.Key,
                    previous,
                    g.Select(c => c.Change).OrderBy(c => c.Name, StringComparer.Ordinal).ToList());
            })
            .OrderByDescending(e => e.ChangeTime)
            .ToList();
    }

    /// <summary>
    /// The ±<see cref="CompareWindowHours"/> h halves around <paramref name="changeTime"/>, the after half
    /// clamped to <paramref name="observedThrough"/> — the pass's window end, which is "now" for a scheduled
    /// pass and the anchor for an anchored one (#2506), so an anchored pass never compares against rows it
    /// is not supposed to see. <see cref="CompareWindows.AfterClamped"/> is true when the clamp bit: the
    /// after half is an honest partial and the fact says so, rather than a fabricated full four hours.
    ///
    /// <para>Both halves share the boundary instant. The collectors read <c>collection_time &gt;= start AND
    /// &lt;= end</c>, so a row stamped at exactly the change time would land in both; the change time is a
    /// config capture's own stamp and the wait rows carry their own, so a same-millisecond coincidence is
    /// not a case worth an off-by-one-tick on the public API.</para>
    /// </summary>
    public static CompareWindows WindowsFor(DateTime changeTime, DateTime observedThrough)
    {
        var nominalAfterEnd = changeTime.AddHours(CompareWindowHours);
        var clamped = observedThrough < nominalAfterEnd;
        var afterEnd = clamped ? observedThrough : nominalAfterEnd;
        if (afterEnd < changeTime)
            afterEnd = changeTime;
        return new CompareWindows(
            changeTime.AddHours(-CompareWindowHours), changeTime,
            changeTime, afterEnd,
            clamped);
    }

    /// <summary>
    /// The coverage caveat the compare is flagged with — the same rule <c>compare_analysis</c> applies on
    /// both SKUs: a side that was partly observed or unobserved flags every verdict row. A null coverage
    /// (collection threw) is not a caveat here; it is <see cref="MetaCompareUnavailable"/>.
    /// </summary>
    public static bool CoverageCaveatFor(WindowCoverage? before, WindowCoverage? after) =>
        (before is not null && (before.IsPartial || !before.IsObserved))
        || (after is not null && (after.IsPartial || !after.IsObserved));

    /// <summary>
    /// Builds the fact. <paramref name="compare"/> is null when the compare could not run at all (both
    /// collections threw, so both coverages are null and both fact lists are empty); an EMPTY compare over
    /// observed windows is not null — it is the "nothing moved" answer, and that is a finding too.
    /// </summary>
    public static Fact BuildFact(
        int serverId,
        ChangeEvent change,
        int earlierEventsInWindow,
        CompareWindows windows,
        ComparisonResult? compare,
        WindowCoverage? beforeCoverage,
        WindowCoverage? afterCoverage)
    {
        ArgumentNullException.ThrowIfNull(change);
        ArgumentNullException.ThrowIfNull(windows);

        var metadata = new Dictionary<string, double>(StringComparer.Ordinal)
        {
            [MetaChangedSettings] = change.Changes.Count,
            [MetaChangeTimeUnix] = new DateTimeOffset(DateTime.SpecifyKind(change.ChangeTime, DateTimeKind.Utc)).ToUnixTimeSeconds(),
            [MetaObservationGapHours] = Math.Max(0, change.ObservationGap.TotalHours),
            [MetaBeforeHours] = CompareWindowHours,
            [MetaAfterHoursObserved] = windows.AfterHoursObserved,
            [MetaAfterWindowClamped] = windows.AfterClamped ? 1 : 0,
            [MetaEarlierEventsInWindow] = Math.Max(0, earlierEventsInWindow),
            [MetaCompareUnavailable] = compare is null ? 1 : 0,
        };

        if (beforeCoverage is not null)
            metadata[MetaBeforeCoverageFraction] = beforeCoverage.Fraction;
        if (afterCoverage is not null)
            metadata[MetaAfterCoverageFraction] = afterCoverage.Fraction;

        foreach (var c in change.Changes)
        {
            if (c.OldValueInUse is { } oldInUse) metadata[OldInUseKey(c.Name)] = oldInUse;
            if (c.NewValueInUse is { } newInUse) metadata[NewInUseKey(c.Name)] = newInUse;
            if (c.OldValueConfigured is { } oldCfg) metadata[OldConfiguredKey(c.Name)] = oldCfg;
            if (c.NewValueConfigured is { } newCfg) metadata[NewConfiguredKey(c.Name)] = newCfg;
            metadata[RequiresRestartKey(c.Name)] = c.RequiresRestart ? 1 : 0;
        }

        if (compare is not null)
        {
            metadata[MetaComparedKeys] = compare.Rows.Count;
            metadata[MetaWorse] = compare.Worse;
            metadata[MetaBetter] = compare.Better;
            metadata[MetaStable] = compare.Stable;

            /* Rows arrive worse → better → stable, larger move first (ComparisonBanding.Compare). */
            var moved = compare.Rows.Where(r => r.Status != ComparisonBanding.StatusStable).ToList();
            foreach (var row in moved.Take(MaxMovedKeysInMetadata))
            {
                metadata[StatusKey(row.Key)] = row.Status == ComparisonBanding.StatusWorse ? 1 : -1;
                if (row.RelativeMove is { } rel) metadata[RelativeMoveKey(row.Key)] = rel;
                if (row.DeltaSigma is { } sigma) metadata[DeltaSigmaKey(row.Key)] = sigma;
                if (row.ValueDelta is { } delta) metadata[ValueDeltaKey(row.Key)] = delta;
            }
            metadata[MetaMovedKeysOmitted] = Math.Max(0, moved.Count - MaxMovedKeysInMetadata);
        }

        return new Fact
        {
            Source = FactSource,
            Key = FactKey,
            Value = change.Changes.Count,
            ServerId = serverId,
            ObjectName = string.Join(SettingSeparator, change.Changes.Select(c => c.Name)),
            /* Preset, never scored: see the class remarks. Both fields, because the mute filter and the
               story reader read Severity while ComparisonBanding's ladder reads BaseSeverity. */
            BaseSeverity = InformationSeverity,
            Severity = InformationSeverity,
            Metadata = metadata
        };
    }

    /// <summary>The setting names a fact's ObjectName carries, in the order they were joined.</summary>
    public static IReadOnlyList<string> SettingNames(Fact fact) =>
        string.IsNullOrEmpty(fact?.ObjectName)
            ? []
            : fact.ObjectName.Split(SettingSeparator, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    /// <summary>The compared keys that moved, read back from the fact's metadata. The banding's own row order
    /// is not recoverable from a dictionary, so this re-derives the same rule: worse first, then better, the
    /// larger relative move first within each, then by key.</summary>
    public static IReadOnlyList<string> MovedKeys(Fact fact)
    {
        if (fact?.Metadata is null)
            return [];
        return fact.Metadata.Keys
            .Where(k => k.EndsWith(StatusSuffix, StringComparison.Ordinal))
            .Select(k => k[..^StatusSuffix.Length])
            .OrderBy(k => fact.Metadata[StatusKey(k)] > 0 ? 0 : 1)
            .ThenByDescending(k => fact.Metadata.GetValueOrDefault(RelativeMoveKey(k)))
            .ThenBy(k => k, StringComparer.Ordinal)
            .ToList();
    }
}
