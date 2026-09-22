/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3653 A10 (Q2): the <c>CONFIG_CHANGED</c> finding, pinned on the shared <see cref="ConfigChangeAttribution"/>
/// both SKUs build the fact through, the composer that freezes its prose, and the two engine seams it
/// depends on — the InferenceEngine root rule that lets a 0.25 fact become a story, and the scorer ordering
/// that forces the fact to be appended AFTER <c>ScoreAll</c>. The pipeline wiring (a snapshot pair in the
/// window yields the finding; one outside does not; the after half is disclosed as partial) is in
/// <see cref="ConfigChangeAttributionPipelineTests"/> against a real DuckDB store.
///
/// <para>The events here are built from real <see cref="ConfigChangeDiff"/> output over planted snapshots,
/// mapped exactly as the services map them, so a change in the diff's record shape breaks these pins
/// rather than passing them over.</para>
///
/// <para>#3740 adds the trace anchor: the default trace's sp_configure line (msg 15457) as the source of
/// WHEN, joined to the diffed change by <see cref="ConfigChangeAttribution.ResolveTraceAnchor"/>. The
/// trace lines here carry the TextData shape measured on SQL Server 2022 — the raw error-log line, with
/// its timestamp and spid prefix — so the parser is pinned against what the store actually holds. The
/// Darling twin of the pure pins is <c>Darling.Tests/ConfigChangeTraceAnchorTests</c>; the DuckDB pipeline
/// arms are in <see cref="ConfigChangeAttributionPipelineTests"/>.</para>
///
/// <para>Slice two (#3653 A10) extends the fact to the <c>database_config</c> and <c>trace_flags</c> families
/// through the same key, the same compare and the same grammar. The pins below the trace-anchor block are
/// built from real <see cref="ConfigChangeDiff.DiffDatabaseConfigChanges"/> / <see cref="ConfigChangeDiff.DiffTraceFlagChanges"/>
/// output mapped exactly as the services map it, and cover: the per-family events, the same-connect fold
/// across families (and its two refusals), the fact's family bits, database seam and segment grammar, the
/// trace anchor's indifference to non-server changes, each family's prose, the status-column exclusion, and
/// the widened <c>next_tools</c> row. The server family's pins above are untouched: its fact, its ObjectName and
/// its prose are byte-identical to slice one's.</para>
/// </summary>
public sealed class ConfigChangeAttributionTests
{
    private static readonly DateTime T0 = new(2026, 9, 18, 14, 0, 0, DateTimeKind.Utc);
    private static readonly IReadOnlyDictionary<string, BaselineBucket> NoDispersion = new Dictionary<string, BaselineBucket>();
    private const string Maxdop = "max degree of parallelism";
    private const string Ctfp = "cost threshold for parallelism";

    /* ── the change events ── */

    /// <summary>
    /// Two settings observed changed on the SAME capture are ONE event: they share the observation time,
    /// so they share the compare (the data cannot say which of the two moved a metric). The previous
    /// capture is the other edge of the span the real change landed in.
    /// </summary>
    [Fact]
    public void GroupIntoEvents_TwoSettingsOnOneCapture_AreOneEvent_WithThePreviousCapture()
    {
        var previous = T0.AddHours(-23);
        var snapshots = Snapshots(
            (previous, "max degree of parallelism", 0, 0),
            (previous, "cost threshold for parallelism", 5, 5),
            (previous, "max server memory (MB)", 2147483647, 2147483647),
            (T0, "max degree of parallelism", 8, 8),
            (T0, "cost threshold for parallelism", 50, 50),
            (T0, "max server memory (MB)", 2147483647, 2147483647));

        var events = Events(snapshots, T0.AddHours(-4), T0.AddHours(1));

        var evt = Assert.Single(events);
        Assert.Equal(T0, evt.ChangeTime);
        Assert.Equal(previous, evt.PreviousCaptureTime);
        Assert.Equal(23.0, evt.ObservationGap.TotalHours, precision: 6);
        Assert.Equal(new[] { "cost threshold for parallelism", "max degree of parallelism" }, evt.Changes.Select(c => c.Name).ToArray());
        var maxdop = evt.Changes.Single(c => c.Name == "max degree of parallelism");
        Assert.Equal(0L, maxdop.OldValueInUse);
        Assert.Equal(8L, maxdop.NewValueInUse);
        Assert.True(maxdop.InUseMoved);
        Assert.False(maxdop.RequiresRestart);
    }

    /// <summary>Two captures with changes inside the window are two events, newest first — the pass takes [0].</summary>
    [Fact]
    public void GroupIntoEvents_TwoCaptures_AreTwoEvents_NewestFirst()
    {
        var snapshots = Snapshots(
            (T0.AddHours(-6), "max degree of parallelism", 0, 0),
            (T0.AddHours(-2), "max degree of parallelism", 4, 4),
            (T0, "max degree of parallelism", 8, 8));

        var events = Events(snapshots, T0.AddHours(-4), T0.AddHours(1));

        Assert.Equal(2, events.Count);
        Assert.Equal(T0, events[0].ChangeTime);
        Assert.Equal(T0.AddHours(-2), events[0].PreviousCaptureTime);
        Assert.Equal(T0.AddHours(-2), events[1].ChangeTime);
        Assert.Equal(T0.AddHours(-6), events[1].PreviousCaptureTime);
    }

    /// <summary>
    /// The window rule is the diff's: a change first observed BEFORE the window start is not an event for
    /// this pass, even though its snapshot is the baseline that makes an in-window change detectable.
    /// </summary>
    [Fact]
    public void GroupIntoEvents_AChangeObservedBeforeTheWindow_IsNotAnEvent()
    {
        var snapshots = Snapshots(
            (T0.AddHours(-30), "max degree of parallelism", 0, 0),
            (T0.AddHours(-10), "max degree of parallelism", 8, 8));

        Assert.Empty(Events(snapshots, T0.AddHours(-4), T0));
        Assert.Empty(ConfigChangeAttribution.GroupIntoEvents([], []));
    }

    /* ── the compare windows ── */

    /// <summary>
    /// A change one hour old has one hour of "after"; the window says so (<c>AfterClamped</c>) instead of
    /// pretending to four. A change five hours old has the full four. An anchored pass clamps to the
    /// anchor, never to the clock.
    /// </summary>
    [Fact]
    public void WindowsFor_ClampsTheAfterHalfToThePassEnd_AndSaysSo()
    {
        var young = ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(1));
        Assert.Equal(T0.AddHours(-4), young.BeforeStart);
        Assert.Equal(T0, young.BeforeEnd);
        Assert.Equal(T0, young.AfterStart);
        Assert.Equal(T0.AddHours(1), young.AfterEnd);
        Assert.True(young.AfterClamped);
        Assert.Equal(1.0, young.AfterHoursObserved, precision: 6);

        var old = ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(5));
        Assert.Equal(T0.AddHours(4), old.AfterEnd);
        Assert.False(old.AfterClamped);
        Assert.Equal(4.0, old.AfterHoursObserved, precision: 6);

        /* Exactly four hours old is not a clamp: nothing was cut. */
        Assert.False(ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4)).AfterClamped);
    }

    /* ── the fact ── */

    /// <summary>
    /// The fact's shape: the ruling's key, the registered <c>"config"</c> source, ObjectName naming the
    /// setting, Value the count, BOTH severity fields preset to the Information root, and the compare's
    /// verdict rows written per key — CPU banded in sigma against a trustworthy bucket, the wait on the
    /// ladder — with stable rows counted, not listed.
    /// </summary>
    [Fact]
    public void BuildFact_CarriesTheChange_TheInformationRoot_AndTheBandedDeltas()
    {
        var evt = MaxdopEvent(T0, T0.AddHours(-23));
        var windows = ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4));

        /* CPU falls 20 points against a tight, trustworthy baseline (sigma from Mad 2 → ~3): far beyond ±1σ,
           better. PAGEIOLATCH_SH doubles on a saturated ladder: worse by the absolute rule. LCK barely moves: stable. */
        var (before, after) = Scored(
            [Cpu(70), Wait("PAGEIOLATCH_SH", 0.30), Wait("LCK", 0.05)],
            [Cpu(50), Wait("PAGEIOLATCH_SH", 0.60), Wait("LCK", 0.052)]);
        var dispersion = new Dictionary<string, BaselineBucket> { [MetricNames.Cpu] = TrustworthyCpuBucket() };
        var compare = ComparisonBanding.Compare(before, after, dispersion, coverageCaveat: false);
        Assert.Equal(1, compare.Worse);
        Assert.Equal(1, compare.Better);
        Assert.Equal(1, compare.Stable);

        var fact = ConfigChangeAttribution.BuildFact(7, evt, earlierEventsInWindow: 0, windows, compare, FullCoverage(), FullCoverage());

        Assert.Equal("CONFIG_CHANGED", fact.Key);
        Assert.Equal(ConfigChangeAttribution.FactKey, fact.Key);
        Assert.Equal("config", fact.Source);
        Assert.Contains(fact.Source, FactScorer.KnownSources);
        Assert.Equal(7, fact.ServerId);
        Assert.Equal("max degree of parallelism", fact.ObjectName);
        Assert.Equal(1, fact.Value);
        Assert.Equal(0.25, ConfigChangeAttribution.InformationSeverity);
        Assert.Equal(ConfigChangeAttribution.InformationSeverity, fact.BaseSeverity);
        Assert.Equal(ConfigChangeAttribution.InformationSeverity, fact.Severity);

        var m = fact.Metadata;
        Assert.Equal(1, m[ConfigChangeAttribution.MetaChangedSettings]);
        Assert.Equal(new DateTimeOffset(T0).ToUnixTimeSeconds(), m[ConfigChangeAttribution.MetaChangeTimeUnix]);
        Assert.Equal(23.0, m[ConfigChangeAttribution.MetaObservationGapHours], precision: 6);
        Assert.Equal(4, m[ConfigChangeAttribution.MetaBeforeHours]);
        Assert.Equal(4.0, m[ConfigChangeAttribution.MetaAfterHoursObserved], precision: 6);
        Assert.Equal(0, m[ConfigChangeAttribution.MetaAfterWindowClamped]);
        Assert.Equal(0, m[ConfigChangeAttribution.MetaCompareUnavailable]);
        Assert.Equal(0, m[ConfigChangeAttribution.MetaEarlierEventsInWindow]);
        Assert.Equal(1.0, m[ConfigChangeAttribution.MetaBeforeCoverageFraction]);
        Assert.Equal(1.0, m[ConfigChangeAttribution.MetaAfterCoverageFraction]);
        Assert.Equal(0, m[ConfigChangeAttribution.OldInUseKey("max degree of parallelism")]);
        Assert.Equal(8, m[ConfigChangeAttribution.NewInUseKey("max degree of parallelism")]);
        Assert.Equal(0, m[ConfigChangeAttribution.RequiresRestartKey("max degree of parallelism")]);

        Assert.Equal(3, m[ConfigChangeAttribution.MetaComparedKeys]);
        Assert.Equal(1, m[ConfigChangeAttribution.MetaWorse]);
        Assert.Equal(1, m[ConfigChangeAttribution.MetaBetter]);
        Assert.Equal(1, m[ConfigChangeAttribution.MetaStable]);
        Assert.Equal(0, m[ConfigChangeAttribution.MetaMovedKeysOmitted]);

        /* Per-key rows for the two that moved; none for the stable one. */
        Assert.Equal(-1, m[ConfigChangeAttribution.StatusKey("CPU_SQL_PERCENT")]);
        Assert.True(m[ConfigChangeAttribution.DeltaSigmaKey("CPU_SQL_PERCENT")] < -1.0, "a 20-point CPU fall against a ~3-point sigma is well past −1σ");
        Assert.Equal(-20.0, m[ConfigChangeAttribution.ValueDeltaKey("CPU_SQL_PERCENT")], precision: 6);
        Assert.Equal(1, m[ConfigChangeAttribution.StatusKey("PAGEIOLATCH_SH")]);
        Assert.False(m.ContainsKey(ConfigChangeAttribution.DeltaSigmaKey("PAGEIOLATCH_SH")), "waits have no per-type baseline: no sigma");
        Assert.Equal(0.5, m[ConfigChangeAttribution.RelativeMoveKey("PAGEIOLATCH_SH")], precision: 6);
        Assert.False(m.ContainsKey(ConfigChangeAttribution.StatusKey("LCK")));

        /* Worse first, then better. */
        Assert.Equal(new[] { "PAGEIOLATCH_SH", "CPU_SQL_PERCENT" }, ConfigChangeAttribution.MovedKeys(fact));
        Assert.Equal(new[] { "max degree of parallelism" }, ConfigChangeAttribution.SettingNames(fact));
    }

    /// <summary>
    /// A null compare (both collections threw) is recorded as unavailable — the change is still a fact —
    /// while an EMPTY compare over observed windows is the "nothing moved" answer with zero verdict rows.
    /// The two must not read alike.
    /// </summary>
    [Fact]
    public void BuildFact_DistinguishesNoCompare_FromNothingMoved()
    {
        var evt = MaxdopEvent(T0, T0.AddHours(-23));
        var windows = ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4));

        var unavailable = ConfigChangeAttribution.BuildFact(1, evt, 0, windows, compare: null, beforeCoverage: null, afterCoverage: null);
        Assert.Equal(1, unavailable.Metadata[ConfigChangeAttribution.MetaCompareUnavailable]);
        Assert.False(unavailable.Metadata.ContainsKey(ConfigChangeAttribution.MetaComparedKeys));
        Assert.False(unavailable.Metadata.ContainsKey(ConfigChangeAttribution.MetaBeforeCoverageFraction));

        var (before, after) = Scored([Cpu(50)], [Cpu(51)]);
        var nothingMoved = ConfigChangeAttribution.BuildFact(1, evt, 0, windows,
            ComparisonBanding.Compare(before, after, NoDispersion, coverageCaveat: false), FullCoverage(), FullCoverage());
        Assert.Equal(0, nothingMoved.Metadata[ConfigChangeAttribution.MetaCompareUnavailable]);
        Assert.Equal(1, nothingMoved.Metadata[ConfigChangeAttribution.MetaComparedKeys]);
        Assert.Equal(1, nothingMoved.Metadata[ConfigChangeAttribution.MetaStable]);
        Assert.Empty(ConfigChangeAttribution.MovedKeys(nothingMoved));
    }

    /// <summary>The per-key cap: the banding's worst-first order decides what survives, and the count of the rest is stated.</summary>
    [Fact]
    public void BuildFact_CapsPerKeyMetadata_AndCountsTheOmitted()
    {
        var evt = MaxdopEvent(T0, T0.AddHours(-1));
        var windows = ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4));
        /* Fifteen graded wait keys, each doubling from 0.30 to 0.60 of observed time: every one clears the absolute
           rule (a half move, at or above a quarter of its ladder), so fifteen rows move and the cap of twelve bites. */
        var keys = new[] { "PAGEIOLATCH_SH", "PAGEIOLATCH_EX", "CXPACKET", "WRITELOG", "HADR_SYNC_COMMIT", "LATCH_EX", "LATCH_SH", "LCK", "LCK_M_S", "LCK_M_IS", "RESOURCE_SEMAPHORE", "RESOURCE_SEMAPHORE_QUERY_COMPILE", "SCH_M", "LCK_M_RS_S", "LCK_M_RX_X" };
        var (before, after) = Scored(
            keys.Select(k => Wait(k, 0.30)).ToList(),
            keys.Select(k => Wait(k, 0.60)).ToList());
        var compare = ComparisonBanding.Compare(before, after, NoDispersion, coverageCaveat: false);
        var movedRows = compare.Rows.Count(r => r.Status != ComparisonBanding.StatusStable);
        Assert.True(movedRows > ConfigChangeAttribution.MaxMovedKeysInMetadata, $"the planted set must exceed the cap to test it ({movedRows} moved)");

        var fact = ConfigChangeAttribution.BuildFact(1, evt, 0, windows, compare, FullCoverage(), FullCoverage());

        Assert.Equal(ConfigChangeAttribution.MaxMovedKeysInMetadata, ConfigChangeAttribution.MovedKeys(fact).Count);
        Assert.Equal(movedRows - ConfigChangeAttribution.MaxMovedKeysInMetadata, fact.Metadata[ConfigChangeAttribution.MetaMovedKeysOmitted]);
    }

    /* ── the two engine seams ── */

    /// <summary>
    /// WHY the fact is appended after scoring: the scorer's <c>"config"</c> arm returns 0 for a key it does
    /// not know, and <c>ScoreAll</c> zeroes the severity of every base-0 fact. A CONFIG_CHANGED fact that
    /// met the scorer would leave the working set. This pin is the ordering constraint in the two services.
    /// </summary>
    [Fact]
    public void TheScorer_WouldZeroTheFact_WhichIsWhyItIsAppendedAfterScoreAll()
    {
        var fact = ConfigChangeAttribution.BuildFact(1, MaxdopEvent(T0, T0.AddHours(-23)), 0,
            ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4)), compare: null, null, null);
        Assert.Equal(ConfigChangeAttribution.InformationSeverity, fact.Severity);

        new FactScorer().ScoreAll([fact]);

        Assert.Equal(0, fact.BaseSeverity);
        Assert.Equal(0, fact.Severity);
    }

    /// <summary>
    /// The root rule: 0.25 is below the engine's 0.5 incident threshold, so CONFIG_CHANGED roots only
    /// because <c>InferenceEngine.ConfigAdvisoryRootKeys</c> names it. A one-node story, its own incident,
    /// confidence at the uncatalogued floor (no amplifiers, no path) — LOW, honestly. The control is the
    /// same fact under a key the rule does not name: no story, and the absolution takes its place.
    /// </summary>
    [Fact]
    public void TheInferenceEngine_RootsTheFactBelowHalf_ByTheExplicitRule()
    {
        var engine = new InferenceEngine(new RelationshipGraph());
        var fact = ConfigChangeAttribution.BuildFact(1, MaxdopEvent(T0, T0.AddHours(-23)), 0,
            ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4)), compare: null, null, null);

        var stories = engine.BuildStories([fact]);

        var story = Assert.Single(stories);
        Assert.False(story.IsAbsolution);
        Assert.Equal(ConfigChangeAttribution.FactKey, story.RootFactKey);
        Assert.Equal(ConfigChangeAttribution.InformationSeverity, story.Severity);
        Assert.Equal("config", story.Category);
        Assert.Equal(new[] { ConfigChangeAttribution.FactKey }, story.Path);
        Assert.Equal(StoryConfidence.Compute(0, 0, 1), story.Confidence, precision: 9);
        Assert.Same(fact.Metadata, story.RootFactMetadata);

        var incidents = engine.ClusterIntoIncidents(stories, [fact]);
        Assert.Single(Assert.Single(incidents));

        /* Control: the same severity under an unlisted key does not root. */
        var control = new Fact { Source = "config", Key = "CONFIG_NOT_A_ROOT", Value = 1, BaseSeverity = 0.25, Severity = 0.25 };
        var absolution = Assert.Single(engine.BuildStories([control]));
        Assert.True(absolution.IsAbsolution);
    }

    /* ── the prose ── */

    /// <summary>
    /// The composed card names the setting and its values, says "first observed" and the span, and lists
    /// the moved metrics with their banded magnitudes — worse first. <c>PopulateStoryText</c> freezes
    /// exactly that block, and the read side recovers it.
    /// </summary>
    [Fact]
    public void Compose_NamesTheSetting_TheObservation_AndWhatMoved_AndFreezesIntoStoryText()
    {
        var evt = MaxdopEvent(T0, T0.AddHours(-23));
        var (before, after) = Scored(
            [Cpu(70), Wait("PAGEIOLATCH_SH", 0.30)],
            [Cpu(50), Wait("PAGEIOLATCH_SH", 0.60)]);
        var compare = ComparisonBanding.Compare(before, after,
            new Dictionary<string, BaselineBucket> { [MetricNames.Cpu] = TrustworthyCpuBucket() }, coverageCaveat: false);
        var fact = ConfigChangeAttribution.BuildFact(1, evt, 0, ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4)), compare, FullCoverage(), FullCoverage());

        var advice = FactAdvice.Compose(ConfigChangeAttribution.FactKey, new[] { fact }.ToFactLookup());

        Assert.NotNull(advice);
        Assert.Contains("`max degree of parallelism` 0 → 8", advice!.Headline, StringComparison.Ordinal);
        Assert.Contains("2 metrics moved beyond band", advice.Headline, StringComparison.Ordinal);
        Assert.Contains("first observed by the configuration snapshot at 2026-09-18 14:00 UTC", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("landed somewhere in the 23 h since the previous snapshot", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("when it was seen, not when it was made", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("compared the 4 h before the observation with the 4 h after it", advice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("still filling in", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("PAGEIOLATCH_SH +50% (worse); CPU_SQL_PERCENT", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("σ (better)", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("not an accusation", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("get_server_config_changes", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("compare_analysis", advice.Remediation, StringComparison.Ordinal);

        /* Frozen at analysis time, recovered at read time — the same block. */
        var story = Assert.Single(new InferenceEngine(new RelationshipGraph()).BuildStories([fact]));
        FactAdvice.PopulateStoryText([story], [fact]);
        var readBack = FactAdvice.TryReadStoryText(story.StoryText);
        Assert.NotNull(readBack);
        Assert.Equal(advice.Headline, readBack!.Headline);
        Assert.Equal(advice.Investigation, readBack.Investigation);
    }

    /// <summary>"Nothing moved" is a finding too, and the card says it in so many words.</summary>
    [Fact]
    public void Compose_WhenNothingMoved_SaysSo()
    {
        var (before, after) = Scored([Cpu(50), Wait("LCK", 0.05)], [Cpu(51), Wait("LCK", 0.052)]);
        var fact = ConfigChangeAttribution.BuildFact(1, MaxdopEvent(T0, T0.AddHours(-2)), 0,
            ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4)),
            ComparisonBanding.Compare(before, after, NoDispersion, coverageCaveat: false), FullCoverage(), FullCoverage());

        var advice = FactAdvice.Compose(ConfigChangeAttribution.FactKey, new[] { fact }.ToFactLookup())!;

        Assert.Contains("nothing moved beyond band in the ±4 h compare", advice.Headline, StringComparison.Ordinal);
        Assert.Contains("No metric in the compare moved beyond its dispersion band", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("that is the finding", advice.Investigation, StringComparison.Ordinal);
        /* A 2 h gap is inside the 4 h before-window: no contamination warning. */
        Assert.DoesNotContain("may already reflect the new value", advice.Investigation, StringComparison.Ordinal);
    }

    /// <summary>
    /// The three disclosures the prose owes: an after half still filling in, an observation span longer
    /// than the before-window (the "before" may already hold the new value), and a compare that could not
    /// run. Each has its own sentence and its own headline.
    /// </summary>
    [Fact]
    public void Compose_DisclosesAPartialAfterHalf_ALongObservationSpan_AndAnUnavailableCompare()
    {
        var (before, after) = Scored([Cpu(50)], [Cpu(51)]);
        var compare = ComparisonBanding.Compare(before, after, NoDispersion, coverageCaveat: false);

        /* Young change: one hour of "after". Long span: 30 h since the previous snapshot. */
        var young = ConfigChangeAttribution.BuildFact(1, MaxdopEvent(T0, T0.AddHours(-30)), 0,
            ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(1)), compare, FullCoverage(), FullCoverage());
        var youngAdvice = FactAdvice.Compose(ConfigChangeAttribution.FactKey, new[] { young }.ToFactLookup())!;
        Assert.Contains("the 1 h after it that exist so far — the after half is still filling in, and later passes complete it", youngAdvice.Investigation, StringComparison.Ordinal);
        Assert.Contains("longer than the 4 h before-window", youngAdvice.Investigation, StringComparison.Ordinal);
        Assert.Contains("may already reflect the new value and a null result here does not mean the change had no effect", youngAdvice.Investigation, StringComparison.Ordinal);

        /* No compare at all. */
        var unavailable = ConfigChangeAttribution.BuildFact(1, MaxdopEvent(T0, T0.AddHours(-2)), 0,
            ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4)), compare: null, null, null);
        var unavailableAdvice = FactAdvice.Compose(ConfigChangeAttribution.FactKey, new[] { unavailable }.ToFactLookup())!;
        Assert.Contains("effect not yet compared", unavailableAdvice.Headline, StringComparison.Ordinal);
        Assert.Contains("could not run this pass", unavailableAdvice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("moved beyond", unavailableAdvice.Investigation, StringComparison.Ordinal);

        /* An earlier event in the same window is counted, not compared. */
        var stacked = ConfigChangeAttribution.BuildFact(1, MaxdopEvent(T0, T0.AddHours(-2)), earlierEventsInWindow: 1,
            ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4)), compare, FullCoverage(), FullCoverage());
        var stackedAdvice = FactAdvice.Compose(ConfigChangeAttribution.FactKey, new[] { stacked }.ToFactLookup())!;
        Assert.Contains("1 earlier configuration change also sat inside this pass's window and is not compared here", stackedAdvice.Investigation, StringComparison.Ordinal);
    }

    /// <summary>
    /// A non-dynamic setting whose configured value moved while in-use did not: the engine is still running
    /// the old value, nothing should have moved, and the card says the change takes effect at restart
    /// rather than reading the null compare as "no effect".
    /// </summary>
    [Fact]
    public void Compose_ANonDynamicChangePendingRestart_SaysNothingShouldHaveMovedYet()
    {
        var snapshots = new List<ConfigChangeDiff.ServerConfigSnapshot>
        {
            new(T0.AddHours(-2), "max worker threads", 0, 0, false, true),
            new(T0, "max worker threads", 2048, 0, false, true),
        };
        var evt = Assert.Single(Events(snapshots, T0.AddHours(-4), T0));
        Assert.True(evt.Changes[0].RequiresRestart);
        Assert.False(evt.Changes[0].InUseMoved);

        var (before, after) = Scored([Cpu(50)], [Cpu(51)]);
        var fact = ConfigChangeAttribution.BuildFact(1, evt, 0, ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4)),
            ComparisonBanding.Compare(before, after, NoDispersion, coverageCaveat: false), FullCoverage(), FullCoverage());
        Assert.Equal(1, fact.Metadata[ConfigChangeAttribution.RequiresRestartKey("max worker threads")]);

        var advice = FactAdvice.Compose(ConfigChangeAttribution.FactKey, new[] { fact }.ToFactLookup())!;
        Assert.Contains("`max worker threads` configured 0 → 2,048 (in use unchanged until restart)", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("takes effect at the next restart", advice.Headline, StringComparison.Ordinal);
        Assert.Contains("Nothing should have moved yet", advice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("had no measurable effect", advice.Investigation, StringComparison.Ordinal);
    }

    /// <summary>Two settings on one capture: the headline counts them, the body names each with its values.</summary>
    [Fact]
    public void Compose_TwoSettingsOnOneCapture_NamesBoth()
    {
        var snapshots = Snapshots(
            (T0.AddHours(-2), "max degree of parallelism", 0, 0),
            (T0.AddHours(-2), "cost threshold for parallelism", 5, 5),
            (T0, "max degree of parallelism", 8, 8),
            (T0, "cost threshold for parallelism", 50, 50));
        var evt = Assert.Single(Events(snapshots, T0.AddHours(-4), T0));
        var fact = ConfigChangeAttribution.BuildFact(1, evt, 0, ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4)), compare: null, null, null);

        Assert.Equal("cost threshold for parallelism; max degree of parallelism", fact.ObjectName);
        Assert.Equal(2, fact.Value);

        var advice = FactAdvice.Compose(ConfigChangeAttribution.FactKey, new[] { fact }.ToFactLookup())!;
        Assert.Contains("2 server settings changed together", advice.Headline, StringComparison.Ordinal);
        Assert.Contains("`cost threshold for parallelism` 5 → 50, `max degree of parallelism` 0 → 8", advice.Investigation, StringComparison.Ordinal);
    }

    /* ── the trace anchor (#3740) ── */

    /// <summary>
    /// The TextData of an ErrorLog trace event is the RAW error-log line — timestamp, spid, then the message
    /// (measured) — so the parser must find msg 15457's fixed words mid-string, read the option name between
    /// the quotes and both values, and reject everything else: another ErrorLog write, a line in another
    /// language, an empty TextData. The option name is the sys.configurations spelling, which is what the diff
    /// calls the setting, so no alias table stands between the two.
    /// </summary>
    [Fact]
    public void ParseReconfigureLine_ReadsTheRawErrorLogLine_AndRejectsOtherText()
    {
        var line = new ConfigChangeAttribution.TraceLine(T0.AddHours(-2), RawLine(Maxdop, 0, 8));
        var parsed = ConfigChangeAttribution.ParseReconfigureLine(line);

        Assert.NotNull(parsed);
        Assert.Equal(Maxdop, parsed!.Subject);
        Assert.Equal(0, parsed.OldValue);
        Assert.Equal(8, parsed.NewValue);
        Assert.Equal(T0.AddHours(-2), parsed.ChangedAtUtc);

        /* A name with parentheses and a large value — 'max server memory (MB)' 2147483647 → 65536. */
        var memory = ConfigChangeAttribution.ParseReconfigureLine(
            new ConfigChangeAttribution.TraceLine(T0, RawLine("max server memory (MB)", 2147483647, 65536)));
        Assert.Equal("max server memory (MB)", memory!.Subject);
        Assert.Equal(2147483647, memory.OldValue);
        Assert.Equal(65536, memory.NewValue);

        /* Not the message: another ErrorLog write that the SQL-side error_number filter would not let
           through anyway, a localized line, an empty TextData, a null TextData. */
        Assert.Null(ConfigChangeAttribution.ParseReconfigureLine(new ConfigChangeAttribution.TraceLine(T0,
            "2026-09-18 13:00:00.12 spid7s      SQL Server has encountered 1 occurrence(s) of I/O requests taking longer than 15 seconds")));
        Assert.Null(ConfigChangeAttribution.ParseReconfigureLine(new ConfigChangeAttribution.TraceLine(T0,
            "2026-09-18 13:00:00.12 spid95      Die Konfigurationsoption 'max degree of parallelism' wurde von 0 in 8 geändert.")));
        Assert.Null(ConfigChangeAttribution.ParseReconfigureLine(new ConfigChangeAttribution.TraceLine(T0, string.Empty)));
        Assert.Null(ConfigChangeAttribution.ParseReconfigureLine(new ConfigChangeAttribution.TraceLine(T0, null)));
    }

    /// <summary>
    /// The join's positive arm: a 15457 line for the SAME option, inside <c>(previous capture, this capture]</c>,
    /// whose values move to the observed new value, anchors the event — and the fact built on it says so in
    /// every place a reader could look: <c>anchor_source</c> = default trace, <c>change_time_unix</c> = the
    /// trace's time, <c>observed_at_unix</c> = the capture, the observation gap collapsed to 0, the lag from
    /// change to observation stated, and the per-setting trace stamp present. The compare windows, computed
    /// over the anchor, are the ±4 h around the CHANGE — so a change 27 h before a fresh observation has a
    /// complete after half where the observation anchor would have had a clamped one.
    /// </summary>
    [Fact]
    public void ResolveTraceAnchor_MatchesTheSameOptionInTheSpan_AndTheFactSaysWhichClockItUsed()
    {
        var previous = T0.AddHours(-30);
        var changedAt = T0.AddHours(-27);
        var evt = MaxdopEvent(T0, previous);

        var anchor = ConfigChangeAttribution.ResolveServerConfigTraceAnchor(evt, new[]
        {
            new ConfigChangeAttribution.TraceLine(changedAt, RawLine(Maxdop, 0, 8)),
        });

        Assert.NotNull(anchor);
        Assert.Equal(changedAt, anchor!.ChangedAtUtc);
        var only = Assert.Single(anchor.Matched);
        Assert.Equal(Maxdop, only.Key);
        Assert.Equal(changedAt, only.Value.ChangedAtUtc);
        Assert.Equal(changedAt, ConfigChangeAttribution.AnchorTime(evt, anchor));

        var windows = ConfigChangeAttribution.WindowsFor(ConfigChangeAttribution.AnchorTime(evt, anchor), T0);
        Assert.Equal(changedAt.AddHours(-4), windows.BeforeStart);
        Assert.Equal(changedAt, windows.BeforeEnd);
        Assert.Equal(changedAt.AddHours(4), windows.AfterEnd);
        Assert.False(windows.AfterClamped);

        var fact = ConfigChangeAttribution.BuildFact(1, evt, 0, windows, compare: null, null, null, anchor);
        var m = fact.Metadata;
        Assert.Equal(ConfigChangeAttribution.AnchorSourceDefaultTrace, m[ConfigChangeAttribution.MetaAnchorClock]);
        Assert.Equal(new DateTimeOffset(changedAt).ToUnixTimeSeconds(), m[ConfigChangeAttribution.MetaChangeTimeUnix]);
        Assert.Equal(new DateTimeOffset(T0).ToUnixTimeSeconds(), m[ConfigChangeAttribution.MetaObservedAtUnix]);
        Assert.Equal(0, m[ConfigChangeAttribution.MetaObservationGapHours]);
        Assert.Equal(27.0, m[ConfigChangeAttribution.MetaObservedLagHours], precision: 6);
        Assert.Equal(new DateTimeOffset(changedAt).ToUnixTimeSeconds(), m[ConfigChangeAttribution.TraceChangeTimeUnixKey(Maxdop)]);
        Assert.Equal(4.0, m[ConfigChangeAttribution.MetaAfterHoursObserved], precision: 6);
        Assert.Equal(0, m[ConfigChangeAttribution.MetaAfterWindowClamped]);

        /* The observation-anchored twin of the same event, for contrast: the capture is the change time, the
           gap is the full span, the after half is clamped to the pass end, and there is no lag key at all. */
        var observed = ConfigChangeAttribution.BuildFact(1, evt, 0, ConfigChangeAttribution.WindowsFor(T0, T0), compare: null, null, null);
        Assert.Equal(ConfigChangeAttribution.AnchorSourceObservation, observed.Metadata[ConfigChangeAttribution.MetaAnchorClock]);
        Assert.Equal(observed.Metadata[ConfigChangeAttribution.MetaChangeTimeUnix], observed.Metadata[ConfigChangeAttribution.MetaObservedAtUnix]);
        Assert.Equal(30.0, observed.Metadata[ConfigChangeAttribution.MetaObservationGapHours], precision: 6);
        Assert.False(observed.Metadata.ContainsKey(ConfigChangeAttribution.MetaObservedLagHours));
        Assert.False(observed.Metadata.ContainsKey(ConfigChangeAttribution.TraceChangeTimeUnixKey(Maxdop)));
        Assert.Equal(1, observed.Metadata[ConfigChangeAttribution.MetaAfterWindowClamped]);
    }

    /// <summary>
    /// The join's negative arms, each for a stated reason: a line for a DIFFERENT option in the span; a line
    /// for the same option OUTSIDE the span on either side (at the previous capture's own instant — that
    /// capture saw it — and after this capture — this capture did not); and no lines at all. Every one
    /// leaves the caller on the observation anchor. A line stamped exactly at this capture's instant is
    /// inside the span.
    /// </summary>
    [Fact]
    public void ResolveTraceAnchor_ADifferentOption_OrALineOutsideTheSpan_DoesNotMatch()
    {
        var previous = T0.AddHours(-30);
        var evt = MaxdopEvent(T0, previous);

        Assert.Null(ConfigChangeAttribution.ResolveServerConfigTraceAnchor(evt, new[]
        {
            new ConfigChangeAttribution.TraceLine(T0.AddHours(-2), RawLine(Ctfp, 5, 50)),
        }));

        Assert.Null(ConfigChangeAttribution.ResolveServerConfigTraceAnchor(evt, new[]
        {
            new ConfigChangeAttribution.TraceLine(previous, RawLine(Maxdop, 0, 8)),               /* at the previous capture: it saw this */
            new ConfigChangeAttribution.TraceLine(previous.AddMinutes(-1), RawLine(Maxdop, 0, 8)), /* before it */
            new ConfigChangeAttribution.TraceLine(T0.AddSeconds(1), RawLine(Maxdop, 0, 8)),        /* after this capture: it did not see this */
        }));

        Assert.Null(ConfigChangeAttribution.ResolveServerConfigTraceAnchor(evt, Array.Empty<ConfigChangeAttribution.TraceLine>()));
        Assert.Equal(T0, ConfigChangeAttribution.AnchorTime(evt, null)); /* the observation anchor IS the capture */

        var atCapture = ConfigChangeAttribution.ResolveServerConfigTraceAnchor(evt, new[]
        {
            new ConfigChangeAttribution.TraceLine(T0, RawLine(Maxdop, 0, 8)),
        });
        Assert.Equal(T0, atCapture!.ChangedAtUtc);
    }

    /// <summary>
    /// The value rule, both halves measured: <c>sp_configure</c> re-run with the current value still writes
    /// the line ("changed from 50 to 50"), and that no-op must not anchor; and a span can hold several real
    /// moves, of which the one that produced the OBSERVED value is the last whose new value is that value.
    /// A span whose lines never reach the observed value (0 → 4 when the capture saw 8) proves nothing about
    /// when 8 arrived and does not anchor.
    /// </summary>
    [Fact]
    public void ResolveTraceAnchor_SkipsTheNoOpRerun_AndTakesTheLastRealMoveToTheObservedValue()
    {
        var previous = T0.AddHours(-30);
        var evt = MaxdopEvent(T0, previous);

        /* 0 → 8 at −10 h, then a no-op 8 → 8 at −2 h: the change is the first line. */
        var noOp = ConfigChangeAttribution.ResolveServerConfigTraceAnchor(evt, new[]
        {
            new ConfigChangeAttribution.TraceLine(T0.AddHours(-10), RawLine(Maxdop, 0, 8)),
            new ConfigChangeAttribution.TraceLine(T0.AddHours(-2), RawLine(Maxdop, 8, 8)),
        });
        Assert.Equal(T0.AddHours(-10), noOp!.ChangedAtUtc);

        /* 0 → 8, 8 → 4, 4 → 8: the value the capture saw was installed by the LAST line. */
        var flapped = ConfigChangeAttribution.ResolveServerConfigTraceAnchor(evt, new[]
        {
            new ConfigChangeAttribution.TraceLine(T0.AddHours(-10), RawLine(Maxdop, 0, 8)),
            new ConfigChangeAttribution.TraceLine(T0.AddHours(-6), RawLine(Maxdop, 8, 4)),
            new ConfigChangeAttribution.TraceLine(T0.AddHours(-3), RawLine(Maxdop, 4, 8)),
        });
        Assert.Equal(T0.AddHours(-3), flapped!.ChangedAtUtc);

        /* Order of arrival is not order of time: the same three lines handed newest-first anchor the same. */
        var reversed = ConfigChangeAttribution.ResolveServerConfigTraceAnchor(evt, new[]
        {
            new ConfigChangeAttribution.TraceLine(T0.AddHours(-3), RawLine(Maxdop, 4, 8)),
            new ConfigChangeAttribution.TraceLine(T0.AddHours(-6), RawLine(Maxdop, 8, 4)),
            new ConfigChangeAttribution.TraceLine(T0.AddHours(-10), RawLine(Maxdop, 0, 8)),
        });
        Assert.Equal(T0.AddHours(-3), reversed!.ChangedAtUtc);

        /* A move that never lands on the observed value is not the change the capture saw. */
        Assert.Null(ConfigChangeAttribution.ResolveServerConfigTraceAnchor(evt, new[]
        {
            new ConfigChangeAttribution.TraceLine(T0.AddHours(-10), RawLine(Maxdop, 0, 4)),
        }));
    }

    /// <summary>
    /// Two settings on one capture, one of them dated by the trace: the event anchors on the dated one (the
    /// moment the observed configuration was complete), the matched map names only it, the fact carries the
    /// per-setting stamp for it alone, and the prose says which setting the trace did not date. Two dated
    /// settings anchor on the LATER line.
    /// </summary>
    [Fact]
    public void ResolveTraceAnchor_TwoSettings_AnchorsOnTheLatestDatedOne_AndNamesTheUndated()
    {
        var previous = T0.AddHours(-2);
        var snapshots = Snapshots(
            (previous, Maxdop, 0, 0),
            (previous, Ctfp, 5, 5),
            (T0, Maxdop, 8, 8),
            (T0, Ctfp, 50, 50));
        var evt = Assert.Single(Events(snapshots, T0.AddHours(-4), T0));

        var oneDated = ConfigChangeAttribution.ResolveServerConfigTraceAnchor(evt, new[]
        {
            new ConfigChangeAttribution.TraceLine(T0.AddMinutes(-50), RawLine(Maxdop, 0, 8)),
        });
        Assert.Equal(T0.AddMinutes(-50), oneDated!.ChangedAtUtc);
        Assert.Equal(new[] { Maxdop }, oneDated.Matched.Keys.ToArray());

        var fact = ConfigChangeAttribution.BuildFact(1, evt, 0, ConfigChangeAttribution.WindowsFor(oneDated.ChangedAtUtc, T0), compare: null, null, null, oneDated);
        Assert.True(fact.Metadata.ContainsKey(ConfigChangeAttribution.TraceChangeTimeUnixKey(Maxdop)));
        Assert.False(fact.Metadata.ContainsKey(ConfigChangeAttribution.TraceChangeTimeUnixKey(Ctfp)));

        var advice = FactAdvice.Compose(ConfigChangeAttribution.FactKey, new[] { fact }.ToFactLookup())!;
        Assert.Contains("changed at 2026-09-18 13:10 UTC (default trace", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains($"The trace dated 1 of the 2 settings; `{Ctfp}` had no matching line in the span between snapshots and shares this anchor.", advice.Investigation, StringComparison.Ordinal);

        var bothDated = ConfigChangeAttribution.ResolveServerConfigTraceAnchor(evt, new[]
        {
            new ConfigChangeAttribution.TraceLine(T0.AddMinutes(-50), RawLine(Maxdop, 0, 8)),
            new ConfigChangeAttribution.TraceLine(T0.AddMinutes(-49), RawLine(Ctfp, 5, 50)),
        });
        Assert.Equal(T0.AddMinutes(-49), bothDated!.ChangedAtUtc);
        Assert.Equal(2, bothDated.Matched.Count);
        var bothFact = ConfigChangeAttribution.BuildFact(1, evt, 0, ConfigChangeAttribution.WindowsFor(bothDated.ChangedAtUtc, T0), compare: null, null, null, bothDated);
        var bothAdvice = FactAdvice.Compose(ConfigChangeAttribution.FactKey, new[] { bothFact }.ToFactLookup())!;
        Assert.DoesNotContain("The trace dated", bothAdvice.Investigation, StringComparison.Ordinal);
    }

    /// <summary>
    /// The prose on the trace anchor: "changed at" with the source named, the snapshot's lateness stated as a
    /// fact about the card rather than the compare, the compare sentence anchored on "the change", and the
    /// observation anchor's span sentences GONE — their premise (the change landed somewhere in a span) is
    /// false once the trace has dated it. A change observed within minutes says so instead of "0 h later".
    /// The static fallback and the headline are unchanged in shape.
    /// </summary>
    [Fact]
    public void Compose_OnTheTraceAnchor_SaysChangedAt_NamesTheTrace_AndDropsTheSpanSentences()
    {
        var evt = MaxdopEvent(T0, T0.AddHours(-30));
        var changedAt = T0.AddHours(-27).AddMinutes(-12);
        var anchor = ConfigChangeAttribution.ResolveServerConfigTraceAnchor(evt, new[] { new ConfigChangeAttribution.TraceLine(changedAt, RawLine(Maxdop, 0, 8)) })!;
        var (before, after) = Scored([Cpu(50), Wait("LCK", 0.05)], [Cpu(51), Wait("LCK", 0.052)]);
        var compare = ComparisonBanding.Compare(before, after, NoDispersion, coverageCaveat: false);

        var fact = ConfigChangeAttribution.BuildFact(1, evt, 0, ConfigChangeAttribution.WindowsFor(changedAt, T0), compare, FullCoverage(), FullCoverage(), anchor);
        var advice = FactAdvice.Compose(ConfigChangeAttribution.FactKey, new[] { fact }.ToFactLookup())!;

        Assert.Contains($"`{Maxdop}` 0 → 8 — changed at 2026-09-17 10:48 UTC (default trace: the sp_configure line, msg 15457).", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("The configuration snapshot (taken on connect) first observed it 27.2 h later, at 2026-09-18 14:00 UTC; the compare below is anchored on the trace's time, not on that observation.", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("compared the 4 h before the change with the 4 h after it", advice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("first observed by the configuration snapshot", advice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("landed somewhere", advice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("when it was seen, not when it was made", advice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("may already reflect the new value", advice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("still filling in", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("nothing moved beyond band in the ±4 h compare", advice.Headline, StringComparison.Ordinal);

        /* Frozen and read back the same. */
        var story = Assert.Single(new InferenceEngine(new RelationshipGraph()).BuildStories([fact]));
        FactAdvice.PopulateStoryText([story], [fact]);
        Assert.Equal(advice.Investigation, FactAdvice.TryReadStoryText(story.StoryText)!.Investigation);

        /* Observed within minutes of the change: no "0 h later". */
        var prompt = MaxdopEvent(T0, T0.AddHours(-2));
        var promptAnchor = ConfigChangeAttribution.ResolveServerConfigTraceAnchor(prompt, new[] { new ConfigChangeAttribution.TraceLine(T0.AddMinutes(-3), RawLine(Maxdop, 0, 8)) })!;
        var promptFact = ConfigChangeAttribution.BuildFact(1, prompt, 0, ConfigChangeAttribution.WindowsFor(T0.AddMinutes(-3), T0), compare, FullCoverage(), FullCoverage(), promptAnchor);
        var promptAdvice = FactAdvice.Compose(ConfigChangeAttribution.FactKey, new[] { promptFact }.ToFactLookup())!;
        Assert.Contains("first observed it at 2026-09-18 14:00 UTC, within minutes of the change.", promptAdvice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("h later", promptAdvice.Investigation, StringComparison.Ordinal);

        /* And the observation anchor still reads exactly as #3720 wrote it. */
        var observed = ConfigChangeAttribution.BuildFact(1, evt, 0, ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4)), compare, FullCoverage(), FullCoverage());
        var observedAdvice = FactAdvice.Compose(ConfigChangeAttribution.FactKey, new[] { observed }.ToFactLookup())!;
        Assert.Contains("first observed by the configuration snapshot at 2026-09-18 14:00 UTC", observedAdvice.Investigation, StringComparison.Ordinal);
        Assert.Contains("landed somewhere in the 30 h since the previous snapshot", observedAdvice.Investigation, StringComparison.Ordinal);
        Assert.Contains("compared the 4 h before the observation with", observedAdvice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("default trace", observedAdvice.Investigation, StringComparison.Ordinal);
    }

    /// <summary>The static fallback exists for findings persisted before the composer, and the composer falls back to it when the fact is absent.</summary>
    [Fact]
    public void TheStaticBlock_Exists_AndIsTheFallbackWithoutTheFact()
    {
        var block = FactAdvice.GetForFactKey(ConfigChangeAttribution.FactKey);
        Assert.NotNull(block);
        Assert.Contains("configuration setting changed", block!.Headline, StringComparison.OrdinalIgnoreCase);

        var composed = FactAdvice.Compose(ConfigChangeAttribution.FactKey, new Dictionary<string, Fact>());
        Assert.Equal(block, composed);
    }

    /// <summary>The finding's <c>next_tools</c> row on the Lite host: the three families' history reads (slice two
    /// widened it from the server read alone), the compare, and the audit.</summary>
    [Fact]
    public void LiteToolRecommendations_PointAtTheHistoryReads_TheCompare_AndTheAudit()
    {
        Assert.Contains(ConfigChangeAttribution.FactKey, ToolRecommendations.FactKeys);
        var json = System.Text.Json.JsonSerializer.Serialize(ToolRecommendations.GetForStoryPath(StoryKeys.OfPath(ConfigChangeAttribution.FactKey)), McpHelpers.JsonOptions);
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        var tools = doc.RootElement.EnumerateArray().Select(e => e.GetProperty("tool").GetString()).ToArray();
        Assert.Equal(new[] { "get_server_config_changes", "get_database_config_changes", "get_trace_flag_changes", "compare_analysis", "audit_config" }, tools);
    }

    /* ── slice two: the database-config and trace-flag families (#3653 A10) ── */

    private const string Db = "AdventureWorks";

    /// <summary>The wire contract for <c>change_family</c>: a bit per family, summed for a folded event.</summary>
    [Fact]
    public void TheFamilyBits_ArePinned()
    {
        Assert.Equal("change_family", ConfigChangeAttribution.MetaChangeFamily);
        Assert.Equal(1, (int)ConfigChangeAttribution.ChangeFamily.ServerConfig);
        Assert.Equal(2, (int)ConfigChangeAttribution.ChangeFamily.DatabaseConfig);
        Assert.Equal(4, (int)ConfigChangeAttribution.ChangeFamily.TraceFlags);
        Assert.Equal(5, (int)(ConfigChangeAttribution.ChangeFamily.ServerConfig | ConfigChangeAttribution.ChangeFamily.TraceFlags));
        Assert.Equal(5, ConfigChangeAttribution.SameConnectToleranceMinutes);
    }

    /// <summary>
    /// A database option observed changed between two captures is one event on that capture, naming the
    /// database and the setting with its TEXT values; <c>log_reuse_wait_desc</c> — a status column the wide
    /// row carries, not an option — is not a change even when it flipped on the same capture, so a
    /// log-truncation wait cannot displace a real change from the card.
    /// </summary>
    [Fact]
    public void GroupIntoEvents_DatabaseConfig_NamesTheDatabaseAndSetting_AndIgnoresTheStatusColumn()
    {
        var previous = T0.AddHours(-23);
        var snapshots = new List<ConfigChangeDiff.DatabaseConfigSnapshot>
        {
            DbSnapshot(previous, Db, ("recovery_model", "FULL"), ("log_reuse_wait_desc", "NOTHING"), ("compatibility_level", "150")),
            DbSnapshot(T0, Db, ("recovery_model", "SIMPLE"), ("log_reuse_wait_desc", "LOG_BACKUP"), ("compatibility_level", "160")),
        };

        var raw = ConfigChangeDiff.DiffDatabaseConfigChanges(snapshots, T0.AddHours(-4), T0.AddHours(1));
        Assert.Equal(3, raw.Count);
        Assert.Contains(raw, c => c.SettingName == "log_reuse_wait_desc");
        Assert.False(ConfigChangeAttribution.IsAttributableDatabaseSetting("log_reuse_wait_desc"));
        Assert.True(ConfigChangeAttribution.IsAttributableDatabaseSetting("recovery_model"));
        Assert.False(ConfigChangeAttribution.IsAttributableDatabaseSetting(null));

        var events = DatabaseEvents(snapshots, T0.AddHours(-4), T0.AddHours(1));

        var evt = Assert.Single(events);
        Assert.Equal(T0, evt.ChangeTime);
        Assert.Equal(previous, evt.PreviousCaptureTime);
        Assert.Equal(ConfigChangeAttribution.ChangeFamily.DatabaseConfig, evt.Families);
        Assert.Equal(new[] { $"{Db}.compatibility_level", $"{Db}.recovery_model" }, evt.Changes.Select(c => c.Name).ToArray());

        var recovery = evt.Changes.Single(c => c.Setting == "recovery_model");
        Assert.Equal(Db, recovery.DatabaseName);
        Assert.Equal("FULL", recovery.OldText);
        Assert.Equal("SIMPLE", recovery.NewText);
        Assert.Null(recovery.OldValueInUse);
        Assert.False(recovery.RequiresRestart);

        /* A numeric option lands in the numeric slots too, so the fact's doubles can carry it. */
        var compat = evt.Changes.Single(c => c.Setting == "compatibility_level");
        Assert.Equal(150L, compat.OldValueInUse);
        Assert.Equal(160L, compat.NewValueInUse);
        Assert.True(compat.InUseMoved);

        Assert.Equal(1L, ConfigChangeAttribution.SettingChange.TryNumeric("true"));
        Assert.Equal(0L, ConfigChangeAttribution.SettingChange.TryNumeric("False"));
        Assert.Null(ConfigChangeAttribution.SettingChange.TryNumeric("SIMPLE"));
        Assert.Null(ConfigChangeAttribution.SettingChange.TryNumeric(null));
    }

    /// <summary>
    /// The trace-flag set-diff's three outcomes, each one change on the newer capture: a flag that appeared is
    /// enabled (OFF → ON, its scope), one that vanished is disabled (ON → OFF, the scope it had), one whose
    /// scope moved is modified. The flag rides as <c>trace flag {n}</c> so its keys cannot collide with an option's.
    /// </summary>
    [Fact]
    public void GroupIntoEvents_TraceFlags_EnabledDisabledModified_FromTheSetDiff()
    {
        var previous = T0.AddHours(-23);
        var snapshots = new List<ConfigChangeDiff.TraceFlagSnapshot>
        {
            new(previous, 3226, true, true, false),
            new(previous, 1222, true, true, false),
            new(previous, 7412, true, false, true),
            new(T0, 3226, true, true, false),
            new(T0, 4199, true, true, false),
            new(T0, 7412, true, true, false),
        };

        var evt = Assert.Single(TraceFlagEvents(snapshots, T0.AddHours(-4), T0.AddHours(1)));
        Assert.Equal(T0, evt.ChangeTime);
        Assert.Equal(previous, evt.PreviousCaptureTime);
        Assert.Equal(ConfigChangeAttribution.ChangeFamily.TraceFlags, evt.Families);
        Assert.Equal(new[] { "trace flag 1222", "trace flag 4199", "trace flag 7412" }, evt.Changes.Select(c => c.Name).ToArray());

        var enabled = evt.Changes.Single(c => c.Setting == "4199");
        Assert.Equal("enabled", enabled.ChangeType);
        Assert.Equal("GLOBAL", enabled.Scope);
        Assert.Equal(0L, enabled.OldValueInUse);
        Assert.Equal(1L, enabled.NewValueInUse);
        Assert.Equal(("OFF", "ON"), (enabled.OldText, enabled.NewText));

        var disabled = evt.Changes.Single(c => c.Setting == "1222");
        Assert.Equal("disabled", disabled.ChangeType);
        Assert.Equal("GLOBAL", disabled.Scope);
        Assert.Equal((1L, 0L), (disabled.OldValueInUse, disabled.NewValueInUse));

        var modified = evt.Changes.Single(c => c.Setting == "7412");
        Assert.Equal("modified", modified.ChangeType);
        Assert.Equal("GLOBAL", modified.Scope);
        Assert.False(modified.InUseMoved);
        Assert.Null(modified.DatabaseName);
    }

    /// <summary>
    /// The same-connect fold: a server setting captured at T and a trace flag captured two seconds later are
    /// ONE event (the later capture's time, the earlier previous capture, changes ordered family then name) —
    /// the data cannot say which moved a metric, exactly as with two settings on one capture. Two refusals:
    /// events of the SAME family are never folded whatever their spacing (two captures of one family are two
    /// connects and the span between them is real), and events beyond the tolerance are separate connects.
    /// </summary>
    [Fact]
    public void MergeSameConnectEvents_FoldsFamiliesObservedAtOneConnect_AndRefusesTheSameFamilyOrADistantOne()
    {
        var serverEvent = MaxdopEvent(T0, T0.AddHours(-23));
        var traceEvent = Assert.Single(TraceFlagEvents(
            [new(T0.AddHours(-23).AddSeconds(3), 3226, true, true, false), new(T0.AddSeconds(2), 3226, true, true, false), new(T0.AddSeconds(2), 4199, true, true, false)],
            T0.AddHours(-4), T0.AddHours(1)));
        var databaseEvent = Assert.Single(DatabaseEvents(
            [DbSnapshot(T0.AddHours(-30), Db, ("recovery_model", "FULL")), DbSnapshot(T0.AddSeconds(1), Db, ("recovery_model", "SIMPLE"))],
            T0.AddHours(-4), T0.AddHours(1)));

        var merged = ConfigChangeAttribution.MergeSameConnectEvents([serverEvent, traceEvent, databaseEvent]);

        var evt = Assert.Single(merged);
        Assert.Equal(T0.AddSeconds(2), evt.ChangeTime);
        Assert.Equal(T0.AddHours(-30), evt.PreviousCaptureTime);
        Assert.Equal(ConfigChangeAttribution.ChangeFamily.ServerConfig | ConfigChangeAttribution.ChangeFamily.DatabaseConfig | ConfigChangeAttribution.ChangeFamily.TraceFlags, evt.Families);
        Assert.Equal(new[] { Maxdop, $"{Db}.recovery_model", "trace flag 4199" }, evt.Changes.Select(c => c.Name).ToArray());

        /* Refusal 1: two server events two minutes apart stay two events. */
        var earlierServer = MaxdopEvent(T0.AddMinutes(-2), T0.AddHours(-23));
        var twoConnects = ConfigChangeAttribution.MergeSameConnectEvents([serverEvent, earlierServer]);
        Assert.Equal(2, twoConnects.Count);
        Assert.Equal(T0, twoConnects[0].ChangeTime);
        Assert.Equal(T0.AddMinutes(-2), twoConnects[1].ChangeTime);

        /* Refusal 2: a trace flag captured six minutes before the server setting is another connect. */
        var distantTrace = Assert.Single(TraceFlagEvents(
            [new(T0.AddHours(-23), 3226, true, true, false), new(T0.AddMinutes(-6), 3226, true, true, false), new(T0.AddMinutes(-6), 4199, true, true, false)],
            T0.AddHours(-4), T0.AddHours(1)));
        var distant = ConfigChangeAttribution.MergeSameConnectEvents([distantTrace, serverEvent]);
        Assert.Equal(2, distant.Count);
        Assert.Equal(ConfigChangeAttribution.ChangeFamily.ServerConfig, distant[0].Families);
        Assert.Equal(ConfigChangeAttribution.ChangeFamily.TraceFlags, distant[1].Families);

        /* A single-family list comes back as it went in. */
        Assert.Equal(new[] { T0 }, ConfigChangeAttribution.MergeSameConnectEvents([serverEvent]).Select(e => e.ChangeTime));
        Assert.Empty(ConfigChangeAttribution.MergeSameConnectEvents([]));
    }

    /// <summary>
    /// The database family's fact: the family bit, the database in the fact's own seam (one database, every
    /// change on it), the segment grammar in ObjectName with the database name LAST so a name holding the
    /// field separator round-trips, numeric slots for the option that parses and none for the one that does
    /// not, and no <c>requires_restart</c> key — that is a sys.configurations concept.
    /// </summary>
    [Fact]
    public void BuildFact_DatabaseFamily_CarriesTheFamily_TheDatabase_AndTheSegments()
    {
        const string oddDb = "Sales|Archive.2024";
        var evt = Assert.Single(DatabaseEvents(
            [
                DbSnapshot(T0.AddHours(-23), oddDb, ("recovery_model", "FULL"), ("compatibility_level", "150"), ("delayed_durability", null)),
                DbSnapshot(T0, oddDb, ("recovery_model", "SIMPLE"), ("compatibility_level", "160"), ("delayed_durability", "FORCED")),
            ],
            T0.AddHours(-4), T0.AddHours(1)));

        var fact = ConfigChangeAttribution.BuildFact(7, evt, 0, ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4)), compare: null, null, null);

        Assert.Equal(ConfigChangeAttribution.FactKey, fact.Key);
        Assert.Equal(oddDb, fact.DatabaseName);
        Assert.Equal(3, fact.Value);
        Assert.Equal(2, fact.Metadata[ConfigChangeAttribution.MetaChangeFamily]);
        Assert.Equal(3, fact.Metadata[ConfigChangeAttribution.MetaChangedSettings]);
        Assert.Equal(ConfigChangeAttribution.InformationSeverity, fact.Severity);

        Assert.Equal(
            $"db|compatibility_level|150|160|{oddDb}; db|delayed_durability||FORCED|{oddDb}; db|recovery_model|FULL|SIMPLE|{oddDb}",
            fact.ObjectName);

        var changes = ConfigChangeAttribution.Changes(fact);
        Assert.Equal(3, changes.Count);
        Assert.All(changes, c => Assert.Equal(oddDb, c.DatabaseName));
        Assert.All(changes, c => Assert.Equal(ConfigChangeAttribution.ChangeFamily.DatabaseConfig, c.Family));
        var durability = changes.Single(c => c.Setting == "delayed_durability");
        Assert.Null(durability.OldText);
        Assert.Equal("FORCED", durability.NewText);

        Assert.Equal(150, fact.Metadata[ConfigChangeAttribution.OldInUseKey($"{oddDb}.compatibility_level")]);
        Assert.Equal(160, fact.Metadata[ConfigChangeAttribution.NewInUseKey($"{oddDb}.compatibility_level")]);
        Assert.False(fact.Metadata.ContainsKey(ConfigChangeAttribution.OldInUseKey($"{oddDb}.recovery_model")));
        Assert.DoesNotContain(fact.Metadata.Keys, k => k.EndsWith("|requires_restart", StringComparison.Ordinal));

        /* Two databases on one capture: no single database to claim; the seam stays server-scoped. */
        var twoDbs = Assert.Single(DatabaseEvents(
            [
                DbSnapshot(T0.AddHours(-23), "A", ("is_auto_shrink_on", "false")), DbSnapshot(T0.AddHours(-23), "B", ("is_auto_shrink_on", "false")),
                DbSnapshot(T0, "A", ("is_auto_shrink_on", "true")), DbSnapshot(T0, "B", ("is_auto_shrink_on", "true")),
            ],
            T0.AddHours(-4), T0.AddHours(1)));
        var twoDbFact = ConfigChangeAttribution.BuildFact(7, twoDbs, 0, ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4)), null, null, null);
        Assert.Null(twoDbFact.DatabaseName);
        Assert.Equal(1, twoDbFact.Metadata[ConfigChangeAttribution.NewInUseKey("A.is_auto_shrink_on")]);
    }

    /// <summary>The trace-flag family's fact and the folded event's: the bits, the 0/1 status slots, the segments, and no database.</summary>
    [Fact]
    public void BuildFact_TraceFlagFamily_AndAFoldedEvent_CarryTheirBits()
    {
        var traceEvent = Assert.Single(TraceFlagEvents(
            [new(T0.AddHours(-23), 3226, true, true, false), new(T0, 3226, true, true, false), new(T0, 4199, true, true, false)],
            T0.AddHours(-4), T0.AddHours(1)));
        var traceFact = ConfigChangeAttribution.BuildFact(7, traceEvent, 0, ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4)), null, null, null);

        Assert.Equal(4, traceFact.Metadata[ConfigChangeAttribution.MetaChangeFamily]);
        Assert.Null(traceFact.DatabaseName);
        Assert.Equal("tf|4199|enabled|GLOBAL", traceFact.ObjectName);
        Assert.Equal(0, traceFact.Metadata[ConfigChangeAttribution.OldInUseKey("trace flag 4199")]);
        Assert.Equal(1, traceFact.Metadata[ConfigChangeAttribution.NewInUseKey("trace flag 4199")]);
        var decoded = Assert.Single(ConfigChangeAttribution.Changes(traceFact));
        Assert.Equal(("4199", "enabled", "GLOBAL", 0L, 1L), (decoded.Setting, decoded.ChangeType, decoded.Scope, decoded.OldValueInUse, decoded.NewValueInUse));

        var folded = Assert.Single(ConfigChangeAttribution.MergeSameConnectEvents([MaxdopEvent(T0.AddSeconds(-2), T0.AddHours(-23)), traceEvent]));
        var foldedFact = ConfigChangeAttribution.BuildFact(7, folded, 0, ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4)), null, null, null);
        Assert.Equal(5, foldedFact.Metadata[ConfigChangeAttribution.MetaChangeFamily]);
        Assert.Null(foldedFact.DatabaseName);
        Assert.Equal($"{Maxdop}; tf|4199|enabled|GLOBAL", foldedFact.ObjectName);
        Assert.Equal(2, foldedFact.Value);
        Assert.Equal(0, foldedFact.Metadata[ConfigChangeAttribution.RequiresRestartKey(Maxdop)]);
        Assert.False(foldedFact.Metadata.ContainsKey(ConfigChangeAttribution.RequiresRestartKey("trace flag 4199")));
    }

    /// <summary>The segment grammar round-trips every family, and a segment slice one wrote decodes as a server setting.</summary>
    [Fact]
    public void SegmentGrammar_RoundTrips_AndTheServerSegmentIsSliceOnes()
    {
        var server = new ConfigChangeAttribution.SettingChange(Maxdop, 0, 8, 0, 8, false);
        Assert.Equal(Maxdop, ConfigChangeAttribution.EncodeSegment(server));
        var backServer = ConfigChangeAttribution.DecodeSegment(Maxdop);
        Assert.Equal((Maxdop, ConfigChangeAttribution.ChangeFamily.ServerConfig), (backServer.Name, backServer.Family));
        Assert.Null(backServer.NewValueInUse);

        var db = ConfigChangeAttribution.SettingChange.ForDatabase("we|ird; name", "collation_name", null, "Latin1_General_CI_AS");
        var dbSegment = ConfigChangeAttribution.EncodeSegment(db);
        Assert.Equal("db|collation_name||Latin1_General_CI_AS|we|ird; name", dbSegment);
        var backDb = ConfigChangeAttribution.DecodeSegment(dbSegment);
        Assert.Equal(db, backDb);

        var tf = ConfigChangeAttribution.SettingChange.ForTraceFlag(1222, "disabled", "GLOBAL", true, null);
        Assert.Equal("tf|1222|disabled|GLOBAL", ConfigChangeAttribution.EncodeSegment(tf));
        Assert.Equal(tf, ConfigChangeAttribution.DecodeSegment("tf|1222|disabled|GLOBAL"));

        /* A malformed prefixed segment is a name, not an exception. */
        Assert.Equal(ConfigChangeAttribution.ChangeFamily.ServerConfig, ConfigChangeAttribution.DecodeSegment("db|only").Family);
        Assert.Equal(ConfigChangeAttribution.ChangeFamily.ServerConfig, ConfigChangeAttribution.DecodeSegment("tf|x|enabled|GLOBAL").Family);
    }

    /// <summary>
    /// The #3740 join is the server family's: a folded event's flag cannot be dated by a 15457 line, and the
    /// event's server setting still is. A trace-flag-only event resolves no anchor at all, whatever lines are handed in.
    /// </summary>
    [Fact]
    public void ResolveTraceAnchor_DatesOnlyTheServerSetting_OfAFoldedEvent()
    {
        var traceEvent = Assert.Single(TraceFlagEvents(
            [new(T0.AddHours(-23), 3226, true, true, false), new(T0, 3226, true, true, false), new(T0, 4199, true, true, false)],
            T0.AddHours(-4), T0.AddHours(1)));
        var folded = Assert.Single(ConfigChangeAttribution.MergeSameConnectEvents([MaxdopEvent(T0.AddSeconds(-2), T0.AddHours(-23)), traceEvent]));
        var lines = new[] { new ConfigChangeAttribution.TraceLine(T0.AddHours(-5), RawLine(Maxdop, 0, 8)) };

        var anchor = ConfigChangeAttribution.ResolveServerConfigTraceAnchor(folded, lines);
        Assert.NotNull(anchor);
        Assert.Equal(T0.AddHours(-5), anchor!.ChangedAtUtc);
        Assert.Equal(new[] { Maxdop }, anchor.Matched.Keys);

        Assert.Null(ConfigChangeAttribution.ResolveServerConfigTraceAnchor(traceEvent, lines));

        var fact = ConfigChangeAttribution.BuildFact(1, folded, 0, ConfigChangeAttribution.WindowsFor(T0.AddHours(-5), T0.AddHours(4)), null, null, null, anchor);
        var advice = FactAdvice.Compose(ConfigChangeAttribution.FactKey, new[] { fact }.ToFactLookup())!;
        Assert.Contains("The trace dated 1 of the 2 settings; `trace flag 4199` had no matching line", advice.Investigation, StringComparison.Ordinal);
    }

    /// <summary>The database family's prose: the headline names the family and the database, the body its option old → new, the remediation its history read and grader.</summary>
    [Fact]
    public void Compose_DatabaseFamily_NamesTheDatabase_TheOption_AndItsOwnTools()
    {
        var evt = Assert.Single(DatabaseEvents(
            [DbSnapshot(T0.AddHours(-23), Db, ("recovery_model", "FULL")), DbSnapshot(T0, Db, ("recovery_model", "SIMPLE"))],
            T0.AddHours(-4), T0.AddHours(1)));
        var (before, after) = Scored([Cpu(50), Wait("WRITELOG", 0.30)], [Cpu(51), Wait("WRITELOG", 0.05)]);
        var compare = ComparisonBanding.Compare(before, after, NoDispersion, coverageCaveat: false);
        var fact = ConfigChangeAttribution.BuildFact(1, evt, 0, ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4)), compare, FullCoverage(), FullCoverage());

        var advice = FactAdvice.Compose(ConfigChangeAttribution.FactKey, new[] { fact }.ToFactLookup())!;

        Assert.StartsWith($"Database configuration changed: `{Db}` recovery_model FULL → SIMPLE — 1 metric moved beyond band after it", advice.Headline, StringComparison.Ordinal);
        Assert.Contains($"`{Db}` recovery_model FULL → SIMPLE — first observed by the configuration snapshot at 2026-09-18 14:00 UTC.", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("landed somewhere in the 23 h since the previous snapshot", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("WRITELOG", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("(better)", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("`get_database_config_changes` lists the change", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("`DB_CONFIG` advisory", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("get_server_config_changes", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("audit_config", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("Server configuration", advice.Headline, StringComparison.Ordinal);

        /* NULL on one side reads as set / cleared, not as "(none) →". */
        var setEvt = Assert.Single(DatabaseEvents(
            [DbSnapshot(T0.AddHours(-2), Db, ("delayed_durability", null), ("collation_name", "X")), DbSnapshot(T0, Db, ("delayed_durability", "FORCED"), ("collation_name", null))],
            T0.AddHours(-4), T0.AddHours(1)));
        var setAdvice = FactAdvice.Compose(ConfigChangeAttribution.FactKey,
            new[] { ConfigChangeAttribution.BuildFact(1, setEvt, 0, ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4)), null, null, null) }.ToFactLookup())!;
        Assert.Contains($"`{Db}` collation_name cleared (was X), `{Db}` delayed_durability set to FORCED", setAdvice.Investigation, StringComparison.Ordinal);
        Assert.Contains("2 database settings changed together", setAdvice.Headline, StringComparison.Ordinal);

        /* Frozen and read back, like every other family. */
        var story = Assert.Single(new InferenceEngine(new RelationshipGraph()).BuildStories([fact]));
        Assert.Equal(Db, story.DatabaseName);
        FactAdvice.PopulateStoryText([story], [fact]);
        Assert.Equal(advice.Headline, FactAdvice.TryReadStoryText(story.StoryText)!.Headline);
    }

    /// <summary>The trace-flag family's prose, and the folded event's: family-named headlines, each change in its own words, the history reads of the families present.</summary>
    [Fact]
    public void Compose_TraceFlagFamily_AndAFoldedEvent_NameEachChangeInItsOwnWords()
    {
        var traceEvent = Assert.Single(TraceFlagEvents(
            [
                new(T0.AddHours(-2), 3226, true, true, false), new(T0.AddHours(-2), 1222, true, true, false),
                new(T0, 3226, true, true, false), new(T0, 4199, true, true, false),
            ],
            T0.AddHours(-4), T0.AddHours(1)));
        var (before, after) = Scored([Cpu(50)], [Cpu(51)]);
        var compare = ComparisonBanding.Compare(before, after, NoDispersion, coverageCaveat: false);
        var traceFact = ConfigChangeAttribution.BuildFact(1, traceEvent, 0, ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4)), compare, FullCoverage(), FullCoverage());

        var traceAdvice = FactAdvice.Compose(ConfigChangeAttribution.FactKey, new[] { traceFact }.ToFactLookup())!;
        Assert.StartsWith("Trace flags changed: 2 trace flags changed together — nothing moved beyond band", traceAdvice.Headline, StringComparison.Ordinal);
        Assert.Contains("trace flag 1222 disabled (was GLOBAL), trace flag 4199 enabled (GLOBAL) — first observed by the configuration snapshot at", traceAdvice.Investigation, StringComparison.Ordinal);
        Assert.Contains("No metric in the compare moved beyond its dispersion band", traceAdvice.Investigation, StringComparison.Ordinal);
        Assert.Contains("`get_trace_flag_changes` lists the change", traceAdvice.Remediation, StringComparison.Ordinal);
        Assert.Contains("`get_trace_flags` lists what is on now", traceAdvice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("audit_config", traceAdvice.Remediation, StringComparison.Ordinal);

        var single = ConfigChangeAttribution.BuildFact(1,
            Assert.Single(TraceFlagEvents([new(T0.AddHours(-2), 3226, true, true, false), new(T0, 3226, true, true, false), new(T0, 4199, true, true, false)], T0.AddHours(-4), T0.AddHours(1))),
            0, ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4)), null, null, null);
        Assert.StartsWith("Trace flag changed: trace flag 4199 enabled (GLOBAL) — effect not yet compared", FactAdvice.Compose(ConfigChangeAttribution.FactKey, new[] { single }.ToFactLookup())!.Headline, StringComparison.Ordinal);

        /* Folded across families: the headline says observed together, the body names both in order, and the
           remediation lists both families' history reads and both graders. */
        var folded = Assert.Single(ConfigChangeAttribution.MergeSameConnectEvents([MaxdopEvent(T0.AddSeconds(-2), T0.AddHours(-2)), traceEvent]));
        var foldedFact = ConfigChangeAttribution.BuildFact(1, folded, 0, ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4)), compare, FullCoverage(), FullCoverage());
        var foldedAdvice = FactAdvice.Compose(ConfigChangeAttribution.FactKey, new[] { foldedFact }.ToFactLookup())!;
        Assert.StartsWith("Configuration changed: 3 configuration changes observed together — nothing moved beyond band", foldedAdvice.Headline, StringComparison.Ordinal);
        Assert.Contains($"`{Maxdop}` 0 → 8, trace flag 1222 disabled (was GLOBAL), trace flag 4199 enabled (GLOBAL) — first observed", foldedAdvice.Investigation, StringComparison.Ordinal);
        Assert.Contains("the changes are the first suspect: `get_server_config_changes` / `get_trace_flag_changes` list the change", foldedAdvice.Remediation, StringComparison.Ordinal);
        Assert.Contains("`audit_config` grades the new value against guidance; `get_trace_flags` lists what is on now", foldedAdvice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("earlier configuration change", foldedAdvice.Investigation, StringComparison.Ordinal);
    }

    /// <summary>The Lite snapshot read projects the 27 option columns in the diff's order after capture_time and database_name.</summary>
    [Fact]
    public void TheLiteDatabaseConfigRead_ProjectsTheDiffsColumnsInOrder()
    {
        var sql = AnalysisService.DatabaseConfigSnapshotsForAttributionSql;
        var projection = sql[..sql.IndexOf("FROM v_database_config", StringComparison.Ordinal)];
        var columns = System.Text.RegularExpressions.Regex.Matches(projection, @"CAST\((\w+) AS VARCHAR\)").Select(m => m.Groups[1].Value).ToArray();
        Assert.Equal(ConfigChangeDiff.DatabaseConfigChangeSettingNames, columns);
        Assert.Contains("SELECT capture_time, database_name,", sql, StringComparison.Ordinal);
        Assert.Contains("(SELECT MAX(capture_time) FROM v_database_config WHERE server_id = $1 AND capture_time < $2)", sql, StringComparison.Ordinal);

        var tf = AnalysisService.TraceFlagSnapshotsForAttributionSql;
        Assert.Contains("SELECT capture_time, trace_flag, status, is_global, is_session", tf, StringComparison.Ordinal);
        Assert.Contains("(SELECT MAX(capture_time) FROM v_trace_flags WHERE server_id = $1 AND capture_time < $2)", tf, StringComparison.Ordinal);
        Assert.EndsWith("ORDER BY capture_time, trace_flag", tf.TrimEnd(), StringComparison.Ordinal);
    }

    /* ── slice-two helpers: the services' mappings, verbatim ── */

    /// <summary>A wide database_config capture with every column NULL except the named ones — the diff walks all 27.</summary>
    private static ConfigChangeDiff.DatabaseConfigSnapshot DbSnapshot(DateTime at, string database, params (string Setting, string? Value)[] set)
    {
        var values = new string?[ConfigChangeDiff.DatabaseConfigChangeSettingNames.Count];
        foreach (var (setting, value) in set)
        {
            var i = ConfigChangeDiff.DatabaseConfigChangeSettingNames.ToList().IndexOf(setting);
            Assert.True(i >= 0, $"{setting} is not a database_config column");
            values[i] = value;
        }
        return new ConfigChangeDiff.DatabaseConfigSnapshot(at, database, values);
    }

    private static List<ConfigChangeAttribution.ChangeEvent> DatabaseEvents(
        List<ConfigChangeDiff.DatabaseConfigSnapshot> snapshots, DateTime windowStart, DateTime windowEnd) =>
        ConfigChangeAttribution.GroupIntoEvents(
            ConfigChangeDiff.DiffDatabaseConfigChanges(snapshots, windowStart, windowEnd)
                .Where(c => ConfigChangeAttribution.IsAttributableDatabaseSetting(c.SettingName))
                .Select(c => (c.ChangeTime, ConfigChangeAttribution.SettingChange.ForDatabase(c.DatabaseName, c.SettingName, c.OldValue, c.NewValue))),
            snapshots.Select(s => s.CaptureTime));

    private static List<ConfigChangeAttribution.ChangeEvent> TraceFlagEvents(
        List<ConfigChangeDiff.TraceFlagSnapshot> snapshots, DateTime windowStart, DateTime windowEnd) =>
        ConfigChangeAttribution.GroupIntoEvents(
            ConfigChangeDiff.DiffTraceFlagChanges(snapshots, windowStart, windowEnd)
                .Select(c => (c.ChangeTime, ConfigChangeAttribution.SettingChange.ForTraceFlag(c.TraceFlag, c.ChangeType, c.Scope, c.PreviousStatus, c.NewStatus))),
            snapshots.Select(s => s.CaptureTime));

    /* ── helpers ── */

    private static List<ConfigChangeDiff.ServerConfigSnapshot> Snapshots(params (DateTime At, string Name, long Configured, long InUse)[] rows) =>
        rows.Select(r => new ConfigChangeDiff.ServerConfigSnapshot(r.At, r.Name, r.Configured, r.InUse, true, true)).ToList();

    /// <summary>The services' mapping, verbatim: the shared diff, then the tuple the attribution groups.</summary>
    private static List<ConfigChangeAttribution.ChangeEvent> Events(
        List<ConfigChangeDiff.ServerConfigSnapshot> snapshots, DateTime windowStart, DateTime windowEnd) =>
        ConfigChangeAttribution.GroupIntoEvents(
            ConfigChangeDiff.DiffServerConfigChanges(snapshots, windowStart, windowEnd)
                .Select(c => (c.ChangeTime, new ConfigChangeAttribution.SettingChange(
                    c.ConfigurationName, c.OldValueConfigured, c.NewValueConfigured, c.OldValueInUse, c.NewValueInUse, c.RequiresRestart))),
            snapshots.Select(s => s.CaptureTime));

    private static ConfigChangeAttribution.ChangeEvent MaxdopEvent(DateTime observedAt, DateTime previousCapture) =>
        Assert.Single(Events(
            Snapshots((previousCapture, "max degree of parallelism", 0, 0), (observedAt, "max degree of parallelism", 8, 8)),
            observedAt.AddHours(-4), observedAt));

    /// <summary>Msg 15457's TextData as the default trace stores it (measured on SQL Server 2022): the raw
    /// error-log line — timestamp, spid, six spaces, then the message.</summary>
    private static string RawLine(string option, long oldValue, long newValue) =>
        $"2026-09-17 10:48:00.91 spid95      Configuration option '{option}' changed from {oldValue} to {newValue}. Run the RECONFIGURE statement to install.";

    private static Fact Wait(string type, double fraction) => new()
    {
        Source = "waits", Key = type, Value = fraction,
        Metadata = new Dictionary<string, double> { ["wait_time_ms"] = fraction * 14_400_000, ["period_duration_ms"] = 14_400_000 }
    };

    private static Fact Cpu(double avgPercent) => new() { Source = "cpu", Key = "CPU_SQL_PERCENT", Value = avgPercent };

    private static (List<Fact>, List<Fact>) Scored(List<Fact> baseline, List<Fact> comparison)
    {
        var scorer = new FactScorer();
        scorer.ScoreAll(baseline);
        scorer.ScoreAll(comparison);
        return (baseline, comparison);
    }

    /// <summary>Full tier, well past the sample and day floors, Mad 2 → a robust sigma near 3 CPU points.</summary>
    private static BaselineBucket TrustworthyCpuBucket() => new()
    {
        HourOfDay = 14, DayOfWeek = 4, Tier = BaselineTier.Full,
        Mean = 60, StdDev = 5, Median = 60, Mad = 2, SampleCount = 200, DistinctDays = 20,
        AbsStdDevFloor = BaselineMath.AbsStdDevFloorFor(MetricNames.Cpu)
    };

    private static WindowCoverage FullCoverage() => new() { NominalMs = 14_400_000, ObservedMs = 14_400_000, SampleCount = 16, LargestGapMs = 0 };
}
