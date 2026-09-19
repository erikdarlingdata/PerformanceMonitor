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
/// </summary>
public sealed class ConfigChangeAttributionTests
{
    private static readonly DateTime T0 = new(2026, 9, 18, 14, 0, 0, DateTimeKind.Utc);
    private static readonly IReadOnlyDictionary<string, BaselineBucket> NoDispersion = new Dictionary<string, BaselineBucket>();

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

    /// <summary>The finding's <c>next_tools</c> row on the Lite host: the history read, the compare, and the audit.</summary>
    [Fact]
    public void LiteToolRecommendations_PointAtTheHistoryRead_TheCompare_AndTheAudit()
    {
        Assert.Contains(ConfigChangeAttribution.FactKey, ToolRecommendations.FactKeys);
        var json = System.Text.Json.JsonSerializer.Serialize(ToolRecommendations.GetForStoryPath(ConfigChangeAttribution.FactKey), McpHelpers.JsonOptions);
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        var tools = doc.RootElement.EnumerateArray().Select(e => e.GetProperty("tool").GetString()).ToArray();
        Assert.Equal(new[] { "get_server_config_changes", "compare_analysis", "audit_config" }, tools);
    }

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
