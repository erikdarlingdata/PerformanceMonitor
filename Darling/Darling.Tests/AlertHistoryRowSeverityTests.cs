/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3539 A8e: an alert-history row is styled and reported at the severity the alert actually FIRED at,
/// not the colour its name implies.
///
/// <para><b>The defect.</b> <see cref="AlertMetricClassifier.IsCritical"/> is by NAME — "Poison Wait" is
/// red — and it was the only severity the grids had. Poison Wait has been graded Warning/Critical at its
/// fire site since #2711 (PostgreSQL) and #3539 A4 (SQL Server), so a Warning-graded fire rendered red in
/// Lite's grid and the Viewer's alike; the same gap ran the other way for every metric the engine grades
/// ABOVE its name (a CRITICAL low-disk fire, a SUSPECT database) — amber rows for critical pages. The tier
/// was on <see cref="AlertContext.SeverityOverride"/> at fire time and the JSON projection dropped it, so
/// the row had nothing else to offer.</para>
///
/// <para><b>The fix, and what these pins hold.</b> <see cref="AlertContextSerializer"/> carries the tier
/// as a trailing nullable member (both SKUs write through it, so no store changed on either);
/// <see cref="AlertHistoryRowSeverity"/> reads it and falls back to the by-name classifier ONLY for rows
/// that carry none. The pins: the member round-trips by name and reads back; a row written before it
/// existed deserializes exactly as it did (the additive-only contract); the fallback fires only where no
/// tier exists, and says the right thing there; both grids' row classes reach the shared decision and no
/// grid still calls the by-name predicates directly.</para>
/// </summary>
public sealed class AlertHistoryRowSeverityTests
{
    /* ─────────────────────────── the member on the wire ─────────────────────────── */

    private static string WithSeverity(AlertSeverityLevel? level)
    {
        var context = new AlertContext { SeverityOverride = level };
        context.Details.Add(new AlertDetailItem { Heading = "THREADPOOL", Fields = { ("Accumulated wait", "61 s") } });
        return AlertContextSerializer.Serialize(context);
    }

    [Theory]
    [InlineData(AlertSeverityLevel.Warning, "\"Severity\":\"Warning\"")]
    [InlineData(AlertSeverityLevel.Critical, "\"Severity\":\"Critical\"")]
    public void TheTierIsPersistedByName_AndReadsBack(AlertSeverityLevel level, string fragment)
    {
        var json = WithSeverity(level);

        /* By NAME, not ordinal: the column outlives any build, and a stored "1" would change meaning the
           day a member is inserted ahead of Critical. */
        Assert.Contains(fragment, json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"Severity\":" + (int)level, json, StringComparison.Ordinal);

        Assert.Equal(level, AlertContextSerializer.TryReadSeverity(json));

        /* The full rehydration agrees with the cheap read, and the Details it always carried are intact. */
        Assert.True(AlertContextSerializer.TryDeserialize(json, out var restored));
        Assert.Equal(level, restored.SeverityOverride);
        Assert.Single(restored.Details);
    }

    /// <summary>
    /// Additive only: a row written BEFORE the member existed — the exact JSON the pre-#3539 serializer
    /// emitted, hand-written here so it cannot drift with the serializer — rehydrates its Details and
    /// Incidents as it always did and reads back NO tier. Null is the honest answer for that row, and it is
    /// what sends the grids to the by-name fallback.
    /// </summary>
    [Fact]
    public void ARowWrittenBeforeTheMemberExisted_DeserializesUnchanged_AndCarriesNoTier()
    {
        const string legacy =
            "{\"Details\":[{\"Heading\":\"THREADPOOL\",\"Fields\":[{\"Label\":\"Avg wait\",\"Value\":\"600 ms\"}],\"Body\":null,\"IsCodeBlock\":false,\"Remediation\":null}],"
            + "\"Incidents\":[{\"DedupKey\":\"abc\",\"InvolvedObjects\":[\"x\"],\"OccurrenceCount\":2,\"WaitRange\":null,\"TotalOccurrences\":null,\"IncidentStartedUtc\":null,\"Database\":null,\"LastEventUtc\":null}]}";

        Assert.Null(AlertContextSerializer.TryReadSeverity(legacy));

        Assert.True(AlertContextSerializer.TryDeserialize(legacy, out var context));
        Assert.Null(context.SeverityOverride);
        var detail = Assert.Single(context.Details);
        Assert.Equal("THREADPOOL", detail.Heading);
        Assert.Equal(("Avg wait", "600 ms"), Assert.Single(detail.Fields));
        var incident = Assert.Single(context.Incidents!);
        Assert.Equal("abc", incident.DedupKey);
        Assert.Equal(2, incident.OccurrenceCount);
    }

    /// <summary>A fire with no override persists a null member and reads back none — the same state a
    /// legacy row is in, which is why one fallback serves both.</summary>
    [Fact]
    public void AFireWithNoOverride_CarriesNoTier()
    {
        var json = WithSeverity(null);
        Assert.Null(AlertContextSerializer.TryReadSeverity(json));
        Assert.True(AlertContextSerializer.TryDeserialize(json, out var restored));
        Assert.Null(restored.SeverityOverride);
    }

    /// <summary>The cheap read refuses everything that is not the writer's exact spelling — garbage, a bare
    /// ordinal (the coupling the string form exists to avoid), a case variant, a non-object root — and a
    /// resolution row's null context. It must never throw: the grids call it once per row.</summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("not json")]
    [InlineData("[]")]
    [InlineData("{\"Details\":[]}")]
    [InlineData("{\"Details\":[],\"Severity\":null}")]
    [InlineData("{\"Details\":[],\"Severity\":1}")]
    [InlineData("{\"Details\":[],\"Severity\":\"1\"}")]
    [InlineData("{\"Details\":[],\"Severity\":\"critical\"}")]
    [InlineData("{\"Details\":[],\"Severity\":\"Fatal\"}")]
    public void TheCheapRead_AnswersNull_ForAnythingThatIsNotAPersistedTier(string? json)
    {
        Assert.Null(AlertContextSerializer.TryReadSeverity(json));
        Assert.Null(AlertHistoryRowSeverity.FiredAt(json));
    }

    /* ─────────────────────────── the row's severity ─────────────────────────── */

    /// <summary>
    /// The headline case: a Poison Wait row that fired WARNING is a warning row, whatever the name says;
    /// one that fired CRITICAL is critical; one that carries no tier is critical BY NAME — every SQL Server
    /// Poison Wait row written before #3539 A4 was a presence-flat critical fire, so the fallback is the
    /// faithful replay for exactly the rows that reach it.
    /// </summary>
    [Fact]
    public void APoisonWaitRow_TakesTheTierItFiredAt_AndTheNameOnlyWhenItHasNone()
    {
        var warning = WithSeverity(AlertSeverityLevel.Warning);
        Assert.True(AlertHistoryRowSeverity.IsWarning("Poison Wait", warning));
        Assert.False(AlertHistoryRowSeverity.IsCritical("Poison Wait", warning));
        Assert.Equal(("warning", AlertHistoryRowSeverity.SourceFired), AlertHistoryRowSeverity.Describe("Poison Wait", warning));

        var critical = WithSeverity(AlertSeverityLevel.Critical);
        Assert.True(AlertHistoryRowSeverity.IsCritical("Poison Wait", critical));
        Assert.False(AlertHistoryRowSeverity.IsWarning("Poison Wait", critical));
        Assert.Equal(("critical", AlertHistoryRowSeverity.SourceFired), AlertHistoryRowSeverity.Describe("Poison Wait", critical));

        /* No tier on the row: the by-name arm, and it says so. */
        Assert.True(AlertHistoryRowSeverity.IsCritical("Poison Wait", null));
        Assert.True(AlertMetricClassifier.IsCritical("Poison Wait"));   // the premise the fallback rests on
        Assert.Equal(("critical", AlertHistoryRowSeverity.SourceMetricName), AlertHistoryRowSeverity.Describe("Poison Wait", null));
    }

    /// <summary>The other direction of the same gap: metrics the engine grades ABOVE their name's colour.
    /// A CRITICAL-graded low-disk fire (#1136) and a SUSPECT database (Database State's critical arm) were
    /// amber rows; with the tier on the row they are red. Their override-less rows keep the name's amber,
    /// which is what the channels rendered for them too.</summary>
    [Theory]
    [InlineData("Volume Free Space")]
    [InlineData("Database State")]
    public void AMetricGradedAboveItsName_RendersTheGradeItFiredAt(string metric)
    {
        Assert.True(AlertMetricClassifier.IsWarning(metric));   // the name alone says amber

        var critical = WithSeverity(AlertSeverityLevel.Critical);
        Assert.True(AlertHistoryRowSeverity.IsCritical(metric, critical));
        Assert.False(AlertHistoryRowSeverity.IsWarning(metric, critical));

        Assert.False(AlertHistoryRowSeverity.IsCritical(metric, null));
        Assert.True(AlertHistoryRowSeverity.IsWarning(metric, null));
    }

    /// <summary>A row with NO tier — written before its metric was graded, or a metric still presence-flat
    /// (Blocking Detected) — keeps the name's colour exactly, and says the name decided. For the three
    /// metrics #3653 graded at the engine (Deadlocks Detected, High CPU, tempdb Space) this is now the
    /// REPLAY arm for pre-#3653 rows: every such deadlock row rendered red, every CPU and tempdb row amber,
    /// so the by-name reading is the faithful one for exactly the rows that reach it.</summary>
    [Theory]
    [InlineData("Deadlocks Detected", true)]
    [InlineData("High CPU", false)]
    [InlineData("tempdb Space", false)]
    [InlineData("Blocking Detected", false)]
    public void ARowWithNoTier_KeepsItsByNameColour(string metric, bool criticalByName)
    {
        Assert.Equal(criticalByName, AlertHistoryRowSeverity.IsCritical(metric, null));
        Assert.Equal(!criticalByName, AlertHistoryRowSeverity.IsWarning(metric, null));
        Assert.Equal(criticalByName, AlertMetricClassifier.IsCritical(metric));
        Assert.Equal(
            (criticalByName ? "critical" : "warning", AlertHistoryRowSeverity.SourceMetricName),
            AlertHistoryRowSeverity.Describe(metric, null));
    }

    /// <summary>
    /// #3653 (A8e), the engine's half landing on the grid: a Deadlocks Detected row that fired WARNING (one
    /// deadlock at a count knob of 1) is an amber row despite the name's red, and a High CPU row that fired
    /// CRITICAL (at the band's 95% bar) is red despite the name's amber — the same projection the Poison Wait
    /// case above proved, applied to the two metrics whose grades run in opposite directions from their
    /// names. tempdb grades only Warning, which agrees with its name; the row still says the FIRE decided.
    /// </summary>
    [Fact]
    public void AGradedDeadlockOrCpuRow_RendersTheTierItFiredAt_NotTheNames()
    {
        var warning = WithSeverity(AlertSeverityLevel.Warning);
        Assert.True(AlertMetricClassifier.IsCritical("Deadlocks Detected"));   // the name alone says red
        Assert.True(AlertHistoryRowSeverity.IsWarning("Deadlocks Detected", warning));
        Assert.False(AlertHistoryRowSeverity.IsCritical("Deadlocks Detected", warning));
        Assert.Equal(("warning", AlertHistoryRowSeverity.SourceFired), AlertHistoryRowSeverity.Describe("Deadlocks Detected", warning));

        var critical = WithSeverity(AlertSeverityLevel.Critical);
        Assert.True(AlertMetricClassifier.IsWarning("High CPU"));               // the name alone says amber
        Assert.True(AlertHistoryRowSeverity.IsCritical("High CPU", critical));
        Assert.False(AlertHistoryRowSeverity.IsWarning("High CPU", critical));
        Assert.Equal(("critical", AlertHistoryRowSeverity.SourceFired), AlertHistoryRowSeverity.Describe("High CPU", critical));

        Assert.True(AlertHistoryRowSeverity.IsWarning("tempdb Space", warning));
        Assert.Equal(("warning", AlertHistoryRowSeverity.SourceFired), AlertHistoryRowSeverity.Describe("tempdb Space", warning));
    }

    /// <summary>Resolution rows are the name's business and never consult a tier — they are persisted with
    /// a null context, and even a stray tier on one must not turn "Poison Waits Cleared" red. The two
    /// deliberate INFO reports fire with no override and stay unhighlighted, as
    /// <see cref="AlertMetricClassifier.IsInformational"/> intends.</summary>
    [Fact]
    public void ResolutionAndInformationalRows_AreTheNamesBusiness()
    {
        foreach (var json in new[] { null, WithSeverity(AlertSeverityLevel.Critical) })
        {
            Assert.False(AlertHistoryRowSeverity.IsCritical("Poison Waits Cleared", json));
            Assert.False(AlertHistoryRowSeverity.IsWarning("Poison Waits Cleared", json));
            Assert.Equal(("resolution", AlertHistoryRowSeverity.SourceMetricName), AlertHistoryRowSeverity.Describe("Poison Waits Cleared", json));
        }

        Assert.False(AlertHistoryRowSeverity.IsCritical("Collector Cost Digest", null));
        Assert.False(AlertHistoryRowSeverity.IsWarning("Collector Cost Digest", null));
        Assert.Equal(("info", AlertHistoryRowSeverity.SourceMetricName), AlertHistoryRowSeverity.Describe("Collector Cost Digest", null));
        Assert.Equal(("info", AlertHistoryRowSeverity.SourceMetricName), AlertHistoryRowSeverity.Describe("Fleet Sweep Rollup", null));
    }

    /* ─────────────────────────── the Viewer's row ─────────────────────────── */

    private static ViewerAlertRow ViewerRow(string metric, string? contextJson) => new()
    {
        AlertTime = new DateTime(2026, 9, 18, 12, 0, 0),
        MetricName = metric,
        CurrentValue = 61_000,
        ThresholdValue = 60_000,
        AlertSent = true,
        NotificationType = "webhook",
        Muted = false,
        ContextJson = contextJson,
    };

    /// <summary>The Viewer's grid row reaches the shared decision: a Warning-graded Poison Wait row is
    /// amber, not red; a row with no tier keeps the name's red. Lite's <c>AlertHistoryRow</c> is pinned the
    /// same way in Lite.Tests (<c>AlertHistoryRowSeverityLiteTests</c>).</summary>
    [Fact]
    public void TheViewerGridRow_RendersTheTierTheAlertFiredAt()
    {
        var graded = ViewerRow("Poison Wait", WithSeverity(AlertSeverityLevel.Warning));
        Assert.True(graded.IsWarning);
        Assert.False(graded.IsCritical);
        Assert.False(graded.IsResolved);

        var legacy = ViewerRow("Poison Wait", null);
        Assert.True(legacy.IsCritical);
        Assert.False(legacy.IsWarning);

        var cleared = ViewerRow("Poison Waits Cleared", null);
        Assert.True(cleared.IsResolved);
        Assert.False(cleared.IsCritical);
        Assert.False(cleared.IsWarning);
    }

    /* ─────────────────────────── one decision, every grid ─────────────────────────── */

    /// <summary>
    /// Both SKUs' grid rows reach <see cref="AlertHistoryRowSeverity"/>, and NO shipping file under Lite/ or
    /// Darling/ calls the by-name <c>IsCritical</c> / <c>IsWarning</c> predicates directly any more — a
    /// grid that did would be back to colouring a Warning-graded row red. The deprecated Dashboard is
    /// outside the census on purpose: its own engine still fires Poison Wait presence-flat CRITICAL, so its
    /// by-name red is faithful to its own rows, and it is a frozen twin.
    /// </summary>
    [Fact]
    public void BothGridRows_ReachTheSharedDecision_AndNoGridStillClassifiesByNameAlone()
    {
        var rows = new[]
        {
            Path.Combine("Lite", "Services", "LocalDataService.AlertHistory.cs"),
            Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.AlertHistory.cs"),
        };
        foreach (var relative in rows)
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile(relative.Split(Path.DirectorySeparatorChar)));
            Assert.Contains("AlertHistoryRowSeverity.IsCritical(MetricName, ContextJson)", code, StringComparison.Ordinal);
            Assert.Contains("AlertHistoryRowSeverity.IsWarning(MetricName, ContextJson)", code, StringComparison.Ordinal);
        }

        var byNameCallers = new[] { "Lite", "Darling" }
            .SelectMany(root => Directory.EnumerateFiles(Path.Combine(RepoFile.Root, root), "*.cs", SearchOption.AllDirectories))
            .Where(f =>
            {
                var segments = f.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
                return !segments.Contains("bin") && !segments.Contains("obj")
                    && !segments.Contains("Darling.Tests") && !segments.Contains("Lite.Tests");
            })
            .Where(f =>
            {
                var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(f));
                return code.Contains("AlertMetricClassifier.IsCritical(", StringComparison.Ordinal)
                    || code.Contains("AlertMetricClassifier.IsWarning(", StringComparison.Ordinal);
            })
            .Select(f => Path.GetRelativePath(RepoFile.Root, f))
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToArray();

        Assert.Empty(byNameCallers);
    }
}
