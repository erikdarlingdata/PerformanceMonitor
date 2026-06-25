using System.Collections.Generic;
using PerformanceMonitor.Analysis;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// Tests the incident id (correlate-and-focus slice 2): one analysis run = one incident, so every
/// non-absolution story shares the id; the id fingerprints the primary (highest-severity) finding +
/// database so the same recurring incident is trackable across runs.
/// </summary>
public class IncidentIdTests
{
    private static AnalysisStory Story(string key, double sev, string? db = null, bool absolution = false) =>
        new() { RootFactKey = key, Severity = sev, DatabaseName = db, IsAbsolution = absolution };

    [Fact]
    public void StampStories_StampsAllNonAbsolution_WithTheSameId()
    {
        var stories = new List<AnalysisStory> { Story("CPU_SQL_PERCENT", 1.6), Story("BLOCKING_EVENTS", 1.0) };
        IncidentId.StampStories("SQL1", stories);

        Assert.NotEmpty(stories[0].IncidentId);
        Assert.Equal(stories[0].IncidentId, stories[1].IncidentId); // one incident per run
    }

    [Fact]
    public void StampStories_SamePrimaryProblem_SameIdAcrossRuns()
    {
        // Different co-fired members, SAME primary (CPU on Sales) -> trackable as one recurring incident.
        var run1 = new List<AnalysisStory> { Story("CPU_SQL_PERCENT", 1.6, "Sales"), Story("CXPACKET", 0.9) };
        var run2 = new List<AnalysisStory> { Story("CPU_SQL_PERCENT", 1.7, "Sales"), Story("WRITELOG", 0.8) };
        IncidentId.StampStories("SQL1", run1);
        IncidentId.StampStories("SQL1", run2);

        Assert.Equal(run1[0].IncidentId, run2[0].IncidentId);
    }

    [Fact]
    public void StampStories_DifferentPrimaryServerOrDb_DifferentId()
    {
        var a = new List<AnalysisStory> { Story("CPU_SQL_PERCENT", 1.6, "Sales") };
        var b = new List<AnalysisStory> { Story("CPU_SQL_PERCENT", 1.6, "Orders") }; // different database
        var c = new List<AnalysisStory> { Story("BLOCKING_EVENTS", 1.6, "Sales") }; // different primary key
        IncidentId.StampStories("SQL1", a);
        IncidentId.StampStories("SQL1", b);
        IncidentId.StampStories("SQL2", c);

        Assert.NotEqual(a[0].IncidentId, b[0].IncidentId);
        Assert.NotEqual(a[0].IncidentId, c[0].IncidentId);
    }

    [Fact]
    public void StampStories_AbsolutionOnly_LeavesIdsEmpty()
    {
        var stories = new List<AnalysisStory> { Story("server_health", 0, absolution: true) };
        IncidentId.StampStories("SQL1", stories);
        Assert.Equal(string.Empty, stories[0].IncidentId);
    }

    [Fact]
    public void Compute_IsDeterministic_OnSeverityTies()
    {
        var s1 = new List<AnalysisStory> { Story("AAA", 1.0), Story("BBB", 1.0) };
        var s2 = new List<AnalysisStory> { Story("BBB", 1.0), Story("AAA", 1.0) }; // same set, reversed order
        Assert.Equal(IncidentId.Compute("SQL1", s1), IncidentId.Compute("SQL1", s2)); // tiebroken by root key
    }

    // ── StampClusters: per-component ids (the clustering refinement) ──

    [Fact]
    public void StampClusters_GivesEachComponentItsOwnId()
    {
        var component1 = new List<AnalysisStory> { Story("CPU_SQL_PERCENT", 1.6), Story("CXPACKET", 0.9) };
        var component2 = new List<AnalysisStory> { Story("DISK_SPACE", 1.5) };

        IncidentId.StampClusters("SQL1", new[] { component1, component2 });

        Assert.NotEmpty(component1[0].IncidentId);
        Assert.Equal(component1[0].IncidentId, component1[1].IncidentId); // same incident -> same id
        Assert.NotEqual(component1[0].IncidentId, component2[0].IncidentId); // different incidents -> different ids
    }
}
