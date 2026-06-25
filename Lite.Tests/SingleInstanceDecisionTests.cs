using System;
using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Unit coverage for the version-aware single-instance upgrade-handoff decision
/// (plans/single-instance-upgrade-handoff.md). The decision is a pure function over already-measured
/// inputs; the brittle Win32 measurement (Win32ProcessInspector) is exercised separately at runtime.
/// </summary>
public class SingleInstanceDecisionTests
{
    private const int Medium = 0x2000;
    private const int High = 0x3000;

    private static Version V(string s) => Version.Parse(s);

    [Fact]
    public void OlderAtSameIntegrity_TakesOver()
    {
        var action = SingleInstanceDecision.Decide(V("3.1.0"), V("3.0.0"), Medium, Medium, canElevateSameUser: false);
        Assert.Equal(HandoffAction.TakeOver, action);
    }

    [Fact]
    public void OlderAtLowerIntegrity_TakesOver()
    {
        // We're High, the old one is Medium — reachable.
        var action = SingleInstanceDecision.Decide(V("3.1.0"), V("3.0.0"), High, Medium, canElevateSameUser: false);
        Assert.Equal(HandoffAction.TakeOver, action);
    }

    [Fact]
    public void OlderButHigherIntegrity_WhenCanElevate_OffersRunas()
    {
        var action = SingleInstanceDecision.Decide(V("3.1.0"), V("3.0.0"), Medium, High, canElevateSameUser: true);
        Assert.Equal(HandoffAction.IntegrityErrorWithRunas, action);
    }

    [Fact]
    public void OlderButHigherIntegrity_WhenCannotElevate_ManualOnly()
    {
        var action = SingleInstanceDecision.Decide(V("3.1.0"), V("3.0.0"), Medium, High, canElevateSameUser: false);
        Assert.Equal(HandoffAction.IntegrityErrorManualOnly, action);
    }

    [Fact]
    public void OlderButIntegrityUnmeasurable_Surfaces()
    {
        // Couldn't read the other instance's IL → don't guess; surface (no false UAC).
        var action = SingleInstanceDecision.Decide(V("3.1.0"), V("3.0.0"), Medium, null, canElevateSameUser: true);
        Assert.Equal(HandoffAction.Surface, action);
    }

    [Fact]
    public void SameVersion_Surfaces_RegardlessOfIntegrity()
    {
        Assert.Equal(HandoffAction.Surface, SingleInstanceDecision.Decide(V("3.0.0"), V("3.0.0"), Medium, Medium, false));
        // An already-running elevated same-version instance must NOT be misclassified as a mismatch.
        Assert.Equal(HandoffAction.Surface, SingleInstanceDecision.Decide(V("3.0.0"), V("3.0.0"), Medium, High, true));
    }

    [Fact]
    public void NewerRunningInstance_Surfaces()
    {
        // An older build launched over a newer one must not evict it.
        var action = SingleInstanceDecision.Decide(V("3.0.0"), V("3.1.0"), Medium, Medium, canElevateSameUser: true);
        Assert.Equal(HandoffAction.Surface, action);
    }

    [Fact]
    public void UnreadableVersions_Surface()
    {
        Assert.Equal(HandoffAction.Surface, SingleInstanceDecision.Decide(null, V("3.0.0"), Medium, Medium, false));
        Assert.Equal(HandoffAction.Surface, SingleInstanceDecision.Decide(V("3.0.0"), null, Medium, Medium, false));
    }

    [Theory]
    [InlineData("3.0.1", "3.0.1")]
    [InlineData("3.0.1.4", "3.0.1.4")]
    [InlineData("3.0.1-nightly.20260618", "3.0.1")]   // pre-release suffix stripped
    [InlineData("3.0.1+abc1234", "3.0.1")]            // build metadata stripped
    [InlineData("  3.0.1  ", "3.0.1")]                // trimmed
    public void ParseProductVersion_ParsesNumericCore(string raw, string expected)
    {
        Assert.Equal(Version.Parse(expected), SingleInstanceDecision.ParseProductVersion(raw));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("not-a-version")]
    [InlineData("-nightly")]
    public void ParseProductVersion_ReturnsNullForUnparseable(string? raw)
    {
        Assert.Null(SingleInstanceDecision.ParseProductVersion(raw));
    }

    [Fact]
    public void DriftedFileVersionButBumpedProductVersion_StillOlder()
    {
        // The C1 regression guard: compare ProductVersion (the bumped <Version>), even if other
        // version fields drift. Here the running build's resolved version is genuinely older.
        var ours = SingleInstanceDecision.ParseProductVersion("3.1.0+ci");
        var other = SingleInstanceDecision.ParseProductVersion("3.0.0+ci");
        Assert.Equal(HandoffAction.TakeOver, SingleInstanceDecision.Decide(ours, other, Medium, Medium, false));
    }
}
