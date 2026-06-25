using System.Collections.Generic;
using PerformanceMonitor.Analysis;
using PerformanceMonitorDashboard.Services.Recommendations;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// Tests the shared "what else fired in this window" cross-reference (correlate-and-focus slice 1,
/// review §1d): the pure <see cref="CoFiredSummary"/> helper and the Dashboard reader's per-card
/// append. The Lite reader's append is covered in LiteRecommendationsReaderTests.
/// </summary>
public class CoFiredSummaryTests
{
    [Fact]
    public void OtherTitles_ExcludesSelf_OrdersBySeverityDesc_Dedups()
    {
        var window = new List<(string, double)> { ("A", 0.5), ("B", 1.9), ("C", 0.8), ("B", 1.0) };
        var others = CoFiredSummary.OtherTitles("A", window);
        Assert.Equal(new[] { "B", "C" }, others); // self A excluded; B(1.9) before C(0.8); duplicate B collapsed
    }

    [Fact]
    public void Line_CapsAndCountsTheRest()
    {
        var others = new List<string> { "A", "B", "C", "D", "E" };
        Assert.Equal(
            "Also surfaced in this analysis window: A; B; C (+2 more).",
            CoFiredSummary.Line(others, cap: 3));
    }

    [Fact]
    public void Line_NoExtra_OmitsTheCount()
    {
        Assert.Equal(
            "Also surfaced in this analysis window: A; B.",
            CoFiredSummary.Line(new List<string> { "A", "B" }, cap: 3));
    }

    [Fact]
    public void Line_NullForEmpty()
    {
        Assert.Null(CoFiredSummary.Line(new List<string>()));
    }

    [Fact]
    public void DashboardReader_AppendCoFired_AppendsTheOthersToEachCard()
    {
        var items = new List<RecommendationItem>
        {
            new() { Title = "High problem", RawSeverity = 1.9, AdviceText = "fix high." },
            new() { Title = "Low problem", RawSeverity = 0.5, AdviceText = "fix low." },
        };

        RecommendationsReader.AppendCoFired(items);

        Assert.Contains("fix high.", items[0].AdviceText);
        Assert.Contains("Also surfaced in this analysis window: Low problem.", items[0].AdviceText);
        Assert.Contains("Also surfaced in this analysis window: High problem.", items[1].AdviceText);
    }

    [Fact]
    public void DashboardReader_AppendCoFired_SingleCard_NoOp()
    {
        var items = new List<RecommendationItem>
        {
            new() { Title = "Only problem", RawSeverity = 1.0, AdviceText = "fix it." },
        };

        RecommendationsReader.AppendCoFired(items);

        Assert.Equal("fix it.", items[0].AdviceText); // nothing else fired -> unchanged
    }
}
