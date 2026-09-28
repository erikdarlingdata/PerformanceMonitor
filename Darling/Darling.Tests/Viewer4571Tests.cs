using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Wait Stats card's collapsible header text (#4571), matching
/// erikdarlingdata/PerformanceStudio@9b8252e: "Wait Stats" alone when empty, "Wait Stats N ms"
/// when there's a total wait time to show.
/// </summary>
public class Viewer4571Tests
{
    [Fact]
    public void WaitStatsHeader_NoWaits_IsPlainLabel()
    {
        Assert.Equal("Wait Stats", PlanDisplayText.WaitStatsHeader(0, 0));
    }

    [Fact]
    public void WaitStatsHeader_WithWaits_AppendsTotalMs()
    {
        Assert.Equal("Wait Stats 1,234 ms", PlanDisplayText.WaitStatsHeader(3, 1234));
    }
}
