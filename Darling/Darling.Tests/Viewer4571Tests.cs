using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Wait Stats card's collapsible header text (#4571), matching
/// erikdarlingdata/PerformanceStudio@9b8252e: "Wait Stats" alone when empty, "Wait Stats \u2014 Nms total"
/// when there's a total wait time to show (matching the desktop viewer's PlanViewerControl.WaitStats.cs).
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
        Assert.Equal("Wait Stats \u2014 1,234ms total", PlanDisplayText.WaitStatsHeader(3, 1234));
    }
}
