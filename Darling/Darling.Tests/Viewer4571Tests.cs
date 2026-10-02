using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Wait Stats card header's tooltip (#4571), matching erikdarlingdata/PerformanceStudio's
/// desktop viewer: null when there are no waits, otherwise the total wait time and wait-type count.
/// </summary>
public class Viewer4571Tests
{
    [Fact]
    public void WaitStatsHeaderTooltip_NoWaits_IsNull()
    {
        Assert.Null(PlanDisplayText.WaitStatsHeaderTooltip(0, 0));
    }

    [Fact]
    public void WaitStatsHeaderTooltip_WithWaits_DescribesCountAndTotal()
    {
        Assert.Equal("1,234 ms of waits across 3 wait types", PlanDisplayText.WaitStatsHeaderTooltip(3, 1234));
    }
}
