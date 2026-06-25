using System.Collections.Generic;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// WS4: the shared PlanAdvisoryAggregator parses collected plan XML into missing-index / warning
/// aggregates that drive the MISSING_INDEX / PLAN_WARNING facts and drill-down. These cover its
/// resilience contract — a malformed, empty, or null plan never throws and is simply skipped, so
/// one bad plan can't abort the fact-collection batch.
///
/// The real parse→emit over production SHOWPLAN XML is exercised by the live analysis path (the
/// collectors run the same ShowPlanParser the Plan Viewer/drill-down already use), not a hand-built
/// fixture: there are no plan-XML fixtures in the repo and the parser's structural/namespace
/// expectations make a faithful minimal one fragile and misleading.
/// </summary>
public class PlanAdvisoryAggregatorTests
{
    [Fact]
    public void Summarize_EmptyInput_ReturnsZeros()
    {
        var summary = PlanAdvisoryAggregator.Summarize(new List<string>());

        Assert.Equal(0, summary.MissingIndexCount);
        Assert.Equal(0, summary.WarningCount);
        Assert.Equal(0, summary.CriticalCount);
        Assert.Equal(0.0, summary.MaxImpact);
    }

    [Fact]
    public void Extract_MalformedOrNullPlans_AreSkipped_NoThrow()
    {
        var plans = new List<string>
        {
            null!,
            "",
            "   ",
            "<not-a-plan/>",
            "<ShowPlanXML>truncated...",
            "{\"json\":true}"
        };

        var ex = Record.Exception(() =>
        {
            var details = PlanAdvisoryAggregator.Extract(plans);
            Assert.Empty(details.MissingIndexes);
            Assert.Empty(details.Warnings);
        });

        Assert.Null(ex);
    }
}
