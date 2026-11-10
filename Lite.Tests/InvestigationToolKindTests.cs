using System;
using System.Linq;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8461: Lite's investigation gate is built from the shared kind list (<see cref="InvestigationToolNames"/>), the one both apps read,
/// so a shared name is gated by the same rule in Darling. Every investigation tool Lite declares is placed on that list as the kind
/// Lite treats it as: the state-changing ones are the gated set and the reads are on the read-only list, so the next investigation
/// tool added to either app fails here until it is placed. Darling's twin is <c>Darling/Darling.Tests/InvestigationToolKindTests.cs</c>.
/// </summary>
public sealed class InvestigationToolKindTests
{
    private static string[] Declared() => typeof(McpInvestigationTools).GetMethods()
        .Select(m => m.GetCustomAttributes(typeof(ModelContextProtocol.Server.McpServerToolAttribute), false)
            .Cast<ModelContextProtocol.Server.McpServerToolAttribute>().SingleOrDefault()?.Name)
        .OfType<string>()
        .Order(StringComparer.Ordinal)
        .ToArray();

    [Fact]
    public void EveryDeclaredInvestigationTool_IsPlacedOnTheSharedList()
    {
        var declared = Declared();

        Assert.NotEmpty(declared);
        Assert.All(declared, name => Assert.True(InvestigationToolNames.IsPlaced(name), $"{name} is neither a shared read nor a shared state-changing tool; place it in InvestigationToolNames"));
    }

    [Fact]
    public void TheGatedSet_IsTheSharedStateChangingTools_LiteHas_AndNothingElse()
    {
        var declared = Declared();
        var expected = InvestigationToolNames.StateChanging.Where(declared.Contains).Order(StringComparer.Ordinal).ToArray();

        Assert.Equal(expected, McpInvestigationPolicy.InvestigationTools.Order(StringComparer.Ordinal));
        Assert.All(McpInvestigationPolicy.InvestigationTools, name => Assert.True(InvestigationToolNames.IsStateChanging(name), name));
        Assert.DoesNotContain(McpInvestigationPolicy.InvestigationTools, InvestigationToolNames.IsRead);
    }

    [Fact]
    public void TheDeclaredReads_AreTheSharedReadsLiteHas_AndAreOnTheReadOnlyList()
    {
        var declared = Declared();
        var reads = declared.Where(InvestigationToolNames.IsRead).ToArray();

        Assert.Equal(new[] { "get_investigation_reports", "list_investigation_requests" }, reads);
        Assert.All(reads, name => Assert.Contains(name, McpToolClassification.ReadOnlyTools));
        Assert.Empty(McpToolClassification.ReadOnlyTools.Intersect(InvestigationToolNames.StateChanging, StringComparer.Ordinal));
    }
}
