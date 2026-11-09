using System;
using System.Linq;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8463: the investigation paragraph of the rendered instructions names every investigation tool the policy can serve, the
/// reads <see cref="McpInvestigationPolicy.ReadTools"/> serves in every configuration and the tools
/// <see cref="McpInvestigationPolicy.InvestigationTools"/> serves under the setting, and states the gate
/// <see cref="McpHostService.DecideServing"/> enforces for the latter: a usable token, and the loopback opt-out off. The reads
/// sentence and the census count are built from those sets, so a tool added to or removed from them is named or counted
/// without an edit to the text.
/// </summary>
public sealed class McpInstructionsInvestigationGateTests
{
    /// <summary>The instructions as a host that serves the tools that claim and report renders them.</summary>
    private static string Rendered()
    {
        var served = McpInvestigationPolicy.InvestigationTools.Concat(McpInvestigationPolicy.ReadTools).ToList();
        return McpInstructions.Build(served, served);
    }

    /// <summary>The investigation paragraph alone: from its heading to the next section.</summary>
    private static string Paragraph(string text)
    {
        const string Heading = "### Investigation requests";
        var start = text.IndexOf(Heading, StringComparison.Ordinal);
        Assert.True(start >= 0, "the rendered instructions have no investigation paragraph");
        var end = text.IndexOf("## Workflow", start, StringComparison.Ordinal);
        return end < 0 ? text[start..] : text[start..end];
    }

    [Fact]
    public void InvestigationParagraph_NamesEveryInvestigationToolThePolicyCanServe()
    {
        var paragraph = Paragraph(Rendered());

        Assert.All(McpInvestigationPolicy.InvestigationTools.Concat(McpInvestigationPolicy.ReadTools),
            name => Assert.Contains($"`{name}`", paragraph, StringComparison.Ordinal));
    }

    [Fact]
    public void InvestigationParagraph_StatesTheTokenRule()
    {
        var paragraph = Paragraph(Rendered());

        Assert.Contains(McpTokenStore.TokenRequirement, paragraph, StringComparison.Ordinal);
        Assert.Contains($"`{McpSettings.AllowTokenlessLoopbackSetting}`", paragraph, StringComparison.Ordinal);
    }

    [Fact]
    public void ReferenceText_CountsTheToolsTheSettingServesFromTheSet()
    {
        Assert.Contains(
            $"`{McpInvestigationPolicy.InvestigationsSetting}` the {McpInvestigationPolicy.InvestigationTools.Count} claim and report tools",
            McpInstructions.Text, StringComparison.Ordinal);
    }
}
