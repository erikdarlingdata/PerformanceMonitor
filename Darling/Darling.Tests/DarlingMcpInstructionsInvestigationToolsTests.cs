using System;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8463: the instructions name every tool in <see cref="DarlingMcpWritePolicy.InvestigationTools"/>: in the settings
/// paragraph every client reads (<see cref="DarlingMcpWritePolicy.InstructionsLine"/>, carried by
/// <see cref="DarlingMcpInstructions.Text"/>) and in the agent-role paragraph a named token reads
/// (<see cref="DarlingMcpWritePolicy.NamedInstructionsLine"/>). Both lists are built from the set, so a tool added to it
/// is named without an edit to the text. <c>DarlingMcpNamedTokenTests</c> pins the converse: the operator paragraph
/// names none of them.
/// </summary>
public sealed class DarlingMcpInstructionsInvestigationToolsTests
{
    private const string Token = "0123456789abcdef0123456789abcdef";

    [Fact]
    public void SettingsParagraph_NamesEveryInvestigationTool()
    {
        Assert.All(DarlingMcpWritePolicy.InvestigationTools,
            name => Assert.Contains(name, DarlingMcpWritePolicy.InstructionsLine, StringComparison.Ordinal));
        Assert.All(DarlingMcpWritePolicy.InvestigationTools,
            name => Assert.Contains(name, DarlingMcpInstructions.Text, StringComparison.Ordinal));
    }

    [Fact]
    public void AgentParagraph_NamesEveryInvestigationTool()
    {
        var agent = DarlingMcpWritePolicy.ResolveForStart(new McpConfig { AllowInvestigations = true }, Token, out _);
        Assert.True(agent.Investigations);

        var line = DarlingMcpWritePolicy.NamedInstructionsLine(agent);
        Assert.All(DarlingMcpWritePolicy.InvestigationTools,
            name => Assert.Contains(name, line, StringComparison.Ordinal));
    }
}
