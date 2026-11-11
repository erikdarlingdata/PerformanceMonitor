using System;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8610: the texts that say which tools the server admin switch (<c>mcp.allowServerAdmin</c>) governs name every tool in
/// <see cref="DarlingMcpWritePolicy.ServerAdminTools"/>: the settings paragraph every client reads
/// (<see cref="DarlingMcpWritePolicy.InstructionsLine"/>) and the start's log lines
/// (<see cref="DarlingMcpHostService.LogWritesGate"/>). They said three tools while the set holds four, the way the
/// investigation paragraph once left a tool out (<c>DarlingMcpInstructionsInvestigationToolsTests</c>). Both are built from the set.
/// </summary>
public sealed class DarlingMcpWriteGateTextTests
{
    private const string Token = "0123456789abcdef0123456789abcdef";

    [Fact]
    public void SettingsParagraph_NamesEveryServerAdminTool()
    {
        Assert.Equal(4, DarlingMcpWritePolicy.ServerAdminTools.Count);
        Assert.All(DarlingMcpWritePolicy.ServerAdminTools,
            name => Assert.Contains(name, DarlingMcpWritePolicy.InstructionsLine, StringComparison.Ordinal));
        Assert.All(DarlingMcpWritePolicy.ServerAdminTools,
            name => Assert.Contains(name, DarlingMcpInstructions.Text, StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(true, "set_server_action on via mcp.allowServerAdmin")]
    [InlineData(false, "set_server_action off")]
    public void Start_WritesServed_WarnsOnce_NamingEveryServerAdminTool(bool serverAdmin, string state)
    {
        var mcp = new McpConfig { AllowWrites = true, AllowServerAdmin = serverAdmin };
        var logger = new CapturingTestLogger();

        DarlingMcpHostService.LogWritesGate(logger, mcp, DarlingMcpWritePolicy.ResolveForStart(mcp, Token, out var problem), problem);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Critical));
        Assert.All(DarlingMcpWritePolicy.ServerAdminTools,
            name => Assert.Contains(name, logger.Joined, StringComparison.Ordinal));
        Assert.Contains(state, logger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public void Start_WritesOff_LogsOneInformationLine_NamingEveryServerAdminTool()
    {
        var mcp = new McpConfig();
        var logger = new CapturingTestLogger();

        DarlingMcpHostService.LogWritesGate(logger, mcp, DarlingMcpWritePolicy.ResolveForStart(mcp, Token, out var problem), problem);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Information));
        Assert.Single(logger.Lines);
        Assert.All(DarlingMcpWritePolicy.ServerAdminTools,
            name => Assert.Contains(name, logger.Joined, StringComparison.Ordinal));
    }

    /// <summary>The floor is a length and a count of different characters, and the Critical lines for a start that has no usable token say both.</summary>
    [Fact]
    public void Start_WritesOnWithNoToken_IsCritical_AndNamesBothHalvesOfTheFloor()
    {
        var mcp = new McpConfig { AllowWrites = true };
        var logger = new CapturingTestLogger();

        DarlingMcpHostService.LogWritesGate(logger, mcp, DarlingMcpWritePolicy.ResolveForStart(mcp, "", out var problem), problem);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Critical));
        Assert.Contains("write tools stay off", logger.Joined, StringComparison.Ordinal);
        Assert.Contains("at least 16 characters with at least 6 different ones", logger.Joined, StringComparison.Ordinal);
    }
}
