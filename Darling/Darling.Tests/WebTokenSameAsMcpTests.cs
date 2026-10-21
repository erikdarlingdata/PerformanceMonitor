/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6265 round 2: a web token with the MCP token's value is refused for web sign-in, because an MCP agent holds the MCP token and the
/// web token signs in as a person. The sign-in refusal is run through the real pipeline in <see cref="DarlingWebSignInHardeningTests"/>;
/// this class pins the decision the start-up warning and <c>--validate-config</c> share (a warning that names a different web token,
/// never an error, so an upgrade still starts).
/// </summary>
public sealed class WebTokenSameAsMcpTests
{
    private const string Shared = "a-shared-token-value-0123456789abcdef";

    private static DarlingConfig Config(string? web, string? mcp) => new()
    {
        Web = new WebConfig { Network = web is null ? null : new WebNetworkConfig { Token = web } },
        Mcp = new McpConfig { Network = mcp is null ? null : new McpNetworkConfig { Token = mcp } },
    };

    [Fact]
    public void ASharedValue_IsFound_AsAWarningSentenceThatTellsTheOperatorToSetADifferentWebToken()
    {
        var finding = DarlingAgentDispatchChecks.WebTokenFinding(Config(Shared, Shared));
        Assert.Equal(DarlingAgentDispatchChecks.WebTokenSameAsMcpMessage, finding);
        Assert.Contains("Set a different web token", finding, System.StringComparison.Ordinal);
        Assert.DoesNotContain(Shared, finding, System.StringComparison.Ordinal);
    }

    [Fact]
    public void ADifferentValue_ANoneSet_OrOnlyOneSide_IsNotAFinding()
    {
        Assert.Null(DarlingAgentDispatchChecks.WebTokenFinding(Config(Shared, Shared + "-other")));
        Assert.Null(DarlingAgentDispatchChecks.WebTokenFinding(Config(null, null)));
        Assert.Null(DarlingAgentDispatchChecks.WebTokenFinding(Config(Shared, null)));
        Assert.Null(DarlingAgentDispatchChecks.WebTokenFinding(Config(null, Shared)));
    }
}
