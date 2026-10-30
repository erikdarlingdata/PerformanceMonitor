/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Net;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6609: a listener URL the service logs puts an IPv6 host in brackets, so a browser (and anything that
/// reads the address back from the log line) can open it.
/// </summary>
public sealed class DarlingListenUrlTests
{
    [Theory]
    [InlineData("fd00::5", "http://[fd00::5]:5150/")]
    [InlineData("::", "http://[::]:5150/")]
    [InlineData("[::1]", "http://[::1]:5150/")]
    [InlineData("127.0.0.1", "http://127.0.0.1:5150/")]
    [InlineData("0.0.0.0", "http://0.0.0.0:5150/")]
    [InlineData("myhost", "http://myhost:5150/")]
    public void FormatHost_GivesTheBrowsableUrl(string host, string expectedUrl)
    {
        Assert.Equal(expectedUrl, $"http://{DarlingListenUrl.FormatHost(host)}:5150/");
    }

    [Theory]
    [InlineData("fd00::5", "[fd00::5]")]
    [InlineData("::", "[::]")]
    [InlineData("192.168.1.205", "192.168.1.205")]
    public void FormatHost_BoundAddress_MatchesTheStringForm(string listen, string expected)
    {
        Assert.Equal(expected, DarlingListenUrl.FormatHost(IPAddress.Parse(listen)));
    }

    [Theory]
    [InlineData("fd00::5", "[fd00::5] or localhost")]
    [InlineData("192.168.1.205", "192.168.1.205 or localhost")]
    [InlineData("myhost", "myhost or localhost")]
    public void OidcRedirectHosts_BracketAnIPv6Listen(string listen, string expected)
    {
        Assert.Equal(expected, DarlingWebHostService.DescribeOidcRedirectHosts(listen));
    }

    [Fact]
    public void McpStartLine_SpecificIPv6Listen_LogsABracketedUrl()
    {
        var line = DarlingMcpHostService.DescribeNetworkStart(
            false, IPAddress.Parse("fd00::5"), 5152, CidrAllowList.Parse("fd00::/64"), "the control plane");

        Assert.StartsWith("Starting MCP server on http://[fd00::5]:5152 (", line, System.StringComparison.Ordinal);
    }
}
