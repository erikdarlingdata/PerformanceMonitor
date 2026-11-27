/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

#nullable enable

using System;
using PerformanceMonitor.Darling.Service.FrontDoor;
using PerformanceMonitor.Darling.Service.Hosting;
using Xunit;

namespace Darling.Tests.FrontDoor;

/// <summary>
/// The front door names a store's <c>web.network.oidc.tokenSignIn</c> refusal by the code in the store's answer (#8735), and leaves
/// every other sign-in failure sentence as it was.
/// </summary>
public sealed class FrontDoorTokenSignInRefusalTests
{
    private const string PlainForbidden = "the store refused the sign-in with 403 (check allowFrom, and that the web token is not the MCP token)";

    [Fact]
    public void Refusal403WithCode_NamesTokenSignIn()
    {
        var body = "<!doctype html><html><head>" + DarlingTokenSignIn.RefusalMarkerHtml + "<title>x</title></head></html>";

        var why = DarlingFrontDoorWebSession.DescribeSignInFailure("use1", 403, body);

        Assert.Contains("tokenSignIn", why, StringComparison.Ordinal);
        Assert.Contains("use1", why, StringComparison.Ordinal);
        Assert.Contains("loopback", why, StringComparison.Ordinal);
        Assert.Contains("off", why, StringComparison.Ordinal);
        Assert.DoesNotContain("allowFrom", why, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("<html>Forbidden</html>")]
    public void Plain403_KeepsTodaysSentence(string? body)
    {
        Assert.Equal(PlainForbidden, DarlingFrontDoorWebSession.DescribeSignInFailure("use1", 403, body));
    }

    [Fact]
    public void Body429_200_Other_Unchanged()
    {
        Assert.Equal("the store is holding sign-ins and answered 429", DarlingFrontDoorWebSession.DescribeSignInFailure("use1", 429, null));
        Assert.Equal(
            "the store answered its login page, so it did not accept the web token",
            DarlingFrontDoorWebSession.DescribeSignInFailure("use1", 200, null));
        Assert.Equal("the store answered HTTP 502 to the sign-in", DarlingFrontDoorWebSession.DescribeSignInFailure("use1", 502, null));
    }

    [Fact]
    public void MarkerInANon403Body_IsIgnored()
    {
        Assert.Equal(
            "the store answered HTTP 502 to the sign-in",
            DarlingFrontDoorWebSession.DescribeSignInFailure("use1", 502, DarlingTokenSignIn.RefusalMarkerHtml));
    }

    [Fact]
    public void ProseWithoutTheMarker_IsNotTheCode()
    {
        var body = "<html><body>The code token_sign_in_disabled is described here.</body></html>";

        Assert.Equal(PlainForbidden, DarlingFrontDoorWebSession.DescribeSignInFailure("use1", 403, body));
    }
}
