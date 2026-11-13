/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net;
using System.Text.Json;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using Xunit;

namespace Darling.Tests;

/// <summary>The <c>web.network.oidc.tokenSignIn</c> setting and its pure decision (#8735).</summary>
public sealed class DarlingTokenSignInTests
{
    [Theory]
    [InlineData(null, "on")]
    [InlineData("", "on")]
    [InlineData("  ", "on")]
    [InlineData("on", "on")]
    [InlineData(" Loopback ", "loopback")]
    [InlineData("OFF", "off")]
    public void TryParse_KnownWords(string? raw, string expected)
    {
        Assert.True(DarlingTokenSignIn.TryParse(raw, out var mode));
        Assert.Equal(expected, DarlingTokenSignIn.Word(mode));
    }

    [Theory]
    [InlineData("disabled")]
    [InlineData("true")]
    [InlineData("loop back")]
    public void TryParse_Unknown_FailsClosed(string raw)
    {
        Assert.False(DarlingTokenSignIn.TryParse(raw, out var mode));
        Assert.Equal("off", DarlingTokenSignIn.Word(mode));
    }

    [Theory]
    [InlineData("on", "on")]
    [InlineData("loopback", "loopback")]
    [InlineData("off", "off")]
    public void Word_NamesTheMode(string name, string expected)
    {
        Assert.Equal(expected, DarlingTokenSignIn.Word(Enum.Parse<TokenSignInMode>(name, true)));
    }

    [Theory]
    [InlineData("on", "192.168.1.50", null, true)]
    [InlineData("on", "127.0.0.1", "192.168.1.10", true)]
    [InlineData("on", null, null, true)]
    [InlineData("off", "127.0.0.1", null, false)]
    [InlineData("off", "::1", "127.0.0.1", false)]
    [InlineData("off", "192.168.1.50", null, false)]
    [InlineData("loopback", "127.0.0.1", null, true)]
    [InlineData("loopback", "::1", null, true)]
    [InlineData("loopback", "::ffff:127.0.0.1", null, true)]
    [InlineData("loopback", "192.168.1.50", null, false)]
    [InlineData("loopback", "127.0.0.1", "192.168.1.10", false)]
    [InlineData("loopback", "192.168.1.50", "127.0.0.1", false)]
    [InlineData("loopback", "127.0.0.1", "127.0.0.1", true)]
    [InlineData("loopback", null, null, false)]
    public void IsAllowed_Matrix(string modeName, string? remote, string? connection, bool expected)
    {
        var mode = Enum.Parse<TokenSignInMode>(modeName, true);
        var remoteIp = remote is null ? null : IPAddress.Parse(remote);
        var connectionIp = connection is null ? null : IPAddress.Parse(connection);

        Assert.Equal(expected, DarlingTokenSignIn.IsAllowed(mode, remoteIp, connectionIp));
    }

    [Fact]
    public void IsTokenSeat_SharedNamedGroup_True_Oidc_False()
    {
        Assert.True(DarlingTokenSignIn.IsTokenSeat(DarlingWebSeat.SharedToken));

        Assert.True(DarlingWebSeat.TryResolveSeat(DarlingWebSeat.EncodeTokenCookieSubject("ops"), out var named));
        Assert.True(DarlingTokenSignIn.IsTokenSeat(named));

        Assert.True(DarlingWebSeat.TryResolveSeat(DarlingWebSeat.EncodeGroupCookieSubject(Guid.NewGuid(), Guid.NewGuid()), out var group));
        Assert.True(DarlingTokenSignIn.IsTokenSeat(group));

        Assert.True(DarlingWebSeat.TryResolveSeat(DarlingWebSeat.EncodeCookieSubject("alice@example.com", WebOidcRole.Admin), out var oidcAdmin));
        Assert.False(DarlingTokenSignIn.IsTokenSeat(oidcAdmin));

        Assert.True(DarlingWebSeat.TryResolveSeat(DarlingWebSeat.EncodeCookieSubject("bob@example.com", WebOidcRole.Viewer, new string('k', DarlingWebSeat.OwnerKeyLength)), out var oidcViewer));
        Assert.False(DarlingTokenSignIn.IsTokenSeat(oidcViewer));
    }

    [Fact]
    public void TokenSignIn_IsReadFromJson_AndDoesNotMakeOidcConfigured()
    {
        var oidc = JsonSerializer.Deserialize<WebOidcConfig>("{\"tokenSignIn\":\"off\"}")!;

        Assert.Equal("off", oidc.TokenSignIn);
        Assert.False(oidc.IsConfigured);
    }
}
