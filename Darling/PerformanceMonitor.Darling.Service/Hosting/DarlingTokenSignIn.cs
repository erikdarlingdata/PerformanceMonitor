/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>The three values of <c>web.network.oidc.tokenSignIn</c> (#8735).</summary>
internal enum TokenSignInMode { On, Loopback, Off }

/// <summary>
/// PURE: the decision behind <c>web.network.oidc.tokenSignIn</c> (#8735), whether the web token sign-in form may
/// be used from a connection, and which seats came from a token. It reads no config and touches no request.
/// </summary>
internal static class DarlingTokenSignIn
{
    /// <summary>The fixed code a refused token sign-in carries.</summary>
    internal const string RefusalCode = "token_sign_in_disabled";

    /// <summary>The exact element that carries <see cref="RefusalCode"/> in a refused sign-in's body.</summary>
    internal const string RefusalMarkerHtml = "<meta name='darling-refusal' content='" + RefusalCode + "'>";

    /// <summary>
    /// Parses the setting, compared case-insensitively after trimming. Null, empty or blank is <see cref="TokenSignInMode.On"/>.
    /// An unknown value returns false with <see cref="TokenSignInMode.Off"/>, so a caller that ignores the result fails closed.
    /// </summary>
    internal static bool TryParse(string? value, out TokenSignInMode mode)
    {
        var word = value?.Trim();
        if (string.IsNullOrEmpty(word) || string.Equals(word, "on", StringComparison.OrdinalIgnoreCase))
        {
            mode = TokenSignInMode.On;
            return true;
        }

        if (string.Equals(word, "loopback", StringComparison.OrdinalIgnoreCase))
        {
            mode = TokenSignInMode.Loopback;
            return true;
        }

        if (string.Equals(word, "off", StringComparison.OrdinalIgnoreCase))
        {
            mode = TokenSignInMode.Off;
            return true;
        }

        mode = TokenSignInMode.Off;
        return false;
    }

    /// <summary>The setting's word for a mode, for logs and messages.</summary>
    internal static string Word(TokenSignInMode mode) => mode switch
    {
        TokenSignInMode.On => "on",
        TokenSignInMode.Loopback => "loopback",
        _ => "off",
    };

    /// <summary>
    /// Whether the token sign-in form may be used from this connection. <c>Loopback</c> asks the one loopback test,
    /// <see cref="DarlingWebHostService.IsLoopbackConnection"/>, so a forwarded <c>127.0.0.1</c> arriving through a remote trusted proxy is not
    /// loopback. Through a trusted proxy on the same host it is: the connection is loopback and the forwarded address is too. A proxy
    /// listed in <c>trustedProxies</c> helps only when it sends <c>X-Forwarded-For</c> on every request; a service-mesh sidecar usually
    /// does not, and behind one <c>Loopback</c> cannot tell local clients from remote ones.
    /// </summary>
    internal static bool IsAllowed(TokenSignInMode mode, IPAddress? remoteIp, IPAddress? connectionIp) => mode switch
    {
        TokenSignInMode.On => true,
        TokenSignInMode.Loopback => DarlingWebHostService.IsLoopbackConnection(remoteIp, connectionIp),
        _ => false,
    };

    /// <summary>
    /// Whether a seat came from a token sign-in (the shared token, a named <c>web.network.tokens</c> entry or a managed group
    /// token) rather than from OIDC. A named seat carries its name as <see cref="DarlingWebSeat.Subject"/>, so
    /// <see cref="DarlingWebSeat.TokenName"/> is what tells it from an OIDC seat. A seat with no subject at all (the shared
    /// token, or the reads-only seat of a dashboard with no sign-in) counts as a token seat.
    /// </summary>
    internal static bool IsTokenSeat(DarlingWebSeat seat)
        => seat.TokenName is not null || seat.GroupId is not null || seat.Subject is null;
}
