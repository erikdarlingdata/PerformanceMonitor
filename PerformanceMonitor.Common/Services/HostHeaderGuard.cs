/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net;

namespace PerformanceMonitor.Common;

/// <summary>
/// The DNS-rebinding guard every embedded HTTP surface in the product installs as its FIRST middleware:
/// Darling's web dashboard (#1576), Darling's MCP host, and Lite's MCP host (both #1648). Authored ONCE here,
/// in the library both apps already reference, because it had drifted — the web host got the guard, the two MCP
/// hosts did not, and an MCP surface that is no longer read-only (custom-view CRUD, <c>add_servers</c> /
/// <c>remove_server</c>, alert-config writes) sat tokenless on loopback behind no Host check at all.
///
/// <para><b>What it defends:</b> a loopback MCP/web bind is tokenless by design, so a browser running ON the
/// host that loads attacker content can be DNS-rebound to <c>127.0.0.1:{port}</c> and reach the whole surface
/// same-origin. The <c>application/json</c> content type would normally force a CORS preflight, but under a
/// rebind the browser considers the request same-origin, so no preflight applies — the Host header is what is
/// left to check, and per the MCP spec checking it is the application's job (the SDK does not).</para>
///
/// <para>PURE (no HTTP types, no logger) so it unit-tests without a server, and so this library keeps no
/// ASP.NET Core dependency — each host owns only the two lines that read <c>Request.Host.Host</c> and return
/// 400.</para>
/// </summary>
public static class HostHeaderGuard
{
    /// <summary>
    /// Is this request's <c>Host</c> header one this server actually binds? Allowed: an absent/empty Host
    /// (HTTP/1.0 and some health probes), the name <c>localhost</c>, any loopback IP literal
    /// (<c>127.0.0.1</c>, <c>::1</c>, and the IPv4-mapped-IPv6 form), and — only when LAN-exposed — the
    /// configured listen IP in <paramref name="networkListenIp"/> — or, when that listen is a WILDCARD
    /// (<c>0.0.0.0</c> / <c>::</c>), any IP literal, because a wildcard names no single address to compare
    /// against (#2569). Everything else is rejected, which is
    /// exactly the rebound foreign hostname resolving to 127.0.0.1. Pass <c>null</c> for
    /// <paramref name="networkListenIp"/> in loopback-only mode (Lite always; Darling until an operator opts
    /// into LAN exposure), where only the loopback Hosts pass.
    /// </summary>
    /// <param name="host">The request's Host header with any port suffix already split off.</param>
    /// <param name="networkListenIp">The configured LAN listen address, or null in loopback-only mode.</param>
    public static bool IsAllowedHost(string? host, IPAddress? networkListenIp)
    {
        if (string.IsNullOrEmpty(host))
        {
            return true;
        }

        if (string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (IPAddress.TryParse(host, out var hostIp))
        {
            var ip = hostIp.IsIPv4MappedToIPv6 ? hostIp.MapToIPv4() : hostIp;
            if (IPAddress.IsLoopback(ip))
            {
                return true;
            }

            if (networkListenIp is null)
            {
                return false;
            }

            if (ip.Equals(networkListenIp))
            {
                return true;
            }

            /* A WILDCARD listen (#2569). Comparing the Host against 0.0.0.0 can only ever match the literal
               string "0.0.0.0", which no client sends — so every real LAN request was refused with a 400, and
               that is precisely what the compose distribution ships for both surfaces. Measured through a real
               pipeline before this arm existed: `Host: 192.168.1.205` -> HTTP 400, `Host: localhost` -> 200.

               "Any IP literal" is the rule here, and the reason is that with a wildcard bind THERE IS NO
               single correct address to compare against. The obvious alternative — enumerate this machine's
               own interfaces at bind time — is worse than it looks and would not have fixed the case that
               motivated this: inside a container the local interfaces are the container's (172.18.0.3), while
               the address the browser used is the host's published one, which the container cannot see. NAT,
               port forwarding and multi-homing break it the same way, and it fails SILENTLY, as a 400.

               THE REBINDING DEFENCE IS UNCHANGED, which is what makes this safe rather than a loosening. A DNS
               rebind necessarily arrives with a HOSTNAME in the Host header — that is the whole mechanism: the
               victim's browser loaded http://evil.com, so it sends `Host: evil.com` and considers the response
               same-origin. A hostname still never reaches this line. What is admitted is an IP literal, which
               a rebind cannot produce, and which in any case still has to pass the surface's CIDR check and
               its token before it reaches anything. */
            if (networkListenIp.Equals(IPAddress.Any) || networkListenIp.Equals(IPAddress.IPv6Any))
            {
                return true;
            }
        }

        return false;
    }
}
