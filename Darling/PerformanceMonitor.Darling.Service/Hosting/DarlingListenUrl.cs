/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Net;
using System.Net.Sockets;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>
/// The host part of a URL the service logs or prints for one of its listeners (#6609). An IPv6 literal in a URL
/// must be bracketed (<c>http://[fd00::5]:5150/</c>); the bare form (<c>http://fd00::5:5150/</c>) is not an address
/// a browser can open, and the installer's ready message passes the logged address on as it reads it. IPv4, host
/// names and an already-bracketed literal come out unchanged. Text only: no listener decision moves here.
/// </summary>
internal static class DarlingListenUrl
{
    /// <summary>The host as a URL needs it: an unbracketed IPv6 literal gains brackets, anything else is returned as given.</summary>
    internal static string FormatHost(string? host)
    {
        if (string.IsNullOrWhiteSpace(host))
        {
            return host ?? string.Empty;
        }

        var text = host.Trim();
        return !text.StartsWith('[')
            && IPAddress.TryParse(text, out var address)
            && address.AddressFamily == AddressFamily.InterNetworkV6
            ? $"[{text}]"
            : text;
    }

    /// <summary>The bound address as a URL host (an <see cref="IPAddress"/> prints an IPv6 literal without brackets).</summary>
    internal static string FormatHost(IPAddress address) => FormatHost(address.ToString());
}
