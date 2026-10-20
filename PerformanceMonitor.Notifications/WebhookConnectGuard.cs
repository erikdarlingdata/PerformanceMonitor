/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// The connect step of a webhook post (#6231): it refuses a link-local address (169.254.0.0/16 and fe80::/10) wherever the
/// address came from, a URL's own host, a DNS answer or a redirect, because the connection is what is checked, not the text of
/// the URL. Loopback and private addresses stay allowed: operators run internal receivers.
/// </summary>
public static class WebhookConnectGuard
{
    /// <summary>The sentence a refused connection carries. It never names the address.</summary>
    public const string RefusalText = "The webhook host resolves to a link-local address (169.254.0.0/16 or fe80::/10), which webhook posts do not connect to.";

    /// <summary>Whether the address is link-local: 169.254.0.0/16, or fe80::/10 (an IPv4-mapped IPv6 address counts as its IPv4).</summary>
    public static bool IsLinkLocal(IPAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);
        if (address.IsIPv4MappedToIPv6)
        {
            address = address.MapToIPv4();
        }

        if (address.AddressFamily == AddressFamily.InterNetwork)
        {
            var bytes = address.GetAddressBytes();
            return bytes[0] == 169 && bytes[1] == 254;
        }

        return address.AddressFamily == AddressFamily.InterNetworkV6 && address.IsIPv6LinkLocal;
    }

    /// <summary>The <c>SocketsHttpHandler.ConnectCallback</c>: resolves the host, drops every link-local answer, and connects to
    /// the first of the others that answers. When only link-local answers exist the connection is refused.</summary>
    public static async ValueTask<Stream> ConnectAsync(SocketsHttpConnectionContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        var endPoint = context.DnsEndPoint;
        var addresses = await Dns.GetHostAddressesAsync(endPoint.Host, cancellationToken).ConfigureAwait(false);
        var allowed = addresses.Where(a => !IsLinkLocal(a)).ToArray();
        if (allowed.Length == 0)
        {
            throw new HttpRequestException(addresses.Length == 0 ? "The webhook host did not resolve." : RefusalText);
        }

        Exception? last = null;
        foreach (var address in allowed)
        {
            var socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
            try
            {
                await socket.ConnectAsync(new IPEndPoint(address, endPoint.Port), cancellationToken).ConfigureAwait(false);
                return new NetworkStream(socket, ownsSocket: true);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                socket.Dispose();
                last = ex;
            }
        }

        throw new HttpRequestException("The webhook host did not accept the connection.", last);
    }
}
