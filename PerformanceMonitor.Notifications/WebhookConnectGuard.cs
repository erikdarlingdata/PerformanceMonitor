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
/// The connect step of a webhook post (#6231): it refuses a link-local address (169.254.0.0/16 and fe80::/10) and the cloud
/// instance addresses fd00:ec2::254, 168.63.129.16 and 100.100.100.200 wherever the address came from, a URL's own host, a DNS
/// answer or a redirect, because the connection is what is checked, not the text of the URL. Loopback and private addresses stay
/// allowed: operators run internal receivers. The check applies to direct connections; when a proxy carries the request, the
/// proxy decides what it can reach.
/// </summary>
public static class WebhookConnectGuard
{
    /// <summary>The sentence a refused connection carries. It never names the address.</summary>
    public const string RefusalText = "The webhook host resolves to a link-local address (169.254.0.0/16 or fe80::/10) or a cloud instance address (fd00:ec2::254, 168.63.129.16, 100.100.100.200), which webhook posts do not connect to.";

    private static readonly IPAddress[] s_instanceAddresses =
    [
        IPAddress.Parse("fd00:ec2::254"),
        IPAddress.Parse("168.63.129.16"),
        IPAddress.Parse("100.100.100.200"),
    ];

    /// <summary>The one way to build a direct webhook HTTP handler: pooled for five minutes, every connection through
    /// <see cref="ConnectAsync(SocketsHttpConnectionContext, CancellationToken)"/>. Every direct client uses it, so none can miss the guard.</summary>
    public static SocketsHttpHandler NewHandler(bool allowAutoRedirect = true) => new()
    {
        PooledConnectionLifetime = TimeSpan.FromMinutes(5),
        AllowAutoRedirect = allowAutoRedirect,
        ConnectCallback = ConnectAsync,
    };

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

    /// <summary>Whether a webhook post refuses to connect to the address: link-local (<see cref="IsLinkLocal"/>), or one of the
    /// exact cloud instance addresses fd00:ec2::254, 168.63.129.16 and 100.100.100.200 (an IPv4-mapped IPv6 form counts as its IPv4).</summary>
    public static bool IsRefused(IPAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);
        if (IsLinkLocal(address))
        {
            return true;
        }

        if (address.IsIPv4MappedToIPv6)
        {
            address = address.MapToIPv4();
        }

        return Array.Exists(s_instanceAddresses, a => a.Equals(address));
    }

    /// <summary>The <c>SocketsHttpHandler.ConnectCallback</c>: resolves the host, drops every refused answer, and connects to
    /// the first of the others that answers. When only refused answers exist the connection is refused.</summary>
    public static ValueTask<Stream> ConnectAsync(SocketsHttpConnectionContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        return ConnectAsync(context.DnsEndPoint, null, cancellationToken);
    }

    /// <summary>The connect with a seam: <paramref name="socketFactory"/> builds each socket (null builds a TCP socket), so a test
    /// can see the socket it was handed disposed when the connect is cancelled or fails.</summary>
    internal static async ValueTask<Stream> ConnectAsync(DnsEndPoint endPoint, Func<AddressFamily, Socket>? socketFactory, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(endPoint);
        var addresses = await Dns.GetHostAddressesAsync(endPoint.Host, cancellationToken).ConfigureAwait(false);
        var allowed = addresses.Where(a => !IsRefused(a)).ToArray();
        if (allowed.Length == 0)
        {
            throw new HttpRequestException(addresses.Length == 0 ? "The webhook host did not resolve." : RefusalText);
        }

        Exception? last = null;
        foreach (var address in allowed)
        {
            var socket = socketFactory is null
                ? new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
                : socketFactory(address.AddressFamily);
            socket.NoDelay = true;
            var connected = false;
            try
            {
                await socket.ConnectAsync(new IPEndPoint(address, endPoint.Port), cancellationToken).ConfigureAwait(false);
                connected = true;
                return new NetworkStream(socket, ownsSocket: true);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                last = ex;
            }
            finally
            {
                /* A cancelled or failed connect never reaches the NetworkStream that owns the socket, so the socket is closed here. */
                if (!connected)
                {
                    socket.Dispose();
                }
            }
        }

        throw new HttpRequestException("The webhook host did not accept the connection.", last);
    }
}
