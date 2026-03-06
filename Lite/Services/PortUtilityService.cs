/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitorLite.Services;

public static partial class PortUtilityService
{
    // Checks whether something is currently LISTENING on this TCP port.
    // Note: the underlying API is synchronous; we yield once to keep an async signature.
    public static async Task<bool> IsTcpPortListeningAsync(
        int port,
        IPAddress? address = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await Task.Yield();

        if (port < IPEndPoint.MinPort || port > IPEndPoint.MaxPort)
            throw new ArgumentOutOfRangeException(nameof(port));

        address ??= IPAddress.Any;
    
        var listeners = IPGlobalProperties.GetIPGlobalProperties().GetActiveTcpListeners();
    
        return listeners.Any(ep =>
            ep.Port == port &&
            (ep.Address.Equals(address) ||
             ep.Address.Equals(IPAddress.Any) ||
             ep.Address.Equals(IPAddress.IPv6Any)));
    }

    // Definitive check: attempt to bind.
    // Note: the underlying API is synchronous; we yield once to keep an async signature.
    public static async Task<bool> CanBindTcpPortAsync(
        int port,
        IPAddress? address = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await Task.Yield();

        if (port < IPEndPoint.MinPort || port > IPEndPoint.MaxPort)
            throw new ArgumentOutOfRangeException(nameof(port));
    
        address ??= IPAddress.Any;
    
        try
        {
            var listener = new TcpListener(address, port);
            listener.Start();
            listener.Stop();
            return true;
        }
        catch (SocketException)
        {
            return false;
        }
    }

    // Let the OS choose a free port (0), then read it back.
    // Note: this is still synchronous under the hood; we yield once to keep an async signature.
    public static async Task<int> GetFreeTcpPortAsync(
        IPAddress? address = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await Task.Yield();

        address ??= IPAddress.Loopback;
    
        var listener = new TcpListener(address, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
