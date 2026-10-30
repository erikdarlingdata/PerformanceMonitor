/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #6583: a SQL Server entry's <c>port</c> is never used to connect. The connection builders read the host alone, so a non-default
/// SQL Server port goes in the host as <c>host,port</c>. (A stored port is not inert: <c>MonitoredServer.StorageName</c> passes it to
/// <c>ServerIdHelper.BuildStorageName</c>, which appends it as <c>host:port</c>, so moving a stored port into the host changes the
/// server's storage name. That is correct, because the old row never connected on that port.) A new entry that carries one is refused, and an entry that already holds one (a darling.json file or a registry
/// row saved before the refusal) is named in a warning, so the port is never silently dropped.
/// </summary>
internal static class SqlServerPortAdvice
{
    /// <summary>The advice every surface gives, the same sentence for a refusal and a warning.</summary>
    internal const string Advice = "A SQL Server port goes in host, as host,port.";

    /// <summary>The refusal for a new entry or an edit that sets a non-zero port on a SQL Server target.</summary>
    internal const string Refusal = Advice + " The port field applies to PostgreSQL only.";

    /// <summary>
    /// One warning per SQL Server entry that holds a non-zero port, which is ignored: the service connects to the host as written.
    /// Names the server and the port, never a secret.
    /// </summary>
    internal static IReadOnlyList<string> Warnings(IEnumerable<MonitoredServer>? servers)
    {
        var warnings = new List<string>();
        foreach (var server in servers ?? Array.Empty<MonitoredServer>())
        {
            if (server is null || server.IsPostgres || server.Port <= 0)
            {
                continue;
            }

            warnings.Add(
                $"Server '{server.DisplayName}': port {server.Port} is ignored for a SQL Server target, so the service connects to host '{server.Host}' as written. " +
                $"{Advice} Move the port into host (for example '{server.Host},{server.Port}') and clear port.");
        }

        return warnings;
    }
}
