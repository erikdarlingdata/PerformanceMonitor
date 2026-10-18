/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Database;

/// <summary>
/// The names Lite's server list holds for one server (#6011, the twin of Darling's <c>RegistryNamesSelect</c>, #5999).
/// <see cref="InstanceName"/> is the list's server name; <see cref="DisplayName"/> the operator's label, null when none is set.
/// </summary>
public sealed record InvestigationServerNames(string InstanceName, string? DisplayName);

/// <summary>
/// Reads the server names an investigation answers with (#6011). Lite's DuckDB <c>servers</c> table is created but nothing writes
/// it, so the names come from <see cref="ServerManager"/>, the list the operator edits, read at call time so a rename shows on
/// the next read. Darling's rule: the display name falls back to the list's server name and then to the name the request was
/// opened with, so it is never null; the instance name is the list's server name, null once the server has left the list.
/// </summary>
public static class InvestigationServerNameLookup
{
    /// <summary>A snapshot of the server list keyed by the id the alert path and a request's <c>server_id</c> use.</summary>
    public delegate IReadOnlyDictionary<int, InvestigationServerNames> Snapshot();

    /// <summary>A snapshot of <paramref name="servers"/>, read at each call.</summary>
    public static Snapshot FromServerManager(ServerManager servers)
    {
        ArgumentNullException.ThrowIfNull(servers);
        return FromServerManager(() => servers);
    }

    /// <summary>As above, for a manager the caller builds after the store (the app builds the store first): a read before the
    /// manager exists names nothing, so the rows keep no names rather than throwing.</summary>
    public static Snapshot FromServerManager(Func<ServerManager?> servers)
    {
        ArgumentNullException.ThrowIfNull(servers);
        return () =>
        {
            var names = new Dictionary<int, InvestigationServerNames>();
            foreach (var server in servers()?.GetAllServers() ?? [])
            {
                var display = string.IsNullOrWhiteSpace(server.DisplayName) ? null : server.DisplayName;
                names.TryAdd(RemoteCollectorService.GetServerId(server), new InvestigationServerNames(server.ServerName, display));
            }

            return names;
        };
    }

    /// <summary>The (display, instance) pair for a request: the display name falls back to the list's server name and then to
    /// <paramref name="openedName"/>; the instance name is null when the server is not in the list.</summary>
    internal static (string Display, string? Instance) Resolve(
        IReadOnlyDictionary<int, InvestigationServerNames>? names, int serverId, string openedName)
    {
        if (names is not null && names.TryGetValue(serverId, out var found))
        {
            var instance = string.IsNullOrWhiteSpace(found.InstanceName) ? null : found.InstanceName;
            return (found.DisplayName ?? instance ?? openedName, instance);
        }

        return (openedName, null);
    }
}
