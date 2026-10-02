/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The engine edition that Lite applies the Azure SQL Database master scope (<see cref="AzureMasterScope"/>, #4894)
/// by. The alert sweep and the provider behind analysis, the overview card, the daily summary and the MCP reads all
/// ask this one class, so they cannot disagree about a server.
///
/// <para><b>Which edition.</b> A live connection status that reports an edition wins, and this class remembers it.
/// Without one, the edition this class last knew for the server: one that a live status reported earlier in this
/// run, else the stored one that <see cref="Seed"/> loaded at startup (the newest <c>v_server_properties</c> row,
/// the #2511 store-not-status rule). With neither, the edition is unknown and the server stays unscoped, as a
/// server that was never collected always was.</para>
///
/// <para><b>Why the live edition alone was not enough.</b> The first alert sweep after a start runs before the
/// first connection check has read the edition, and a failed check or an edit writes a blank status. On the live
/// edition alone, a master target was unscoped in those sweeps, so its Blocking and Deadlocks alerts counted the
/// events of the databases that alert on their own targets.</para>
///
/// <para>Keyed by the storage server id (<see cref="StorageId"/>), the id the stored rows and the analysis provider
/// use. The alert sweep holds the configured server and maps it through the same function.</para>
/// </summary>
internal sealed class KnownEngineEditions
{
    private readonly ConcurrentDictionary<int, int> _editions = new();

    /// <summary>
    /// Loads the stored editions, keyed by storage server id. An unknown stored edition is skipped, and an edition
    /// that a live status already reported is kept.
    /// </summary>
    public void Seed(IReadOnlyDictionary<int, int> storedEditions)
    {
        foreach (var (serverId, edition) in storedEditions)
        {
            if (edition != CollectorEngineCapability.UnknownEngineEdition)
            {
                _editions.TryAdd(serverId, edition);
            }
        }
    }

    /// <summary>
    /// The edition to judge the server by. A known <paramref name="liveEdition"/> wins and is remembered. An unknown
    /// one (0) answers the remembered edition, or unknown when there is none.
    /// </summary>
    public int Resolve(int serverId, int liveEdition)
    {
        if (liveEdition != CollectorEngineCapability.UnknownEngineEdition)
        {
            _editions[serverId] = liveEdition;
            return liveEdition;
        }

        return _editions.TryGetValue(serverId, out var known) ? known : CollectorEngineCapability.UnknownEngineEdition;
    }

    /// <summary>Whether the configured server is an Azure SQL Database, by <see cref="Resolve"/>.</summary>
    public bool IsAzureSqlDatabase(ServerConnection server, int liveEdition) =>
        Resolve(StorageId(server), liveEdition) == CollectorEngineCapability.AzureSqlDatabaseEngineEdition;

    /// <summary>
    /// The databases whose blocking and deadlock events the configured server skips
    /// (<see cref="AzureMasterScope.SeparatelyMonitoredDatabases"/>), with the edition from <see cref="Resolve"/>.
    /// Self is identified by its configuration id. Each target carries its enabled flag and read-only intent.
    /// </summary>
    public IReadOnlyList<string> SeparatelyMonitoredDatabases(
        ServerConnection self, int liveEdition, IEnumerable<ServerConnection> servers) =>
        AzureMasterScope.SeparatelyMonitoredDatabases(
            IsAzureSqlDatabase(self, liveEdition),
            self.Id, self.ServerName, self.DatabaseName,
            servers.Select(t => new AlertTargetIdentity(t.Id, t.ServerName, t.DatabaseName, t.IsEnabled, t.ReadOnlyIntent)));

    /// <summary>
    /// The analysis provider's answer for a storage server id: <see cref="SeparatelyMonitoredDatabases"/> for the
    /// configured server with that id, with its live edition from <paramref name="liveEditionOf"/> (called with the
    /// configuration id). Null when no configured server has the id or the list is empty, so analysis, the card and
    /// the daily summary stay exactly as they were.
    /// </summary>
    public IReadOnlyList<string>? SeparatelyMonitoredDatabasesOrNull(
        int serverId, IReadOnlyList<ServerConnection> servers, Func<string, int> liveEditionOf)
    {
        var self = servers.FirstOrDefault(t => StorageId(t) == serverId);
        if (self is null) return null;
        var list = SeparatelyMonitoredDatabases(self, liveEditionOf(self.Id), servers);
        return list.Count > 0 ? list : null;
    }

    /// <summary>The storage server id of a configured server: the id the collector stores its rows under.</summary>
    public static int StorageId(ServerConnection server) =>
        RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
}
