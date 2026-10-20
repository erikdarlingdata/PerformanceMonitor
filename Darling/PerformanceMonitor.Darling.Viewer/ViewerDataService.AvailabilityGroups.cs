/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Availability Group topology read for the viewer's fleet-level AG tab (#991) — the STORE half only. Every
/// banding rule and the card projection live in <see cref="AgTopology"/> (PerformanceMonitor.Common), shared
/// with the web dashboard and Lite's AG tab, so the three surfaces cannot reach different verdicts about the
/// same AG. This file maps <see cref="DarlingAgStatesReader"/>'s rows (#4228) into
/// <see cref="AgTopologyReplicaRow"/> / <see cref="AgTopologyDatabaseRow"/>; it decides nothing.
///
/// <para>The statement text and the two-step, floor-bounded read used to be a separate copy here — the Viewer
/// had no route to the Service assembly that carried the other copy. Both now call
/// <see cref="DarlingAgStatesReader"/> in <c>PerformanceMonitor.Darling.Storage</c>, which both projects already
/// reference, so there is one implementation instead of two that could disagree about what "the newest
/// snapshot" means.</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>
    /// Reads the fleet's AG topology into per-(reporting server, AG) cards. An AG-less fleet — the common case —
    /// costs exactly one indexed read that returns nothing, because the database-grain read is skipped entirely
    /// when there are no replicas. This runs on the viewer's refresh timer, so that short-circuit matters.
    /// </summary>
    public async Task<List<AgTopologyCard>> GetAvailabilityGroupsAsync(CancellationToken cancellationToken = default)
    {
        var nowUtc = DateTime.UtcNow;
        var replicas = await ReadAgReplicasAsync(nowUtc, cancellationToken);
        if (replicas.Count == 0)
        {
            return new List<AgTopologyCard>();
        }

        /* One display-name lookup serves both uses (#6157, #6159): the card's "As reported by" server, and a primary learned from the
           stored column, which is shown by its display name when it is a monitored server. */
        var byId = DisplayNamesByServerId(await GetServersAsync(cancellationToken));
        Func<int, string?> displayNameOf = id => byId.TryGetValue(id, out var name) ? name : null;
        var cards = AgTopology.BuildCards(replicas, await ReadAgDatabasesAsync(nowUtc, cancellationToken), nowUtc, displayNameOf);
        ApplyNames(cards, byId);
        return cards;
    }

    private static Dictionary<int, string?> DisplayNamesByServerId(IEnumerable<DarlingServer> servers) =>
        servers.GroupBy(s => s.ServerId).ToDictionary(g => g.Key, g => (string?)g.First().DisplayName);

    /// <summary>#6157: the card's "As reported by" line names the reporting server the way the server list does, by display name,
    /// not by the connection key the store rows carry. A server with no display name keeps the key (GetServersAsync already
    /// falls back to it), and a card whose server is not in the list is left alone.</summary>
    internal static void ApplyDisplayNames(IEnumerable<AgTopologyCard> cards, IEnumerable<DarlingServer> servers) =>
        ApplyNames(cards, DisplayNamesByServerId(servers));

    private static void ApplyNames(IEnumerable<AgTopologyCard> cards, Dictionary<int, string?> byId)
    {
        foreach (var card in cards)
        {
            if (byId.TryGetValue(card.ServerId, out var name) && !string.IsNullOrWhiteSpace(name))
            {
                card.ServerName = name;
            }
        }
    }

    /// <summary>Maps <see cref="DarlingAgStatesReader"/>'s raw rows into <see cref="AgTopologyReplicaRow"/>,
    /// field for field, so <see cref="AgTopology.BuildCards"/> needed no change when the read moved (#4228).
    /// No server filter — the viewer always reads the whole fleet.</summary>
    private async Task<List<AgTopologyReplicaRow>> ReadAgReplicasAsync(DateTime nowUtc, CancellationToken cancellationToken)
    {
        var raw = await DarlingAgStatesReader.GetReplicaStatesAsync(_dataSource, null, nowUtc, cancellationToken);
        var rows = new List<AgTopologyReplicaRow>(raw.Count);
        foreach (var row in raw)
        {
            rows.Add(new AgTopologyReplicaRow
            {
                ServerId = row.ServerId,
                ServerName = row.ServerName,
                CollectionTime = row.CollectionTime,
                AgName = row.AgName,
                ReplicaServerName = row.ReplicaServerName,
                RoleDesc = row.RoleDesc,
                IsLocal = row.IsLocal,
                OperationalStateDesc = row.OperationalStateDesc,
                ConnectedStateDesc = row.ConnectedStateDesc,
                RecoveryHealthDesc = row.RecoveryHealthDesc,
                SynchronizationHealthDesc = row.SynchronizationHealthDesc,
                AvailabilityModeDesc = row.AvailabilityModeDesc,
                FailoverModeDesc = row.FailoverModeDesc,
                EndpointUrl = row.EndpointUrl,
                GroupId = row.GroupId,
                PrimaryReplica = row.PrimaryReplica,
            });
        }

        return rows;
    }

    /// <summary>Same mapping as <see cref="ReadAgReplicasAsync"/>, for the database grain. The shared reader's
    /// row also carries <c>LastHardenedLsn</c> / <c>LastCommitLsn</c> (the Service's columns); the viewer's card
    /// does not surface them today, so they are simply not read here, same as before the move.</summary>
    private async Task<List<AgTopologyDatabaseRow>> ReadAgDatabasesAsync(DateTime nowUtc, CancellationToken cancellationToken)
    {
        var raw = await DarlingAgStatesReader.GetDatabaseReplicaStatesAsync(_dataSource, null, nowUtc, cancellationToken);
        var rows = new List<AgTopologyDatabaseRow>(raw.Count);
        foreach (var row in raw)
        {
            rows.Add(new AgTopologyDatabaseRow
            {
                ServerId = row.ServerId,
                ServerName = row.ServerName,
                CollectionTime = row.CollectionTime,
                AgName = row.AgName,
                DatabaseName = row.DatabaseName,
                ReplicaServerName = row.ReplicaServerName,
                IsLocal = row.IsLocal,
                SynchronizationStateDesc = row.SynchronizationStateDesc,
                LogSendQueueKb = row.LogSendQueueSize,
                RedoQueueKb = row.RedoQueueSize,
                LogSendRateKbPerSec = row.LogSendRate,
                RedoRateKbPerSec = row.RedoRate,
                IsSuspended = row.IsSuspended,
                SuspendReasonDesc = row.SuspendReasonDesc,
                AvailabilityModeDesc = row.AvailabilityModeDesc,
                SecondaryLagSeconds = row.SecondaryLagSeconds,
                GroupId = row.GroupId,
            });
        }

        return rows;
    }
}
