/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The rule behind #9084: turning collection off for a server is how a person tells Lite to leave it alone (a maintenance window,
/// a load test, a timed demo), so no background loop sends it a query. The collectors and the periodic connection check already
/// obey it. This is the same answer for the Overview alert sweep, which used to run its live msdb reads for every server in the list.
/// A person's own Connect or Refresh on a server is not a background loop and does not go through here.
/// </summary>
internal static class DisabledServerGate
{
    /// <summary>
    /// The Overview summaries the alert sweep runs over: every summary except the card of a server whose collection is off. A card
    /// no server in the list owns any more is kept, as before: the sweep itself drops a summary whose server was removed.
    /// </summary>
    internal static List<ServerSummaryItem> SummariesToSweep(
        IEnumerable<ServerSummaryItem> summaries, IReadOnlyCollection<ServerConnection> servers) =>
        summaries
            .Where(summary =>
            {
                var owners = servers.Where(summary.IsCardFor).ToList();
                return owners.Count == 0 || owners.Any(owner => owner.IsEnabled);
            })
            .ToList();

    /// <summary>
    /// True when a live msdb read for the alert sweep may go to this server: collection is on, the last connection check found it
    /// online, it is not an Azure SQL Database (no SQL Agent) and the login can read msdb.
    /// </summary>
    internal static bool MayReadLiveMsdb(ServerConnection? server, ServerConnectionStatus? status) =>
        server is { IsEnabled: true }
        && status is { IsOnline: true, SqlEngineEdition: not 5, HasMsdbAccess: true };

    /// <summary>
    /// The failed-jobs fetch behind the Failed Job alert: <paramref name="read"/> is the live msdb read, and it is called only when
    /// <see cref="MayReadLiveMsdb"/> allows it. Every other case is an empty list, which is what the engine's fetcher contract asks for.
    /// </summary>
    internal static async Task<List<FailedJobInfo>> FetchFailedJobsAsync(
        ServerConnection? server,
        ServerConnectionStatus? status,
        Func<ServerConnection, Task<List<FailedJobInfo>>>? read)
    {
        if (server == null || read == null || !MayReadLiveMsdb(server, status))
        {
            return new List<FailedJobInfo>();
        }

        return await read(server);
    }
}
