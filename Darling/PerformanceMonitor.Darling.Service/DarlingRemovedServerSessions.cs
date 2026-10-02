/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// A removed server, as it stood before the registry forgot it (#4961): the definition the loop held, the runtime that
/// connects to it, and what its last long-query reconcile left (<see cref="DarlingWorker.ServerLoopState.LongQueryTraceApplied"/>:
/// null is not yet reconciled, true is on, false is confirmed dropped).
/// </summary>
internal sealed record RemovedLongQueryServer(MonitoredServer Config, ServerRuntime Runtime, bool? LongQueryTraceApplied);

/// <summary>
/// Removing a server drops this install's Extended Events sessions on it, and nothing else (#4961): the long-query trace's
/// session, then the deadlock and blocked-process sessions this install chose for itself in the server's databases.
/// </summary>
internal static class DarlingRemovedServerSessions
{
    /// <summary>How long a server's removal waits for its sessions to drop, for the whole step.</summary>
    internal static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);

    /// <summary>Stub, filled in by the removal's change.</summary>
    internal static List<RemovedLongQueryServer> Capture(
        IReadOnlyList<DarlingWorker.ServerLoopState> servers, IReadOnlyList<MonitoredServer> desired, DarlingCollectorRunner runner) =>
        new();

    /// <summary>Stub, filled in by the removal's change.</summary>
    internal static Task DropAsync(
        RemovedLongQueryServer removed,
        DarlingCollectorRunner runner,
        IReadOnlyList<MonitoredServer>? remaining,
        Func<int, bool> traceOn,
        Func<CancellationToken, Task<LongQueryTraceInstanceGuard>> instanceGuard,
        ILogger logger,
        CancellationToken cancellationToken) =>
        Task.CompletedTask;
}
