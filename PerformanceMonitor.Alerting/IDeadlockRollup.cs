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

namespace PerformanceMonitor.Alerting;

/// <summary>
/// #6005 (#5632 Phase C, D13): a host's hook for the daily rollup of deadlock signatures. The alert pass hands it the deadlock
/// rows it read for a server on every sweep that read them. Darling implements it; Lite passes none, so Lite does nothing extra.
/// </summary>
public interface IDeadlockRollup
{
    /// <summary>
    /// Counts the graphs in <paramref name="rows"/> (the window's rows, excluded databases already removed) that this sink has
    /// not counted before. <paramref name="serverKey"/> is the engine's server key.
    /// </summary>
    Task RecordAsync(string serverKey, IReadOnlyList<DeadlockAlertRow> rows, DateTime nowUtc, CancellationToken cancellationToken);
}
