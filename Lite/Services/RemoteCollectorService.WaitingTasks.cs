/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

public partial class RemoteCollectorService
{
    /// <summary>
    /// Collects point-in-time waiting tasks via the shared <see cref="WaitingTasksCollector"/>
    /// definition (smallint widening, the NULL resource_description placeholder, and the
    /// exclusion splice live there — the cross-SKU parity contract).
    /// </summary>
    private Task<int> CollectWaitingTasksAsync(ServerConnection server, CancellationToken cancellationToken)
        => RunCollectorDefinitionAsync(WaitingTasksCollector.Instance, server, cancellationToken);
}
