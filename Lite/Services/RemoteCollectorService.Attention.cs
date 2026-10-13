/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

public partial class RemoteCollectorService
{
    /// <summary>
    /// Collects client attentions (#5640) via the shared <see cref="AttentionCollector"/> definition: the event_counter
    /// delta (a count row per run with a non-zero delta), the ring-buffer samples and the statement filter all live there, the same cross-SKU
    /// parity contract as <see cref="CollectBlockedProcessReportsAsync"/>. A missing or inaccessible session is NOT
    /// tolerated as zero rows: the definition's batch raises "XE session ... is not running", and
    /// <see cref="ReadXeSessionAsync"/> turns that into <see cref="XeSessionEnsureException"/>, so the run never records a
    /// SUCCESS over a source it could not read.
    /// </summary>
    private Task<int> CollectAttentionAsync(ServerConnection server, CancellationToken cancellationToken)
        => ReadXeSessionAsync(
            "attention",
            () => RunCollectorDefinitionAsync(AttentionCollector.Instance, server, cancellationToken));
}
