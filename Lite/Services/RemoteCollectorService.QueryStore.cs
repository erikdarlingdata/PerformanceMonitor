/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

public partial class RemoteCollectorService
{
    /// <summary>
    /// Collects Query Store data via the shared <see cref="QueryStoreCollector"/> definition
    /// (the actual_state enumeration probe, the live PRODUCTVERSION check deciding the
    /// 2017+/2022+ column gates, the last_execution_time incremental watermark, and the
    /// per-database sp_executesql query live there — the cross-SKU parity contract).
    /// </summary>
    private async Task<int> CollectQueryStoreAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        /* #2165: never run beside this server's own Query Store BACKFILL slice. The two loops are
           independent and used to overlap freely — measured as two heavy QS text extractions in flight at
           once on a 4-core box, because a large catalog arriving is what triggers both. Gated inside the
           collector's entry point rather than at the tick's dispatch switch so every caller is covered,
           including an on-demand collection.

           Zero-wait by construction (see QueryStoreServerGate): a blocking acquire here would let one
           server's slice stall the whole sweep, which is the #2148 wedge wearing a lock. Skipping costs
           nothing durable because this collector's window is a watermark (#1960) — the next tick resumes
           from the same boundary. */
        using var gate = _queryStoreGates
            .GetOrAdd(server.Id, static _ => new QueryStoreServerGate())
            .TryAcquire();

        if (gate is null)
        {
            _logger?.LogInformation(
                "query_store collection on '{Server}' skipped this tick — its Query Store backfill slice is " +
                "mid-flight (#2165). Resumes next tick from the same watermark; no rows are lost.",
                server.DisplayName);
            return 0;
        }

        return await RunCollectorDefinitionAsync(QueryStoreCollector.Instance, server, cancellationToken);
    }
}
