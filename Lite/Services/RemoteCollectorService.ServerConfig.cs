/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

public partial class RemoteCollectorService
{
    /// <summary>
    /// Collects server configuration via the shared <see cref="ServerConfigCollector"/> definition.
    /// On-load only, not scheduled.
    /// </summary>
    private Task<int> CollectServerConfigAsync(ServerConnection server, CancellationToken cancellationToken)
        => RunCollectorDefinitionAsync(ServerConfigCollector.Instance, server, cancellationToken);

    /// <summary>
    /// Collects database configuration via the shared <see cref="DatabaseConfigCollector"/>
    /// definition (the 2019/2025 version gates and the exclusion-filter splice live there —
    /// the cross-SKU parity contract). On-load only, not scheduled.
    /// </summary>
    private Task<int> CollectDatabaseConfigAsync(ServerConnection server, CancellationToken cancellationToken)
        => RunCollectorDefinitionAsync(DatabaseConfigCollector.Instance, server, cancellationToken);

    /// <summary>
    /// Collects database-scoped configurations via the shared
    /// <see cref="DatabaseScopedConfigCollector"/> definition (the database enumeration with the
    /// AG-primary filter and the [db].sys.sp_executesql per-database loop live there — the
    /// cross-SKU parity contract). On-load only, not scheduled.
    /// </summary>
    private Task<int> CollectDatabaseScopedConfigAsync(ServerConnection server, CancellationToken cancellationToken)
        => RunCollectorDefinitionAsync(DatabaseScopedConfigCollector.Instance, server, cancellationToken);

    /// <summary>
    /// Collects per-database Query Store health via the shared <see cref="QueryStoreHealthCollector"/>
    /// definition (#2319 — the database enumeration with the AG-primary filter and the
    /// [db].sys.sp_executesql per-database loop live there, the cross-SKU parity contract). Hourly, not
    /// on-load: actual_state and the storage numbers change by themselves, and the cap-hit transition
    /// to READ_ONLY is the point of collecting this.
    /// </summary>
    private Task<int> CollectQueryStoreHealthAsync(ServerConnection server, CancellationToken cancellationToken)
        => RunCollectorDefinitionAsync(QueryStoreHealthCollector.Instance, server, cancellationToken);

    /// <summary>
    /// Collects active trace flags via the shared <see cref="TraceFlagsCollector"/> definition.
    /// Wrapped in a permission-tolerant catch — DBCC may be denied — so a failure degrades to
    /// zero rows with a warning, exactly as the original collector did. On-load only.
    /// </summary>
    private async Task<int> CollectTraceFlagsAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        try
        {
            return await RunCollectorDefinitionAsync(TraceFlagsCollector.Instance, server, cancellationToken);
        }
        catch (SqlException ex)
        {
            _logger?.LogWarning("Failed to collect trace flags on '{Server}' (may lack DBCC permissions): {Message}",
                server.DisplayName, ex.Message);
            return 0;
        }
    }
}
