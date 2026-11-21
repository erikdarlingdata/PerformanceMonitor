/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
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
    /// #9270: after a <c>database_config</c> run, makes the collector due again in minutes when its newest capture holds a database in a
    /// transitional state (<see cref="ScheduleManager.ScheduleTransitionalRecaptureForServer"/>). A failed read leaves the schedule as it was.
    /// </summary>
    internal async Task ScheduleTransitionalRecaptureAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        try
        {
            var transitional = await ReadTransitionalDatabasesAsync(GetServerId(server), cancellationToken);
            _scheduleManager.ScheduleTransitionalRecaptureForServer(server.Id, "database_config", transitional, DateTime.UtcNow);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogDebug("Reading the transitional databases of server '{Server}' failed: {Message}", server.DisplayName, ex.Message);
        }
    }

    /// <summary>#9270: whether the newest <c>database_config</c> capture for the storage server id holds a database in a transitional state.</summary>
    internal async Task<bool> ReadTransitionalDatabasesAsync(int storageServerId, CancellationToken cancellationToken)
    {
        using var readLock = _duckDb.AcquireReadLock(cancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(cancellationToken);
        using var command = connection.CreateCommand();
        var states = string.Join(", ", ConfigChangeDiff.TransitionalDatabaseStates.Select(state => "'" + state + "'"));
        command.CommandText =
            "SELECT count(*) FROM v_database_config WHERE server_id = $1 " +
            "AND capture_time = (SELECT max(capture_time) FROM v_database_config WHERE server_id = $1) " +
            "AND upper(state_desc) IN (" + states + ")";
        command.Parameters.Add(new DuckDBParameter { Value = storageServerId });
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture) > 0;
    }

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
    /// Collects active trace flags via the shared <see cref="TraceFlagsCollector"/> definition. On-load only.
    /// <para>#5633 item 19: no catch of its own. This used to swallow every SqlException into 0 rows, which logged the run
    /// SUCCESS, and a SUCCESS run with 0 rows is now read as "no flag is enabled" (the trace-flag change diff, get_trace_flags and
    /// the analysis attribution), so a denied DBCC or a timeout read as every flag turned off. A failure now reaches
    /// <c>RunCollectorAsync</c>, which records PERMISSIONS for a denied permission and ERROR otherwise, like every other
    /// collector. Azure SQL Database, where DBCC TRACESTATUS is not supported, never reaches this: the collector's
    /// <c>AppliesTo</c> gate skips it (SKIPPED).</para>
    /// </summary>
    private Task<int> CollectTraceFlagsAsync(ServerConnection server, CancellationToken cancellationToken)
        => RunCollectorDefinitionAsync(TraceFlagsCollector.Instance, server, cancellationToken);
}
