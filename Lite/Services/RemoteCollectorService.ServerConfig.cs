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
    /// #9270: after a <c>database_config</c> run, makes the collector due again in minutes when the stored captures say so
    /// (<see cref="ScheduleManager.ScheduleTransitionalRecaptureForServer"/>). A failed read leaves the schedule as it was.
    /// </summary>
    internal async Task ScheduleTransitionalRecaptureAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        try
        {
            var recaptureDue = await ReadRecaptureDueAsync(GetServerId(server), cancellationToken);
            _scheduleManager.ScheduleTransitionalRecaptureForServer(server.Id, "database_config", recaptureDue, DateTime.UtcNow);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogDebug("Reading the transitional databases of server '{Server}' failed: {Message}", server.DisplayName, ex.Message);
        }
    }

    /// <summary>
    /// #9270: <see cref="TransitionalDatabaseRuns.RecaptureIsDue"/> over the stored <c>database_config</c> captures of the storage server id's
    /// databases that are transitional in its newest capture.
    /// </summary>
    internal async Task<bool> ReadRecaptureDueAsync(int storageServerId, CancellationToken cancellationToken)
    {
        using var readLock = _duckDb.AcquireReadLock(cancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(cancellationToken);
        using var command = connection.CreateCommand();
        var states = string.Join(", ", ConfigChangeDiff.TransitionalDatabaseStates.Select(state => "'" + state + "'"));
        command.CommandText =
            "SELECT CAST(d.database_name AS VARCHAR), d.capture_time, CAST(d.state_desc AS VARCHAR) FROM v_database_config AS d WHERE d.server_id = $1 " +
            "AND d.database_name IN (SELECT n.database_name FROM v_database_config AS n WHERE n.server_id = $1 " +
            "AND n.capture_time = (SELECT max(m.capture_time) FROM v_database_config AS m WHERE m.server_id = $1) " +
            "AND upper(n.state_desc) IN (" + states + "))";
        command.Parameters.Add(new DuckDBParameter { Value = storageServerId });
        var captures = new List<TransitionalDatabaseRuns.Capture>();
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (!reader.IsDBNull(0) && !reader.IsDBNull(1))
            {
                captures.Add(new TransitionalDatabaseRuns.Capture(reader.GetDateTime(1), reader.GetString(0), reader.IsDBNull(2) ? null : reader.GetString(2)));
            }
        }

        return TransitionalDatabaseRuns.RecaptureIsDue(captures);
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
