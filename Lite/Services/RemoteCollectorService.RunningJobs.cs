/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

public partial class RemoteCollectorService
{
    /// <summary>
    /// Collects currently running SQL Agent jobs via the shared <see cref="RunningJobsCollector"/>
    /// definition (the p95 duration comparison and the no-collection_id prefix live there — the
    /// cross-SKU parity contract). The failed-jobs alert query below rides the shared
    /// <see cref="FailedJobsQuery"/> (Phase-5 slice E).
    /// </summary>
    private Task<int> CollectRunningJobsAsync(ServerConnection server, CancellationToken cancellationToken)
        => RunCollectorDefinitionAsync(RunningJobsCollector.Instance, server, cancellationToken);

    /// <summary>
    /// Live query against the monitored server's msdb for SQL Agent job runs that FAILED within the
    /// lookback window — the SQL text and row mapping live in the shared
    /// <see cref="FailedJobsQuery"/> (Phase-5 slice E; see its doc for the query semantics and the
    /// server-local RunDateTime note). Runs at alert-check time — failure outcomes are not part of
    /// the collected running_jobs snapshot. Reuses the collector's connection path (MFA
    /// serialization, throttle, retry) and degrades gracefully: any error (a login without SELECT on
    /// msdb.dbo.sysjobs and sysjobhistory, a transient failure, etc.) returns an empty list rather
    /// than failing the alert cycle. The caller skips Azure SQL DB (no Agent) and no-msdb logins
    /// before calling.
    /// </summary>
    public async Task<List<FailedJobInfo>> GetRecentlyFailedJobsAsync(
        ServerConnection server,
        int lookbackMinutes,
        CancellationToken cancellationToken = default)
    {
        var items = new List<FailedJobInfo>();

        try
        {
            using var sqlConnection = await CreateConnectionAsync(server, cancellationToken);
            using var command = new SqlCommand(FailedJobsQuery.Sql, sqlConnection);
            command.CommandTimeout = CommandTimeoutSeconds;
            command.Parameters.AddWithValue(FailedJobsQuery.LookbackMinutesParameter, lookbackMinutes);

            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            items = await FailedJobsQuery.ReadAsync(reader, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Microsoft.Data.SqlClient.SqlException ex) when (SqlServerPermissionErrors.IsPermissionDenied(ex.Number))
        {
            /* #2512: the shared set, so this and Darling's twin cannot drift apart again — they were
               already identical here, which is precisely why the next number added to one of them
               would have been added to only one.
               Login cannot read the msdb job tables — expected for read-only monitoring accounts;
               skip quietly so a permission gap doesn't fail the whole alert cycle. The remedy is
               direct table SELECTs, NOT SQLAgentReaderRole (#1823): that role gates the sp_help_job*
               procedures, which this product never calls, and grants no SELECT on the base tables. */
            _logger?.LogDebug("Skipping failed-job check for '{Server}' (needs SELECT on msdb.dbo.sysjobs and sysjobhistory — SQLAgentReaderRole alone is not enough; see the monitoring-login grants in the README): {Message}", server.DisplayName, ex.Message);
            return new List<FailedJobInfo>();
        }
        catch (Exception ex)
        {
            /* Unexpected error (timeout, transient, etc.) — surface at Warning so a genuine read
               failure can't masquerade as "no failed jobs", but still don't fault the alert cycle. */
            _logger?.LogWarning("Failed-job check for '{Server}' errored: {Message}", server.DisplayName, ex.Message);
            return new List<FailedJobInfo>();
        }

        return items;
    }
}
