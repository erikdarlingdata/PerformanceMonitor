/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

public partial class RemoteCollectorService
{
    /// <summary>
    /// Collects currently running SQL Agent jobs with historical duration comparison.
    /// Flags jobs running longer than their p95 historical duration.
    /// </summary>
    private async Task<int> CollectRunningJobsAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    running_jobs AS
(
    SELECT
        job_name = j.name,
        job_id = CONVERT(varchar(36), j.job_id),
        job_enabled = j.enabled,
        start_time = ja.start_execution_date,
        current_duration_seconds =
            DATEDIFF(SECOND, ja.start_execution_date, GETDATE())
    FROM msdb.dbo.sysjobactivity AS ja
    JOIN msdb.dbo.sysjobs AS j
      ON j.job_id = ja.job_id
    JOIN msdb.dbo.syssessions AS s
      ON s.session_id = ja.session_id
    WHERE ja.start_execution_date IS NOT NULL
    AND   ja.stop_execution_date IS NULL
    AND   s.agent_start_date =
    (
        SELECT
            MAX(s2.agent_start_date)
        FROM msdb.dbo.syssessions AS s2
    )
),
    job_history_raw AS
(
    SELECT
        job_id = CONVERT(varchar(36), jh.job_id),
        duration_seconds =
            (jh.run_duration / 10000) * 3600 +
            ((jh.run_duration / 100) % 100) * 60 +
            (jh.run_duration % 100),
        p95_duration_seconds =
            CONVERT(bigint,
                PERCENTILE_CONT(0.95) WITHIN GROUP
                (
                    ORDER BY
                        (jh.run_duration / 10000) * 3600 +
                        ((jh.run_duration / 100) % 100) * 60 +
                        (jh.run_duration % 100)
                ) OVER
                (
                    PARTITION BY
                        jh.job_id
                )
            )
    FROM msdb.dbo.sysjobhistory AS jh
    WHERE jh.step_id = 0
    AND   jh.run_status = 1
    AND   jh.run_date >= CONVERT(integer, CONVERT(varchar(8), DATEADD(DAY, -30, GETDATE()), 112))
),
    job_history_stats AS
(
    SELECT
        job_id = jhr.job_id,
        avg_duration_seconds = AVG(jhr.duration_seconds),
        p95_duration_seconds = MAX(jhr.p95_duration_seconds),
        successful_run_count = COUNT_BIG(*)
    FROM job_history_raw AS jhr
    GROUP BY
        jhr.job_id
)
SELECT
    rj.job_name,
    rj.job_id,
    rj.job_enabled,
    rj.start_time,
    rj.current_duration_seconds,
    avg_duration_seconds = ISNULL(jhs.avg_duration_seconds, 0),
    p95_duration_seconds = ISNULL(jhs.p95_duration_seconds, 0),
    successful_run_count = ISNULL(jhs.successful_run_count, 0),
    is_running_long =
        CASE
            WHEN jhs.p95_duration_seconds > 0
            AND  rj.current_duration_seconds > jhs.p95_duration_seconds
            THEN CONVERT(bit, 1)
            ELSE CONVERT(bit, 0)
        END,
    percent_of_average =
        CASE
            WHEN jhs.avg_duration_seconds > 0
            THEN CONVERT(decimal(10,1), rj.current_duration_seconds * 100.0 / jhs.avg_duration_seconds)
            ELSE NULL
        END
FROM running_jobs AS rj
LEFT JOIN job_history_stats AS jhs
  ON jhs.job_id = rj.job_id
ORDER BY
    rj.current_duration_seconds DESC
OPTION(RECOMPILE);";

        var serverId = GetServerId(server);
        var collectionTime = DateTime.UtcNow;
        var rowsCollected = 0;
        _lastSqlMs = 0;
        _lastDuckDbMs = 0;

        var rows = new List<(string JobName, string JobId, bool JobEnabled, DateTime StartTime,
            long CurrentDuration, long AvgDuration, long P95Duration, long SuccessfulRunCount,
            bool IsRunningLong, decimal? PercentOfAverage)>();

        var sqlSw = Stopwatch.StartNew();
        using var sqlConnection = await CreateConnectionAsync(server, cancellationToken);
        using var command = new SqlCommand(query, sqlConnection);
        command.CommandTimeout = CommandTimeoutSeconds;

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add((
                reader.GetString(0),
                reader.GetString(1),
                Convert.ToBoolean(reader.GetValue(2)),
                reader.GetDateTime(3),
                Convert.ToInt64(reader.GetValue(4)),
                Convert.ToInt64(reader.GetValue(5)),
                Convert.ToInt64(reader.GetValue(6)),
                Convert.ToInt64(reader.GetValue(7)),
                Convert.ToBoolean(reader.GetValue(8)),
                reader.IsDBNull(9) ? null : Convert.ToDecimal(reader.GetValue(9))));
        }
        sqlSw.Stop();

        var duckSw = Stopwatch.StartNew();

        using (var duckConnection = _duckDb.CreateConnection())
        {
            await duckConnection.OpenAsync(cancellationToken);

            using (var appender = duckConnection.CreateAppender("running_jobs"))
            {
                foreach (var r in rows)
                {
                    var row = appender.CreateRow();
                    row.AppendValue(collectionTime)
                       .AppendValue(serverId)
                       .AppendValue(server.ServerName)
                       .AppendValue(r.JobName)
                       .AppendValue(r.JobId)
                       .AppendValue(r.JobEnabled)
                       .AppendValue(r.StartTime)
                       .AppendValue(r.CurrentDuration)
                       .AppendValue(r.AvgDuration)
                       .AppendValue(r.P95Duration)
                       .AppendValue(r.SuccessfulRunCount)
                       .AppendValue(r.IsRunningLong)
                       .AppendValue(r.PercentOfAverage)
                       .EndRow();
                    rowsCollected++;
                }
            }
        }

        duckSw.Stop();
        _lastSqlMs = sqlSw.ElapsedMilliseconds;
        _lastDuckDbMs = duckSw.ElapsedMilliseconds;

        _logger?.LogDebug("Collected {RowCount} running job rows for server '{Server}'", rowsCollected, server.DisplayName);
        return rowsCollected;
    }
}
