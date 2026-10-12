/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Alerting;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// The Runnable Queue alert's window read (#5641): the number of <c>cpu_scheduler_stats</c> samples in the last
    /// <paramref name="windowMinutes"/> minutes, the AVERAGE of their <c>avg_runnable_tasks_count</c> (the collector's
    /// <c>AVG(runnable_tasks_count)</c> over <c>VISIBLE ONLINE</c> schedulers: runnable tasks per scheduler, the number
    /// the bars were measured on), and the NEWEST sample's scheduler count, total runnable tasks and collection time.
    /// The twin of <c>DarlingAlertReadAdapter.RunnableQueueSql</c>, in DuckDB's dialect. See
    /// <see cref="RunnableQueueWindowSql"/>.
    /// </summary>
    public async Task<RunnableQueueWindow> GetRunnableQueueWindowAsync(int serverId, int windowMinutes)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = RunnableQueueWindowSql;
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        /* The caller's window, as a naive-UTC floor: DuckDB keeps microseconds in collection_time, and this is the
           clock style the poison-wait read binds. */
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddMinutes(-windowMinutes) });

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return RunnableQueueWindow.Empty;
        }

        return new RunnableQueueWindow(
            reader.GetInt64(0),
            reader.IsDBNull(1) ? 0 : reader.GetDouble(1),
            reader.IsDBNull(2) ? null : reader.GetInt32(2),
            reader.IsDBNull(3) ? null : reader.GetInt64(3),
            reader.GetDateTime(4));
    }

    /// <summary>
    /// <see cref="GetRunnableQueueWindowAsync"/>'s text, exposed for the parity pins. One row: the window's sample count
    /// and average, carried on the NEWEST sample (collection_id breaks a same-instant tie, #3936). A sample whose
    /// <c>avg_runnable_tasks_count</c> is NULL (no visible online scheduler) is not a sample. No rows at all when the
    /// window is empty. $1 server_id, $2 the naive-UTC window floor. DuckDB's AVG over a DECIMAL is a DOUBLE.
    /// </summary>
    public const string RunnableQueueWindowSql = @"
WITH win AS
(
    SELECT
        collection_time,
        collection_id,
        avg_runnable_tasks_count,
        scheduler_count,
        total_runnable_tasks_count
    FROM v_cpu_scheduler_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   avg_runnable_tasks_count IS NOT NULL
)
SELECT
    CAST(COUNT(*) OVER () AS BIGINT) AS sample_count,
    CAST(AVG(avg_runnable_tasks_count) OVER () AS DOUBLE) AS avg_runnable_per_scheduler,
    CAST(scheduler_count AS INTEGER) AS scheduler_count,
    CAST(total_runnable_tasks_count AS BIGINT) AS total_runnable_tasks_count,
    collection_time
FROM win
ORDER BY collection_time DESC, collection_id DESC
LIMIT 1";
}
