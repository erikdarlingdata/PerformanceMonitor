/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// The live-msdb failed-jobs alert feed (Phase-5 slice E): the query and row mapping behind
/// <see cref="FailedJobInfo"/>, previously duplicated verbatim in Lite's
/// <c>RemoteCollectorService.RunningJobs.cs</c> and the Dashboard's
/// <c>DatabaseService.NocHealth.cs</c>. This is NOT a collected table — it runs against the
/// monitored server's msdb at alert-check time, because failure outcomes are not part of the
/// collected <c>running_jobs</c> snapshot. It finds job runs that FAILED within the lookback
/// window (the step_id = 0 outcome row, run_status = 0) and correlates the actual failing step
/// (step_id &gt; 0, run_status = 0) from that run via instance_id, so step_id/step_name/message
/// describe the failing step rather than the generic "(Job outcome)" row.
/// <para>
/// Hosts execute <see cref="Sql"/> on their own connections with their own gating:
/// Lite pre-gates the call (online + non-Azure-SQL-DB + <c>HasMsdbAccess</c>) and rides the
/// collector's connection path (MFA serialization, throttle, retry); the Dashboard skips Azure
/// SQL DB (engineEdition == 5) at the call site and degrades a login that cannot SELECT the msdb
/// job tables (SqlException 229/297/300/916) to an empty list; the future Darling
/// engine gates on !IsAzureSqlDb + the same exception degrade — per the Phase-5 review finding
/// (F11), Darling must NOT invent an msdb access probe (no <c>HasMsdbAccess</c> exists there).
/// Every host returns an empty list on failure so a permission gap or transient error never
/// fails the alert cycle.
/// </para>
/// </summary>
public static class FailedJobsQuery
{
    /// <summary>
    /// The single parameter <see cref="Sql"/> expects: the lookback window in minutes (int).
    /// Hosts bind it on their own command (SqlParameter / AddWithValue).
    /// </summary>
    public const string LookbackMinutesParameter = "@lookback_minutes";

    /// <summary>
    /// run_date/run_time integers are converted to a SERVER-LOCAL datetime (GETDATE()-relative
    /// lookback filter) — <see cref="FailedJobInfo.RunDateTime"/> is server-local, not UTC. The server's
    /// own UTC offset rides along as <c>utc_offset_minutes</c> so
    /// <see cref="FailedJobInfo.RunDateTimeUtc"/> can state the same instant in the frame the alert body
    /// and <c>alert_time</c> share.
    /// <para>The offset is the CURRENT one, applied to instants up to the lookback window old, so a
    /// window straddling a DST transition converts the far side by an hour too much. Accepted: SQL Server
    /// can only do better with <c>AT TIME ZONE</c> and a named zone, which the product does not collect,
    /// and the alternative is being wrong by the whole offset every day of the year instead of by an hour
    /// twice. The store-side de-skews apply a single scalar offset for the same reason.</para>
    /// </summary>
    public const string Sql = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP (50)
    job_name = j.name,
    job_id = CONVERT(varchar(36), j.job_id),
    run_datetime =
        DATEADD
        (
            SECOND,
            (jh.run_time / 10000) * 3600 +
            ((jh.run_time / 100) % 100) * 60 +
            (jh.run_time % 100),
            CONVERT(datetime, CONVERT(varchar(8), jh.run_date))
        ),
    step_id = ISNULL(fs.step_id, jh.step_id),
    step_name = ISNULL(fs.step_name, jh.step_name),
    message = ISNULL(fs.message, jh.message),
    utc_offset_minutes = DATEDIFF(MINUTE, GETUTCDATE(), GETDATE())
FROM msdb.dbo.sysjobhistory AS jh
JOIN msdb.dbo.sysjobs AS j
  ON j.job_id = jh.job_id
OUTER APPLY
(
    /* The actual failing step (step_id > 0, run_status = 0) from THIS run, correlated by
       instance_id and bounded to be after this job's previous outcome row, so the alert names
       the failing step instead of the generic job-outcome row. Falls back to the outcome row
       for a rare job-level failure with no failed step row. */
    SELECT TOP (1)
        s.step_id,
        s.step_name,
        s.message
    FROM msdb.dbo.sysjobhistory AS s
    WHERE s.job_id = jh.job_id
    AND   s.step_id > 0
    AND   s.run_status = 0
    AND   s.instance_id < jh.instance_id
    AND   s.instance_id >
          (
              SELECT ISNULL(MAX(p.instance_id), 0)
              FROM msdb.dbo.sysjobhistory AS p
              WHERE p.job_id = jh.job_id
              AND   p.step_id = 0
              AND   p.instance_id < jh.instance_id
          )
    ORDER BY s.instance_id DESC
) AS fs
WHERE jh.step_id = 0
AND   jh.run_status = 0
AND   jh.run_date >= CONVERT(integer, CONVERT(varchar(8), DATEADD(MINUTE, -@lookback_minutes, GETDATE()), 112))
AND   DATEADD
      (
          SECOND,
          (jh.run_time / 10000) * 3600 +
          ((jh.run_time / 100) % 100) * 60 +
          (jh.run_time % 100),
          CONVERT(datetime, CONVERT(varchar(8), jh.run_date))
      ) >= DATEADD(MINUTE, -@lookback_minutes, GETDATE())
ORDER BY
    run_datetime DESC
OPTION(RECOMPILE);";

    /// <summary>
    /// Maps <see cref="Sql"/>'s result rows (job_name, job_id, run_datetime, step_id, step_name,
    /// message, utc_offset_minutes — in that ordinal order) into <see cref="FailedJobInfo"/>. Moved
    /// verbatim from the two host copies; the ONE reconciled difference at extraction time: the step_id
    /// conversion adopts the Dashboard's explicit <see cref="CultureInfo.InvariantCulture"/> (Lite omitted
    /// the provider — behaviorally identical for an int column).
    /// <para>The offset is last so the six original ordinals are untouched.</para>
    /// </summary>
    public static async Task<List<FailedJobInfo>> ReadAsync(DbDataReader reader, CancellationToken cancellationToken)
    {
        var items = new List<FailedJobInfo>();

        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new FailedJobInfo
            {
                JobName = reader.GetString(0),
                JobId = reader.IsDBNull(1) ? "" : reader.GetString(1),
                RunDateTime = reader.GetDateTime(2),
                StepId = reader.IsDBNull(3) ? 0 : Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture),
                StepName = reader.IsDBNull(4) ? "" : reader.GetString(4),
                Message = reader.IsDBNull(5) ? "" : reader.GetString(5),
                /* Read by ordinal like every other column, with no FieldCount guard: a guard would turn a
                   mapper-versus-SQL divergence into every alert quietly reporting "offset not collected"
                   forever, where the bare read makes it a logged fault the hosts already degrade to an
                   empty list. DBNull stays a real outcome — it is what a provider that cannot evaluate
                   the expression returns. */
                UtcOffsetMinutes = reader.IsDBNull(6)
                    ? null
                    : Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture)
            });
        }

        return items;
    }
}
