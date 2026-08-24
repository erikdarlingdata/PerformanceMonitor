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

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Currently running SQL Agent jobs with historical duration comparison (p95 over the last 30
/// days of successful outcome rows; flags jobs past their p95). Extracted verbatim from Lite's
/// RemoteCollectorService.RunningJobs.cs. Note: running_jobs is the one table without a
/// collection_id — <see cref="IncludesCollectionId"/> is false. The failed-jobs alert query
/// (GetRecentlyFailedJobsAsync) is alert-engine surface and lives in the alerting library
/// (<c>PerformanceMonitor.Alerting.FailedJobsQuery</c>, Phase-5 slice E), not here.
/// </summary>
public sealed class RunningJobsCollector : CollectorDefinitionBase<RunningJobsCollector.Row>
{
    public static RunningJobsCollector Instance { get; } = new();

    private RunningJobsCollector()
    {
    }

    public readonly record struct Row(
        string JobName,
        string JobId,
        bool JobEnabled,
        DateTime StartTime,
        long CurrentDurationSeconds,
        long AvgDurationSeconds,
        long P95DurationSeconds,
        long SuccessfulRunCount,
        bool IsRunningLong,
        decimal? PercentOfAverage);

    private const string QueryText = @"
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

    public override string Name => "running_jobs";

    public override string TargetTable => "running_jobs";

    /// <summary>running_jobs is keyed by (collection_time, server) alone — no collection_id.</summary>
    public override bool IncludesCollectionId => false;

    /// <summary>
    /// Azure SQL Database has no SQL Agent — <c>msdb.dbo.sysjobactivity</c> is unreachable there
    /// (the three-part msdb reference raises "not supported in this version of SQL Server"), so skip
    /// the collector rather than log a per-cycle ERROR. Managed Instance (edition 8) DOES have Agent,
    /// so it collects there; on-prem collects too. A login without msdb access is skipped: every table
    /// this reads lives in msdb. Both gates live here in the shared AppliesTo — the single authoritative
    /// gate both SKUs consult (mirrors the failed-jobs alert path's Azure/msdb skip).
    ///
    /// <para><b>AWS RDS is NOT skipped, since #2575.</b> It used to be, on the reasoning that this query
    /// joins <c>msdb.dbo.syssessions</c>, which requires sysadmin and which RDS never grants, so the read
    /// "would ERROR every cycle". Two things retired that. The MEASUREMENT: across 84 RDS instances this
    /// collector returns SUCCESS on every one, with zero non-SUCCESS rows, so the premise is not true of
    /// RDS today. The MECHANISM: a denial is SQL error 229, which <see cref="CollectorTargetFault"/>
    /// classifies as a PERMISSIONS degrade rather than an ERROR, so even where the premise does hold the
    /// cost is one non-fatal row that the runtime-precondition read then explains with the grant to issue.
    /// The gate was guarding against a failure mode the fault classifier did not yet handle.</para>
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) =>
        !target.IsAzureSqlDb && target.HasMsdbAccess;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("job_name", CollectorColumnType.Varchar),
        new CollectorColumn("job_id", CollectorColumnType.Varchar),
        new CollectorColumn("job_enabled", CollectorColumnType.Boolean),
        new CollectorColumn("start_time", CollectorColumnType.Timestamp),
        new CollectorColumn("current_duration_seconds", CollectorColumnType.BigInt),
        new CollectorColumn("avg_duration_seconds", CollectorColumnType.BigInt),
        new CollectorColumn("p95_duration_seconds", CollectorColumnType.BigInt),
        new CollectorColumn("successful_run_count", CollectorColumnType.BigInt),
        new CollectorColumn("is_running_long", CollectorColumnType.Boolean),
        new CollectorColumn("percent_of_average", CollectorColumnType.Decimal, 10, 1),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                reader.GetString(0),
                reader.GetString(1),
                Convert.ToBoolean(reader.GetValue(2), CultureInfo.InvariantCulture),
                reader.GetDateTime(3),
                Convert.ToInt64(reader.GetValue(4), CultureInfo.InvariantCulture),
                Convert.ToInt64(reader.GetValue(5), CultureInfo.InvariantCulture),
                Convert.ToInt64(reader.GetValue(6), CultureInfo.InvariantCulture),
                Convert.ToInt64(reader.GetValue(7), CultureInfo.InvariantCulture),
                Convert.ToBoolean(reader.GetValue(8), CultureInfo.InvariantCulture),
                reader.IsDBNull(9) ? null : Convert.ToDecimal(reader.GetValue(9), CultureInfo.InvariantCulture)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.JobName)                 /* job_name VARCHAR */
            .Value(row.JobId)                   /* job_id VARCHAR */
            .Value(row.JobEnabled)              /* job_enabled BOOLEAN */
            .Value(row.StartTime)               /* start_time TIMESTAMP */
            .Value(row.CurrentDurationSeconds)  /* current_duration_seconds BIGINT */
            .Value(row.AvgDurationSeconds)      /* avg_duration_seconds BIGINT */
            .Value(row.P95DurationSeconds)      /* p95_duration_seconds BIGINT */
            .Value(row.SuccessfulRunCount)      /* successful_run_count BIGINT */
            .Value(row.IsRunningLong)           /* is_running_long BOOLEAN */
            .Value(row.PercentOfAverage);       /* percent_of_average DECIMAL(10,1) */
    }
}
