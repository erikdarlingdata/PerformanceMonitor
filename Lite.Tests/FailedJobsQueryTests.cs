/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Identity pins for the Phase-5 slice-E extraction: the live msdb failed-jobs alert query moved
/// verbatim from this app's <c>RemoteCollectorService.RunningJobs.cs</c> (whitespace-normalized
/// identical to the Dashboard's <c>DatabaseService.NocHealth.cs</c> twin) into the shared
/// <see cref="FailedJobsQuery"/>. The clause pins are derived from the pre-extraction SQL, so a
/// drift in the shared copy fails a pin; the ReadAsync pins reproduce the pre-extraction row
/// mapping including its null handling. The ONE documented reconciliation: the step_id conversion
/// adopts the Dashboard's explicit InvariantCulture (behaviorally identical for an int column).
/// </summary>
public class FailedJobsQueryTests
{
    /* ---------------- the SQL contract ---------------- */

    [Fact]
    public void Sql_ContainsLoadBearingClauses()
    {
        var sql = FailedJobsQuery.Sql;

        /* Dirty read — never block a monitored server's Agent from an alert check. */
        Assert.Contains("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;", sql);

        /* The sources: job history joined to job names. */
        Assert.Contains("FROM msdb.dbo.sysjobhistory AS jh", sql);
        Assert.Contains("JOIN msdb.dbo.sysjobs AS j", sql);

        /* Bounded row count, newest failures first. */
        Assert.Contains("SELECT TOP (50)", sql);
        Assert.Contains("run_datetime DESC", sql);
        Assert.Contains("OPTION(RECOMPILE);", sql);

        /* The outcome-row filter: step_id = 0 is the per-run outcome row, run_status = 0 = FAILED. */
        Assert.Contains("WHERE jh.step_id = 0", sql);
        Assert.Contains("AND   jh.run_status = 0", sql);

        /* The failing-step resolution: OUTER APPLY correlates the actual failed step (step_id > 0,
           run_status = 0) from THIS run via instance_id, bounded after the previous outcome row. */
        Assert.Contains("OUTER APPLY", sql);
        Assert.Contains("AND   s.step_id > 0", sql);
        Assert.Contains("AND   s.run_status = 0", sql);
        Assert.Contains("AND   s.instance_id < jh.instance_id", sql);
        Assert.Contains("AND   p.step_id = 0", sql);

        /* The outcome-row fallback when a job-level failure has no failed step row. */
        Assert.Contains("step_id = ISNULL(fs.step_id, jh.step_id)", sql);
        Assert.Contains("step_name = ISNULL(fs.step_name, jh.step_name)", sql);
        Assert.Contains("message = ISNULL(fs.message, jh.message)", sql);

        /* #3421: the server's own UTC offset, measured in the same statement that read the server-local
           run_datetime, so FailedJobInfo can state the failure instant in the frame the alert body and
           alert_time share. Measured on the server rather than read from collected server_properties
           because this feed is already connected to it. */
        Assert.Contains("utc_offset_minutes = DATEDIFF(MINUTE, GETUTCDATE(), GETDATE())", sql);
    }

    [Fact]
    public void Sql_LookbackParameter_AppearsAtBothFilterSites()
    {
        Assert.Equal("@lookback_minutes", FailedJobsQuery.LookbackMinutesParameter);

        /* The lookback binds twice: the coarse run_date integer filter and the exact
           server-local run_datetime filter. Both must survive — dropping the coarse one
           makes the query scan all of sysjobhistory. */
        Assert.Equal(2, Regex.Matches(FailedJobsQuery.Sql, Regex.Escape(FailedJobsQuery.LookbackMinutesParameter)).Count);
        Assert.Contains("jh.run_date >= CONVERT(integer, CONVERT(varchar(8), DATEADD(MINUTE, -@lookback_minutes, GETDATE()), 112))", FailedJobsQuery.Sql);
        Assert.Contains(") >= DATEADD(MINUTE, -@lookback_minutes, GETDATE())", FailedJobsQuery.Sql);
    }

    /* ---------------- the row mapping ---------------- */

    [Fact]
    public async Task ReadAsync_MapsRepresentativeRow()
    {
        var runTime = new DateTime(2026, 6, 30, 23, 45, 12);
        var reader = new FakeCollectorDataReader(new object[]
        {
            "Nightly ETL",
            "3f2504e0-4f89-11d3-9a0c-0305e82c3301",
            runTime,
            3,
            "Load fact table",
            "Executed as user: NT SERVICE\\SQLSERVERAGENT. The step failed.",
            -240
        });

        var items = await FailedJobsQuery.ReadAsync(reader, CancellationToken.None);

        var job = Assert.Single(items);
        Assert.Equal("Nightly ETL", job.JobName);
        Assert.Equal("3f2504e0-4f89-11d3-9a0c-0305e82c3301", job.JobId);
        Assert.Equal(runTime, job.RunDateTime);
        /* #3421: the offset is ordinal 6, and the row states the run instant in UTC from it. */
        Assert.Equal(-240, job.UtcOffsetMinutes);
        Assert.Equal(runTime.AddMinutes(240), job.RunDateTimeUtc);
        Assert.Equal("2026-07-01 03:45:12Z", job.RunDateTimeFormatted);
        Assert.Equal(3, job.StepId);
        Assert.Equal("Load fact table", job.StepName);
        Assert.Equal("Executed as user: NT SERVICE\\SQLSERVERAGENT. The step failed.", job.Message);
    }

    [Fact]
    public async Task ReadAsync_NullColumns_MapToEmptyStringAndZero()
    {
        /* job_name is never null (sysjobs.name); every other column degrades like the
           pre-extraction mapping: "" for strings, 0 for step_id. */
        var reader = new FakeCollectorDataReader(new object[]
        {
            "Nightly ETL",
            DBNull.Value,
            new DateTime(2026, 6, 30, 23, 45, 12),
            DBNull.Value,
            DBNull.Value,
            DBNull.Value,
            DBNull.Value
        });

        var items = await FailedJobsQuery.ReadAsync(reader, CancellationToken.None);

        var job = Assert.Single(items);
        Assert.Equal("", job.JobId);
        Assert.Equal(0, job.StepId);
        Assert.Equal("", job.StepName);
        Assert.Equal("", job.Message);

        /* #3421: a provider that could not evaluate the offset expression leaves the instant
           unconvertible, and the rendering SAYS so rather than claiming UTC it has not earned. */
        Assert.Null(job.UtcOffsetMinutes);
        Assert.Null(job.RunDateTimeUtc);
        Assert.EndsWith(AlertTimestamp.UnknownOffsetMarker, job.RunDateTimeFormatted, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReadAsync_EmptyReader_ReturnsEmptyList()
    {
        var items = await FailedJobsQuery.ReadAsync(new FakeCollectorDataReader(), CancellationToken.None);
        Assert.Empty(items);
    }
}
