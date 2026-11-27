/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
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
            -240,
            3844
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

        /* #8972: no duration, no failure instant. */
        Assert.Null(job.RunDurationSeconds);
        Assert.Null(job.FailedAt);
        Assert.Null(job.FailedAtFormatted);
    }

    /// <summary>
    /// A reader shaped like <see cref="FailedJobsQuery.Sql"/>'s result, built from a DataTable so no server is
    /// involved. The column order is the mapper's ordinal contract.
    /// </summary>
    private static DataTableReader ReaderOf(params object?[][] rows)
    {
        var table = new DataTable();
        table.Columns.Add("job_name", typeof(string));
        table.Columns.Add("job_id", typeof(string));
        table.Columns.Add("run_datetime", typeof(DateTime));
        table.Columns.Add("step_id", typeof(int));
        table.Columns.Add("step_name", typeof(string));
        table.Columns.Add("message", typeof(string));
        table.Columns.Add("utc_offset_minutes", typeof(int));
        table.Columns.Add("run_duration", typeof(int));
        foreach (var row in rows)
        {
            table.Rows.Add(row.Select(v => v ?? DBNull.Value).ToArray());
        }

        return table.CreateDataReader();
    }

    [Fact]
    public async Task ReadAsync_ReadsTheRunDurationColumn_AsSeconds_AndLeavesTheOtherColumnsAlone()
    {
        var start = new DateTime(2026, 11, 16, 1, 0, 0);
        var reader = ReaderOf(
            ["Nightly ETL", "job-1", start, 2, "Load", "boom", -300, 3844],
            ["Quick Job", "job-2", start, 0, "(Job outcome)", "no step", -300, 0],
            ["No Duration", "job-3", start, 1, "Step", "x", -300, null]);

        var items = await FailedJobsQuery.ReadAsync(reader, CancellationToken.None);

        Assert.Equal(3, items.Count);
        Assert.Equal(2324, items[0].RunDurationSeconds);
        Assert.Equal(start.AddSeconds(2324), items[0].FailedAt);
        Assert.Equal("2026-11-16 06:38:44Z", items[0].FailedAtFormatted);
        Assert.Equal("2026-11-16 06:00:00Z", items[0].RunDateTimeFormatted);
        Assert.Equal(-300, items[0].UtcOffsetMinutes);
        Assert.Equal(2, items[0].StepId);
        Assert.Equal("boom", items[0].Message);

        /* A real zero: the run failed within the second it started, and that is a value, not "unknown". */
        Assert.Equal(0, items[1].RunDurationSeconds);
        Assert.Equal("2026-11-16 06:00:00Z", items[1].FailedAtFormatted);

        Assert.Null(items[2].RunDurationSeconds);
        Assert.Null(items[2].FailedAtFormatted);
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(5, 5)]
    [InlineData(59, 59)]
    [InlineData(100, 60)]
    [InlineData(3844, 2324)]            /* 0h 38m 44s: the measured case */
    [InlineData(10000, 3600)]
    [InlineData(235959, 86399)]
    [InlineData(1000000, 360000)]       /* 100 hours exactly */
    [InlineData(1013045, 365445)]       /* 101h 30m 45s: more than 99 hours */
    public void RunDurationToSeconds_ReadsMsdbHhmmss(int stored, int seconds)
    {
        Assert.Equal(seconds, FailedJobsQuery.RunDurationToSeconds(stored));
    }

    [Fact]
    public async Task ReadAsync_EmptyReader_ReturnsEmptyList()
    {
        var items = await FailedJobsQuery.ReadAsync(new FakeCollectorDataReader(), CancellationToken.None);
        Assert.Empty(items);
    }
}
