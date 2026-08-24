/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the parity contract of the extracted running_jobs definition — notably the ONLY collector
/// without a collection_id prefix column, plus the p95/HHMMSS-decode query and 10-column payload.
/// </summary>
public sealed class RunningJobsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    [Fact]
    public void NoCollectionId_IsThePinnedException()
    {
        Assert.False(RunningJobsCollector.Instance.IncludesCollectionId);
        /* Every other definition keeps the standard 4-column prefix. */
        Assert.True(WaitStatsCollector.Instance.IncludesCollectionId);
        Assert.True(SessionStatsCollector.Instance.IncludesCollectionId);
    }

    [Fact]
    public void AppliesTo_SkipsAzureSqlDbAndNoMsdb_ButCollectsOnPremManagedInstanceAndRds()
    {
        /* Azure SQL DB has no SQL Agent (msdb unreachable) — skip rather than log a per-cycle ERROR. */
        Assert.False(RunningJobsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureSqlDb = true }));
        /* #2575: RDS COLLECTS. The old reasoning was that the msdb.dbo.syssessions join needs sysadmin,
           which RDS never grants, so the read "would ERROR every cycle". Measured across 84 RDS instances:
           SUCCESS on every one, with zero non-SUCCESS rows — the premise is not true of RDS today. And a
           denial is SQL 229, which CollectorTargetFault classifies as a PERMISSIONS degrade rather than an
           ERROR, so even where the premise holds the cost is one non-fatal row the precondition read then
           explains with the grant. The gate guarded a failure mode the fault classifier did not yet
           handle. */
        Assert.True(RunningJobsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAwsRds = true }));
        /* No msdb access → every table this reads lives in msdb; skip (gate collapsed from
           IsCollectorSupported into the shared AppliesTo). */
        Assert.False(RunningJobsCollector.Instance.AppliesTo(new CollectorTargetInfo { HasMsdbAccess = false }));
        /* Managed Instance has Agent; on-prem collects too. */
        Assert.True(RunningJobsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureManagedInstance = true }));
        Assert.True(RunningJobsCollector.Instance.AppliesTo(new CollectorTargetInfo()));
    }

    [Fact]
    public void NamesColumnsAndQuery_MatchSchema()
    {
        Assert.Equal("running_jobs", RunningJobsCollector.Instance.Name);
        var query = RunningJobsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas)).Text;
        Assert.Contains("msdb.dbo.sysjobactivity", query, StringComparison.Ordinal);
        Assert.Contains("PERCENTILE_CONT(0.95)", query, StringComparison.Ordinal);
        Assert.Contains("(jh.run_duration / 10000) * 3600", query, StringComparison.Ordinal);
        Assert.Equal(
            new[]
            {
                "job_name", "job_id", "job_enabled", "start_time", "current_duration_seconds",
                "avg_duration_seconds", "p95_duration_seconds", "successful_run_count",
                "is_running_long", "percent_of_average",
            },
            RunningJobsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray());
    }

    [Fact]
    public async Task ReadAndWrite_RoundTrip_WithNullPercentOfAverage()
    {
        var start = new DateTime(2026, 7, 2, 1, 0, 0, DateTimeKind.Utc);
        using var reader = new FakeCollectorDataReader(
            new object[] { "Nightly ETL", "9F0C", true, start, 4200L, 3600L, 4000L, 30L, true, 116.7m },
            new object[] { "New Job", "AB12", true, start, 60L, 0L, 0L, 0L, false, DBNull.Value });

        var rows = await RunningJobsCollector.Instance.ReadAsync(reader, CollectorTestContext.Make(s_deltas), CancellationToken.None);

        Assert.Equal(2, rows.Count);
        Assert.Null(rows[1].PercentOfAverage);

        var writer = new RecordingCollectorRowWriter();
        RunningJobsCollector.Instance.WritePayload(rows[0], writer, CollectorTestContext.Make(s_deltas));
        Assert.Equal(new object?[] { "Nightly ETL", "9F0C", true, start, 4200L, 3600L, 4000L, 30L, true, 116.7m }, writer.Values);
    }
}
