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
/// Pins the cross-SKU parity contract of the shared job_history collector definition (issue #1433): the
/// msdb.dbo.sysjobhistory read with the sysjobs/syscategories joins and the HHMMSS-integer run_datetime /
/// run_duration decode, the THREE incremental-filter modes (steady instance_id high-water mark; TRUE
/// first-run all-history; Lite archival-emptied bounded run_datetime window), the collect-everywhere-but-
/// Azure-SQL-DB AppliesTo, the dual watermark declaration (numeric instance_id + timestamp run_datetime),
/// the 13-column payload, and the null-tolerant ReadAsync/WritePayload. An intentional change to any of
/// these must consciously update this test.
/// </summary>
public sealed class JobHistoryCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext(
        bool isAzureSqlDb = false,
        bool isAzureManagedInstance = false,
        long? numericWatermark = null,
        bool hasCollectedBefore = false,
        DateTime? collectionTime = null)
        => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = collectionTime ?? new DateTime(2026, 7, 11, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo { IsAzureSqlDb = isAzureSqlDb, IsAzureManagedInstance = isAzureManagedInstance },
            NumericWatermark = numericWatermark,
            HasCollectedBefore = hasCollectedBefore,
        };

    [Fact]
    public void Identity_Pinned()
    {
        Assert.Equal("job_history", JobHistoryCollector.Instance.Name);
        Assert.Equal("job_history", JobHistoryCollector.Instance.TargetTable);
        Assert.Equal("job_history_id", JobHistoryCollector.Instance.PrefixIdColumnName);
        /* Dedup is primarily numeric (instance_id); run_datetime is declared so the host computes
           HasCollectedBefore for the Lite archival-emptied fallback. */
        Assert.Equal("instance_id", JobHistoryCollector.Instance.NumericWatermarkColumn);
        Assert.Equal("run_datetime", JobHistoryCollector.Instance.WatermarkColumn);
    }

    [Fact]
    public void BuildQuery_ReadsSysjobhistory_WithDecodeIdioms()
    {
        var text = JobHistoryCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("msdb.dbo.sysjobhistory", text, StringComparison.Ordinal);
        Assert.Contains("msdb.dbo.sysjobs", text, StringComparison.Ordinal);
        Assert.Contains("msdb.dbo.syscategories", text, StringComparison.Ordinal);
        /* run_datetime decode from run_date + run_time; run_duration_seconds decode from run_duration. */
        Assert.Contains("(jh.run_time / 10000) * 3600", text, StringComparison.Ordinal);
        Assert.Contains("(jh.run_duration / 10000) * 3600", text, StringComparison.Ordinal);
        Assert.Contains("CONVERT(varchar(36), jh.job_id)", text, StringComparison.Ordinal);
        /* run_status decode covers every documented outcome. */
        Assert.Contains("N'Failed'", text, StringComparison.Ordinal);
        Assert.Contains("N'Succeeded'", text, StringComparison.Ordinal);
        Assert.Contains("N'Retry'", text, StringComparison.Ordinal);
        Assert.Contains("N'Canceled'", text, StringComparison.Ordinal);
        Assert.Contains("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED", text, StringComparison.Ordinal);
        Assert.Contains("OPTION(RECOMPILE)", text, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_SteadyState_FiltersOnInstanceIdWatermark_AsBigInt()
    {
        var query = JobHistoryCollector.Instance.BuildQuery(MakeContext(numericWatermark: 987654));

        Assert.Contains("jh.instance_id > @last_instance_id", query.Text, StringComparison.Ordinal);
        var p = query.Parameters.Single(x => x.Name == "@last_instance_id");
        Assert.Equal(987654L, p.Value);
        Assert.Equal(CollectorParameterType.BigInt, p.Type);
    }

    [Fact]
    public void BuildQuery_TrueFirstRun_CollectsEverything_NoIncrementalFilter()
    {
        /* No numeric watermark AND never succeeded → collect ALL of sysjobhistory (bounded by msdb's own
           history cap), the same first-run-collects-all choice default_trace_events makes. No filter params. */
        var query = JobHistoryCollector.Instance.BuildQuery(MakeContext(numericWatermark: null, hasCollectedBefore: false));

        Assert.DoesNotContain("jh.instance_id > @last_instance_id", query.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("DATEADD(HOUR, -24, GETDATE())", query.Text, StringComparison.Ordinal);
        Assert.Empty(query.Parameters);
    }

    [Fact]
    public void BuildQuery_ArchivalEmptied_UsesBoundedRunDatetimeWindow_ServerSide()
    {
        /* No numeric watermark but HAS succeeded before (Lite hot store emptied by parquet archival): a
           BOUNDED recent run_datetime window computed server-side against GETDATE() (run_datetime is server-
           local), so we never re-scan rows already in parquet — which v_job_history would double-count. */
        var query = JobHistoryCollector.Instance.BuildQuery(MakeContext(numericWatermark: null, hasCollectedBefore: true));

        Assert.DoesNotContain("jh.instance_id > @last_instance_id", query.Text, StringComparison.Ordinal);
        Assert.Contains("DATEADD(HOUR, -24, GETDATE())", query.Text, StringComparison.Ordinal);
        Assert.Contains("jh.run_date >=", query.Text, StringComparison.Ordinal);
        Assert.Empty(query.Parameters);
        Assert.Equal(24, JobHistoryCollector.ArchivalEmptyFallbackHours);
    }

    [Fact]
    public void AppliesTo_CollectsEverywhereExceptAzureSqlDbAndNoMsdb()
    {
        /* No SQL Agent / sysjobhistory on Azure SQL DB; Managed Instance and on-prem / RDS have it. */
        Assert.False(JobHistoryCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureSqlDb = true }));
        /* NOT gated on msdb access (#2559). It is a GRANT rather than an engine capability, and it was
           probed once and cached for the connection's life - so running the GRANT we advise did nothing
           until a restart. This now attempts and fails into PERMISSIONS, which error 916 already maps to,
           and CollectorHealthClassifier bands a never-permitted collector as NO_PERMISSIONS ahead of
           FAILING, so it does not read as broken. */
        Assert.True(JobHistoryCollector.Instance.AppliesTo(new CollectorTargetInfo { HasMsdbAccess = false }));
        /* NOT gated on AWS RDS — unlike running_jobs it never touches syssessions, so history reads fine. */
        Assert.True(JobHistoryCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAwsRds = true }));
        Assert.True(JobHistoryCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureManagedInstance = true }));
        Assert.True(JobHistoryCollector.Instance.AppliesTo(new CollectorTargetInfo()));
    }

    [Fact]
    public void PayloadColumns_ThirteenColumns_InAppendOrder()
    {
        var columns = JobHistoryCollector.Instance.PayloadColumns;

        Assert.Equal(new[]
        {
            "instance_id", "job_id", "job_name", "job_enabled", "category_name", "step_id", "step_name",
            "run_status", "run_status_desc", "run_datetime", "run_duration_seconds", "retries_attempted", "message",
        }, columns.Select(c => c.Name).ToArray());

        Assert.Equal(CollectorColumnType.BigInt, columns.Single(c => c.Name == "instance_id").Type);
        Assert.Equal(CollectorColumnType.Boolean, columns.Single(c => c.Name == "job_enabled").Type);
        Assert.Equal(CollectorColumnType.Integer, columns.Single(c => c.Name == "step_id").Type);
        Assert.Equal(CollectorColumnType.Timestamp, columns.Single(c => c.Name == "run_datetime").Type);
        Assert.Equal(CollectorColumnType.BigInt, columns.Single(c => c.Name == "run_duration_seconds").Type);
    }

    [Fact]
    public async Task ReadAsync_WritePayload_PinsThirteenColumnOrder_AndToleratesNulls()
    {
        var context = MakeContext();
        var runDateTime = new DateTime(2026, 7, 11, 2, 0, 0, DateTimeKind.Unspecified);

        var fullRow = new object[]
        {
            98765L, "AAAAAAAA-1111-2222-3333-444444444444", "Nightly Backup", true, "Database Maintenance",
            0, "(Job outcome)", 1, "Succeeded", runDateTime, 3661L, 0, "The job succeeded.",
        };
        /* An outcome row whose message/category are NULL still writes 13 values. */
        var nullishRow = new object[]
        {
            98766L, "BBBBBBBB-1111-2222-3333-444444444444", "Ad-hoc Job", false, (object)DBNull.Value,
            1, (object)DBNull.Value, 0, "Failed", runDateTime, 5L, 3, (object)DBNull.Value,
        };

        using var reader = new FakeCollectorDataReader(fullRow, nullishRow);
        var rows = await JobHistoryCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        Assert.Equal(2, rows.Count);

        var writer = new RecordingCollectorRowWriter();
        JobHistoryCollector.Instance.WritePayload(rows[0], writer, context);

        Assert.Equal(13, writer.Values.Count);
        Assert.Equal(98765L, writer.Values[0]);                         /* instance_id */
        Assert.Equal("Nightly Backup", writer.Values[2]);               /* job_name */
        Assert.Equal(true, writer.Values[3]);                           /* job_enabled */
        Assert.Equal("Database Maintenance", writer.Values[4]);         /* category_name */
        Assert.Equal(0, writer.Values[5]);                              /* step_id (outcome) */
        Assert.Equal(1, writer.Values[7]);                              /* run_status Succeeded */
        Assert.Equal(runDateTime, writer.Values[9]);                    /* run_datetime */
        Assert.Equal(3661L, writer.Values[10]);                         /* run_duration_seconds */
        Assert.Equal(0, writer.Values[11]);                             /* retries_attempted */
        Assert.Equal("The job succeeded.", writer.Values[12]);          /* message */

        var nullWriter = new RecordingCollectorRowWriter();
        JobHistoryCollector.Instance.WritePayload(rows[1], nullWriter, context);
        Assert.Equal(13, nullWriter.Values.Count);
        Assert.Null(nullWriter.Values[4]);   /* category_name NULL */
        Assert.Null(nullWriter.Values[6]);   /* step_name NULL */
        Assert.Null(nullWriter.Values[12]);  /* message NULL */
        Assert.Equal(3, nullWriter.Values[11]); /* retries_attempted */

        /* Capture is a point-in-time read with no delta interaction. */
        Assert.Empty(s_deltas.Calls);
    }
}
