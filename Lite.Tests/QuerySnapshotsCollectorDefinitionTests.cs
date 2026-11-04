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
/// Pins the parity contract of the extracted query_snapshots definition: the live-query-plan
/// version gate (2016 SP1+/MI, never Azure SQL DB — #857), the Azure #req-staging variant, the
/// CDC-capture detection on boxed vs the constant-0 on Azure, the exclusion clause injected ahead
/// of the ORDER BY, and the 35-column payload with the #1286 triage columns.
/// </summary>
public sealed class QuerySnapshotsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    [Theory]
    [InlineData(false, 16, false, true)]   /* modern boxed */
    [InlineData(false, 13, false, true)]   /* 2016 boundary */
    [InlineData(false, 0, false, true)]    /* version unknown — optimistic */
    [InlineData(false, 12, false, false)]  /* 2014 boxed — too old */
    [InlineData(false, 12, true, true)]    /* Managed Instance overrides version */
    [InlineData(true, 16, false, false)]   /* Azure SQL DB — never (#857) */
    public void SupportsLiveQueryPlan_PinsVersionGate(bool isAzureSqlDb, int major, bool isManagedInstance, bool expected)
    {
        var target = new CollectorTargetInfo
        {
            IsAzureSqlDb = isAzureSqlDb,
            IsAzureManagedInstance = isManagedInstance,
            SqlMajorVersion = major,
        };

        Assert.Equal(expected, QuerySnapshotsCollector.SupportsLiveQueryPlan(target));
    }

    [Fact]
    public void BuildSnapshotQuery_Standard_WithLivePlan_FillsBothHoles()
    {
        var sql = QuerySnapshotsCollector.BuildSnapshotQuery(supportsLiveQueryPlan: true, isAzureSqlDatabase: false);

        Assert.Contains("live_query_plan = deqs.query_plan,", sql, StringComparison.Ordinal);
        Assert.Contains("OUTER APPLY sys.dm_exec_query_statistics_xml(der.session_id) AS deqs", sql, StringComparison.Ordinal);
        Assert.Contains("msdb.dbo.cdc_jobs", sql, StringComparison.Ordinal);
        /* The existence pre-guard, not just the TRY/CATCH: cdc_jobs is created lazily on first CDC
           configuration, and TRY/CATCH suppresses the failure but NOT the server-side error_reported
           event — without the guard every no-CDC server fed fleet error monitoring a once-per-cycle
           "Invalid object name" 208. */
        Assert.Contains("IF OBJECT_ID(N'msdb.dbo.cdc_jobs') IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("sp_MScdc_capture_job", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("#req", sql, StringComparison.Ordinal);
        Assert.Contains("der.database_id <> ISNULL(DB_ID(N'PerformanceMonitor'), 0)", sql, StringComparison.Ordinal);
        Assert.Contains("OPTION(MAXDOP 1, RECOMPILE);", sql, StringComparison.Ordinal);
    }

    /* #7416 item 11: Active Queries on an idle server showed only the monitor's own plan read. Both reads (the standard one and
       Azure SQL Database's staged one) leave out a session whose program name starts with the monitor's prefix, keep a NULL program
       name, and share the pattern with SessionStatsCollector. The old text had no program_name predicate at all. */
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void BuildSnapshotQuery_LeavesOutTheMonitorsOwnSessions_ByTheSharedPrefix(bool azure)
    {
        var sql = QuerySnapshotsCollector.BuildSnapshotQuery(supportsLiveQueryPlan: false, isAzureSqlDatabase: azure);

        Assert.Contains("(des.program_name IS NULL OR des.program_name NOT LIKE N'PerformanceMonitor%')", sql, StringComparison.Ordinal);
        Assert.Contains("NOT LIKE N'PerformanceMonitor%'", PerformanceMonitor.Collectors.SessionStatsCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = 1,
            ServerName = "s",
            CollectionTime = DateTime.UtcNow,
            Deltas = s_deltas,
        }).Text, StringComparison.Ordinal);
    }

    /* Every name the monitor connects to a monitored SQL Server under begins with the shared prefix, so the LIKE catches it. A user's
       application that merely contains the words, or has no program name, is not the monitor's. */
    [Theory]
    [InlineData("PerformanceMonitorDarling", true)]
    [InlineData("PerformanceMonitorDarling-Remediation", true)]
    [InlineData("PerformanceMonitorDarling-HypotheticalIndex", true)]
    [InlineData("PerformanceMonitorLite", true)]
    [InlineData("MyPerformanceMonitorTool", false)]
    [InlineData("Orders.Api", false)]
    [InlineData("", false)]
    [InlineData(null, false)]
    public void MonitorProgramNamePrefix_MatchesTheMonitorsConnectionNames_AndNotAUserApplicationThatContainsTheWords(string? name, bool monitors)
    {
        Assert.Equal(monitors, PerformanceMonitor.Common.MonitorApplicationNames.MatchesSqlProgramNamePattern(name));
    }

    [Fact]
    public void MonitorProgramNamePrefix_CoversEveryConstantTheConnectionBuildersUse()
    {
        Assert.True(PerformanceMonitor.Common.MonitorApplicationNames.MatchesSqlProgramNamePattern(PerformanceMonitor.Common.MonitorApplicationNames.Darling));
        Assert.True(PerformanceMonitor.Common.MonitorApplicationNames.MatchesSqlProgramNamePattern(PerformanceMonitor.Common.MonitorApplicationNames.Lite));
    }

    /* #7416 item 35: a blank des.login_name fell through as '' and split one caller into two rows. The projection keeps the column name
       and falls back to original_login_name (on sys.dm_exec_sessions in every engine this collector runs on, Azure SQL Database too). */
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void BuildSnapshotQuery_BlankLoginFallsBackToTheOriginalLogin(bool azure)
    {
        var sql = QuerySnapshotsCollector.BuildSnapshotQuery(supportsLiveQueryPlan: false, isAzureSqlDatabase: azure);

        Assert.Contains("login_name = COALESCE(NULLIF(des.login_name, N''), des.original_login_name),", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("    des.login_name,", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildSnapshotQuery_Standard_WithoutLivePlan_NullColumnAndNoApply()
    {
        var sql = QuerySnapshotsCollector.BuildSnapshotQuery(supportsLiveQueryPlan: false, isAzureSqlDatabase: false);

        Assert.Contains("live_query_plan = CONVERT(xml, NULL),", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("dm_exec_query_statistics_xml", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildSnapshotQuery_Azure_StagesReqTable_NoCdcJobs()
    {
        var sql = QuerySnapshotsCollector.BuildSnapshotQuery(supportsLiveQueryPlan: false, isAzureSqlDatabase: true);

        Assert.Contains("INTO #req", sql, StringComparison.Ordinal);
        Assert.Contains("AND   der.database_id = DB_ID()", sql, StringComparison.Ordinal);
        Assert.Contains("FROM #req AS der", sql, StringComparison.Ordinal);
        Assert.Contains("is_cdc_capture = CONVERT(bit, 0),", sql, StringComparison.Ordinal);
        Assert.Contains("DROP TABLE #req;", sql, StringComparison.Ordinal);
        /* No CDC job probe on Azure (the template's comment mentions cdc_jobs, so pin the machinery). */
        Assert.DoesNotContain("@cdc_readable", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@cdc_capture_jobs", sql, StringComparison.Ordinal);
        Assert.Contains("live_query_plan = CONVERT(xml, NULL),", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_InjectsExclusionClause_BeforeOrderBy()
    {
        var plan = QuerySnapshotsCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = DateTime.UtcNow,
            Deltas = s_deltas,
            ExcludedDatabases = new[] { "SO", "AdventureWorks" },
        });

        Assert.Contains(
            "AND d.name NOT IN (@excl_db_0, @excl_db_1)\nORDER BY der.cpu_time DESC, der.parallel_worker_count DESC",
            plan.Text,
            StringComparison.Ordinal);
        Assert.Equal(2, plan.Parameters.Count);
        Assert.Equal("SO", plan.Parameters[0].Value);
        Assert.Equal("AdventureWorks", plan.Parameters[1].Value);
        /* Default target = version-unknown boxed, so the live-plan hole fills optimistically. */
        Assert.Contains("live_query_plan = deqs.query_plan,", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_Azure_UsesReqStaging_DisablesLivePlan_SingleConnection()
    {
        var plan = QuerySnapshotsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas, isAzureSqlDb: true));

        Assert.Contains("INTO #req", plan.Text, StringComparison.Ordinal);
        Assert.Contains("live_query_plan = CONVERT(xml, NULL),", plan.Text, StringComparison.Ordinal);
        Assert.Empty(plan.Parameters);
        /* Unlike query_stats, snapshots stay single-connection on Azure — the #req staging scopes it. */
        Assert.False(QuerySnapshotsCollector.Instance.RunsPerDatabase(new CollectorTargetInfo { IsAzureSqlDb = true }));
    }

    [Fact]
    public void PayloadColumns_MatchSchemaOrder_35Columns()
    {
        var names = QuerySnapshotsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Equal(35, names.Length);
        Assert.Equal("session_id", names[0]);
        Assert.Equal("live_query_plan", names[5]);
        Assert.Equal("is_cdc_capture", names[25]);
        Assert.Equal("query_hash", names[26]);
        Assert.Equal("requested_memory_mb", names[27]);
        Assert.Equal("tran_start_time", names[33]);
        Assert.Equal("request_id", names[34]);
        Assert.Equal("query_snapshots", QuerySnapshotsCollector.Instance.Name);
        Assert.Equal("query_snapshots", QuerySnapshotsCollector.Instance.TargetTable);
    }

    [Fact]
    public async Task WritePayload_Pins35ColumnOrder_NullHandling_NoDeltas()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var context = CollectorTestContext.Make(deltas);
        var tranStart = new DateTime(2026, 7, 2, 3, 0, 0, DateTimeKind.Utc);

        var row = new object[35];
        row[0] = 55;                         /* session_id */
        row[1] = "SO";                       /* database_name */
        row[2] = "00 00:00:01.000";          /* elapsed_time_formatted */
        row[3] = "SELECT 1";                 /* query_text */
        row[4] = "<plan/>";                  /* query_plan */
        row[5] = "<live/>";                  /* live_query_plan */
        row[6] = "running";                  /* status */
        row[7] = DBNull.Value;               /* blocking_session_id -> 0 */
        row[8] = "CXPACKET";                 /* wait_type */
        row[9] = 123L;                       /* wait_time_ms */
        row[10] = "PAGE: 1:1:1";             /* wait_resource */
        row[11] = 456L;                      /* cpu_time_ms */
        row[12] = 789L;                      /* total_elapsed_time_ms */
        row[13] = 1L;                        /* reads */
        row[14] = 2L;                        /* writes */
        row[15] = 3L;                        /* logical_reads */
        row[16] = 1.50m;                     /* granted_query_memory_gb (GetDecimal) */
        row[17] = "Read Committed";          /* transaction_isolation_level */
        row[18] = 8;                         /* dop */
        row[19] = 16;                        /* parallel_worker_count */
        row[20] = "sa";                      /* login_name */
        row[21] = "host";                    /* host_name */
        row[22] = "app";                     /* program_name */
        row[23] = 2;                         /* open_transaction_count */
        row[24] = 50.5f;                     /* percent_complete: DMV real -> Convert.ToDecimal */
        row[25] = true;                      /* is_cdc_capture */
        row[26] = "0x1234";                  /* query_hash */
        row[27] = DBNull.Value;              /* requested_memory_mb -> null */
        row[28] = 10.25m;                    /* used_memory_mb: decimal -> Convert.ToDouble */
        row[29] = 20.50m;                    /* max_used_memory_mb */
        row[30] = 1.25m;                     /* tempdb_current_mb */
        row[31] = 2.50m;                     /* tempdb_allocations_mb */
        row[32] = DBNull.Value;              /* tran_log_used_mb -> null */
        row[33] = tranStart;                 /* tran_start_time */
        row[34] = 0;                         /* request_id */

        using var reader = new FakeCollectorDataReader(row);
        var rows = await QuerySnapshotsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var writer = new RecordingCollectorRowWriter();
        QuerySnapshotsCollector.Instance.WritePayload(Assert.Single(rows), writer, context);

        Assert.Equal(35, writer.Values.Count);
        Assert.Equal(55, writer.Values[0]);
        Assert.Equal("<live/>", writer.Values[5]);
        Assert.Equal(0, writer.Values[7]);              /* NULL blocking_session_id coalesces to 0 */
        Assert.Equal(1.50m, writer.Values[16]);
        Assert.Equal(50.5m, writer.Values[24]);         /* float converted to decimal */
        Assert.Equal(true, writer.Values[25]);
        Assert.Null(writer.Values[27]);                 /* NULL grant columns stay null */
        Assert.Equal(10.25, writer.Values[28]);         /* decimal converted to double */
        Assert.Null(writer.Values[32]);
        Assert.Equal(tranStart, writer.Values[33]);
        Assert.Equal(0, writer.Values[34]);
        Assert.Empty(deltas.Calls);                     /* point-in-time snapshot — no deltas */
    }
}
