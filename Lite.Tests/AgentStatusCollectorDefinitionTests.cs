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
/// Pins the cross-SKU parity contract of the shared agent_status collector definition (issue #1433
/// Phase 2): the sys.dm_server_services Agent-service read + the msdb.dbo.sysjobschedules next-run decode
/// (via msdb.dbo.agent_datetime, NOT the syssessions path — so it does not inherit running_jobs' RDS gate),
/// the snapshot shape (no watermark), the collect-everywhere-but-Azure-SQL-DB AppliesTo, the 4-column
/// payload, and the null-tolerant ReadAsync/WritePayload.
/// </summary>
public sealed class AgentStatusCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext(bool isAzureSqlDb = false, bool isAzureManagedInstance = false)
        => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 7, 11, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo { IsAzureSqlDb = isAzureSqlDb, IsAzureManagedInstance = isAzureManagedInstance },
        };

    [Fact]
    public void Identity_Pinned()
    {
        Assert.Equal("agent_status", AgentStatusCollector.Instance.Name);
        Assert.Equal("agent_status", AgentStatusCollector.Instance.TargetTable);
        /* Snapshot collector — no watermark of either kind. */
        Assert.Null(AgentStatusCollector.Instance.WatermarkColumn);
        Assert.Null(AgentStatusCollector.Instance.NumericWatermarkColumn);
    }

    [Fact]
    public void BuildQuery_ReadsAgentServiceAndNextScheduledRun()
    {
        var text = AgentStatusCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("sys.dm_server_services", text, StringComparison.Ordinal);
        Assert.Contains("N'SQL Server Agent%'", text, StringComparison.Ordinal);
        Assert.Contains("status_desc = N'Running'", text, StringComparison.Ordinal);
        /* Next-run via sysjobschedules + agent_datetime (msdb-readable), NOT the syssessions path. */
        Assert.Contains("msdb.dbo.sysjobschedules", text, StringComparison.Ordinal);
        Assert.Contains("msdb.dbo.agent_datetime", text, StringComparison.Ordinal);
        Assert.DoesNotContain("syssessions", text, StringComparison.Ordinal);
        Assert.Contains("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED", text, StringComparison.Ordinal);
        Assert.Contains("OPTION(RECOMPILE)", text, StringComparison.Ordinal);
    }

    [Fact]
    public void AppliesTo_CollectsEverywhereExceptAzureSqlDbRdsAndNoMsdb()
    {
        /* Azure SQL DB has no Agent / no sys.dm_server_services; AWS RDS is a managed service that doesn't
           expose the OS/service-control state sys.dm_server_services reads — skip both rather than return an
           empty snapshot that would drive a false "Agent Not Running" alert. */
        Assert.False(AgentStatusCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.False(AgentStatusCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAwsRds = true }));
        /* NOT gated on msdb access (#2559). It is a GRANT rather than an engine capability, and it was
           probed once and cached for the connection's life - so running the GRANT we advise did nothing
           until a restart. This now attempts and fails into PERMISSIONS, which error 916 already maps to,
           and CollectorHealthClassifier bands a never-permitted collector as NO_PERMISSIONS ahead of
           FAILING, so it does not read as broken. */
        Assert.True(AgentStatusCollector.Instance.AppliesTo(new CollectorTargetInfo { HasMsdbAccess = false }));
        /* Managed Instance has Agent; on-prem collects too. Bare target = msdb assumed accessible. */
        Assert.True(AgentStatusCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureManagedInstance = true }));
        Assert.True(AgentStatusCollector.Instance.AppliesTo(new CollectorTargetInfo()));
    }

    [Fact]
    public void PayloadColumns_FourColumns_InAppendOrder()
    {
        var columns = AgentStatusCollector.Instance.PayloadColumns;

        Assert.Equal(new[]
        {
            "agent_running", "agent_status_desc", "agent_startup_desc", "next_scheduled_run",
        }, columns.Select(c => c.Name).ToArray());

        Assert.Equal(CollectorColumnType.Boolean, columns.Single(c => c.Name == "agent_running").Type);
        Assert.Equal(CollectorColumnType.Varchar, columns.Single(c => c.Name == "agent_status_desc").Type);
        Assert.Equal(CollectorColumnType.Timestamp, columns.Single(c => c.Name == "next_scheduled_run").Type);
    }

    [Fact]
    public async Task ReadAsync_WritePayload_PinsFourColumnOrder_AndToleratesNulls()
    {
        var context = MakeContext();
        var nextRun = new DateTime(2026, 7, 12, 1, 0, 0, DateTimeKind.Unspecified);

        var runningRow = new object[] { true, "Running", "Automatic", nextRun };
        /* Agent stopped, no schedule → status/startup present, next_scheduled_run NULL. */
        var stoppedRow = new object[] { false, "Stopped", "Manual", (object)DBNull.Value };

        using var reader = new FakeCollectorDataReader(runningRow, stoppedRow);
        var rows = await AgentStatusCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        Assert.Equal(2, rows.Count);

        var writer = new RecordingCollectorRowWriter();
        AgentStatusCollector.Instance.WritePayload(rows[0], writer, context);
        Assert.Equal(4, writer.Values.Count);
        Assert.Equal(true, writer.Values[0]);       /* agent_running */
        Assert.Equal("Running", writer.Values[1]);  /* agent_status_desc */
        Assert.Equal("Automatic", writer.Values[2]);/* agent_startup_desc */
        Assert.Equal(nextRun, writer.Values[3]);    /* next_scheduled_run */

        var stoppedWriter = new RecordingCollectorRowWriter();
        AgentStatusCollector.Instance.WritePayload(rows[1], stoppedWriter, context);
        Assert.Equal(4, stoppedWriter.Values.Count);
        Assert.Equal(false, stoppedWriter.Values[0]);
        Assert.Null(stoppedWriter.Values[3]);       /* next_scheduled_run NULL */

        Assert.Empty(s_deltas.Calls);
    }
}
