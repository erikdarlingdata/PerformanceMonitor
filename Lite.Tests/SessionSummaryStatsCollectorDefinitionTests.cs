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
/// Pins the cross-SKU parity contract of the shared session_summary_stats collector definition (the
/// Dashboard-parity port of install/36_collect_session_stats.sql — the server-wide session SUMMARY /
/// connection-leak / idle signal): verbatim query text, the collect-everywhere AppliesTo (the DMVs
/// exist on all platforms; Azure SQL DB merely scopes the counts), payload column order, ReadAsync
/// mapping (including the nullable top-application/host columns), and the point-in-time no-delta
/// WritePayload. An intentional change to any of these must consciously update these tests. Distinct
/// from the per-application session_stats collector.
/// </summary>
public sealed class SessionSummaryStatsCollectorDefinitionTests
{
    private const string ExpectedQuery = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */
    total_sessions = CONVERT(integer, COUNT_BIG(*)),
    running_sessions =
        SUM
        (
            CASE
                WHEN des.status = N'running'
                THEN 1
                ELSE 0
            END
        ),
    sleeping_sessions =
        SUM
        (
            CASE
                WHEN des.status = N'sleeping'
                THEN 1
                ELSE 0
            END
        ),
    background_sessions =
        SUM
        (
            CASE
                WHEN des.is_user_process = 0
                THEN 1
                ELSE 0
            END
        ),
    dormant_sessions =
        SUM
        (
            CASE
                WHEN des.status = N'dormant'
                THEN 1
                ELSE 0
            END
        ),
    idle_sessions_over_30min =
        SUM
        (
            CASE
                WHEN des.status = N'sleeping'
                AND des.last_request_end_time < DATEADD(MINUTE, -30, SYSDATETIME())
                AND des.is_user_process = 1
                THEN 1
                ELSE 0
            END
        ),
    sessions_waiting_for_memory =
        SUM
        (
            CASE
                WHEN der.wait_type LIKE N'%MEMORY%'
                THEN 1
                ELSE 0
            END
        ),
    databases_with_connections = CONVERT(integer, COUNT_BIG(DISTINCT des.database_id)),
    top_application_name = MAX(top_app.program_name),
    top_application_connections = CONVERT(integer, MAX(top_app.connection_count)),
    top_host_name = MAX(top_host.host_name),
    top_host_connections = CONVERT(integer, MAX(top_host.connection_count))
FROM sys.dm_exec_sessions AS des
LEFT JOIN sys.dm_exec_requests AS der
  ON der.session_id = des.session_id
OUTER APPLY
(
    SELECT TOP (1)
        program_name = des2.program_name,
        connection_count = COUNT_BIG(*)
    FROM sys.dm_exec_sessions AS des2
    WHERE des2.session_id > 50
    AND   des2.is_user_process = 1
    AND   des2.program_name IS NOT NULL
    GROUP BY
        des2.program_name
    ORDER BY
        COUNT_BIG(*) DESC
) AS top_app
OUTER APPLY
(
    SELECT TOP (1)
        host_name = des3.host_name,
        connection_count = COUNT_BIG(*)
    FROM sys.dm_exec_sessions AS des3
    WHERE des3.session_id > 50
    AND   des3.is_user_process = 1
    AND   des3.host_name IS NOT NULL
    GROUP BY
        des3.host_name
    ORDER BY
        COUNT_BIG(*) DESC
) AS top_host
WHERE des.session_id > 50
OPTION(RECOMPILE);";

    [Fact]
    public void Query_IsTheVerbatimParityContract()
    {
        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());
        Assert.Equal(ExpectedQuery, SessionSummaryStatsCollector.Instance.BuildQuery(context).Text);
        Assert.Empty(SessionSummaryStatsCollector.Instance.BuildQuery(context).Parameters);
        Assert.Null(SessionSummaryStatsCollector.Instance.WatermarkColumn);
        Assert.Equal("session_summary_stats", SessionSummaryStatsCollector.Instance.Name);
        Assert.Equal("session_summary_stats", SessionSummaryStatsCollector.Instance.TargetTable);
    }

    [Fact]
    public void AppliesTo_CollectsEverywhere_IncludingAzureSqlDb()
    {
        /* Both DMVs exist on SQL Server, Azure SQL DB, and Managed Instance (MS Learn-verified) and
           the query never fails — Azure SQL DB just scopes the counts. Mirrors SessionStatsCollector. */
        Assert.True(SessionSummaryStatsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.True(SessionSummaryStatsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureManagedInstance = true }));
        Assert.True(SessionSummaryStatsCollector.Instance.AppliesTo(new CollectorTargetInfo()));
    }

    [Fact]
    public void PayloadColumns_AreInAppendOrder()
    {
        var names = SessionSummaryStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Equal(
            new[]
            {
                "total_sessions",
                "running_sessions",
                "sleeping_sessions",
                "background_sessions",
                "dormant_sessions",
                "idle_sessions_over_30min",
                "sessions_waiting_for_memory",
                "databases_with_connections",
                "top_application_name",
                "top_application_connections",
                "top_host_name",
                "top_host_connections",
            },
            names);

        /* All counts are integer; the two top_* names are varchar (nullable), matching the Dashboard. */
        Assert.Equal(CollectorColumnType.Integer, SessionSummaryStatsCollector.Instance.PayloadColumns.Single(c => c.Name == "idle_sessions_over_30min").Type);
        Assert.Equal(CollectorColumnType.Varchar, SessionSummaryStatsCollector.Instance.PayloadColumns.Single(c => c.Name == "top_application_name").Type);
        Assert.Equal(CollectorColumnType.Integer, SessionSummaryStatsCollector.Instance.PayloadColumns.Single(c => c.Name == "top_host_connections").Type);
    }

    [Fact]
    public async Task ReadAsync_MapsColumns_IncludingNullableTopAppHostColumns()
    {
        using var reader = new FakeCollectorDataReader(
            /* A busy row: a top application and host both present. */
            new object[]
            {
                42, 3, 30, 5, 1, 8, 0, 4, "MyApp", 20, "WEB01", 15,
            },
            /* A quiet row (no qualifying user session for the top-app/host APPLYs): the three top_*
               columns come back NULL. */
            new object[]
            {
                1, 1, 0, 0, 0, 0, 0, 1, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
            });

        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());

        var rows = await SessionSummaryStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(2, rows.Count);
        Assert.Equal(
            new SessionSummaryStatsCollector.Row(42, 3, 30, 5, 1, 8, 0, 4, "MyApp", 20, "WEB01", 15),
            rows[0]);
        Assert.Equal(
            new SessionSummaryStatsCollector.Row(1, 1, 0, 0, 0, 0, 0, 1, null, null, null, null),
            rows[1]);
    }

    [Fact]
    public void WritePayload_EmitsPayloadOrder_PointInTime_NoDeltas()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var context = CollectorTestContext.Make(deltas);
        var writer = new RecordingCollectorRowWriter();
        var row = new SessionSummaryStatsCollector.Row(42, 3, 30, 5, 1, 8, 0, 4, "MyApp", 20, "WEB01", 15);

        SessionSummaryStatsCollector.Instance.WritePayload(row, writer, context);

        Assert.Equal(
            new object?[]
            {
                42, 3, 30, 5, 1, 8, 0, 4, "MyApp", 20, "WEB01", 15,
            },
            writer.Values);

        /* Point-in-time snapshot — no delta calculator interaction at all. */
        Assert.Empty(deltas.Calls);
    }
}
