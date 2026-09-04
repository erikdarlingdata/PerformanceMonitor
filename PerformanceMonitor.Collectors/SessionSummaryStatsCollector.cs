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
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Server-wide session SUMMARY snapshot from sys.dm_exec_sessions (+ sys.dm_exec_requests for the
/// memory-wait count) — the connection-leak / idle-session signal that is the Dashboard-parity port
/// of install/36_collect_session_stats.sql. A POINT-IN-TIME snapshot, not a cumulative-counter delta
/// collector: it reports the current session-status breakdown directly, ONE aggregate row per
/// collection — no delta groups (mirrors <see cref="MemoryStatsCollector"/> /
/// <see cref="CpuSchedulerStatsCollector"/>). Counts total/running/sleeping/background/dormant
/// sessions, idle sessions over 30 minutes (the leak signal), sessions waiting on memory, distinct
/// databases with connections, and the top application/host by connection count. The Dashboard proc's
/// collection_log logging and RAISERROR idle-percent debug output are the collector framework's job
/// and are intentionally dropped.
///
/// <para>Distinct from the existing <see cref="SessionStatsCollector"/> (table session_stats), which
/// emits ONE ROW PER program_name for per-application resource use — a different shape from a
/// different Dashboard source. This collector's server-wide summary is deliberately named
/// session_summary_stats so the two never collide.</para>
///
/// <para>Available on SQL Server, Azure SQL Database, and Azure SQL Managed Instance — both DMVs are
/// verified against MS Learn ("Applies to" lists all three) and the query never fails on any of them,
/// so <see cref="AppliesTo"/> is unconditionally true (mirroring the sibling
/// <see cref="SessionStatsCollector"/>, which reads the same sys.dm_exec_sessions everywhere, and the
/// Dashboard proc, which has no platform guard). On Azure SQL Database the counts are inherently
/// SCOPED, not absent: sys.dm_exec_sessions with VIEW DATABASE STATE sees only sessions in the
/// current database, and sys.dm_exec_requests is always limited to the current connection there
/// (VIEW SERVER STATE cannot be granted on Azure SQL DB), so sessions_waiting_for_memory reflects
/// only this connection. That is a documented platform property of the DMVs — a narrower view, not a
/// collection gap — exactly the kind of Azure scoping <see cref="PlanCacheStatsCollector"/> notes and
/// keeps collecting through.</para>
/// </summary>
public sealed class SessionSummaryStatsCollector : CollectorDefinitionBase<SessionSummaryStatsCollector.Row>
{
    public static SessionSummaryStatsCollector Instance { get; } = new();

    private SessionSummaryStatsCollector()
    {
    }

    public readonly record struct Row(
        int TotalSessions,
        int RunningSessions,
        int SleepingSessions,
        int BackgroundSessions,
        int DormantSessions,
        int IdleSessionsOver30Min,
        int SessionsWaitingForMemory,
        int DatabasesWithConnections,
        string? TopApplicationName,
        int? TopApplicationConnections,
        string? TopHostName,
        int? TopHostConnections);

    /* Ported verbatim from install/36_collect_session_stats.sql's SELECT (the point-in-time
       aggregate — one row, no GROUP BY). The only additions over the proc body are explicit
       CONVERT(integer, ...) wrappers the proc relied on the INSERT into its integer columns to do
       implicitly (COUNT_BIG for total_sessions / databases_with_connections, and MAX-of-COUNT_BIG
       for the two top_*_connections) so the positional reader gets clean int32s — mirroring the
       CONVERT(integer, COUNT_BIG(*)) the plan_cache_stats port added. Values stored are identical to
       the Dashboard table (all counts integer NOT NULL; the four top_* columns nullable). */
    private const string QueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
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

    public override string Name => "session_summary_stats";

    public override string TargetTable => "session_summary_stats";

    public override string? WatermarkColumn => null;

    /// <summary>
    /// Collects everywhere: sys.dm_exec_sessions and sys.dm_exec_requests exist on SQL Server, Azure
    /// SQL Database, and Azure SQL Managed Instance (MS Learn-verified) and the query never fails.
    /// On Azure SQL DB the counts are scoped (sessions to the current database, the memory-wait count
    /// to the current connection) — a platform property, not a gap. Mirrors SessionStatsCollector.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("total_sessions", CollectorColumnType.Integer),
        new CollectorColumn("running_sessions", CollectorColumnType.Integer),
        new CollectorColumn("sleeping_sessions", CollectorColumnType.Integer),
        new CollectorColumn("background_sessions", CollectorColumnType.Integer),
        new CollectorColumn("dormant_sessions", CollectorColumnType.Integer),
        new CollectorColumn("idle_sessions_over_30min", CollectorColumnType.Integer),
        new CollectorColumn("sessions_waiting_for_memory", CollectorColumnType.Integer),
        new CollectorColumn("databases_with_connections", CollectorColumnType.Integer),
        new CollectorColumn("top_application_name", CollectorColumnType.Varchar),
        new CollectorColumn("top_application_connections", CollectorColumnType.Integer),
        new CollectorColumn("top_host_name", CollectorColumnType.Varchar),
        new CollectorColumn("top_host_connections", CollectorColumnType.Integer),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                TotalSessions: reader.GetInt32(0),
                RunningSessions: reader.GetInt32(1),
                SleepingSessions: reader.GetInt32(2),
                BackgroundSessions: reader.GetInt32(3),
                DormantSessions: reader.GetInt32(4),
                IdleSessionsOver30Min: reader.GetInt32(5),
                SessionsWaitingForMemory: reader.GetInt32(6),
                DatabasesWithConnections: reader.GetInt32(7),
                /* The three top_* OUTER APPLYs yield NULL when no user session qualifies (no program /
                   host); the name and its count are nullable, matching the Dashboard columns. */
                TopApplicationName: reader.IsDBNull(8) ? null : reader.GetString(8),
                TopApplicationConnections: reader.IsDBNull(9) ? null : reader.GetInt32(9),
                TopHostName: reader.IsDBNull(10) ? null : reader.GetString(10),
                TopHostConnections: reader.IsDBNull(11) ? null : reader.GetInt32(11)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.TotalSessions)             /* total_sessions INTEGER */
            .Value(row.RunningSessions)           /* running_sessions INTEGER */
            .Value(row.SleepingSessions)          /* sleeping_sessions INTEGER */
            .Value(row.BackgroundSessions)        /* background_sessions INTEGER */
            .Value(row.DormantSessions)           /* dormant_sessions INTEGER */
            .Value(row.IdleSessionsOver30Min)     /* idle_sessions_over_30min INTEGER */
            .Value(row.SessionsWaitingForMemory)  /* sessions_waiting_for_memory INTEGER */
            .Value(row.DatabasesWithConnections)  /* databases_with_connections INTEGER */
            .Value(row.TopApplicationName)        /* top_application_name VARCHAR (nullable) */
            .Value(row.TopApplicationConnections) /* top_application_connections INTEGER (nullable) */
            .Value(row.TopHostName)               /* top_host_name VARCHAR (nullable) */
            .Value(row.TopHostConnections);       /* top_host_connections INTEGER (nullable) */
    }
}
