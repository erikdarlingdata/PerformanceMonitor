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
/// SQL Agent service status snapshot (issue #1433 Phase 2) — the current-state facts retained job
/// history can't hold: whether the Agent service is Running/Stopped (<c>sys.dm_server_services</c>), its
/// startup type, and the next scheduled run across all enabled jobs (<c>msdb.dbo.sysjobschedules</c>).
/// One row per collection cycle per server; the Job History tab reads the latest row for its header
/// ("Agent: Running · next run …"), and Darling's self-alert path raises an "Agent Not Running" alert off
/// the same latest row (an unwatched stopped Agent on a headless 24/7 service is exactly the silent
/// failure nobody notices).
///
/// <para><b>Snapshot, not incremental:</b> like <c>server_properties</c> / <c>running_jobs</c> there is no
/// watermark — each cycle overwrites the picture (readers take the newest row). <c>next_scheduled_run</c>
/// is decoded from <c>sysjobschedules.next_run_date/next_run_time</c> via <c>msdb.dbo.agent_datetime</c>
/// (msdb-readable, unlike the <c>syssessions</c> path RunningJobsCollector uses, so it does not add the
/// running-jobs AWS RDS gate); the MIN over enabled jobs' future runs is the fleet-relevant "next thing
/// that will happen".</para>
///
/// <para><b>Azure SQL Database (edition 5), AWS RDS, and no-msdb-access logins: do NOT apply</b> — Azure SQL DB
/// has no SQL Agent and no <c>sys.dm_server_services</c>; AWS RDS is a managed service that does not expose the
/// OS/service-control state <c>sys.dm_server_services</c> reads; a login without msdb access can't read the
/// <c>sysjobschedules</c> next-run decode. <see cref="AppliesTo"/> =
/// <c>!IsAzureSqlDb &amp;&amp; !IsAwsRds &amp;&amp; HasMsdbAccess</c>, the single authoritative gate both hosts
/// consult, mirroring <see cref="RunningJobsCollector"/>. On-prem and Managed Instance collect normally.</para>
/// </summary>
public sealed class AgentStatusCollector : CollectorDefinitionBase<AgentStatusCollector.Row>
{
    public static AgentStatusCollector Instance { get; } = new();

    private AgentStatusCollector()
    {
    }

    public readonly record struct Row(
        bool AgentRunning,
        string? AgentStatusDesc,
        string? AgentStartupDesc,
        DateTime? NextScheduledRun);

    /* Agent service status from sys.dm_server_services (the service row whose name starts "SQL Server
       Agent"), plus the next scheduled run across all enabled jobs decoded from sysjobschedules via
       msdb.dbo.agent_datetime. Scalar subqueries with no outer FROM => exactly one row every cycle, even
       when the Agent service row is absent (agent_running 0, descriptions NULL). READ UNCOMMITTED like every
       collector; OPTION(RECOMPILE) for a trivially cheap plan. */
    private const string QueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    agent_running =
        CASE
            WHEN EXISTS
            (
                SELECT 1
                FROM sys.dm_server_services
                WHERE servicename LIKE N'SQL Server Agent%'
                AND   status_desc = N'Running'
            )
            THEN CONVERT(bit, 1)
            ELSE CONVERT(bit, 0)
        END,
    agent_status_desc =
    (
        SELECT TOP (1) dss.status_desc
        FROM sys.dm_server_services AS dss
        WHERE dss.servicename LIKE N'SQL Server Agent%'
        ORDER BY dss.servicename
    ),
    agent_startup_desc =
    (
        SELECT TOP (1) dss.startup_type_desc
        FROM sys.dm_server_services AS dss
        WHERE dss.servicename LIKE N'SQL Server Agent%'
        ORDER BY dss.servicename
    ),
    next_scheduled_run =
    (
        SELECT MIN(msdb.dbo.agent_datetime(sjs.next_run_date, sjs.next_run_time))
        FROM msdb.dbo.sysjobschedules AS sjs
        JOIN msdb.dbo.sysjobs AS j
          ON j.job_id = sjs.job_id
        WHERE j.enabled = 1
        AND   sjs.next_run_date > 0
    )
OPTION(RECOMPILE);";

    public override string Name => "agent_status";

    public override string TargetTable => "agent_status";

    /// <summary>
    /// Collects on SQL Server and Azure SQL Managed Instance. NOT Azure SQL Database (edition 5): no Agent
    /// and no sys.dm_server_services there. NOT AWS RDS either: sys.dm_server_services reads OS/service-control
    /// state that RDS (a managed service with no OS access) does not expose, so the query returns nothing
    /// useful and would drive a false "Agent Not Running" alert. NOT a login without msdb access either: the
    /// next-run decode reads <c>msdb.dbo.sysjobschedules</c>. All three gates live here in the shared
    /// AppliesTo — the single authoritative gate both SKUs consult.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) =>
        !target.IsAzureSqlDb && !target.IsAwsRds && target.HasMsdbAccess;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("agent_running", CollectorColumnType.Boolean),
        new CollectorColumn("agent_status_desc", CollectorColumnType.Varchar),
        new CollectorColumn("agent_startup_desc", CollectorColumnType.Varchar),
        new CollectorColumn("next_scheduled_run", CollectorColumnType.Timestamp),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                !reader.IsDBNull(0) && Convert.ToBoolean(reader.GetValue(0), CultureInfo.InvariantCulture),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetDateTime(3)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.AgentRunning)       /* agent_running BOOLEAN */
            .Value(row.AgentStatusDesc)    /* agent_status_desc VARCHAR */
            .Value(row.AgentStartupDesc)   /* agent_startup_desc VARCHAR */
            .Value(row.NextScheduledRun);  /* next_scheduled_run TIMESTAMP */
    }
}
