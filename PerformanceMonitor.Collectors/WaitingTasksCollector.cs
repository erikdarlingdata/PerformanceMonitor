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
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Point-in-time waiting tasks from sys.dm_os_waiting_tasks. Extracted verbatim from Lite's
/// RemoteCollectorService.WaitingTasks.cs — session ids read as smallint and widened to int on
/// write, resource_description stays a NULL placeholder (no longer collected), and the
/// database-exclusion filter splices at /*EXCLUSION_FILTER*/. Benign "ignored" wait types are
/// skipped in <see cref="ReadAsync"/> via <c>context.IgnoredWaitTypes</c> — the same filter
/// <see cref="WaitStatsCollector"/> applies — so the "Current Waits" surface honors the user's ignore
/// list in both SKUs (Lite: ignored_wait_types.json, Darling: IgnoredWaitDefaults.All).
/// </summary>
public sealed class WaitingTasksCollector : CollectorDefinitionBase<WaitingTasksCollector.Row>
{
    public static WaitingTasksCollector Instance { get; } = new();

    private WaitingTasksCollector()
    {
    }

    public readonly record struct Row(short SessionId, string? WaitType, long WaitDurationMs, short? BlockingSessionId, string? DatabaseName);

    private const string QueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */
    session_id = wt.session_id,
    wait_type = wt.wait_type,
    wait_duration_ms = wt.wait_duration_ms,
    blocking_session_id = wt.blocking_session_id,
    database_name = d.name
FROM sys.dm_os_waiting_tasks AS wt
LEFT JOIN sys.dm_exec_requests AS er
  ON er.session_id = wt.session_id
LEFT JOIN sys.databases AS d
  ON d.database_id = er.database_id
WHERE wt.session_id >= 50
AND   wt.session_id <> @@SPID
AND   wt.wait_type IS NOT NULL
AND   er.database_id <> ISNULL(DB_ID(N'PerformanceMonitor'), 0)
/*EXCLUSION_FILTER*/
OPTION(RECOMPILE);";

    public override string Name => "waiting_tasks";

    public override string TargetTable => "waiting_tasks";

    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        var (exclusionClause, exclusionParameters) = DatabaseExclusionFilter.Build(context.ExcludedDatabases, "d.name");
        return new CollectorQuery(
            QueryText.Replace("/*EXCLUSION_FILTER*/", exclusionClause, StringComparison.Ordinal),
            exclusionParameters);
    }

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("session_id", CollectorColumnType.Integer),
        new CollectorColumn("wait_type", CollectorColumnType.Varchar),
        new CollectorColumn("wait_duration_ms", CollectorColumnType.BigInt),
        new CollectorColumn("blocking_session_id", CollectorColumnType.Integer),
        new CollectorColumn("resource_description", CollectorColumnType.Varchar),
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            /* Trimmed first, exactly as WaitStatsCollector.ReadAsync does (see WaitTypeName): a name with a
               trailing space would miss its ignore entry and be stored under a key no lookup uses. */
            var waitType = reader.IsDBNull(1) ? null : WaitTypeName.Trim(reader.GetString(1));

            /* Skip ignored (benign) wait types so "Current Waits" honors the same filter as wait_stats
               — mirrors WaitStatsCollector.ReadAsync. The query already drops NULL wait_type; the null
               guard keeps the Contains call safe regardless. */
            if (waitType is not null && context.IgnoredWaitTypes.Contains(waitType))
            {
                continue;
            }

            /* session_id and blocking_session_id are smallint in sys.dm_os_waiting_tasks */
            rows.Add(new Row(
                reader.IsDBNull(0) ? (short)0 : reader.GetInt16(0),
                waitType,
                reader.IsDBNull(2) ? 0L : reader.GetInt64(2),
                reader.IsDBNull(3) ? null : reader.GetInt16(3),
                reader.IsDBNull(4) ? null : reader.GetString(4)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value((int)row.SessionId)                                              /* session_id INTEGER (widened) */
            .Value(row.WaitType)                                                    /* wait_type VARCHAR */
            .Value(row.WaitDurationMs)                                              /* wait_duration_ms BIGINT */
            .Value(row.BlockingSessionId.HasValue ? row.BlockingSessionId.Value : (int?)null) /* blocking_session_id INTEGER (widened) */
            .Value((string?)null)                                                   /* resource_description — no longer collected */
            .Value(row.DatabaseName);                                               /* database_name VARCHAR */
    }
}
