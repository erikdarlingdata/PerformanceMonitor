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
/// Per-application session statistics from sys.dm_exec_sessions, grouped by program_name
/// (connection counts, status breakdown, cumulative resource usage). Extracted verbatim from
/// Lite's RemoteCollectorService.SessionStats.cs.
/// </summary>
public sealed class SessionStatsCollector : CollectorDefinitionBase<SessionStatsCollector.Row>
{
    public static SessionStatsCollector Instance { get; } = new();

    private SessionStatsCollector()
    {
    }

    public readonly record struct Row(
        string ProgramName,
        long ConnectionCount,
        int RunningCount,
        int SleepingCount,
        int DormantCount,
        long? TotalCpuTimeMs,
        long? TotalReads,
        long? TotalWrites,
        long? TotalLogicalReads);

    private const string QueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */
    program_name =
        ISNULL(des.program_name, N''),
    connection_count =
        COUNT_BIG(*),
    running_count =
        SUM
        (
            CASE
                WHEN des.status = N'running'
                THEN 1
                ELSE 0
            END
        ),
    sleeping_count =
        SUM
        (
            CASE
                WHEN des.status = N'sleeping'
                THEN 1
                ELSE 0
            END
        ),
    dormant_count =
        SUM
        (
            CASE
                WHEN des.status = N'dormant'
                THEN 1
                ELSE 0
            END
        ),
    total_cpu_time_ms =
        SUM(des.cpu_time),
    total_reads =
        SUM(des.reads),
    total_writes =
        SUM(des.writes),
    total_logical_reads =
        SUM(des.logical_reads)
FROM sys.dm_exec_sessions AS des
WHERE des.session_id > 50
AND   des.is_user_process = 1
AND   des.program_name IS NOT NULL
AND   des.program_name <> N''
AND   des.program_name NOT LIKE N'PerformanceMonitor%'
GROUP BY
    des.program_name
ORDER BY
    COUNT_BIG(*) DESC
OPTION(RECOMPILE);";

    public override string Name => "session_stats";

    public override string TargetTable => "session_stats";

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("program_name", CollectorColumnType.Varchar),
        new CollectorColumn("connection_count", CollectorColumnType.BigInt),
        new CollectorColumn("running_count", CollectorColumnType.Integer),
        new CollectorColumn("sleeping_count", CollectorColumnType.Integer),
        new CollectorColumn("dormant_count", CollectorColumnType.Integer),
        new CollectorColumn("total_cpu_time_ms", CollectorColumnType.BigInt),
        new CollectorColumn("total_reads", CollectorColumnType.BigInt),
        new CollectorColumn("total_writes", CollectorColumnType.BigInt),
        new CollectorColumn("total_logical_reads", CollectorColumnType.BigInt),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                reader.GetString(0),
                Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture),
                Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture),
                Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture),
                Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture),
                reader.IsDBNull(5) ? null : Convert.ToInt64(reader.GetValue(5), CultureInfo.InvariantCulture),
                reader.IsDBNull(6) ? null : Convert.ToInt64(reader.GetValue(6), CultureInfo.InvariantCulture),
                reader.IsDBNull(7) ? null : Convert.ToInt64(reader.GetValue(7), CultureInfo.InvariantCulture),
                reader.IsDBNull(8) ? null : Convert.ToInt64(reader.GetValue(8), CultureInfo.InvariantCulture)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.ProgramName)        /* program_name VARCHAR */
            .Value(row.ConnectionCount)    /* connection_count BIGINT */
            .Value(row.RunningCount)       /* running_count INTEGER */
            .Value(row.SleepingCount)      /* sleeping_count INTEGER */
            .Value(row.DormantCount)       /* dormant_count INTEGER */
            .Value(row.TotalCpuTimeMs)     /* total_cpu_time_ms BIGINT */
            .Value(row.TotalReads)         /* total_reads BIGINT */
            .Value(row.TotalWrites)        /* total_writes BIGINT */
            .Value(row.TotalLogicalReads); /* total_logical_reads BIGINT */
    }
}
