/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Wait statistics from sys.dm_os_wait_stats. Extracted verbatim from Lite's
/// RemoteCollectorService.WaitStats.cs (query text, ignored-wait filtering, delta groups/keys/gap
/// policy, and payload order are the parity contract — WaitStatsCollectorDefinitionTests pins them).
/// </summary>
public sealed class WaitStatsCollector : CollectorDefinitionBase<WaitStatsCollector.Row>
{
    public static WaitStatsCollector Instance { get; } = new();

    private WaitStatsCollector()
    {
    }

    public readonly record struct Row(string WaitType, long WaitingTasks, long WaitTimeMs, long SignalWaitTimeMs);

    private const string QueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    wait_type = ws.wait_type,
    waiting_tasks_count = ws.waiting_tasks_count,
    wait_time_ms = ws.wait_time_ms,
    signal_wait_time_ms = ws.signal_wait_time_ms
FROM sys.dm_os_wait_stats AS ws
WHERE ws.wait_time_ms > 0
OPTION(RECOMPILE);";

    public override string Name => "wait_stats";

    public override string TargetTable => "wait_stats";

    public override string? WatermarkColumn => null;

    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("wait_type", CollectorColumnType.Varchar),
        new CollectorColumn("waiting_tasks_count", CollectorColumnType.BigInt),
        new CollectorColumn("wait_time_ms", CollectorColumnType.BigInt),
        new CollectorColumn("signal_wait_time_ms", CollectorColumnType.BigInt),
        new CollectorColumn("delta_waiting_tasks", CollectorColumnType.BigInt),
        new CollectorColumn("delta_wait_time_ms", CollectorColumnType.BigInt),
        new CollectorColumn("delta_signal_wait_time_ms", CollectorColumnType.BigInt),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            var waitType = reader.GetString(0);

            /* Skip ignored wait types (Lite: ignored_wait_types.json — #1240) */
            if (context.IgnoredWaitTypes.Contains(waitType))
            {
                continue;
            }

            rows.Add(new Row(
                WaitType: waitType,
                WaitingTasks: reader.GetInt64(1),
                WaitTimeMs: reader.GetInt64(2),
                SignalWaitTimeMs: reader.GetInt64(3)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* Delta groups, keys, and the shared gap policy are the parity contract — do not reorder. */
        var deltaWaitingTasks = context.Deltas.CalculateDelta(context.ServerId, "wait_stats_tasks", row.WaitType, row.WaitingTasks, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaWaitTimeMs = context.Deltas.CalculateDelta(context.ServerId, "wait_stats_time", row.WaitType, row.WaitTimeMs, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaSignalWaitTimeMs = context.Deltas.CalculateDelta(context.ServerId, "wait_stats_signal", row.WaitType, row.SignalWaitTimeMs, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);

        writer
            .Value(row.WaitType)              /* wait_type VARCHAR */
            .Value(row.WaitingTasks)          /* waiting_tasks_count BIGINT */
            .Value(row.WaitTimeMs)            /* wait_time_ms BIGINT */
            .Value(row.SignalWaitTimeMs)      /* signal_wait_time_ms BIGINT */
            .Value(deltaWaitingTasks)         /* delta_waiting_tasks BIGINT */
            .Value(deltaWaitTimeMs)           /* delta_wait_time_ms BIGINT */
            .Value(deltaSignalWaitTimeMs);    /* delta_signal_wait_time_ms BIGINT */
    }
}
