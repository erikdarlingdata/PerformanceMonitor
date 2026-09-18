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
        /* Appended (Darling V127 / Lite v60, #3540): the measured seconds the row's three deltas accrued
           over, or 0 when no delta was knowable. Appended at the END because both stores' writers are
           positional — the same rule GoldenCollectorSchema's header states for every column a numbered
           migration adds by ALTER TABLE. */
        new CollectorColumn("sample_interval_seconds", CollectorColumnType.Integer),
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
        /* Delta groups, keys, and the shared gap policy are the parity contract — do not reorder.

           The interval is stored beside the deltas (#3540). The calculator reports (delta 0, interval 0)
           when no delta is knowable — first sighting, counter reset, a gap past the policy — and (0, n)
           when the interval was genuinely idle, and that pairing is the ONLY way a reader can tell the two
           apart. This collector used to discard the interval at the write, so the fabricated zero survived
           as a measured one and every per-second reader LAG-divided it into a confident 0.00 ms/sec at
           exactly the moments (restarts) it was unknowable.

           One interval per ROW, the minimum over the row's three groups. The groups share a key and a
           collection time, so first-sighting, gap-policy and seeding decisions are identical across them
           and the three intervals agree in every case but an independent single-counter reset — which for
           this DMV means DBCC SQLPERF CLEAR, and that resets all three together. Taking the minimum rather
           than one headline group's value makes the stored pair mean "every delta in this row is knowable",
           so a reader never divides a reset counter's 0 by a sibling's real interval and reads it as idle. */
        var deltaWaitingTasks = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "wait_stats_tasks", row.WaitType, row.WaitingTasks, out var tasksInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaWaitTimeMs = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "wait_stats_time", row.WaitType, row.WaitTimeMs, out var timeInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaSignalWaitTimeMs = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "wait_stats_signal", row.WaitType, row.SignalWaitTimeMs, out var signalInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var sampleIntervalSeconds = Math.Min(tasksInterval, Math.Min(timeInterval, signalInterval));

        writer
            .Value(row.WaitType)              /* wait_type VARCHAR */
            .Value(row.WaitingTasks)          /* waiting_tasks_count BIGINT */
            .Value(row.WaitTimeMs)            /* wait_time_ms BIGINT */
            .Value(row.SignalWaitTimeMs)      /* signal_wait_time_ms BIGINT */
            .Value(deltaWaitingTasks)         /* delta_waiting_tasks BIGINT */
            .Value(deltaWaitTimeMs)           /* delta_wait_time_ms BIGINT */
            .Value(deltaSignalWaitTimeMs)     /* delta_signal_wait_time_ms BIGINT */
            .Value(sampleIntervalSeconds);    /* sample_interval_seconds INTEGER — measured, 0 = unknowable */
    }
}
