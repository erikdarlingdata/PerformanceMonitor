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
/// Latch statistics from sys.dm_os_latch_stats — the cumulative per-latch-class waits that surface
/// contention on internal SQL Server structures (the Dashboard-parity port of
/// install/32_collect_latch_stats.sql). A cumulative-counter DMV like wait_stats, so it mirrors
/// <see cref="WaitStatsCollector"/> exactly: single DMV, delta-based between snapshots. The
/// Dashboard proc's server_start_time reset marker is intentionally dropped — the shared
/// <see cref="CollectorContext.Deltas"/> calculator detects a counter reset itself (a value drop
/// yields a 0 delta) and applies the shared gap policy, so no reset column is needed. Available on
/// SQL Server, Azure SQL Database, and Azure SQL Managed Instance (verified against MS Learn), so
/// <see cref="AppliesTo"/> is unconditionally true, matching wait_stats.
/// </summary>
public sealed class LatchStatsCollector : CollectorDefinitionBase<LatchStatsCollector.Row>
{
    public static LatchStatsCollector Instance { get; } = new();

    private LatchStatsCollector()
    {
    }

    public readonly record struct Row(string LatchClass, long WaitingRequestsCount, long WaitTimeMs, long MaxWaitTimeMs);

    private const string QueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    latch_class = ls.latch_class,
    waiting_requests_count = ls.waiting_requests_count,
    wait_time_ms = ls.wait_time_ms,
    max_wait_time_ms = ls.max_wait_time_ms
FROM sys.dm_os_latch_stats AS ls
WHERE ls.wait_time_ms > 0
OPTION(RECOMPILE);";

    public override string Name => "latch_stats";

    public override string TargetTable => "latch_stats";

    public override string? WatermarkColumn => null;

    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("latch_class", CollectorColumnType.Varchar),
        new CollectorColumn("waiting_requests_count", CollectorColumnType.BigInt),
        new CollectorColumn("wait_time_ms", CollectorColumnType.BigInt),
        new CollectorColumn("max_wait_time_ms", CollectorColumnType.BigInt),
        new CollectorColumn("delta_waiting_requests_count", CollectorColumnType.BigInt),
        new CollectorColumn("delta_wait_time_ms", CollectorColumnType.BigInt),
        new CollectorColumn("delta_max_wait_time_ms", CollectorColumnType.BigInt),
        /* Appended (Darling V127 / Lite v60, #3540): the measured seconds the row's deltas accrued over, or
           0 when no delta was knowable. At the END because both stores' writers are positional. */
        new CollectorColumn("sample_interval_seconds", CollectorColumnType.Integer),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                LatchClass: reader.GetString(0),
                WaitingRequestsCount: reader.GetInt64(1),
                WaitTimeMs: reader.GetInt64(2),
                MaxWaitTimeMs: reader.GetInt64(3)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* Delta groups, key (latch_class), and the shared gap policy are the parity contract.

           The interval is stored beside the deltas (#3540), the minimum over the row's three groups, so the
           stored (0, 0) pair means "no delta in this row is knowable" and a reader can tell it from a
           genuinely idle (0, n) — see WaitStatsCollector.WritePayload for the full argument; this collector
           mirrors it exactly, as its class comment says it mirrors everything else. The one latch-specific
           note: max_wait_time_ms is a high-water mark, not a rate counter, so its delta is usually 0 over a
           REAL interval — that is the idle case, correctly reported, and not a reason to leave it out of the
           minimum (a reset of the DMV resets all three together). */
        var deltaWaitingRequests = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "latch_stats_waiting_requests", row.LatchClass, row.WaitingRequestsCount, out var requestsInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaWaitTimeMs = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "latch_stats_wait_time", row.LatchClass, row.WaitTimeMs, out var timeInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaMaxWaitTimeMs = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "latch_stats_max_wait", row.LatchClass, row.MaxWaitTimeMs, out var maxWaitInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var sampleIntervalSeconds = Math.Min(requestsInterval, Math.Min(timeInterval, maxWaitInterval));

        writer
            .Value(row.LatchClass)             /* latch_class VARCHAR */
            .Value(row.WaitingRequestsCount)   /* waiting_requests_count BIGINT */
            .Value(row.WaitTimeMs)             /* wait_time_ms BIGINT */
            .Value(row.MaxWaitTimeMs)          /* max_wait_time_ms BIGINT */
            .Value(deltaWaitingRequests)       /* delta_waiting_requests_count BIGINT */
            .Value(deltaWaitTimeMs)            /* delta_wait_time_ms BIGINT */
            .Value(deltaMaxWaitTimeMs)         /* delta_max_wait_time_ms BIGINT */
            .Value(sampleIntervalSeconds);     /* sample_interval_seconds INTEGER — measured, 0 = unknowable */
    }
}
