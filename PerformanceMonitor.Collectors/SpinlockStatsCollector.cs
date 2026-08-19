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
/// Spinlock statistics from sys.dm_os_spinlock_stats — the cumulative per-spinlock collision/spin
/// counters that expose low-level internal contention (the Dashboard-parity port of
/// install/33_collect_spinlock_stats.sql). A cumulative-counter DMV like wait_stats, so it mirrors
/// <see cref="WaitStatsCollector"/>: single DMV, delta-based between snapshots. collisions, spins,
/// sleep_time, and backoffs are the cumulative counters that get deltas; spins_per_collision is a
/// point-in-time ratio the DMV computes (type <c>real</c>) and is passed through without a delta,
/// matching the Dashboard table. The proc's server_start_time reset marker is intentionally
/// dropped — the shared <see cref="CollectorContext.Deltas"/> calculator detects a counter reset
/// itself (a value drop yields a 0 delta). Available on SQL Server, Azure SQL Database, and Azure
/// SQL Managed Instance (verified against MS Learn), so <see cref="AppliesTo"/> is unconditionally
/// true, matching wait_stats.
/// </summary>
public sealed class SpinlockStatsCollector : CollectorDefinitionBase<SpinlockStatsCollector.Row>
{
    public static SpinlockStatsCollector Instance { get; } = new();

    private SpinlockStatsCollector()
    {
    }

    public readonly record struct Row(string SpinlockName, long Collisions, long Spins, double SpinsPerCollision, long SleepTime, long Backoffs);

    private const string QueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    spinlock_name = ss.name,
    collisions = ss.collisions,
    spins = ss.spins,
    spins_per_collision = ss.spins_per_collision,
    sleep_time = ss.sleep_time,
    /* backoffs is int in sys.dm_os_spinlock_stats on SQL 2016/2017 and was widened to bigint on
       2019+. CONVERT so the wire type is always bigint and ReadAsync's GetInt64 doesn't throw an
       Int32->Int64 InvalidCast on the older versions (collisions/spins/sleep_time are bigint on
       every supported version, so they need no cast). */
    backoffs = CONVERT(bigint, ss.backoffs)
FROM sys.dm_os_spinlock_stats AS ss
WHERE ss.collisions > 0
OR    ss.spins > 0
OPTION(RECOMPILE);";

    public override string Name => "spinlock_stats";

    public override string TargetTable => "spinlock_stats";

    public override string? WatermarkColumn => null;

    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("spinlock_name", CollectorColumnType.Varchar),
        new CollectorColumn("collisions", CollectorColumnType.BigInt),
        new CollectorColumn("spins", CollectorColumnType.BigInt),
        new CollectorColumn("spins_per_collision", CollectorColumnType.Double),
        new CollectorColumn("sleep_time", CollectorColumnType.BigInt),
        new CollectorColumn("backoffs", CollectorColumnType.BigInt),
        new CollectorColumn("delta_collisions", CollectorColumnType.BigInt),
        new CollectorColumn("delta_spins", CollectorColumnType.BigInt),
        new CollectorColumn("delta_sleep_time", CollectorColumnType.BigInt),
        new CollectorColumn("delta_backoffs", CollectorColumnType.BigInt),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            /* spins_per_collision is a real (float4) in the DMV — read with GetFloat and widen to
               double; GetDouble would throw an invalid-cast on a real column. */
            rows.Add(new Row(
                SpinlockName: reader.GetString(0),
                Collisions: reader.GetInt64(1),
                Spins: reader.GetInt64(2),
                SpinsPerCollision: reader.GetFloat(3),
                SleepTime: reader.GetInt64(4),
                Backoffs: reader.GetInt64(5)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* Delta groups, key (spinlock_name), and the shared gap policy are the parity contract.
           spins_per_collision is a computed ratio, not a cumulative counter — no delta (mirrors
           the Dashboard's collect.spinlock_stats table). */
        var deltaCollisions = context.Deltas.CalculateDelta(context.ServerId, "spinlock_stats_collisions", row.SpinlockName, row.Collisions, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaSpins = context.Deltas.CalculateDelta(context.ServerId, "spinlock_stats_spins", row.SpinlockName, row.Spins, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaSleepTime = context.Deltas.CalculateDelta(context.ServerId, "spinlock_stats_sleep_time", row.SpinlockName, row.SleepTime, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaBackoffs = context.Deltas.CalculateDelta(context.ServerId, "spinlock_stats_backoffs", row.SpinlockName, row.Backoffs, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);

        writer
            .Value(row.SpinlockName)       /* spinlock_name VARCHAR */
            .Value(row.Collisions)         /* collisions BIGINT */
            .Value(row.Spins)              /* spins BIGINT */
            .Value(row.SpinsPerCollision)  /* spins_per_collision DOUBLE */
            .Value(row.SleepTime)          /* sleep_time BIGINT */
            .Value(row.Backoffs)           /* backoffs BIGINT */
            .Value(deltaCollisions)        /* delta_collisions BIGINT */
            .Value(deltaSpins)             /* delta_spins BIGINT */
            .Value(deltaSleepTime)         /* delta_sleep_time BIGINT */
            .Value(deltaBackoffs);         /* delta_backoffs BIGINT */
    }
}
