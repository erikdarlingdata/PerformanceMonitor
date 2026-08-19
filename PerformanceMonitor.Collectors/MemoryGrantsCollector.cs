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
/// Memory grant statistics from sys.dm_exec_query_resource_semaphores (same DMV as Dashboard,
/// for parity). Extracted verbatim from Lite's RemoteCollectorService.MemoryGrants.cs — the
/// composite "{pool}_{semaphore}" delta key and the two delta groups are the parity contract.
/// </summary>
public sealed class MemoryGrantsCollector : CollectorDefinitionBase<MemoryGrantsCollector.Row>
{
    public static MemoryGrantsCollector Instance { get; } = new();

    private MemoryGrantsCollector()
    {
    }

    public readonly record struct Row(
        short ResourceSemaphoreId,
        int PoolId,
        decimal TargetMb,
        decimal MaxTargetMb,
        decimal TotalMb,
        decimal AvailableMb,
        decimal GrantedMb,
        decimal UsedMb,
        int GranteeCount,
        int WaiterCount,
        long TimeoutErrorCount,
        long ForcedGrantCount);

    public override string Name => "memory_grant_stats";

    public override string TargetTable => "memory_grant_stats";

    public override string? WatermarkColumn => null;

    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    private const string QueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    resource_semaphore_id = deqrs.resource_semaphore_id,
    pool_id = deqrs.pool_id,
    target_memory_mb = CONVERT(decimal(18,2), deqrs.target_memory_kb / 1024.0),
    max_target_memory_mb = CONVERT(decimal(18,2), deqrs.max_target_memory_kb / 1024.0),
    total_memory_mb = CONVERT(decimal(18,2), deqrs.total_memory_kb / 1024.0),
    available_memory_mb = CONVERT(decimal(18,2), deqrs.available_memory_kb / 1024.0),
    granted_memory_mb = CONVERT(decimal(18,2), ISNULL(deqrs.granted_memory_kb, 0) / 1024.0),
    used_memory_mb = CONVERT(decimal(18,2), ISNULL(deqrs.used_memory_kb, 0) / 1024.0),
    grantee_count = deqrs.grantee_count,
    waiter_count = deqrs.waiter_count,
    timeout_error_count = ISNULL(deqrs.timeout_error_count, 0),
    forced_grant_count = ISNULL(deqrs.forced_grant_count, 0)
FROM sys.dm_exec_query_resource_semaphores AS deqrs
WHERE deqrs.max_target_memory_kb IS NOT NULL
OPTION(RECOMPILE);";

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("resource_semaphore_id", CollectorColumnType.SmallInt),
        new CollectorColumn("pool_id", CollectorColumnType.Integer),
        new CollectorColumn("target_memory_mb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("max_target_memory_mb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("total_memory_mb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("available_memory_mb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("granted_memory_mb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("used_memory_mb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("grantee_count", CollectorColumnType.Integer),
        new CollectorColumn("waiter_count", CollectorColumnType.Integer),
        new CollectorColumn("timeout_error_count", CollectorColumnType.BigInt),
        new CollectorColumn("forced_grant_count", CollectorColumnType.BigInt),
        new CollectorColumn("timeout_error_count_delta", CollectorColumnType.BigInt),
        new CollectorColumn("forced_grant_count_delta", CollectorColumnType.BigInt),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                Convert.ToInt16(reader.GetValue(0), CultureInfo.InvariantCulture),
                Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture),
                reader.IsDBNull(2) ? 0m : reader.GetDecimal(2),
                reader.IsDBNull(3) ? 0m : reader.GetDecimal(3),
                reader.IsDBNull(4) ? 0m : reader.GetDecimal(4),
                reader.IsDBNull(5) ? 0m : reader.GetDecimal(5),
                reader.IsDBNull(6) ? 0m : reader.GetDecimal(6),
                reader.IsDBNull(7) ? 0m : reader.GetDecimal(7),
                reader.IsDBNull(8) ? 0 : Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture),
                reader.IsDBNull(9) ? 0 : Convert.ToInt32(reader.GetValue(9), CultureInfo.InvariantCulture),
                reader.IsDBNull(10) ? 0L : Convert.ToInt64(reader.GetValue(10), CultureInfo.InvariantCulture),
                reader.IsDBNull(11) ? 0L : Convert.ToInt64(reader.GetValue(11), CultureInfo.InvariantCulture)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* Composite delta key and group names are the parity contract — do not change casually. */
        var deltaKey = $"{row.PoolId}_{row.ResourceSemaphoreId}";
        var deltaTimeouts = context.Deltas.CalculateDelta(context.ServerId, "memory_grants_timeouts", deltaKey, row.TimeoutErrorCount, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaForced = context.Deltas.CalculateDelta(context.ServerId, "memory_grants_forced", deltaKey, row.ForcedGrantCount, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);

        writer
            .Value(row.ResourceSemaphoreId)   /* resource_semaphore_id (appended as SHORT, matching the original) */
            .Value(row.PoolId)                /* pool_id INTEGER */
            .Value(row.TargetMb)              /* target_memory_mb DECIMAL */
            .Value(row.MaxTargetMb)           /* max_target_memory_mb DECIMAL */
            .Value(row.TotalMb)               /* total_memory_mb DECIMAL */
            .Value(row.AvailableMb)           /* available_memory_mb DECIMAL */
            .Value(row.GrantedMb)             /* granted_memory_mb DECIMAL */
            .Value(row.UsedMb)                /* used_memory_mb DECIMAL */
            .Value(row.GranteeCount)          /* grantee_count INTEGER */
            .Value(row.WaiterCount)           /* waiter_count INTEGER */
            .Value(row.TimeoutErrorCount)     /* timeout_error_count BIGINT */
            .Value(row.ForcedGrantCount)      /* forced_grant_count BIGINT */
            .Value(deltaTimeouts)             /* timeout_error_count_delta BIGINT */
            .Value(deltaForced);              /* forced_grant_count_delta BIGINT */
    }
}
