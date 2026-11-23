/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

public static partial class TimescaleSupport
{
    /// <summary>
    /// The hypertable conversion, compression settings and compression policy of the two per-snapshot object usage chain tables
    /// (<see cref="ObjectUsageChain.PerSnapshotTables"/>, #9349). They sit outside the collector catalog, so
    /// <see cref="ConvertToHypertablesAsync"/> and <see cref="ApplyCompressionPolicyAsync"/> never reach them, and they are not a migration's
    /// work either: a migration runs before the extension is created on a fresh store, and a store without TimescaleDB keeps them plain (the
    /// retention pass then purges them with the time-sliced DELETE). The same three idempotent statements the collector tables get, as
    /// <see cref="EnsureCollectionLogHypertableAsync"/> runs them: <c>create_hypertable</c> on <c>collection_time</c> at
    /// <see cref="ChunkIntervalDays"/> (a table the V299 rung created is empty, so nothing moves), the compression enable with
    /// <see cref="ObjectUsageChain.CompressionSegmentBy"/> (the reader filters on server and database, then a time range) and the policy at
    /// <see cref="CompressAfterDays"/>, the same chunk width and age as <c>index_object_stats</c>. The ALTER takes an ACCESS EXCLUSIVE lock even
    /// when it changes nothing (#3817), so it runs only for a table whose settings differ, in its own transaction, waiting at most
    /// <see cref="HourlyDdlLockTimeout"/>. A table that cannot be converted keeps its current form and the next pass tries again.
    ///
    /// <para>Returns true only when both tables are hypertables with the settings and the policy in place.</para>
    /// </summary>
    public static async Task<bool> EnsureObjectUsageHypertablesAsync(NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var allReady = true;
        foreach (var table in ObjectUsageChain.PerSnapshotTables)
        {
            var qualified = "collect." + table;
            try
            {
                using var convert = new NpgsqlCommand(CreateHypertableSql(qualified, ObjectUsageChain.TimeColumn), connection) { CommandTimeout = SetupTimeoutSeconds };
                await convert.ExecuteNonQueryAsync(cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogWarning(
                    "{Table} could not be converted to a hypertable, so it keeps its current form (as a plain table, its DELETE-based retention still honors its horizon); the next pass tries again: {Message}",
                    qualified, ex.Message);
                allReady = false;
                continue;
            }

            var converged = await ReadTablesNeedingCompressionEnableAsync(connection, logger, cancellationToken);
            if (converged is null || !converged.Contains(table))
            {
                var outcome = await TryRunBoundedDdlAsync(
                    connection, new[] { EnableCompressionSql(qualified) }, logger, qualified + "'s compression settings", cancellationToken);
                if (outcome == BoundedDdlOutcome.LockBusy)
                {
                    allReady = false;
                    continue;
                }

                if (outcome == BoundedDdlOutcome.Failed)
                {
                    allReady = false;
                }
            }

            try
            {
                using var policy = new NpgsqlCommand(AddCompressionPolicySql(qualified), connection) { CommandTimeout = SetupTimeoutSeconds };
                await policy.ExecuteNonQueryAsync(cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogWarning(
                    "{Table}'s compression policy could not be added; a policy that already exists keeps running, and the next pass tries again: {Message}",
                    qualified, ex.Message);
                allReady = false;
            }
        }

        return allReady;
    }
}
