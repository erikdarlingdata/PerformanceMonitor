// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>Which interval table a floor warm-up reads (#5581).</summary>
public enum IntervalFloorTable
{
    /// <summary><c>collect.query_store_interval_wide</c>, read by <see cref="QueryStoreIntervalWide.PlainTableFloorSql"/>.</summary>
    Wide,

    /// <summary><c>collect.query_store_interval_latest</c>, read by <see cref="QueryStoreIntervalLatest.PlainTableFloorSql"/>.</summary>
    Latest,
}

/// <summary>What one table's warm-up did, for the log line and for the tests (#5581).</summary>
internal sealed record IntervalFloorWarmUpResult(
    IntervalFloorTable Table,
    int ServersWarmed,
    int ServersSkippedOnLock,
    int ServersFailed,
    TimeSpan Elapsed,
    int? SlowestServerId,
    TimeSpan SlowestElapsed,
    bool Cancelled);

/// <summary>
/// #5581: after a retention drain removed rows from an interval table, runs the read gate's own floor read once
/// per server, so the first real Query Store read after the drain does not pay for the walk.
///
/// <para><b>Why.</b> The gate's floor read (<c>MIN(first_execution_time) WHERE server_id = $1</c>) walks the
/// (server_id, first_execution_time) index from the oldest entry. The drain just deleted the oldest rows of every
/// server, and until vacuum removes their index entries, or a read marks them dead, each one costs a heap fetch
/// to find out the row is gone: on a large store 19.6 s and 854,535 heap fetches for one server, against 1.8 s
/// before the drain. The first walk marks the dead entries (the index scan's kill-prior-tuple hint), so the
/// next read took 15 ms. This pays that first walk in the background, at the end of the retention pass,
/// instead of on someone's first read. Autovacuum stays in charge of reclaiming space.</para>
///
/// <para><b>Same statement, same plan.</b> It executes the shipped <c>PlainTableFloorSql</c> constants by
/// reference, never a copy, so it walks the index the gate walks. <c>FloorSql</c> is the only place that
/// chooses between them, and a source pin fails if this file runs anything else against an interval table.</para>
///
/// <para><b>Cost when nothing is dead.</b> Each read is then one index probe (milliseconds), which is why there
/// is no size threshold: the caller gates on "the purge deleted any row or failed" (<see cref="LeftDeadEntries"/>),
/// and a no-op warm-up is cheap.</para>
///
/// <para><b>Partitioned tables (#5573).</b> Both interval tables are partitioned by day, so the floor read on the
/// parent is a MIN over each partition's (server_id, first_execution_time) index. Only the legacy table and the
/// DEFAULT partition are ever row-deleted; a whole expired day is dropped with its indexes. So the dead entries the
/// read walks past are in those two, and a pass that only dropped partitions leaves none.</para>
///
/// <para><b>The limit.</b> PostgreSQL marks an index entry dead only when its row is dead to every open
/// snapshot. A transaction that was open before the drain finished keeps the entries live, so the warm-up pays
/// the walk and the next read pays it again. The warm-up cannot see or end such a transaction.</para>
///
/// <para><b>Where it runs.</b> At the end of <see cref="DarlingRetention.PurgeWithPacerAsync"/>, which both of its
/// callers run in the background <c>_purgeTask</c> slot, never on the collection loop: the daily purge
/// (<c>TryStartScheduledPurge</c> in <c>DarlingWorker</c>, fire-and-track) and <c>purge_now</c>
/// (<c>RunPurgeNowBackgroundAsync</c>). A pass over many servers on a large store can take about ten minutes (43 servers,
/// 645 s measured), so the slot stays occupied that long; one connection, one server at a time, no parallel reads.</para>
///
/// <para><b>Isolation.</b> One server at a time, each on its own connection with a short <c>lock_timeout</c>:
/// the read takes only ACCESS SHARE, so a lock timeout means an ACCESS EXCLUSIVE request is queued (a schema
/// upgrade step, a partition step), and the server is skipped and logged rather than queued behind it, which
/// would stall every collector write behind that request. A timeout, a cancel or a failure is logged and never
/// counts as a failed purge.</para>
/// </summary>
internal static class QueryStoreIntervalFloorWarmUp
{
    /// <summary>
    /// The client-side command timeout for one server's floor read, in seconds. The slowest first read measured
    /// after a drain on a large store was 38.9 s (19.6 s on the first store looked at), so this is about 7x that. A
    /// read that still passes it is logged and skipped, but it has already marked the dead entries it walked past,
    /// so the next pass's warm-up (or the read itself) continues from there rather than from the start.
    /// </summary>
    internal const int FloorReadTimeoutSeconds = 300;

    /// <summary>The server-side <c>lock_timeout</c> for the warm-up's connection, in milliseconds.</summary>
    internal const int LockTimeoutMilliseconds = 3000;

    /// <summary>The registry read: every registered server's id, newest registrations included. Tiny.</summary>
    private const string ServerIdsSql = "SELECT server_id FROM collect.servers ORDER BY server_id";

    /// <summary>PostgreSQL's <c>lock_not_available</c> SQLSTATE, what <c>lock_timeout</c> raises.</summary>
    private const string LockNotAvailable = "55P03";

    /// <summary>The shipped floor read for <paramref name="table"/>; never a copy.</summary>
    internal static string FloorSql(IntervalFloorTable table) => table switch
    {
        IntervalFloorTable.Wide => QueryStoreIntervalWide.PlainTableFloorSql,
        IntervalFloorTable.Latest => QueryStoreIntervalLatest.PlainTableFloorSql,
        _ => throw new ArgumentOutOfRangeException(nameof(table), table, null),
    };

    /// <summary>
    /// The retention pass's entry: warms each table whose interval purge may have left dead index entries.
    /// <paramref name="wide"/> and <paramref name="latest"/> are <see cref="DarlingRetention.PurgeIntervalTableAsync"/>'s
    /// results for the two day-partitioned tables (#5573). See <see cref="LeftDeadEntries"/> for which results warm.
    /// Never throws.
    /// </summary>
    internal static async Task RunAfterDrainAsync(
        NpgsqlDataSource postgres,
        DarlingRetention.IntervalPartitionPurge wide,
        DarlingRetention.IntervalPartitionPurge latest,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        try
        {
            if (LeftDeadEntries(wide))
            {
                await WarmAsync(postgres, IntervalFloorTable.Wide, logger, cancellationToken);
            }

            if (LeftDeadEntries(latest))
            {
                await WarmAsync(postgres, IntervalFloorTable.Latest, logger, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            /* WarmAsync isolates its own faults; this is the belt for anything it did not foresee. A warm-up is
               an optimisation: it must not fail, or even disturb, the purge that already finished. */
            logger?.LogWarning("Interval floor warm-up stopped: {Failure}", ex.Message);
        }
    }

    /// <summary>
    /// Whether one table's interval purge may have left dead index entries (#5573, #5581): it deleted rows, or a part
    /// of it failed (a part that failed may have committed batches before it did). The dead entries live in the legacy
    /// table and the DEFAULT partition, the only places rows are deleted one by one. A pass that only dropped whole
    /// day partitions deleted no row and failed nothing: a dropped partition takes its indexes with it, so there is
    /// nothing to walk past and nothing to warm.
    /// </summary>
    internal static bool LeftDeadEntries(DarlingRetention.IntervalPartitionPurge purge) =>
        purge.RowsDeleted > 0 || purge.Failed > 0;

    /// <summary>
    /// Runs <paramref name="table"/>'s floor read once per registered server, one at a time. Logs one line for
    /// the table. Never throws: a cancel stops the table and is reported in the result.
    /// </summary>
    internal static async Task<IntervalFloorWarmUpResult> WarmAsync(
        NpgsqlDataSource postgres, IntervalFloorTable table, ILogger? logger, CancellationToken cancellationToken,
        int lockTimeoutMilliseconds = LockTimeoutMilliseconds, int commandTimeoutSeconds = FloorReadTimeoutSeconds)
    {
        var sql = FloorSql(table);
        var tableName = table.ToString().ToLowerInvariant();
        var total = Stopwatch.StartNew();
        var warmed = 0;
        var skippedOnLock = 0;
        var failed = 0;
        int? slowestServer = null;
        var slowest = TimeSpan.Zero;
        var cancelled = false;

        List<int> serverIds;
        try
        {
            serverIds = await ReadServerIdsAsync(postgres, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            logger?.LogInformation("Interval floor warm-up for {Table} cancelled before it read the server list", tableName);
            return new IntervalFloorWarmUpResult(table, 0, 0, 0, total.Elapsed, null, TimeSpan.Zero, true);
        }
        catch (Exception ex)
        {
            logger?.LogWarning("Interval floor warm-up for {Table} could not read the server list: {Failure}", tableName, ex.Message);
            return new IntervalFloorWarmUpResult(table, 0, 0, 1, total.Elapsed, null, TimeSpan.Zero, false);
        }

        foreach (var serverId in serverIds)
        {
            var one = Stopwatch.StartNew();
            try
            {
                /* Its own connection per server, so a read that timed out (which breaks the connection) cannot
                   poison the next server's. lock_timeout is a session setting: it never touches a pooled
                   connection's next user, because the connection is reset on return to the pool (Npgsql sends
                   DISCARD ALL), and this one is also set explicitly before every read. */
                await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
                await using (var setLock = new NpgsqlCommand(
                    "SET lock_timeout = " + lockTimeoutMilliseconds.ToString(CultureInfo.InvariantCulture), connection))
                {
                    await setLock.ExecuteNonQueryAsync(cancellationToken);
                }

                await using var read = new NpgsqlCommand(sql, connection) { CommandTimeout = commandTimeoutSeconds };
                read.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
                await read.ExecuteScalarAsync(cancellationToken);
                warmed++;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                cancelled = true;
                logger?.LogInformation(
                    "Interval floor warm-up for {Table} cancelled at server {ServerId}; {Warmed} server(s) done",
                    tableName, serverId, warmed);
                break;
            }
            catch (PostgresException ex) when (ex.SqlState == LockNotAvailable)
            {
                skippedOnLock++;
                logger?.LogWarning(
                    "Interval floor warm-up for {Table} skipped server {ServerId}: the table is locked ({LockMs} ms lock timeout), probably a schema or partition step",
                    tableName, serverId, lockTimeoutMilliseconds);
            }
            catch (Exception ex)
            {
                failed++;
                logger?.LogWarning(
                    "Interval floor warm-up for {Table} failed for server {ServerId}: {Failure}",
                    tableName, serverId, ex.InnerException?.Message ?? ex.Message);
            }

            if (one.Elapsed > slowest)
            {
                slowest = one.Elapsed;
                slowestServer = serverId;
            }
        }

        var result = new IntervalFloorWarmUpResult(
            table, warmed, skippedOnLock, failed, total.Elapsed, slowestServer, slowest, cancelled);
        logger?.LogInformation(
            "Interval floor warm-up for {Table}: {Warmed} server(s) warmed, {Skipped} skipped on a lock, {Failed} failed, {ElapsedMs} ms total, slowest server {SlowestServer} at {SlowestMs} ms",
            tableName, warmed, skippedOnLock, failed, (long)total.Elapsed.TotalMilliseconds,
            slowestServer?.ToString(CultureInfo.InvariantCulture) ?? "none", (long)slowest.TotalMilliseconds);
        return result;
    }

    private static async Task<List<int>> ReadServerIdsAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        var ids = new List<int>();
        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(ServerIdsSql, connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            ids.Add(reader.GetInt32(0));
        }

        return ids;
    }
}
