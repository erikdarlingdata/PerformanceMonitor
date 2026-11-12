/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The covering index on the CLOSED day leaves of <c>collect.query_store_interval_wide</c> (#8549), built by the hourly
/// convergence pass (<see cref="QueryStoreIntervalPartitions.RunMaintenancePassAsync(NpgsqlConnection, DateTime, ILogger, CancellationToken)"/>).
///
/// <para><b>Why.</b> A 30-day Query Store read (the Viewer's grid ranking step and its slicer) reads one server's rows of every
/// day leaf, and every server's rows share the leaf's pages. With only the plain <c>(server_id, first_execution_time)</c> index the
/// plan is a bitmap heap scan that reads nearly every heap block of every day: about 37,000 blocks a day on a field store,
/// 373,514 blocks and 15.3 s cold for the read. An index on the same two keys that also INCLUDEs the columns those two statements
/// read (<see cref="IncludeColumns"/>) lets the planner run an index-only scan: 37 times fewer blocks on a seeded store with the
/// visibility map set. No read names the index, so a leaf without it reads as it did before.</para>
///
/// <para><b>Why only closed leaves.</b> Three of the included columns (<c>execution_count</c>, <c>avg_duration_us</c>,
/// <c>collection_time</c>) are set by the collector's upsert (<see cref="QueryStoreIntervalWide.UpsertSql"/>). An index that holds
/// a column an UPDATE sets turns that update from heap-only (HOT) into a normal one, which writes a new entry into every index and
/// makes autovacuum clean them up (#4250, #4605): measured 0 of 1,500 updates HOT with the covering index and all 1,500 without it.
/// So the index may only sit where the upsert can no longer update a row.</para>
///
/// <para><b>Which leaves the upsert can reach.</b> It updates a row only through <c>ON CONFLICT</c> on the identity, which holds
/// <c>first_execution_time</c> and is never assigned, so a row never leaves its day leaf. The conflict updates a stored interval row
/// when a later collection of that same Query Store interval arrives (<c>(collection_time, execution_count)</c> newer), and the
/// collector re-reads an interval only while its <c>end_time</c> is past its cutoff. That bounds the last collection that can touch
/// a row at <c>first_execution_time + </c><see cref="QueryStoreIntervalWide.PurgeEdgeMargin"/>: one interval length (at most a
/// day, <see cref="QueryStoreIntervalWide.IntervalSpanMargin"/>, because <c>first_execution_time</c> lies inside the interval), the
/// slowest collection cadence (<see cref="QueryStoreIntervalWide.MaxBelowFloorCadence"/>) and the catch-up cap
/// (<see cref="WatermarkPolicy.MaxCatchup"/>); the same argument that gives the wide reads their 26-hour floor.
/// <c>collection_time</c> is stamped by the service's clock and <c>first_execution_time</c> is not compared with it across clocks
/// (<see cref="QueryStoreIntervalWide.PurgeEdgeMargin"/>'s own remarks), so skew adds nothing. Every row of a leaf has
/// <c>first_execution_time</c> below the leaf's upper bound, so a leaf is closed once the clock is
/// <see cref="UpsertReach"/> past its upper bound (<see cref="IsClosed"/>). Today's leaf and yesterday's stay plain
/// by that rule: a leaf closes between 26 and 50 hours after its day began, so a store keeps three or four open leaves, depending
/// on the hour. A catch-up after an outage, and a backfill, insert old rows: an INSERT writes a new entry into every index either
/// way, so it is not a HOT question.</para>
///
/// <para><b>Built on the hourly pass, like the BRIN steps.</b> <see cref="EnsureAsync"/> reads the catalog first (one read: each
/// partition's bound and whether its covering index exists and is valid), so a converged store pays that one read and nothing else.
/// Otherwise it builds <c>CREATE INDEX CONCURRENTLY IF NOT EXISTS</c> on ONE leaf at a time, newest closed leaf first (the oldest is
/// the next one retention drops), at most <see cref="MaxLeavesPerPass"/> a pass, under a <c>lock_timeout</c> of
/// <see cref="BuildLockTimeoutSeconds"/>. Never on the parent: a partitioned parent index is cloned onto every leaf, hot ones
/// included. Never on DEFAULT or the legacy table (<see cref="IsDayLeaf"/>). An INVALID index left by a build that failed or was
/// cancelled is dropped, and the leaf is built again on a later pass. A leaf dropped at retention takes its index with it. The plain
/// <c>(server_id, first_execution_time)</c> index stays: other reads and the second ranking pass still use it.</para>
/// </summary>
public static class QueryStoreIntervalWideCoverIndex
{
    /// <summary>
    /// How far past a leaf's upper bound the clock must be before the collector's upsert can no longer update a row in it (#8549):
    /// <see cref="QueryStoreIntervalWide.PurgeEdgeMargin"/>, 26 hours. See the class remarks for the code this rests on.
    /// </summary>
    public static readonly TimeSpan UpsertReach = QueryStoreIntervalWide.PurgeEdgeMargin;

    /// <summary>The most leaves one pass builds a covering index on (or drops an INVALID one from).</summary>
    public const int MaxLeavesPerPass = 3;

    /// <summary>The lock wait of the build, in seconds. A concurrent build waits for every older transaction in the database twice.</summary>
    public const int BuildLockTimeoutSeconds = 30;

    /// <summary>The command deadline of one leaf's build, in seconds; the pass's own budget cancels it sooner.</summary>
    public const int BuildTimeoutSeconds = 600;

    /// <summary>Deadline for the catalog read and the drop of an INVALID leftover, in seconds.</summary>
    public const int CatalogTimeoutSeconds = 60;

    /// <summary>The covering index's key columns.</summary>
    public const string KeyColumns = "server_id, first_execution_time";

    /// <summary>
    /// The columns the two 30-day reads need from the index: the Viewer's grid ranking step (<c>keys</c>: the group key and the
    /// duration rank, plus the interval-start and collection-time window predicates) and its slicer (<c>query_id</c>, the bucket's
    /// <c>COALESCE(interval_start_time_utc, collection_time)</c>, <c>database_name</c>, <c>execution_count</c> and the five
    /// averages), as of #8549. The MCP top read ranks and aggregates in one pass over many more columns, so it is not served.
    /// </summary>
    public const string IncludeColumns =
        "database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role, collection_time, interval_start_time_utc, "
        + "execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads";

    private const string IndexPrefix = "ix_";

    private const string IndexSuffix = "_cover";

    /* Each partition directly under $1 with its bound text, and the state of its covering index (named $2 + leaf name + $3):
       NULL when it does not exist, else indisvalid. */
    internal const string StateSql = @"
SELECT format('%I.%I', n.nspname, c.relname),
       pg_get_expr(c.relpartbound, c.oid),
       (SELECT x.indisvalid FROM pg_index AS x WHERE x.indexrelid = to_regclass(format('%I.%I', n.nspname, $2 || c.relname || $3)))
FROM pg_inherits AS ih
JOIN pg_class AS c ON c.oid = ih.inhrelid
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE ih.inhparent = to_regclass($1)
ORDER BY c.relname DESC;";

    /// <summary>The covering index's name for a leaf (<c>collect.query_store_interval_wide_p20261101</c>), without its schema.</summary>
    public static string ShortIndexName(string leaf) => IndexPrefix + leaf[(leaf.IndexOf('.', StringComparison.Ordinal) + 1)..] + IndexSuffix;

    /// <summary>The covering index's schema-qualified name for a schema-qualified leaf.</summary>
    public static string IndexName(string leaf) => leaf[..(leaf.IndexOf('.', StringComparison.Ordinal) + 1)] + ShortIndexName(leaf);

    /// <summary>The build for one leaf. Idempotent; never run inside a transaction.</summary>
    public static string CreateSql(string leaf) =>
        $"CREATE INDEX CONCURRENTLY IF NOT EXISTS {ShortIndexName(leaf)} ON {leaf} ({KeyColumns}) INCLUDE ({IncludeColumns});";

    /// <summary>
    /// True for a day leaf: a bounded range partition (not DEFAULT, not the legacy table, whose lower bound is MINVALUE) one day wide.
    /// </summary>
    public static bool IsDayLeaf(QueryStoreIntervalPartitions.PartitionInfo partition) =>
        !partition.IsDefault
        && !partition.UpperUnbounded
        && partition.Lower is DateTime lower
        && partition.Upper is DateTime upper
        && upper - lower == TimeSpan.FromDays(1);

    /// <summary>
    /// True when the collector's upsert can no longer update a row of <paramref name="partition"/> at <paramref name="utcNow"/>:
    /// it is a day leaf and <paramref name="utcNow"/> is <see cref="UpsertReach"/> or more past its upper bound.
    /// </summary>
    public static bool IsClosed(QueryStoreIntervalPartitions.PartitionInfo partition, DateTime utcNow) =>
        IsDayLeaf(partition)
        && partition.Upper!.Value + UpsertReach <= DateTime.SpecifyKind(utcNow, DateTimeKind.Unspecified);

    /// <summary>The leaf's state read from the catalog: its bound and its covering index (null: none, else whether it is valid).</summary>
    internal readonly record struct LeafState(QueryStoreIntervalPartitions.PartitionInfo Partition, bool? CoverValid);

    internal static async Task<IReadOnlyList<LeafState>> ReadLeavesAsync(
        NpgsqlConnection connection, QueryStoreIntervalPartitions.IntervalTable table, CancellationToken cancellationToken)
    {
        var leaves = new List<LeafState>();
        await using var command = new NpgsqlCommand(StateSql, connection) { CommandTimeout = CatalogTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = table.Parent });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = IndexPrefix });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = IndexSuffix });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (!reader.IsDBNull(1)
                && QueryStoreIntervalPartitions.TryParsePartitionBound(
                    reader.GetString(1), out var isDefault, out var lower, out var upper, out var upperUnbounded))
            {
                var partition = new QueryStoreIntervalPartitions.PartitionInfo(reader.GetString(0), isDefault, lower, upper, upperUnbounded);
                leaves.Add(new LeafState(partition, reader.IsDBNull(2) ? null : reader.GetBoolean(2)));
            }
        }

        return leaves;
    }

    /// <summary>
    /// The convergence step (#8549): for the wide table, a covering index on up to <see cref="MaxLeavesPerPass"/> closed day leaves
    /// that lack one, newest first. Count is the leaves built. Any other table, and a store whose leaves all have theirs (or none is
    /// closed), is <see cref="QueryStoreIntervalPartitions.StepOutcome.NothingToDo"/> after the one catalog read. A lock timeout or
    /// a deadlock ends the pass with <see cref="QueryStoreIntervalPartitions.StepOutcome.RetryLater"/> (not an error); the next pass
    /// drops the INVALID leftover and builds again. A leaf that retention dropped between the read and its build is skipped.
    /// </summary>
    public static async Task<QueryStoreIntervalPartitions.StepResult> EnsureAsync(
        NpgsqlConnection connection, QueryStoreIntervalPartitions.IntervalTable table, DateTime utcNow, ILogger logger,
        CancellationToken cancellationToken)
    {
        if (!string.Equals(table.Name, QueryStoreIntervalPartitions.Wide.Name, StringComparison.Ordinal))
        {
            return new QueryStoreIntervalPartitions.StepResult(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, "only the wide table has a covering index");
        }

        var work = (await ReadLeavesAsync(connection, table, cancellationToken).ConfigureAwait(false))
            .Where(leaf => IsClosed(leaf.Partition, utcNow) && leaf.CoverValid != true)
            .Take(MaxLeavesPerPass)
            .ToList();
        if (work.Count == 0)
        {
            return new QueryStoreIntervalPartitions.StepResult(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, "every closed day leaf has its covering index");
        }

        var built = 0;
        var started = System.Diagnostics.Stopwatch.StartNew();
        foreach (var (partition, coverValid) in work)
        {
            var leaf = partition.Name;
            var index = IndexName(leaf);
            try
            {
                if (coverValid == false)
                {
                    logger.LogWarning(
                        "Query Store interval table {Table}: covering index {Index} is INVALID (an interrupted build); dropping it, the next pass builds it again.",
                        table.Parent, index);
                    await SetLockTimeoutAsync(connection, cancellationToken).ConfigureAwait(false);
                    await using var drop = new NpgsqlCommand($"DROP INDEX CONCURRENTLY IF EXISTS {index};", connection) { CommandTimeout = CatalogTimeoutSeconds };
                    await drop.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    continue;
                }

                await SetLockTimeoutAsync(connection, cancellationToken).ConfigureAwait(false);
                var leafStarted = System.Diagnostics.Stopwatch.StartNew();
                await using var build = new NpgsqlCommand(CreateSql(leaf), connection) { CommandTimeout = BuildTimeoutSeconds };
                await build.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                built++;
                logger.LogInformation(
                    "Query Store interval table {Table}: built covering index {Index} on closed day leaf {Leaf} in {Seconds:F1}s.",
                    table.Parent, index, leaf, leafStarted.Elapsed.TotalSeconds);
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
            {
                logger.LogInformation(
                    "Query Store interval table {Table}: leaf {Leaf} was dropped by retention before its covering index was built; skipping it.",
                    table.Parent, leaf);
            }
            catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.LockNotAvailable or PostgresErrorCodes.DeadlockDetected)
            {
                logger.LogInformation(
                    "Query Store interval table {Table}: building the covering index on {Leaf} hit a {Reason}; the next pass retries.",
                    table.Parent, leaf, ex.SqlState == PostgresErrorCodes.LockNotAvailable ? "lock timeout" : "deadlock");
                return new QueryStoreIntervalPartitions.StepResult(QueryStoreIntervalPartitions.StepOutcome.RetryLater, "lock contention", built);
            }
            finally
            {
                await ResetLockTimeoutAsync(connection, logger).ConfigureAwait(false);
            }
        }

        return built > 0
            ? new QueryStoreIntervalPartitions.StepResult(
                QueryStoreIntervalPartitions.StepOutcome.Done,
                string.Create(CultureInfo.InvariantCulture, $"built {built} covering index(es) in {started.Elapsed.TotalSeconds:F1}s"),
                built)
            : new QueryStoreIntervalPartitions.StepResult(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, "dropped an INVALID covering index; the next pass builds it");
    }

    /* A concurrent build cannot run inside a transaction, so SET LOCAL is not available: the session setting is reset in a finally. */
    private static async Task SetLockTimeoutAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand($"SET lock_timeout = '{BuildLockTimeoutSeconds}s';", connection) { CommandTimeout = CatalogTimeoutSeconds };
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task ResetLockTimeoutAsync(NpgsqlConnection connection, ILogger logger)
    {
        if (connection.State != System.Data.ConnectionState.Open)
        {
            return;
        }

        try
        {
            await using var command = new NpgsqlCommand("RESET lock_timeout;", connection) { CommandTimeout = CatalogTimeoutSeconds };
            await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is NpgsqlException or InvalidOperationException)
        {
            logger.LogDebug("Resetting lock_timeout after a covering index build failed: {Message}", ex.Message);
        }
    }
}
