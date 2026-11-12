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
/// <para><b>And vacuumed.</b> An index-only scan goes to the heap for every page the visibility map does not mark all-visible, and a
/// field leaf is never vacuumed by hand: autovacuum's insert trigger waits for about 20 percent new rows since its last run, so a
/// leaf closes with up to its last fifth of pages not all-visible, and nothing writes to it again to trigger another run. So the
/// step also runs <c>VACUUM (SKIP_LOCKED)</c> on a closed leaf that has rows inserted since its last vacuum
/// (<see cref="VacuumAsync"/>), right after the build or on a later pass when that failed or was skipped.</para>
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
       NULL when it does not exist, else indisvalid. The last column is the rows inserted since the leaf was last vacuumed, by hand or by
   autovacuum (#8549: a closed leaf with some is one whose newest pages are not all-visible, see VacuumAsync). */
    internal const string StateSql = @"
SELECT format('%I.%I', n.nspname, c.relname),
       pg_get_expr(c.relpartbound, c.oid),
       (SELECT x.indisvalid FROM pg_index AS x WHERE x.indexrelid = to_regclass(format('%I.%I', n.nspname, $2 || c.relname || $3))),
       pg_stat_get_ins_since_vacuum(c.oid)
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
    internal readonly record struct LeafState(QueryStoreIntervalPartitions.PartitionInfo Partition, bool? CoverValid, long InsertsSinceVacuum);

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
                leaves.Add(new LeafState(partition, reader.IsDBNull(2) ? null : reader.GetBoolean(2), reader.IsDBNull(3) ? 0 : reader.GetInt64(3)));
            }
        }

        return leaves;
    }

    /// <summary>
    /// The convergence step (#8549): for the wide table, a covering index on up to <see cref="MaxLeavesPerPass"/> closed day leaves
    /// that lack one, newest first, each then vacuumed when it has rows inserted since its last vacuum (a leaf that has its index and
/// still needs that is vacuumed alone). Count is the leaves worked. Any other table, and a store whose leaves all have theirs (or none is
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
            .Where(leaf => IsClosed(leaf.Partition, utcNow) && (leaf.CoverValid != true || leaf.InsertsSinceVacuum > 0))
            .Take(MaxLeavesPerPass)
            .ToList();
        if (work.Count == 0)
        {
            return new QueryStoreIntervalPartitions.StepResult(
                QueryStoreIntervalPartitions.StepOutcome.NothingToDo, "every closed day leaf has its covering index and is vacuumed");
        }

        var built = 0;
        var vacuumed = 0;
        var started = System.Diagnostics.Stopwatch.StartNew();
        foreach (var (partition, coverValid, insertsSinceVacuum) in work)
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
                if (coverValid != true)
                {
                    var leafStarted = System.Diagnostics.Stopwatch.StartNew();
                    await using var build = new NpgsqlCommand(CreateSql(leaf), connection) { CommandTimeout = BuildTimeoutSeconds };
                    await build.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    built++;
                    logger.LogInformation(
                        "Query Store interval table {Table}: built covering index {Index} on closed day leaf {Leaf} in {Seconds:F1}s.",
                        table.Parent, index, leaf, leafStarted.Elapsed.TotalSeconds);
                }

                if (insertsSinceVacuum > 0 && await VacuumAsync(connection, table, leaf, logger, cancellationToken).ConfigureAwait(false))
                {
                    vacuumed++;
                }
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

        return built > 0 || vacuumed > 0
            ? new QueryStoreIntervalPartitions.StepResult(
                QueryStoreIntervalPartitions.StepOutcome.Done,
                string.Create(CultureInfo.InvariantCulture, $"built {built} covering index(es) and vacuumed {vacuumed} leaf(s) in {started.Elapsed.TotalSeconds:F1}s"),
                Math.Max(built, vacuumed))
            : new QueryStoreIntervalPartitions.StepResult(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, "dropped an INVALID covering index or could not vacuum a leaf; the next pass tries again");
    }

    /// <summary>
    /// <c>VACUUM (SKIP_LOCKED)</c> of one closed leaf (#8549), under the caller's <c>lock_timeout</c>; true when it ran to the end.
    /// An index-only scan reads the heap for every page the visibility map does not mark all-visible. A leaf is written all day, and
    /// autovacuum's insert trigger fires only after about 20 percent new rows since its last run, so a leaf closes with up to the last
    /// fifth of its pages not all-visible, and nothing writes to it again to trigger another run before retention drops it. Measured on
    /// a seeded store with autovacuum on at its defaults: 15 percent of a closed leaf's pages not all-visible, 1,875 heap fetches
    /// and 1,455 blocks for one server's read of the leaf, against 0 and 301 once vacuumed (about 0.1 s per leaf). SKIP_LOCKED leaves a
    /// leaf an autovacuum is working on alone instead of cancelling it (that run does the same job), and the next pass looks again.
    /// A failure is logged and left for autovacuum: a vacuum never fails the pass, and the index is already built.
    /// </summary>
    internal static async Task<bool> VacuumAsync(
        NpgsqlConnection connection, QueryStoreIntervalPartitions.IntervalTable table, string leaf, ILogger logger, CancellationToken cancellationToken)
    {
        try
        {
            var started = System.Diagnostics.Stopwatch.StartNew();
            await using var vacuum = new NpgsqlCommand($"VACUUM (SKIP_LOCKED) {leaf};", connection) { CommandTimeout = BuildTimeoutSeconds };
            await vacuum.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            logger.LogDebug(
                "Query Store interval table {Table}: vacuumed closed day leaf {Leaf} in {Seconds:F2}s so its covering index scans without the heap.",
                table.Parent, leaf, started.Elapsed.TotalSeconds);
            return true;
        }
        catch (NpgsqlException ex)
        {
            logger.LogInformation(
                "Query Store interval table {Table}: vacuuming closed day leaf {Leaf} failed ({Message}); autovacuum or the next pass does it.",
                table.Parent, leaf, ex.Message);
            return false;
        }
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
