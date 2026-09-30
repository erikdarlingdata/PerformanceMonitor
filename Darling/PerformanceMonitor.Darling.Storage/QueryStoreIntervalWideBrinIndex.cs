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

/// <summary>
/// A BRIN index on <c>collect.query_store_interval_wide (collection_time)</c> (#4605), built in the
/// background after the service is up rather than by a migration rung.
///
/// <para><b>Why the table needs it.</b> Nothing serves <c>collection_time</c> on this table: its only
/// secondary index leads with <c>first_execution_time</c>. A Custom Views Query Store panel over 12 hours or
/// more reads the table on <c>collection_time</c> and Parallel-Seq-Scans all of it. On a large production
/// monitoring store (28 GB, 3.1 of 9 days held) a 24-hour fleet panel read 3.18M blocks, 98.8 s cold. With
/// the index, on the same store at <c>random_page_cost</c> 1.1, a warm Parallel Bitmap Heap Scan read 24 h in
/// 4.3 s (1.14M blocks), 12 h in 1.6 s, 6 h in 0.8 s and 48 h in 9.2 s. Those figures are from a store still
/// inside its first retention cycle. Once the 9-day purge runs, the freed early pages are reused by new
/// rows, so BRIN ranges widen: windows ending now should stay selective, while historical windows lose
/// selectivity and the plan may flip back to a Seq Scan for part of the cycle. Results stay exact and
/// nothing is slower than without the index. If that matters, the remedy is a periodic
/// <c>REINDEX INDEX CONCURRENTLY</c> or re-summarizing the ranges, which this class does not do.</para>
///
/// <para><b>Why BRIN and not a btree.</b> The writer's upsert (<c>QueryStoreIntervalWide.cs</c>,
/// <c>ON CONFLICT ... DO UPDATE SET collection_time = EXCLUDED.collection_time, ...</c>) rewrites
/// <c>collection_time</c> on every update, and 99.9% of this table's updates are HOT (heap-only) today. Any
/// btree on <c>collection_time</c>, or a covering <c>INCLUDE</c> of the measures, would make every one of them
/// non-HOT: the regression class #4250 fixed. From PostgreSQL 16 an update stays HOT when the only indexed
/// columns it changes are summarizing (BRIN) ones. Measured on that store in one rolled-back transaction:
/// updating <c>collection_time</c> and <c>execution_count</c> on 20 rows was 20/20 HOT; the control, updating
/// <c>first_execution_time</c> (btree-indexed) on 20 other rows, was 0/20. The column's physical correlation
/// was 0.9998, which is what makes a BRIN selective: rows land in time order and an update rewrites the
/// value to a newer time within the same neighbourhood.</para>
///
/// <para><b>Below PostgreSQL 16 the index is not built.</b> Without summarizing-index HOT support the BRIN
/// would make every upsert non-HOT, so <see cref="Decide"/> skips the build and the ensure logs why. A
/// bring-your-own store on PostgreSQL 14 or 15 keeps today's plans and today's HOT rate.</para>
///
/// <para><b>Why not a migration rung.</b> Migrations run in one transaction and block startup, and
/// <c>CREATE INDEX CONCURRENTLY</c> cannot run inside a transaction (25001). A plain in-rung build would hold
/// a lock that blocks the writer for the whole heap read: 108 s at 25 GB, minutes at a full 9 days. A
/// background <c>CONCURRENTLY</c> build blocked no writes when measured. An applied rung also never
/// changes, so the ensure is idempotent and runs on every start instead: a fresh store builds an empty
/// table's index instantly and no table-creation change is needed.</para>
///
/// <para><b>Hypertables are refused.</b> The table is a plain heap. TimescaleDB refuses <c>CONCURRENTLY</c>
/// on a hypertable, so if the table is ever converted the ensure skips with a warning instead of failing
/// or retrying.</para>
///
/// <para><b>Autosummarize.</b> <c>autosummarize = on</c> lets autovacuum work items summarize each new block
/// range as the table grows. A range that is not summarized yet is always read by the scan, so results stay
/// correct while the summary catches up; the index is 856 kB on that store.</para>
///
/// <para><b>The start delay.</b> <see cref="RunDelayedAsync"/> waits <see cref="StartDelay"/> (20 minutes)
/// first. After an install or restart a big store's volume sits at its IOPS cap for about 15 minutes (cold
/// cache, migrations, the retention purge, the continuous-aggregate refresh), and a full-heap read on top
/// slows all of it. One attempt is made per service start; a failure or an interrupted build is retried at
/// the next start. An interrupted <c>CONCURRENTLY</c> build leaves an INVALID index behind, which
/// <c>IF NOT EXISTS</c> would silently keep, so the ensure reads validity first and drops an INVALID
/// leftover before building.</para>
///
/// <para><b>Reads take the index only at a low <c>random_page_cost</c>.</b> At the default of 4 the planner
/// still chooses the sequential scan for a 12-hour window even with this index present; at about 1.1 or
/// lower it chooses a Bitmap Heap Scan over the BRIN. The managed store sets 1.1; a bring-your-own store
/// on SSD needs its operator to.</para>
/// </summary>
public static class QueryStoreIntervalWideBrinIndex
{
    /// <summary>The index's schema-qualified name.</summary>
    public const string IndexName = "collect.ix_query_store_interval_wide_collection_time_brin";

    /// <summary>How long after the service is up the background step waits before its one attempt.</summary>
    public static readonly TimeSpan StartDelay = TimeSpan.FromMinutes(20);

    /// <summary>
    /// Command deadline for the build and the drop, in seconds. The build reads the whole heap: about 108 s at
    /// 25 GB, several minutes at a full 9 days' retention. Two hours leaves headroom on a slow volume while
    /// still bounding a build stuck behind a long transaction; shutdown cancels it sooner.
    /// </summary>
    public const int BuildTimeoutSeconds = 7200;

    /// <summary>Deadline for the catalog reads, in seconds.</summary>
    public const int CatalogReadTimeoutSeconds = 30;

    /// <summary>First PostgreSQL version whose HOT logic tolerates changes to summarizing-index columns.</summary>
    public const int MinimumServerVersionNum = 160000;

    internal const string CreateSql =
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_query_store_interval_wide_collection_time_brin "
        + "ON collect.query_store_interval_wide USING brin (collection_time) WITH (autosummarize = on);";

    internal const string DropSql =
        "DROP INDEX CONCURRENTLY IF EXISTS collect.ix_query_store_interval_wide_collection_time_brin;";

    internal const string StateSql = @"
SELECT
    current_setting('server_version_num')::int AS server_version_num,
    to_regclass('collect.query_store_interval_wide') IS NOT NULL AS table_exists,
    to_regclass('timescaledb_information.hypertables') IS NOT NULL AS has_hypertable_view,
    (SELECT i.indisvalid
     FROM pg_index AS i
     WHERE i.indexrelid = to_regclass('collect.ix_query_store_interval_wide_collection_time_brin')) AS index_valid;";

    /* Reached only when StateSql reported the view exists, so a store without TimescaleDB never parses it. */
    internal const string HypertableSql = @"
SELECT EXISTS
(
    SELECT 1
    FROM timescaledb_information.hypertables AS h
    WHERE h.hypertable_schema = 'collect'
    AND   h.hypertable_name = 'query_store_interval_wide'
);";

    /// <summary>What the ensure does about the index.</summary>
    public enum BrinAction
    {
        /// <summary>Build (or keep) the index.</summary>
        Build,

        /// <summary>Skip: the server predates summarizing-index HOT support.</summary>
        SkipServerVersionBelowSixteen,

        /// <summary>Skip: the table is a hypertable, which refuses <c>CREATE INDEX CONCURRENTLY</c>.</summary>
        SkipHypertable,
    }

    /// <summary>The decision and, for a skip, the reason to log.</summary>
    public readonly record struct BrinDecision(BrinAction Action, string Reason);

    /// <summary>
    /// The pure build-or-skip decision. The version check comes first: below 160000 a BRIN on
    /// <c>collection_time</c> makes every upsert non-HOT, whatever the table is.
    /// </summary>
    public static BrinDecision Decide(int serverVersionNum, bool tableIsHypertable)
    {
        if (serverVersionNum < MinimumServerVersionNum)
        {
            return new BrinDecision(
                BrinAction.SkipServerVersionBelowSixteen,
                $"server_version_num {serverVersionNum} is below {MinimumServerVersionNum}: a BRIN index on "
                + "collection_time would make the upsert non-HOT below PG 16");
        }

        if (tableIsHypertable)
        {
            return new BrinDecision(
                BrinAction.SkipHypertable,
                "collect.query_store_interval_wide is a hypertable and TimescaleDB refuses "
                + "CREATE INDEX CONCURRENTLY on a hypertable");
        }

        return new BrinDecision(BrinAction.Build, string.Empty);
    }

    /// <summary>
    /// The background entry point: waits <paramref name="delay"/>, makes one <see cref="EnsureAsync"/> attempt
    /// on its own connection, and never throws. Cancellation (shutdown) ends it quietly; any other failure is
    /// a Warning and the next service start retries.
    /// </summary>
    public static async Task RunDelayedAsync(
        NpgsqlDataSource postgres, ILogger logger, TimeSpan delay, CancellationToken cancellationToken)
    {
        var delayFinished = false;
        try
        {
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            delayFinished = true;
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await EnsureAsync(connection, logger, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!delayFinished)
        {
            logger.LogDebug(
                "Query Store interval index ensure ({Index}) was cancelled before it started.",
                IndexName);
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation(
                "Query Store interval index ensure ({Index}) was cancelled at shutdown; the next start retries, "
                + "and drops any half-built leftover first.",
                IndexName);
        }
        catch (Exception ex)
        {
            var sqlState = ex is NpgsqlException { SqlState: { Length: > 0 } state } ? $", SQLSTATE {state}" : string.Empty;
            logger.LogWarning(
                "Query Store interval index ensure ({Index}) failed and is retried at the next start: {ExceptionType}{SqlState}: {Message}",
                IndexName, ex.GetType().Name, sqlState, ex.Message);
        }
    }

    /// <summary>
    /// Makes sure the BRIN index exists and is valid, on <paramref name="connection"/>, which must be open and
    /// outside any transaction (<c>CONCURRENTLY</c> fails with 25001 inside one).
    /// </summary>
    public static async Task EnsureAsync(NpgsqlConnection connection, ILogger logger, CancellationToken cancellationToken)
    {
        int serverVersionNum;
        bool tableExists;
        bool hasHypertableView;
        bool? indexValid;

        await using (var state = new NpgsqlCommand(StateSql, connection) { CommandTimeout = CatalogReadTimeoutSeconds })
        await using (var reader = await state.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            serverVersionNum = reader.GetInt32(0);
            tableExists = reader.GetBoolean(1);
            hasHypertableView = reader.GetBoolean(2);
            indexValid = reader.IsDBNull(3) ? null : reader.GetBoolean(3);
        }

        if (!tableExists)
        {
            logger.LogDebug("Query Store interval index ensure skipped: collect.query_store_interval_wide does not exist yet.");
            return;
        }

        var isHypertable = false;
        if (hasHypertableView)
        {
            await using var hypertable = new NpgsqlCommand(HypertableSql, connection) { CommandTimeout = CatalogReadTimeoutSeconds };
            isHypertable = (bool)(await hypertable.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
        }

        var decision = Decide(serverVersionNum, isHypertable);
        if (decision.Action == BrinAction.SkipServerVersionBelowSixteen)
        {
            logger.LogInformation("Query Store interval index {Index} not built: {Reason}.", IndexName, decision.Reason);
            return;
        }

        if (decision.Action == BrinAction.SkipHypertable)
        {
            logger.LogWarning("Query Store interval index {Index} not built: {Reason}.", IndexName, decision.Reason);
            return;
        }

        if (indexValid == true)
        {
            logger.LogDebug("Query Store interval index {Index} already exists and is valid.", IndexName);
            return;
        }

        if (indexValid == false)
        {
            logger.LogWarning(
                "Query Store interval index {Index} exists but is INVALID (an interrupted CONCURRENTLY build); dropping it and rebuilding.",
                IndexName);
            await using var drop = new NpgsqlCommand(DropSql, connection) { CommandTimeout = BuildTimeoutSeconds };
            await drop.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        var started = System.Diagnostics.Stopwatch.StartNew();
        await using (var build = new NpgsqlCommand(CreateSql, connection) { CommandTimeout = BuildTimeoutSeconds })
        {
            await build.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        logger.LogInformation(
            "Query Store interval index {Index} built in {Seconds:F1}s (valid).", IndexName, started.Elapsed.TotalSeconds);
    }
}
