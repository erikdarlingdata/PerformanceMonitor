/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The Query Store read indexes that are built in the background after the service is up rather than by a
/// migration rung (#4605's BRIN, #4952's btree), and the one piece of machinery they share: the state read,
/// the build-or-skip decision, the invalid-leftover drop, the build and the delayed start.
///
/// <para><b>Why not a migration rung.</b> Migrations run in one transaction and block startup, and
/// <c>CREATE INDEX CONCURRENTLY</c> cannot run inside a transaction (25001). A plain in-rung build would hold a lock
/// that blocks the writer for the whole heap read. An applied rung also never changes, so each ensure is idempotent
/// and runs on every start: a fresh store builds an empty table's index instantly and no table-creation change is
/// needed.</para>
///
/// <para><b>One launch, one build at a time.</b> <see cref="RunDelayedAsync"/> waits <see cref="StartDelay"/>
/// (20 minutes) once, then ensures each index in <see cref="All"/> in order, each on its own connection and each
/// failure-isolated: a store's volume sits at its IOPS cap for about 15 minutes after an install or restart (cold
/// cache, migrations, the retention purge, the continuous-aggregate refresh), and builds that each read a whole
/// table must not stack on top of it, or on each other. One attempt is made per service start; a failure or an
/// interrupted build is retried at the next start.</para>
///
/// <para><b>An interrupted build leaves an INVALID index behind</b> (a <c>CREATE INDEX CONCURRENTLY</c> that is
/// cancelled or fails mid-build). <c>IF NOT EXISTS</c> would silently keep it, so <see cref="EnsureAsync"/> reads
/// validity first and drops an INVALID leftover before building.</para>
///
/// <para><b>Plain tables only.</b> Every index here is built <c>CREATE INDEX CONCURRENTLY</c>
/// (<see cref="IndexSpec.PlainCreateSql"/>), which blocks no writes. TimescaleDB refuses <c>CONCURRENTLY</c> on a
/// hypertable ("hypertables do not support concurrent index creation"), so a spec whose table is a hypertable is
/// skipped with a warning instead of failing or retrying. The per-chunk alternative
/// (<c>WITH (timescaledb.transaction_per_chunk)</c>) is deliberately not offered: it takes a ShareLock on each chunk
/// it builds, for that chunk's scan, and the collector's COPY into a chunk waits behind it under a 10 s deadline.
/// The Query Store backfill writes backdated rows into older chunks as well as the newest, so no chunk of
/// <c>collect.query_store_stats</c>, a hypertable, is safe to lock while the service runs, and no spec
/// targets it. The partial index that serves the legacy-row check is built on the start path instead
/// (<c>PgTableTuning</c>), before collectors start.</para>
/// </summary>
public static class QueryStoreBackgroundIndexes
{
    /// <summary>How long after the service is up the background step waits before its one attempt.</summary>
    public static readonly TimeSpan StartDelay = TimeSpan.FromMinutes(20);

    /// <summary>
    /// Command deadline for a build and a drop, in seconds. A build reads a whole heap (about 108 s at 25 GB for the
    /// BRIN, several minutes at a full 9 days' retention). Two hours leaves headroom on a slow volume while still
    /// bounding a build stuck behind a long transaction; shutdown cancels it sooner.
    /// </summary>
    public const int BuildTimeoutSeconds = 7200;

    /// <summary>Deadline for the catalog reads, in seconds.</summary>
    public const int CatalogReadTimeoutSeconds = 30;

    /// <summary>Name of the btree that lets the per-server table read range-scan <c>first_execution_time</c> (#4952).</summary>
    public const string WideServerFirstExecIndexName = "collect.ix_query_store_interval_wide_server_first_exec";

    /// <summary>What the ensure does about one index.</summary>
    public enum IndexAction
    {
        /// <summary>Build (or keep) the index on a plain table.</summary>
        Build,

        /// <summary>Skip: the server predates the index's version floor.</summary>
        SkipServerVersion,

        /// <summary>Skip: the table is a hypertable, which refuses <c>CREATE INDEX CONCURRENTLY</c>.</summary>
        SkipHypertable,
    }

    /// <summary>The decision and, for a skip, the reason to log.</summary>
    public readonly record struct IndexDecision(IndexAction Action, string Reason);

    /// <summary>One background index: its names, the statements that build and drop it, and when it may not be built.</summary>
    /// <param name="IndexName">The index's schema-qualified name.</param>
    /// <param name="TableName">The table's schema-qualified name (<c>schema.name</c>); the hypertable probe splits it at the last dot.</param>
    /// <param name="PlainCreateSql">The build for a plain table; <c>CONCURRENTLY</c>, idempotent. A hypertable is skipped instead.</param>
    /// <param name="PlainDropSql">The drop of an INVALID leftover on a plain table.</param>
    /// <param name="MinimumServerVersionNum">The first <c>server_version_num</c> that may build it; 0 for any.</param>
    /// <param name="BelowMinimumReason">Why a server below the floor must not build it, for the log.</param>
    public sealed record IndexSpec(
        string IndexName,
        string TableName,
        string PlainCreateSql,
        string PlainDropSql,
        int MinimumServerVersionNum = 0,
        string BelowMinimumReason = "");

    /// <summary>
    /// A btree on <c>collect.query_store_interval_wide (server_id, first_execution_time)</c> (#4952), built
    /// <c>CONCURRENTLY</c> (the table is a plain heap).
    ///
    /// <para><b>Why.</b> The per-server table reads bound <c>first_execution_time</c> (#4861), but in the unique
    /// key it is the seventh column, behind <c>database_name</c>, <c>runtime_stats_interval_id</c>, <c>plan_id</c>,
    /// <c>query_id</c> and <c>replica_role</c>, so the bound cannot narrow the range: the scan reads every index entry
    /// the server has and tests the bound on each (4.3 s and 32,787 index blocks of a 7.7 s read at 24 h; at 168 h the
    /// planner switched to an index-order walk with random heap fetches, 28 s). The other index on the column
    /// (<c>idx_query_store_interval_wide_first_exec</c>) does not lead with <c>server_id</c>. This one does, so the
    /// bound becomes the range and the 168 h read can take a bitmap heap scan.</para>
    ///
    /// <para><b>HOT.</b> <c>first_execution_time</c> is part of the upsert's identity and <c>server_id</c> is its
    /// first key, so <c>ON CONFLICT ... DO UPDATE</c> rewrites neither and an update stays heap-only (the regression
    /// class #4250 fixed); the live tests pin both the catalog fact and a rolled-back update.</para>
    /// </summary>
    public static readonly IndexSpec WideServerFirstExec = new(
        WideServerFirstExecIndexName,
        "collect.query_store_interval_wide",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_query_store_interval_wide_server_first_exec "
        + "ON collect.query_store_interval_wide (server_id, first_execution_time);",
        "DROP INDEX CONCURRENTLY IF EXISTS collect.ix_query_store_interval_wide_server_first_exec;");

    /// <summary>Every background index, in the order the one delayed task ensures them.</summary>
    public static readonly IReadOnlyList<IndexSpec> All = new[]
    {
        QueryStoreIntervalWideBrinIndex.Spec,
        WideServerFirstExec,
    };

    /* One read of everything the decision needs. The names are bound, never interpolated. */
    internal const string StateSql = @"
SELECT
    current_setting('server_version_num')::int AS server_version_num,
    to_regclass($1) IS NOT NULL AS table_exists,
    to_regclass('timescaledb_information.hypertables') IS NOT NULL AS has_hypertable_view,
    (SELECT i.indisvalid
     FROM pg_index AS i
     WHERE i.indexrelid = to_regclass($2)) AS index_valid;";

    /* Reached only when StateSql reported the view exists, so a store without TimescaleDB never parses it. $1 is the
       table's schema and $2 its name (SplitTableName), each compared as it is rather than through a concatenation, so
       how a spec spells its TableName decides nothing beyond the schema and the name themselves. */
    internal const string HypertableSql = @"
SELECT EXISTS
(
    SELECT 1
    FROM timescaledb_information.hypertables AS h
    WHERE h.hypertable_schema = $1
    AND   h.hypertable_name = $2
);";

    /// <summary>
    /// A schema-qualified table name split at its last dot into the schema and the name, the two values the hypertable
    /// probe binds. A name without both parts is a defect in the spec, so it is rejected rather than guessed at.
    /// </summary>
    internal static (string Schema, string Name) SplitTableName(string tableName)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        var dot = tableName.LastIndexOf('.');
        if (dot <= 0 || dot == tableName.Length - 1)
        {
            throw new ArgumentException($"A table name must be schema-qualified (schema.name); got '{tableName}'.", nameof(tableName));
        }

        return (tableName[..dot], tableName[(dot + 1)..]);
    }

    /// <summary>
    /// The pure build-or-skip decision. The version check comes first: below the floor a build is wrong whatever the
    /// table is. A hypertable is then skipped, because TimescaleDB refuses <c>CONCURRENTLY</c> on one.
    /// </summary>
    public static IndexDecision Decide(IndexSpec spec, int serverVersionNum, bool tableIsHypertable)
    {
        if (serverVersionNum < spec.MinimumServerVersionNum)
        {
            return new IndexDecision(
                IndexAction.SkipServerVersion,
                $"server_version_num {serverVersionNum} is below {spec.MinimumServerVersionNum}: {spec.BelowMinimumReason}");
        }

        if (tableIsHypertable)
        {
            return new IndexDecision(
                IndexAction.SkipHypertable,
                $"{spec.TableName} is a hypertable and TimescaleDB refuses CREATE INDEX CONCURRENTLY on a hypertable");
        }

        return new IndexDecision(IndexAction.Build, string.Empty);
    }

    /// <summary>
    /// The background entry point: waits <paramref name="delay"/>, then makes one <see cref="EnsureAsync"/> attempt
    /// per spec, in order, each on its own connection, and never throws. A failure of one index is a Warning and the
    /// next index still runs; the next service start retries it. Cancellation (shutdown) ends it quietly, and the line
    /// it logs names the index in progress (before the delay is over, every index the run would have built); an error
    /// raised while shutting down is logged the same way, at Information.
    /// </summary>
    public static Task RunDelayedAsync(
        NpgsqlDataSource postgres,
        ILogger logger,
        TimeSpan delay,
        IReadOnlyList<IndexSpec> specs,
        CancellationToken cancellationToken) =>
        RunDelayedAsync(
            logger,
            delay,
            specs,
            async (spec, token) =>
            {
                await using var connection = await postgres.OpenConnectionAsync(token).ConfigureAwait(false);
                await EnsureAsync(connection, spec, logger, token).ConfigureAwait(false);
            },
            cancellationToken);

    /// <summary>
    /// <see cref="RunDelayedAsync(NpgsqlDataSource, ILogger, TimeSpan, IReadOnlyList{IndexSpec}, CancellationToken)"/>
    /// with the one ensure attempt injected, so the in-order, failure-isolated loop runs without a store.
    /// </summary>
    internal static async Task RunDelayedAsync(
        ILogger logger,
        TimeSpan delay,
        IReadOnlyList<IndexSpec> specs,
        Func<IndexSpec, CancellationToken, Task> ensureOne,
        CancellationToken cancellationToken)
    {
        var delayFinished = false;

        /* The index the run is on, so a shutdown line can name it: the delay has no index, and the loop sets this before
           each attempt. */
        IndexSpec? inProgress = null;
        try
        {
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            delayFinished = true;
            foreach (var spec in specs)
            {
                inProgress = spec;
                try
                {
                    await ensureOne(spec, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                {
                    LogEnsureFailure(logger, spec.IndexName, ex);
                }
            }
        }
        catch (OperationCanceledException) when (!delayFinished)
        {
            logger.LogDebug(
                "Query Store index ensure ({Indexes}) was cancelled before it started.",
                IndexNames(specs));
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation(
                "Query Store index ensure ({Index}) was cancelled at shutdown; the next start retries, "
                + "and drops any half-built leftover first.",
                inProgress?.IndexName ?? IndexNames(specs));
        }
        catch (Exception ex)
        {
            /* An error raised while the service shuts down (a connection closed under the build, say) is part of the
               shutdown, so it is Information like the line above, not a failure to retry-warn about. */
            logger.LogInformation(
                "Query Store index ensure ({Index}) stopped at shutdown: {ExceptionType}{SqlState}: {Message}; "
                + "the next start retries, and drops any half-built leftover first.",
                inProgress?.IndexName ?? IndexNames(specs), ex.GetType().Name, SqlStateSuffix(ex), ex.Message);
        }
    }

    private static string IndexNames(IReadOnlyList<IndexSpec> specs) => string.Join(", ", specs.Select(spec => spec.IndexName));

    /* ", SQLSTATE 53100" for a server error that carries a SQLSTATE (a full disk reads differently from a lock timeout),
       nothing otherwise. A block body, because TsqlConventionGuardTests' member scan stops short of an expression-bodied
       member that has a property pattern in it. */
    private static string SqlStateSuffix(Exception ex)
    {
        if (ex is NpgsqlException npgsql && !string.IsNullOrEmpty(npgsql.SqlState))
        {
            return $", SQLSTATE {npgsql.SqlState}";
        }

        return string.Empty;
    }

    private static void LogEnsureFailure(ILogger logger, string indexName, Exception ex)
    {
        logger.LogWarning(
            "Query Store index ensure ({Index}) failed and is retried at the next start: {ExceptionType}{SqlState}: {Message}",
            indexName, ex.GetType().Name, SqlStateSuffix(ex), ex.Message);
    }

    /// <summary>
    /// Makes sure one index exists and is valid, on <paramref name="connection"/>, which must be open and outside
    /// any transaction (<c>CONCURRENTLY</c> fails inside one).
    /// </summary>
    public static async Task EnsureAsync(
        NpgsqlConnection connection, IndexSpec spec, ILogger logger, CancellationToken cancellationToken)
    {
        int serverVersionNum;
        bool tableExists;
        bool hasHypertableView;
        bool? indexValid;

        await using (var state = new NpgsqlCommand(StateSql, connection) { CommandTimeout = CatalogReadTimeoutSeconds })
        {
            state.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = spec.TableName });
            state.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = spec.IndexName });
            await using var reader = await state.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            serverVersionNum = reader.GetInt32(0);
            tableExists = reader.GetBoolean(1);
            hasHypertableView = reader.GetBoolean(2);
            indexValid = reader.IsDBNull(3) ? null : reader.GetBoolean(3);
        }

        if (!tableExists)
        {
            logger.LogDebug("Query Store index ensure skipped: {Table} does not exist yet.", spec.TableName);
            return;
        }

        var isHypertable = false;
        if (hasHypertableView)
        {
            var (tableSchema, tableName) = SplitTableName(spec.TableName);
            await using var hypertable = new NpgsqlCommand(HypertableSql, connection) { CommandTimeout = CatalogReadTimeoutSeconds };
            hypertable.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = tableSchema });
            hypertable.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = tableName });
            isHypertable = (bool)(await hypertable.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
        }

        var decision = Decide(spec, serverVersionNum, isHypertable);
        if (decision.Action == IndexAction.SkipServerVersion)
        {
            logger.LogInformation("Query Store index {Index} not built: {Reason}.", spec.IndexName, decision.Reason);
            return;
        }

        if (decision.Action == IndexAction.SkipHypertable)
        {
            logger.LogWarning("Query Store index {Index} not built: {Reason}.", spec.IndexName, decision.Reason);
            return;
        }

        if (indexValid == true)
        {
            logger.LogDebug("Query Store index {Index} already exists and is valid.", spec.IndexName);
            return;
        }

        if (indexValid == false)
        {
            logger.LogWarning(
                "Query Store index {Index} exists but is INVALID (an interrupted build); dropping it and rebuilding.",
                spec.IndexName);
            await using var drop = new NpgsqlCommand(spec.PlainDropSql, connection) { CommandTimeout = BuildTimeoutSeconds };
            await drop.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        var started = System.Diagnostics.Stopwatch.StartNew();
        await using (var build = new NpgsqlCommand(spec.PlainCreateSql, connection) { CommandTimeout = BuildTimeoutSeconds })
        {
            await build.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        logger.LogInformation(
            "Query Store index {Index} built in {Seconds:F1}s (valid).", spec.IndexName, started.Elapsed.TotalSeconds);
    }
}
