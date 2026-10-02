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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The Query Store read indexes that are built in the background after the service is up rather than by a
/// migration rung (#4605's BRIN, #4952's two btrees), and the one piece of machinery they share: the state read,
/// the build-or-skip decision, the invalid-leftover drop, the build and the delayed start.
///
/// <para><b>Why not a migration rung.</b> Migrations run in one transaction and block startup, and
/// <c>CREATE INDEX CONCURRENTLY</c> cannot run inside a transaction (25001), nor can the hypertable per-chunk form
/// below. A plain in-rung build would hold a lock that blocks the writer for the whole heap read. An applied rung
/// also never changes, so each ensure is idempotent and runs on every start: a fresh store builds an empty table's
/// index instantly and no table-creation change is needed.</para>
///
/// <para><b>One launch, one build at a time.</b> <see cref="RunDelayedAsync"/> waits <see cref="StartDelay"/>
/// (20 minutes) once, then ensures each index in <see cref="All"/> in order, each on its own connection and each
/// failure-isolated: a store's volume sits at its IOPS cap for about 15 minutes after an install or restart (cold
/// cache, migrations, the retention purge, the continuous-aggregate refresh), and builds that each read a whole
/// table or chunk set must not stack on top of it, or on each other. One attempt is made per service start; a
/// failure or an interrupted build is retried at the next start. The one exception is an attempt deferred because the
/// newest chunk is too big for the per-chunk build (<see cref="IndexAction.SkipNewestChunkLarge"/>): that index alone
/// is tried again every <see cref="RetryInterval"/> until it is built or the service stops.</para>
///
/// <para><b>An interrupted build leaves an INVALID index behind</b> (a <c>CONCURRENTLY</c> build, and the
/// hypertable per-chunk build too: a cancel mid-build commits the chunks built so far and leaves the root index
/// <c>indisvalid = false</c>). <c>IF NOT EXISTS</c> would silently keep it, so <see cref="EnsureAsync"/> reads
/// validity first and drops an INVALID leftover before building. That read-then-drop is what makes the per-chunk
/// form safe here, where <c>PgTableTuning</c> rejects it for its start-time build.</para>
///
/// <para><b>Two build forms, chosen by the table.</b> A plain table takes <c>CREATE INDEX CONCURRENTLY</c>
/// (<see cref="IndexSpec.PlainCreateSql"/>), which blocks no writes. TimescaleDB refuses <c>CONCURRENTLY</c> on a
/// hypertable ("hypertables do not support concurrent index creation"), and also refuses
/// <c>DROP INDEX CONCURRENTLY</c> on a hypertable index, so a spec that supports hypertables carries the per-chunk
/// form (<see cref="IndexSpec.HypertableCreateSql"/>: <c>WITH (timescaledb.transaction_per_chunk)</c>) and a plain
/// <c>DROP INDEX</c> (<see cref="IndexSpec.HypertableDropSql"/>). A spec without one skips a hypertable with a
/// warning instead of failing or retrying.</para>
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

    /// <summary>
    /// The largest heap, in bytes, the newest chunk of <c>collect.query_store_stats</c> may have for the per-chunk
    /// partial-index build to start (256 MB), and the reason for the number (#4952).
    ///
    /// <para>The per-chunk build holds a ShareLock on the chunk it is scanning for that chunk's scan time, and the
    /// collector's COPY into the newest chunk (RowExclusiveLock) waits behind it. That COPY has a 10 s deadline
    /// (<c>ServiceCommandDeadlines.CollectionSweepSeconds</c>, on both the start and the data phase): a wait past it
    /// fails the batch. Nothing is lost, because the next cycle re-reads from the stored per-database watermark, but
    /// the cycle's rows for that database are not stored until then. The seeded store scanned about 1 GB/s warm and a
    /// volume capped at 250 MB/s reads 256 MB in about a second, so this limit keeps the lock far below the 10 s
    /// deadline, where a 5-9 GB chunk would hold it for tens of seconds. The newest chunk is the one the collector
    /// writes: the live cycle and the backfill both stamp the current time, so older chunks, which take the same lock
    /// for their own longer scans, are not written to.</para>
    /// </summary>
    public const long NewestChunkMaxBytes = 256L * 1024 * 1024;

    /// <summary>
    /// How long a deferred attempt (<see cref="IndexAction.SkipNewestChunkLarge"/>) waits before the next one: the
    /// Query Store collection cycle, the <c>query_store</c> default in <c>CollectorScheduleDefaults</c> (5 minutes).
    /// The newest chunk is small only for a while after each chunk boundary (at 5-9 GB per chunk, roughly the first
    /// 40-75 minutes), and a retry on a long interval keeps a fixed phase from the service start, so on such a store
    /// it can miss that window every day and the index is never built. A try is one catalog read
    /// (<c>NewestChunkSql</c>), so the extra tries cost almost nothing. The ensure keeps trying until the index is
    /// built or the service stops.
    /// </summary>
    public static readonly TimeSpan RetryInterval =
        TimeSpan.FromMinutes(CollectorScheduleDefaults.All["query_store"].FrequencyMinutes);

    /// <summary>Name of the partial btree that turns the legacy-row probe into an index probe (#4952).</summary>
    public const string LegacyProbeIndexName = "collect.ix_query_store_stats_legacy_server_time";

    /// <summary>Name of the btree that lets the per-server table read range-scan <c>first_execution_time</c> (#4952).</summary>
    public const string WideServerFirstExecIndexName = "collect.ix_query_store_interval_wide_server_first_exec";

    /// <summary>What the ensure does about one index.</summary>
    public enum IndexAction
    {
        /// <summary>Build (or keep) the index on a plain table.</summary>
        Build,

        /// <summary>Build (or keep) the index on a hypertable, with the per-chunk form.</summary>
        BuildPerChunk,

        /// <summary>Skip: the server predates the index's version floor.</summary>
        SkipServerVersion,

        /// <summary>Skip: the table is a hypertable and the spec has no hypertable form.</summary>
        SkipHypertable,

        /// <summary>
        /// Defer: the per-chunk build would hold a ShareLock on a newest chunk bigger than the spec's limit, long
        /// enough to time out the collector's write into it. Not a failure and not final: the ensure is tried again
        /// after <see cref="RetryInterval"/>.
        /// </summary>
        SkipNewestChunkLarge,
    }

    /// <summary>What one ensure attempt leaves for the caller to do.</summary>
    public enum EnsureOutcome
    {
        /// <summary>Nothing more to do this run: the index is built, was already valid, or is not wanted on this store.</summary>
        Settled,

        /// <summary>The attempt was deferred (<see cref="IndexAction.SkipNewestChunkLarge"/>); try the same index again later.</summary>
        RetryLater,
    }

    /// <summary>The decision and, for a skip, the reason to log.</summary>
    public readonly record struct IndexDecision(IndexAction Action, string Reason);

    /// <summary>One background index: its names, the statements that build and drop it, and when it may not be built.</summary>
    /// <param name="IndexName">The index's schema-qualified name.</param>
    /// <param name="TableName">The table's schema-qualified name.</param>
    /// <param name="PlainCreateSql">The build for a plain table; <c>CONCURRENTLY</c>, idempotent.</param>
    /// <param name="HypertableCreateSql">The build for a hypertable (the per-chunk form), or null when a hypertable is refused.</param>
    /// <param name="PlainDropSql">The drop of an INVALID leftover on a plain table.</param>
    /// <param name="HypertableDropSql">The drop of an INVALID leftover on a hypertable (never <c>CONCURRENTLY</c>).</param>
    /// <param name="MinimumServerVersionNum">The first <c>server_version_num</c> that may build it; 0 for any.</param>
    /// <param name="BelowMinimumReason">Why a server below the floor must not build it, for the log.</param>
    /// <param name="MaxNewestChunkBytes">
    /// For the per-chunk form only: the largest heap, in bytes, the hypertable's newest chunk may have when the build
    /// starts. Above it the attempt is deferred (<see cref="IndexAction.SkipNewestChunkLarge"/>). Null means no limit.
    /// </param>
    public sealed record IndexSpec(
        string IndexName,
        string TableName,
        string PlainCreateSql,
        string? HypertableCreateSql,
        string PlainDropSql,
        string HypertableDropSql,
        int MinimumServerVersionNum = 0,
        string BelowMinimumReason = "",
        long? MaxNewestChunkBytes = null);

    /// <summary>
    /// A partial btree on <c>collect.query_store_stats (server_id, collection_time) WHERE interval_start_time_utc IS
    /// NULL</c> (#4952), the legacy-row probe's own predicate (<see cref="QueryStoreIntervalWide.HasLegacyRowSql"/>).
    ///
    /// <para><b>Why.</b> The probe is an <c>EXISTS</c> that finds nothing on a field store (raw retention has aged every
    /// pre-tier-2 row out), so it must visit every raw row the server's window holds to say no: on a large store
    /// 855 k raw rows and 56 k heap blocks at 24 h for one server, about 4 s of a 10-15 s Query Store top call. No row
    /// satisfies the index's predicate there, so the index is empty, costs almost nothing to write, and the probe
    /// becomes an Index Only Scan with zero heap fetches. It is not a cached answer: the collector can still store a
    /// NULL <c>interval_start_time_utc</c> on a catalog join miss, and the duration-trend reads need every such row
    /// (see <see cref="QueryStoreIntervalWide.HasLegacyRowSql"/>), so the probe stays and stays exact.</para>
    ///
    /// <para><b>Built only while the newest chunk is small.</b> The per-chunk build holds a ShareLock on the chunk it
    /// is scanning, and the collector's COPY into the newest chunk waits behind it under a 10 s deadline, so the build
    /// starts only when that chunk's heap is at most <see cref="NewestChunkMaxBytes"/>. A bigger chunk defers the
    /// attempt, logged once, and it is tried again every <see cref="RetryInterval"/>: a chunk is small for a while
    /// after each chunk boundary, so a later attempt finds one.</para>
    ///
    /// <para><b>The hypertable form.</b> <c>query_store_stats</c> is a compressed hypertable.
    /// <c>CREATE INDEX CONCURRENTLY</c> is refused on one, so this builds with
    /// <c>WITH (timescaledb.transaction_per_chunk)</c> (the <c>WITH</c> goes before the <c>WHERE</c>), which indexes
    /// each chunk in its own transaction and works with a compressed chunk present. A compressed chunk's
    /// uncompressed relation is an empty shell that takes an 8 kB index, so a window that reaches a compressed
    /// chunk still decompresses and filters it: the index does not serve that part of a wide window.</para>
    /// </summary>
    public static readonly IndexSpec LegacyProbe = new(
        LegacyProbeIndexName,
        "collect.query_store_stats",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_query_store_stats_legacy_server_time "
        + "ON collect.query_store_stats (server_id, collection_time) WHERE interval_start_time_utc IS NULL;",
        "CREATE INDEX IF NOT EXISTS ix_query_store_stats_legacy_server_time "
        + "ON collect.query_store_stats (server_id, collection_time) WITH (timescaledb.transaction_per_chunk) "
        + "WHERE interval_start_time_utc IS NULL;",
        "DROP INDEX CONCURRENTLY IF EXISTS collect.ix_query_store_stats_legacy_server_time;",
        "DROP INDEX IF EXISTS collect.ix_query_store_stats_legacy_server_time;",
        MaxNewestChunkBytes: NewestChunkMaxBytes);

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
        null,
        "DROP INDEX CONCURRENTLY IF EXISTS collect.ix_query_store_interval_wide_server_first_exec;",
        "DROP INDEX IF EXISTS collect.ix_query_store_interval_wide_server_first_exec;");

    /// <summary>Every background index, in the order the one delayed task ensures them.</summary>
    public static readonly IReadOnlyList<IndexSpec> All = new[]
    {
        QueryStoreIntervalWideBrinIndex.Spec,
        WideServerFirstExec,
        LegacyProbe,
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

    /* Reached only when StateSql reported the view exists, so a store without TimescaleDB never parses it. */
    internal const string HypertableSql = @"
SELECT EXISTS
(
    SELECT 1
    FROM timescaledb_information.hypertables AS h
    WHERE h.hypertable_schema || '.' || h.hypertable_name = $1
);";

    /* The heap, in bytes, of the hypertable's newest chunk (the one the collector's COPY writes into): the main fork
       only, which is what an index build scans (a compressed chunk's uncompressed relation is an empty shell, so it
       reads 0). The catalog and the file sizes answer it: no table is scanned. to_regclass rather than a cast, so a
       chunk the retention purge drops between the view and the size read gives NULL, not an error. 0 when there is no
       chunk yet. Reached only for a hypertable, so timescaledb_information exists. */
    internal const string NewestChunkSql = @"
SELECT COALESCE
(
    (
        SELECT pg_relation_size(to_regclass(format('%I.%I', c.chunk_schema, c.chunk_name)))
        FROM timescaledb_information.chunks AS c
        WHERE c.hypertable_schema || '.' || c.hypertable_name = $1
        ORDER BY c.range_end DESC
        LIMIT 1
    ),
    0
)::bigint;";

    /// <summary>
    /// The pure build-or-skip decision. The version check comes first: below the floor a build is wrong whatever the
    /// table is. A hypertable then takes the per-chunk form, or is skipped when the spec has none.
    /// </summary>
    public static IndexDecision Decide(IndexSpec spec, int serverVersionNum, bool tableIsHypertable, long newestChunkBytes = 0)
    {
        if (serverVersionNum < spec.MinimumServerVersionNum)
        {
            return new IndexDecision(
                IndexAction.SkipServerVersion,
                $"server_version_num {serverVersionNum} is below {spec.MinimumServerVersionNum}: {spec.BelowMinimumReason}");
        }

        if (tableIsHypertable)
        {
            if (spec.HypertableCreateSql is null)
            {
                return new IndexDecision(
                    IndexAction.SkipHypertable,
                    $"{spec.TableName} is a hypertable and TimescaleDB refuses CREATE INDEX CONCURRENTLY on a hypertable");
            }

            /* Strictly above the limit defers; at it builds. A hypertable with no chunk yet reads as 0 and builds. */
            if (spec.MaxNewestChunkBytes is { } limit && newestChunkBytes > limit)
            {
                return new IndexDecision(
                    IndexAction.SkipNewestChunkLarge,
                    $"{spec.TableName}'s newest chunk is {FormatBytes(newestChunkBytes)}, above the {FormatBytes(limit)} "
                    + "the per-chunk build may hold a lock on while the collector writes into it");
            }

            return new IndexDecision(IndexAction.BuildPerChunk, string.Empty);
        }

        return new IndexDecision(IndexAction.Build, string.Empty);
    }

    private static string FormatBytes(long bytes) =>
        bytes >= 1024L * 1024 * 1024
            ? string.Create(CultureInfo.InvariantCulture, $"{bytes / (1024.0 * 1024 * 1024):F1} GB")
            : string.Create(CultureInfo.InvariantCulture, $"{bytes / (1024.0 * 1024):F0} MB");

    /// <summary>
    /// The background entry point: waits <paramref name="delay"/>, then makes one <see cref="EnsureAsync"/> attempt
    /// per spec, in order, each on its own connection, and never throws. A failure of one index is a Warning and the
    /// next index still runs; the next service start retries it. Cancellation (shutdown) ends it quietly.
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
            RetryInterval,
            specs,
            async (spec, isRetry, token) =>
            {
                await using var connection = await postgres.OpenConnectionAsync(token).ConfigureAwait(false);
                return await EnsureAsync(connection, spec, logger, token, isRetry).ConfigureAwait(false);
            },
            cancellationToken);

    /// <summary>
    /// <see cref="RunDelayedAsync(NpgsqlDataSource, ILogger, TimeSpan, IReadOnlyList{IndexSpec}, CancellationToken)"/>
    /// with the one ensure attempt injected, so the pass-and-retry loop runs without a store.
    /// </summary>
    internal static async Task RunDelayedAsync(
        ILogger logger,
        TimeSpan delay,
        TimeSpan retryInterval,
        IReadOnlyList<IndexSpec> specs,
        Func<IndexSpec, bool, CancellationToken, Task<EnsureOutcome>> ensureOne,
        CancellationToken cancellationToken)
    {
        var delayFinished = false;
        var waitingToRetry = false;
        try
        {
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            delayFinished = true;

            /* One pass over every index, in order. An index whose attempt was deferred (the newest chunk was too big
               for the per-chunk build) goes into the next pass, after the retry interval, alone: an index that was
               built, was already valid or was not wanted is never attempted again, and a failed one is left for the
               next start. The wait takes the stopping token, so a shutdown does not sit out the retry interval. */
            var pending = specs;
            var isRetry = false;
            while (pending.Count > 0)
            {
                var deferred = new List<IndexSpec>();
                foreach (var spec in pending)
                {
                    try
                    {
                        if (await ensureOne(spec, isRetry, cancellationToken).ConfigureAwait(false) == EnsureOutcome.RetryLater)
                        {
                            deferred.Add(spec);
                        }
                    }
                    catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                    {
                        LogEnsureFailure(logger, spec.IndexName, ex);
                    }
                }

                pending = deferred;
                if (pending.Count > 0)
                {
                    waitingToRetry = true;
                    await Task.Delay(retryInterval, cancellationToken).ConfigureAwait(false);
                    waitingToRetry = false;
                    isRetry = true;
                }
            }
        }
        catch (OperationCanceledException) when (!delayFinished)
        {
            logger.LogDebug("Query Store index ensure was cancelled before it started.");
        }
        catch (OperationCanceledException) when (waitingToRetry)
        {
            logger.LogDebug("Query Store index ensure was cancelled while waiting to retry a deferred index; the next start retries.");
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation(
                "Query Store index ensure was cancelled at shutdown; the next start retries, "
                + "and drops any half-built leftover first.");
        }
        catch (Exception ex)
        {
            LogEnsureFailure(logger, "(shutdown)", ex);
        }
    }

    /// <summary>
    /// The deferral's log line: Information the first time an index is deferred in this run, Debug on every retry
    /// after it, so a newest chunk that stays big for days writes one line at Information, not one per retry.
    /// </summary>
    internal static void LogDeferred(ILogger logger, IndexSpec spec, string reason, bool isRetry)
    {
        if (isRetry)
        {
            logger.LogDebug(
                "Query Store index {Index} is still deferred: {Reason}. Trying again in {Minutes} minutes.",
                spec.IndexName, reason, (int)RetryInterval.TotalMinutes);
            return;
        }

        logger.LogInformation(
            "Query Store index {Index} is not built yet: {Reason}. It is tried again every {Minutes} minutes "
            + "until it is built or the service stops.",
            spec.IndexName, reason, (int)RetryInterval.TotalMinutes);
    }

    private static void LogEnsureFailure(ILogger logger, string indexName, Exception ex)
    {
        var sqlState = ex is NpgsqlException { SqlState: { Length: > 0 } state } ? $", SQLSTATE {state}" : string.Empty;
        logger.LogWarning(
            "Query Store index ensure ({Index}) failed and is retried at the next start: {ExceptionType}{SqlState}: {Message}",
            indexName, ex.GetType().Name, sqlState, ex.Message);
    }

    /// <summary>
    /// Makes sure one index exists and is valid, on <paramref name="connection"/>, which must be open and outside
    /// any transaction (<c>CONCURRENTLY</c> and the per-chunk form both fail inside one).
    /// </summary>
    public static async Task<EnsureOutcome> EnsureAsync(
        NpgsqlConnection connection, IndexSpec spec, ILogger logger, CancellationToken cancellationToken, bool isRetry = false)
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
            return EnsureOutcome.Settled;
        }

        var isHypertable = false;
        if (hasHypertableView)
        {
            await using var hypertable = new NpgsqlCommand(HypertableSql, connection) { CommandTimeout = CatalogReadTimeoutSeconds };
            hypertable.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = spec.TableName });
            isHypertable = (bool)(await hypertable.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
        }

        /* The newest chunk's heap, read only when the answer can matter: a per-chunk build is coming (the index is not
           already valid) for a spec that limits it. A hypertable with no chunk yet reads as 0 and builds. */
        var newestChunkBytes = 0L;
        if (isHypertable && spec.MaxNewestChunkBytes is not null && indexValid != true)
        {
            await using var newest = new NpgsqlCommand(NewestChunkSql, connection) { CommandTimeout = CatalogReadTimeoutSeconds };
            newest.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = spec.TableName });
            newestChunkBytes = (long)(await newest.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
        }

        var decision = Decide(spec, serverVersionNum, isHypertable, newestChunkBytes);
        if (decision.Action == IndexAction.SkipServerVersion)
        {
            logger.LogInformation("Query Store index {Index} not built: {Reason}.", spec.IndexName, decision.Reason);
            return EnsureOutcome.Settled;
        }

        if (decision.Action == IndexAction.SkipHypertable)
        {
            logger.LogWarning("Query Store index {Index} not built: {Reason}.", spec.IndexName, decision.Reason);
            return EnsureOutcome.Settled;
        }

        if (decision.Action == IndexAction.SkipNewestChunkLarge)
        {
            LogDeferred(logger, spec, decision.Reason, isRetry);
            return EnsureOutcome.RetryLater;
        }

        if (indexValid == true)
        {
            logger.LogDebug("Query Store index {Index} already exists and is valid.", spec.IndexName);
            return EnsureOutcome.Settled;
        }

        if (indexValid == false)
        {
            logger.LogWarning(
                "Query Store index {Index} exists but is INVALID (an interrupted build); dropping it and rebuilding.",
                spec.IndexName);
            var dropSql = decision.Action == IndexAction.BuildPerChunk ? spec.HypertableDropSql : spec.PlainDropSql;
            await using var drop = new NpgsqlCommand(dropSql, connection) { CommandTimeout = BuildTimeoutSeconds };
            await drop.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        var createSql = decision.Action == IndexAction.BuildPerChunk ? spec.HypertableCreateSql! : spec.PlainCreateSql;
        var started = System.Diagnostics.Stopwatch.StartNew();
        await using (var build = new NpgsqlCommand(createSql, connection) { CommandTimeout = BuildTimeoutSeconds })
        {
            await build.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        logger.LogInformation(
            "Query Store index {Index} built in {Seconds:F1}s (valid).", spec.IndexName, started.Elapsed.TotalSeconds);
        return EnsureOutcome.Settled;
    }
}
