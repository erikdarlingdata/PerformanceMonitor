/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The opt-in trigram index on the Query Store text, <c>collect.query_store_text (server_id, query_sql_text)</c> (#8570), which lets
/// <c>find_queries_by_text</c> search a server's Query Store text through the index instead of walking the server's rows in primary-key
/// order. Measured on a seeded store of 1,000,000 texts of about 250 characters (40 servers): a 29 to 32 second build, 315 MB (86% of the
/// table), and about 0.02 ms added to each inserted row (0.002 ms without it), the same as the index on the text alone on those rows. A
/// no-match search of one server read 1,301 buffers in 34 ms without it and 340 buffers in 0.7 ms with it. So it is off by default, it is
/// never built by a migration rung, and turning the option off drops it.
///
/// <para><b>The server is a key of the index.</b> <c>server_id</c> is its first column (the <c>btree_gin</c> operator class), so the server
/// filter is applied inside the index, beside the text's trigrams, and only that server's candidates are read from the table and rechecked.
/// An index on the text alone hands back every server's candidates and filters them after the recheck: on a seeded store of 6.9 million texts
/// over 40 servers, a one-server search of a pattern made of common trigrams rechecked 521,267 rows to keep 27, and took 2.0 to 2.5 s; with the
/// server key it rechecked 13,023 and took 0.37 to 0.48 s, and a no-match one went from 0.81 s to 0.07 s. The two indexes are the same size
/// (2.1 GB there).</para>
///
/// <para><b>The index it replaces.</b> The index on the text alone (<see cref="ReplacedIndexName"/>) is left in place until this one is valid,
/// so the search keeps reading through it while this one waits for its extensions or builds, and it is dropped once this one is valid. Turning
/// the option off drops both.</para>
///
/// <para><b>Write cost.</b> An insert, a text overwrite and any update of a text row that cannot stay on its page (a non-HOT update) each
/// add the row's trigrams, which GIN's fast update collects in its pending list and merges in bulk. Texts are wide, so few updates find
/// room on their page: on 3.45 million texts of about 1.4 KB at fillfactor 90, about 9% of a 1,500-row batch of <c>last_seen</c> updates
/// stayed HOT, and the batch took 308 to 455 ms and 33 to 55 MB of WAL with this index against 10 to 13 ms and 5 MB without it. That is
/// why the liveness stamp lives in <see cref="QueryStoreTextSeen"/> and the fetch probe's touch writes it there (4 to 5 ms and 0.5 MB for
/// the same batch). The touch updates a text row only through <see cref="QueryStoreTextStore.RefreshTextRowSql"/>, once every
/// <see cref="QueryStoreTextSeen.TextRowRefreshHours"/> hours a row at most.</para>
///
/// <para><b>The pending list.</b> The insert that takes the pending list past <c>gin_pending_list_limit</c> merges the whole list inside its
/// own call, and at the server default of 4 MB that is the collector's insert every few dozen calls. Both indexes carry the plan-cache
/// index's limit (<see cref="PendingListLimitKb"/>, 64 MB): the build creates this one with it, and the clean sets it on an index that
/// lacks it, under <see cref="LockTimeout"/>. The clean (<see cref="CleanPendingListsAsync"/>) runs every <see cref="CleanInterval"/> on
/// its own connection, apart from the pass, and empties the list of each index that is valid, this one and the one it replaces for as long
/// as that one exists, so the merge is the clean's work and not the collector's.</para>
///
/// <para><b>One pass.</b> <see cref="RunPassAsync"/> follows the plan-cache index's pass (<see cref="QueryTextTrigramIndex"/>): option off,
/// drop this index and the one it replaces (<c>DROP INDEX CONCURRENTLY</c>); option on and index valid, drop the replaced one if it is still
/// there; a build running in another backend, nothing;
/// inside the wait after a failed build, nothing; then create <c>pg_trgm</c> and <c>btree_gin</c> (a login that cannot is reported as
/// <see cref="TrigramIndexStatus.WaitingOnExtension"/> with the statement an operator runs, and is not asked again), drop an INVALID
/// leftover, build with <c>CREATE INDEX CONCURRENTLY</c>, and drop the replaced one. A failed build drops its own leftover at once and waits
/// <see cref="QueryTextTrigramIndex.Backoff"/> before the next try. The table is a plain table on every store (not a hypertable), so both
/// statements are always <c>CONCURRENTLY</c>.</para>
///
/// <para><b>Its own memory.</b> The pass records into <see cref="SharedTracker"/>, a tracker of its own, so a failure or an extension block
/// on one index never shows on the other's health block. Both passes read the same <c>pg_trgm</c> catalog row, so an operator's
/// <c>CREATE EXTENSION</c> clears both.</para>
/// </summary>
public static class QueryStoreTextTrigramIndex
{
    /// <summary>The table the index is on.</summary>
    public const string TableName = "collect.query_store_text";

    /// <summary>The index's bare name.</summary>
    public const string IndexBareName = "ix_query_store_text_server_trgm";

    /// <summary>The index's schema-qualified name.</summary>
    public const string IndexName = "collect." + IndexBareName;

    /// <summary>The index this one replaces, on <c>query_sql_text</c> alone (see the class summary).</summary>
    public const string ReplacedIndexName = "collect.ix_query_store_text_trgm";

    /// <summary>The statement an operator runs when the store login cannot create the extensions the index needs.</summary>
    public const string OperatorStatement = "CREATE EXTENSION IF NOT EXISTS pg_trgm; CREATE EXTENSION IF NOT EXISTS btree_gin;";

    /// <summary>The server key's extension, created after <c>pg_trgm</c>.</summary>
    internal const string CreateKeyExtensionSql = "CREATE EXTENSION IF NOT EXISTS btree_gin";

    /// <summary>The option's name in <c>darling.json</c>, which the health note and the search's messages name.</summary>
    public const string OptionName = "queryStoreTextTrigramIndexEnabled";

    /// <summary>The instance the service's worker writes and the health read and the search read.</summary>
    public static QueryTextTrigramIndexTracker SharedTracker { get; } = new();

    /// <summary>How long after the service is up the first pass waits when the option is on, so a build stays off the post-restart IO burst.</summary>
    public static readonly TimeSpan StartDelay = QueryStoreBackgroundIndexes.StartDelay;

    /// <summary>Time between passes.</summary>
    public static readonly TimeSpan PassInterval = QueryTextTrigramIndex.PassInterval;

    /// <summary>Both indexes' <c>gin_pending_list_limit</c> in kB: the plan-cache index's value.</summary>
    internal const int PendingListLimitKb = QueryTextTrigramIndex.PendingListLimitKb;

    /// <summary>How long a limit change on either index waits for its lock: the plan-cache index's value.</summary>
    internal const string LockTimeout = QueryTextTrigramIndex.LockTimeout;

    /// <summary>The time between cleans of the two indexes' pending lists. Each row that reaches this GIN carries a whole Query Store
    /// statement's trigrams, about 456 trigrams or about 1.9 pending-list pages on a text of 1.4 KB, so its list fills faster per row than the
    /// plan-cache index's: at a store ten times the field's write rate a list cleaned every 5 minutes reaches the
    /// <see cref="PendingListLimitKb"/> limit, and 2 minutes keeps it under half of it. #9303: the plan-cache index's
    /// <see cref="QueryTextTrigramIndex.CleanInterval"/> is the same 2 minutes, so a collection insert on either one rarely meets a long list.
    /// #9460 measured 2, 5 and 10 minutes at 60,000 new texts an hour: 2 minutes peaked just under half of the limit, and 5 and 10 minutes
    /// reached it and made the inserts merge the list inline. 2 minutes is the longest safe interval.</summary>
    public static readonly TimeSpan CleanInterval = TimeSpan.FromMinutes(2);

    /// <summary>One catalog read: the table, the index, the index it replaces, both extensions and any build in progress on the table. No
    /// parameters.</summary>
    internal const string StateSql = $@"
SELECT to_regclass('{TableName}') IS NOT NULL AS table_exists,
       i.indisvalid,
       CASE WHEN i.indexrelid IS NULL THEN NULL ELSE pg_relation_size(i.indexrelid) END AS index_size_bytes,
       ext.nspname AS extension_schema,
       (SELECT count(*) FROM pg_available_extensions WHERE name IN ('pg_trgm', 'btree_gin')) = 2 AS extension_available,
       EXISTS (SELECT 1 FROM pg_stat_progress_create_index AS p
               WHERE p.relid = to_regclass('{TableName}')
               AND   p.datid = (SELECT oid FROM pg_database WHERE datname = current_database())) AS build_running,
       key_ext.nspname AS key_extension_schema,
       to_regclass('{ReplacedIndexName}') IS NOT NULL AS replaced_index_exists
FROM (SELECT 1) AS one
LEFT JOIN pg_index AS i ON i.indexrelid = to_regclass('{IndexName}')
LEFT JOIN (SELECT n.nspname FROM pg_extension AS e JOIN pg_namespace AS n ON n.oid = e.extnamespace WHERE e.extname = 'pg_trgm') AS ext ON true
LEFT JOIN (SELECT n.nspname FROM pg_extension AS e JOIN pg_namespace AS n ON n.oid = e.extnamespace WHERE e.extname = 'btree_gin') AS key_ext ON true";

    internal const string DropSql = "DROP INDEX CONCURRENTLY IF EXISTS " + IndexName;

    internal const string DropReplacedSql = "DROP INDEX CONCURRENTLY IF EXISTS " + ReplacedIndexName;

    /// <summary>The build, with each operator class named in the schema its extension lives in (it is not always on the search path), and
    /// with <see cref="PendingListLimitKb"/> as the index's pending list limit.</summary>
    internal static string CreateIndexSql(string extensionSchema, string keyExtensionSchema) =>
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS " + IndexBareName
        + " ON " + TableName + " USING gin (server_id "
        + QueryTextTrigramIndex.QuoteIdentifier(keyExtensionSchema) + ".int4_ops, query_sql_text "
        + QueryTextTrigramIndex.QuoteIdentifier(extensionSchema) + ".gin_trgm_ops) WITH (gin_pending_list_limit = " + PendingListLimitKb + ")";

    /// <summary>Reads the catalog state on an open connection. The sample-index fields of the record stay at their defaults.</summary>
    public static async Task<TrigramCatalogState> ReadStateAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(StateSql, connection) { CommandTimeout = QueryStoreBackgroundIndexes.CatalogReadTimeoutSeconds };
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        return new TrigramCatalogState(
            reader.GetBoolean(0),
            reader.IsDBNull(1) ? null : reader.GetBoolean(1),
            reader.IsDBNull(2) ? null : reader.GetInt64(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.GetBoolean(4),
            reader.GetBoolean(5),
            KeyExtensionSchema: reader.IsDBNull(6) ? null : reader.GetString(6),
            ReplacedIndexExists: reader.GetBoolean(7));
    }

    /// <summary>
    /// One pass (see the class summary). Returns true when there is nothing to do: the option is off and no index remains. The loop goes on
    /// regardless, because the option is read at every pass (#8978) and can be turned on later. Throws only on cancellation or on a failure
    /// to read the catalog; a failed build, drop or extension create is recorded in <paramref name="tracker"/> and logged.
    /// </summary>
    public static Task<bool> RunPassAsync(
        NpgsqlDataSource postgres, bool enabled, QueryTextTrigramIndexTracker tracker, DateTime utcNow, ILogger logger,
        CancellationToken cancellationToken) =>
        RunPassAsync(postgres, enabled, tracker, utcNow, logger, QueryStoreBackgroundIndexes.BuildTimeoutSeconds, cancellationToken);

    /// <summary>The pass with the build's command deadline passed in, so a test can make a build fail without a large table.</summary>
    internal static async Task<bool> RunPassAsync(
        NpgsqlDataSource postgres, bool enabled, QueryTextTrigramIndexTracker tracker, DateTime utcNow, ILogger logger,
        int buildTimeoutSeconds, CancellationToken cancellationToken)
    {
        await using var connection = await postgres.OpenConnectionAsync(StoreSessionNames.TrigramIndex, cancellationToken).ConfigureAwait(false);
        var state = await ReadStateAsync(connection, cancellationToken).ConfigureAwait(false);
        if (!state.TableExists)
        {
            logger.LogDebug("Query Store text trigram index pass skipped: {Table} does not exist yet.", TableName);
            return false;
        }

        if (state.BuildRunning)
        {
            /* An index build on the table is running in another backend (ours would not be, passes are sequential). Its INVALID
               index must not be dropped from under it. */
            logger.LogDebug("Query Store text trigram index pass skipped: an index build on {Table} is already running.", TableName);
            return false;
        }

        if (!enabled)
        {
            if (state.IndexValid is null && !state.ReplacedIndexExists)
            {
                return true;
            }

            if (state.IndexValid is not null)
            {
                await DropAsync(connection, tracker, logger, DropSql, IndexName, "the option is off", cancellationToken).ConfigureAwait(false);
            }

            if (state.ReplacedIndexExists)
            {
                await DropAsync(connection, tracker, logger, DropReplacedSql, ReplacedIndexName, "the option is off", cancellationToken).ConfigureAwait(false);
            }

            return false;
        }

        if (state.IndexValid == true)
        {
            tracker.ClearExtensionBlock();
            tracker.Succeeded();
            if (state.ReplacedIndexExists)
            {
                await DropReplacedAsync(connection, logger, cancellationToken).ConfigureAwait(false);
            }

            return false;
        }

        var snapshot = tracker.Read();
        if (snapshot.NextAttemptUtc is { } next && utcNow < next)
        {
            logger.LogDebug("Query Store text trigram index build waits until {Next:o} after {Failures} failed attempt(s).", next, snapshot.Failures);
            return false;
        }

        var extensionSchema = state.ExtensionSchema;
        var keyExtensionSchema = state.KeyExtensionSchema;
        if (extensionSchema is null || keyExtensionSchema is null)
        {
            if (snapshot.ExtensionBlocked)
            {
                /* Not asked again: the statement was refused once, and a retry every pass would log and fail forever. The catalog read above
                   notices when an operator creates the extensions. */
                return false;
            }

            try
            {
                if (extensionSchema is null)
                {
                    await using var create = new NpgsqlCommand(QueryTextTrigramIndex.CreateExtensionSql, connection) { CommandTimeout = QueryStoreBackgroundIndexes.CatalogReadTimeoutSeconds };
                    await create.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                if (keyExtensionSchema is null)
                {
                    await using var createKey = new NpgsqlCommand(CreateKeyExtensionSql, connection) { CommandTimeout = QueryStoreBackgroundIndexes.CatalogReadTimeoutSeconds };
                    await createKey.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                var created = await ReadStateAsync(connection, cancellationToken).ConfigureAwait(false);
                (extensionSchema, keyExtensionSchema) = (created.ExtensionSchema, created.KeyExtensionSchema);
            }
            catch (PostgresException ex)
            {
                /* The plan-cache index's pass may have created pg_trgm at the same moment (two IF NOT EXISTS creates race on the catalog's
                   unique index), so the catalog decides: an extension that exists now is used. */
                var after = await ReadStateAsync(connection, cancellationToken).ConfigureAwait(false);
                (extensionSchema, keyExtensionSchema) = (after.ExtensionSchema, after.KeyExtensionSchema);
                if (extensionSchema is null || keyExtensionSchema is null)
                {
                    tracker.BlockExtension(QueryTextTrigramIndex.ErrorText(ex));
                    logger.LogWarning(
                        "The trigram index on {Table} is waiting on the {Extension} extension, which this login could not create ({SqlState}: {Message}). "
                        + "Run this in the store database as a role that may create it, and the next pass builds the index: {Statement}",
                        TableName, extensionSchema is null ? "pg_trgm" : "btree_gin", ex.SqlState, QueryTextTrigramIndex.ErrorText(ex), OperatorStatement);
                    return false;
                }
            }
        }

        tracker.ClearExtensionBlock();
        if (extensionSchema is null || keyExtensionSchema is null)
        {
            return false;
        }

        if (state.IndexValid == false)
        {
            logger.LogWarning("Query Store text trigram index {Index} exists but is INVALID (an interrupted build); dropping it and rebuilding.", IndexName);
            await DropAsync(connection, tracker, logger, DropSql, IndexName, "an INVALID leftover", cancellationToken).ConfigureAwait(false);
        }

        tracker.BeginBuild(utcNow);
        var started = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            await using var build = new NpgsqlCommand(CreateIndexSql(extensionSchema, keyExtensionSchema), connection) { CommandTimeout = buildTimeoutSeconds };
            await build.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            tracker.Succeeded();
            logger.LogInformation("Query Store text trigram index {Index} built in {Seconds:F1}s (valid).", IndexName, started.Elapsed.TotalSeconds);
            if (state.ReplacedIndexExists)
            {
                await DropReplacedAsync(connection, logger, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            /* Shutdown: the leftover, if any, is dropped by the next start. */
            throw;
        }
        catch (Exception ex)
        {
            var wait = QueryTextTrigramIndex.Backoff(tracker.Read().Failures + 1);
            tracker.Failed(ex, utcNow, wait);
            logger.LogWarning(
                "Query Store text trigram index {Index} build failed after {Seconds:F1}s and is tried again after {Wait}: {Message}",
                IndexName, started.Elapsed.TotalSeconds, wait, QueryTextTrigramIndex.ErrorText(ex));
            await DropLeftoverAfterFailureAsync(postgres, logger, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            tracker.EndPhase();
        }

        return false;
    }

    private static async Task DropAsync(
        NpgsqlConnection connection, QueryTextTrigramIndexTracker tracker, ILogger logger, string dropSql, string index, string reason,
        CancellationToken cancellationToken)
    {
        tracker.BeginDrop();
        try
        {
            await using var drop = new NpgsqlCommand(dropSql, connection) { CommandTimeout = QueryStoreBackgroundIndexes.BuildTimeoutSeconds };
            await drop.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            logger.LogInformation("Query Store text trigram index {Index} dropped ({Reason}).", index, reason);
        }
        finally
        {
            tracker.EndPhase();
        }
    }

    /* The index on the text alone goes once this one is valid. A drop that does not finish is a Warning, not a failed build: this index
       serves the search either way, and the next pass drops the replaced one again. */
    private static async Task DropReplacedAsync(NpgsqlConnection connection, ILogger logger, CancellationToken cancellationToken)
    {
        try
        {
            await using var drop = new NpgsqlCommand(DropReplacedSql, connection) { CommandTimeout = QueryStoreBackgroundIndexes.BuildTimeoutSeconds };
            await drop.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            logger.LogInformation("Query Store text trigram index {Index} dropped (replaced by {Replacement}).", ReplacedIndexName, IndexName);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(
                "Dropping {Index}, which {Replacement} replaces, did not finish and the next pass tries again: {Message}",
                ReplacedIndexName, IndexName, QueryTextTrigramIndex.ErrorText(ex));
        }
    }

    /* A failed build's INVALID index is still maintained by every write to the table, so it goes at once. A fresh connection: the failed
       one may be broken. Best effort: the next attempt drops a leftover first anyway. */
    private static async Task DropLeftoverAfterFailureAsync(NpgsqlDataSource postgres, ILogger logger, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(StoreSessionNames.TrigramIndex, cancellationToken).ConfigureAwait(false);
            await using var drop = new NpgsqlCommand(DropSql, connection) { CommandTimeout = QueryStoreBackgroundIndexes.BuildTimeoutSeconds };
            await drop.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogDebug("Dropping the failed Query Store text trigram index build's leftover did not finish ({Message}); the next attempt drops it first.", QueryTextTrigramIndex.ErrorText(ex));
        }
    }

    /// <summary>
    /// The background entry point: makes a pass every <paramref name="interval"/> until shutdown, after <paramref name="firstDelay"/>.
    /// <paramref name="enabled"/> is the option's live, resolved value (#8978: the store's <c>config.config_service</c> column when it holds
    /// one, else <c>darling.json</c>), read at the start of every pass, so a store write takes effect at the next pass with no restart. With
    /// the option off and no index the pass is one catalog read. Never throws: a pass that fails is a Warning and the next interval tries again.
    /// The clean of the pending lists runs beside the pass on its own loop, every <see cref="CleanInterval"/>, and ends with it at
    /// shutdown. #9303: it starts at once and does not wait for <paramref name="firstDelay"/>, which holds back only the build.
    /// </summary>
    public static Task RunAsync(
        NpgsqlDataSource postgres, Func<bool> enabled, QueryTextTrigramIndexTracker tracker, ILogger logger,
        TimeSpan firstDelay, TimeSpan interval, CancellationToken cancellationToken) =>
        QueryTextTrigramIndex.RunLoopsAsync(
            enabled, tracker, logger, "Query Store text trigram index", firstDelay, interval, CleanInterval,
            (on, ct) => RunPassAsync(postgres, on, tracker, DateTime.UtcNow, logger, ct),
            ct => CleanPendingListsAsync(postgres, enabled, logger, ct),
            static (wait, ct) => Task.Delay(wait, ct),
            cancellationToken);

    /// <summary>
    /// One clean of both indexes, this one and the one it replaces, each on the plan-cache index's terms
    /// (<see cref="QueryTextTrigramIndex.CleanPendingListAsync(NpgsqlDataSource, Func{bool}, string, ILogger, CancellationToken)"/>): only when
    /// the option is on and the index exists and is valid, an index that lacks <see cref="PendingListLimitKb"/> first gets it in its own
    /// transaction under <see cref="LockTimeout"/>, and a lock that is not free in time is a quiet skip that the next clean retries. Each
    /// index is handled on its own connection, so a skip or a failure on one never keeps the other from its limit or its clean. Throws only on
    /// cancellation.
    /// </summary>
    internal static async Task CleanPendingListsAsync(NpgsqlDataSource postgres, Func<bool> enabled, ILogger logger, CancellationToken cancellationToken)
    {
        await QueryTextTrigramIndex.CleanPendingListAsync(postgres, enabled, IndexName, logger, cancellationToken).ConfigureAwait(false);
        await QueryTextTrigramIndex.CleanPendingListAsync(postgres, enabled, ReplacedIndexName, logger, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The status word for a catalog reading and the tracker's memory. Pure; the plan-cache index's rules, with the extensions
    /// counted as installed only when both are.</summary>
    public static TrigramIndexStatus Classify(TrigramCatalogState state, QueryTextTrigramIndexTracker.Snapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(state);
        return QueryTextTrigramIndex.Classify(state with { ExtensionSchema = state.KeyExtensionSchema is null ? null : state.ExtensionSchema }, snapshot);
    }

    /// <summary>The store health read for the index: the catalog on <paramref name="connection"/> beside the tracker's memory.</summary>
    public static async Task<TrigramIndexHealth> ReadHealthAsync(
        NpgsqlConnection connection, QueryTextTrigramIndexTracker tracker, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(tracker);
        var state = await ReadStateAsync(connection, cancellationToken).ConfigureAwait(false);
        var snapshot = tracker.Read();
        var status = Classify(state, snapshot);
        return new TrigramIndexHealth(
            status,
            snapshot.Enabled,
            state.IndexValid is not null,
            state.IndexValid,
            state.IndexSizeBytes,
            status == TrigramIndexStatus.Building,
            snapshot.BuildStartedUtc,
            state.ExtensionSchema is not null && state.KeyExtensionSchema is not null,
            state.ExtensionAvailable,
            snapshot.Failures,
            snapshot.LastError ?? (status == TrigramIndexStatus.WaitingOnExtension ? snapshot.ExtensionError : null),
            snapshot.LastSqlState,
            snapshot.LastFailedUtc,
            snapshot.NextAttemptUtc,
            status == TrigramIndexStatus.WaitingOnExtension ? OperatorStatement : null,
            OptionSource: snapshot.OptionSource,
            ReplacedIndexExists: state.ReplacedIndexExists);
    }

    /// <summary>One sentence for the health block's note, per status.</summary>
    public static string Note(TrigramIndexHealth health)
    {
        ArgumentNullException.ThrowIfNull(health);
        var size = health.SizeBytes is { } bytes ? (bytes / 1048576.0).ToString("F1", CultureInfo.InvariantCulture) + " MB" : "no size";
        return health.Status switch
        {
            TrigramIndexStatus.Off => "The option " + OptionName + " is off and there is no trigram index on the Query Store text. find_queries_by_text reads one server's Query Store text in key order under a time budget.",
            TrigramIndexStatus.DropPending => "The option " + OptionName + " is off. An index still exists and the service drops it on its next pass.",
            TrigramIndexStatus.Dropping => "The option " + OptionName + " is off. The service is dropping the index now.",
            TrigramIndexStatus.Pending => "The option is on and the index does not exist yet. The service builds it in the background after startup, and an INVALID leftover is dropped first.",
            TrigramIndexStatus.Building => "A build is running. Collection is not waiting for it. The size is what has been written so far (" + size + ").",
            TrigramIndexStatus.Ready => "The trigram index on the Query Store text exists and is valid (" + size + "). find_queries_by_text searches a server's Query Store text through it.",
            TrigramIndexStatus.Unconfigured => "The service has not read darling.json yet, so the option " + OptionName + " is not known. Try again in a moment.",
            TrigramIndexStatus.Failed => "The last build failed. Its leftover was dropped and the service tries again at next_attempt_at, after a wait that doubles to a cap of "
                + ((int)QueryTextTrigramIndex.BackoffCap.TotalHours).ToString(CultureInfo.InvariantCulture) + " hours.",
            _ => health.ExtensionAvailable
                ? "The store login cannot create the pg_trgm and btree_gin extensions the index needs. Run operator_statement in the store database as a role that may create them (a superuser, or the database owner). The next pass then builds the index. It is not retried until then."
                : "The server does not ship the pg_trgm and btree_gin extensions. Install the PostgreSQL contrib package, then run operator_statement in the store database as a role that may create them.",
        } + (health.ReplacedIndexExists && health.Enabled && health.Status != TrigramIndexStatus.Ready
            ? " Until then the index it replaces, on the text alone (" + ReplacedIndexName + "), still serves the search."
            : "");
    }
}
