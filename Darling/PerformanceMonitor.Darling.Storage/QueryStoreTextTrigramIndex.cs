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
/// The opt-in trigram index on the Query Store text, <c>collect.query_store_text.query_sql_text</c> (#8570), which lets
/// <c>find_queries_by_text</c> search a server's Query Store text through the index instead of walking the server's rows in primary-key
/// order. Measured on a seeded store of 1,000,000 texts of about 300 characters (40 servers): a 32.7 second build, 398 MB (92% of the
/// table), and about 0.07 ms added to each inserted row (0.004 ms without it). A no-match search of one server read 11,325 buffers in
/// 52 ms without it and 47 buffers in 0.3 ms with it. So it is off by default, it is never built by a migration rung, and turning the
/// option off drops it.
///
/// <para><b>Write cost.</b> <c>query_sql_text</c> only changes when Query Store renumbers a <c>query_id</c>, so the liveness touch of
/// <c>last_seen</c> stays a HOT update when the page has room (the table runs at fillfactor 90) and writes nothing to this index. A touch
/// that cannot stay on its page, an insert and a text overwrite each add the row's trigrams, which GIN's fast update collects in its
/// pending list and merges in bulk.</para>
///
/// <para><b>One pass.</b> <see cref="RunPassAsync"/> follows the plan-cache index's pass (<see cref="QueryTextTrigramIndex"/>): option off and
/// index present, drop it (<c>DROP INDEX CONCURRENTLY</c>); option on and index valid, nothing; a build running in another backend, nothing;
/// inside the wait after a failed build, nothing; then create <c>pg_trgm</c> (a login that cannot is reported as
/// <see cref="TrigramIndexStatus.WaitingOnExtension"/> with the statement an operator runs, and is not asked again), drop an INVALID
/// leftover, and build with <c>CREATE INDEX CONCURRENTLY</c>. A failed build drops its own leftover at once and waits
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
    public const string IndexBareName = "ix_query_store_text_trgm";

    /// <summary>The index's schema-qualified name.</summary>
    public const string IndexName = "collect." + IndexBareName;

    /// <summary>The option's name in <c>darling.json</c>, which the health note and the search's messages name.</summary>
    public const string OptionName = "queryStoreTextTrigramIndexEnabled";

    /// <summary>The instance the service's worker writes and the health read and the search read.</summary>
    public static QueryTextTrigramIndexTracker SharedTracker { get; } = new();

    /// <summary>How long after the service is up the first pass waits when the option is on, so a build stays off the post-restart IO burst.</summary>
    public static readonly TimeSpan StartDelay = QueryStoreBackgroundIndexes.StartDelay;

    /// <summary>Time between passes.</summary>
    public static readonly TimeSpan PassInterval = QueryTextTrigramIndex.PassInterval;

    /// <summary>One catalog read: the table, the index, the extension and any build in progress on the table. No parameters.</summary>
    internal const string StateSql = $@"
SELECT to_regclass('{TableName}') IS NOT NULL AS table_exists,
       i.indisvalid,
       CASE WHEN i.indexrelid IS NULL THEN NULL ELSE pg_relation_size(i.indexrelid) END AS index_size_bytes,
       ext.nspname AS extension_schema,
       EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'pg_trgm') AS extension_available,
       EXISTS (SELECT 1 FROM pg_stat_progress_create_index AS p
               WHERE p.relid = to_regclass('{TableName}')
               AND   p.datid = (SELECT oid FROM pg_database WHERE datname = current_database())) AS build_running
FROM (SELECT 1) AS one
LEFT JOIN pg_index AS i ON i.indexrelid = to_regclass('{IndexName}')
LEFT JOIN (SELECT n.nspname FROM pg_extension AS e JOIN pg_namespace AS n ON n.oid = e.extnamespace WHERE e.extname = 'pg_trgm') AS ext ON true";

    internal const string DropSql = "DROP INDEX CONCURRENTLY IF EXISTS " + IndexName;

    /// <summary>The build, with the operator class named in the schema the extension lives in (it is not always on the search path).</summary>
    internal static string CreateIndexSql(string extensionSchema) =>
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS " + IndexBareName
        + " ON " + TableName + " USING gin (query_sql_text "
        + QueryTextTrigramIndex.QuoteIdentifier(extensionSchema) + ".gin_trgm_ops)";

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
            reader.GetBoolean(5));
    }

    /// <summary>
    /// One pass (see the class summary). Returns true when the pass is settled for good: the option is off and no index remains, so the
    /// loop can end (the option is read at start). Throws only on cancellation or on a failure to read the catalog; a failed build, drop or
    /// extension create is recorded in <paramref name="tracker"/> and logged.
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
        await using var connection = await postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
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
            if (state.IndexValid is null)
            {
                return true;
            }

            await DropAsync(connection, tracker, logger, "the option is off", cancellationToken).ConfigureAwait(false);
            return false;
        }

        if (state.IndexValid == true)
        {
            tracker.ClearExtensionBlock();
            tracker.Succeeded();
            return false;
        }

        var snapshot = tracker.Read();
        if (snapshot.NextAttemptUtc is { } next && utcNow < next)
        {
            logger.LogDebug("Query Store text trigram index build waits until {Next:o} after {Failures} failed attempt(s).", next, snapshot.Failures);
            return false;
        }

        var extensionSchema = state.ExtensionSchema;
        if (extensionSchema is null)
        {
            if (snapshot.ExtensionBlocked)
            {
                /* Not asked again: the statement was refused once, and a retry every pass would log and fail forever. The catalog read above
                   notices when an operator creates the extension. */
                return false;
            }

            try
            {
                await using var create = new NpgsqlCommand(QueryTextTrigramIndex.CreateExtensionSql, connection) { CommandTimeout = QueryStoreBackgroundIndexes.CatalogReadTimeoutSeconds };
                await create.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                extensionSchema = (await ReadStateAsync(connection, cancellationToken).ConfigureAwait(false)).ExtensionSchema;
            }
            catch (PostgresException ex)
            {
                /* The plan-cache index's pass may have created the extension at the same moment (two IF NOT EXISTS creates race on the
                   catalog's unique index), so the catalog decides: an extension that exists now is used. */
                extensionSchema = (await ReadStateAsync(connection, cancellationToken).ConfigureAwait(false)).ExtensionSchema;
                if (extensionSchema is null)
                {
                    tracker.BlockExtension(QueryTextTrigramIndex.ErrorText(ex));
                    logger.LogWarning(
                        "The trigram index on {Table} is waiting on the pg_trgm extension, which this login could not create ({SqlState}: {Message}). "
                        + "Run this in the store database as a role that may create it, and the next pass builds the index: {Statement}",
                        TableName, ex.SqlState, QueryTextTrigramIndex.ErrorText(ex), QueryTextTrigramIndex.OperatorStatement);
                    return false;
                }
            }
        }

        tracker.ClearExtensionBlock();
        if (extensionSchema is null)
        {
            return false;
        }

        if (state.IndexValid == false)
        {
            logger.LogWarning("Query Store text trigram index {Index} exists but is INVALID (an interrupted build); dropping it and rebuilding.", IndexName);
            await DropAsync(connection, tracker, logger, "an INVALID leftover", cancellationToken).ConfigureAwait(false);
        }

        tracker.BeginBuild(utcNow);
        var started = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            await using var build = new NpgsqlCommand(CreateIndexSql(extensionSchema), connection) { CommandTimeout = buildTimeoutSeconds };
            await build.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            tracker.Succeeded();
            logger.LogInformation("Query Store text trigram index {Index} built in {Seconds:F1}s (valid).", IndexName, started.Elapsed.TotalSeconds);
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
        NpgsqlConnection connection, QueryTextTrigramIndexTracker tracker, ILogger logger, string reason, CancellationToken cancellationToken)
    {
        tracker.BeginDrop();
        try
        {
            await using var drop = new NpgsqlCommand(DropSql, connection) { CommandTimeout = QueryStoreBackgroundIndexes.BuildTimeoutSeconds };
            await drop.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            logger.LogInformation("Query Store text trigram index {Index} dropped ({Reason}).", IndexName, reason);
        }
        finally
        {
            tracker.EndPhase();
        }
    }

    /* A failed build's INVALID index is still maintained by every write to the table, so it goes at once. A fresh connection: the failed
       one may be broken. Best effort: the next attempt drops a leftover first anyway. */
    private static async Task DropLeftoverAfterFailureAsync(NpgsqlDataSource postgres, ILogger logger, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var drop = new NpgsqlCommand(DropSql, connection) { CommandTimeout = QueryStoreBackgroundIndexes.BuildTimeoutSeconds };
            await drop.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogDebug("Dropping the failed Query Store text trigram index build's leftover did not finish ({Message}); the next attempt drops it first.", QueryTextTrigramIndex.ErrorText(ex));
        }
    }

    /// <summary>
    /// The background entry point: records the option, waits <paramref name="firstDelay"/>, then makes a pass every
    /// <paramref name="interval"/> until shutdown (or, with the option off, until no index remains). Never throws: a pass that fails is a
    /// Warning and the next interval tries again.
    /// </summary>
    public static async Task RunAsync(
        NpgsqlDataSource postgres, bool enabled, QueryTextTrigramIndexTracker tracker, ILogger logger,
        TimeSpan firstDelay, TimeSpan interval, CancellationToken cancellationToken)
    {
        tracker.Configure(enabled);
        try
        {
            await Task.Delay(firstDelay, cancellationToken).ConfigureAwait(false);
            while (true)
            {
                try
                {
                    if (await RunPassAsync(postgres, enabled, tracker, DateTime.UtcNow, logger, cancellationToken).ConfigureAwait(false))
                    {
                        return;
                    }
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                {
                    logger.LogWarning("Query Store text trigram index pass failed and is tried again in {Interval}: {ExceptionType}: {Message}", interval, ex.GetType().Name, QueryTextTrigramIndex.ErrorText(ex));
                }

                await Task.Delay(interval, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogDebug("Query Store text trigram index pass was cancelled at shutdown.");
        }
        catch (Exception ex)
        {
            logger.LogInformation("Query Store text trigram index pass stopped at shutdown: {ExceptionType}: {Message}", ex.GetType().Name, QueryTextTrigramIndex.ErrorText(ex));
        }
    }

    /// <summary>The status word for a catalog reading and the tracker's memory. Pure; the same rules as the plan-cache index's.</summary>
    public static TrigramIndexStatus Classify(TrigramCatalogState state, QueryTextTrigramIndexTracker.Snapshot snapshot) =>
        QueryTextTrigramIndex.Classify(state, snapshot);

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
            state.ExtensionSchema is not null,
            state.ExtensionAvailable,
            snapshot.Failures,
            snapshot.LastError ?? (status == TrigramIndexStatus.WaitingOnExtension ? snapshot.ExtensionError : null),
            snapshot.LastSqlState,
            snapshot.LastFailedUtc,
            snapshot.NextAttemptUtc,
            status == TrigramIndexStatus.WaitingOnExtension ? QueryTextTrigramIndex.OperatorStatement : null);
    }

    /// <summary>One sentence for the health block's note, per status.</summary>
    public static string Note(TrigramIndexHealth health)
    {
        ArgumentNullException.ThrowIfNull(health);
        var size = health.SizeBytes is { } bytes ? (bytes / 1048576.0).ToString("F1", CultureInfo.InvariantCulture) + " MB" : "no size";
        return health.Status switch
        {
            TrigramIndexStatus.Off => "The option " + OptionName + " is off and there is no trigram index on the Query Store text. find_queries_by_text reads one server's Query Store text in key order under a time budget.",
            TrigramIndexStatus.DropPending => "The option " + OptionName + " is off. The index still exists and the service drops it on its next pass.",
            TrigramIndexStatus.Dropping => "The option " + OptionName + " is off. The service is dropping the index now.",
            TrigramIndexStatus.Pending => "The option is on and the index does not exist yet. The service builds it in the background after startup, and an INVALID leftover is dropped first.",
            TrigramIndexStatus.Building => "A build is running. Collection is not waiting for it. The size is what has been written so far (" + size + ").",
            TrigramIndexStatus.Ready => "The trigram index on the Query Store text exists and is valid (" + size + "). find_queries_by_text searches a server's Query Store text through it.",
            TrigramIndexStatus.Unconfigured => "The service has not read darling.json yet, so the option " + OptionName + " is not known. Try again in a moment.",
            TrigramIndexStatus.Failed => "The last build failed. Its leftover was dropped and the service tries again at next_attempt_at, after a wait that doubles to a cap of "
                + ((int)QueryTextTrigramIndex.BackoffCap.TotalHours).ToString(CultureInfo.InvariantCulture) + " hours.",
            _ => health.ExtensionAvailable
                ? "The store login cannot create the pg_trgm extension. Run operator_statement in the store database as a role that may create it (a superuser, or the database owner). The next pass then builds the index. It is not retried until then."
                : "The server does not ship the pg_trgm extension. Install the PostgreSQL contrib package, then run operator_statement in the store database as a role that may create it.",
        };
    }
}
