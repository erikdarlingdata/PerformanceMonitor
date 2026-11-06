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

/// <summary>What <see cref="QueryTextTrigramIndex.ReadHealthAsync"/> says about the opt-in trigram index (#7674).</summary>
public enum TrigramIndexStatus
{
    /// <summary>The option is off and there is no index.</summary>
    Off,

    /// <summary>The option is off and the index still exists; the service drops it on its next pass.</summary>
    DropPending,

    /// <summary>The option is off and the service is dropping the index now.</summary>
    Dropping,

    /// <summary>The option is on and the index does not exist yet (no pass has built it, or a leftover waits to be rebuilt).</summary>
    Pending,

    /// <summary>A build is running.</summary>
    Building,

    /// <summary>The index exists and is valid.</summary>
    Ready,

    /// <summary>The last build failed. It is dropped and tried again after a growing wait.</summary>
    Failed,

    /// <summary>The store login cannot create <c>pg_trgm</c>. An operator runs the statement the health read names.</summary>
    WaitingOnExtension,
}

/// <summary>What one catalog read found about the table, the index and the extension.</summary>
/// <param name="TableExists">Whether <c>collect.query_text_dim</c> exists.</param>
/// <param name="IndexValid">Null when the index does not exist, otherwise <c>pg_index.indisvalid</c>.</param>
/// <param name="IndexSizeBytes">The index's size, null when it does not exist.</param>
/// <param name="ExtensionSchema">The schema <c>pg_trgm</c> is installed in, null when it is not installed.</param>
/// <param name="ExtensionAvailable">Whether the server ships <c>pg_trgm</c> at all.</param>
/// <param name="BuildRunning">Whether a backend this login can see is building an index on the table.</param>
public sealed record TrigramCatalogState(
    bool TableExists, bool? IndexValid, long? IndexSizeBytes, string? ExtensionSchema, bool ExtensionAvailable, bool BuildRunning);

/// <summary>The store health read's answer for the trigram index.</summary>
public sealed record TrigramIndexHealth(
    TrigramIndexStatus Status,
    bool Enabled,
    bool Exists,
    bool? Valid,
    long? SizeBytes,
    bool BuildRunning,
    DateTime? BuildStartedUtc,
    bool ExtensionInstalled,
    bool ExtensionAvailable,
    int FailedAttempts,
    string? LastError,
    string? LastSqlState,
    DateTime? LastFailedUtc,
    DateTime? NextAttemptUtc,
    string? OperatorStatement);

/// <summary>
/// What this process remembers about the trigram index between passes: whether the option is on, whether a build or a drop is
/// running, the last failure and its backoff, and whether <c>pg_trgm</c> could not be created. The catalog says what exists;
/// only this says why it does not. Thread-safe: the worker's background pass writes it and the MCP and web hosts, which run in
/// the same process, read it. Tests make their own instance; production uses <see cref="Shared"/>.
/// </summary>
public sealed class QueryTextTrigramIndexTracker
{
    /// <summary>The instance the service's worker writes and the health read reads.</summary>
    public static QueryTextTrigramIndexTracker Shared { get; } = new();

    /// <summary>What a build or drop in progress is, so the health read can say so.</summary>
    public enum PhaseKind { Idle, Building, Dropping }

    /// <summary>A consistent copy of the tracker's fields.</summary>
    public sealed record Snapshot(
        bool Enabled, PhaseKind Phase, DateTime? BuildStartedUtc, int Failures, string? LastError, string? LastSqlState,
        DateTime? LastFailedUtc, DateTime? NextAttemptUtc, bool ExtensionBlocked, string? ExtensionError);

    private readonly object _gate = new();
    private bool _enabled;
    private PhaseKind _phase;
    private DateTime? _buildStartedUtc;
    private int _failures;
    private string? _lastError;
    private string? _lastSqlState;
    private DateTime? _lastFailedUtc;
    private DateTime? _nextAttemptUtc;
    private bool _extensionBlocked;
    private string? _extensionError;
    private int _buildsStarted;

    /// <summary>How many builds this instance started. A test reads it to prove a pass built once and the next did not.</summary>
    public int BuildsStarted { get { lock (_gate) { return _buildsStarted; } } }

    /// <summary>Sets whether the option is on. Called once, before the first pass, so the health read never sees a half-set tracker.</summary>
    public void Configure(bool enabled)
    {
        lock (_gate) { _enabled = enabled; }
    }

    /// <summary>A copy of the fields.</summary>
    public Snapshot Read()
    {
        lock (_gate)
        {
            return new Snapshot(_enabled, _phase, _buildStartedUtc, _failures, _lastError, _lastSqlState, _lastFailedUtc,
                _nextAttemptUtc, _extensionBlocked, _extensionError);
        }
    }

    internal void BeginBuild(DateTime utcNow)
    {
        lock (_gate)
        {
            _phase = PhaseKind.Building;
            _buildStartedUtc = utcNow;
            _buildsStarted++;
        }
    }

    internal void BeginDrop()
    {
        lock (_gate) { _phase = PhaseKind.Dropping; }
    }

    internal void EndPhase()
    {
        lock (_gate)
        {
            _phase = PhaseKind.Idle;
            _buildStartedUtc = null;
        }
    }

    internal void Succeeded()
    {
        lock (_gate)
        {
            _failures = 0;
            _lastError = null;
            _lastSqlState = null;
            _lastFailedUtc = null;
            _nextAttemptUtc = null;
        }
    }

    internal void Failed(Exception ex, DateTime utcNow, TimeSpan wait)
    {
        lock (_gate)
        {
            _failures++;
            _lastError = QueryTextTrigramIndex.ErrorText(ex);
            _lastSqlState = (ex as PostgresException)?.SqlState;
            _lastFailedUtc = utcNow;
            _nextAttemptUtc = utcNow + wait;
        }
    }

    internal void BlockExtension(string error)
    {
        lock (_gate)
        {
            _extensionBlocked = true;
            _extensionError = error;
        }
    }

    internal void ClearExtensionBlock()
    {
        lock (_gate)
        {
            _extensionBlocked = false;
            _extensionError = null;
        }
    }
}

/// <summary>
/// The opt-in trigram index on the plan-cache text, <c>collect.query_text_dim.query_text</c> (#7674). Measured on a store the size of the
/// largest field store: 3.1 GB (52% of the table), a 16.6 minute build, about 0.5 ms added to each written row. So it is off by default,
/// it is never built by a migration rung (an upgrade must not wait that long), and turning the option off drops it.
///
/// <para><b>Why the Query Store text is not indexed.</b> Only the plan-cache text was measured. <c>collect.query_store_text</c> is read in
/// primary-key order with a row cap for one server, which stops early on a common pattern, and nothing measured shows it needs the index
/// for a whole-table search. Part B of #7674 decides that with a measurement, not a guess.</para>
///
/// <para><b>One pass.</b> <see cref="RunPassAsync"/> reads the catalog and does the one thing the option calls for: option off and index
/// present, drop it (<c>DROP INDEX CONCURRENTLY</c>); option on and index valid, nothing; option on and a build running in another
/// backend (a build of a service that was killed keeps running until PostgreSQL notices), nothing; option on and an INVALID leftover, drop it;
/// then create <c>pg_trgm</c> (once per process: a login that cannot is reported as <see cref="TrigramIndexStatus.WaitingOnExtension"/>
/// with the statement an operator runs, and is not asked again) and build with <c>CREATE INDEX CONCURRENTLY</c>. A failed build drops its
/// own INVALID leftover at once (a leftover is still maintained by every write) and waits <see cref="Backoff"/> before the next try.</para>
///
/// <para><b>Collection never waits.</b> <see cref="RunAsync"/> is a background task the worker starts after migrations and drains at shutdown, the
/// way the Query Store index ensures are. It uses its own connections and never holds a collector's.</para>
/// </summary>
public static class QueryTextTrigramIndex
{
    /// <summary>The table the index is on.</summary>
    public const string TableName = "collect.query_text_dim";

    /// <summary>The index's schema-qualified name.</summary>
    public const string IndexName = "collect.ix_query_text_dim_trgm";

    /// <summary>The statement an operator runs when the store login cannot create the extension.</summary>
    public const string OperatorStatement = "CREATE EXTENSION IF NOT EXISTS pg_trgm;";

    /// <summary>How long after the service is up the first pass waits when the option is on, so a build stays off the post-restart IO burst.</summary>
    public static readonly TimeSpan StartDelay = QueryStoreBackgroundIndexes.StartDelay;

    /// <summary>Time between passes.</summary>
    public static readonly TimeSpan PassInterval = TimeSpan.FromMinutes(30);

    /// <summary>The wait after the first failed build; it doubles with each failure in a row.</summary>
    public static readonly TimeSpan BackoffBase = TimeSpan.FromMinutes(30);

    /// <summary>The longest wait between builds.</summary>
    public static readonly TimeSpan BackoffCap = TimeSpan.FromHours(12);

    /// <summary>The wait before the next build after <paramref name="failures"/> failures in a row (at least 1).</summary>
    public static TimeSpan Backoff(int failures)
    {
        var steps = Math.Clamp(failures, 1, 30) - 1;
        var wait = TimeSpan.FromTicks(BackoffBase.Ticks * (1L << steps));
        return wait > BackoffCap || wait < TimeSpan.Zero ? BackoffCap : wait;
    }

    /// <summary>One catalog read: the table, the index, the extension and any build in progress. No parameters.</summary>
    internal const string StateSql = @"
SELECT to_regclass('collect.query_text_dim') IS NOT NULL AS table_exists,
       i.indisvalid,
       CASE WHEN i.indexrelid IS NULL THEN NULL ELSE pg_relation_size(i.indexrelid) END AS index_size_bytes,
       ext.nspname AS extension_schema,
       EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'pg_trgm') AS extension_available,
       EXISTS (SELECT 1 FROM pg_stat_progress_create_index AS p
               WHERE p.relid = to_regclass('collect.query_text_dim')
               AND   p.datid = (SELECT oid FROM pg_database WHERE datname = current_database())) AS build_running
FROM (SELECT 1) AS one
LEFT JOIN pg_index AS i ON i.indexrelid = to_regclass('collect.ix_query_text_dim_trgm')
LEFT JOIN (SELECT n.nspname FROM pg_extension AS e JOIN pg_namespace AS n ON n.oid = e.extnamespace WHERE e.extname = 'pg_trgm') AS ext ON true";

    internal const string CreateExtensionSql = "CREATE EXTENSION IF NOT EXISTS pg_trgm";

    internal const string DropSql = "DROP INDEX CONCURRENTLY IF EXISTS collect.ix_query_text_dim_trgm";

    /// <summary>The build, with the operator class named in the schema the extension lives in (it is not always on the search path).</summary>
    internal static string CreateIndexSql(string extensionSchema) =>
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_query_text_dim_trgm ON collect.query_text_dim USING gin (query_text "
        + QuoteIdentifier(extensionSchema) + ".gin_trgm_ops)";

    internal static string QuoteIdentifier(string name) => "\"" + name.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    internal static string ErrorText(Exception ex)
    {
        var text = ex is PostgresException pg ? pg.MessageText : ex.Message;
        return text.Length > 500 ? text[..500] : text;
    }

    /// <summary>Reads the catalog state on an open connection.</summary>
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
            logger.LogDebug("Trigram index pass skipped: {Table} does not exist yet.", TableName);
            return false;
        }

        if (state.BuildRunning)
        {
            /* An index build on the table is running in another backend (ours would not be, passes are sequential). Its INVALID
               index must not be dropped from under it. */
            logger.LogDebug("Trigram index pass skipped: an index build on {Table} is already running.", TableName);
            return false;
        }

        var snapshot = tracker.Read();
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
            tracker.Succeeded();
            tracker.ClearExtensionBlock();
            return false;
        }

        if (snapshot.NextAttemptUtc is { } next && utcNow < next)
        {
            logger.LogDebug("Trigram index build waits until {Next:o} after {Failures} failed attempt(s).", next, snapshot.Failures);
            return false;
        }

        var extensionSchema = state.ExtensionSchema;
        if (extensionSchema is null)
        {
            if (snapshot.ExtensionBlocked)
            {
                /* Not asked again: the statement was refused once, and a retry every pass would log and fail forever. The catalog read above
                   notices when an operator creates the extension, which clears the block. */
                return false;
            }

            try
            {
                await using var create = new NpgsqlCommand(CreateExtensionSql, connection) { CommandTimeout = QueryStoreBackgroundIndexes.CatalogReadTimeoutSeconds };
                await create.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                extensionSchema = (await ReadStateAsync(connection, cancellationToken).ConfigureAwait(false)).ExtensionSchema;
            }
            catch (PostgresException ex)
            {
                tracker.BlockExtension(ErrorText(ex));
                logger.LogWarning(
                    "The trigram index on {Table} is waiting on the pg_trgm extension, which this login could not create ({SqlState}: {Message}). "
                    + "Run this in the store database as a role that may create it, and the next pass builds the index: {Statement}",
                    TableName, ex.SqlState, ErrorText(ex), OperatorStatement);
                return false;
            }
        }

        tracker.ClearExtensionBlock();
        if (extensionSchema is null)
        {
            return false;
        }

        if (state.IndexValid == false)
        {
            logger.LogWarning("Trigram index {Index} exists but is INVALID (an interrupted build); dropping it and rebuilding.", IndexName);
            await DropAsync(connection, tracker, logger, "an INVALID leftover", cancellationToken).ConfigureAwait(false);
        }

        tracker.BeginBuild(utcNow);
        var started = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            await using var build = new NpgsqlCommand(CreateIndexSql(extensionSchema), connection) { CommandTimeout = buildTimeoutSeconds };
            await build.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            tracker.Succeeded();
            logger.LogInformation("Trigram index {Index} built in {Seconds:F1}s (valid).", IndexName, started.Elapsed.TotalSeconds);
        }
        catch (OperationCanceledException)
        {
            /* Shutdown: the leftover, if any, is dropped by the next start. */
            throw;
        }
        catch (Exception ex)
        {
            var wait = Backoff(tracker.Read().Failures + 1);
            tracker.Failed(ex, utcNow, wait);
            logger.LogWarning(
                "Trigram index {Index} build failed after {Seconds:F1}s and is tried again after {Wait}: {Message}",
                IndexName, started.Elapsed.TotalSeconds, wait, ErrorText(ex));
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
            logger.LogInformation("Trigram index {Index} dropped ({Reason}).", IndexName, reason);
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
            logger.LogDebug("Dropping the failed trigram index build's leftover did not finish ({Message}); the next attempt drops it first.", ErrorText(ex));
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
                    logger.LogWarning("Trigram index pass failed and is tried again in {Interval}: {ExceptionType}: {Message}", interval, ex.GetType().Name, ErrorText(ex));
                }

                await Task.Delay(interval, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogDebug("Trigram index pass was cancelled at shutdown.");
        }
        catch (Exception ex)
        {
            logger.LogInformation("Trigram index pass stopped at shutdown: {ExceptionType}: {Message}", ex.GetType().Name, ErrorText(ex));
        }
    }

    /// <summary>The status word for a catalog reading and the tracker's memory. Pure.</summary>
    public static TrigramIndexStatus Classify(TrigramCatalogState state, QueryTextTrigramIndexTracker.Snapshot snapshot)
    {
        var exists = state.IndexValid is not null;
        if (!snapshot.Enabled)
        {
            if (!exists)
            {
                return TrigramIndexStatus.Off;
            }

            return snapshot.Phase == QueryTextTrigramIndexTracker.PhaseKind.Dropping ? TrigramIndexStatus.Dropping : TrigramIndexStatus.DropPending;
        }

        if (snapshot.Phase == QueryTextTrigramIndexTracker.PhaseKind.Building || state.BuildRunning)
        {
            return TrigramIndexStatus.Building;
        }

        if (state.IndexValid == true)
        {
            return TrigramIndexStatus.Ready;
        }

        if (snapshot.ExtensionBlocked && state.ExtensionSchema is null)
        {
            return TrigramIndexStatus.WaitingOnExtension;
        }

        return snapshot.LastError is not null ? TrigramIndexStatus.Failed : TrigramIndexStatus.Pending;
    }

    /// <summary>The store health read for the trigram index: the catalog on <paramref name="connection"/> beside the tracker's memory.</summary>
    public static async Task<TrigramIndexHealth> ReadHealthAsync(
        NpgsqlConnection connection, QueryTextTrigramIndexTracker tracker, CancellationToken cancellationToken)
    {
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
            status == TrigramIndexStatus.WaitingOnExtension ? OperatorStatement : null);
    }

    /// <summary>One sentence for the health block's note, per status.</summary>
    public static string Note(TrigramIndexHealth health)
    {
        ArgumentNullException.ThrowIfNull(health);
        var size = health.SizeBytes is { } bytes ? (bytes / 1048576.0).ToString("F1", CultureInfo.InvariantCulture) + " MB" : "no size";
        return health.Status switch
        {
            TrigramIndexStatus.Off => "The option queryTextTrigramIndexEnabled is off and there is no trigram index on the plan-cache text.",
            TrigramIndexStatus.DropPending => "The option queryTextTrigramIndexEnabled is off. The index still exists and the service drops it on its next pass.",
            TrigramIndexStatus.Dropping => "The option queryTextTrigramIndexEnabled is off. The service is dropping the index now.",
            TrigramIndexStatus.Pending => "The option is on and the index does not exist yet. The service builds it in the background after startup, and an INVALID leftover is dropped first.",
            TrigramIndexStatus.Building => "A build is running. Collection is not waiting for it. The size is what has been written so far (" + size + ").",
            TrigramIndexStatus.Ready => "The trigram index exists and is valid (" + size + ").",
            TrigramIndexStatus.Failed => "The last build failed. Its leftover was dropped and the service tries again at next_attempt_at, after a wait that doubles to a cap of "
                + ((int)BackoffCap.TotalHours).ToString(CultureInfo.InvariantCulture) + " hours.",
            _ => health.ExtensionAvailable
                ? "The store login cannot create the pg_trgm extension. Run operator_statement in the store database as a role that may create it (a superuser, or the database owner). The next pass then builds the index. It is not retried until then."
                : "The server does not ship the pg_trgm extension. Install the PostgreSQL contrib package, then run operator_statement in the store database as a role that may create it.",
        };
    }
}
