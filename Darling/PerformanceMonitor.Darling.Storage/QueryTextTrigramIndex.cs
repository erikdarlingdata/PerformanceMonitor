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

    /// <summary>
    /// This process has not read the option yet (the worker is still loading <c>darling.json</c>), so it says nothing about the index.
    /// It is never reported as Off or DropPending: the option may well be on.
    /// </summary>
    Unconfigured,
}

/// <summary>What one catalog read found about the table, the index and the extension, and about the sample index (#7849).</summary>
/// <param name="TableExists">Whether <c>collect.query_text_dim</c> exists.</param>
/// <param name="IndexValid">Null when the index does not exist, otherwise <c>pg_index.indisvalid</c>.</param>
/// <param name="IndexSizeBytes">The index's size, null when it does not exist.</param>
/// <param name="ExtensionSchema">The schema <c>pg_trgm</c> is installed in, null when it is not installed.</param>
/// <param name="ExtensionAvailable">Whether the server ships <c>pg_trgm</c> at all.</param>
/// <param name="BuildRunning">Whether a backend this login can see is building an index on the table.</param>
/// <param name="SampleIndexValid">Null when the sample index on <c>collect.query_stats</c> does not exist, otherwise its <c>pg_index.indisvalid</c>.</param>
/// <param name="SampleIndexSizeBytes">The sample index's size over the table and its chunks, null when it does not exist.</param>
/// <param name="SampleBuildRunning">Whether a backend this login can see is building an index on <c>collect.query_stats</c> or one of its chunks.</param>
/// <param name="KeyExtensionSchema">#8570, the Query Store text index only: the schema <c>btree_gin</c> is installed in (the operator class of its
/// server key), null when it is not installed.</param>
/// <param name="ReplacedIndexExists">#8570, the Query Store text index only: whether the index it replaces, on the text alone, still exists.</param>
public sealed record TrigramCatalogState(
    bool TableExists, bool? IndexValid, long? IndexSizeBytes, string? ExtensionSchema, bool ExtensionAvailable, bool BuildRunning,
    bool? SampleIndexValid = null, long? SampleIndexSizeBytes = null, bool SampleBuildRunning = false,
    string? KeyExtensionSchema = null, bool ReplacedIndexExists = false);

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
    string? OperatorStatement,
    bool SampleIndexExists = false,
    bool? SampleIndexValid = null,
    long? SampleIndexSizeBytes = null,
    string? OptionSource = null,
    bool ReplacedIndexExists = false);

/// <summary>
/// What this process remembers about the trigram index between passes: whether the option is on, whether a build or a drop is
/// running, the last failure and its backoff, and whether <c>pg_trgm</c> could not be created. The catalog says what exists;
/// only this says why it does not. Thread-safe: the worker's background pass writes it and the MCP and web hosts, which run in
/// the same process, read it. Tests make their own instance; production uses <see cref="Shared"/> for the plan-cache index and
/// <see cref="QueryStoreTextTrigramIndex.SharedTracker"/> for the Query Store text index.
/// </summary>
public sealed class QueryTextTrigramIndexTracker
{
    /// <summary>The instance the service's worker writes and the health read reads.</summary>
    public static QueryTextTrigramIndexTracker Shared { get; } = new();

    /// <summary>What a build or drop in progress is, so the health read can say so.</summary>
    public enum PhaseKind { Idle, Building, Dropping }

    /// <summary>A consistent copy of the tracker's fields.</summary>
    public sealed record Snapshot(
        bool Configured, bool Enabled, PhaseKind Phase, DateTime? BuildStartedUtc, int Failures, string? LastError, string? LastSqlState,
        DateTime? LastFailedUtc, DateTime? NextAttemptUtc, bool ExtensionBlocked, string? ExtensionError, string? OptionSource = null);

    /// <summary>#8978: the option's value came from <c>darling.json</c> (the store column is NULL, or the store is not read yet).</summary>
    public const string SourceFile = "file";

    /// <summary>#8978: the option's value came from the store's <c>config.config_service</c> column, which overrides the file.</summary>
    public const string SourceStore = "store";

    private readonly object _gate = new();
    private bool _configured;
    private bool _enabled;
    private bool _resolved;
    private string? _optionSource;
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
    private int _sampleBuildsStarted;

    /// <summary>How many trigram index builds this instance started. A test reads it to prove a pass built once and the next did not.</summary>
    public int BuildsStarted { get { lock (_gate) { return _buildsStarted; } } }

    /// <summary>How many sample index builds (#7849, <see cref="QueryTextTrigramIndex.SampleIndexName"/>) this instance started.</summary>
    public int SampleBuildsStarted { get { lock (_gate) { return _sampleBuildsStarted; } } }

    /// <summary>
    /// Sets whether the option is on, as resolved: the store's value when its column holds one, else <c>darling.json</c>'s (#8978). The
    /// pass calls it at the start of every pass with the value it acts on, and the worker calls it with <paramref name="optionSource"/>
    /// the moment it loads <c>darling.json</c> (before it migrates the store) and each time it applies the store's config, so the health
    /// read carries the option from process start and follows a store write before the next pass. Once called, a later
    /// <see cref="ConfigureFromFile"/> is ignored. Until either is called the tracker is unconfigured and
    /// <see cref="QueryTextTrigramIndex.Classify"/> reports <see cref="TrigramIndexStatus.Unconfigured"/>, never Off or DropPending.
    /// </summary>
    public void Configure(bool enabled, string? optionSource = null)
    {
        lock (_gate)
        {
            _configured = true;
            _enabled = enabled;
            _resolved = true;
            _optionSource = optionSource ?? _optionSource;
        }
    }

    /// <summary>
    /// Sets the option from <c>darling.json</c> alone, for a host that loads the file on its own: the MCP and web hosts call it from
    /// their own loads, so get_store_metrics reads the real option even when such a host loads before the worker does. Ignored once
    /// <see cref="Configure"/> has set the value, so a host that loads the file later (the MCP host started by the store's mcp_enabled)
    /// cannot put back a value the store has overridden.
    /// </summary>
    public void ConfigureFromFile(bool enabled)
    {
        lock (_gate)
        {
            if (_resolved)
            {
                return;
            }

            _configured = true;
            _enabled = enabled;
            _optionSource = SourceFile;
        }
    }

    /// <summary>A copy of the fields.</summary>
    public Snapshot Read()
    {
        lock (_gate)
        {
            return new Snapshot(_configured, _enabled, _phase, _buildStartedUtc, _failures, _lastError, _lastSqlState, _lastFailedUtc,
                _nextAttemptUtc, _extensionBlocked, _extensionError, _optionSource);
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

    internal void BeginSampleBuild(DateTime utcNow)
    {
        lock (_gate)
        {
            _phase = PhaseKind.Building;
            _buildStartedUtc = utcNow;
            _sampleBuildsStarted++;
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
/// <para><b>The Query Store text has its own index and option.</b> <c>collect.query_store_text</c> is indexed by
/// <see cref="QueryStoreTextTrigramIndex"/> under its own option, with its own tracker, so either index can be on without the other.</para>
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
///
/// <para><b>#7849: the sample index rides on the same option, and the search does not wait for it.</b> The trigram index finds the matching
/// texts; the all-servers search then has to find where those texts ran, which is a read of <c>collect.query_stats</c> by <c>query_text_digest</c>, and
/// no index a migration ships has that column as a key. The search reads the matched digests first and binds them to the sample read as an array
/// (at most 1,000 of them; past that it joins the window to the matches, which is the right plan for a pattern most texts contain), so it is served
/// as soon as the trigram index is valid. The same pass then owns a second index, <see cref="SampleIndexName"/> on <c>collect.query_stats
/// (query_text_digest, collection_time DESC) WHERE query_text_digest IS NOT NULL</c>, built after the trigram index is valid and dropped with it when
/// the option is off, which turns the probe's filtered scan of an uncompressed chunk into an index scan. <c>Ready</c> is the trigram index valid; the
/// health read carries the sample index's own state beside it. Measured on TimescaleDB 2.30.1 (40 servers, 3.0 M rows over four days, 300,000 texts,
/// the older half of the window compressed the way the product runs it): a pattern matching one text took 0.73 s cold through the join, 0.33 s
/// through the probe with no sample index, and 0.11 s with it, where the uncompressed chunks cost 0.02 ms each and the compressed chunks' decompression
/// is what remains; a pattern a third of the texts contain stays at about 2 s through the join, with or without the index. A compressed chunk keeps no
/// rows in the index, and the probe applies the digest's per-batch min/max metadata there instead, which skips nothing when the batch spans the digest's
/// whole range, so each compressed chunk is still decompressed once per read.
/// <c>query_stats</c> is a hypertable on a TimescaleDB store, which refuses <c>CREATE INDEX CONCURRENTLY</c> and <c>DROP INDEX CONCURRENTLY</c>,
/// so there the build is <c>WITH (timescaledb.transaction_per_chunk)</c>, which takes a ShareLock on one chunk at a time for that chunk's scan
/// and none on the root (measured on TimescaleDB 2.30.1: an insert into the newest chunk during the build of an older one waited 58 ms, against
/// 0.7 s behind a plain build of the same index), and the drop is a plain <c>DROP INDEX</c>, which is a catalog change (14 ms on the seeded store).
/// A build that stops part way leaves the parent INVALID, which the next pass drops and rebuilds, as it does a trigram leftover. On a plain PostgreSQL
/// store both are <c>CONCURRENTLY</c>, as the trigram index's are.</para>
/// </summary>
public static class QueryTextTrigramIndex
{
    /// <summary>The table the index is on.</summary>
    public const string TableName = "collect.query_text_dim";

    /// <summary>The index's schema-qualified name.</summary>
    public const string IndexName = "collect.ix_query_text_dim_trgm";

    /// <summary>#7849: the table the sample index is on.</summary>
    public const string SampleTableName = "collect.query_stats";

    /// <summary>#7849: the sample index's bare name, which a hypertable's chunk indexes carry as their suffix.</summary>
    public const string SampleIndexBareName = "ix_query_stats_text_digest";

    /// <summary>#7849: the sample index's schema-qualified name.</summary>
    public const string SampleIndexName = "collect." + SampleIndexBareName;

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

    /// <summary>One catalog read: the table, the index, the extension and any build in progress, and the sample index (#7849). The sample index's
    /// size sums the parent and the chunk indexes (a hypertable's parent index is an 8 KB shell), and a build on any chunk counts as a build on the
    /// table, because a per-chunk build reports the chunk's relid. No parameters.</summary>
    internal const string StateSql = $@"
SELECT to_regclass('{TableName}') IS NOT NULL AS table_exists,
       i.indisvalid,
       CASE WHEN i.indexrelid IS NULL THEN NULL ELSE pg_relation_size(i.indexrelid) END AS index_size_bytes,
       ext.nspname AS extension_schema,
       EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'pg_trgm') AS extension_available,
       EXISTS (SELECT 1 FROM pg_stat_progress_create_index AS p
               WHERE p.relid = to_regclass('{TableName}')
               AND   p.datid = (SELECT oid FROM pg_database WHERE datname = current_database())) AS build_running,
       si.indisvalid AS sample_index_valid,
       CASE WHEN si.indexrelid IS NULL THEN NULL
            ELSE pg_relation_size(si.indexrelid)
                 + COALESCE((SELECT sum(pg_relation_size(ci.indexrelid))
                             FROM pg_index AS ci
                             JOIN pg_class AS cc ON cc.oid = ci.indexrelid
                             WHERE ci.indrelid IN (SELECT h.inhrelid FROM pg_inherits AS h WHERE h.inhparent = to_regclass('{SampleTableName}'))
                             AND   cc.relname LIKE '%\_{SampleIndexBareName}'), 0)
       END AS sample_index_size_bytes,
       EXISTS (SELECT 1 FROM pg_stat_progress_create_index AS p
               WHERE (p.relid = to_regclass('{SampleTableName}')
                      OR p.relid IN (SELECT h.inhrelid FROM pg_inherits AS h WHERE h.inhparent = to_regclass('{SampleTableName}')))
               AND   p.datid = (SELECT oid FROM pg_database WHERE datname = current_database())) AS sample_build_running
FROM (SELECT 1) AS one
LEFT JOIN pg_index AS i ON i.indexrelid = to_regclass('{IndexName}')
LEFT JOIN pg_index AS si ON si.indexrelid = to_regclass('{SampleIndexName}')
LEFT JOIN (SELECT n.nspname FROM pg_extension AS e JOIN pg_namespace AS n ON n.oid = e.extnamespace WHERE e.extname = 'pg_trgm') AS ext ON true";

    internal const string CreateExtensionSql = "CREATE EXTENSION IF NOT EXISTS pg_trgm";

    internal const string DropSql = "DROP INDEX CONCURRENTLY IF EXISTS " + IndexName;

    /// <summary>#7849: the sample index's build. On a hypertable the per-chunk form, because TimescaleDB refuses <c>CONCURRENTLY</c> there and a plain
    /// build holds a ShareLock on the root and every chunk for its whole length; on a plain table <c>CONCURRENTLY</c>, as the trigram index's build is.
    /// Partial on <c>query_text_digest IS NOT NULL</c>: the join's equality is strict, so the planner proves the predicate from it, and a row with no
    /// digest can never be a hit.</summary>
    internal static string CreateSampleIndexSql(bool hypertable) =>
        "CREATE INDEX " + (hypertable ? "" : "CONCURRENTLY ") + "IF NOT EXISTS " + SampleIndexBareName
        + " ON " + SampleTableName + " (query_text_digest, collection_time DESC)"
        + (hypertable ? " WITH (timescaledb.transaction_per_chunk)" : "")
        + " WHERE query_text_digest IS NOT NULL";

    /// <summary>#7849: the sample index's drop. A hypertable index refuses <c>DROP INDEX CONCURRENTLY</c> (it is several objects), so there the drop is
    /// plain, which is a catalog change (14 ms on a seeded large store).</summary>
    internal static string DropSampleIndexSql(bool hypertable) =>
        "DROP INDEX " + (hypertable ? "" : "CONCURRENTLY ") + "IF EXISTS " + SampleIndexName;

    /// <summary>The build, with the operator class named in the schema the extension lives in (it is not always on the search path).</summary>
    internal static string CreateIndexSql(string extensionSchema) =>
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS " + IndexName[(IndexName.IndexOf('.', StringComparison.Ordinal) + 1)..]
        + " ON " + TableName + " USING gin (query_text "
        + QuoteIdentifier(extensionSchema) + ".gin_trgm_ops) WITH (gin_pending_list_limit = " + PendingListLimitKb + ")";

    /// <summary>The index's <c>gin_pending_list_limit</c> in kB (64 MB, the option's own unit), against the server default of 4 MB.
    /// Every row of a collector batch adds its text's trigrams to the index's pending list, and the insert that takes the list past
    /// the limit flushes the whole list inside its own call. A 4 MB list is reached every few dozen calls and each flush reads
    /// a share of the index; 64 MB makes the flush rare, and the <see cref="CleanInterval"/> clean empties the list long
    /// before it can reach that size, so the insert rarely pays for one.</summary>
    internal const int PendingListLimitKb = 65536;

    /// <summary>The time between cleans of the pending list. The list grows with the collector's calls, and the clean must run often enough
    /// that a store far busier than the largest field store still never fills it between cleans.
    /// <para>#9460 measured a longer interval and found 2 minutes is the longest safe one. At 60,000 new distinct texts an hour into one table
    /// (texts of about 1.4 KB with about 460 distinct trigrams, so about 2 pending pages a row) the list peaked at about 4,000 pages between
    /// cleans every 2 minutes, just under half of the 8,192-page limit, and no insert merged it. Cleaning every 5 or 10 minutes let it reach the
    /// limit: the inserts then merged it inline, the stall #9303 fixed. A longer interval
    /// would save WAL only by moving the merge into the collection insert. <see cref="QueryStoreTextTrigramIndex.CleanInterval"/> stays the same.</para></summary>
    public static readonly TimeSpan CleanInterval = TimeSpan.FromMinutes(2);

    /// <summary>#9303: the wait before the first clean after a start. Zero: the clean does not wait for the build's <see cref="StartDelay"/>,
    /// because it only works on an index that exists, and the first inserts after a restart are the ones most likely to take a cold pending
    /// list past its limit and merge it inside their own statement.</summary>
    internal static readonly TimeSpan CleanFirstDelay = TimeSpan.Zero;

    /// <summary>#8570: a clean statement that takes this long or longer is logged at Information, so a slow clean shows in the service log by default.</summary>
    internal static readonly TimeSpan SlowCleanLogThreshold = TimeSpan.FromSeconds(10);

    /// <summary>How long the limit change waits for its lock. <c>ALTER INDEX ... SET (gin_pending_list_limit)</c> takes <c>ACCESS EXCLUSIVE</c> on the
    /// index, so while a long search holds the index it queues, and every insert into the table queues behind it.</summary>
    internal const string LockTimeout = "2s";

    /// <summary>One row, when <paramref name="indexName"/> exists: whether it is valid, and whether it already carries
    /// <see cref="PendingListLimitKb"/>. The name is one of this assembly's own index names (this index, and the two the Query Store text
    /// index manages), never outside input.</summary>
    internal static string PendingListStateSql(string indexName) =>
        "SELECT i.indisvalid, coalesce('gin_pending_list_limit=" + PendingListLimitKb + "' = ANY (c.reloptions), false) "
        + "FROM pg_index AS i JOIN pg_class AS c ON c.oid = i.indexrelid WHERE i.indexrelid = to_regclass('" + indexName + "')";

    /// <summary>Sets the limit on an index that does not carry it. Run in its own transaction under <see cref="LockTimeout"/>.</summary>
    internal static string SetPendingListLimitSql(string indexName) =>
        "ALTER INDEX IF EXISTS " + indexName + " SET (gin_pending_list_limit = " + PendingListLimitKb + ")";

    /// <summary>Empties the pending list, so no insert has to.</summary>
    internal static string CleanPendingListSql(string indexName) => "SELECT gin_clean_pending_list(to_regclass('" + indexName + "'))";

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
            reader.GetBoolean(5),
            reader.IsDBNull(6) ? null : reader.GetBoolean(6),
            reader.IsDBNull(7) ? null : reader.GetInt64(7),
            reader.GetBoolean(8));
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
            if (state.IndexValid is null && state.SampleIndexValid is null)
            {
                return true;
            }

            if (state.IndexValid is not null)
            {
                await DropAsync(connection, tracker, logger, "the option is off", cancellationToken).ConfigureAwait(false);
            }

            if (state.SampleIndexValid is not null && !state.SampleBuildRunning)
            {
                await DropSampleAsync(connection, tracker, logger, "the option is off", cancellationToken).ConfigureAwait(false);
            }

            return false;
        }

        if (state.IndexValid == true)
        {
            tracker.ClearExtensionBlock();
            await EnsureSampleIndexAsync(postgres, connection, state, tracker, utcNow, logger, buildTimeoutSeconds, cancellationToken).ConfigureAwait(false);
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
        var built = false;
        try
        {
            await using var build = new NpgsqlCommand(CreateIndexSql(extensionSchema), connection) { CommandTimeout = buildTimeoutSeconds };
            await build.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            tracker.Succeeded();
            built = true;
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

        if (built)
        {
            /* The same pass goes on to the sample index, so one pass takes a fresh store from nothing to Ready. */
            state = await ReadStateAsync(connection, cancellationToken).ConfigureAwait(false);
            await EnsureSampleIndexAsync(postgres, connection, state, tracker, utcNow, logger, buildTimeoutSeconds, cancellationToken).ConfigureAwait(false);
        }

        return false;
    }

    /// <summary>
    /// #7849: the sample index on <see cref="SampleTableName"/>, once the trigram index is valid. Valid, nothing (and the tracker's failure record is
    /// cleared, so the health read says <c>Ready</c>); a build running in another backend, nothing; inside the wait after a failed build, nothing; an
    /// INVALID leftover (a per-chunk build that stopped part way, or an interrupted <c>CONCURRENTLY</c> build), dropped first; then the build, with the
    /// form the table calls for (<see cref="CreateSampleIndexSql"/>). A failed build is recorded on the tracker with the same backoff as a failed
    /// trigram build, and its leftover is dropped at once.
    /// </summary>
    private static async Task EnsureSampleIndexAsync(
        NpgsqlDataSource postgres, NpgsqlConnection connection, TrigramCatalogState state, QueryTextTrigramIndexTracker tracker, DateTime utcNow,
        ILogger logger, int buildTimeoutSeconds, CancellationToken cancellationToken)
    {
        if (state.SampleIndexValid == true)
        {
            tracker.Succeeded();
            return;
        }

        if (state.SampleBuildRunning)
        {
            logger.LogDebug("Sample index pass skipped: an index build on {Table} is already running.", SampleTableName);
            return;
        }

        var snapshot = tracker.Read();
        if (snapshot.NextAttemptUtc is { } next && utcNow < next)
        {
            logger.LogDebug("Sample index build waits until {Next:o} after {Failures} failed attempt(s).", next, snapshot.Failures);
            return;
        }

        var hypertable = await QueryStoreBackgroundIndexes.IsHypertableAsync(connection, SampleTableName, cancellationToken).ConfigureAwait(false);
        if (state.SampleIndexValid == false)
        {
            logger.LogWarning("Sample index {Index} exists but is INVALID (an interrupted build); dropping it and rebuilding.", SampleIndexName);
            await DropSampleAsync(connection, tracker, logger, "an INVALID leftover", cancellationToken, hypertable).ConfigureAwait(false);
        }

        tracker.BeginSampleBuild(utcNow);
        var started = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            await using var build = new NpgsqlCommand(CreateSampleIndexSql(hypertable), connection) { CommandTimeout = buildTimeoutSeconds };
            await build.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            tracker.Succeeded();
            logger.LogInformation("Sample index {Index} built in {Seconds:F1}s (valid).", SampleIndexName, started.Elapsed.TotalSeconds);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            var wait = Backoff(tracker.Read().Failures + 1);
            tracker.Failed(ex, utcNow, wait);
            logger.LogWarning(
                "Sample index {Index} build failed after {Seconds:F1}s and is tried again after {Wait}: {Message}",
                SampleIndexName, started.Elapsed.TotalSeconds, wait, ErrorText(ex));
            await DropSampleLeftoverAfterFailureAsync(postgres, logger, hypertable, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            tracker.EndPhase();
        }
    }

    private static async Task DropSampleAsync(
        NpgsqlConnection connection, QueryTextTrigramIndexTracker tracker, ILogger logger, string reason, CancellationToken cancellationToken, bool? hypertable = null)
    {
        hypertable ??= await QueryStoreBackgroundIndexes.IsHypertableAsync(connection, SampleTableName, cancellationToken).ConfigureAwait(false);
        tracker.BeginDrop();
        try
        {
            await using var drop = new NpgsqlCommand(DropSampleIndexSql(hypertable.Value), connection) { CommandTimeout = QueryStoreBackgroundIndexes.BuildTimeoutSeconds };
            await drop.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            logger.LogInformation("Sample index {Index} dropped ({Reason}).", SampleIndexName, reason);
        }
        finally
        {
            tracker.EndPhase();
        }
    }

    private static async Task DropSampleLeftoverAfterFailureAsync(NpgsqlDataSource postgres, ILogger logger, bool hypertable, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(StoreSessionNames.TrigramIndex, cancellationToken).ConfigureAwait(false);
            await using var drop = new NpgsqlCommand(DropSampleIndexSql(hypertable), connection) { CommandTimeout = QueryStoreBackgroundIndexes.BuildTimeoutSeconds };
            await drop.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogDebug("Dropping the failed sample index build's leftover did not finish ({Message}); the next attempt drops it first.", ErrorText(ex));
        }
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
            await using var connection = await postgres.OpenConnectionAsync(StoreSessionNames.TrigramIndex, cancellationToken).ConfigureAwait(false);
            await using var drop = new NpgsqlCommand(DropSql, connection) { CommandTimeout = QueryStoreBackgroundIndexes.BuildTimeoutSeconds };
            await drop.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogDebug("Dropping the failed trigram index build's leftover did not finish ({Message}); the next attempt drops it first.", ErrorText(ex));
        }
    }

    /// <summary>
    /// The background entry point: makes a pass every <paramref name="interval"/> until shutdown, after <paramref name="firstDelay"/>.
    /// <paramref name="enabled"/> is the option's live, resolved value (#8978: the store's <c>config.config_service</c> column when it holds
    /// one, else <c>darling.json</c>), read at the start of every pass, so a store write takes effect at the next pass with no restart. With
    /// the option off and no index the pass is one catalog read. Never throws: a pass that fails is a Warning and the next interval tries again.
    /// </summary>
    public static Task RunAsync(
        NpgsqlDataSource postgres, Func<bool> enabled, QueryTextTrigramIndexTracker tracker, ILogger logger,
        TimeSpan firstDelay, TimeSpan interval, CancellationToken cancellationToken) =>
        RunLoopsAsync(
            enabled, tracker, logger, "Trigram index", firstDelay, interval, CleanInterval,
            (on, ct) => RunPassAsync(postgres, on, tracker, DateTime.UtcNow, logger, ct),
            ct => CleanPendingListAsync(postgres, enabled, logger, ct),
            static (wait, ct) => Task.Delay(wait, ct),
            cancellationToken);

    /// <summary>
    /// #9303: the two loops under both indexes' <c>RunAsync</c>, with the pass, the clean and the wait passed in so a test steps them without a
    /// store or a clock. The build pass waits <paramref name="firstDelay"/>, which keeps a whole-table build off the post-restart IO burst. The
    /// clean starts at once (<see cref="CleanFirstDelay"/>): it only touches an index that already exists, and it is what keeps a collection
    /// insert from merging the pending list inside its own statement, which is most likely right after a restart while the list is cold.
    /// </summary>
    internal static Task RunLoopsAsync(
        Func<bool> enabled, QueryTextTrigramIndexTracker tracker, ILogger logger, string indexLabel, TimeSpan firstDelay, TimeSpan interval,
        TimeSpan cleanInterval, Func<bool, CancellationToken, Task<bool>> pass, Func<CancellationToken, Task> clean,
        Func<TimeSpan, CancellationToken, Task> delay, CancellationToken cancellationToken) =>
        Task.WhenAll(
            RunLoopAsync(enabled, tracker, logger, indexLabel, firstDelay, interval, pass, delay, cancellationToken),
            RunCleanLoopAsync(clean, logger, indexLabel, CleanFirstDelay, cleanInterval, delay, cancellationToken));

    /// <summary>
    /// #9044: the clean loop under both indexes' <c>RunAsync</c>. Runs <paramref name="clean"/> every <paramref name="interval"/> (each index's
    /// own <c>CleanInterval</c>) until shutdown, after <paramref name="firstDelay"/>, apart from the build pass, whose retries and backoff stay
    /// on <see cref="PassInterval"/>. Never throws: <paramref name="clean"/> throws only on cancellation.
    /// </summary>
    internal static async Task RunCleanLoopAsync(
        Func<CancellationToken, Task> clean, ILogger logger, string indexLabel, TimeSpan firstDelay, TimeSpan interval,
        Func<TimeSpan, CancellationToken, Task> delay, CancellationToken cancellationToken)
    {
        try
        {
            await delay(firstDelay, cancellationToken).ConfigureAwait(false);
            while (true)
            {
                await clean(cancellationToken).ConfigureAwait(false);
                await delay(interval, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogDebug("{Index} clean loop was cancelled at shutdown.", indexLabel);
        }
    }

    /// <summary>#9044: one clean of this index (see the overload that names the index).</summary>
    internal static Task CleanPendingListAsync(NpgsqlDataSource postgres, Func<bool> enabled, ILogger logger, CancellationToken cancellationToken) =>
        CleanPendingListAsync(postgres, enabled, IndexName, logger, cancellationToken);

    /// <summary>
    /// #9044: one clean of <paramref name="indexName"/>, when the option is on and the index exists and is valid. An index that does not carry
    /// <see cref="PendingListLimitKb"/> first gets it, once, in its own transaction under <see cref="LockTimeout"/>: a lock that is not free in
    /// time is a quiet skip that the next clean retries (the clean itself takes no lock an insert or a search waits on). Best effort: the index
    /// works the same without either, so any other failure is a Warning and the next clean tries again. Throws only on cancellation.
    /// </summary>
    internal static async Task CleanPendingListAsync(
        NpgsqlDataSource postgres, Func<bool> enabled, string indexName, ILogger logger, CancellationToken cancellationToken)
    {
        try
        {
            if (!enabled())
            {
                return;
            }

            var total = System.Diagnostics.Stopwatch.StartNew();
            await using var connection = await postgres.OpenConnectionAsync(StoreSessionNames.TrigramIndex, cancellationToken).ConfigureAwait(false);
            var openSeconds = total.Elapsed.TotalSeconds;
            bool limitSet;
            await using (var read = new NpgsqlCommand(PendingListStateSql(indexName), connection) { CommandTimeout = QueryStoreBackgroundIndexes.CatalogReadTimeoutSeconds })
            await using (var reader = await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false) || !reader.GetBoolean(0))
                {
                    return;
                }

                limitSet = reader.GetBoolean(1);
            }

            if (!limitSet)
            {
                await SetPendingListLimitAsync(connection, indexName, logger, cancellationToken).ConfigureAwait(false);
            }

            await using var clean = new NpgsqlCommand(CleanPendingListSql(indexName), connection) { CommandTimeout = QueryStoreBackgroundIndexes.BuildTimeoutSeconds };
            var cleanTimer = System.Diagnostics.Stopwatch.StartNew();
            var result = await clean.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            cleanTimer.Stop();
            var pages = result is null or DBNull ? 0L : Convert.ToInt64(result, System.Globalization.CultureInfo.InvariantCulture);

            /* #8570: the clean's result and timing, so the log says which index a clean worked on and how long it took. */
            if (cleanTimer.Elapsed >= SlowCleanLogThreshold)
            {
                logger.LogInformation(
                    "Cleaned the pending list of trigram index {Index}: {Pages} pages in {CleanSeconds:F1}s ({TotalSeconds:F1}s from start, {OpenSeconds:F1}s waiting for a connection).",
                    indexName, pages, cleanTimer.Elapsed.TotalSeconds, total.Elapsed.TotalSeconds, openSeconds);
            }
            else
            {
                logger.LogDebug(
                    "Cleaned the pending list of trigram index {Index}: {Pages} pages in {CleanSeconds:F1}s ({TotalSeconds:F1}s from start, {OpenSeconds:F1}s waiting for a connection).",
                    indexName, pages, cleanTimer.Elapsed.TotalSeconds, total.Elapsed.TotalSeconds, openSeconds);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogWarning("Could not clean the pending list of trigram index {Index}: {Message}", indexName, ErrorText(ex));
        }
    }

    private static async Task SetPendingListLimitAsync(NpgsqlConnection connection, string indexName, ILogger logger, CancellationToken cancellationToken)
    {
        try
        {
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await using (var timeout = new NpgsqlCommand("SET LOCAL lock_timeout = '" + LockTimeout + "'", connection, transaction) { CommandTimeout = QueryStoreBackgroundIndexes.CatalogReadTimeoutSeconds })
            {
                await timeout.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await using (var set = new NpgsqlCommand(SetPendingListLimitSql(indexName), connection, transaction) { CommandTimeout = QueryStoreBackgroundIndexes.CatalogReadTimeoutSeconds })
            {
                await set.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.LockNotAvailable)
        {
            logger.LogDebug("Setting the pending list limit on trigram index {Index} waited {Timeout} for its lock and is skipped; the next clean tries again.", indexName, LockTimeout);
        }
    }

    /// <summary>
    /// #8978: the loop under both indexes' <c>RunAsync</c>, with the pass and the wait passed in so a test steps it without a store or a clock.
    /// Reads <paramref name="enabled"/> once before the first wait and again at the start of every pass, sets <paramref name="tracker"/> to the
    /// value each pass acts on, and runs until <paramref name="cancellationToken"/> is cancelled, whatever a pass returns.
    /// </summary>
    internal static async Task RunLoopAsync(
        Func<bool> enabled, QueryTextTrigramIndexTracker tracker, ILogger logger, string indexLabel, TimeSpan firstDelay, TimeSpan interval,
        Func<bool, CancellationToken, Task<bool>> pass, Func<TimeSpan, CancellationToken, Task> delay, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(enabled);
        try
        {
            bool? last = null;
            tracker.Configure(enabled());
            await delay(firstDelay, cancellationToken).ConfigureAwait(false);
            while (true)
            {
                try
                {
                    var on = enabled();
                    tracker.Configure(on);
                    if (last is { } previous && previous != on)
                    {
                        logger.LogInformation("{Index} option is now {State}; this pass acts on it.", indexLabel, on ? "on" : "off");
                    }

                    last = on;
                    await pass(on, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                {
                    logger.LogWarning("{Index} pass failed and is tried again in {Interval}: {ExceptionType}: {Message}", indexLabel, interval, ex.GetType().Name, ErrorText(ex));
                }

                await delay(interval, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogDebug("{Index} pass was cancelled at shutdown.", indexLabel);
        }
        catch (Exception ex)
        {
            logger.LogInformation("{Index} pass stopped at shutdown: {ExceptionType}: {Message}", indexLabel, ex.GetType().Name, ErrorText(ex));
        }
    }

    /// <summary>The status word for a catalog reading and the tracker's memory. Pure.</summary>
    public static TrigramIndexStatus Classify(TrigramCatalogState state, QueryTextTrigramIndexTracker.Snapshot snapshot)
    {
        if (!snapshot.Configured)
        {
            return TrigramIndexStatus.Unconfigured;
        }

        /* #7849: the sample index is part of the same state, and #8570 the index the Query Store text index replaces. Any of them present
           means there is something to drop. */
        var exists = state.IndexValid is not null || state.SampleIndexValid is not null || state.ReplacedIndexExists;
        if (!snapshot.Enabled)
        {
            if (!exists)
            {
                return TrigramIndexStatus.Off;
            }

            return snapshot.Phase == QueryTextTrigramIndexTracker.PhaseKind.Dropping ? TrigramIndexStatus.Dropping : TrigramIndexStatus.DropPending;
        }

        /* #7849: Ready is the trigram index valid. The sample index on query_stats is an accelerator for the all-servers sample read, which is served
           without it (the matched digests are bound to the read), so its build or absence never takes the search away; its own state is in the health's
           sample fields and in the note. */
        if (state.IndexValid == true)
        {
            return TrigramIndexStatus.Ready;
        }

        if (snapshot.Phase == QueryTextTrigramIndexTracker.PhaseKind.Building || state.BuildRunning)
        {
            return TrigramIndexStatus.Building;
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
            status == TrigramIndexStatus.WaitingOnExtension ? OperatorStatement : null,
            state.SampleIndexValid is not null,
            state.SampleIndexValid,
            state.SampleIndexSizeBytes,
            snapshot.OptionSource);
    }

    /// <summary>One sentence for the health block's note, per status.</summary>
    public static string Note(TrigramIndexHealth health)
    {
        ArgumentNullException.ThrowIfNull(health);
        var size = health.SizeBytes is { } bytes ? (bytes / 1048576.0).ToString("F1", CultureInfo.InvariantCulture) + " MB" : "no size";
        var sampleSize = health.SampleIndexSizeBytes is { } sampleBytes ? (sampleBytes / 1048576.0).ToString("F1", CultureInfo.InvariantCulture) + " MB" : "no size";
        return health.Status switch
        {
            TrigramIndexStatus.Off => "The option queryTextTrigramIndexEnabled is off and there is no trigram index on the plan-cache text, and no sample index on query_stats.",
            TrigramIndexStatus.DropPending => "The option queryTextTrigramIndexEnabled is off. An index still exists and the service drops it on its next pass.",
            TrigramIndexStatus.Dropping => "The option queryTextTrigramIndexEnabled is off. The service is dropping the index now.",
            TrigramIndexStatus.Pending => "The option is on and the index does not exist yet. The service builds it in the background after startup, and an INVALID leftover is dropped first. The sample index on query_stats follows it.",
            TrigramIndexStatus.Building => "A build is running. Collection is not waiting for it. The size is what has been written so far (trigram index " + size + ", sample index " + sampleSize + ").",
            TrigramIndexStatus.Ready => "The trigram index exists and is valid (" + size + ")" + health.SampleIndexValid switch
            {
                true => ", and so does the sample index on query_stats (" + sampleSize + "), which the all-servers search reads the matched texts' samples through.",
                false => ". The sample index on query_stats is INVALID (an interrupted build): the service drops it and builds it again, and until then the all-servers search reads the samples by the matched digests without it.",
                null => ". The sample index on query_stats does not exist yet: the service builds it in the background"
                    + (health.LastError is not null ? ", trying again at next_attempt_at after a failed build" : "")
                    + ", and until then the all-servers search reads the samples by the matched digests without it.",
            },
            TrigramIndexStatus.Unconfigured => "The service has not read darling.json yet, so the option queryTextTrigramIndexEnabled is not known. Try again in a moment.",
            TrigramIndexStatus.Failed => "The last build failed. Its leftover was dropped and the service tries again at next_attempt_at, after a wait that doubles to a cap of "
                + ((int)BackoffCap.TotalHours).ToString(CultureInfo.InvariantCulture) + " hours.",
            _ => health.ExtensionAvailable
                ? "The store login cannot create the pg_trgm extension. Run operator_statement in the store database as a role that may create it (a superuser, or the database owner). The next pass then builds the index. It is not retried until then."
                : "The server does not ship the pg_trgm extension. Install the PostgreSQL contrib package, then run operator_statement in the store database as a role that may create it.",
        };
    }
}
