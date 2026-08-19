using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitorLite.Database;

/// <summary>
/// Initializes the DuckDB database and creates tables on first run.
/// </summary>
public class DuckDbInitializer
{
    private readonly string _databasePath;
    private readonly ILogger<DuckDbInitializer>? _logger;

    /// <summary>
    /// Coordinates UI readers with maintenance writers (CHECKPOINT, archive DELETEs, compaction).
    /// Read locks allow unlimited concurrent UI queries. Write locks are exclusive and wait
    /// for all readers to finish before proceeding.
    /// </summary>
    private static readonly ReaderWriterLockSlim s_dbLock = new(LockRecursionPolicy.NoRecursion);

    /// <summary>
    /// Acquires a read lock on the database. Multiple readers can hold this concurrently.
    /// Dispose the returned object to release the lock.
    /// If the current thread already owns a read lock (e.g., leaked by an unhandled exception),
    /// returns a no-op disposable to allow the operation to proceed.
    /// </summary>
    public IDisposable AcquireReadLock()
    {
        try
        {
            s_dbLock.EnterReadLock();
        }
        catch (LockRecursionException)
        {
            /* The current thread already owns a read lock — likely leaked by an unhandled
               exception that prevented Dispose(). Since we're already protected by a read lock,
               return a no-op disposable so the caller can proceed normally. */
            return NoOpDisposable.Instance;
        }
        return new LockReleaser(s_dbLock, write: false);
    }

    /// <summary>
    /// Acquires an exclusive write lock on the database. Blocks until all readers finish.
    /// Dispose the returned object to release the lock.
    /// When a timeout is specified, throws <see cref="TimeoutException"/> if the lock
    /// cannot be acquired within the given duration (e.g., archival is in progress).
    /// </summary>
    public IDisposable AcquireWriteLock(TimeSpan? timeout = null)
    {
        if (timeout.HasValue)
        {
            if (!s_dbLock.TryEnterWriteLock(timeout.Value))
                throw new TimeoutException(
                    "Could not acquire database write lock — another operation (archival or maintenance) may be in progress. Please try again in a few moments.");
        }
        else
        {
            s_dbLock.EnterWriteLock();
        }
        return new LockReleaser(s_dbLock, write: true);
    }

    private sealed class NoOpDisposable : IDisposable
    {
        public static readonly NoOpDisposable Instance = new();
        public void Dispose() { }
    }

    private sealed class LockReleaser : IDisposable
    {
        private readonly ReaderWriterLockSlim _lock;
        private readonly bool _write;
        private bool _disposed;

        public LockReleaser(ReaderWriterLockSlim rwLock, bool write)
        {
            _lock = rwLock;
            _write = write;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            if (_write) _lock.ExitWriteLock();
            else _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Current schema version. Increment this when schema changes require table rebuilds.
    /// </summary>
    internal const int CurrentSchemaVersion = 54;

    private readonly string _archivePath;

    public DuckDbInitializer(string databasePath, ILogger<DuckDbInitializer>? logger = null)
    {
        _databasePath = databasePath;
        _logger = logger;
        _archivePath = Path.Combine(Path.GetDirectoryName(databasePath) ?? ".", "archive");
    }

    /* Tables that have parquet archives — views are created to UNION hot data with archived parquet files.
       Catalog-driven: every collector table (from CollectorCatalog) plus the two non-collector time-series
       tables (config_alert_log, collection_log). Adding a collector to the catalog gives it an archive view
       for free — no hand-maintained list to keep in sync. Mirrors ArchiveService.ArchivableTables (same set,
       same derivation); a test pins the two against each other and against the catalog. */
    internal static readonly string[] ArchivableTables =
        /* StoredCollectors, not CollectorCatalog.All: Lite does not CREATE the PostgreSQL collectors' tables
           (it has no PostgreSQL target and cannot get one), so an archive view over them would reference a
           table that does not exist. */
        DuckDbSchemaGenerator.StoredCollectors.Select(c => c.TargetTable)
            .Concat(["config_alert_log", "collection_log"])
            .ToArray();

    /* Archive views for these tables must DEDUP the hot∪parquet union on a server-side natural key.
       The 512MB emergency reset (ArchiveService.ArchiveAllAndResetAsync) archives all hot data to parquet
       AND wipes collection_log, so the next cycle re-collects recent history into the hot store while the
       parquet tier still holds it — the plain UNION ALL would then show each re-collected event twice.
       The local surrogate prefix id (job_history_id / default_trace_event_id) is a per-process counter
       (CollectionIdGenerator), so it is NOT stable across re-collection and cannot be the key — only the
       SQL-Server-side identity is. Other archivable tables can't double up this way (normal archival keeps
       hot and parquet disjoint, and their rows aren't re-collected after a reset), so they keep the plain
       union. Value = the PARTITION BY column list for the QUALIFY ROW_NUMBER dedup. */
    private static readonly Dictionary<string, string> ArchiveViewDedupKeys =
        new(StringComparer.Ordinal)
        {
            /* sysjobhistory.instance_id: a unique monotonic IDENTITY per server that survives
               sp_purge_jobhistory — JobHistoryCollector's exact-and-complete dedup watermark. */
            ["job_history"] = "server_id, instance_id",
            /* The default trace's EventSequence is unique within a trace; pairing it with event_time
               (the StartTime watermark) keeps events distinct across the server restarts that reset
               EventSequence, and groups identical re-collected rows (NULLs included) for dedup. */
            ["default_trace_events"] = "server_id, event_time, event_sequence",
        };

    /// <summary>
    /// Gets the connection string for the DuckDB database.
    /// - checkpoint_threshold=1GB: disables automatic WAL checkpoints to prevent
    ///   2-3s stop-the-world stalls during collector writes. Manual CHECKPOINT
    ///   runs between collection cycles instead.
    /// - memory_limit=1GB: caps the resting buffer pool so it doesn't grow
    ///   unbounded as the archive directory fills with parquet files (the
    ///   ".tmp dir caching" path is the actual driver of #933's titled
    ///   complaint — uncapped, buffer pool grows toward 80% of system RAM).
    ///   ArchiveService raises this temporarily for parquet COPY operations,
    ///   which need more headroom due to a DuckDB pre-reservation behavior.
    /// </summary>
    public string ConnectionString => $"Data Source={_databasePath};memory_limit=1GB;checkpoint_threshold=1GB";

    /// <summary>
    /// Ensures the database exists and all tables are created.
    /// Handles DuckDB version mismatches by exporting data to Parquet, recreating the database, and importing.
    /// </summary>
    public async Task InitializeAsync()
    {
        _logger?.LogInformation("Initializing DuckDB database at {Path}", _databasePath);

        var directory = Path.GetDirectoryName(_databasePath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
            _logger?.LogInformation("Created database directory: {Directory}", directory);
        }

        var archivePath = Path.Combine(directory ?? ".", "archive");
        if (!Directory.Exists(archivePath))
        {
            Directory.CreateDirectory(archivePath);
            _logger?.LogInformation("Created archive directory: {ArchivePath}", archivePath);
        }

        /* Open the database. Only a genuine storage-version mismatch triggers the
           destructive Parquet rebuild; transient lock contention is retried instead. */
        DuckDBConnection connection = await OpenDatabaseAsync(archivePath);

        using (connection)
        {
            await ExecuteNonQueryAsync(connection,
                "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)");

            var existingVersion = await GetSchemaVersionAsync(connection);

            /* On a fresh/reset database (v0), skip migrations entirely — they DROP tables
               expecting CREATE TABLE to follow, which is destructive on a blank DB.
               Just create tables with the current schema and stamp the version. */
            if (existingVersion > 0 && existingVersion < CurrentSchemaVersion)
            {
                _logger?.LogInformation("Schema upgrade needed: v{Old} -> v{New}", existingVersion, CurrentSchemaVersion);
                await RunMigrationsAsync(connection, existingVersion);
            }

            foreach (var tableStatement in Schema.GetAllTableStatements())
            {
                await ExecuteNonQueryAsync(connection, tableStatement);
            }

            foreach (var indexStatement in Schema.GetAllIndexStatements())
            {
                await ExecuteNonQueryAsync(connection, indexStatement);
            }

            if (existingVersion < CurrentSchemaVersion)
            {
                await SetSchemaVersionAsync(connection, CurrentSchemaVersion);
            }

            /* Table count on the init connection — makes a failed reset (schema not persisting to the
               file for the next connection to see) diagnosable from the log alone. */
            using (var tableCountCmd = connection.CreateCommand())
            {
                tableCountCmd.CommandText = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main'";
                var tableCount = Convert.ToInt64(await tableCountCmd.ExecuteScalarAsync());
                _logger?.LogInformation("Schema initialization created {Count} tables", tableCount);
            }

            _logger?.LogInformation("Database initialization complete. Schema version: {Version}", CurrentSchemaVersion);
        }

        await CreateArchiveViewsAsync();

        await InitializeAnalysisSchemaAsync();
    }

    /// <summary>
    /// Opens the DuckDB database, handling the two failure modes distinctly:
    /// a genuine storage-version mismatch is migrated via Parquet export/import,
    /// while transient lock contention (e.g. an instance that was just killed,
    /// or antivirus holding the file) is retried before giving up.
    /// Any other open failure is rethrown — it must NOT trigger the destructive
    /// Parquet rebuild, which would move the live database aside (Issue #977).
    /// </summary>
    private async Task<DuckDBConnection> OpenDatabaseAsync(string archivePath)
    {
        const int maxLockRetries = 5;
        const int lockRetryDelayMs = 1000;

        for (int attempt = 1; ; attempt++)
        {
            var connection = new DuckDBConnection(ConnectionString);
            try
            {
                await connection.OpenAsync();
                return connection;
            }
            catch (Exception ex) when (IsStorageVersionError(ex))
            {
                connection.Dispose();
                _logger?.LogWarning("DuckDB storage version mismatch detected. Migrating data via Parquet export/import.");
                await MigrateViaParquetAsync(archivePath);

                var migrated = new DuckDBConnection(ConnectionString);
                await migrated.OpenAsync();
                return migrated;
            }
            catch (Exception ex) when (IsTransientLockError(ex) && attempt < maxLockRetries)
            {
                connection.Dispose();
                _logger?.LogWarning(
                    "DuckDB database is locked (attempt {Attempt}/{Max}); retrying in {Delay}ms. {Error}",
                    attempt, maxLockRetries, lockRetryDelayMs, ex.Message);
                await Task.Delay(lockRetryDelayMs);
            }
            catch
            {
                connection.Dispose();
                throw;
            }
        }
    }

    /// <summary>
    /// Checks if an exception is a genuine DuckDB storage-version mismatch — an
    /// incompatible on-disk format that cannot be opened as-is and must be
    /// rebuilt. Deliberately narrow: generic open failures and lock contention
    /// must NOT match, or the destructive Parquet rebuild fires on a database
    /// that is merely locked or recovering a WAL from an unclean shutdown.
    /// </summary>
    private static bool IsStorageVersionError(Exception ex)
    {
        /* DuckDB reports a genuine version mismatch as one of:
           - "Serialization Error: Failed to deserialize: ..." (incompatible storage format)
           - "IO Error: Trying to read a database file with version number X,
              but we can only read version Y"
           Since DuckDB v0.10+, newer libraries read older files, so this almost
           always means an older library was pointed at a newer file. */
        var message = ex.ToString().ToLowerInvariant();
        return message.Contains("failed to deserialize")
            || message.Contains("trying to read a database file with version")
            || message.Contains("storage version");
    }

    /// <summary>
    /// Checks if an exception is transient lock contention on the database file —
    /// another process (a just-killed prior instance, antivirus) is holding it.
    /// These are safe to retry and must never trigger the Parquet rebuild.
    /// </summary>
    private static bool IsTransientLockError(Exception ex)
    {
        var message = ex.ToString().ToLowerInvariant();
        return message.Contains("conflicting lock")
            || message.Contains("could not set lock")
            || message.Contains("being used by another process");
    }

    /// <summary>
    /// Exports all tables from the old database to Parquet, deletes the database, and reimports.
    /// Uses DuckDB's EXPORT DATABASE which writes one Parquet file per table.
    /// </summary>
    private async Task MigrateViaParquetAsync(string archivePath)
    {
        var exportDir = Path.Combine(archivePath, $"upgrade_{DateTime.Now:yyyyMMdd_HHmmss}");
        Directory.CreateDirectory(exportDir);

        /* Step 1: Try to export from the old database using EXPORT DATABASE.
           Since DuckDB v0.10+, newer versions can read older files (backward compat),
           so upgrading DuckDB should normally open the file without hitting this path.
           This mainly handles edge cases (e.g., downgrade, corruption).
           If the file is truly unreadable, the backup preserves it for manual recovery
           using the original DuckDB version's CLI: duckdb old.db "EXPORT DATABASE 'dir'" */
        var exported = false;
        try
        {
            /* Attempt read-only open — some version mismatches allow read but not write */
            var readOnlyConnStr = $"Data Source={_databasePath};ACCESS_MODE=READ_ONLY";
            using (var oldConn = new DuckDBConnection(readOnlyConnStr))
            {
                await oldConn.OpenAsync();

                /* Export all tables to Parquet */
                using var cmd = oldConn.CreateCommand();
                cmd.CommandText = $"EXPORT DATABASE '{exportDir.Replace("'", "''")}' (FORMAT PARQUET)";
                await cmd.ExecuteNonQueryAsync();
                exported = true;
                _logger?.LogInformation("Exported old database to {ExportDir}", exportDir);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Could not export old database — data will be preserved as backup file only");
        }

        /* Step 2: Back up and delete the old database file */
        var backupPath = _databasePath + $".backup_{DateTime.Now:yyyyMMdd_HHmmss}";
        try
        {
            /* DuckDB may have .wal files too */
            File.Move(_databasePath, backupPath);
            _logger?.LogInformation("Backed up old database to {BackupPath}", backupPath);

            var walPath = _databasePath + ".wal";
            if (File.Exists(walPath))
            {
                File.Move(walPath, backupPath + ".wal");
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failed to back up old database, deleting instead");
            File.Delete(_databasePath);

            var walPath = _databasePath + ".wal";
            if (File.Exists(walPath)) File.Delete(walPath);
        }

        /* Step 3: If we exported successfully, import into the fresh database */
        if (exported)
        {
            try
            {
                using var newConn = new DuckDBConnection(ConnectionString);
                await newConn.OpenAsync();

                using var cmd = newConn.CreateCommand();
                cmd.CommandText = $"IMPORT DATABASE '{exportDir.Replace("'", "''")}' ";
                await cmd.ExecuteNonQueryAsync();
                _logger?.LogInformation("Imported data from Parquet export into new database");
            }
            catch (Exception ex)
            {
                /* Import may fail if schema changed between versions — that's okay,
                   the normal initialization will create fresh tables */
                _logger?.LogWarning(ex, "Could not import Parquet data — starting with fresh tables. " +
                    "Parquet files preserved at {ExportDir} for manual recovery.", exportDir);
            }
        }
    }

    private async Task<int> GetSchemaVersionAsync(DuckDBConnection connection)
    {
        try
        {
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT COALESCE(MAX(version), 0) FROM schema_version";
            var result = await command.ExecuteScalarAsync();
            return Convert.ToInt32(result);
        }
        catch
        {
            return 0;
        }
    }

    private async Task SetSchemaVersionAsync(DuckDBConnection connection, int version)
    {
        await ExecuteNonQueryAsync(connection, "DELETE FROM schema_version");
        using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO schema_version (version) VALUES ($1)";
        command.Parameters.Add(new DuckDBParameter { Value = version });
        await command.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Runs schema migrations from the given version up to CurrentSchemaVersion.
    /// Each migration drops and recreates affected tables.
    ///
    /// IMPORTANT: When adding a new data collection table, you must also register it in:
    ///   1. Schema.cs — GetAllTableStatements() and GetAllIndexStatements()
    ///   2. DuckDbInitializer.cs — ArchivableTables (archive view creation)
    ///   3. ArchiveService.cs — ArchivableTables (parquet export + purge)
    /// Forgetting any of these causes unbounded growth and 512 MB reset loops.
    /// </summary>
    private async Task RunMigrationsAsync(DuckDBConnection connection, int fromVersion)
    {
        if (fromVersion < 2)
        {
            /* v2: Added delta columns to query_stats (delta_logical_writes, delta_physical_reads, delta_spills)
                   and procedure_stats (delta_logical_reads, delta_logical_writes, delta_physical_reads).
                   Added plan_id, avg_logical_writes, avg_physical_reads to query_store_stats.
                   Restructured blocked_process_reports. */
            _logger?.LogInformation("Running migration to v2: rebuilding query_stats, procedure_stats, query_store_stats, blocked_process_reports");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS query_stats");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS procedure_stats");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS query_store_stats");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS blocking_snapshots"); /* Cleanup - table no longer used */
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS blocked_process_reports");
        }

        if (fromVersion < 3)
        {
            /* v3: Fix server_id values. Previously used string.GetHashCode() which is
                   randomized per process in .NET Core, producing different IDs on each restart.
                   Now uses a deterministic FNV-1a hash of server_name. This migration updates
                   all existing rows to use the correct deterministic server_id. */
            _logger?.LogInformation("Running migration to v3: fixing server_id values (non-deterministic hash -> deterministic)");
            await FixServerIdsAsync(connection);
        }

        if (fromVersion < 4)
        {
            /* v4: Added sql_duration_ms and duckdb_duration_ms columns to collection_log
                   for split collector timing (SQL query vs DuckDB insert).
                   Only ALTER if the table already exists — on fresh installs it will be
                   created with these columns by GetAllTableStatements(). */
            _logger?.LogInformation("Running migration to v4: adding timing columns to collection_log");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE collection_log ADD COLUMN IF NOT EXISTS sql_duration_ms INTEGER");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE collection_log ADD COLUMN IF NOT EXISTS duckdb_duration_ms INTEGER");
            }
            catch
            {
                /* Table doesn't exist yet — will be created with correct schema below */
            }
        }

        if (fromVersion < 5)
        {
            /* v5: Added database_scoped_config and trace_flags tables
                   for database-scoped configuration and active trace flag collection. */
            _logger?.LogInformation("Running migration to v5: adding database_scoped_config and trace_flags tables");
            /* Generated from the catalog (same source as GetAllTableStatements, which also recreates these
               with IF NOT EXISTS immediately after migrations); byte-equivalent to the former hand-written
               Schema constants this migration used before the schema was made catalog-driven. The index is
               null-checked (both collectors have one today) rather than asserted, mirroring the generator. */
            foreach (ICollectorSchemaInfo collector in new[]
                { (ICollectorSchemaInfo)DatabaseScopedConfigCollector.Instance, TraceFlagsCollector.Instance })
            {
                await ExecuteNonQueryAsync(connection, DuckDbSchemaGenerator.CreateTable(collector));
                var collectorIndex = DuckDbSchemaGenerator.CreateIndex(collector);
                if (collectorIndex is not null)
                {
                    await ExecuteNonQueryAsync(connection, collectorIndex);
                }
            }
        }

        if (fromVersion < 6)
        {
            /* v6: Added sql_handle and plan_handle to query_stats and procedure_stats,
                   and query_plan_hash to query_store_stats for cross-referencing.
                   Must drop/recreate because ALTER TABLE appends columns at the end,
                   but the DuckDB appender writes by position and expects specific column order. */
            _logger?.LogInformation("Running migration to v6: rebuilding query_stats, procedure_stats, query_store_stats for handle columns");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS query_stats");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS procedure_stats");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS query_store_stats");
        }

        if (fromVersion < 7)
        {
            /* v7: Changed collection_log.log_id from INTEGER to BIGINT.
                   GenerateCollectionId() returns a long seeded from DateTime.UtcNow.Ticks
                   which overflows 32-bit INTEGER, causing all collection_log INSERTs to fail silently. */
            _logger?.LogInformation("Running migration to v7: rebuilding collection_log (log_id INTEGER -> BIGINT)");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS collection_log");
        }

        if (fromVersion < 8)
        {
            /* v8: Added min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
                   and total_spills columns to procedure_stats for parity with Dashboard.
                   Must drop/recreate because DuckDB appender writes by position. */
            _logger?.LogInformation("Running migration to v8: rebuilding procedure_stats for min/max/spills columns");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS procedure_stats");
        }

        if (fromVersion < 9)
        {
            /* v9: Added dismissed column to config_alert_log for hide/dismiss functionality.
                   Safe to ALTER because this table uses INSERT (not appender). */
            _logger?.LogInformation("Running migration to v9: adding dismissed column to config_alert_log");
            try
            {
                /* DuckDB does not support ADD COLUMN with NOT NULL — use nullable with DEFAULT */
                await ExecuteNonQueryAsync(connection, "ALTER TABLE config_alert_log ADD COLUMN IF NOT EXISTS dismissed BOOLEAN DEFAULT false");
            }
            catch
            {
                /* Table doesn't exist yet — will be created with correct schema below */
            }
        }

        if (fromVersion < 10)
        {
            /* v10: Added server_name column to collection_log so log entries
                    can be identified by server without needing a lookup table. */
            _logger?.LogInformation("Running migration to v10: adding server_name column to collection_log");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE collection_log ADD COLUMN IF NOT EXISTS server_name VARCHAR");
            }
            catch
            {
                /* Table doesn't exist yet — will be created with correct schema below */
            }
        }

        if (fromVersion < 11)
        {
            /* v11: Expanded database_config from 9 to 28 columns (sys.databases).
                    Added state_desc, collation, RCSI, snapshot isolation, stats settings,
                    encryption, security, and version-gated columns (ADR, memory optimized, optimized locking). */
            _logger?.LogInformation("Running migration to v11: rebuilding database_config for expanded sys.databases columns");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS database_config");
        }

        if (fromVersion < 12)
        {
            /* v12: Added login_name, host_name, program_name, open_transaction_count,
                    percent_complete columns to query_snapshots for Issue #149. */
            _logger?.LogInformation("Running migration to v12: adding session columns to query_snapshots");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS login_name VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS host_name VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS program_name VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS open_transaction_count INTEGER");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS percent_complete DECIMAL(5,2)");
            }
            catch
            {
                /* Table doesn't exist yet — will be created with correct schema below */
            }
        }

        if (fromVersion < 13)
        {
            /* v13: Full column parity with Dashboard for all three query/procedure collectors.
                    query_stats: added creation_time, last_execution_time, total_clr_time,
                      min/max physical_reads, rows, spills, memory grant columns (6), thread columns (4).
                    procedure_stats: added cached_time, last_execution_time,
                      min/max logical_reads, physical_reads, logical_writes, spills.
                    query_store_stats: complete rebuild with all min/max columns, DOP, CLR,
                      memory, tempdb, plan forcing, compilation metrics, version-gated columns.
                    Must drop/recreate because DuckDB appender writes by position. */
            _logger?.LogInformation("Running migration to v13: rebuilding query_stats, procedure_stats, query_store_stats for full Dashboard column parity");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS query_stats");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS procedure_stats");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS query_store_stats");
        }

        if (fromVersion < 14)
        {
            /* v14: Switched memory_grant_stats from per-session (dm_exec_query_memory_grants)
                    to semaphore-level (dm_exec_query_resource_semaphores) for parity with Dashboard.
                    Old schema had session_id, query_text, dop, etc. New schema has
                    resource_semaphore_id, pool_id, and delta columns.
                    Must drop/recreate because column layout is completely different. */
            _logger?.LogInformation("Running migration to v14: rebuilding memory_grant_stats for resource semaphore schema");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS memory_grant_stats");
        }

        if (fromVersion < 15)
        {
            /* v15: Added queued I/O columns (io_stall_queued_read_ms, io_stall_queued_write_ms)
                    and their delta counterparts to file_io_stats for latency overlay charts.
                    Must drop/recreate because DuckDB appender writes by position. */
            _logger?.LogInformation("Running migration to v15: rebuilding file_io_stats for queued I/O columns");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS file_io_stats");
        }

        if (fromVersion < 16)
        {
            /* v16: Added database_size_stats and server_properties tables for FinOps monitoring.
                    New tables only — no existing table changes needed. Tables created by
                    GetAllTableStatements() during initialization. */
            _logger?.LogInformation("Running migration to v16: adding FinOps tables (database_size_stats, server_properties)");
        }

        if (fromVersion < 17)
        {
            /* v17: Added volume-level drive space columns to database_size_stats.
                    Columns appended at end — safe for DuckDB appender positional writes. */
            _logger?.LogInformation("Running migration to v17: adding volume stats columns to database_size_stats");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE database_size_stats ADD COLUMN IF NOT EXISTS volume_mount_point VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE database_size_stats ADD COLUMN IF NOT EXISTS volume_total_mb DECIMAL(19,2)");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE database_size_stats ADD COLUMN IF NOT EXISTS volume_free_mb DECIMAL(19,2)");
            }
            catch
            {
                /* Table doesn't exist yet — will be created with correct schema below */
            }
        }

        if (fromVersion < 18)
        {
            /* v18: Added session_stats table for per-application connection tracking
                    from sys.dm_exec_sessions. New table only — created by GetAllTableStatements(). */
            _logger?.LogInformation("Running migration to v18: adding session_stats table for application connections");
        }

        if (fromVersion < 19)
        {
            _logger?.LogInformation("Running migration to v19: adding worker thread columns to memory_stats");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE memory_stats ADD COLUMN IF NOT EXISTS max_workers_count INTEGER");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE memory_stats ADD COLUMN IF NOT EXISTS current_workers_count INTEGER");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v19 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 20)
        {
            _logger?.LogInformation("Running migration to v20: adding mute rules table and muted column to alert log");
            try
            {
                /* DuckDB does not support ADD COLUMN with NOT NULL — use nullable with DEFAULT */
                await ExecuteNonQueryAsync(connection, "ALTER TABLE config_alert_log ADD COLUMN IF NOT EXISTS muted BOOLEAN DEFAULT false");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v20 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 21)
        {
            _logger?.LogInformation("Running migration to v21: adding detail_text column to alert log");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE config_alert_log ADD COLUMN IF NOT EXISTS detail_text VARCHAR");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v21 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 22)
        {
            _logger?.LogInformation("Running migration to v22: adding growth rate and VLF count columns to database_size_stats");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE database_size_stats ADD COLUMN IF NOT EXISTS is_percent_growth BOOLEAN");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE database_size_stats ADD COLUMN IF NOT EXISTS growth_pct INTEGER");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE database_size_stats ADD COLUMN IF NOT EXISTS vlf_count INTEGER");
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Migration to v22 failed");
                throw;
            }
        }

        if (fromVersion < 23)
        {
            _logger?.LogInformation("Running migration to v23: adding dismissed_archive_alerts sidecar table");
            try
            {
                await ExecuteNonQueryAsync(connection, Schema.CreateDismissedArchiveAlertsTable);
                await ExecuteNonQueryAsync(connection, Schema.CreateDismissedArchiveAlertsIndex);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Migration to v23 failed");
                throw;
            }
        }

        if (fromVersion < 24)
        {
            _logger?.LogInformation("Running migration to v24: adding vcore_count column to server_properties for Azure SQL DB vCore tracking");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS vcore_count INTEGER");
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Migration to v24 failed");
                throw;
            }
        }

        if (fromVersion < 25)
        {
            /* v25: Added memory_pressure_events table for RING_BUFFER_RESOURCE_MONITOR notifications.
                    New table only — created by GetAllTableStatements(). */
            _logger?.LogInformation("Running migration to v25: adding memory_pressure_events table");
        }

        if (fromVersion < 26)
        {
            _logger?.LogInformation("Running migration to v26: adding context_json column to alert log");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE config_alert_log ADD COLUMN IF NOT EXISTS context_json VARCHAR");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v26 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 27)
        {
            _logger?.LogInformation("Running migration to v27: adding server-health columns (LPIM/IFI/memory dumps) to server_properties");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS lock_pages_in_memory BOOLEAN");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS instant_file_initialization_enabled BOOLEAN");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS memory_dump_count INTEGER");
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Migration to v27 failed");
                throw;
            }
        }

        if (fromVersion < 28)
        {
            /* v28: Added is_cdc_capture flag to query_snapshots so the long-running query
                    alert can exclude CDC capture sessions. The collector computes the flag
                    server-side (program_name -> job_id via msdb.dbo.cdc_jobs, text fallback).
                    Appended at the end to match the DuckDB appender's positional order. */
            _logger?.LogInformation("Running migration to v28: adding is_cdc_capture column to query_snapshots");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS is_cdc_capture BOOLEAN DEFAULT false");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v28 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 30)
        {
            /* v30 (#1140): dedup-fingerprint support. blocked_process_reports gains the contentious
               object the blocked_process_report event already carries (object_id/database_id) plus the
               resolved name; query_snapshots gains query_hash for the long-running-query dedup key.
               Appended at the end to keep the positional appender aligned; the v_ views union BY NAME
               so old parquet reads back NULL for these. */
            _logger?.LogInformation("Running migration to v30: dedup fingerprint columns (#1140)");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE blocked_process_reports ADD COLUMN IF NOT EXISTS object_id INTEGER");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE blocked_process_reports ADD COLUMN IF NOT EXISTS database_id INTEGER");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE blocked_process_reports ADD COLUMN IF NOT EXISTS contentious_object VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS query_hash VARCHAR");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v30 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 31)
        {
            /* v31: failed-Agent-job watermark persistence. The blocking/deadlock edge-trigger
               watermarks already survive restart (#1145); the failed-job watermark did not, so a
               reopen re-fired tray toasts for failures still inside the lookback window that the
               user had already seen and dismissed. Adds a nullable watermark_time column to the
               existing watermark table to hold the newest already-alerted failure's server-local
               run time. Only ALTER if the table exists — fresh installs get the column from
               GetAllTableStatements(). */
            _logger?.LogInformation("Running migration to v31: failed-job watermark column");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE config_edge_trigger_watermarks ADD COLUMN IF NOT EXISTS watermark_time TIMESTAMP");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v31 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 32)
        {
            /* v32: block-chain reconstruction now keys sessions by spid:ecid within monitor_loop (mirroring
               sp_HumanEventsBlockViewer). blocked_process_reports gains monitor_loop (the blocked-process-report
               episode). Appended at the end to keep the positional appender aligned; the v_ view (SELECT *,
               recreated on startup) surfaces it; old parquet reads back NULL (union BY NAME). The collector
               appender now writes monitor_loop, so an un-migrated DB would mis-align — this ALTER is required. */
            _logger?.LogInformation("Running migration to v32: blocked_process_reports.monitor_loop");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE blocked_process_reports ADD COLUMN IF NOT EXISTS monitor_loop INTEGER");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v32 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 33)
        {
            /* v33: procedure_stats gains delta_spills, and query_stats gains plan_generation_num +
               sample_interval_seconds. All appended at the end to keep the positional appenders aligned; the
               v_ views (SELECT *) surface them; old parquet reads back NULL (union BY NAME). The collectors now
               write these columns, so an un-migrated DB would mis-align — these ALTERs are required.
               - procedure_stats.delta_spills: spill-delta parity with query_stats (proc Total/Avg Spills now
                 reflect per-window work, not summed cumulative DMV totals).
               - query_stats.plan_generation_num: plan-stability signal.
               - query_stats.sample_interval_seconds: lets the display derive worker_time_per_second (CPU-ms/sec). */
            _logger?.LogInformation("Running migration to v33: proc delta_spills + query_stats signal columns");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE procedure_stats ADD COLUMN IF NOT EXISTS delta_spills BIGINT");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_stats ADD COLUMN IF NOT EXISTS plan_generation_num BIGINT");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_stats ADD COLUMN IF NOT EXISTS sample_interval_seconds INTEGER");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v33 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 34)
        {
            /* v34: query_snapshots gains the wait-drilldown triage columns Dashboard collects via
               sp_WhoIsActive — memory-grant requested/used/max-used (MB), tempdb current/allocations
               (MB), transaction log used (MB) + transaction start time, and request_id. All appended
               at the end to keep the positional appender aligned; the v_ view (SELECT *) surfaces
               them and old parquet reads back NULL (union BY NAME). The snapshot collector writes
               these columns, so an un-migrated DB would mis-align — these ALTERs are required. */
            _logger?.LogInformation("Running migration to v34: query_snapshots memory-grant/tempdb/transaction columns");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS requested_memory_mb DOUBLE PRECISION");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS used_memory_mb DOUBLE PRECISION");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS max_used_memory_mb DOUBLE PRECISION");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS tempdb_current_mb DOUBLE PRECISION");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS tempdb_allocations_mb DOUBLE PRECISION");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS tran_log_used_mb DOUBLE PRECISION");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS tran_start_time TIMESTAMP");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS request_id INTEGER");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v34 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 35)
        {
            /* v35: deferred execution-plan capture (#1262) — procedure_stats.query_plan_xml,
               blocked_process_reports.blocked_query_plan_xml + blocking_query_plan_xml, and
               deadlocks.victim_query_plan_xml. All appended at the end to keep the positional appenders
               aligned; the v_ views (SELECT *) surface them and old parquet reads back NULL (union BY
               NAME). The collectors now write these columns unconditionally — always NULL on Lite, which
               never sets CapturePlanXml (Darling-only) — so an un-migrated DB would mis-align on the next
               append; these ALTERs are required. */
            _logger?.LogInformation("Running migration to v35: procedure/blocked-process/deadlock plan columns");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE procedure_stats ADD COLUMN IF NOT EXISTS query_plan_xml VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE blocked_process_reports ADD COLUMN IF NOT EXISTS blocked_query_plan_xml VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE blocked_process_reports ADD COLUMN IF NOT EXISTS blocking_query_plan_xml VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE deadlocks ADD COLUMN IF NOT EXISTS victim_query_plan_xml VARCHAR");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v35 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 36)
        {
            /* v36: server_properties gains sqlserver_start_time / host_os_version / ag_replica_role — the
               three fields the shared ServerPropertiesCollector now SELECTs (previously read only from a
               live query in the FinOps Server Inventory). All appended at the end to keep the positional
               appender aligned; the collector writes them unconditionally, so an un-migrated DB would
               mis-align on the next append — these ALTERs are required. */
            _logger?.LogInformation("Running migration to v36: server_properties start-time / host-OS / AG-role columns");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS sqlserver_start_time TIMESTAMP");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS host_os_version VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS ag_replica_role VARCHAR");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v36 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 37)
        {
            /* v37: added latch_stats (sys.dm_os_latch_stats) and spinlock_stats
                    (sys.dm_os_spinlock_stats) shared collectors for Dashboard->Darling collection
                    parity. New tables only — created by GetAllTableStatements() below; the v_ views
                    come from CreateArchiveViewsAsync via ArchivableTables. */
            _logger?.LogInformation("Running migration to v37: adding latch_stats and spinlock_stats tables");
        }

        if (fromVersion < 38)
        {
            /* v38: added cpu_scheduler_stats (sys.dm_os_schedulers + workload groups + NUMA nodes +
                    OS memory) and plan_cache_stats (sys.dm_exec_cached_plans) shared collectors for
                    Dashboard->Darling collection parity. New tables only — created by
                    GetAllTableStatements() below; the v_ views come from CreateArchiveViewsAsync via
                    ArchivableTables. */
            _logger?.LogInformation("Running migration to v38: adding cpu_scheduler_stats and plan_cache_stats tables");
        }

        if (fromVersion < 39)
        {
            /* v39: added session_summary_stats (server-wide session SUMMARY from sys.dm_exec_sessions
                    + sys.dm_exec_requests: total/running/sleeping/background/dormant sessions, idle
                    sessions over 30 min, memory-wait count, top application/host) — the Dashboard->
                    Darling connection-leak / idle parity collector. Distinct from the per-application
                    session_stats table. New table only — created by GetAllTableStatements() below; the
                    v_ view comes from CreateArchiveViewsAsync via ArchivableTables. */
            _logger?.LogInformation("Running migration to v39: adding session_summary_stats table");
        }

        if (fromVersion < 40)
        {
            /* v40: added system_health_events (Stage 1 raw system_health Extended Events capture —
                    one row per event, raw XML only, no shredding) for Dashboard->Darling health-parser
                    parity. New table only — created by GetAllTableStatements() below; the v_ view comes
                    from CreateArchiveViewsAsync via ArchivableTables. */
            _logger?.LogInformation("Running migration to v40: adding system_health_events table");
        }

        if (fromVersion < 41)
        {
            /* v41: index_object_stats gains the per-index DEFINITION metadata monitor-side
                    UNUSED/DUPLICATE analysis needs (FinOps Index Analysis, Stage 1): the ordered
                    key_columns / included_columns lists (sp_IndexCleanup's delimited representation),
                    filter_definition, the uniqueness/constraint/FK discriminators + is_disabled, and
                    the reconstruct-a-CREATE options (data_compression_desc, optimize_for_sequential_key,
                    fill_factor, is_padded, allow_page_locks, allow_row_locks). All appended at the end
                    to keep the positional appender aligned; the collector now writes them, so an
                    un-migrated DB would mis-align on the next append — these ALTERs are required. The
                    v_ view (SELECT *) surfaces them and old parquet reads back NULL (union BY NAME). */
            _logger?.LogInformation("Running migration to v41: adding index_object_stats index-definition columns");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS key_columns VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS included_columns VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS filter_definition VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_unique_constraint BOOLEAN");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_foreign_key BOOLEAN");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_foreign_key_reference BOOLEAN");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_disabled BOOLEAN");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS data_compression_desc VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS optimize_for_sequential_key BOOLEAN");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS fill_factor SMALLINT");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_padded BOOLEAN");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS allow_page_locks BOOLEAN");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS allow_row_locks BOOLEAN");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_indexed_view BOOLEAN");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v41 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 42)
        {
            /* v42: server_properties gains utc_offset_minutes — the monitored server's UTC offset the
                    shared collector now writes (DATEDIFF(MINUTE, GETUTCDATE(), GETDATE())). Appended at
                    the end to keep the positional appender aligned; the collector now writes it, so an
                    un-migrated DB would mis-align on the next append — this ALTER is required. Nullable
                    (DuckDB has no ADD COLUMN NOT NULL); the offset is a stored fact, not a delta. */
            _logger?.LogInformation("Running migration to v42: adding utc_offset_minutes column to server_properties");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS utc_offset_minutes INTEGER");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v42 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 43)
        {
            /* v43: added default_trace_events (built-in Default Trace read via sys.fn_trace_gettable —
                    file auto-grow/shrink stalls, severe ErrorLog writes, schema DDL, security audits,
                    Server Memory Change) for Dashboard->shared parity. New table only — created by
                    GetAllTableStatements() below; the v_ view comes from CreateArchiveViewsAsync via
                    ArchivableTables. */
            _logger?.LogInformation("Running migration to v43: adding default_trace_events table");
        }

        if (fromVersion < 44)
        {
            /* v44: added job_history (retained SQL Agent job-run history from msdb.dbo.sysjobhistory —
                    every step row + the job-outcome row, deduped on the monotonic instance_id high-water
                    mark, 365-day retention) for the fleet-wide Job History tab (issue #1433). New table
                    only — created by GetAllTableStatements() below; the v_ view comes from
                    CreateArchiveViewsAsync via ArchivableTables. */
            _logger?.LogInformation("Running migration to v44: adding job_history table");
        }

        if (fromVersion < 45)
        {
            /* v45: added agent_status (SQL Agent service Running/Stopped from sys.dm_server_services +
                    next scheduled run from msdb.dbo.sysjobschedules) — the current-state snapshot behind the
                    Job History tab header (and Darling's "Agent Not Running" alert; Lite has no such alert of
                    its own — issue #1433 Phase 2). New table
                    only — created by GetAllTableStatements() below; the v_ view comes from
                    CreateArchiveViewsAsync via ArchivableTables. */
            _logger?.LogInformation("Running migration to v45: adding agent_status table");
        }

        if (fromVersion < 46)
        {
            /* v46: deadlocks.database_name — the victim process's currentdbname, keying the Azure SQL DB
                    per-database watermark (#1535: capture is now one database-scoped session per monitored
                    database). Appended at the end to keep the positional appender aligned; the collector
                    writes it unconditionally (null when the graph carries no currentdbname), so an
                    un-migrated DB would mis-align on the next append — this ALTER is required. The v_ view
                    (SELECT *) is rebuilt every startup and picks it up; old parquet reads back NULL (union
                    BY NAME). blocked_process_reports already had database_name. */
            _logger?.LogInformation("Running migration to v46: deadlocks database_name column");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE deadlocks ADD COLUMN IF NOT EXISTS database_name VARCHAR");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v46 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 47)
        {
            /* v47: query_store_stats.replica_role — the replica role SQL Server 2022+ attributed each
                    runtime-stats row to (sys.query_store_replicas.replica_name). With "Query Store for
                    secondary replicas" on, an AG has ONE shared Query Store living on the primary, so the
                    primary's rows silently blend in secondary workload unless split by replica. Appended at
                    the end to keep the positional appender aligned; the collector writes it unconditionally
                    (NULL pre-2022, and NULL on a 2022 standalone whose sys.query_store_replicas is empty),
                    so an un-migrated DB would mis-align on the next append — this ALTER is required. The v_
                    view (SELECT *) is rebuilt every startup and picks it up; old parquet reads back NULL
                    (union BY NAME). */
            _logger?.LogInformation("Running migration to v47: query_store_stats replica_role column");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_store_stats ADD COLUMN IF NOT EXISTS replica_role VARCHAR");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v47 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 48)
        {
            /* v48: drop NOT NULL from server_properties.cpu_count / hyperthread_ratio /
                    physical_memory_mb (#1591). Those three are the only columns in the collector
                    sourced from sys.dm_os_sys_info, which needs VIEW SERVER STATE (VIEW DATABASE
                    STATE on Azure SQL DB). The collector now reads them in a TRY/CATCH so a login
                    without that grant keeps every permission-free column instead of losing the whole
                    row — but that only helps if the column can actually hold NULL, so an existing
                    database has to have the constraint dropped. New databases get it from the
                    generator. Column types and ordinals are unchanged, so the positional appender
                    and old parquet are unaffected. */
            _logger?.LogInformation("Running migration to v48: server_properties hardware columns become nullable");
            foreach (var column in new[] { "cpu_count", "hyperthread_ratio", "physical_memory_mb" })
            {
                try
                {
                    await ExecuteNonQueryAsync(connection, $"ALTER TABLE server_properties ALTER COLUMN {column} DROP NOT NULL");
                }
                catch (Exception ex)
                {
                    /* Already nullable, or the table does not exist yet (fresh install creates it
                       correctly from the generator) — neither is fatal. */
                    _logger?.LogWarning("Migration to v48 on {Column} encountered an error (non-fatal): {Error}", column, ex.Message);
                }
            }
        }

        if (fromVersion < 49)
        {
            /* v49: query_store_stats gains the REAL Query Store interval identity (#1841 tier 2) —
                    runtime_stats_interval_id + interval_start_time_utc. The rows are cumulative
                    per-interval snapshots and the collector re-fetches the OPEN interval every cycle, so
                    every aggregate read has to collapse an interval to its latest snapshot before summing;
                    until now the only identity in the schema was the first_execution_time PROXY, and the
                    only time axis was collection_time (the cycle that last FETCHED an interval, reliably
                    one bucket after the one it ran in on Query Store's default 60-minute interval).

                    Both appended at the end to keep the positional appender aligned; the collector writes
                    them unconditionally, so an un-migrated database would mis-align on the next append —
                    this ALTER is required, not cosmetic. Nullable and NOT backfilled: rows already in the
                    store were collected without the identity and nothing can reconstruct it, so every
                    reader keys on the real id only WHEN PRESENT and falls back to the proxy otherwise. The
                    v_ view (SELECT *) is rebuilt every startup and picks them up; old parquet reads back
                    NULL (union BY NAME). */
            _logger?.LogInformation("Running migration to v49: query_store_stats interval identity columns");
            foreach (var column in new[] { "runtime_stats_interval_id BIGINT", "interval_start_time_utc TIMESTAMP" })
            {
                try
                {
                    await ExecuteNonQueryAsync(connection, $"ALTER TABLE query_store_stats ADD COLUMN IF NOT EXISTS {column}");
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning("Migration to v49 on {Column} encountered an error (non-fatal): {Error}", column, ex.Message);
                }
            }
        }

        if (fromVersion < 50)
        {
            /* v50: added plan_correction (automatic plan correction — per-database FORCE_LAST_GOOD_PLAN
                    enablement from sys.database_automatic_tuning_options, plus the engine's live
                    recommendation set from sys.dm_db_tuning_recommendations with the regressed query's
                    text resolved through Query Store at collection time; issue #1952). New table only —
                    created by GetAllTableStatements() below; the v_ view comes from
                    CreateArchiveViewsAsync via ArchivableTables. */
            _logger?.LogInformation("Running migration to v50: adding plan_correction table");
        }

        if (fromVersion < 51)
        {
            /* v51: added pvs_stats (Accelerated Database Recovery persistent version store size and
                    cleanup state per database from sys.dm_tran_persistent_version_store_stats, SQL
                    Server 2019+; issue #1951). New table only — created by GetAllTableStatements()
                    below; the v_pvs_stats view the FinOps grid reads comes from CreateArchiveViewsAsync
                    via ArchivableTables, which is derived from CollectorCatalog. */
            _logger?.LogInformation("Running migration to v51: adding pvs_stats table");
        }

        if (fromVersion < 52)
        {
            /* v52 (#2012 stage 2): the statement's HOST OBJECT on query_stats — dm_exec_sql_text.objectid
               resolved to schema.name at collection, NULL for ad-hoc/prepared text. Splits INSERT...EXEC
               callers that share a query_hash in the hash-grouped readers; history rows stay NULL and age
               out with retention. Appended last to match the collector's append-only payload; the
               v_query_stats archive union re-derives per start with union_by_name, so no view work. */
            _logger?.LogInformation("Running migration to v52: adding host_object_name to query_stats");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_stats ADD COLUMN IF NOT EXISTS host_object_name VARCHAR");
            }
            catch
            {
                /* Table doesn't exist yet — will be created with the full schema below */
            }
        }

        if (fromVersion < 53)
        {
            /* v53 (#2203): the database-state alert's edge-trigger memory, porting Darling's V60 pair.
               Without it Lite's alreadyAnnounced is always false, so a database parked OFFLINE for a month
               alerts every cooldown forever - the original #2166 complaint, still live in Lite after the
               Darling half shipped. Nullable on purpose: NULL means "never announced", which is what a
               first observation, a fresh store and a recovered database all look like. */
            _logger?.LogInformation("Running migration to v53: adding the alerted-state memory to config_database_state_expected");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE config_database_state_expected ADD COLUMN IF NOT EXISTS last_alerted_state VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE config_database_state_expected ADD COLUMN IF NOT EXISTS last_alerted_at TIMESTAMP");
            }
            catch
            {
                /* Table doesn't exist yet — will be created with the full schema below */
            }
        }

        if (fromVersion < 54)
        {
            /* v54 (#2216): the per-fingerprint occurrence counters, porting Darling's V61. The count on an
               alert incident is a rolling-window gauge, so a consumer receiving only throttled deliveries
               cannot recover the true total from a sequence of readings. New table only — fresh installs
               get it from GetAllTableStatements(); this CREATE is for an existing database, and it is
               idempotent so a re-run is a no-op. Nothing to backfill: an absent row means "no incident in
               flight for this fingerprint", which is what every fingerprint looks like before the feature
               existed, so the first delivery after the upgrade opens an incident and counts from there. */
            _logger?.LogInformation("Running migration to v54: adding config_incident_occurrences");
            try
            {
                await ExecuteNonQueryAsync(connection, Schema.CreateIncidentOccurrencesTable);
            }
            catch (Exception ex)
            {
                /* Non-fatal, matching v31's posture: without the table the store's load returns empty and
                   the accumulator degrades to reporting the total as the window count — the pre-#2216
                   information rather than a broken alert path. */
                _logger?.LogWarning("Migration to v54 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }
    }

    /// <summary>
    /// Fixes server_id values in all tables by recomputing from server_name using the
    /// deterministic hash function. Previous versions used string.GetHashCode() which
    /// is randomized per process in .NET Core.
    /// </summary>
    private async Task FixServerIdsAsync(DuckDBConnection connection)
    {
        var tablesWithServerId = new[]
        {
            "servers", "collection_log", "wait_stats", "query_stats", "cpu_utilization_stats",
            "file_io_stats", "memory_stats", "memory_clerks", "memory_pressure_events",
            "deadlocks", "procedure_stats", "query_store_stats", "query_snapshots",
            "tempdb_stats", "perfmon_stats", "server_config", "database_config",
            "blocked_process_reports", "memory_grant_stats", "waiting_tasks"
        };

        foreach (var table in tablesWithServerId)
        {
            try
            {
                /* Get distinct server_name values from this table */
                using var queryCmd = connection.CreateCommand();
                queryCmd.CommandText = $"SELECT DISTINCT server_name FROM {table}";
                var serverNames = new List<string>();
                using (var reader = await queryCmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        if (!reader.IsDBNull(0))
                            serverNames.Add(reader.GetString(0));
                    }
                }

                /* Update server_id for each server_name */
                foreach (var serverName in serverNames)
                {
                    var newId = Services.RemoteCollectorService.GetDeterministicHashCode(serverName);
                    using var updateCmd = connection.CreateCommand();
                    updateCmd.CommandText = $"UPDATE {table} SET server_id = $1 WHERE server_name = $2";
                    updateCmd.Parameters.Add(new DuckDBParameter { Value = newId });
                    updateCmd.Parameters.Add(new DuckDBParameter { Value = serverName });
                    await updateCmd.ExecuteNonQueryAsync();
                }

                if (serverNames.Count > 0)
                    _logger?.LogInformation("Fixed server_id in {Table} for {Count} server(s)", table, serverNames.Count);
            }
            catch (Exception ex)
            {
                /* Table might not exist yet — that's fine, it will be created with correct IDs */
                _logger?.LogDebug(ex, "Skipped server_id fix for {Table} (may not exist yet)", table);
            }
        }
    }

    /// <summary>
    /// Creates a new connection to the database.
    /// </summary>
    public DuckDBConnection CreateConnection()
    {
        return new DuckDBConnection(ConnectionString);
    }

    /// <summary>
    /// Creates or refreshes views that UNION hot DuckDB tables with archived parquet files.
    /// Call at startup and after each archive cycle so newly archived data is queryable.
    /// </summary>
    public async Task CreateArchiveViewsAsync()
    {
        using var connection = CreateConnection();
        await connection.OpenAsync();

        /* This fresh connection must see every table InitializeAsync just created. If it sees none,
           the reset left an empty database — surface it loudly rather than only failing per-table below. */
        using (var tableCountCmd = connection.CreateCommand())
        {
            tableCountCmd.CommandText = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main'";
            var tableCount = Convert.ToInt64(await tableCountCmd.ExecuteScalarAsync());
            if (tableCount == 0)
                _logger?.LogError("Archive-view refresh opened a database with no tables — the reset did not persist the schema; collectors will fail until restart");
            else
                _logger?.LogInformation("Archive-view refresh sees {Count} tables", tableCount);
        }

        foreach (var table in ArchivableTables)
        {
            try
            {
                var parquetGlob = Path.Combine(_archivePath, $"*_{table}.parquet");
                var hasParquetFiles = Directory.Exists(_archivePath)
                    && Directory.GetFiles(_archivePath, $"*_{table}.parquet").Length > 0;

                string viewSql;
                if (hasParquetFiles)
                {
                    var globPath = EscapeSqlPath(parquetGlob.Replace("\\", "/"));
                    if (table == "config_alert_log")
                    {
                        viewSql = $@"CREATE OR REPLACE VIEW v_{table} AS
SELECT *, 'live' AS source FROM {table}
UNION ALL BY NAME
SELECT *, 'archive' AS source FROM read_parquet('{globPath}', union_by_name=true) p
WHERE NOT EXISTS (
    SELECT 1 FROM dismissed_archive_alerts d
    WHERE d.alert_time = p.alert_time
    AND   d.server_id  = p.server_id
    AND   d.metric_name = p.metric_name
)";
                    }
                    else if (ArchiveViewDedupKeys.TryGetValue(table, out var dedupKey))
                    {
                        /* Dedup the hot∪parquet union on the server-side natural key so a logical event that was
                           re-collected after the 512MB emergency reset (still present in parquet) appears exactly
                           once. QUALIFY keeps the newest-collected copy — the re-collected hot row outranks its
                           archived parquet twin (identical content either way). */
                        viewSql = $@"CREATE OR REPLACE VIEW v_{table} AS
SELECT *
FROM
(
    SELECT * FROM {table}
    UNION ALL BY NAME
    SELECT * FROM read_parquet('{globPath}', union_by_name=true)
)
QUALIFY ROW_NUMBER() OVER (PARTITION BY {dedupKey} ORDER BY collection_time DESC) = 1";
                    }
                    else
                    {
                        viewSql = $"CREATE OR REPLACE VIEW v_{table} AS SELECT * FROM {table} UNION ALL BY NAME SELECT * FROM read_parquet('{globPath}', union_by_name=true)";
                    }
                }
                else
                {
                    if (table == "config_alert_log")
                        viewSql = $"CREATE OR REPLACE VIEW v_{table} AS SELECT *, 'live' AS source FROM {table}";
                    else
                        viewSql = $"CREATE OR REPLACE VIEW v_{table} AS SELECT * FROM {table}";
                }

                using var cmd = connection.CreateCommand();
                cmd.CommandText = viewSql;
                await cmd.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                /* Schema mismatch between hot table and old parquet — fall back to table-only view */
                _logger?.LogWarning(ex, "Failed to create archive view for {Table}, using table-only view", table);
                try
                {
                    using var fallbackCmd = connection.CreateCommand();
                    if (table == "config_alert_log")
                        fallbackCmd.CommandText = $"CREATE OR REPLACE VIEW v_{table} AS SELECT *, 'live' AS source FROM {table}";
                    else
                        fallbackCmd.CommandText = $"CREATE OR REPLACE VIEW v_{table} AS SELECT * FROM {table}";
                    await fallbackCmd.ExecuteNonQueryAsync();
                }
                catch (Exception fallbackEx)
                {
                    _logger?.LogError(fallbackEx, "Failed to create fallback view for {Table}", table);
                }
            }
        }

        _logger?.LogDebug("Archive views created/refreshed for {Count} tables", ArchivableTables.Length);
    }

    /// <summary>
    /// Initializes the analysis engine schema (separate version track from main schema).
    /// Only called when App.AnalysisEnabled is true.
    /// Internal for test access.
    /// </summary>
    internal async Task InitializeAnalysisSchemaAsync()
    {
        using var connection = CreateConnection();
        await connection.OpenAsync();

        await ExecuteNonQueryAsync(connection,
            "CREATE TABLE IF NOT EXISTS analysis_schema_version (version INTEGER NOT NULL)");

        var existingVersion = 0;
        try
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT COALESCE(MAX(version), 0) FROM analysis_schema_version";
            var result = await cmd.ExecuteScalarAsync();
            existingVersion = Convert.ToInt32(result);
        }
        catch { /* Table doesn't exist yet */ }

        foreach (var tableStatement in AnalysisSchema.GetAllTableStatements())
        {
            await ExecuteNonQueryAsync(connection, tableStatement);
        }

        foreach (var indexStatement in AnalysisSchema.GetAllIndexStatements())
        {
            await ExecuteNonQueryAsync(connection, indexStatement);
        }

        if (existingVersion < AnalysisSchema.CurrentVersion)
        {
            // Run migrations for version upgrades
            foreach (var migration in AnalysisSchema.GetMigrationStatements(existingVersion))
            {
                try { await ExecuteNonQueryAsync(connection, migration); }
                catch { /* Column/table may already exist */ }
            }

            await ExecuteNonQueryAsync(connection, "DELETE FROM analysis_schema_version");
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "INSERT INTO analysis_schema_version (version) VALUES ($1)";
            cmd.Parameters.Add(new DuckDBParameter { Value = AnalysisSchema.CurrentVersion });
            await cmd.ExecuteNonQueryAsync();
            _logger?.LogInformation("Analysis schema initialized at version {Version}", AnalysisSchema.CurrentVersion);
        }
    }

    /// <summary>
    /// Executes a non-query SQL statement.
    /// </summary>
    private async Task ExecuteNonQueryAsync(DuckDBConnection connection, string sql)
    {
        try
        {
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failed to execute SQL: {Sql}", sql.Substring(0, Math.Min(100, sql.Length)));
            throw;
        }
    }

    /// <summary>
    /// Checks if the database file exists.
    /// </summary>
    private bool DatabaseExists()
    {
        return File.Exists(_databasePath);
    }

    /// <summary>
    /// Gets the database file size in megabytes.
    /// </summary>
    public double GetDatabaseSizeMb()
    {
        if (!DatabaseExists())
        {
            return 0;
        }

        var fileInfo = new FileInfo(_databasePath);
        return fileInfo.Length / (1024.0 * 1024.0);
    }

    /// <summary>
    /// Gets the actual used data size inside the database by querying pragma_database_size().
    /// Returns null if the query fails (e.g., database busy).
    /// </summary>
    public double? GetUsedDataSizeMb()
    {
        try
        {
            using var connection = CreateConnection();
            connection.Open();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT (used_blocks * block_size)::BIGINT FROM pragma_database_size()";
            var result = cmd.ExecuteScalar();
            if (result != null && result != DBNull.Value)
            {
                return Convert.ToInt64(result) / (1024.0 * 1024.0);
            }
        }
        catch
        {
            /* Database may be busy — fall back to null */
        }
        return null;
    }

    /// <summary>
    /// Deletes the database and WAL files, then reinitializes with fresh empty tables
    /// and archive views pointing at the parquet files.
    /// Acquires its own write lock — caller must NOT already hold the lock.
    /// </summary>
    public async Task ResetDatabaseAsync()
    {
        using var writeLock = AcquireWriteLock();

        if (File.Exists(_databasePath))
            File.Delete(_databasePath);

        var walPath = _databasePath + ".wal";
        if (File.Exists(walPath))
            File.Delete(walPath);

        _logger?.LogInformation("Database files deleted, reinitializing");
        await InitializeAsync();
    }

    /// <summary>
    /// Escapes single quotes in a file path for safe interpolation into DuckDB SQL.
    /// DuckDB does not support parameterized paths in read_parquet() or COPY TO.
    /// </summary>
    internal static string EscapeSqlPath(string path) => path.Replace("'", "''");
}
