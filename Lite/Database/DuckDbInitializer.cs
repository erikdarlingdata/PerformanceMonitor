using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;

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
    /// </summary>
    public IDisposable AcquireReadLock()
    {
        s_dbLock.EnterReadLock();
        return new LockReleaser(s_dbLock, write: false);
    }

    /// <summary>
    /// Acquires an exclusive write lock on the database. Blocks until all readers finish.
    /// Dispose the returned object to release the lock.
    /// </summary>
    public IDisposable AcquireWriteLock()
    {
        s_dbLock.EnterWriteLock();
        return new LockReleaser(s_dbLock, write: true);
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
    internal const int CurrentSchemaVersion = 12;

    private readonly string _archivePath;

    public DuckDbInitializer(string databasePath, ILogger<DuckDbInitializer>? logger = null)
    {
        _databasePath = databasePath;
        _logger = logger;
        _archivePath = Path.Combine(Path.GetDirectoryName(databasePath) ?? ".", "archive");
    }

    /* Tables that have parquet archives — views are created to UNION hot data with archived parquet files */
    private static readonly string[] ArchivableTables =
    [
        "wait_stats", "query_stats", "procedure_stats", "query_store_stats",
        "query_snapshots", "cpu_utilization_stats", "file_io_stats", "memory_stats",
        "memory_clerks", "tempdb_stats", "perfmon_stats", "deadlocks",
        "blocked_process_reports", "collection_log"
    ];

    /// <summary>
    /// Gets the connection string for the DuckDB database.
    /// Disables automatic WAL checkpoints to prevent 2-3s stop-the-world stalls
    /// during collector writes. Manual CHECKPOINT runs between collection cycles instead.
    /// </summary>
    public string ConnectionString => $"Data Source={_databasePath};checkpoint_threshold=1GB";

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

        /* Try to open the database. If the DuckDB storage version has changed,
           this will throw. We handle it by exporting to Parquet, rebuilding, and importing. */
        DuckDBConnection connection;
        try
        {
            connection = new DuckDBConnection(ConnectionString);
            await connection.OpenAsync();
        }
        catch (Exception ex) when (IsStorageVersionError(ex))
        {
            _logger?.LogWarning("DuckDB storage version mismatch detected. Migrating data via Parquet export/import.");
            await MigrateViaParquetAsync(archivePath);

            connection = new DuckDBConnection(ConnectionString);
            await connection.OpenAsync();
        }

        using (connection)
        {
            await ExecuteNonQueryAsync(connection,
                "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)");

            var existingVersion = await GetSchemaVersionAsync(connection);

            if (existingVersion < CurrentSchemaVersion)
            {
                _logger?.LogInformation("Schema upgrade needed: v{Old} -> v{New}", existingVersion, CurrentSchemaVersion);
                await RunMigrationsAsync(connection, existingVersion);
                await SetSchemaVersionAsync(connection, CurrentSchemaVersion);
            }

            foreach (var tableStatement in Schema.GetAllTableStatements())
            {
                await ExecuteNonQueryAsync(connection, tableStatement);
            }

            foreach (var indexStatement in Schema.GetAllIndexStatements())
            {
                await ExecuteNonQueryAsync(connection, indexStatement);
            }

            _logger?.LogInformation("Database initialization complete. Schema version: {Version}", CurrentSchemaVersion);
        }

        await CreateArchiveViewsAsync();
    }

    /// <summary>
    /// Checks if an exception is a DuckDB storage version mismatch.
    /// </summary>
    private static bool IsStorageVersionError(Exception ex)
    {
        /* DuckDB version mismatch errors include:
           - "Serialization Error: Failed to deserialize" (incompatible storage format)
           - "IO Error: Trying to read a database file with version number X, but we can only read version Y"
           Note: Since DuckDB v0.10+, backward compatibility is maintained (newer reads older).
           This primarily catches forward-incompatibility (older library, newer file). */
        var message = ex.ToString().ToLowerInvariant();
        return message.Contains("serialization error")
            || message.Contains("failed to deserialize")
            || message.Contains("trying to read a database file with version")
            || message.Contains("storage version")
            || message.Contains("unable to open database");
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
            await ExecuteNonQueryAsync(connection, Schema.CreateDatabaseScopedConfigTable);
            await ExecuteNonQueryAsync(connection, Schema.CreateDatabaseScopedConfigIndex);
            await ExecuteNonQueryAsync(connection, Schema.CreateTraceFlagsTable);
            await ExecuteNonQueryAsync(connection, Schema.CreateTraceFlagsIndex);
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
            "file_io_stats", "memory_stats", "memory_clerks", "deadlocks",
            "procedure_stats", "query_store_stats", "query_snapshots", "tempdb_stats",
            "perfmon_stats", "server_config", "database_config",
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
                    var globPath = parquetGlob.Replace("\\", "/");
                    viewSql = $"CREATE OR REPLACE VIEW v_{table} AS SELECT * FROM {table} UNION ALL BY NAME SELECT * FROM read_parquet('{globPath}', union_by_name=true)";
                }
                else
                {
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
    /// Runs a manual WAL checkpoint. Call this between collection cycles
    /// to flush the WAL during idle time instead of during collector writes.
    /// </summary>
    public async Task CheckpointAsync()
    {
        using var writeLock = AcquireWriteLock();
        try
        {
            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var command = connection.CreateCommand();
            command.CommandText = "CHECKPOINT";
            await command.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Manual checkpoint failed (non-critical)");
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
    public bool DatabaseExists()
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


}
