using System;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #2748: a real user's database, aged past v47, failed both the v48 and v53 migrations on upgrade
/// to v3.6.0.0. Both errors were logged as "non-fatal" and the migration chain nominally completed to
/// v56, but the app then failed to start — the two swallowed failures left the database missing
/// state later code assumes is there. These tests seed the exact aged-database preconditions that
/// triggered each failure and assert the upgrade both succeeds AND leaves the database actually
/// correct, not just quietly incomplete.
/// </summary>
public class AgedDatabaseMigrationTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;

    public AgedDatabaseMigrationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    /// <summary>
    /// v48 drops NOT NULL from three server_properties columns. On any database that has completed a
    /// prior startup, Schema.GetAllIndexStatements() already created idx_server_properties_time — a
    /// real, persisted index on (server_id, collection_time) — and DuckDB's ALTER COLUMN dependency
    /// check refuses to touch a table with ANY index on it, even one naming none of the altered columns
    /// (confirmed empirically: a plain SELECT * archive view does NOT trigger this, only the index
    /// does). Seeds that exact precondition and asserts the upgrade both completes AND the column is
    /// actually nullable afterward, not merely that it didn't throw.
    /// </summary>
    [Fact]
    public async Task UpgradeFromV47_DropsServerPropertiesNotNull_EvenWithAPreExistingIndex()
    {
        using (var seed = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await seed.OpenAsync();
            await ExecAsync(seed, "CREATE TABLE schema_version (version INTEGER NOT NULL)");
            await ExecAsync(seed, "INSERT INTO schema_version VALUES (47)");
            await ExecAsync(seed, @"CREATE TABLE server_properties (
                server_id INTEGER NOT NULL,
                collection_time TIMESTAMP NOT NULL,
                cpu_count INTEGER NOT NULL,
                hyperthread_ratio INTEGER NOT NULL,
                physical_memory_mb BIGINT NOT NULL
            )");
            await ExecAsync(seed, "INSERT INTO server_properties VALUES (1, current_timestamp, 4, 1, 16384)");
            /* The real dependent object: DuckDbSchemaGenerator.CreateIndex's default case for any
               collector table, including server_properties, is exactly this index/column shape. */
            await ExecAsync(seed, "CREATE INDEX idx_server_properties_time ON server_properties(server_id, collection_time)");
        }

        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        using var verify = new DuckDBConnection($"Data Source={_dbPath}");
        await verify.OpenAsync();

        /* The real assertion: a permission-free collector row (NULL hardware columns) must actually be
           insertable now. Before the fix, the dependency error silently left the NOT NULL constraint in
           place, so this insert would throw — the exact failure mode #2748's v48 fix exists to prevent. */
        await ExecAsync(verify, "INSERT INTO server_properties VALUES (2, current_timestamp, NULL, NULL, NULL)");

        using var countCmd = verify.CreateCommand();
        countCmd.CommandText = "SELECT COUNT(*) FROM server_properties WHERE server_id = 2 AND cpu_count IS NULL";
        Assert.Equal(1L, Convert.ToInt64(await countCmd.ExecuteScalarAsync()));
    }

    /// <summary>
    /// v53 adds two columns to config_database_state_expected via ALTER TABLE. That table was never
    /// given its own numbered migration — it only exists because Schema.GetAllTableStatements()
    /// unconditionally creates it, which does not run until AFTER migrations. A database old enough to
    /// predate the table (introduced by #2166/#2203, well after v47) hits the ALTER before the table
    /// exists at all. Seeds a v47 database with NO config_database_state_expected table, and asserts
    /// the upgrade both completes AND the table exists afterward with both new columns present and
    /// actually writable.
    /// </summary>
    [Fact]
    public async Task UpgradeFromV47_CreatesConfigDatabaseStateExpected_WhenItPredatesTheTable()
    {
        using (var seed = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await seed.OpenAsync();
            await ExecAsync(seed, "CREATE TABLE schema_version (version INTEGER NOT NULL)");
            await ExecAsync(seed, "INSERT INTO schema_version VALUES (47)");
            /* Deliberately absent: config_database_state_expected. #2748's real-world database was old
               enough that this table had never been created — that is the entire bug. */
        }

        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        using var verify = new DuckDBConnection($"Data Source={_dbPath}");
        await verify.OpenAsync();

        /* The real assertion: the columns v53 exists to add must actually be writable afterward — this
           is what DuckDbAlertHistoryStore's UPDATE (the database-state alert's edge-trigger memory)
           depends on, and what #2748's user's app crashed trying to do. */
        await ExecAsync(verify,
            "INSERT INTO config_database_state_expected (server_id, database_name, expected_state, last_alerted_state, last_alerted_at) " +
            "VALUES (1, 'TestDb', 'ONLINE', 'ONLINE', current_timestamp)");

        using var countCmd = verify.CreateCommand();
        countCmd.CommandText = "SELECT COUNT(*) FROM config_database_state_expected WHERE server_id = 1 AND database_name = 'TestDb'";
        Assert.Equal(1L, Convert.ToInt64(await countCmd.ExecuteScalarAsync()));
    }

    private static async Task ExecAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }
}
