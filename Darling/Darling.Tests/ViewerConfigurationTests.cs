/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Configuration-tab reads (W1g viewer copy-parity) against the Darling store contract, no
/// live Postgres. Each of the four reads is a latest-snapshot query — the newest capture is chosen in
/// SQL via <c>capture_time = (SELECT MAX(capture_time) ...)</c> against a config passthrough view — so
/// the tests pin the SELECT column lists (the 28-column database-config order is load-bearing: the
/// reader maps by incrementing ordinal), the MAX(capture_time) shape, and that every selected column
/// exists in the collector-generated table. Config reads take only <c>server_id</c> ($1); unlike the
/// windowed reads there is no <c>$2</c> time bound.
/// </summary>
public sealed class ViewerConfigurationSqlTests
{
    /* The 28 database-config columns in Lite's exact SELECT order — the database-config reader maps
       them by incrementing ordinal, so this contiguous order is the load-bearing contract. */
    private static readonly string[] DatabaseConfigColumns =
    {
        "database_name", "state_desc", "compatibility_level", "collation_name", "recovery_model",
        "is_read_only", "is_auto_close_on", "is_auto_shrink_on",
        "is_auto_create_stats_on", "is_auto_update_stats_on", "is_auto_update_stats_async_on",
        "is_read_committed_snapshot_on", "snapshot_isolation_state", "is_parameterization_forced",
        "is_query_store_on", "is_encrypted", "is_trustworthy_on", "is_db_chaining_on",
        "is_broker_enabled", "is_cdc_enabled", "is_mixed_page_allocation_on",
        "log_reuse_wait_desc", "page_verify_option", "target_recovery_time_seconds", "delayed_durability",
        "is_accelerated_database_recovery_on", "is_memory_optimized_enabled", "is_optimized_locking_on",
    };

    private static readonly string[] ServerConfigColumns =
        { "configuration_name", "value_configured", "value_in_use", "is_dynamic", "is_advanced" };

    private static readonly string[] ScopedConfigColumns =
        { "database_name", "configuration_name", "value", "value_for_secondary" };

    private static readonly string[] TraceFlagColumns =
        { "trace_flag", "status", "is_global", "is_session" };

    private static string StripWhitespace(string sql) =>
        new string(sql.Where(c => !char.IsWhiteSpace(c)).ToArray());

    [Fact]
    public void ServerConfigSql_SelectsSnapshotColumns_FromView_LatestCapture()
    {
        var sql = ViewerDataService.ServerConfigSql;
        Assert.Contains("FROM v_server_config", sql, StringComparison.Ordinal);
        Assert.Contains("configuration_name,value_configured,value_in_use,is_dynamic,is_advanced",
            StripWhitespace(sql), StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("capture_time = (SELECT MAX(capture_time) FROM v_server_config WHERE server_id = $1)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY configuration_name", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DatabaseConfigSql_Pins28ColumnOrdinalOrder_FromView_LatestCapture()
    {
        var sql = ViewerDataService.DatabaseConfigSql;
        Assert.Contains("FROM v_database_config", sql, StringComparison.Ordinal);

        /* Whitespace-stripped contiguous order: a reordered or dropped column would shift the
           incrementing-ordinal reader onto the wrong property. */
        Assert.Contains(string.Join(",", DatabaseConfigColumns), StripWhitespace(sql), StringComparison.Ordinal);

        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("capture_time = (SELECT MAX(capture_time) FROM v_database_config WHERE server_id = $1)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY database_name", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DatabaseScopedConfigSql_SelectsSnapshotColumns_FromView_LatestCapture()
    {
        var sql = ViewerDataService.DatabaseScopedConfigSql;
        Assert.Contains("FROM v_database_scoped_config", sql, StringComparison.Ordinal);
        Assert.Contains(string.Join(",", ScopedConfigColumns), StripWhitespace(sql), StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("capture_time = (SELECT MAX(capture_time) FROM v_database_scoped_config WHERE server_id = $1)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY database_name, configuration_name", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TraceFlagsSql_SelectsSnapshotColumns_FromView_LatestCapture()
    {
        var sql = ViewerDataService.TraceFlagsSql;
        Assert.Contains("FROM v_trace_flags", sql, StringComparison.Ordinal);
        Assert.Contains(string.Join(",", TraceFlagColumns), StripWhitespace(sql), StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("capture_time = (SELECT MAX(capture_time) FROM v_trace_flags WHERE server_id = $1)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY trace_flag", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ServerConfigSql_ReadsColumnsThatExistInTheGeneratedTable()
    {
        Assert.Equal("server_config", ServerConfigCollector.Instance.TargetTable);
        var ddl = PgSchemaGenerator.CreateTable(ServerConfigCollector.Instance);
        Assert.Contains("capture_time", ddl, StringComparison.Ordinal);
        foreach (var column in ServerConfigColumns)
        {
            Assert.Contains(column, ddl, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void DatabaseConfigSql_ReadsColumnsThatExistInTheGeneratedTable()
    {
        Assert.Equal("database_config", DatabaseConfigCollector.Instance.TargetTable);
        var ddl = PgSchemaGenerator.CreateTable(DatabaseConfigCollector.Instance);
        Assert.Contains("capture_time", ddl, StringComparison.Ordinal);
        foreach (var column in DatabaseConfigColumns)
        {
            Assert.Contains(column, ddl, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void DatabaseScopedConfigSql_ReadsColumnsThatExistInTheGeneratedTable()
    {
        Assert.Equal("database_scoped_config", DatabaseScopedConfigCollector.Instance.TargetTable);
        var ddl = PgSchemaGenerator.CreateTable(DatabaseScopedConfigCollector.Instance);
        Assert.Contains("capture_time", ddl, StringComparison.Ordinal);
        foreach (var column in ScopedConfigColumns)
        {
            Assert.Contains(column, ddl, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TraceFlagsSql_ReadsColumnsThatExistInTheGeneratedTable()
    {
        Assert.Equal("trace_flags", TraceFlagsCollector.Instance.TargetTable);
        var ddl = PgSchemaGenerator.CreateTable(TraceFlagsCollector.Instance);
        Assert.Contains("capture_time", ddl, StringComparison.Ordinal);
        foreach (var column in TraceFlagColumns)
        {
            Assert.Contains(column, ddl, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("server")]
    [InlineData("database")]
    [InlineData("scoped")]
    [InlineData("traceflags")]
    public void ConfigReads_PgDialect_ServerIdOnly_NoWindowParam_NoBareNow_NoNLiterals(string which)
    {
        var sql = which switch
        {
            "server" => ViewerDataService.ServerConfigSql,
            "database" => ViewerDataService.DatabaseConfigSql,
            "scoped" => ViewerDataService.DatabaseScopedConfigSql,
            _ => ViewerDataService.TraceFlagsSql,
        };

        Assert.DoesNotContain("now(", sql.ToLowerInvariant());
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.Contains("$1", sql, StringComparison.Ordinal);
        if (which is "database" or "scoped")
        {
            /* #1319: the database-scoped config reads gain the global database filter — a single nullable
               text[] param ($2) applied as database_name = ANY($2). It is NOT a time window (the
               latest-snapshot capture_time subquery still reuses $1). */
            Assert.Contains("database_name = ANY($2)", sql, StringComparison.Ordinal);
        }
        else
        {
            /* Server-config + trace-flag reads are server-wide (not database-scoped) — server_id ($1) only. */
            Assert.DoesNotContain("$2", sql, StringComparison.Ordinal);
        }
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the four Configuration reads. Each plants two
/// capture_time snapshots (an older and a newer) for a negative sentinel server and asserts the read
/// returns only the newest snapshot's rows, in the read's ORDER BY. The database-config test sets all
/// 28 payload columns to distinct alternating values so the incrementing-ordinal mapping is verified
/// end-to-end (an off-by-one would flip an asserted value). Shares the serialized "live-postgres"
/// collection and cleans up in finally.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerConfigurationLivePostgresTests
{
    private const int ServerConfigServerId = -970701;
    private const int DatabaseConfigServerId = -970702;
    private const int ScopedConfigServerId = -970703;
    private const int TraceFlagsServerId = -970704;

    [Fact]
    public async Task ServerConfig_LatestCaptureWins_OrderedByName_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live server-config test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "server_config", ServerConfigServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var older = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));
            var newer = older.AddMinutes(5);

            /* Older capture — must be shadowed by the newer one. */
            await InsertServerConfigAsync(connection, older, "max degree of parallelism", 0, 0, true, true);
            /* Newer capture — the snapshot the read returns. */
            await InsertServerConfigAsync(connection, newer, "max degree of parallelism", 8, 4, true, false);
            await InsertServerConfigAsync(connection, newer, "cost threshold for parallelism", 50, 50, true, false);

            var rows = await viewer.GetLatestServerConfigAsync(ServerConfigServerId);

            /* Only the newer capture's two rows, ordered by configuration_name. */
            Assert.Equal(2, rows.Count);
            Assert.Equal(new[] { "cost threshold for parallelism", "max degree of parallelism" },
                rows.Select(r => r.ConfigurationName));

            var maxdop = rows.Single(r => r.ConfigurationName == "max degree of parallelism");
            Assert.Equal(8, maxdop.ValueConfigured);
            Assert.Equal(4, maxdop.ValueInUse);
            Assert.False(maxdop.ValuesMatch);
            Assert.Equal("No", maxdop.AdvancedDisplay);
            Assert.Equal("Yes", maxdop.DynamicDisplay);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "server_config", ServerConfigServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task DatabaseConfig_LatestCaptureWins_All28ColumnsMapByOrdinal_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live database-config test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "database_config", DatabaseConfigServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var older = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));
            var newer = older.AddMinutes(5);

            /* Older capture with a different database name — must be shadowed. */
            await InsertDatabaseConfigOldAsync(connection, older, "ShadowedDb");
            /* Newer capture: all 28 payload columns set to distinct, alternating values. */
            await InsertDatabaseConfigNewAsync(connection, newer, "TestDb");

            var rows = await viewer.GetLatestDatabaseConfigAsync(DatabaseConfigServerId);

            Assert.Single(rows);
            var r = rows[0];

            /* Spot-checks spanning the ordinal range (0,1,2,3,5,6,11,12,14,18,21,22,23,24,25,27) — an
               off-by-one in the incrementing-ordinal reader would flip one of the alternating values. */
            Assert.Equal("TestDb", r.DatabaseName);          // 0
            Assert.Equal("ONLINE", r.StateDesc);             // 1
            Assert.Equal(160, r.CompatibilityLevel);         // 2
            Assert.Equal("SQL_Latin1_General_CP1_CI_AS", r.CollationName); // 3
            Assert.False(r.IsReadOnly);                      // 5
            Assert.True(r.IsAutoCloseOn);                    // 6  (catches the 5/6 boundary)
            Assert.True(r.IsRcsiOn);                         // 11
            Assert.Equal("ON", r.SnapshotIsolationState);    // 12
            Assert.True(r.IsQueryStoreOn);                   // 14
            Assert.True(r.IsBrokerEnabled);                  // 18
            Assert.Equal("NOTHING", r.LogReuseWaitDesc);     // 21
            Assert.Equal("CHECKSUM", r.PageVerifyOption);    // 22
            Assert.Equal(60, r.TargetRecoveryTimeSeconds);   // 23
            Assert.Equal("DISABLED", r.DelayedDurability);   // 24
            Assert.True(r.IsAcceleratedDatabaseRecoveryOn);  // 25
            Assert.True(r.IsOptimizedLockingOn);             // 27

            /* Display projections (pure C#) resolve as Lite's XAML binds them. */
            Assert.Equal("Yes", r.RcsiDisplay);
            Assert.Equal("Yes", r.AutoCloseDisplay);
            Assert.Equal("No", r.AutoShrinkDisplay);
            Assert.Equal("Yes", r.OptimizedLockingDisplay);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "database_config", DatabaseConfigServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task ScopedConfig_LatestCaptureWins_OrderedByDbThenName_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live scoped-config test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "database_scoped_config", ScopedConfigServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var older = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));
            var newer = older.AddMinutes(5);

            await InsertScopedConfigAsync(connection, older, "ShadowedDb", "MAXDOP", "0", "0");
            /* Newer capture inserted out of order to prove ORDER BY database_name, configuration_name. */
            await InsertScopedConfigAsync(connection, newer, "db2", "MAXDOP", "8", "8");
            await InsertScopedConfigAsync(connection, newer, "db1", "MAXDOP", "4", "2");
            await InsertScopedConfigAsync(connection, newer, "db1", "LEGACY_CARDINALITY_ESTIMATION", "1", "0");

            var rows = await viewer.GetLatestDatabaseScopedConfigAsync(ScopedConfigServerId);

            Assert.Equal(3, rows.Count);
            Assert.Equal(
                new[] { ("db1", "LEGACY_CARDINALITY_ESTIMATION"), ("db1", "MAXDOP"), ("db2", "MAXDOP") },
                rows.Select(r => (r.DatabaseName, r.ConfigurationName)));

            var db1Maxdop = rows.Single(r => r.DatabaseName == "db1" && r.ConfigurationName == "MAXDOP");
            Assert.Equal("4", db1Maxdop.Value);
            Assert.Equal("2", db1Maxdop.ValueForSecondary);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "database_scoped_config", ScopedConfigServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task TraceFlags_LatestCaptureWins_OrderedByFlag_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live trace-flags test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "trace_flags", TraceFlagsServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var older = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));
            var newer = older.AddMinutes(5);

            await InsertTraceFlagAsync(connection, older, 1117, status: true, isGlobal: true, isSession: false);
            /* Newer capture inserted high-then-low to prove ORDER BY trace_flag. */
            await InsertTraceFlagAsync(connection, newer, 3226, status: true, isGlobal: true, isSession: false);
            await InsertTraceFlagAsync(connection, newer, 1118, status: false, isGlobal: true, isSession: false);

            var rows = await viewer.GetLatestTraceFlagsAsync(TraceFlagsServerId);

            Assert.Equal(2, rows.Count);
            Assert.Equal(new[] { 1118, 3226 }, rows.Select(r => r.TraceFlag));

            Assert.Equal("Disabled", rows[0].StatusDisplay);   // 1118, status false
            Assert.Equal("Enabled", rows[1].StatusDisplay);    // 3226, status true
            Assert.Equal("Yes", rows[1].GlobalDisplay);
            Assert.Equal("No", rows[1].SessionDisplay);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "trace_flags", TraceFlagsServerId, cleanupCt));
        }
    }

    /// <summary>
    /// #3999: the all-off case. A capture that finds every flag off writes ZERO rows to <c>trace_flags</c>
    /// (<c>DBCC TRACESTATUS(-1)</c> only ever lists flags that are ON), so the newest ROW can be older than
    /// the newest SUCCESSFUL run. Before this fix, <c>capture_time = MAX(capture_time)</c> could not see that
    /// the collector had run again and fell back to the stale ON row - reporting a flag enabled days after it
    /// (and every other flag) was turned off. Seeded here at the exact shape: one old ON row, then a newer
    /// SUCCESS in collection_log with no corresponding trace_flags row at all.
    /// </summary>
    [Fact]
    public async Task TraceFlags_AllOffSinceNewerSuccessfulRun_ReadsNoFlags_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live trace-flags all-off test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "trace_flags", TraceFlagsServerId, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "collection_log", TraceFlagsServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var flagWasOnAt = TruncateToSeconds(DateTime.UtcNow.AddDays(-2));
            var laterSuccessfulRunFoundNothingAt = flagWasOnAt.AddDays(1);

            await InsertTraceFlagAsync(connection, flagWasOnAt, 1117, status: true, isGlobal: true, isSession: false);
            await InsertCollectionLogSuccessAsync(connection, laterSuccessfulRunFoundNothingAt);

            /* The control: without the newer collection_log SUCCESS, the same row still reads (proven by
               TraceFlags_LatestCaptureWins_OrderedByFlag_AgainstDevPostgres above with no collection_log row
               at all — the COALESCE floor covers that case). This test is the case that row's coverage
               cannot reach: a genuinely newer successful capture that stored nothing. */
            var rows = await viewer.GetLatestTraceFlagsAsync(TraceFlagsServerId);

            Assert.Empty(rows);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteRowsAsync(cleanup, "trace_flags", TraceFlagsServerId, cleanupCt);
                await DeleteRowsAsync(cleanup, "collection_log", TraceFlagsServerId, cleanupCt);
            });
        }
    }

    private static async Task InsertCollectionLogSuccessAsync(NpgsqlConnection connection, DateTime collectionTimeUtc)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) " +
            "VALUES ($1, $2, $3, 'trace_flags', $4, 'SUCCESS')", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(TraceFlagsServerId);
        command.Parameters.AddWithValue("viewer-trace-flags-e2e");
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertServerConfigAsync(
        NpgsqlConnection connection, DateTime captureTimeUtc,
        string configurationName, long valueConfigured, long valueInUse, bool isDynamic, bool isAdvanced)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO server_config
    (config_id, capture_time, server_id, server_name,
     configuration_name, value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(captureTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerConfigServerId);
        command.Parameters.AddWithValue("viewer-server-config-e2e");
        command.Parameters.AddWithValue(configurationName);
        command.Parameters.AddWithValue(valueConfigured);
        command.Parameters.AddWithValue(valueInUse);
        command.Parameters.AddWithValue(isDynamic);
        command.Parameters.AddWithValue(isAdvanced);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertDatabaseConfigOldAsync(NpgsqlConnection connection, DateTime captureTimeUtc, string databaseName)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO database_config
    (config_id, capture_time, server_id, server_name, database_name, state_desc, compatibility_level)
VALUES ($1, $2, $3, $4, $5, $6, $7)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(captureTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DatabaseConfigServerId);
        command.Parameters.AddWithValue("viewer-database-config-e2e");
        command.Parameters.AddWithValue(databaseName);
        command.Parameters.AddWithValue("RESTORING");
        command.Parameters.AddWithValue(150);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /* Sets all 28 payload columns to distinct, alternating values — the newer capture the read returns. */
    private static async Task InsertDatabaseConfigNewAsync(NpgsqlConnection connection, DateTime captureTimeUtc, string databaseName)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO database_config
    (config_id, capture_time, server_id, server_name,
     database_name, state_desc, compatibility_level, collation_name, recovery_model,
     is_read_only, is_auto_close_on, is_auto_shrink_on,
     is_auto_create_stats_on, is_auto_update_stats_on, is_auto_update_stats_async_on,
     is_read_committed_snapshot_on, snapshot_isolation_state, is_parameterization_forced,
     is_query_store_on, is_encrypted, is_trustworthy_on, is_db_chaining_on,
     is_broker_enabled, is_cdc_enabled, is_mixed_page_allocation_on,
     log_reuse_wait_desc, page_verify_option, target_recovery_time_seconds, delayed_durability,
     is_accelerated_database_recovery_on, is_memory_optimized_enabled, is_optimized_locking_on)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
        $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32)", connection);
        command.Parameters.AddWithValue(2L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(captureTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DatabaseConfigServerId);
        command.Parameters.AddWithValue("viewer-database-config-e2e");
        command.Parameters.AddWithValue(databaseName);                     // database_name (0)
        command.Parameters.AddWithValue("ONLINE");                         // state_desc (1)
        command.Parameters.AddWithValue(160);                              // compatibility_level (2)
        command.Parameters.AddWithValue("SQL_Latin1_General_CP1_CI_AS");   // collation_name (3)
        command.Parameters.AddWithValue("FULL");                           // recovery_model (4)
        command.Parameters.AddWithValue(false);                            // is_read_only (5)
        command.Parameters.AddWithValue(true);                             // is_auto_close_on (6)
        command.Parameters.AddWithValue(false);                            // is_auto_shrink_on (7)
        command.Parameters.AddWithValue(true);                             // is_auto_create_stats_on (8)
        command.Parameters.AddWithValue(true);                             // is_auto_update_stats_on (9)
        command.Parameters.AddWithValue(false);                            // is_auto_update_stats_async_on (10)
        command.Parameters.AddWithValue(true);                             // is_read_committed_snapshot_on (11)
        command.Parameters.AddWithValue("ON");                             // snapshot_isolation_state (12)
        command.Parameters.AddWithValue(false);                            // is_parameterization_forced (13)
        command.Parameters.AddWithValue(true);                             // is_query_store_on (14)
        command.Parameters.AddWithValue(false);                            // is_encrypted (15)
        command.Parameters.AddWithValue(false);                            // is_trustworthy_on (16)
        command.Parameters.AddWithValue(false);                            // is_db_chaining_on (17)
        command.Parameters.AddWithValue(true);                             // is_broker_enabled (18)
        command.Parameters.AddWithValue(false);                            // is_cdc_enabled (19)
        command.Parameters.AddWithValue(false);                            // is_mixed_page_allocation_on (20)
        command.Parameters.AddWithValue("NOTHING");                        // log_reuse_wait_desc (21)
        command.Parameters.AddWithValue("CHECKSUM");                       // page_verify_option (22)
        command.Parameters.AddWithValue(60);                               // target_recovery_time_seconds (23)
        command.Parameters.AddWithValue("DISABLED");                       // delayed_durability (24)
        command.Parameters.AddWithValue(true);                             // is_accelerated_database_recovery_on (25)
        command.Parameters.AddWithValue(false);                            // is_memory_optimized_enabled (26)
        command.Parameters.AddWithValue(true);                             // is_optimized_locking_on (27)
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertScopedConfigAsync(
        NpgsqlConnection connection, DateTime captureTimeUtc,
        string databaseName, string configurationName, string value, string valueForSecondary)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO database_scoped_config
    (config_id, capture_time, server_id, server_name,
     database_name, configuration_name, value, value_for_secondary)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(captureTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ScopedConfigServerId);
        command.Parameters.AddWithValue("viewer-scoped-config-e2e");
        command.Parameters.AddWithValue(databaseName);
        command.Parameters.AddWithValue(configurationName);
        command.Parameters.AddWithValue(value);
        command.Parameters.AddWithValue(valueForSecondary);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertTraceFlagAsync(
        NpgsqlConnection connection, DateTime captureTimeUtc,
        int traceFlag, bool status, bool isGlobal, bool isSession)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO trace_flags
    (config_id, capture_time, server_id, server_name,
     trace_flag, status, is_global, is_session)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(captureTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(TraceFlagsServerId);
        command.Parameters.AddWithValue("viewer-trace-flags-e2e");
        command.Parameters.AddWithValue(traceFlag);
        command.Parameters.AddWithValue(status);
        command.Parameters.AddWithValue(isGlobal);
        command.Parameters.AddWithValue(isSession);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, string table, int serverId, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
