/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// sys.databases.is_optimized_locking_on exists on SQL Server 2025 and Azure SQL Database (not Managed
/// Instance). The collector must select it on both, and a stored NULL (never read) must show as unknown,
/// never as "off".
/// </summary>
public sealed class DatabaseConfigOptimizedLockingGateTests
{
    private static CollectorContext Ctx(int major, bool azureSqlDb = false, bool managedInstance = false) => new()
    {
        ServerId = 42,
        ServerName = "test-server",
        CollectionTime = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
        Deltas = new CollectorDeltaCalculator(),
        Target = new CollectorTargetInfo
        {
            IsAzureSqlDb = azureSqlDb,
            IsAzureManagedInstance = managedInstance,
            SqlMajorVersion = major,
        },
    };

    [Theory]
    [InlineData(12, true, false, true)]    /* Azure SQL Database reports major 12: selected */
    [InlineData(12, false, true, false)]   /* Managed Instance: the column is not documented there */
    [InlineData(16, false, false, false)]  /* SQL Server 2022: no column */
    [InlineData(17, false, false, true)]   /* SQL Server 2025: selected */
    public void OptimizedLockingColumn_IsSelectedOnlyWhereTheEngineHasIt(int major, bool azureSqlDb, bool managedInstance, bool expected)
    {
        var text = DatabaseConfigCollector.Instance.BuildQuery(Ctx(major, azureSqlDb, managedInstance)).Text;

        Assert.Equal(expected, text.Contains("is_optimized_locking_on", StringComparison.Ordinal));
        /* The 2019 columns stay selected on both Azure flavours. */
        Assert.Equal(azureSqlDb || managedInstance || major >= 15,
            text.Contains("is_accelerated_database_recovery_on", StringComparison.Ordinal));
    }

    [Fact]
    public void ViewerRow_UnreadOptimizedLocking_DisplaysUnknown_NotNo()
    {
        Assert.Equal("Unknown", new DatabaseConfigRow { IsOptimizedLockingOn = null }.OptimizedLockingDisplay);
        Assert.Equal("Yes", new DatabaseConfigRow { IsOptimizedLockingOn = true }.OptimizedLockingDisplay);
        Assert.Equal("No", new DatabaseConfigRow { IsOptimizedLockingOn = false }.OptimizedLockingDisplay);
    }
}

[Collection("live-postgres")]
public sealed class DatabaseConfigOptimizedLockingLivePostgresTests
{
    private const string ServerName = "optlock-null-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task StoredNull_ReadsAsUnknown_InTheViewer_TheMcpReader_AndTheMcpJson()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live optimized-locking test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        await using var viewer = new ViewerDataService(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var when = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);
            await InsertAsync(connection, ct, when, "NullDb", null);
            await InsertAsync(connection, ct, when, "OnDb", true);
            await InsertAsync(connection, ct, when, "OffDb", false);

            var viewerRows = await viewer.GetLatestDatabaseConfigAsync(ServerId);
            Assert.Null(viewerRows.Single(r => r.DatabaseName == "NullDb").IsOptimizedLockingOn);
            Assert.Equal("Unknown", viewerRows.Single(r => r.DatabaseName == "NullDb").OptimizedLockingDisplay);
            Assert.True(viewerRows.Single(r => r.DatabaseName == "OnDb").IsOptimizedLockingOn);
            Assert.False(viewerRows.Single(r => r.DatabaseName == "OffDb").IsOptimizedLockingOn);

            var snapshot = await DarlingCurrentConfigReader.GetLatestDatabaseConfigAsync(postgres, ServerId, ct);
            Assert.Null(snapshot.Rows.Single(r => r.DatabaseName == "NullDb").IsOptimizedLockingOn);
            Assert.True(snapshot.Rows.Single(r => r.DatabaseName == "OnDb").IsOptimizedLockingOn);
            Assert.False(snapshot.Rows.Single(r => r.DatabaseName == "OffDb").IsOptimizedLockingOn);

            var json = await DarlingMcpConfigTools.GetDatabaseConfig(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(json, ServerName, "databases");
            using var doc = JsonDocument.Parse(json);
            var byName = doc.RootElement.GetProperty("databases").EnumerateArray()
                .ToDictionary(e => e.GetProperty("database_name").GetString()!, e => e.GetProperty("optimized_locking"));
            Assert.Equal(JsonValueKind.Null, byName["NullDb"].ValueKind);
            Assert.Equal(JsonValueKind.True, byName["OnDb"].ValueKind);
            Assert.Equal(JsonValueKind.False, byName["OffDb"].ValueKind);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static Task InsertAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, DateTime when, string db, bool? optimizedLocking) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO database_config (config_id, capture_time, server_id, server_name, database_name, state_desc, compatibility_level, collation_name, recovery_model,
    is_read_only, is_auto_close_on, is_auto_shrink_on, is_auto_create_stats_on, is_auto_update_stats_on, is_auto_update_stats_async_on,
    is_read_committed_snapshot_on, snapshot_isolation_state, is_parameterization_forced, is_query_store_on, is_encrypted, is_trustworthy_on,
    is_db_chaining_on, is_broker_enabled, is_cdc_enabled, is_mixed_page_allocation_on, log_reuse_wait_desc, page_verify_option,
    target_recovery_time_seconds, delayed_durability, is_accelerated_database_recovery_on, is_memory_optimized_enabled, is_optimized_locking_on)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32)",
            CollectionIdGenerator.Next(), when, ServerId, ServerName, db, "ONLINE", 160, "SQL_Latin1_General_CP1_CI_AS", "FULL",
            false, false, false, true, true, false,
            true, "OFF", false, true, false, false,
            false, true, false, false, "NOTHING", "CHECKSUM",
            60, "DISABLED", false, false, optimizedLocking);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM database_config WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
