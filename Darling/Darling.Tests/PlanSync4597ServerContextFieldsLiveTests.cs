/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4597 — <see cref="DarlingServerMetadataReader"/> also fills cost threshold, max server memory and
/// the plan's database (name + compat level), the same way it already fills edition/MAXDOP (#4530).
/// Seeds <c>server_config</c> with all three settings and <c>database_config</c> with a database row,
/// reads them back through the reader with a database name supplied, and confirms a missing
/// database_config row (no name given, or a name the store has no row for) leaves
/// <see cref="PerformanceMonitor.PlanAnalysis.ServerMetadata.Database"/> null rather than erroring.
///
/// <para>#1776 own-store: this uses <c>ScratchPostgres</c>, not the shared <c>live-postgres</c>
/// collection, and never touches another test's rows.</para>
/// </summary>
public sealed class PlanSync4597ServerContextFieldsLiveTests
{
    private const int TestServerId = -459_700;

    private static async Task SeedServerPropertiesAsync(NpgsqlConnection connection)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name, edition, product_version, product_level,
     cpu_count, physical_memory_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(TestServerId);
        command.Parameters.AddWithValue("PlanSync4597Srv");
        command.Parameters.AddWithValue("Enterprise Edition (64-bit)");
        command.Parameters.AddWithValue("16.0.4215.2");
        command.Parameters.AddWithValue("RTM");
        command.Parameters.AddWithValue(4);
        command.Parameters.AddWithValue(16384L);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task SeedServerConfigAsync(
        NpgsqlConnection connection, string configurationName, long valueInUse)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO server_config
    (config_id, capture_time, server_id, server_name, configuration_name, value_configured, value_in_use)
VALUES ($1, $2, $3, $4, $5, $6, $7)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(TestServerId);
        command.Parameters.AddWithValue("PlanSync4597Srv");
        command.Parameters.AddWithValue(configurationName);
        command.Parameters.AddWithValue(valueInUse);
        command.Parameters.AddWithValue(valueInUse);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task SeedDatabaseConfigAsync(
        NpgsqlConnection connection, string databaseName, int compatibilityLevel)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO database_config
    (config_id, capture_time, server_id, server_name, database_name, compatibility_level, collation_name,
     is_read_committed_snapshot_on, is_auto_create_stats_on, is_auto_update_stats_on,
     is_auto_update_stats_async_on, is_parameterization_forced)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(TestServerId);
        command.Parameters.AddWithValue("PlanSync4597Srv");
        command.Parameters.AddWithValue(databaseName);
        command.Parameters.AddWithValue(compatibilityLevel);
        command.Parameters.AddWithValue("SQL_Latin1_General_CP1_CI_AS");
        command.Parameters.AddWithValue(true);
        command.Parameters.AddWithValue(true);
        command.Parameters.AddWithValue(true);
        command.Parameters.AddWithValue(false);
        command.Parameters.AddWithValue(false);
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task DarlingServerMetadataReader_ReadsCostThresholdMaxMemoryAndDatabase_FromAMigratedStore()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #4597 reader test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);

        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            await SeedServerPropertiesAsync(connection);
            await SeedServerConfigAsync(connection, "max degree of parallelism", 8);
            await SeedServerConfigAsync(connection, "cost threshold for parallelism", 50);
            await SeedServerConfigAsync(connection, "max server memory (MB)", 32768);
            await SeedDatabaseConfigAsync(connection, "AdventureWorks", 160);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* ---- (1) database name given, and the store has a row for it: everything fills. ---- */
        var withDb = await DarlingServerMetadataReader.ReadAsync(postgres, TestServerId, "AdventureWorks", ct);
        Assert.NotNull(withDb);
        Assert.Equal(8, withDb!.MaxDop);
        Assert.Equal(50, withDb.CostThresholdForParallelism);
        Assert.Equal(32768L, withDb.MaxServerMemoryMB);
        Assert.NotNull(withDb.Database);
        Assert.Equal("AdventureWorks", withDb.Database!.Name);
        Assert.Equal(160, withDb.Database.CompatibilityLevel);
        Assert.Equal("SQL_Latin1_General_CP1_CI_AS", withDb.Database.CollationName);
        Assert.True(withDb.Database.IsReadCommittedSnapshotOn);
        Assert.True(withDb.Database.IsAutoCreateStatsOn);
        Assert.True(withDb.Database.IsAutoUpdateStatsOn);
        Assert.False(withDb.Database.IsAutoUpdateStatsAsyncOn);
        Assert.False(withDb.Database.IsParameterizationForced);

        /* ---- (2) no database name given: everything else still fills, Database stays null. ---- */
        var noDbName = await DarlingServerMetadataReader.ReadAsync(postgres, TestServerId, cancellationToken: ct);
        Assert.NotNull(noDbName);
        Assert.Equal(50, noDbName!.CostThresholdForParallelism);
        Assert.Null(noDbName.Database);

        /* ---- (3) a database name the store has no database_config row for: Database stays null,
           not an error, same non-fatal contract as a missing server_properties row (#4530). ---- */
        var missingDb = await DarlingServerMetadataReader.ReadAsync(postgres, TestServerId, "NoSuchDatabase", ct);
        Assert.NotNull(missingDb);
        Assert.Null(missingDb!.Database);
    }

    /// <summary>
    /// Source-census pin: Lite's <c>GetServerMetadataForPlanAnalysisAsync</c> SQL (<c>Lite.Tests</c>
    /// can't run on macOS, so this is read by hand there — see the RED note in the PR) names the same
    /// <c>v_database_config</c> columns Darling's migrated <c>database_config</c> schema does, so both
    /// SKUs fill <see cref="PerformanceMonitor.PlanAnalysis.DatabaseMetadata"/> off equivalent data.
    /// </summary>
    [Fact]
    public void LiteReaderSql_NamesTheSameDatabaseConfigColumns_AsDarlingsMigratedSchema()
    {
        var liteSource = ReadRepoFile(Path.Combine("Lite", "Services", "LocalDataService.PlanServerMetadata.cs"));

        Assert.Contains("FROM v_database_config", liteSource, StringComparison.Ordinal);
        Assert.Contains("database_name", liteSource, StringComparison.Ordinal);
        Assert.Contains("compatibility_level", liteSource, StringComparison.Ordinal);
        Assert.Contains("collation_name", liteSource, StringComparison.Ordinal);
        Assert.Contains("is_read_committed_snapshot_on", liteSource, StringComparison.Ordinal);
        Assert.Contains("is_auto_create_stats_on", liteSource, StringComparison.Ordinal);
        Assert.Contains("is_auto_update_stats_on", liteSource, StringComparison.Ordinal);
        Assert.Contains("is_auto_update_stats_async_on", liteSource, StringComparison.Ordinal);
        Assert.Contains("is_parameterization_forced", liteSource, StringComparison.Ordinal);

        /* Darling's own reader SQL, so a rename on one side without the other fails here. */
        Assert.Contains("database_name", DarlingServerMetadataReader.ServerMetadataSql, StringComparison.Ordinal);
        Assert.Contains("compatibility_level", DarlingServerMetadataReader.ServerMetadataSql, StringComparison.Ordinal);
        Assert.Contains("collation_name", DarlingServerMetadataReader.ServerMetadataSql, StringComparison.Ordinal);
        Assert.Contains("is_read_committed_snapshot_on", DarlingServerMetadataReader.ServerMetadataSql, StringComparison.Ordinal);
        Assert.Contains("is_auto_create_stats_on", DarlingServerMetadataReader.ServerMetadataSql, StringComparison.Ordinal);
        Assert.Contains("is_auto_update_stats_on", DarlingServerMetadataReader.ServerMetadataSql, StringComparison.Ordinal);
        Assert.Contains("is_auto_update_stats_async_on", DarlingServerMetadataReader.ServerMetadataSql, StringComparison.Ordinal);
        Assert.Contains("is_parameterization_forced", DarlingServerMetadataReader.ServerMetadataSql, StringComparison.Ordinal);
    }
}
