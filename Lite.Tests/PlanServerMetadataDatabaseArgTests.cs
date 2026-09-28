/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4597 — the database-argument half of <see cref="LocalDataService.GetServerMetadataForPlanAnalysisAsync"/>:
/// when a database name is given and the store has a <c>database_config</c> row for it, the reader fills
/// <see cref="PerformanceMonitor.PlanAnalysis.ServerMetadata.Database"/>; when no row matches, it stays
/// null, the same non-fatal contract <c>PlanServerMetadataReaderTests</c> pins for the no-server-row case.
/// This is build-only here (Lite.Tests targets net10.0-windows and can't run on macOS); CI decides it.
///
/// <para>Copies <c>PlanServerMetadataReaderTests</c>' setup (<see cref="SharedDuckDbFixture"/>, its own
/// negative id range) rather than writing new setup, per the Lite-pin setup guidance: every NOT NULL
/// column of <c>server_properties</c> and <c>database_config</c> is filled in the seed.</para>
/// </summary>
public sealed class PlanServerMetadataDatabaseArgTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly SharedDuckDbFixture _fixture;
    private readonly LocalDataService _dataService;
    private long _nextId = -4_597_900;
    private DuckDBConnection? _seedConn;

    private const int ServerId = -459_790;

    public PlanServerMetadataDatabaseArgTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _fixture = fixture;
        _dataService = new LocalDataService(fixture.DuckDb);
    }

    public void Dispose() => _seedConn?.Dispose();

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _fixture.DuckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedServerPropertiesAsync()
    {
        using var readLock = _fixture.DuckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition, cpu_count, physical_memory_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(DateTime.UtcNow);
        P(ServerId);
        P("PlanServerMetadataDbArgSrv");
        P("Enterprise Edition (64-bit)");
        P("16.0.4150.1");
        P("RTM");
        P(3);
        P(4);
        P(16384L);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedDatabaseConfigAsync(string databaseName)
    {
        using var readLock = _fixture.DuckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO database_config (config_id, capture_time, server_id, server_name, database_name, compatibility_level, collation_name, is_read_committed_snapshot_on, is_auto_create_stats_on, is_auto_update_stats_on, is_auto_update_stats_async_on, is_parameterization_forced)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(DateTime.UtcNow);
        P(ServerId);
        P("PlanServerMetadataDbArgSrv");
        P(databaseName);
        P(160);
        P("SQL_Latin1_General_CP1_CI_AS");
        P(true);
        P(true);
        P(true);
        P(false);
        P(false);
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task GetServerMetadataForPlanAnalysisAsync_WithDatabaseName_FillsDatabase()
    {
        await SeedServerPropertiesAsync();
        await SeedDatabaseConfigAsync("AdventureWorks");

        var metadata = await _dataService.GetServerMetadataForPlanAnalysisAsync(ServerId, "AdventureWorks");

        Assert.NotNull(metadata);
        Assert.NotNull(metadata!.Database);
        Assert.Equal("AdventureWorks", metadata.Database!.Name);
        Assert.Equal(160, metadata.Database.CompatibilityLevel);
        Assert.True(metadata.Database.IsReadCommittedSnapshotOn);
    }

    [Fact]
    public async Task GetServerMetadataForPlanAnalysisAsync_NoDatabaseName_LeavesDatabaseNull()
    {
        await SeedServerPropertiesAsync();
        await SeedDatabaseConfigAsync("AdventureWorks");

        var metadata = await _dataService.GetServerMetadataForPlanAnalysisAsync(ServerId);

        Assert.NotNull(metadata);
        Assert.Null(metadata!.Database);
    }

    [Fact]
    public async Task GetServerMetadataForPlanAnalysisAsync_DatabaseNameWithNoMatchingRow_LeavesDatabaseNull()
    {
        await SeedServerPropertiesAsync();
        await SeedDatabaseConfigAsync("AdventureWorks");

        var metadata = await _dataService.GetServerMetadataForPlanAnalysisAsync(ServerId, "NoSuchDatabase");

        Assert.NotNull(metadata);
        Assert.Null(metadata!.Database);
    }
}
