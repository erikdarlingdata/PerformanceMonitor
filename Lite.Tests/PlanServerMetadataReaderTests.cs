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
/// #4530 — the untested half of Lite's plan-metadata wiring. <see cref="PlanSync4530Tests"/> (Darling.Tests,
/// ported to this repo) only ever passes a hand-built <c>ServerMetadata</c> into the analyzer; this pins
/// <see cref="LocalDataService.GetServerMetadataForPlanAnalysisAsync"/> against a seeded DuckDB fixture, so
/// a wrong view or column name in <c>PlanServerMetadataSql</c> fails here instead of returning null on
/// every real install and leaving rule 38 on its uninformative Info branch.
/// </summary>
public sealed class PlanServerMetadataReaderTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly SharedDuckDbFixture _fixture;
    private readonly LocalDataService _dataService;
    private long _nextId = -4_530_900;
    private DuckDBConnection? _seedConn;

    private const int ServerId = -453_090;

    public PlanServerMetadataReaderTests(SharedDuckDbFixture fixture)
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

    private async Task SeedServerPropertiesAsync(string edition)
    {
        using var readLock = _fixture.DuckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, cpu_count, physical_memory_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(DateTime.UtcNow);
        P(ServerId);
        P("PlanServerMetadataSrv");
        P(edition);
        P(4);
        P(16384L);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedMaxDopAsync(int maxDop)
    {
        using var readLock = _fixture.DuckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO server_config (config_id, capture_time, server_id, server_name, configuration_name, value_configured, value_in_use)
VALUES ($1, $2, $3, $4, $5, $6, $7)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(DateTime.UtcNow);
        P(ServerId);
        P("PlanServerMetadataSrv");
        P("max degree of parallelism");
        P((long)maxDop);
        P((long)maxDop);
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task GetServerMetadataForPlanAnalysisAsync_ReadsEditionAndMaxDop_FromTheSeededFixture()
    {
        await SeedServerPropertiesAsync("Standard Edition (64-bit)");
        await SeedMaxDopAsync(8);

        var metadata = await _dataService.GetServerMetadataForPlanAnalysisAsync(ServerId);

        Assert.NotNull(metadata);
        Assert.Contains("Standard", metadata!.Edition, StringComparison.Ordinal);
        Assert.Equal(8, metadata.MaxDop);
    }

    [Fact]
    public async Task GetServerMetadataForPlanAnalysisAsync_NoRows_ReturnsNull()
    {
        var metadata = await _dataService.GetServerMetadataForPlanAnalysisAsync(ServerId);

        Assert.Null(metadata);
    }
}
