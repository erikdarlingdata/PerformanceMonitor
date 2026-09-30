/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// On an Azure SQL Database (engine edition 5) <c>sys.dm_os_sys_info</c> describes the HOST: a 1-vCore serverless
/// General Purpose database read 2 logical CPUs and 911.9 GB of physical memory. <see cref="AzureSqlDatabaseHardwareTests"/>
/// pins that nothing SHOWS those values as the database's. These pins are the calculations that USED them: the attributed-CPU
/// denominator, the FinOps utilization card's CPU count, and the FinOps health score.
///
/// <para>The rule: on an Azure SQL Database each of those uses the database's own figure where one is collected (the
/// <c>vcore_count</c> parsed from the service objective) and is otherwise NOT APPLICABLE. A DTU-model objective has no vCore
/// count, so its CPU count is not applicable and nothing is computed from the host's. SQL Server (editions 1 to 4) and Managed
/// Instance (8) behave exactly as before, and every test has that twin. The Darling.Tests twin pins the same table for the
/// other app, in the same words.</para>
/// </summary>
public sealed class AzureSqlDatabaseHostMathTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -487_002;
    private static readonly DateTime s_start = new(2026, 9, 30, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime s_end = s_start.AddHours(1);

    private readonly SharedDuckDbFixture _fixture;
    private DuckDBConnection? _seedConn;

    public AzureSqlDatabaseHostMathTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _fixture = fixture;
    }

    public void Dispose() => _seedConn?.Dispose();

    // ── CPU attribution ──

    /// <summary>Half of one CPU for an hour is 1,800 CPU-seconds; half of the host's two would be 3,600.</summary>
    private static CpuAttribution.Result Attribute(int? engineEdition, int storedCpuCount, int? vcoreCount) =>
        CpuAttribution.Compute(
            rankedCpuSeconds: 900, s_start, s_end,
            sampleCount: 60, firstSampleUtc: s_start, lastSampleUtc: s_end, avgSqlCpuPercent: 50,
            engineEdition, storedCpuCount, vcoreCount);

    [Fact]
    public void Attribution_OnAzureSqlDatabase_WithVcores_DividesByTheVcores_NotTheHostsCpus()
    {
        var result = Attribute(5, storedCpuCount: 2, vcoreCount: 1);

        Assert.Equal(1800, result.SqlCpuSecondsInWindow);
        Assert.Equal(0.5, result.AttributedCpuRatio);
        Assert.Null(result.Note);
    }

    [Theory]
    [InlineData(null)]
    [InlineData(0)]
    public void Attribution_OnAzureSqlDatabase_WithNoVcores_IsNotApplicable_AndComputesNothingFromTheHost(int? vcoreCount)
    {
        var result = Attribute(5, storedCpuCount: 2, vcoreCount);

        Assert.Equal(900, result.RankedCpuSeconds);
        Assert.Null(result.SqlCpuSecondsInWindow);
        Assert.Null(result.AttributedCpuRatio);
        Assert.Equal(CpuAttribution.CoreCountNotApplicableNote, result.Note);
        Assert.Contains("not applicable", result.Note, StringComparison.Ordinal);
        Assert.Contains("DTU", result.Note, StringComparison.Ordinal);
        Assert.DoesNotContain("no server_properties snapshot", result.Note, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(8)]
    public void Attribution_OnEveryOtherEdition_IsTheStoredCountMathItAlwaysWas(int engineEdition)
    {
        var legacy = CpuAttribution.Compute(900, s_start, s_end, 60, s_start, s_end, 50, 8);

        Assert.Equal(legacy, Attribute(engineEdition, storedCpuCount: 8, vcoreCount: null));
        /* A vcore_count beside a non-Azure edition is not read: only edition 5 resolves it. */
        Assert.Equal(legacy, Attribute(engineEdition, storedCpuCount: 8, vcoreCount: 1));
        Assert.Equal(14400, legacy.SqlCpuSecondsInWindow);
    }

    [Fact]
    public void Attribution_WithNoServerPropertiesRow_KeepsItsUnavailableNote()
    {
        var result = Attribute(engineEdition: null, storedCpuCount: 0, vcoreCount: null);

        Assert.Null(result.AttributedCpuRatio);
        Assert.Equal(CpuAttribution.CoreCountUnavailableNote, result.Note);
        Assert.Contains("no server_properties snapshot", result.Note, StringComparison.Ordinal);
    }

    [Fact]
    public void OwnCpuCount_IsTheVcoresOnAzureSqlDatabase_AndTheStoredCountEverywhereElse()
    {
        Assert.Equal(1, ServerHardwareScope.OwnCpuCount(5, cpuCount: 2, vcoreCount: 1));
        Assert.Null(ServerHardwareScope.OwnCpuCount(5, cpuCount: 2, vcoreCount: null));
        Assert.Null(ServerHardwareScope.OwnCpuCount(5, cpuCount: 2, vcoreCount: 0));
        Assert.Equal(16, ServerHardwareScope.OwnCpuCount(3, cpuCount: 16, vcoreCount: null));
        Assert.Equal(4, ServerHardwareScope.OwnCpuCount(8, cpuCount: 4, vcoreCount: null));
        Assert.Null(ServerHardwareScope.OwnCpuCount(3, cpuCount: null, vcoreCount: null));
    }

    [Fact]
    public void TopQueriesAndTopProceduresTools_PassTheEditionAndTheVcores_NotOnlyTheStoredCount()
    {
        var tool = ReadRepoFile("Lite/Mcp/McpQueryTools.cs");

        Assert.Equal(2, CountOf(tool, "properties?.EngineEdition, properties?.CpuCount ?? 0, properties?.VcoreCount);"));
        Assert.DoesNotContain("properties?.CpuCount ?? 0);", tool, StringComparison.Ordinal);
    }

    // ── FinOps utilization card: the CPU count ──

    [Fact]
    public void CpuCountText_OnAzureSqlDatabase_IsNotApplicableWithNoVcores_AndTheVcoresOtherwise()
    {
        Assert.Equal(ServerHardwareScope.NotApplicable, ServerHardwareScope.CpuCountText(5, 0));
        Assert.Equal("n/a", ServerHardwareScope.CpuCountText(5, 0));
        Assert.Equal("1", ServerHardwareScope.CpuCountText(5, 1));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(8)]
    [InlineData(null)]
    public void CpuCountText_OnEveryOtherEdition_IsTheCountAsItAlwaysWas(int? engineEdition)
    {
        Assert.Equal(1234.ToString("N0", System.Globalization.CultureInfo.CurrentCulture), ServerHardwareScope.CpuCountText(engineEdition, 1234));
        Assert.Equal("16", ServerHardwareScope.CpuCountText(engineEdition, 16));
        Assert.Equal("0", ServerHardwareScope.CpuCountText(engineEdition, 0));
    }

    [Theory]
    [InlineData(5, 1, 1)]     // Azure SQL Database, vCore objective: its vCores, not the host's 2
    [InlineData(5, null, 0)]  // Azure SQL Database, DTU objective: no count, never the host's 2
    [InlineData(3, null, 2)]  // SQL Server: the stored count
    [InlineData(8, null, 2)]  // Managed Instance: the stored count
    public async Task UtilizationRead_ResolvesTheCpuCountThroughTheEdition(int engineEdition, int? vcoreCount, int expectedCpuCount)
    {
        await SeedAsync(engineEdition, hostCpuCount: 2, vcoreCount);

        var row = await new LocalDataService(_fixture.DuckDb).GetUtilizationEfficiencyAsync(ServerId);

        Assert.NotNull(row);
        Assert.Equal(expectedCpuCount, row!.CpuCount);
        Assert.Equal(engineEdition, row.EngineEdition);
    }

    // ── FinOps utilization card: the health score ──

    /// <summary>CPU p95 of 7% scores 95 and 50% free storage scores 100. Buffer pool 40 GB of a 933,888 MB host is 4%, which
    /// scores 60 when memory counts: 95 * 0.4 + 60 * 0.3 + 100 * 0.3 = 86, the score a 1-vCore database showed.</summary>
    private static UtilizationEfficiencyRow Utilization(int engineEdition, int bufferPoolMb, int physicalMemoryMb) => new()
    {
        EngineEdition = engineEdition,
        ProvisioningStatus = ProvisioningVerdict.RightSized, // a measured window: a window with no CPU sample has no CPU term
        P95CpuPct = 7m,
        BufferPoolMb = bufferPoolMb,
        PhysicalMemoryMb = physicalMemoryMb,
        FreeSpacePct = 50m,
    };

    [Fact]
    public void HealthScore_OnAzureSqlDatabase_LeavesMemoryOut_AndDoesNotMoveWithTheHostsMemory()
    {
        Assert.Equal(97, Utilization(5, 40_960, 933_888).ComputeHealthScore());
        Assert.Equal(97, Utilization(5, 40_960, 65_536).ComputeHealthScore());
        Assert.Equal(97, Utilization(5, 800_000, 933_888).ComputeHealthScore());
        Assert.Equal(97, Utilization(5, 0, 0).ComputeHealthScore());
        Assert.Equal(FinOpsHealthCalculator.Overall(95, null, 100), Utilization(5, 40_960, 933_888).ComputeHealthScore());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(8)]
    public void HealthScore_OnEveryOtherEdition_KeepsItsMemoryTerm(int engineEdition)
    {
        Assert.Equal(86, Utilization(engineEdition, 40_960, 933_888).ComputeHealthScore());
        Assert.Equal(98, Utilization(engineEdition, 600_000, 933_888).ComputeHealthScore());
    }

    [Fact]
    public void Overall_WithNoMemoryScore_WeighsCpuAndStorageOverTheirOwnSeventyPercent()
    {
        Assert.Equal(100, FinOpsHealthCalculator.Overall(100, null, 100));
        Assert.Equal(0, FinOpsHealthCalculator.Overall(0, null, 0));
        Assert.Equal(97, FinOpsHealthCalculator.Overall(95, null, 100));
        Assert.Equal(62, FinOpsHealthCalculator.Overall(80, null, 40));
        /* With a memory score, the long-standing arithmetic, byte for byte. */
        Assert.Equal(86, FinOpsHealthCalculator.Overall(95, 60, 100));
        Assert.Equal(62, FinOpsHealthCalculator.Overall(80, 60, 40));
    }

    // ── the wiring, pinned at the source ──

    private static string ReadRepoFile(string relativePath, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var parts = relativePath.Split('/');
        while (dir is not null && !File.Exists(Path.Combine(new[] { dir }.Concat(parts).ToArray())))
            dir = Path.GetDirectoryName(dir);

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(new[] { dir! }.Concat(parts).ToArray()));
    }

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var at = text.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = text.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
            count++;
        return count;
    }

    [Fact]
    public void FinOpsUtilizationCard_AsksTheSharedRule_ForTheCpuCountAndTheHealthScore()
    {
        var tab = ReadRepoFile("Lite/Controls/FinOpsTab.xaml.cs");
        var read = ReadRepoFile("Lite/Services/LocalDataService.FinOps.Utilization.cs");

        Assert.Contains("CpuCountText.Text = ServerHardwareScope.CpuCountText(data.EngineEdition, data.CpuCount);", tab, StringComparison.Ordinal);
        Assert.DoesNotContain("data.CpuCount.ToString(", tab, StringComparison.Ordinal);
        Assert.Contains("data.HealthScore = data.ComputeHealthScore();", tab, StringComparison.Ordinal);
        Assert.Contains("HealthScoreBorder.ToolTip = azureSqlDb ? ServerHardwareScope.HealthScoreWithoutMemoryNote : null;", tab, StringComparison.Ordinal);
        Assert.DoesNotContain("FinOpsHealthCalculator.MemoryScore(", tab, StringComparison.Ordinal);

        var edition = ServerHardwareScope.AzureSqlDatabaseEngineEdition;
        Assert.Contains(
            $"SELECT CASE WHEN engine_edition = {edition} THEN vcore_count ELSE COALESCE(vcore_count, cpu_count) END AS cpu_count, engine_edition",
            read, StringComparison.Ordinal);
    }

    // ── seeding ──

    private async Task SeedAsync(int engineEdition, int hostCpuCount, int? vcoreCount)
    {
        using var readLock = _fixture.DuckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _fixture.DuckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using (var cmd = _seedConn.CreateCommand())
        {
            cmd.CommandText = @"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition,
     cpu_count, hyperthread_ratio, physical_memory_mb, socket_count, cores_per_socket, service_objective, vcore_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)";
            void P(object? v) => cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
            P(-487_002L); P(DateTime.UtcNow); P(ServerId); P("AzureHostMathSrv"); P("SQL Azure"); P("12.0.2000.8"); P("RTM");
            P(engineEdition); P(hostCpuCount); P(64); P(933_888L); P(0); P(32); P(vcoreCount.HasValue ? "GP_S_Gen5_" + vcoreCount : "S0"); P(vcoreCount);
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = _seedConn.CreateCommand())
        {
            cmd.CommandText = @"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb,
     target_server_memory_mb, total_server_memory_mb, buffer_pool_mb)
VALUES ($1, $2, $3, $4, 933888, 900000, 40000, 40000, 30000)";
            void P(object? v) => cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
            P(-487_003L); P(DateTime.UtcNow); P(ServerId); P("AzureHostMathSrv");
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
