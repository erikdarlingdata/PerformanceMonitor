/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins for <c>get_finops_inventory</c> that need no store: the tool surface, the refusals (which the tool decides
/// before it touches the data source), and the row shape on hand-built inventory rows.
/// </summary>
public sealed class DarlingMcpFinOpsInventoryToolsTests
{
    private static ServerInventoryDto Dto(
        string name = "alpha-server", string edition = "Enterprise Edition (64-bit)", int engineEdition = 3, int cpus = 8,
        long memoryMb = 65536, string? note = null, bool enabled = true, decimal monthly = 100m) =>
        new(1, name, edition, "16.0.1000 - RTM", engineEdition, cpus, memoryMb, note, 2, 4, false, false,
            new DateTime(2026, 10, 1, 12, 30, 15, DateTimeKind.Unspecified), new DateTime(2026, 9, 1, 3, 4, 5, DateTimeKind.Unspecified),
            "Windows Server 2022", "Standalone", enabled, monthly, new DateTime(2026, 10, 2, 8, 0, 0, DateTimeKind.Unspecified));

    private static JsonElement Row(ServerInventoryDto dto, ServerMetricsDto metrics) =>
        JsonSerializer.SerializeToElement(DarlingMcpFinOpsInventoryTools.InventoryRow(dto, metrics), DarlingMcpFinOpsInventoryTools.WireOptions);

    [Fact]
    public void ToolSurface_ExactlyGetFinOpsInventory_WithItsParametersAndViews()
    {
        var methods = typeof(DarlingMcpFinOpsInventoryTools)
            .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
            .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
            .ToArray();
        Assert.Equal(new[] { "get_finops_inventory" }, methods.Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name).ToArray());
        Assert.NotNull(typeof(DarlingMcpFinOpsInventoryTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        var method = methods.Single();
        Assert.True(method.IsStatic);
        Assert.Equal(typeof(Task<string>), method.ReturnType);

        var described = method.GetParameters().Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null).ToArray();
        Assert.Equal(new[] { "view", "limit" }, described.Select(p => p.Name).ToArray());
        Assert.False(described.Single(p => p.Name == "view").HasDefaultValue, "view is required");
        Assert.Equal(DarlingMcpFinOpsInventoryTools.DefaultLimit, described.Single(p => p.Name == "limit").DefaultValue);
        Assert.Equal(200, DarlingMcpFinOpsInventoryTools.MaxLimit);
        Assert.Equal(new[] { "server_inventory" }, DarlingMcpFinOpsInventoryTools.Views);
    }

    [Theory]
    [InlineData("", "view")]
    [InlineData("  ", "view")]
    [InlineData("nope", "view")]
    public async Task BadView_IsRefusedNamingView_BeforeTheStoreIsTouched(string view, string parameter)
    {
        var result = await DarlingMcpFinOpsInventoryTools.GetFinOpsInventory(null!, view);
        using var doc = JsonDocument.Parse(result);
        Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal(parameter, doc.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
        Assert.Contains("server_inventory", doc.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(201)]
    public async Task BadLimit_IsRefusedNamingLimit_WithOneRange_BeforeTheStoreIsTouched(int limit)
    {
        var result = await DarlingMcpFinOpsInventoryTools.GetFinOpsInventory(null!, "server_inventory", limit);
        using var doc = JsonDocument.Parse(result);
        Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal("limit", doc.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
        Assert.Equal($"Invalid limit value '{limit}'. Must be an integer from 1 to 200.", doc.RootElement.GetProperty("message").GetString());
    }

    [Theory]
    [InlineData(null, 100, "good")]
    [InlineData(35.0, 100, "good")]
    [InlineData(100.0, 100, "poor")]
    public void Row_HealthScoreAndBand_ComeFromTheSharedFigures(double? cpu, int _, string band)
    {
        var metrics = new ServerMetricsDto(cpu is double c ? (decimal)c : null, 10m, 0, "RIGHT_SIZED");
        var row = Row(Dto(), metrics);
        var expectedScore = FinOpsInventoryFigures.HealthScore(metrics.AvgCpuPct);
        Assert.Equal(expectedScore, row.GetProperty("health_score").GetInt32());
        Assert.Equal(FinOpsUtilizationFigures.HealthBand(expectedScore), row.GetProperty("health_band").GetString());
        Assert.Equal(band, row.GetProperty("health_band").GetString());
    }

    [Fact]
    public void Row_CostsTimesAndRounding_FollowTheContract()
    {
        var row = Row(Dto(monthly: 100m), new ServerMetricsDto(12.25m, 3.05m, 4, "RIGHT_SIZED"));
        Assert.Equal(100m, row.GetProperty("monthly_cost_usd").GetDecimal());
        Assert.Equal(1200m, row.GetProperty("annual_cost_usd").GetDecimal());
        Assert.Equal(12.3m, row.GetProperty("avg_cpu_pct").GetDecimal());
        Assert.Equal(3.1m, row.GetProperty("storage_total_gb").GetDecimal());
        Assert.Equal("2026-10-01T12:30:15Z", row.GetProperty("inventory_as_of").GetString());
        Assert.Equal("2026-10-02T08:00:00Z", row.GetProperty("last_collected").GetString());
        Assert.Equal("2026-09-01T03:04:05", row.GetProperty("sqlserver_start_time_local").GetString());
        Assert.Equal("active", row.GetProperty("monitoring").GetString());
        Assert.False(row.TryGetProperty("server_id", out _));
        Assert.False(row.TryGetProperty("uptime", out _));
    }

    [Fact]
    public void Row_NoOverlay_LeavesTheMetricCellsNull_AndStoppedServersSaySo()
    {
        var row = Row(Dto(enabled: false), default);
        Assert.Equal("stopped", row.GetProperty("monitoring").GetString());
        Assert.False(row.TryGetProperty("avg_cpu_pct", out _), "avg_cpu_pct is left out");
        Assert.False(row.TryGetProperty("storage_total_gb", out _), "storage_total_gb is left out");
        Assert.False(row.TryGetProperty("idle_db_count", out _), "idle_db_count is left out");
    }

    [Fact]
    public void Row_AzureSqlDatabase_NullsTheHostHardware_AndNeverWarnsOnItsMemory()
    {
        var row = Row(Dto(edition: "Azure SQL Database (Standard)", engineEdition: 5, cpus: 2, memoryMb: 934000), default);
        Assert.False(row.TryGetProperty("physical_memory_mb", out _), "physical_memory_mb is left out");
        Assert.False(row.TryGetProperty("socket_count", out _), "socket_count is left out");
        Assert.False(row.TryGetProperty("cores_per_socket", out _), "cores_per_socket is left out");
        Assert.False(row.TryGetProperty("license_warning", out _), "license_warning is left out");
        Assert.Equal(2, row.GetProperty("cpu_count").GetInt32());
        Assert.Equal("host_scoped", row.GetProperty("hardware_note").GetString());
    }

    [Fact]
    public void Legend_IsEmittedOnlyWhenARowCarriesTheCode_AndHoldsTheFullNote()
    {
        var withCode = JsonSerializer.SerializeToElement(Envelope(new[] { Dto(edition: "Azure SQL Database (Standard)", engineEdition: 5, cpus: 2) }), DarlingMcpFinOpsInventoryTools.WireOptions);
        Assert.Equal(ServerHardwareScope.InventoryHardwareNote, withCode.GetProperty("hardware_note_legend").GetProperty("host_scoped").GetString());
        var without = JsonSerializer.SerializeToElement(Envelope(new[] { Dto() }), DarlingMcpFinOpsInventoryTools.WireOptions);
        Assert.False(without.TryGetProperty("hardware_note_legend", out _));
    }

    private static object Envelope(ServerInventoryDto[] dtos) => DarlingMcpFinOpsInventoryTools.Envelope(
        "server_inventory", dtos.Length, dtos, new System.Collections.Generic.Dictionary<int, ServerMetricsDto>());

    [Fact]
    public void Row_DeniedHardwareRead_NullsAllFourHardwareFields_NeverZero()
    {
        var row = Row(Dto(cpus: 0, memoryMb: 0, note: "unavailable - VIEW SERVER STATE"), default);
        Assert.False(row.TryGetProperty("cpu_count", out _));
        Assert.False(row.TryGetProperty("physical_memory_mb", out _));
        Assert.False(row.TryGetProperty("socket_count", out _));
        Assert.False(row.TryGetProperty("cores_per_socket", out _));
        Assert.Equal("unavailable - VIEW SERVER STATE", row.GetProperty("hardware_note").GetString());
        var plain = Row(Dto(cpus: 0, memoryMb: 0), default);
        Assert.Equal(0, plain.GetProperty("cpu_count").GetInt32());
        Assert.Equal(0, plain.GetProperty("physical_memory_mb").GetInt64());
    }

    [Fact]
    public void Row_StandardEditionOverTheCaps_CarriesTheLicenseWarning()
    {
        var row = Row(Dto(edition: "Standard Edition (64-bit)", cpus: 32, memoryMb: 262144), default);
        Assert.Equal("CPU: 32 cores (Standard limited to 24); RAM: 256GB (Standard limited to 128GB)",
            row.GetProperty("license_warning").GetString());
    }

    /// <summary>The widest on-premises row: Standard edition, a two-part license warning, long version and OS strings.</summary>
    private static ServerInventoryDto OnPremDto(int n) =>
        new(n, "prod-sql-cluster-" + n.ToString("D3") + ".corp.example.internal\\INSTANCE_NAME_LONG", "Standard Edition (64-bit)",
            "16.0.4175.1 - RTM-CU18-GDR (KB5046861)", 3, 48, 524288L, null, 4, 12, true, true,
            new DateTime(2026, 10, 1, 12, 30, 15, DateTimeKind.Unspecified), new DateTime(2026, 9, 1, 3, 4, 5, DateTimeKind.Unspecified),
            "Windows Server 2022 Datacenter 10.0 <X64> (Build 20348: ) (Hypervisor)", "SECONDARY", true, 12345.67m,
            new DateTime(2026, 10, 2, 8, 0, 0, DateTimeKind.Unspecified));

    /// <summary>The widest Azure SQL Database row: the host-scoped code, a long name, a Standard-tier edition.</summary>
    private static ServerInventoryDto AzureDto(int n) =>
        new(n, "prod-sql-server-" + n.ToString("D3") + ".database.windows.net\\ELASTIC_POOL_DATABASE_NAME", "Azure SQL Database (Standard)",
            "12.0.2000.8 - Azure SQL Database", 5, 8, 934000L, null, null, null, true, false,
            new DateTime(2026, 10, 1, 12, 30, 15, DateTimeKind.Unspecified), new DateTime(2026, 9, 1, 3, 4, 5, DateTimeKind.Unspecified),
            "", "PRIMARY", true, 12345.67m, new DateTime(2026, 10, 2, 8, 0, 0, DateTimeKind.Unspecified));

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DefaultLimit_OfTheWidestRowOfEachType_FitsTheDefaultResponseBudget(bool azure)
    {
        var metrics = new ServerMetricsDto(63.45m, 98765.4m, 12, "RIGHT_SIZED");
        var page = Enumerable.Range(1, DarlingMcpFinOpsInventoryTools.DefaultLimit)
            .Select(n => azure ? AzureDto(n) : OnPremDto(n)).ToList();
        var byId = page.ToDictionary(d => d.ServerId, _ => metrics);
        var body = JsonSerializer.Serialize(DarlingMcpFinOpsInventoryTools.Envelope("server_inventory", 999, page, byId),
            DarlingMcpFinOpsInventoryTools.WireOptions);
        var bytes = System.Text.Encoding.UTF8.GetByteCount(body);
        Assert.True(bytes <= 30 * 1024, $"{page.Count} widest rows (azure={azure}) serialize to {bytes} bytes");
        Assert.True(bytes <= McpResponseBudget.DefaultBytes);
        Assert.Equal(azure, body.Contains("\"hardware_note_legend\"", StringComparison.Ordinal));
    }
}
