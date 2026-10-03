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
        JsonSerializer.SerializeToElement(DarlingMcpFinOpsInventoryTools.InventoryRow(dto, metrics), McpHelpers.JsonOptions);

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
        Assert.Equal(33, DarlingMcpFinOpsInventoryTools.DefaultLimit);
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
    public async Task BadLimit_IsRefusedNamingLimit_BeforeTheStoreIsTouched(int limit)
    {
        var result = await DarlingMcpFinOpsInventoryTools.GetFinOpsInventory(null!, "server_inventory", limit);
        using var doc = JsonDocument.Parse(result);
        Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal("limit", doc.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
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
        Assert.Equal(JsonValueKind.Null, row.GetProperty("avg_cpu_pct").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("storage_total_gb").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("idle_db_count").ValueKind);
    }

    [Fact]
    public void Row_AzureSqlDatabase_NullsTheHostHardware_AndNeverWarnsOnItsMemory()
    {
        var row = Row(Dto(edition: "Azure SQL Database (Standard)", engineEdition: 5, cpus: 2, memoryMb: 934000), default);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("physical_memory_mb").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("socket_count").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("cores_per_socket").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("license_warning").ValueKind);
        Assert.Equal(2, row.GetProperty("cpu_count").GetInt32());
        Assert.False(string.IsNullOrEmpty(row.GetProperty("hardware_note").GetString()));
    }

    [Fact]
    public void Row_ZeroCpuWithAHardwareNote_IsUnknownNotZero()
    {
        var row = Row(Dto(cpus: 0, memoryMb: 0, note: "no permission"), default);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("cpu_count").ValueKind);
        Assert.Equal("no permission", row.GetProperty("hardware_note").GetString());
        var plain = Row(Dto(cpus: 0, memoryMb: 0), default);
        Assert.Equal(0, plain.GetProperty("cpu_count").GetInt32());
    }

    [Fact]
    public void Row_StandardEditionOverTheCaps_CarriesTheLicenseWarning()
    {
        var row = Row(Dto(edition: "Standard Edition (64-bit)", cpus: 32, memoryMb: 262144), default);
        Assert.Equal("CPU: 32 cores (Standard limited to 24); RAM: 256GB (Standard limited to 128GB)",
            row.GetProperty("license_warning").GetString());
    }

    /// <summary>The longest plausible row: every string field long, a two-part license warning, a hardware note.</summary>
    private static ServerInventoryDto RealisticDto(int n) =>
        new(n, "prod-sql-cluster-" + n.ToString("D3") + ".corp.example.internal\\INSTANCE_NAME_LONG", "Standard Edition (64-bit)",
            "16.0.4175.1 - RTM-CU18-GDR (KB5046861)", 3, 48, 524288L, null, 4, 12, true, true,
            new DateTime(2026, 10, 1, 12, 30, 15, DateTimeKind.Unspecified), new DateTime(2026, 9, 1, 3, 4, 5, DateTimeKind.Unspecified),
            "Windows Server 2022 Datacenter 10.0 <X64> (Build 20348: ) (Hypervisor)", "SECONDARY", true, 12345.67m,
            new DateTime(2026, 10, 2, 8, 0, 0, DateTimeKind.Unspecified));

    [Fact]
    public void DefaultLimit_OfRealisticRows_FitsTheDefaultResponseBudget()
    {
        var metrics = new ServerMetricsDto(63.45m, 98765.4m, 12, "RIGHT_SIZED");
        var rows = Enumerable.Range(1, DarlingMcpFinOpsInventoryTools.DefaultLimit)
            .Select(n => DarlingMcpFinOpsInventoryTools.InventoryRow(RealisticDto(n), metrics)).ToList();
        var body = JsonSerializer.Serialize(new
        {
            view = "server_inventory",
            cpu_window_hours = 24,
            idle_window_days = 7,
            total_servers = 999,
            servers_returned = rows.Count,
            truncated = true,
            servers = rows,
        }, McpHelpers.JsonOptions);
        Assert.True(System.Text.Encoding.UTF8.GetByteCount(body) <= 30 * 1024,
            $"{DarlingMcpFinOpsInventoryTools.DefaultLimit} realistic rows serialize to {body.Length} bytes");
        Assert.True(System.Text.Encoding.UTF8.GetByteCount(body) <= McpResponseBudget.DefaultBytes);
    }
}
