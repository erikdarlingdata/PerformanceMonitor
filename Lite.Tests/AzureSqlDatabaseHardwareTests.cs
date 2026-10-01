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
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// On an Azure SQL Database (engine edition 5) the collected <c>server_properties</c> hardware columns are the
/// HOST's: a 1-vCore serverless General Purpose database read 2 logical CPUs, 0 sockets, 32 cores per socket, a
/// hyperthread ratio of 64 and 911.9 GB of physical memory. Nothing may present them as the database's. What does
/// describe the database is the service objective and the <c>vcore_count</c> parsed from it. The <c>memory_stats</c> table is
/// a different source: its memory figures are the database's own and are pinned in <see cref="AzureSqlDatabaseMemoryScopeTests"/>.
///
/// <para>The rule is pinned where it is applied: <c>get_server_properties</c> (the payload the web Server Properties
/// list reads too), the FinOps Server Inventory row the grid binds, and the words the FinOps utilization card takes from
/// the shared rule. Every test has an edition-3 twin that keeps today's values, so the rule cannot leak into the boxed
/// engine. The Darling.Tests twin pins the same table for the other app, in the same words.</para>
/// </summary>
public sealed class AzureSqlDatabaseHardwareTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -487_001;
    private readonly SharedDuckDbFixture _fixture;
    private DuckDBConnection? _seedConn;

    public AzureSqlDatabaseHardwareTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _fixture = fixture;
    }

    public void Dispose() => _seedConn?.Dispose();

    private static ServerPropertiesRow HostRow(int engineEdition, string edition, string? objective, int? vcores) => new()
    {
        Edition = edition,
        ProductVersion = "12.0.2000.8",
        ProductLevel = "RTM",
        EngineEdition = engineEdition,
        CpuCount = 2,
        HyperthreadRatio = 64,
        PhysicalMemoryMb = 933_888,
        SocketCount = 0,
        CoresPerSocket = 32,
        ServiceObjective = objective ?? "",
        VcoreCount = vcores,
        CollectionTime = new DateTime(2026, 9, 30, 12, 0, 0, DateTimeKind.Utc),
    };

    private static JsonElement Payload(ServerPropertiesRow row) =>
        JsonDocument.Parse(McpServerInfoTools.ServerPropertiesPayload("Srv", row)).RootElement.Clone();

    private static readonly string[] s_hostKeys =
        ["cpu_count", "hyperthread_ratio", "socket_count", "cores_per_socket", "physical_memory_mb"];

    // ── get_server_properties ──

    [Fact]
    public void GetServerProperties_OnAzureSqlDatabase_ReturnsTheHostsFiveAsNull_AndTheDatabasesVcores_WithANote()
    {
        var json = Payload(HostRow(5, "SQL Azure", "GP_S_Gen5_1", 1));

        foreach (var key in s_hostKeys)
            Assert.Equal(JsonValueKind.Null, json.GetProperty(key).ValueKind);

        Assert.Equal("GP_S_Gen5_1", json.GetProperty("service_objective").GetString());
        Assert.Equal(1, json.GetProperty("vcore_count").GetInt32());
        var note = json.GetProperty("hardware_note").GetString();
        Assert.Equal(ServerHardwareScope.McpHardwareNote, note);
        Assert.Contains("service_objective", note, StringComparison.Ordinal);
        Assert.Contains("vcore_count", note, StringComparison.Ordinal);
        Assert.Contains("not this database's allocation", note, StringComparison.Ordinal);

        /* vcore_count sits beside service_objective, and the rest of the payload is still there. */
        var names = json.EnumerateObject().Select(p => p.Name).ToList();
        Assert.Equal(names.IndexOf("service_objective") + 1, names.IndexOf("vcore_count"));
        Assert.Equal("SQL Azure", json.GetProperty("edition").GetString());
        Assert.Equal(5, json.GetProperty("engine_edition").GetInt32());
    }

    [Fact]
    public void GetServerProperties_OnAzureSqlDatabase_WithNoVcores_StillHidesTheHost_AndReturnsANullVcoreCount()
    {
        var json = Payload(HostRow(5, "SQL Azure", "S0", null));

        foreach (var key in s_hostKeys)
            Assert.Equal(JsonValueKind.Null, json.GetProperty(key).ValueKind);
        Assert.Equal(JsonValueKind.Null, json.GetProperty("vcore_count").ValueKind);
        Assert.True(json.TryGetProperty("hardware_note", out _));
    }

    [Fact]
    public void GetServerProperties_OnEdition3_IsUnchanged()
    {
        var json = Payload(HostRow(3, "Enterprise Edition (64-bit)", null, null));

        Assert.Equal(2, json.GetProperty("cpu_count").GetInt32());
        Assert.Equal(64, json.GetProperty("hyperthread_ratio").GetInt32());
        Assert.Equal(0, json.GetProperty("socket_count").GetInt32());
        Assert.Equal(32, json.GetProperty("cores_per_socket").GetInt32());
        Assert.Equal(933_888L, json.GetProperty("physical_memory_mb").GetInt64());
        Assert.False(json.TryGetProperty("vcore_count", out _), "an engine that is not an Azure SQL Database gets no new key");
        Assert.False(json.TryGetProperty("hardware_note", out _), "an engine that is not an Azure SQL Database gets no new key");
        Assert.Equal(
            new[]
            {
                "server", "captured_at", "edition", "engine_edition", "product_version", "product_level", "product_update_level",
                "cpu_count", "hyperthread_ratio", "socket_count", "cores_per_socket", "physical_memory_mb", "is_hadr_enabled",
                "is_clustered", "enterprise_features", "service_objective", "utc_offset_minutes", "time_zone_id", "time_zone_note",
            },
            json.EnumerateObject().Select(p => p.Name).ToArray());
    }

    [Fact]
    public async Task LatestServerProperties_ReadsTheVcoreCountTheCollectorStored()
    {
        await SeedAsync(engineEdition: 5, objective: "GP_S_Gen5_1", vcores: 1);

        var row = await new LocalDataService(_fixture.DuckDb).GetLatestServerPropertiesAsync(ServerId);

        Assert.NotNull(row);
        Assert.Equal(1, row!.VcoreCount);
        Assert.Equal(5, row.EngineEdition);
        var json = Payload(row);
        Assert.Equal(1, json.GetProperty("vcore_count").GetInt32());
        Assert.Equal(JsonValueKind.Null, json.GetProperty("physical_memory_mb").ValueKind);
    }

    private async Task SeedAsync(int engineEdition, string? objective, int? vcores)
    {
        using var readLock = _fixture.DuckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _fixture.DuckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition,
     cpu_count, hyperthread_ratio, physical_memory_mb, socket_count, cores_per_socket, service_objective, vcore_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)";
        void P(object? v) => cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
        P(-487_001L); P(DateTime.UtcNow); P(ServerId); P("AzureHardwareSrv"); P("SQL Azure"); P("12.0.2000.8"); P("RTM");
        P(engineEdition); P(2); P(64); P(933_888L); P(0); P(32); P(objective); P(vcores);
        await cmd.ExecuteNonQueryAsync();
    }

    // ── FinOps Server Inventory row ──

    [Fact]
    public void InventoryRow_OnAzureSqlDatabase_LeavesTheMemoryAndCoreCellsBlank_AndSaysWhy()
    {
        var row = new ServerPropertyRow
        {
            Edition = "SQL Azure", EngineEdition = 5, CpuCount = 2, PhysicalMemoryMb = 933_888, SocketCount = 0, CoresPerSocket = 32,
        };

        Assert.Null(row.CpuCount);
        Assert.Null(row.PhysicalMemoryMb);
        Assert.Null(row.SocketCount);
        Assert.Null(row.CoresPerSocket);
        Assert.Equal(ServerHardwareScope.InventoryHardwareNote, row.HardwareUnavailableReason);
    }

    [Fact]
    public void InventoryRow_OnAzureSqlDatabase_DoesNotDependOnTheOrderTheLoaderAssignsInAndKeepsAReadsOwnReason()
    {
        var row = new ServerPropertyRow { CpuCount = 2, PhysicalMemoryMb = 933_888, SocketCount = 0, CoresPerSocket = 32 };
        Assert.Equal(2, row.CpuCount);

        row.EngineEdition = 5;
        row.HardwareUnavailableReason = "Hardware read denied";

        Assert.Null(row.CpuCount);
        Assert.Equal("Hardware read denied", row.HardwareUnavailableReason);
    }

    [Fact]
    public void InventoryRow_OnEdition3_KeepsItsHardware_AndHasNoNote()
    {
        var row = new ServerPropertyRow
        {
            Edition = "Enterprise Edition (64-bit)", EngineEdition = 3, CpuCount = 16, PhysicalMemoryMb = 131_072, SocketCount = 2, CoresPerSocket = 4,
        };

        Assert.Equal(16, row.CpuCount);
        Assert.Equal(131_072L, row.PhysicalMemoryMb);
        Assert.Equal(2, row.SocketCount);
        Assert.Equal(4, row.CoresPerSocket);
        Assert.Null(row.HardwareUnavailableReason);
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

    [Fact]
    public void FinOpsUtilizationCard_AsksTheSharedRule_ForTheWordsAroundItsMemoryFigures()
    {
        var tab = ReadRepoFile("Lite/Controls/FinOpsTab.xaml.cs");
        var xaml = ReadRepoFile("Lite/Controls/FinOpsTab.xaml");
        var read = ReadRepoFile("Lite/Services/LocalDataService.FinOps.Utilization.cs");

        Assert.Contains("ServerHardwareScope.HardwareIsTheHosts(data.EngineEdition)", tab, StringComparison.Ordinal);
        Assert.Contains("PhysicalMemoryCaption.Text = ServerHardwareScope.PhysicalMemoryCaption(data.EngineEdition);", tab, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"PhysicalMemoryCaption\"", xaml, StringComparison.Ordinal);
        Assert.Contains("ServerHardwareScope.OverProvisionedExplanation(", tab, StringComparison.Ordinal);
        Assert.Contains("ServerHardwareScope.RightSizedExplanation(", tab, StringComparison.Ordinal);
        Assert.DoesNotContain("of physical RAM", tab, StringComparison.Ordinal);
        /* The CPU count is resolved through the edition (AzureSqlDatabaseHostMathTests pins the CASE): off edition 5 it is still
           COALESCE(vcore_count, cpu_count), and the edition still rides along for the card. */
        Assert.Contains("ELSE COALESCE(vcore_count, cpu_count) END AS cpu_count, engine_edition", read, StringComparison.Ordinal);
        Assert.Contains("EngineEdition = reader.IsDBNull(16)", read, StringComparison.Ordinal);
    }

    [Fact]
    public void GetServerPropertiesTool_BuildsItsPayloadThroughTheScopedBuilder()
    {
        var tool = ReadRepoFile("Lite/Mcp/McpServerInfoTools.cs");

        Assert.Contains("return ServerPropertiesPayload(resolved.ServerName, row);", tool, StringComparison.Ordinal);
        Assert.Contains("ServerHardwareScope.ScopeServerPropertiesPayload(", tool, StringComparison.Ordinal);
    }
}
