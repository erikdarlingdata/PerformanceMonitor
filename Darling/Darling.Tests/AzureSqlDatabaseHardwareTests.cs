/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// On an Azure SQL Database (engine edition 5) four of the collected <c>server_properties</c> hardware columns are the
/// HOST's: a 1-vCore serverless General Purpose database read 0 sockets, 32 cores per socket, a hyperthread ratio of 64 and
/// 911.9 GB of physical memory. Nothing may present those as the database's. The fifth column, <c>cpu_count</c>, is the
/// database's own scheduler count (that database read 2), so it is shown as read. The service objective and the
/// <c>vcore_count</c> parsed from it describe the allocation. The <c>memory_stats</c> table is a different source: its memory
/// figures are the database's own and are pinned in <see cref="AzureSqlDatabaseMemoryScopeTests"/>.
///
/// <para>Pinned where the rule is applied: the <c>get_server_properties</c> payload (which the web Server Properties
/// list reads through <c>/api/read</c>), the web list's own tiles, the FinOps Server Inventory row the grid binds, and
/// the words the FinOps utilization card takes from the shared rule. Every test has an edition-3 twin that keeps today's
/// values. Lite.Tests pins the same table for the other app, in the same words.</para>
/// </summary>
public sealed class AzureSqlDatabaseHardwareTests
{
    /// <summary>The four columns that describe the host on an Azure SQL Database. <c>cpu_count</c> is not among them.</summary>
    private static readonly string[] s_hostKeys =
        ["hyperthread_ratio", "socket_count", "cores_per_socket", "physical_memory_mb"];

    private static DarlingDataReader.ServerPropertiesReadRow HostRow(int engineEdition, string edition, string? objective, int? vcores) => new(
        new DateTime(2026, 9, 30, 12, 0, 0, DateTimeKind.Utc), edition, "12.0.2000.8", "RTM", null,
        engineEdition, 2, 64, 933_888L, 0, 32, false, false, null, objective, null, null, vcores);

    private static JsonElement Payload(DarlingDataReader.ServerPropertiesReadRow row) =>
        JsonDocument.Parse(DarlingMcpDataTools.ServerPropertiesPayload("Srv", row)).RootElement.Clone();

    // ── get_server_properties ──

    [Fact]
    public void GetServerProperties_OnAzureSqlDatabase_ReturnsTheHostsFourAsNull_PassesItsOwnCpuCountThrough_AndReturnsTheVcores_WithANote()
    {
        var json = Payload(HostRow(5, "SQL Azure", "GP_S_Gen5_1", 1));

        foreach (var key in s_hostKeys)
            Assert.Equal(JsonValueKind.Null, json.GetProperty(key).ValueKind);

        /* The stored count is the database's own scheduler count (2 for this 1-vCore database), not the host's, and not the vCores. */
        Assert.Equal(2, json.GetProperty("cpu_count").GetInt32());

        Assert.Equal("GP_S_Gen5_1", json.GetProperty("service_objective").GetString());
        Assert.Equal(1, json.GetProperty("vcore_count").GetInt32());
        var note = json.GetProperty("hardware_note").GetString();
        Assert.Equal(ServerHardwareScope.McpHardwareNote, note);
        Assert.Contains("service_objective", note, StringComparison.Ordinal);
        Assert.Contains("vcore_count", note, StringComparison.Ordinal);
        Assert.Contains("describe the host machine, not this database", note, StringComparison.Ordinal);
        Assert.Contains("cpu_count is the database's own scheduler count", note, StringComparison.Ordinal);

        var names = json.EnumerateObject().Select(p => p.Name).ToList();
        Assert.Equal(names.IndexOf("service_objective") + 1, names.IndexOf("vcore_count"));
        Assert.Equal(5, json.GetProperty("engine_edition").GetInt32());
    }

    [Fact]
    public void GetServerProperties_OnAzureSqlDatabase_WithNoVcores_StillHidesTheHost_KeepsItsCpuCount_AndReturnsANullVcoreCount()
    {
        var json = Payload(HostRow(5, "SQL Azure", "S0", null));

        foreach (var key in s_hostKeys)
            Assert.Equal(JsonValueKind.Null, json.GetProperty(key).ValueKind);
        Assert.Equal(2, json.GetProperty("cpu_count").GetInt32());
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
    public void ServerPropertiesRead_CarriesTheStoredVcoreCount()
    {
        Assert.Contains("vcore_count", DarlingDataReader.LatestServerPropertiesSql, StringComparison.Ordinal);
        Assert.Equal(1, HostRow(5, "SQL Azure", "GP_S_Gen5_1", 1).VcoreCount);
    }

    // ── the web Server Properties list ──

    [Fact]
    public void WebServerProperties_DescriptorHidesTheHostFour_OnEdition5_KeepsLogicalCpus_AndShowsTheServiceObjectiveAndVcores()
    {
        var js = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");
        var start = js.IndexOf("const PROPERTY_STATS = [", StringComparison.Ordinal);
        Assert.True(start > 0, "PROPERTY_STATS is missing");
        var list = js[start..js.IndexOf("];", start, StringComparison.Ordinal)];

        Assert.Contains("const AZURE_SQL_DATABASE = { key: \"engine_edition\", equals: 5 };", js, StringComparison.Ordinal);
        foreach (var key in s_hostKeys)
        {
            var line = list.Split('\n').Single(l => l.Contains($"key: \"{key}\"", StringComparison.Ordinal));
            Assert.Contains("hideWhen: AZURE_SQL_DATABASE", line, StringComparison.Ordinal);
        }

        /* Logical CPUs is the database's own scheduler count on edition 5, so its tile is drawn there like any other. */
        var cpuCount = list.Split('\n').Single(l => l.Contains("key: \"cpu_count\"", StringComparison.Ordinal));
        Assert.Contains("label: \"Logical CPUs\"", cpuCount, StringComparison.Ordinal);
        Assert.DoesNotContain("hideWhen", cpuCount, StringComparison.Ordinal);
        Assert.DoesNotContain("showWhen", cpuCount, StringComparison.Ordinal);

        Assert.Contains("{ key: \"vcore_count\", label: \"vCores\", format: \"int\", showWhen: AZURE_SQL_DATABASE }", list, StringComparison.Ordinal);
        var objective = list.Split('\n').Single(l => l.Contains("key: \"service_objective\"", StringComparison.Ordinal));
        Assert.DoesNotContain("hideWhen", objective, StringComparison.Ordinal);
        Assert.DoesNotContain("showWhen", objective, StringComparison.Ordinal);
    }

    [Fact]
    public void WebStatRenderer_DropsTilesByTheirCondition_OnEdition5_AndKeepsEveryTileOtherwise()
    {
        var panels = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "panels.js");
        Assert.Contains("const stats = visibleStats(Array.isArray(desc.stats) ? desc.stats : [], data);", panels, StringComparison.Ordinal);

        var script = """
            const m = await import(process.argv[1]);
            const H = { key: "engine_edition", equals: 5 };
            const stats = [
              { key: "cpu_count" }, { key: "socket_count", hideWhen: H }, { key: "physical_memory_mb", hideWhen: H },
              { key: "service_objective" }, { key: "vcore_count", showWhen: H },
            ];
            const names = (d) => m.visibleStats(stats, d).map((s) => s.key).join(",");
            console.log(JSON.stringify({ e5: names({ engine_edition: 5 }), e3: names({ engine_edition: 3 }), none: names({}) }));
            """;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add("--input-type=module");
        psi.ArgumentList.Add("-e");
        psi.ArgumentList.Add(script);
        psi.ArgumentList.Add(new Uri(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "panels.js")).AbsoluteUri);

        Process proc;
        try { proc = Process.Start(psi)!; }
        catch (Win32Exception) { Assert.Skip("Node is not installed, so the shipped page script cannot be run."); throw; } // The source pins above still hold the change in place.

        using (proc)
        {
            var output = proc.StandardOutput.ReadToEnd().Trim();
            var error = proc.StandardError.ReadToEnd();
            Assert.True(proc.WaitForExit(20000) && proc.ExitCode == 0, "node failed: " + error);
            using var doc = JsonDocument.Parse(output);
            Assert.Equal("cpu_count,service_objective,vcore_count", doc.RootElement.GetProperty("e5").GetString());
            Assert.Equal("cpu_count,socket_count,physical_memory_mb,service_objective", doc.RootElement.GetProperty("e3").GetString());
            Assert.Equal("cpu_count,socket_count,physical_memory_mb,service_objective", doc.RootElement.GetProperty("none").GetString());
        }
    }

    // ── FinOps Server Inventory row ──

    [Fact]
    public void InventoryRow_OnAzureSqlDatabase_LeavesTheHostCellsBlank_ShowsItsOwnCpuCount_AndSaysWhy()
    {
        var row = new ServerPropertyRow
        {
            Edition = "SQL Azure", EngineEdition = 5, CpuCount = 2, PhysicalMemoryMb = 933_888, SocketCount = 0, CoresPerSocket = 32,
        };

        Assert.Equal(2, row.CpuCount);
        Assert.Null(row.PhysicalMemoryMb);
        Assert.Null(row.SocketCount);
        Assert.Null(row.CoresPerSocket);
        Assert.Equal(ServerHardwareScope.InventoryHardwareNote, row.HardwareUnavailableReason);
        Assert.Contains("memory, sockets, cores per socket and hyperthread ratio are the host's", row.HardwareUnavailableReason, StringComparison.Ordinal);
        Assert.Contains("Logical CPUs is the database's own scheduler count", row.HardwareUnavailableReason, StringComparison.Ordinal);
    }

    [Fact]
    public void InventoryRow_OnAzureSqlDatabase_DoesNotDependOnTheOrderTheLoaderAssignsInAndKeepsAReadsOwnReason()
    {
        var row = new ServerPropertyRow { CpuCount = 2, PhysicalMemoryMb = 933_888, SocketCount = 0, CoresPerSocket = 32 };
        Assert.Equal(933_888L, row.PhysicalMemoryMb);

        row.EngineEdition = 5;
        row.HardwareUnavailableReason = "Hardware read denied";

        Assert.Null(row.PhysicalMemoryMb);
        Assert.Equal(2, row.CpuCount);
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

    // ── FinOps utilization card ──

    [Fact]
    public void FinOpsUtilizationCard_AsksTheSharedRule_ForTheWordsAroundItsMemoryFigures()
    {
        var tab = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.Loaders.cs");
        var xaml = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.xaml");
        var read = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "FinOps", "DarlingFinOpsUtilizationReader.cs");
        var figures = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "FinOps", "FinOpsUtilizationFigures.cs");

        Assert.Contains("ServerHardwareScope.HardwareIsTheHosts(d.EngineEdition)", figures, StringComparison.Ordinal);
        Assert.Contains("FinOpsUtilizationFigures.Explanation(", tab, StringComparison.Ordinal);
        Assert.Contains("FinOpsPhysicalMemoryCaption.Text = ServerHardwareScope.PhysicalMemoryCaption(data.EngineEdition);", tab, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"FinOpsPhysicalMemoryCaption\"", xaml, StringComparison.Ordinal);
        Assert.Contains("ServerHardwareScope.OverProvisionedExplanation(", figures, StringComparison.Ordinal);
        Assert.Contains("ServerHardwareScope.RightSizedExplanation(", figures, StringComparison.Ordinal);
        Assert.DoesNotContain("of physical RAM", tab, StringComparison.Ordinal);
        Assert.DoesNotContain("of physical RAM", figures, StringComparison.Ordinal);
        /* The CPU count is resolved through the edition (AzureSqlDatabaseHostMathTests pins the CASE): off edition 5 it is still
           COALESCE(vcore_count, cpu_count), and the edition still rides along for the card. */
        Assert.Contains("ELSE COALESCE(vcore_count, cpu_count) END AS cpu_count, engine_edition", read, StringComparison.Ordinal);
        Assert.Contains("EngineEdition: reader.IsDBNull(16)", read, StringComparison.Ordinal);
    }

    [Fact]
    public void GetServerPropertiesTool_BuildsItsPayloadThroughTheScopedBuilder()
    {
        var tool = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs");

        Assert.Contains("return ServerPropertiesPayload(resolved.ServerName, row);", tool, StringComparison.Ordinal);
        Assert.Contains("ServerHardwareScope.ScopeServerPropertiesPayload(", tool, StringComparison.Ordinal);
    }
}
