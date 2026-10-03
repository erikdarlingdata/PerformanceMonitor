/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Source pins for the FinOps Utilization tab's raw panels (#4843): the CPU chart, memory tiles and database
/// size table, each over a read that already exists. Text scans, like <see cref="FinOpsPageShellTests"/>.
/// </summary>
public sealed class FinOpsTabUtilizationRawPageTests
{
    private static readonly string[] Svc = ["Darling", "PerformanceMonitor.Darling.Service"];

    private static string Tab() =>
        ReadRepoFileLf(Svc.Concat(["wwwroot", "js", "pages", "finops", "utilization.js"]).ToArray()).ReplaceLineEndings("\n");

    private static string Cs(params string[] rel) =>
        ReadRepoFileLf(rel).ReplaceLineEndings("\n");

    [Fact]
    public void TheTab_CallsTheThreeExistingReadsWithItsParams()
    {
        var src = Tab();
        Assert.Contains("read: \"get_cpu_utilization\"", src);
        Assert.Contains("params: { server, hours: CPU_HOURS }", src);
        Assert.Contains("const CPU_HOURS = 24;", src);
        Assert.Contains("read: \"get_memory_stats\"", src);
        Assert.Contains("read: \"get_database_sizes\"", src);
        Assert.Contains("viz: \"line\"", src);
        Assert.Contains("viz: \"stat\"", src);
        Assert.Contains("viz: \"table\"", src);
    }

    /// <summary>The text from <paramref name="start"/> up to the next occurrence of <paramref name="end"/>, so a key is
    /// matched inside the one method that emits it and not anywhere in a large file.</summary>
    private static string Slice(string src, string start, string end)
    {
        var i = src.IndexOf(start, StringComparison.Ordinal);
        Assert.True(i >= 0, "missing: " + start);
        var j = src.IndexOf(end, i + start.Length, StringComparison.Ordinal);
        Assert.True(j > i, "missing end: " + end);
        return src.Substring(i, j - i);
    }

    [Fact]
    public void EveryColumnKeyShown_IsEmittedByItsReadsSource()
    {
        var src = Tab();
        var trend = Cs("PerformanceMonitor.Common", "Mcp", "TrendPayloads.cs");
        var data = Cs(Svc.Concat(["Mcp", "DarlingMcpDataTools.cs"]).ToArray());
        var sizes = Cs(Svc.Concat(["Mcp", "DarlingMcpObjectStatsTools.cs"]).ToArray());
        var sibling = Cs("PerformanceMonitor.Common", "AzureSiblingDatabaseSize.cs");

        // The CPU series come from the shared catalog; pin that, then that the read emits every key the catalog names.
        Assert.Contains("READ_FIELDS.get_cpu_utilization.line.series", src);
        var cpu = Slice(trend, "public static string CpuUtilization(", "TempDbFiguresNote");
        foreach (var k in new[] { "sample_time", "sql_server_cpu", "other_process_cpu", "total_cpu" })
            Assert.Contains(k + " = ", cpu);

        var memory = Slice(data, "internal static string MemoryStatsPayload(", "[McpServerTool(Name = \"get_memory_clerks\")");
        foreach (var k in new[]
        {
            "captured_at", "total_physical_memory_mb", "available_physical_memory_mb", "memory_utilization_pct", "total_server_memory_mb",
            "target_server_memory_mb", "buffer_pool_mb", "plan_cache_mb", "system_memory_state", "system_memory_state_note",
            "sql_memory_model", "engine_edition",
        })
        {
            Assert.Contains("\"" + k + "\"", src);
            Assert.Contains(k + " = ", memory);
        }

        Assert.Contains("ServerHardwareScope.WithMemoryNote(", memory);
        Assert.Contains("\"memory_note\"", src);

        // The per-database payload only: FilePayload's per-file keys must not satisfy these.
        Assert.Contains("READ_FIELDS.get_database_sizes.table.columns", src);
        var perDatabase = Slice(sizes, "internal static string DatabaseSizesPayload(", "private static Dictionary<string, object?> FilePayload(");
        foreach (var k in new[] { "database_name", "total_size_mb", "used_size_mb" })
            Assert.Contains("[\"" + k + "\"] =", perDatabase);
        Assert.Matches(new Regex("\\n\\s+databases\\n"), perDatabase);
        Assert.Contains("captured_at = ", perDatabase);
        Assert.Contains("AzureSiblingDatabaseSize.RowNoteKey", perDatabase);
        Assert.Contains("RowNoteKey = \"size_note\"", sibling);
        Assert.Contains("\"captured_at\"", src);
    }

    [Fact]
    public void TheMemoryTiles_NameTheLimitOnAnAzureSqlDatabase_AndShowTheMemoryNote()
    {
        var src = Tab().ReplaceLineEndings("\n");
        Assert.Contains("const AZURE_SQL_DATABASE = { key: \"engine_edition\", equals: 5 };", src);
        Assert.Contains("{ key: \"total_physical_memory_mb\", label: \"Physical\", format: \"mb\", hideWhen: AZURE_SQL_DATABASE }", src);
        Assert.Contains("{ key: \"total_physical_memory_mb\", label: \"Memory limit\", format: \"mb\", showWhen: AZURE_SQL_DATABASE }", src);
        Assert.Contains("{ key: \"available_physical_memory_mb\", label: \"Available\", format: \"mb\", hideWhen: AZURE_SQL_DATABASE }", src);
        Assert.Contains("{ key: \"available_physical_memory_mb\", label: \"Available under limit\", format: \"mb\", showWhen: AZURE_SQL_DATABASE }", src);
        Assert.Contains("noteKey: \"memory_note\"", src);

        // Same predicate as the Server page, so the two cannot drift.
        var server = Cs(Svc.Concat(["wwwroot", "js", "pages", "server-tabs.js"]).ToArray());
        Assert.Contains("const AZURE_SQL_DATABASE = { key: \"engine_edition\", equals: 5 };", server);
    }

    [Fact]
    public void TheMemoryPanel_ShowsWhenItWasCollected_AndTheSizesAreReadOnce()
    {
        var src = Tab();
        var tile = "{ key: \"captured_at\", label: \"Collected\", format: \"reltime\", small: true }";
        Assert.Single(Regex.Matches(src, Regex.Escape(tile)));
        Assert.Single(Regex.Matches(src, "read: \"get_database_sizes\""));
        Assert.Contains("reltime: relTime,", ReadRepoFileLf(Svc.Concat(["wwwroot", "js", "util.js"]).ToArray()).ReplaceLineEndings("\n"));
    }

    [Fact]
    public void TheTab_ImportsOnlyFromTheSharedModules()
    {
        var imports = Regex.Matches(Tab(), "from \"([^\"]+)\"").Select(m => m.Groups[1].Value).ToList();
        Assert.NotEmpty(imports);
        Assert.All(imports, i => Assert.Contains(i, new[] { "../../panels.js", "../../charts.js", "../../util.js", "../../read-fields.js" }));
    }

    [Fact]
    public void TheTab_IsNoLongerTheShellStub_AndHasOneContainerPerSection()
    {
        var src = Tab();
        foreach (var id in new[] { "verdict", "cpu", "memory", "sizes" })
            Assert.Contains("section(\"" + id + "\"", src);
        Assert.DoesNotContain("Coming:", src);
        Assert.Contains("Not on the web yet: the provisioning verdict, health score, cost cards, 7-day trend and the Top Databases by Total CPU and Top Databases by Avg CPU / Execution grids. The desktop viewer shows them.", src);
    }
}
