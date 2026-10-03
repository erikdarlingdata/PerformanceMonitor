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

    [Fact]
    public void EveryColumnKeyShown_IsEmittedByItsReadsSource()
    {
        var src = Tab();
        var trend = Cs("PerformanceMonitor.Common", "Mcp", "TrendPayloads.cs");
        var data = Cs(Svc.Concat(["Mcp", "DarlingMcpDataTools.cs"]).ToArray());
        var sizes = Cs(Svc.Concat(["Mcp", "DarlingMcpObjectStatsTools.cs"]).ToArray());
        var sibling = Cs("PerformanceMonitor.Common", "AzureSiblingDatabaseSize.cs");

        foreach (var k in new[] { "sample_time", "sql_server_cpu", "other_process_cpu", "total_cpu" })
        {
            Assert.Contains("\"" + k + "\"", src);
            Assert.Matches(new Regex("\\b" + k + "\\b"), trend + Cs(Svc.Concat(["Mcp", "DarlingDataReader.cs"]).ToArray()));
        }

        foreach (var k in new[]
        {
            "total_physical_memory_mb", "available_physical_memory_mb", "memory_utilization_pct", "total_server_memory_mb",
            "target_server_memory_mb", "buffer_pool_mb", "plan_cache_mb", "system_memory_state", "system_memory_state_note",
            "sql_memory_model",
        })
        {
            Assert.Contains("\"" + k + "\"", src);
            Assert.Contains(k + " = ", data);
        }

        foreach (var k in new[] { "database_name", "total_size_mb", "used_size_mb", "databases" })
        {
            Assert.Contains("\"" + k + "\"", src);
            Assert.Contains("[\"" + k + "\"]", sizes + "[\"databases\"]");
        }

        Assert.Contains("\"size_note\"", src);
        Assert.Contains("RowNoteKey = \"size_note\"", sibling);
        Assert.Contains("\"note\"", src);
    }

    [Fact]
    public void TheTab_ImportsOnlyFromTheSharedModules()
    {
        var imports = Regex.Matches(Tab(), "from \"([^\"]+)\"").Select(m => m.Groups[1].Value).ToList();
        Assert.NotEmpty(imports);
        Assert.All(imports, i => Assert.Contains(i, new[] { "../../panels.js", "../../charts.js", "../../util.js" }));
    }

    [Fact]
    public void TheTab_IsNoLongerTheShellStub_AndHasOneContainerPerSection()
    {
        var src = Tab();
        Assert.DoesNotContain("Not on the web yet", src);
        foreach (var id in new[] { "verdict", "cpu", "memory", "sizes" })
            Assert.Contains("section(\"" + id + "\"", src);
        Assert.Contains("Coming:", src);
    }
}
