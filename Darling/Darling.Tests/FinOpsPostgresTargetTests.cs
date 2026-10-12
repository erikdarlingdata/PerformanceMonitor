/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Darling web click-through, FinOps on a PostgreSQL target, run under Node (<c>finops-postgres-target-harness.mjs</c>): a
/// bare FinOps visit opens on the first SQL Server target in the sidebar's order, the server list says which targets are
/// PostgreSQL, and a panel that does not apply to a PostgreSQL target says one short line instead of the gate paragraph (a
/// SQL Server target keeps the server's own sentence). Node is skipped when it is not installed.
/// </summary>
public sealed class FinOpsPostgresTargetTests
{
    private const string Gate = "Collector query_stats is not collected for PostgreSQL targets; this server uses pg_stat_statements instead, which has none of these figures and is read on the PostgreSQL pages.";

    private static readonly string Registry = JsonSerializer.Serialize(new object[]
    {
        new { server_name = "AA-PG", display_name = "AA-PG", engine_kind = "postgres" },
        new { server_name = "BB-AURORA", display_name = "BB-AURORA", engine_kind = "aurora-postgres" },
        new { server_name = "CC-SQL", display_name = "CC-SQL", engine_kind = "sqlserver" },
        new { server_name = "DD-SQL", display_name = "DD-SQL", engine_kind = (string?)null },
    });

    private static JsonElement Run(string scenario, string registry, string reads = "{}", string server = "", string tab = "")
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "finops-postgres-target-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);
        psi.Environment["HARNESS_REGISTRY"] = registry;
        psi.Environment["HARNESS_READS"] = reads;
        psi.Environment["HARNESS_SERVER"] = server;
        psi.Environment["HARNESS_TAB"] = tab;

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the FinOps harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the FinOps harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            var result = doc.RootElement.Clone();
            Assert.Empty(result.GetProperty("rejections").EnumerateArray());
            return result;
        }
    }

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    [Fact]
    public void ABareFinOpsVisit_OpensOnTheFirstSqlServerTarget_AndTheListNamesThePostgresOnes()
    {
        var r = Run("open", Registry);

        Assert.Equal("CC-SQL", r.GetProperty("selected").GetString());
        Assert.Equal(["AA-PG (PostgreSQL)", "BB-AURORA (PostgreSQL)", "CC-SQL", "DD-SQL"], Strings(r.GetProperty("options")));
    }

    [Fact]
    public void ABareFinOpsVisit_WithOnlyPostgresTargets_StillOpensOnTheFirstOne()
    {
        var onlyPg = JsonSerializer.Serialize(new object[]
        {
            new { server_name = "AA-PG", display_name = "AA-PG", engine_kind = "postgres" },
            new { server_name = "ZZ-PG", display_name = "ZZ-PG", engine_kind = "postgres" },
        });

        Assert.Equal("AA-PG", Run("open", onlyPg).GetProperty("selected").GetString());
    }

    [Theory]
    [InlineData("high-impact")]
    [InlineData("application-connections")]
    [InlineData("database-resources")]
    [InlineData("database-sizes")]
    [InlineData("recommendations")]
    [InlineData("index-analysis")]
    public void APanelThatDoesNotApplyToAPostgresTarget_SaysOneShortLine(string tab)
    {
        var reads = JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["get_finops"] = new { status = "not_collected", message = Gate },
            ["get_finops_recommendations"] = new { status = "not_collected", message = Gate },
        });

        var pg = Strings(Run("strips", Registry, reads, "AA-PG", tab).GetProperty("strips"));
        Assert.Contains("Not collected for PostgreSQL", pg);
        Assert.DoesNotContain(pg, s => s.Contains("pg_stat_statements"));

        // The same answer for a SQL Server target keeps the server's own sentence.
        var sql = Strings(Run("strips", Registry, reads, "CC-SQL", tab).GetProperty("strips"));
        Assert.Contains(Gate, sql);
        Assert.DoesNotContain("Not collected for PostgreSQL", sql);
    }

    [Fact]
    public void TheSectionsOfOptimization_OnAPostgresTarget_SayOneShortLineEach()
    {
        var section = new { status = "not_collected", message = Gate, rows = Array.Empty<object>() };
        var reads = JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["get_finops"] = new { idle_databases = section, tempdb_pressure = section, wait_categories = section, expensive_queries = section, memory_grant_efficiency = section },
        });

        var pg = Strings(Run("strips", Registry, reads, "AA-PG", "optimization").GetProperty("strips"));
        Assert.DoesNotContain(pg, s => s.Contains("pg_stat_statements"));
        Assert.True(pg.Count(s => s == "Not collected for PostgreSQL") >= 5, string.Join(" | ", pg));

        var sql = Strings(Run("strips", Registry, reads, "CC-SQL", "optimization").GetProperty("strips"));
        Assert.True(sql.Count(s => s == Gate) >= 5, string.Join(" | ", sql));
    }

    [Fact]
    public void ThePanelsOfUtilizationAndLocking_OnAPostgresTarget_SayOneShortLine()
    {
        var reads = JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["get_finops"] = new { status = "not_collected", message = Gate },
            ["get_cpu_utilization"] = new { status = "not_collected", message = Gate },
            ["get_memory_stats"] = new { status = "not_collected", message = Gate },
            ["get_database_sizes"] = new { status = "not_collected", message = Gate },
            ["get_object_locking"] = new { status = "not_collected", message = Gate },
        });

        foreach (var tab in new[] { "utilization", "locking" })
        {
            var pg = Strings(Run("strips", Registry, reads, "AA-PG", tab).GetProperty("strips"));
            Assert.DoesNotContain(pg, s => s.Contains("pg_stat_statements"));
            Assert.Contains("Not collected for PostgreSQL", pg);
        }
    }
}
