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
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #8095: five web pages whose sentence or control describes the data beside it. The shipped pages are run under Node on a fake DOM
/// (<c>web-text-truths-8095-harness.mjs</c>, and <c>finops-postgres-target-harness.mjs</c> for FinOps) with fixture answers, and the
/// words a reader meets are read back. Skipped when Node is not installed.
/// </summary>
public sealed class WebTextTruths8095Tests
{
    private static JsonElement Run(string harness, string scenario, Action<ProcessStartInfo>? configure = null)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", harness));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);
        configure?.Invoke(psi);
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
                Assert.Fail("the harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static JsonElement RunTruths(string scenario) => Run("web-text-truths-8095-harness.mjs", scenario);

    private static string[] Strings(JsonElement array) => array.EnumerateArray().Select(e => e.GetString() ?? "").ToArray();

    /// <summary>Item 1: an engine-gate answer whose engine is Amazon RDS for SQL Server names the engine and what it does not expose, the way
    /// the Azure line names its engine, instead of the generic "does not apply to this kind of server" line that sits above a Job History
    /// grid full of Agent runs. Any other engine keeps the generic line.</summary>
    [Fact]
    public void AnAmazonRdsEngineGateAnswer_NamesTheEngineAndWhatItDoesNotExpose()
    {
        var r = RunTruths("rds-line");

        var rds = r.GetProperty("rds").GetString()!;
        var generic = r.GetProperty("generic").GetString()!;
        Assert.NotEqual(generic, rds);
        Assert.StartsWith("Amazon RDS for SQL Server does not expose the SQL Agent running-job snapshot", rds, StringComparison.Ordinal);
        Assert.DoesNotContain("running_jobs", rds, StringComparison.Ordinal);
        Assert.Equal("Azure SQL Database does not have this data.", r.GetProperty("azure").GetString());
        Assert.Equal(generic, r.GetProperty("other").GetString());
    }

    /// <summary>Item 2: the "Needs attention only" control says what its predicate keeps (not Healthy, or Healthy with a warning tile), and
    /// the filter notice and the overflow line say how many of the servers they count are Healthy-banded servers with an advisory, so
    /// "1 Critical" in the roll-up and "3 need attention" below it can be reconciled on one screen.</summary>
    [Fact]
    public void TheNeedsAttentionFilter_SaysWhatItKeeps_AndCountsTheHealthyServersWithAdvisories()
    {
        var r = RunTruths("fleet-attention");

        var title = r.GetProperty("title").GetString()!;
        Assert.Contains("warning tile", title, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("any band other than Healthy", title, StringComparison.Ordinal);
        Assert.Equal(3, r.GetProperty("shown").GetInt32());
        var notice = Assert.Single(Strings(r.GetProperty("notice")));
        Assert.Equal("Needs attention only — showing 3 of 4 (2 healthy with advisories).", notice);
        var more = Assert.Single(Strings(r.GetProperty("more")));
        Assert.Equal("+ 2 more need attention (2 healthy with advisories)", more);
    }

    /// <summary>Item 4: under All servers a not_collected answer is the whole store's (no registered server runs SQL Agent), so the sentence
    /// names the store, not "this server". A server picked by name keeps the page's own sentence, an Azure SQL Database keeps its line, and
    /// an ordinary empty answer gets no engine sentence at all.</summary>
    [Fact]
    public void JobHistoryUnderAllServers_SaysNoServerInTheStoreRunsSqlAgent()
    {
        var r = RunTruths("job-history-fleet");

        Assert.Equal("None of this store's servers runs SQL Agent, so there is no job history.", r.GetProperty("allServers").GetString());
        Assert.Equal(JsonValueKind.Null, r.GetProperty("oneServer").ValueKind);
        Assert.Equal("Azure SQL Database has no SQL Agent, so it has no job history.", r.GetProperty("azureAllServers").GetString());
        Assert.Equal(JsonValueKind.Null, r.GetProperty("emptyAllServers").ValueKind);
    }

    /// <summary>Item 5: on a store whose every target is PostgreSQL, the FinOps page says once at the top that FinOps covers SQL Server
    /// targets and this store has none, above the panels that each say "not collected". A store with a SQL Server target gets no such line.</summary>
    [Fact]
    public void FinOpsOnAStoreWithOnlyPostgresTargets_SaysSoOnceAtTheTop()
    {
        static JsonElement Strips(string registry) => Run("finops-postgres-target-harness.mjs", "strips", psi =>
        {
            psi.Environment["HARNESS_REGISTRY"] = registry;
            psi.Environment["HARNESS_READS"] = "{}";
            psi.Environment["HARNESS_SERVER"] = "AA-PG";
            psi.Environment["HARNESS_TAB"] = "utilization";
        }).GetProperty("strips");

        var onlyPg = JsonSerializer.Serialize(new object[]
        {
            new { server_name = "AA-PG", display_name = "AA-PG", engine_kind = "postgres" },
            new { server_name = "ZZ-PG", display_name = "ZZ-PG", engine_kind = "aurora-postgres" },
        });
        var mixed = JsonSerializer.Serialize(new object[]
        {
            new { server_name = "AA-PG", display_name = "AA-PG", engine_kind = "postgres" },
            new { server_name = "CC-SQL", display_name = "CC-SQL", engine_kind = "sqlserver" },
        });

        const string line = "FinOps covers SQL Server targets, and this store has none. Every panel below says so for its own data.";
        Assert.Equal(line, Strings(Strips(onlyPg)).First());
        Assert.DoesNotContain(line, Strings(Strips(mixed)));
    }

    /// <summary>Item 6: a store section of the front door's Fleet Overview carries the reading aids the single-store page has: the
    /// "Needs attention" list, the overflow line and the "Updated" stamp. A row in the list opens the server in that section's store.</summary>
    [Fact]
    public void AFrontDoorStoreSection_ListsTheServersThatNeedAttention_AndSaysWhenItWasUpdated()
    {
        var r = RunTruths("front-door-fleet");

        Assert.Contains("Needs attention", Strings(r.GetProperty("titles")));
        var row = Assert.Single(Strings(r.GetProperty("rows")));
        Assert.Contains("alpha", row, StringComparison.Ordinal);
        Assert.Contains("CPU 95%", row, StringComparison.Ordinal);
        var more = Assert.Single(Strings(r.GetProperty("more")));
        Assert.Equal("+ 2 more need attention (2 healthy with advisories)", more);
        var updated = Assert.Single(Strings(r.GetProperty("updated")));
        Assert.StartsWith("Updated ", updated, StringComparison.Ordinal);
        Assert.Equal(4, r.GetProperty("cards").GetInt32());
        Assert.Equal("#/s/use1/server/alpha", r.GetProperty("hash").GetString());
    }
}
