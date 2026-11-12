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
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #7557: the Why column of the Configuration Audit (PostgreSQL and SQL Server) is read as page text: no MCP tool name shows, a read becomes the tab that
/// shows it, and a sentence that opens with the swapped name opens with a capital. These run the shipped <c>pg-config.js</c>, <c>sql-config.js</c> and
/// <c>panels.js</c> under Node (<c>web-config-audit-why-harness.mjs</c>); skipped when Node is not installed.
/// </summary>
public sealed class WebConfigAuditWhyBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-config-audit-why-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped panel script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the config audit harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the config audit harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Why(string grid) => Run().GetProperty(grid).EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void ThePostgresWhyCells_NameNoToolAndNameTheTabThatShowsTheNumber()
    {
        var why = Why("pg");
        Assert.Equal(7, why.Length);
        Assert.DoesNotContain(why, w => Regex.IsMatch(w, @"get_[a-z0-9_]+"));
        Assert.Contains("read the I/O tab over a busy window for wal_bytes", why[0], StringComparison.Ordinal);
        Assert.Contains("(the Activity tab) rather than assuming", why[1], StringComparison.Ordinal);
        Assert.Contains("The Activity tab has the statements writing the most temp blocks", why[2], StringComparison.Ordinal);
        Assert.Contains("scanning in the Activity tab", why[3], StringComparison.Ordinal);
        // The Deadlock Graphs and Logging Settings Audit panels are on the Activity tab, so those reads are named by it too, not left as tool names.
        // Two reads on that one tab ("get_pg_deadlocks, then get_pg_deadlock_detail for one") name the tab once.
        Assert.Equal("Read the captured graphs (the Activity tab for one) for the lock modes and the statements on each side.", why[5]);
        Assert.Equal("Check the log settings with the Activity tab before changing anything.", why[6]);
    }

    [Fact]
    public void TheSqlServerWhyCell_ForTheRealMaxdopMessage_IsUnchangedByPlain()
    {
        // No SQL Server audit_config message names a get_ tool today, so plain: true leaves the real MAXDOP-0 sentence exactly as the service wrote it.
        const string real = "MAXDOP is 0 (unlimited). This lets one query fan out across all schedulers, leading to CXPACKET waits and thread exhaustion under load. " +
            "Microsoft's guidance is topology-based: keep MAXDOP at or under the logical processors in a single NUMA node, capped at 8. " +
            "Start with 8 (8 logical processors in one NUMA node, capped at 8) and adjust to the workload.";
        Assert.Equal(real, Why("sql").Single());
    }

    [Fact]
    public void TheSqlServerWhyCell_IsPlainToo_IfAMessageEverNamesARead()
    {
        // Synthetic: no SQL Server message names a read today. This guards the SQL grid keeping the same rule as the PostgreSQL grid if one ever does.
        var why = Why("sqlSynthetic").Single();
        Assert.DoesNotMatch(new Regex(@"get_[a-z0-9_]+"), why);
        Assert.Contains("the Wait Stats tab", why, StringComparison.Ordinal);
        Assert.Contains("the Queries tab", why, StringComparison.Ordinal);
    }

    [Fact]
    public void ASentenceThatOpensWithASwappedToolName_OpensWithACapital()
    {
        // The last PostgreSQL row is one sentence that begins with the tool name, and the third opens its second sentence with one.
        Assert.StartsWith("The I/O tab over a busy window", Why("pg")[4], StringComparison.Ordinal);
        Assert.DoesNotContain(Why("pg"), w => Regex.IsMatch(w, @"(^|[.!?] )the [A-Z/]"));
    }
}
