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
        Assert.Equal(5, why.Length);
        Assert.DoesNotContain(why, w => Regex.IsMatch(w, @"get_[a-z0-9_]+"));
        Assert.Contains("read the I/O tab over a busy window for wal_bytes", why[0], StringComparison.Ordinal);
        Assert.Contains("(the Activity tab) rather than assuming", why[1], StringComparison.Ordinal);
        Assert.Contains("The Activity tab has the statements writing the most temp blocks", why[2], StringComparison.Ordinal);
        Assert.Contains("scanning in the Activity tab", why[3], StringComparison.Ordinal);
    }

    [Fact]
    public void TheSqlServerWhyCell_IsPlainToo()
    {
        var why = Why("sql").Single();
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
