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
/// #7539: the PostgreSQL Activity tab's Plan Capture Readiness grid names every facet in words, and the Logging Settings Audit grid names no MCP tool and
/// keeps Unlocks and Recommended short with the whole text one click away. These run the shipped <c>pg-activity.js</c> and <c>panels.js</c> under Node
/// (<c>web-pg-logging-audit-harness.mjs</c>); skipped when Node is not installed.
/// </summary>
public sealed class WebPgLoggingAuditBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-pg-logging-audit-harness.mjs"));
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
                Assert.Fail("the logging audit harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the logging audit harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void EveryPlanCaptureFacet_ShowsWords_AndNoneShowsItsRawKey()
    {
        var cells = Run().GetProperty("facetCells").EnumerateArray().Select(e => e.GetString()!).ToArray();
        Assert.Equal(7, cells.Length);
        Assert.Equal("Log line prefix is readable", cells[^1]);
        Assert.DoesNotContain(cells, c => Regex.IsMatch(c, "^[a-z0-9]+(_[a-z0-9]+)+$"));
    }

    [Fact]
    public void TheLoggingAuditCells_NameNoMcpTool()
    {
        var drawn = Run().GetProperty("auditRows").EnumerateArray()
            .SelectMany(row => row.EnumerateArray())
            .SelectMany(cell => new[] { cell.GetProperty("text").GetString(), cell.GetProperty("summary").GetString(), cell.GetProperty("detail").GetString() })
            .Where(t => t is not null);
        foreach (var text in drawn)
        {
            Assert.DoesNotMatch(new Regex(@"\bget_[a-z0-9_]+"), text!);
        }
    }

    [Fact]
    public void UnlocksAndRecommended_ShowShortText_WithTheWholeTextOneClickAway()
    {
        var first = Run().GetProperty("auditRows")[0];
        // Setting, Verdict, Value, Change needs, Recommended, Unlocks, Cost and remedy.
        Assert.Equal(7, first.GetArrayLength());
        foreach (var at in new[] { 4, 5 })
        {
            var cell = first[at];
            var summary = cell.GetProperty("summary").GetString()!;
            var detail = cell.GetProperty("detail").GetString()!;
            Assert.True(summary.Length <= 70, "the closed text is short: " + summary);
            Assert.True(detail.Length > 150, "the whole text opens on a click: " + detail);
            Assert.Contains("the Activity tab", detail, StringComparison.Ordinal);
        }
    }
}
