/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #8082: the Mute Rules and Known Patterns tables scroll sideways inside their own box, as every other grid does. The page's main
/// element clips sideways and the document never scrolls, so a table drawn straight into the page loses its right-hand columns (Expires,
/// Created and the Edit / Pause / Delete cell) at laptop widths. The shipped pages are drawn under Node on a fake DOM
/// (<c>web-list-table-wrap-harness.mjs</c>) with a rule and a pattern whose scope and free text run long. Skipped when Node is not
/// installed.
/// </summary>
public sealed class ListPageTableScrollTests
{
    private static JsonElement Run(string page)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-list-table-wrap-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(page);
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
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the list table harness did not finish in 20 s for page " + page);
            }

            Assert.True(proc.ExitCode == 0, "the list table harness failed for page " + page + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    /// <summary>The box the browser scrolls is the table's own parent, and it is a <c>.table-wrap</c>: the one container app.css gives
    /// <c>overflow: auto</c>. A plain div between the table and main would clip with the page instead.</summary>
    private static void AssertTableSitsInItsOwnScrollBox(JsonElement r)
    {
        Assert.Equal(1, r.GetProperty("tables").GetInt32());
        var boxes = r.GetProperty("boxes").EnumerateArray().Select(b => b.GetString()).ToArray();
        Assert.NotEmpty(boxes);
        Assert.Contains("table-wrap", (" " + boxes[0] + " ").Split(' '));
    }

    [Fact]
    public void TheMuteRulesTable_SitsInItsOwnScrollBox_WithTheActionCellInsideIt()
    {
        var r = Run("mute-rules");

        AssertTableSitsInItsOwnScrollBox(r);
        Assert.Contains("mute-rules", r.GetProperty("tableClass").GetString()!.Split(' '));
        var headers = r.GetProperty("headers").EnumerateArray().Select(h => h.GetString()).ToArray();
        Assert.Equal(new[] { "Expires", "Created", "" }, headers.TakeLast(3).ToArray());
        Assert.Equal(new[] { "Edit", "Pause", "Delete" }, r.GetProperty("actionButtons").EnumerateArray().Select(b => b.GetString()).ToArray());
    }

    [Fact]
    public void TheKnownPatternsTable_SitsInItsOwnScrollBox_WithTheActionCellInsideIt()
    {
        var r = Run("known-patterns");

        AssertTableSitsInItsOwnScrollBox(r);
        Assert.Contains("known-patterns", r.GetProperty("tableClass").GetString()!.Split(' '));
        var headers = r.GetProperty("headers").EnumerateArray().Select(h => h.GetString()).ToArray();
        Assert.Equal(new[] { "Expires", "Added by", "" }, headers.TakeLast(3).ToArray());
        Assert.Equal(new[] { "Edit", "Delete" }, r.GetProperty("actionButtons").EnumerateArray().Select(b => b.GetString()).ToArray());
    }
}
