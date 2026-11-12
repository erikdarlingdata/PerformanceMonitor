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
/// #8096: two layout details of the web dashboard. A view card's or alert rule card's name is clipped to one line with an ellipsis by the
/// stylesheet, so the span carries the whole name as its title, the way every other clipped text on the dashboard does. The Collection Log's
/// Error and Run note cells are one-line clamps that open on a click, with the whole text in the cell, so a long note never grows a row to
/// several lines while it sits past the grid's visible edge. The shipped pages and the shared renderer are drawn under Node on a fake DOM
/// (<c>web-layout-details-8096-harness.mjs</c>). Skipped when Node is not installed.
/// </summary>
public sealed class WebLayoutDetails8096Tests
{
    private static JsonElement Run(string piece)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-layout-details-8096-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(piece);
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
                Assert.Fail("the layout details harness did not finish in 20 s for " + piece);
            }

            Assert.True(proc.ExitCode == 0, "the layout details harness failed for " + piece + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    /// <summary>One card was drawn, and its clipped name span carries the whole name as its title.</summary>
    private static void AssertTheOneCardNameCarriesItsTitle(JsonElement r)
    {
        var names = r.GetProperty("names").EnumerateArray().ToArray();
        var name = Assert.Single(names);
        var text = name.GetProperty("text").GetString();
        Assert.False(string.IsNullOrEmpty(text));
        Assert.Equal(text, name.GetProperty("title").GetString());
    }

    [Fact]
    public void AViewCardsName_CarriesTheWholeNameAsItsTitle()
        => AssertTheOneCardNameCarriesItsTitle(Run("views"));

    [Fact]
    public void AnAlertRuleCardsName_CarriesTheWholeNameAsItsTitle()
        => AssertTheOneCardNameCarriesItsTitle(Run("alert-rules"));

    /// <summary>The cell is a one-line clamp (the <c>clamp</c> cell class, never <c>wrap</c>), and the clamp node inside it holds the whole
    /// text as its content and as its title, so a selection, the tooltip and a screen reader read all of it.</summary>
    private static void AssertOneLineClampWithTheWholeText(JsonElement cell)
    {
        var classes = cell.GetProperty("tdClass").GetString()!.Split(' ');
        Assert.Contains("clamp", classes);
        Assert.DoesNotContain("wrap", classes);
        var text = cell.GetProperty("text").GetString();
        Assert.False(string.IsNullOrEmpty(text));
        var clamp = cell.GetProperty("clamp");
        Assert.NotEqual(JsonValueKind.Null, clamp.ValueKind);
        Assert.Equal(text, clamp.GetProperty("text").GetString());
        Assert.Equal(text, clamp.GetProperty("title").GetString());
    }

    [Fact]
    public void TheCollectionLogsErrorAndRunNoteCells_AreOneLineClamps_WithTheWholeTextInTheCell()
    {
        var r = Run("collection-log");

        var headers = r.GetProperty("headers").EnumerateArray().Select(h => h.GetString()).ToArray();
        Assert.Equal(new[] { "Error", "Run note" }, headers.TakeLast(2).ToArray());
        AssertOneLineClampWithTheWholeText(r.GetProperty("runNote"));
        AssertOneLineClampWithTheWholeText(r.GetProperty("error"));
    }
}
