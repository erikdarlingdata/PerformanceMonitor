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
/// The web PostgreSQL plan viewer (#5229), from the shipped <c>pg-plan-viewer.js</c> run under Node
/// (<c>web-pg-plan-viewer-harness.mjs</c>). Node is skipped when it is not installed.
/// </summary>
public sealed class PgPlanViewerBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-pg-plan-viewer-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);

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
                Assert.Fail("the pg plan viewer harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the pg plan viewer harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static string Str(JsonElement r, string name) => r.GetProperty(name).GetString()!;

    [Fact]
    public void ThePlanButton_OpensTheRowsOwnJson_IndentedAndWithoutARead()
    {
        var r = Run("open");
        Assert.True(r.GetProperty("buttonBefore").GetBoolean());
        Assert.False(r.GetProperty("preBefore").GetBoolean());
        Assert.Contains("\n  \"Plan\": {\n    \"Node Type\": \"Seq Scan\"", Str(r, "text"));
        Assert.True(r.GetProperty("hasCopy").GetBoolean());
        Assert.True(r.GetProperty("hasDownload").GetBoolean());
        Assert.Equal("true", Str(r, "expanded"));
        Assert.Equal(0, r.GetProperty("fetches").GetInt32());
        Assert.False(r.GetProperty("preAfterHide").GetBoolean());
    }

    [Fact]
    public void ThePlan_IsOnlyEverText()
    {
        var r = Run("textOnly");
        // The FakeNode DOM never parses markup, so these two counts are 0 whatever the module does. They are
        // not XSS protection. The Contains check below is: an innerHTML write would leave textContent empty.
        Assert.Equal(0, r.GetProperty("preChildElements").GetInt32());
        Assert.Equal(0, r.GetProperty("scriptNodes").GetInt32());
        Assert.Contains("<script>alert(1)</script>", Str(r, "text"));
    }

    [Fact]
    public void Download_IsAJsonFile_WithTheShownTextAndASafeName()
    {
        var r = Run("download");
        Assert.Equal("pg-plan-q_1_x-h_1_.json", Str(r, "name"));
        Assert.Equal("application/json", Str(r, "type"));
        Assert.Equal(Str(r, "shown"), Str(r, "body"));
        Assert.Equal("pg-plan--8126435036642491494-ABC123.json", Str(r, "fileName"));
        Assert.Equal("pg-plan-unknown-unknown.json", Str(r, "missing"));
    }

    [Fact]
    public void Copy_PutsTheShownTextOnTheClipboard()
    {
        var r = Run("copy");
        Assert.Equal(Str(r, "shown"), Str(r, "clip"));
    }

    [Fact]
    public void APlanThatIsNotJson_IsShownAsStored_WithDownloadOff()
    {
        var r = Run("unparsed");
        Assert.Contains("not valid JSON", Str(r, "notice"));
        Assert.True(r.GetProperty("downloadDisabled").GetBoolean());
        Assert.Equal(0, r.GetProperty("downloads").GetInt32());
        Assert.True(r.GetProperty("copyEnabled").GetBoolean());
        Assert.Equal("{\"Plan\": {\"Node Ty", Str(r, "shown"));
    }

    [Fact]
    public void TheSummary_ListsOnlyFiguresThePlanCarries_AndTheQueryIdCaveatOnlyWhenItApplies()
    {
        var r = Run("summary");
        var text = Str(r, "text");
        // The figures are formatted with the machine's locale, so expect what the same call gives here.
        Assert.Contains("Total Cost: " + Str(r, "expectedCost"), text);
        Assert.Contains("Plan Rows: " + Str(r, "expectedRows"), text);
        Assert.Contains("Execution Time", text);
        Assert.DoesNotContain("Actual", text);
        Assert.True(r.GetProperty("caveat").GetBoolean());
        Assert.False(r.GetProperty("plainCaveat").GetBoolean());
        Assert.Equal(0, r.GetProperty("plainSummary").GetInt32());
    }

    [Fact]
    public void ARowWithoutAPlan_ShowsADash()
    {
        var r = Run("noPlan");
        Assert.Equal("\u2014", Str(r, "text"));
        Assert.Equal("\u2014", Str(r, "emptyText"));
        Assert.False(r.GetProperty("hasButton").GetBoolean());
    }

    [Fact]
    public void ARebuiltCell_KeepsItsPanelOpen_ForTheSameServerOnly()
    {
        var r = Run("rebuild");
        Assert.True(r.GetProperty("sameOpen").GetBoolean());
        Assert.False(r.GetProperty("otherOpen").GetBoolean());
        Assert.False(r.GetProperty("otherHashOpen").GetBoolean());
        Assert.Equal(new[] { "srv-a|-8126435036642491494|ABC123" }, r.GetProperty("keys").EnumerateArray().Select(k => k.GetString()).ToArray());
    }

    [Fact]
    public void TheColumn_IsNotSortableNorExported_AndHidesWhenEmpty()
    {
        var r = Run("column");
        Assert.Equal("plan", Str(r, "key"));
        Assert.False(r.GetProperty("csv").GetBoolean());
        Assert.False(r.GetProperty("sortable").GetBoolean());
        Assert.False(r.GetProperty("filter").GetBoolean());
        Assert.False(r.GetProperty("copy").GetBoolean());
        Assert.True(r.GetProperty("hideWhenEmpty").GetBoolean());
        Assert.True(r.GetProperty("rendered").GetBoolean());
    }
}
