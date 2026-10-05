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
/// The web stored-plan viewer (#4843), from the shipped <c>plan-viewer.js</c> run under Node
/// (<c>web-plan-viewer-harness.mjs</c>). Node is skipped when it is not installed.
/// </summary>
public sealed class PlanViewerBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-plan-viewer-harness.mjs"));
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
                Assert.Fail("the plan viewer harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the plan viewer harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static string Str(JsonElement r, string name) => r.GetProperty(name).GetString()!;

    [Fact]
    public void ThePlanButton_ReadsTheStoredPlan_AndShowsItIndented()
    {
        var r = Run("open");
        Assert.True(r.GetProperty("buttonBefore").GetBoolean());
        Assert.Equal("/api/read/get_plan_xml", Str(r, "path"));
        var q = r.GetProperty("query");
        Assert.Equal("srv-a", q.GetProperty("server").GetString());
        Assert.Equal("0xABCDEF0123456789", q.GetProperty("query_hash").GetString());
        Assert.Equal("Orders", q.GetProperty("database_name").GetString());
        Assert.Contains("Loading", Str(r, "loading"));
        Assert.Equal("pre", Str(r, "preTag"));
        Assert.StartsWith("<ShowPlanXML", Str(r, "pre"));
        Assert.Contains("\n  <BatchSequence>\n    <Batch>", Str(r, "pre"));
        Assert.True(r.GetProperty("hasCopy").GetBoolean());
        Assert.True(r.GetProperty("hasDownload").GetBoolean());
        Assert.Equal("\u2014", Str(r, "noHashCell"));
        Assert.True(r.GetProperty("closed").GetBoolean());
    }

    [Fact]
    public void ThePrettyPrint_IsText_NotMarkup()
    {
        var r = Run("textOnly");
        Assert.Equal(0, r.GetProperty("preChildren").GetInt32());
        Assert.Equal(0, r.GetProperty("scriptElements").GetInt32());
        // The entity-escaped script text in the statement attribute stays escaped text, character for character.
        Assert.Contains("&lt;script&gt;", Str(r, "text"));
        Assert.True(r.GetProperty("lines").GetInt32() > 5);
        Assert.Equal("<a>\n  <b x=\"1>2\">t</b>\n  <c/>\n  <d>\n    <e/>\n  </d>\n</a>", Str(r, "pretty"));
        Assert.Equal("not xml <at all", Str(r, "notXml"));
    }

    [Fact]
    public void Download_IsAnXmlBlob_NamedForTheQueryHash_HoldingTheStoredXml()
    {
        var r = Run("download");
        Assert.Equal("0xABCDEF0123456789.sqlplan", Str(r, "name"));
        Assert.Equal("application/xml", Str(r, "type"));
        Assert.True(r.GetProperty("bodyIsStored").GetBoolean());
        Assert.Equal("a_b_c.sqlplan", Str(r, "safeName"));
    }

    [Fact]
    public void Copy_PutsTheShownTextOnTheClipboard()
    {
        var r = Run("copy");
        Assert.True(r.GetProperty("matchesShown").GetBoolean());
        Assert.Contains("\n  <BatchSequence>", Str(r, "copied"));
    }

    [Fact]
    public void ATruncatedPlan_SaysSo_AndTurnsDownloadOff()
    {
        var r = Run("truncated");
        Assert.True(r.GetProperty("disabled").GetBoolean());
        Assert.Equal(0, r.GetProperty("downloads").GetInt32());
        Assert.Contains("500 KB", Str(r, "notice"));
        Assert.Contains("Download is turned off", Str(r, "notice"));
        Assert.False(r.GetProperty("shown").GetBoolean());
        Assert.True(r.GetProperty("copyOn").GetBoolean());
    }

    [Fact]
    public void AQueryWithNoStoredPlan_SaysSo_WithNoDownload()
    {
        var r = Run("noPlan");
        Assert.Contains("No stored plan found", Str(r, "text"));
        Assert.False(r.GetProperty("hasPre").GetBoolean());
        Assert.False(r.GetProperty("hasDownload").GetBoolean());
    }

    [Fact]
    public void AnOpenPanel_SurvivesTheRebuild_WithoutAnotherRead()
    {
        var r = Run("rebuild");
        Assert.True(r.GetProperty("open").GetBoolean());
        Assert.True(r.GetProperty("showsPlan").GetBoolean());
        Assert.Equal(0, r.GetProperty("refetched").GetInt32());
        Assert.False(r.GetProperty("otherServerOpen").GetBoolean());
        Assert.True(r.GetProperty("loadingInNew").GetBoolean());
        Assert.True(r.GetProperty("filledNew").GetBoolean());
    }
}
