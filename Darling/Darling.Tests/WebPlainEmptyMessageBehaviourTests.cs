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
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #7410: a PostgreSQL panel whose read says a setup step is missing shows one plain sentence, and the tool's full text is behind a click.
/// The sentence is built from the reply's structured <c>hints</c>, never from the message. These run the shipped <c>panels.js</c> and
/// <c>util.js</c> under Node (<c>web-plain-empty-harness.mjs</c>); skipped when Node is not installed.
/// </summary>
public sealed class WebPlainEmptyMessageBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-plain-empty-harness.mjs"));
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
                Assert.Fail("the plain empty message harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the plain empty message harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void AMissingExtension_ShowsOnePlainSentence_AndTheFullTextBehindAClick()
    {
        var r = Run();
        var e = r.GetProperty("extension");
        var shown = e.GetProperty("text").GetString()!;
        Assert.StartsWith("pg_buffercache is not installed on this server, so this panel has nothing to show. Install the extension to fill this panel.", shown, StringComparison.Ordinal);
        Assert.DoesNotContain("restart", shown, StringComparison.Ordinal);
        Assert.Equal(1, e.GetProperty("hasDetails").GetInt32());
        Assert.Equal("More detail", e.GetProperty("summary").GetString());
        Assert.Equal(r.GetProperty("message").GetString(), e.GetProperty("detailBody").GetString());
        Assert.Contains("SQLSTATE 42P01", e.GetProperty("detailBody").GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public void AnExtensionThatNeedsAPreload_SaysSo()
    {
        var shown = Run().GetProperty("extensionRestart").GetProperty("text").GetString()!;
        Assert.Contains("pg_qualstats is not installed on this server", shown, StringComparison.Ordinal);
        Assert.Contains("(it needs shared_preload_libraries and a restart)", shown, StringComparison.Ordinal);
    }

    [Fact]
    public void APanelsOwnExtensionSentence_IsUsedWhenTheReplyNamesAMissingExtension()
    {
        var p = Run().GetProperty("panelOwnLine");
        Assert.StartsWith("pg_stat_kcache is not installed on this server, so per-query OS CPU is not collected.", p.GetProperty("text").GetString(), StringComparison.Ordinal);
        Assert.Equal(1, p.GetProperty("hasDetails").GetInt32());
    }

    [Fact]
    public void ASourceTheLoginCannotRead_ShowsOneSentence_AndKeepsTheText()
    {
        var p = Run().GetProperty("sourceUnreadable");
        Assert.StartsWith("The monitoring login could not read the source for this panel on this server.", p.GetProperty("text").GetString(), StringComparison.Ordinal);
        Assert.DoesNotContain("named non-fatal skip", p.GetProperty("text").GetString()!.Split("More detail")[0], StringComparison.Ordinal);
        Assert.Equal(1, p.GetProperty("hasDetails").GetInt32());
    }

    [Fact]
    public void PlanCaptureThatIsNotReady_SaysHowManySettings_WithoutTheFacetDump()
    {
        var p = Run().GetProperty("planCapture");
        var shown = p.GetProperty("text").GetString()!;
        Assert.StartsWith("2 settings that plan capture needs are not ready on this server, so no plans are captured.", shown, StringComparison.Ordinal);
        Assert.Equal(1, p.GetProperty("hasDetails").GetInt32());
        Assert.Contains("capture_threshold = -1 | extension_available = false", p.GetProperty("detailBody").GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public void AReplyWithNoStructuredReason_StillShowsItsText_WithNothingBehindAClick()
    {
        var r = Run();
        foreach (var name in new[] { "noReason", "unknownReason" })
        {
            var p = r.GetProperty(name);
            Assert.Equal(0, p.GetProperty("hasDetails").GetInt32());
            Assert.Equal("strip empty", p.GetProperty("cls").GetString());
        }

        Assert.Equal("No query statistics in this window.", r.GetProperty("noReason").GetProperty("text").GetString());
        Assert.Equal("Plain text.", r.GetProperty("unknownReason").GetProperty("text").GetString());
        Assert.Equal("No deadlocks.", r.GetProperty("emptyWins").GetProperty("text").GetString());
    }

    private const string SamePointer = "Empty for the same reason as the panel that says why.";

    private static string[] Bodies(JsonElement r, string name)
        => System.Linq.Enumerable.ToArray(System.Linq.Enumerable.Select(r.GetProperty(name).EnumerateArray(), x => x.GetString()!));

    [Fact]
    public void PanelsOverOneEmptyRead_SayTheMessageOnce_AndTheRestPointAtIt()
    {
        var r = Run();
        var message = r.GetProperty("fanoutMessage").GetString()!;
        var bodies = Bodies(r, "fanoutThreeEmpty");
        Assert.Equal(message, bodies[0]);
        Assert.Equal(SamePointer, bodies[1]);
        Assert.Equal(SamePointer, bodies[2]);
        // Panels with span sit side by side, so the first can be to the left: the pointer must not claim a direction.
        Assert.DoesNotContain("above", bodies[1], StringComparison.Ordinal);
    }

    [Fact]
    public void AnEmptyWinsPanel_NeitherSaysTheMessage_NorTakesThePointer()
    {
        var r = Run();
        var message = r.GetProperty("fanoutMessage").GetString()!;

        // Its own sentence first: the next plain panel is the first to say the message, the one after points at it.
        var first = Bodies(r, "fanoutEmptyWinsFirst");
        Assert.Equal("Nothing for A.", first[0]);
        Assert.Equal(message, first[1]);
        Assert.Equal(SamePointer, first[2]);

        // Between two plain panels: it keeps its own sentence and the third still points at the first.
        var between = Bodies(r, "fanoutEmptyWinsBetween");
        Assert.Equal(message, between[0]);
        Assert.Equal("Nothing for B.", between[1]);
        Assert.Equal(SamePointer, between[2]);

        // A reply that is not a true negative never takes the panel's sentence, so the emptyWins panel is the one that says it.
        var notEmpty = Bodies(r, "fanoutNotEmptyStatus");
        Assert.Equal(message, notEmpty[0]);
        Assert.Equal(SamePointer, notEmpty[1]);
    }

    [Fact]
    public void APanelWithEmptyLineTrue_SaysItsOwnSentenceForATrueNegative_AndKeepsTheToolTextBehindAClick()
    {
        var r = Run();
        const string line = "No lock activity was sampled in this window.";
        var yes = r.GetProperty("emptyLineTrue");
        Assert.StartsWith(line, yes.GetProperty("text").GetString(), StringComparison.Ordinal);
        Assert.Equal(1, yes.GetProperty("hasDetails").GetInt32());
        Assert.Equal(r.GetProperty("message").GetString(), yes.GetProperty("detailBody").GetString());

        // A reply that is not a true negative (the server is offline, say) is not an "empty" answer, so the tool's text is what shows.
        var no = r.GetProperty("emptyLineUnavailable");
        Assert.Contains("collector is running against example-pg-01", no.GetProperty("text").GetString(), StringComparison.Ordinal);
        Assert.Equal(0, no.GetProperty("hasDetails").GetInt32());
    }

    [Fact]
    public void ThePageCutFlag_IsASentenceUnderTheTiles_NotATile()
    {
        var r = Run();
        var cut = r.GetProperty("cut");
        Assert.Equal(new[] { "Databases active" }, System.Linq.Enumerable.ToArray(System.Linq.Enumerable.Select(cut.GetProperty("labels").EnumerateArray(), x => x.GetString()!)));
        Assert.Equal("The window held more databases than the grid below lists.", Assert.Single(cut.GetProperty("notices").EnumerateArray()).GetString());
        Assert.Empty(r.GetProperty("notCut").GetProperty("notices").EnumerateArray());
    }

    [Fact]
    public void ASmallSizeInMegabytes_IsWrittenInTheUnitThatFitsIt()
    {
        var fit = Run().GetProperty("fit");
        Assert.Equal("143.4 KB", fit.GetProperty("small").GetString());
        Assert.Equal("2.0 MB", fit.GetProperty("whole").GetString());
        Assert.Equal("4.0 GB", fit.GetProperty("large").GetString());
        Assert.Equal("0 B", fit.GetProperty("zero").GetString());
        Assert.Equal("—", fit.GetProperty("none").GetString());
    }
}
