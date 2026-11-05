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
    public void TheQueryHeatmap_StoppedCollectorReply_IsShownAsTheToolSaidIt_NotAsAnIdleServer()
    {
        var shown = Run().GetProperty("heatmapStopped").GetString()!;
        Assert.Contains("nothing collected IN the last 6 hours", shown, StringComparison.Ordinal);
        Assert.Contains("a collector that stopped looks exactly like this", shown, StringComparison.Ordinal);
        Assert.DoesNotContain("idle", shown, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("More detail", shown, StringComparison.Ordinal);
    }

    [Fact]
    public void SnapshotsWithoutAMeasuredDelta_AreAPlainSentenceBuiltFromTheHints_AndTheToolTextIsBehindAClick()
    {
        var r = Run();
        var six = r.GetProperty("noDelta");
        Assert.StartsWith("No query in this range changed enough between snapshots to attribute CPU to it. A range of the last 6 hours or more reaches one that did.", six.GetProperty("text").GetString(), StringComparison.Ordinal);
        Assert.Equal(r.GetProperty("deltaMessage").GetString(), six.GetProperty("detailBody").GetString());
        Assert.Contains("the last 1 hour or more", r.GetProperty("noDeltaOneHour").GetProperty("text").GetString(), StringComparison.Ordinal);
        Assert.Contains("A longer range may.", r.GetProperty("noDeltaNoHours").GetProperty("text").GetString(), StringComparison.Ordinal);
        // Some snapshot did measure a delta, so this is not the sentence for that case: the tool's own text stays.
        var measured = r.GetProperty("deltaMeasured");
        Assert.Equal(0, measured.GetProperty("hasDetails").GetInt32());
        Assert.DoesNotContain("attribute CPU", measured.GetProperty("text").GetString(), StringComparison.Ordinal);
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
    public void TheWaitAndDurationTrends_SayOnePlainSentenceForATrueNegative_AndKeepTheToolTextOtherwise()
    {
        var r = Run();
        var message = r.GetProperty("trendMessage").GetString()!;

        var wait = Bodies(r, "waitTrendEmpty");
        Assert.StartsWith("No samples for this wait event in this window.", wait[0], StringComparison.Ordinal);
        Assert.Contains(message, wait[0], StringComparison.Ordinal);

        var duration = Bodies(r, "durationTrendEmpty");
        Assert.StartsWith("No statement execution time to chart in this window.", duration[0], StringComparison.Ordinal);
        Assert.Contains(message, duration[0], StringComparison.Ordinal);

        // An answer that is not a true negative has no plain sentence: the tool's text is shown as it is.
        Assert.Equal(message, Bodies(r, "waitTrendUnavailable")[0]);
        Assert.Equal(message, Bodies(r, "durationTrendUnavailable")[0]);
    }

    [Fact]
    public void TheQueryStoreMemoryClerk_SaysItsNoteOnce_AndTheOverheadNoteNamesNoCodeConstant()
    {
        var r = Run();
        // The clerk never appeared: the note says why, and the panel adds no second empty sentence beneath it.
        var clerk = r.GetProperty("qsClerk");
        Assert.Equal(0, clerk.GetProperty("empties").GetInt32());
        Assert.StartsWith("The clerk never appeared in the window.", clerk.GetProperty("text").GetString(), StringComparison.Ordinal);
        Assert.DoesNotContain("No memory clerk figures", clerk.GetProperty("text").GetString(), StringComparison.Ordinal);

        // The overhead note keeps its short line, and the full text behind "More detail" no longer names the collector's constant.
        var overhead = r.GetProperty("qsOverview").GetProperty("text").GetString()!;
        Assert.Contains("More detail", overhead, StringComparison.Ordinal);
        Assert.DoesNotContain("IgnoredWaitDefaults", overhead, StringComparison.Ordinal);
        Assert.DoesNotContain("SKUs", overhead, StringComparison.Ordinal);
    }

    [Fact]
    public void TheQueryHeatmap_TrueNegative_SaysOnePlainSentence_AndTheToolTextIsBehindAClick()
    {
        var r = Run();
        var heatmap = r.GetProperty("heatmapEmpty");
        var text = heatmap.GetProperty("text").GetString()!;
        Assert.StartsWith("No query ran in this window.", text, StringComparison.Ordinal);
        Assert.Equal(1, heatmap.GetProperty("details").GetInt32());
        Assert.DoesNotContain("database_name", text, StringComparison.Ordinal);
        Assert.DoesNotContain("SECOND", text, StringComparison.Ordinal);
    }

    [Fact]
    public void AnEmptyAlertList_ThatHidDismissedAlerts_SaysSoInWords_AndAnyOtherEmptyReplyKeepsItsText()
    {
        var r = Run();
        var many = r.GetProperty("alertsDismissed");
        Assert.StartsWith("3 alerts in this range were dismissed, so they are hidden. Tick Show dismissed to see them.", many.GetProperty("text").GetString(), StringComparison.Ordinal);
        Assert.Equal(r.GetProperty("alertsToolSaid").GetString(), many.GetProperty("detailBody").GetString());
        Assert.StartsWith("1 alert in this range was dismissed, so it is hidden.", r.GetProperty("alertsDismissedOne").GetProperty("text").GetString(), StringComparison.Ordinal);
        var quiet = r.GetProperty("alertsQuiet");
        Assert.Equal("No alerts found in the specified time range.", quiet.GetProperty("text").GetString());
        Assert.Equal(0, quiet.GetProperty("hasDetails").GetInt32());
    }

    [Fact]
    public void TheAdminSettingsTab_WithNoSavedSettings_SaysSoInWords_AndAnyOtherReplyIsLeftToTheCaller()
    {
        var r = Run();
        var shown = r.GetProperty("settingsUnavailable");
        Assert.StartsWith("No alert settings have been saved yet, so the service is running on its defaults.", shown.GetProperty("text").GetString(), StringComparison.Ordinal);
        Assert.Equal(1, shown.GetProperty("hasDetails").GetInt32());
        Assert.Equal(r.GetProperty("settingsToolSaid").GetString(), shown.GetProperty("detailBody").GetString());
        Assert.True(r.GetProperty("settingsOtherReply").GetBoolean());
    }

    [Fact]
    public void ASmallSizeInMegabytes_IsWrittenInTheUnitThatFitsIt()
    {
        var fit = Run().GetProperty("fit");
        // #7564: 0.14 MB stays in MB. Turned into KB it printed 143.4 KB, digits the rounded input never held.
        Assert.Equal("0.14 MB", fit.GetProperty("small").GetString());
        Assert.Equal("2.0 MB", fit.GetProperty("whole").GetString());
        Assert.Equal("4.0 GB", fit.GetProperty("large").GetString());
        Assert.Equal("0 MB", fit.GetProperty("zero").GetString());
        Assert.Equal("—", fit.GetProperty("none").GetString());
    }
}
