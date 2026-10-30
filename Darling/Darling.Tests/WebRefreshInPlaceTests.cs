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
/// #6933: the auto-refresh keeps what is on screen. The shipped Fleet page and server page (with the real panel renderer) run under
/// Node (<c>web-refresh-in-place-harness.mjs</c>) against a fake DOM and a fetch the scenario holds open. A poll keeps the existing
/// elements and shows no loading strip while its read is outstanding, a failed poll read keeps the old content and shows the error,
/// and the reader's own navigation still draws fresh. Skipped when Node is not installed.
/// </summary>
public sealed class WebRefreshInPlaceTests
{
    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-refresh-in-place-harness.mjs"));
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
            return false;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the refresh-in-place harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the refresh-in-place harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    [Fact]
    public void FleetPoll_KeepsThePageOnScreenWhileItsReadRuns_ThenSwapsTheNewOneIn()
    {
        if (!TryRun("fleetPoll", out var r)) return;
        Assert.True(r.GetProperty("sameHeadWhileReading").GetBoolean());
        Assert.True(r.GetProperty("textUnchangedWhileReading").GetBoolean());
        Assert.Equal(0, r.GetProperty("loadingWhileReading").GetInt32());
        Assert.True(r.GetProperty("swapped").GetBoolean());
        Assert.Equal(0, r.GetProperty("loadingAfter").GetInt32());
        Assert.True(r.GetProperty("hasServerAfter").GetBoolean());
    }

    [Fact]
    public void FleetPoll_WhoseReadFails_KeepsTheOldPageAndShowsTheError_UntilTheNextPollSucceeds()
    {
        if (!TryRun("fleetPollFails", out var r)) return;
        Assert.True(r.GetProperty("sameHead").GetBoolean());
        Assert.True(r.GetProperty("hasServer").GetBoolean());
        Assert.Equal(0, r.GetProperty("loading").GetInt32());
        var error = Assert.Single(r.GetProperty("errors").EnumerateArray().Select(e => e.GetString()!));
        Assert.Contains("fleet down", error);
        Assert.Equal(0, r.GetProperty("errorsAfterRecovery").GetInt32());
    }

    [Fact]
    public void FleetNavigation_StillDrawsFresh_WithTheLoadingStrip()
    {
        if (!TryRun("fleetNavigation", out var r)) return;
        Assert.Equal(1, r.GetProperty("loadingWhileReading").GetInt32());
        Assert.False(r.GetProperty("hasServerWhileReading").GetBoolean());
        Assert.Equal(0, r.GetProperty("loadingAfter").GetInt32());
    }

    [Fact]
    public void ServerPoll_KeepsEachPanelsElementAndContent_ThenFillsItInPlace()
    {
        if (!TryRun("serverPoll", out var r)) return;
        Assert.True(r.GetProperty("sameGrid").GetBoolean());
        Assert.True(r.GetProperty("samePanel").GetBoolean());
        Assert.Equal(0, r.GetProperty("loadingWhileReading").GetInt32());
        Assert.Contains("value-1", r.GetProperty("textWhileReading").GetString());
        Assert.True(r.GetProperty("samePanelAfter").GetBoolean());
        Assert.Equal(1, r.GetProperty("panelCount").GetInt32());
        Assert.Contains("value-2", r.GetProperty("textAfter").GetString());
        Assert.Equal(0, r.GetProperty("loadingAfter").GetInt32());
    }

    [Fact]
    public void ServerPoll_WhosePanelReadFails_KeepsTheOldContentAndShowsTheError_UntilTheNextPollSucceeds()
    {
        if (!TryRun("serverPollFails", out var r)) return;
        Assert.True(r.GetProperty("samePanel").GetBoolean());
        Assert.True(r.GetProperty("stillShowsOld").GetBoolean());
        Assert.Equal(0, r.GetProperty("loading").GetInt32());
        var error = Assert.Single(r.GetProperty("errors").EnumerateArray().Select(e => e.GetString()!));
        Assert.Contains("read down", error);
        Assert.Equal(0, r.GetProperty("errorsAfterRecovery").GetInt32());
        Assert.Contains("value-3", r.GetProperty("textAfterRecovery").GetString());
    }

    [Fact]
    public void ServerPoll_WithAHiddenPanelThatKeepsItsLoadingStrip_StillRefreshesTheOtherPanels()
    {
        if (!TryRun("hiddenPanelPoll", out var r)) return;
        Assert.True(r.GetProperty("sameShown").GetBoolean());
        Assert.True(r.GetProperty("sameHidden").GetBoolean());
        Assert.Contains("new", r.GetProperty("textAfter").GetString());
    }

    [Fact]
    public void ServerNavigation_StillDrawsAFreshGrid_WithLoadingStrips()
    {
        if (!TryRun("serverNavigation", out var r)) return;
        Assert.False(r.GetProperty("samePanel").GetBoolean());
        Assert.Equal(1, r.GetProperty("loadingWhileReading").GetInt32());
        Assert.Equal(0, r.GetProperty("loadingAfter").GetInt32());
    }

    [Fact]
    public void FleetPoll_ThatLandsAfterTheReaderLeft_DrawsNothingOverTheNewPage()
    {
        if (!TryRun("fleetPollAfterNavigation", out var r)) return;
        Assert.True(r.GetProperty("onlyOtherPage").GetBoolean());
        Assert.False(r.GetProperty("hasServer").GetBoolean());
    }

    [Fact]
    public void ServerPoll_WhoseCardFlipsTheServerOffline_RepaintsThePanelsInPlace()
    {
        if (!TryRun("serverPollOfflineFlip", out var r)) return;
        Assert.True(r.GetProperty("samePanelWhileReading").GetBoolean());
        Assert.Equal(0, r.GetProperty("loadingWhileReading").GetInt32());
        Assert.Contains("value-1", r.GetProperty("textWhileReading").GetString());
        Assert.True(r.GetProperty("samePanelAfter").GetBoolean());
        Assert.Contains("value-2", r.GetProperty("textAfter").GetString());
        Assert.Equal(0, r.GetProperty("loadingAfter").GetInt32());
    }

    [Fact]
    public void WholeGridSwap_StartedBeforeTheGridWasRedrawn_DoesNotLandOverTheNewPanels()
    {
        if (!TryRun("staleGridSwap", out var r)) return;
        Assert.True(r.GetProperty("keepsNewScope").GetBoolean());
        Assert.Contains("new-scope", r.GetProperty("text").GetString());
        Assert.True(r.GetProperty("swapsWhenUnchanged").GetBoolean());
    }
}
