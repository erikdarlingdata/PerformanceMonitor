/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #8554: which fleet roll-ups a server page's reads wait for. Behind a front door the shell reads one roll-up per store, and the server page
/// reads the one of its own store (the card says which tab set the server has and gives its id). The shipped app.js runs under Node
/// (<c>web-server-fleet-gate-harness.mjs</c>) against three stores whose roll-ups are held until the scenario releases them. A saved
/// database filter is keyed by the card's server id, so it used to hold every read of every server for the card, a second or more on a big store.
/// The browser now remembers the id each server's card gave, so the filter is known before the card arrives. Where the id is not known the
/// reads wait for the server's own store's roll-up only, never for the other stores'. Skipped when Node is not installed.
/// </summary>
public sealed class WebServerFleetGateTests
{
    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-server-fleet-gate-harness.mjs"));
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
                Assert.Fail("the server fleet gate harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the server fleet gate harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    private static int Reads(JsonElement look) => look.GetProperty("reads").GetInt32();

    private static int Filtered(JsonElement look) => look.GetProperty("filtered").GetInt32();

    [Fact]
    public void WithAllThreeStoresRollUpsStillOut_TheServersReadsAndCatalogAreAlreadySent()
    {
        if (!TryRun("allFleetsHeld", out var r)) return;
        var before = r.GetProperty("beforeFleet");
        Assert.Equal(3, before.GetProperty("fleet").GetInt32());
        Assert.True(Reads(before) > 0, "no server read left while the three roll-ups were out");
        Assert.Equal(1, before.GetProperty("catalog").GetInt32());
    }

    [Fact]
    public void WhenOnlyTheServersOwnStoreHasAnswered_TheOtherStoresSlowRollUpsHoldNoRead()
    {
        if (!TryRun("otherStoresHeld", out var r)) return;
        Assert.True(Reads(r.GetProperty("ownStoreAnswered")) > 0);
    }

    [Fact]
    public void ASavedFilterForThisServer_IsAppliedToTheReadsBeforeAnyRollUpAnswers_WhenTheBrowserRemembersTheServersId()
    {
        if (!TryRun("filterRememberedId", out var r)) return;
        var before = r.GetProperty("beforeFleet");
        Assert.True(Reads(before) > 0, "the reads waited for a roll-up although the server's id was remembered");
        Assert.True(Filtered(before) > 0, "the reads left without the saved database filter");
        Assert.Equal(Filtered(before), Filtered(r.GetProperty("afterFleet")));
    }

    [Fact]
    public void ASavedFilterForAnotherServer_DoesNotHoldThisServersReads_AndLeavesThemUnfiltered()
    {
        if (!TryRun("filterForAnotherServer", out var r)) return;
        var before = r.GetProperty("beforeFleet");
        Assert.True(Reads(before) > 0, "another server's filter held this server's reads for the roll-up");
        Assert.Equal(0, Filtered(before));
        Assert.Equal(0, Filtered(r.GetProperty("afterFleet")));
    }

    [Fact]
    public void ASavedFilterWithNoRememberedId_HoldsTheReadsForTheServersOwnStoreOnly()
    {
        if (!TryRun("filterNoRememberedId", out var r)) return;
        Assert.Equal(0, Reads(r.GetProperty("beforeFleet")));
        Assert.True(Reads(r.GetProperty("ownStoreAnswered")) > 0, "the reads did not start when the server's own store answered, with two other stores still out");
    }

    [Fact]
    public void AServerNoStoreHas_StillGetsItsReadsAndKeepsItsNameInTheHeading()
    {
        if (!TryRun("missingServer", out var r)) return;
        Assert.True(Reads(r.GetProperty("beforeFleet")) > 0);
        Assert.True(r.GetProperty("title").GetBoolean());
    }

    [Fact]
    public void AStoreThatIsDown_KeepsTheReadsAlreadySent_ReadsNothingTwice_AndKeepsTheHeading()
    {
        if (!TryRun("storeDown", out var r)) return;
        var before = r.GetProperty("beforeFleet");
        Assert.True(Reads(before) > 0);
        Assert.Equal(Reads(before), Reads(r.GetProperty("afterFleet")));
        Assert.True(r.GetProperty("title").GetBoolean());
    }
}
