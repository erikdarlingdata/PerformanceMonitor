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
/// The fleet page's Silence / Unsilence control, from the shipped <c>fleet.js</c> run under Node
/// (<c>web-fleet-silence-harness.mjs</c>) against a fake DOM and fetch. Skipped when Node is not installed.
/// </summary>
public sealed class FleetSilenceBehaviourTests
{
    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-fleet-silence-harness.mjs"));
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
                Assert.Fail("the fleet silence harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the fleet silence harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    private static string[] Labels(JsonElement node, string name) =>
        node.GetProperty(name).EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void ASeatThatCannotEdit_SeesNoSilenceButton()
    {
        if (!TryRun("gate", out var r)) return;
        Assert.Equal(0, r.GetProperty("buttons").GetInt32());
    }

    [Fact]
    public void Silence_ConfirmsThenPostsTheServerSilenceRule_AndTheRebuiltCardKeepsItsAnswer()
    {
        if (!TryRun("silence", out var r)) return;

        Assert.Equal(new[] { "Silence" }, Labels(r, "before"));
        Assert.Equal(new[] { "Unsilence" }, Labels(r, "afterClick"));
        Assert.Equal(new[] { "Unsilence" }, Labels(r, "afterRebuild"));
        Assert.Equal(1, r.GetProperty("bell").GetInt32());
        Assert.Single(r.GetProperty("confirms").EnumerateArray());
        var write = Assert.Single(r.GetProperty("writes").EnumerateArray());
        Assert.Equal("POST", write.GetProperty("method").GetString());
        Assert.Equal("/api/mute-rules", write.GetProperty("url").GetString());
        Assert.Equal("{\"server_id\":10,\"server_name\":\"srv-a\",\"reason\":\"Silenced from server list\"}", write.GetProperty("body").GetRawText());
    }

    [Fact]
    public void ADeclinedConfirm_WritesNothing()
    {
        if (!TryRun("declined", out var r)) return;
        Assert.Equal(0, r.GetProperty("writes").GetInt32());
        Assert.Equal(1, r.GetProperty("confirms").GetInt32());
    }

    [Fact]
    public void AWriteInFlightAcrossARebuild_DoesNotFireTwice_AndTheRebuiltButtonIsDisabled()
    {
        if (!TryRun("pending", out var r)) return;
        Assert.True(r.GetProperty("disabled").GetBoolean());
        Assert.Equal(1, r.GetProperty("posts").GetInt32());
        Assert.Equal("Unsilence", r.GetProperty("label").GetString());
    }

    [Fact]
    public void Unsilence_DeletesOnlyTheRuleTheSilenceCreated()
    {
        if (!TryRun("unsilence", out var r)) return;

        Assert.Equal(new[] { "Unsilence" }, Labels(r, "before"));
        var write = Assert.Single(r.GetProperty("writes").EnumerateArray());
        Assert.Equal("DELETE", write.GetProperty("method").GetString());
        Assert.Equal("/api/mute-rules/own-1", write.GetProperty("url").GetString());
        Assert.Equal(new[] { "Silence" }, Labels(r, "after"));
    }

    [Fact]
    public void Unsilence_LeavesAHandBuiltWholeServerRuleAlone_AndSaysSo()
    {
        if (!TryRun("unsilenceHandBuilt", out var r)) return;
        Assert.Equal(0, r.GetProperty("writes").GetInt32());
        Assert.Contains("Mute Rules page", Labels(r, "notice").Single());
    }
}
