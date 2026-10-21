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
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #6292: the Block chain action on a Blocking row shows the lead blocker at the root and the sessions it blocks under it. The shipped
/// <c>block-chain.js</c> runs under Node (<c>block-chain-harness.mjs</c>) against a recording fetch: one keyed
/// <c>get_block_chain</c> read on the first click, every session of the tree drawn under the session that blocks it, every
/// collected value drawn as text, an empty answer shown as its own sentence, and a failed read shown as an error. Node is skipped
/// when it is not installed.
/// </summary>
public sealed class BlockChainWebTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "block-chain-harness.mjs"));
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
                Assert.Fail("the block chain harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the block chain harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Strings(JsonElement array) => array.EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void AClick_ReadsTheChainOnce_AndDrawsEverySessionUnderItsBlocker()
    {
        var r = Run("tree");
        Assert.Equal("Block chain", r.GetProperty("before").GetProperty("label").GetString());
        Assert.Empty(r.GetProperty("before").GetProperty("shape").EnumerateArray());

        var request = Assert.Single(r.GetProperty("reads").EnumerateArray());
        Assert.Equal("srv", request.GetProperty("server").GetString());
        Assert.Equal("2026-01-01T12:00:00.0000000", request.GetProperty("event_time").GetString());
        Assert.Equal("58", request.GetProperty("blocked_spid").GetString());
        Assert.Equal("0", request.GetProperty("blocked_ecid").GetString());

        var shape = r.GetProperty("shape").EnumerateArray().ToArray();
        Assert.Equal(3, shape.Length);
        Assert.Equal("SPID 52 - lead blocker", shape[0].GetProperty("title").GetString());
        Assert.Equal(new[] { "SPID 55" }, Strings(shape[0].GetProperty("children")));
        Assert.Equal("SPID 55", shape[1].GetProperty("title").GetString());
        Assert.Equal(new[] { "SPID 58" }, Strings(shape[1].GetProperty("children")));
        Assert.Equal("SPID 58", shape[2].GetProperty("title").GetString());
        Assert.Empty(shape[2].GetProperty("children").EnumerateArray());
        Assert.Equal("Hide block chain", r.GetProperty("label").GetString());

        /* Hiding takes the tree off the page; showing it again does not read again. */
        Assert.Empty(r.GetProperty("hiddenShape").EnumerateArray());
        Assert.Equal(3, r.GetProperty("shownAgainShape").GetArrayLength());
        Assert.Equal(1, r.GetProperty("readsAfterToggle").GetInt32());
    }

    [Fact]
    public void CollectedText_ReachesThePageAsText_NeverAsMarkup()
    {
        var r = Run("tree");
        Assert.Equal(0, r.GetProperty("injectedElements").GetInt32());
        Assert.True(r.GetProperty("hostileLoginAsText").GetBoolean());
        Assert.Contains("<script>x</script>", r.GetProperty("text").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void AnEmptyAnswer_SaysWhyInOneSentence_AndDrawsNoTree()
    {
        var r = Run("empty");
        Assert.Equal(0, r.GetProperty("nodes").GetInt32());
        Assert.Contains("No blocking chain could be rebuilt for session 58 near this report.", r.GetProperty("text").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void AFailedRead_ShowsAnError_AndDrawsNoTree()
    {
        var r = Run("failed");
        Assert.Equal(0, r.GetProperty("nodes").GetInt32());
        Assert.Contains("read failed", r.GetProperty("text").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void AGridRefresh_KeepsAnOpenChainOpen_WithoutReadingItAgain()
    {
        var r = Run("refresh");

        /* The redrawn cell of the same row comes back open, already drawn. */
        Assert.Equal("Hide block chain", r.GetProperty("afterRedraw").GetProperty("label").GetString());
        Assert.Equal(3, r.GetProperty("afterRedraw").GetProperty("shape").GetArrayLength());
        Assert.Equal(3, r.GetProperty("afterToggle").GetInt32());

        /* A redraw mid-read joins the read in flight. */
        Assert.Equal(3, r.GetProperty("joinedShape").GetInt32());
        Assert.Equal(1, r.GetProperty("readsForInFlight").GetInt32());

        /* Another row is its own, closed cell. Two rows were read, once each, in the whole scenario. */
        Assert.Equal("Block chain", r.GetProperty("otherLabel").GetString());
        Assert.Equal(0, r.GetProperty("otherShape").GetInt32());
        Assert.Equal(2, r.GetProperty("reads").GetInt32());
    }

    [Fact]
    public void ARowWithNoEventTime_HasNoButton()
    {
        var r = Run("nokey");
        Assert.Equal(0, r.GetProperty("buttons").GetInt32());
        Assert.Equal("—", r.GetProperty("cellText").GetString());
    }
}
