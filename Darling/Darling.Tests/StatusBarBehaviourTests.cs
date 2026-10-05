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
/// What the web shell's status bar draws, from the shipped <c>app.js</c> status-bar code run under Node
/// (<c>status-bar-harness.mjs</c>): the seat label from the session probe, the store size from
/// <c>get_store_host</c> read at most once per five minutes, a collection state only when <c>/api/ping</c> is not
/// ok, and a bar that survives any of those reads failing. Node is skipped when it is not installed.
/// </summary>
public sealed class StatusBarBehaviourTests
{
    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "status-bar-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "app.js"));
        psi.ArgumentList.Add(scenario);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped status-bar code cannot be run.");
            return false;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the status-bar harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the status-bar harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    private static string[] Items(JsonElement r) =>
        r.GetProperty("items").EnumerateArray().Where(e => e.ValueKind == JsonValueKind.String).Select(e => e.GetString()!).ToArray();

    [Theory]
    [InlineData("seatWrite", "Seat: read-write")]
    [InlineData("seatRead", "Seat: read-only")]
    [InlineData("seatProbeFailed", "Seat: not connected")]
    public void TheSeatLabel_FollowsTheSessionProbe_InTheViewersWording(string scenario, string expected)
    {
        if (!TryRun(scenario, out var r)) return;
        Assert.Contains(expected, Items(r));
    }

    [Theory]
    [InlineData("storeSize", "Database: 5.0 GB")]
    [InlineData("storeSizeMb", "Database: 300 MB")]
    public void TheStoreSize_ComesFromTheStoreHostRead_InTheViewersUnits(string scenario, string expected)
    {
        if (!TryRun(scenario, out var r)) return;
        Assert.Contains(expected, Items(r));
    }

    [Fact]
    public void TheStoreSize_IsReadOncePerFiveMinutes_NotOnEveryPoll()
    {
        if (!TryRun("cached", out var r)) return;
        Assert.Equal(1, r.GetProperty("reads").GetInt32());
        Assert.Contains("Database: 5.0 GB", Items(r));

        if (!TryRun("refetched", out var later)) return;
        Assert.Equal(2, later.GetProperty("reads").GetInt32());
        Assert.Contains("Database: 6.0 GB", Items(later));
    }

    [Theory]
    [InlineData("ping_starting", "Collection: Starting")]
    [InlineData("ping_degraded", "Collection: Degraded")]
    [InlineData("ping_stopped", "Collection: Stopped")]
    public void ANonOkPingStatus_IsShown(string scenario, string expected)
    {
        if (!TryRun(scenario, out var r)) return;
        Assert.Contains(expected, Items(r));
    }

    [Fact]
    public void AnOkPing_AddsNothing()
    {
        if (!TryRun("ping_ok", out var r)) return;
        Assert.DoesNotContain(Items(r), i => i.StartsWith("Collection", StringComparison.Ordinal));
    }

    [Fact]
    public void AFailedStoreSizeRead_NeverBreaksTheBar()
    {
        if (!TryRun("storeFails", out var r)) return;
        Assert.True(r.GetProperty("hasBar").GetBoolean());
        Assert.DoesNotContain(Items(r), i => i.StartsWith("Database", StringComparison.Ordinal));
        Assert.Contains("Seat: read-write", Items(r));

        if (!TryRun("storeFailsAfterGood", out var stale)) return;
        Assert.Contains("Database: 5.0 GB (stale)", Items(stale));
    }

    [Theory]
    [InlineData("pingFails")]
    [InlineData("sessionFails")]
    public void AFailedPingOrSessionRead_LeavesTheRestOfTheBar(string scenario)
    {
        if (!TryRun(scenario, out var r)) return;
        Assert.Contains("Database: 5.0 GB", Items(r));
        Assert.Contains("1 server", Items(r));
    }
}
