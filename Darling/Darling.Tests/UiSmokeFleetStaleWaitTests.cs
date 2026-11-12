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
/// #8069: the web UI smoke's screenshot stage waits for the seed fleet to read collection stale before it shoots. One server in the
/// smoke config is seeded as unreachable on purpose (#7590), so its card reads Offline from the first poll and must count neither
/// as a card to wait for nor as the fleet having aged past the band. The decision (<c>tools/ui-smoke/fleet-stale.mjs</c>) runs under
/// Node over fixture card lists (<c>ui-smoke-fleet-stale-harness.mjs</c>), with the expected-Offline set read from
/// <c>tools/ui-smoke/pages.json</c>; Node is skipped when it is not installed.
/// </summary>
public sealed class UiSmokeFleetStaleWaitTests
{
    private static readonly Lazy<JsonElement> Judged = new(Run);

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "ui-smoke-fleet-stale-harness.mjs"));
        psi.ArgumentList.Add(PathTo("tools", "ui-smoke"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the smoke's stale-wait decision cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the fleet stale-wait harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the fleet stale-wait harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static JsonElement Case(string name) => Judged.Value.GetProperty("cases").GetProperty(name);

    private static string Verdict(string name) => Case(name).GetProperty("verdict").GetString()!;

    [Fact]
    public void TheSmokeConfigNamesTheServerSeededAsUnreachable()
    {
        var expected = Judged.Value.GetProperty("expectedOffline").EnumerateArray().Select(e => e.GetString()).ToArray();
        Assert.Equal(new[] { "ui-down-sql" }, expected);
    }

    [Fact]
    public void EveryOtherCardStaleAndTheSeededDownCardOffline_IsReady()
    {
        Assert.Equal("ready", Verdict("seededDownOnly"));
    }

    [Fact]
    public void TheSeededDownCardOfflineWhileTheOthersAreStillFresh_Waits()
    {
        Assert.Equal("wait", Verdict("stillFresh"));
    }

    [Fact]
    public void ACardTheSeedDidNotTakeDownReadingOffline_IsAProblemThatNamesIt()
    {
        var judged = Case("otherOffline");
        Assert.Equal("problem", judged.GetProperty("verdict").GetString());
        var problem = judged.GetProperty("problem").GetString()!;
        Assert.StartsWith("screenshots:", problem, StringComparison.Ordinal);
        Assert.Contains("not seeded", problem, StringComparison.Ordinal);
        Assert.DoesNotContain("#7569", problem, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSeededDownCardReadingOnline_IsAProblemThatNamesTheSeed()
    {
        var judged = Case("downNotOffline");
        Assert.Equal("problem", judged.GetProperty("verdict").GetString());
        Assert.Contains("ui-down-sql", judged.GetProperty("problem").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheSeededDownCardMissingFromTheFleet_Waits()
    {
        Assert.Equal("wait", Verdict("downMissing"));
    }

    [Fact]
    public void WithNoServerSeededDown_EveryCardStaleIsReady_AndAnyOfflineCardIsAProblem()
    {
        Assert.Equal("ready", Verdict("noneExpectedAllStale"));
        Assert.Equal("problem", Verdict("noneExpectedOneOffline"));
    }
}
