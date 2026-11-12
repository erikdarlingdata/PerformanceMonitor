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
/// #8554: what a server's page waits for before its panels start reading. The shipped server page (with the real panel renderer) runs
/// under Node (<c>web-server-first-reads-harness.mjs</c>) against a fetch that holds the fleet roll-up until the scenario releases it.
/// On a first visit the page has no card, and the roll-up of a big store took a second or more, so every panel read waited behind it.
/// The panels now start on the SQL Server tab set while the roll-up is out, and the card's arrival keeps them when it agrees. Where an
/// early start would be wrong (a server this browser has seen as PostgreSQL, a tab only the PostgreSQL set has, a saved database
/// filter that only the card's server id can find) the page waits for the card as before. Skipped when Node is not installed.
/// </summary>
public sealed class WebServerFirstReadsTests
{
    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-server-first-reads-harness.mjs"));
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
                Assert.Fail("the server first-reads harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the server first-reads harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    private static string[] Shown(JsonElement look) => look.GetProperty("shown").EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void AFirstVisit_StartsTheSqlServerPanelsBeforeTheFleetAnswers_AndKeepsThemWhenTheCardAgrees()
    {
        if (!TryRun("sqlServer", out var r)) return;
        var before = r.GetProperty("beforeFleet");
        Assert.Equal(1, before.GetProperty("sql").GetInt32());
        Assert.Equal(["sql-value"], Shown(before));
        Assert.Equal(0, before.GetProperty("bar").GetInt32());

        var after = r.GetProperty("afterFleet");
        Assert.Equal(1, after.GetProperty("sql").GetInt32());
        Assert.Equal(0, after.GetProperty("pg").GetInt32());
        Assert.Equal(1, after.GetProperty("fleet").GetInt32());
        Assert.Equal(["sql-value"], Shown(after));
        Assert.Equal(1, after.GetProperty("bar").GetInt32());
    }

    [Fact]
    public void AFirstVisitToAPostgresServer_ShowsThePostgresPanelsOnceTheCardSaysPostgres()
    {
        if (!TryRun("postgresServer", out var r)) return;
        Assert.Equal(1, r.GetProperty("beforeFleet").GetProperty("sql").GetInt32());

        var after = r.GetProperty("afterFleet");
        Assert.Equal(1, after.GetProperty("pg").GetInt32());
        Assert.Equal(["pg-value"], Shown(after));
        Assert.Equal(1, after.GetProperty("bar").GetInt32());
    }

    [Fact]
    public void ThePostgresServerOnTheNextLoad_ReadsNothingUntilTheCardSaysWhichTabsItHas()
    {
        if (!TryRun("postgresServerNextLoad", out var r)) return;
        var before = r.GetProperty("beforeFleet2");
        Assert.Equal(0, before.GetProperty("sql").GetInt32());
        Assert.Equal(0, before.GetProperty("pg").GetInt32());

        var after = r.GetProperty("afterFleet2");
        Assert.Equal(0, after.GetProperty("sql").GetInt32());
        Assert.Equal(1, after.GetProperty("pg").GetInt32());
        Assert.Equal(["pg-value"], Shown(after));
    }

    [Fact]
    public void ASavedDatabaseFilter_HoldsEveryReadForTheCard_BecauseTheFilterIsKeyedByTheCardsServerId()
    {
        if (!TryRun("savedDatabaseFilter", out var r)) return;
        var before = r.GetProperty("beforeFleet");
        Assert.Equal(0, before.GetProperty("sql").GetInt32());
        Assert.Equal(0, before.GetProperty("pg").GetInt32());

        Assert.Equal(1, r.GetProperty("afterFleet").GetProperty("sql").GetInt32());
    }

    [Fact]
    public void ATabOnlyThePostgresSetHas_IsNotStartedOnTheSqlServerSet()
    {
        if (!TryRun("postgresOnlyTab", out var r)) return;
        var before = r.GetProperty("beforeFleet");
        Assert.Equal(0, before.GetProperty("sql").GetInt32());
        Assert.Equal(0, before.GetProperty("pg").GetInt32());

        var after = r.GetProperty("afterFleet");
        Assert.Equal(0, after.GetProperty("sql").GetInt32());
        Assert.Equal(1, after.GetProperty("pg").GetInt32());
    }

    [Fact]
    public void AFleetReadThatFails_KeepsThePanelsAlreadyStarted_DrawsTheTabBarAndReadsNothingTwice()
    {
        if (!TryRun("fleetFails", out var r)) return;
        var after = r.GetProperty("afterFleet");
        Assert.Equal(1, after.GetProperty("sql").GetInt32());
        Assert.Equal(["sql-value"], Shown(after));
        Assert.Equal(1, after.GetProperty("bar").GetInt32());
    }
}
