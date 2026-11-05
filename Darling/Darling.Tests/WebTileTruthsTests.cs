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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Tiles, grid columns and legends that said something other than what they hold (#7416). The page script runs under Node
/// (<c>web-tile-truths-harness.mjs</c>), skipped when Node is not installed. The desktop apps' copies of the same words are checked directly.
/// </summary>
public sealed class WebTileTruthsTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-tile-truths-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

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
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the tile-truths harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the tile-truths harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    [Fact]
    public void TheProtectionTile_SaysItemsAndDatabases_NotABareCount()
    {
        var chip = Run().GetProperty("protectionChip").GetString()!;
        Assert.Contains("6 items on 3 databases", chip, StringComparison.Ordinal);
    }

    [Fact]
    public void TheIdentityCapacityTile_PrintsEveryDigitOfARemainingCountAbove2Pow53()
    {
        // #7416: the service sends the bigint remaining count as digits because a JSON number rounds above 2^53. Number() on it printed 2,767,011,611,056,432,600.
        var chip = Run().GetProperty("bigIdentityChip").GetString()!;
        var digits = new string(chip.Where(char.IsDigit).ToArray());
        Assert.Contains("2767011611056432742", digits, StringComparison.Ordinal);
        Assert.Contains("left", chip, StringComparison.Ordinal);
    }

    [Fact]
    public void TheIdentityCapacityTile_NamesTheFullestColumn_AndAClickOpensThePanel()
    {
        var r = Run();
        var chip = r.GetProperty("identityChip").GetString()!;
        Assert.Contains("Sales.dbo.Orders.OrderId: 100% used, 0 left", chip, StringComparison.Ordinal);
        Assert.Contains("fullest of 3 columns", chip, StringComparison.Ordinal);
        Assert.True(r.GetProperty("identityClickStopped").GetBoolean(), "the tile keeps its click, so the card around it does not also open the server");
        Assert.Equal("#/server/srv-a/config", r.GetProperty("hashAfterClick").GetString());
        Assert.Contains("[data-title=\"Identity Capacity\"]", Strings(r.GetProperty("scrolledTo")));

        /* A service that sends no column keeps the generic sentence. */
        Assert.Contains("The fullest is warning", r.GetProperty("legacyIdentityChip").GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public void TheServiceObjectiveTile_IsDrawnOnAzureSqlDatabaseOnly()
    {
        var r = Run();
        Assert.DoesNotContain("Service objective", Strings(r.GetProperty("boxedLabels")));
        Assert.Contains("Service objective", Strings(r.GetProperty("azureLabels")));
    }

    [Fact]
    public void HealthChecks_ShowsItsSentenceAsText_NotAsATile()
    {
        var r = Run();
        Assert.Single(Strings(r.GetProperty("healthSentenceClasses")));
        var tiles = Strings(r.GetProperty("healthTiles"));
        Assert.Single(tiles);
        Assert.EndsWith("Captured", tiles[0], StringComparison.Ordinal);
    }

    [Fact]
    public void AlertSetupGaps_DropsTheColumnThatRepeatsTheSentence_AndKeepsItWhenAnAlertIsNamed()
    {
        var r = Run();
        Assert.Equal(["What is wrong"], Strings(r.GetProperty("gapHeadersWithoutAlerts")));
        Assert.Equal(["What is wrong", "Alerts"], Strings(r.GetProperty("gapHeadersWithAlerts")));
    }

    [Fact]
    public void MemoryPressureEvents_ShowsWords_AndFillsTheRow()
    {
        var p = Run().GetProperty("pressure");
        var cells = Strings(p.GetProperty("cells"));
        Assert.Contains("Normal", cells);
        Assert.Contains("Medium", cells);
        Assert.Contains("Severe", cells);
        Assert.DoesNotContain("2", cells);
        Assert.True(p.GetProperty("wide").GetBoolean());
    }

    [Fact]
    public void TheGrantChartLegend_NamesTheBuiltInPools_AndKeepsTheNumberForAnyOther()
    {
        Assert.Equal(["Internal pool: Available MB", "Default pool: Available MB", "Pool 5: Available MB"], Strings(Run().GetProperty("legend")));
        Assert.Equal(["Internal pool", "Default pool", "Pool 5"], new[] { 1, 2, 5 }.Select(ResourceGovernorPools.Name).ToArray());
    }

    [Fact]
    public void PerfmonCounters_PrintWholeNumbersWhole_AndShowTheObjectWhereACounterRepeats()
    {
        var r = Run();
        var repeat = r.GetProperty("perfmonRepeat");
        var plain = r.GetProperty("perfmonPlain");
        var cells = Strings(plain.GetProperty("cells"));
        Assert.Contains("1", cells);
        Assert.Contains("86", cells);
        Assert.DoesNotContain("1.00", cells);
        Assert.DoesNotContain("86.00", cells);
        Assert.DoesNotContain("Object", Strings(plain.GetProperty("headers")));
        Assert.Contains("Object", Strings(repeat.GetProperty("headers")));
        Assert.Contains("SQLServer:Database Replica", Strings(repeat.GetProperty("cells")));
        Assert.Contains("SQLServer:Database Mirroring", Strings(repeat.GetProperty("cells")));
    }

    [Fact]
    public void TheDesktopProtectionTile_SaysItemsAndDatabases()
    {
        var items = new[]
        {
            new DatabaseProtectionOverdue(DatabaseProtectionBands.KindFullBackup, "Sales", "overdue", TimeSpan.FromDays(9)),
            new DatabaseProtectionOverdue(DatabaseProtectionBands.KindCheckDb, "Sales", "never", null),
            new DatabaseProtectionOverdue(DatabaseProtectionBands.KindCheckDb, "Hr", "never", null),
        };
        Assert.Equal("3 items on 2 databases", DatabaseProtectionBands.ChipValue(items));
        Assert.Equal("1 item on 1 database", DatabaseProtectionBands.ChipValue(items.Take(1).ToList()));
        Assert.Null(DatabaseProtectionBands.ChipValue(Array.Empty<DatabaseProtectionOverdue>()));
    }

    [Fact]
    public void TheDesktopAlertGapColumn_NamesAlertsOnly()
    {
        Assert.Equal("", ViewerAgentAlertsText.GapWhich(new AgentAlertGap("missing_severity_alerts", "No alert for severity 21.", [], [21])));
        Assert.Equal("Sev 20, Sev 19", ViewerAgentAlertsText.GapWhich(new AgentAlertGap("alert_disabled", "An alert is disabled, so it cannot fire.", ["Sev 20", "Sev 19"], [])));
    }

    // #7560: PostgreSQL has no memory-grant reading and no blocking count behind the fleet card, so those two tiles
    // say n/a (as CPU and Threads do) instead of an OK and a 0 that read as measured. A SQL Server card keeps both.
    [Fact]
    public void ThePostgresCard_PrintsNotAvailableForMemoryAndBlocking_AndASqlServerCardKeepsItsReadings()
    {
        var engines = Run().GetProperty("engineChips");
        var pg = engines.GetProperty("postgres");
        Assert.Equal("Memoryn/a", pg.GetProperty("Memory").GetString());
        Assert.Equal("Blockingn/a", pg.GetProperty("Blocking").GetString());

        var sql = engines.GetProperty("sqlServer");
        Assert.StartsWith("MemoryOK", sql.GetProperty("Memory").GetString(), StringComparison.Ordinal);
        Assert.StartsWith("Blocking0", sql.GetProperty("Blocking").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheAlertSetupTile_CountsTheGaps_AndAStatusWordHasOneCasing()
    {
        var r = Run();
        var setup = r.GetProperty("alertSetup");
        Assert.Equal("3 gaps", setup.GetProperty("gaps3").GetString());
        Assert.Equal("1 gap", setup.GetProperty("gaps1").GetString());
        Assert.Equal("OK", setup.GetProperty("ok").GetString());

        var chips = r.GetProperty("quietChips").EnumerateObject().Select(p => p.Name).ToArray();
        Assert.Contains("ThreadsOK", chips);
        Assert.Contains("MemoryOK", chips);

        var sweep = r.GetProperty("sweepTiles");
        Assert.Equal("OK", sweep.GetProperty("Verdict").GetString());
        Assert.Equal("Unavailable", sweep.GetProperty("Read").GetString());
    }

    [Fact]
    public void TheRecommendationTiles_AreSentencesAcrossThePanel_NotStatTiles()
    {
        var r = Run().GetProperty("recommendations");
        foreach (var page in new[] { "scheduler", "planCache" })
        {
            var one = r.GetProperty(page);
            Assert.Single(one.GetProperty("sentences").EnumerateArray());
            Assert.DoesNotContain("Recommendation", Strings(one.GetProperty("tileLabels")));
        }
    }

    [Fact]
    public void ThePoolColumn_OfTheMemoryGrantsGrids_NamesThePool()
    {
        var pools = Run().GetProperty("pools");
        foreach (var grid in new[] { "grants", "semaphores", "semaphoreWindow" })
        {
            var cells = Strings(pools.GetProperty(grid));
            Assert.Contains("Internal pool", cells);
            Assert.Contains("Default pool", cells);
            Assert.Contains("Pool 7", cells);
            Assert.DoesNotContain("1", cells);
            Assert.DoesNotContain("2", cells);
        }
    }

    /// <summary>#7578: the Semaphore column of the Memory grids prints "Regular" and "Small query", not the bare 0 and 1.</summary>
    [Fact]
    public void TheSemaphoreColumn_NamesTheSemaphore()
    {
        var words = Run().GetProperty("semaphoreWords");
        var window = Strings(words.GetProperty("window"));
        Assert.Contains("Regular", window);
        Assert.Contains("Small query", window);
        Assert.Contains("5", window);
        var newest = Strings(words.GetProperty("newest"));
        Assert.Contains("Regular", newest);
        Assert.Contains("Small query", newest);
    }

    /// <summary>#7578: the date-time columns of Resource Semaphore Pressure wrap with the narrow flow-cell floor, so First and Last are not pushed past the panel.</summary>
    [Fact]
    public void TheSemaphorePressureGrid_WrapsItsDateTimeColumns()
    {
        var classes = Run().GetProperty("windowCellClasses");
        foreach (var header in new[] { "Peak waiters at", "Peak granted at", "First", "Last" })
        {
            var cls = classes.GetProperty(header).GetString()!.Split(' ');
            Assert.Contains("wrap", cls);
            Assert.Contains("flow-cell", cls);
            Assert.Contains("flow-tight", cls);
        }
        // #7578: every header wraps at its spaces, so twelve columns fit the 948 px panel of a 1280 px window (a header of nowrap text kept the grid at 1389 px).
        var headers = Run().GetProperty("windowHeaderClasses");
        foreach (var header in headers.EnumerateObject())
            Assert.Contains("wrap-head", header.Value.GetString()!.Split(' '));
        // A number column keeps its single line.
        Assert.DoesNotContain("wrap", classes.GetProperty("Snapshots").GetString()!.Split(' '));
    }

    /// <summary>#7578: the File I/O Stats latency columns carry their unit, as the charts on the same tab do.</summary>
    [Fact]
    public void TheFileIoLatencyColumns_NameTheirUnit()
    {
        var headers = Strings(Run().GetProperty("fileIoHeaders"));
        Assert.Contains("Read latency (ms)", headers);
        Assert.Contains("Write latency (ms)", headers);
    }

    /// <summary>#7578: a quiet server's null Runnable % is a real answer (no other request was running), so the tile prints 0.0, not a dash, and says what the figure is.</summary>
    [Fact]
    public void TheRunnableTile_PrintsZeroForANullShare_AndExplainsTheFigure()
    {
        var runnable = Run().GetProperty("runnable");
        Assert.Matches("^0[.,]0$", runnable.GetProperty("nullPercent").GetString()!);
        Assert.NotEqual("—", runnable.GetProperty("value").GetString());
        Assert.Contains("no other request was running", Strings(runnable.GetProperty("tooltip")).Single(), StringComparison.Ordinal);
    }

    [Fact]
    public void ThePlatformColumn_IsDroppedWhenNoRowNamesAPlatform_AndKeptWhenOneDoes()
    {
        var p = Run().GetProperty("platform");
        Assert.DoesNotContain("Platform", Strings(p.GetProperty("boxed")));
        Assert.Contains("Platform", Strings(p.GetProperty("managed")));
        Assert.Contains("platform managed", Strings(p.GetProperty("managedCells")));
    }

    [Fact]
    public void TheBlockingAndDeadlocksTiles_NameTheWindowTheReplyCovered_EvenWithNoEvents()
    {
        var w = Run().GetProperty("windowChips");
        foreach (var tile in new[] { "Blocking", "Deadlocks" })
        {
            var hourQuiet = w.GetProperty("hour").GetProperty("quiet").GetProperty(tile).GetString();
            var dayQuiet = w.GetProperty("day").GetProperty("quiet").GetProperty(tile).GetString();
            Assert.False(string.IsNullOrWhiteSpace(hourQuiet), tile + " with no events carries no detail, so nothing says what window it counts");
            Assert.NotEqual(hourQuiet, dayQuiet);

            // The window is read from the reply, and the figures the tile already showed stay after it.
            foreach (var span in new[] { "hour", "day" })
            {
                var quiet = w.GetProperty(span).GetProperty("quiet").GetProperty(tile).GetString()!;
                var busy = w.GetProperty(span).GetProperty("busy").GetProperty(tile).GetString()!;
                Assert.StartsWith(quiet, busy, StringComparison.Ordinal);
                Assert.True(busy.Length > quiet.Length, tile + " lost its figures when it named the window");
            }

            // A reply that carries no window leaves the tile as it was: no detail on a quiet card.
            Assert.Equal(JsonValueKind.Null, w.GetProperty("noWindow").GetProperty(tile).ValueKind);
        }
    }

    [Fact]
    public void TheUnrateableRowsColumn_IsDroppedWhenEveryRowIsZero_AndKeptWhenOneSampleIsLeftOut()
    {
        var u = Run().GetProperty("unrateable");
        Assert.DoesNotContain("Unrateable Rows", Strings(u.GetProperty("allZero")));
        Assert.Contains("Unrateable Rows", Strings(u.GetProperty("someSamples")));
    }

    [Fact]
    public void TheDesktopAlertSetupWord_IsOkOrTheGapCount()
    {
        AgentAlertGap Gap(string m) => new("missing_severity_alerts", m, [], [21]);
        Assert.Equal("OK", ViewerAgentAlertsText.SetupWord(new AgentAlertInstanceStatus("ok", [])));
        Assert.Equal("1 gap", ViewerAgentAlertsText.SetupWord(new AgentAlertInstanceStatus("gaps", [Gap("a")])));
        Assert.Equal("3 gaps", ViewerAgentAlertsText.SetupWord(new AgentAlertInstanceStatus("gaps", [Gap("a"), Gap("b"), Gap("c")])));
    }
}
