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
/// #7552: small display details on the web, drawn by the shipped grids under Node (<c>web-display-details-harness.mjs</c>): a Configuration Audit row
/// that says <c>review</c> has the warning colour on both engines, a count of identities left is aligned like the numbers beside it, and a process id,
/// query id or plan id prints with no thousands separator so it can be copied into a command. Skipped when Node is not installed.
/// </summary>
public sealed class WebDisplayDetailsBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-display-details-harness.mjs"));
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
                Assert.Fail("the display details harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the display details harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    private static string Cls(string cell) => Run().GetProperty(cell).GetProperty("cls").GetString()!;

    private static string Text(string cell) => Run().GetProperty(cell).GetProperty("text").GetString()!;

    [Theory]
    [InlineData("sqlReview")]
    [InlineData("pgReview")]
    public void AReviewRow_HasTheWarningColour_OnBothEngines(string cell) => Assert.Contains("sev-Warning", Cls(cell), System.StringComparison.Ordinal);

    [Fact]
    public void AnOkRow_StillHasTheHealthyColour() => Assert.Contains("sev-Healthy", Cls("sqlOk"), System.StringComparison.Ordinal);

    [Theory]
    [InlineData("identityRemaining")]
    [InlineData("bloatSize")]
    [InlineData("bloatReclaimable")]
    public void ANumberColumn_IsAlignedLikeTheNumbersBesideIt(string cell) => Assert.Contains("num", Cls(cell).Split(' '));

    [Theory]
    [InlineData("pid", "5828")]
    [InlineData("queryId", "5568")]
    [InlineData("planId", "7712")]
    public void AnId_PrintsWithNoThousandsSeparator_AndStaysAlignedWithTheNumbers(string cell, string expected)
    {
        Assert.Equal(expected, Text(cell));
        Assert.Contains("num", Cls(cell).Split(' '));
    }

    private static string Str(string name) => Run().GetProperty(name).GetString()!;

    [Fact]
    public void ARoundTickStep_PrintsNoDecimals_AndASmallStepPrintsExactlyWhatItNeeds()
    {
        Assert.Equal("0 | 100 | 200 | 300 | 400", Str("ticksRound"));
        Assert.Equal("0.000 | 0.005 | 0.010 | 0.015", Str("ticksSmall"));
        // A tooltip has no step and keeps its two places, so only the axis changed.
        Assert.Equal("1,234.50", Str("tooltip"));
    }

    [Fact]
    public void ACustomViewAxisInASecondsUnit_PrintsNoDecimalsForAWholeStep()
    {
        Assert.Equal("0 s | 200 s | 400 s | 800 s", Str("composedTicks"));
        Assert.Equal("800.0 s", Str("composedTooltip"));
    }

    [Theory]
    [InlineData("identityBandCritical", "Critical")]
    [InlineData("identityBandOk", "OK")]
    public void AnIdentityCapacityBand_IsCapitalised(string cell, string expected) => Assert.Equal(expected, Text(cell));

    [Fact]
    public void ALinePanelWithNoFormat_PrintsItsWholeNumberAxisWithTheThousandsSeparator() =>
        Assert.Equal("0 | 20,000 | 40,000 | 60,000", Str("axisTicks"));

    [Fact]
    public void ATopWaitName_MayBreakAfterEachUnderscore_AndStillReadsAsTheOneName()
    {
        var tile = Run().GetProperty("topWait");
        Assert.Equal("SOS_SCHEDULER_YIELD", tile.GetProperty("text").GetString());
        Assert.Equal(2, tile.GetProperty("breaks").GetInt32());
    }

    [Theory]
    [InlineData("server")]
    [InlineData("finops")]
    [InlineData("template")]
    public void TheMemoryStateTile_ShowsTheServersNote_WhereTheStateIsNull(string where)
    {
        var tile = Run().GetProperty("memoryState").GetProperty(where);
        Assert.Equal(1, tile.GetProperty("count").GetInt32());
        Assert.Equal("n/a (Azure SQL Database)", tile.GetProperty("noted").GetString());
        // With no note either, the dash stays; with a state, the state wins over the note.
        var bare = tile.GetProperty("bare").GetString()!;
        Assert.NotEmpty(bare);
        Assert.DoesNotContain("n/a", bare, System.StringComparison.Ordinal);
        Assert.Equal("Available physical memory is high", tile.GetProperty("valued").GetString());
    }

    [Fact]
    public void AnHourOfMemoryPressure_IsStampedAtItsMiddle() => Assert.Equal("2026-11-05T17:30:00", Str("pressureStamp"));

    [Fact]
    public void TheAutomaticTuningNote_NamesPlanCorrections() => Assert.Equal("Empty for the same reason as Plan Corrections.", Str("autoTuning"));

    [Fact]
    public void TopQueriesWithSnapshotsAndNoMeasuredChange_SaysTheShortSentence_OnTheQueriesTabToo()
    {
        var text = Str("topQueriesEmpty");
        Assert.Contains("No query in this range changed enough between snapshots to attribute CPU to it. A range of the last 111 hours or more reaches one that did.", text, System.StringComparison.Ordinal);
    }
}
