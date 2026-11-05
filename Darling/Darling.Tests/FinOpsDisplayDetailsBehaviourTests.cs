/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Diagnostics;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #7562: the display details on FinOps (the Verdict tile, the Stolen memory tile, the Database Sizes headers and Max size, the High Impact
/// Band, the Host OS text), Fleet Sweeps (RAM and the liveness block in words) and Availability Groups (one spelling of the primary). The
/// shipped page scripts run under Node (<c>finops-display-details-harness.mjs</c>); Node is skipped when it is not installed.
/// </summary>
public sealed class FinOpsDisplayDetailsBehaviourTests
{
    private static readonly Lazy<JsonElement> Result = new(Run);

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "finops-display-details-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page scripts cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the display details harness did not finish in 60 s");
            }

            Assert.True(proc.ExitCode == 0, "the display details harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void TheVerdictTileUsesTheSmallSize_SoUnderProvisionedIsNotBrokenBeforeItsLastLetter()
    {
        var tile = Result.Value.GetProperty("verdictTile");
        Assert.Equal("UNDER PROVISIONED", tile[1].GetString());
        Assert.Contains("small", tile[0].GetString());
    }

    [Fact]
    public void TheStolenMemoryTileIsColoredByTheThresholdsItsSentenceNames()
    {
        var run = Result.Value;
        Assert.DoesNotContain("sev-", run.GetProperty("stolenPlain")[0].GetString());
        Assert.Contains("sev-Warning", run.GetProperty("stolenAmber")[0].GetString());
        Assert.Contains("sev-Warning", run.GetProperty("stolenJustUnderRed")[0].GetString());
        Assert.DoesNotContain("sev-Critical", run.GetProperty("stolenJustUnderRed")[0].GetString());
        Assert.Contains("sev-Critical", run.GetProperty("stolenRed")[0].GetString());
        // Exactly 85 is amber: both desktop bars go red only above 85, so the web tile agrees with them.
        Assert.Contains("sev-Warning", run.GetProperty("stolenOnTheLine")[0].GetString());
        Assert.DoesNotContain("sev-Critical", run.GetProperty("stolenOnTheLine")[0].GetString());
    }

    [Fact]
    public void TheDatabaseSizesFreeColumnIsNamedFree_AndMaxSizeStepsUpToTheUnitThatFits()
    {
        var run = Result.Value;
        var headers = run.GetProperty("dbSizeHeaders").EnumerateArray().Select(e => e.GetString()).ToArray();
        Assert.Contains("Free", headers);
        Assert.DoesNotContain("Free MB", headers);
        var cells = run.GetProperty("maxSizeCells").EnumerateArray().Select(e => e.GetString()).ToArray();
        Assert.Contains("2.0 TB", cells);
        Assert.DoesNotContain("2,048.0 GB", cells);
    }

    [Fact]
    public void TheHighImpactBandWordsTakeTheColorsTheyHaveOnRecommendations()
    {
        var band = Result.Value.GetProperty("band");
        Assert.Contains("sev-Critical", band.GetProperty("high").GetString());
        Assert.Contains("sev-Warning", band.GetProperty("medium").GetString());
        Assert.Contains("sev-Healthy", band.GetProperty("low").GetString());
    }

    [Fact]
    public void TheHostOsFallbackText_IsCutToTheNameBeforeTheVersionAndPlatformTag_AndALinuxNameIsKept()
    {
        var run = Result.Value;
        Assert.Equal("short", run.GetProperty("hostOsFallback").GetString());
        Assert.Equal("named", run.GetProperty("hostOsFallbackText").GetString());
        Assert.True(run.GetProperty("hostOsLinux").GetBoolean());
    }

    [Fact]
    public void FleetSweepsRam_SaysWhereTheFigureCameFromInWords_NotTheNameOfAWindowsCall()
    {
        var ram = Result.Value.GetProperty("ram").GetString()!;
        Assert.DoesNotContain("GlobalMemoryStatusEx", ram);
        Assert.Contains("read from Windows", ram);
    }

    [Fact]
    public void FleetSweepsLiveness_SaysWhatHappenedAfterARestart_InWords()
    {
        var text = Result.Value.GetProperty("livenessReset").GetString()!;
        Assert.Contains("The service restarted, so this sweep cannot tell whether alerts kept running", text);
        Assert.DoesNotContain("Pass-counter", text);
        Assert.DoesNotContain("judgeable", text);
        Assert.DoesNotContain("Alert passes", text);
    }

    [Fact]
    public void AvailabilityGroups_TwoViewsOfOneGroupPrintThePrimaryWithOneSpelling()
    {
        var names = Result.Value.GetProperty("primaries").EnumerateArray().Select(e => e.GetString()).ToArray();
        Assert.Equal(new[] { "SQL01", "SQL01" }, names);
    }
}
