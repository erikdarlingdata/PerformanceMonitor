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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #7706: small details on the FinOps pages. The shipped page scripts run under Node (<c>finops-web-details-7706-harness.mjs</c>); Node is skipped
/// when it is not installed. The service-side pieces (the idle strip's period, the Version Store note) are plain calls.
/// </summary>
public sealed class FinOpsWebDetails7706Tests
{
    private static readonly Lazy<JsonElement> Result = new(Run);

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "finops-web-details-7706-harness.mjs"));
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
                Assert.Fail("the #7706 harness did not finish in 60 s");
            }

            Assert.True(proc.ExitCode == 0, "the #7706 harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void ExpensiveQueries_HeadingHasNoCount_AndTheCaptionKeepsTheRowsThatCameBack()
    {
        var run = Result.Value;
        var headings = run.GetProperty("headings").EnumerateArray().Select(h => h.GetString()).ToArray();
        Assert.Contains("Expensive Queries", headings);
        Assert.DoesNotContain(headings, h => h!.Contains("Top ", StringComparison.Ordinal));
        Assert.Equal("Top 5 by CPU, last 24 hours.", run.GetProperty("queryCaption").GetString());
    }

    [Fact]
    public void AnEmptyRecommendationsTab_SaysNoRecommendationsOnce_AndATabWithRowsKeepsItsCountCaption()
    {
        var run = Result.Value;
        Assert.Equal(1, run.GetProperty("emptyRecommendationsMentions").GetInt32());
        Assert.True(run.GetProperty("recommendationsCaption").GetBoolean());
    }

    [Fact]
    public void VersionStore_PrintsTheServicesNoteForASecondaryCopy_AndNothingWithoutOne()
    {
        var run = Result.Value;
        Assert.True(run.GetProperty("pvsNoted").GetBoolean());
        Assert.False(run.GetProperty("pvsNoNote").GetBoolean());
    }

    [Fact]
    public void StorageGrowth_PrintsSizesAsMbfit_ABlankBaselineAsADashWithTheSentenceAsItsHint_AndTheButtonColumnDoesNotSort()
    {
        var run = Result.Value;
        var cells = run.GetProperty("cells").EnumerateArray().Select(c => c.GetString()).ToArray();
        Assert.Contains("648.0 MB", cells);
        Assert.Contains("256.0 MB", cells);
        Assert.DoesNotContain("No sample from 30 days ago", cells);
        Assert.Equal("—", run.GetProperty("dashHint").GetString());

        var headers = run.GetProperty("headers").EnumerateArray().ToDictionary(h => h[0].GetString()!, h => h[1].GetBoolean());
        Assert.False(headers["Show objects"]);
        Assert.True(headers["Database"]);
        Assert.True(headers["Current"]);
    }

    [Fact]
    public void TheIdleDatabasesStrip_EndsWithAPeriod()
    {
        Assert.EndsWith(".", DarlingFinOpsOptimizationReader.IdleDatabasesEmptyText(false), StringComparison.Ordinal);
    }

    [Fact]
    public void ASecondaryCopyWithNoPvsRow_IsCounted_OnlyWhenItIsInTheChosenDatabases()
    {
        string[] secondary = ["AgDb", "OtherAg"];
        string[] withRows = ["master", "tempdb"];

        Assert.Equal(
            "2 databases have no row here: this server holds a secondary copy of them in an availability group. Check the primary replica for their persistent version store.",
            PvsSecondaryNote.WithoutRow(secondary, withRows, DatabaseFilter.All));
        Assert.StartsWith("1 database has no row here:", PvsSecondaryNote.WithoutRow(secondary, withRows, DatabaseFilter.One("AgDb")), StringComparison.Ordinal);
        // A secondary copy that does have a row is not named, and a filter that leaves the secondary out says nothing.
        Assert.Null(PvsSecondaryNote.WithoutRow(secondary, ["AgDb", "OtherAg"], DatabaseFilter.All));
        Assert.Null(PvsSecondaryNote.WithoutRow(secondary, withRows, DatabaseFilter.One("master")));
        Assert.Null(PvsSecondaryNote.WithoutRow(Array.Empty<string>(), withRows, DatabaseFilter.All));
    }
}
