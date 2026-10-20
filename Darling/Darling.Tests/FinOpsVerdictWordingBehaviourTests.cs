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
using PerformanceMonitor.Common;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #6139: the web FinOps Utilization and Server Inventory tabs, run from the shipped page scripts under Node
/// (<c>finops-verdict-wording-harness.mjs</c>), show the Viewer's words for a server with too few CPU samples and say why a
/// high Stolen memory % reads as it does. The expected text is built from <see cref="ProvisioningVerdict"/>, the Viewer's own.
/// Node is skipped when it is not installed.
/// </summary>
public sealed class FinOpsVerdictWordingBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "finops-verdict-wording-harness.mjs"));
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
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the verdict-wording harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the verdict-wording harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void TheUtilizationTabShowsNotEnoughDataWithTheSampleCount_NotTheRawToken()
    {
        var run = Run();
        var six = run.GetProperty("lowSamples").GetString()!;
        Assert.Contains(ProvisioningVerdict.NotEnoughDataLabel(6), six);
        Assert.Contains(ProvisioningVerdict.NotEnoughDataExplanation(6), six);
        Assert.DoesNotContain("NOT ENOUGH DATA", six);
        Assert.DoesNotContain("OVER PROVISIONED", six);
        // The trend has no count per day: the short form, the same words.
        Assert.Contains(ProvisioningVerdict.NotEnoughDataShortLabel, six);

        Assert.Contains(ProvisioningVerdict.NotEnoughDataLabel(1), run.GetProperty("oneSample").GetString()!);
    }

    [Fact]
    public void TheUtilizationTabExplainsAHighStolenMemoryFigure_AndStaysQuietOtherwise()
    {
        var run = Run();
        var stolen = run.GetProperty("highStolen").GetString()!;
        Assert.Contains("UNDER PROVISIONED", stolen);
        Assert.Contains("outside the buffer pool", stolen);

        Assert.DoesNotContain("outside the buffer pool", run.GetProperty("plain").GetString()!);
    }

    [Fact]
    public void TheServerInventoryGridShowsNotEnoughDataInTheViewersWords()
    {
        var run = Run();
        Assert.Contains(ProvisioningVerdict.NotEnoughDataShortLabel, run.GetProperty("inventoryLowSamples").GetString()!);
        Assert.DoesNotContain("NOT ENOUGH DATA", run.GetProperty("inventoryLowSamples").GetString()!);
        Assert.Contains("OVER PROVISIONED", run.GetProperty("inventoryOver").GetString()!);
    }
}
