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

/// <summary>#6159: the small web fixes, run as the shipped modules under Node (<c>web-polish-6159-harness.mjs</c>). Skipped when Node is not installed.</summary>
public sealed class WebPolish6159BehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-polish-6159-harness.mjs"));
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
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the web polish harness did not finish in 60 s");
            }

            Assert.True(proc.ExitCode == 0, "the web polish harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void TheAlertBadgeName_IsSingularForOne()
    {
        var run = Run();
        Assert.Equal("1 alert, click to acknowledge", run.GetProperty("badgeOne").GetString());
        Assert.Equal("3 alerts, click to acknowledge", run.GetProperty("badgeMany").GetString());
    }

    [Fact]
    public void AChangeOfRoute_ScrollsToTop_ButAPollOrTheSameRouteDoesNot()
    {
        Assert.Equal(1, Run().GetProperty("scrollJumps").GetInt32());
    }

    [Fact]
    public void TheLevelTile_AddsTheUpdateLevel_WhenThereIsOne()
    {
        var run = Run();
        Assert.Equal("RTM CU20", run.GetProperty("levelWithUpdate")[0].GetString());
        Assert.Equal("RTM", run.GetProperty("levelWithoutUpdate")[0].GetString());
    }

    [Fact]
    public void ASecondaryCardThatCannotSeeThePrimary_SaysSoPlainly()
    {
        var run = Run();
        Assert.Equal("Primary not visible from this secondary", run.GetProperty("primaryNoteHidden").GetString());
        Assert.Equal("No primary reported", run.GetProperty("primaryNoteNone").GetString());
    }
}
