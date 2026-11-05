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
}
