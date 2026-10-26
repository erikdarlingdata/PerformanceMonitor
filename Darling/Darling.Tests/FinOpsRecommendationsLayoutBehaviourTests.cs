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
/// The FinOps Recommendations tab's Finding and Detail cells carry the <c>rec-text</c> class, which app.css gives a minimum width. At
/// 400 px the Detail text had no minimum, wrapped to a one-word-wide column and made the Licensing row about 280 px tall. With the
/// minimum, the grid's own box scrolls sideways instead. The short columns (Category) stay unclassed. Run from the shipped page script
/// under Node (<c>finops-recommendations-harness.mjs</c>), skipped when Node is not installed.
/// </summary>
public sealed class FinOpsRecommendationsLayoutBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "finops-recommendations-harness.mjs"));
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
                Assert.Fail("the recommendations harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the recommendations harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void TheFindingAndDetailCells_CarryTheMinimumWidthClass_AndTheShortColumnsDoNot()
    {
        var r = Run();

        Assert.Equal(1, r.GetProperty("rows").GetInt32());
        var classes = r.GetProperty("classes");
        Assert.Contains("rec-text", classes.GetProperty("Finding").GetString());
        Assert.Contains("rec-text", classes.GetProperty("Detail").GetString());
        Assert.DoesNotContain("rec-text", classes.GetProperty("Category").GetString() ?? "");
    }
}
