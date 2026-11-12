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
/// #7959: a read that answers 401 (an expired session, or a front door answering a sign-in page) makes the shell put its sign-in
/// strip in <c>main</c>. A page whose render was waiting on that read must stop there, not paint its empty state over the strip or
/// into the box it was filling. The shipped <c>ag.js</c>, <c>known-patterns.js</c> and <c>analysis-findings.js</c> run under Node
/// (<c>web-auth-keeps-strip-harness.mjs</c>) with the shell's strip stood in for, skipped when Node is not installed.
/// </summary>
public sealed class WebAuthKeepsSignInStripTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-auth-keeps-strip-harness.mjs"));
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
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the auth-keeps-strip harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the auth-keeps-strip harness failed for " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    /// <summary>The Availability Groups page writes into <c>main</c> itself. With both reads answering 401, and with the groups read
    /// answering data while the fleet read answers 401, the sign-in strip is all that <c>main</c> holds afterwards: no "no groups" line
    /// and no cards.</summary>
    [Theory]
    [InlineData("ag")]
    [InlineData("agFleet")]
    public void AvailabilityGroups_LeavesTheSignInStrip_WhenARead_AnswersAuth(string scenario)
    {
        var r = Run(scenario);
        Assert.Equal(1, r.GetProperty("expiries").GetInt32());
        Assert.True(r.GetProperty("stripKept").GetBoolean(), "main holds: " + r.GetProperty("mainText").GetString());
        Assert.False(r.GetProperty("emptyTextDrawn").GetBoolean());
        Assert.False(r.GetProperty("groupsDrawn").GetBoolean());
    }

    /// <summary>Known Patterns fills a list box inside <c>main</c>. On a 401 from its read it draws nothing: not "No known patterns
    /// are recorded." into the box the strip has detached.</summary>
    [Fact]
    public void KnownPatterns_DrawsNoEmptyList_WhenItsRead_AnswersAuth()
    {
        var r = Run("knownPatterns");
        Assert.Equal(1, r.GetProperty("expiries").GetInt32());
        Assert.True(r.GetProperty("stripKept").GetBoolean(), "main holds: " + r.GetProperty("mainText").GetString());
        Assert.False(r.GetProperty("emptyTextDrawn").GetBoolean());
    }

    /// <summary>The Recommendations tab fills a panel body. On a 401 from its read the body is left as it was, with its loading strip,
    /// rather than emptied with no text.</summary>
    [Fact]
    public void Recommendations_LeavesItsBody_WhenItsRead_AnswersAuth()
    {
        var r = Run("recommendations");
        Assert.Equal(1, r.GetProperty("expiries").GetInt32());
        Assert.False(r.GetProperty("bodyEmptied").GetBoolean());
        Assert.True(r.GetProperty("bodyLoading").GetBoolean(), "body holds: " + r.GetProperty("bodyText").GetString());
        Assert.False(r.GetProperty("emptyTextDrawn").GetBoolean());
    }
}
