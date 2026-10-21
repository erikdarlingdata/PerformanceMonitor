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
/// The web shell's "Sign in again" link after a session expiry (#6278), from the shipped <c>app.js</c> run under Node
/// (<c>sign-in-again-harness.mjs</c>): a click must reload the document (or go to a same-origin sign-in address the
/// server named), never leave the page on a same-document link that does nothing. Skipped when Node is not installed.
/// </summary>
public sealed class SignInAgainBehaviourTests
{
    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "sign-in-again-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "app.js"));
        psi.ArgumentList.Add(scenario);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped sign-in-again code cannot be run.");
            return false;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the sign-in-again harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the sign-in-again harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    // The server sends "/" with its 401 today: the click reloads the page being read, so its #/ route survives the sign-in.
    [Theory]
    [InlineData("server")]
    [InlineData("missing")]
    [InlineData("nonString")]
    [InlineData("offSite")]
    [InlineData("protocolRelative")]
    [InlineData("backslash")]
    [InlineData("javascript")]
    public void ClickingSignInAgain_ReloadsTheDocument_WhenTheServerGaveNoUsableSignInAddress(string scenario)
    {
        if (!TryRun(scenario, out var r)) return;

        Assert.True(r.GetProperty("prevented").GetBoolean());
        Assert.Equal(1, r.GetProperty("reload").GetInt32());
        Assert.Empty(r.GetProperty("assign").EnumerateArray());
        // The href is the page's own address, so a middle click or a copied link still opens it.
        Assert.Equal("/?x=1#/job-history", r.GetProperty("href").GetString());
    }

    [Fact]
    public void ClickingSignInAgain_GoesToTheSameOriginAddress_TheServerNamed()
    {
        if (!TryRun("sameOriginPath", out var r)) return;

        Assert.True(r.GetProperty("prevented").GetBoolean());
        Assert.Equal(["/auth/login?return=%2F"], r.GetProperty("assign").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Equal(0, r.GetProperty("reload").GetInt32());
        Assert.Equal("/auth/login?return=%2F", r.GetProperty("href").GetString());
    }
}
