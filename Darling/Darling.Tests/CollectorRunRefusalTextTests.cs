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
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #8781: the web viewer's Run collectors request shows the service's sentence when the store cannot queue a run (403 with
/// <c>store_cannot_queue</c>), and keeps the read-only sentence for any other 403. Runs the shipped page script under Node
/// (<c>web-collector-run-refusal-harness.mjs</c>); skipped when Node is not installed.
/// </summary>
public sealed class CollectorRunRefusalTextTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-collector-run-refusal-harness.mjs"));
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
                Assert.Fail("the collector-run harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the collector-run harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            var result = doc.RootElement.Clone();
            Assert.Equal("/api/collectors/run", result.GetProperty("requests")[0].GetProperty("url").GetString());
            Assert.False(result.GetProperty("ok").GetBoolean());
            return result;
        }
    }

    [Fact]
    public void AStoreThatCannotQueue_ShowsTheServiceSentence_NotReadOnly()
    {
        var message = Run("store_cannot_queue").GetProperty("message").GetString()!;
        Assert.Contains("provision-roles.sql", message, StringComparison.Ordinal);
        Assert.Contains("Nothing was queued.", message, StringComparison.Ordinal);
        Assert.DoesNotContain("read-only", message, StringComparison.Ordinal);
    }

    [Fact]
    public void AReadOnlySeat_StillShowsTheReadOnlySentence()
    {
        var message = Run("read_only").GetProperty("message").GetString()!;
        Assert.Equal("This account has read-only access, so it cannot run collectors.", message);
    }
}
