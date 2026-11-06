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
/// The "When" column of the Run Now result table (#7930): the shipped <c>pages/collector-run.js</c>, run under Node
/// (<c>web-collector-run-when-harness.mjs</c>) with the process zone pinned off UTC, through a scripted run whose
/// <c>get_collection_log</c> answer carries the bare stamp the service writes (a <c>timestamp</c> column's
/// <c>ToString("o")</c>, no zone). Skipped when Node is not installed.
/// </summary>
public sealed class CollectorRunWhenColumnTests
{
    private static bool TryRun(string zone, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-collector-run-when-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.Environment["TZ"] = zone;

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return false;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the collector run When harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the collector run When harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    [Fact]
    public void TheWhenCell_ReadsTheBareCollectionTimeAsUtc_InAZoneAheadOfUtc()
    {
        if (!TryRun("Asia/Kolkata", out var r)) return;

        /* The pin took: Node is 5 h 30 min ahead of UTC, so 12:00Z is 17:30 on the wall. */
        Assert.Equal(-330, r.GetProperty("offsetMinutes").GetInt32());
        Assert.Equal(17, r.GetProperty("localHour").GetInt32());

        Assert.Equal("When", r.GetProperty("headers")[1].GetString());
        Assert.Equal("wait_stats", r.GetProperty("collector").GetString());

        /* The cell is localTime of the same stamp, like every other time cell, and not a local-time parse of it. */
        var when = r.GetProperty("when").GetString();
        Assert.Equal(r.GetProperty("expected").GetString(), when);
        Assert.NotEqual(r.GetProperty("bare").GetString(), when);
    }
}
