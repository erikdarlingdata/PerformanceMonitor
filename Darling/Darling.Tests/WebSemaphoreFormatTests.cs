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
/// #7676: the Memory tab's Resource Semaphore panels show which semaphore a row is: id 0 reads "Regular", id 1 reads "Small query"
/// (the one behind RESOURCE_SEMAPHORE_SMALL_QUERY waits), not the bare id. This runs the shipped <c>util.js</c> under Node and checks
/// what the "semaphore" format shows. Node is skipped when it is not installed.
/// </summary>
public sealed class WebSemaphoreFormatTests
{
    [Fact]
    public async System.Threading.Tasks.Task TheSemaphoreFormat_NamesTheSemaphore_NotItsId()
    {
        var js = PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js");
        var harness = PathTo("Darling", "Darling.Tests", "web-semaphore-format-harness.mjs");

        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(harness);
        psi.ArgumentList.Add(js);
        Process? proc = null;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped formatter cannot be run.");
        }

        using (proc!)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the semaphore-format harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the semaphore-format harness failed: " + await error);
            using var doc = JsonDocument.Parse(output);
            var r = doc.RootElement;
            Assert.True(r.GetProperty("registered").GetBoolean());
            Assert.Equal("Regular", r.GetProperty("regular").GetString());
            Assert.Equal("Small query", r.GetProperty("smallQuery").GetString());
            Assert.Equal("Small query", r.GetProperty("fromText").GetString());
            Assert.Equal("—", r.GetProperty("nullCell").GetString());
            Assert.Equal("7", r.GetProperty("unknownId").GetString());
        }
    }
}
