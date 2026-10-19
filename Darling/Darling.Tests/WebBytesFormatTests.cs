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
using System.IO;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5632 part 4: the web Blocking grid's "Head log used" column shows the head blocker's transaction log bytes in human units.
/// This runs the shipped <c>util.js</c> under Node and checks what the "bytes" format shows: a NULL is the dash (never 0 B), a real
/// zero is "0 B". Node is skipped when it is not installed.
/// </summary>
public sealed class WebBytesFormatTests
{
    [Fact]
    public async System.Threading.Tasks.Task TheBytesFormat_UsesHumanUnits_AndShowsNullAsADashNotZero()
    {
        var js = PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js");
        var harness = PathTo("Darling", "Darling.Tests", "web-bytes-format-harness.mjs");

        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(harness);
        psi.ArgumentList.Add(js);
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped formatter cannot be run.");
            return;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the bytes-format harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the bytes-format harness failed: " + await error);
            using var doc = JsonDocument.Parse(output);
            var r = doc.RootElement;
            Assert.True(r.GetProperty("registered").GetBoolean());
            Assert.Equal("—", r.GetProperty("nullCell").GetString());
            Assert.Equal("0 B", r.GetProperty("zero").GetString());
            Assert.Equal("512 B", r.GetProperty("bytes").GetString());
            Assert.Equal("1.5 KB", r.GetProperty("kb").GetString());
            Assert.Equal("5.5 MB", r.GetProperty("mb").GetString());
            Assert.Equal("3.0 GB", r.GetProperty("gb").GetString());
            Assert.Equal("—", r.GetProperty("notANumber").GetString());
        }
    }
}
