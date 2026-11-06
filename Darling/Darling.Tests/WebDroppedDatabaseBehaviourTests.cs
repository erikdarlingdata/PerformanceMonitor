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
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #7725: a row the service marked <c>database_dropped</c> (work in a database the server no longer lists) reads "name (dropped)" in its
/// Database cell. Runs the shipped <c>panels.js</c> table renderer under Node (<c>web-dropped-database-harness.mjs</c>); skipped when
/// Node is not installed.
/// </summary>
public sealed class WebDroppedDatabaseBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-dropped-database-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped table script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the dropped database harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the dropped database harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void ADatabaseCell_OfAMarkedRow_ReadsDropped_AndOtherRowsAreUnchanged()
    {
        var cells = Run().GetProperty("databaseCells").EnumerateArray().Select(c => c.GetString()).ToArray();
        Assert.Equal(new[] { "Sales", "OldSales (dropped)", "Other" }, cells);
    }
}
