/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Runs node --check on FinOps tab scripts, so a syntax error that would blank the tab in the browser fails here.
/// (The other FinOps page text pins were deleted in #6486; this is the one that executes a tool.)
/// </summary>
public sealed class FinOpsTabsParseUnderNodeTests
{
    [Theory]
    [InlineData("index-analysis.js")]
    [InlineData("recommendations.js")]
    [InlineData("server-inventory.js")]
    public async System.Threading.Tasks.Task TheTabParsesUnderNode(string tabFile)
    {
        var source = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "finops", tabFile);
        var path = Path.Combine(Path.GetTempPath(), "finops-tab-" + System.Guid.NewGuid().ToString("N") + ".mjs");
        File.WriteAllText(path, source);
        try
        {
            var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
            psi.ArgumentList.Add("--check");
            psi.ArgumentList.Add(path);
            Process proc;
            try
            {
                proc = Process.Start(psi)!;
            }
            catch (Win32Exception)
            {
                Assert.Skip("Node is not installed, so the tab script cannot be parsed.");
                throw;
            }

            using (proc)
            {
                var error = proc.StandardError.ReadToEndAsync();
                if (!proc.WaitForExit(20000))
                {
                    proc.Kill(entireProcessTree: true);
                    Assert.Fail("node --check did not finish in 20 s");
                }

                Assert.True(proc.ExitCode == 0, "node --check failed on " + tabFile + ": " + await error);
            }
        }
        finally
        {
            File.Delete(path);
        }
    }
}
