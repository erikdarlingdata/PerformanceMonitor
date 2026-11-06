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
using System.Threading;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #7864: opening a web dashboard page never writes, exports or confirms anything by itself. Since #7847 a page can be reached
/// signed in by a redirect from another site with no click, so a route that wrote on load would act for whoever followed the link.
/// The Node harness draws every route the router has (<c>ROUTE_RENDERERS</c> in app.js, through the real route()), plus every
/// server, FinOps and Admin tab, with fetch stubbed, and fails on any POST, PUT, PATCH or DELETE that is not one of the three
/// reads that travel as a POST (<c>/api/compose/run</c>, <c>/api/alerts/validate</c>, <c>/api/alerts/test</c>). It also fails when
/// the router has a renderer its sample addresses do not reach, so a new route cannot go unchecked. The test is skipped when Node is
/// not installed.
/// </summary>
public sealed class WebRouteLoadWritesTests
{
    private static readonly string[] JsRoot = { "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js" };

    [Fact]
    public async System.Threading.Tasks.Task NoRouteSendsAWriteWhileItLoads()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "WebHarness", "route-load-writes.mjs"));
        psi.ArgumentList.Add(PathTo(JsRoot));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the web routes cannot be drawn.");
            throw;
        }

        using (proc)
        {
            var errorTask = proc.StandardError.ReadToEndAsync();
            var outputTask = proc.StandardOutput.ReadToEndAsync();
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(90));
            try
            {
                await proc.WaitForExitAsync(timeout.Token);
            }
            catch (OperationCanceledException) when (!TestContext.Current.CancellationToken.IsCancellationRequested)
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the route load script did not finish in 90 s");
            }

            var output = await outputTask;
            Assert.True(proc.ExitCode == 0, "a route wrote while it loaded, has no sample address, or sent nothing: " + await errorTask);
            Assert.Contains("none wrote", output);
        }
    }
}
