/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8184 I3: the health host's listener takes nothing from the ambient ASP.NET configuration. A <c>Kestrel__Endpoints__*</c> variable
/// in the service environment must not add a second, unauthenticated listener (the #5288 pin the sibling hosts carry). The test sets
/// that variable process-wide, which would add the same listener to any other test's host built from default configuration, so the
/// class is in the <c>darling-config-env</c> collection, which runs alone; the variable's previous value is put back in a
/// <c>finally</c>.
/// </summary>
[Collection("darling-config-env")]
public sealed class DarlingHealthHostAmbientConfigTests
{
    private const string ExtraEndpointVariable = "Kestrel__Endpoints__Extra__Url";

    [Fact]
    public async Task KestrelEndpointsInTheEnvironment_AddNoListener()
    {
        var ct = TestContext.Current.CancellationToken;
        var healthPort = DarlingHealthHostTests.FreePort();
        var extraPort = DarlingHealthHostTests.FreePort();
        Assert.NotEqual(healthPort, extraPort);
        var previous = Environment.GetEnvironmentVariable(ExtraEndpointVariable);
        Environment.SetEnvironmentVariable(ExtraEndpointVariable, $"http://127.0.0.1:{extraPort}");
        try
        {
            var logger = new DarlingHealthHostTests.CapturingLogger();
            await using var host = new DarlingHealthHostService(
                new CollectorRuntimeState(), new DarlingHealthHostTests.EmptyDirectory(), logger, () => DarlingHealthHostTests.EnabledOn(healthPort));

            await host.StartAsync(ct);

            Assert.True(host.IsListening);
            using var client = new HttpClient();
            using var live = await client.GetAsync($"http://127.0.0.1:{healthPort}/livez", ct);
            Assert.Equal(HttpStatusCode.OK, live.StatusCode);
            await Assert.ThrowsAnyAsync<HttpRequestException>(() => client.GetAsync($"http://127.0.0.1:{extraPort}/livez", ct));
            await host.StopAsync(ct);
        }
        finally
        {
            Environment.SetEnvironmentVariable(ExtraEndpointVariable, previous);
        }
    }
}
