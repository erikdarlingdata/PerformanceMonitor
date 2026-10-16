/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4726: the MCP host must hand every tool call its OWN analysis service. One shared instance answers a second,
/// overlapping analyze_server call with an empty list (the busy check) and the tool then reads the first call's
/// running state, so the second server got "No significant findings" for a server it never analyzed.
///
/// <para>No end-to-end overlap test here, unlike Lite's: this product's analyze_server resolves the server through
/// the store's registry before it analyzes, so that test would need a live store. The registration test below
/// covers what matters, that the host's registration builds a fresh service per resolution.</para>
/// </summary>
public sealed class AnalyzeServerPerCallAnalysisServiceTests
{
    /// <summary>
    /// The runtime pin: the host's own registration method, on a fresh collection, resolved twice the way two MCP
    /// calls resolve it. A singleton registration fails the <see cref="Assert.NotSame"/> line.
    /// </summary>
    [Fact]
    public void TwoResolutionsOfTheHostRegistration_AreDistinctServices_ThatShareTheOneBaselineCache()
    {
        /* Creating the data source connects to nothing, and the service constructors do no I/O, so no store is needed. */
        using var postgres = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=postgres;Username=darling");
        var hostBaselines = new BaselineCache();

        var services = new ServiceCollection();
        DarlingMcpHostService.RegisterAnalysisService(services, postgres, planFetcher: null, NullLogger.Instance, hostBaselines, analyzer: null);

        using var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<DarlingAnalysisService>();
        var second = provider.GetRequiredService<DarlingAnalysisService>();

        Assert.NotSame(first, second);

        /* Every per-call instance still gets the ONE process-wide BaselineCache (#3941), never a fresh one. */
        Assert.NotNull(first.SharedBaselineCache);
        Assert.Same(hostBaselines, first.SharedBaselineCache);
        Assert.Same(first.SharedBaselineCache, second.SharedBaselineCache);
    }

    /// <summary>
    /// The host's tool-service registration (<c>RegisterToolServices</c>, the method the start path calls), resolved twice: the
    /// analysis service it registers is one per call, and every instance holds the host's ONE shared BaselineCache (#3941).
    /// Replaces a pin on the host source's call text (#5950).
    /// </summary>
    [Fact]
    public void TheMcpHostsToolServices_ResolveOneAnalysisServicePerCall_OverTheHostsSharedBaselineCache()
    {
        using var postgres = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=postgres;Username=darling");
        var hostBaselines = new BaselineCache();

        var services = new ServiceCollection();
        DarlingMcpHostService.RegisterToolServices(
            services, postgres, new MonitoredServerRegistryState(), new PostgresConfig(), analyzer: null,
            planFetcher: null, NullLogger.Instance, hostBaselines);

        using var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<DarlingAnalysisService>();
        var second = provider.GetRequiredService<DarlingAnalysisService>();

        Assert.NotSame(first, second);
        Assert.Same(hostBaselines, first.SharedBaselineCache);
        Assert.Same(hostBaselines, second.SharedBaselineCache);
    }
}
