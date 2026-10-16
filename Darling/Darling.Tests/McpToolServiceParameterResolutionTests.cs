/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4214 round-1 review, Low 4, rewritten as a behaviour test (#5950): every DI-service-typed parameter a registered
/// <c>[McpServerTool]</c> method declares must resolve from the host's REAL service collection. If a registration is ever
/// dropped (or put behind a condition), the SDK stops resolving that parameter from DI and instead serves it as a CLIENT
/// argument; for <c>PostgresConfig</c> specifically, a remote MCP caller could then set <c>managed: true</c> with a UNC
/// <c>dataDirectory</c> and make the service open a conf file on a remote share as its own account (the NTLM-relay vector
/// the review describes). This test builds the collection with the same two registration methods the host runs
/// (<c>RegisterToolServices</c> and <c>ConfigureMcpServices</c>) and resolves each parameter type from it, so the property
/// is checked by running it, not by searching the host's source text. It names no roster of types: a tool that gains a
/// service parameter with no seat fails here, and one with a seat passes.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class McpToolServiceParameterResolutionTests
{
    [Fact]
    public void EveryToolServiceParameter_ResolvesFromTheHostsRealServiceCollection()
    {
        var (_, toolClasses, _) = McpToolsListBudgetTests.BuildServedTools();

        var services = new ServiceCollection();
        /* A data source that never connects: building it opens nothing, and the tools are not called. */
        var postgres = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=x;Username=x");
        DarlingMcpHostService.RegisterToolServices(
            services, postgres, new MonitoredServerRegistryState(), new PostgresConfig(), analyzer: null,
            planFetcher: null, NullLogger.Instance, new BaselineCache());
        DarlingMcpHostService.ConfigureMcpServices(services, DarlingPeerDirectory.Snapshot.Empty);
        using var provider = services.BuildServiceProvider();
        var isService = provider.GetRequiredService<IServiceProviderIsService>();

        var serviceParameterTypes = toolClasses
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static))
            .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
            .SelectMany(m => m.GetParameters())
            .Select(p => p.ParameterType)
            .Where(McpServedSchema.IsServiceParameter)
            /* The SDK binds a CancellationToken parameter to the call's own cancellation (#4203), never from DI and
               never as a client argument, so it has no seat. */
            .Where(t => t != typeof(CancellationToken))
            .Select(t => Nullable.GetUnderlyingType(t) ?? t)
            .Distinct()
            .ToList();

        Assert.NotEmpty(serviceParameterTypes);

        var unresolved = serviceParameterTypes
            .Where(t => !isService.IsService(t) || provider.GetService(t) is null)
            .Select(t => t.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        Assert.True(
            unresolved.Count == 0,
            "The host's real service collection cannot resolve: " + string.Join(", ", unresolved) +
            ". A DI-service-typed [McpServerTool] parameter with no registration is served as a client argument instead of resolved from DI.");
    }
}
