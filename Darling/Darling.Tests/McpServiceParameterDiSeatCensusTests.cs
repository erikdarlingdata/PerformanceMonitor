/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Round-1 review (#4214), Low 4: every DI-service-typed parameter a registered <c>[McpServerTool]</c> method
/// declares must be registered somewhere in <c>DarlingMcpHostService.cs</c>'s own source text via
/// <c>AddSingleton&lt;T&gt;</c> or <c>AddSingleton(new T</c>. If a registration is ever dropped (or put behind a
/// condition), the SDK stops resolving that parameter from DI and instead serves it as a CLIENT argument — for
/// <c>PostgresConfig</c> specifically, a remote MCP caller could then set <c>managed: true</c> with a UNC
/// <c>dataDirectory</c> and make the service open a conf file on a remote share as its own account (the NTLM-relay
/// vector Low 4 in the round-1 review describes). Reuses
/// <see cref="McpToolsListBudgetTests.BuildServedTools"/>'s registered-tool-class regex rather than re-deriving
/// it, and <see cref="McpServedSchema.IsServiceParameter"/> for the same service-vs-model-supplied split the
/// budget census already uses.
///
/// <para>Coupled to item 3 by design: before item 3 trimmed the MCP host's DI seat to
/// <c>AddSingleton(new PostgresConfig { Managed = ..., DataDirectory = ... })</c>, the old
/// <c>AddSingleton(config.Postgres)</c> line did not name <c>PostgresConfig</c> anywhere in source text, so this
/// test only starts passing once item 3 lands.</para>
/// </summary>
public sealed class McpServiceParameterDiSeatCensusTests
{
    [Fact]
    public void EveryServiceParameterType_HasAnAddSingletonSeatInHostServiceSource()
    {
        var (_, types, _) = McpToolsListBudgetTests.BuildServedTools();
        var hostServiceSource = File.ReadAllText(
            RepoPath("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpHostService.cs"));

        var serviceParameterTypeNames = types
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static))
            .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
            .SelectMany(m => m.GetParameters())
            .Select(p => p.ParameterType)
            .Where(McpServedSchema.IsServiceParameter)
            /* McpSchemaCompat registers McpToolGuideCatalog itself (get_tool_guide's own seat), never through
               DarlingMcpHostService.cs's builder.Services calls — out of scope for this census. */
            .Where(t => t != typeof(McpToolGuideCatalog))
            /* The SDK binds a CancellationToken parameter to the call's own cancellation (#4203), never from DI
               and never as a client argument, so it has no seat to census. */
            .Where(t => t != typeof(CancellationToken))
            .Select(t => (Nullable.GetUnderlyingType(t) ?? t).Name)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        var missing = serviceParameterTypeNames
            .Where(name => !hostServiceSource.Contains($"AddSingleton<{name}>", StringComparison.Ordinal)
                && !hostServiceSource.Contains($"AddSingleton(new {name}", StringComparison.Ordinal))
            .ToList();

        Assert.True(
            missing.Count == 0,
            "DarlingMcpHostService.cs has no AddSingleton<T>/AddSingleton(new T seat for: " +
            string.Join(", ", missing) +
            ". A DI-service-typed [McpServerTool] parameter with no registration is served as a client " +
            "argument instead of resolved from DI.");

        /* A change worth knowing about even when nothing is missing: today's distinct complex service types are
           exactly these five (NpgsqlDataSource, PostgresConfig, DarlingAnalysisService, ILogger,
           StoreHostProfileCache) — pin the set so a sixth type appearing here is a deliberate, reviewed
           addition rather than a silent one. StoreHostProfileCache added deliberately (#4214 round-1 review,
           Medium 2): get_store_host's 5-minute shared cache, registered via the typed-generic
           AddSingleton<StoreHostProfileCache> overload this test's own Contains check requires. */
        Assert.Equal(
            new List<string> { "DarlingAnalysisService", "ILogger", "NpgsqlDataSource", "PostgresConfig", "StoreHostProfileCache" },
            serviceParameterTypeNames);
    }

    private static string RepoPath(params string[] segments) => Path.Combine(new[] { RepoRoot() }.Concat(segments).ToArray());

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
}
