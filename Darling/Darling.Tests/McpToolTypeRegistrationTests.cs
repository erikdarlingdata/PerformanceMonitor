// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using ModelContextProtocol.Server;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every <see cref="McpServerToolTypeAttribute"/> class must actually be registered with the MCP host
/// (#2659).
///
/// <para><b>Six were not.</b> Ten shipped PostgreSQL reads — <c>get_pg_write_stats</c>,
/// <c>get_pg_buffer_usage</c>, <c>get_pg_extensions</c>, <c>get_pg_lock_stats</c>,
/// <c>get_pg_index_bloat</c>, <c>get_pg_column_stats</c>, <c>get_pg_kernel_stats</c>,
/// <c>get_pg_predicate_stats</c>, <c>get_pg_replication_stats</c>, <c>get_pg_wait_sampling</c> — were
/// implemented, documented, dispatched by the web API, counted in the instructions census and covered by
/// the name-based inventory pin, and an agent could not call any of them. Asked of the running service,
/// <c>tools/list</c> answered 116 tools where the census claimed 126.</para>
///
/// <para><b>Why the existing guards could not see it.</b> The inventory pin checks tool NAMES, and the
/// names exist — the attribute is on the method whether or not the class is registered. The
/// <c>POSTGRES_TABS</c> pin asserts every <c>get_pg_*</c> read reaches a web tab, and its own header says
/// it exists so a new read "cannot ship reachable only through MCP". This is the exact inverse, and there
/// was no pin for it: these shipped reachable only through the WEB.</para>
///
/// <para><b>Derived, not enumerated.</b> The check walks the assembly for the attribute and the host source
/// for its registrations, so it cannot go stale the way a hand-kept list does — and it fails the moment
/// someone adds a class, rather than whenever an agent next reaches for the tool. That is the same
/// reasoning as the tab pin being derived from the dispatch.</para>
/// </summary>
public sealed class McpToolTypeRegistrationTests
{
    [Fact]
    public void EveryMcpServerToolTypeClass_IsRegisteredWithTheHost()
    {
        var declared = typeof(DarlingMcpHostService).Assembly
            .GetTypes()
            .Where(t => t.GetCustomAttribute<McpServerToolTypeAttribute>() is not null)
            .Select(t => t.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        Assert.NotEmpty(declared);

        var registered = RegisteredToolTypeNames();

        var missing = declared.Where(n => !registered.Contains(n)).ToList();

        Assert.True(
            missing.Count == 0,
            "These [McpServerToolType] classes are never registered with the MCP host, so every tool they "
            + "declare is unreachable over MCP even though its name exists and the web API dispatches it: "
            + string.Join(", ", missing)
            + ". Add a .WithGeminiCompatibleTools<T>() line in DarlingMcpHostService.");
    }

    /// <summary>
    /// Reads the registrations out of the host SOURCE rather than by invoking the builder, because the
    /// builder needs a host, a store and a live configuration, and this is a wiring question that should be
    /// answerable without any of them.
    /// </summary>
    private static HashSet<string> RegisteredToolTypeNames()
    {
        var path = HostSourcePath();
        var source = File.ReadAllText(path);

        var names = Regex
            .Matches(source, @"WithGeminiCompatibleTools<(\w+)>")
            .Select(m => m.Groups[1].Value)
            .ToHashSet(StringComparer.Ordinal);

        Assert.True(
            names.Count > 0,
            $"Found no .WithGeminiCompatibleTools<T>() registrations in {path}. If the registration style "
            + "changed, this test needs to learn the new one rather than be deleted — it is the only thing "
            + "standing between a new tools class and shipping unreachable.");

        return names;
    }

    private static string HostSourcePath()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);

        while (dir is not null)
        {
            var candidate = Path.Combine(
                dir.FullName,
                "Darling",
                "PerformanceMonitor.Darling.Service",
                "Mcp",
                "DarlingMcpHostService.cs");

            if (File.Exists(candidate))
            {
                return candidate;
            }

            dir = dir.Parent;
        }

        throw new FileNotFoundException(
            "Could not locate DarlingMcpHostService.cs by walking up from the test output directory.");
    }
}
