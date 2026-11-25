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
/// <para><b>Derived, not enumerated.</b> The check walks the assembly for the attribute and the tool methods, and
/// reads the host's registrations from the discovery list it registers (<see cref="DarlingMcpToolTypes"/>), so it cannot go stale the way a hand-kept list does, and it fails the moment
/// someone adds a class, rather than whenever an agent next reaches for the tool. That is the same
/// reasoning as the tab pin being derived from the dispatch.</para>
///
/// <para><b>#3466 nearly repeated #2659.</b> <c>DarlingMcpFleetSweepTools</c> shipped with its tool
/// documented, web-dispatched and counted in the instructions census — and no registration line, the exact
/// shape above. This pin would have gone red where it runs; review caught it first. Two things hardened
/// here in response: the declared set now also derives from <c>[McpServerTool]</c> METHODS (a tool method
/// on a class that lost its type attribute is registered-and-invisible to an attribute-only census, and
/// <c>WithGeminiCompatibleTools</c> scans methods, not the type attribute), and the assert became
/// SET-EQUALITY — a registration naming a class that declares no tools registers NOTHING silently, a stale
/// line that reads as coverage while delivering none, so it reds too.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class McpToolTypeRegistrationTests
{
    [Fact]
    public void TheRegisteredToolTypes_AreExactlyTheDeclaredToolTypes()
    {
        var declared = DeclaredToolTypeNames();

        Assert.NotEmpty(declared);

        var registered = RegisteredToolTypeNames();

        var missing = declared.Where(n => !registered.Contains(n))
            .OrderBy(n => n, StringComparer.Ordinal).ToList();
        var stale = registered.Where(n => !declared.Contains(n))
            .OrderBy(n => n, StringComparer.Ordinal).ToList();

        Assert.True(
            missing.Count == 0,
            "These tool classes are never registered with the MCP host, so every tool they "
            + "declare is unreachable over MCP even though its name exists and the web API dispatches it: "
            + string.Join(", ", missing)
            + ". Put [McpServerToolType] on the class, so the host's discovery (DarlingMcpToolTypes) registers it.");

        Assert.True(
            stale.Count == 0,
            "These .WithGeminiCompatibleTools<T>() registrations name classes that declare no MCP tools, "
            + "so each line registers nothing while reading as coverage: "
            + string.Join(", ", stale)
            + ". Remove the attribute, or restore the class's [McpServerTool] members.");
    }

    /// <summary>
    /// The declared set, derived BOTH ways the SDK vocabulary can express a tool: classes carrying
    /// <see cref="McpServerToolTypeAttribute"/>, unioned with classes carrying at least one
    /// <see cref="McpServerToolAttribute"/> method — because the registration path
    /// (<c>WithGeminiCompatibleTools</c>) scans METHODS, so a method-carrying class is a real tool surface
    /// whether or not its type attribute survived a refactor.
    /// </summary>
    private static HashSet<string> DeclaredToolTypeNames()
    {
        var declared = new HashSet<string>(StringComparer.Ordinal);

        foreach (var type in typeof(DarlingMcpHostService).Assembly.GetTypes())
        {
            if (type.GetCustomAttribute<McpServerToolTypeAttribute>() is not null
                || type.GetMethods(
                        BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
                    .Any(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null))
            {
                declared.Add(type.Name);
            }
        }

        return declared;
    }

    /// <summary>
    /// The tool classes the host registers: <see cref="DarlingMcpToolTypes.All"/>, the list <c>WithDarlingTools</c> registers. It finds a
    /// class by its <see cref="McpServerToolTypeAttribute"/>, so a class whose tool methods lost that attribute is missing here.
    /// </summary>
    private static HashSet<string> RegisteredToolTypeNames()
    {
        var names = DarlingMcpToolTypes.All.Select(t => t.Name).ToHashSet(StringComparer.Ordinal);

        Assert.True(
            names.Count > 0,
            "The host registers no tool classes. It is the only thing standing between a new tools class and shipping unreachable.");

        return names;
    }
}
