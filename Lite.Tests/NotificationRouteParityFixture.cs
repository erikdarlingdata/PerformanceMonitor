/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The one fleet and the one expected-result table for the scoped-route parity pin (#5750). Both products expand a route's
/// scope into a server-id set through the shared <c>NotificationRouteScopeExpander</c>, each from its own store: Darling from
/// PostgreSQL, Lite from DuckDB. No test project can open both stores (Darling.Tests has no Lite reference and Lite.Tests
/// has no PostgreSQL), so each project builds THIS fleet in its own store, reads it back through its own gather method,
/// expands THIS list of routes, and asserts THIS table. The two products agree because both match the one table; that is
/// the weaker form of a parity pin, and the reason the table is in server and tag NAMES: Lite's server id is a hash of the
/// name and Darling's is the registry id, so each side maps its ids back to names through its own name-to-id table and
/// compares names. Compiled into Darling.Tests as well (Compile Include, as <c>AlertLegendParityTests</c> is), so it uses
/// only the BCL.
/// </summary>
internal static class NotificationRouteParityFixture
{
    internal enum Kind { Fleet, Server, Tag }

    /// <summary>One route to store and expand. <see cref="Target"/> is a server or tag name; null for the fleet-wide route.</summary>
    internal sealed record Case(string Name, Kind Kind, string? Target, string[]? ExpectedServers, int ExpectedDepth, bool ExpectedOrphaned);

    /// <summary>The registry. <see cref="RemovedServer"/> is created with the rest and removed after its route is stored.</summary>
    internal static readonly string[] Servers = ["alpha", "bravo", "charlie", "delta", "echo", RemovedServer];

    internal const string RemovedServer = "foxtrot";

    /// <summary>The tags in creation order, each with its parent's name. prod > east > rack1 is three deep; dev and spare
    /// are roots; <see cref="DeletedTag"/> is created, stored in a route, then deleted.</summary>
    internal static readonly (string Name, string? Parent)[] Tags =
    [
        ("prod", null), ("east", "prod"), ("rack1", "east"), ("dev", null), ("spare", null), (DeletedTag, null)
    ];

    internal const string DeletedTag = "gone";

    /// <summary>Server to tag. delta is on two tags (rack1 and dev); spare has no server; echo has no tag.</summary>
    internal static readonly (string Server, string Tag)[] Assignments =
    [
        ("alpha", "rack1"), ("bravo", "east"), ("charlie", "prod"), ("delta", "rack1"), ("delta", "dev"), ("echo", DeletedTag)
    ];

    internal static readonly Case[] Cases =
    [
        new("fleet-wide", Kind.Fleet, null, null, 0, false),
        new("server alpha", Kind.Server, "alpha", ["alpha"], 0, false),
        new("server removed from the registry", Kind.Server, RemovedServer, [], 0, true),
        new("tag prod (depth 0)", Kind.Tag, "prod", ["alpha", "bravo", "charlie", "delta"], 0, false),
        new("tag east (depth 1)", Kind.Tag, "east", ["alpha", "bravo", "delta"], 1, false),
        new("tag rack1 (depth 2)", Kind.Tag, "rack1", ["alpha", "delta"], 2, false),
        new("tag dev (server on two tags)", Kind.Tag, "dev", ["delta"], 0, false),
        new("tag spare (empty)", Kind.Tag, "spare", [], 0, false),
        new("tag deleted", Kind.Tag, DeletedTag, [], 0, true)
    ];

    /// <summary>One comparable line per case: the case name, the sorted server NAMES (or "fleet-wide"), the depth, the orphan flag.</summary>
    internal static string Describe(string caseName, IReadOnlyCollection<int>? expandedIds, int depth, bool orphaned,
        IReadOnlyDictionary<int, string> nameById)
    {
        var servers = expandedIds is null
            ? "fleet-wide"
            : "[" + string.Join(",", expandedIds.Select(id => nameById.TryGetValue(id, out var n) ? n : $"?{id}").OrderBy(n => n, StringComparer.Ordinal)) + "]";
        return $"{caseName} | {servers} | depth {depth} | orphaned {orphaned}";
    }

    /// <summary>The expected table in the same line form.</summary>
    internal static IReadOnlyList<string> ExpectedLines() =>
        Cases.Select(c => $"{c.Name} | " +
            (c.ExpectedServers is null ? "fleet-wide" : "[" + string.Join(",", c.ExpectedServers.OrderBy(n => n, StringComparer.Ordinal)) + "]") +
            $" | depth {c.ExpectedDepth} | orphaned {c.ExpectedOrphaned}").ToList();
}
