/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// THE COVERAGE PIN (parity board Tier 0), Lite side. Every collector table in
/// <see cref="CollectorCatalog"/> must have a consumer in Lite's reader layer
/// (<c>Lite/Services/LocalDataService*.cs</c>) — referenced directly by table name or via its
/// <c>v_</c> view — OR be listed in <see cref="KnownStoreOnlyOrUnbuiltTables"/> as tracked debt.
///
/// This turns "collect-but-don't-show" from a silently-green build into a red one: today a collector
/// plus a DuckDB table can land with zero UI ever reading it and nothing complains (the parity audit
/// found several such tables). With this pin, a NEW collector table that no reader touches — and that
/// nobody deliberately allow-lists — fails immediately.
///
/// RATCHET: the allow-list is a visible, shrinking to-do. When a tab/reader ships for one of these
/// tables the reader starts referencing it, <see cref="AllowList_HasNoEntryThatIsActuallyRead"/> goes
/// red, and the entry MUST be deleted. The list can only get smaller; it can never quietly absorb a
/// regression, because <see cref="AllowList_NamesOnlyRealCatalogTables"/> rejects anything that is not
/// a real catalog table.
///
/// Text-scans the reader source (no DuckDB / WPF needed) and locates Lite via
/// <see cref="CallerFilePathAttribute"/>, exactly like <c>ThemeCompletenessTests</c>.
/// </summary>
public sealed class CollectorViewerCoverageTests
{
    /// <summary>
    /// Collector tables that Lite collects and stores but whose data no reader/viewer surfaces yet.
    /// Every entry is Tier-1 unbuilt UI: Lite already collects the data, the Darling viewer already has
    /// the tab, and the Lite port is pending.
    ///
    /// The list has been fully drained twice now. The two Availability Group tables (#991) re-opened it
    /// while their v1 was deliberately collection-only, and #1696 drained them again by giving Lite a
    /// reader for both grains to drive its AG alerts — which is exactly the transition this ratchet exists
    /// to force: the moment a reader appears, <see cref="AllowList_HasNoEntryThatIsActuallyRead"/> goes red
    /// and the entry has to go. Leave the set EMPTY rather than deleting it; an empty allow-list is the
    /// goal state and re-adding an entry should be a deliberate, visible act.
    /// </summary>
    private static readonly HashSet<string> KnownStoreOnlyOrUnbuiltTables = new(StringComparer.OrdinalIgnoreCase);

    [Fact]
    public void EveryCollectorTable_HasALiteReader_OrIsAllowListed()
    {
        var readerText = ReaderLayerText();

        var uncovered = new List<string>();
        /* StoredCollectors, not CollectorCatalog.All: the shared catalog is engine-mixed, and Lite neither
           collects nor creates a table for the PostgreSQL definitions — it has no PostgreSQL target and the
           engine gate means it can never dispatch one. A reader for a table this SKU does not have would be
           dead code, so they are out of scope here rather than allow-listed exceptions. */
        foreach (var definition in DuckDbSchemaGenerator.StoredCollectors)
        {
            var table = definition.TargetTable;
            if (ReferencedIn(readerText, table))
            {
                continue; // read directly or via its v_ view
            }
            if (KnownStoreOnlyOrUnbuiltTables.Contains(table))
            {
                continue; // tracked debt — the ratchet keeps this list shrinking
            }
            uncovered.Add(table);
        }

        Assert.True(uncovered.Count == 0,
            "Collector table(s) have no Lite reader and are not allow-listed. Add a reader/tab in " +
            "Lite/Services/LocalDataService*.cs, or — if the UI is intentionally not built yet — add the " +
            "table to KnownStoreOnlyOrUnbuiltTables with a Tier-1 comment:\n  " +
            string.Join("\n  ", uncovered.OrderBy(t => t, StringComparer.Ordinal)));
    }

    [Fact]
    public void AllowList_NamesOnlyRealCatalogTables()
    {
        var catalog = CollectorCatalog.All
            .Select(d => d.TargetTable)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var unknown = KnownStoreOnlyOrUnbuiltTables
            .Where(t => !catalog.Contains(t))
            .OrderBy(t => t, StringComparer.Ordinal)
            .ToList();

        Assert.True(unknown.Count == 0,
            "Allow-list names table(s) that are not in CollectorCatalog (typo, or a removed/renamed " +
            "collector): " + string.Join(", ", unknown));
    }

    [Fact]
    public void AllowList_HasNoEntryThatIsActuallyRead()
    {
        // The ratchet enforcement: once a table gains a reader, its allow-list entry is stale debt and
        // must be deleted. This fails the build until it is, so the list can only shrink.
        var readerText = ReaderLayerText();

        var stale = KnownStoreOnlyOrUnbuiltTables
            .Where(t => ReferencedIn(readerText, t))
            .OrderBy(t => t, StringComparer.Ordinal)
            .ToList();

        Assert.True(stale.Count == 0,
            "Allow-list entr(y/ies) now HAVE a Lite reader — the tab/reader shipped, so delete them from " +
            "KnownStoreOnlyOrUnbuiltTables: " + string.Join(", ", stale));
    }

    /// <summary>A table is "referenced" if its name appears anywhere in the reader source — this matches
    /// both a direct <c>FROM table</c> and the usual <c>FROM v_table</c> view form (the view name contains
    /// the table name as a substring). No catalog table name is a substring of another, so this is
    /// collision-free across the catalog.</summary>
    private static bool ReferencedIn(string readerText, string table) =>
        readerText.Contains(table, StringComparison.OrdinalIgnoreCase);

    /// <summary>Concatenated text of Lite's reader layer (<c>Lite/Services/LocalDataService*.cs</c>),
    /// located from this test file's compile-time path (Lite.Tests is a sibling of the Lite project).</summary>
    private static string ReaderLayerText([CallerFilePath] string thisFile = "")
    {
        var testDir = Path.GetDirectoryName(thisFile)!;
        var readerDir = Path.GetFullPath(Path.Combine(testDir, "..", "Lite", "Services"));

        // Enumerate all *.cs and filter by filename prefix rather than passing "LocalDataService*.cs" as the
        // search pattern: Directory.EnumerateFiles matches the pattern against BOTH the long name and the 8.3
        // short name, which makes multi-dot names (e.g. LocalDataService.FinOps.IndexObjects.cs) match
        // unreliably. A plain StartsWith is unambiguous. The obj/bin exclusion checks whole path SEGMENTS,
        // not a substring — a substring "obj" would wrongly match LocalDataService.FinOps.Index*Obj*ects.cs.
        var files = Directory
            .EnumerateFiles(readerDir, "*.cs", SearchOption.AllDirectories)
            .Where(f => Path.GetFileName(f).StartsWith("LocalDataService", StringComparison.OrdinalIgnoreCase))
            .Where(f => !HasBuildOutputSegment(f))
            .ToList();

        Assert.False(files.Count == 0,
            $"No LocalDataService*.cs files found under {readerDir} — the coverage pin cannot read Lite's " +
            "reader layer (did the reader layer move?).");

        return string.Join("\n", files.Select(File.ReadAllText));
    }

    /// <summary>True if any whole path segment is a build-output directory (obj/bin). Segment-based, not a
    /// substring test — a plain Contains("obj") would false-positive on a source file such as
    /// LocalDataService.FinOps.IndexObjects.cs.</summary>
    private static bool HasBuildOutputSegment(string path) =>
        path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
            .Any(seg => seg.Equals("obj", StringComparison.OrdinalIgnoreCase) ||
                        seg.Equals("bin", StringComparison.OrdinalIgnoreCase));
}
