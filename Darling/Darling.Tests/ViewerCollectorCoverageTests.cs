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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// THE COVERAGE PIN (parity board Tier 0), Darling side — the mirror of Lite's
/// <c>CollectorViewerCoverageTests</c>. Every collector table in <see cref="CollectorCatalog"/> must have
/// a consumer in the Darling viewer's reader layer
/// (<c>PerformanceMonitor.Darling.Viewer/ViewerDataService*.cs</c>, plus the shared store readers those
/// files CALL — see <see cref="ReaderLayerText"/>) — referenced directly by table name or
/// via its <c>v_</c> view — OR be listed in <see cref="KnownStoreOnlyOrUnbuiltTables"/>.
///
/// Darling currently reads EVERY catalog table, so the allow-list is empty and this pin is fully green.
/// Its job is forward protection: if a new collector is added to the catalog (both apps generate/consume
/// from it) and the Darling viewer never wires a reader for it, this goes red instead of shipping a
/// silently-invisible collector.
///
/// RATCHET: same contract as the Lite mirror — the allow-list can only shrink,
/// <see cref="AllowList_HasNoEntryThatIsActuallyRead"/> deletes stale entries the moment a reader appears,
/// and <see cref="AllowList_NamesOnlyRealCatalogTables"/> rejects a name that is not a real catalog table.
///
/// Text-scans the reader source (no Postgres / WPF needed) and locates the viewer via
/// <see cref="CallerFilePathAttribute"/>, exactly like <c>ThemeCompletenessTests</c>.
/// </summary>
public sealed class ViewerCollectorCoverageTests
{
    /// <summary>
    /// Collector tables the Darling service stores but whose data no viewer reader surfaces yet. A genuinely
    /// store-only/headless table (one deliberately never shown) would go here with a comment; an unbuilt-UI
    /// table would go here with a <c>// UNBUILT UI (parity board Tier 1) -- remove when the tab ships</c>
    /// comment.
    /// </summary>
    private static readonly HashSet<string> KnownStoreOnlyOrUnbuiltTables = new(StringComparer.OrdinalIgnoreCase)
    {
        // database_states is read by ViewerDataService.DatabaseStates.cs (the override editor's backing
        // store), so it is covered by the reader-layer scan and needs no allow-list entry.

        // EMPTY, and it stays empty. The nine PostgreSQL collector tables were the last entries here,
        // carrying "remove each when the PostgreSQL tab ships" — and #2530 shipped exactly those tabs, so
        // all nine came off in one commit. Every table in the catalog now has a Darling viewer reader.
    };

    [Fact]
    public void EveryCollectorTable_HasAViewerReader_OrIsAllowListed()
    {
        var readerText = ReaderLayerText();

        var uncovered = new List<string>();
        foreach (var definition in CollectorCatalog.All)
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
            "Collector table(s) have no Darling viewer reader and are not allow-listed. Add a reader in " +
            "PerformanceMonitor.Darling.Viewer/ViewerDataService*.cs, or — if intentionally store-only/" +
            "unbuilt — add the table to KnownStoreOnlyOrUnbuiltTables with a comment:\n  " +
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
            "Allow-list entr(y/ies) now HAVE a Darling viewer reader — delete them from " +
            "KnownStoreOnlyOrUnbuiltTables: " + string.Join(", ", stale));
    }

    /// <summary>A table is "referenced" if its name appears anywhere in the reader source — this matches
    /// both a direct <c>FROM table</c> and the usual <c>FROM v_table</c> view form (the view name contains
    /// the table name as a substring). No catalog table name is a substring of another, so this is
    /// collision-free across the catalog.</summary>
    private static bool ReferencedIn(string readerText, string table) =>
        readerText.Contains(table, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Concatenated text of the Darling viewer's reader layer
    /// (<c>PerformanceMonitor.Darling.Viewer/ViewerDataService*.cs</c>), located from this test file's
    /// compile-time path (Darling.Tests sits at <c>Darling/Darling.Tests/</c>; the viewer is the sibling
    /// <c>Darling/PerformanceMonitor.Darling.Viewer/</c>) — PLUS the shared store readers those files name.
    ///
    /// <para><b>Why the second half exists (#2530).</b> The PostgreSQL reads run the SAME query text on
    /// the MCP surface and on the WPF tabs, from <c>DarlingPg</c>-prefixed readers in
    /// <c>PerformanceMonitor.Darling.Storage</c>. A scan that looked only at <c>ViewerDataService*.cs</c>
    /// would have demanded a SECOND copy of that SQL — including a 200-line recursive blocking walk whose
    /// revisit guard, root attribution and truncation flag were each a separate review finding — purely to
    /// satisfy a text match. Duplicating load-bearing SQL so a coverage pin passes is the opposite of what
    /// the pin is for.</para>
    ///
    /// <para><b>It is still DERIVED, and still viewer-shaped.</b> A shared reader is followed only when the
    /// viewer's own source NAMES its class — one hop, outwards from the viewer, never the reverse, so a
    /// shared reader naming another cannot drag in coverage the viewer never asked for. Stop calling a
    /// reader from the viewer and its table loses coverage immediately, which is exactly the failure this
    /// pin exists to catch; proved by deleting one call and watching that table appear in the uncovered
    /// list.</para>
    /// </summary>
    private static string ReaderLayerText([CallerFilePath] string thisFile = "")
    {
        var testDir = Path.GetDirectoryName(thisFile)!;
        var readerDir = Path.GetFullPath(Path.Combine(testDir, "..", "PerformanceMonitor.Darling.Viewer"));

        // Enumerate all *.cs and filter by filename prefix rather than passing "ViewerDataService*.cs" as the
        // search pattern: Directory.EnumerateFiles matches the pattern against BOTH the long name and the 8.3
        // short name, which makes multi-dot names (e.g. ViewerDataService.FinOps.IndexAnalysis.cs) match
        // unreliably. A plain StartsWith is unambiguous. The obj/bin exclusion checks whole path SEGMENTS,
        // not a substring — a substring "obj"/"bin" would wrongly match a source file whose name contains it.
        var files = Directory
            .EnumerateFiles(readerDir, "*.cs", SearchOption.AllDirectories)
            .Where(f => Path.GetFileName(f).StartsWith("ViewerDataService", StringComparison.OrdinalIgnoreCase))
            .Where(f => !HasBuildOutputSegment(f))
            .ToList();

        Assert.False(files.Count == 0,
            $"No ViewerDataService*.cs files found under {readerDir} — the coverage pin cannot read the " +
            "Darling viewer's reader layer (did the reader layer move?).");

        var viewerText = string.Join("\n", files.Select(f => StripSchemaProbeLines(File.ReadAllText(f))));

        return viewerText + "\n" + SharedReadersNamedBy(viewerText, testDir);
    }

    /// <summary>
    /// The source of every shared store reader the viewer's own reader layer names, concatenated.
    /// <para>A name that resolves to no file under <c>PerformanceMonitor.Darling.Storage</c> is skipped
    /// rather than asserted on: the viewer legitimately mentions reader classes it cannot reference (the
    /// service's own, in prose), and a missing file grants no coverage — which is the safe direction to
    /// fail in.</para>
    /// </summary>
    private static string SharedReadersNamedBy(string viewerText, string testDir)
    {
        var sharedDir = Path.GetFullPath(Path.Combine(testDir, "..", "PerformanceMonitor.Darling.Storage"));

        var named = Regex
            .Matches(viewerText, @"\bDarling[A-Za-z0-9]*Reader\b")
            .Select(m => m.Value)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        var followed = new List<string>();
        foreach (var name in named)
        {
            var path = Path.Combine(sharedDir, name + ".cs");
            if (File.Exists(path) && !HasBuildOutputSegment(path))
            {
                followed.Add(StripSchemaProbeLines(File.ReadAllText(path)));
            }
        }

        return string.Join("\n", followed);
    }

    /// <summary>
    /// Drops every <c>information_schema</c> line before the substring scan. Those lines are the
    /// connect-time store-schema PROBE (<c>ViewerDataService.StoreSchemaProbeSql</c>), which asks whether a
    /// table EXISTS — it never reads a row out of one. Without this, naming a collector table as a migration
    /// sentinel (V29 long_query_completions, V34 ag_database_replica_states) would make that table look
    /// "covered" and silently exempt it from the ratchet — the exact failure this pin exists to catch.
    /// <c>information_schema</c> appears nowhere else in the reader layer, so the filter costs no real
    /// coverage.
    /// </summary>
    private static string StripSchemaProbeLines(string source) =>
        string.Join("\n", source
            .Split('\n')
            .Where(line => !line.Contains("information_schema", StringComparison.OrdinalIgnoreCase)));

    /// <summary>True if any whole path segment is a build-output directory (obj/bin). Segment-based, not a
    /// substring test — a plain Contains("obj") would false-positive on a source file whose name embeds it.</summary>
    private static bool HasBuildOutputSegment(string path) =>
        path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
            .Any(seg => seg.Equals("obj", StringComparison.OrdinalIgnoreCase) ||
                        seg.Equals("bin", StringComparison.OrdinalIgnoreCase));
}
