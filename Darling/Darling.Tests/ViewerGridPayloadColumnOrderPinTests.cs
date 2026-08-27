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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #1949 — pins the DISPLAY position of the query text and query plan columns (Darling side; Lite.Tests
/// carries the mirror-image pin). On a query surface the text and plan columns are the payload, so they
/// belong immediately right of the collection/execution time column that orients the row — not behind a
/// dozen numeric columns. They drifted to the back over years precisely because nothing asserted display
/// order anywhere in either app: the eight <c>Pins*ColumnOrder</c> tests that do exist live in Lite.Tests
/// and assert the collector WRITE payload into the store, never a <c>DataGrid</c>. Worst case before the
/// fix was <c>QueryStoreGrid</c>, where the query text was column 55 of 55.
///
/// The pin is the table below, transcribed from the #1949 census, plus EVERY web column list in the service
/// that renders query text — one when this pin was written, six since the web server page grew sub-tabs
/// (#2475), which is why the web half is now a table of its own rather than a single hardcoded array. For each
/// grid it records the time (or identity) ANCHOR and the payload columns that must follow it immediately.
/// Adding a grid to this table whose payload columns still sit at the back FAILS — that is the point.
///
/// Two grids here have no Lite twin: <c>CurrentActiveQueriesGrid</c> and <c>QueryStoreRegressionsGrid</c>.
/// The FinOps grids differ from their Lite twins by a <c>FinOps</c> name prefix only. Everything else is
/// byte-symmetric with Lite by column sequence, which <see cref="TwinGridsCarryIdenticalColumnSequences"/>
/// enforces rather than leaving to a comment.
///
/// XAML <c>Columns</c> order IS display order in both apps: there is no <c>DisplayIndex</c> assignment, no
/// <c>Columns.Move</c>, and no index-based <c>Columns[i]</c> access anywhere. The one runtime column call is
/// the <c>Columns.Insert(0, ...)</c> both apps' <c>WaitDrillDownWindow</c> does for blocking-chain
/// drill-downs (Lite factors it into <c>InsertChainColumns</c>; the Viewer inlines it). It prepends, so it
/// shifts every XAML index by +1 without disturbing this relative ordering.
///
/// Text-scans SOURCE XAML and JS located from this file's compile-time path — no WPF or assembly load,
/// exactly like the other parity pins. Headers are read verbatim, so a header carrying an XML entity would
/// have to be written into the table in its raw <c>&amp;#x0394;</c> form; no pinned header has one today.
/// </summary>
public sealed class ViewerGridPayloadColumnOrderPinTests
{
    /// <summary>A grid whose payload columns are pinned: where the anchor sits, and what must follow it.</summary>
    private sealed record GridPin(
        string RelativePath,
        string GridName,
        string AnchorHeader,
        int AnchorIndex,
        string[] PayloadColumns);

    private static readonly string ViewerDir = Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer");
    private static readonly string ViewerServerTab = Path.Combine(ViewerDir, "ViewerServerTab.xaml");
    private static readonly string ViewerFinOpsTab = Path.Combine(ViewerDir, "FinOpsTab.xaml");
    private static readonly string ViewerWaitDrillDown = Path.Combine(ViewerDir, "WaitDrillDownWindow.xaml");
    private static readonly string ViewerProcedureHistory = Path.Combine(ViewerDir, "ProcedureHistoryWindow.xaml");
    private static readonly string ViewerQueryStatsHistory = Path.Combine(ViewerDir, "QueryStatsHistoryWindow.xaml");
    private static readonly string ViewerQueryStoreHistory = Path.Combine(ViewerDir, "QueryStoreHistoryWindow.xaml");

    /* The web server page's descriptor arrays moved out of pages/server.js and into pages/server-tabs.js when
       the page grew sub-tabs (#2475): server.js is now the shell (header, tab bar, range picker, panel grid)
       and the tab registry owns every column array. The pin follows the array, not the filename it used to
       sit in. */
    private static readonly string ServerPageJs = Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");

    /* The #1949 move list for the Darling viewer. Comparison and FinOps grids carry no time column, so their
       anchor is the identity block the grid is ranked by (Database, or Score+Database on High Impact).
       QueryStoreHistory anchors after index 4 because its time columns are non-contiguous — Plan ID and
       Exec Type interleave — and the whole time block stays intact. */
    private static readonly GridPin[] Pins =
    [
        new(ViewerServerTab, "QuerySnapshotsGrid", "Collected", 1, ["Query Text", "Query Plan"]),
        new(ViewerServerTab, "CurrentActiveQueriesGrid", "Collected", 1, ["Query Text", "Query Plan"]),
        new(ViewerServerTab, "QueryStatsGrid", "Creation Time", 3, ["Query Text", "Query Plan"]),
        new(ViewerServerTab, "QueryStatsComparisonGrid", "Database", 1, ["Query Text"]),
        new(ViewerServerTab, "ProcedureStatsGrid", "Cached Time", 4, ["Query Plan"]),
        new(ViewerServerTab, "QueryStoreGrid", "First Execution", 5, ["Query Text", "Query Plan"]),
        new(ViewerServerTab, "QueryStoreComparisonGrid", "Database", 1, ["Query Text"]),
        new(ViewerServerTab, "QueryStoreRegressionsGrid", "Last Execution", 0, ["Query Text"]),
        new(ViewerServerTab, "PlanCorrectionGrid", "Collected", 0, ["Query Text"]),
        new(ViewerServerTab, "BlockedProcessReportGrid", "Event Time", 0, ["Blocked SQL", "Blocking SQL", "XML"]),
        new(ViewerServerTab, "DeadlockGrid", "Time", 0, ["SQL Text", "XML"]),
        new(ViewerServerTab, "SignificantWaitsGrid", "Event Time", 0, ["Query Text"]),
        new(ViewerServerTab, "LongQueryCompletionsGrid", "Event Time", 0, ["Statement"]),
        new(ViewerWaitDrillDown, "ResultsDataGrid", "Collected", 1, ["Query Text"]),
        new(ViewerProcedureHistory, "HistoryDataGrid", "Cached Time", 2, ["Plan"]),
        new(ViewerQueryStatsHistory, "HistoryDataGrid", "Creation Time", 2, ["Plan"]),
        new(ViewerQueryStoreHistory, "HistoryDataGrid", "Last Execution", 4, ["Plan"]),
        new(ViewerFinOpsTab, "FinOpsExpensiveQueriesDataGrid", "Database", 0, ["Query Preview"]),
        new(ViewerFinOpsTab, "FinOpsHighImpactDataGrid", "Database", 1, ["Query Preview"]),
    ];

    private static readonly string LiteServerTab = Path.Combine("Lite", "Controls", "ServerTab.xaml");
    private static readonly string LiteFinOpsTab = Path.Combine("Lite", "Controls", "FinOpsTab.xaml");
    private static readonly string LiteWaitDrillDown = Path.Combine("Lite", "Windows", "WaitDrillDownWindow.xaml");
    private static readonly string LiteProcedureHistory = Path.Combine("Lite", "Windows", "ProcedureHistoryWindow.xaml");
    private static readonly string LiteQueryStatsHistory = Path.Combine("Lite", "Windows", "QueryStatsHistoryWindow.xaml");
    private static readonly string LiteQueryStoreHistory = Path.Combine("Lite", "Windows", "QueryStoreHistoryWindow.xaml");

    /* Every query grid that exists in BOTH apps, plus the #1951 PVS pair — that grid carries no query text
       (so it is not in the Pins table above), but it was ported to the Viewer as a copy of Lite's and the
       symmetry is worth holding the same way. The FinOps pairs differ by the Viewer's `FinOps` x:Name
       prefix only. CurrentActiveQueriesGrid and QueryStoreRegressionsGrid are Viewer-only and have no row. */
    private static readonly (string LiteFile, string LiteGrid, string ViewerFile, string ViewerGrid)[] Twins =
    [
        (LiteServerTab, "QuerySnapshotsGrid", ViewerServerTab, "QuerySnapshotsGrid"),
        (LiteServerTab, "QueryStatsGrid", ViewerServerTab, "QueryStatsGrid"),
        (LiteServerTab, "QueryStatsComparisonGrid", ViewerServerTab, "QueryStatsComparisonGrid"),
        (LiteServerTab, "ProcedureStatsGrid", ViewerServerTab, "ProcedureStatsGrid"),
        (LiteServerTab, "QueryStoreGrid", ViewerServerTab, "QueryStoreGrid"),
        (LiteServerTab, "QueryStoreComparisonGrid", ViewerServerTab, "QueryStoreComparisonGrid"),
        (LiteServerTab, "PlanCorrectionGrid", ViewerServerTab, "PlanCorrectionGrid"),
        (LiteServerTab, "BlockedProcessReportGrid", ViewerServerTab, "BlockedProcessReportGrid"),
        (LiteServerTab, "DeadlockGrid", ViewerServerTab, "DeadlockGrid"),
        (LiteServerTab, "SignificantWaitsGrid", ViewerServerTab, "SignificantWaitsGrid"),
        (LiteServerTab, "LongQueryCompletionsGrid", ViewerServerTab, "LongQueryCompletionsGrid"),
        (LiteWaitDrillDown, "ResultsDataGrid", ViewerWaitDrillDown, "ResultsDataGrid"),
        (LiteProcedureHistory, "HistoryDataGrid", ViewerProcedureHistory, "HistoryDataGrid"),
        (LiteQueryStatsHistory, "HistoryDataGrid", ViewerQueryStatsHistory, "HistoryDataGrid"),
        (LiteQueryStoreHistory, "HistoryDataGrid", ViewerQueryStoreHistory, "HistoryDataGrid"),
        (LiteFinOpsTab, "ExpensiveQueriesDataGrid", ViewerFinOpsTab, "FinOpsExpensiveQueriesDataGrid"),
        (LiteFinOpsTab, "HighImpactDataGrid", ViewerFinOpsTab, "FinOpsHighImpactDataGrid"),
        (LiteFinOpsTab, "PvsStatsDataGrid", ViewerFinOpsTab, "FinOpsPvsStatsDataGrid"),
    ];

    public static TheoryData<string, string> PinKeys()
    {
        var data = new TheoryData<string, string>();
        foreach (var pin in Pins)
        {
            data.Add(pin.RelativePath, pin.GridName);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(PinKeys))]
    public void PayloadColumnsFollowTheAnchor(string relativePath, string gridName)
    {
        var pin = Pins.Single(p => p.RelativePath == relativePath && p.GridName == gridName);
        var headers = ColumnHeaders(ReadRepoFile(pin.RelativePath), pin.GridName);

        Assert.True(headers.Count > pin.AnchorIndex + pin.PayloadColumns.Length,
            $"{pin.GridName}: only {headers.Count} columns parsed — too few to carry the pinned payload. " +
            "The column scan is broken, not the grid.");

        var anchorAt = headers.IndexOf(pin.AnchorHeader);
        Assert.True(anchorAt == pin.AnchorIndex,
            $"{pin.GridName} ({pin.RelativePath}): anchor column '{pin.AnchorHeader}' is at index {anchorAt}, " +
            $"expected {pin.AnchorIndex}. The row's orienting column moved; re-derive this pin's anchor " +
            "before touching the payload columns.");

        var actual = headers.Skip(anchorAt + 1).Take(pin.PayloadColumns.Length).ToArray();
        Assert.True(actual.SequenceEqual(pin.PayloadColumns),
            $"{pin.GridName} ({pin.RelativePath}): the columns after '{pin.AnchorHeader}' are " +
            $"[{string.Join(", ", actual)}], expected [{string.Join(", ", pin.PayloadColumns)}]. " +
            "Query text and plan columns belong immediately right of the time/identity anchor (#1949), " +
            $"not behind the metrics. Full order: [{string.Join(", ", headers)}]");
    }

    /// <summary>
    /// Every web column list that renders query text puts it immediately right of the anchor.
    ///
    /// <para>This used to pin <c>ACTIVE_COLUMNS</c> alone, because it was the only such list in the service. The
    /// web server page's sub-tabs (#2475) added five more, and a rule enforced on one of six lists is a rule that
    /// will be broken on the other five — the same reason the XAML half is a table rather than a spot-check. A new
    /// grid whose text sits behind its metrics FAILS here, which is the point.</para>
    /// </summary>
    [Theory]
    [InlineData("ACTIVE_COLUMNS", "collection_time", "query_text", 9)]
    [InlineData("TOP_QUERY_COLUMNS", "database_name", "query_text", 10)]
    [InlineData("QUERY_STORE_COLUMNS", "database_name", "query_text", 8)]
    [InlineData("LONG_QUERY_COLUMNS", "event_time", "statement", 8)]
    [InlineData("PLAN_CORRECTION_COLUMNS", "collection_time", "query_text", 8)]
    [InlineData("BLOCKING_COLUMNS", "event_time", "blocked_sql_text", 9)]
    [InlineData("DEADLOCK_COLUMNS", "deadlock_time", "victim_sql_text", 5)]
    public void EveryWebGridWithQueryText_PutsItRightOfTheAnchor(string array, string anchor, string text, int minKeys)
    {
        // Array order IS the rendered column order — the page hands the array straight to the table renderer.
        var keys = JsColumnKeys(ReadRepoFile(ServerPageJs), array);
        Assert.True(keys.Count >= minKeys, $"only {keys.Count} {array} keys parsed — the scan is broken.");
        Assert.Equal(anchor, keys[0]);
        Assert.Equal(text, keys[1]);

        // Without this, re-adding a second text column at the BACK would leave the pin green — the same hole
        // EveryPinnedPayloadColumnIsUniqueInItsGrid closes on the XAML side.
        Assert.True(keys.Count(k => k == text) == 1,
            $"{text} appears {keys.Count(k => k == text)} times in {array}; expected once.");
    }

    [Fact]
    public void TwinGridsCarryIdenticalColumnSequences()
    {
        // Lite is the reference front end and the Viewer is a copy of it, so every twinned query grid must
        // carry the SAME column sequence — the #1949 moves were made byte-symmetric on purpose. Without
        // this, the two pin tables above would still pass while the apps drifted anywhere OUTSIDE the
        // anchor/payload window, which is how the two front ends fork in the first place.
        foreach (var (liteFile, liteGrid, viewerFile, viewerGrid) in Twins)
        {
            var lite = ColumnHeaders(ReadRepoFile(liteFile), liteGrid);
            var viewer = ColumnHeaders(ReadRepoFile(viewerFile), viewerGrid);
            if (lite.SequenceEqual(viewer))
            {
                continue;
            }

            var firstDrift = lite.Zip(viewer)
                .Select((pair, index) => (pair, index))
                .Where(x => x.pair.First != x.pair.Second)
                .Select(x => $" First difference at index {x.index}: " +
                             $"Lite '{x.pair.First}' vs Viewer '{x.pair.Second}'.")
                .FirstOrDefault(string.Empty);

            Assert.Fail(
                $"{liteGrid} (Lite) and {viewerGrid} (Viewer) have drifted apart: " +
                $"{lite.Count} vs {viewer.Count} columns.{firstDrift} " +
                "Keep the two front ends symmetric; a change to one belongs in the other in the same PR.");
        }
    }

    [Fact]
    public void EveryPinnedPayloadColumnIsUniqueInItsGrid()
    {
        // A duplicated header would make IndexOf ambiguous and could let a back-of-row copy satisfy the pin.
        foreach (var pin in Pins)
        {
            var headers = ColumnHeaders(ReadRepoFile(pin.RelativePath), pin.GridName);
            foreach (var payload in pin.PayloadColumns.Append(pin.AnchorHeader))
            {
                Assert.True(headers.Count(h => h == payload) == 1,
                    $"{pin.GridName}: header '{payload}' appears {headers.Count(h => h == payload)} times; " +
                    "the pin needs it to be unique.");
            }
        }
    }

    [Fact]
    public void ColumnScanIsNotVacuous()
    {
        // Self-check: the parser must actually walk the grids, not silently return empty lists. The #1949
        // census counted 501 columns across 18 viewer grids, and #1952's 27-column PlanCorrectionGrid brings
        // that to 528 across 19; the floor is deliberately below that so adding a column never fails this,
        // but a broken scan does. It moved 480 -> 507 with that grid — up by exactly the 27 columns it adds,
        // which preserves the guard's existing slack rather than widening or narrowing it.
        var total = Pins.Sum(p => ColumnHeaders(ReadRepoFile(p.RelativePath), p.GridName).Count);
        Assert.True(total >= 507, $"only {total} columns walked across {Pins.Length} grids — the scan is broken.");
    }

    [Fact]
    public void NoGridDroppedOutOfTheTable()
    {
        // The anti-deletion half of the ratchet: quietly deleting a row would make this suite green by
        // covering less, which is the one way a pin can rot without anything going red.
        Assert.True(Pins.Length == 19,
            $"the pin table holds {Pins.Length} grids, expected the 18 Viewer grids on the #1949 move list " +
            "plus #1952's PlanCorrectionGrid. " +
            "Removing a grid needs a stated reason (the grid or its payload column is gone); adding one is " +
            "free, but bump this number in the same commit.");
        Assert.True(Twins.Length == 18,
            $"the twin table holds {Twins.Length} pairs, expected 18: the 19 Viewer grids less the two " +
            "Viewer-only ones (CurrentActiveQueriesGrid and QueryStoreRegressionsGrid), plus the #1951 " +
            "PVS pair, which carries no query text and so is twinned without being pinned.");
    }

    /* ---------------- JS column scan ---------------- */

    private static readonly Regex JsKey = new(@"\bkey:\s*""([^""]+)""");

    /// <summary>Keys of a <c>const NAME = [ ... ];</c> column array, in declaration order (= render order).</summary>
    private static List<string> JsColumnKeys(string js, string arrayName)
    {
        var marker = $"const {arrayName} = [";
        var at = js.IndexOf(marker, StringComparison.Ordinal);
        Assert.True(at >= 0, $"{arrayName} not found in the scanned module");
        var end = js.IndexOf("\n];", at, StringComparison.Ordinal);
        Assert.True(end >= 0, $"{arrayName} is unterminated");
        return JsKey.Matches(js[(at + marker.Length)..end]).Select(m => m.Groups[1].Value).ToList();
    }

    /* ---------------- XAML column scan ---------------- */

    /// <summary>Headers of the named grid's columns, in declaration order (which is display order).</summary>
    private static List<string> ColumnHeaders(string xaml, string gridName)
    {
        var marker = $"x:Name=\"{gridName}\"";
        var at = xaml.IndexOf(marker, StringComparison.Ordinal);
        Assert.True(at >= 0, $"grid {gridName} not found in the XAML");
        Assert.True(xaml.IndexOf(marker, at + marker.Length, StringComparison.Ordinal) < 0,
            $"grid {gridName} is declared more than once — the scan would pick the wrong one");

        const string open = "<DataGrid.Columns>";
        const string close = "</DataGrid.Columns>";
        var start = xaml.IndexOf(open, at, StringComparison.Ordinal);
        Assert.True(start >= 0, $"grid {gridName} has no <DataGrid.Columns>");
        start += open.Length;

        var depth = 1;
        var cursor = start;
        var end = -1;
        while (depth > 0)
        {
            var nextOpen = xaml.IndexOf(open, cursor, StringComparison.Ordinal);
            var nextClose = xaml.IndexOf(close, cursor, StringComparison.Ordinal);
            Assert.True(nextClose >= 0, $"grid {gridName} has an unterminated <DataGrid.Columns>");
            if (nextOpen >= 0 && nextOpen < nextClose)
            {
                depth++;
                cursor = nextOpen + open.Length;
            }
            else
            {
                depth--;
                end = nextClose;
                cursor = nextClose + close.Length;
            }
        }

        return TopLevelElements(xaml.Substring(start, end - start)).Select(HeaderOf).ToList();
    }

    private static List<string> TopLevelElements(string xaml)
    {
        var elements = new List<string>();
        int i = 0, depth = 0, elementStart = -1;
        while (i < xaml.Length)
        {
            if (xaml[i] != '<')
            {
                i++;
                continue;
            }

            if (string.CompareOrdinal(xaml, i, "<!--", 0, 4) == 0)
            {
                var commentEnd = xaml.IndexOf("-->", i, StringComparison.Ordinal);
                Assert.True(commentEnd >= 0, "unterminated XAML comment");
                i = commentEnd + 3;
                continue;
            }

            var tagEnd = TagEnd(xaml, i);
            if (string.CompareOrdinal(xaml, i, "</", 0, 2) == 0)
            {
                depth--;
                if (depth == 0)
                {
                    elements.Add(xaml.Substring(elementStart, tagEnd + 1 - elementStart));
                }
            }
            else
            {
                if (depth == 0)
                {
                    elementStart = i;
                }

                if (xaml[tagEnd - 1] == '/')
                {
                    if (depth == 0)
                    {
                        elements.Add(xaml.Substring(elementStart, tagEnd + 1 - elementStart));
                    }
                }
                else
                {
                    depth++;
                }
            }

            i = tagEnd + 1;
        }

        Assert.True(depth == 0, "unbalanced XAML element nesting in a DataGrid.Columns block");
        return elements;
    }

    /// <summary>Index of the '&gt;' closing the tag that starts at <paramref name="i"/>, ignoring quoted attribute values.</summary>
    private static int TagEnd(string xaml, int i)
    {
        var quote = '\0';
        for (var j = i; j < xaml.Length; j++)
        {
            var c = xaml[j];
            if (quote != '\0')
            {
                if (c == quote)
                {
                    quote = '\0';
                }
            }
            else if (c is '"' or '\'')
            {
                quote = c;
            }
            else if (c == '>')
            {
                return j;
            }
        }

        throw new InvalidOperationException("unterminated XAML tag");
    }

    private static readonly Regex HeaderPropertyElement = new(@"^<DataGrid\w*Column\.Header[\s>]");

    private static readonly Regex BoldHeaderText = new(@"<TextBlock Text=""([^""]*)""\s+FontWeight=""Bold""");

    private static readonly Regex HeaderAttribute = new(@"\bHeader=""([^""]*)""");

    /// <summary>
    /// The header a column renders: the bold TextBlock inside its Header property element (the filter-button
    /// header shape), else the Header attribute on the column's own opening tag. The Header property element
    /// is looked up among the column's DIRECT children, so a Header nested inside a CellTemplate can never
    /// stand in for the column's own — that is the one way this could return a plausible but wrong name.
    /// </summary>
    private static string HeaderOf(string element)
    {
        var header = DirectChildren(element).FirstOrDefault(c => HeaderPropertyElement.IsMatch(c));
        if (header is not null)
        {
            var text = BoldHeaderText.Match(header);
            if (text.Success)
            {
                return text.Groups[1].Value;
            }
        }

        var attribute = HeaderAttribute.Match(element[..(TagEnd(element, 0) + 1)]);
        return attribute.Success ? attribute.Groups[1].Value : "(no header)";
    }

    /// <summary>The element's immediate child elements. Empty for a self-closing element.</summary>
    private static List<string> DirectChildren(string element)
    {
        var openEnd = TagEnd(element, 0);
        if (element[openEnd - 1] == '/')
        {
            return [];
        }

        var closeStart = element.LastIndexOf("</", StringComparison.Ordinal);
        return closeStart <= openEnd
            ? []
            : TopLevelElements(element[(openEnd + 1)..closeStart]);
    }

    /* Locate the repo from this file — the DarlingLockTimeoutYieldTests idiom; no build-output copying. */
    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
