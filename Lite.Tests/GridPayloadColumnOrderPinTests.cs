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
using System.Text.RegularExpressions;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #1949 — pins the DISPLAY position of the query text and query plan columns (Lite side; Darling.Tests
/// carries the mirror-image pin). On a query surface the text and plan columns are the payload, so they
/// belong immediately right of the collection/execution time column that orients the row — not behind a
/// dozen numeric columns. They drifted to the back over years precisely because nothing asserted display
/// order: the eight existing <c>Pins*ColumnOrder</c> tests all assert the collector WRITE payload
/// (<c>Definition.PayloadColumns</c>) into DuckDB, never a <c>DataGrid</c>. Worst case before the fix was
/// <c>QueryStoreGrid</c>, where the query text was column 55 of 55.
///
/// The pin is the table below, transcribed from the #1949 census. For each grid it records the time (or
/// identity) ANCHOR and the payload columns that must follow it immediately. Adding a grid to this table
/// whose payload columns still sit at the back FAILS — that is the point. Moving a payload column back, or
/// wedging a metric column between the anchor and the payload, also fails and names the grid.
///
/// XAML <c>Columns</c> order IS display order in both apps: there is no <c>DisplayIndex</c> assignment, no
/// <c>Columns.Move</c>, and no index-based <c>Columns[i]</c> access anywhere. The one runtime column call is
/// <c>WaitDrillDownWindow.InsertChainColumns</c>, which does <c>Columns.Insert(0, ...)</c> — it prepends,
/// so it shifts every XAML index by +1 without disturbing this relative ordering.
///
/// Text-scans SOURCE XAML located from this file's compile-time path — no WPF or assembly load, exactly
/// like the other parity pins. Headers are read verbatim, so a header carrying an XML entity would have to
/// be written into the table in its raw <c>&amp;#x0394;</c> form; no pinned header has one today.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class GridPayloadColumnOrderPinTests
{
    /// <summary>A grid whose payload columns are pinned: where the anchor sits, and what must follow it.</summary>
    private sealed record GridPin(
        string RelativePath,
        string GridName,
        string AnchorHeader,
        int AnchorIndex,
        string[] PayloadColumns);

    private static readonly string LiteServerTab = Path.Combine("Lite", "Controls", "ServerTab.xaml");
    private static readonly string LiteFinOpsTab = Path.Combine("Lite", "Controls", "FinOpsTab.xaml");
    private static readonly string LiteWaitDrillDown = Path.Combine("Lite", "Windows", "WaitDrillDownWindow.xaml");
    private static readonly string LiteProcedureHistory = Path.Combine("Lite", "Windows", "ProcedureHistoryWindow.xaml");
    private static readonly string LiteQueryStatsHistory = Path.Combine("Lite", "Windows", "QueryStatsHistoryWindow.xaml");
    private static readonly string LiteQueryStoreHistory = Path.Combine("Lite", "Windows", "QueryStoreHistoryWindow.xaml");

    /* The #1949 move list for Lite. Comparison and FinOps grids carry no time column, so their anchor is the
       identity block the grid is ranked by (Database, or Score+Database on High Impact). QueryStoreHistory
       anchors after index 4 because its time columns are non-contiguous — Plan ID and Exec Type interleave —
       and the whole time block stays intact. */
    private static readonly GridPin[] Pins =
    [
        new(LiteServerTab, "QuerySnapshotsGrid", "Collected", 1, ["Query Text", "Query Plan"]),
        new(LiteServerTab, "QueryStatsGrid", "Creation Time", 3, ["Query Text", "Query Plan"]),
        new(LiteServerTab, "QueryStatsComparisonGrid", "Database", 1, ["Query Text"]),
        new(LiteServerTab, "ProcedureStatsGrid", "Cached Time", 4, ["Query Plan"]),
        new(LiteServerTab, "QueryStoreGrid", "First Execution", 5, ["Query Text", "Query Plan"]),
        new(LiteServerTab, "QueryStoreComparisonGrid", "Database", 1, ["Query Text"]),
        new(LiteServerTab, "PlanCorrectionGrid", "Collected", 0, ["Query Text"]),
        new(LiteServerTab, "BlockedProcessReportGrid", "Event Time", 0, ["Blocked SQL", "Blocking SQL", "XML"]),
        new(LiteServerTab, "DeadlockGrid", "Time", 0, ["SQL Text", "XML"]),
        new(LiteServerTab, "SignificantWaitsGrid", "Event Time", 0, ["Query Text"]),
        new(LiteServerTab, "LongQueryCompletionsGrid", "Event Time", 0, ["Statement"]),
        new(LiteWaitDrillDown, "ResultsDataGrid", "Collected", 1, ["Query Text"]),
        new(LiteProcedureHistory, "HistoryDataGrid", "Cached Time", 2, ["Plan"]),
        new(LiteQueryStatsHistory, "HistoryDataGrid", "Creation Time", 2, ["Plan"]),
        new(LiteQueryStoreHistory, "HistoryDataGrid", "Last Execution", 4, ["Plan"]),
        new(LiteFinOpsTab, "ExpensiveQueriesDataGrid", "Database", 0, ["Query Preview"]),
        new(LiteFinOpsTab, "HighImpactDataGrid", "Database", 1, ["Query Preview"]),
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
        // census counted 457 columns across 16 Lite grids, and #1952's 27-column PlanCorrectionGrid brings
        // that to 484 across 17; the floor is deliberately below that so adding a column never fails this,
        // but a broken scan does. It moved 440 -> 467 with that grid — up by exactly the 27 columns it adds,
        // which preserves the guard's existing slack rather than widening or narrowing it.
        var total = Pins.Sum(p => ColumnHeaders(ReadRepoFile(p.RelativePath), p.GridName).Count);
        Assert.True(total >= 467, $"only {total} columns walked across {Pins.Length} grids — the scan is broken.");
    }

    [Fact]
    public void NoGridDroppedOutOfTheTable()
    {
        // The anti-deletion half of the ratchet: quietly deleting a row would make this suite green by
        // covering less, which is the one way a pin can rot without anything going red.
        Assert.True(Pins.Length == 17,
            $"the pin table holds {Pins.Length} grids, expected the 16 Lite grids on the #1949 move list " +
            "plus #1952's PlanCorrectionGrid. " +
            "Removing a grid needs a stated reason (the grid or its payload column is gone); adding one is " +
            "free, but bump this number in the same commit.");
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
