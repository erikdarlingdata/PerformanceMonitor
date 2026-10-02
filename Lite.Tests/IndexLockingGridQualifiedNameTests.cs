/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3576: the Locking &amp; Contention grid showed the bare table name, so two tables that share a name across
/// schemas — <c>dbo.Orders</c> and <c>archive.Orders</c>, the everyday shape on Azure SQL DB where per-tenant or
/// per-environment schemas are the norm — read as one object. The store always carried <c>schema_name</c>
/// (the index drill's Identity panel shows it); only the grid dropped it. The fix is a computed
/// <see cref="IndexLockingRow.FullName"/> the column binds, sorts, filters, and exports as one string.
///
/// <para>Two things are pinned beyond the string itself. First, the popup column filter resolves its button's
/// <c>Tag</c> to a row property BY REFLECTION (<see cref="ColumnFilterMatcher.MatchesFilter"/>), and a Tag that
/// names a property the row does not expose returns <c>true</c> for every row — the filter silently does nothing.
/// So the Tag must move with the binding, and this proves the moved Tag actually filters. Second, both apps carry
/// a hand-duplicated copy of this grid (no shared XAML across SKUs), so the XAML pin reads both files, the same
/// drift-guard shape as <see cref="AvailabilityGroupsGridSortTests"/>.</para>
/// </summary>
public sealed class IndexLockingGridQualifiedNameTests
{
    /* ---------------- the row property ---------------- */

    [Fact]
    public void FullName_IsSchemaDotTable_WhenSchemaIsPresent()
    {
        var row = new IndexLockingRow { SchemaName = "archive", TableName = "Orders" };

        Assert.Equal("archive.Orders", row.FullName);
    }

    [Fact]
    public void FullName_FallsBackToBareTable_WhenSchemaIsEmpty()
    {
        /* The reader writes "" (never null) for a NULL schema_name, so "" is the real fallback input. */
        var row = new IndexLockingRow { SchemaName = "", TableName = "Orders" };

        Assert.Equal("Orders", row.FullName);
    }

    [Fact]
    public void FullName_DistinguishesTheSameTableNameAcrossSchemas()
    {
        /* The reported defect in one assertion: these two rows used to render identically. */
        var dbo = new IndexLockingRow { SchemaName = "dbo", TableName = "Orders" };
        var archive = new IndexLockingRow { SchemaName = "archive", TableName = "Orders" };

        Assert.Equal(dbo.TableName, archive.TableName);
        Assert.NotEqual(dbo.FullName, archive.FullName);
    }

    /* ---------------- the filter mechanism ---------------- */

    [Fact]
    public void ColumnFilter_OnFullName_ActuallyFilters()
    {
        /* The Tag -> property hop the popup filter makes. A Tag naming a property the row lacks makes
           MatchesFilter return true for every row, which is a filter that filters nothing. */
        Assert.NotNull(typeof(IndexLockingRow).GetProperty("FullName"));

        var filter = new ColumnFilterState { ColumnName = "FullName", Operator = FilterOperator.Contains, Value = "archive." };
        var dbo = new IndexLockingRow { SchemaName = "dbo", TableName = "Orders" };
        var archive = new IndexLockingRow { SchemaName = "archive", TableName = "Orders" };

        Assert.True(filter.IsActive);
        Assert.True(ColumnFilterMatcher.MatchesFilter(archive, filter));
        Assert.False(ColumnFilterMatcher.MatchesFilter(dbo, filter));
    }

    /* ---------------- the XAML, both SKUs ---------------- */

    private const string LiteGrid = "Lite/Controls/FinOpsTab.xaml";
    private const string DarlingGrid = "Darling/PerformanceMonitor.Darling.Viewer/FinOpsTab.xaml";

    [Theory]
    [InlineData(LiteGrid, "IndexLockingDataGrid")]
    [InlineData(DarlingGrid, "FinOpsIndexLockingDataGrid")]
    public void LockingGrid_TableColumn_BindsAndFiltersOnFullName(string gridPath, string gridName)
    {
        var grid = LockingGridXaml(gridPath, gridName);

        /* The column and its filter button name the same property, and it is the qualified one. */
        Assert.Contains("Binding=\"{Binding FullName}\"", grid);
        Assert.Contains("Tag=\"FullName\"", grid);

        /* The regression's fingerprint: the bare name must not come back as either the binding or the Tag. */
        Assert.DoesNotContain("Binding=\"{Binding TableName}\"", grid);
        Assert.DoesNotContain("Tag=\"TableName\"", grid);
    }

    [Theory]
    [InlineData(LiteGrid, "IndexLockingDataGrid")]
    [InlineData(DarlingGrid, "FinOpsIndexLockingDataGrid")]
    public void LockingGrid_EveryFilterButton_TagsThePropertyItsColumnBinds(string gridPath, string gridName)
    {
        var grid = LockingGridXaml(gridPath, gridName);

        /* Each filterable text column: a simple {Binding X} followed by a filter button whose Tag must be X.
           A Tag that drifts from its binding filters a property the user cannot see (or none at all). The
           middle is fenced at the column's own closing tag so a filter-less column cannot borrow its
           neighbor's Tag and report a false mismatch. */
        var columns = Regex.Matches(
            grid,
            @"<DataGridTextColumn Binding=""\{Binding (\w+)\}""(?:(?!</DataGridTextColumn>).)*?Tag=""(\w+)""(?:(?!</DataGridTextColumn>).)*?</DataGridTextColumn>",
            RegexOptions.Singleline);

        var mismatches = new List<string>();
        foreach (Match column in columns)
        {
            var bound = column.Groups[1].Value;
            var tagged = column.Groups[2].Value;
            if (bound != tagged)
            {
                mismatches.Add($"binds {bound} but its filter Tag is {tagged}");
            }
        }

        /* Positive control: Database, Table, Index all carry filter buttons. A regex that finds nothing
           would otherwise pass vacuously — see the negative-census lesson behind ParitySource. */
        Assert.True(columns.Count >= 3,
            $"{gridPath}/{gridName}: expected at least 3 filterable text columns, found {columns.Count} — the column shape changed or the parser is dead.");
        Assert.True(mismatches.Count == 0,
            $"{gridPath}/{gridName}: filter Tag drifted from its column binding: {string.Join("; ", mismatches)}");
    }

    /// <summary>The one <c>&lt;DataGrid x:Name="..."&gt;</c> element (through its closing tag) out of the sub-tab's XAML.</summary>
    private static string LockingGridXaml(string gridPath, string gridName)
    {
        var xaml = ParitySource.ReadFile(gridPath);
        var start = xaml.IndexOf($"x:Name=\"{gridName}\"", System.StringComparison.Ordinal);
        Assert.True(start >= 0, $"{gridPath}: no element named {gridName} — the Locking grid was renamed or removed.");

        var end = xaml.IndexOf("</DataGrid>", start, System.StringComparison.Ordinal);
        Assert.True(end > start, $"{gridPath}: {gridName} has no closing </DataGrid>.");

        return xaml.Substring(start, end - start);
    }
}
