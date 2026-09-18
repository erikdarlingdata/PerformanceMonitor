/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3576, the viewer's half. The FinOps Locking &amp; Contention grid binds <see cref="IndexLockingRow.FullName"/>
/// (<c>schema.table</c>) instead of the bare table name, so two tables sharing a name across schemas no longer
/// read as one. The viewer's row model is a hand-kept copy of Lite's, so the property is pinned here against
/// the copy the viewer actually compiles; the grid XAML of both SKUs is pinned from Lite.Tests
/// (<c>IndexLockingGridQualifiedNameTests</c>), which reads both files from source.
/// </summary>
public sealed class ViewerIndexLockingRowFullNameTests
{
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
    public void ColumnFilter_OnFullName_ActuallyFilters()
    {
        /* The popup filter resolves its button's Tag to a row property by reflection; a Tag naming a property
           the row lacks makes MatchesFilter return true for every row — a filter that filters nothing. */
        Assert.NotNull(typeof(IndexLockingRow).GetProperty("FullName"));

        var filter = new ColumnFilterState { ColumnName = "FullName", Operator = FilterOperator.Contains, Value = "archive." };

        Assert.True(ColumnFilterMatcher.MatchesFilter(new IndexLockingRow { SchemaName = "archive", TableName = "Orders" }, filter));
        Assert.False(ColumnFilterMatcher.MatchesFilter(new IndexLockingRow { SchemaName = "dbo", TableName = "Orders" }, filter));
    }
}
