/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>#7643: what differs between two versions of an object.</summary>
[Trait("Stage", "Guard")]
public class SchemaObjectDiffTests
{
    [Fact]
    public void Diff_ReportsAddedRemovedAndChangedColumnsByName()
    {
        var before = SchemaTestDefinitions.Orders();
        var columns = before.Columns.Where(c => c.Name != "Note").ToList();
        columns[1] = columns[1] with { TypeName = "bigint", IsNullable = true };
        columns.Add(new SchemaColumn(6, "Status", "tinyint", 1, 3, 0, false, null, null, null, null, null, null, null, false, false));
        var after = before with { Columns = columns };

        var diff = SchemaObjectDiff.Compare(before, after);

        Assert.Equal(["Status"], diff.ColumnsAdded);
        Assert.Equal(["Note"], diff.ColumnsRemoved);
        var changed = Assert.Single(diff.ColumnsChanged);
        Assert.Equal("CustomerId", changed.Name);
        Assert.Equal(["TypeName", "IsNullable"], changed.Fields);
        Assert.Null(diff.Renamed);
        Assert.False(diff.ViewTextChanged);
    }

    [Fact]
    public void Diff_ReportsIndexConstraintAndTriggerChanges()
    {
        var before = SchemaTestDefinitions.Orders();
        var after = before with
        {
            Indexes = before.Indexes.Where(i => i.Name != "PK_Orders").Select(i => i with { KeyColumns = ["CustomerId"] })
                .Append(new SchemaIndex(3, "IX_Orders_New", "NONCLUSTERED", false, false, false, ["Total"], [], null, null, false)).ToList(),
            Constraints = before.Constraints.Where(k => k.Kind != 'C').Select(k => k.Kind == 'F' ? k with { DeleteAction = "CASCADE" } : k).ToList(),
            Triggers = [before.Triggers[0] with { Events = "INSERT" }]
        };

        var diff = SchemaObjectDiff.Compare(before, after);

        Assert.Equal(["IX_Orders_New"], diff.IndexesAdded);
        Assert.Equal(["PK_Orders"], diff.IndexesRemoved);
        Assert.Equal(["KeyColumns"], Assert.Single(diff.IndexesChanged).Fields);
        Assert.Equal(["CK_Orders_Total"], diff.ConstraintsRemoved);
        Assert.Equal(["DeleteAction"], Assert.Single(diff.ConstraintsChanged).Fields);
        Assert.Equal(["TR_Orders_Audit"], diff.TriggersAdded);
        Assert.Equal(["TR_Orders_Audit", "TR_Orders_Guard"], diff.TriggersRemoved);
    }

    [Fact]
    public void Diff_ReportsARenameAndChangedProperties()
    {
        var before = SchemaTestDefinitions.Orders();
        var after = before with { Name = "OrdersNew", Table = before.Table! with { LockEscalation = "DISABLE" } };

        var diff = SchemaObjectDiff.Compare(before, after);

        Assert.Equal(("dbo.Orders", "dbo.OrdersNew"), diff.Renamed);
        Assert.Equal(["LockEscalation"], diff.PropertiesChanged);
    }

    [Fact]
    public void Diff_ViewTextChange_IsFlagged()
    {
        var before = SchemaTestDefinitions.OrdersView();
        var after = before with { View = before.View! with { ViewDefinition = "select 2" } };

        Assert.True(SchemaObjectDiff.Compare(before, after).ViewTextChanged);
        Assert.False(SchemaObjectDiff.Compare(before, before).ViewTextChanged);
    }

    [Fact]
    public void Diff_WithNoBefore_ReportsEverythingAsAdded()
    {
        var after = SchemaTestDefinitions.Orders();

        var diff = SchemaObjectDiff.Compare(null, after);

        Assert.Equal(5, diff.ColumnsAdded.Count);
        Assert.Equal(2, diff.IndexesAdded.Count);
        Assert.Equal(3, diff.ConstraintsAdded.Count);
        Assert.Equal(2, diff.TriggersAdded.Count);
        Assert.Null(diff.Renamed);
    }
}
