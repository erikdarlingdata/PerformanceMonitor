/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>#7643: the canonical form and hash of a table or view definition.</summary>
[Trait("Stage", "Guard")]
public class SchemaCanonicalFormTests
{
    [Fact]
    public void Hash_IsIndependentOfCatalogRowOrder()
    {
        var a = SchemaTestDefinitions.Orders();
        var b = a with
        {
            Indexes = a.Indexes.Reverse().ToList(),
            Constraints = a.Constraints.Reverse().ToList(),
            Triggers = a.Triggers.Reverse().ToList(),
            Columns = a.Columns.Reverse().ToList()
        };

        Assert.Equal(SchemaCanonicalForm.Serialize(a), SchemaCanonicalForm.Serialize(b));
        Assert.Equal(SchemaCanonicalForm.Hash(a), SchemaCanonicalForm.Hash(b));
    }

    [Fact]
    public void Hash_IgnoresObjectIdModifyDateAndName()
    {
        var a = SchemaTestDefinitions.Orders();
        var b = a with
        {
            ObjectId = 999,
            ModifyDate = new DateTime(2030, 1, 1),
            Schema = "sales",
            Name = "OrdersRenamed",
            Indexes = a.Indexes.Select(i => i with { IndexId = i.IndexId + 7 }).ToList()
        };

        Assert.Equal(SchemaCanonicalForm.Hash(a), SchemaCanonicalForm.Hash(b));
    }

    /// <summary>
    /// A BULK INSERT or bcp run without CHECK_CONSTRAINTS flips a constraint to not trusted with no DDL. Were the flag in the hash, the next
    /// unrelated full read would write a false altered version, credited to whoever caused that read.
    /// </summary>
    [Fact]
    public void Hash_IgnoresWhetherAConstraintIsTrusted()
    {
        var a = SchemaTestDefinitions.Orders();
        var b = a with { Constraints = a.Constraints.Select(k => k with { IsNotTrusted = !k.IsNotTrusted }).ToList() };

        Assert.Equal(SchemaCanonicalForm.Hash(a), SchemaCanonicalForm.Hash(b));
    }

    private static SchemaColumn Change(string field, SchemaColumn c) => field switch
    {
        "TypeName" => c with { TypeName = "bigint" },
        "MaxLength" => c with { MaxLength = 100 },
        "Precision" => c with { Precision = 18 },
        "Scale" => c with { Scale = 4 },
        "IsNullable" => c with { IsNullable = !c.IsNullable },
        "IdentitySeed" => c with { IdentitySeed = 100 },
        "IdentityIncrement" => c with { IdentityIncrement = 5 },
        "ComputedDefinition" => c with { ComputedDefinition = "([Id]*(2))" },
        "IsPersisted" => c with { IsPersisted = true },
        "DefaultName" => c with { DefaultName = "DF_Other" },
        "DefaultDefinition" => c with { DefaultDefinition = "((1))" },
        "CollationName" => c with { CollationName = "Latin1_General_BIN2" },
        "IsSparse" => c with { IsSparse = true },
        "IsRowGuidCol" => c with { IsRowGuidCol = true },
        "Name" => c with { Name = "Renamed" },
        "Ordinal" => c with { Ordinal = 9 },
        _ => throw new ArgumentOutOfRangeException(nameof(field), field, null)
    };

    [Theory]
    [InlineData("TypeName")]
    [InlineData("MaxLength")]
    [InlineData("Precision")]
    [InlineData("Scale")]
    [InlineData("IsNullable")]
    [InlineData("IdentitySeed")]
    [InlineData("IdentityIncrement")]
    [InlineData("ComputedDefinition")]
    [InlineData("IsPersisted")]
    [InlineData("DefaultName")]
    [InlineData("DefaultDefinition")]
    [InlineData("CollationName")]
    [InlineData("IsSparse")]
    [InlineData("IsRowGuidCol")]
    [InlineData("Name")]
    [InlineData("Ordinal")]
    public void Hash_ChangesWhenAColumnTypeLengthNullabilityDefaultOrCollationChanges(string field)
    {
        var a = SchemaTestDefinitions.Orders();
        var columns = a.Columns.ToList();
        columns[1] = Change(field, columns[1]);
        var b = a with { Columns = columns };

        Assert.NotEqual(SchemaCanonicalForm.Hash(a), SchemaCanonicalForm.Hash(b));
    }

    [Fact]
    public void Hash_ChangesWhenKeyColumnOrderChanges_ButNotIncludedColumnOrder()
    {
        var a = SchemaTestDefinitions.Orders();
        var ix = a.Indexes.Single(i => i.Name == "IX_Orders_Customer");

        var includedReordered = a with
        {
            Indexes = a.Indexes.Select(i => i == ix ? i with { IncludedColumns = ["Total", "Note"] } : i).ToList()
        };
        var keyReordered = a with
        {
            Indexes = a.Indexes.Select(i => i == ix ? i with { KeyColumns = ["CreatedAt DESC", "CustomerId"] } : i).ToList()
        };

        Assert.Equal(SchemaCanonicalForm.Hash(a), SchemaCanonicalForm.Hash(includedReordered));
        Assert.NotEqual(SchemaCanonicalForm.Hash(a), SchemaCanonicalForm.Hash(keyReordered));
    }

    [Fact]
    public void Hash_ChangesWhenAViewTextOrATriggerOrAConstraintNameChanges()
    {
        var a = SchemaTestDefinitions.Orders();
        var view = SchemaTestDefinitions.OrdersView();

        Assert.NotEqual(SchemaCanonicalForm.Hash(view), SchemaCanonicalForm.Hash(view with { View = view.View! with { ViewDefinition = "select 2" } }));
        Assert.NotEqual(SchemaCanonicalForm.Hash(a), SchemaCanonicalForm.Hash(a with { Triggers = [a.Triggers[0] with { Events = "DELETE" }, a.Triggers[1]] }));
        Assert.NotEqual(SchemaCanonicalForm.Hash(a), SchemaCanonicalForm.Hash(a with { Constraints = a.Constraints.Select(k => k.Kind == 'C' ? k with { Name = "CK_Other" } : k).ToList() }));
    }

    [Fact]
    public void Serialize_KeepsValuesWithQuotesAndLineBreaksApart()
    {
        var a = SchemaTestDefinitions.OrdersView();
        var b = a with { View = a.View! with { ViewDefinition = "select 1\r\nunion all select \"x\"" } };
        var c = a with { View = a.View! with { ViewDefinition = "select 1\nunion all select \"x\"" } };

        var text = SchemaCanonicalForm.Serialize(b);
        Assert.Equal(3, text.Count(ch => ch == '\n'));
        Assert.NotEqual(SchemaCanonicalForm.Hash(b), SchemaCanonicalForm.Hash(c));
    }
}

internal static class SchemaTestDefinitions
{
    public static SchemaObjectDefinition Orders() => new(
        ObjectId: 101,
        Schema: "dbo",
        Name: "Orders",
        Type: 'U',
        ModifyDate: new DateTime(2026, 3, 1),
        Table: new SchemaTableProperties(null, null, false, null, null, null, "TABLE"),
        View: null,
        Columns:
        [
            new SchemaColumn(1, "Id", "int", 4, 10, 0, false, 1, 1, null, null, null, null, null, false, false),
            new SchemaColumn(2, "CustomerId", "int", 4, 10, 0, false, null, null, null, null, "DF_Orders_Customer", "((0))", null, false, false),
            new SchemaColumn(3, "Note", "nvarchar", 200, 0, 0, true, null, null, null, null, null, null, "SQL_Latin1_General_CP1_CI_AS", false, false),
            new SchemaColumn(4, "Total", "decimal", 9, 18, 2, false, null, null, null, null, null, null, null, false, false),
            new SchemaColumn(5, "CreatedAt", "datetime2", 8, 27, 7, false, null, null, null, null, null, null, null, false, false)
        ],
        Indexes:
        [
            new SchemaIndex(1, "PK_Orders", "CLUSTERED", true, true, false, ["Id"], [], null, "NONE", false),
            new SchemaIndex(2, "IX_Orders_Customer", "NONCLUSTERED", false, false, false, ["CustomerId", "CreatedAt"], ["Note", "Total"], "([Total]>(0))", "PAGE", false)
        ],
        Constraints:
        [
            new SchemaConstraint('P', "PK_Orders", null, [], null, null, [], null, null, false, false, false),
            new SchemaConstraint('F', "FK_Orders_Customers", null, ["CustomerId"], "dbo", "Customers", ["Id"], "NO_ACTION", "NO_ACTION", false, false, false),
            new SchemaConstraint('C', "CK_Orders_Total", "([Total]>=(0))", [], null, null, [], null, null, false, false, false)
        ],
        Triggers:
        [
            new SchemaTrigger("TR_Orders_Audit", false, "INSERT,UPDATE"),
            new SchemaTrigger("TR_Orders_Guard", true, "DELETE")
        ]);

    public static SchemaObjectDefinition OrdersView() => new(
        ObjectId: 202,
        Schema: "dbo",
        Name: "OrdersView",
        Type: 'V',
        ModifyDate: new DateTime(2026, 3, 2),
        Table: null,
        View: new SchemaViewProperties(false, false, "select 1"),
        Columns: [new SchemaColumn(1, "Id", "int", 4, 10, 0, false, null, null, null, null, null, null, null, null, null)],
        Indexes: [],
        Constraints: [],
        Triggers: []);
}
