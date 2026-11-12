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

/// <summary>#7643: what a read of an object means against its current stored version.</summary>
[Trait("Stage", "Guard")]
public class SchemaChangeClassifierTests
{
    private static StoredSchemaObject Stored(SchemaObjectDefinition d) =>
        new(1, d.ObjectId, d.Schema, d.Name, SchemaCanonicalForm.Hash(d));

    [Fact]
    public void Classify_NoCurrentAndBaselineRead_IsBaseline()
    {
        Assert.Equal(SchemaChangeKind.Baseline, SchemaChangeClassifier.Classify(null, SchemaTestDefinitions.Orders(), baselineRead: true));
    }

    [Fact]
    public void Classify_NoCurrent_IsCreated()
    {
        Assert.Equal(SchemaChangeKind.Created, SchemaChangeClassifier.Classify(null, SchemaTestDefinitions.Orders(), baselineRead: false));
    }

    [Fact]
    public void Classify_SameHashSameName_IsNone()
    {
        var d = SchemaTestDefinitions.Orders();
        Assert.Equal(SchemaChangeKind.None, SchemaChangeClassifier.Classify(Stored(d), d with { ModifyDate = null, ObjectId = 5 }, baselineRead: false));
    }

    [Fact]
    public void Classify_SameHashNewName_IsRenamed()
    {
        var d = SchemaTestDefinitions.Orders();
        Assert.Equal(SchemaChangeKind.Renamed, SchemaChangeClassifier.Classify(Stored(d), d with { Name = "OrdersNew" }, baselineRead: false));
        Assert.Equal(SchemaChangeKind.Renamed, SchemaChangeClassifier.Classify(Stored(d), d with { Schema = "sales" }, baselineRead: false));
    }

    [Fact]
    public void Classify_HashAndNameBothChanged_IsAlteredWithTheNewName()
    {
        var d = SchemaTestDefinitions.Orders();
        var read = d with { Name = "OrdersNew", Columns = d.Columns.Take(3).ToList() };
        Assert.Equal(SchemaChangeKind.Altered, SchemaChangeClassifier.Classify(Stored(d), read, baselineRead: false));
    }

    [Fact]
    public void Classify_ABaselineReadOverAnExistingVersion_IsStillComparedByHash()
    {
        var d = SchemaTestDefinitions.Orders();
        Assert.Equal(SchemaChangeKind.None, SchemaChangeClassifier.Classify(Stored(d), d, baselineRead: true));
    }
}
