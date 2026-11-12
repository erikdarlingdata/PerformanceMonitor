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

namespace PerformanceMonitor.Collectors;

/// <summary>A column, index or constraint present on both sides, with the names of the fields that differ.</summary>
public sealed record SchemaChangedItem(string Name, IReadOnlyList<string> Fields);

/// <summary>
/// #7643: what differs between two versions of an object, by name. A trigger whose events or INSTEAD OF flag changed is reported as
/// removed and added. <see cref="PropertiesChanged"/> names the table or view properties that differ, except the view text, which is
/// <see cref="ViewTextChanged"/>. With no <c>before</c>, everything in <c>after</c> counts as added.
/// </summary>
public sealed class SchemaObjectDiff
{
    public IReadOnlyList<string> ColumnsAdded { get; init; } = [];
    public IReadOnlyList<string> ColumnsRemoved { get; init; } = [];
    public IReadOnlyList<SchemaChangedItem> ColumnsChanged { get; init; } = [];
    public IReadOnlyList<string> IndexesAdded { get; init; } = [];
    public IReadOnlyList<string> IndexesRemoved { get; init; } = [];
    public IReadOnlyList<SchemaChangedItem> IndexesChanged { get; init; } = [];
    public IReadOnlyList<string> ConstraintsAdded { get; init; } = [];
    public IReadOnlyList<string> ConstraintsRemoved { get; init; } = [];
    public IReadOnlyList<SchemaChangedItem> ConstraintsChanged { get; init; } = [];
    public IReadOnlyList<string> TriggersAdded { get; init; } = [];
    public IReadOnlyList<string> TriggersRemoved { get; init; } = [];
    public bool ViewTextChanged { get; init; }
    public (string From, string To)? Renamed { get; init; }
    public IReadOnlyList<string> PropertiesChanged { get; init; } = [];

    public static SchemaObjectDiff Compare(SchemaObjectDefinition? before, SchemaObjectDefinition after)
    {
        ArgumentNullException.ThrowIfNull(after);
        var b = before ?? new SchemaObjectDefinition(after.ObjectId, after.Schema, after.Name, after.Type, null, null, null, [], [], [], []);

        var (colAdded, colRemoved, colChanged) = Match(b.Columns, after.Columns, c => c.Name, ColumnFields);
        var (ixAdded, ixRemoved, ixChanged) = Match(b.Indexes, after.Indexes, i => i.Name ?? string.Empty, IndexFields);
        var (ckAdded, ckRemoved, ckChanged) = Match(b.Constraints, after.Constraints, k => k.Name, ConstraintFields);
        var (trAdded, trRemoved, trChanged) = Match(b.Triggers, after.Triggers, g => g.Name, TriggerFields);

        var oldView = before?.View;
        var newView = after.View;
        var props = new List<string>();
        Field(props, "TemporalType", b.Table?.TemporalType, after.Table?.TemporalType);
        Field(props, "HistoryTable", b.Table?.HistoryTable, after.Table?.HistoryTable);
        Field(props, "IsMemoryOptimized", b.Table?.IsMemoryOptimized, after.Table?.IsMemoryOptimized);
        Field(props, "Durability", b.Table?.Durability, after.Table?.Durability);
        Field(props, "PartitionScheme", b.Table?.PartitionScheme, after.Table?.PartitionScheme);
        Field(props, "PartitionColumn", b.Table?.PartitionColumn, after.Table?.PartitionColumn);
        Field(props, "LockEscalation", b.Table?.LockEscalation, after.Table?.LockEscalation);
        Field(props, "IsSchemaBound", oldView?.IsSchemaBound, newView?.IsSchemaBound);
        Field(props, "IsIndexed", oldView?.IsIndexed, newView?.IsIndexed);

        (string From, string To)? renamed = null;
        if (before != null && (before.Schema != after.Schema || before.Name != after.Name))
        {
            renamed = (before.Schema + "." + before.Name, after.Schema + "." + after.Name);
        }

        return new SchemaObjectDiff
        {
            ColumnsAdded = colAdded,
            ColumnsRemoved = colRemoved,
            ColumnsChanged = colChanged,
            IndexesAdded = ixAdded,
            IndexesRemoved = ixRemoved,
            IndexesChanged = ixChanged,
            ConstraintsAdded = ckAdded,
            ConstraintsRemoved = ckRemoved,
            ConstraintsChanged = ckChanged,
            TriggersAdded = trAdded.Concat(trChanged.Select(c => c.Name)).OrderBy(n => n, StringComparer.Ordinal).ToList(),
            TriggersRemoved = trRemoved.Concat(trChanged.Select(c => c.Name)).OrderBy(n => n, StringComparer.Ordinal).ToList(),
            ViewTextChanged = !string.Equals(oldView?.ViewDefinition, newView?.ViewDefinition, StringComparison.Ordinal),
            Renamed = renamed,
            PropertiesChanged = props
        };
    }

    private static (List<string> Added, List<string> Removed, List<SchemaChangedItem> Changed) Match<T>(
        IReadOnlyList<T> before, IReadOnlyList<T> after, Func<T, string> key, Func<T, T, List<string>> fields)
    {
        var old = before.ToDictionary(key, StringComparer.Ordinal);
        var now = after.ToDictionary(key, StringComparer.Ordinal);
        var added = now.Keys.Where(k => !old.ContainsKey(k)).OrderBy(k => k, StringComparer.Ordinal).ToList();
        var removed = old.Keys.Where(k => !now.ContainsKey(k)).OrderBy(k => k, StringComparer.Ordinal).ToList();
        var changed = new List<SchemaChangedItem>();
        foreach (var k in now.Keys.Where(old.ContainsKey).OrderBy(k => k, StringComparer.Ordinal))
        {
            var f = fields(old[k], now[k]);
            if (f.Count > 0)
            {
                changed.Add(new SchemaChangedItem(k, f));
            }
        }

        return (added, removed, changed);
    }

    private static void Field<T>(List<string> into, string name, T? a, T? b)
    {
        if (!EqualityComparer<T?>.Default.Equals(a, b))
        {
            into.Add(name);
        }
    }

    private static bool Same(IEnumerable<string> a, IEnumerable<string> b) => a.SequenceEqual(b, StringComparer.Ordinal);

    private static List<string> ColumnFields(SchemaColumn a, SchemaColumn b)
    {
        var f = new List<string>();
        Field(f, "Ordinal", a.Ordinal, b.Ordinal);
        Field(f, "TypeName", a.TypeName, b.TypeName);
        Field(f, "MaxLength", a.MaxLength, b.MaxLength);
        Field(f, "Precision", a.Precision, b.Precision);
        Field(f, "Scale", a.Scale, b.Scale);
        Field(f, "IsNullable", a.IsNullable, b.IsNullable);
        Field(f, "IdentitySeed", a.IdentitySeed, b.IdentitySeed);
        Field(f, "IdentityIncrement", a.IdentityIncrement, b.IdentityIncrement);
        Field(f, "ComputedDefinition", a.ComputedDefinition, b.ComputedDefinition);
        Field(f, "IsPersisted", a.IsPersisted, b.IsPersisted);
        Field(f, "DefaultName", a.DefaultName, b.DefaultName);
        Field(f, "DefaultDefinition", a.DefaultDefinition, b.DefaultDefinition);
        Field(f, "CollationName", a.CollationName, b.CollationName);
        Field(f, "IsSparse", a.IsSparse, b.IsSparse);
        Field(f, "IsRowGuidCol", a.IsRowGuidCol, b.IsRowGuidCol);
        return f;
    }

    private static List<string> IndexFields(SchemaIndex a, SchemaIndex b)
    {
        var f = new List<string>();
        Field(f, "TypeDesc", a.TypeDesc, b.TypeDesc);
        Field(f, "IsUnique", a.IsUnique, b.IsUnique);
        Field(f, "IsPrimaryKey", a.IsPrimaryKey, b.IsPrimaryKey);
        Field(f, "IsUniqueConstraint", a.IsUniqueConstraint, b.IsUniqueConstraint);
        if (!Same(a.KeyColumns, b.KeyColumns))
        {
            f.Add("KeyColumns");
        }

        if (!Same(a.IncludedColumns.OrderBy(x => x, StringComparer.Ordinal), b.IncludedColumns.OrderBy(x => x, StringComparer.Ordinal)))
        {
            f.Add("IncludedColumns");
        }

        Field(f, "FilterDefinition", a.FilterDefinition, b.FilterDefinition);
        Field(f, "DataCompression", a.DataCompression, b.DataCompression);
        Field(f, "IsDisabled", a.IsDisabled, b.IsDisabled);
        return f;
    }

    private static List<string> ConstraintFields(SchemaConstraint a, SchemaConstraint b)
    {
        var f = new List<string>();
        Field(f, "Kind", a.Kind, b.Kind);
        Field(f, "Definition", a.Definition, b.Definition);
        if (!Same(a.FkColumns, b.FkColumns))
        {
            f.Add("FkColumns");
        }

        Field(f, "ReferencedSchema", a.ReferencedSchema, b.ReferencedSchema);
        Field(f, "ReferencedTable", a.ReferencedTable, b.ReferencedTable);
        if (!Same(a.ReferencedColumns, b.ReferencedColumns))
        {
            f.Add("ReferencedColumns");
        }

        Field(f, "DeleteAction", a.DeleteAction, b.DeleteAction);
        Field(f, "UpdateAction", a.UpdateAction, b.UpdateAction);
        Field(f, "IsDisabled", a.IsDisabled, b.IsDisabled);
        Field(f, "IsNotTrusted", a.IsNotTrusted, b.IsNotTrusted);
        Field(f, "IsSystemNamed", a.IsSystemNamed, b.IsSystemNamed);
        return f;
    }

    private static List<string> TriggerFields(SchemaTrigger a, SchemaTrigger b)
    {
        var f = new List<string>();
        Field(f, "IsInsteadOf", a.IsInsteadOf, b.IsInsteadOf);
        Field(f, "Events", a.Events, b.Events);
        return f;
    }
}
