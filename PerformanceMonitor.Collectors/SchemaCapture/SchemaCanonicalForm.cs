/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// #7643: the canonical text of a table or view definition, and its SHA-256. The text is line based with a fixed order, so the same
/// definition always gives the same bytes whatever order the catalog returned its rows in: columns by ordinal, indexes by name,
/// constraints by kind then name, triggers by name; key columns stay in key order, included columns are sorted. Names of columns,
/// indexes, constraints and triggers are part of it. The object's own schema and name, its object_id and its modify_date are not,
/// and neither is an index's index_id or a trigger's enabled state.
/// </summary>
public static class SchemaCanonicalForm
{
    public static string Serialize(SchemaObjectDefinition d)
    {
        ArgumentNullException.ThrowIfNull(d);
        var sb = new StringBuilder();
        sb.Append("type ").Append(d.Type).Append('\n');

        if (d.Table is { } t)
        {
            Line(sb, "table", t.TemporalType, t.HistoryTable, Flag(t.IsMemoryOptimized), t.Durability, t.PartitionScheme, t.PartitionColumn, t.LockEscalation);
        }

        if (d.View is { } v)
        {
            Line(sb, "view", Flag(v.IsSchemaBound), Flag(v.IsIndexed), v.ViewDefinition);
        }

        foreach (var c in d.Columns.OrderBy(c => c.Ordinal))
        {
            Line(sb, "column", Num(c.Ordinal), c.Name, c.TypeName, Num(c.MaxLength), Num(c.Precision), Num(c.Scale), Flag(c.IsNullable),
                Num(c.IdentitySeed), Num(c.IdentityIncrement), c.ComputedDefinition, Flag(c.IsPersisted), c.DefaultName, c.DefaultDefinition,
                c.CollationName, Flag(c.IsSparse), Flag(c.IsRowGuidCol));
        }

        foreach (var i in d.Indexes.OrderBy(i => i.Name ?? string.Empty, StringComparer.Ordinal).ThenBy(i => i.TypeDesc, StringComparer.Ordinal))
        {
            Line(sb, "index", i.Name, i.TypeDesc, Flag(i.IsUnique), Flag(i.IsPrimaryKey), Flag(i.IsUniqueConstraint), List(i.KeyColumns),
                List(i.IncludedColumns.OrderBy(x => x, StringComparer.Ordinal)), i.FilterDefinition, i.DataCompression, Flag(i.IsDisabled));
        }

        foreach (var k in d.Constraints.OrderBy(k => k.Kind).ThenBy(k => k.Name, StringComparer.Ordinal))
        {
            Line(sb, "constraint", k.Kind.ToString(), k.Name, k.Definition, List(k.FkColumns), k.ReferencedSchema, k.ReferencedTable,
                List(k.ReferencedColumns), k.DeleteAction, k.UpdateAction, Flag(k.IsDisabled), Flag(k.IsNotTrusted), Flag(k.IsSystemNamed));
        }

        foreach (var g in d.Triggers.OrderBy(g => g.Name, StringComparer.Ordinal))
        {
            Line(sb, "trigger", g.Name, Flag(g.IsInsteadOf), g.Events);
        }

        return sb.ToString();
    }

    public static byte[] Hash(SchemaObjectDefinition d)
    {
        return SHA256.HashData(Encoding.UTF8.GetBytes(Serialize(d)));
    }

    /* A field is either ~ (null) or a quoted string with backslash, quote, CR and LF escaped, so no value can read as another field
       or as a line break. */
    private static void Line(StringBuilder sb, string tag, params string?[] fields)
    {
        sb.Append(tag);
        foreach (var f in fields)
        {
            sb.Append(' ');
            if (f == null)
            {
                sb.Append('~');
                continue;
            }

            sb.Append('"');
            foreach (var ch in f)
            {
                switch (ch)
                {
                    case '\\': sb.Append("\\\\"); break;
                    case '"': sb.Append("\\\""); break;
                    case '\n': sb.Append("\\n"); break;
                    case '\r': sb.Append("\\r"); break;
                    default: sb.Append(ch); break;
                }
            }

            sb.Append('"');
        }

        sb.Append('\n');
    }

    private static string? Flag(bool? value) => value == null ? null : value.Value ? "1" : "0";

    private static string? Num(int? value) => value?.ToString(CultureInfo.InvariantCulture);

    private static string? Num(decimal? value) => value?.ToString(CultureInfo.InvariantCulture);

    private static string List(IEnumerable<string> items) =>
        string.Join(",", items.Select(i => i.Replace("\\", "\\\\", StringComparison.Ordinal).Replace(",", "\\,", StringComparison.Ordinal)));
}
