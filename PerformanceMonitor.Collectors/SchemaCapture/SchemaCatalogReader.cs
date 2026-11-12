/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// #7643: what one catalog read of a database found. <see cref="HasViewDefinition"/> is false when the login lacks VIEW DEFINITION, so
/// definitions are missing and the object list is only what the login can see: the read is incomplete. <see cref="OverCap"/> is true when
/// the database holds more objects than the read returns; <see cref="Objects"/> is then the first ones in object_id order.
/// </summary>
public sealed record SchemaCatalogSnapshot(bool HasViewDefinition, bool OverCap, IReadOnlyList<SchemaObjectDefinition> Objects);

/// <summary>#7643: turns the six result sets of <see cref="SchemaCatalogQueries"/> into <see cref="SchemaObjectDefinition"/> records.</summary>
public static class SchemaCatalogReader
{
    public static async Task<SchemaCatalogSnapshot> ReadAsync(DbDataReader reader, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(reader);

        var hasViewDefinition = false;
        var objectCount = 0;
        if (await reader.ReadAsync(ct))
        {
            hasViewDefinition = reader.GetBoolean(0);
            objectCount = reader.GetInt32(1);
        }

        await reader.NextResultAsync(ct);
        var heads = new List<ObjectHead>();
        while (await reader.ReadAsync(ct))
        {
            heads.Add(new ObjectHead(
                reader.GetInt32(0), reader.GetString(1), reader.GetString(2), reader.GetString(3).Trim()[0],
                reader.IsDBNull(4) ? null : reader.GetDateTime(4),
                new SchemaTableProperties(Str(reader, 5), Str(reader, 6), Bool(reader, 7), Str(reader, 8), Str(reader, 9), Str(reader, 10), Str(reader, 11)),
                new SchemaViewProperties(Bool(reader, 12), Bool(reader, 13), Str(reader, 14))));
        }

        await reader.NextResultAsync(ct);
        var columns = new Dictionary<int, List<SchemaColumn>>();
        while (await reader.ReadAsync(ct))
        {
            Add(columns, reader.GetInt32(0), new SchemaColumn(
                reader.GetInt32(1), reader.GetString(2), reader.GetString(3), Int(reader, 4), Int(reader, 5), Int(reader, 6), reader.GetBoolean(7),
                Scale0(reader, 8), Scale0(reader, 9), Str(reader, 10), Bool(reader, 11), Str(reader, 12), Str(reader, 13), Str(reader, 14),
                Bool(reader, 15), Bool(reader, 16)));
        }

        await reader.NextResultAsync(ct);
        var indexes = new Dictionary<int, List<SchemaIndex>>();
        while (await reader.ReadAsync(ct))
        {
            Add(indexes, reader.GetInt32(0), new SchemaIndex(
                reader.GetInt32(1), Str(reader, 2), reader.GetString(3), reader.GetBoolean(4), reader.GetBoolean(5), reader.GetBoolean(6),
                SplitList(Str(reader, 7)), SplitList(Str(reader, 8)), Str(reader, 9), Str(reader, 10), reader.GetBoolean(11)));
        }

        await reader.NextResultAsync(ct);
        var constraints = new Dictionary<int, List<SchemaConstraint>>();
        while (await reader.ReadAsync(ct))
        {
            Add(constraints, reader.GetInt32(0), new SchemaConstraint(
                reader.GetString(1)[0], reader.GetString(2), Str(reader, 3), SplitList(Str(reader, 4)), Str(reader, 5), Str(reader, 6),
                SplitList(Str(reader, 7)), Str(reader, 8), Str(reader, 9), reader.GetBoolean(10), reader.GetBoolean(11), reader.GetBoolean(12)));
        }

        await reader.NextResultAsync(ct);
        var triggers = new Dictionary<int, List<SchemaTrigger>>();
        while (await reader.ReadAsync(ct))
        {
            Add(triggers, reader.GetInt32(0), new SchemaTrigger(reader.GetString(1), reader.GetBoolean(2), Str(reader, 3) ?? string.Empty));
        }

        var objects = heads.Select(h => new SchemaObjectDefinition(
            h.ObjectId, h.Schema, h.Name, h.Type, h.ModifyDate,
            h.Type == 'U' ? h.Table : null,
            h.Type == 'V' ? h.View : null,
            Get(columns, h.ObjectId), Get(indexes, h.ObjectId), Get(constraints, h.ObjectId), Get(triggers, h.ObjectId))).ToList();

        return new SchemaCatalogSnapshot(hasViewDefinition, objectCount > objects.Count, objects);
    }

    private sealed record ObjectHead(
        int ObjectId, string Schema, string Name, char Type, DateTime? ModifyDate, SchemaTableProperties Table, SchemaViewProperties View);

    private static void Add<T>(Dictionary<int, List<T>> map, int objectId, T item)
    {
        if (!map.TryGetValue(objectId, out var list))
        {
            list = [];
            map[objectId] = list;
        }

        list.Add(item);
    }

    private static List<T> Get<T>(Dictionary<int, List<T>> map, int objectId) => map.TryGetValue(objectId, out var list) ? list : [];

    private static string? Str(DbDataReader r, int i) => r.IsDBNull(i) ? null : r.GetString(i);

    private static int? Int(DbDataReader r, int i) => r.IsDBNull(i) ? null : r.GetInt32(i);

    private static bool? Bool(DbDataReader r, int i) => r.IsDBNull(i) ? null : r.GetBoolean(i);

    /* The catalog hands identity seed and increment over as decimal(38, 0); a .NET decimal read from it already has scale 0, but a value
       that arrives as 1.0 would hash differently from 1, so it is rebuilt without a scale whatever the driver returned. */
    private static decimal? Scale0(DbDataReader r, int i)
    {
        if (r.IsDBNull(i))
        {
            return null;
        }

        var value = decimal.Truncate(r.GetDecimal(i));
        return value >= long.MinValue && value <= long.MaxValue ? new decimal((long)value) : value;
    }

    /* The inverse of the catalog query's escaping: a comma separates names, a backslash escapes a backslash or a comma. */
    public static IReadOnlyList<string> SplitList(string? joined)
    {
        if (string.IsNullOrEmpty(joined))
        {
            return [];
        }

        var items = new List<string>();
        var sb = new StringBuilder();
        for (var i = 0; i < joined.Length; i++)
        {
            var ch = joined[i];
            if (ch == '\\' && i + 1 < joined.Length)
            {
                sb.Append(joined[++i]);
            }
            else if (ch == ',')
            {
                items.Add(sb.ToString());
                sb.Clear();
            }
            else
            {
                sb.Append(ch);
            }
        }

        items.Add(sb.ToString());
        return items;
    }

    /* The exact inverse of SplitList: a backslash and a comma are escaped with a backslash, the names are joined by commas, and a list
       with no names is null. The store writes its column lists with this, and its readers split them with SplitList. */
    public static string? JoinList(IReadOnlyList<string> items)
    {
        ArgumentNullException.ThrowIfNull(items);
        return items.Count == 0
            ? null
            : string.Join(",", items.Select(i => i.Replace("\\", "\\\\", StringComparison.Ordinal).Replace(",", "\\,",StringComparison.Ordinal)));
    }
}
