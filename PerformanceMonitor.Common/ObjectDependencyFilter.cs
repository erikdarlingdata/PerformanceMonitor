/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 *
 * The filters are ported from SQLExtended's DependencyClassifier (MIT, Copyright (c) 2026 James Noonan).
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Common;

/// <summary>One row of <c>sys.sql_expression_dependencies</c> as the <c>object_dependencies</c> collector stores it (#8784 part 2).</summary>
/// <param name="ReferencingSchema">The schema of the object whose definition holds the reference.</param>
/// <param name="ReferencingName">Its name.</param>
/// <param name="ReferencingType">Its <c>sys.objects.type</c>.</param>
/// <param name="ReferencedServer">The server part as written, or null.</param>
/// <param name="ReferencedDatabase">The database part as written, or null.</param>
/// <param name="ReferencedSchema">The bound object's schema where SQL Server bound the name, otherwise the schema as written, or null.</param>
/// <param name="ReferencedName">The bound object's name, otherwise the name as written.</param>
/// <param name="IsBound">True where <c>referenced_id</c> is not null.</param>
/// <param name="IsCallerDependent">The catalog's <c>is_caller_dependent</c>.</param>
/// <param name="IsAmbiguous">The catalog's <c>is_ambiguous</c>.</param>
public sealed record RawObjectDependency(
    string ReferencingSchema,
    string ReferencingName,
    string ReferencingType,
    string? ReferencedServer,
    string? ReferencedDatabase,
    string? ReferencedSchema,
    string ReferencedName,
    bool IsBound,
    bool IsCallerDependent,
    bool IsAmbiguous);

/// <summary>
/// Turns the rows of <c>sys.sql_expression_dependencies</c> into the edges the view usage read walks (#8784 part 2), leaving out the rows
/// that are never a reference to a user object in the same database:
/// <list type="bullet">
/// <item>a reference on a linked server or into another database (the read is per database; the other database reads it as an inbound edge
/// through <see cref="ToInboundEdge"/>);</item>
/// <item>the bogus multi-part names that <c>xmlcol.value('...')</c> and the other XML type methods (<c>value</c>, <c>exist</c>,
/// <c>query</c>, <c>nodes</c>, <c>modify</c>) leave, where the "database" part is really a table alias;</item>
/// <item>references into <c>sys</c> and <c>INFORMATION_SCHEMA</c>;</item>
/// <item><c>inserted</c> and <c>deleted</c> inside a trigger.</item>
/// </list>
///
/// <para><b>Caller-dependent and ambiguous rows are kept</b>, unlike a broken-reference check, which skips them because it cannot tell
/// whether they resolve. Here a kept row can only make a view look used, and a dropped one could make a used view look unused, so where the
/// catalog cannot say, the edge stays and is matched by name.</para>
/// </summary>
public static class ObjectDependencyFilter
{
    private static readonly HashSet<string> TypeMethodNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "value", "exist", "query", "nodes", "modify",
    };

    private static readonly HashSet<string> TriggerPseudoTables = new(StringComparer.OrdinalIgnoreCase)
    {
        "inserted", "deleted",
    };

    /// <summary>The edge for <paramref name="row"/>, or null where the row is never a reference to a user object in
    /// <paramref name="databaseName"/>.</summary>
    public static ObjectDependency? ToEdge(RawObjectDependency row, string databaseName)
    {
        ArgumentNullException.ThrowIfNull(row);

        if (string.IsNullOrEmpty(row.ReferencedName))
        {
            return null;
        }

        var hasServer = !string.IsNullOrEmpty(row.ReferencedServer);
        var hasDatabase = !string.IsNullOrEmpty(row.ReferencedDatabase);

        /* xmlcol.value('...') is stored as alias.column.value: an outer part that is really an alias, and a type method as the name. */
        if ((hasServer || hasDatabase) && TypeMethodNames.Contains(row.ReferencedName))
        {
            return null;
        }

        if (hasServer)
        {
            return null;
        }

        if (hasDatabase && !string.Equals(row.ReferencedDatabase, databaseName, StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        if (string.Equals(row.ReferencedSchema, "sys", StringComparison.OrdinalIgnoreCase)
            || string.Equals(row.ReferencedSchema, "INFORMATION_SCHEMA", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        if (!row.IsBound
            && string.IsNullOrEmpty(row.ReferencedSchema)
            && string.Equals(row.ReferencingType?.Trim(), "TR", StringComparison.OrdinalIgnoreCase)
            && TriggerPseudoTables.Contains(row.ReferencedName))
        {
            return null;
        }

        return new ObjectDependency(
            row.ReferencingSchema,
            row.ReferencingName,
            row.ReferencingType?.Trim() ?? "",
            row.ReferencedSchema ?? "",
            row.ReferencedName,
            row.IsBound);
    }

    /// <summary>
    /// The inbound edge for a row of <paramref name="sourceDatabase"/>'s list that names an object of <paramref name="targetDatabase"/> by a
    /// three-part name (#8784 review: <c>A.dbo.vReport</c> selecting from <c>B.dbo.vCustomer</c>), or null where it does not: no database
    /// part, another database, a linked server, an XML type method artifact, or <c>sys</c> and <c>INFORMATION_SCHEMA</c>. The edge carries
    /// <paramref name="sourceDatabase"/> as its <see cref="ObjectDependency.ReferencingDatabase"/>.
    /// </summary>
    public static ObjectDependency? ToInboundEdge(RawObjectDependency row, string sourceDatabase, string targetDatabase)
    {
        ArgumentNullException.ThrowIfNull(row);

        if (string.IsNullOrEmpty(row.ReferencedName)
            || !string.IsNullOrEmpty(row.ReferencedServer)
            || string.IsNullOrEmpty(row.ReferencedDatabase)
            || !string.Equals(row.ReferencedDatabase, targetDatabase, StringComparison.OrdinalIgnoreCase)
            || string.Equals(sourceDatabase, targetDatabase, StringComparison.OrdinalIgnoreCase)
            || TypeMethodNames.Contains(row.ReferencedName)
            || string.Equals(row.ReferencedSchema, "sys", StringComparison.OrdinalIgnoreCase)
            || string.Equals(row.ReferencedSchema, "INFORMATION_SCHEMA", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        return new ObjectDependency(
            row.ReferencingSchema,
            row.ReferencingName,
            row.ReferencingType?.Trim() ?? "",
            row.ReferencedSchema ?? "",
            row.ReferencedName,
            row.IsBound,
            sourceDatabase);
    }

    /// <summary>The edges for <paramref name="rows"/>, the rows <see cref="ToEdge"/> leaves out dropped.</summary>
    public static IReadOnlyList<ObjectDependency> ToEdges(IEnumerable<RawObjectDependency> rows, string databaseName)
    {
        ArgumentNullException.ThrowIfNull(rows);

        var edges = new List<ObjectDependency>();
        foreach (var row in rows)
        {
            if (ToEdge(row, databaseName) is { } edge)
            {
                edges.Add(edge);
            }
        }

        return edges;
    }
}
