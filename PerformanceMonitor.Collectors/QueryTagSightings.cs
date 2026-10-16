/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Common.QueryTags;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The one way every collector describes the query keys and caller fields a row saw (#5980, part of #5971).
/// A collector's <c>DescribeQueryTagSightings</c> builds its sightings here so the rules that must be the same
/// everywhere are written once: a NULL or empty key yields no sighting, a text sample is cut to
/// <see cref="QueryTagLimits.TextSampleCap"/>, and a caller key with no application, login or host yields
/// nothing. It reads values and returns records; it opens no connection and writes nothing.
/// </summary>
public static class QueryTagSightings
{
    /* Separates the parts of a caller key. A control character no application, login or host name carries, so two
       different callers can never build the same key text. */
    private const char CallerKeySeparator = '\u001F';

    /// <summary>A hash sighting for <paramref name="queryHash"/>, or null when the hash is NULL or empty.</summary>
    public static QueryTagSighting? Hash(
        string? queryHash,
        string? database,
        QueryTagObject? objectRef,
        string? application,
        string? login,
        string? host,
        string? textSample)
        => string.IsNullOrEmpty(queryHash)
            ? null
            : new QueryTagSighting(
                QueryTagKeyKind.Hash,
                queryHash,
                database,
                objectRef,
                application,
                login,
                host,
                Cut(textSample));

    /// <summary>An object sighting for the database, schema and name, or null when the name is NULL or empty.</summary>
    public static QueryTagSighting? ObjectKey(string? database, string? schema, string? name)
    {
        if (string.IsNullOrEmpty(name))
        {
            return null;
        }

        var objectRef = new QueryTagObject(database, schema, name);
        return new QueryTagSighting(QueryTagKeyKind.Object, objectRef.Qualified, database, objectRef);
    }

    /// <summary>
    /// A caller sighting for a row that carries no query_hash, or null when it names no application, login or
    /// host (a database alone is not a caller).
    /// </summary>
    public static QueryTagSighting? Caller(
        string? database,
        string? application,
        string? login,
        string? host,
        string? textSample)
    {
        if (string.IsNullOrEmpty(application) && string.IsNullOrEmpty(login) && string.IsNullOrEmpty(host))
        {
            return null;
        }

        string key = string.Join(
            CallerKeySeparator.ToString(),
            application ?? string.Empty,
            login ?? string.Empty,
            host ?? string.Empty,
            database ?? string.Empty);
        return new QueryTagSighting(QueryTagKeyKind.Caller, key, database, null, application, login, host, Cut(textSample));
    }

    /// <summary>The sightings that are not null, as one list, or null when there are none.</summary>
    public static IReadOnlyList<QueryTagSighting>? Collect(params QueryTagSighting?[] sightings)
    {
        List<QueryTagSighting>? list = null;
        foreach (var s in sightings)
        {
            if (s is not null)
            {
                (list ??= new List<QueryTagSighting>(sightings.Length)).Add(s);
            }
        }

        return list;
    }

    /// <summary>
    /// Cuts <paramref name="text"/> to the shared cap. A cut that would split a surrogate pair stops one
    /// character earlier, so the sample is always valid text for the store.
    /// </summary>
    public static string? Cut(string? text)
    {
        if (text is null || text.Length <= QueryTagLimits.TextSampleCap)
        {
            return text;
        }

        int length = QueryTagLimits.TextSampleCap;
        if (char.IsHighSurrogate(text[length - 1]))
        {
            length--;
        }

        return text.Substring(0, length);
    }

    /// <summary>
    /// A module (object_name) that carries no schema, as long-query completions report it: the database and the name.
    /// </summary>
    public static QueryTagObject? NamedObject(string? database, string? name)
        => string.IsNullOrEmpty(name) ? null : new QueryTagObject(database, null, name);

    /// <summary>
    /// Splits Query Store's <c>module_name</c> (<c>schema.name</c>; <c>Adhoc</c> for no module; <c>Unknown</c>
    /// for a module whose name could not be resolved) into an object, or null for the two placeholders.
    /// </summary>
    public static QueryTagObject? ModuleObject(string? database, string? moduleName)
    {
        if (string.IsNullOrEmpty(moduleName)
            || moduleName == "Adhoc"
            || moduleName == "Unknown")
        {
            return null;
        }

        int dot = moduleName.IndexOf('.', StringComparison.Ordinal);
        return dot > 0 && dot < moduleName.Length - 1
            ? new QueryTagObject(database, moduleName.Substring(0, dot), moduleName.Substring(dot + 1))
            : new QueryTagObject(database, null, moduleName);
    }
}
