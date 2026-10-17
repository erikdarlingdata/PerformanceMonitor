/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common.QueryTags;

/// <summary>
/// The kind of key a query tag is attached to (#5971, #5979). A hash key is a query_hash; an object key is
/// a database, schema and name; a caller key is the application, login, host and database of a row that
/// carries no query_hash (blocking and deadlock rows).
/// </summary>
public enum QueryTagKeyKind
{
    Hash = 0,
#pragma warning disable CA1720 // the stored value name is "object"
    Object = 1,
#pragma warning restore CA1720
    Caller = 2,
}

/// <summary>A database, schema and object name. Equality helpers ignore case, like the rest of tag matching.</summary>
public sealed record QueryTagObject(string? Database, string? Schema, string? Name)
{
    /// <summary>
    /// The form an <c>object</c> rule pattern is matched against: <c>database.schema.name</c>, a missing
    /// part written as an empty string. Fully qualified so one rule cannot hit the same name in two databases
    /// by accident; a rule that does not care about the database starts with <c>*.</c>.
    /// </summary>
    public string Qualified => (Database ?? string.Empty) + "." + (Schema ?? string.Empty) + "." + (Name ?? string.Empty);

    public bool SameObjectAs(QueryTagObject? other) =>
        other is not null
        && string.Equals(Database ?? string.Empty, other.Database ?? string.Empty, StringComparison.OrdinalIgnoreCase)
        && string.Equals(Schema ?? string.Empty, other.Schema ?? string.Empty, StringComparison.OrdinalIgnoreCase)
        && string.Equals(Name ?? string.Empty, other.Name ?? string.Empty, StringComparison.OrdinalIgnoreCase);
}

/// <summary>
/// What one collected row saw, for one key: the key plus the caller fields and a sample of the query text.
/// A plain value with no store references. The collectors (#5980) fill it, and the map builders (#5985
/// Darling, #5990 Lite) hand every sighting of one key on one server to <see cref="QueryTagResolver"/>.
/// </summary>
/// <param name="KeyKind">Which kind of key this sighting is for.</param>
/// <param name="Key">The key itself: the query_hash text for a hash key, the qualified object name for an
/// object key, or a caller key's own text.</param>
/// <param name="Database">The database the row ran in, if known.</param>
/// <param name="ObjectRef">The object (procedure, function, trigger) the row ran under, if any.</param>
/// <param name="Application">The client application name, if known.</param>
/// <param name="Login">The login name, if known.</param>
/// <param name="Host">The client host name, if known.</param>
/// <param name="TextSample">The query text, or the start of it. The resolver reads at most
/// <see cref="QueryTagLimits.TextSampleCap"/> characters of it, so a collector need not cut it first.</param>
public sealed record QueryTagSighting(
    QueryTagKeyKind KeyKind,
    string Key,
    string? Database = null,
    QueryTagObject? ObjectRef = null,
    string? Application = null,
    string? Login = null,
    string? Host = null,
    string? TextSample = null);

/// <summary>The limits both stores share, so Lite and Darling cut and cap the same way (#5979).</summary>
public static class QueryTagLimits
{
    /// <summary>Text rules see at most this many characters of the query text.</summary>
    public const int TextSampleCap = 4000;

    /// <summary>
    /// Cuts <paramref name="text"/> to <see cref="TextSampleCap"/>. A cut that would split a surrogate pair stops
    /// one character earlier, so the sample is always valid text for the store. The one cut: the resolver and the
    /// collectors both call it (#5980, #3684).
    /// </summary>
    public static string? CutText(string? text)
    {
        if (text is null || text.Length <= TextSampleCap)
        {
            return text;
        }

        int length = TextSampleCap;
        if (char.IsHighSurrogate(text[length - 1]))
        {
            length--;
        }

        return text.Substring(0, length);
    }

    /// <summary>The most keys one pass resolves for one server. The collectors (#5980) enforce it; this type only
    /// names the value, so the resolver and the matcher never cut a pass short themselves.</summary>
    public const int MaxKeysPerServerPerPass = 10000;

    /// <summary>The longest rule pattern, in characters.</summary>
    public const int MaxPatternLength = 400;
}
