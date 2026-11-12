/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;

namespace PerformanceMonitor.Common.QueryTags;

/// <summary>A query-tag key as the MCP tools parse it: trimmed, with the unused parts null. <c>Kind</c> is "hash" or "object".</summary>
public sealed record QueryTagKeyParts(string Kind, int? ServerId, string? Hash, string? Database, string? Schema, string? Name);

/// <summary>Why a key's arguments were refused: the machine code and the sentence for the caller. Every refusal has status "invalid".</summary>
public sealed record QueryTagKeyProblem(string Code, string Message);

/// <summary>
/// The one parse-and-validate of a query-tag key's arguments (#7224), shared by Lite's and Darling's MCP query-tag tools so
/// the two products cannot drift on what a key may hold. Each host wraps the problem in its own refusal envelope, as it does
/// for <see cref="QueryTagRuleValidator"/>.
/// </summary>
public static class QueryTagKeyParser
{
    public const int MaxKeyPartLength = 256;

    private const string KindHash = "hash";
    private const string KindObject = "object";

    /// <summary>
    /// Validates and trims a key's arguments. Null plus <paramref name="problem"/> when they cannot be used.
    /// <paramref name="requireHashShape"/> (#6455) makes a NEW hash assignment name something a query hash can be; unassign
    /// leaves it off so a key stored before the check can still be removed.
    /// </summary>
    public static QueryTagKeyParts? TryParse(
        string? kind, string? queryHash, string? databaseName, string? schemaName, string? objectName, int? serverId,
        bool requireHashShape, out QueryTagKeyProblem? problem)
    {
        problem = null;
        var cleanKind = kind?.Trim().ToLowerInvariant();
        if (cleanKind is not (KindHash or KindObject))
        {
            problem = new QueryTagKeyProblem("bad_kind", "kind must be 'hash' or 'object'.");
            return null;
        }

        /* No sign test on serverId: it is an FNV hash cast to a signed int (ServerIdHelper), so about half of all real ids are
           negative. The caller's registry check is what refuses an id that is not a monitored server (#6020). */
        var hash = Clean(queryHash);
        var database = Clean(databaseName);
        var schema = Clean(schemaName);
        var obj = Clean(objectName);
        foreach (var (field, value) in new[] { ("query_hash", hash), ("database_name", database), ("schema_name", schema), ("object_name", obj) })
        {
            if (value is not null && (value.Length > MaxKeyPartLength || value.Any(char.IsControl)))
            {
                problem = new QueryTagKeyProblem("bad_key", string.Create(CultureInfo.InvariantCulture, $"{field} must be at most {MaxKeyPartLength} characters with no control characters."));
                return null;
            }
        }

        if (cleanKind == KindHash)
        {
            if (hash is null)
            {
                problem = new QueryTagKeyProblem("missing_key", "kind 'hash' needs query_hash.");
                return null;
            }

            if (database is not null || schema is not null || obj is not null)
            {
                problem = new QueryTagKeyProblem("bad_key", "kind 'hash' takes query_hash only; database_name, schema_name and object_name belong to kind 'object'.");
                return null;
            }

            if (requireHashShape && !QueryHashIdentity.IsAssignableHash(hash))
            {
                problem = new QueryTagKeyProblem("bad_key", "query_hash must be hexadecimal: 0x followed by 1 to 16 hex digits (for example 0x1A2B3C4D5E6F7081), or the decimal number the long-query trace stores.");
                return null;
            }

            /* The resolver matches a hash ignoring case, so the key stores one spelling of it (#6020). Unassign skips the shape
               check above, so a key stored before it (all zeros, or neither hex nor decimal) is kept as typed and can be removed. */
            return new QueryTagKeyParts(KindHash, serverId, QueryHashIdentity.CanonicalKey(hash) ?? hash, null, null, null);
        }

        var missing = new[] { ("database_name", database), ("schema_name", schema), ("object_name", obj) }.Where(p => p.Item2 is null).Select(p => p.Item1).ToList();
        if (missing.Count > 0)
        {
            problem = new QueryTagKeyProblem("missing_key", $"kind 'object' needs database_name, schema_name and object_name; missing: {string.Join(", ", missing)}.");
            return null;
        }

        if (hash is not null)
        {
            problem = new QueryTagKeyProblem("bad_key", "kind 'object' takes database_name, schema_name and object_name only; query_hash belongs to kind 'hash'.");
            return null;
        }

        return new QueryTagKeyParts(KindObject, serverId, null, database, schema, obj);
    }

    private static string? Clean(string? value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();
}
