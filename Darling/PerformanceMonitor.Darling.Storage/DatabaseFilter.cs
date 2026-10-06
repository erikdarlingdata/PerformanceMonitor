/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #5245: which databases a database-scoped read covers. A set of names, and an empty set means every database,
/// so <c>default</c> is "all".
///
/// <para><b>One rule for a blank name.</b> A null, empty or whitespace-only name means "no name", so
/// <see cref="One"/> of such a value is <see cref="All"/>, and <see cref="Of"/> drops such values from a list
/// (a list with nothing else left is <see cref="All"/>). Every other name is kept exactly as given: it is never
/// trimmed and never case-folded, because the SQL arm compares it with <c>=</c> and a database called
/// <c> SalesDb</c> (leading space) is not <c>SalesDb</c>. A repeated name is kept once, in the order first seen.
/// The MCP tools wrap their one <c>database_name</c> in <see cref="One"/>, the web route wraps its repeated keys
/// in <see cref="Of"/>, and both reach the same SQL shape.</para>
///
/// <para><b>One SQL shape.</b> A reader's statement carries <see cref="Clause"/> with one <c>$n</c>, and always
/// binds <see cref="Parameter"/> at that index, so the statement text never changes with the selection: an
/// empty selection binds SQL NULL and the guard short-circuits to unfiltered. The names travel as ONE
/// <c>text[]</c> parameter and are never spliced into SQL text, so a name with a comma, a quote or a bracket in
/// it is still one value. <see cref="Clause"/> and <see cref="Parameter"/> are byte-for-byte the desktop viewer's
/// <c>DatabaseFilterClause</c> and <c>DatabaseFilterParameter</c> (a test keeps them equal).</para>
///
/// <para>The caps on how many names a web request may carry (50, 128 characters each, 4,096 encoded bytes) are
/// the web route's, not this type's: the route refuses a request over them before it builds a filter.</para>
/// </summary>
public readonly record struct DatabaseFilter
{
    /// <summary>What <see cref="Describe"/> says for a selection of two or more databases.</summary>
    public const string ManyDatabasesDescription = "the chosen databases";

    /// <summary>The kept names: null for "all", otherwise at least one, distinct, never blank.</summary>
    private readonly string[]? _names;

    private DatabaseFilter(string[] names) => _names = names;

    /// <summary>Every database (the same value as <c>default</c>).</summary>
    public static DatabaseFilter All => default;

    /// <summary>One database by name; a null, empty or whitespace-only name is <see cref="All"/>.</summary>
    public static DatabaseFilter One(string? name) =>
        string.IsNullOrWhiteSpace(name) ? default : new DatabaseFilter([name]);

    /// <summary>
    /// The listed databases: blank names are dropped, repeats are kept once (ordinal, first seen first), and a
    /// list with no name left, or a null list, is <see cref="All"/>.
    /// </summary>
    public static DatabaseFilter Of(IEnumerable<string?>? names)
    {
        if (names is null)
        {
            return default;
        }

        var kept = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in names)
        {
            if (!string.IsNullOrWhiteSpace(name) && seen.Add(name))
            {
                kept.Add(name);
            }
        }

        return kept.Count == 0 ? default : new DatabaseFilter(kept.ToArray());
    }

    /// <summary>True when no database is named, so the read covers every database.</summary>
    public bool IsAll => _names is null;

    /// <summary>The kept names, in the order they were given; empty for <see cref="All"/>.</summary>
    public IReadOnlyList<string> Names => _names is null ? Array.Empty<string>() : Array.AsReadOnly(_names);

    /// <summary>
    /// The one <c>text[]</c> parameter a statement carrying <see cref="Clause"/> binds: SQL NULL for
    /// <see cref="All"/>, the names otherwise. A new parameter on every call, because a parameter belongs to
    /// one command.
    /// </summary>
    public NpgsqlParameter Parameter() =>
        new()
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text,
            Value = _names is null ? DBNull.Value : _names.Clone(),
        };

    /// <summary>
    /// The always-present predicate, spliced into a WHERE clause. <paramref name="column"/> is the (optionally
    /// qualified) database-name column and <paramref name="index"/> the positional index
    /// <see cref="Parameter"/> is bound at. It does not depend on the selection, which is the point.
    /// </summary>
    public string Clause(string column, int index) =>
        $" AND (${index}::text[] IS NULL OR {column} = ANY(${index}))";

    /// <summary>
    /// How an envelope or a notice names the selection: null for <see cref="All"/>, the name itself for one
    /// database, and <see cref="ManyDatabasesDescription"/> for two or more.
    /// </summary>
    public string? Describe() =>
        _names switch
        {
            null => null,
            { Length: 1 } => _names[0],
            _ => ManyDatabasesDescription,
        };

    /// <summary>Two filters are equal when they name the same set of databases, whatever the order.</summary>
    public bool Equals(DatabaseFilter other)
    {
        if (_names is null || other._names is null)
        {
            return _names is null && other._names is null;
        }

        if (_names.Length != other._names.Length)
        {
            return false;
        }

        // The names are distinct, so the same length with every name present is the same set.
        foreach (var name in _names)
        {
            if (Array.IndexOf(other._names, name) < 0)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>The hash of the set: it does not depend on the order, as <see cref="Equals(DatabaseFilter)"/> does not.</summary>
    public override int GetHashCode()
    {
        var hash = 0;
        if (_names is not null)
        {
            foreach (var name in _names)
            {
                hash += StringComparer.Ordinal.GetHashCode(name);
            }
        }

        return hash;
    }
}
