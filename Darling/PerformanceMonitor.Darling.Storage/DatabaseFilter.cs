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

    /// <summary>#5633 review L2 leftover: names a read must NOT cover, set only by <see cref="Excluding"/> on an <see cref="All"/>
    /// filter. Null when nothing is excluded. Only the readers that carry <see cref="ExcludeClause(string, int)"/> apply it.</summary>
    private readonly string[]? _excluded;

    private DatabaseFilter(string[] names) => _names = names;

    private DatabaseFilter(string[]? names, string[]? excluded)
    {
        _names = names;
        _excluded = excluded;
    }

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

    /// <summary>
    /// #5633 review: every database EXCEPT <paramref name="excluded"/> (blank names dropped, repeats kept once). It is still
    /// <see cref="IsAll"/> (no database is chosen), so a caller that does not apply the exclusion reads every database as before.
    /// A list with no name left is plain <see cref="All"/>. The top-procedures ranking and the Query Store regressions read
    /// (the MCP tool and the Viewer grid, #6654) apply it (see <see cref="ExcludeClause(string, int)"/>); <c>CurrentDatabaseScope.NarrowAsync</c> is
    /// its one producer.
    /// </summary>
    public static DatabaseFilter Excluding(IEnumerable<string?>? excluded)
    {
        var kept = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in excluded ?? Array.Empty<string?>())
        {
            if (!string.IsNullOrWhiteSpace(name) && seen.Add(name))
            {
                kept.Add(name);
            }
        }

        return kept.Count == 0 ? default : new DatabaseFilter(null, kept.ToArray());
    }

    /// <summary>The names a read leaves out (empty when none): see <see cref="Excluding"/>.</summary>
    public IReadOnlyList<string> ExcludedNames => _excluded is null ? Array.Empty<string>() : Array.AsReadOnly(_excluded);

    /// <summary>
    /// The exclusion's <c>text[]</c> parameter for a statement carrying <see cref="ExcludeClause(string, int)"/>: SQL NULL when nothing is
    /// excluded. A new parameter on every call.
    /// </summary>
    public NpgsqlParameter ExcludeParameter() =>
        new()
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text,
            Value = _excluded is null ? DBNull.Value : _excluded.Clone(),
        };

    /// <summary>
    /// The always-present exclusion predicate, bound at <paramref name="index"/> by <see cref="ExcludeParameter"/>. NULL-safe on the
    /// column: a row with no database name is not a dropped database, so it stays. Independent of the selection, like
    /// <see cref="Clause"/>.
    /// </summary>
    public static string ExcludeClause(string column, int index) => ExcludeClause(column, "$" + index.ToString(System.Globalization.CultureInfo.InvariantCulture));

    /// <summary>
    /// <see cref="ExcludeClause(string, int)"/> with the parameter written as <paramref name="parameter"/> (a <c>$n</c> token or a
    /// placeholder a shared statement binds later, such as <c>QueryStoreRegressionBaseline.ExcludeToken</c>): the one definition of the
    /// predicate, so a NULL-handling change reaches every statement that carries it (#6654 review).
    /// </summary>
    public static string ExcludeClause(string column, string parameter) =>
        $" AND ({parameter}::text[] IS NULL OR {column} IS NULL OR {column} <> ALL({parameter}))";

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
        if (!SameSet(_excluded, other._excluded))
        {
            return false;
        }

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

    private static bool SameSet(string[]? left, string[]? right)
    {
        if (left is null || right is null)
        {
            return left is null && right is null;
        }

        return left.Length == right.Length && left.All(name => Array.IndexOf(right, name) >= 0);
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

        if (_excluded is not null)
        {
            foreach (var name in _excluded)
            {
                hash += 31 * StringComparer.Ordinal.GetHashCode(name);
            }
        }

        return hash;
    }
}
