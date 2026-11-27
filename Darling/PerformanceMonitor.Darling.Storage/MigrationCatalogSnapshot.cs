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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One object a migration rung created, changed or dropped (#9438). <paramref name="Column"/> is set for a column row only.</summary>
/// <param name="Schema">The schema the object lives in.</param>
/// <param name="ObjectName">The table, view, index, function or hypertable name. A function carries its identity arguments, and a policy names its hypertable and the policy.</param>
/// <param name="Column">The column, for a column row. Null for every other kind.</param>
/// <param name="Kind">One of the <see cref="MigrationObjectKind"/> words.</param>
/// <param name="Action">One of the <see cref="MigrationObjectAction"/> words.</param>
public sealed record MigrationObjectChange(string Schema, string ObjectName, string? Column, string Kind, string Action);

/// <summary>The kind words an object row carries (#9438).</summary>
public static class MigrationObjectKind
{
    public const string Table = "table";
    public const string Column = "column";
    public const string Index = "index";
    public const string View = "view";
    public const string Function = "function";
    public const string Hypertable = "hypertable";
    public const string Policy = "policy";
    public const string Constraint = "constraint";

    /// <summary>The marker row's kind: it stands for the whole ladder, not an object.</summary>
    public const string Ladder = "ladder";
}

/// <summary>The action words an object row carries (#9438).</summary>
public static class MigrationObjectAction
{
    public const string Create = "create";

    /// <summary>A column's type, nullability or default changed, an index was redefined, a constraint was redefined, a view's or function's body changed, or a policy's settings changed.</summary>
    public const string Alter = "alter";

    public const string Drop = "drop";

    /// <summary>The marker a start writes instead of object rows when its ladder began on an empty store.</summary>
    public const string FreshInstall = "fresh_install";
}

/// <summary>
/// The store's own objects at one moment, taken from the catalog (#9438), so the runner can diff the catalog before and after a rung and say what
/// the rung created, changed or dropped.
///
/// <para><b>Why a diff and not an event trigger.</b> <c>CREATE EVENT TRIGGER</c> needs a superuser, and the migration role of a managed cloud store is
/// not one. A catalog read needs no privilege.</para>
///
/// <para><b>What it covers.</b> The <c>collect</c> and <c>config</c> schemas only: tables (plain and partitioned), views and materialized views, their
/// columns with their types, nullability and defaults, indexes and constraints with their definitions, views with a hash of their body, functions and procedures with their identity arguments and a hash of their body, and,
/// when TimescaleDB is installed, hypertables and their compression and retention policies. Chunks and other TimescaleDB internals live in
/// <c>_timescaledb_*</c> schemas and are never read.</para>
///
/// <para><b>A column or index of a table the same rung created or dropped is not listed:</b> the table row says it already, and a new table would
/// otherwise list every column and index.</para>
/// </summary>
public sealed class MigrationCatalogSnapshot
{
    /// <summary>The schemas the snapshot reads.</summary>
    internal const string SchemaList = "'collect', 'config'";

    /// <summary>Whether the TimescaleDB information views exist, so the snapshot may read them.</summary>
    internal const string TimescaleProbeSql =
        "SELECT to_regclass('timescaledb_information.hypertables') IS NOT NULL AND to_regclass('timescaledb_information.jobs') IS NOT NULL";

    /// <summary>The relations, columns, indexes and functions. One row per object: kind, schema, object, parent table (columns and indexes), column, signature.
    /// <para><b>No relation lock (#9486).</b> The snapshot runs inside the rung's transaction before the rung sets its <c>lock_timeout</c>, and every lock it took would be held to the end of the rung.
    /// <c>pg_get_viewdef</c> and <c>pg_get_expr(adbin, adrelid)</c> open the view or the table with <c>AccessShareLock</c>, so a view body is compared by the hash of its stored rule tree (<c>pg_rewrite.ev_action</c>)
    /// and a column default by the hash of its stored expression tree (<c>pg_attrdef.adbin</c>): both are plain catalog columns. <c>pg_get_indexdef</c>, <c>pg_get_constraintdef</c>, <c>pg_get_function_identity_arguments</c>
    /// and <c>format_type</c> read the catalog caches only and open no relation.</para></summary>
    internal const string CoreSql = @"
SELECT 'table' AS kind, n.nspname AS schema_name, c.relname AS object_name, NULL::text AS parent_name, NULL::text AS column_name, c.relkind::text AS signature
FROM pg_class AS c
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname IN (" + SchemaList + @") AND c.relkind IN ('r', 'p') AND NOT c.relispartition
UNION ALL
SELECT 'view', n.nspname, c.relname, NULL, NULL, c.relkind::text || ':' || coalesce(md5(r.ev_action::text), '')
FROM pg_class AS c
JOIN pg_namespace AS n ON n.oid = c.relnamespace
LEFT JOIN pg_rewrite AS r ON r.ev_class = c.oid AND r.rulename = '_RETURN'
WHERE n.nspname IN (" + SchemaList + @") AND c.relkind IN ('v', 'm')
UNION ALL
SELECT 'column', n.nspname, c.relname, c.relname, a.attname::text,
       format_type(a.atttypid, a.atttypmod) || '|' || a.attnotnull::text || '|' || coalesce(md5(d.adbin::text), '')
FROM pg_attribute AS a
JOIN pg_class AS c ON c.oid = a.attrelid
LEFT JOIN pg_attrdef AS d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname IN (" + SchemaList + @") AND c.relkind IN ('r', 'p', 'v', 'm') AND NOT c.relispartition
AND a.attnum > 0 AND NOT a.attisdropped
UNION ALL
SELECT 'index', n.nspname, ci.relname, t.relname, NULL, pg_get_indexdef(ci.oid)
FROM pg_index AS i
JOIN pg_class AS ci ON ci.oid = i.indexrelid
JOIN pg_class AS t ON t.oid = i.indrelid
JOIN pg_namespace AS n ON n.oid = t.relnamespace
WHERE n.nspname IN (" + SchemaList + @") AND NOT ci.relispartition
UNION ALL
SELECT 'constraint', n.nspname, t.relname || '.' || k.conname, t.relname, NULL, k.contype::text || ':' || md5(pg_get_constraintdef(k.oid))
FROM pg_constraint AS k
JOIN pg_class AS t ON t.oid = k.conrelid
JOIN pg_namespace AS n ON n.oid = t.relnamespace
WHERE n.nspname IN (" + SchemaList + @") AND NOT t.relispartition AND k.contype <> 'n'
UNION ALL
SELECT 'function', n.nspname, p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')', NULL, NULL, md5(p.prosrc)
FROM pg_proc AS p
JOIN pg_namespace AS n ON n.oid = p.pronamespace
WHERE n.nspname IN (" + SchemaList + @") AND p.prokind IN ('f', 'p')";

    /// <summary>The hypertables and their compression and retention policies, read only when <see cref="TimescaleProbeSql"/> says the views exist.</summary>
    internal const string TimescaleSql = @"
SELECT 'hypertable' AS kind, h.hypertable_schema::text AS schema_name, h.hypertable_name::text AS object_name, NULL::text AS parent_name, NULL::text AS column_name, NULL::text AS signature
FROM timescaledb_information.hypertables AS h
WHERE h.hypertable_schema IN (" + SchemaList + @")
UNION ALL
SELECT 'policy', j.hypertable_schema::text, j.hypertable_name::text || ' ' || replace(j.proc_name::text, 'policy_', ''), NULL, NULL, j.config::text
FROM timescaledb_information.jobs AS j
WHERE j.hypertable_schema IN (" + SchemaList + @") AND j.proc_name IN ('policy_compression', 'policy_retention')";

    private readonly Dictionary<Key, Entry> _entries;

    private MigrationCatalogSnapshot(Dictionary<Key, Entry> entries) => _entries = entries;

    /// <summary>How many objects the snapshot holds.</summary>
    public int Count => _entries.Count;

    private readonly record struct Key(string Kind, string Schema, string ObjectName, string Column);

    private readonly record struct Entry(string? Parent, string? Signature);

    /// <summary>
    /// Reads the catalog inside <paramref name="transaction"/>, or returns null when the read failed. The read runs under a savepoint, so a failure
    /// rolls back to it and leaves the rung's transaction usable: a diagnostic must never fail a rung. A cancel is not swallowed.
    /// </summary>
    internal static async Task<MigrationCatalogSnapshot?> TryTakeAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, ILogger? logger, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        const string savepoint = "darling_rung_catalog_snapshot";
        await transaction.SaveAsync(savepoint, cancellationToken).ConfigureAwait(false);
        try
        {
            var entries = new Dictionary<Key, Entry>();
            await ReadAsync(connection, transaction, CoreSql, commandTimeoutSeconds, entries, cancellationToken).ConfigureAwait(false);

            bool timescale;
            using (var probe = new NpgsqlCommand(TimescaleProbeSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds })
            {
                timescale = await probe.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
            }

            if (timescale)
            {
                await ReadAsync(connection, transaction, TimescaleSql, commandTimeoutSeconds, entries, cancellationToken).ConfigureAwait(false);
            }

            await transaction.ReleaseAsync(savepoint, cancellationToken).ConfigureAwait(false);
            return new MigrationCatalogSnapshot(entries);
        }
#pragma warning disable CA1031 // A failed snapshot records nothing for this rung: the start history never fails a rung.
        catch (Exception ex) when (ex is not OperationCanceledException)
#pragma warning restore CA1031
        {
            logger?.LogDebug(ex, "The catalog could not be read around a migration rung, so the rung's objects are not recorded.");
            await transaction.RollbackAsync(savepoint, CancellationToken.None).ConfigureAwait(false);
            return null;
        }
    }

    private static async Task ReadAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, string sql, int commandTimeoutSeconds,
        Dictionary<Key, Entry> entries, CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
        using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var key = new Key(
                reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.IsDBNull(4) ? string.Empty : reader.GetString(4));
            entries[key] = new Entry(
                reader.IsDBNull(3) ? null : reader.GetString(3), reader.IsDBNull(5) ? null : reader.GetString(5));
        }
    }

    /// <summary>
    /// What changed from <paramref name="before"/> to <paramref name="after"/>: an object only in <paramref name="after"/> is created, one only in
    /// <paramref name="before"/> is dropped, and one in both whose signature differs (a column's type, nullability or default, an index's or constraint's definition, a view's or
    /// function's body hash, a policy's settings) is altered. The columns, indexes and constraints of a table or view that was created or dropped are left out. Sorted by kind, then name.
    /// </summary>
    public static IReadOnlyList<MigrationObjectChange> Diff(MigrationCatalogSnapshot before, MigrationCatalogSnapshot after)
    {
        ArgumentNullException.ThrowIfNull(before);
        ArgumentNullException.ThrowIfNull(after);

        var changes = new List<(Key Key, string Action, string? Parent)>();
        foreach (var (key, entry) in after._entries)
        {
            if (!before._entries.TryGetValue(key, out var old))
            {
                changes.Add((key, MigrationObjectAction.Create, entry.Parent));
            }
            else if (!string.Equals(old.Signature, entry.Signature, StringComparison.Ordinal))
            {
                changes.Add((key, MigrationObjectAction.Alter, entry.Parent));
            }
        }

        foreach (var (key, entry) in before._entries)
        {
            if (!after._entries.ContainsKey(key))
            {
                changes.Add((key, MigrationObjectAction.Drop, entry.Parent));
            }
        }

        var wholeRelations = changes
            .Where(c => c.Action != MigrationObjectAction.Alter && (c.Key.Kind == MigrationObjectKind.Table || c.Key.Kind == MigrationObjectKind.View))
            .Select(c => (c.Key.Schema, c.Key.ObjectName))
            .ToHashSet();

        return changes
            .Where(c => !((c.Key.Kind == MigrationObjectKind.Column || c.Key.Kind == MigrationObjectKind.Index || c.Key.Kind == MigrationObjectKind.Constraint)
                          && c.Parent is not null && wholeRelations.Contains((c.Key.Schema, c.Parent))))
            .OrderBy(c => KindRank(c.Key.Kind))
            .ThenBy(c => c.Key.Schema, StringComparer.Ordinal)
            .ThenBy(c => c.Key.ObjectName, StringComparer.Ordinal)
            .ThenBy(c => c.Key.Column, StringComparer.Ordinal)
            .Select(c => new MigrationObjectChange(
                c.Key.Schema, c.Key.ObjectName, c.Key.Column.Length == 0 ? null : c.Key.Column, c.Key.Kind, c.Action))
            .ToList();
    }

    private static int KindRank(string kind) => kind switch
    {
        MigrationObjectKind.Table => 0,
        MigrationObjectKind.Column => 1,
        MigrationObjectKind.Index => 2,
        MigrationObjectKind.Constraint => 7,
        MigrationObjectKind.View => 3,
        MigrationObjectKind.Function => 4,
        MigrationObjectKind.Hypertable => 5,
        _ => 6,
    };
}
