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
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The two checks <c>--prepare-provision-roles</c> makes against the store before it renders <c>provision-roles.sql</c>
/// (#8779): the store has reached this build's schema version (a post-upgrade hook can start before the new pod has
/// migrated), and no object in the <c>collect</c> and <c>config</c> schemas belongs to another role (the script runs as
/// the owner and could not alter it).
/// </summary>
internal static class ProvisionRolesStoreChecks
{
    private static readonly TimeSpan LogInterval = TimeSpan.FromMinutes(1);

    /// <summary>An object the login does not own. <paramref name="Kind"/> is spelled as <c>ALTER &lt;Kind&gt;</c> spells it;
    /// for a function or procedure <paramref name="Name"/> includes the argument types.</summary>
    internal sealed record ForeignOwnedObject(string Kind, string Schema, string Name, string Owner);

    internal static async Task<int?> ReadStoredSchemaVersionAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await using (var probe = new NpgsqlCommand("SELECT to_regclass('darling_schema_version') IS NOT NULL", c))
        {
            if (!(bool)(await probe.ExecuteScalarAsync(ct))!)
            {
                return null;
            }
        }

        await using var read = new NpgsqlCommand("SELECT max(version) FROM darling_schema_version", c);
        var value = await read.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null : Convert.ToInt32(value, CultureInfo.InvariantCulture);
    }

    internal static bool SchemaReached(int? stored, int required) => stored >= required;

    /// <summary>The timeout message. When no version was ever read and a connection error was seen, the error replaces the claim that
    /// the new pod has not finished migrating, which nothing then supports. The error never holds the connection string.</summary>
    internal static string DescribeSchemaTimeout(int? lastSeen, int required, int waitedSeconds, string? lastError = null)
    {
        var stored = lastSeen is { } seen ? seen.ToString(CultureInfo.InvariantCulture) : "none";
        if (lastSeen is null && lastError is not null)
        {
            return string.Create(CultureInfo.InvariantCulture,
                $"The store is at schema version {stored}; this build needs {required}. Waited {waitedSeconds} seconds and never read the schema version. The last error was {lastError}");
        }

        return string.Create(CultureInfo.InvariantCulture,
            $"The store is at schema version {stored}; this build needs {required}. "
            + $"Waited {waitedSeconds} seconds: the new Darling pod has not finished migrating the store.");
    }

    internal static async Task<IReadOnlyList<ForeignOwnedObject>> ReadForeignOwnedObjectsAsync(NpgsqlConnection c, CancellationToken ct)
    {
        const string sql = @"
SELECT 'schema', n.nspname::text, n.nspname::text, r.rolname::text
FROM pg_namespace n JOIN pg_roles r ON r.oid = n.nspowner
WHERE n.nspname IN ('collect', 'config') AND r.rolname <> current_user
UNION ALL
SELECT CASE cl.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' ELSE 'sequence' END,
       n.nspname::text, cl.relname::text, r.rolname::text
FROM pg_class cl JOIN pg_namespace n ON n.oid = cl.relnamespace JOIN pg_roles r ON r.oid = cl.relowner
WHERE n.nspname IN ('collect', 'config') AND cl.relkind IN ('r', 'p', 'v', 'm', 'S') AND r.rolname <> current_user
  AND NOT (cl.relkind = 'S' AND EXISTS (SELECT 1 FROM pg_depend d
           WHERE d.classid = 'pg_class'::regclass AND d.objid = cl.oid AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i')))
UNION ALL
SELECT CASE p.prokind WHEN 'p' THEN 'procedure' ELSE 'function' END,
       n.nspname::text, p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')', r.rolname::text
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace JOIN pg_roles r ON r.oid = p.proowner
WHERE n.nspname IN ('collect', 'config') AND p.prokind IN ('f', 'p') AND r.rolname <> current_user
ORDER BY 2, 1, 3";
        var found = new List<ForeignOwnedObject>();
        await using var command = new NpgsqlCommand(sql, c);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            found.Add(new ForeignOwnedObject(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3)));
        }

        return found;
    }

    internal static string? DescribeOwnership(string currentUser, IReadOnlyList<ForeignOwnedObject> foreign)
    {
        if (foreign.Count == 0)
        {
            return null;
        }

        var text = new StringBuilder();
        text.Append(CultureInfo.InvariantCulture, $"{foreign.Count} object(s) in the collect and config schemas are not owned by {currentUser}:\n");
        foreach (var item in foreign)
        {
            text.Append(CultureInfo.InvariantCulture, $"  {item.Kind} {item.Schema}.{item.Name} (owner {item.Owner})\n");
        }

        text.Append("Run once as a superuser, then run this again:\n");
        foreach (var item in foreign)
        {
            text.Append(CultureInfo.InvariantCulture, $"ALTER {item.Kind.ToUpperInvariant()} {QualifiedName(item)} OWNER TO {Quote(currentUser)};\n");
        }

        return text.ToString();
    }

    /// <summary>Polls until the stored schema version reaches <paramref name="required"/> or the budget is gone. An unreachable store counts as
    /// not migrated yet; the last connection error (type and message, which carry no password) is returned and shown in the per-minute line
    /// until a read succeeds, so "unreachable" and "not migrated" can be told apart.</summary>
    internal static async Task<(bool Reached, int? LastSeen, string? LastError)> WaitForSchemaAsync(
        Func<CancellationToken, Task<int?>> readVersion, int required, TimeSpan budget, TimeSpan poll,
        TextWriter log, TimeProvider time, CancellationToken ct)
    {
        var start = time.GetUtcNow();
        var lastLogged = start - LogInterval;
        int? lastSeen = null;
        string? lastError = null;
        while (true)
        {
            try
            {
                lastSeen = await readVersion(ct);
                lastError = null;
                if (SchemaReached(lastSeen, required))
                {
                    return (true, lastSeen, null);
                }
            }
            catch (Exception ex) when (ex is NpgsqlException or SocketException)
            {
                lastError = $"{ex.GetType().Name}: {ex.Message}";
            }

            var now = time.GetUtcNow();
            var remaining = budget - (now - start);
            if (remaining <= TimeSpan.Zero)
            {
                return (false, lastSeen, lastError);
            }

            if (now - lastLogged >= LogInterval)
            {
                lastLogged = now;
                await log.WriteLineAsync(string.Create(CultureInfo.InvariantCulture,
                    $"Waiting for the store schema: stored {(lastSeen is { } seen ? seen.ToString(CultureInfo.InvariantCulture) : "none")}, required {required}.{(lastError is null ? string.Empty : " Last error: " + lastError)}"));
            }

            await Task.Delay(remaining < poll ? remaining : poll, time, ct);
        }
    }

    private static string QualifiedName(ForeignOwnedObject item)
    {
        if (item.Kind == "schema")
        {
            return Quote(item.Schema);
        }

        if (item.Kind is "function" or "procedure")
        {
            var open = item.Name.IndexOf('(', StringComparison.Ordinal);
            return $"{Quote(item.Schema)}.{Quote(item.Name[..open])}{item.Name[open..]}";
        }

        return $"{Quote(item.Schema)}.{Quote(item.Name)}";
    }

    private static string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
}
