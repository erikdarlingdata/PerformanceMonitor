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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The runner-side half of the composer's server scope (#5525). The compiler scopes every fact read by
/// <c>server_id</c> (see <see cref="ComposeCompiler.ServerScope"/>), resolving the scoped names through the registry inside the
/// SQL. A name with no registry row resolves to no id, so a scope that named one would stop matching the rows written under it.
/// This finds those names before the compile, so the compiler can keep matching them on the row's stored <c>server_name</c> and
/// a scope over a name that was never registered (or was removed) still returns what it returned before. The lookup is one read of
/// the small registry table (its only key is <c>server_id</c>; there is no index on <c>server_name</c>, and none is needed at fleet
/// size); it runs once per run, not per read, and its result is <see cref="ComposeRunContext.UnregisteredServers"/>.
/// <para>What a scoped name means is stated on <see cref="ComposeCompiler.ServerScope"/>: the server or servers the registry holds
/// under that name now, with all their rows.</para>
/// </summary>
internal static class ComposeServerScope
{
    /// <summary>
    /// #5633 item 17: maps each scoped name to the registry's stored <c>server_name</c> by an EXACT match through the resolver every
    /// other server-scoped tool uses (stored name, then display name, case-insensitive), so a panel spec naming a server by its display
    /// name or a case-different spelling reads that server instead of returning no rows. The resolver's PARTIAL tier is deliberately not
    /// used (review M1): "SQL01" would otherwise be remapped to an enabled "SQL01-DR" and show another server's rows. A name with no
    /// exact match stays as written: it is either a removed or disabled server whose history is still stored (#5525) or a name nobody
    /// registered, which the run's unknown-server answer (W13, a <c>not_found</c> naming it) reports when nothing is stored under it.
    /// A name that matches SEVERAL servers exactly is refused with the resolver's own sentence, because reading an arbitrary one of
    /// them would be a silent guess. The registry read faulting leaves the
    /// scope untouched (the same fail-open the unregistered-name lookup has).
    /// </summary>
    public static async Task<(List<string>? Scope, string? Refusal)> ResolveNamesAsync(
        NpgsqlDataSource postgres, List<string>? scope, CancellationToken cancellationToken, ILogger? logger = null)
    {
        if (scope is not { Count: > 0 })
        {
            return (scope, null);
        }

        var (servers, fault) = await LoadAllOrFaultAsync(postgres, cancellationToken);
        if (fault is not null)
        {
            logger?.LogWarning("Compose server-scope resolution skipped: {Fault}", fault);
            return (scope, null);
        }

        return ResolveNames(scope, servers);
    }

    /// <summary>#5633 review L1: every registered server, enabled or not. The exact-name test must see a disabled registration whose
    /// history is still stored, or an enabled server's display name could take its name. Parameter-free, like
    /// <see cref="DarlingServerResolver.LoadEnabledServersSql"/>.</summary>
    internal const string LoadAllServersSql = @"
SELECT server_id, server_name, display_name
FROM servers
ORDER BY server_name";

    private static async Task<(List<RegisteredServer> Servers, string? Fault)> LoadAllOrFaultAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        try
        {
            var servers = new List<RegisteredServer>();
            await using var command = postgres.CreateCommand(LoadAllServersSql);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                servers.Add(new RegisteredServer(
                    reader.GetInt32(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2)));
            }

            return (servers, null);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* The same fault sentence and the same cancellation rule as the enabled-only read (#4203, #5678). */
            return DarlingServerResolver.FaultOrCancel(ex, cancellationToken);
        }
    }

    /// <summary>The pure half of <see cref="ResolveNamesAsync"/>: the scope with each name placed in <paramref name="servers"/>, or the
    /// resolver's refusal for a name that is ambiguous.</summary>
    internal static (List<string> Scope, string? Refusal) ResolveNames(
        IReadOnlyList<string> scope, IReadOnlyList<RegisteredServer> servers)
    {
        var resolvedNames = new List<string>(scope.Count);
        foreach (var rawName in scope)
        {
            /* Review L1: trimmed, as the resolver trims (ResolveForRemoval), so " SQL01" resolves as "SQL01" does. A name that
               matches nothing stays as it was written. */
            var name = rawName.Trim();
            if (servers.Any(s => string.Equals(s.ServerName, rawName, StringComparison.Ordinal)))
            {
                resolvedNames.Add(rawName);
                continue;
            }

            /* Review L1: an exact stored server_name of ANY registration (enabled or not) is kept before display names are tried, so a
               disabled "SQL01" with stored history is not taken by an enabled server whose display name is "SQL01". */
            if (name.Length > 0 && servers.Any(s => string.Equals(s.ServerName, name, StringComparison.Ordinal)))
            {
                resolvedNames.Add(name);
                continue;
            }

            /* #5633 review M1: EXACT matches only (stored name, then display name, either ignoring case). The resolver's partial tier
               is not used: "SQL01" is a removed or disabled server whose history is still stored (#5525), and "SQL01-DR" is a
               different server that merely contains it. A name with no exact match stays as written. */
            var hasExact = servers.Any(s => string.Equals(s.ServerName, name, StringComparison.OrdinalIgnoreCase)
                || string.Equals(s.DisplayName, name, StringComparison.OrdinalIgnoreCase));
            if (!hasExact)
            {
                resolvedNames.Add(rawName);
                continue;
            }

            /* An exact tier always answers (one server, or several that share the name, which the resolver refuses). */
            var (resolved, error) = DarlingServerResolver.ResolveOrError(servers, name, NameClash.StoredNameWins);
            if (error is null)
            {
                resolvedNames.Add(resolved.ServerName);
            }
            else
            {
                return (resolvedNames, error);
            }
        }

        return (resolvedNames.Distinct(StringComparer.Ordinal).ToList(), null);
    }

    /// <summary>The scoped names that no <c>collect.servers</c> row carries (exact, case-sensitive: the match the SQL scope uses).
    /// $1 is the distinct scoped names.</summary>
    internal const string FindUnregisteredSql =
        "SELECT n FROM unnest($1::text[]) AS n WHERE NOT EXISTS (SELECT 1 FROM collect.servers AS s WHERE s.server_name = n)";

    /// <summary>
    /// The unregistered subset of <paramref name="servers"/>, or null when the scope is the whole fleet or every name is registered
    /// (the common case, and the one whose SQL carries no extra branch). A fault answers with ALL the names: a name matched by
    /// stored <c>server_name</c> as well as by id still returns everything the old scope returned, so a store that cannot answer the
    /// lookup costs the old read's price, never a missing server; the fault is logged as a warning. A cancelled run is not a fault and
    /// propagates.
    /// <para><paramref name="onLookupFailed"/> is called when the lookup faulted, so a caller that must tell "every name is unregistered" from
    /// "the registry could not be read" (the web panel run's unknown-server answer, W13) can; the returned names are the same either way.</para>
    /// <para>The lookup runs outside the read's own transaction (and outside the hourly-edges snapshot). If a re-connect renames a
    /// registry row between the lookup and the read, that one run can miss the rows stored under the old name, because the lookup saw
    /// the name as registered and the read no longer resolves it to an id. The next run is right.</para>
    /// </summary>
    public static async Task<IReadOnlyList<string>?> FindUnregisteredAsync(
        NpgsqlDataSource postgres, IReadOnlyList<string>? servers, CancellationToken cancellationToken, ILogger? logger = null,
        Action? onLookupFailed = null)
    {
        if (servers is not { Count: > 0 })
        {
            return null;
        }

        var names = servers.Distinct(StringComparer.Ordinal).ToArray();
        try
        {
            await using var command = postgres.CreateCommand(FindUnregisteredSql);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.Add(new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text,
                Value = names,
            });
            var missing = new List<string>();
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                missing.Add(reader.GetString(0));
            }

            return missing.Count == 0 ? null : missing;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(
                "Compose server-scope lookup failed ({Message}); the read matches the scoped names on the stored server name as well as by id",
                ex.Message);
            onLookupFailed?.Invoke();
            return names;
        }
    }
}
