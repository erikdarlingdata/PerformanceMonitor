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
using Npgsql;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The runner-side half of the composer's server scope (#5525). The compiler scopes every fact read by
/// <c>server_id</c> (see <see cref="ComposeCompiler.ServerScope"/>), resolving the scoped names through the registry inside the
/// SQL. A name with no registry row resolves to no id, so a scope that named one would stop matching the rows written under it.
/// This finds those names before the compile, so the compiler can keep matching them on the row's stored <c>server_name</c> and
/// no scope loses what it matched before. The lookup is one read of the registry's unique server_name index; it runs once per
/// run, not per read, and its result is <see cref="ComposeRunContext.UnregisteredServers"/>.
/// </summary>
internal static class ComposeServerScope
{
    /// <summary>The scoped names that no <c>collect.servers</c> row carries (exact, case-sensitive: the match the SQL scope uses).
    /// $1 is the distinct scoped names.</summary>
    internal const string FindUnregisteredSql =
        "SELECT n FROM unnest($1::text[]) AS n WHERE NOT EXISTS (SELECT 1 FROM collect.servers AS s WHERE s.server_name = n)";

    /// <summary>
    /// The unregistered subset of <paramref name="servers"/>, or null when the scope is the whole fleet or every name is registered
    /// (the common case, and the one whose SQL carries no extra branch). A fault answers with ALL the names: a name matched by
    /// stored <c>server_name</c> as well as by id still returns everything the old scope returned, so a store that cannot answer the
    /// lookup costs the old read's price, never a missing server. A cancelled run is not a fault and propagates.
    /// </summary>
    public static async Task<IReadOnlyList<string>?> FindUnregisteredAsync(
        NpgsqlDataSource postgres, IReadOnlyList<string>? servers, CancellationToken cancellationToken)
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
            return names;
        }
    }
}
