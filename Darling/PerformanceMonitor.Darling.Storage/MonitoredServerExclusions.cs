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
using NpgsqlTypes;
using PerformanceMonitor.Alerting;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The one write of a monitored server's excluded-databases list (#6230). The Viewer's Excluded Databases editor and the
/// <c>set_server_excluded_databases</c> MCP tool both build their statement here, so the SQL and the bounds (at most
/// <see cref="ExcludedDatabaseNames.MaxEntries"/> names of at most <see cref="ExcludedDatabaseNames.MaxNameLength"/> characters,
/// no control character) exist once. The service re-reads the list on its next config poll; no restart.
/// </summary>
public static class MonitoredServerExclusions
{
    /// <summary>Rewrites just <c>excluded_databases</c> and stamps <c>modified_at</c>. $1 server_id, $2 text[].</summary>
    public const string SetSql =
        "UPDATE config.config_monitored_servers SET excluded_databases = $2, modified_at = (now() AT TIME ZONE 'UTC') WHERE server_id = $1";

    /// <summary>The refusal text for a list that breaks the bounds, or null when it is acceptable.</summary>
    public static string? Refusal(IEnumerable<string>? names) => ExcludedDatabaseNames.Refusal(names, "excluded_databases");

    /// <summary>The list as it is stored: blank names dropped, repeats (any casing) kept once, in the order given.</summary>
    public static IReadOnlyList<string> Prepare(IEnumerable<string>? names) =>
        (names ?? Enumerable.Empty<string>())
            .Where(n => !string.IsNullOrWhiteSpace(n))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

    /// <summary>The write as a command the caller runs (the Viewer through its own write wrapper, the service directly).</summary>
    public static NpgsqlCommand CreateSetCommand(NpgsqlDataSource dataSource, int serverId, IEnumerable<string> names, int commandTimeoutSeconds)
    {
        var command = dataSource.CreateCommand(SetSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text,
            Value = (names ?? Enumerable.Empty<string>()).ToArray(),
        });
        return command;
    }

    /// <summary>One server's stored list, or null when no such server is registered.</summary>
    public static async Task<IReadOnlyList<string>?> ReadAsync(NpgsqlDataSource dataSource, int serverId, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("SELECT excluded_databases FROM config.config_monitored_servers WHERE server_id = $1");
        command.CommandTimeout = 15;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return reader.IsDBNull(0) ? Array.Empty<string>() : reader.GetFieldValue<string[]>(0);
    }
}
