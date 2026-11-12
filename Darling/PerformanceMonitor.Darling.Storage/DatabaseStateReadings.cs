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
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #7662: the database_states readings (collected every minute) that date a <c>state_desc</c> change by the minute the new state
/// was seen instead of the next config snapshot. The web read (the MCP tool) and the desktop viewer's grid both run this, so the
/// SQL and the bounds cannot drift. It lives here, in the project both of them reference.
/// </summary>
public static class DatabaseStateReadings
{
    /// <summary>The readings of the databases named in $2 between $3 (exclusive) and $4 (inclusive).
    /// $1 server_id, $2 database names, $3 and $4 naive UTC.</summary>
    public const string Sql = """
        SELECT collection_time, database_name, state_desc
        FROM database_states
        WHERE server_id = $1
        AND   database_name = ANY($2)
        AND   collection_time > $3
        AND   collection_time <= $4
        AND   state_desc IS NOT NULL
        ORDER BY collection_time
        """;

    /// <summary>The readings that can date the <c>state_desc</c> rows of <paramref name="changes"/>, from the snapshot before the
    /// earliest such change (a day before it when it has none) to the latest one. None when there are no such rows.</summary>
    public static async Task<List<ConfigChangeDiff.DatabaseStateObservation>> ReadAsync(
        NpgsqlDataSource dataSource, int serverId, IReadOnlyList<ConfigChangeDiff.DatabaseConfigChange> changes,
        int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        var rows = new List<ConfigChangeDiff.DatabaseStateObservation>();
        var stateChanges = changes.Where(c => c.SettingName == "state_desc").ToList();
        if (stateChanges.Count == 0)
        {
            return rows;
        }

        await using var command = dataSource.CreateCommand(Sql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = stateChanges.Select(c => c.DatabaseName).Distinct(StringComparer.Ordinal).ToArray() });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(stateChanges.Min(c => c.PreviousCaptureTime ?? c.ChangeTime.AddDays(-1)), DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(stateChanges.Max(c => c.ChangeTime), DateTimeKind.Unspecified) });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ConfigChangeDiff.DatabaseStateObservation(reader.GetDateTime(0), reader.GetString(1), reader.GetString(2)));
        }

        return rows;
    }
}
