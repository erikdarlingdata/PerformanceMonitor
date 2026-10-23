/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The one copy of the SQL over <c>config.database_state_expected</c>, the per-(server, database) expected state the
/// database-state alert compares the current state against (#6397). The Viewer's override editor and the MCP tools
/// <c>get_database_state_expectations</c> / <c>set_database_state_expected</c> both run it; neither holds its own.
/// Commands are built here and run by the caller, so the Viewer can route a write through its read-only-seat
/// translation. Seed and heal mirror <c>DarlingAlertReadAdapter</c>'s, so the editor and the alert agree on what
/// "expected" means (#2189).
/// </summary>
public static class DatabaseStateExpectedStore
{
    /// <summary>The state tokens the Viewer's editor and the MCP set tool offer, including the <c>(ignore)</c> opt-out. The editor builds its dropdown from this list, so the two cannot drift.</summary>
    public static readonly IReadOnlyList<string> StandardStates =
    [
        DatabaseStateTokens.Online, DatabaseStateTokens.Offline, DatabaseStateTokens.Restoring,
        DatabaseStateTokens.Recovering, DatabaseStateTokens.RecoveryPending, DatabaseStateTokens.Suspect,
        DatabaseStateTokens.Emergency, DatabaseStateTokens.Standby, DatabaseStateTokens.Ignore
    ];

    /* An integrity or transient effective state is NOT baselined so it stays pending (#2189); the list is shared
       with the service rather than spelled twice. */
    public const string SeedSql = $@"
INSERT INTO config.database_state_expected (server_id, database_name, expected_state, is_user_override, updated_at)
SELECT $1, ds.database_name, CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END, false, (now() AT TIME ZONE 'UTC')
FROM collect.database_states ds
WHERE ds.server_id = $1
AND   ds.collection_time = (SELECT MAX(collection_time) FROM collect.database_states WHERE server_id = $1)
AND   ds.state_desc IS NOT NULL
AND   (CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END) NOT IN ({DatabaseStateTokens.NeverBaselinedSqlList})
ON CONFLICT (server_id, database_name) DO NOTHING";

    /* The #2189 heal: auto-baselines only, and only ones recording a state the seed would refuse to learn. */
    public const string HealToOnlineSql = $@"
UPDATE config.database_state_expected e
SET expected_state = 'ONLINE',
    updated_at = (now() AT TIME ZONE 'UTC'),
    last_alerted_state = NULL,
    last_alerted_at = NULL
WHERE e.server_id = $1
AND   e.is_user_override = false
AND   e.expected_state IN ({DatabaseStateTokens.NeverBaselinedSqlList})
AND   EXISTS (
    SELECT 1
    FROM collect.database_states ds
    WHERE ds.server_id = $1
    AND   ds.collection_time = (SELECT MAX(collection_time) FROM collect.database_states WHERE server_id = $1)
    AND   ds.database_name = e.database_name
    AND   (CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END) = 'ONLINE'
)";

    /// <summary>Every database in the server's latest snapshot with its current effective state and expectation (a
    /// database with none has NULL expected columns).</summary>
    public const string ReadSql = @"
WITH latest AS (
    SELECT ds.collection_time, ds.database_name, CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END AS eff
    FROM collect.database_states ds
    WHERE ds.server_id = $1
    AND   ds.collection_time = (SELECT MAX(collection_time) FROM collect.database_states WHERE server_id = $1)
)
SELECT l.database_name, l.eff, e.expected_state, e.is_user_override, l.collection_time
FROM latest l
LEFT JOIN config.database_state_expected e
  ON  e.server_id = $1
  AND e.database_name = l.database_name
ORDER BY l.database_name";

    public const string SetSql = @"
INSERT INTO config.database_state_expected (server_id, database_name, expected_state, is_user_override, updated_at)
VALUES ($1, $2, $3, true, (now() AT TIME ZONE 'UTC'))
ON CONFLICT (server_id, database_name)
DO UPDATE SET expected_state = EXCLUDED.expected_state, is_user_override = true, updated_at = (now() AT TIME ZONE 'UTC')";

    public const string ResetToCurrentSql = @"
INSERT INTO config.database_state_expected (server_id, database_name, expected_state, is_user_override, updated_at)
SELECT $1, $2, CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END, false, (now() AT TIME ZONE 'UTC')
FROM collect.database_states ds
WHERE ds.server_id = $1
AND   ds.database_name = $2
AND   ds.collection_time = (SELECT MAX(collection_time) FROM collect.database_states WHERE server_id = $1)
ON CONFLICT (server_id, database_name)
DO UPDATE SET expected_state = EXCLUDED.expected_state, is_user_override = false, updated_at = (now() AT TIME ZONE 'UTC')";

    /// <summary>Removes one expectation. The service re-learns a baseline from the database's next observation (not
    /// for an integrity or transient state, which stays pending).</summary>
    public const string ClearSql = @"
DELETE FROM config.database_state_expected
WHERE server_id = $1
AND   database_name = $2
RETURNING expected_state, is_user_override";

    private static NpgsqlCommand Create(
        NpgsqlDataSource dataSource, string sql, int timeoutSeconds, int serverId, string? databaseName, string? expectedState)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        var command = dataSource.CreateCommand(sql);
        command.CommandTimeout = timeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        if (databaseName is not null)
        {
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName });
        }

        if (expectedState is not null)
        {
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = expectedState });
        }

        return command;
    }

    public static NpgsqlCommand CreateSeedCommand(NpgsqlDataSource dataSource, int serverId, int timeoutSeconds) =>
        Create(dataSource, SeedSql, timeoutSeconds, serverId, null, null);

    public static NpgsqlCommand CreateHealCommand(NpgsqlDataSource dataSource, int serverId, int timeoutSeconds) =>
        Create(dataSource, HealToOnlineSql, timeoutSeconds, serverId, null, null);

    public static NpgsqlCommand CreateReadCommand(NpgsqlDataSource dataSource, int serverId, int timeoutSeconds) =>
        Create(dataSource, ReadSql, timeoutSeconds, serverId, null, null);

    public static NpgsqlCommand CreateSetCommand(
        NpgsqlDataSource dataSource, int serverId, string databaseName, string expectedState, int timeoutSeconds) =>
        Create(dataSource, SetSql, timeoutSeconds, serverId, databaseName, expectedState);

    public static NpgsqlCommand CreateResetToCurrentCommand(
        NpgsqlDataSource dataSource, int serverId, string databaseName, int timeoutSeconds) =>
        Create(dataSource, ResetToCurrentSql, timeoutSeconds, serverId, databaseName, null);

    public static NpgsqlCommand CreateClearCommand(
        NpgsqlDataSource dataSource, int serverId, string databaseName, int timeoutSeconds) =>
        Create(dataSource, ClearSql, timeoutSeconds, serverId, databaseName, null);

    /// <summary>One database of the latest snapshot: its current effective state and expectation (null when none).</summary>
    public sealed record Row(string DatabaseName, string CurrentState, string? ExpectedState, bool IsUserOverride, DateTime CapturedAt);

    /// <summary>Reads the latest snapshot's databases with their expectations. Writes nothing.</summary>
    public static async Task<List<Row>> ReadAsync(
        NpgsqlDataSource dataSource, int serverId, int timeoutSeconds, CancellationToken cancellationToken = default)
    {
        var rows = new List<Row>();
        await using var command = CreateReadCommand(dataSource, serverId, timeoutSeconds);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                !reader.IsDBNull(3) && reader.GetBoolean(3),
                reader.GetDateTime(4)));
        }

        return rows;
    }

    /// <summary>Removes the expectation and returns what it was, or null when the database had none.</summary>
    public static async Task<(string ExpectedState, bool IsUserOverride)?> ClearAsync(
        NpgsqlDataSource dataSource, int serverId, string databaseName, int timeoutSeconds,
        CancellationToken cancellationToken = default)
    {
        await using var command = CreateClearCommand(dataSource, serverId, databaseName, timeoutSeconds);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return (reader.GetString(0), reader.GetBoolean(1));
    }

    /// <summary>Sets the expectation (an override) and runs the write; the caller reads the previous row first.</summary>
    public static async Task SetAsync(
        NpgsqlDataSource dataSource, int serverId, string databaseName, string expectedState, int timeoutSeconds,
        CancellationToken cancellationToken = default)
    {
        await using var command = CreateSetCommand(dataSource, serverId, databaseName, expectedState, timeoutSeconds);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
