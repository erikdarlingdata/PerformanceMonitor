/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's reads/writes for the database-state alert's per-(server, database) expected state
/// (<c>config.database_state_expected</c>) — the override editor's backing store. Mirrors the service's
/// <c>DarlingAlertReadAdapter</c> seed/deviation SQL so the editor and the alert agree on what "expected"
/// means: opening the editor seeds a first-observation baseline for any current database that has none
/// (skipped on a read-only seat, which can't write — the service seeds during its sweeps anyway). Writes
/// route through <see cref="ExecuteWriteAsync"/> so a read-only <c>viewer</c> seat degrades to
/// <see cref="ViewerReadOnlyException"/> rather than a raw 42501. Bare <c>database_states</c> resolves to
/// <c>collect</c> through the search_path; the config table is schema-qualified.
/// </summary>
public sealed partial class ViewerDataService
{
    /* Matches the service alert read's seed: effective state (STANDBY for a log-shipping secondary, else
       state_desc), and an integrity or transient effective state is NOT baselined so it stays pending —
       otherwise opening the editor mid-outage would baseline SUSPECT and silence the alert, or opening it
       mid-restore would baseline RESTORING and invert it (#2189). The state list is shared with the service
       rather than spelled twice: this project cannot reference the service's, and two hand-kept copies of
       "what must never be learned" is the drift that would let the editor seed what the alert refuses to. */
    private const string DatabaseStateSeedSql = $@"
INSERT INTO config.database_state_expected (server_id, database_name, expected_state, is_user_override, updated_at)
SELECT $1, ds.database_name, CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END, false, (now() AT TIME ZONE 'UTC')
FROM database_states ds
WHERE ds.server_id = $1
AND   ds.collection_time = (SELECT MAX(collection_time) FROM database_states WHERE server_id = $1)
AND   ds.state_desc IS NOT NULL
AND   (CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END) NOT IN ({DatabaseStateTokens.NeverBaselinedSqlList})
ON CONFLICT (server_id, database_name) DO NOTHING";

    /* The service adapter's #2189 heal, run for the same reason the seed is: the editor must not SHOW a
       stale auto-baseline the alert has already stopped honouring, and a viewer seat pointed at a store
       whose service is down would otherwise display — and keep — an expectation the database outgrew.
       Auto-baselines only, and only ones recording a state the seed would refuse to learn: a user override
       is the operator's declaration, and OFFLINE/STANDBY are steady states whose departure is real news. */
    private const string DatabaseStateHealToOnlineSql = $@"
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
    FROM database_states ds
    WHERE ds.server_id = $1
    AND   ds.collection_time = (SELECT MAX(collection_time) FROM database_states WHERE server_id = $1)
    AND   ds.database_name = e.database_name
    AND   (CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END) = 'ONLINE'
)";

    private const string DatabaseStateExpectationsSql = @"
WITH latest AS (
    SELECT ds.database_name, CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END AS eff
    FROM database_states ds
    WHERE ds.server_id = $1
    AND   ds.collection_time = (SELECT MAX(collection_time) FROM database_states WHERE server_id = $1)
)
SELECT l.database_name, l.eff, e.expected_state, e.is_user_override
FROM latest l
LEFT JOIN config.database_state_expected e
  ON  e.server_id = $1
  AND e.database_name = l.database_name
ORDER BY l.database_name";

    private const string DatabaseStateSetExpectedSql = @"
INSERT INTO config.database_state_expected (server_id, database_name, expected_state, is_user_override, updated_at)
VALUES ($1, $2, $3, true, (now() AT TIME ZONE 'UTC'))
ON CONFLICT (server_id, database_name)
DO UPDATE SET expected_state = EXCLUDED.expected_state, is_user_override = true, updated_at = (now() AT TIME ZONE 'UTC')";

    private const string DatabaseStateResetToCurrentSql = @"
INSERT INTO config.database_state_expected (server_id, database_name, expected_state, is_user_override, updated_at)
SELECT $1, $2, CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END, false, (now() AT TIME ZONE 'UTC')
FROM database_states ds
WHERE ds.server_id = $1
AND   ds.database_name = $2
AND   ds.collection_time = (SELECT MAX(collection_time) FROM database_states WHERE server_id = $1)
ON CONFLICT (server_id, database_name)
DO UPDATE SET expected_state = EXCLUDED.expected_state, is_user_override = false, updated_at = (now() AT TIME ZONE 'UTC')";

    /// <summary>
    /// Every database in the latest snapshot for the override editor: current state, expected state, and
    /// whether the expected state is a user override. Seeds missing baselines first (unless read-only).
    /// </summary>
    public async Task<List<DatabaseStateExpectedRow>> GetDatabaseStateExpectationsAsync(int serverId, CancellationToken cancellationToken = default)
    {
        if (!IsReadOnly)
        {
            await using var seed = _dataSource.CreateCommand(DatabaseStateSeedSql);
            seed.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
            seed.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            await seed.ExecuteNonQueryAsync(cancellationToken);

            await using var heal = _dataSource.CreateCommand(DatabaseStateHealToOnlineSql);
            heal.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
            heal.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            await heal.ExecuteNonQueryAsync(cancellationToken);
        }

        var rows = new List<DatabaseStateExpectedRow>();
        await using var command = _dataSource.CreateCommand(DatabaseStateExpectationsSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new DatabaseStateExpectedRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                CurrentState = reader.IsDBNull(1) ? "" : reader.GetString(1),
                ExpectedState = reader.IsDBNull(2) ? "" : reader.GetString(2),
                IsUserOverride = !reader.IsDBNull(3) && reader.GetBoolean(3)
            });
        }

        return rows;
    }

    /// <summary>Sets a database's expected state (the override); pass <see cref="DatabaseStateTokens.Ignore"/>
    /// to opt the database out of the alert.</summary>
    public async Task SetDatabaseStateExpectedAsync(int serverId, string databaseName, string expectedState, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(DatabaseStateSetExpectedSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = expectedState });
        await ExecuteWriteAsync(command, cancellationToken);
    }

    /// <summary>Re-baselines a database: sets its expected state to its current state and clears the override flag.</summary>
    public async Task ResetDatabaseStateExpectedToCurrentAsync(int serverId, string databaseName, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(DatabaseStateResetToCurrentSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName });
        await ExecuteWriteAsync(command, cancellationToken);
    }
}

/// <summary>
/// One row of the database-state override editor: a database's current collected state alongside its
/// expected state (auto-seeded baseline or user override).
/// </summary>
public sealed class DatabaseStateExpectedRow
{
    public string DatabaseName { get; set; } = "";
    public string CurrentState { get; set; } = "";
    public string ExpectedState { get; set; } = "";
    public bool IsUserOverride { get; set; }

    public bool IsIgnored => ExpectedState == DatabaseStateTokens.Ignore;

    public bool IsDeviating => !IsIgnored && !string.Equals(CurrentState, ExpectedState, System.StringComparison.Ordinal);
}
