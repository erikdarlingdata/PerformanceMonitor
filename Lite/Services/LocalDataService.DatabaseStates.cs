/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Alerting;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /* Effective state = STANDBY for a read-only log-shipping secondary (is_in_standby), else the raw
       state_desc. A standby secondary reports state_desc = ONLINE with is_in_standby = 1 and flips through
       RESTORING on every log restore; collapsing it to a single stable STANDBY token means it baselines as
       STANDBY and never churns. Composed identically in the seed, the deviation read, the editor and reset. */
    private const string EffectiveStateSql = "CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END";

    /// <summary>
    /// The databases whose collected state deviates from their expected state, for the baseline-deviation
    /// database-state alert. Fires only when the deviation is present in the TWO most recent collections
    /// (so a restart's RECOVERY_PENDING / RECOVERING transients — and a standby secondary's per-restore
    /// RESTORING flicker — don't page unless the condition actually sticks). First AUTO-SEEDS a baseline
    /// (the effective state) for any database in the newest snapshot that has none — EXCEPT a critical
    /// effective state (SUSPECT / RECOVERY_PENDING / EMERGENCY), which is left pending so onboarding a
    /// server mid-outage doesn't learn the bad state as expected. Also tidies auto-baselines for databases
    /// that have dropped off the newest snapshot (user overrides are preserved). The base table always
    /// holds the newest snapshots (archival only moves older rows to parquet), so it is queried directly.
    /// </summary>
    public async Task<List<DatabaseStateInfo>> GetDatabaseStateDeviationsAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();

        /* #2189, BEFORE the seed: drop any auto-baseline that recorded a TRANSIENT state, so the seed below
           re-learns the real steady state in the SAME cycle. A database observed mid-restore used to get
           RESTORING written as its expected state and then deviated forever by being healthy — 636 alerts in
           24 hours from 5 databases on the production fleet. The seed is insert-if-absent, so it can never
           correct a row itself; this is what makes the fix self-healing for baselines already written.
           is_user_override = false is load-bearing: an operator who deliberately expects RESTORING (a
           permanently log-shipped target) means it. */
        using (var repair = connection.CreateCommand())
        {
            repair.CommandText = @"
DELETE FROM config_database_state_expected
WHERE server_id = $1
AND   is_user_override = false
AND   expected_state IN ('RESTORING', 'RECOVERING', 'SUSPECT', 'RECOVERY_PENDING', 'EMERGENCY')";
            repair.Parameters.Add(new DuckDBParameter { Value = serverId });
            await repair.ExecuteNonQueryAsync();
        }

        /* Seed missing baselines from the latest snapshot (insert-if-absent; effective state; non-critical
           and non-transient only — a critical or mid-operation first observation stays pending, and the
           critical ones alert via the no-baseline arm below). */
        using (var seed = connection.CreateCommand())
        {
            seed.CommandText = $@"
INSERT INTO config_database_state_expected (server_id, database_name, expected_state, is_user_override, updated_at)
SELECT $1, ds.database_name, {EffectiveStateSql}, false, now()::TIMESTAMP
FROM database_states ds
WHERE ds.server_id = $1
AND   ds.collection_time = (SELECT MAX(collection_time) FROM database_states WHERE server_id = $1)
AND   ds.state_desc IS NOT NULL
AND   {EffectiveStateSql} NOT IN ('SUSPECT', 'RECOVERY_PENDING', 'EMERGENCY', 'RESTORING', 'RECOVERING')
AND   NOT EXISTS (
    SELECT 1 FROM config_database_state_expected e
    WHERE e.server_id = $1 AND e.database_name = ds.database_name
)";
            seed.Parameters.Add(new DuckDBParameter { Value = serverId });
            await seed.ExecuteNonQueryAsync();
        }

        /* Tidy auto-baselines for databases no longer in the newest snapshot (dropped/renamed). User
           overrides are kept — an operator's intent shouldn't vanish because a database is briefly gone. */
        using (var prune = connection.CreateCommand())
        {
            prune.CommandText = @"
DELETE FROM config_database_state_expected
WHERE server_id = $1
AND   is_user_override = false
AND   database_name NOT IN (
    SELECT database_name FROM database_states
    WHERE server_id = $1
    AND   collection_time = (SELECT MAX(collection_time) FROM database_states WHERE server_id = $1)
)";
            prune.Parameters.Add(new DuckDBParameter { Value = serverId });
            await prune.ExecuteNonQueryAsync();
        }

        using var command = connection.CreateCommand();
        command.CommandText = $@"
WITH newest AS (
    SELECT MAX(collection_time) AS t FROM database_states WHERE server_id = $1
),
prev AS (
    SELECT MAX(collection_time) AS t FROM database_states
    WHERE server_id = $1 AND collection_time < (SELECT t FROM newest)
),
latest AS (
    SELECT ds.database_name, {EffectiveStateSql} AS eff
    FROM database_states ds
    WHERE ds.server_id = $1 AND ds.collection_time = (SELECT t FROM newest)
),
previous AS (
    SELECT ds.database_name, {EffectiveStateSql} AS eff
    FROM database_states ds
    WHERE ds.server_id = $1 AND ds.collection_time = (SELECT t FROM prev)
)
SELECT l.database_name, l.eff, COALESCE(e.expected_state, '')
FROM latest l
JOIN previous p
  ON p.database_name = l.database_name
LEFT JOIN config_database_state_expected e
  ON  e.server_id = $1
  AND e.database_name = l.database_name
WHERE (e.expected_state IS NULL
        AND l.eff IN ('SUSPECT', 'RECOVERY_PENDING', 'EMERGENCY')
        AND p.eff IN ('SUSPECT', 'RECOVERY_PENDING', 'EMERGENCY'))
   OR (e.expected_state IS NOT NULL AND e.expected_state <> '(ignore)'
        AND l.eff IS DISTINCT FROM e.expected_state
        AND p.eff IS DISTINCT FROM e.expected_state)
ORDER BY l.database_name";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<DatabaseStateInfo>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new DatabaseStateInfo
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                StateDesc = reader.IsDBNull(1) ? "" : reader.GetString(1),
                ExpectedState = reader.IsDBNull(2) ? "" : reader.GetString(2)
            });
        }

        return items;
    }

    /// <summary>
    /// Every database in the latest <c>database_states</c> snapshot for the override editor: its current
    /// EFFECTIVE state joined to its expected state (and whether that expected state is a user override vs
    /// the auto-seeded baseline). Seeds/prunes first via the alert read so the editor and the alert always
    /// agree on what "expected" is.
    /// </summary>
    public async Task<List<DatabaseStateExpectedRow>> GetDatabaseStateExpectationsAsync(int serverId)
    {
        /* Reuse the seed/prune side-effect so the editor shows a baseline for every current database. */
        await GetDatabaseStateDeviationsAsync(serverId);

        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = $@"
WITH latest AS (
    SELECT ds.database_name, {EffectiveStateSql} AS eff
    FROM database_states ds
    WHERE ds.server_id = $1
    AND   ds.collection_time = (SELECT MAX(collection_time) FROM database_states WHERE server_id = $1)
)
SELECT
    l.database_name,
    l.eff,
    e.expected_state,
    e.is_user_override
FROM latest l
LEFT JOIN config_database_state_expected e
  ON  e.server_id = $1
  AND e.database_name = l.database_name
ORDER BY l.database_name";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<DatabaseStateExpectedRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new DatabaseStateExpectedRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                CurrentState = reader.IsDBNull(1) ? "" : reader.GetString(1),
                ExpectedState = reader.IsDBNull(2) ? "" : reader.GetString(2),
                IsUserOverride = !reader.IsDBNull(3) && reader.GetBoolean(3)
            });
        }

        return items;
    }

    /// <summary>
    /// Sets a database's expected state (the override) — pass <see cref="DatabaseStateTokens.Ignore"/>
    /// to opt the database out of the alert. Upserts on (server_id, database_name) and marks the row a
    /// user override so a later auto-seed never clobbers it.
    /// </summary>
    public async Task SetDatabaseStateExpectedAsync(int serverId, string databaseName, string expectedState)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        /* now()::TIMESTAMP, not a bare current_timestamp: DuckDB resolves a bare current_timestamp against
           the table's columns (and errors) inside a VALUES row and an ON CONFLICT DO UPDATE SET, so the
           override write uses the function form — which binds as an expression everywhere. INSERT ... SELECT
           (not VALUES) for the same reason, matching the auto-seed and reset shape. */
        command.CommandText = @"
INSERT INTO config_database_state_expected (server_id, database_name, expected_state, is_user_override, updated_at)
SELECT $1, $2, $3, true, now()::TIMESTAMP
ON CONFLICT (server_id, database_name)
DO UPDATE SET expected_state = EXCLUDED.expected_state, is_user_override = true, updated_at = now()::TIMESTAMP";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = databaseName });
        command.Parameters.Add(new DuckDBParameter { Value = expectedState });
        await command.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Re-baselines a database: sets its expected state to its current EFFECTIVE state and clears the
    /// user-override flag, so the alert stops firing for a state the operator has accepted as the new normal.
    /// </summary>
    public async Task ResetDatabaseStateExpectedToCurrentAsync(int serverId, string databaseName)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = $@"
INSERT INTO config_database_state_expected (server_id, database_name, expected_state, is_user_override, updated_at)
SELECT $1, $2, {EffectiveStateSql}, false, now()::TIMESTAMP
FROM database_states ds
WHERE ds.server_id = $1
AND   ds.database_name = $2
AND   ds.collection_time = (SELECT MAX(collection_time) FROM database_states WHERE server_id = $1)
ON CONFLICT (server_id, database_name)
DO UPDATE SET expected_state = EXCLUDED.expected_state, is_user_override = false, updated_at = now()::TIMESTAMP";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = databaseName });
        await command.ExecuteNonQueryAsync();
    }
}

/// <summary>
/// One row of the database-state override editor: a database's current effective state alongside its
/// expected state (auto-seeded baseline or user override).
/// </summary>
public sealed class DatabaseStateExpectedRow
{
    public string DatabaseName { get; set; } = "";
    public string CurrentState { get; set; } = "";
    public string ExpectedState { get; set; } = "";
    public bool IsUserOverride { get; set; }

    public bool IsIgnored => ExpectedState == PerformanceMonitor.Alerting.DatabaseStateTokens.Ignore;

    /// <summary>True when the current state differs from the expected state and the DB isn't ignored.</summary>
    public bool IsDeviating => !IsIgnored && !string.Equals(CurrentState, ExpectedState, System.StringComparison.Ordinal);
}
