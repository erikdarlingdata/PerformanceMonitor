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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The one copy of the database-state deviation read (#7647). It moved here from <c>DarlingAlertReadAdapter</c> so the Database State alert, the
/// fleet roll-up (<c>get_fleet_overview</c> and the Fleet page) and the Viewer's server card all run the same statement, and so carry the same
/// two-sample rule, expected states and <c>(ignore)</c> setting. The adapter's <c>DatabaseStateDeviationsSql</c> and
/// <c>DatabaseStateDeviationsSinceSql</c> are these two.
/// </summary>
public static class DarlingDatabaseStateDeviations
{
    /// <summary>
    /// The databases whose state deviates from their expected state in BOTH of the two most recent
    /// collections (a two-sample rule that absorbs restart transients — RECOVERY_PENDING / RECOVERING — and
    /// a standby secondary's per-restore RESTORING flicker), plus databases with no baseline yet whose
    /// effective state is critical in both samples (a pending critical first observation). Uses the
    /// effective state (STANDBY for a log-shipping secondary, else state_desc). Skips the "(ignore)"
    /// sentinel; each row carries current + expected (expected is empty for a pending row). Lite's DuckDB
    /// read ported to Postgres. $1 server_id.
    /// </summary>
    public const string Sql = $@"
WITH newest AS (
    SELECT MAX(collection_time) AS t FROM database_states WHERE server_id = $1
),
prev AS (
    SELECT MAX(collection_time) AS t FROM database_states
    WHERE server_id = $1 AND collection_time < (SELECT t FROM newest)
),
latest AS (
    SELECT ds.database_name, CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END AS eff
    FROM database_states ds
    WHERE ds.server_id = $1 AND ds.collection_time = (SELECT t FROM newest)
),
previous AS (
    SELECT ds.database_name, CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END AS eff
    FROM database_states ds
    WHERE ds.server_id = $1 AND ds.collection_time = (SELECT t FROM prev)
),{Tail}";

    /// <summary>
    /// #7017: <see cref="Sql"/> bounded below by <c>collection_time</c> $2. The floor is the PREVIOUS
    /// collection's time (the alert reader's newest-row floor), one collection back from the newest, never the newest itself: the
    /// two-sample rule reads the newest collection AND the one before it, so a floor at the newest row would hide the row that
    /// holds the previous state and turn every deviation into "no deviation". With the floor at the previous collection both
    /// samples stay visible whether or not a new collection arrived since the last read: a new one makes the old newest the
    /// previous, which is above the floor, and none leaves the same pair. Same rows, same order.
    /// </summary>
    public const string SinceSql = $@"
WITH newest AS (
    SELECT MAX(collection_time) AS t FROM database_states WHERE server_id = $1 AND collection_time >= $2
),
prev AS (
    SELECT MAX(collection_time) AS t FROM database_states
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < (SELECT t FROM newest)
),
latest AS (
    SELECT ds.database_name, CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END AS eff
    FROM database_states ds
    WHERE ds.server_id = $1 AND ds.collection_time >= $2 AND ds.collection_time = (SELECT t FROM newest)
),
previous AS (
    SELECT ds.database_name, CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END AS eff
    FROM database_states ds
    WHERE ds.server_id = $1 AND ds.collection_time >= $2 AND ds.collection_time = (SELECT t FROM prev)
),{Tail}";

    /// <summary>
    /// The deviation rows and the final select the two reads above share. #7017: the read now ALWAYS returns at least one row
    /// (a row whose <c>database_name</c> is NULL when nothing deviates), carrying the previous and newest collection times, because
    /// the floor for the next read comes from those times and a tick with no deviation, the usual one, would otherwise leave it
    /// nothing to take a floor from. A deviation row never has a NULL name (the join on the name drops those), so the reader treats
    /// a NULL name as "no deviation" and skips it.
    /// </summary>
    private const string Tail = $@"
dev AS (
    SELECT l.database_name, l.eff,
           COALESCE(e.expected_state, '') AS expected_state,
           COALESCE(e.last_alerted_state, '') AS last_alerted_state
    FROM latest l
    JOIN previous p
      ON p.database_name = l.database_name
    LEFT JOIN config.database_state_expected e
      ON  e.server_id = $1
      AND e.database_name = l.database_name
    WHERE (e.expected_state IS NULL
            AND l.eff IN ({DatabaseStateTokens.CriticalSqlList})
            AND p.eff IN ({DatabaseStateTokens.CriticalSqlList}))
       OR (e.expected_state IS NOT NULL AND e.expected_state <> '(ignore)'
            AND l.eff IS DISTINCT FROM e.expected_state
            AND p.eff IS DISTINCT FROM e.expected_state)
)
SELECT d.database_name, d.eff, d.expected_state, d.last_alerted_state,
       (SELECT t FROM prev) AS previous_collection_time,
       (SELECT t FROM newest) AS newest_collection_time
FROM (SELECT 1) AS one
LEFT JOIN dev AS d ON true
ORDER BY d.database_name";

    /// <summary>How far back a database-state snapshot may be and still count for the fleet card and the Viewer's server card (#7647). It is the floor of
    /// <see cref="SinceSql"/>, so a server whose database_states collector has not written in this long reads as not collected, and the chunks the
    /// read touches stay the newest few. The collector runs every 1 to 30 minutes, so two snapshots (the two-sample rule) fit well inside it.</summary>
    public static readonly TimeSpan Freshness = TimeSpan.FromHours(2);

    /// <summary>
    /// Reads one server's deviations for the fleet card (#7647), through the SAME statement the Database State alert runs (<see cref="SinceSql"/>), so the
    /// two-sample rule, the expected states and the <c>(ignore)</c> setting carry over. A server with no snapshot inside <see cref="Freshness"/> comes back
    /// <see cref="DatabaseStateRead.Collected"/> false: not collected, never healthy and never zero.
    /// </summary>
    public static async Task<DatabaseStateRead> ReadAsync(
        NpgsqlConnection connection, int serverId, DateTime nowUtc, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        var deviations = new List<DatabaseStateDeviation>();
        var collected = false;
        using var command = new NpgsqlCommand(SinceSql, connection) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Timestamp,
            Value = DateTime.SpecifyKind(nowUtc - Freshness, DateTimeKind.Unspecified)
        });
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            /* The read always returns a row carrying the times (database_name NULL when nothing deviates); a NULL newest time is no snapshot in the window. */
            collected = collected || !reader.IsDBNull(5);
            if (reader.IsDBNull(0))
            {
                continue;
            }

            deviations.Add(new DatabaseStateDeviation(
                reader.GetString(0),
                reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                reader.IsDBNull(2) ? string.Empty : reader.GetString(2)));
        }

        return new DatabaseStateRead(collected, deviations);
    }

    /// <summary>Which of the deviations the alert grades Critical (<see cref="DatabaseStateTokens.SeverityFor"/>), in the order the read returned them.</summary>
    public static IReadOnlyList<DatabaseStateDeviation> Critical(IReadOnlyList<DatabaseStateDeviation> deviations) =>
        deviations.Where(d => DatabaseStateTokens.SeverityFor(d.State) == AlertSeverityLevel.Critical).ToList();

    /// <summary>
    /// The Needs attention reason clause for the database-state tile, said the same on the web, the Viewer and the MCP tool. A Critical state bands the
    /// server, so it leads and says so ("Database state critical (Sales RECOVERY_PENDING, Orders SUSPECT and 2 more)"). Any other deviation reads
    /// "Database state OFFLINE: Archive". At most <see cref="NamesShown"/> databases are named, then "and N more".
    /// </summary>
    public static string Reason(IReadOnlyList<DatabaseStateDeviation> deviations)
    {
        var critical = Critical(deviations);
        var ordered = critical.Concat(deviations.Where(d => !critical.Contains(d))).ToList();
        var named = ordered.Take(NamesShown).Select(d => d.Database + " " + d.State).ToList();
        var more = ordered.Count - named.Count;
        var list = string.Join(", ", named) + (more > 0 ? string.Create(CultureInfo.InvariantCulture, $" and {more} more") : string.Empty);
        return (critical.Count > 0 ? "Database state critical (" : "Database state deviates (") + list + ")";
    }

    /// <summary>How many database names the reason and the tile name before "and N more".</summary>
    public const int NamesShown = 3;
}

/// <summary>One database whose state is not its expected state in both of the two newest snapshots (#7647). <see cref="State"/> is the effective state
/// (STANDBY for a log-shipping secondary), <see cref="Expected"/> is empty for a pending critical first observation.</summary>
public sealed record DatabaseStateDeviation(string Database, string State, string Expected);

/// <summary>What the deviation read found for one server. <see cref="Collected"/> false means no snapshot inside the freshness window: the state of its
/// databases is unknown, which is not the same as none deviating.</summary>
public sealed record DatabaseStateRead(bool Collected, IReadOnlyList<DatabaseStateDeviation> Deviations);
