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
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Alerting;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Postgres-backed <see cref="IAlertStateStore"/> over the V3 <c>config_edge_trigger_watermarks</c>
/// table — the Darling twin of Lite's <c>DuckDbAlertHistoryStore</c> watermark methods, with
/// DuckDB's <c>INSERT OR REPLACE</c> upsert expressed as <c>INSERT ... ON CONFLICT ... DO UPDATE</c>
/// on the same (server_id, metric_name) primary key. Count metrics (blocking/deadlock) live in
/// the INTEGER <c>watermark</c> column with <c>watermark_time</c> NULL; the failed-job metric
/// reserves the 'Failed Agent Job' row, keeps <c>watermark</c> at 0 (the column is NOT NULL) and
/// stores the SERVER-LOCAL run time in <c>watermark_time</c> — Lite's exact row shapes, so a
/// future cross-store viewer reads both identically. All methods are failure-isolated like Lite's
/// (a broken store logs and degrades: loads return null, saves drop the write — the in-memory
/// watermark still gates the current process). <c>serverKey</c> is the storage-name hash string,
/// parsed to the <c>server_id</c> int (the DarlingAlertReadAdapter convention).
/// </summary>
public sealed class PgAlertStateStore : IAlertStateStore
{
    /* Lite's reserved failed-job row key — DuckDbAlertHistoryStore.FailedJobWatermarkMetric. */
    private const string FailedJobWatermarkMetric = "Failed Agent Job";

    private readonly NpgsqlDataSource _postgres;
    private readonly ILogger? _logger;

    public PgAlertStateStore(NpgsqlDataSource postgres, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _logger = logger;
    }

    public async Task<int?> LoadEdgeTriggerWatermarkAsync(string serverKey, string metricName)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync();
            using var command = new NpgsqlCommand(@"
SELECT watermark
FROM config_edge_trigger_watermarks
WHERE server_id = $1
AND   metric_name = $2
AND   watermark_time IS NULL", connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
            command.Parameters.AddWithValue(ParseServerKey(serverKey));
            command.Parameters.AddWithValue(metricName);

            var result = await command.ExecuteScalarAsync();
            if (result == null || result == DBNull.Value)
            {
                return null;
            }

            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }
        catch (Exception ex)
        {
            _logger?.LogError("Could not load edge-trigger watermark ({Metric}): {Message}", metricName, ex.Message);
            return null;
        }
    }

    public async Task SaveEdgeTriggerWatermarkAsync(string serverKey, string metricName, int watermark)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync();
            /* Full-row replace semantics, matching Lite's INSERT OR REPLACE: a count row always
               carries watermark_time NULL. */
            using var command = new NpgsqlCommand(@"
INSERT INTO config_edge_trigger_watermarks (server_id, metric_name, watermark, watermark_time, updated_at)
VALUES ($1, $2, $3, NULL, $4)
ON CONFLICT (server_id, metric_name) DO UPDATE SET
    watermark = EXCLUDED.watermark,
    watermark_time = NULL,
    updated_at = EXCLUDED.updated_at", connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
            command.Parameters.AddWithValue(ParseServerKey(serverKey));
            command.Parameters.AddWithValue(metricName);
            command.Parameters.AddWithValue(watermark);
            command.Parameters.AddWithValue(NaiveUtcNow());

            await command.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger?.LogError("Could not persist edge-trigger watermark ({Metric}): {Message}", metricName, ex.Message);
        }
    }

    public async Task<DateTime?> LoadFailedJobWatermarkAsync(string serverKey)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync();
            using var command = new NpgsqlCommand(@"
SELECT watermark_time
FROM config_edge_trigger_watermarks
WHERE server_id = $1
AND   metric_name = $2
AND   watermark_time IS NOT NULL", connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
            command.Parameters.AddWithValue(ParseServerKey(serverKey));
            command.Parameters.AddWithValue(FailedJobWatermarkMetric);

            var result = await command.ExecuteScalarAsync();
            if (result == null || result == DBNull.Value)
            {
                return null;
            }

            /* Returned in its native server-local basis — NEVER coerced to UTC (the
               IAlertStateStore contract; it compares against FailedJobInfo.RunDateTime). */
            return Convert.ToDateTime(result, CultureInfo.InvariantCulture);
        }
        catch (Exception ex)
        {
            _logger?.LogError("Could not load failed-job watermark: {Message}", ex.Message);
            return null;
        }
    }

    public async Task SaveFailedJobWatermarkAsync(string serverKey, DateTime watermark)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync();
            /* watermark (the INTEGER count column) is unused for the time-based failed-job row;
               it is non-nullable, so write 0 — Lite's exact shape. The value is server-local;
               only the Kind flag is normalized. The Kind matters, but not the way this comment
               used to claim: Npgsql does NOT reject Kind=Utc against `timestamp` on this version
               — it infers timestamptz and PostgreSQL casts into the SERVER'S zone, silently
               storing a value offset from every naive-UTC timestamp in the store (measured at
               exactly the server's UTC offset during #1969's review). The false 'it throws'
               claim here misled two reviewers in one night; the real failure mode is quiet
               zone-shift, which only a read-back value assertion catches. */
            using var command = new NpgsqlCommand(@"
INSERT INTO config_edge_trigger_watermarks (server_id, metric_name, watermark, watermark_time, updated_at)
VALUES ($1, $2, 0, $3, $4)
ON CONFLICT (server_id, metric_name) DO UPDATE SET
    watermark = 0,
    watermark_time = EXCLUDED.watermark_time,
    updated_at = EXCLUDED.updated_at", connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
            command.Parameters.AddWithValue(ParseServerKey(serverKey));
            command.Parameters.AddWithValue(FailedJobWatermarkMetric);
            command.Parameters.AddWithValue(DateTime.SpecifyKind(watermark, DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(NaiveUtcNow());

            await command.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger?.LogError("Could not persist failed-job watermark: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// #2166: stamps the alerted state onto the database's row in <c>config.database_state_expected</c>,
    /// the table that already holds this alert's per-database config.
    ///
    /// <para>UPDATE, never upsert. An INSERT here would have to supply <c>expected_state</c> (NOT NULL), and
    /// the only value available is the state being alerted ON — so a database first observed SUSPECT would
    /// get SUSPECT written as its accepted baseline. It would then stop deviating, drop out of the deviation
    /// query, be read as RECOVERED (firing a false "resolved" on a still-corrupt database), and never alert
    /// again. The seed logic refuses to baseline any of <see cref="DatabaseStateTokens.NeverBaselinedSqlList"/>
    /// for exactly this reason; this write must not do behind its back what it declines to do in front.</para>
    ///
    /// <para>Nothing is lost by skipping the no-row case: a database with no baseline row was first observed
    /// in an integrity state, and those states are never edge-suppressed (RepeatsAreNoise is false for them),
    /// so the memory this writes is never consulted for them. The parked-database case that NEEDS the memory
    /// always has a row — it was baselined when the database was still healthy.</para>
    /// </summary>
    public async Task SaveDatabaseStateAlertedAsync(string serverKey, string databaseName, string effectiveState)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync();
            using var command = new NpgsqlCommand(@"
UPDATE config.database_state_expected
SET last_alerted_state = $3,
    last_alerted_at = (now() AT TIME ZONE 'UTC')
WHERE server_id = $1
AND   database_name = $2", connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
            command.Parameters.AddWithValue(ParseServerKey(serverKey));
            command.Parameters.AddWithValue(databaseName);
            command.Parameters.AddWithValue(effectiveState);
            await command.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            /* Same posture as the watermark writes: a failed stamp costs a duplicate alert next cycle,
               never a missed one, so it logs and continues rather than failing the sweep. */
            _logger?.LogWarning("Could not record the alerted database state for {Database}: {Message}", databaseName, ex.Message);
        }
    }

    /// <summary>
    /// #2166: forgets the alerted state when a database returns to its expected one, so the NEXT episode is
    /// a fresh transition. Without this the memory is permanent: park a database OFFLINE (alerts), bring it
    /// back (resolves), park it OFFLINE again weeks later — the stale <c>last_alerted_state</c> still reads
    /// OFFLINE, the repeat is judged already-announced, and the second parking never alerts at all. Edge
    /// triggering has to reset on the falling edge or it only ever fires once per database, forever.
    /// </summary>
    public async Task ClearDatabaseStateAlertedAsync(string serverKey, string databaseName)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync();
            using var command = new NpgsqlCommand(@"
UPDATE config.database_state_expected
SET last_alerted_state = NULL,
    last_alerted_at = NULL
WHERE server_id = $1
AND   database_name = $2", connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
            command.Parameters.AddWithValue(ParseServerKey(serverKey));
            command.Parameters.AddWithValue(databaseName);
            await command.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            /* A failed clear costs a MISSED alert on the next episode rather than a duplicate, so it is the
               more consequential of the two failures — logged at warning with the database named, same as
               its sibling, because the sweep must still finish for every other database. */
            _logger?.LogWarning("Could not clear the alerted database state for {Database}: {Message}", databaseName, ex.Message);
        }
    }

    /// <summary>
    /// #2216: loads the per-fingerprint occurrence accounting for one server/metric from the V61
    /// <c>config.incident_occurrences</c> table.
    ///
    /// <para>Returns an EMPTY map on any failure rather than the rows it managed to read. A partial map is
    /// the worst of the three outcomes: the fingerprints that made it keep accumulating while the ones that
    /// did not silently restart their totals mid-incident, so the same alert reports some counters
    /// continuing and others reset with a fresh start time. Empty is at least uniform — every fingerprint
    /// reads as new, totals equal the window counts, which is the documented degradation.</para>
    /// </summary>
    public async Task<IReadOnlyDictionary<string, IncidentOccurrenceState>> LoadIncidentOccurrencesAsync(
        string serverKey, string metricName)
    {
        var states = new Dictionary<string, IncidentOccurrenceState>(StringComparer.Ordinal);

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync();
            using var command = new NpgsqlCommand(@"
SELECT dedup_key, total_occurrences, observed_window_count, incident_started_at, last_observed_at
FROM config.incident_occurrences
WHERE server_id = $1
AND   metric_name = $2", connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
            command.Parameters.AddWithValue(ParseServerKey(serverKey));
            command.Parameters.AddWithValue(metricName);

            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                /* The two timestamps come back Kind=Unspecified from a `timestamp` column, and they are
                   naive UTC by the store-wide convention. Deliberately NOT coerced to Kind=Utc: the
                   accumulator only ever subtracts them from its own UTC clock, which is plain arithmetic
                   on both Kinds, and a ToUniversalTime() "fix" here would shift every value by the host's
                   offset and make live incidents look stale. */
                states[reader.GetString(0)] = new IncidentOccurrenceState(
                    reader.GetInt64(1),
                    reader.GetInt32(2),
                    reader.GetDateTime(3),
                    reader.GetDateTime(4));
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError("Could not load incident occurrences ({Metric}): {Message}", metricName, ex.Message);
            return new Dictionary<string, IncidentOccurrenceState>(StringComparer.Ordinal);
        }

        return states;
    }

    /// <summary>
    /// #2216: REPLACES the persisted occurrence set for one server/metric — upserts every row in
    /// <paramref name="states"/> and deletes the (server, metric)'s rows that are not in it. An empty map
    /// clears the metric, which is how the falling edge is recorded.
    ///
    /// <para>ONE transaction, because the delete and the upsert are two halves of one replacement: a delete
    /// that commits without its upsert would zero live counters, and an upsert without its delete strands
    /// finished fingerprints for the accumulator's staleness horizon to clean up later. The horizon exists
    /// for crashes, not as cover for a half-applied write.</para>
    /// </summary>
    public async Task SaveIncidentOccurrencesAsync(
        string serverKey, string metricName, IReadOnlyDictionary<string, IncidentOccurrenceState> states)
    {
        if (states is null)
        {
            return;
        }

        try
        {
            int serverId = ParseServerKey(serverKey);

            var dedupKeys = new string[states.Count];
            var totals = new long[states.Count];
            var observed = new int[states.Count];
            var started = new DateTime[states.Count];
            var lastObserved = new DateTime[states.Count];

            int n = 0;
            foreach (var entry in states)
            {
                dedupKeys[n] = entry.Key;
                totals[n] = entry.Value.TotalOccurrences;
                observed[n] = entry.Value.ObservedWindowCount;
                started[n] = Naive(entry.Value.IncidentStartedUtc);
                lastObserved[n] = Naive(entry.Value.LastObservedUtc);
                n++;
            }

            await using var connection = await _postgres.OpenConnectionAsync();
            await using var transaction = await connection.BeginTransactionAsync();

            /* `<> ALL` over an EMPTY array is true for every row, so the no-states case degenerates to
               "delete them all" with no special-casing — the clear path and the replace path are one
               statement. It relies on the array being NULL-free, since `<> ALL` over an array containing
               NULL yields NULL and deletes nothing: the keys come from the accumulator's state map, which
               never admits a blank or null fingerprint (it passes those incidents through unkeyed). The
               explicit cast is here because the bind is the only thing telling PostgreSQL the element type,
               and an inference failure on this shape is a RUNTIME error, not a compile one. */
            using (var prune = new NpgsqlCommand(@"
DELETE FROM config.incident_occurrences
WHERE server_id = $1
AND   metric_name = $2
AND   dedup_key <> ALL($3::text[])", connection, transaction) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds })
            {
                prune.Parameters.AddWithValue(serverId);
                prune.Parameters.AddWithValue(metricName);
                prune.Parameters.AddWithValue(dedupKeys);
                await prune.ExecuteNonQueryAsync();
            }

            if (states.Count > 0)
            {
                using var upsert = new NpgsqlCommand(@"
INSERT INTO config.incident_occurrences
    (server_id, metric_name, dedup_key, total_occurrences, observed_window_count, incident_started_at, last_observed_at)
SELECT $1, $2, d.dedup_key, d.total_occurrences, d.observed_window_count, d.incident_started_at, d.last_observed_at
FROM unnest($3::text[], $4::bigint[], $5::integer[], $6::timestamp[], $7::timestamp[])
     AS d(dedup_key, total_occurrences, observed_window_count, incident_started_at, last_observed_at)
ON CONFLICT (server_id, metric_name, dedup_key) DO UPDATE SET
    total_occurrences = EXCLUDED.total_occurrences,
    observed_window_count = EXCLUDED.observed_window_count,
    incident_started_at = EXCLUDED.incident_started_at,
    last_observed_at = EXCLUDED.last_observed_at", connection, transaction) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
                upsert.Parameters.AddWithValue(serverId);
                upsert.Parameters.AddWithValue(metricName);
                upsert.Parameters.AddWithValue(dedupKeys);
                upsert.Parameters.AddWithValue(totals);
                upsert.Parameters.AddWithValue(observed);
                upsert.Parameters.AddWithValue(started);
                upsert.Parameters.AddWithValue(lastObserved);
                await upsert.ExecuteNonQueryAsync();
            }

            await transaction.CommitAsync();
        }
        catch (Exception ex)
        {
            /* Same posture as the watermark writes: a dropped write costs accuracy on the NEXT delivery's
               total (the fingerprint reads as new and restarts, with a fresh start time saying so), never
               a missed or duplicated alert. The alert itself has already been decided by the gate. */
            _logger?.LogError("Could not persist incident occurrences ({Metric}): {Message}", metricName, ex.Message);
        }
    }

    /// <summary>Naive-UTC now, Kind-Unspecified — the product's PG timestamp discipline.</summary>
    private static DateTime NaiveUtcNow() =>
        DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

    /// <summary>
    /// Kind-stripped for a `timestamp` bind. Npgsql does NOT reject Kind=Utc on this version — it infers
    /// timestamptz and PostgreSQL casts into the SERVER's zone, silently storing a value offset from every
    /// other naive-UTC timestamp in the store (see the failed-job write's remarks; that quiet zone-shift
    /// misled two reviewers in one night).
    /// </summary>
    private static DateTime Naive(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private static int ParseServerKey(string serverKey) =>
        int.Parse(serverKey, CultureInfo.InvariantCulture);
}
