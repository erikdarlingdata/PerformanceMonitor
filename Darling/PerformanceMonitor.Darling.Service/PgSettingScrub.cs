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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// One-time scrub of secrets <see cref="PgSettingRedactor"/> would now mask but an older collector build
/// stored in plain text — most notably a standby's replication password inside <c>primary_conninfo</c>
/// (#4348 S1b; S1a is the redactor and the collector wiring that stops new rows from ever carrying one).
///
/// <para><b>A background job, not a migration.</b> The ruling (issuecomment-5838869407) is explicit: this
/// reads candidate rows with a coarse SQL filter, redacts them in C# with the same
/// <see cref="PgSettingRedactor.Redact"/> the collector uses, and updates only the rows whose text actually
/// changes, by their full key, in small batches. It runs after startup, concurrently with the rest of the
/// startup sequence exactly like <c>DarlingWorker.RunMaterializationHoleRepairAsync</c> and
/// <c>RunBaselineBackfillAsync</c>, and it never holds up collection.</para>
///
/// <para><b>The marker reuses <c>collect.collector_state</c> (V44)</b> — the store's general key/value state
/// table — under the fleet-sentinel <c>server_id</c> (<see cref="DarlingObservability.FleetServerId"/>) and
/// an owner name of its own, exactly the shape <c>PgSelfAlertDeliveryStampStore</c> and
/// <c>QueryStoreBackfill</c> already use for store-wide (not per-server) one-shot state. No migration rung:
/// the table already holds one short row per (server, collector, key). The stored value is
/// <see cref="PgSettingRedactor.RulesVersion"/> as text, so a later rules change — one that would redact a
/// previously-unmasked value differently — is a version bump the marker compares against, and the scrub runs
/// again rather than trusting a stale "done".</para>
///
/// <para><b>Batches are grouped by calendar day, and every UPDATE carries that day as a literal range
/// predicate — measured, not assumed (#4348 issue comment).</b> <c>collect.pg_server_config</c> compresses
/// with <c>compress_segmentby = server_id</c> on 1-day chunks. An UPDATE that reaches a compressed chunk only
/// through a JOIN against an unnested key array, with no CONSTANT <c>collection_time</c> predicate the
/// planner can evaluate at plan time, cannot exclude any chunk — confirmed live: a 2-row batch shaped that
/// way decompressed 8,846,250 of about 9.2M tuples and tripped TimescaleDB's
/// <c>timescaledb.max_tuples_decompressed_per_dml_transaction</c> safety limit (default 100,000, error
/// 53400) almost immediately. Adding the day's literal <c>&gt;= / &lt;</c> range alongside the join — redundant
/// with the join equality, but load-bearing — lets TimescaleDB exclude every other chunk, and the same shape
/// of batch then succeeds at once. A day's candidates rarely near <see cref="MaxKeysPerUpdate"/> (a target
/// with 350 settings has at most 350 candidates on its worst day), but a day is still sub-batched past that
/// cap, so one target's answer never determines the batch size.</para>
///
/// <para><b>No explicit recompress step, and the touched chunk stays partly compressed until the standing
/// compression policy next runs.</b> Measured live on TimescaleDB 2.30.1: after an UPDATE scoped by the
/// day's range predicate, the touched chunk's <c>_timescaledb_catalog.chunk.status</c> read "compressed +
/// partial" — the chunk's <c>is_compressed</c> flag stays <c>true</c>, but the rows this batch touched moved
/// into that chunk's uncompressed heap and stay there, growing the store, until the compression policy's
/// next run recompresses them. A manual <c>compress_chunk</c> right after each batch would avoid that window,
/// but the scrub deliberately leaves it to the standing policy rather than adding its own compression step —
/// see the release note below for the space and retention consequence.</para>
///
/// <para><b>Failure is isolated and retried, never fatal.</b> Any exception — the connection, the candidate
/// read, a batch — is caught, logged once at WARNING, and the marker is left unwritten, so the next service
/// start tries again from the top. A store whose scrub cannot run degrades to the plaintext it already had,
/// never to a service that did not start.</para>
/// </summary>
public static class PgSettingScrub
{
    /// <summary>The owner name in <c>collect.collector_state</c>, deliberately not a collector definition's —
    /// the <c>QueryStoreBackfill</c> / <c>self_alert</c> precedent, so no collector's declared-key read or
    /// per-database prune can reach it.</summary>
    public const string StateCollectorName = "pg_setting_scrub";

    /// <summary>The marker's one key: the <see cref="PgSettingRedactor.RulesVersion"/> the store was last
    /// scrubbed under, as text.</summary>
    public const string RulesVersionStateKey = "rules_version";

    /// <summary>The candidate read's deadline — generous against the measured 14.6 s over a synthetic 9.2M-row
    /// / 3-target / 365-day table (#4348 issue comment), because a fleet's real store can hold far more
    /// history than that and this runs once, off the collection path.</summary>
    internal const int CandidateReadTimeoutSeconds = 300;

    /// <summary>Each day-batch UPDATE's deadline — small on purpose, since a batch is bounded to one chunk's
    /// worth of a day's candidates.</summary>
    internal const int UpdateBatchTimeoutSeconds = 60;

    /// <summary>The marker read/write's deadline — one small keyed row.</summary>
    internal const int MarkerTimeoutSeconds = 30;

    /// <summary>Sub-batch cap WITHIN a single day, so one day whose candidate count is unusually large still
    /// keeps each UPDATE's key-array small. See the type remarks for why a day is the outer grouping.</summary>
    internal const int MaxKeysPerUpdate = 500;

    /// <summary>What one run of the scrub found and did, for the caller's summary log line.</summary>
    public sealed class Summary
    {
        public static readonly Summary NoOp = new(alreadyDone: true, 0, 0, 0);

        internal Summary(bool alreadyDone, int candidatesRead, int rowsUpdated, int daysTouched)
        {
            AlreadyDone = alreadyDone;
            CandidatesRead = candidatesRead;
            RowsUpdated = rowsUpdated;
            DaysTouched = daysTouched;
        }

        /// <summary>True when the marker already named the current <see cref="PgSettingRedactor.RulesVersion"/>
        /// and the run did nothing else.</summary>
        public bool AlreadyDone { get; }

        /// <summary>Rows the coarse filter returned (an over-inclusive superset; not all of them changed).</summary>
        public int CandidatesRead { get; }

        /// <summary>Rows actually written, because at least one of <c>setting</c>/<c>boot_val</c>/<c>reset_val</c>
        /// differed from its redacted form.</summary>
        public int RowsUpdated { get; }

        /// <summary>Distinct calendar days that needed at least one UPDATE.</summary>
        public int DaysTouched { get; }
    }

    private const string MarkerGetSql = @"
SELECT state_value
FROM collect.collector_state
WHERE server_id = $1 AND collector_name = $2 AND state_key = $3";

    private const string MarkerUpsertSql = @"
INSERT INTO collect.collector_state (server_id, collector_name, state_key, state_value, updated_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (server_id, collector_name, state_key)
DO UPDATE SET state_value = EXCLUDED.state_value, updated_at = EXCLUDED.updated_at";

    /// <summary>
    /// The coarse filter: a superset of every row <see cref="PgSettingRedactor.Redact"/> could possibly
    /// change, cheap enough to run over a compressed hypertable (ILIKE substring tests, no regex). See the
    /// type remarks and <c>PgSettingRedactor</c>'s own remarks for the rule this mirrors rule-for-rule:
    /// a libpq/URI password or an assignment naming PASSWORD/PASSWD/SECRET/TOKEN anywhere in a value;
    /// <c>ssl_passphrase_command</c> by name; a dotted extension setting whose name contains one of the
    /// whole-value markers. Over-inclusive on purpose — the fine-grained decision is
    /// <see cref="PgSettingRedactor.Redact"/>, run per candidate row below.
    /// </summary>
    private const string CandidateSql = @"
SELECT server_id, collection_time, name, database_name, role_name, setting, boot_val, reset_val
FROM collect.pg_server_config
WHERE
    setting ILIKE '%password%' OR reset_val ILIKE '%password%' OR boot_val ILIKE '%password%'
    OR setting ILIKE '%passwd%' OR reset_val ILIKE '%passwd%' OR boot_val ILIKE '%passwd%'
    OR setting ILIKE '%secret%' OR reset_val ILIKE '%secret%' OR boot_val ILIKE '%secret%'
    OR setting ILIKE '%token%' OR reset_val ILIKE '%token%' OR boot_val ILIKE '%token%'
    OR setting ILIKE '%://%@%' OR reset_val ILIKE '%://%@%' OR boot_val ILIKE '%://%@%'
    OR setting ILIKE '%pass%' OR reset_val ILIKE '%pass%' OR boot_val ILIKE '%pass%'
    OR setting ILIKE '%key%' OR reset_val ILIKE '%key%' OR boot_val ILIKE '%key%'
    OR setting ILIKE '%credential%' OR reset_val ILIKE '%credential%' OR boot_val ILIKE '%credential%'
    OR setting ILIKE '%pwd%' OR reset_val ILIKE '%pwd%' OR boot_val ILIKE '%pwd%'
    OR (name = 'ssl_passphrase_command' AND (setting <> '' OR boot_val <> '' OR reset_val <> ''))
    OR (name LIKE '%.%' AND (
        name ILIKE '%password%' OR name ILIKE '%passwd%' OR name ILIKE '%passphrase%'
        OR name ILIKE '%secret%' OR name ILIKE '%salt%' OR name ILIKE '%token%' OR name ILIKE '%key%'
        OR name ILIKE '%credential%' OR name ILIKE '%pwd%' OR name ILIKE '%pass%'))";

    private const string BatchUpdateSql = @"
WITH batch AS (
    SELECT * FROM unnest($1::int[], $2::timestamp[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[], $8::text[])
        AS k(server_id, collection_time, name, database_name, role_name, new_setting, new_boot_val, new_reset_val)
)
UPDATE collect.pg_server_config t
SET setting = b.new_setting, boot_val = b.new_boot_val, reset_val = b.new_reset_val
FROM batch b
WHERE t.server_id = b.server_id
AND   t.collection_time = b.collection_time
AND   t.name = b.name
AND   t.database_name IS NOT DISTINCT FROM b.database_name
AND   t.role_name IS NOT DISTINCT FROM b.role_name
AND   t.collection_time >= $9 AND t.collection_time < $10
AND   t.server_id = $11";

    private sealed record CandidateRow(
        int ServerId, DateTime CollectionTime, string Name, string? DatabaseName, string? RoleName,
        string? Setting, string? BootVal, string? ResetVal);

    /// <summary>
    /// Runs the scrub once against <paramref name="postgres"/>, or returns <see cref="Summary.NoOp"/>
    /// at once if the marker already names the current rules version. Propagates a failure to the caller,
    /// which isolates it exactly like <c>DarlingWorker.RunMaterializationHoleRepairAsync</c> — see the type
    /// remarks.
    /// </summary>
    public static async Task<Summary> RunAsync(NpgsqlDataSource postgres, ILogger? logger, CancellationToken cancellationToken)
    {
        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);

        var currentVersion = PgSettingRedactor.RulesVersion.ToString(CultureInfo.InvariantCulture);
        var marker = await ReadMarkerAsync(connection, cancellationToken);
        if (marker == currentVersion)
        {
            return Summary.NoOp;
        }

        var candidates = new List<CandidateRow>();
        await using (var read = new NpgsqlCommand(CandidateSql, connection) { CommandTimeout = CandidateReadTimeoutSeconds })
        await using (var reader = await read.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
        if (reader.IsDBNull(2))
                {
                    /* V102's name column is nullable; a NULL name cannot be redacted (PgSettingRedactor keys
                       its decision off the name) and cannot be batched (it is a join/predicate key), so skip
                       it rather than throw and fail the whole scrub at every start (R1 L5). */
                    continue;
                }

                candidates.Add(new CandidateRow(
                    reader.GetInt32(0),
                    reader.GetDateTime(1),
                    reader.GetString(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3),
                    reader.IsDBNull(4) ? null : reader.GetString(4),
                    reader.IsDBNull(5) ? null : reader.GetString(5),
                    reader.IsDBNull(6) ? null : reader.GetString(6),
                    reader.IsDBNull(7) ? null : reader.GetString(7)));
            }
        }

/* Grouped by (server, day), not day alone — see the type remarks' H1 fix. A day-only batch joins many
           servers' rows through an unnested array with no constant server_id predicate, so TimescaleDB cannot
           exclude any other server's rows in that day's chunk and decompresses the whole chunk for every
           server on the fleet; past about 11 targets that blows the default
           timescaledb.max_tuples_decompressed_per_dml_transaction. A constant server_id predicate per batch,
           alongside the day range, lets the planner exclude everything but this one server's segment. */
        var byServerDay = new SortedDictionary<(int ServerId, DateTime Day), List<CandidateRow>>();
        foreach (var c in candidates)
        {
            var key = (c.ServerId, c.CollectionTime.Date);
            if (!byServerDay.TryGetValue(key, out var list))
            {
                list = new List<CandidateRow>();
                byServerDay[key] = list;
            }
            list.Add(c);
        }

        var rowsUpdated = 0;
        var changedRowCount = 0;
        var daysTouched = new HashSet<DateTime>();
        foreach (var ((serverId, day), dayCandidates) in byServerDay)
        {
            var changed = new List<CandidateRow>();
            var newSettings = new List<string?>();
            var newBootVals = new List<string?>();
            var newResetVals = new List<string?>();

            foreach (var c in dayCandidates)
            {
                var newSetting = PgSettingRedactor.Redact(c.Name, c.Setting);
                var newBootVal = PgSettingRedactor.Redact(c.Name, c.BootVal);
                var newResetVal = PgSettingRedactor.Redact(c.Name, c.ResetVal);

                if (newSetting == c.Setting && newBootVal == c.BootVal && newResetVal == c.ResetVal)
                {
                    continue;
                }

                changed.Add(c);
                newSettings.Add(newSetting);
                newBootVals.Add(newBootVal);
                newResetVals.Add(newResetVal);
            }

            if (changed.Count == 0)
            {
                continue;
            }

            daysTouched.Add(day);
            changedRowCount += changed.Count;
            for (var i = 0; i < changed.Count; i += MaxKeysPerUpdate)
            {
                var take = Math.Min(MaxKeysPerUpdate, changed.Count - i);
                rowsUpdated += await RunBatchAsync(
                    connection, changed, newSettings, newBootVals, newResetVals, i, take, day, serverId, cancellationToken);
            }
        }

        if (rowsUpdated == changedRowCount)
        {
            await WriteMarkerAsync(connection, currentVersion, cancellationToken);
        }
        else
        {
            /* R1 L4: a systematic key mismatch would otherwise mark the store scrubbed while plaintext rows
               remain. Skipping the marker means the next start re-reads and re-tries; a day dropped by
               retention mid-run (benign) just means those rows are gone by then. */
            logger?.LogWarning(
                "pg_setting_scrub: updated {RowsUpdated} of {ChangedRowCount} changed rows; leaving the marker unwritten so the next start retries",
                rowsUpdated, changedRowCount);
        }

        return new Summary(alreadyDone: false, candidates.Count, rowsUpdated, daysTouched.Count);
    }

    private static async Task<int> RunBatchAsync(
        NpgsqlConnection connection, List<CandidateRow> changed, List<string?> newSettings, List<string?> newBootVals,
        List<string?> newResetVals, int offset, int count, DateTime day, int serverId, CancellationToken cancellationToken)
    {
        var serverIds = new int[count];
        var times = new DateTime[count];
        var names = new string[count];
        var databaseNames = new string?[count];
        var roleNames = new string?[count];
        var settings = new string?[count];
        var bootVals = new string?[count];
        var resetVals = new string?[count];

        for (var j = 0; j < count; j++)
        {
            var c = changed[offset + j];
            serverIds[j] = c.ServerId;
            times[j] = c.CollectionTime;
            names[j] = c.Name;
            databaseNames[j] = c.DatabaseName;
            roleNames[j] = c.RoleName;
            settings[j] = newSettings[offset + j];
            bootVals[j] = newBootVals[offset + j];
            resetVals[j] = newResetVals[offset + j];
        }

        await using var update = new NpgsqlCommand(BatchUpdateSql, connection) { CommandTimeout = UpdateBatchTimeoutSeconds };
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = serverIds });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = times });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = names });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = databaseNames });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = roleNames });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = settings });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = bootVals });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = resetVals });
        /* The literal day range is what lets TimescaleDB exclude every chunk but this one — see the type
           remarks. Redundant with the join equality on collection_time, and load-bearing anyway. */
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = day });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = day.AddDays(1) });
        /* The constant server_id predicate is what lets TimescaleDB exclude every other server's segment in
           this day's chunk — see the H1 fix note above RunAsync's grouping. */
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });

        return await update.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<string?> ReadMarkerAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(MarkerGetSql, connection) { CommandTimeout = MarkerTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DarlingObservability.FleetServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = StateCollectorName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = RulesVersionStateKey });

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result as string;
    }

    private static async Task WriteMarkerAsync(NpgsqlConnection connection, string rulesVersion, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(MarkerUpsertSql, connection) { CommandTimeout = MarkerTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DarlingObservability.FleetServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = StateCollectorName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = RulesVersionStateKey });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = rulesVersion });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Timestamp,
            /* Naive UTC, Kind-Unspecified — the product-wide PG `timestamp` discipline (PgAlertStateStore.
               NaiveUtcNow, PgSelfAlertDeliveryStampStore.RecordDeliveredAtUtcAsync). */
            Value = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified),
        });

        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
