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
/// <para><b>Batches are grouped by (server, day), and every UPDATE carries both as literal predicates —
/// measured, not assumed (#4348 issue comment; the server predicate is round 2's H1 fix).</b>
/// <c>collect.pg_server_config</c> compresses with <c>compress_segmentby = server_id</c> on 1-day chunks. An
/// UPDATE that reaches a compressed chunk only through a JOIN against an unnested key array, with no CONSTANT
/// <c>collection_time</c>/<c>server_id</c> predicate the planner can evaluate at plan time, cannot exclude any
/// chunk or segment — confirmed live: a 2-row batch shaped that way decompressed 8,846,250 of about 9.2M
/// tuples and tripped TimescaleDB's <c>timescaledb.max_tuples_decompressed_per_dml_transaction</c> safety
/// limit (default 100,000, error 53400) almost immediately; day-only grouping repeated the same failure once
/// a fleet passed about 11 targets, because every target's rows for that day still shared the chunk. Adding
/// the day's literal <c>&gt;= / &lt;</c> range AND a constant <c>server_id</c> predicate alongside the join —
/// redundant with the join equality, but load-bearing — lets TimescaleDB exclude every other chunk and every
/// other server's segment, and the same shape of batch then succeeds at once. A target's candidates rarely
/// near <see cref="MaxKeysPerUpdate"/> (a target with 350 settings has at most 350 candidates on its worst
/// day), but one (server, day) group is still sub-batched past that cap, so one target's answer never
/// determines the batch size.</para>
///
/// <para><b>No explicit recompress step, and the touched chunk stays partly compressed until the standing
/// compression policy next runs.</b> Measured live on TimescaleDB 2.30.1: after an UPDATE scoped by the
/// day's range predicate, the touched chunk's <c>_timescaledb_catalog.chunk.status</c> read "compressed +
/// partial" — the chunk's <c>is_compressed</c> flag stays <c>true</c>, but the rows this batch touched moved
/// into that chunk's uncompressed heap and stay there, growing the store, until the compression policy's
/// next run recompresses them. A manual <c>compress_chunk</c> right after each batch would avoid that window,
/// but the scrub deliberately leaves it to the standing policy rather than adding its own compression step —
/// see the CHANGELOG entry for #4351 for the space and retention consequence.</para>
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

    /// <summary>When one (server, day) group's coarse candidate count passes this, the day is processed as
    /// 24 hour slices of <c>collection_time</c> instead of one range covering the whole day. The type remarks'
    /// own estimate puts an hourly-cadence day at about 8,000 candidate rows and a 1-minute-cadence day at
    /// about 500,000; this threshold sits comfortably above the former and well below the latter, so normal
    /// cadences take the single-range path and only a materially faster cadence — the one that risks a
    /// day's first UPDATE decompressing enough of the chunk to run past <see cref="UpdateBatchTimeoutSeconds"/>
    /// — gets sliced. Slicing bounds how much of the day's compressed segment any one transaction touches,
    /// so a restart after a mid-day failure resumes at the next unfinished hour rather than re-attempting the
    /// whole day's decompression from scratch.</summary>
    internal const int HourSliceCandidateThreshold = 20_000;

    /// <summary>Test-only seam: overrides <see cref="HourSliceCandidateThreshold"/> so a live test can force
    /// the hour-slicing path with a data volume far smaller than the real threshold.</summary>
    internal static int? TestOnlyHourSliceCandidateThresholdOverride;

    /// <summary>Test-only seam: when set, overrides <see cref="UpdateBatchTimeoutSeconds"/> for the UPDATE
    /// command (not the SET LOCAL) so a live test can force a client-side command timeout without needing a
    /// data volume that would actually run 60 seconds.</summary>
    internal static int? TestOnlyUpdateCommandTimeoutSecondsOverride;

    /// <summary>Test-only seam: when set, a <c>pg_sleep</c> for this many seconds runs, under
    /// <see cref="TestOnlyUpdateCommandTimeoutSecondsOverride"/>'s timeout, right before each batch's
    /// UPDATE — a genuine client-side command timeout against a real (slow) server command, not a thrown
    /// stand-in.</summary>
    internal static double? TestOnlyPreUpdateDelaySeconds;

    /// <summary>Test-only seam: when set, <see cref="TestOnlyPreUpdateDelaySeconds"/> only applies to this
    /// one server id's batches, so a multi-server live test can force a timeout on ONE target without
    /// stalling every other target's batch too.</summary>
    internal static int? TestOnlyPreUpdateDelayServerId;

    /// <summary>Test-only seam: when true, <see cref="TestOnlyPreUpdateDelaySeconds"/> fires for one batch
    /// only and then clears itself, so a live test can force exactly one timeout on a target that has more
    /// than one day of work, and observe the target's remaining day(s) skipped rather than timed out again.
    /// Defaults to false, leaving every-batch delay (the prior behavior) unchanged for callers that don't set
    /// it.</summary>
    internal static bool TestOnlyPreUpdateDelayOnce;

    /// <summary>Test-only seam: invoked once after each slice's UPDATE batch commits, so a live test can force
    /// a mid-day failure after a chosen number of slices have already committed, to prove the ones already
    /// done stay done across a restart.</summary>
    internal static Action? TestOnlyAfterSliceCommitted;

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
    /// The single source of ILIKE terms <see cref="CandidateSql"/> builds its <c>setting</c>/<c>boot_val</c>/
    /// <c>reset_val</c> OR-chain from, and the same list <c>PgSettingScrubCandidateCensusTests</c> checks every
    /// masked corpus case's VALUE against, so the two can never drift out of rule-for-rule sync again (#4348).
    /// Each entry is a plain ILIKE pattern; <c>%</c>/<c>_</c> are literal wildcards, no escaping needed here
    /// because none of these terms contain a literal <c>%</c> or <c>_</c> character themselves (the
    /// percent-encoding catch-all is expressed separately in <see cref="CandidateSql"/> with its own ESCAPE
    /// clause). This is only half of the coarse filter — <c>CandidateSql</c> also selects on the setting's
    /// NAME (<c>ssl_passphrase_command</c> by exact name, or a dotted extension name containing one of
    /// <see cref="CandidateNameTerms"/>), which the census test checks separately by name.
    /// </summary>
    internal static readonly string[] CandidateLikeTerms =
    [
        "%password%", "%passwd%", "%secret%", "%token%", "%://%@%", "%pass%", "%key%",
        "%credential%", "%pwd%", "%-u %", "%--user%", "%-U %", "%sshpass%", "%sig=%",
        "%signature=%", "%-u%", "%--proxy-user%",
    ];

    /// <summary>
    /// The whole-value name markers <see cref="CandidateSql"/> tests against a dotted extension setting's
    /// NAME (not its value) — mirrors <see cref="PgSettingRedactor"/>'s own whole-value-mask decision for
    /// extension settings. Kept alongside <see cref="CandidateLikeTerms"/> so the census test can check both
    /// halves of the coarse filter against the corpus.
    /// </summary>
    internal static readonly string[] CandidateNameTerms =
    [
        "%password%", "%passwd%", "%passphrase%", "%secret%", "%salt%", "%token%", "%key%",
        "%credential%", "%pwd%", "%pass%",
    ];

    private static readonly string CandidateSql = BuildCandidateSql();

    private static string BuildCandidateSql()
    {
        var termClauses = new List<string>();
        foreach (var term in CandidateLikeTerms)
        {
            termClauses.Add(
                $"setting ILIKE '{term}' OR reset_val ILIKE '{term}' OR boot_val ILIKE '{term}'");
        }

        var terms = string.Join("\n    OR ", termClauses);

        var nameClauses = new List<string>();
        foreach (var term in CandidateNameTerms)
        {
            nameClauses.Add($"name ILIKE '{term}'");
        }

        var nameTerms = string.Join("\n        OR ", nameClauses);

        return $@"
SELECT server_id, collection_time, name, database_name, role_name, setting, boot_val, reset_val
FROM collect.pg_server_config
WHERE
    {terms}
    OR setting LIKE '%\%%' ESCAPE '\' OR reset_val LIKE '%\%%' ESCAPE '\' OR boot_val LIKE '%\%%' ESCAPE '\'
    OR (name = 'ssl_passphrase_command' AND (setting <> '' OR boot_val <> '' OR reset_val <> ''))
    OR (name LIKE '%.%' AND (
        {nameTerms}))";
    }

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
        var failedServerIds = new SortedSet<int>();
        foreach (var ((serverId, day), dayCandidates) in byServerDay)
        {
            /* M3 (b): catch per server and continue — see the type remarks. Once this server's batch has
               failed, skip its remaining days this run (they share the same failure mode) and move on to the
               next server; the marker withheld below (rowsUpdated < changedRowCount) makes the WHOLE run
               retry every server, including ones that already succeeded, on the next start — cheap and
               correct, since a server whose rows are already redacted contributes nothing on a re-run.
               failedServerIds already records the failure, so the skip does not depend on byServerDay's
               iteration order grouping a server's days contiguously. */
            if (failedServerIds.Contains(serverId))
            {
                continue;
            }

            var changed = new List<CandidateRow>();
            var newSettings = new List<string?>();
            var newBootVals = new List<string?>();
            var newResetVals = new List<string?>();

            foreach (var c in dayCandidates)
            {
                // #4348: a pattern that times out on one of these three values masks it whole and warns
                // with the setting's NAME only — never the value or any fragment of it.
                void LogTimeout(string? name) =>
                    logger?.LogWarning("PgSettingRedactor timed out matching setting '{Name}'; the value was masked whole.", name);

                var newSetting = PgSettingRedactor.Redact(c.Name, c.Setting, LogTimeout);
                var newBootVal = PgSettingRedactor.Redact(c.Name, c.BootVal, LogTimeout);
                var newResetVal = PgSettingRedactor.Redact(c.Name, c.ResetVal, LogTimeout);

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

            changedRowCount += changed.Count;
            var dayUpdated = 0;
            try
            {
                if (dayCandidates.Count > (TestOnlyHourSliceCandidateThresholdOverride ?? HourSliceCandidateThreshold))
                {
                    /* One slice per hour of collection_time within this day, each its own transaction with
                       its own literal [start, end) bound alongside the constant server_id predicate. A slice
                       whose changed set is still large gets MaxKeysPerUpdate sub-batching same as the
                       single-range path below. The coarse ILIKE filter DOES re-select an already-redacted
                       row on a restart (a masked value such as password=******** still matches %password%),
                       but resume is still correct: PgSettingRedactor.Redact is idempotent, so a re-selected,
                       already-redacted row's new value equals its current value, the changed-set comparison
                       drops it, and no UPDATE is issued for it. */
                    for (var hour = 0; hour < 24; hour++)
                    {
                        var sliceStart = day.AddHours(hour);
                        var sliceEnd = sliceStart.AddHours(1);
                        var sliceChanged = new List<CandidateRow>();
                        var sliceSettings = new List<string?>();
                        var sliceBootVals = new List<string?>();
                        var sliceResetVals = new List<string?>();
                        for (var k = 0; k < changed.Count; k++)
                        {
                            if (changed[k].CollectionTime >= sliceStart && changed[k].CollectionTime < sliceEnd)
                            {
                                sliceChanged.Add(changed[k]);
                                sliceSettings.Add(newSettings[k]);
                                sliceBootVals.Add(newBootVals[k]);
                                sliceResetVals.Add(newResetVals[k]);
                            }
                        }

                        if (sliceChanged.Count == 0)
                        {
                            continue;
                        }

                        for (var i = 0; i < sliceChanged.Count; i += MaxKeysPerUpdate)
                        {
                            var take = Math.Min(MaxKeysPerUpdate, sliceChanged.Count - i);
                            dayUpdated += await RunBatchAsync(
                                connection, sliceChanged, sliceSettings, sliceBootVals, sliceResetVals, i, take,
                                sliceStart, sliceEnd, serverId, cancellationToken);
                        }

                        TestOnlyAfterSliceCommitted?.Invoke();
                    }
                }
                else
                {
                    for (var i = 0; i < changed.Count; i += MaxKeysPerUpdate)
                    {
                        var take = Math.Min(MaxKeysPerUpdate, changed.Count - i);
                        dayUpdated += await RunBatchAsync(
                            connection, changed, newSettings, newBootVals, newResetVals, i, take, day, day.AddDays(1), serverId, cancellationToken);
                    }
                }
            }
            catch (NpgsqlException ex)
            {
                /* Never the exception TEXT (it can carry the value that tripped it) — just the server id and
                   the SQLSTATE, e.g. 53400 when a target's day still exceeds the decompress limit even under
                   SET LOCAL = 0 (which cannot happen on its own, but a future change to the limit's floor
                   should still be caught here rather than stopping every later target). */
                logger?.LogWarning(
                    "pg_setting_scrub: server {ServerId} failed on {Day:yyyy-MM-dd} with SQLSTATE {SqlState}; skipping this server for the rest of the run, will retry on the next start",
                    serverId, day, ex.SqlState ?? (ex.InnerException is TimeoutException ? "timeout" : "client"));
                failedServerIds.Add(serverId);

                /* A timed-out command whose own cancel request also fails can leave the connector
                   broken. The next server's BeginTransactionAsync would then throw InvalidOperationException,
                   which is not an NpgsqlException and is not caught here — that would end the whole run
                   rather than just skipping this one server. Reopen in place so the remaining servers this
                   run still get attempted; the marker stays withheld regardless. */
                if (connection.State != System.Data.ConnectionState.Open)
                {
                    await connection.CloseAsync();
                    await connection.OpenAsync(cancellationToken);
                }

                continue;
            }

            daysTouched.Add(day);
            rowsUpdated += dayUpdated;
        }

        if (rowsUpdated == changedRowCount && failedServerIds.Count == 0)
        {
            await WriteMarkerAsync(connection, currentVersion, cancellationToken);
        }
        else if (failedServerIds.Count > 0)
        {
            logger?.LogWarning(
                "pg_setting_scrub: {FailedCount} server(s) failed this run ({FailedServerIds}); leaving the marker unwritten so the next start retries them",
                failedServerIds.Count, string.Join(",", failedServerIds));
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
        List<string?> newResetVals, int offset, int count, DateTime rangeStart, DateTime rangeEnd, int serverId,
        CancellationToken cancellationToken)
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

        /* M3 (a): SET LOCAL, not a batch-per-collection_time split — it keeps H1's shape (one literal target
           filter, one literal day range, per UPDATE) instead of adding a third grouping key. The GUC's
           pg_settings context is `user`, so the store's own non-superuser role may set it (confirmed live
           against TimescaleDB 2.30.1 on docker: SET SESSION AUTHORIZATION to an ordinary LOGIN role, then
           SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0 succeeds and reads back as
           0). Scoped to this one batch's transaction only — LOCAL means it reverts at COMMIT/ROLLBACK, so it
           never leaks into any other batch, any other server, or the marker read/write, which do not run
           inside this transaction. 0 disables the limit entirely for this transaction, which is safe here
           because the literal server_id + day-range predicates confine decompression to ONE target's
           segment in ONE day's chunk (settings × collections that day — ~8k rows hourly, ~500k at a
           1-minute cadence), not the whole chunk; the literal server_id plus the day range bound the
           decompress, not MaxKeysPerUpdate, which only bounds the key array passed to one UPDATE. */
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        /* Guarded, not a bare SET LOCAL: a bring-your-own store on TimescaleDB older than 2.14 (the GUC
           arrived in timescale/timescaledb PR #6566) has no such setting. current_setting(name, true)
           returns NULL rather than raising when the GUC is absent, so the WHERE makes this a no-op on those
           stores instead of a 42704 on every batch, every start, for good — set_config(..., true) is the
           SET LOCAL equivalent (transaction-scoped, reverts at COMMIT/ROLLBACK). */
        await using (var setLocal = new NpgsqlCommand(
            "SELECT set_config('timescaledb.max_tuples_decompressed_per_dml_transaction', '0', true) " +
            "WHERE current_setting('timescaledb.max_tuples_decompressed_per_dml_transaction', true) IS NOT NULL",
            connection, transaction)
            { CommandTimeout = UpdateBatchTimeoutSeconds })
        {
            await setLocal.ExecuteNonQueryAsync(cancellationToken);
        }

        if (TestOnlyPreUpdateDelaySeconds is { } delaySeconds &&
            (TestOnlyPreUpdateDelayServerId is null || TestOnlyPreUpdateDelayServerId == serverId))
        {
            await using var delay = new NpgsqlCommand("SELECT pg_sleep($1)", connection, transaction)
            {
                CommandTimeout = TestOnlyUpdateCommandTimeoutSecondsOverride ?? UpdateBatchTimeoutSeconds,
            };
            delay.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Double, Value = delaySeconds });
            await delay.ExecuteNonQueryAsync(cancellationToken);

            if (TestOnlyPreUpdateDelayOnce)
            {
                TestOnlyPreUpdateDelaySeconds = null;
            }
        }

        await using var update = new NpgsqlCommand(BatchUpdateSql, connection, transaction)
        {
            CommandTimeout = TestOnlyUpdateCommandTimeoutSecondsOverride ?? UpdateBatchTimeoutSeconds,
        };
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = serverIds });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = times });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = names });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = databaseNames });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = roleNames });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = settings });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = bootVals });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = resetVals });
        /* The literal range is what lets TimescaleDB exclude every chunk but this one — see the type
           remarks. Redundant with the join equality on collection_time, and load-bearing anyway. A slice
           passes its own [start, end) hour bound here; the single-range path passes the whole day. */
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = rangeStart });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = rangeEnd });
        /* The constant server_id predicate is what lets TimescaleDB exclude every other server's segment in
           this day's chunk — see the H1 fix note above RunAsync's grouping. */
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });

        var updated = await update.ExecuteNonQueryAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return updated;
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
