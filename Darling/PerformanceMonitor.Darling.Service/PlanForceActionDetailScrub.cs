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
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// One-time scrub of the legacy <c>state_unavailable:</c> evidence line in
/// <c>collect.plan_force_actions.detail</c>, for a row an older build wrote before #4326/#4363 fixed the
/// PRODUCERS (<c>PgPlanForceActionStore.TryGetTargetStatesAsync</c> and <c>ForcePlanBotPolicy.Blockers</c>)
/// to stop putting raw exception text there (Erik: "Yes, one-time scrub" — #4346). <c>JournalAsync</c>
/// itself does not sanitize — it writes whatever <c>Detail</c> it is given — so a new row is safe only
/// because the producers that build it are; this scrub exists for the rows written before they were.
///
/// <para><b>Why this method reads <c>detail</c> raw.</b> Every OTHER reader of this table is built on
/// <c>PgPlanForceActionStore.ReadRecord</c> (#4376), which already applies
/// <see cref="PgPlanForceActionStore.SanitizeDetailForAudit"/> on the way OUT — so a caller of
/// <c>GetRecentActionsAsync</c>/<c>GetPendingReviewsAsync</c> never sees the legacy text, but the STORED row
/// still carries it. A scrub built on those readers could never find what it exists to fix. This class is
/// therefore the one named exemption in <c>PlanForceActionDetailCensusTests.RawDetailReaderExemptions</c>
/// (#4377/#4384): its own SELECT reads <c>detail</c> directly, decides with the SAME
/// <see cref="PgPlanForceActionStore.SanitizeDetailForAudit"/> the readers use — no second copy of the
/// allow-list — and writes back only that function's output. It never returns a <c>detail</c> string to a
/// caller; the public surface is counts only (see <see cref="Summary"/>).</para>
///
/// <para><b>The marker</b> reuses <c>collect.collector_state</c> (V44) under the fleet-sentinel
/// <c>server_id</c> (<see cref="DarlingObservability.FleetServerId"/>), the same shape
/// <see cref="PgSettingScrub"/> and <c>PgSelfAlertDeliveryStampStore</c> already use for store-wide one-shot
/// state — no new migration rung. Unlike <see cref="PgSettingScrub"/>'s versioned marker, the sanitizer here
/// has no rules version to track (one fixed allow-list, #4376), so the marker's value is a fixed literal; a
/// future change to the allow-list that needs a second pass is a new key, not a version bump on this one.</para>
///
/// <para><b>Not a hypertable.</b> <c>collect.plan_force_actions</c> is a plain PostgreSQL table with a
/// <c>bigint GENERATED ALWAYS AS IDENTITY</c> primary key (V107) — never converted (see
/// <c>DarlingRetention</c>'s remarks on why the journal keeps DELETE-based retention). So unlike
/// <see cref="PgSettingScrub"/>'s (server, day) grouping around TimescaleDB's compressed-chunk decompress
/// limit, this scrub batches purely by row count: an ordinary keyed UPDATE against <c>action_id</c>, safe on
/// any batch size the store can hold in one transaction, with <see cref="MaxKeysPerUpdate"/> as a courtesy
/// cap rather than a workaround for anything.</para>
///
/// <para><b>Failure is isolated and retried, never fatal.</b> The candidate read and each batch are
/// independent; a batch that throws is logged once at WARNING (SQLSTATE only, never the exception's own
/// text, which could carry the very secret being scrubbed), and the run STOPS — it does not attempt any
/// later batch this pass. The marker is written ONLY when every batch this run touched succeeded — a run
/// that failed even one batch leaves the marker unset, so the next service start retries every batch, not
/// just the ones that failed (cheap and correct: a row already scrubbed contributes nothing to the coarse
/// filter's next pass).</para>
/// </summary>
public static class PlanForceActionDetailScrub
{
    /// <summary>The owner name in <c>collect.collector_state</c> — the <c>QueryStoreBackfill</c> /
    /// <c>PgSettingScrub</c> precedent, so no collector's declared-key read or per-database prune can reach
    /// it.</summary>
    public const string StateCollectorName = "plan_force_action_detail_scrub";

    /// <summary>The marker's one key. No rules version — see the type remarks.</summary>
    public const string DoneStateKey = "done";

    private const string DoneStateValue = "1";

    /// <summary>The candidate read's deadline — one pass over the whole journal, generous because this runs
    /// once, off the collection path.</summary>
    internal const int CandidateReadTimeoutSeconds = 300;

    /// <summary>Each batch UPDATE's deadline.</summary>
    internal const int UpdateBatchTimeoutSeconds = 60;

    /// <summary>The marker read/write's deadline — one small keyed row.</summary>
    internal const int MarkerTimeoutSeconds = 30;

    /// <summary>Rows per UPDATE — a courtesy cap, not a workaround; see the type remarks.</summary>
    internal const int MaxKeysPerUpdate = 500;

    /// <summary>What one run found and did. Counts only — never <c>detail</c> text; see the type
    /// remarks.</summary>
    public sealed class Summary
    {
        public static readonly Summary NoOp = new(alreadyDone: true, 0, 0);

        internal Summary(bool alreadyDone, int candidatesRead, int rowsUpdated)
        {
            AlreadyDone = alreadyDone;
            CandidatesRead = candidatesRead;
            RowsUpdated = rowsUpdated;
        }

        /// <summary>True when the marker already recorded completion and the run did nothing else.</summary>
        public bool AlreadyDone { get; }

        /// <summary>Rows the coarse filter returned (an over-inclusive superset; not all of them changed
        /// under the sanitizer).</summary>
        public int CandidatesRead { get; }

        /// <summary>Rows actually written, because <see cref="PgPlanForceActionStore.SanitizeDetailForAudit"/>
        /// changed their text.</summary>
        public int RowsUpdated { get; }
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

    /// <summary>The coarse filter: a superset of every row the sanitizer could possibly change — any row
    /// whose <c>detail</c> mentions the block's prefix at all. Over-inclusive on purpose (a row whose ONLY
    /// <c>state_unavailable:</c> block is already the safe post-fix shape matches this filter too); the
    /// fine-grained decision is <see cref="PgPlanForceActionStore.SanitizeDetailForAudit"/>, run per
    /// candidate row below — this is the one and only exempted raw read of the column (#4377/#4384).
    ///
    /// <para>Named <c>LegacyDetailCandidateSql</c>, not the bare <c>CandidateSql</c> the census's
    /// earlier revision guarded on: <c>PgSettingScrub</c> and <c>QueryStoreBackfill</c> each declare their
    /// own unrelated <c>CandidateSql</c> field, and a guard matching that bare name fails on the real tree
    /// before the census even runs. This name cannot collide with either.</para></summary>
    private const string LegacyDetailCandidateSql = @"
SELECT action_id, detail
FROM collect.plan_force_actions
WHERE detail LIKE '%state_unavailable:%'";

    private const string BatchUpdateSql = @"
WITH batch AS (
    SELECT * FROM unnest($1::bigint[], $2::text[]) AS k(action_id, new_detail)
)
UPDATE collect.plan_force_actions t
SET detail = b.new_detail
FROM batch b
WHERE t.action_id = b.action_id";

    /// <summary>
    /// Runs the scrub once against <paramref name="postgres"/>, or returns <see cref="Summary.NoOp"/> at
    /// once if the marker already recorded completion. Never returns or logs <c>detail</c> text — see the
    /// type remarks' contract.
    /// </summary>
    public static async Task<Summary> RunAsync(NpgsqlDataSource postgres, ILogger? logger, CancellationToken cancellationToken)
    {
        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);

        var marker = await ReadMarkerAsync(connection, cancellationToken);
        if (marker == DoneStateValue)
        {
            return Summary.NoOp;
        }

        var toUpdateIds = new List<long>();
        var toUpdateDetails = new List<string?>();
        var candidateCount = 0;

        await using (var read = new NpgsqlCommand(LegacyDetailCandidateSql, connection) { CommandTimeout = CandidateReadTimeoutSeconds })
        await using (var reader = await read.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                candidateCount++;
                var actionId = reader.GetInt64(0);
                var detail = reader.IsDBNull(1) ? null : reader.GetString(1);
                var sanitized = PgPlanForceActionStore.SanitizeDetailForAudit(detail);

                if (sanitized == detail)
                {
                    continue;
                }

                toUpdateIds.Add(actionId);
                toUpdateDetails.Add(sanitized);
            }
        }

        var rowsUpdated = 0;
        var batchFailed = false;
        for (var i = 0; i < toUpdateIds.Count; i += MaxKeysPerUpdate)
        {
            var take = Math.Min(MaxKeysPerUpdate, toUpdateIds.Count - i);
            try
            {
                rowsUpdated += await RunBatchAsync(connection, toUpdateIds, toUpdateDetails, i, take, cancellationToken);
            }
            catch (NpgsqlException ex)
            {
                /* Never the exception TEXT — it can carry the value that tripped it. */
                logger?.LogWarning(
                    "plan_force_action_detail_scrub: batch at offset {Offset} failed with SQLSTATE {SqlState}; leaving the marker unwritten so the next start retries",
                    i, ex.SqlState);
                batchFailed = true;
                break;
            }
        }

        if (!batchFailed && rowsUpdated == toUpdateIds.Count)
        {
            await WriteMarkerAsync(connection, cancellationToken);
        }
        else if (!batchFailed)
        {
            /* A systematic key mismatch (row deleted by retention between read and write, say) would
               otherwise mark the store scrubbed while a legacy row remains. Leave the marker unwritten. */
            logger?.LogWarning(
                "plan_force_action_detail_scrub: updated {RowsUpdated} of {ToUpdate} changed row(s); leaving the marker unwritten so the next start retries",
                rowsUpdated, toUpdateIds.Count);
        }

        return new Summary(alreadyDone: false, candidateCount, rowsUpdated);
    }

    private static async Task<int> RunBatchAsync(
        NpgsqlConnection connection, List<long> ids, List<string?> details, int offset, int count, CancellationToken cancellationToken)
    {
        var idSlice = new long[count];
        var detailSlice = new string?[count];
        for (var j = 0; j < count; j++)
        {
            idSlice[j] = ids[offset + j];
            detailSlice[j] = details[offset + j];
        }

        await using var update = new NpgsqlCommand(BatchUpdateSql, connection) { CommandTimeout = UpdateBatchTimeoutSeconds };
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = idSlice });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = detailSlice });

        return await update.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<string?> ReadMarkerAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(MarkerGetSql, connection) { CommandTimeout = MarkerTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DarlingObservability.FleetServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = StateCollectorName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DoneStateKey });

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result as string;
    }

    private static async Task WriteMarkerAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(MarkerUpsertSql, connection) { CommandTimeout = MarkerTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DarlingObservability.FleetServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = StateCollectorName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DoneStateKey });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DoneStateValue });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Timestamp,
            Value = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified),
        });

        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
