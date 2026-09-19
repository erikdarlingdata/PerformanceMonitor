/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetFactCollector
{
    /// <summary>
    /// The standby read: ONE row, the standby furthest behind in the window by peak <c>replay_bytes_behind</c>, with
    /// its latest stage gaps and the two half-window means the drift arm grades. Pattern:
    /// <c>DarlingPgReplicationStatsReader.PgReplicationStatsSql</c> — identity is <c>(application_name, client_addr)</c>
    /// (two replicas commonly report the same default <c>application_name</c>; collapsing them would average two
    /// servers), joined <c>IS NOT DISTINCT FROM</c> because <c>client_addr</c> is NULL on a Unix socket, ranked by
    /// BYTES and never by <c>replay_lag_ms</c> (the time lag understates a stall, and the measured managed flavour
    /// reports it NULL on every row). What only an analysis read adds:
    /// <list type="bullet">
    /// <item><description><b>Halves</b>: the span's midpoint is the series' own <c>MIN</c>/<c>MAX</c>
    /// <c>collection_time</c> halved — never the nominal window — and each half's mean is a <c>FILTER</c>ed
    /// aggregate, so a window one collection wide or a standby present in one half only yields a NULL mean and the
    /// caller reports the drift as not computable rather than dividing by a fabricated level.</description></item>
    /// <item><description><b>Stages</b>: the latest <c>sent/write/flush/replay_bytes_behind</c> ride out so the
    /// caller can name WHICH stage is furthest behind — the network and sender, the standby's disk, or its apply
    /// path — instead of one undifferentiated "behind".</description></item>
    /// <item><description><b>Own coverage</b>: <c>collections_in_window</c> (distinct collection times) and the
    /// standby's own <c>samples</c>, because the replication collector runs every five minutes and the fact must
    /// state its own sample count rather than borrow the one-minute coverage fraction.</description></item>
    /// </list>
    /// <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC).
    /// </summary>
    public const string PgTargetReplicationLagSql = @"
WITH bounded AS (
    SELECT application_name, client_addr, collection_time, state, sync_state,
           sent_bytes_behind, write_bytes_behind, flush_bytes_behind, replay_bytes_behind, replay_lag_ms
    FROM pg_replication_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
),
span AS (
    SELECT MIN(collection_time) AS first_at,
           MAX(collection_time) AS last_at,
           MIN(collection_time) + (MAX(collection_time) - MIN(collection_time)) / 2 AS midpoint,
           COUNT(DISTINCT collection_time) AS collections_in_window
    FROM bounded
),
per_standby AS (
    SELECT b.application_name, b.client_addr,
           MAX(b.replay_bytes_behind) AS peak_replay_bytes,
           AVG(b.replay_bytes_behind) FILTER (WHERE b.collection_time <  s.midpoint) AS first_half_mean,
           AVG(b.replay_bytes_behind) FILTER (WHERE b.collection_time >= s.midpoint) AS second_half_mean,
           MAX(b.replay_lag_ms) AS peak_replay_lag_ms,
           COUNT(*) FILTER (WHERE b.replay_lag_ms IS NOT NULL) AS lag_ms_samples,
           COUNT(*) AS samples
    FROM bounded AS b
    CROSS JOIN span AS s
    GROUP BY b.application_name, b.client_addr
),
latest AS (
    SELECT DISTINCT ON (application_name, client_addr)
           application_name, client_addr, state, sync_state,
           sent_bytes_behind, write_bytes_behind, flush_bytes_behind, replay_bytes_behind, replay_lag_ms
    FROM bounded
    ORDER BY application_name, client_addr, collection_time DESC
)
SELECT
    l.application_name,
    l.state,
    l.sync_state,
    l.sent_bytes_behind,
    l.write_bytes_behind,
    l.flush_bytes_behind,
    l.replay_bytes_behind,
    l.replay_lag_ms,
    p.peak_replay_bytes,
    p.first_half_mean,
    p.second_half_mean,
    p.peak_replay_lag_ms,
    p.lag_ms_samples,
    p.samples,
    s.collections_in_window,
    s.first_at,
    s.last_at,
    (SELECT COUNT(*) FROM per_standby) AS standbys_in_window
FROM latest AS l
JOIN per_standby AS p
  ON  p.application_name IS NOT DISTINCT FROM l.application_name
  AND p.client_addr      IS NOT DISTINCT FROM l.client_addr
CROSS JOIN span AS s
ORDER BY p.peak_replay_bytes DESC NULLS LAST, l.application_name
LIMIT 1";

    /// <summary>
    /// The slot read: EVERY slot in the window (slots are few; the two facts pick different worsts from the same
    /// rows — retention by bytes, xmin by age), each as its latest state beside its earliest retained bytes and
    /// its own sample counts. Pattern: <c>DarlingPgSlotReader.PgSlotsSql</c> — latest per slot <c>DISTINCT ON</c>,
    /// the earliest reading joined back because "45 GB all window" (a consumer behind but pacing) and "2 GB an hour
    /// ago, 45 GB now" (a volume filling in front of you) are different emergencies a single figure cannot tell
    /// apart. What only an analysis read adds: <c>observations_above_xmin_bar</c>, the slot's samples whose older
    /// horizon (<c>GREATEST(xmin_age, catalog_xmin_age)</c>) sat at or above the shared xmin bar (<c>$4</c> =
    /// <see cref="PostgresOutagePredictorThresholds.XminAgeWarningThreshold"/>, bound so the condition counted here
    /// and the one graded cannot drift) — the persistence numerator the slot-xmin fact grades on.
    /// <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC), <c>$4</c> age bar.
    /// </summary>
    public const string PgTargetReplicationSlotsSql = @"
WITH bounded AS (
    SELECT slot_name, collection_time, slot_type, plugin, database_name, is_active, wal_status,
           retained_wal_bytes, safe_wal_size_bytes, xmin_age, catalog_xmin_age,
           inactive_since, invalidation_reason, conflicting
    FROM pg_replication_slot_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
),
latest AS (
    SELECT DISTINCT ON (slot_name)
           slot_name, collection_time, slot_type, plugin, database_name, is_active, wal_status,
           retained_wal_bytes, safe_wal_size_bytes, xmin_age, catalog_xmin_age,
           inactive_since, invalidation_reason, conflicting
    FROM bounded
    ORDER BY slot_name, collection_time DESC
),
earliest AS (
    SELECT DISTINCT ON (slot_name)
           slot_name,
           retained_wal_bytes AS first_retained_wal_bytes,
           collection_time    AS first_seen_at
    FROM bounded
    ORDER BY slot_name, collection_time ASC
),
window_stats AS (
    SELECT slot_name,
           COUNT(*) AS samples,
           COUNT(*) FILTER (WHERE GREATEST(COALESCE(xmin_age, 0), COALESCE(catalog_xmin_age, 0)) >= $4) AS observations_above_xmin_bar
    FROM bounded
    GROUP BY slot_name
)
SELECT
    l.slot_name,
    l.collection_time,
    l.slot_type,
    l.plugin,
    l.database_name,
    l.is_active,
    l.wal_status,
    l.retained_wal_bytes,
    l.safe_wal_size_bytes,
    l.xmin_age,
    l.catalog_xmin_age,
    l.inactive_since,
    l.conflicting,
    e.first_retained_wal_bytes,
    e.first_seen_at,
    w.samples,
    w.observations_above_xmin_bar,
    COUNT(*) OVER () AS slots_in_window
FROM latest AS l
JOIN earliest AS e
  ON  e.slot_name IS NOT DISTINCT FROM l.slot_name
JOIN window_stats AS w
  ON  w.slot_name IS NOT DISTINCT FROM l.slot_name
ORDER BY l.retained_wal_bytes DESC NULLS LAST, l.slot_name";

    /// <summary>
    /// <c>PG_REPLICATION_LAG</c> (the standby furthest behind, its peak, latest, stage and steady-versus-drifting
    /// halves), <c>PG_SLOT_RETENTION</c> (the slot retaining the most WAL, its growth and how long inactive) and
    /// <c>PG_SLOT_XMIN</c> (the slot with the oldest horizon and its persistence at the shared bar). Two reads, each
    /// behind its own degrade so a missing table (a pre-V67 / pre-V92 store, a flavour on which a collector does not
    /// run) costs only its own facts.
    /// <para>/* filled by lane 12 of #3691 — the pattern is <c>PgTargetFactCollector.Vacuum.cs</c>: per-object
    /// composition naming the worst standby / slot; every command sets <c>CommandTimeout = FactCommandTimeoutSeconds</c>,
    /// every store call passes <c>context.CancellationToken</c>, the catch is the <c>IsExpectedAbandon</c> shape
    /// around <see cref="ReportCollectionFailure"/>; no rate here divides by anything — the facts are levels, halves
    /// and slopes over the series' own span, and each states its own five-minute sample count. */</para>
    ///
    /// <para><b>Emission is evidence-gated, and absence is normal.</b> A primary with no standby and no slot has
    /// no rows in either table and emits NOTHING — not <c>unavailable</c>, because "no replication" is a
    /// configuration, not a missing measurement. Rows in <c>pg_replication_stats</c> with none in
    /// <c>pg_replication_slot_stats</c> is a different case (a standby without a slot, or the slot collector not
    /// running): the lag fact is emitted and stamped <c>slots_observed_in_window = 0</c> so its advice can say slot
    /// state was not observed, and the two slot facts are simply not emitted. The lag fact and the slot facts ARE
    /// emitted at base 0 when rows exist but nothing graded — a standby steady at 900 kB and a 1 MB slot are
    /// context the story engine ignores and <c>get_analysis_facts</c> shows.</para>
    ///
    /// <para><b>NULL <c>replay_lag_ms</c> is the measured norm</b> on the managed flavour (every row, 2026-09-19),
    /// so bytes are the graded quantity and the millisecond columns are metadata present only when reported.
    /// A row with NULL <c>replay_bytes_behind</c> (a standby in <c>startup</c> before the first flush report)
    /// contributes to the sample count and nothing to the peak.</para>
    ///
    /// <para>The slot read runs after the lag read so it can stamp the lag fact, and both run after the vacuum
    /// family (emission order in the root file) so the slot-xmin fact can read the wraparound fact's
    /// <c>autovacuum_freeze_max_age</c> for its ramp's top — the same hand-over <c>CollectVacuumFactsAsync</c>
    /// makes to its own xmin fact.</para>
    /// </summary>
    private async partial Task CollectReplicationFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        var lag = await ReadReplicationLagAsync(context);
        if (lag is not null) facts.Add(lag);
        await ReadReplicationSlotsAsync(context, facts, lag);
    }

    private async Task<Fact?> ReadReplicationLagAsync(AnalysisContext context)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PgTargetReplicationLagSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            /* Zero rows: no standby connected in the window — a configuration, not a missing measurement. */
            if (!await reader.ReadAsync(context.CancellationToken)) return null;

            var applicationName = reader.IsDBNull(0) ? null : reader.GetString(0);
            var state = reader.IsDBNull(1) ? null : reader.GetString(1);
            var syncState = reader.IsDBNull(2) ? null : reader.GetString(2);
            var sent = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));
            var write = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));
            var flush = reader.IsDBNull(5) ? 0L : ToInt64(reader.GetValue(5));
            var replayLatest = reader.IsDBNull(6) ? 0L : ToInt64(reader.GetValue(6));
            double? replayMsLatest = reader.IsDBNull(7) ? null : Convert.ToDouble(reader.GetValue(7));
            var peak = reader.IsDBNull(8) ? 0L : ToInt64(reader.GetValue(8));
            double? firstHalfMean = reader.IsDBNull(9) ? null : Convert.ToDouble(reader.GetValue(9));
            double? secondHalfMean = reader.IsDBNull(10) ? null : Convert.ToDouble(reader.GetValue(10));
            double? replayMsPeak = reader.IsDBNull(11) ? null : Convert.ToDouble(reader.GetValue(11));
            var lagMsSamples = reader.IsDBNull(12) ? 0L : ToInt64(reader.GetValue(12));
            var samples = ToInt64(reader.GetValue(13));
            var collections = ToInt64(reader.GetValue(14));
            var firstAt = reader.GetDateTime(15);
            var lastAt = reader.GetDateTime(16);
            var standbys = ToInt64(reader.GetValue(17));

            /* Which stage is furthest behind at the latest sample. The four gaps are cumulative along the pipeline
               (replay ≥ flush ≥ write ≥ sent on a healthy standby), so "the stage behind" is the FIRST stage whose
               gap accounts for the distance: the largest INCREMENT over the stage before it. */
            var stage = PgTargetScorer.LagStageSent;
            var stageBytes = sent;
            var increments = new[] { (PgTargetScorer.LagStageWrite, write - sent), (PgTargetScorer.LagStageFlush, flush - write), (PgTargetScorer.LagStageReplay, replayLatest - flush) };
            foreach (var (code, increment) in increments)
            {
                if (increment > stageBytes)
                {
                    stage = code;
                    stageBytes = increment;
                }
            }

            var fact = new Fact
            {
                Source = PgTargetSources.ReplicationSource,
                Key = PgTargetFactKeys.ReplicationLag,
                Value = peak,
                ServerId = context.ServerId,
                /* The standby's application_name — the operator's own label for it — and never client_addr. */
                ObjectName = string.IsNullOrWhiteSpace(applicationName) ? null : applicationName,
                Metadata =
                {
                    [PgTargetScorer.LagPeakBytesKey] = peak,
                    [PgTargetScorer.LagLatestBytesKey] = replayLatest,
                    [PgTargetScorer.LagStageKey] = stage,
                    [PgTargetScorer.LagStageBytesKey] = stageBytes,
                    [PgTargetScorer.LagSyncStateKey] = PgTargetScorer.SyncStateCode(syncState),
                    [PgTargetScorer.LagStandbyStreamingKey] = string.Equals(state, "streaming", StringComparison.OrdinalIgnoreCase) ? 1 : 0,
                    [PgTargetScorer.LagMsReportedKey] = lagMsSamples > 0 ? 1 : 0,
                    [PgTargetScorer.LagSamplesKey] = samples,
                    [PgTargetScorer.LagCollectionsKey] = collections,
                    [PgTargetScorer.LagStandbysKey] = standbys,
                    [PgTargetScorer.LagSpanHoursKey] = (lastAt - firstAt).TotalHours,
                },
            };

            /* Drift: both half-means or neither. A one-collection window has an empty first half; a standby that
               connected mid-window has an empty first half too. Both read as "not computable", never as a drift
               from zero. */
            if (firstHalfMean.HasValue && secondHalfMean.HasValue)
            {
                fact.Metadata[PgTargetScorer.LagFirstHalfMeanBytesKey] = firstHalfMean.Value;
                fact.Metadata[PgTargetScorer.LagSecondHalfMeanBytesKey] = secondHalfMean.Value;
                fact.Metadata[PgTargetScorer.LagDriftComputableKey] = 1;
            }
            else
            {
                fact.Metadata[PgTargetScorer.LagDriftComputableKey] = 0;
            }

            if (replayMsLatest.HasValue) fact.Metadata[PgTargetScorer.LagReplayMsLatestKey] = replayMsLatest.Value;
            if (replayMsPeak.HasValue) fact.Metadata[PgTargetScorer.LagReplayMsPeakKey] = replayMsPeak.Value;

            return fact;
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_replication_stats arrived in V92 — a pre-migration store raises 42P01, classified quiet. An
               abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
            return null;
        }
    }

    /// <summary>
    /// Emits <c>PG_SLOT_RETENTION</c> for the slot the shared grade ranks worst (severity first, then retained
    /// bytes — a <c>lost</c> 200 MB slot outranks a healthy 20 GB one, as the alert would page it) and
    /// <c>PG_SLOT_XMIN</c> for the slot with the oldest horizon, and stamps the lag fact with how many slots the
    /// window held. Two facts from one read because they answer different questions about possibly different
    /// slots — the disk-fill path and the vacuum-horizon path are the two independent harms an abandoned slot does.
    /// </summary>
    private async Task ReadReplicationSlotsAsync(AnalysisContext context, List<Fact> facts, Fact? lag)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PgTargetReplicationSlotsSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
            cmd.Parameters.AddWithValue(PostgresOutagePredictorThresholds.XminAgeWarningThreshold);

            var windowEnd = AsNaive(context.TimeRangeEnd);
            /* The engine-defined top of the slot-xmin ramp, handed over from the vacuum family's wraparound fact
               (emitted earlier in the pass); 0 when that read produced nothing, and the ramp stays flat. */
            var freezeMaxAge = facts.Find(f => f.Key == PgTargetFactKeys.WraparoundTrend)
                ?.Metadata.GetValueOrDefault(PgTargetScorer.WraparoundFreezeMaxAgeKey) ?? 0.0;

            Fact? worstRetention = null;
            var worstRetentionSeverity = -1.0;
            var worstRetentionBytes = -1L;
            Fact? worstXmin = null;
            var worstXminAge = -1L;
            long slotsInWindow = 0;
            var slotsGraded = 0;

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
            {
                var slotName = reader.IsDBNull(0) ? null : reader.GetString(0);
                var latestAt = reader.GetDateTime(1);
                var slotType = reader.IsDBNull(2) ? null : reader.GetString(2);
                var databaseName = reader.IsDBNull(4) ? null : reader.GetString(4);
                var isActive = !reader.IsDBNull(5) && reader.GetBoolean(5);
                var walStatus = PgTargetScorer.WalStatusCode(reader.IsDBNull(6) ? null : reader.GetString(6));
                var retained = reader.IsDBNull(7) ? 0L : ToInt64(reader.GetValue(7));
                var safeWal = reader.IsDBNull(8) ? -1L : ToInt64(reader.GetValue(8));
                var xminAge = reader.IsDBNull(9) ? 0L : ToInt64(reader.GetValue(9));
                var catalogXminAge = reader.IsDBNull(10) ? 0L : ToInt64(reader.GetValue(10));
                DateTime? inactiveSince = reader.IsDBNull(11) ? null : reader.GetDateTime(11);
                var conflicting = !reader.IsDBNull(12) && reader.GetBoolean(12);
                var firstRetained = reader.IsDBNull(13) ? retained : ToInt64(reader.GetValue(13));
                var firstSeenAt = reader.GetDateTime(14);
                var samples = ToInt64(reader.GetValue(15));
                var above = ToInt64(reader.GetValue(16));
                slotsInWindow = ToInt64(reader.GetValue(17));

                var growth = retained - firstRetained;
                var (severity, _) = PgTargetScorer.GradeSlotRetention(retained, walStatus, isActive, growth);
                if (severity > 0) slotsGraded++;

                /* ── retention: worst by the shared grade, then by bytes. */
                if (severity > worstRetentionSeverity || (severity == worstRetentionSeverity && retained > worstRetentionBytes))
                {
                    worstRetentionSeverity = severity;
                    worstRetentionBytes = retained;
                    var fact = new Fact
                    {
                        Source = PgTargetSources.ReplicationSource,
                        Key = PgTargetFactKeys.SlotRetention,
                        Value = retained,
                        ServerId = context.ServerId,
                        DatabaseName = databaseName,
                        ObjectName = slotName,
                        Metadata =
                        {
                            [PgTargetScorer.SlotRetainedBytesKey] = retained,
                            [PgTargetScorer.SlotFirstRetainedBytesKey] = firstRetained,
                            [PgTargetScorer.SlotGrowthBytesKey] = growth,
                            [PgTargetScorer.SlotActiveKey] = isActive ? 1 : 0,
                            [PgTargetScorer.SlotWalStatusKey] = walStatus,
                            [PgTargetScorer.SlotKeepSizeSetKey] = safeWal >= 0 ? 1 : 0,
                            [PgTargetScorer.SlotLogicalKey] = string.Equals(slotType, "logical", StringComparison.OrdinalIgnoreCase) ? 1 : 0,
                            [PgTargetScorer.SlotConflictingKey] = conflicting ? 1 : 0,
                            [PgTargetScorer.SlotSamplesKey] = samples,
                        },
                    };
                    /* Growth per hour over the slot's OWN span — the series' timestamps, never the nominal window. */
                    var spanHours = (latestAt - firstSeenAt).TotalHours;
                    fact.Metadata[PgTargetScorer.SlotSpanHoursKey] = spanHours;
                    if (spanHours > 0)
                        fact.Metadata[PgTargetScorer.SlotGrowthBytesPerHourKey] = growth / spanHours;
                    /* safe_wal_size is NULL (stored −1) whenever max_slot_wal_keep_size = −1, the shipped default — the
                       "unbounded" case the V67 migration note records; present only when a ceiling exists. */
                    if (safeWal >= 0)
                        fact.Metadata[PgTargetScorer.SlotSafeWalBytesKey] = safeWal;
                    StampInactiveSince(fact, inactiveSince, windowEnd);
                    worstRetention = fact;
                }

                /* ── xmin: worst by the older of the two horizons. */
                var age = Math.Max(xminAge, catalogXminAge);
                if (age > worstXminAge)
                {
                    worstXminAge = age;
                    var fact = new Fact
                    {
                        Source = PgTargetSources.ReplicationSource,
                        Key = PgTargetFactKeys.SlotXmin,
                        Value = age,
                        ServerId = context.ServerId,
                        DatabaseName = databaseName,
                        ObjectName = slotName,
                        Metadata =
                        {
                            [PgTargetScorer.SlotXminAgeKey] = age,
                            [PgTargetScorer.SlotXminPhysicalAgeKey] = xminAge,
                            [PgTargetScorer.SlotXminCatalogAgeKey] = catalogXminAge,
                            [PgTargetScorer.SlotXminArmIsCatalogKey] = catalogXminAge > xminAge ? 1 : 0,
                            [PgTargetScorer.SlotXminSamplesKey] = samples,
                            [PgTargetScorer.SlotXminObservationsAboveKey] = above,
                            [PgTargetScorer.SlotActiveKey] = isActive ? 1 : 0,
                            [PgTargetScorer.SlotLogicalKey] = string.Equals(slotType, "logical", StringComparison.OrdinalIgnoreCase) ? 1 : 0,
                        },
                    };
                    if (freezeMaxAge > 0)
                        fact.Metadata[PgTargetScorer.XminFreezeMaxAgeKey] = freezeMaxAge;
                    StampInactiveSince(fact, inactiveSince, windowEnd);
                    worstXmin = fact;
                }
            }

            /* The lag fact learns whether slot state was observed at all — 0 here is "not collected / no slot",
               which its advice states as such; the fact is never emitted as unavailable, because a standby can
               legitimately run without a slot. */
            if (lag is not null)
                lag.Metadata[PgTargetScorer.LagSlotsObservedKey] = slotsInWindow;

            if (worstRetention is null || worstXmin is null) return;

            worstRetention.Metadata[PgTargetScorer.SlotsInWindowKey] = slotsInWindow;
            worstRetention.Metadata[PgTargetScorer.SlotsGradedKey] = slotsGraded;
            worstXmin.Metadata[PgTargetScorer.SlotsInWindowKey] = slotsInWindow;
            facts.Add(worstRetention);
            facts.Add(worstXmin);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_replication_slot_stats arrived in V67 — a pre-migration store raises 42P01, classified quiet. An
               abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>
    /// <c>inactive_since</c> is PostgreSQL 17+ (NULL on older majors, and on an active slot), so "how long
    /// inactive" is stated only when the engine said: the flag carries which case it was.
    /// </summary>
    private static void StampInactiveSince(Fact fact, DateTime? inactiveSince, DateTime windowEnd)
    {
        if (inactiveSince.HasValue)
        {
            fact.Metadata[PgTargetScorer.SlotInactiveSinceKnownKey] = 1;
            fact.Metadata[PgTargetScorer.SlotInactiveHoursKey] = Math.Max(0.0, (windowEnd - inactiveSince.Value).TotalHours);
        }
        else
        {
            fact.Metadata[PgTargetScorer.SlotInactiveSinceKnownKey] = 0;
        }
    }
}
