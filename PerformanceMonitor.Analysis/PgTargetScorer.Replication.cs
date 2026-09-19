/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// <c>pg_replication</c> — replication lag and slot retention (filled by lane 12 of #3691, design §3.5 / §3.10):
/// <c>PG_REPLICATION_LAG</c> from <c>pg_replication_stats</c>, <c>PG_SLOT_RETENTION</c> / <c>PG_SLOT_XMIN</c> from
/// <c>pg_replication_slot_stats</c>.
///
/// <para><b>Lineage, by arm.</b> Every bar the Tier-0 slot alert grades on is referenced from
/// <see cref="PostgresOutagePredictorThresholds"/> (D9: the alert pages, this narrates, the two cannot disagree
/// because there is one symbol) — <c>SlotRetainedWalWarningBytes</c> for retained WAL, and the SAME symbol as the
/// absolute arm of the lag fact, because the WAL a standby has not replayed IS the WAL a slotted primary must
/// retain for it: one quantity, one bar, one place a number can move. The terminal <c>wal_status</c> values are
/// the engine's own verdict ("the WAL is gone / about to go") and grade CRITICAL at any size, as the alert does.
/// The slot-xmin bars are the xmin family's (<c>XminAgeWarningThreshold</c>, majority persistence, minimum
/// observations, the server's own <c>autovacuum_freeze_max_age</c> as the ramp's top) — referenced through the
/// same symbols <c>PgTargetScorer.Vacuum.cs</c> reads, never copied. The DRIFT arm of the lag fact — second-half
/// mean more than <see cref="ReplayLagDriftMultiple"/> times the first-half mean — is <b>unmeasured</b> (the dogfood
/// fleet has ONE standby, sitting steady at under a megabyte, 2026-09-19 — n = 1 cannot calibrate a multiple) and a
/// fact whose drift arm was consulted carries <c>threshold_lineage = 0</c>. The noise floor under the drift arm and
/// the anomaly detector is PostgreSQL's default <c>wal_segment_size</c>: WAL is shipped and replayed a segment at a
/// time, so a standby less than one segment behind is inside the unit of measurement.</para>
///
/// <para><b>Cadence.</b> The replication collectors run every five minutes, not every minute, so every fact here
/// states its own <c>samples_in_window</c> / <c>collections_in_window</c> and never borrows the one-minute
/// coverage fraction the pass computes over <c>pg_database_stats</c>. The scorer's gates are ratios of the fact's
/// own samples for the same reason.</para>
///
/// <para><b>Metadata contract with the collector</b> (<c>PgTargetFactCollector.Replication.cs</c>): the keys below
/// are the whole interface between the read and the grade, and the advice partial reads the same names. A missing
/// key reads as 0 through <c>GetValueOrDefault</c>, which for every gate here means "does not fire".</para>
/// </summary>
public static partial class PgTargetScorer
{
    /* ── PG_REPLICATION_LAG ── */

    /// <summary>Metadata key: the worst standby's window peak <c>replay_bytes_behind</c> — the fact's Value.</summary>
    public const string LagPeakBytesKey = "replay_bytes_behind_peak";
    /// <summary>Metadata key: the same standby's LATEST <c>replay_bytes_behind</c>.</summary>
    public const string LagLatestBytesKey = "replay_bytes_behind_latest";
    /// <summary>Metadata key: mean <c>replay_bytes_behind</c> over the first half of the window's span (by the
    /// series' own timestamps, not the nominal window). Absent when the drift is not computable.</summary>
    public const string LagFirstHalfMeanBytesKey = "replay_bytes_first_half_mean";
    /// <summary>Metadata key: mean <c>replay_bytes_behind</c> over the second half of the span.</summary>
    public const string LagSecondHalfMeanBytesKey = "replay_bytes_second_half_mean";
    /// <summary>Metadata key: 1 when both half-means exist (the span held samples on both sides of its midpoint),
    /// 0 when the window was one collection wide or the standby appeared in one half only.</summary>
    public const string LagDriftComputableKey = "drift_computable";
    /// <summary>Metadata key: the drift ratio the scorer graded — second-half mean over first-half mean, or over
    /// <see cref="ReplayLagNoiseFloorBytes"/> when the first half was caught up (see <see cref="ScoreReplicationLag"/>).</summary>
    public const string LagDriftRatioKey = "drift_ratio";
    /// <summary>Metadata key: 1 when the drift arm fired (the design's "drifting", against "steady").</summary>
    public const string LagDriftingKey = "lag_drifting";
    /// <summary>Metadata key: which stage is furthest behind at the latest sample — <see cref="LagStageSent"/> …
    /// <see cref="LagStageReplay"/>. A gap that is largest at <c>sent</c> is the network or the primary's sender;
    /// at <c>replay</c> it is the standby's apply path (the D7 delivered-vs-offered reading).</summary>
    public const string LagStageKey = "stage_behind";
    /// <summary>Metadata key: the bytes behind at <see cref="LagStageKey"/>'s stage.</summary>
    public const string LagStageBytesKey = "stage_behind_bytes";
    /// <summary>Metadata key: <c>sync_state</c> encoded by <see cref="SyncStateCode"/>.</summary>
    public const string LagSyncStateKey = "sync_state";
    /// <summary>Metadata key: 1 when the standby's latest <c>state</c> is <c>streaming</c>; 0 for catchup / startup
    /// / backup / stopping.</summary>
    public const string LagStandbyStreamingKey = "standby_streaming";
    /// <summary>Metadata key: 1 when the engine reported <c>replay_lag_ms</c> on at least one sample. The measured
    /// managed flavour reports bytes only (<c>replay_lag_ms</c> NULL on every row, 2026-09-19), so bytes are the
    /// primary quantity and milliseconds ride as metadata when present.</summary>
    public const string LagMsReportedKey = "lag_ms_reported";
    /// <summary>Metadata key: latest <c>replay_lag_ms</c>; present only when reported.</summary>
    public const string LagReplayMsLatestKey = "replay_lag_ms_latest";
    /// <summary>Metadata key: window peak <c>replay_lag_ms</c>; present only when reported.</summary>
    public const string LagReplayMsPeakKey = "replay_lag_ms_peak";
    /// <summary>Metadata key: rows the worst standby had in the window (its own sample count).</summary>
    public const string LagSamplesKey = "samples_in_window";
    /// <summary>Metadata key: distinct collection times the replication collector wrote in the window.</summary>
    public const string LagCollectionsKey = "collections_in_window";
    /// <summary>Metadata key: distinct standbys seen in the window (the fact names the worst).</summary>
    public const string LagStandbysKey = "standbys_in_window";
    /// <summary>Metadata key: hours from the first to the last replication sample in the window.</summary>
    public const string LagSpanHoursKey = "span_hours";
    /// <summary>Metadata key: distinct slots the SLOT collector observed in the window — stamped by the slot read
    /// so the lag advice can say "no slot state was observed" (0: a standby without a slot, or the slot collector
    /// not running) rather than leave the absence of the slot facts ambiguous.</summary>
    public const string LagSlotsObservedKey = "slots_observed_in_window";

    /// <summary><see cref="LagStageKey"/> codes: the four stages <c>pg_stat_replication</c> reports, in pipeline order.</summary>
    public const int LagStageSent = 0;
    public const int LagStageWrite = 1;
    public const int LagStageFlush = 2;
    public const int LagStageReplay = 3;

    /// <summary>
    /// The noise floor under the drift arm and the anomaly detector's magnitude floor: PostgreSQL's default
    /// <c>wal_segment_size</c>, 16 MB. Lineage: <b>engine-defined</b> — WAL is written, shipped and replayed a
    /// segment at a time, so a standby less than one segment behind is within the unit the quantity is measured in
    /// (the segment size is an <c>initdb</c> option this fact does not read; the shipped default is the line, the
    /// same posture <c>CONFIG_PG_MAX_WAL_SIZE</c> takes against its default).
    /// </summary>
    public const long ReplayLagNoiseFloorBytes = 16L * 1024 * 1024;

    /// <summary>
    /// The drift arm's CONCERNING multiple: the second-half mean of <c>replay_bytes_behind</c> more than twice the
    /// first-half mean, with the second half at least <see cref="ReplayLagNoiseFloorBytes"/>. A standby steadily N
    /// bytes behind is a consumer keeping pace at a distance; one whose distance doubles within the window is
    /// falling behind — the design's steady-versus-drifting distinction as a number. Lineage: <b>unmeasured</b> —
    /// chosen, not measured; the dogfood fleet has one standby (n = 1, steady), so calibrate against
    /// <c>pg_replication_stats</c> once the population exists. A fact graded through it carries
    /// <c>threshold_lineage = 0</c>.
    /// </summary>
    public const double ReplayLagDriftMultiple = 2.0;

    /// <summary>
    /// The drift arm's CRITICAL multiple: ten times within one window. Lineage: <b>unmeasured</b> — chosen, not
    /// measured; calibrate against <c>pg_replication_stats</c> before the next release (n = 1 today).
    /// </summary>
    public const double ReplayLagDriftCriticalMultiple = 10.0;

    /// <summary>
    /// The <c>ANOMALY_PG_REPLICATION_LAG</c> detector's magnitude floor on the trusted (z) path — an alias of
    /// <see cref="ReplayLagNoiseFloorBytes"/>, so the anomaly and the fact it folds into share one floor. Declared
    /// here rather than in <c>AnomalyThresholds</c> because that file's PostgreSQL block is pinned by a hand list
    /// (<c>PgTargetAnomalyTests</c>) that every v2 lane would otherwise edit at once; the deadlock-rate fallback set
    /// the precedent of a detector bar living beside its fact's scorer. Lineage: <b>engine-defined</b>, as the alias target.
    /// </summary>
    public const double PgReplayLagBytesFloor = ReplayLagNoiseFloorBytes;

    /// <summary>
    /// The detector's absolute-fallback bar on an untrustworthy baseline — the slot alert's
    /// <see cref="PostgresOutagePredictorThresholds.SlotRetainedWalWarningBytes"/> BY REFERENCE: a young store's lag
    /// anomaly fires exactly where the alert would page a slot retaining the same WAL, which is the honest reading
    /// of "no baseline yet" — the alert band, not an invented multiple. Lineage: <b>engine-shared</b> (the alert's
    /// own bar; in the lineage vocabulary, engine-defined by reference — the design's D9 shape).
    /// </summary>
    public const double PgReplayLagBytesFallback = PostgresOutagePredictorThresholds.SlotRetainedWalWarningBytes;

    /* ── PG_SLOT_RETENTION ── */

    /// <summary>Metadata key: latest <c>retained_wal_bytes</c> of the worst slot — the fact's Value.</summary>
    public const string SlotRetainedBytesKey = "retained_wal_bytes";
    /// <summary>Metadata key: the same slot's earliest <c>retained_wal_bytes</c> in the window.</summary>
    public const string SlotFirstRetainedBytesKey = "first_retained_wal_bytes";
    /// <summary>Metadata key: latest minus earliest — the alert's <c>RetainedWalGrowthBytes</c>; positive is growing.</summary>
    public const string SlotGrowthBytesKey = "retained_growth_bytes";
    /// <summary>Metadata key: growth per hour over the slot's own span; absent when the span is zero.</summary>
    public const string SlotGrowthBytesPerHourKey = "retained_growth_bytes_per_hour";
    /// <summary>Metadata key: hours from the slot's first to last sample in the window.</summary>
    public const string SlotSpanHoursKey = "span_hours";
    /// <summary>Metadata key: 1 when <c>is_active</c> at the latest sample.</summary>
    public const string SlotActiveKey = "slot_active";
    /// <summary>Metadata key: 1 when <c>inactive_since</c> was reported (PostgreSQL 17+ column; NULL on older majors
    /// and on an active slot).</summary>
    public const string SlotInactiveSinceKnownKey = "inactive_since_known";
    /// <summary>Metadata key: hours from <c>inactive_since</c> to the window end; present only when known.</summary>
    public const string SlotInactiveHoursKey = "inactive_hours";
    /// <summary>Metadata key: <c>wal_status</c> encoded by <see cref="WalStatusCode"/>.</summary>
    public const string SlotWalStatusKey = "wal_status";
    /// <summary>Metadata key: 1 when <c>max_slot_wal_keep_size</c> is set (the store's <c>safe_wal_size_bytes</c> is
    /// not the −1 sentinel); 0 means retention is unbounded, the shipped default.</summary>
    public const string SlotKeepSizeSetKey = "max_slot_wal_keep_size_set";
    /// <summary>Metadata key: <c>safe_wal_size_bytes</c> — WAL the slot may still retain before invalidation;
    /// present only when <see cref="SlotKeepSizeSetKey"/> is 1.</summary>
    public const string SlotSafeWalBytesKey = "safe_wal_size_bytes";
    /// <summary>Metadata key: 1 for a logical slot, 0 for physical.</summary>
    public const string SlotLogicalKey = "slot_logical";
    /// <summary>Metadata key: 1 when the slot's <c>conflicting</c> flag was set (a logical slot invalidated by a
    /// recovery conflict, PostgreSQL 16+).</summary>
    public const string SlotConflictingKey = "slot_conflicting";
    /// <summary>Metadata key: the arm that graded the slot — 0 none, 1 over the byte bar (Warning), 2 inactive AND
    /// growing over the bar (Critical, the disk-fill emergency), 3 a terminal <c>wal_status</c> (Critical, any size).</summary>
    public const string SlotArmKey = "graded_arm";
    /// <summary>Metadata key: distinct slots in the window (the fact names the worst).</summary>
    public const string SlotsInWindowKey = "slots_in_window";
    /// <summary>Metadata key: how many of them graded above 0.</summary>
    public const string SlotsGradedKey = "slots_graded";
    /// <summary>Metadata key: rows the worst slot had in the window.</summary>
    public const string SlotSamplesKey = "samples_in_window";

    /// <summary><see cref="SlotWalStatusKey"/> codes, in the engine's own order of alarm.</summary>
    public const int WalStatusUnknown = 0;
    public const int WalStatusReserved = 1;
    public const int WalStatusExtended = 2;
    public const int WalStatusUnreserved = 3;
    public const int WalStatusLost = 4;

    /* ── PG_SLOT_XMIN ── */

    /// <summary>Metadata key: the greater of the slot's <c>xmin_age</c> and <c>catalog_xmin_age</c> at the latest
    /// sample — the fact's Value.</summary>
    public const string SlotXminAgeKey = "xmin_age";
    /// <summary>Metadata key: the slot's <c>xmin_age</c> (physical slots, and logical ones with feedback).</summary>
    public const string SlotXminPhysicalAgeKey = "slot_xmin_age";
    /// <summary>Metadata key: the slot's <c>catalog_xmin_age</c> (logical decoding's catalog horizon).</summary>
    public const string SlotXminCatalogAgeKey = "catalog_xmin_age";
    /// <summary>Metadata key: 1 when the catalog horizon is the older of the two (the CDC / logical-subscriber shape).</summary>
    public const string SlotXminArmIsCatalogKey = "xmin_arm_is_catalog";
    /// <summary>Metadata key: rows the slot had in the window — the persistence denominator.</summary>
    public const string SlotXminSamplesKey = "samples_in_window";
    /// <summary>Metadata key: rows in which the slot's older horizon sat at or above the shared xmin bar.</summary>
    public const string SlotXminObservationsAboveKey = "observations_above_threshold";
    /// <summary>Metadata key: 1 when the identity arm held (age at the bar, majority of the slot's own samples above
    /// it, at least the minimum observations) — the xmin alert's chronic-holder shape applied to a slot.</summary>
    public const string SlotXminIdentityArmKey = "identity_arm";

    /// <summary>
    /// Layer-1 base severity for the replication family. One arm per key; an unknown key under this source is 0.
    /// </summary>
    private static partial double ScoreReplicationFact(Fact fact) => fact.Key switch
    {
        PgTargetFactKeys.ReplicationLag => ScoreReplicationLag(fact),
        PgTargetFactKeys.SlotRetention => ScoreSlotRetention(fact),
        PgTargetFactKeys.SlotXmin => ScoreSlotXmin(fact),
        _ => 0.0,
    };

    /// <summary>
    /// Two arms, the worse taken. ABSOLUTE: the window peak at or past the slot alert's byte bar scores 0.5 flat
    /// (the alert's Warning; there is no lag Critical in the evaluator, and a bar is not invented here). DRIFT:
    /// second-half mean over first-half mean against <see cref="ReplayLagDriftMultiple"/> → 0.5, ramping to 1.0 at
    /// <see cref="ReplayLagDriftCriticalMultiple"/>, gated on the second half being at least one WAL segment. A
    /// first half that was caught up (mean 0) has nothing to multiply, so its ratio is the second-half mean in
    /// segments — how far it fell behind from nothing — which keeps a 20 MB wobble below the bar and a 200 MB slide
    /// past it. Steady at any distance under the byte bar scores 0 and stays visible as context with its stage
    /// and sync state; the baseline-relative judgment is <c>ANOMALY_PG_REPLICATION_LAG</c>'s, which folds into
    /// this fact.
    /// </summary>
    private static double ScoreReplicationLag(Fact fact)
    {
        var peak = fact.Metadata.GetValueOrDefault(LagPeakBytesKey, fact.Value);

        /* engine-defined (shared with the slot alert, D9): PostgresOutagePredictorThresholds.SlotRetainedWalWarningBytes
           by reference — unreplayed WAL on a standby is retained WAL on its primary; 0.5 is the alert's Warning. */
        var absolute = peak >= PostgresOutagePredictorThresholds.SlotRetainedWalWarningBytes ? 0.5 : 0.0;

        var drift = 0.0;
        fact.Metadata[LagDriftingKey] = 0;
        if (fact.Metadata.GetValueOrDefault(LagDriftComputableKey) >= 1)
        {
            var first = fact.Metadata.GetValueOrDefault(LagFirstHalfMeanBytesKey);
            var second = fact.Metadata.GetValueOrDefault(LagSecondHalfMeanBytesKey);
            /* engine-defined: ReplayLagNoiseFloorBytes (one default WAL segment) is the floor and the caught-up
               denominator. unmeasured: ReplayLagDriftMultiple / ReplayLagDriftCriticalMultiple grade the ratio —
               the fact carries threshold_lineage = 0 whenever this arm was consulted. */
            var ratio = first > 0 ? second / first : second / ReplayLagNoiseFloorBytes;
            fact.Metadata[LagDriftRatioKey] = ratio;
            fact.Metadata["threshold_lineage"] = 0;
            if (second >= ReplayLagNoiseFloorBytes && ratio > ReplayLagDriftMultiple)
            {
                fact.Metadata[LagDriftingKey] = 1;
                drift = FactScorer.ApplyThresholdFormula(ratio, ReplayLagDriftMultiple, ReplayLagDriftCriticalMultiple);
            }
        }

        return Math.Max(absolute, drift);
    }

    /// <summary>
    /// The slot grade, on the SHARED bars — the same walk <c>PostgresAlertEvaluator.EvaluateSlot</c> takes: a
    /// terminal <c>wal_status</c> (<c>unreserved</c> / <c>lost</c>) is Critical at any size; over the byte bar,
    /// inactive and still growing is Critical (the disk-fill emergency — unbounded by default, nothing will stop
    /// it); anything else over the bar is Warning; under the bar with a live status is nothing. Public because the
    /// collector uses the same function to choose WHICH slot the one fact carries, and because
    /// <c>PgTargetReplicationTests</c> pins it against the evaluator's verdict across a grid so the two walks cannot
    /// drift.
    /// </summary>
    /// <returns>Severity on the shared 0–1 base scale (0 = not graded) and the arm that produced it (the
    /// <see cref="SlotArmKey"/> encoding).</returns>
    public static (double Severity, int Arm) GradeSlotRetention(long retainedWalBytes, int walStatus, bool isActive, long growthBytes)
    {
        var terminal = walStatus is WalStatusUnreserved or WalStatusLost;
        /* engine-defined (shared with the slot alert, D9): PostgresOutagePredictorThresholds.SlotRetainedWalWarningBytes by reference. */
        var overBytes = retainedWalBytes >= PostgresOutagePredictorThresholds.SlotRetainedWalWarningBytes;

        if (terminal)
            return (1.0, 3);
        if (!overBytes)
            return (0.0, 0);
        /* engine-defined: the alert's Critical condition — inactive, growing, over the bar. */
        if (!isActive && growthBytes > 0)
            return (1.0, 2);
        return (0.5, 1);
    }

    /// <summary>One fact per server carrying the worst slot; graded through <see cref="GradeSlotRetention"/>.</summary>
    private static double ScoreSlotRetention(Fact fact)
    {
        var (severity, arm) = GradeSlotRetention(
            (long)fact.Metadata.GetValueOrDefault(SlotRetainedBytesKey, fact.Value),
            (int)fact.Metadata.GetValueOrDefault(SlotWalStatusKey),
            fact.Metadata.GetValueOrDefault(SlotActiveKey) >= 1,
            (long)fact.Metadata.GetValueOrDefault(SlotGrowthBytesKey));
        fact.Metadata[SlotArmKey] = arm;
        return severity;
    }

    /// <summary>
    /// The xmin alert's IDENTITY arm applied to a slot's own horizon: the older of <c>xmin_age</c> and
    /// <c>catalog_xmin_age</c> at the shared warning bar, held there in a majority of the slot's own samples in the
    /// window, over at least the minimum observations — a slot is durable, but its horizon advances whenever its
    /// consumer confirms, so a single high sample is a consumer that was briefly behind, not a hold. Base 0.5 at
    /// the bar (the alert's Warning), ramping to 1.0 where the age reaches the server's own
    /// <c>autovacuum_freeze_max_age</c> (handed over from the wraparound fact by the collector; flat 0.5 when
    /// unknown) — the <c>PgTargetScorer.Vacuum.cs</c> ramp, same symbols, same shape, so the slot leaf and
    /// <c>PG_XMIN_HOLD</c> grade one horizon one way.
    /// </summary>
    private static double ScoreSlotXmin(Fact fact)
    {
        var age = (long)fact.Metadata.GetValueOrDefault(SlotXminAgeKey, fact.Value);
        var samples = fact.Metadata.GetValueOrDefault(SlotXminSamplesKey);
        var above = fact.Metadata.GetValueOrDefault(SlotXminObservationsAboveKey);

        /* engine-defined: the three gates are the shared PostgresOutagePredictorThresholds symbols the xmin alert's
           identity arm applies (age bar, majority fraction, minimum observations). */
        var identityArm = age >= PostgresOutagePredictorThresholds.XminAgeWarningThreshold
            && samples >= PostgresOutagePredictorThresholds.XminMinimumObservations
            && samples > 0
            && above / samples >= PostgresOutagePredictorThresholds.XminPersistenceFraction;

        fact.Metadata[SlotXminIdentityArmKey] = identityArm ? 1 : 0;
        if (!identityArm)
            return 0.0;

        /* engine-defined: concerning = the shared xmin warning age; critical = the server's own
           autovacuum_freeze_max_age when known and above it. Flat 0.5 otherwise. */
        var freezeMaxAge = fact.Metadata.GetValueOrDefault(XminFreezeMaxAgeKey);
        if (freezeMaxAge <= PostgresOutagePredictorThresholds.XminAgeWarningThreshold)
            return 0.5;

        return FactScorer.ApplyThresholdFormula(age, PostgresOutagePredictorThresholds.XminAgeWarningThreshold, freezeMaxAge);
    }

    /// <summary>Encodes <c>pg_replication_slots.wal_status</c>; anything unrecognised is <see cref="WalStatusUnknown"/>.</summary>
    public static int WalStatusCode(string? walStatus) => walStatus?.Trim().ToLowerInvariant() switch
    {
        "reserved" => WalStatusReserved,
        "extended" => WalStatusExtended,
        "unreserved" => WalStatusUnreserved,
        "lost" => WalStatusLost,
        _ => WalStatusUnknown,
    };

    /// <summary>Decodes <see cref="WalStatusCode"/> for prose.</summary>
    public static string WalStatusName(double code) => (int)code switch
    {
        WalStatusReserved => "reserved",
        WalStatusExtended => "extended",
        WalStatusUnreserved => "unreserved",
        WalStatusLost => "lost",
        _ => "unknown",
    };

    /// <summary><see cref="LagSyncStateKey"/> codes for <c>pg_stat_replication.sync_state</c>.</summary>
    public const int SyncStateUnknown = -1;
    public const int SyncStateAsync = 0;
    public const int SyncStateSync = 1;
    public const int SyncStateQuorum = 2;
    public const int SyncStatePotential = 3;

    /// <summary>Encodes <c>sync_state</c>; anything unrecognised is <see cref="SyncStateUnknown"/>.</summary>
    public static int SyncStateCode(string? syncState) => syncState?.Trim().ToLowerInvariant() switch
    {
        "async" => SyncStateAsync,
        "sync" => SyncStateSync,
        "quorum" => SyncStateQuorum,
        "potential" => SyncStatePotential,
        _ => SyncStateUnknown,
    };

    /// <summary>Decodes <see cref="SyncStateCode"/> for prose.</summary>
    public static string SyncStateName(double code) => (int)code switch
    {
        SyncStateAsync => "async",
        SyncStateSync => "sync",
        SyncStateQuorum => "quorum",
        SyncStatePotential => "potential",
        _ => "unknown",
    };

    /// <summary>Decodes <see cref="LagStageKey"/> for prose.</summary>
    public static string LagStageName(double code) => (int)code switch
    {
        LagStageSent => "sent",
        LagStageWrite => "write",
        LagStageFlush => "flush",
        _ => "replay",
    };

    /// <summary>
    /// Layer-2 amplifiers for the replication family. Every boost is a co-fire read from the fact set — the
    /// §3.5 chain made numeric: a standby's unreplayed WAL is its slot's retained WAL, a slot pins the horizon
    /// as well as the disk, a synchronous standby's lag is commit latency on the primary, and a WAL-volume shift
    /// on the primary is more being offered than the standby can apply. Boost values are <b>unmeasured</b> —
    /// chosen, not measured; calibrate against the dogfood PostgreSQL fleet's co-fire rates before the next
    /// release. Every predicate reads the sibling's <see cref="Fact.BaseSeverity"/>, never its
    /// <see cref="Fact.Severity"/> (the vacuum family's emission-order lesson).
    ///
    /// <para><b>The WAL-volume co-fire reads the ANOMALY, not the shift (#3691 between waves, lane 15's report).</b>
    /// Lane 12 wrote both WAL predicates against <c>PG_WAL_VOLUME_SHIFT</c>; lane 15 shipped that key as a CONTEXT
    /// fact — base severity 0 by design, the numbers for the advice to state — and put the judgment in
    /// <c>ANOMALY_PG_WAL_VOLUME</c>, so <c>BaseSeverity &gt; 0</c> on the shift was never true and both amplifiers
    /// were inert. They now read the anomaly's base severity, the shape lane 15 used for the checkpoint family's
    /// trigger amplifier (<c>PgTargetScorer.Write.cs</c>; <c>PgTargetWriteTests.TheCheckpointTriggerAmplifier_ReadsTheAnomaly_NotTheContextFact</c>),
    /// and the graph's dead shift → slot edge is gone with them (<c>PgTargetRelationshipGraph.Replication.cs</c>).</para>
    /// </summary>
    private static partial List<AmplifierDefinition> ReplicationAmplifiers(string key) => key switch
    {
        PgTargetFactKeys.ReplicationLag => ReplicationLagAmplifiers(),
        PgTargetFactKeys.SlotRetention => SlotRetentionAmplifiers(),
        PgTargetFactKeys.SlotXmin => SlotXminAmplifiers(),
        _ => [],
    };

    private static List<AmplifierDefinition> ReplicationLagAmplifiers() =>
    [
        new()
        {
            Description = "PG_SLOT_RETENTION co-fired — the primary is retaining the WAL this standby has not replayed; the lag is also a disk-fill path",
            /* unmeasured: chosen, not measured — calibrate against the dogfood PostgreSQL fleet before the next release. */
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.SlotRetention, out var f) && f.BaseSeverity > 0,
        },
        new()
        {
            Description = "ANOMALY_PG_REPLICATION_LAG co-fired — against this server's own hour-of-week baseline the lag is anomalous, not this standby's routine distance",
            /* unmeasured: chosen, not measured — calibrate against the dogfood PostgreSQL fleet before the next release. */
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.AnomalyReplicationLag, out var f) && f.BaseSeverity > 0,
        },
        new()
        {
            Description = "The standby is SYNCHRONOUS (sync_state sync or quorum) — its lag is commit latency on the primary, not only staleness on the replica",
            /* unmeasured: chosen, not measured — calibrate against the dogfood PostgreSQL fleet before the next release. */
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.ReplicationLag, out var f)
                && f.Metadata.GetValueOrDefault(LagSyncStateKey, SyncStateUnknown) is SyncStateSync or SyncStateQuorum,
        },
        new()
        {
            Description = "ANOMALY_PG_WAL_VOLUME co-fired — the primary is writing more WAL than its own hour-of-week baseline; the standby is being offered more than it can apply",
            /* unmeasured: chosen, not measured — calibrate against the dogfood PostgreSQL fleet before the next release. */
            Boost = 0.2,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.AnomalyWalVolume, out var f) && f.BaseSeverity > 0,
        },
    ];

    private static List<AmplifierDefinition> SlotRetentionAmplifiers() =>
    [
        new()
        {
            Description = "PG_SLOT_XMIN co-fired — a slot is pinning the xmin horizon as well as the disk: the two independent ways an abandoned slot takes a server down, both present",
            /* unmeasured: chosen, not measured — calibrate against the dogfood PostgreSQL fleet before the next release. */
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.SlotXmin, out var f) && f.BaseSeverity > 0,
        },
        new()
        {
            Description = "PG_REPLICATION_LAG co-fired — a connected standby is behind too; the retained WAL has a consumer that is not keeping up rather than none at all",
            /* unmeasured: chosen, not measured — calibrate against the dogfood PostgreSQL fleet before the next release. */
            Boost = 0.2,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.ReplicationLag, out var f) && f.BaseSeverity > 0,
        },
        new()
        {
            Description = "ANOMALY_PG_WAL_VOLUME co-fired — the primary is writing more WAL than its own hour-of-week baseline, so the pile behind this slot grows faster than its history says",
            /* unmeasured: chosen, not measured — calibrate against the dogfood PostgreSQL fleet before the next release. */
            Boost = 0.2,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.AnomalyWalVolume, out var f) && f.BaseSeverity > 0,
        },
    ];

    private static List<AmplifierDefinition> SlotXminAmplifiers() =>
    [
        new()
        {
            Description = "PG_XMIN_HOLD co-fired with a replication-slot holder — the vacuum family's own read attributes the cluster's horizon to a slot",
            /* unmeasured: chosen, not measured — calibrate against the dogfood PostgreSQL fleet before the next release. */
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.XminHold, out var f) && f.BaseSeverity > 0
                && PgTargetAdvice.HolderSourceName(f.Metadata.GetValueOrDefault(XminHolderSourceKey)) is "replication_slot" or "replication_slot_catalog",
        },
        new()
        {
            Description = "PG_AUTOVACUUM_BACKLOG co-fired — tables are past their trigger line and staying there: dead tuples newer than the slot's horizon cannot be removed",
            /* unmeasured: chosen, not measured — calibrate against the dogfood PostgreSQL fleet before the next release. */
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.AutovacuumBacklog, out var f) && f.BaseSeverity > 0,
        },
        new()
        {
            Description = "PG_WRAPAROUND_TREND co-fired — the held horizon is already showing as a freeze age autovacuum cannot bring down",
            /* unmeasured: chosen, not measured — calibrate against the dogfood PostgreSQL fleet before the next release. */
            Boost = 0.5,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.WraparoundTrend, out var f) && f.BaseSeverity > 0,
        },
    ];
}
