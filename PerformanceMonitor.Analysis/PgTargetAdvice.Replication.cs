/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Text;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Advice for replication lag and slot retention (filled by lane 12 of #3691, design §3.5 / §3.10). Value-stated
/// from the replication facts — the worst standby by its <c>application_name</c>, bytes behind and at which stage,
/// steady or drifting; the slot by name, plugin kind, WAL retained against the shared bar, how long inactive — and
/// honest that dropping a slot is a data-loss decision the operator owns (the subscriber loses its position),
/// never a performance win.
///
/// <para><b>House rules this file keeps.</b> Every number in the prose is read from the fact. Every
/// recommendation names its counter-objective. A lagging standby is read in the D7 frame — delivered versus
/// offered on the apply path: the primary offered N bytes of WAL, the standby has applied N − lag — so the stage
/// named tells the operator whether the gap is the network and sender (<c>sent</c>), the standby's disk
/// (<c>write</c> / <c>flush</c>) or its apply path (<c>replay</c>). Time-to-disk-full for a retaining slot is NOT
/// computed — there is no disk collector for PostgreSQL targets — and the block says so rather than inventing a
/// volume size. No <c>CREATE INDEX</c>; the one statement named (<c>pg_drop_replication_slot</c>) rides in the
/// prose with its cost, never in <see cref="AdviceBlock.RemediationTsql"/>.</para>
/// </summary>
public static partial class PgTargetAdvice
{
    private static partial AdviceBlock? ComposeReplication(string key, IReadOnlyDictionary<string, Fact> factsByKey) => key switch
    {
        PgTargetFactKeys.ReplicationLag => ComposeReplicationLag(factsByKey),
        PgTargetFactKeys.SlotRetention => ComposeSlotRetention(factsByKey),
        PgTargetFactKeys.SlotXmin => ComposeSlotXmin(factsByKey),
        _ => null,
    };

    /* ── PG_REPLICATION_LAG ── */

    private static readonly AdviceBlock s_lagStatic = new(
        Headline: "A standby is behind the primary by more WAL than its own recent history, or by more than the slot alert's bar",
        Investigation:
            "pg_stat_replication reports, per connected standby, how many bytes of WAL the primary has generated " +
            "that the standby has not yet received (sent), written, flushed, or replayed. The collector stores those " +
            "four gaps every five minutes; this finding names the standby furthest behind by its window peak on " +
            "replay — bytes, not replay_lag_ms, because the time lag understates a stall and some managed flavours " +
            "report it NULL. Read it as delivered versus offered: the primary offered N bytes, the standby applied " +
            "N minus the gap, and the stage where the gap opens says whether the bottleneck is the network and the " +
            "sender, the standby's disk, or its apply path. Steady at a distance is a consumer pacing behind; a gap " +
            "whose second-half mean is more than twice its first-half mean is a standby falling behind.",
        Remediation:
            "If the gap opens at replay, the standby's single apply process is the bottleneck: look for long queries " +
            "on the standby holding replay (max_standby_streaming_delay) and for the standby's own I/O; if at " +
            "write/flush, the standby's disk; if at sent, the network or the primary's wal_sender. For a SYNCHRONOUS " +
            "standby the lag is commit latency on the primary — the counter-objective of keeping it synchronous. " +
            "Check PG_SLOT_RETENTION: the WAL this standby has not replayed is WAL its slot makes the primary keep.");

    private static AdviceBlock ComposeReplicationLag(IReadOnlyDictionary<string, Fact> facts)
    {
        if (!facts.TryGetValue(PgTargetFactKeys.ReplicationLag, out var f))
            return s_lagStatic;

        var m = f.Metadata;
        var standby = string.IsNullOrEmpty(f.ObjectName) ? "an unnamed standby (no application_name)" : $"standby [{f.ObjectName}]";
        var peak = m.GetValueOrDefault(PgTargetScorer.LagPeakBytesKey, f.Value);
        var latest = m.GetValueOrDefault(PgTargetScorer.LagLatestBytesKey);
        var stage = PgTargetScorer.LagStageName(m.GetValueOrDefault(PgTargetScorer.LagStageKey, PgTargetScorer.LagStageReplay));
        var stageBytes = m.GetValueOrDefault(PgTargetScorer.LagStageBytesKey);
        var syncState = PgTargetScorer.SyncStateName(m.GetValueOrDefault(PgTargetScorer.LagSyncStateKey, PgTargetScorer.SyncStateUnknown));
        var synchronous = syncState is "sync" or "quorum";
        var streaming = m.GetValueOrDefault(PgTargetScorer.LagStandbyStreamingKey) >= 1;
        var drifting = m.GetValueOrDefault(PgTargetScorer.LagDriftingKey) >= 1;
        var driftComputable = m.GetValueOrDefault(PgTargetScorer.LagDriftComputableKey) >= 1;
        var firstHalf = m.GetValueOrDefault(PgTargetScorer.LagFirstHalfMeanBytesKey);
        var secondHalf = m.GetValueOrDefault(PgTargetScorer.LagSecondHalfMeanBytesKey);
        var msReported = m.GetValueOrDefault(PgTargetScorer.LagMsReportedKey) >= 1;
        var samples = m.GetValueOrDefault(PgTargetScorer.LagSamplesKey);
        var collections = m.GetValueOrDefault(PgTargetScorer.LagCollectionsKey);
        var standbys = m.GetValueOrDefault(PgTargetScorer.LagStandbysKey);
        var spanHours = m.GetValueOrDefault(PgTargetScorer.LagSpanHoursKey);
        var slotsObserved = m.GetValueOrDefault(PgTargetScorer.LagSlotsObservedKey);
        var overBar = peak >= PostgresOutagePredictorThresholds.SlotRetainedWalWarningBytes;
        var slot = facts.TryGetValue(PgTargetFactKeys.SlotRetention, out var sr) && sr.Severity > 0;
        var anomaly = facts.TryGetValue(PgTargetFactKeys.AnomalyReplicationLag, out var an) && an.Severity > 0;
        /* #3691 between waves: the WAL-volume co-fire is the ANOMALY's verdict — PG_WAL_VOLUME_SHIFT is a context
           fact at severity 0 by design (lane 15), so reading it here never produced the sentence. */
        var walShift = facts.TryGetValue(PgTargetFactKeys.AnomalyWalVolume, out var ws) && ws.Severity > 0;

        var shape = drifting ? "DRIFTING — falling further behind through the window" : "steady — pacing behind at a distance";
        var headline = f.Severity > 0
            ? $"{standby} peaked {FmtBytes(peak)} behind the primary at {stage}, {shape}"
            : $"{standby} is {FmtBytes(latest)} behind the primary at {stage} — {shape}; context, not a finding";

        var inv = new StringBuilder();
        inv.Append($"Worst of {standbys:0} standby(s) in the window: {standby}, state {(streaming ? "streaming" : "not streaming")}, sync_state {syncState}. ");
        inv.Append($"Window peak replay gap {FmtBytes(peak)}, latest {FmtBytes(latest)}; the stage furthest behind at the latest sample is {stage} ({FmtBytes(stageBytes)} opened there). ");
        inv.Append(driftComputable
            ? $"First-half mean {FmtBytes(firstHalf)} → second-half mean {FmtBytes(secondHalf)} over a {FmtHours(spanHours)} span: {(drifting ? "more than the drift multiple — the standby is falling behind" : "within the drift multiple — steady")}. "
            : "The drift could not be judged: the window held samples on one side of its midpoint only (one collection, or a standby that connected mid-window). ");
        inv.Append(msReported
            ? $"replay_lag_ms was reported (latest {m.GetValueOrDefault(PgTargetScorer.LagReplayMsLatestKey):N0} ms, peak {m.GetValueOrDefault(PgTargetScorer.LagReplayMsPeakKey):N0} ms); bytes are the graded quantity because the time lag understates a stall. "
            : "replay_lag_ms was not reported by this engine (NULL on every sample) — bytes are the only measure and are the graded one. ");
        inv.Append($"Evidence: {samples:0} samples of this standby across {collections:0} five-minute collections (the replication collector's own cadence, not the pass's one-minute coverage). ");
        if (overBar)
            inv.Append($"The peak is past the slot alert's bar ({FmtBytes(PostgresOutagePredictorThresholds.SlotRetainedWalWarningBytes)}) — unreplayed WAL on the standby is WAL a slotted primary must retain. ");
        if (synchronous)
            inv.Append("This standby is SYNCHRONOUS: every commit on the primary waits for it, so its lag is the primary's commit latency. ");
        if (slotsObserved <= 0)
            inv.Append("No slot state was observed in this window — either the standby runs without a replication slot (the primary may recycle WAL it still needs: wal_keep_size is then its only protection) or the slot collector did not run; the slot facts are absent, not clear. ");
        if (slot)
            inv.Append("PG_SLOT_RETENTION co-fired: a slot is retaining the WAL this standby has not consumed. ");
        if (anomaly)
            inv.Append("ANOMALY_PG_REPLICATION_LAG co-fired: against this server's own hour-of-week baseline the gap is anomalous, not this standby's routine distance. ");
        if (walShift)
            inv.Append("ANOMALY_PG_WAL_VOLUME co-fired: the primary is writing more WAL than its own hour-of-week baseline — more is being offered than the standby can apply. ");

        var rem = new StringBuilder();
        rem.Append(stage switch
        {
            "replay" => "The gap opens at REPLAY: the standby has the WAL and cannot apply it fast enough. Recovery is single-threaded, so look first for a long query on the standby holding replay (pg_stat_activity there; max_standby_streaming_delay decides whether replay waits or the query is cancelled — the counter-objective is cancelled replica queries), then at the standby's own I/O (a smaller instance than the primary cannot replay what the primary writes). ",
            "flush" or "write" => $"The gap opens at {stage.ToUpperInvariant()}: the standby received the WAL and its disk is not keeping up. The standby's storage is the bottleneck, not the network; the counter-objective of faster storage is cost. ",
            _ => "The gap opens at SENT: the WAL has not left the primary. Check the network between the two and the wal_sender process on the primary (pg_stat_replication.state, and whether the sender is throttled by a synchronous standby elsewhere). ",
        });
        if (synchronous)
            rem.Append("Because the standby is synchronous, the primary's commits are waiting on it now; moving it to async (synchronous_standby_names) restores commit latency at the cost of possible data loss on failover — that is the trade, stated. ");
        rem.Append(drifting
            ? "Drifting means the standby is not merely behind but losing ground: if the trend continues its slot (if any) retains WAL without bound and a failover loses more each hour. "
            : "Steady means the standby is pacing at a distance; a large steady distance is failover exposure of that many bytes, not an active failure. ");
        rem.Append("Verify with get_pg_replication_stats (per-standby stages and the worst-in-window values) and get_pg_replication_slots (what the primary retains for it).");

        return new AdviceBlock(headline, inv.ToString().TrimEnd(), rem.ToString().TrimEnd());
    }

    /* ── ANOMALY_PG_REPLICATION_LAG ── */

    /// <summary>The anomaly's composed block (called from <c>ComposeAnomaly</c>'s <c>ANOMALY_PG_REPLICATION_LAG</c> arm, lane
    /// 11's precedent): the peak worst-standby replay gap, sigmas above the hour-of-week baseline, the baseline itself —
    /// or the first-occurrence rendering on a low-quality bucket. The static block claims no figure.</summary>
    private static AdviceBlock ComposeReplicationLagAnomaly(IReadOnlyDictionary<string, Fact> factsByKey)
    {
        var fallback = ReplicationLagAnomalyStatic();
        return factsByKey.TryGetValue(PgTargetFactKeys.AnomalyReplicationLag, out var anomaly)
            ? ComposeDeviation(anomaly, fallback, "Standby replay gap", "peak_replay_bytes", v => FmtBytes(v) + " behind")
            : fallback;
    }

    /// <summary>Built per call (not <c>static readonly</c>) for the reason <c>PgTargetAdvice.Io.cs</c> states: it composes the
    /// hedge and remediation strings declared in <c>PgTargetAdvice.Anomaly.cs</c>, and static initialisers across partial files
    /// have no defined order.</summary>
    private static AdviceBlock ReplicationLagAnomalyStatic() => new(
        Headline: "A standby fell further behind the primary than this server's normal for this time of week",
        Investigation:
            "The window's peak worst-standby replay gap (MAX(replay_bytes_behind) per five-minute collection from " +
            "pg_replication_stats — bytes, because the millisecond lag understates a stall and some engines report it " +
            "NULL) was judged against this server's hour-of-week baseline of the same quantity over the last 30 days. " +
            "A lag anomaly says a standby is further behind than it usually is at this hour — not that it is far behind " +
            "in absolute terms; PG_REPLICATION_LAG names the standby, the stage the gap opens at, and whether it is " +
            "steady or drifting, and grades the absolute distance against the slot alert's bar." + s_anomalyHedge,
        Remediation:
            "get_pg_replication_stats shows each standby's stages with the worst-in-window values; get_pg_replication_slots " +
            "shows what the primary is retaining for it. " + s_anomalyRemediation);

    /* ── PG_SLOT_RETENTION ── */

    private static readonly AdviceBlock s_slotStatic = new(
        Headline: "A replication slot is retaining WAL past the slot alert's bar, or its wal_status says the WAL is gone",
        Investigation:
            "A replication slot makes the primary keep every WAL segment its consumer has not confirmed. With " +
            "max_slot_wal_keep_size at its default of -1 that retention is unbounded, so an abandoned slot fills the " +
            "volume and stops the server — one of the few PostgreSQL conditions that takes a server down by itself. " +
            "The collector computes retained bytes from the slot's restart_lsn every five minutes; this finding grades " +
            "the worst slot on the same bars the slot alert pages on: wal_status unreserved or lost is critical at " +
            "any size (the engine's own verdict that required WAL is gone or about to be), retained WAL at or past " +
            "the alert's byte bar is a warning, and inactive AND still growing past it is the disk-fill emergency. " +
            "Time to disk full is NOT computed: there is no disk collector for PostgreSQL targets.",
        Remediation:
            "If the consumer is gone for good (a removed CDC task, a finished blue/green, a decommissioned " +
            "subscriber), drop the slot — SELECT pg_drop_replication_slot('name') — and the primary recycles the WAL " +
            "at the next checkpoint. Counter-objective: the consumer loses its position and must re-initialise from " +
            "a fresh base copy or snapshot; a lost slot cannot be resumed regardless. If the consumer is merely " +
            "behind, fix or restart it. Set max_slot_wal_keep_size so the next abandoned slot is invalidated instead " +
            "of filling the disk — at the cost of that consumer needing a resync.");

    private static AdviceBlock ComposeSlotRetention(IReadOnlyDictionary<string, Fact> facts)
    {
        if (!facts.TryGetValue(PgTargetFactKeys.SlotRetention, out var f))
            return s_slotStatic;

        var m = f.Metadata;
        var slot = string.IsNullOrEmpty(f.ObjectName) ? "an unnamed slot" : $"slot [{f.ObjectName}]";
        var retained = m.GetValueOrDefault(PgTargetScorer.SlotRetainedBytesKey, f.Value);
        var first = m.GetValueOrDefault(PgTargetScorer.SlotFirstRetainedBytesKey);
        var growth = m.GetValueOrDefault(PgTargetScorer.SlotGrowthBytesKey);
        var spanHours = m.GetValueOrDefault(PgTargetScorer.SlotSpanHoursKey);
        var perHour = m.GetValueOrDefault(PgTargetScorer.SlotGrowthBytesPerHourKey);
        var active = m.GetValueOrDefault(PgTargetScorer.SlotActiveKey) >= 1;
        var walStatus = PgTargetScorer.WalStatusName(m.GetValueOrDefault(PgTargetScorer.SlotWalStatusKey));
        var terminal = walStatus is "lost" or "unreserved";
        var keepSizeSet = m.GetValueOrDefault(PgTargetScorer.SlotKeepSizeSetKey) >= 1;
        var safeWal = m.GetValueOrDefault(PgTargetScorer.SlotSafeWalBytesKey);
        var logical = m.GetValueOrDefault(PgTargetScorer.SlotLogicalKey) >= 1;
        var conflicting = m.GetValueOrDefault(PgTargetScorer.SlotConflictingKey) >= 1;
        var inactiveKnown = m.GetValueOrDefault(PgTargetScorer.SlotInactiveSinceKnownKey) >= 1;
        var inactiveHours = m.GetValueOrDefault(PgTargetScorer.SlotInactiveHoursKey);
        var arm = (int)m.GetValueOrDefault(PgTargetScorer.SlotArmKey);
        var slots = m.GetValueOrDefault(PgTargetScorer.SlotsInWindowKey);
        var graded = m.GetValueOrDefault(PgTargetScorer.SlotsGradedKey);
        var samples = m.GetValueOrDefault(PgTargetScorer.SlotSamplesKey);
        var kind = logical ? "logical" : "physical";
        var xmin = facts.TryGetValue(PgTargetFactKeys.SlotXmin, out var sx) && sx.Severity > 0;
        var lag = facts.TryGetValue(PgTargetFactKeys.ReplicationLag, out var rl) && rl.Severity > 0;
        /* #3691 between waves: the WAL-volume co-fire is the ANOMALY's verdict — PG_WAL_VOLUME_SHIFT is a context
           fact at severity 0 by design (lane 15), so reading it here never produced the sentence. */
        var walShift = facts.TryGetValue(PgTargetFactKeys.AnomalyWalVolume, out var ws) && ws.Severity > 0;

        var headline = arm switch
        {
            3 => $"{slot} ({kind}) is {walStatus.ToUpperInvariant()} — the WAL its consumer needs is gone or about to be; it retained {FmtBytes(retained)}",
            2 => $"{slot} ({kind}) is inactive and still accumulating: {FmtBytes(retained)} retained, up {FmtBytes(growth)} this window",
            1 => $"{slot} ({kind}) is retaining {FmtBytes(retained)} of WAL, {(growth > 0 ? $"growing {FmtBytes(growth)} this window" : "not growing")}",
            _ => $"{slot} ({kind}) retains {FmtBytes(retained)} of WAL, {walStatus}, {(active ? "consumer active" : "consumer inactive")} — under the bar; context, not a finding",
        };

        var inv = new StringBuilder();
        inv.Append($"Worst of {slots:0} slot(s) in the window ({graded:0} graded above zero): {slot}, {kind}, wal_status {walStatus}, consumer {(active ? "ACTIVE" : "INACTIVE")}. ");
        inv.Append($"Retained WAL {FmtBytes(retained)} at the latest sample against {FmtBytes(first)} at the first ({(growth > 0 ? $"+{FmtBytes(growth)}" : growth < 0 ? $"−{FmtBytes(-growth)}" : "unchanged")} over {FmtHours(spanHours)}{(spanHours > 0 && growth != 0 ? $", {FmtBytes(perHour)}/h" : string.Empty)}); the shared bar is {FmtBytes(PostgresOutagePredictorThresholds.SlotRetainedWalWarningBytes)}, the same symbol the slot alert pages on. ");
        inv.Append(inactiveKnown
            ? $"The engine reports it inactive for {FmtHours(inactiveHours)} (inactive_since). "
            : active ? string.Empty : "How long it has been inactive is not reported by this major (inactive_since is PostgreSQL 17+), so only the window's own samples witness the inactivity. ");
        inv.Append(keepSizeSet
            ? $"max_slot_wal_keep_size is set: {FmtBytes(safeWal)} of headroom remains before the engine invalidates the slot. "
            : "max_slot_wal_keep_size is at its default (-1): retention is unbounded and nothing will stop this pile short of the disk. ");
        if (conflicting)
            inv.Append("The slot is flagged conflicting (invalidated by a recovery conflict on a standby). ");
        inv.Append("Time to disk full is not computable here — no disk collector runs against PostgreSQL targets — so the growth rate above is the honest figure, against a volume size the operator knows. ");
        inv.Append($"Evidence: {samples:0} five-minute samples of this slot in the window. ");
        if (xmin)
            inv.Append("PG_SLOT_XMIN co-fired: a slot is also pinning the vacuum horizon — the second independent harm. ");
        if (lag)
            inv.Append("PG_REPLICATION_LAG co-fired: a connected standby is behind, so the retained WAL has a consumer that is not keeping up rather than none. ");
        if (walShift)
            inv.Append("ANOMALY_PG_WAL_VOLUME co-fired: the primary is writing more WAL than its own hour-of-week baseline, so the pile grows faster than its history suggests. ");

        var rem = new StringBuilder();
        rem.Append(arm switch
        {
            3 when walStatus == "lost" => "The slot is LOST: the WAL its consumer needs has been removed and the slot cannot resume. Drop it (SELECT pg_drop_replication_slot('…'); the finding names it) and re-create the consumer from a fresh base copy or snapshot — that resync is the cost, and it is already incurred. ",
            3 => "The slot is UNRESERVED: required WAL has already been removed and its consumer will fail on next connect. Treat it as lost — drop and resync — unless the consumer reconnects before the next checkpoint. ",
            2 => $"An inactive slot still accumulating is the disk-fill emergency. If its consumer is gone for good{(inactiveKnown ? $" (inactive {FmtHours(inactiveHours)})" : string.Empty)}, drop it now: SELECT pg_drop_replication_slot('…'). Counter-objective: the {(logical ? "logical subscriber (CDC pipeline, subscription)" : "standby")} loses its position and must re-initialise. If the consumer is expected back, its return must come before the volume fills — there is no time-to-full estimate here, so read the volume yourself. ",
            1 => $"Over the bar with {(active ? "an active" : "an inactive")} consumer{(growth > 0 ? " and growing" : ", not growing")}: find out whether the {(logical ? "subscriber" : "standby")} is merely behind (PG_REPLICATION_LAG names a connected standby's stage) or silently gone. Drop only a slot whose consumer will not return — the counter-objective is that consumer's resync. ",
            _ => "No action on this evidence alone; the slot is under the bar. If it recurs or grows, read the volume it lives on. ",
        });
        if (!keepSizeSet)
            rem.Append("Set max_slot_wal_keep_size so the NEXT abandoned slot is invalidated by the engine instead of filling the disk; counter-objective: a consumer that falls that far behind then needs a resync rather than a catch-up. ");
        rem.Append("Verify with get_pg_replication_slots (every slot, growth across the window) and, for the horizon side, get_pg_xmin_horizon.");

        return new AdviceBlock(headline, inv.ToString().TrimEnd(), rem.ToString().TrimEnd());
    }

    /* ── PG_SLOT_XMIN ── */

    private static readonly AdviceBlock s_slotXminStatic = new(
        Headline: "A replication slot's xmin or catalog_xmin is holding the vacuum horizon back, persistently",
        Investigation:
            "A slot with an xmin (a physical slot whose standby sends hot_standby_feedback, or a logical slot) " +
            "exports its consumer's oldest snapshot to the primary: VACUUM cannot remove tuples newer than it " +
            "anywhere in the cluster, and a logical slot's catalog_xmin does the same for the system catalogs. The " +
            "collector stores both ages every five minutes; this finding grades the slot with the older horizon on " +
            "the same bars the xmin alert uses — the shared age bar, held in a majority of the slot's own samples, " +
            "over at least the minimum observations — so a consumer briefly behind is context and a stuck one is a " +
            "finding. It is the slot-side leaf of PG_XMIN_HOLD, kept separate so a session holder and a slot holder " +
            "are never conflated.",
        Remediation:
            "If the consumer is gone, drop the slot (pg_drop_replication_slot) — the counter-objective is that " +
            "consumer's resync. If it is a standby with hot_standby_feedback = on, the hold follows its longest " +
            "query: end that query or turn feedback off there and accept cancelled replica queries instead. If it " +
            "is a logical consumer that has stopped confirming, fix the pipeline; an abandoned CDC pipeline later " +
            "revived must resnapshot. Do not turn autovacuum off to quiet the symptom.");

    private static AdviceBlock ComposeSlotXmin(IReadOnlyDictionary<string, Fact> facts)
    {
        if (!facts.TryGetValue(PgTargetFactKeys.SlotXmin, out var f))
            return s_slotXminStatic;

        var m = f.Metadata;
        var slot = string.IsNullOrEmpty(f.ObjectName) ? "an unnamed slot" : $"slot [{f.ObjectName}]";
        var age = m.GetValueOrDefault(PgTargetScorer.SlotXminAgeKey, f.Value);
        var xminAge = m.GetValueOrDefault(PgTargetScorer.SlotXminPhysicalAgeKey);
        var catalogAge = m.GetValueOrDefault(PgTargetScorer.SlotXminCatalogAgeKey);
        var catalogArm = m.GetValueOrDefault(PgTargetScorer.SlotXminArmIsCatalogKey) >= 1;
        var samples = m.GetValueOrDefault(PgTargetScorer.SlotXminSamplesKey);
        var above = m.GetValueOrDefault(PgTargetScorer.SlotXminObservationsAboveKey);
        var identityArm = m.GetValueOrDefault(PgTargetScorer.SlotXminIdentityArmKey) >= 1;
        var freezeMaxAge = m.GetValueOrDefault(PgTargetScorer.XminFreezeMaxAgeKey);
        var active = m.GetValueOrDefault(PgTargetScorer.SlotActiveKey) >= 1;
        var logical = m.GetValueOrDefault(PgTargetScorer.SlotLogicalKey) >= 1;
        var inactiveKnown = m.GetValueOrDefault(PgTargetScorer.SlotInactiveSinceKnownKey) >= 1;
        var inactiveHours = m.GetValueOrDefault(PgTargetScorer.SlotInactiveHoursKey);
        var horizon = catalogArm ? "catalog_xmin" : "xmin";
        var kind = logical ? "logical" : "physical";
        var hold = facts.TryGetValue(PgTargetFactKeys.XminHold, out var xh) && xh.Severity > 0;
        var backlog = facts.TryGetValue(PgTargetFactKeys.AutovacuumBacklog, out var bl) && bl.Severity > 0;
        var wraparound = facts.TryGetValue(PgTargetFactKeys.WraparoundTrend, out var wa) && wa.Severity > 0;

        var headline = identityArm
            ? $"{slot} ({kind}) is holding the vacuum horizon {Fmt(age)} transactions back through its {horizon}, in {above:0} of {samples:0} samples"
            : $"{slot} ({kind}) held the horizon {Fmt(age)} transactions back through its {horizon} — briefly, not persistently; context";

        var inv = new StringBuilder();
        inv.Append($"{slot}, {kind}, consumer {(active ? "active" : "INACTIVE")}{(inactiveKnown ? $" for {FmtHours(inactiveHours)}" : string.Empty)}: xmin_age {Fmt(xminAge)}, catalog_xmin_age {Fmt(catalogAge)}; the older horizon ({horizon}) is {Fmt(age)} transactions back against the shared warning bar of {Fmt(PostgresOutagePredictorThresholds.XminAgeWarningThreshold)}. ");
        inv.Append($"Persistence: at or above the bar in {above:0} of the slot's {samples:0} five-minute samples ({(samples > 0 ? above / samples : 0):P0}; the alert's standard is {PostgresOutagePredictorThresholds.XminPersistenceFraction:P0} over at least {PostgresOutagePredictorThresholds.XminMinimumObservations} observations). ");
        inv.Append(identityArm
            ? "That is the chronic-holder shape the xmin alert pages on, read from the slot's own rows. "
            : "That does not meet the chronic-holder standard — a consumer that fell behind and caught up looks like this — so it is context, not a finding. ");
        if (freezeMaxAge > 0)
            inv.Append($"Held past this server's autovacuum_freeze_max_age ({Fmt(freezeMaxAge)}) the slot would also stop the anti-wraparound vacuum advancing relfrozenxid; the severity ramps toward critical as the age approaches it. ");
        if (hold)
            inv.Append("PG_XMIN_HOLD co-fired: the vacuum family's own read attributes the cluster's winning horizon holder — check whether it names this slot. ");
        if (backlog)
            inv.Append("PG_AUTOVACUUM_BACKLOG co-fired: tables are past their trigger line and staying there — dead tuples newer than this horizon cannot be removed. ");
        if (wraparound)
            inv.Append("PG_WRAPAROUND_TREND co-fired: the freeze age is already climbing past the engine's line. ");

        var rem = logical
            ? $"A logical slot's {horizon} advances only when its subscriber confirms. If the pipeline (CDC, logical subscription) is stopped and will not return, drop the slot — SELECT pg_drop_replication_slot('…') — and the horizon releases at once; counter-objective: a revived pipeline must resnapshot from scratch. If it is running but behind, find why it is not confirming (a subscriber applying slowly, a decoder stuck on a large transaction) before dropping anything."
            : "A physical slot carries an xmin only when its standby sends hot_standby_feedback: the hold follows the standby's longest-running query. End that query, or set hot_standby_feedback = off on the standby and accept query cancellations there (max_standby_streaming_delay) — counter-objective: replica queries cancelled by conflicting cleanup. If the standby is gone for good, drop the slot; counter-objective: a returning standby must be re-based.";
        if (!identityArm)
            rem = "No action on this evidence alone — a transient hold is normal; if it recurs: " + rem;
        rem += " Verify with get_pg_xmin_horizon (which source is winning the horizon) and get_pg_replication_slots.";

        return new AdviceBlock(headline, inv.ToString().TrimEnd(), rem);
    }
}
