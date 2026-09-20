/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// The per-server input to one <see cref="AlertEngine.EvaluateServerAsync"/> sweep — the fields
/// Lite's <c>ServerSummaryItem</c> fed <c>CheckPerformanceAlerts</c>, minus everything the engine
/// now derives itself through <see cref="IAlertReadAdapter"/> (blocking/deadlock counts) and minus
/// the UI-only members (display brushes, badges). The host builds one per server per sweep.
/// </summary>
/// <param name="ServerKey">
/// The engine's stable identity for the server — the same key convention as
/// <see cref="IAlertStateStore"/>/<see cref="IAlertReadAdapter"/> (Lite/Darling: the deterministic
/// storage-name hash rendered as a string). All engine state dictionaries key on it.
/// </param>
/// <param name="ServerName">Display name rendered in notifications and matched by mute rules.</param>
/// <param name="IsOnline">
/// Whether the monitored server is currently reachable. Gates ONLY the live failed-jobs check
/// (Lite gates it on <c>connStatus.IsOnline == true</c>); every collected-store check evaluates
/// regardless, exactly like Lite's loop.
/// </param>
/// <param name="SqlCpuPercent">
/// Latest collected SQL Server scheduler ProcessUtilization %, or null when no sample exists
/// (mirrors <c>ServerSummaryItem.CpuPercent</c>).
/// </param>
/// <param name="TotalCpuPercent">
/// Latest total non-idle CPU % (SQL process + other processes), or null when the SQL sample is
/// null (mirrors <c>ServerSummaryItem.TotalCpuPercent</c> = CpuPercent + (OtherProcessCpuPercent ?? 0)).
/// </param>
/// <param name="IsAzureSqlDb">
/// True for Azure SQL DB — skips the failed-jobs check (no SQL Agent), mirroring Lite's
/// <c>SqlEngineEdition != 5</c> call-site gate.
/// </param>
/// <param name="CpuSampleTimeUtc">
/// The instant of the CPU sample <paramref name="SqlCpuPercent"/>/<paramref name="TotalCpuPercent"/> came
/// from, or null when the host has no sample instant for it. This is the CPU check's persistence-gate
/// observation identity (#3282), not display data: the gate counts consecutive breaching SAMPLES, and the
/// alert sweep is twice as fast as the samples arrive, so without it a re-read of one sample would count as
/// a second observation of the condition.
/// <para><b>Which clock (#3744).</b> The row's <c>sample_time_utc</c> twin where the store has one — every
/// <c>cpu_utilization_stats</c> row written since Darling V134 / Lite v63 (#3730), so on any live store the
/// name is true within one collector poll of the upgrade — and otherwise the row's <c>sample_time</c>, which
/// on the ring-buffer arm is the MONITORED SERVER'S LOCAL wall clock (<c>CollectorTimestampFrameTests</c> pins
/// that frame). Both hosts resolve it the same way, <c>sample_time_utc ?? sample_time</c>, at the read: Darling
/// in <c>DarlingWorker.ReadLatestCpuAsync</c>, Lite in <c>MainWindow.AlertEngine</c> off the overview read's
/// two columns. There is no frame field beside this one, deliberately: the gate compares identities for
/// EQUALITY (<c>AlertEngine.ObservePersistenceAsync</c>), so which clock stamped the value is not an input
/// to any decision the engine makes — the local→UTC switch at the upgrade reads as one new sample, once, and
/// the fall-back's repeated hour as an hour of new samples, exactly as it should. A field nothing reads is a
/// field that drifts. Before #3744 the name was a lie on every row (the value was always the local stamp);
/// it is now true wherever the store can say the instant, and documented-local where it cannot.</para>
/// <para>No default, so every host states it. A null degrades the gate to counting SWEEPS rather than
/// samples — weaker persistence, but the alert still fires, which is the correct direction for a monitoring
/// product where silence is indistinguishable from health. The opposite default (treat an unknown instant as
/// stale and never count it) would make the alert silent on a host that forgot to wire this, and nothing
/// would say so.</para>
/// </param>
/// <param name="Suppressed">
/// Suppression is an INPUT (Phase-5 review): true = evaluate-but-don't-deliver, exactly Lite's
/// <c>suppressPopups</c> — edge-trigger watermarks don't advance where Lite's don't. Lite forwards
/// its per-server acknowledge/silence state here; Darling always passes false.
/// </param>
public sealed record AlertServerSnapshot(
    string ServerKey,
    string ServerName,
    bool IsOnline,
    double? SqlCpuPercent,
    double? TotalCpuPercent,
    bool IsAzureSqlDb,
    bool Suppressed,
    DateTime? CpuSampleTimeUtc);
