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

namespace PerformanceMonitor.Alerting;

/// <summary>
/// Watermark / edge-trigger state persistence for the Phase-5 shared alert engine — the state that
/// must survive a host restart so the first post-restart sweep doesn't re-alert (and re-post to
/// Teams/Slack) conditions the user already saw (#1145). One of the three engine seams (with
/// <see cref="IAlertEngineSettings"/> and <see cref="IAlertDeliverer"/>), consumed by the headless
/// Darling alert engine first; Lite forwards later; Dashboard convergence is a separately-decided
/// migration.
/// <para>
/// The method names and semantics are modeled on Lite's <c>DuckDbAlertHistoryStore</c> watermark
/// methods, genericized from Lite's <c>int serverId</c> to a string <paramref name="serverKey"/> so
/// any host identity scheme fits (Lite: the deterministic storage-name hash rendered as a string;
/// Darling: its server key; Dashboard: its GUID server id). Loads are per-key here where Lite's are
/// bulk-at-startup — an implementation over a bulk-loading store can cache, as long as saves are
/// read-your-writes within the engine's sweep loop.
/// </para>
/// <para>
/// Expected implementations: Lite = the DuckDB <c>config_edge_trigger_watermarks</c> table
/// (INSERT OR REPLACE upsert on the server/metric primary key); Darling = Postgres
/// (INSERT ... ON CONFLICT upsert); Dashboard = the <c>UserPreferences</c> ticks dictionary
/// (<c>FailedJobAlertWatermarkTicks</c>) if it ever migrates. Saves are on-change only and
/// low-frequency — the gate returns the same watermark on the vast majority of sweeps.
/// </para>
/// </summary>
public interface IAlertStateStore
{
    /// <summary>
    /// Loads the persisted rolling-count edge-trigger watermark (#1091/#1145: the highest
    /// already-alerted rolling-window count) for one server/metric, or <c>null</c> when none has
    /// been persisted. The engine seeds its in-memory watermark from this at startup so lingering
    /// blocked-process reports / deadlocks don't re-alert on the first sweep.
    /// </summary>
    Task<int?> LoadEdgeTriggerWatermarkAsync(string serverKey, string metricName);

    /// <summary>
    /// Upserts one rolling-count edge-trigger watermark. Called on-change only — a low-frequency
    /// write (Lite piggybacks it on the alert-store write lock; Darling upserts a Postgres row).
    /// </summary>
    Task SaveEdgeTriggerWatermarkAsync(string serverKey, string metricName, int watermark);

    /// <summary>
    /// Loads the persisted failed-job watermark — the newest already-alerted failure's
    /// SERVER-LOCAL run time — or <c>null</c> when none has been persisted. The value is stored
    /// and returned in its native server-local basis (NEVER coerced to UTC) so it compares
    /// directly against <see cref="FailedJobInfo.RunDateTime"/>; mixing bases here re-fires or
    /// eats failed-job alerts by the timezone offset.
    /// </summary>
    Task<DateTime?> LoadFailedJobWatermarkAsync(string serverKey);

    /// <summary>
    /// Upserts the failed-job watermark for one server (see
    /// <see cref="LoadFailedJobWatermarkAsync"/> for the time-basis contract). Called on-change
    /// only — when a failed-job alert actually fires.
    /// </summary>
    Task SaveFailedJobWatermarkAsync(string serverKey, DateTime watermark);

    /// <summary>
    /// Records the effective state a database was just alerted about (#2166), so the next evaluation can
    /// tell a NEW deviation from the same one it already reported. Called on-fire only, which makes it a
    /// low-frequency write like the watermarks above.
    ///
    /// <para>Persistence is the requirement, not an optimization: the case this exists for is a database
    /// deliberately parked OFFLINE for weeks. In-memory edge state would re-fire every parked database on
    /// every service restart, which is worse than the cooldown-repeat being replaced.</para>
    ///
    /// <para>A host that does not persist it may no-op; the engine then sees no memory, every deviation
    /// reads as new, and behavior is exactly the pre-#2166 cooldown-repeat. That is the intended fallback
    /// rather than a broken state.</para>
    /// </summary>
    Task SaveDatabaseStateAlertedAsync(string serverKey, string databaseName, string effectiveState);

    /// <summary>
    /// Forgets what <see cref="SaveDatabaseStateAlertedAsync"/> recorded, called when a database returns to
    /// its expected state (#2166). The falling edge of an edge trigger: without it the memory is permanent
    /// and each database can only ever announce once, so a second parking of the same database in the same
    /// state — the repeat soft-delete workflow this alert is FOR — would be silently swallowed.
    ///
    /// <para>Same no-op fallback as its sibling for hosts that do not persist: no memory to clear means
    /// nothing to go stale.</para>
    /// </summary>
    Task ClearDatabaseStateAlertedAsync(string serverKey, string databaseName);

    /// <summary>
    /// Loads the per-fingerprint occurrence accounting for one server/metric (#2216), keyed by #1140 dedup
    /// fingerprint — the state <see cref="IncidentOccurrenceAccumulator"/> needs to turn the groupers'
    /// window gauge into a monotonic total. Returns an empty map when nothing is persisted.
    ///
    /// <para>Persistence is the requirement, not an optimization: the value of the counter is that it
    /// survives the throttling the consumer is subject to, and a counter reset by every service restart
    /// would be a second gauge wearing a total's name. A host that cannot persist may return an empty map
    /// and no-op the save — the accumulator then reports the total as equal to the window count, which is
    /// exactly the pre-#2216 information rather than a wrong number.</para>
    /// </summary>
    Task<IReadOnlyDictionary<string, IncidentOccurrenceState>> LoadIncidentOccurrencesAsync(
        string serverKey, string metricName);

    /// <summary>
    /// REPLACES the persisted occurrence set for one server/metric with <paramref name="states"/> (#2216):
    /// rows in the map are upserted, and rows the store holds for this (server, metric) that are NOT in the
    /// map are deleted. An empty map therefore clears the metric — which is how the falling edge is
    /// recorded, so there is no separate clear method to forget to call.
    ///
    /// <para>Replace-the-set rather than upsert-each-row because absence carries meaning: a fingerprint
    /// with no events left in the window has a FINISHED incident, and leaving its row behind would make the
    /// next incident on that fingerprint read as a continuation of the old one — an undercount reported
    /// with a stale start time. The accumulator's staleness horizon is the backstop for rows a crash
    /// stranded before any of this could run; it is not a substitute for deleting them here.</para>
    ///
    /// <para>Called once per delivered alert (cooldown-gated by construction), so it is a low-frequency
    /// write like the watermarks above. Implementations should make the upsert-and-delete atomic: a
    /// partially-applied set can strand a row that the staleness horizon then has to catch.</para>
    /// </summary>
    Task SaveIncidentOccurrencesAsync(
        string serverKey, string metricName, IReadOnlyDictionary<string, IncidentOccurrenceState> states);
}
