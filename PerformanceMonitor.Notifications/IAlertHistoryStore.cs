/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// Persistence boundary for the alert/notification history that the alert
/// services write to and read their cooldown seeds from. Implemented per-app:
///   Lite      → DuckDB <c>config_alert_log</c> (real async I/O).
///   Dashboard → in-memory <c>List&lt;AlertLogEntry&gt;</c> persisted to
///               <c>alert_history.json</c> (the read scan is wrapped in
///               <see cref="Task.FromResult{TResult}(TResult)"/>).
/// All reads are async so the shared service body is identical across apps.
/// The Dashboard-only history-management surface (GetAlertHistory / Hide*) is
/// NOT on this interface — it lives on the Dashboard JSON store impl.
/// </summary>
public interface IAlertHistoryStore
{
    /// <summary>
    /// Persists one alert event. The record carries BOTH display strings and
    /// optional numerics so each impl persists its native shape without loss:
    ///   Lite  → INSERT into config_alert_log (numeric current/threshold → DOUBLE cols)
    ///   Dash  → new AlertLogEntry{...} added to in-memory list + trim to 1000
    /// The alert timestamp is stamped <see cref="DateTime.UtcNow"/> at record time.
    /// </summary>
    Task RecordAlertAsync(AlertHistoryRecord record);

    /// <summary>
    /// MAX(alert_time) filtered to a *successful email send* — seeds the email
    /// cooldown across restart (#981). Lite: notification_type IN
    /// ('email','email+webhook') AND send_error IS NULL. Dash: NotificationType
    /// == "email" AND SendError empty.
    /// <para>
    /// The send_error clause is why <see cref="AlertDelivery.FromFanout"/> keeps a failed webhook's error
    /// off a row that delivered: an error borrowed from a sibling channel would take a successful email out
    /// of this seed and re-send it after a restart.
    /// </para>
    /// <para>
    /// When <paramref name="dedupKey"/> is non-null (#1154 per-fingerprint cooldown), the result is
    /// additionally restricted to rows whose persisted <c>ContextJson</c> carries that #1140 dedup
    /// fingerprint, so the seed reconstructs the per-incident last-sent time. Null = the metric-level
    /// seed (the pre-#1154 behavior, used by the non-fingerprinted fallback).
    /// </para>
    /// </summary>
    Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null);

    /// <summary>
    /// MAX(alert_time) filtered to a *successful webhook send* — seeds the webhook
    /// cooldown across restart so a Teams/Slack alert delivered shortly before a restart
    /// is not re-posted afterward (#1145, mirroring the email seed #981). The
    /// notification_type already implies the webhook delivered (it's only written on a
    /// successful post), so send_error is NOT filtered on. It can hold a webhook channel's error
    /// (<see cref="AlertDelivery.ChannelFailed"/>), but only on a row that delivered nothing, and those
    /// rows carry neither of the notification_type values below.
    /// Lite: notification_type IN ('webhook','email+webhook'). Dash: NotificationType == "webhook".
    /// <para>
    /// When <paramref name="dedupKey"/> is non-null (#1154 per-fingerprint cooldown), the result is
    /// additionally restricted to rows whose persisted <c>ContextJson</c> carries that #1140 dedup
    /// fingerprint. Null = the metric-level seed (the pre-#1154 behavior).
    /// </para>
    /// </summary>
    Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null);

    /// <summary>
    /// MAX(alert_time) over every row that was a PAGING candidate (any channel/result, muted or not) — the
    /// #2716 seed of Darling's Postgres Tier-0-predictor cooldowns (called with <paramref name="dedupKey"/>).
    /// Those live cooldowns are stamped UNCONDITIONALLY, even when muted or undelivered (a muted alert still
    /// consumes its cooldown, mirroring AlertEngine), so "any result" is the persisted equivalent THERE.
    /// <para>#3916: this is no longer the analysis seed. The analysis #2054 hold must be EARNED by a delivery,
    /// and a row that reached no one (a month-old undelivered <c>tray</c> row held one production story silent
    /// for 35 days) must not arm it — the analysis seed moved to <see cref="GetLastDeliveredPageUtcAsync"/>.
    /// Do not add <c>alert_sent</c> here: it would break the Tier-0 family's seed/stamp agreement.</para>
    /// <para>#3712: rows the corroboration gate routed to the digest (<c>notification_type =
    /// <see cref="AlertDelivery.ChannelDigest"/></c>) are EXCLUDED. A digest entry is not a page, and a
    /// seed that read one would hold the page a story earns when it later gains corroboration as a repeat
    /// of a page that never happened — design point 2 of #3712 makes that escalation a NEW firing. Before
    /// #3712 this read was unfiltered, and every row it could see was a paging candidate, so the exclusion
    /// changes nothing about the rows that existed then.</para>
    /// <para>
    /// When <paramref name="dedupKey"/> is non-null (#1154 per-fingerprint cooldown, reused by #2716 to
    /// seed Darling's Postgres Tier-0-predictor cooldowns), the result is additionally restricted to rows
    /// whose persisted <c>ContextJson</c> carries that #1140 dedup fingerprint. Null = the metric-level
    /// seed (the pre-#1154 behavior, used by the non-fingerprinted fallback).
    /// </para>
    /// </summary>
    Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null);

    /// <summary>
    /// MAX(alert_time) over the rows that were a DELIVERED page — <c>alert_sent</c> true, minus the #3712
    /// digest exclusion — for (serverId, metricName). Seeds the analysis per-finding #2054 hold across a
    /// restart (#3916): a hold must be earned by a delivery, so a row that reached no one (no channel
    /// configured, every channel failed, muted) seeds nothing and the story is heard on its next firing.
    /// <para>The digest exclusion is redundant by construction (a digest row is never Sent) and is kept so
    /// the exclusion stays one symbol and stays pinned.</para>
    /// <para>Dashboard's JSON store answers with ANY row: every Dashboard analysis page raises the tray
    /// balloon through the shared service's sink, so every page row there is a delivered page.</para>
    /// </summary>
    Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName);

    /// <summary>
    /// #5715 part B: the tier of the newest FIRE row for (<paramref name="serverId"/>, <paramref name="metricName"/>)
    /// at or after <paramref name="sinceUtc"/>, or null. Seeds <c>TierEscalation</c> across a restart, so a Warning
    /// delivered just before the restart still lets the Critical that follows inside the cooldown through at once.
    /// <para>The tier is read from the row's context JSON as <c>AlertHistoryRowSeverity.FiredAt</c> reads it; a row with
    /// none (Volume Free Space persists a tier only for a Critical fire) is <paramref name="tierWhenRowCarriesNone"/>,
    /// which the engine sets to Warning for that one family and leaves null for every other, so a tierless row seeds
    /// nothing there.</para>
    /// <para>Null when no such row exists, when the newest one carries no tier and no fallback is given, or when a resolution row (<paramref name="resolutionMetricName"/>,
    /// the resolution's title: "CPU Resolved", "Volume Free Space Resolved") is as new as the fire or newer. At an exact
    /// tie the resolution wins (<see cref="AlertHistoryEscalationSeed.Resolve"/>).</para>
    /// <para>Counted: a muted fire (the engine records its tier in process; pass <paramref name="countMuted"/> false for
    /// the one family whose fire site does not), and a fire that was throttled, unconfigured or folded. Not counted: a fire
    /// whose every attempted channel failed (<see cref="AlertHistoryEscalationSeed.KeepUnlessEveryChannelFailedFilter"/>),
    /// because in process that tier is put back.</para>
    /// <para><paramref name="untilUtc"/> (round 1 of #5715) is an EXCLUSIVE upper bound on the fire row's time, null for
    /// none: the engine passes its own start instant, because a row at or after it was written by this process, whose
    /// in-memory tier already covers it. Both stores bound at microsecond precision
    /// (<see cref="AlertHistoryEscalationSeed.TruncateToMicroseconds"/>), the precision they store, so a row written
    /// in the same tick the engine started is not read back as history.</para>
    /// <para>Server identity is scoped as <see cref="GetLastEmailSentUtcAsync"/> scopes it
    /// (<see cref="AlertHistoryServerIdentity.SeedScope"/>): a key with no per-key rows answers null.</para>
    /// <para>Unlike the cooldown seeds above, a failed read THROWS instead of degrading to null: the engine must be able
    /// to tell "no history" from "could not read", so it logs the second and retries it on the next sweep. The default
    /// body answers null for a store that keeps no tier history (a test double, the deprecated Dashboard store); both
    /// real stores override it, and a test pins that.</para>
    /// </summary>
    Task<AlertSeverityLevel?> GetLastFiredTierAsync(
        string serverId, string metricName, string resolutionMetricName, DateTime sinceUtc, bool countMuted = true,
        AlertSeverityLevel? tierWhenRowCarriesNone = null, DateTime? untilUtc = null) =>
        Task.FromResult<AlertSeverityLevel?>(null);

    /// <summary>
    /// #7068: the newest FIRE row for (<paramref name="serverId"/>, <paramref name="metricName"/>) that no resolution row
    /// closed, with the value it was written at and its context JSON, or null. Seeds a standing alert's "already told" state
    /// across a restart: a Volume Free Space or Version Store (PVS) condition that has stood for days must not read as a new
    /// incident each time the service starts. The row is the newest one before <paramref name="untilUtc"/> (EXCLUSIVE, the
    /// engine's start: a later row is this process's own) and, when <paramref name="sinceUtc"/> is given, at or after it.
    /// <para>Open means no row under any of <paramref name="resolutionMetricNames"/> is as new as the fire (a tie closes it,
    /// as <see cref="AlertHistoryEscalationSeed.Resolve"/> has it). A fire whose every attempted channel failed is skipped
    /// (<see cref="AlertHistoryEscalationSeed.KeepUnlessEveryChannelFailedFilter"/>), so the retry still fires; a muted fire
    /// counts unless <paramref name="countMuted"/> is false. Server identity is scoped as
    /// <see cref="GetLastEmailSentUtcAsync"/> scopes it. A failed read THROWS, like <see cref="GetLastFiredTierAsync"/>.</para>
    /// </summary>
    Task<AlertHistoryFire?> GetOpenFireAsync(
        string serverId, string metricName, System.Collections.Generic.IReadOnlyList<string> resolutionMetricNames,
        DateTime? sinceUtc, DateTime? untilUtc, bool countMuted = true) =>
        Task.FromResult<AlertHistoryFire?>(null);

    /// <summary>
    /// #7068: every fire row and every resolution row for (<paramref name="serverId"/>, <paramref name="metricName"/>), under the
    /// same window, mute and failed-send rules as <see cref="GetOpenFireAsync"/>, so a caller can ask which fires are still open
    /// FOR ONE GRAIN (a file, a plan, an availability group database) with <see cref="AlertHistoryStanding.OpenFor"/>. The newest
    /// open row alone is not enough when several things stand at once on one server: only the one it names would stay quiet
    /// after a restart. One read per server and metric. The default body wraps <see cref="GetOpenFireAsync"/>: a store that
    /// answers only that (a test double) yields at most the one row. A failed read THROWS, like <see cref="GetOpenFireAsync"/>.
    /// </summary>
    async Task<AlertHistoryStanding> GetStandingAsync(
        string serverId, string metricName, System.Collections.Generic.IReadOnlyList<string> resolutionMetricNames,
        DateTime? sinceUtc, DateTime? untilUtc, bool countMuted = true) =>
        AlertHistoryStanding.Of(await GetOpenFireAsync(serverId, metricName, resolutionMetricNames, sinceUtc, untilUtc, countMuted));

    /// <summary>
    /// #7076: <see cref="GetStandingAsync"/> for a store-wide alert keyed by job or policy: the fire and resolution rows written under
    /// <c>server_id</c> 0 whose <c>alert_key</c> is <paramref name="alertKey"/>. Rows from before the column existed carry no key and are
    /// not matched. The default body wraps <see cref="GetOpenFireAsync"/> with the key as the server key, as
    /// <see cref="GetStandingAsync"/> does: a store that does not persist the key (Lite never writes these alerts) finds no row for a
    /// non-numeric key, and a test double that answers only that read yields at most the one row. A failed read THROWS, like
    /// <see cref="GetStandingAsync"/>.
    /// </summary>
    async Task<AlertHistoryStanding> GetKeyedStandingAsync(
        string alertKey, string metricName, System.Collections.Generic.IReadOnlyList<string> resolutionMetricNames,
        DateTime? sinceUtc, DateTime? untilUtc, bool countMuted = true) =>
        AlertHistoryStanding.Of(await GetOpenFireAsync(alertKey, metricName, resolutionMetricNames, sinceUtc, untilUtc, countMuted));
}

/// <summary>#7068: one fire row read back by <see cref="IAlertHistoryStore.GetOpenFireAsync"/>: when it was written, the
/// numeric value it carried (the worst volume's free percent, the worst database's PVS percent) and its context JSON.</summary>
public sealed record AlertHistoryFire(DateTime FireAtUtc, double CurrentValue, string? ContextJson);

/// <summary>#7068: one resolution row (a clear) read back with the fire rows: when it was written, its context and its message.
/// A clear that names its grain in either lets <see cref="AlertHistoryStanding.OpenFor"/> close only that grain's fires.</summary>
public sealed record AlertHistoryClear(DateTime AtUtc, string? ContextJson, string? DetailText);

/// <summary>
/// #7068: what <see cref="IAlertHistoryStore.GetStandingAsync"/> read: the fire rows (newest first) and the resolution rows of one
/// server and metric. <see cref="OpenFor"/> answers, for one grain, the newest fire row that grain owns that none of its clears
/// closed. Pure: the tie rule is <see cref="AlertHistoryEscalationSeed.IsOpen"/>, shared with both stores.
/// </summary>
public sealed class AlertHistoryStanding
{
    public static readonly AlertHistoryStanding Empty = new(
        System.Array.Empty<AlertHistoryFire>(), System.Array.Empty<AlertHistoryClear>());

    public AlertHistoryStanding(
        System.Collections.Generic.IReadOnlyList<AlertHistoryFire> fires, System.Collections.Generic.IReadOnlyList<AlertHistoryClear> clears)
    {
        Fires = fires;
        Clears = clears;
    }

    /// <summary>Fire rows, newest first.</summary>
    public System.Collections.Generic.IReadOnlyList<AlertHistoryFire> Fires { get; }

    public System.Collections.Generic.IReadOnlyList<AlertHistoryClear> Clears { get; }

    /// <summary>A standing read that holds at most one fire and no clears (a store that answers only the newest open row).</summary>
    public static AlertHistoryStanding Of(AlertHistoryFire? fire) =>
        fire is null ? Empty : new(new[] { fire }, System.Array.Empty<AlertHistoryClear>());

    /// <summary>
    /// The standing a store read when its resolution read may have stopped at its row cap. The clears are the NEWEST
    /// <paramref name="clears"/> the cap allowed, so a clear older than the oldest one read may exist and was not read: a fire
    /// at or before that instant could have been closed by it, and reads as open only because the closing row fell outside the
    /// window. Those fires are dropped (not open, so the alert fires: the safe direction, never a silence). When the read did
    /// not reach the cap (<paramref name="oldestClearReadUtc"/> null: the caller passes the oldest read only when the read hit its cap) every clear after the oldest fire is in hand and nothing is
    /// dropped. Strictly newer, because a cap can cut a run of rows that share the oldest read instant.
    /// </summary>
    public static AlertHistoryStanding FromCappedRead(
        System.Collections.Generic.IReadOnlyList<AlertHistoryFire> fires,
        System.Collections.Generic.IReadOnlyList<AlertHistoryClear> clears,
        DateTime? oldestClearReadUtc)
    {
        if (oldestClearReadUtc is not { } cutoff)
        {
            return new AlertHistoryStanding(fires, clears);
        }

        var known = new System.Collections.Generic.List<AlertHistoryFire>(fires.Count);
        foreach (var fire in fires)
        {
            if (fire.FireAtUtc > cutoff)
            {
                known.Add(fire);
            }
        }

        return new AlertHistoryStanding(known, clears);
    }

    /// <summary>The newest fire no clear closed, whatever grain: the answer of <see cref="IAlertHistoryStore.GetOpenFireAsync"/>.</summary>
    public AlertHistoryFire? NewestOpen() => OpenFor(_ => true);

    /// <summary>
    /// The newest fire row <paramref name="ownsFire"/> says is the grain's own, or null when none is open. It is open when no
    /// clear <paramref name="ownsClear"/> says is the grain's (null: every clear is, a clear that ends the whole alert) is as
    /// new as the fire. Only the grain's newest fire is considered: an older one is the same incident, and a clear after
    /// the newest fire closes it whichever row opened it.
    /// </summary>
    public AlertHistoryFire? OpenFor(Func<AlertHistoryFire, bool> ownsFire, Func<AlertHistoryClear, bool>? ownsClear = null)
    {
        foreach (var fire in Fires)
        {
            if (!ownsFire(fire))
            {
                continue;
            }

            foreach (var clear in Clears)
            {
                if ((ownsClear is null || ownsClear(clear)) && !AlertHistoryEscalationSeed.IsOpen(fire.FireAtUtc, clear.AtUtc))
                {
                    return null;
                }
            }

            return fire;
        }

        return null;
    }
}

/// <summary>
/// One alert event to persist. Carries both the display strings (Dashboard
/// persists these verbatim) and the optional resolved numerics (Lite persists
/// these into DOUBLE columns, falling back to parsing the display text).
///
/// <para>The delivery disposition arrives as one <see cref="AlertDelivery"/> rather than as a loose
/// <c>bool</c> + two strings. A producer cannot then state a send outcome without stating the channel it
/// belongs to, cannot transpose the two strings past the compiler, and — because
/// <see cref="AlertDelivery"/> has no public constructor — cannot hand-write a delivered row at all. The
/// three column-shaped members below are projections for the store writers, which are unchanged.</para>
/// </summary>
public sealed record AlertHistoryRecord(
    string  ServerId, string ServerName, string MetricName,
    string  CurrentValueText, string ThresholdValueText,    // Dashboard persists these
    double? NumericCurrentValue, double? NumericThresholdValue, // Lite persists these
    AlertDelivery Delivery,
    bool    Muted, string? DetailText, string? ContextJson)
{
    /// <summary>The <c>alert_sent</c> column. A delivery measurement on every row.</summary>
    public bool AlertSent => Delivery.Sent;

    /// <summary>The <c>notification_type</c> column — which channel the row is about.</summary>
    public string NotificationType => Delivery.Channel;

    /// <summary>The <c>send_error</c> column.</summary>
    public string? SendError => Delivery.SendError;

    /// <summary>
    /// The <c>alert_time</c> the row is written with (UTC, microsecond precision), or null for the store's own clock
    /// (#5632 Phase A). The Darling deliverer and finding sender stamp it so the investigation request's link row carries
    /// the same instant as the history row it names. Darling's and Lite's stores honor it when set.
    /// </summary>
    public DateTime? AlertTimeUtc { get; init; }

    /// <summary>
    /// The exact key of a store-wide alert whose rows share <c>server_id</c> 0 and a metric name across many keys (a job, a policy), or
    /// null (#7076). Darling persists it as <c>alert_key</c> on the alert's fire and resolution rows so a restart can find one key's own
    /// open row. Every other row leaves it null.
    /// </summary>
    public string? AlertKey { get; init; }

    /// <summary>When a resolution was delivered to at least one channel (UTC), or null (#5825). Written only by Darling,
    /// on a resolution row, as <c>resolution_sent_at</c>; every other row leaves the three resolution members null.</summary>
    public DateTime? ResolutionSentAtUtc { get; init; }

    /// <summary>The channels a resolution went to, or would have gone to when muted: comma-separated
    /// <see cref="NotificationRouter"/> channel names (#5825). Persisted as <c>resolution_channels</c>.</summary>
    public string? ResolutionChannels { get; init; }

    /// <summary>What the resolution's send did: <c>sent</c>, <c>failed: </c> and the failed channels, or <c>muted</c>
    /// (#5825). Persisted as <c>resolution_outcome</c>.</summary>
    public string? ResolutionOutcome { get; init; }
}
