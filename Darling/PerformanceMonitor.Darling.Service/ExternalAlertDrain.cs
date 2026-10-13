/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>A monitored server an external alert's server name resolved to.</summary>
public sealed record ExternalAlertServer(int ServerId, string DisplayName);

/// <summary>What one pass of <see cref="ExternalAlertDrain.RunPassAsync"/> did, for the log line and the tests.</summary>
public sealed record ExternalAlertDrainResult(
    int Expired, int ReapedDelivered, int ReapedInterrupted, int Claimed,
    int Delivered, int Muted, int NoChannel, int Retried, int Failed, int Ignored, int Interrupted);

/// <summary>
/// Delivers the alerts other services queued through <c>config.submit_external_alert</c> (#5654) through the pipeline
/// Darling's own alerts use: the same mute check, the same record-and-send deliverer (its channels, routes, per-fingerprint
/// cooldown, statement filter and history row), and a resolution row the way Darling's own clears write one.
///
/// <para><b>Once only.</b> A row is claimed (<c>claimed</c>), handled exactly once, and then marked. The send happens
/// before the mark, so a run that dies between them leaves a claimed row; the reap at the next start marks it
/// <c>delivered</c> when the alert history holds the row the deliverer writes after a send (the row carries the pending id in
/// its <c>context_json</c>), else <c>interrupted</c>. A claimed row is never sent a second time. The one return to
/// <c>pending</c> is a send that every channel refused, which the deliverer reports and has not delivered anywhere.</para>
///
/// <para><b>Order.</b> The store's claim hands out at most one row per source and key per call, and none while an earlier
/// row of the key is still pending or claimed, so a fire and its clear are handled in the order they were submitted. A fire
/// waiting for a retry holds its clear back.</para>
///
/// <para><b>What the submitting service controls</b> is the source name (taken from its login), the key, severity, a title,
/// a detail, a server name and a number. The context and the history row are built here. Titles, details and server names
/// are cut to one line and capped, so they cannot forge the label lines the mute-from-history pre-fill reads.</para>
/// </summary>
public sealed class ExternalAlertDrain
{
    /// <summary>A fire is sent at most this many times; the last failed attempt marks the row <c>failed</c>. With waits of
    /// 1, 2 and 4 minutes between the four, the last attempt lands inside the queue's 15-minute deadline.</summary>
    public const int MaxSendAttempts = 4;

    /// <summary>The longest wait between two attempts of one row.</summary>
    public static readonly TimeSpan RetryDelayCap = TimeSpan.FromMinutes(5);

    /// <summary>How old a claim must be for the start-of-run reap to take it. Far longer than any live pass holds one.</summary>
    public static readonly TimeSpan ReapClaimsOlderThan = TimeSpan.FromMinutes(5);

    private const int MaxTitleLength = 200;
    private const int MaxDetailLength = 2000;
    private const int MaxServerLength = 128;

    private readonly ExternalAlertStore _store;
    private readonly Func<AlertOutcome, CancellationToken, Task<AlertDelivery?>> _deliver;
    private readonly Func<string, string, string, string?, CancellationToken, Task<AlertChannelOutcome>> _resolvePagerDuty;
    private readonly IAlertHistoryStore _history;
    private readonly Func<AlertMuteContext, bool> _isMuted;
    private readonly Func<string, ExternalAlertServer?> _resolveServer;
    private readonly string _serviceInstance;
    private readonly ILogger _logger;
    private readonly Func<DateTime> _utcNow;

    /// <param name="deliver">The record-and-send seam: <see cref="DarlingAlertDeliverer.DeliverAndReportAsync"/>.</param>
    /// <param name="resolvePagerDuty">(firing metric, server name, server key, detail, token): closes the PagerDuty incident
    /// of a fire, and only with auto-resolve on: <see cref="DarlingAlertDeliverer.TryResolvePagerDutyAsync"/>.</param>
    /// <param name="isMuted">The mute check every family shares (<c>MuteRuleService.IsAlertMuted</c>).</param>
    /// <param name="resolveServer">Maps the submitted server name to a monitored server, or null when it is not registered;
    /// an unregistered name is kept as a label in the server_id 0 bucket.</param>
    public ExternalAlertDrain(
        ExternalAlertStore store,
        Func<AlertOutcome, CancellationToken, Task<AlertDelivery?>> deliver,
        Func<string, string, string, string?, CancellationToken, Task<AlertChannelOutcome>> resolvePagerDuty,
        IAlertHistoryStore history,
        Func<AlertMuteContext, bool> isMuted,
        Func<string, ExternalAlertServer?> resolveServer,
        string serviceInstance,
        ILogger logger,
        Func<DateTime>? utcNow = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _deliver = deliver ?? throw new ArgumentNullException(nameof(deliver));
        _resolvePagerDuty = resolvePagerDuty ?? throw new ArgumentNullException(nameof(resolvePagerDuty));
        _history = history ?? throw new ArgumentNullException(nameof(history));
        _isMuted = isMuted ?? throw new ArgumentNullException(nameof(isMuted));
        _resolveServer = resolveServer ?? throw new ArgumentNullException(nameof(resolveServer));
        _serviceInstance = serviceInstance ?? throw new ArgumentNullException(nameof(serviceInstance));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
    }

    /// <summary>The metric name an external alert is delivered, muted, routed and recorded under:
    /// <c>External:&lt;source&gt;:&lt;key&gt;</c>, in the <see cref="AlertFamily.External"/> family.</summary>
    public static string MetricNameFor(string source, string key) => AlertFamily.ExternalMetricPrefix + source + ":" + key;

    /// <summary>The metric name of the resolution row a clear writes: the fire's name and " Resolved".</summary>
    public static string ResolvedMetricNameFor(string source, string key) => MetricNameFor(source, key) + " Resolved";

    /// <summary>
    /// One 15-second pass: expire what nobody took in time, on the first pass of a run take back what a dead run left
    /// claimed, then claim a batch and handle each row. Expiry always runs, so rows left while alerts were off or the
    /// service was paused still age out and are counted; the claim and the sends run only when
    /// <paramref name="deliver"/> is true.
    /// </summary>
    public async Task<ExternalAlertDrainResult> RunPassAsync(bool reapFirst, bool deliver, CancellationToken cancellationToken)
    {
        var expired = await _store.ExpireAsync(cancellationToken);

        var reapedDelivered = 0;
        var reapedInterrupted = 0;
        if (reapFirst)
        {
            (reapedDelivered, reapedInterrupted) = await _store.ReapWithHistoryAsync(ReapClaimsOlderThan, cancellationToken);
            if (reapedDelivered + reapedInterrupted > 0)
            {
                _logger.LogWarning(
                    "External alerts: {Delivered} claimed row(s) a stopped run left were found in the alert history and marked delivered; {Interrupted} had no history row and were marked interrupted. None is sent again.",
                    reapedDelivered, reapedInterrupted);
            }
        }

        if (!deliver)
        {
            return new ExternalAlertDrainResult(expired, reapedDelivered, reapedInterrupted, 0, 0, 0, 0, 0, 0, 0, 0);
        }

        var rows = await _store.ClaimAsync(_serviceInstance, ExternalAlertStore.DefaultClaimBatch, cancellationToken);
        int delivered = 0, muted = 0, noChannel = 0, retried = 0, failed = 0, ignored = 0, interrupted = 0;

        foreach (var row in rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ExternalAlertOutcome? outcome;
            try
            {
                outcome = string.Equals(row.Kind, "clear", StringComparison.Ordinal)
                    ? await HandleClearAsync(row, cancellationToken)
                    : await HandleFireAsync(row, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                /* Something threw outside the deliverer's own isolation (the state or the mark). Whether anything left the
                   process is unknown, so the row is closed as interrupted and never sent again. */
                _logger.LogError(ex, "External alert {PendingId} ({Source}/{Key}) could not be handled", row.PendingId, row.SourceName, row.AlertKey);
                await TryMarkAsync(row.PendingId, ExternalAlertOutcome.Interrupted, "the drain failed while handling it: " + ex.GetType().Name);
                outcome = ExternalAlertOutcome.Interrupted;
            }

            switch (outcome)
            {
                case null: retried++; break;
                case ExternalAlertOutcome.Delivered: delivered++; break;
                case ExternalAlertOutcome.Muted: muted++; break;
                case ExternalAlertOutcome.NoChannel: noChannel++; break;
                case ExternalAlertOutcome.Failed: failed++; break;
                case ExternalAlertOutcome.Ignored: ignored++; break;
                case ExternalAlertOutcome.Interrupted: interrupted++; break;
                default: break;
            }
        }

        return new ExternalAlertDrainResult(
            expired, reapedDelivered, reapedInterrupted, rows.Count, delivered, muted, noChannel, retried, failed, ignored, interrupted);
    }

    private async Task<ExternalAlertOutcome?> HandleFireAsync(ExternalAlertPendingRow row, CancellationToken cancellationToken)
    {
        var server = Identify(row);
        var metric = MetricNameFor(row.SourceName, row.AlertKey);
        var severity = string.Equals(row.Severity, "critical", StringComparison.OrdinalIgnoreCase)
            ? AlertSeverityLevel.Critical
            : AlertSeverityLevel.Warning;
        var title = Clean(row.Title, MaxTitleLength);
        if (title.Length == 0)
        {
            title = row.AlertKey;
        }

        var detail = Clean(row.Detail, MaxDetailLength);
        var muted = _isMuted(new AlertMuteContext { ServerName = server.Label, ServerId = server.MuteId, MetricName = metric });

        /* The firing state is written before the send, so a clear that follows pairs with this fire even when the send
           has to be retried or a stopped run never marked it. A repeat fire only refreshes it. */
        var (wasFiring, previousSeverity) = await _store.RecordFireAsync(
            row.SourceId, row.AlertKey, server.BucketId, row.Severity, cancellationToken);

        var context = new AlertContext { External = new AlertExternalOrigin(row.SourceName, row.AlertKey, row.PendingId) };
        var currentText = row.Value is { } value
            ? value.ToString("0.###", CultureInfo.InvariantCulture)
            : row.Severity;
        var detailText = detail.Length == 0 ? title : $"{title} - {detail}";

        var outcome = new AlertOutcome(
            server.Key, server.Label, metric, currentText, "",
            Context: context, DetailText: detailText,
            NumericCurrentValue: row.Value ?? 0, NumericThresholdValue: null,
            Muted: muted, Severity: severity,
            ShortMessage: title, DisplayName: title);

        var delivery = await _deliver(outcome, cancellationToken);

        if (delivery is null)
        {
            /* The deliverer gave no answer: something outside its channels threw. Whether a post went out is unknown, so
               the row is closed and not sent again. */
            await TryMarkAsync(row.PendingId, ExternalAlertOutcome.Interrupted, "the deliverer gave no answer; not sent again");
            return ExternalAlertOutcome.Interrupted;
        }

        if (muted)
        {
            await TryMarkAsync(row.PendingId, ExternalAlertOutcome.Muted, "a mute rule matched");
            return ExternalAlertOutcome.Muted;
        }

        if (FailedSendBackoff.EveryChannelFailed(delivery))
        {
            return await RetryOrFailAsync(row, "every channel failed; the alert history row has the error", cancellationToken);
        }

        if (delivery.Sent)
        {
            await TryMarkAsync(row.PendingId, ExternalAlertOutcome.Delivered,
                wasFiring && !string.Equals(previousSeverity, row.Severity, StringComparison.OrdinalIgnoreCase)
                    ? $"severity changed from {previousSeverity} to {row.Severity}"
                    : null);
            return ExternalAlertOutcome.Delivered;
        }

        if (string.Equals(delivery.Channel, AlertDelivery.ChannelNoneConfigured, StringComparison.Ordinal))
        {
            await TryMarkAsync(row.PendingId, ExternalAlertOutcome.NoChannel, "no channel or route wanted it");
            return ExternalAlertOutcome.NoChannel;
        }

        /* Throttled by the delivery cooldown, folded onto a roster or sent to a digest: the deliverer took it and wrote its
           history row, and no channel is owed a second post. */
        await TryMarkAsync(row.PendingId, ExternalAlertOutcome.Delivered, "handled by the delivery policy: " + delivery.Channel);
        return ExternalAlertOutcome.Delivered;
    }

    private async Task<ExternalAlertOutcome?> HandleClearAsync(ExternalAlertPendingRow row, CancellationToken cancellationToken)
    {
        var server = Identify(row);
        var state = await _store.GetStateAsync(row.SourceId, row.AlertKey, server.BucketId, cancellationToken);
        if (state is not { Firing: true })
        {
            await TryMarkAsync(row.PendingId, ExternalAlertOutcome.Ignored, "a clear for a key that was not firing");
            return ExternalAlertOutcome.Ignored;
        }

        var firingMetric = MetricNameFor(row.SourceName, row.AlertKey);
        var title = Clean(row.Title, MaxTitleLength);
        var detail = Clean(row.Detail, MaxDetailLength);
        var message = $"{server.Label}: {(title.Length == 0 ? row.AlertKey : title)}{(detail.Length == 0 ? "" : " - " + detail)}";

        /* The PagerDuty resolve goes first: it posts only with auto-resolve on and a PagerDuty destination for this alert, and
           if it fails the firing state is left alone so the retry still finds a fire to close. */
        var pagerDuty = await _resolvePagerDuty(firingMetric, server.Label, server.Key, message, cancellationToken);
        if (pagerDuty == AlertChannelOutcome.Failed)
        {
            return await RetryOrFailAsync(row, "the PagerDuty resolve failed", cancellationToken);
        }

        var firedSeverity = await _store.RecordClearAsync(row.SourceId, row.AlertKey, server.BucketId, cancellationToken);
        if (firedSeverity is null)
        {
            await TryMarkAsync(row.PendingId, ExternalAlertOutcome.Ignored, "the key was cleared by another run");
            return ExternalAlertOutcome.Ignored;
        }

        /* Darling's own clears write a history row and post nothing (BuildResolutionRecord); this one carries the pending
           id in its context so the reap can tell it was written. */
        var resolvedContext = AlertContextSerializer.Serialize(new AlertContext
        {
            External = new AlertExternalOrigin(row.SourceName, row.AlertKey, row.PendingId),
        });
        await _history.RecordAlertAsync(DarlingSelfAlertEvaluator.BuildResolutionRecord(new AlertResolution(
            server.Key, server.Label, firingMetric, ResolvedMetricNameFor(row.SourceName, row.AlertKey), message))
            with { ContextJson = resolvedContext });

        await TryMarkAsync(row.PendingId, ExternalAlertOutcome.Delivered,
            pagerDuty == AlertChannelOutcome.Delivered ? "resolved; the PagerDuty incident was closed" : "resolved");
        return ExternalAlertOutcome.Delivered;
    }

    /// <summary>A send every channel refused: wait 1, 2, 4 minutes and try again, then give up. Null means the row went back to the queue.</summary>
    private async Task<ExternalAlertOutcome?> RetryOrFailAsync(ExternalAlertPendingRow row, string reason, CancellationToken cancellationToken)
    {
        var attempt = row.Attempts + 1;
        if (attempt >= MaxSendAttempts)
        {
            await TryMarkAsync(row.PendingId, ExternalAlertOutcome.Failed, $"{reason} (attempt {attempt} of {MaxSendAttempts})");
            return ExternalAlertOutcome.Failed;
        }

        var next = _utcNow() + AlertEngine.ChannelFailureRetryDelay(attempt, RetryDelayCap);
        if (!await _store.ReleaseForRetryAsync(row.PendingId, next, Trim($"{reason} (attempt {attempt} of {MaxSendAttempts})"), cancellationToken))
        {
            _logger.LogWarning("External alert {PendingId} was no longer claimed when its retry was recorded", row.PendingId);
        }

        return null;
    }

    /// <summary>A failed mark leaves a claimed row, which the next start's reap closes; it must never undo a handled alert.</summary>
    private async Task TryMarkAsync(long pendingId, ExternalAlertOutcome outcome, string? reason)
    {
        try
        {
            if (!await _store.MarkAsync(pendingId, outcome, reason))
            {
                _logger.LogWarning("External alert {PendingId} was no longer claimed when it was marked {Outcome}", pendingId, outcome);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "External alert {PendingId} could not be marked {Outcome}", pendingId, outcome);
        }
    }

    private readonly record struct Identity(string Key, string Label, int? MuteId, int BucketId);

    /// <summary>A registered server is its id and display name. An unregistered name is a label only, in the server_id 0
    /// bucket: its key is not a number (and carries a prefix, so a server named "5" can never collide with server 5).</summary>
    private Identity Identify(ExternalAlertPendingRow row)
    {
        var name = Clean(row.ServerName, MaxServerLength);
        if (name.Length > 0 && _resolveServer(name) is { } server)
        {
            return new Identity(server.ServerId.ToString(CultureInfo.InvariantCulture), server.DisplayName, server.ServerId, server.ServerId);
        }

        var label = name.Length > 0 ? name : "External (" + row.SourceName + ")";
        return new Identity("external:" + label, label, null, 0);
    }

    private static string Clean(string? value, int maxLength) => CustomAlertEvaluator.SanitizeDisplayText(value, maxLength);

    private static string Trim(string value) => value.Length <= 500 ? value : value[..500];
}
