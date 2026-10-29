/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
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
/// <para><b>At least once.</b> A row is claimed (<c>claimed</c>), handled, and then marked. The send happens before the mark,
/// so a run that dies between them leaves a claimed row; the reap, which every pass runs, marks it <c>delivered</c> when the
/// alert history holds the row the deliverer writes after a send (the row carries the pending id in its <c>context_json</c>),
/// else <c>interrupted</c>, once its claim is older than <see cref="ReapClaimsOlderThan"/>. A pass restamps its claims before
/// each row, so the age is never reached by a live pass. A reaped row is not claimed again. A row goes back to <c>pending</c>
/// for a state write that timed out on a lock, a fire every channel failed to send, a PagerDuty resolve that failed, and the
/// rows of a stopping pass that were never attempted. A fire that every channel failed to send waits 1, 2 and 4 minutes
/// and is marked <c>failed</c> on the fourth attempt. A send that timed out after its request left can post twice:
/// PagerDuty drops a repeat trigger with the same dedup key, but Slack, Teams, a webhook and email can show a duplicate.
/// A row goes back to <c>pending</c> with no cancellation token and a short timeout, so a stop right after a failed send
/// keeps the retry. Two cases still end a row <c>interrupted</c>: a release that fails because the store is unreachable
/// (the rows stay claimed until the reap takes them, and the reap cannot tell whether they were attempted), and two
/// services on one store, where a batch whose sends run past <see cref="ReapClaimsOlderThan"/> has its remaining rows
/// reaped by the other service.</para>
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

    /// <summary>How old a claim must be for the reap to take it. A pass restamps its claims before each row, so a live run's
    /// claims stay far younger than this.</summary>
    public static readonly TimeSpan ReapClaimsOlderThan = TimeSpan.FromMinutes(5);

    private const int MaxTitleLength = 200;
    private const int MaxDetailLength = 2000;
    private const int MaxServerLength = 128;

    private readonly ExternalAlertStore _store;
    private readonly Func<bool> _alertsEnabled;
    private readonly Func<AlertOutcome, CancellationToken, Task<AlertDelivery?>> _deliver;
    private readonly Func<string, string, string, string?, IReadOnlyCollection<AlertSeverityLevel?>, CancellationToken, Task<AlertChannelOutcome>> _resolvePagerDuty;
    private readonly IAlertHistoryStore _history;
    private readonly Func<AlertMuteContext, bool> _isMuted;
    private readonly Func<string, ExternalAlertServer?> _resolveServer;
    private readonly string _serviceInstance;
    private readonly ILogger _logger;
    private readonly Func<DateTime> _utcNow;
    private readonly Func<AlertResolution, AlertResolutionSendOptions, CancellationToken, Task>? _deliverResolution;

    /// <param name="alertsEnabled">The live master switch every alert family consults (#3464). Off, a pass only expires rows.</param>
    /// <param name="deliver">The record-and-send seam: <see cref="DarlingAlertDeliverer.DeliverAndReportAsync"/>.</param>
    /// <param name="resolvePagerDuty">(firing metric, server name, server key, detail, every severity the episode's fires went out at, token): closes the PagerDuty incident
    /// of a fire, and only with auto-resolve on: <see cref="DarlingAlertDeliverer.TryResolvePagerDutyAsync"/>.</param>
    /// <param name="isMuted">The mute check every family shares (<c>MuteRuleService.IsAlertMuted</c>).</param>
    /// <param name="resolveServer">Maps the submitted server name to a monitored server, or null when it is not registered;
    /// an unregistered name is kept as a label in the server_id 0 bucket.</param>
    public ExternalAlertDrain(
        ExternalAlertStore store,
        Func<bool> alertsEnabled,
        Func<AlertOutcome, CancellationToken, Task<AlertDelivery?>> deliver,
        Func<string, string, string, string?, IReadOnlyCollection<AlertSeverityLevel?>, CancellationToken, Task<AlertChannelOutcome>> resolvePagerDuty,
        IAlertHistoryStore history,
        Func<AlertMuteContext, bool> isMuted,
        Func<string, ExternalAlertServer?> resolveServer,
        string serviceInstance,
        ILogger logger,
        Func<DateTime>? utcNow = null,
        Func<AlertResolution, AlertResolutionSendOptions, CancellationToken, Task>? deliverResolution = null)
    {
        _deliverResolution = deliverResolution;
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _alertsEnabled = alertsEnabled ?? throw new ArgumentNullException(nameof(alertsEnabled));
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
    /// One pass, run on the drain's own loop every 15 seconds: expire what nobody took in time, reap what a stopped run left
    /// claimed (every pass, so a row stranded in a live run is found too), then claim a batch and handle each row. Expiry and
    /// the reap run while the master switch is off, so rows left while alerts were off still age out and are counted; the
    /// claim and the sends run only while it is on. Before each row's send the pass restamps the claims of that row and of
    /// those after it. A pass that stops with rows unattempted (the service is stopping, or the store failed) gives them back
    /// to <c>pending</c> with no attempt counted.
    /// </summary>
    public async Task<ExternalAlertDrainResult> RunPassAsync(CancellationToken cancellationToken)
    {
        var expired = await _store.ExpireAsync(cancellationToken);

        var (reapedDelivered, reapedInterrupted) = await _store.ReapWithHistoryAsync(
            ReapClaimsOlderThan, AlertContextSerializer.BuildPendingIdLikePattern, cancellationToken);
        if (reapedDelivered + reapedInterrupted > 0)
        {
            _logger.LogWarning(
                "External alerts: {Delivered} claimed row(s) a stopped run left were found in the alert history and marked delivered; {Interrupted} had no history row and were marked interrupted. None is sent again.",
                reapedDelivered, reapedInterrupted);
        }

        if (!_alertsEnabled())
        {
            return new ExternalAlertDrainResult(expired, reapedDelivered, reapedInterrupted, 0, 0, 0, 0, 0, 0, 0, 0);
        }

        var rows = await _store.ClaimAsync(_serviceInstance, ExternalAlertStore.DefaultClaimBatch, cancellationToken);
        int delivered = 0, muted = 0, noChannel = 0, retried = 0, failed = 0, ignored = 0, interrupted = 0;

        /* Rows before this index were attempted (or were taken by the reap). A pass that stops gives back the rest. */
        var firstUnattempted = 0;
        try
        {
            for (var index = 0; index < rows.Count; index++)
            {
                var row = rows[index];
                cancellationToken.ThrowIfCancellationRequested();

                var stillClaimed = await _store.RestampClaimsAsync(
                    _serviceInstance, rows.Skip(index).Select(r => r.PendingId).ToArray(), cancellationToken);
                firstUnattempted = index + 1;
                if (!stillClaimed.Contains(row.PendingId))
                {
                    _logger.LogWarning("External alert {PendingId} was no longer claimed by this run before its send; it is left alone", row.PendingId);
                    continue;
                }

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
                catch (ExternalAlertStateLockTimeoutException ex)
                {
                    /* A fire writes its state before the send, so nothing went out. A clear may have posted the PagerDuty
                       resolve before its state write: the row goes back for a later attempt, and the repeat resolves again
                       and clears the state again, which both steps allow. */
                    _logger.LogWarning(ex, "External alert {PendingId} ({Source}/{Key}) waited on a lock held by another session", row.PendingId, row.SourceName, row.AlertKey);
                    outcome = await RetryOrFailAsync(row, "a state write waited on a lock held by another session");
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
        }
        catch (Exception)
        {
            await ReleaseUnattemptedAsync(rows, firstUnattempted);
            throw;
        }

        return new ExternalAlertDrainResult(
            expired, reapedDelivered, reapedInterrupted, rows.Count, delivered, muted, noChannel, retried, failed, ignored, interrupted);
    }

    /// <summary>The longest the release of a stopping pass's rows may take. It runs with no cancellation token, because
    /// the token is the reason the pass is stopping.</summary>
    public static readonly TimeSpan ReleaseOnStopTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Gives the rows from <paramref name="firstUnattempted"/> on back to the queue. A failure is logged and left
    /// to the reap: it must not replace the exception that stopped the pass.</summary>
    private async Task ReleaseUnattemptedAsync(IReadOnlyList<ExternalAlertPendingRow> rows, int firstUnattempted)
    {
        if (firstUnattempted >= rows.Count)
        {
            return;
        }

        try
        {
            using var timeout = new CancellationTokenSource(ReleaseOnStopTimeout);
            var ids = rows.Skip(firstUnattempted).Select(r => r.PendingId).ToArray();
            var released = await _store.ReleaseUnattemptedAsync(_serviceInstance, ids, timeout.Token);
            _logger.LogInformation("External alerts: {Released} claimed row(s) this pass had not reached went back to pending", released);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "External alerts: the rows this pass had not reached could not be given back; the reap takes them once their claim is old enough");
        }
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
        /* The firing state is written before the send, so a clear that follows pairs with this fire even when the send
           has to be retried or a stopped run never marked it. A repeat fire only refreshes it. The severity the operator was
           told about is recorded only once a send has gone out (ConfirmFireAsync), and a fire that opens the subject and gives
           up undelivered ends the incident (AbandonFireAsync), as a built-in alert's fire that no channel took leaves no
           incident for a resolve to close. */
        var fired = await _store.RecordFireAsync(
            row.SourceId, row.AlertKey, server.StateLabel, server.BucketId, server.Key, server.Label, cancellationToken);
        var wasFiring = fired.WasFiring;
        var previousSeverity = fired.PreviousSeverity;

        /* The mute check names the server the send names: the episode's kept identity, which a repeat fire carries over from
           the first fire of the episode even when the server's name has changed since. */
        var muted = _isMuted(new AlertMuteContext
        {
            ServerName = fired.ServerName,
            /* A registered server's key is its id (Identify); an unregistered name's key carries a prefix and is no number. */
            ServerId = int.TryParse(fired.ServerKey, NumberStyles.None, CultureInfo.InvariantCulture, out var keptId) ? keptId : null,
            MetricName = metric,
            /* A mute rule can name the source (#5654): it silences this source's alerts and no other source's. */
            ExternalSource = row.SourceName,
        });

        var context = new AlertContext { External = new AlertExternalOrigin(row.SourceName, row.AlertKey, row.PendingId) };
        var currentText = row.Value is { } value
            ? value.ToString("0.###", CultureInfo.InvariantCulture)
            : row.Severity;
        var detailText = detail.Length == 0 ? title : $"{title} - {detail}";

        var outcome = new AlertOutcome(
            fired.ServerKey, fired.ServerName, metric, currentText, "",
            Context: context, DetailText: detailText,
            /* A submitted value is the measurement. A submission without one is a state: the explicit 0 the history column
               needs, which the External: family's IsStateOnly arm shows as a dash instead of 0.00 (#1881). */
            NumericCurrentValue: row.Value ?? 0, NumericThresholdValue: null,
            Muted: muted, Severity: severity,
            ShortMessage: title, DisplayName: title);

        var delivery = await _deliver(outcome, cancellationToken);

        if (delivery is null)
        {
            /* The deliverer gave no answer: something outside its channels threw. Whether a post went out is unknown, so
               the row is closed and not sent again. */
            await TryConfirmFireAsync(row, server.StateLabel);
            await TryMarkAsync(row.PendingId, ExternalAlertOutcome.Interrupted, "the deliverer gave no answer; not sent again");
            return ExternalAlertOutcome.Interrupted;
        }

        if (muted)
        {
            await TryConfirmFireAsync(row, server.StateLabel);
            await TryMarkAsync(row.PendingId, ExternalAlertOutcome.Muted, "a mute rule matched");
            return ExternalAlertOutcome.Muted;
        }

        if (FailedSendBackoff.EveryChannelFailed(delivery))
        {
            /* Delivery is at least once: a fire every channel refused goes back for another attempt. The answer cannot show
               that no request left the process (a webhook that timed out after its request was sent reads the same as a
               refused connection), so a repeat can post twice. PagerDuty drops a repeat trigger that carries the same dedup
               key, and the key is derived from the server and the metric name, so every attempt of this row sends the same one. */
            var result = await RetryOrFailAsync(row, "every channel failed; the alert history row has the error");
            if (result == ExternalAlertOutcome.Failed)
            {
                await TryAbandonFireAsync(row, server.StateLabel);
            }

            return result;
        }

        await TryConfirmFireAsync(row, server.StateLabel);
        if (delivery.Sent)
        {
            await TryMarkAsync(row.PendingId, ExternalAlertOutcome.Delivered,
                wasFiring && previousSeverity is not null && !string.Equals(previousSeverity, row.Severity, StringComparison.OrdinalIgnoreCase)
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

    /// <summary>The tier a fire went out at (#6928), read back from the state's stored word, so the close is routed as the fire was:
    /// to the destination of the route that took that severity. Null when none was confirmed, which every route takes.</summary>
    private static AlertSeverityLevel? SeverityOfFired(string? firedSeverity) =>
        string.Equals(firedSeverity, "critical", StringComparison.OrdinalIgnoreCase) ? AlertSeverityLevel.Critical
        : string.Equals(firedSeverity, "warning", StringComparison.OrdinalIgnoreCase) ? AlertSeverityLevel.Warning
        : null;

    /// <summary>Every tier an episode's fires went out at (#6928), so the close reaches the destination of each tier's route: a
    /// Warning-only route's incident stays open when the alert climbs to Critical unless the close names Warning too. An episode
    /// opened before the list was kept has only the newest word. Empty (one close with no tier) when none was confirmed.</summary>
    private static List<AlertSeverityLevel?> SeveritiesOfFired(ExternalAlertFiringState state)
    {
        var tiers = new List<AlertSeverityLevel?>();
        foreach (var word in (state.FiredSeverities ?? state.FiredSeverity ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (SeverityOfFired(word) is { } tier && !tiers.Contains(tier))
            {
                tiers.Add(tier);
            }
        }

        return tiers;
    }

    private async Task<ExternalAlertOutcome?> HandleClearAsync(ExternalAlertPendingRow row, CancellationToken cancellationToken)
    {
        var server = Identify(row);
        var state = await _store.GetStateAsync(row.SourceId, row.AlertKey, server.StateLabel, cancellationToken);
        if (state is not { Firing: true })
        {
            await TryMarkAsync(row.PendingId, ExternalAlertOutcome.Ignored, "a clear for a key that was not firing");
            return ExternalAlertOutcome.Ignored;
        }

        var firingMetric = MetricNameFor(row.SourceName, row.AlertKey);
        var title = Clean(row.Title, MaxTitleLength);
        var detail = Clean(row.Detail, MaxDetailLength);

        /* The server the fire was sent under is the one the state stored: a server registered, renamed or removed since the
           fire must not move the incident this clear closes. */
        var firedKey = state.ServerKey;
        var firedLabel = state.ServerName;
        var message = $"{firedLabel}: {(title.Length == 0 ? row.AlertKey : title)}{(detail.Length == 0 ? "" : " - " + detail)}";

        /* The PagerDuty resolve goes first: it posts only with auto-resolve on and a PagerDuty destination for this alert, and
           if it fails the firing state is left alone so the retry still finds a fire to close. */
        var pagerDuty = await _resolvePagerDuty(
            firingMetric, firedLabel, firedKey, message, SeveritiesOfFired(state), cancellationToken);
        if (pagerDuty == AlertChannelOutcome.Failed)
        {
            return await RetryOrFailAsync(row, "the PagerDuty resolve failed");
        }

        var firedSeverity = await _store.RecordClearAsync(row.SourceId, row.AlertKey, server.StateLabel, cancellationToken);
        if (firedSeverity is null)
        {
            await TryMarkAsync(row.PendingId, ExternalAlertOutcome.Ignored, "the key was cleared by another run");
            return ExternalAlertOutcome.Ignored;
        }

        /* The clear's history row (BuildResolutionRecord, written by the resolution send below, #5825) carries the pending
           id in its context so the reap can tell it was written. */
        var resolvedContext = AlertContextSerializer.Serialize(new AlertContext
        {
            External = new AlertExternalOrigin(row.SourceName, row.AlertKey, row.PendingId),
        });
        var resolution = new AlertResolution(
            firedKey, firedLabel, firingMetric, ResolvedMetricNameFor(row.SourceName, row.AlertKey), message);
        if (_deliverResolution is not null)
        {
            /* #5825: the clear follows the same setting as every other clear, to the destinations its fire reached. The
               PagerDuty resolve above already ran, so this one does not close the incident again. */
            await _deliverResolution(
                resolution, new AlertResolutionSendOptions(row.SourceName, ClosePagerDuty: false, resolvedContext), cancellationToken);
        }
        else
        {
            await _history.RecordAlertAsync(DarlingSelfAlertEvaluator.BuildResolutionRecord(resolution, resolvedContext));
        }

        await TryMarkAsync(row.PendingId, ExternalAlertOutcome.Delivered,
            pagerDuty == AlertChannelOutcome.Delivered ? "resolved; the PagerDuty incident was closed" : "resolved");
        return ExternalAlertOutcome.Delivered;
    }

    /// <summary>A row whose handling sent nothing (or whose send a repeat leaves unchanged): wait 1, 2, 4 minutes and try
    /// again, then give up as failed. Null means the row went back to the queue. The release runs with no cancellation
    /// token and <see cref="ReleaseOnStopTimeout"/>, like the release of a stopping pass: a stop right after the failed
    /// send must not leave the row claimed.</summary>
    private async Task<ExternalAlertOutcome?> RetryOrFailAsync(ExternalAlertPendingRow row, string reason)
    {
        var attempt = row.Attempts + 1;
        if (attempt >= MaxSendAttempts)
        {
            await TryMarkAsync(row.PendingId, ExternalAlertOutcome.Failed, $"{reason} (attempt {attempt} of {MaxSendAttempts})");
            return ExternalAlertOutcome.Failed;
        }

        var next = _utcNow() + AlertEngine.ChannelFailureRetryDelay(attempt, RetryDelayCap);
        /* A release that fails (the store is unreachable) throws to the pass's handler, which closes the row as interrupted. */
        using var timeout = new CancellationTokenSource(ReleaseOnStopTimeout);
        if (!await _store.ReleaseForRetryAsync(row.PendingId, next, Trim($"{reason} (attempt {attempt} of {MaxSendAttempts})"), timeout.Token))
        {
            _logger.LogWarning("External alert {PendingId} was no longer claimed when its retry was recorded", row.PendingId);
        }

        return null;
    }

    /// <summary>Records the severity a fire's send went out at. A failure is logged and left: the send is done, and an
    /// unconfirmed severity only means a later fire that gives up could end an incident this one opened.</summary>
    private async Task TryConfirmFireAsync(ExternalAlertPendingRow row, string stateLabel)
    {
        try
        {
            await _store.ConfirmFireAsync(row.SourceId, row.AlertKey, stateLabel, row.Severity);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "External alert {PendingId} was sent but its severity could not be recorded on the firing state", row.PendingId);
        }
    }

    /// <summary>Ends the incident a fire opened when the fire gave up with nothing sent, so a clear that follows has no
    /// incident to resolve. A repeat fire that gives up leaves an incident an earlier delivered fire opened. A failure is
    /// logged and left.</summary>
    private async Task TryAbandonFireAsync(ExternalAlertPendingRow row, string stateLabel)
    {
        try
        {
            if (await _store.AbandonFireAsync(row.SourceId, row.AlertKey, stateLabel))
            {
                _logger.LogInformation("External alert {PendingId} ({Source}/{Key}) gave up before any send went out; the subject is no longer firing", row.PendingId, row.SourceName, row.AlertKey);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "External alert {PendingId} gave up but the firing state could not be ended", row.PendingId);
        }
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

    /// <summary><c>StateLabel</c> is what the firing state is keyed on: the submitted name, cleaned and lower-cased (empty when
    /// none was given), so a fire and its clear meet whatever the server registry says in between.</summary>
    private readonly record struct Identity(string Key, string Label, int BucketId, string StateLabel);

    /// <summary>A registered server is its id and display name. An unregistered name is a label only, in the server_id 0
    /// bucket: its key is not a number (and carries a prefix, so a server named "5" can never collide with server 5).</summary>
    private Identity Identify(ExternalAlertPendingRow row)
    {
        var name = Clean(row.ServerName, MaxServerLength);
        var stateLabel = name.ToLowerInvariant();
        if (name.Length > 0 && _resolveServer(name) is { } server)
        {
            return new Identity(server.ServerId.ToString(CultureInfo.InvariantCulture), server.DisplayName, server.ServerId, stateLabel);
        }

        var label = name.Length > 0 ? name : "External (" + row.SourceName + ")";
        return new Identity("external:" + label, label, 0, stateLabel);
    }

    private static string Clean(string? value, int maxLength) => CleanText(value, maxLength);

    /// <summary>
    /// Sanitizes <paramref name="value"/> and caps it at <paramref name="maxLength"/> code points, the unit the submit
    /// function counts in (<c>char_length</c>). The sanitizer's own cap counts UTF-16 units, so it only ever sees a length that
    /// cannot bite; the cap that does is cut on a code point boundary, and a surrogate pair is never split.
    /// </summary>
    internal static string CleanText(string? value, int maxLength)
    {
        var cleaned = CustomAlertEvaluator.SanitizeDisplayText(value, value?.Length ?? 0);
        if (maxLength <= 0)
        {
            return string.Empty;
        }

        var codePoints = 0;
        var index = 0;
        foreach (var rune in cleaned.EnumerateRunes())
        {
            if (codePoints == maxLength)
            {
                return cleaned[..index].TrimEnd();
            }

            codePoints++;
            index += rune.Utf16SequenceLength;
        }

        return cleaned;
    }

    private static string Trim(string value) => value.Length <= 500 ? value : value[..500];
}
