/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Darling's settings adapter for the Phase-5 shared alert engine: implements BOTH the engine
/// threshold surface (<see cref="IAlertEngineSettings"/>) and the delivery surface
/// (<see cref="IAlertSettings"/>, consumed by the shared <c>EmailSendCore</c>/
/// <c>WebhookAlertService</c>) over one <see cref="DarlingConfig"/> — the Darling twin of Lite's
/// <c>AppAlertSettings</c>. Pass-through reads (the config object is held by reference, so a
/// future config-reload feature is reflected immediately); the few clamps mirror the ones Lite
/// applies at settings-load time (cooldowns 1–120, failed-job lookback 1–1440, low-disk % 0–100,
/// low-disk GB ≥ 0). SMTP is "enabled" when host + from + to are all configured and a webhook
/// channel when its URL is set — no speculative enable flags.
/// </summary>
public sealed class DarlingAlertSettings : IAlertEngineSettings, IAlertSettings
{
    private readonly DarlingConfig _config;

    public DarlingAlertSettings(DarlingConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
    }

    /* ---------------- IAlertEngineSettings (thresholds) ---------------- */

    public bool AlertsEnabled => _config.Alerts.Enabled;

    public bool CpuEnabled => _config.Alerts.CpuEnabled;
    public bool BlockingEnabled => _config.Alerts.BlockingEnabled;
    public bool DeadlockEnabled => _config.Alerts.DeadlockEnabled;
    public bool PoisonWaitEnabled => _config.Alerts.PoisonWaitEnabled;
    public bool LongRunningQueryEnabled => _config.Alerts.LongRunningQueryEnabled;
    public bool TempDbSpaceEnabled => _config.Alerts.TempDbSpaceEnabled;
    public bool LowDiskEnabled => _config.Alerts.LowDiskEnabled;
    public bool LongRunningJobEnabled => _config.Alerts.LongRunningJobEnabled;
    public bool FailedJobEnabled => _config.Alerts.FailedJobEnabled;
    public bool PvsEnabled => _config.Alerts.PvsEnabled;
    public bool DatabaseStateEnabled => _config.Alerts.DatabaseStateEnabled;

    /* #2157: ON with no store column yet — see AppAlertEngineSettings for the reasoning, including why a
       darling.json-only flag would not survive a store reload (ApplyToConfig swaps Alerts wholesale). */
    public bool ForcePlanFailureEnabled => true;

    public int CpuThresholdPercent => _config.Alerts.CpuThresholdPercent;
    public int BlockingCountThreshold => _config.Alerts.BlockingCountThreshold;

    /* #1839: floored at 0 (= off) so a negative in darling.json or the store can't make the
       "is it above threshold" test true for every snapshot. */
    public int BlockingWaitSecondsThreshold => Math.Max(0, _config.Alerts.BlockingWaitSecondsThreshold);
    public int DeadlockCountThreshold => _config.Alerts.DeadlockCountThreshold;
    public int PoisonWaitThresholdMs => _config.Alerts.PoisonWaitThresholdMs;
    public int LongRunningQueryThresholdMinutes => _config.Alerts.LongRunningQueryThresholdMinutes;
    public int TempDbSpaceThresholdPercent => _config.Alerts.TempDbSpaceThresholdPercent;
    public int LowDiskThresholdPercent => Math.Clamp(_config.Alerts.LowDiskThresholdPercent, 0, 100);
    public int LowDiskThresholdGb => Math.Max(0, _config.Alerts.LowDiskThresholdGb);

    /* #2107: the previously-hardcoded thresholds, clamped on read like their siblings so a
       hand-edited store value can't drive a nonsense threshold. The critical floors keep low-disk's
       0-100 percent clamp and 0-floor GB shape; the staleness window and failure fast-path get
       floors that keep the self-alerts meaningful (a 0-minute window would fire on every sweep). */
    public int DiskCriticalFreePercent => Math.Clamp(_config.Alerts.DiskCriticalFreePercent, 0, 100);
    public int DiskCriticalFreeGb => Math.Max(0, _config.Alerts.DiskCriticalFreeGb);
    public int SelfDiskFreeWarnPercent => Math.Clamp(_config.Alerts.SelfDiskFreeWarnPercent, 0, 100);
    public int CollectionStaleMinutes => Math.Clamp(_config.Alerts.CollectionStaleMinutes, 5, 1440);

    /// <summary>#2136: the Store Job Over Cadence warning percent. Clamped [5, 100].
    ///
    /// <para><b>The ceiling is structural.</b> At 100 the Warning tier merges into the fixed Critical tier,
    /// so higher values would only disable the warning silently.</para>
    ///
    /// <para><b>The floor is 5 because the distribution is bimodal, NOT because the worst job runs ~7% of
    /// cadence</b> — #2136 said the latter and it is measurably wrong (#3060). Measured over
    /// <c>collect.store_metrics</c>' <c>object_kind = 'background_job'</c> series on both production
    /// 42-server stores: the store carrying the Query Store workload has p50 0.003%, p90 0.056%, p99 6.1%,
    /// p99.9 69.6% and a MAXIMUM of 369.5% of cadence over 48,030 readings (2026-08-17 to 2026-09-06); its
    /// sibling has p50 0.003%, p90 0.016%, p99 0.29% and a maximum of 62.6% over 64,646 readings
    /// (2026-08-09 to 2026-09-06). So the worst is 53x the figure #2136 cited, and the tail is owned almost
    /// entirely by the continuous-aggregate refresh family.</para>
    ///
    /// <para><b>Which changes the reasoning and not the number.</b> A floor exists to stop a setting that
    /// fires on the healthy body of the distribution, and 5 is still ~90x the 90th percentile and three
    /// orders of magnitude above the median — 1.1% of readings on the busier store reach it at all. Moving
    /// it buys nothing in either direction: only 73 of those 48,030 readings sit in [5, 7), so raising the
    /// floor to the old rationale's own number would change almost no outcome, and lowering it would admit
    /// settings that fire on compression policies doing ordinary work. Left at 5 deliberately — an
    /// operator-facing knob's bound is not something to move as a side effect of correcting the sentence
    /// that justified it.</para></summary>
    public int StoreJobCadenceWarnPercent => Math.Clamp(_config.Alerts.StoreJobCadenceWarnPercent, 5, 100);

    /// <summary>#3297 (V119): the Retention Held WARNING tier, clamped like its siblings so a hand-edited
    /// store value cannot drive a nonsense threshold. Both bounds are named constants shared with the MCP
    /// write bound, because a wider bound there would let <c>update_alert_settings</c> ACCEPT a value this
    /// clamp then rewrites — the "setting did not stick" failure the write-bound parity pins exist for. The
    /// floor's reasoning (it is the shipped default, so the knob raises and cannot lower) and the ceiling's
    /// live on the constants.</summary>
    public double RetentionHoldWarnRatio => Math.Clamp(
        _config.Alerts.RetentionHoldWarnRatio,
        TimescaleSupport.RetentionHoldRatioFloor,
        TimescaleSupport.RetentionHoldRatioCeiling);

    /// <summary>#3297 (V119): the Retention Held CRITICAL tier, clamped to the same bounds as its warning
    /// sibling and INDEPENDENTLY of it. Not floored at the warning ratio: a critical tier below the warning
    /// tier means every fire is Critical and the Warning tier is empty, which is a coherent reading of what
    /// an operator who set it there asked for — where a <c>Math.Max</c> here would silently use a number
    /// other than the one <c>get_alert_settings</c> reports back.</summary>
    public double RetentionHoldCriticalRatio => Math.Clamp(
        _config.Alerts.RetentionHoldCriticalRatio,
        TimescaleSupport.RetentionHoldRatioFloor,
        TimescaleSupport.RetentionHoldRatioCeiling);
    public int CollectionFailureThreshold => Math.Clamp(_config.Alerts.CollectionFailureThreshold, 1, 1000);

    /* #1984: percent clamped like low-disk's (0 = off); the GB floor merely floored at 0 — unlike
       the percent it has no meaningful upper bound. */
    public int PvsThresholdPercent => Math.Clamp(_config.Alerts.PvsThresholdPercent, 0, 100);
    public int PvsFloorGb => Math.Max(0, _config.Alerts.PvsFloorGb);

    /* #2349: the file-growth gates. Clamped the same way the neighbours are -- a negative threshold would
       make the comparison always true, which for a gate whose whole job is to be quiet until something moves
       is the worst possible default. A ZERO is meaningful here rather than nonsense: it disables that one
       gate, so an operator can run rise-only or level-only without a second switch. */
    public bool FileGrowthEnabled => _config.Alerts.FileGrowthEnabled;
    public int FileGrowthRiseMb => Math.Max(0, _config.Alerts.FileGrowthRiseMb);
    public int FileGrowthVolumePercent => Math.Clamp(_config.Alerts.FileGrowthVolumePercent, 0, 100);
    public int FileGrowthLookbackMinutes => Math.Clamp(_config.Alerts.FileGrowthLookbackMinutes, 5, 1440);
    public int LongRunningJobMultiplier => _config.Alerts.LongRunningJobMultiplier;
    public int FailedJobLookbackMinutes => Math.Clamp(_config.Alerts.FailedJobLookbackMinutes, 1, 1440);
    public int CooldownMinutes => Math.Clamp(_config.Alerts.CooldownMinutes, 1, 120);

    public IReadOnlyList<string> ExcludedDatabases => _config.Alerts.ExcludedDatabases;

    /// <summary>
    /// The GLOBAL deadlock/blocking delivery mode (#1141), read live through the by-reference config seam so a
    /// store reload reflects immediately. The deliverer resolves a per-server override against this via the
    /// shared <c>AlertDeliveryModeResolver</c>. Not on the shared IAlertSettings surface — it is Darling's
    /// delivery concern, consumed by <see cref="DarlingAlertDeliverer"/> off the concrete type.
    /// </summary>
    public AlertNotificationMode DeliveryMode => _config.Alerts.DeliveryMode;

    /// <summary>Per-event mode's per-cycle incident cap before the "+N more" batch (#1141); clamped 1–100 like
    /// Lite/the viewer so a hand-edited store value can't drive an unbounded fan-out.</summary>
    public int PerEventMax => Math.Clamp(_config.Alerts.PerEventMax, 1, 100);

    /// <summary>
    /// Whether the Server-Unreachable / Server-Restored connect-edge alerts are delivered (V20), read live
    /// through the by-reference config seam. Not on the shared <see cref="IAlertEngineSettings"/> surface — the
    /// connect edge is not a sweep condition; it is Darling's own service-health concern, consumed by
    /// <see cref="DarlingSelfAlertEvaluator"/> off this concrete type (the DeliveryMode precedent). Default true.
    /// </summary>
    public bool NotifyConnectionChanges => _config.Alerts.NotifyConnectionChanges;

    /// <summary>#1659 opt-in (V33), read live like <see cref="NotifyConnectionChanges"/>: announce a server
    /// already down on its first-ever connect attempt. Default false.</summary>
    public bool NotifyConnectionDownAtStartup => _config.Alerts.NotifyConnectionDownAtStartup;

    /// <summary>#1659 opt-in (V33), read live: re-announce a standing outage every N minutes (0 = off).
    /// Clamped 0–1440 like the other store-fed numerics, so a hand-edited row can't drive a per-sweep spam
    /// loop or a never-fires interval.</summary>
    public int ConnectionRefireMinutes => Math.Clamp(_config.Alerts.ConnectionRefireMinutes, 0, 1440);

    /// <summary>#991 master switch (V35), read live like <see cref="NotifyConnectionChanges"/>: whether the
    /// Availability Group alert family evaluates and delivers at all. Default true.</summary>
    public bool NotifyAgHealth => _config.Alerts.NotifyAgHealth;

    /// <summary>#991 (V35), read live: the "AG Sync Fell Behind" lag trigger in seconds (0 = off). Clamped
    /// 0–86400 (one day) like the other store-fed numerics, so a hand-edited row can neither fire on every
    /// sweep for a negative value nor set a window so wide the alert can never fire.</summary>
    public int AgLagAlertSeconds => Math.Clamp(_config.Alerts.AgLagAlertSeconds, 0, 86400);

    /// <summary>#991 (V35), read live: the "AG Sync Fell Behind" redo-queue trigger in KB (0 = off). Clamped
    /// 0–1073741824 (1 TB expressed in KB) — above that the threshold is larger than any real redo queue, so
    /// it is indistinguishable from off, and a negative would fire on every healthy row.</summary>
    public long AgRedoQueueAlertKb => Math.Clamp(_config.Alerts.AgRedoQueueAlertKb, 0L, 1073741824L);

    /// <summary>#1696 (V37), read live: re-announce a still-disconnected AG replica every N minutes
    /// (0 = off). Clamped 0–1440 like the sibling connection re-fire, so a hand-edited row can drive
    /// neither a per-sweep spam loop nor a never-fires interval.</summary>
    public int AgDisconnectRefireMinutes => Math.Clamp(_config.Alerts.AgDisconnectRefireMinutes, 0, 1440);

    /// <summary>"sql" → SqlProcess; anything else (incl. Lite's default "total") → TotalServer.</summary>
    public CpuAlertMode CpuAlertMode =>
        string.Equals(_config.Alerts.CpuMode, "sql", StringComparison.OrdinalIgnoreCase)
            ? CpuAlertMode.SqlProcess
            : CpuAlertMode.TotalServer;

    /* The long-running-query read shape — control-plane knobs since V20, read live through the by-reference
       config seam so a store reload reflects immediately (the read adapter re-clamps max results 1–1000). The
       shipped defaults still match Lite's App.* (5 rows, every filter on), so an un-customized store is
       unchanged from the previously-hardcoded behavior. */
    public int LongRunningQueryMaxResults => _config.Alerts.LongRunningQueryMaxResults;
    public bool LongRunningQueryExcludeSpServerDiagnostics => _config.Alerts.LongRunningQueryExcludeSpServerDiagnostics;
    public bool LongRunningQueryExcludeWaitFor => _config.Alerts.LongRunningQueryExcludeWaitFor;
    public bool LongRunningQueryExcludeBackups => _config.Alerts.LongRunningQueryExcludeBackups;
    public bool LongRunningQueryExcludeMiscWaits => _config.Alerts.LongRunningQueryExcludeMiscWaits;
    public bool LongRunningQueryExcludeCdc => _config.Alerts.LongRunningQueryExcludeCdc;

    /* ---------------- IAlertSettings (delivery) ---------------- */

    public bool SmtpEnabled =>
        !string.IsNullOrWhiteSpace(_config.Smtp.Host)
        && !string.IsNullOrWhiteSpace(_config.Smtp.From)
        && !string.IsNullOrWhiteSpace(_config.Smtp.To);

    public string SmtpServer => _config.Smtp.Host;
    public int SmtpPort => _config.Smtp.Port;
    public bool SmtpUseSsl => _config.Smtp.UseSsl;
    public string SmtpUsername => _config.Smtp.Username ?? "";
    public string SmtpFromAddress => _config.Smtp.From;
    public string SmtpRecipients => _config.Smtp.To;

    /// <summary>
    /// The SMTP password: smtp.encryptedPassword (DPAPI, Windows, preferred) else smtp.password — a
    /// literal or an <c>env:</c>/<c>file:</c> reference (#1804), the only non-Windows email path; null
    /// when neither is set. Called inside EmailSendCore's send try/catch, so a decrypt/dereference
    /// failure surfaces as that alert's send_error rather than killing the sweep.
    /// </summary>
    public string? GetSmtpPassword()
    {
        var blob = _config.Smtp.EncryptedPassword;
        if (!string.IsNullOrWhiteSpace(blob))
        {
            if (!OperatingSystem.IsWindows())
            {
                throw new PlatformNotSupportedException(
                    "smtp.encryptedPassword requires Windows (DPAPI); use smtp.password with an env:/file: reference on other platforms.");
            }

            return DarlingSecrets.Unprotect(blob);
        }

        var password = _config.Smtp.Password;
        if (string.IsNullOrWhiteSpace(password))
        {
            return null;
        }

        return DarlingSecretSource.Resolve(password, "smtp.password");
    }

    public int EmailCooldownMinutes => Math.Clamp(_config.Smtp.EmailCooldownMinutes, 1, 120);

    public bool TeamsWebhookEnabled => !string.IsNullOrWhiteSpace(_config.Webhooks.TeamsUrl);
    public string TeamsWebhookUrl => _config.Webhooks.TeamsUrl;
    public string TeamsProxyAddress => _config.Webhooks.TeamsProxy;

    public bool SlackWebhookEnabled => !string.IsNullOrWhiteSpace(_config.Webhooks.SlackUrl);
    public string SlackWebhookUrl => _config.Webhooks.SlackUrl;
    public string SlackProxyAddress => _config.Webhooks.SlackProxy;

    /* Generic webhook (#1506) — enabled by a non-empty URL, the same no-speculative-enable-flag derivation
       the sibling channels use. */
    public bool GenericWebhookEnabled => !string.IsNullOrWhiteSpace(_config.Webhooks.GenericUrl);
    public string GenericWebhookUrl => _config.Webhooks.GenericUrl;
    public string GenericWebhookHeadersJson => _config.Webhooks.GenericHeaders;
    public string GenericWebhookBodyTemplate => _config.Webhooks.GenericBodyTemplate;
    public string GenericWebhookProxyAddress => _config.Webhooks.GenericProxy;

    /* PagerDuty webhook — enabled by a non-empty routing key, like the sibling channels. */
    public bool PagerDutyEnabled => !string.IsNullOrWhiteSpace(_config.Webhooks.PagerDutyRoutingKey);
    public string PagerDutyRoutingKey => _config.Webhooks.PagerDutyRoutingKey;
    public bool PagerDutyUseEuRegion => _config.Webhooks.PagerDutyUseEuRegion;
    public string PagerDutyProxyAddress => _config.Webhooks.PagerDutyProxy;

    /* Scheduled-analysis notifications (AN3): the shared AnalysisNotificationService's severity floor
       + per-finding re-notify cooldown. The severity floor is now a control-plane knob (config Stage
       1) read through the by-reference config seam — a store reload reflects it immediately; clamped
       0–2 like Lite/Dashboard. The re-notify cooldown stays Lite's hardcoded default (not a knob). */
    public double AnalysisNotifySeverity => Math.Clamp(_config.Analysis.NotifySeverity, 0.0, 2.0);
    /* #2107: was a hardcoded 360 while the shared engine accepts a clamped [30, 10080] value and
       Lite always passed a configured one through — the Darling parity gap gotqn called out. */
    public int AnalysisNotifyCooldownMinutes => Math.Clamp(_config.Alerts.AnalysisNotifyCooldownMinutes, 30, 10080);

    /// <summary>#2710: the triage-link base — <c>web.publicBaseUrl</c>, read live through the by-reference
    /// config seam like every sibling. File-authoritative on purpose (see the WebConfig doc comment): a store
    /// config reload overwrites only Web.Enabled/Web.Port, so this survives it.</summary>
    public string TriageBaseUrl => _config.Web.PublicBaseUrl ?? "";
}
