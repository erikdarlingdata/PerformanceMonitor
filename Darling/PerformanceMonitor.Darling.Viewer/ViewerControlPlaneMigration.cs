/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Versioning;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The one-time import of a pre-Stage-3b viewer's OPERATIONAL settings (the alert engine, automated analysis,
/// SMTP + Teams/Slack delivery, and the MCP toggle) out of the now-severed <c>viewer-settings.json</c>
/// (<see cref="ViewerAppSettings"/>) into the control-plane store. Stage 3b makes the store the source of
/// truth for what the running service honors; this carries a viewer's existing customizations across so they
/// keep taking effect without re-entering them — the operational twin of <see cref="ViewerServerMigration"/>
/// (which carried the SERVER definitions).
///
/// <para><b>Defaults-only, no clobber.</b> A section is imported ONLY when the store's copy is still at the
/// V17 seed defaults (the operator has NOT configured it service-side via darling.json) AND the viewer's copy
/// actually differs from those defaults (there is a real customization to carry). So a store an operator
/// already tuned is never overwritten, and a fresh viewer with nothing customized writes nothing (no spurious
/// <c>config_version</c> bump). <b>Runs once.</b> A marker file guards it after a clean pass. Read-only seats
/// and a disconnected viewer skip it (nothing to write); a <see cref="ViewerReadOnlyException"/> mid-pass
/// leaves the marker unwritten so a later writable run finishes.</para>
///
/// <para><b>Secrets.</b> The SMTP password is read from Windows Credential Manager (the old viewer-local vault)
/// and re-sealed as the service-decryptable DPAPI-LocalMachine blob (<see cref="ViewerServerSecret"/>); the
/// Teams/Slack URLs (also formerly in the vault) carry as the store's plain-text channel URLs. So the SMTP
/// secret path is Windows-only; the alert/analysis/MCP imports are platform-independent.</para>
///
/// <para><b>Schedules.</b> There is deliberately NO schedule import: the Darling viewer never had a local
/// collector-schedule editor (it was an informational panel before Stage 3b), so there is no source to carry —
/// an un-customized store already collects on the shared <c>CollectorScheduleDefaults</c>, which is the intent.</para>
/// </summary>
public sealed class ViewerControlPlaneMigration
{
    private readonly ViewerAppSettings _appSettings;
    private readonly string _markerPath;

    /// <param name="appSettings">The loaded viewer app settings (the pre-Stage-3b operational values).</param>
    /// <param name="markerPath">Override the once-marker path (tests pass a temp file); null uses <see cref="DefaultMarkerPath"/>.</param>
    public ViewerControlPlaneMigration(ViewerAppSettings appSettings, string? markerPath = null)
    {
        _appSettings = appSettings ?? throw new ArgumentNullException(nameof(appSettings));
        _markerPath = markerPath ?? DefaultMarkerPath();
    }

    /// <summary>%APPDATA%\PerformanceMonitorDarling\viewer-controlplane-migrated.marker.</summary>
    public static string DefaultMarkerPath()
    {
        var directory = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
            "PerformanceMonitorDarling");
        return Path.Combine(directory, "viewer-controlplane-migrated.marker");
    }

    /// <summary>True once the migrate-in has completed a clean pass (the marker exists).</summary>
    public bool AlreadyMigrated => File.Exists(_markerPath);

    /// <summary>
    /// Imports every still-default store section the viewer has a customization for, then writes the marker. A
    /// no-op (returns 0) when the viewer is disconnected, connected read-only, or already migrated. Returns the
    /// number of sections actually written (0–3). A <see cref="ViewerReadOnlyException"/> mid-pass stops
    /// without marking done, so a later writable run retries.
    /// </summary>
    [SupportedOSPlatform("windows")]
    public async Task<int> MigrateAsync(ViewerDataService? dataService, CancellationToken cancellationToken = default)
    {
        if (dataService is null || dataService.IsReadOnly || AlreadyMigrated)
        {
            return 0;
        }

        /* Wait for the service to finish seeding before importing. config_service is written LAST (its presence
           marks the seed complete), so on a reachable-but-unseeded (or partially-seeded) store every section
           read returns null: importing nothing AND burning the once-marker would strand the operator's pre-3b
           settings forever, and a partial seed would drop the MCP section. Return WITHOUT the marker so a
           later, fully-seeded run carries everything across. */
        if (!await dataService.IsConfigSeededAsync(cancellationToken))
        {
            return 0;
        }

        var imported = 0;
        try
        {
            /* Alerts + automated analysis. */
            var storeAlerts = await dataService.GetAlertSettingsAsync(cancellationToken);
            var viewerAlerts = BuildAlertRow(_appSettings);
            if (storeAlerts is not null && ShouldImportAlerts(storeAlerts, viewerAlerts))
            {
                await dataService.UpsertAlertSettingsAsync(viewerAlerts, cancellationToken);
                imported++;
            }

            /* SMTP + Teams/Slack delivery (secrets from the old vault, re-sealed for the service). */
            var storeNotify = await dataService.GetNotificationAsync(cancellationToken);
            var viewerNotify = BuildNotificationRow(
                _appSettings,
                ViewerSecretStore.GetSmtpPassword(),
                ViewerSecretStore.GetTeamsWebhookUrl(),
                ViewerSecretStore.GetSlackWebhookUrl());
            if (storeNotify is not null && ShouldImportNotification(storeNotify, viewerNotify))
            {
                await dataService.UpsertNotificationAsync(viewerNotify, cancellationToken);
                imported++;
            }

            /* MCP toggle/port (config_service, UPDATE-only; preserve the store's current capture_plans). The web
               dashboard columns (#1562) are carried through from the viewer's local settings — they default to
               the store defaults (off / 5153) for a pre-web viewer, so this is a no-op unless the operator set them. */
            var storeService = await dataService.GetServiceConfigAsync(cancellationToken);
            if (storeService is not null && ShouldImportMcp(storeService, _appSettings))
            {
                /* #2167: carry the store's current backfill flag through unchanged — this migration imports
                   MCP/web settings and must never flip an operator's backfill switch as a side effect. */
                await dataService.UpdateServiceFlagsAsync(
                    storeService.CapturePlans, _appSettings.McpEnabled, _appSettings.McpPort,
                    _appSettings.WebEnabled, _appSettings.WebPort, storeService.QueryStoreBackfillEnabled,
                    storeService.QueryStoreTextBudgetMb, storeService.MaxConcurrentSweeps, cancellationToken);
                imported++;
            }
        }
        catch (ViewerReadOnlyException)
        {
            /* Grants tightened under us — leave the marker unwritten so a later writable run finishes. */
            ViewerLogger.Warn("ViewerControlPlaneMigration", "Store went read-only mid-migrate; will retry next run.");
            return imported;
        }

        WriteMarker();
        return imported;
    }

    /// <summary>Projects the viewer's alert + analysis app settings onto a store row (pure — pinned by tests).</summary>
    public static AlertSettingsRow BuildAlertRow(ViewerAppSettings s)
    {
        ArgumentNullException.ThrowIfNull(s);
        return new AlertSettingsRow
        {
            Enabled = s.AlertsEnabled,
            /* V20: carry a pre-3b viewer's connection-change notify choice into the store now that the service
               honors it (was a dead knob before V20). Fresh-viewer default matches Defaults() → nothing imported. */
            NotifyConnectionChanges = s.NotifyConnectionChanges,
            CpuEnabled = s.AlertCpuEnabled,
            CpuThresholdPercent = s.AlertCpuThreshold,
            CpuMode = ViewerDataService.MapCpuModeToStore(s.AlertCpuMode),
            BlockingEnabled = s.AlertBlockingEnabled,
            BlockingCountThreshold = s.AlertBlockingThreshold,
            DeadlockEnabled = s.AlertDeadlockEnabled,
            DeadlockCountThreshold = s.AlertDeadlockThreshold,
            PoisonWaitEnabled = s.AlertPoisonWaitEnabled,
            PoisonWaitThresholdMs = s.AlertPoisonWaitThresholdMs,
            LongRunningQueryEnabled = s.AlertLongRunningQueryEnabled,
            LongRunningQueryThresholdMinutes = s.AlertLongRunningQueryThresholdMinutes,
            TempDbSpaceEnabled = s.AlertTempDbSpaceEnabled,
            TempDbSpaceThresholdPercent = s.AlertTempDbSpaceThresholdPercent,
            LowDiskEnabled = s.AlertLowDiskEnabled,
            LowDiskThresholdPercent = s.AlertLowDiskThresholdPercent,
            LowDiskThresholdGb = s.AlertLowDiskThresholdGb,
            LongRunningJobEnabled = s.AlertLongRunningJobEnabled,
            LongRunningJobMultiplier = s.AlertLongRunningJobMultiplier,
            FailedJobEnabled = s.AlertFailedJobEnabled,
            FailedJobLookbackMinutes = s.AlertFailedJobLookbackMinutes,
            CooldownMinutes = s.AlertCooldownMinutes,
            ExcludedDatabases = new List<string>(s.AlertExcludedDatabases ?? new List<string>()),
            AnalysisEnabled = s.AnalysisEnabled,
            AnalysisIntervalMinutes = s.AnalysisIntervalMinutes,
            AnalysisNotificationsEnabled = s.AnalysisNotificationsEnabled,
            AnalysisNotifySeverity = s.AnalysisNotifySeverity,
            /* #1141/#1236: carry a pre-3b viewer's delivery customization into the store now that the service
               honors it (was a dead knob before V18). Fresh-viewer defaults match Defaults() → nothing imported. */
            DeliveryMode = (s.AlertDeliveryMode is "Summary" or "PerEvent") ? s.AlertDeliveryMode : "Summary",
            PerEventMax = s.AlertPerEventMaxPerCycle,
            /* V20: carry a pre-3b viewer's long-running-query read customization into the store now that the
               service honors it (was a dead knob before V20). Fresh-viewer defaults match Defaults() → nothing imported. */
            LongRunningQueryMaxResults = s.AlertLongRunningQueryMaxResults,
            LongRunningQueryExcludeSpServerDiagnostics = s.AlertLongRunningQueryExcludeSpServerDiagnostics,
            LongRunningQueryExcludeWaitFor = s.AlertLongRunningQueryExcludeWaitFor,
            LongRunningQueryExcludeBackups = s.AlertLongRunningQueryExcludeBackups,
            LongRunningQueryExcludeMiscWaits = s.AlertLongRunningQueryExcludeMiscWaits,
            LongRunningQueryExcludeCdc = s.AlertLongRunningQueryExcludeCdc,
        };
    }

    /// <summary>
    /// Projects the viewer's SMTP/webhook app settings + the vault secrets onto a store row (pure — pinned by
    /// tests). A DISABLED channel writes EMPTY key fields (SMTP host/from/to; a webhook URL) so the service —
    /// which derives enablement from non-empty fields — treats it as off; an enabled channel carries its
    /// values, with the SMTP password sealed as the service-decryptable DPAPI blob.
    /// </summary>
    [SupportedOSPlatform("windows")]
    public static NotificationRow BuildNotificationRow(ViewerAppSettings s, string? smtpPassword, string teamsUrl, string slackUrl)
    {
        ArgumentNullException.ThrowIfNull(s);
        var row = new NotificationRow
        {
            EmailCooldownMinutes = s.EmailCooldownMinutes,
        };

        if (s.SmtpEnabled)
        {
            row.SmtpHost = s.SmtpServer ?? "";
            row.SmtpPort = s.SmtpPort;
            row.SmtpUseSsl = s.SmtpUseSsl;
            row.SmtpUsername = string.IsNullOrWhiteSpace(s.SmtpUsername) ? null : s.SmtpUsername;
            row.SmtpFromAddress = s.SmtpFromAddress ?? "";
            row.SmtpRecipients = s.SmtpRecipients ?? "";
            row.SmtpEncryptedPassword = string.IsNullOrEmpty(smtpPassword) ? null : ViewerServerSecret.Protect(smtpPassword);
        }

        if (s.TeamsWebhookEnabled)
        {
            row.TeamsUrl = teamsUrl ?? "";
            row.TeamsProxy = s.TeamsProxyAddress ?? "";
        }

        if (s.SlackWebhookEnabled)
        {
            row.SlackUrl = slackUrl ?? "";
            row.SlackProxy = s.SlackProxyAddress ?? "";
        }

        return row;
    }

    /// <summary>Import alerts only when the store section is untouched (defaults) AND the viewer has a real
    /// customization to carry — never clobber an operator-tuned store, never write defaults-over-defaults.</summary>
    public static bool ShouldImportAlerts(AlertSettingsRow storeRow, AlertSettingsRow viewerRow)
    {
        ArgumentNullException.ThrowIfNull(storeRow);
        ArgumentNullException.ThrowIfNull(viewerRow);
        return storeRow.ValueEquals(AlertSettingsRow.Defaults()) && !viewerRow.ValueEquals(AlertSettingsRow.Defaults());
    }

    /// <summary>Import notification config only when the store section is untouched AND the viewer differs.</summary>
    public static bool ShouldImportNotification(NotificationRow storeRow, NotificationRow viewerRow)
    {
        ArgumentNullException.ThrowIfNull(storeRow);
        ArgumentNullException.ThrowIfNull(viewerRow);
        return storeRow.ValueEquals(NotificationRow.Defaults()) && !viewerRow.ValueEquals(NotificationRow.Defaults());
    }

    /// <summary>Import the MCP toggle/port only when the store's MCP flags are still at defaults AND the viewer
    /// differs (capture_plans/paused are not viewer-local, so they don't gate this).</summary>
    public static bool ShouldImportMcp(ServiceConfigRow storeRow, ViewerAppSettings viewer)
    {
        ArgumentNullException.ThrowIfNull(storeRow);
        ArgumentNullException.ThrowIfNull(viewer);
        var storeAtMcpDefaults = !storeRow.McpEnabled && storeRow.McpPort == 5152;
        var viewerCustomized = viewer.McpEnabled || viewer.McpPort != 5152;
        return storeAtMcpDefaults && viewerCustomized;
    }

    private void WriteMarker()
    {
        try
        {
            var directory = Path.GetDirectoryName(_markerPath);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            File.WriteAllText(_markerPath, $"migrated {DateTime.UtcNow:O}");
        }
        catch (Exception ex)
        {
            /* A failed marker write only means the migrate re-runs next launch; the defaults-only guard makes
               a re-run a no-op once the store carries the imported values. Don't crash the viewer. */
            ViewerLogger.Warn("ViewerControlPlaneMigration", $"Could not write the migrate marker '{_markerPath}': {ex.Message}");
        }
    }
}
