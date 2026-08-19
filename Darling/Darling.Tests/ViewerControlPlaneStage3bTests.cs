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
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Stage 3b — the viewer's control-plane writes for the OPERATOR config sections: alert settings, notification
/// (SMTP + webhooks), the service flags, and the collector-schedule overrides. Pins the SQL shape
/// (parameterization, single-row id=1 upserts, the sparse-schedule ON CONFLICT arbiters) + the column parity
/// with the service's <see cref="StoreConfigProvider"/>, plus the pure overlay/preset/migration logic — all
/// with no live Postgres, exactly like the Stage-3a server + mute pins.
/// </summary>
public sealed class ViewerAlertSettingsSqlTests
{
    /* The 42 AlertsConfig + AnalysisConfig columns the service reads (StoreConfigProvider.ReadAlertSettingsAsync)
       — delivery_mode/per_event_max appended in V18 (#1141/#1236), the six long-running-query read knobs + the
       connection-change notify toggle in V20, the two connection opt-ins in V33 (#1659), and the three
       Availability Group knobs in V35 (#991).

       This list is the guard against the highest-risk defect class in the alert-settings plumbing: four
       parallel sequences (this list, the upsert's $N placeholders, the bind order, and the reader ordinals)
       have to agree, and a mismatch only shows up against a live Postgres. It had stopped at 36 while the
       store grew to 41, so the five newest columns — every one added since — were unguarded. */
    private static readonly string[] Columns =
    {
        "enabled", "cpu_enabled", "cpu_threshold_percent", "cpu_mode", "blocking_enabled", "blocking_count_threshold",
        "deadlock_enabled", "deadlock_count_threshold", "poison_wait_enabled", "poison_wait_threshold_ms",
        "long_running_query_enabled", "long_running_query_threshold_minutes", "tempdb_space_enabled",
        "tempdb_space_threshold_percent", "low_disk_enabled", "low_disk_threshold_percent", "low_disk_threshold_gb",
        "long_running_job_enabled", "long_running_job_multiplier", "failed_job_enabled", "failed_job_lookback_minutes",
        "cooldown_minutes", "excluded_databases", "analysis_enabled", "analysis_interval_minutes",
        "analysis_notifications_enabled", "analysis_notify_severity", "delivery_mode", "per_event_max",
        "long_running_query_max_results", "long_running_query_exclude_sp_server_diagnostics",
        "long_running_query_exclude_wait_for", "long_running_query_exclude_backups",
        "long_running_query_exclude_misc_waits", "long_running_query_exclude_cdc", "notify_connection_changes",
        "notify_connection_down_at_startup", "connection_refire_minutes",
        "notify_ag_health", "ag_lag_alert_seconds", "ag_redo_queue_alert_kb",
        "ag_disconnect_refire_minutes",
    };

    [Fact]
    public void UpsertSql_WritesEveryColumn_AsBoundParameters_SingleRowOnConflictUpdate()
    {
        var sql = ViewerDataService.AlertSettingsUpsertSql;

        Assert.Contains("INSERT INTO config_alert_settings", sql, StringComparison.Ordinal);
        Assert.Contains("VALUES (1,", sql, StringComparison.Ordinal); /* pinned to the single global row */
        foreach (var column in Columns)
        {
            Assert.Contains(column, sql, StringComparison.Ordinal);
        }

        /* Driven off Columns.Length rather than a literal, so adding a column to the list above is enough to
           extend the placeholder check too — the previous literal is exactly how this drifted to 36. */
        for (var i = 1; i <= Columns.Length; i++)
        {
            Assert.Contains("$" + i.ToString(System.Globalization.CultureInfo.InvariantCulture), sql, StringComparison.Ordinal);
        }

        Assert.Contains("ON CONFLICT (id) DO UPDATE", sql, StringComparison.Ordinal);
        Assert.Contains("modified_at = (now() AT TIME ZONE 'UTC')", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SelectSql_ReadsEveryColumn_FromTheSingleRow()
    {
        var sql = ViewerDataService.AlertSettingsSelectSql;
        Assert.Contains("FROM config_alert_settings WHERE id = 1", sql, StringComparison.Ordinal);
        foreach (var column in Columns)
        {
            Assert.Contains(column, sql, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("Total", "total")]
    [InlineData("SqlOnly", "sql")]
    public void CpuMode_MapsToTheServiceVocabulary_RoundTrips(string viewer, string store)
    {
        /* The service compares cpu_mode case-insensitively against "sql"; the viewer must emit that word. */
        Assert.Equal(store, ViewerDataService.MapCpuModeToStore(viewer));
        Assert.Equal(viewer, ViewerDataService.MapCpuModeFromStore(store));
    }

    [Fact]
    public void DefaultsRow_EqualsItself_AndDiffersOnAnyChange()
    {
        var a = AlertSettingsRow.Defaults();
        Assert.True(a.ValueEquals(AlertSettingsRow.Defaults()));

        var b = AlertSettingsRow.Defaults();
        b.CpuThresholdPercent = 55;
        Assert.False(a.ValueEquals(b));

        var c = AlertSettingsRow.Defaults();
        c.ExcludedDatabases = new List<string> { "msdb" };
        Assert.False(a.ValueEquals(c));
    }
}

/// <summary>The notification write partial: SQL shape + the DPAPI SMTP blob (never plaintext into Postgres).</summary>
public sealed class ViewerNotificationSqlTests
{
    private static readonly string[] Columns =
    {
        "smtp_host", "smtp_port", "smtp_use_ssl", "smtp_username", "smtp_encrypted_password", "smtp_from_address",
        "smtp_recipients", "email_cooldown_minutes", "teams_url", "teams_proxy", "slack_url", "slack_proxy",
        "generic_url", "generic_headers", "generic_body_template", "generic_proxy",
    };

    [Fact]
    public void UpsertSql_WritesEveryColumn_AsBoundParameters_SingleRowOnConflictUpdate()
    {
        var sql = ViewerDataService.NotificationUpsertSql;
        Assert.Contains("INSERT INTO config_notification", sql, StringComparison.Ordinal);
        Assert.Contains("VALUES (1,", sql, StringComparison.Ordinal);
        foreach (var column in Columns)
        {
            Assert.Contains(column, sql, StringComparison.Ordinal);
        }

        for (var i = 1; i <= 16; i++)
        {
            Assert.Contains("$" + i.ToString(System.Globalization.CultureInfo.InvariantCulture), sql, StringComparison.Ordinal);
        }

        Assert.Contains("ON CONFLICT (id) DO UPDATE", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SelectSql_ReadsEveryColumn_FromTheSingleRow()
    {
        var sql = ViewerDataService.NotificationSelectSql;
        Assert.Contains("FROM config_notification WHERE id = 1", sql, StringComparison.Ordinal);
        foreach (var column in Columns)
        {
            Assert.Contains(column, sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void SmtpPassword_IsSealedAsAServiceReadableBlob_NeverPlaintext()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "DPAPI requires Windows.");

        var settings = new ViewerAppSettings
        {
            SmtpEnabled = true,
            SmtpServer = "smtp.contoso.com",
            SmtpFromAddress = "alerts@contoso.com",
            SmtpRecipients = "dba@contoso.com",
        };

        var row = ViewerControlPlaneMigration.BuildNotificationRow(settings, "hunter2", "", "");

        Assert.NotNull(row.SmtpEncryptedPassword);
        Assert.NotEqual("hunter2", row.SmtpEncryptedPassword); /* not plaintext */
        /* The SERVICE must be able to read what the viewer sealed (identical DPAPI entropy + scope). */
        Assert.Equal("hunter2", DarlingSecrets.Unprotect(row.SmtpEncryptedPassword!));
    }

    [Fact]
    public void DisabledChannels_WriteEmptyFields_SoTheServiceTreatsThemOff()
    {
        var settings = new ViewerAppSettings
        {
            SmtpEnabled = false,
            SmtpServer = "smtp.contoso.com", /* typed but disabled */
            TeamsWebhookEnabled = false,
            SlackWebhookEnabled = false,
        };

        var row = ViewerControlPlaneMigration.BuildNotificationRow(settings, "pw", "https://teams", "https://slack");

        Assert.Equal("", row.SmtpHost);
        Assert.Null(row.SmtpEncryptedPassword);
        Assert.Equal("", row.TeamsUrl);
        Assert.Equal("", row.SlackUrl);
    }
}

/// <summary>The service-config write partial: the flag update touches only the viewer-owned columns (never
/// paused), and the reload-signal touch bumps the beacon (F16).</summary>
public sealed class ViewerServiceConfigSqlTests
{
    [Fact]
    public void UpdateFlagsSql_SetsOnlyCapturePlansMcp_OnTheSingleRow_NeverPaused()
    {
        var sql = ViewerDataService.ServiceConfigUpdateFlagsSql;
        Assert.Contains("UPDATE config_service", sql, StringComparison.Ordinal);
        Assert.Contains("capture_plans = $1", sql, StringComparison.Ordinal);
        Assert.Contains("mcp_enabled = $2", sql, StringComparison.Ordinal);
        Assert.Contains("mcp_port = $3", sql, StringComparison.Ordinal);
        /* #1562: the web dashboard toggle/port ride the same viewer-owned flag update. */
        Assert.Contains("web_enabled = $4", sql, StringComparison.Ordinal);
        Assert.Contains("web_port = $5", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE id = 1", sql, StringComparison.Ordinal);
        /* paused is a command, not a settings write — the flag update must never touch it. */
        Assert.DoesNotContain("paused", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SelectSql_ReadsPausedAndTheViewerOwnedFlags()
    {
        Assert.Contains("SELECT paused, capture_plans, mcp_enabled, mcp_port, web_enabled, web_port, query_store_backfill_enabled, query_store_text_budget_mb, max_concurrent_sweeps FROM config_service WHERE id = 1",
            ViewerDataService.ServiceConfigSelectSql, StringComparison.Ordinal);
    }

    [Fact]
    public void ReloadSignalSql_TouchesConfigService_ToBumpTheBeacon_WithoutChangingAFlag()
    {
        /* F16: config_mute_rules has no bump trigger, so a viewer mute write must touch config_service to fire
           the self-bump (config_version++) and make the service re-LoadAsync() its mute cache next sweep. */
        var sql = ViewerDataService.ConfigReloadSignalSql;
        Assert.Contains("UPDATE config_service SET updated_at = (now() AT TIME ZONE 'UTC') WHERE id = 1", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("paused", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("capture_plans", sql, StringComparison.Ordinal);
    }
}

/// <summary>The collector-schedule write partial: the sparse-table ON CONFLICT arbiters must match V17's partial
/// unique indexes AND the service's own enable/disable_collector upsert shape.</summary>
public sealed class ViewerCollectorSchedulesSqlTests
{
    [Fact]
    public void FleetUpsert_ArbitratesOnCollectorName_WhereServerIdIsNull()
    {
        var sql = ViewerDataService.CollectorScheduleFleetUpsertSql;
        Assert.Contains("INSERT INTO config_collector_schedules", sql, StringComparison.Ordinal);
        Assert.Contains("VALUES (NULL, $1, $2, $3, $4)", sql, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT (collector_name) WHERE server_id IS NULL DO UPDATE", sql, StringComparison.Ordinal);
        Assert.Contains("frequency_minutes = EXCLUDED.frequency_minutes", sql, StringComparison.Ordinal);
        Assert.Contains("retention_days = EXCLUDED.retention_days", sql, StringComparison.Ordinal);
        Assert.Contains("enabled = EXCLUDED.enabled", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ServerUpsert_ArbitratesOnServerIdCollectorName_WhereServerIdIsNotNull()
    {
        var sql = ViewerDataService.CollectorScheduleServerUpsertSql;
        Assert.Contains("VALUES ($1, $2, $3, $4, $5)", sql, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT (server_id, collector_name) WHERE server_id IS NOT NULL DO UPDATE", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DeleteScopeSql_KeyOnTheScope()
    {
        Assert.Contains("DELETE FROM config_collector_schedules WHERE server_id IS NULL",
            ViewerDataService.CollectorScheduleDeleteFleetScopeSql, StringComparison.Ordinal);
        Assert.Contains("DELETE FROM config_collector_schedules WHERE server_id = $1",
            ViewerDataService.CollectorScheduleDeleteServerScopeSql, StringComparison.Ordinal);
    }

    [Fact]
    public void DeleteAllServerScopesSql_ClearsEveryPerServerOverride_ButNotTheFleetDefault()
    {
        /* The "Apply Default to All" bulk reset deletes every per-server row (server_id IS NOT NULL) in one
           statement and must leave the fleet-wide default (server_id IS NULL) untouched. */
        var sql = ViewerDataService.CollectorScheduleDeleteAllServerScopesSql;
        Assert.Contains("DELETE FROM config_collector_schedules WHERE server_id IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("server_id IS NULL", sql, StringComparison.Ordinal);
    }
}

/// <summary>The pure store↔editor overlay + the preset table (ported from Lite, frequency-only).</summary>
public sealed class ViewerCollectorScheduleLogicTests
{
    [Fact]
    public void BuildDefaultSchedule_CoversEveryKnownCollector_AtItsCodeDefault()
    {
        var schedule = CollectorSchedulePresets.BuildDefaultSchedule();
        Assert.Equal(CollectorScheduleDefaults.All.Count, schedule.Count);

        foreach (var item in schedule)
        {
            var def = CollectorScheduleDefaults.All[item.Name];
            Assert.Equal(def.FrequencyMinutes, item.FrequencyMinutes);
            Assert.Equal(def.RetentionDays, item.RetentionDays);
            /* #2064: each collector's OWN shipped enabled state. The old blanket Assert.True is
               what let the seed hardcode true — which showed default-OFF long_query_completions
               as CHECKED in the editor while the feature was off (#2061). */
            Assert.Equal(def.DefaultEnabled, item.Enabled);
        }
    }

    [Fact]
    public void Presets_OnlyReferenceKnownCollectors()
    {
        foreach (var (_, intervals) in CollectorSchedulePresets.Presets)
        {
            foreach (var collector in intervals.Keys)
            {
                Assert.True(CollectorScheduleDefaults.All.ContainsKey(collector), $"preset references unknown collector '{collector}'");
            }
        }
    }

    [Fact]
    public void Presets_CoverTheSamePinnedCollectorSet()
    {
        /* The three preset tables are duplicated byte-for-byte from Lite's ScheduleManager.s_presets (the
           projects don't share a schedule library). Mirror Lite's own SchedulePresets_CoverTheSamePinnedCollectorSet
           guard HERE so a collector added to Lite's presets (or to one Darling preset but not the others) without
           being mirrored into all three fails this test instead of silently shipping a preset that skips it — the
           anti-drift pin the class docstring promises. (job_history/agent_status/default_trace_events fell behind
           exactly this way; see the #1494 review.) */
        var expected = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "wait_stats", "latch_stats", "spinlock_stats", "cpu_scheduler_stats", "plan_cache_stats",
            "query_stats", "procedure_stats", "query_store", "query_snapshots", "cpu_utilization",
            "file_io_stats", "memory_stats", "memory_clerks", "memory_pressure_events", "tempdb_stats",
            "perfmon_stats", "deadlocks", "memory_grant_stats", "waiting_tasks", "dmv_blocking_snapshot",
            "blocked_process_report", "running_jobs", "session_summary_stats", "system_health_events",
            "default_trace_events", "job_history", "agent_status",
            "ag_replica_states", "ag_database_replica_states", "plan_correction", "database_states"
        };

        Assert.Equal(3, CollectorSchedulePresets.Presets.Count);
        foreach (var (presetName, intervals) in CollectorSchedulePresets.Presets)
        {
            Assert.True(expected.SetEquals(intervals.Keys),
                $"preset '{presetName}' collector set drifted from the pinned set");
        }
    }

    [Fact]
    public void ApplyPreset_ChangesFrequenciesOnly_AndDetectRoundTrips()
    {
        var schedule = CollectorSchedulePresets.BuildDefaultSchedule();
        var retentionBefore = schedule.ToDictionary(s => s.Name, s => s.RetentionDays);

        CollectorSchedulePresets.ApplyPreset(schedule, "Low-Impact");

        /* Frequencies now match the Low-Impact table; retention untouched. */
        Assert.Equal(30, schedule.First(s => s.Name == "query_store").FrequencyMinutes);
        foreach (var item in schedule)
        {
            Assert.Equal(retentionBefore[item.Name], item.RetentionDays);
        }

        Assert.Equal("Low-Impact", CollectorSchedulePresets.DetectPreset(schedule));
    }

    [Fact]
    public void Overlay_FleetRowWins_OverCodeDefault_AndPerServerWinsOverFleet()
    {
        var overrides = new List<CollectorScheduleRow>
        {
            new(null, "wait_stats", 9, null, true),   /* fleet: freq 9 (retention falls through) */
            new(1234, "wait_stats", 3, 45, true),     /* server 1234: freq 3, retention 45 */
        };

        var fleet = CollectorScheduleOverlay.BuildEffectiveSchedule(overrides, serverId: null)
            .First(s => s.Name == "wait_stats");
        Assert.Equal(9, fleet.FrequencyMinutes);
        Assert.Equal(CollectorScheduleDefaults.All["wait_stats"].RetentionDays, fleet.RetentionDays); /* NULL fell through */

        var server = CollectorScheduleOverlay.BuildEffectiveSchedule(overrides, serverId: 1234)
            .First(s => s.Name == "wait_stats");
        Assert.Equal(3, server.FrequencyMinutes);
        Assert.Equal(45, server.RetentionDays);
    }

    [Fact]
    public void FleetOverrideRows_AreSparse_OnlyForCollectorsDifferingFromDefault()
    {
        var edited = CollectorSchedulePresets.BuildDefaultSchedule();
        edited.First(s => s.Name == "wait_stats").FrequencyMinutes = 15; /* the only change */

        var rows = CollectorScheduleOverlay.ToFleetOverrideRows(edited);

        var row = Assert.Single(rows);
        Assert.Null(row.ServerId);
        Assert.Equal("wait_stats", row.CollectorName);
        Assert.Equal(15, row.FrequencyMinutes);
    }

    [Fact]
    public void FleetOverrideRows_EnablingADefaultOffCollector_IsPersisted()
    {
        /* #2064 regression (#2061 as reported): enabling a default-OFF collector at FLEET scope while
           its frequency/retention stay at the code default must still write a row. The sparse filter
           used to test `item.Enabled` as bare truth, so this exact edit produced NO row — the save
           looked successful, the store held nothing, and the Long Queries tab correctly reported the
           feature OFF while the editor showed it checked. Server scope always worked (full snapshot),
           which is what made the asymmetry so confusing to diagnose. */
        var offByDefault = CollectorScheduleDefaults.All.First(kv => !kv.Value.DefaultEnabled).Key;

        var edited = CollectorSchedulePresets.BuildDefaultSchedule();
        var item = edited.First(s => s.Name == offByDefault);
        Assert.False(item.Enabled);          /* the seed is honest now */
        item.Enabled = true;                 /* the operator opts in, nothing else touched */

        var row = Assert.Single(CollectorScheduleOverlay.ToFleetOverrideRows(edited));
        Assert.Equal(offByDefault, row.CollectorName);
        Assert.True(row.Enabled);
        Assert.Null(row.ServerId);
    }

    [Fact]
    public void ServerOverrideRows_AreAFullSnapshot_ForWysiwyg()
    {
        var edited = CollectorSchedulePresets.BuildDefaultSchedule();
        var rows = CollectorScheduleOverlay.ToServerOverrideRows(edited, serverId: 7);

        Assert.Equal(CollectorScheduleDefaults.All.Count, rows.Count);
        Assert.All(rows, r => Assert.Equal(7, r.ServerId));
    }

    [Fact]
    public void ServerHasOverride_ReflectsTheServersRows()
    {
        var overrides = new List<CollectorScheduleRow> { new(7, "wait_stats", 5, null, true) };
        Assert.True(CollectorScheduleOverlay.ServerHasOverride(overrides, 7));
        Assert.False(CollectorScheduleOverlay.ServerHasOverride(overrides, 8));
    }
}

/// <summary>The viewer's control commands agree with the service executor's dispatch (the two ends must use the
/// same command_type words).</summary>
public sealed class ViewerControlCommandParityTests
{
    private static ClaimedCommand Command(string type, int? target = null) => new(1, type, target, null, "viewer");

    [Fact]
    public void PauseResume_AreRecognizedStoreWrites()
    {
        Assert.Equal("pause", ViewerDataService.CommandPause);
        Assert.Equal("resume", ViewerDataService.CommandResume);
        Assert.Equal(CommandKind.StoreWrite, DarlingCommandExecutor.ResolvePlan(Command(ViewerDataService.CommandPause)).Kind);
        Assert.Equal(CommandKind.StoreWrite, DarlingCommandExecutor.ResolvePlan(Command(ViewerDataService.CommandResume)).Kind);
    }

    [Fact]
    public void SnapshotAnalyze_AreRecognizedImperatives_RequiringATarget()
    {
        Assert.Equal("snapshot_now", ViewerDataService.CommandSnapshotNow);
        Assert.Equal("analyze_now", ViewerDataService.CommandAnalyzeNow);

        Assert.Equal(CommandKind.Snapshot, DarlingCommandExecutor.ResolvePlan(Command(ViewerDataService.CommandSnapshotNow, target: 1)).Kind);
        Assert.Equal(CommandKind.Analyze, DarlingCommandExecutor.ResolvePlan(Command(ViewerDataService.CommandAnalyzeNow, target: 1)).Kind);

        /* Without a target they fail — matching the viewer only ever sending them with a server id. */
        Assert.Equal(CommandKind.Fail, DarlingCommandExecutor.ResolvePlan(Command(ViewerDataService.CommandSnapshotNow)).Kind);
        Assert.Equal(CommandKind.Fail, DarlingCommandExecutor.ResolvePlan(Command(ViewerDataService.CommandAnalyzeNow)).Kind);
    }

    [Fact]
    public void PurgeNow_IsRecognizedFleetWide_WithNoTargetRequired()
    {
        Assert.Equal("purge_now", ViewerDataService.CommandPurgeNow);

        /* Fleet-wide over the shared tables — resolves to Purge with NO target (unlike snapshot_now/analyze_now,
           which the viewer only ever sends with a server id). */
        Assert.Equal(CommandKind.Purge, DarlingCommandExecutor.ResolvePlan(Command(ViewerDataService.CommandPurgeNow)).Kind);
        Assert.Equal(CommandKind.Purge, DarlingCommandExecutor.ResolvePlan(Command(ViewerDataService.CommandPurgeNow, target: 1)).Kind);
    }

    [Fact]
    public void FetchPlan_IsRecognized_WithATargetAndAViewerBuiltHandleArgs()
    {
        Assert.Equal("fetch_plan", ViewerDataService.CommandFetchPlan);

        /* The viewer builds the args; the executor must recognize them as a live-plan fetch. */
        var args = ViewerDataService.BuildPlanFetchArgsByPlanHandle("0x0600AA", "master");
        var command = new ClaimedCommand(1, ViewerDataService.CommandFetchPlan, 5, args, "viewer");
        Assert.Equal(CommandKind.FetchPlan, DarlingCommandExecutor.ResolvePlan(command).Kind);

        /* Without a target it fails — the viewer only ever sends it with a server id. */
        Assert.Equal(CommandKind.Fail,
            DarlingCommandExecutor.ResolvePlan(new ClaimedCommand(1, ViewerDataService.CommandFetchPlan, null, args, "viewer")).Kind);
    }
}

/// <summary>The one-time operational migrate-in: the defaults-only import decision + the projection, with no
/// live store or vault.</summary>
public sealed class ViewerControlPlaneMigrationTests
{
    [Fact]
    public void ShouldImportAlerts_OnlyWhenStoreIsDefault_AndViewerDiffers()
    {
        var defaults = AlertSettingsRow.Defaults();

        var viewerCustom = AlertSettingsRow.Defaults();
        viewerCustom.CpuThresholdPercent = 60;

        Assert.True(ViewerControlPlaneMigration.ShouldImportAlerts(AlertSettingsRow.Defaults(), viewerCustom));  /* store default, viewer differs */
        Assert.False(ViewerControlPlaneMigration.ShouldImportAlerts(AlertSettingsRow.Defaults(), defaults));      /* viewer also default -> nothing to carry */

        var storeCustom = AlertSettingsRow.Defaults();
        storeCustom.CpuThresholdPercent = 95;
        Assert.False(ViewerControlPlaneMigration.ShouldImportAlerts(storeCustom, viewerCustom));                  /* store already tuned -> never clobber */
    }

    [Fact]
    public void ShouldImportMcp_OnlyWhenStoreMcpIsDefault_AndViewerDiffers()
    {
        var storeDefault = new ServiceConfigRow();                                  /* mcp off, 5152 */
        var storeTuned = new ServiceConfigRow { McpEnabled = true, McpPort = 6000 };

        Assert.True(ViewerControlPlaneMigration.ShouldImportMcp(storeDefault, new ViewerAppSettings { McpEnabled = true }));
        Assert.False(ViewerControlPlaneMigration.ShouldImportMcp(storeDefault, new ViewerAppSettings()));         /* viewer also default */
        Assert.False(ViewerControlPlaneMigration.ShouldImportMcp(storeTuned, new ViewerAppSettings { McpEnabled = true }));
    }

    [Fact]
    public void BuildAlertRow_ProjectsEveryFieldAndMapsCpuMode()
    {
        var settings = new ViewerAppSettings
        {
            AlertsEnabled = false,
            NotifyConnectionChanges = false,
            AlertCpuThreshold = 66,
            AlertCpuMode = "SqlOnly",
            AlertExcludedDatabases = new List<string> { "msdb", "tempdb" },
            AnalysisIntervalMinutes = 45,
            AnalysisNotifySeverity = 1.2,
            AlertDeliveryMode = "PerEvent",
            AlertPerEventMaxPerCycle = 7,
            AlertLongRunningQueryMaxResults = 20,
            AlertLongRunningQueryExcludeSpServerDiagnostics = false,
            AlertLongRunningQueryExcludeCdc = false,
        };

        var row = ViewerControlPlaneMigration.BuildAlertRow(settings);

        Assert.False(row.Enabled);
        Assert.False(row.NotifyConnectionChanges);
        Assert.Equal(66, row.CpuThresholdPercent);
        Assert.Equal("sql", row.CpuMode);
        Assert.Equal(new[] { "msdb", "tempdb" }, row.ExcludedDatabases);
        Assert.Equal(45, row.AnalysisIntervalMinutes);
        Assert.Equal(1.2, row.AnalysisNotifySeverity, 3);
        /* #1141/#1236: the delivery customization carries into the store row. */
        Assert.Equal("PerEvent", row.DeliveryMode);
        Assert.Equal(7, row.PerEventMax);
        /* V20: the long-running-query read-shape customization carries into the store row too. */
        Assert.Equal(20, row.LongRunningQueryMaxResults);
        Assert.False(row.LongRunningQueryExcludeSpServerDiagnostics);
        Assert.False(row.LongRunningQueryExcludeCdc);
        Assert.True(row.LongRunningQueryExcludeWaitFor); /* untouched viewer default carries through */
    }

    [Fact]
    public async Task MigrateAsync_Disconnected_IsANoOp_AndLeavesTheMarkerUnwritten()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The migrate-in path is Windows-only (DPAPI secret re-seal).");

        using var dir = new TempDir();
        var marker = Path.Combine(dir.Path, "cp.marker");
        var migration = new ViewerControlPlaneMigration(new ViewerAppSettings(), marker);

        var imported = await migration.MigrateAsync(dataService: null);

        Assert.Equal(0, imported);
        Assert.False(migration.AlreadyMigrated);
        Assert.False(File.Exists(marker));
    }

    [Fact]
    public void Marker_GatesTheOnceGuard()
    {
        using var dir = new TempDir();
        var marker = Path.Combine(dir.Path, "cp.marker");
        Assert.False(new ViewerControlPlaneMigration(new ViewerAppSettings(), marker).AlreadyMigrated);

        File.WriteAllText(marker, "done");
        Assert.True(new ViewerControlPlaneMigration(new ViewerAppSettings(), marker).AlreadyMigrated);
    }

    [Fact]
    public async Task MigrateAsync_UnseededOrUnreachableStore_IsANoOp_AndLeavesTheMarkerUnwritten()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The migrate-in path is Windows-only (DPAPI secret re-seal).");

        using var dir = new TempDir();
        var marker = Path.Combine(dir.Path, "cp.marker");

        /* An unreachable store: IsConfigSeededAsync fails safe to false, so the migrate must import nothing AND
           NOT burn the once-marker (a later, seeded run still carries the operator's settings across). This
           pins the seeded-guard that prevents a partially-seeded store from stranding the import. Port 1 has no
           listener -> a fast connection-refused; Timeout=1 caps it. */
        await using var dataService = new ViewerDataService(
            "Host=127.0.0.1;Port=1;Username=x;Password=x;Timeout=1;Command Timeout=1");
        var migration = new ViewerControlPlaneMigration(new ViewerAppSettings(), marker);

        var imported = await migration.MigrateAsync(dataService);

        Assert.Equal(0, imported);
        Assert.False(migration.AlreadyMigrated);
        Assert.False(File.Exists(marker));
    }

    private sealed class TempDir : IDisposable
    {
        public string Path { get; } = Directory.CreateTempSubdirectory("darling-cp3b-").FullName;
        public void Dispose() { try { Directory.Delete(Path, recursive: true); } catch { /* best-effort */ } }
    }
}
