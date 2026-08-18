/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The service side of the store&lt;-&gt;service control plane (Stage 1): the service reads its LIVE
/// operational config from the V17 <c>config.*</c> tables instead of only from darling.json. Generalizes
/// the mute-rule load pattern (<see cref="PgMuteRuleStore"/>): on first startup, if a config section is
/// empty, SEED it once from darling.json; thereafter the STORE is authoritative. The worker polls
/// <c>config_service.config_version</c> each sweep and, on change, re-reads and hot-swaps the held
/// <see cref="DarlingConfig"/> in place — the by-reference <see cref="DarlingAlertSettings"/> seam and the
/// runner's <c>() =&gt; config.CapturePlans</c> provider reflect the change immediately.
///
/// <para>Store-unreachable is non-fatal (Lite's mute-store posture): the read/seed methods log a warning
/// and return <c>null</c> / no-op so the service keeps running on the darling.json-loaded config — never
/// worse than before this feature. Timestamps are naive-UTC (Npgsql rejects Kind=Utc against
/// <c>timestamp</c>). Secrets are never plaintext in the store: <c>encrypted_password</c> carries the DPAPI
/// blob, and a darling.json plaintext dev password is backfilled from the in-memory bootstrap config by
/// <c>server_id</c> at read time (so it drives the connect path without ever being written to Postgres).</para>
/// </summary>
public sealed class StoreConfigProvider
{
    private readonly NpgsqlDataSource _postgres;
    private readonly ILogger? _logger;

    public StoreConfigProvider(NpgsqlDataSource postgres, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _logger = logger;
    }

    /* ---------------- reload beacon ---------------- */

    /// <summary>
    /// Reads <c>config_service.config_version</c> — the reload beacon the worker polls each sweep.
    /// Returns null when the store is unreachable or the single row is missing (the caller keeps its
    /// last-seen version and reloads nothing, never crashing on a transient store blip).
    /// </summary>
    public async Task<long?> ReadConfigVersionAsync(CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand("SELECT config_version FROM config_service WHERE id = 1", connection);
            var result = await command.ExecuteScalarAsync(cancellationToken);
            return result is null or DBNull ? null : Convert.ToInt64(result, System.Globalization.CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogWarning("Could not read config_version — keeping the current live config: {Message}", ex.Message);
            return null;
        }
    }

    /* ---------------- seed (darling.json -> store, once, only empty sections) ---------------- */

    /// <summary>
    /// Seeds the store from darling.json ONCE — idempotent, seeding only sections that are still empty
    /// (each guarded by a row-count check so a re-seed writes nothing AND fires no config_version bump).
    /// The desired-state tables seed first and <c>config_service</c> (the beacon + completion marker) LAST,
    /// so a seed interrupted before it completes leaves <c>config_service</c> absent — the worker then reads
    /// a null config_version and never reloads a half-seeded store, re-seeding on the next start.
    /// <c>config_collector_schedules</c> is intentionally left empty (absent row =
    /// <see cref="CollectorScheduleDefaults"/>). Failure-isolated: a seed error is warned and the service
    /// proceeds on darling.json.
    /// </summary>
    public async Task SeedIfEmptyAsync(DarlingConfig config, CancellationToken cancellationToken)
    {
        if (config is null)
        {
            throw new ArgumentNullException(nameof(config));
        }

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            var now = Naive(DateTime.UtcNow);

            if (await CountAsync(connection, "config_alert_settings", cancellationToken) == 0)
            {
                await SeedAlertSettingsAsync(connection, config, now, cancellationToken);
            }

            if (await CountAsync(connection, "config_notification", cancellationToken) == 0)
            {
                await SeedNotificationAsync(connection, config, now, cancellationToken);
            }

            if (await CountAsync(connection, "config_monitored_servers", cancellationToken) == 0)
            {
                await SeedMonitoredServersAsync(connection, config, now, cancellationToken);
            }
            else
            {
                /* #2254: the seed is skipped, so any server added to darling.json AFTER the first start is
                   silently ignored — and --test-connection reads the FILE, so it validates those servers
                   happily while the service never monitors them. Say so once per start instead of leaving the
                   operator to discover it. */
                await WarnAboutFileOnlyServersAsync(connection, config, cancellationToken);
            }

            /* LAST — its presence marks the seed complete (the reload gate keys on config_version). */
            if (await CountAsync(connection, "config_service", cancellationToken) == 0)
            {
                await SeedServiceRowAsync(connection, config, now, cancellationToken);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogWarning(
                "Could not seed the config store from darling.json — running on the file config; the store will seed on a later start: {Message}",
                ex.Message);
        }
    }

    /// <summary>
    /// #2254: names the servers present in darling.json that the store does not have, once per start.
    ///
    /// <para>The seed runs only while <c>config_monitored_servers</c> is empty, so a server added to the file
    /// after the first successful start is a permanent no-op. What made that expensive in the field is that
    /// <c>--test-connection</c> reads the FILE and validated the new server as PASS, so the operator had two
    /// outputs that were each correct about different things and no way to see the disagreement: config edit,
    /// service restart, support round trip.</para>
    ///
    /// <para>Compared on <b>server_id OR name</b> (#2158). It used to be id alone, on the grounds that the id
    /// is what the collectors key on — correct while every row's id equalled the hash of its own address, and
    /// wrong the moment an edit began PRESERVING a row's identity so a re-addressed server keeps its history.
    /// After such an edit the file's derived id matches nothing, and an id-only comparison would report a
    /// server that IS monitored as absent, then advise re-adding it — wrong advice, on every start, about the
    /// one server the operator had just fixed. The name arm covers that; a genuinely removed server is gone
    /// from the store under both keys, so the Viewer-Remove case still reports exactly as before.</para>
    /// </summary>
    private async Task WarnAboutFileOnlyServersAsync(
        NpgsqlConnection connection, DarlingConfig config, CancellationToken ct)
    {
        var storeIds = new HashSet<int>();
        var storeNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using (var command = new NpgsqlCommand("SELECT server_id, name FROM config_monitored_servers", connection))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                storeIds.Add(reader.GetInt32(0));
                if (!reader.IsDBNull(1))
                {
                    storeNames.Add(reader.GetString(1));
                }
            }
        }

        var fileOnly = ServersOnlyInFile(config.Servers, storeIds, storeNames);
        if (fileOnly.Count == 0)
        {
            return;
        }

        /* #2258: the OBSERVED registry is the tombstone, and it already exists. collect.servers gets a row
           upserted on every successful connect, and the Viewer's Remove deletes only from
           config_monitored_servers (the DESIRED config) — so a row surviving there means "this server really
           was monitored once", which is exactly the fact that separates the two causes. Nothing purges it
           either: it is a registry, not a time series, so retention leaves it alone. */
        var everMonitored = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using (var observed = new NpgsqlCommand("SELECT display_name, server_name FROM collect.servers", connection))
        await using (var reader = await observed.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                if (!reader.IsDBNull(0))
                {
                    everMonitored.Add(reader.GetString(0));
                }

                if (!reader.IsDBNull(1))
                {
                    everMonitored.Add(reader.GetString(1));
                }
            }
        }

        var (neverRegistered, deliberatelyRemoved) = SplitByEverMonitored(fileOnly, everMonitored);

        /* Cause 1 — in the file, never monitored. This is the field report (#2252): the operator edited the
           file expecting it to be picked up, and it silently was not. A WARNING, because it is the case where
           something the operator wants is not happening and only they can fix it. */
        if (neverRegistered.Count > 0)
        {
            _logger?.LogWarning(
                "darling.json lists {Count} server(s) that are NOT monitored and never have been: {Servers}. "
                + "The store is authoritative after the first seed, so adding a server to the file does not "
                + "register it and a restart cannot change that — add them with the Viewer's Add Server dialog "
                + "or the MCP add_servers tool. Note --test-connection reads darling.json, so it will keep "
                + "reporting them as PASS while they collect nothing.",
                neverRegistered.Count,
                string.Join(", ", neverRegistered));
        }

        /* Cause 2 — monitored once, then removed, and the file was left alone. A CORRECT state, so this is
           Information and says so plainly rather than advising anything. It is not silent because the file
           still names them and --test-connection will still call them PASS, which is worth one line at
           startup; but it no longer tells the operator to re-add a server they deliberately dropped. */
        if (deliberatelyRemoved.Count > 0)
        {
            _logger?.LogInformation(
                "darling.json still lists {Count} server(s) that were monitored and have since been removed: "
                + "{Servers}. That is expected — the Viewer's Remove deletes the registration and never edits "
                + "the file. Delete them from darling.json to silence this line; their collected history is "
                + "kept either way.",
                deliberatelyRemoved.Count,
                string.Join(", ", deliberatelyRemoved));
        }
    }

    /// <summary>
    /// Splits the file-only servers into "never monitored" and "monitored once, since removed" (#2258), using the
    /// observed registry as the evidence.
    ///
    /// <para><b>Why this needs no tombstone table.</b> #2258 proposed one — a <c>config_removed_servers</c> table
    /// or an <c>is_removed</c> flag, plus a rung. But the fact it wanted is already recorded:
    /// <c>collect.servers</c> holds a row per server the service has successfully connected to, the Viewer's
    /// Remove deletes only from <c>config_monitored_servers</c>, and nothing purges the observed registry. So
    /// "was this ever really monitored" is answerable today, for free, without a schema change and without a
    /// second piece of state that could disagree with the first.</para>
    ///
    /// <para>An <c>is_removed</c> flag was the option worth rejecting explicitly: <c>is_enabled = FALSE</c>
    /// already means "registered but paused", so a second flag on the same row makes
    /// <c>(is_enabled, is_removed)</c> a four-state space where two combinations are meaningless, and every
    /// existing reader of that table would have to learn the new flag or silently start including removed
    /// servers. That is the same seam failure that #2280 had to fix in the dedupe gate — a widened concept that
    /// old call sites never learned about.</para>
    ///
    /// <para><b>The limits, stated because they bound what the log may claim.</b> A server registered but never
    /// successfully connected to has no observed row, so it reports as never-monitored — which is the right
    /// answer to the operator's actual question ("is this being monitored?"), even though it is the wrong answer
    /// to "was it ever registered?". And a store rebuilt from scratch has no observed rows at all, so everything
    /// reads as never-monitored until it connects once; that degrades to a warning rather than to silence, which
    /// is the safe direction for a fresh store where the file genuinely is the intent.</para>
    ///
    /// <para>Matched on either name for the reason <see cref="ServersOnlyInFile"/> gives: the observed registry
    /// carries both the storage name and the display name, and identity drift predating #2158 means the id is
    /// the less reliable key of the three. A miss here warns rather than going quiet, so the failure direction
    /// is the harmless one.</para>
    /// </summary>
    internal static (IReadOnlyList<string> NeverRegistered, IReadOnlyList<string> DeliberatelyRemoved)
        SplitByEverMonitored(IEnumerable<string> fileOnlyNames, ISet<string> everMonitoredNames)
    {
        var never = new List<string>();
        var removed = new List<string>();

        foreach (var name in fileOnlyNames ?? Enumerable.Empty<string>())
        {
            if (everMonitoredNames is not null && everMonitoredNames.Contains(name))
            {
                removed.Add(name);
            }
            else
            {
                never.Add(name);
            }
        }

        return (never, removed);
    }

    /// <summary>
    /// The file entries whose <c>server_id</c> is absent from the store, by display name. Pure so the
    /// comparison is testable without a store — the log line above is the only part that needs one.
    /// </summary>
    internal static IReadOnlyList<string> ServersOnlyInFile(
        IEnumerable<MonitoredServer> fileServers, ISet<int> storeServerIds, ISet<string>? storeNames = null)
    {
        var missing = new List<string>();
        if (fileServers is null)
        {
            return missing;
        }

        foreach (var server in fileServers)
        {
            /* A file entry has no StoredServerId, so ServerId here IS the derivation: "would the id this file
               entry describes be in the store". That was the whole test until #2158 made an edit preserve its
               identity — a re-addressed server keeps its own id so its history stays attached, which means the
               file's derived id no longer matches it and the id arm alone now reports a monitored server as
               absent. The NAME arm answers the question the log actually asks, "is this file entry represented
               in the store at all", and it is the operator-facing key: the display name is what they typed and
               what the Viewer shows, and an edit does not change it.

               Deliberately either-or rather than name-only. Two different file entries can share a display
               name (nothing enforces uniqueness), so name-only would hide a genuinely unmonitored server
               behind a same-named sibling; and the id arm still resolves the common case exactly. */
            if (storeServerIds.Contains(server.ServerId))
            {
                continue;
            }

            if (storeNames is not null && storeNames.Contains(server.DisplayName))
            {
                continue;
            }

            missing.Add(server.DisplayName);
        }

        return missing;
    }

    private static async Task<long> CountAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        /* table is a compile-time constant name, never user input — interpolation is safe. */
        using var command = new NpgsqlCommand($"SELECT COUNT(*) FROM config.{table}", connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task SeedServiceRowAsync(NpgsqlConnection connection, DarlingConfig config, DateTime now, CancellationToken ct)
    {
        /* config_version starts at 0; the four desired-state seed writes below bump it via the trigger,
           so the worker's post-seed baseline read reflects the seeded state and triggers no spurious reload. */
        using var command = new NpgsqlCommand(@"
INSERT INTO config_service (id, paused, capture_plans, query_store_backfill_enabled, query_store_text_budget_mb, max_concurrent_sweeps, plan_xml_compression, mcp_enabled, mcp_port, web_enabled, web_port, plan_content_retention_days, config_version, updated_at, updated_by)
VALUES (1, FALSE, $1, $7, $8, $9, $10, $2, $3, $4, $5, $11, 0, $6, 'seed')
ON CONFLICT (id) DO NOTHING", connection);
        command.Parameters.AddWithValue(config.CapturePlans);
        command.Parameters.AddWithValue(config.Mcp.Enabled);
        command.Parameters.AddWithValue(config.Mcp.Port);
        command.Parameters.AddWithValue(config.Web.Enabled);
        command.Parameters.AddWithValue(config.Web.Port);
        command.Parameters.AddWithValue(now);
        command.Parameters.AddWithValue(config.QueryStoreBackfillEnabled);
        command.Parameters.AddWithValue(config.QueryStoreTextBudgetMb);
        command.Parameters.AddWithValue(config.MaxConcurrentSweeps);
        /* Normalized at the WRITE too, not just the read: the V62 CHECK is case-sensitive by design
           (it mirrors this normalizer's output), so seeding the raw file value would turn
           "planXmlCompression": "GZIP" in darling.json into a CHECK violation during store bring-up —
           the seed is the last step of first contact, and a cosmetic casing choice must not fail it. */
        command.Parameters.AddWithValue(NormalizePlanXmlCompression(config.PlanXmlCompression));
        command.Parameters.AddWithValue(ClampPlanContentRetentionDays(config.PlanContentRetentionDays));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task SeedAlertSettingsAsync(NpgsqlConnection connection, DarlingConfig config, DateTime now, CancellationToken ct)
    {
        var a = config.Alerts;
        var an = config.Analysis;
        using var command = new NpgsqlCommand(@"
INSERT INTO config_alert_settings (
    id, enabled, cpu_enabled, cpu_threshold_percent, cpu_mode, blocking_enabled, blocking_count_threshold,
    deadlock_enabled, deadlock_count_threshold, poison_wait_enabled, poison_wait_threshold_ms,
    long_running_query_enabled, long_running_query_threshold_minutes, tempdb_space_enabled,
    tempdb_space_threshold_percent, low_disk_enabled, low_disk_threshold_percent, low_disk_threshold_gb,
    long_running_job_enabled, long_running_job_multiplier, failed_job_enabled, failed_job_lookback_minutes,
    cooldown_minutes, excluded_databases, analysis_enabled, analysis_interval_minutes,
    analysis_notifications_enabled, analysis_notify_severity, delivery_mode, per_event_max,
    long_running_query_max_results, long_running_query_exclude_sp_server_diagnostics,
    long_running_query_exclude_wait_for, long_running_query_exclude_backups,
    long_running_query_exclude_misc_waits, long_running_query_exclude_cdc, notify_connection_changes,
    notify_connection_down_at_startup, connection_refire_minutes,
    notify_ag_health, ag_lag_alert_seconds, ag_redo_queue_alert_kb,
    ag_disconnect_refire_minutes, blocking_wait_seconds_threshold, pvs_enabled, pvs_threshold_percent,
    pvs_floor_gb, modified_at, database_state_enabled,
    self_disk_free_warn_percent, collection_stale_minutes, collection_failure_threshold,
    disk_critical_free_percent, disk_critical_free_gb, analysis_notify_cooldown_minutes,
    store_job_cadence_warn_percent)
VALUES (1, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21,
        $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42,
        $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55)
ON CONFLICT (id) DO NOTHING", connection);
        command.Parameters.AddWithValue(a.Enabled);
        command.Parameters.AddWithValue(a.CpuEnabled);
        command.Parameters.AddWithValue(a.CpuThresholdPercent);
        command.Parameters.AddWithValue(a.CpuMode);
        command.Parameters.AddWithValue(a.BlockingEnabled);
        command.Parameters.AddWithValue(a.BlockingCountThreshold);
        command.Parameters.AddWithValue(a.DeadlockEnabled);
        command.Parameters.AddWithValue(a.DeadlockCountThreshold);
        command.Parameters.AddWithValue(a.PoisonWaitEnabled);
        command.Parameters.AddWithValue(a.PoisonWaitThresholdMs);
        command.Parameters.AddWithValue(a.LongRunningQueryEnabled);
        command.Parameters.AddWithValue(a.LongRunningQueryThresholdMinutes);
        command.Parameters.AddWithValue(a.TempDbSpaceEnabled);
        command.Parameters.AddWithValue(a.TempDbSpaceThresholdPercent);
        command.Parameters.AddWithValue(a.LowDiskEnabled);
        command.Parameters.AddWithValue(a.LowDiskThresholdPercent);
        command.Parameters.AddWithValue(a.LowDiskThresholdGb);
        command.Parameters.AddWithValue(a.LongRunningJobEnabled);
        command.Parameters.AddWithValue(a.LongRunningJobMultiplier);
        command.Parameters.AddWithValue(a.FailedJobEnabled);
        command.Parameters.AddWithValue(a.FailedJobLookbackMinutes);
        command.Parameters.AddWithValue(a.CooldownMinutes);
        AddTextArray(command, a.ExcludedDatabases);
        command.Parameters.AddWithValue(an.Enabled);
        command.Parameters.AddWithValue(an.IntervalMinutes);
        command.Parameters.AddWithValue(an.NotificationsEnabled);
        command.Parameters.AddWithValue(an.NotifySeverity);
        /* #1141 delivery mode: the enum name ("Summary"/"PerEvent") into the text column; the read parses it back. */
        command.Parameters.AddWithValue(a.DeliveryMode.ToString());
        command.Parameters.AddWithValue(a.PerEventMax);
        /* V20 long-running-query read shape (max results + the five noise-filter opt-outs). */
        command.Parameters.AddWithValue(a.LongRunningQueryMaxResults);
        command.Parameters.AddWithValue(a.LongRunningQueryExcludeSpServerDiagnostics);
        command.Parameters.AddWithValue(a.LongRunningQueryExcludeWaitFor);
        command.Parameters.AddWithValue(a.LongRunningQueryExcludeBackups);
        command.Parameters.AddWithValue(a.LongRunningQueryExcludeMiscWaits);
        command.Parameters.AddWithValue(a.LongRunningQueryExcludeCdc);
        /* V20 connection-change notify gate. */
        command.Parameters.AddWithValue(a.NotifyConnectionChanges);
        /* V33 #1659 opt-ins: already-down-at-first-sight + standing-outage re-fire. */
        command.Parameters.AddWithValue(a.NotifyConnectionDownAtStartup);
        command.Parameters.AddWithValue(a.ConnectionRefireMinutes);
        /* V35 #991 Availability Group knobs: master switch + the two sync-behind triggers. */
        command.Parameters.AddWithValue(a.NotifyAgHealth);
        command.Parameters.AddWithValue(a.AgLagAlertSeconds);
        command.Parameters.AddWithValue(a.AgRedoQueueAlertKb);
        /* V37 #1696: AG disconnect re-fire. */
        command.Parameters.AddWithValue(a.AgDisconnectRefireMinutes);
        /* V40 #1839: total-blocked-wait gate (0 = off). */
        command.Parameters.AddWithValue(a.BlockingWaitSecondsThreshold);
        /* V48 #1984: PVS-pressure alert (enable + percent trigger + GB floor). */
        command.Parameters.AddWithValue(a.PvsEnabled);
        command.Parameters.AddWithValue(a.PvsThresholdPercent);
        command.Parameters.AddWithValue(a.PvsFloorGb);
        command.Parameters.AddWithValue(now);
        /* V49 database-state alert master switch (appended last, matching the ALTER's physical order). */
        command.Parameters.AddWithValue(a.DatabaseStateEnabled);
        /* V55 #2107: the previously-hardcoded threshold knobs, appended in the ALTER's order. */
        command.Parameters.AddWithValue(a.SelfDiskFreeWarnPercent);
        command.Parameters.AddWithValue(a.CollectionStaleMinutes);
        command.Parameters.AddWithValue(a.CollectionFailureThreshold);
        command.Parameters.AddWithValue(a.DiskCriticalFreePercent);
        command.Parameters.AddWithValue(a.DiskCriticalFreeGb);
        command.Parameters.AddWithValue(a.AnalysisNotifyCooldownMinutes);
        command.Parameters.AddWithValue(a.StoreJobCadenceWarnPercent);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task SeedNotificationAsync(NpgsqlConnection connection, DarlingConfig config, DateTime now, CancellationToken ct)
    {
        var s = config.Smtp;
        var w = config.Webhooks;
        using var command = new NpgsqlCommand(@"
INSERT INTO config_notification (
    id, smtp_host, smtp_port, smtp_use_ssl, smtp_username, smtp_encrypted_password, smtp_from_address,
    smtp_recipients, email_cooldown_minutes, teams_url, teams_proxy, slack_url, slack_proxy, modified_at)
VALUES (1, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT (id) DO NOTHING", connection);
        command.Parameters.AddWithValue(s.Host);
        command.Parameters.AddWithValue(s.Port);
        command.Parameters.AddWithValue(s.UseSsl);
        AddNullableText(command, s.Username);
        AddNullableText(command, s.EncryptedPassword);
        command.Parameters.AddWithValue(s.From);
        command.Parameters.AddWithValue(s.To);
        command.Parameters.AddWithValue(s.EmailCooldownMinutes);
        command.Parameters.AddWithValue(w.TeamsUrl);
        command.Parameters.AddWithValue(w.TeamsProxy);
        command.Parameters.AddWithValue(w.SlackUrl);
        command.Parameters.AddWithValue(w.SlackProxy);
        command.Parameters.AddWithValue(now);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task SeedMonitoredServersAsync(NpgsqlConnection connection, DarlingConfig config, DateTime now, CancellationToken ct)
    {
        /* Guarded by the caller's COUNT == 0 check — only reached when the registry is empty, so a later
           Viewer deletion (Stage 3) is never resurrected by a re-seed. */
        foreach (var server in config.Servers)
        {
            using var command = new NpgsqlCommand(@"
INSERT INTO config_monitored_servers (
    server_id, name, host, database, auth, username, encrypted_password, encrypt_mode,
    trust_server_certificate, read_only_intent, multi_subnet_failover, excluded_databases,
    monthly_cost_usd, capture_plans, alert_delivery_mode_override, engine, port, is_enabled, created_at, modified_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NULL, $14, $16, $17, TRUE, $15, $15)
ON CONFLICT (server_id) DO NOTHING", connection);
            /* THE ALLOCATION SITE. A darling.json entry has no StoredServerId, so this is the derivation —
               and this is where it is minted and made permanent. When new rows stop being hash-keyed
               (#2218), this is the write that changes; every READ already goes through the stored value. */
            command.Parameters.AddWithValue(server.ServerId);
            command.Parameters.AddWithValue(server.DisplayName);
            command.Parameters.AddWithValue(server.Host);
            AddNullableText(command, server.Database);
            command.Parameters.AddWithValue(server.Auth);
            AddNullableText(command, server.Username);
            /* Only the DPAPI blob is ever stored; a plaintext dev password stays in darling.json and is
               backfilled at read time (BuildServerFromRow's bootstrap merge). */
            AddNullableText(command, server.EncryptedPassword);
            command.Parameters.AddWithValue(server.EncryptMode);
            command.Parameters.AddWithValue(server.TrustServerCertificate);
            command.Parameters.AddWithValue(server.ReadOnlyIntent);
            command.Parameters.AddWithValue(server.MultiSubnetFailover);
            AddTextArray(command, server.ExcludedDatabases);
            command.Parameters.AddWithValue(server.MonthlyCostUsd);
            /* Per-server delivery override (#1236): the enum name or NULL = "inherit the global". */
            AddNullableText(command, server.AlertDeliveryModeOverride?.ToString());
            command.Parameters.AddWithValue(now);
            /* V68: the engine, persisted as the raw darling.json string rather than the parsed enum, so the
               store round-trips exactly what the operator wrote — including an alias like "aurora" — and the
               single parse in MonitoredServer.TargetEngine stays the only place that interprets it. */
            command.Parameters.AddWithValue(server.Engine);
            /* V68: the port, PostgreSQL-only (0 = the driver's default). Persisted for the same reason as the
               engine — a non-default port dropped here would connect to 5432 and fail with an error naming
               the right host. */
            command.Parameters.AddWithValue(server.Port);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    /* ---------------- read (store -> in-memory view) ---------------- */

    /// <summary>
    /// Reads every <c>config.*</c> table into an in-memory <see cref="StoreConfigView"/> the worker applies.
    /// The bootstrap <paramref name="bootstrap"/> config supplies the plaintext-dev-password backfill for
    /// SQL-auth servers whose store row carries no DPAPI blob (never persisted; matched by <c>server_id</c>).
    /// Returns null when the store is unreachable, so the caller keeps the current live config.
    /// </summary>
    public async Task<StoreConfigView?> LoadViewAsync(
        DarlingConfig bootstrap, CancellationToken cancellationToken)
    {
        if (bootstrap is null)
        {
            throw new ArgumentNullException(nameof(bootstrap));
        }

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);

            var (paused, capturePlans, backfillEnabled, textBudgetMb, maxSweeps, planXmlCompression, mcpEnabled, mcpPort, webEnabled, webPort, planContentRetentionDays, configVersion) = await ReadServiceRowAsync(connection, cancellationToken);
            var (alerts, analysis) = await ReadAlertSettingsAsync(connection, cancellationToken);

            /* The notification row is the ONLY read here that touches secret columns — the SMTP password and
               username, and the Teams/Slack/generic/PagerDuty bearer URLs. DarlingManagedRoles deliberately
               revokes table-wide SELECT on config_notification from BOTH viewer and mcp and re-grants only
               the non-secret columns, so a caller connecting as one of those roles and asking for the full
               row gets 42501 for the whole table — and because every section here shares one try/catch, one
               denied column would cost the WHOLE view (that is how #2293 lost the MCP host its registry).
               That is precisely why no restricted-role caller reads this view any more: the MCP host used to
               (skipping this row via an includeNotification parameter, #2293) until #2298 removed its config
               read entirely — the worker publishes the server registry to it instead. Every remaining caller
               is the worker or a test on the privileged connection, so the row is read unconditionally and
               the skip parameter is gone with its last caller. */
            var (smtp, webhooks) = await ReadNotificationAsync(connection, cancellationToken);

            var servers = await ReadMonitoredServersAsync(connection, bootstrap, cancellationToken);
            var schedules = await ReadScheduleOverridesAsync(connection, cancellationToken);

            return new StoreConfigView
            {
                ConfigVersion = configVersion,
                Paused = paused,
                CapturePlans = capturePlans,
                QueryStoreBackfillEnabled = backfillEnabled,
                QueryStoreTextBudgetMb = textBudgetMb,
                PlanContentRetentionDays = planContentRetentionDays,
                MaxConcurrentSweeps = maxSweeps,
                PlanXmlCompression = planXmlCompression,
                McpEnabled = mcpEnabled,
                McpPort = mcpPort,
                WebEnabled = webEnabled,
                WebPort = webPort,
                Alerts = alerts,
                Analysis = analysis,
                Smtp = smtp,
                Webhooks = webhooks,
                EnabledServers = servers,
                ScheduleOverrides = schedules,
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogWarning("Could not read the config store — keeping the current live config: {Message}", ex.Message);
            return null;
        }
    }

    /// <summary>The V59 collector-memory knob clamps (#2164/#2170) — a bad stored value degrades to a sane
    /// one rather than failing the config load, matching the alert knobs' posture.</summary>
    internal const int MinTextBudgetMb = 4;
    internal const int MaxTextBudgetMb = 256;
    internal const int MinConcurrentSweeps = 1;
    internal const int MaxConcurrentSweepsLimit = 16;

    internal static int ClampTextBudgetMb(int value) => Math.Clamp(value, MinTextBudgetMb, MaxTextBudgetMb);

    internal static int ClampConcurrentSweeps(int value) => Math.Clamp(value, MinConcurrentSweeps, MaxConcurrentSweepsLimit);

    /// <summary>The V75 plan-content horizon clamps (#2316) — 0 (and any negative) means DISABLED
    /// (the fact-coupled dimension horizon stands alone); an enabled value clamps to [7,365], because a
    /// sub-week horizon would age plan XML out from under the viewer's default history windows.</summary>
    internal const int MinPlanContentRetentionDays = 7;
    internal const int MaxPlanContentRetentionDays = 365;

    internal static int ClampPlanContentRetentionDays(int value) =>
        value <= 0 ? 0 : Math.Clamp(value, MinPlanContentRetentionDays, MaxPlanContentRetentionDays);

    /// <summary>#2171: unknown values normalize to 'gzip' (fail to the shipped default) so a hand-edited
    /// row cannot switch the writer into an undefined mode; the V62 CHECK constraint enforces the same
    /// set DB-side, and this guard covers pre-constraint rows and direct writes with the constraint
    /// dropped.</summary>
    internal static string NormalizePlanXmlCompression(string? value) =>
        string.Equals(value?.Trim(), "none", StringComparison.OrdinalIgnoreCase) ? "none" : "gzip";

    private static async Task<(bool Paused, bool CapturePlans, bool QueryStoreBackfillEnabled, int QueryStoreTextBudgetMb, int MaxConcurrentSweeps, string PlanXmlCompression, bool McpEnabled, int McpPort, bool WebEnabled, int WebPort, int PlanContentRetentionDays, long ConfigVersion)>
        ReadServiceRowAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "SELECT paused, capture_plans, query_store_backfill_enabled, query_store_text_budget_mb, max_concurrent_sweeps, plan_xml_compression, mcp_enabled, mcp_port, web_enabled, web_port, plan_content_retention_days, config_version FROM config_service WHERE id = 1", connection);
        using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            /* Row missing (unseeded) — treat as defaults; capture and backfill stay on, the memory
               knobs reproduce the pre-V59 compile-time constants (64 MB budget, 4-wide sweep), and
               plan content keeps the V75 default 21-day horizon. */
            return (false, true, true, 64, 4, "gzip", false, 5152, false, 5153, 21, 0);
        }

        return (reader.GetBoolean(0), reader.GetBoolean(1), reader.GetBoolean(2),
            ClampTextBudgetMb(reader.GetInt32(3)), ClampConcurrentSweeps(reader.GetInt32(4)),
            NormalizePlanXmlCompression(reader.GetString(5)),
            reader.GetBoolean(6), reader.GetInt32(7),
            reader.GetBoolean(8), reader.GetInt32(9),
            ClampPlanContentRetentionDays(reader.GetInt32(10)), reader.GetInt64(11));
    }

    private static async Task<(AlertsConfig Alerts, AnalysisConfig Analysis)> ReadAlertSettingsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
SELECT enabled, cpu_enabled, cpu_threshold_percent, cpu_mode, blocking_enabled, blocking_count_threshold,
       deadlock_enabled, deadlock_count_threshold, poison_wait_enabled, poison_wait_threshold_ms,
       long_running_query_enabled, long_running_query_threshold_minutes, tempdb_space_enabled,
       tempdb_space_threshold_percent, low_disk_enabled, low_disk_threshold_percent, low_disk_threshold_gb,
       long_running_job_enabled, long_running_job_multiplier, failed_job_enabled, failed_job_lookback_minutes,
       cooldown_minutes, excluded_databases, analysis_enabled, analysis_interval_minutes,
       analysis_notifications_enabled, analysis_notify_severity, delivery_mode, per_event_max,
       long_running_query_max_results, long_running_query_exclude_sp_server_diagnostics,
       long_running_query_exclude_wait_for, long_running_query_exclude_backups,
       long_running_query_exclude_misc_waits, long_running_query_exclude_cdc, notify_connection_changes,
       notify_connection_down_at_startup, connection_refire_minutes,
       notify_ag_health, ag_lag_alert_seconds, ag_redo_queue_alert_kb,
       ag_disconnect_refire_minutes, blocking_wait_seconds_threshold, pvs_enabled, pvs_threshold_percent,
       pvs_floor_gb, database_state_enabled,
       self_disk_free_warn_percent, collection_stale_minutes, collection_failure_threshold,
       disk_critical_free_percent, disk_critical_free_gb, analysis_notify_cooldown_minutes,
       store_job_cadence_warn_percent
FROM config_alert_settings WHERE id = 1", connection);
        using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return (new AlertsConfig(), new AnalysisConfig());
        }

        var alerts = new AlertsConfig
        {
            Enabled = reader.GetBoolean(0),
            CpuEnabled = reader.GetBoolean(1),
            CpuThresholdPercent = reader.GetInt32(2),
            CpuMode = reader.GetString(3),
            BlockingEnabled = reader.GetBoolean(4),
            BlockingCountThreshold = reader.GetInt32(5),
            DeadlockEnabled = reader.GetBoolean(6),
            DeadlockCountThreshold = reader.GetInt32(7),
            PoisonWaitEnabled = reader.GetBoolean(8),
            PoisonWaitThresholdMs = reader.GetInt32(9),
            LongRunningQueryEnabled = reader.GetBoolean(10),
            LongRunningQueryThresholdMinutes = reader.GetInt32(11),
            TempDbSpaceEnabled = reader.GetBoolean(12),
            TempDbSpaceThresholdPercent = reader.GetInt32(13),
            LowDiskEnabled = reader.GetBoolean(14),
            LowDiskThresholdPercent = reader.GetInt32(15),
            LowDiskThresholdGb = reader.GetInt32(16),
            LongRunningJobEnabled = reader.GetBoolean(17),
            LongRunningJobMultiplier = reader.GetInt32(18),
            FailedJobEnabled = reader.GetBoolean(19),
            FailedJobLookbackMinutes = reader.GetInt32(20),
            CooldownMinutes = reader.GetInt32(21),
            ExcludedDatabases = ReadTextArray(reader, 22),
            /* delivery_mode/per_event_max appended (V18) so ordinals 0–26 stay pinned; a store row from before
               V18 can't reach here (the column is NOT NULL DEFAULT), but ParseDeliveryMode fails safe to Summary. */
            DeliveryMode = ParseDeliveryMode(reader.IsDBNull(27) ? null : reader.GetString(27)),
            PerEventMax = reader.GetInt32(28),
            /* long-running-query read shape appended (V20) at ordinals 29–34; NOT NULL DEFAULT so a pre-V20
               store row can't reach here without the columns present. */
            LongRunningQueryMaxResults = reader.GetInt32(29),
            LongRunningQueryExcludeSpServerDiagnostics = reader.GetBoolean(30),
            LongRunningQueryExcludeWaitFor = reader.GetBoolean(31),
            LongRunningQueryExcludeBackups = reader.GetBoolean(32),
            LongRunningQueryExcludeMiscWaits = reader.GetBoolean(33),
            LongRunningQueryExcludeCdc = reader.GetBoolean(34),
            /* connection-change notify gate appended (V20) at ordinal 35. */
            NotifyConnectionChanges = reader.GetBoolean(35),
            /* #1659 opt-ins appended (V33) at ordinals 36–37; NOT NULL DEFAULT so a pre-V33 row can't
               reach here without the columns present. */
            NotifyConnectionDownAtStartup = reader.GetBoolean(36),
            ConnectionRefireMinutes = reader.GetInt32(37),
            /* #991 AG knobs appended (V35) at ordinals 38–40; NOT NULL DEFAULT so a pre-V35 row can't reach
               here without the columns present. */
            NotifyAgHealth = reader.GetBoolean(38),
            AgLagAlertSeconds = reader.GetInt32(39),
            AgRedoQueueAlertKb = reader.GetInt64(40),
            /* #1696 AG disconnect re-fire appended (V37) at ordinal 41. */
            AgDisconnectRefireMinutes = reader.GetInt32(41),
            /* #1839 total-blocked-wait gate appended (V40) at ordinal 42. This read is what makes the
               setting REACHABLE at all: ApplyToConfig replaces config.Alerts wholesale with what the
               store returned, so a column missing here would reset the knob to 0 on every worker start
               and the alert could never fire, whatever darling.json said. */
            BlockingWaitSecondsThreshold = reader.GetInt32(42),
            /* #1984 PVS-pressure knobs appended (V48) at ordinals 43–45; NOT NULL DEFAULT so a pre-V48
               row can't reach here without the columns present. Same reachability rule as V40's note:
               ApplyToConfig replaces config.Alerts wholesale, so a column missing here would silently
               reset the knob on every worker start. */
            PvsEnabled = reader.GetBoolean(43),
            PvsThresholdPercent = reader.GetInt32(44),
            PvsFloorGb = reader.GetInt32(45),
            /* database-state alert master switch appended (V49) at ordinal 46; NOT NULL DEFAULT true so a
               pre-V49 row can't reach here without the column present. */
            DatabaseStateEnabled = reader.GetBoolean(46),
            /* #2107 threshold knobs appended (V55) at ordinals 47–52; NOT NULL DEFAULTs are the
               constants they replace, so a pre-V55 row can't reach here without the columns present
               and the wholesale ApplyToConfig replacement never resets a knob. */
            SelfDiskFreeWarnPercent = reader.GetInt32(47),
            CollectionStaleMinutes = reader.GetInt32(48),
            CollectionFailureThreshold = reader.GetInt32(49),
            DiskCriticalFreePercent = reader.GetInt32(50),
            DiskCriticalFreeGb = reader.GetInt32(51),
            AnalysisNotifyCooldownMinutes = reader.GetInt32(52),
            /* #2136 cadence-warn knob appended (V57) at ordinal 53; NOT NULL DEFAULT 25, and the same
               reachability rule as every appended knob above: ApplyToConfig replaces config.Alerts
               wholesale, so a column missing here would silently reset the knob on every worker start. */
            StoreJobCadenceWarnPercent = reader.GetInt32(53),
        };
        var analysis = new AnalysisConfig
        {
            Enabled = reader.GetBoolean(23),
            IntervalMinutes = reader.GetInt32(24),
            NotificationsEnabled = reader.GetBoolean(25),
            NotifySeverity = reader.GetDouble(26),
        };
        return (alerts, analysis);
    }

    private static async Task<(SmtpConfig Smtp, WebhooksConfig Webhooks)> ReadNotificationAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
SELECT smtp_host, smtp_port, smtp_use_ssl, smtp_username, smtp_encrypted_password, smtp_from_address,
       smtp_recipients, email_cooldown_minutes, teams_url, teams_proxy, slack_url, slack_proxy,
       generic_url, generic_headers, generic_body_template, generic_proxy,
       pagerduty_routing_key, pagerduty_use_eu_region, pagerduty_proxy
FROM config_notification WHERE id = 1", connection);
        using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return (new SmtpConfig(), new WebhooksConfig());
        }

        var smtp = new SmtpConfig
        {
            Host = reader.GetString(0),
            Port = reader.GetInt32(1),
            UseSsl = reader.GetBoolean(2),
            Username = reader.IsDBNull(3) ? null : reader.GetString(3),
            EncryptedPassword = reader.IsDBNull(4) ? null : reader.GetString(4),
            From = reader.GetString(5),
            To = reader.GetString(6),
            EmailCooldownMinutes = reader.GetInt32(7),
        };
        var webhooks = new WebhooksConfig
        {
            TeamsUrl = reader.GetString(8),
            TeamsProxy = reader.GetString(9),
            SlackUrl = reader.GetString(10),
            SlackProxy = reader.GetString(11),
            GenericUrl = reader.GetString(12),
            GenericHeaders = reader.GetString(13),
            GenericBodyTemplate = reader.GetString(14),
            GenericProxy = reader.GetString(15),
            PagerDutyRoutingKey = reader.GetString(16),
            PagerDutyUseEuRegion = reader.GetBoolean(17),
            PagerDutyProxy = reader.GetString(18),
        };
        return (smtp, webhooks);
    }

    private static async Task<IReadOnlyList<MonitoredServer>> ReadMonitoredServersAsync(
        NpgsqlConnection connection, DarlingConfig bootstrap, CancellationToken ct)
    {
        var servers = new List<MonitoredServer>();
        /* server_id is LAST rather than first (#2218): every ordinal in BuildServerFromRow is positional, so
           appending is the only addition that cannot silently re-map an existing column onto the wrong
           property. It was absent entirely before this — the registry's own PRIMARY KEY was read past, and
           twelve downstream sites re-derived it from the mutable columns instead. */
        using var command = new NpgsqlCommand(@"
SELECT name, host, database, auth, username, encrypted_password, encrypt_mode, trust_server_certificate,
       read_only_intent, multi_subnet_failover, excluded_databases, monthly_cost_usd, alert_delivery_mode_override,
       engine, port, server_id
FROM config_monitored_servers WHERE is_enabled = TRUE
ORDER BY name", connection);
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            servers.Add(BuildServerFromRow(reader, bootstrap));
        }

        return servers;
    }

    /// <summary>
    /// Reconstructs a <see cref="MonitoredServer"/> from a store row, backfilling the SQL-auth secret from
    /// the in-memory bootstrap config when the store row carries no DPAPI blob — this is how a darling.json
    /// plaintext dev password (never stored) still drives the connect path. The bootstrap match is on the
    /// FULL identity (exact storage name + auth kind + username), NOT the derived <c>server_id</c> alone:
    /// <see cref="ServerIdHelper.GetDeterministicHashCode"/> is a 32-bit hash whose collisions are craftable,
    /// so matching on the id alone would let a config-write principal cross-wire another server's plaintext
    /// secret onto an attacker-chosen host. The secret is copied only when EXACTLY ONE bootstrap server
    /// matches the full identity.
    /// </summary>
    private static MonitoredServer BuildServerFromRow(NpgsqlDataReader reader, DarlingConfig bootstrap)
    {
        var server = new MonitoredServer
        {
            Name = reader.GetString(0),
            Host = reader.GetString(1),
            Database = reader.IsDBNull(2) ? null : reader.GetString(2),
            Auth = reader.GetString(3),
            Username = reader.IsDBNull(4) ? null : reader.GetString(4),
            EncryptedPassword = reader.IsDBNull(5) ? null : reader.GetString(5),
            EncryptMode = reader.GetString(6),
            TrustServerCertificate = reader.GetBoolean(7),
            ReadOnlyIntent = reader.GetBoolean(8),
            MultiSubnetFailover = reader.GetBoolean(9),
            ExcludedDatabases = ReadTextArray(reader, 10),
            MonthlyCostUsd = reader.GetDecimal(11),
            /* #1236: the per-server delivery override (null = inherit the global), available at delivery time. */
            AlertDeliveryModeOverride = ParseDeliveryOverride(reader.IsDBNull(12) ? null : reader.GetString(12)),
            /* V68. Without this the registry — which is authoritative once seeded — silently downgraded every
               PostgreSQL target to the "sqlserver" property default, and the service opened a SqlConnection to
               port 5432. NOT NULL DEFAULT in both columns means the DBNull guards are belt-and-braces for a
               store mid-migration, not an expected path. */
            Engine = reader.IsDBNull(13) ? "sqlserver" : reader.GetString(13),
            Port = reader.IsDBNull(14) ? 0 : reader.GetInt32(14),
            /* #2218: the row's OWN primary key, which this read used to discard. NOT NULL in the table, so
               the DBNull guard is for a store mid-migration rather than an expected path — and a null there
               falls back to the derivation, which is exactly what it did before this column was read at all. */
            StoredServerId = reader.IsDBNull(15) ? null : reader.GetInt32(15),
        };

        if (server.UsesSqlAuth && string.IsNullOrWhiteSpace(server.EncryptedPassword))
        {
            var matches = bootstrap.Servers.Where(s =>
                s.UsesSqlAuth
                && string.Equals(s.StorageName, server.StorageName, StringComparison.OrdinalIgnoreCase)
                && string.Equals(s.Username, server.Username, StringComparison.Ordinal)).ToList();

            if (matches.Count == 1)
            {
                server.EncryptedPassword = matches[0].EncryptedPassword;
                server.Password = matches[0].Password;
            }
        }

        return server;
    }

    private static async Task<IReadOnlyList<ScheduleOverride>> ReadScheduleOverridesAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var overrides = new List<ScheduleOverride>();
        using var command = new NpgsqlCommand(
            "SELECT server_id, collector_name, frequency_minutes, retention_days, enabled FROM config_collector_schedules", connection);
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            overrides.Add(new ScheduleOverride(
                reader.IsDBNull(0) ? null : reader.GetInt32(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetInt32(2),
                reader.IsDBNull(3) ? null : reader.GetInt32(3),
                reader.GetBoolean(4)));
        }

        return overrides;
    }

    /* ---------------- apply (view -> held config, in place) ---------------- */

    /// <summary>
    /// Swaps the held <see cref="DarlingConfig"/>'s <c>.Alerts/.Analysis/.Smtp/.Webhooks/.CapturePlans/.Mcp/.Web</c>
    /// to the store view IN PLACE — the by-reference <see cref="DarlingAlertSettings"/> seam and the runner's
    /// capture-plans provider read the new values on their next use, no reconstruction needed. Pure (no I/O),
    /// so it is unit-testable without a live store.
    /// </summary>
    public static void ApplyToConfig(DarlingConfig config, StoreConfigView view)
    {
        if (config is null)
        {
            throw new ArgumentNullException(nameof(config));
        }

        if (view is null)
        {
            throw new ArgumentNullException(nameof(view));
        }

        config.Alerts = view.Alerts;
        config.Analysis = view.Analysis;
        config.Smtp = view.Smtp;
        config.Webhooks = view.Webhooks;
        config.CapturePlans = view.CapturePlans;
        config.QueryStoreBackfillEnabled = view.QueryStoreBackfillEnabled;
        config.QueryStoreTextBudgetMb = view.QueryStoreTextBudgetMb;
        config.PlanContentRetentionDays = view.PlanContentRetentionDays;
        config.MaxConcurrentSweeps = view.MaxConcurrentSweeps;
        config.PlanXmlCompression = view.PlanXmlCompression;
        config.Mcp.Enabled = view.McpEnabled;
        config.Mcp.Port = view.McpPort;
        config.Web.Enabled = view.WebEnabled;
        config.Web.Port = view.WebPort;
    }

    /* ---------------- schedule resolution (pure) ---------------- */

    /// <summary>
    /// The effective schedule for one collector on one server: a per-server override wins over a fleet-wide
    /// override (<c>server_id</c> NULL) wins over the <see cref="CollectorScheduleDefaults"/> code default,
    /// per column (a NULL override column falls through to the next level). Pure — unit-testable without a store.
    /// </summary>
    public static EffectiveSchedule ResolveSchedule(string collectorName, int serverId, IReadOnlyList<ScheduleOverride> overrides)
    {
        var def = CollectorScheduleDefaults.All[collectorName];

        ScheduleOverride? perServer = null;
        ScheduleOverride? fleet = null;
        if (overrides is not null)
        {
            foreach (var o in overrides)
            {
                if (!string.Equals(o.CollectorName, collectorName, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (o.ServerId == serverId)
                {
                    perServer = o;
                }
                else if (o.ServerId is null)
                {
                    fleet = o;
                }
            }
        }

        /* Sanitize operator-supplied overrides before they drive scheduling / a destructive purge: a
           negative frequency or a retention < 1 (0 would invert the purge cutoff and wipe the table)
           is treated as "no override" and falls through to the next level. Defense in depth with the
           V17 CHECK constraints and the DarlingRetention sink clamp. */
        var frequency = ValidFrequency(perServer?.FrequencyMinutes) ?? ValidFrequency(fleet?.FrequencyMinutes) ?? def.FrequencyMinutes;
        var retention = ValidRetention(perServer?.RetentionDays) ?? ValidRetention(fleet?.RetentionDays) ?? def.RetentionDays;
        /* No override row falls back to the collector's shared default enabled state — true for nearly
           every collector, but false for an opt-in one like long_query_completions (#1496). Falling back
           to def.DefaultEnabled (not a bare true) is what makes "reset to defaults" — which DELETES the
           override rows — return a default-off collector to OFF instead of silently re-enabling it. */
        var enabled = perServer?.Enabled ?? fleet?.Enabled ?? def.DefaultEnabled;
        return new EffectiveSchedule(frequency, retention, enabled);
    }

    /// <summary>
    /// The effective FLEET-WIDE retention horizon for a collector (a per-server override can't apply to a
    /// shared-table purge): the fleet override (<c>server_id</c> NULL) <c>retention_days</c> if set, else the
    /// <see cref="CollectorScheduleDefaults"/> default. Pure. Feeds <see cref="DarlingRetention"/>.
    /// </summary>
    public static int ResolveFleetRetentionDays(string collectorName, IReadOnlyList<ScheduleOverride> overrides)
    {
        var def = CollectorScheduleDefaults.All[collectorName];
        if (overrides is not null)
        {
            foreach (var o in overrides)
            {
                if (o.ServerId is null
                    && string.Equals(o.CollectorName, collectorName, StringComparison.OrdinalIgnoreCase)
                    && ValidRetention(o.RetentionDays) is int days)
                {
                    return days;
                }
            }
        }

        return def.RetentionDays;
    }

    /// <summary>A retention override is honored only when &gt;= 1 day; 0/negative would invert the purge
    /// cutoff and delete everything, so it degrades to "no override" (fall through to the default).</summary>
    private static int? ValidRetention(int? days) => days is int v && v >= 1 ? v : null;

    /// <summary>A frequency override is honored only when &gt;= 0 (0 = on-load-only); negative degrades to
    /// "no override" so a bad value can't make a collector run every sweep.</summary>
    private static int? ValidFrequency(int? minutes) => minutes is int v && v >= 0 ? v : null;

    /* ---------------- helpers ---------------- */

    /// <summary>Npgsql rejects Kind=Utc against `timestamp`; store all timestamps naive-UTC.</summary>
    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /// <summary>Parses the <c>delivery_mode</c>/<c>alert_delivery_mode_override</c> text ("Summary"/"PerEvent")
    /// to the enum; an unknown or empty value fails safe to <see cref="AlertNotificationMode.Summary"/>.</summary>
    private static AlertNotificationMode ParseDeliveryMode(string? value) =>
        Enum.TryParse<AlertNotificationMode>(value, ignoreCase: true, out var mode) ? mode : AlertNotificationMode.Summary;

    /// <summary>Parses a nullable per-server delivery override; null/empty = "inherit the global" (returns null).</summary>
    private static AlertNotificationMode? ParseDeliveryOverride(string? value) =>
        string.IsNullOrWhiteSpace(value) ? null : ParseDeliveryMode(value);

    private static void AddNullableText(NpgsqlCommand command, string? value) =>
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)value ?? DBNull.Value });

    private static void AddTextArray(NpgsqlCommand command, IEnumerable<string>? values) =>
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text,
            Value = (values ?? Enumerable.Empty<string>()).ToArray(),
        });

    private static List<string> ReadTextArray(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? new List<string>() : reader.GetFieldValue<string[]>(ordinal).ToList();
}

/// <summary>One sparse <c>config_collector_schedules</c> row — NULL <c>ServerId</c> = fleet-wide.</summary>
public sealed record ScheduleOverride(int? ServerId, string CollectorName, int? FrequencyMinutes, int? RetentionDays, bool Enabled);

/// <summary>The resolved per-collector schedule (override layered on <see cref="CollectorScheduleDefaults"/>).</summary>
public sealed record EffectiveSchedule(int FrequencyMinutes, int RetentionDays, bool Enabled);

/// <summary>
/// The in-memory snapshot of the <c>config.*</c> tables the worker applies on a reload. Sub-configs are
/// fresh instances (not the darling.json ones), so <see cref="StoreConfigProvider.ApplyToConfig"/> can swap
/// them into the held <see cref="DarlingConfig"/> by reference.
/// </summary>
public sealed class StoreConfigView
{
    public long ConfigVersion { get; init; }

    /// <summary>
    /// The service-pause flag. Surfaced from config_service for completeness but NOT enforced in Stage 1 —
    /// gating the collection loop on it is Stage 2 (the command plane), where pause/resume becomes reachable.
    /// Nothing writes it in Stage 1 (no viewer/command path), so there is no operator-facing dormant toggle.
    /// </summary>
    public bool Paused { get; init; }

    public bool CapturePlans { get; init; }

    /// <summary>The #2167 backfill off switch (config_service, V58) — worker reads it live each backfill cycle.</summary>
    public bool QueryStoreBackfillEnabled { get; init; } = true;

    /// <summary>The #2164 per-database query_store text budget in MB (config_service, V59), already clamped.</summary>
    public int QueryStoreTextBudgetMb { get; init; } = 64;

    /// <summary>The V75 plan-content horizon (#2316): days a stored plan XML outlives its last sighting.
    /// 0 = disabled (the fact-coupled dimension horizon stands alone).</summary>
    public int PlanContentRetentionDays { get; init; } = 21;

    /// <summary>
    /// The #2171 plan-XML storage codec (config_service, V62), already normalized to 'gzip' or 'none'.
    /// 'gzip' (the default, unchanged behavior): the plan dim stores gzip bytes in query_plan_gz,
    /// 14.0x measured, and only the apps/MCP can read plans back. 'none': plain text into
    /// query_plan_xml - lz4 TOAST compresses at ~8.9x and any direct-SQL consumer (Grafana, report
    /// tooling) reads the column bare, no extension, no UDF. Existing rows are untouched either way;
    /// the readers' text-first-else-gz resolution covers every mix of eras and modes.
    /// </summary>
    public string PlanXmlCompression { get; init; } = "gzip";

    /// <summary>The #2170 fleet sweep width (config_service, V59), already clamped.</summary>
    public int MaxConcurrentSweeps { get; init; } = 4;

    public bool McpEnabled { get; init; }
    public int McpPort { get; init; }
    public bool WebEnabled { get; init; }
    public int WebPort { get; init; } = 5153;
    public AlertsConfig Alerts { get; init; } = new();
    public AnalysisConfig Analysis { get; init; } = new();
    public SmtpConfig Smtp { get; init; } = new();
    public WebhooksConfig Webhooks { get; init; } = new();
    public IReadOnlyList<MonitoredServer> EnabledServers { get; init; } = Array.Empty<MonitoredServer>();
    public IReadOnlyList<ScheduleOverride> ScheduleOverrides { get; init; } = Array.Empty<ScheduleOverride>();
}
