/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The MCP surface for four server-state reads that had a Viewer panel and nothing else (#2629): what is
/// resident in the buffer pool, which extensions the server has, what locking was sampled, and the
/// checkpoint / WAL write picture.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgServerStateTools
{
    /// <summary>
    /// The major that removed <c>buffers_backend</c> and <c>buffers_backend_fsync</c> from
    /// <c>pg_stat_bgwriter</c> (#2653). They moved nowhere in that view - the fact lives in
    /// <c>pg_stat_io</c> from 17 on - so <see cref="PgWriteStatsCollector"/> writes NULL for both at this
    /// major and above, and this read names that rather than letting a structural absence read as a missing
    /// measurement.
    /// </summary>
    private const int BuffersBackendRemovedInMajor = 17;

    [McpServerTool(Name = "get_pg_buffer_usage"), Description("Gets what is actually resident in the PostgreSQL shared buffer pool, per relation, from the pg_buffercache extension: how many buffers each table or index occupies, how many of those are dirty, and the average usage count that PostgreSQL's clock-sweep eviction reads. This answers which objects the cache is actually spent on, which is a different question from which objects are read most - a small hot table and a large one scanned once can produce similar read counts and completely different residency. High dirty counts concentrated in one relation point at write pressure. Note that scanning pg_buffercache takes a lock on the buffer mapping, so the collector runs it sparingly.")]
    public static async Task<string> GetPgBufferUsage(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum rows to return. Default 25.")] int limit = 25,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var rows = await DarlingPgBufferUsageReader.GetPgBufferUsageAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, limit);

            if (rows.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_buffer_usage")
                    ?? await DarlingRuntimePrecondition.StatusAsync(
                        postgres, resolved.ServerId, resolved.ServerName, "pg_buffer_usage")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No buffer pool contents recorded for {resolved.ServerName} in the last "
                        + $"{hours_back} hour(s). This needs the pg_buffercache extension in the database "
                        + "the collector connects to.");
            }

            var newest = rows[0];

            var relations = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                relation_name = r.RelationName,
                relation_kind = r.RelationKind,
                buffers = r.Buffers,
                buffer_mb = Math.Round(r.Buffers * 8.0 / 1024.0, 1),
                dirty_buffers = r.DirtyBuffers,
                pct_dirty = Math.Round(r.PctDirty, 1),
                pct_of_pool = Math.Round(r.PctOfPool, 1),
                /* The eviction signal: PostgreSQL's clock sweep decrements this on each pass and evicts at
                   zero, so a high average means the relation is being touched faster than it decays. */
                avg_usage_count = r.AvgUsageCount,
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                relation_count = rows.Count,
                /* 8 kB is PostgreSQL's near-universal block size but it IS a compile-time setting, so the
                   buffer COUNT is the measurement and the megabyte figure is a convenience beside it. */
                pool_buffers_total = newest.PoolBuffersTotal,
                pool_buffers_used = newest.PoolBuffersUsed,
                note = "Residency, not read volume — a small hot table and a large one scanned once can "
                     + "read alike and occupy the pool completely differently. avg_usage_count is what the "
                     + "clock-sweep eviction reads; buffer sizes assume the standard 8 kB block.",
                relations,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL buffer usage failed: {ex.Message}");
        }
    }

    [McpServerTool(Name = "get_pg_extensions"), Description("Gets which PostgreSQL extensions are installed, outdated, merely available, or absent, per database. Use this to answer why another read is empty: pg_stat_statements, pg_wait_sampling, pg_stat_kcache, pg_qualstats, pgstattuple, pg_buffercache and hypopg each back a specific tool, and 'absent' here is the reason that tool has nothing to show. State is one of installed, outdated, available or absent - 'available' means the files are on the server and CREATE EXTENSION would work, which is a different situation from absent and usually a one-line fix. IMPORTANT: extensions are per-DATABASE, so installed_version reflects the database the row names and not the cluster; an extension can be installed in the application database and absent from postgres.")]
    public static async Task<string> GetPgExtensions(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 168 (7 days) - this collector runs daily.")] int hours_back = 168,
        [Description("Maximum rows to return. Default 50.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var rows = await DarlingPgExtensionAvailabilityReader.GetPgExtensionAvailabilityAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, limit);

            if (rows.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_extension_availability")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No extension inventory for {resolved.ServerName} in the last {hours_back} "
                        + "hour(s). This collector runs DAILY, so a short window can be empty on a healthy "
                        + "server — widen it before concluding anything.");
            }

            /* The result is capped at `limit`, so a summary count taken over the returned rows describes
               the PAGE and reads as a fact about the SERVER. Measured while writing this: at limit=50 the
               tool reported "installed: 10" for a server with more than that, because 50 was all it had
               looked at. Suppressed rather than renamed — "installed_in_this_page" is a number nobody
               wants. */
            var truncated = rows.Count >= limit;

            var extensions = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                extension_name = r.ExtensionName,
                state = r.State,
                installed_version = r.InstalledVersion,
                default_version = r.DefaultVersion,
                /* Whether THIS PRODUCT can use it, not merely whether it exists — the distinction that
                   makes this list actionable rather than an inventory. */
                monitoring_relevant = r.IsMonitoringRelevant,
                comment = r.Comment,
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                extension_count = rows.Count,
                truncated,
                installed = truncated
                    ? (int?)null
                    : rows.Count(r => string.Equals(r.State, "installed", StringComparison.OrdinalIgnoreCase)),
                available_not_installed = truncated
                    ? (int?)null
                    : rows.Count(r => string.Equals(r.State, "available", StringComparison.OrdinalIgnoreCase)),
                note = "Per DATABASE, not per cluster: installed_version describes the database the row "
                     + "names. 'available' means the files are present and CREATE EXTENSION would work — "
                     + "usually a one-line fix for an empty panel elsewhere."
                     + (truncated
                         ? " TRUNCATED at the row limit, so the state totals are WITHHELD: counting them "
                           + "over a capped result would describe this page rather than the server. Raise "
                           + "the limit for a complete inventory."
                         : string.Empty),
                extensions,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL extension availability failed: {ex.Message}");
        }
    }

    [McpServerTool(Name = "get_pg_lock_stats"), Description("Gets PostgreSQL lock activity as SAMPLED by the collector: lock type, mode, whether it was granted, the relation involved, how many captures saw it and the worst wait observed. This is a SAMPLE, not an event log - the collector periodically photographs pg_locks, so a lock held briefly between two samples is invisible here and an absence is not proof nothing was locked. Ungranted rows are the ones that matter: a granted lock is normal operation, while a lock waiting to be granted is a session blocked behind another. For a blocking chain with the root blocker attributed, use get_pg_blocking instead - this tool answers which lock modes and relations are contended over time rather than who is blocking whom right now.")]
    public static async Task<string> GetPgLockStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum rows to return. Default 25.")] int limit = 25,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var rows = await DarlingPgLockStatsReader.GetPgLockStatsAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, limit);

            if (rows.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_lock_stats")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No lock activity sampled on {resolved.ServerName} in the last {hours_back} "
                        + "hour(s). This is a SAMPLE of pg_locks rather than an event log, so this is the "
                        + "healthy state on a server without sustained contention — and it is not proof "
                        + "that nothing was ever locked.");
            }

            var truncated = rows.Count >= limit;
            var ungranted = rows.Count(r => !r.Granted);

            var locks = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                lock_type = r.LockType,
                mode = r.Mode,
                granted = r.Granted,
                relation_name = r.RelationName,
                captures = r.Captures,
                /* The denominator: how many captures happened at all, so `captures` can be read as a rate
                   of presence rather than an unanchored count. */
                total_captures = r.TotalCaptures,
                max_backends = r.MaxBackends,
                max_wait_ms = r.MaxWaitMs,
                last_seen = r.LastSeen,
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                lock_count = rows.Count,
                truncated,
                /* Counted over the returned rows, and reported only when they are all of them. */
                ungranted_count = truncated ? (int?)null : ungranted,
                note = (truncated
                         ? "TRUNCATED at the row limit — the ungranted total is WITHHELD, because counting "
                           + "over a capped result would describe this page rather than the server. "
                         : string.Empty)
                     + "A SAMPLE of pg_locks, not an event log: a lock taken and released between two "
                     + "captures does not appear. Read `captures` against `total_captures` — that ratio is "
                     + "how much of the window the lock was present for. Ungranted rows are the contended "
                     + "ones; for who is blocking whom, use get_pg_blocking.",
                locks,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL lock stats failed: {ex.Message}");
        }
    }

    [McpServerTool(Name = "get_pg_write_stats"), Description("Gets PostgreSQL checkpoint and WAL write activity across the window: how many checkpoints were timed versus REQUESTED, how long they spent writing and syncing, how many buffers were written by checkpoints, by the background writer and by backends themselves, and the WAL record, full-page-image and byte totals. Requested checkpoints are the signal to look for - a timed checkpoint is the scheduled one, while a requested checkpoint means WAL filled max_wal_size before the interval elapsed, so a high requested share means checkpoints are being forced by write volume. buffers_backend counts writes a query had to do itself because no clean buffer was available, which is backpressure landing on user queries - PostgreSQL 17 removed that column from pg_stat_bgwriter, so it is null on 17 and later and the note says so. wal_fpi counts full-page images, which is why write volume spikes immediately after each checkpoint. Returns one row describing the whole window, not a series.")]
    public static async Task<string> GetPgWriteStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var row = await DarlingPgWriteStatsReader.GetPgWriteStatsAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd);

            if (row is null)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_write_stats")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No checkpoint or WAL activity recorded for {resolved.ServerName} in the last "
                        + $"{hours_back} hour(s). These are differenced across snapshots, so a single "
                        + "collection has nothing to difference against and the window fills on the "
                        + "second one.");
            }

            var timed = row.CheckpointsTimed ?? 0;
            var requested = row.CheckpointsRequested ?? 0;

            /* #2653: 17 removed buffers_backend / buffers_backend_fsync from pg_stat_bgwriter with no
               successor there, so the collector writes NULL for them and the fact now lives in pg_stat_io.
               Without the registry's major this read cannot tell that structural absence from a measurement
               that did not happen, and its note explained a column that will never have a value here. */
            var postgresMajor = await DarlingEngineCapability.PostgresMajorVersionAsync(
                postgres, resolved.ServerId);
            var backendCountersRemoved = postgresMajor >= BuffersBackendRemovedInMajor;

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                window_start = row.WindowStartUtc,
                window_end = row.WindowEndUtc,
                checkpoints_timed = row.CheckpointsTimed,
                checkpoints_requested = row.CheckpointsRequested,
                /* The whole reason both numbers are here. Null rather than 0 when there were no
                   checkpoints at all — 0% requested would claim a healthy result from no evidence. */
                pct_checkpoints_requested = timed + requested > 0
                    ? Math.Round((double)requested / (timed + requested) * 100, 1)
                    : (double?)null,
                checkpoint_write_time_ms = row.CheckpointWriteTimeMs,
                checkpoint_sync_time_ms = row.CheckpointSyncTimeMs,
                buffers_written_checkpoint = row.BuffersWrittenCheckpoint,
                buffers_clean = row.BuffersClean,
                /* Backpressure landing on user queries: a backend writing its own buffer is a query
                   paying for the write because no clean buffer was free. */
                buffers_backend = row.BuffersBackend,
                buffers_backend_fsync = row.BuffersBackendFsync,
                /* Named only when the registry actually carries a major that removed them: absent from the
                   payload otherwise, so a server whose version is unknown gets no claim rather than a
                   guessed one. */
                buffers_backend_availability = backendCountersRemoved
                    ? $"not_collected: removed from pg_stat_bgwriter in PostgreSQL 17 (this server runs {postgresMajor}); the fact now lives in pg_stat_io, read by get_pg_io_stats"
                    : null,
                buffers_alloc = row.BuffersAlloc,
                maxwritten_clean = row.MaxwrittenClean,
                wal_records = row.WalRecords,
                wal_fpi = row.WalFpi,
                wal_bytes = row.WalBytes,
                wal_buffers_full = row.WalBuffersFull,
                wal_write_time_ms = row.WalWriteTimeMs,
                wal_sync_time_ms = row.WalSyncTimeMs,
                counter_reset = row.ResetDuringWindow,
                note = "Requested checkpoints mean max_wal_size filled before the scheduled interval, so a "
                     + "high requested share means write volume is forcing them. "
                     + (backendCountersRemoved
                         ? "buffers_backend and buffers_backend_fsync are NULL because PostgreSQL 17 removed "
                           + "them from pg_stat_bgwriter, not because nothing was measured: the question they "
                           + "answered - whether backends are writing their own buffers - is answered on this "
                           + "server by pg_stat_io, which get_pg_io_stats reads. "
                         : "buffers_backend is a write a QUERY had to perform itself for want of a clean "
                           + "buffer. ")
                     + "wal_fpi counts full-page images, which is why WAL volume spikes just after each "
                     + "checkpoint."
                     + (row.ResetDuringWindow
                         ? " The counters were RESET inside this window, so these figures cover only the "
                           + "time since the reset."
                         : string.Empty),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL write stats failed: {ex.Message}");
        }
    }

    [McpServerTool(Name = "get_pg_server_config"), Description("Gets the PostgreSQL server's configuration from pg_settings - what each parameter is set to, whether it differs from the compiled-in default, where the value came from (configuration file, command line, ALTER SYSTEM, per-database or per-role), and whether changing it needs a restart or only a reload. Non-default settings are listed FIRST, because a server has several hundred parameters and only the ones somebody chose are an answer. Reports pending_restart loudly: that means postgresql.conf was edited and reloaded but the running server is still using the old value, so the file and the server disagree with no symptom until the next restart. Session-scoped rows are excluded - pg_settings is a per-connection view and its client-source rows describe the monitoring connection, not the server. Snapshot from the most recent collection, not a window.")]
    public static async Task<string> GetPgServerConfig(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Maximum settings to return. Default 100.")] int limit = 100,
        [Description("When true, include settings still at their default. Default false - the non-default ones are the answer.")] bool include_defaults = false)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var limitError = McpHelpers.ValidateTop(limit);
        if (limitError != null) return McpHelpers.Status("error", limitError);

        try
        {
            var rows = await DarlingPgServerConfigReader.GetCurrentConfigAsync(
                postgres, resolved.ServerId, limit);

            if (rows.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_server_config")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No configuration snapshot has been collected for {resolved.ServerName} yet. "
                        + "This collector runs hourly, so a server registered in the last hour has not "
                        + "reached its first collection.");
            }

            var shown = include_defaults ? rows : rows.Where(r => !r.IsDefault).ToList();
            var pendingRestart = rows.Where(r => r.PendingRestart).Select(r => r.Name).ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                status = "server_config",
                /* Both counts, because they answer different questions and one without the other invites
                   the wrong conclusion: a small returned count is reassuring only if you know it was
                   filtered rather than truncated. */
                settings_returned = shown.Count,
                non_default_count = rows.Count(r => !r.IsDefault),
                truncated = rows.Count >= limit,
                pending_restart_count = pendingRestart.Count,
                pending_restart_settings = pendingRestart.Count > 0 ? pendingRestart : null,
                note = pendingRestart.Count > 0
                    ? "One or more settings are marked PENDING RESTART: the configuration file has been "
                      + "changed and reloaded, but the running server is still using the previous value. "
                      + "The file and the server disagree until the next restart, at which point the "
                      + "behaviour changes with no deployment to explain it."
                    : "Non-default settings first. 'source' says where the value came from; 'context' says "
                      + "what changing it would take - postmaster needs a restart, sighup a reload, user "
                      + "nothing. Session-scoped rows are excluded: pg_settings is a per-connection view "
                      + "and those describe the monitoring connection rather than the server.",
                settings = shown.Select(r => new
                {
                    name = r.Name,
                    setting = r.Setting,
                    unit = r.Unit,
                    /* The compiled-in default, so a reader can see what was moved away FROM without
                       needing a table of defaults that would rot at every major. */
                    default_value = r.BootValue,
                    is_default = r.IsDefault,
                    source = r.Source,
                    context = r.Context,
                    requires_restart_to_change = string.Equals(r.Context, "postmaster", StringComparison.Ordinal),
                    pending_restart = r.PendingRestart,
                    category = r.Category,
                    description = r.ShortDescription,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL server config failed: {ex.Message}");
        }
    }

    [McpServerTool(Name = "get_pg_server_config_changes"), Description("Gets PostgreSQL configuration parameters whose value CHANGED during the window, newest first, with the old and new value side by side. This is the read that answers 'this got slow sometime last month, what changed' - and nothing else in the stack can reconstruct it after the fact, because a configuration history that was not recorded cannot be recovered from the server. A setting appearing for the first time is deliberately NOT reported as a change: the first snapshot after an upgrade, or after an extension is loaded, would otherwise manufacture hundreds of changes nobody made. Session-scoped rows are excluded, so a monitoring reconnect does not read as a configuration change.")]
    public static async Task<string> GetPgServerConfigChanges(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 168 (one week).")] int hours_back = 168,
        [Description("Maximum changes to return. Default 100.")] int limit = 100,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        var limitError = McpHelpers.ValidateTop(limit);
        if (limitError != null) return McpHelpers.Status("error", limitError);

        try
        {
            var rows = await DarlingPgServerConfigReader.GetConfigChangesAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, limit);

            if (rows.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_server_config")
                    ?? McpHelpers.Status(
                        "no_changes",
                        $"No configuration parameter changed value on {resolved.ServerName} in the last "
                        + $"{hours_back} hour(s). That is a real finding rather than missing data - this "
                        + "read compares consecutive snapshots, so an unchanged server produces no rows.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                status = "config_changes",
                change_count = rows.Count,
                truncated = rows.Count >= limit,
                note = "changed_at is the time of the snapshot that FIRST reported the new value, so the "
                     + "change happened at some point in the hour before it - this collector runs hourly. "
                     + "A setting appearing for the first time is not reported here.",
                changes = rows.Select(r => new
                {
                    changed_at = r.ChangedAtUtc,
                    name = r.Name,
                    old_value = r.OldValue,
                    new_value = r.NewValue,
                    unit = r.Unit,
                    source = r.Source,
                    context = r.Context,
                    description = r.ShortDescription,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL config changes failed: {ex.Message}");
        }
    }

}
