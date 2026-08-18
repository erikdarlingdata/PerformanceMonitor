/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Manages collector schedules and determines when each collector should run.
/// Supports per-server schedule overrides (v2 config format).
/// </summary>
public class ScheduleManager
{
    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    // Single source of truth for the schedule-editor presets. Exposed (internal) so
    // CollectorScheduleEditorWindow reads/applies these instead of holding its own copy — the two
    // preset tables previously drifted (the editor's fell ~8 collectors behind). See
    // SharedCollectorDefaultsPinTests for the integrity guard.
    internal static readonly Dictionary<string, Dictionary<string, int>> s_presets = new(StringComparer.OrdinalIgnoreCase)
    {
        ["Aggressive"] = new(StringComparer.OrdinalIgnoreCase)
        {
            ["wait_stats"] = 1, ["latch_stats"] = 1, ["spinlock_stats"] = 1,
            ["cpu_scheduler_stats"] = 1, ["plan_cache_stats"] = 2,
            ["query_stats"] = 1, ["procedure_stats"] = 1,
            ["query_store"] = 2, ["query_snapshots"] = 1, ["cpu_utilization"] = 1,
            ["file_io_stats"] = 1, ["memory_stats"] = 1, ["memory_clerks"] = 2,
            ["memory_pressure_events"] = 5,
            ["tempdb_stats"] = 1, ["perfmon_stats"] = 1, ["deadlocks"] = 2,
            ["memory_grant_stats"] = 1, ["waiting_tasks"] = 1,
            ["dmv_blocking_snapshot"] = 1,
            ["blocked_process_report"] = 1, ["running_jobs"] = 2,
            ["session_summary_stats"] = 2, ["system_health_events"] = 2,
            ["default_trace_events"] = 2, ["job_history"] = 2, ["agent_status"] = 2,
            ["ag_replica_states"] = 1, ["ag_database_replica_states"] = 1,
            /* plan_correction tracks query_store across the presets: same per-database enumeration
               shape, same default tier, so an operator backing one off wants the other to follow. */
            ["plan_correction"] = 2,
            ["database_states"] = 1
        },
        ["Balanced"] = new(StringComparer.OrdinalIgnoreCase)
        {
            ["wait_stats"] = 1, ["latch_stats"] = 1, ["spinlock_stats"] = 1,
            ["cpu_scheduler_stats"] = 1, ["plan_cache_stats"] = 5,
            ["query_stats"] = 1, ["procedure_stats"] = 1,
            ["query_store"] = 5, ["query_snapshots"] = 1, ["cpu_utilization"] = 1,
            ["file_io_stats"] = 1, ["memory_stats"] = 1, ["memory_clerks"] = 5,
            ["memory_pressure_events"] = 5,
            /* deadlocks follows its new 5-minute default tier (#1963) - Balanced mirrors the defaults. */
            ["tempdb_stats"] = 1, ["perfmon_stats"] = 1, ["deadlocks"] = 5,
            ["memory_grant_stats"] = 1, ["waiting_tasks"] = 1,
            ["dmv_blocking_snapshot"] = 1,
            ["blocked_process_report"] = 1, ["running_jobs"] = 5,
            ["session_summary_stats"] = 5, ["system_health_events"] = 5,
            ["default_trace_events"] = 5, ["job_history"] = 5, ["agent_status"] = 5,
            ["ag_replica_states"] = 1, ["ag_database_replica_states"] = 1,
            ["plan_correction"] = 5,
            ["database_states"] = 1
        },
        ["Low-Impact"] = new(StringComparer.OrdinalIgnoreCase)
        {
            ["wait_stats"] = 5, ["latch_stats"] = 5, ["spinlock_stats"] = 5,
            ["cpu_scheduler_stats"] = 5, ["plan_cache_stats"] = 15,
            ["query_stats"] = 10, ["procedure_stats"] = 10,
            ["query_store"] = 30, ["query_snapshots"] = 5, ["cpu_utilization"] = 5,
            ["file_io_stats"] = 10, ["memory_stats"] = 10, ["memory_clerks"] = 30,
            ["memory_pressure_events"] = 15,
            ["tempdb_stats"] = 5, ["perfmon_stats"] = 5, ["deadlocks"] = 15,
            ["memory_grant_stats"] = 5, ["waiting_tasks"] = 5,
            ["dmv_blocking_snapshot"] = 5,
            ["blocked_process_report"] = 5, ["running_jobs"] = 30,
            ["session_summary_stats"] = 15, ["system_health_events"] = 15,
            ["default_trace_events"] = 15, ["job_history"] = 15, ["agent_status"] = 15,
            ["ag_replica_states"] = 5, ["ag_database_replica_states"] = 5,
            ["plan_correction"] = 30,
            ["database_states"] = 5
        }
    };

    private readonly string _schedulePath;
    private readonly ILogger<ScheduleManager>? _logger;
    private readonly object _lock = new();

    private List<CollectorSchedule> _defaultSchedule;
    private Dictionary<string, ServerScheduleOverride> _serverOverrides;

    /// <summary>
    /// Per-server runtime state: serverId → (collectorName → lastRunTime).
    /// Kept separate from config because runtime state is not persisted to JSON.
    /// </summary>
    private readonly Dictionary<string, Dictionary<string, DateTime>> _serverRunState = new();

    public ScheduleManager(string configDirectory, ILogger<ScheduleManager>? logger = null)
    {
        _schedulePath = Path.Combine(configDirectory, "collection_schedule.json");
        _logger = logger;
        _defaultSchedule = new List<CollectorSchedule>();
        _serverOverrides = new Dictionary<string, ServerScheduleOverride>();

        LoadSchedules();
    }

    // ──────────────────────────────────────────────────────────────────
    //  Default-schedule public API.
    // ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// Updates a collector's schedule settings (default schedule).
    /// </summary>
    public void UpdateSchedule(string collectorName, bool? enabled = null, int? frequencyMinutes = null, int? retentionDays = null)
    {
        lock (_lock)
        {
            var schedule = _defaultSchedule.FirstOrDefault(s =>
                s.Name.Equals(collectorName, StringComparison.OrdinalIgnoreCase));

            if (schedule == null)
            {
                throw new InvalidOperationException($"Collector '{collectorName}' not found");
            }

            if (enabled.HasValue)
            {
                schedule.Enabled = enabled.Value;
            }

            if (frequencyMinutes.HasValue)
            {
                schedule.FrequencyMinutes = frequencyMinutes.Value;
            }

            if (retentionDays.HasValue)
            {
                schedule.RetentionDays = retentionDays.Value;
            }

            SaveSchedules();

            _logger?.LogInformation("Updated schedule for collector '{Name}': Enabled={Enabled}, Frequency={Frequency}m, Retention={Retention}d",
                collectorName, schedule.Enabled, schedule.FrequencyMinutes, schedule.RetentionDays);
        }
    }

    /// <summary>
    /// Detects which preset matches the current default schedule intervals, or returns "Custom".
    /// </summary>
    public string GetActivePreset()
    {
        lock (_lock)
        {
            return DetectPreset(_defaultSchedule);
        }
    }

    // ──────────────────────────────────────────────────────────────────
    //  New per-server API (v2)
    // ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns the default schedule list.
    /// </summary>
    public IReadOnlyList<CollectorSchedule> GetDefaultSchedule()
    {
        lock (_lock)
        {
            return _defaultSchedule.ToList();
        }
    }

    /// <summary>
    /// Returns the schedule for a specific server.
    /// If the server has an override, returns those collectors; otherwise returns a copy of the default.
    /// </summary>
    public IReadOnlyList<CollectorSchedule> GetSchedulesForServer(string serverId)
    {
        lock (_lock)
        {
            if (_serverOverrides.TryGetValue(serverId, out var over))
            {
                return over.Collectors.ToList();
            }

            return CloneScheduleList(_defaultSchedule);
        }
    }

    /// <summary>
    /// Gets collectors that are due to run for a specific server, using per-server run state.
    /// </summary>
    public IReadOnlyList<CollectorSchedule> GetDueCollectorsForServer(string serverId)
    {
        lock (_lock)
        {
            var schedules = _serverOverrides.TryGetValue(serverId, out var over)
                ? over.Collectors
                : _defaultSchedule;

            _serverRunState.TryGetValue(serverId, out var runState);

            var due = new List<CollectorSchedule>();
            foreach (var s in schedules)
            {
                if (!s.Enabled || !s.IsScheduled)
                    continue;

                if (runState == null || !runState.TryGetValue(s.Name, out var lastRun))
                {
                    due.Add(s); // never run — due immediately
                    continue;
                }

                var elapsed = DateTime.UtcNow - lastRun;
                if (elapsed.TotalMinutes >= s.FrequencyMinutes)
                {
                    due.Add(s);
                }
            }

            return due;
        }
    }

    /// <summary>
    /// Gets on-load only collectors for a specific server.
    /// </summary>
    public IReadOnlyList<CollectorSchedule> GetOnLoadCollectorsForServer(string serverId)
    {
        lock (_lock)
        {
            var schedules = _serverOverrides.TryGetValue(serverId, out var over)
                ? over.Collectors
                : _defaultSchedule;

            return schedules.Where(s => s.Enabled && !s.IsScheduled).ToList();
        }
    }

    /// <summary>
    /// Gets a specific collector schedule by name for a server.
    /// </summary>
    public CollectorSchedule? GetScheduleForServer(string serverId, string collectorName)
    {
        lock (_lock)
        {
            var schedules = _serverOverrides.TryGetValue(serverId, out var over)
                ? over.Collectors
                : _defaultSchedule;

            return schedules.FirstOrDefault(s =>
                s.Name.Equals(collectorName, StringComparison.OrdinalIgnoreCase));
        }
    }

    /// <summary>
    /// Records a collector run for a specific server.
    /// </summary>
    public void MarkCollectorRunForServer(string serverId, string collectorName, DateTime runTime)
    {
        lock (_lock)
        {
            if (!_serverRunState.TryGetValue(serverId, out var runState))
            {
                runState = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
                _serverRunState[serverId] = runState;
            }

            runState[collectorName] = runTime;

            _logger?.LogDebug("Marked collector '{Name}' as run for server {ServerId} at {Time}",
                collectorName, serverId, runTime);
        }
    }

    /// <summary>
    /// Creates or updates a per-server schedule override.
    /// </summary>
    public void SetScheduleForServer(string serverId, List<CollectorSchedule> schedules)
    {
        lock (_lock)
        {
            _serverOverrides[serverId] = new ServerScheduleOverride { Collectors = schedules };
            SaveSchedules();

            _logger?.LogInformation("Set schedule override for server {ServerId} ({Count} collectors)",
                serverId, schedules.Count);
        }
    }

    /// <summary>
    /// Removes a server's schedule override, reverting it to the default.
    /// </summary>
    public void RemoveServerOverride(string serverId)
    {
        lock (_lock)
        {
            if (_serverOverrides.Remove(serverId))
            {
                SaveSchedules();
                _logger?.LogInformation("Removed schedule override for server {ServerId}", serverId);
            }
        }
    }

    /// <summary>
    /// Returns true if the server has a custom schedule override.
    /// </summary>
    public bool HasServerOverride(string serverId)
    {
        lock (_lock)
        {
            return _serverOverrides.ContainsKey(serverId);
        }
    }

    /// <summary>
    /// Detects which preset matches a server's active schedule.
    /// </summary>
    public string GetActivePresetForServer(string serverId)
    {
        lock (_lock)
        {
            var schedules = _serverOverrides.TryGetValue(serverId, out var over)
                ? over.Collectors
                : _defaultSchedule;

            return DetectPreset(schedules);
        }
    }

    // ──────────────────────────────────────────────────────────────────
    //  Persistence
    // ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// Saves schedules to the JSON config file (v2 format).
    /// </summary>
    public void SaveSchedules()
    {
        lock (_lock)
        {
            try
            {
                var config = new ScheduleConfigV2
                {
                    Version = 2,
                    DefaultSchedule = _defaultSchedule,
                    ServerOverrides = _serverOverrides
                };
                string json = JsonSerializer.Serialize(config, s_jsonOptions);
                File.WriteAllText(_schedulePath, json);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to save collection_schedule.json");
                throw;
            }
        }
    }

    /// <summary>
    /// Loads schedules from the JSON config file, handling v1→v2 migration.
    /// </summary>
    private void LoadSchedules()
    {
        if (!File.Exists(_schedulePath))
        {
            _logger?.LogInformation("Schedule file not found, using defaults");
            _defaultSchedule = GetDefaultSchedules();
            _serverOverrides = new Dictionary<string, ServerScheduleOverride>();
            SaveSchedules();
            return;
        }

        try
        {
            string json = File.ReadAllText(_schedulePath);

            if (TryLoadV2(json))
            {
                /* Create backup of valid config */
                try { File.Copy(_schedulePath, _schedulePath + ".bak", overwrite: true); }
                catch { /* best effort */ }

                _logger?.LogInformation(
                    "Loaded v2 schedule config: {DefaultCount} default collectors, {OverrideCount} server override(s)",
                    _defaultSchedule.Count, _serverOverrides.Count);
            }
            else
            {
                /* v1 format — migrate */
                var v1Config = JsonSerializer.Deserialize<ScheduleConfigV1>(json);
                _defaultSchedule = v1Config?.Collectors ?? GetDefaultSchedules();
                _serverOverrides = new Dictionary<string, ServerScheduleOverride>();

                /* Backup the v1 file before overwriting */
                try { File.Copy(_schedulePath, _schedulePath + ".v1.bak", overwrite: true); }
                catch { /* best effort */ }

                SaveSchedules();

                _logger?.LogInformation(
                    "Migrated v1 schedule config to v2: {Count} collectors moved to default_schedule",
                    _defaultSchedule.Count);
            }

            MergeNewDefaults();
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failed to load collection_schedule.json, attempting backup restore");

            /* Try to restore from backup */
            var bakPath = _schedulePath + ".bak";
            if (File.Exists(bakPath))
            {
                try
                {
                    string bakJson = File.ReadAllText(bakPath);
                    if (TryLoadV2(bakJson))
                    {
                        _logger?.LogInformation("Restored schedules from backup file");
                        return;
                    }

                    var bakConfig = JsonSerializer.Deserialize<ScheduleConfigV1>(bakJson);
                    _defaultSchedule = bakConfig?.Collectors ?? GetDefaultSchedules();
                    _serverOverrides = new Dictionary<string, ServerScheduleOverride>();
                    _logger?.LogInformation("Restored v1 schedules from backup file");
                    return;
                }
                catch { /* backup also corrupt, fall through to defaults */ }
            }

            _defaultSchedule = GetDefaultSchedules();
            _serverOverrides = new Dictionary<string, ServerScheduleOverride>();
            SaveSchedules();
        }
    }

    /// <summary>
    /// Attempts to load JSON as v2 format. Returns true if successful.
    /// </summary>
    private bool TryLoadV2(string json)
    {
        using var doc = JsonDocument.Parse(json);
        if (!doc.RootElement.TryGetProperty("version", out var versionProp) || versionProp.GetInt32() < 2)
            return false;

        var config = JsonSerializer.Deserialize<ScheduleConfigV2>(json);
        if (config == null)
            return false;

        _defaultSchedule = config.DefaultSchedule ?? GetDefaultSchedules();
        _serverOverrides = config.ServerOverrides ?? new Dictionary<string, ServerScheduleOverride>();
        return true;
    }

    /// <summary>
    /// Merges any new default collectors into the default schedule and all server overrides.
    /// Also removes obsolete collectors that no longer have a dispatch case.
    /// </summary>
    private void MergeNewDefaults()
    {
        var defaults = GetDefaultSchedules();
        var defaultNames = new HashSet<string>(defaults.Select(s => s.Name), StringComparer.OrdinalIgnoreCase);
        var changed = false;

        /* Merge into default schedule */
        changed |= MergeIntoList(_defaultSchedule, defaults, defaultNames);

        /* Merge into each server override */
        foreach (var over in _serverOverrides.Values)
        {
            changed |= MergeIntoList(over.Collectors, defaults, defaultNames);
        }

        if (changed)
        {
            SaveSchedules();
        }
    }

    /// <summary>
    /// Merges new defaults into a collector list. Removes obsolete, adds missing.
    /// Returns true if any changes were made.
    /// </summary>
    private bool MergeIntoList(List<CollectorSchedule> list, List<CollectorSchedule> defaults, HashSet<string> defaultNames)
    {
        var loadedNames = new HashSet<string>(list.Select(s => s.Name), StringComparer.OrdinalIgnoreCase);
        var changed = false;

        /* Remove obsolete collectors */
        var removed = list.RemoveAll(s => !defaultNames.Contains(s.Name));
        if (removed > 0)
        {
            _logger?.LogInformation("Removed {Count} obsolete collector(s) from schedule", removed);
            changed = true;
        }

        /* Add missing collectors */
        foreach (var defaultSchedule in defaults)
        {
            if (!loadedNames.Contains(defaultSchedule.Name))
            {
                list.Add(CloneSchedule(defaultSchedule));
                _logger?.LogInformation("Added missing collector '{Name}' from defaults", defaultSchedule.Name);
                changed = true;
            }
        }

        return changed;
    }

    // ──────────────────────────────────────────────────────────────────
    //  Helpers
    // ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// Detects which preset matches a list of collector schedules, or "Custom". This is the single
    /// source of preset logic; <see cref="ApplyPreset"/> and the collector-schedule editor window both
    /// call these static methods rather than keeping a second preset table, so the two cannot drift.
    /// </summary>
    internal static string DetectPreset(List<CollectorSchedule> schedules)
    {
        foreach (var (presetName, intervals) in s_presets)
        {
            bool matches = true;
            foreach (var (collector, freq) in intervals)
            {
                var schedule = schedules.FirstOrDefault(s =>
                    s.Name.Equals(collector, StringComparison.OrdinalIgnoreCase));
                if (schedule != null && schedule.FrequencyMinutes != freq)
                {
                    matches = false;
                    break;
                }
            }
            if (matches) return presetName;
        }
        return "Custom";
    }

    /// <summary>
    /// Applies a named preset's intervals to a schedule list in place (frequencies only; enabled and
    /// retention are untouched). An unknown preset name is a no-op. Shared with the editor window so the
    /// preset table lives exactly once.
    /// </summary>
    internal static void ApplyPreset(List<CollectorSchedule> schedules, string presetName)
    {
        if (!s_presets.TryGetValue(presetName, out var intervals))
        {
            return;
        }

        foreach (var (collector, freq) in intervals)
        {
            var schedule = schedules.FirstOrDefault(s =>
                s.Name.Equals(collector, StringComparison.OrdinalIgnoreCase));
            if (schedule != null)
            {
                schedule.FrequencyMinutes = freq;
            }
        }
    }

    /// <summary>
    /// Deep-clones a schedule list (for creating overrides from defaults).
    /// </summary>
    private static List<CollectorSchedule> CloneScheduleList(List<CollectorSchedule> source)
    {
        return source.Select(CloneSchedule).ToList();
    }

    /// <summary>
    /// Deep-clones a single CollectorSchedule (config properties only, not runtime state).
    /// </summary>
    private static CollectorSchedule CloneSchedule(CollectorSchedule s)
    {
        return new CollectorSchedule
        {
            Name = s.Name,
            Enabled = s.Enabled,
            FrequencyMinutes = s.FrequencyMinutes,
            RetentionDays = s.RetentionDays,
            Description = s.Description
        };
    }

    /// <summary>
    /// Gets the default collector schedules. Internal so the identity-pin test can assert this
    /// table matches the shared <see cref="PerformanceMonitor.Collectors.CollectorScheduleDefaults"/>
    /// (which the Darling service schedules by) — the two cannot drift.
    /// </summary>
    internal static List<CollectorSchedule> GetDefaultSchedules()
    {
        return new List<CollectorSchedule>
        {
            new() { Name = "wait_stats", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "Wait statistics from sys.dm_os_wait_stats" },
            new() { Name = "latch_stats", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "Latch statistics from sys.dm_os_latch_stats" },
            new() { Name = "spinlock_stats", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "Spinlock statistics from sys.dm_os_spinlock_stats" },
            new() { Name = "cpu_scheduler_stats", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "CPU scheduler, workload group, NUMA, and OS memory pressure snapshot (not collected on Azure SQL DB)" },
            new() { Name = "plan_cache_stats", Enabled = true, FrequencyMinutes = 5, RetentionDays = 30, Description = "Plan cache composition (single-use vs multi-use bloat) from sys.dm_exec_cached_plans" },
            new() { Name = "query_stats", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "Query statistics from sys.dm_exec_query_stats" },
            new() { Name = "procedure_stats", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "Stored procedure statistics from sys.dm_exec_procedure_stats" },
            new() { Name = "query_store", Enabled = true, FrequencyMinutes = 5, RetentionDays = 30, Description = "Query Store data (top 100 queries per database)" },
            new() { Name = "query_snapshots", Enabled = true, FrequencyMinutes = 1, RetentionDays = 7, Description = "Currently running queries snapshot" },
            new() { Name = "cpu_utilization", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "CPU utilization from ring buffer" },
            new() { Name = "file_io_stats", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "File I/O statistics from sys.dm_io_virtual_file_stats" },
            new() { Name = "memory_stats", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "Memory statistics from sys.dm_os_sys_memory and performance counters" },
            new() { Name = "memory_clerks", Enabled = true, FrequencyMinutes = 5, RetentionDays = 30, Description = "Memory clerk allocations from sys.dm_os_memory_clerks" },
            new() { Name = "memory_pressure_events", Enabled = true, FrequencyMinutes = 5, RetentionDays = 30, Description = "Memory pressure notifications from RING_BUFFER_RESOURCE_MONITOR" },
            new() { Name = "tempdb_stats", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "tempdb space usage from sys.dm_db_file_space_usage" },
            new() { Name = "perfmon_stats", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "Key performance counters from sys.dm_os_performance_counters" },
            new() { Name = "deadlocks", Enabled = true, FrequencyMinutes = 5, RetentionDays = 30, Description = "Deadlocks from a dedicated PerformanceMonitor_Deadlock XE session (xml_deadlock_report)" },
            new() { Name = "server_config", Enabled = true, FrequencyMinutes = 0, RetentionDays = 30, Description = "Server configuration (on-load only)" },
            new() { Name = "database_config", Enabled = true, FrequencyMinutes = 0, RetentionDays = 30, Description = "Database configuration (on-load only)" },
            new() { Name = "database_states", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "Per-database state (sys.databases.state_desc) time series; feeds the baseline-deviation database-state alert (not collected on Azure SQL DB)" },
            new() { Name = "memory_grant_stats", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "Memory grant statistics from sys.dm_exec_query_memory_grants" },
            new() { Name = "waiting_tasks", Enabled = true, FrequencyMinutes = 1, RetentionDays = 7, Description = "Point-in-time waiting tasks from sys.dm_os_waiting_tasks" },
            new() { Name = "dmv_blocking_snapshot", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "Always-on point-in-time blocking snapshot from DMVs (BPR-independent fallback; works when blocked process threshold is unset, e.g. AWS RDS)" },
            new() { Name = "blocked_process_report", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "Blocked process reports from XE ring buffer session (opt-out)" },
            new() { Name = "long_query_completions", Enabled = false, FrequencyMinutes = 1, RetentionDays = 30, Description = "Long-running query completions (rpc_completed/sql_batch_completed >= threshold, plus attention/cancels) from a dedicated XE ring buffer session. OPT-IN, default OFF: enabling creates the session on monitored servers, disabling drops it (#1496)" },
            new() { Name = "database_scoped_config", Enabled = true, FrequencyMinutes = 0, RetentionDays = 30, Description = "Database-scoped configurations (on-load only)" },
            new() { Name = "trace_flags", Enabled = true, FrequencyMinutes = 0, RetentionDays = 30, Description = "Active trace flags via DBCC TRACESTATUS (on-load only)" },
            new() { Name = "running_jobs", Enabled = true, FrequencyMinutes = 5, RetentionDays = 7, Description = "Currently running SQL Agent jobs with duration comparison" },
            new() { Name = "database_size_stats", Enabled = true, FrequencyMinutes = 60, RetentionDays = 90, Description = "Database file sizes for growth trending and capacity planning" },
            new() { Name = "index_object_stats", Enabled = true, FrequencyMinutes = 1440, RetentionDays = 90, Description = "Per-object table/index size, usage, and locking stats for growth, unused-index, and contention analysis (daily collection)" },
            new() { Name = "server_properties", Enabled = true, FrequencyMinutes = 0, RetentionDays = 365, Description = "Server edition, licensing, CPU/memory hardware metadata (on-load only)" },
            new() { Name = "session_stats", Enabled = true, FrequencyMinutes = 5, RetentionDays = 30, Description = "Per-application session counts from sys.dm_exec_sessions" },
            new() { Name = "session_summary_stats", Enabled = true, FrequencyMinutes = 5, RetentionDays = 30, Description = "Server-wide session summary (idle/leak signal): total/running/sleeping/idle-over-30min counts, memory waits, top app/host from sys.dm_exec_sessions + sys.dm_exec_requests" },
            new() { Name = "system_health_events", Enabled = true, FrequencyMinutes = 5, RetentionDays = 30, Description = "Raw system_health Extended Events (memory broker/OOM, scheduler monitor, sp_server_diagnostics, severe errors, significant waits) captured as XML for health-parser analysis (not collected on Azure SQL DB)" },
            new() { Name = "default_trace_events", Enabled = true, FrequencyMinutes = 5, RetentionDays = 30, Description = "Built-in Default Trace events via sys.fn_trace_gettable: file auto-grow/shrink stalls, severe ErrorLog writes, schema DDL, security audits, and Server Memory Change (not collected on Azure SQL DB)" },
            new() { Name = "job_history", Enabled = true, FrequencyMinutes = 5, RetentionDays = 365, Description = "Retained SQL Agent job-run history from msdb.dbo.sysjobhistory (per-step results, retries, durations, failures) deduped on the instance_id high-water mark; up to a year retained for the Job History tab (not collected on Azure SQL DB)" },
            new() { Name = "agent_status", Enabled = true, FrequencyMinutes = 5, RetentionDays = 7, Description = "SQL Agent service status (Running/Stopped from sys.dm_server_services) and next scheduled run from msdb; drives the Job History tab header (not collected on Azure SQL DB)" },
            new() { Name = "ag_replica_states", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "Availability Group replica health (role, operational/connected state, recovery and synchronization health) from sys.dm_hadr_availability_replica_states; zero rows on a server with no AGs (not collected on Azure SQL DB)" },
            new() { Name = "ag_database_replica_states", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "Availability Group per-database replica health (synchronization state, send/redo queue sizes and rates, secondary lag) from sys.dm_hadr_database_replica_states; zero rows on a server with no AGs (not collected on Azure SQL DB)" },
            new() { Name = "plan_correction", Enabled = true, FrequencyMinutes = 5, RetentionDays = 30, Description = "Automatic plan correction: per-database FORCE_LAST_GOOD_PLAN enablement from sys.database_automatic_tuning_options plus the engine's live recommendation set from sys.dm_db_tuning_recommendations, with the regressed query's text resolved through Query Store (SQL Server 2017+ and Azure; Enterprise/Developer edition)" },
            new() { Name = "pvs_stats", Enabled = true, FrequencyMinutes = 60, RetentionDays = 90, Description = "Accelerated Database Recovery persistent version store size and cleanup state per database from sys.dm_tran_persistent_version_store_stats, with the aborted-transaction count and the skipped-page counters that say why cleanup is not reclaiming; SQL Server 2019+ only, always collected on Azure SQL DB (ADR is always on there)" },
            new() { Name = "query_store_health", Enabled = true, FrequencyMinutes = 60, RetentionDays = 30, Description = "Per-database Query Store health from sys.database_query_store_options: actual vs desired state (the cap-hit READ_ONLY transition and its readonly_reason), current vs max storage, cleanup mode and thresholds, and the runtime-stats interval length; one row per database, OFF recorded explicitly" }
        };
    }

    // ──────────────────────────────────────────────────────────────────
    //  JSON config models
    // ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// v1 JSON format: { "collectors": [...] }
    /// </summary>
    private class ScheduleConfigV1
    {
        [JsonPropertyName("collectors")]
        public List<CollectorSchedule> Collectors { get; set; } = new();
    }

    /// <summary>
    /// v2 JSON format: { "version": 2, "default_schedule": [...], "server_overrides": { "guid": { "collectors": [...] } } }
    /// </summary>
    private class ScheduleConfigV2
    {
        [JsonPropertyName("version")]
        public int Version { get; set; } = 2;

        [JsonPropertyName("default_schedule")]
        public List<CollectorSchedule> DefaultSchedule { get; set; } = new();

        [JsonPropertyName("server_overrides")]
        public Dictionary<string, ServerScheduleOverride> ServerOverrides { get; set; } = new();
    }
}
