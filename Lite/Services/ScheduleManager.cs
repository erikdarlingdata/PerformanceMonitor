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
/// </summary>
public class ScheduleManager
{
    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public static readonly string[] PresetNames = ["Low-Impact", "Balanced", "Aggressive"];

    private static readonly Dictionary<string, Dictionary<string, int>> s_presets = new(StringComparer.OrdinalIgnoreCase)
    {
        ["Aggressive"] = new(StringComparer.OrdinalIgnoreCase)
        {
            ["wait_stats"] = 1, ["query_stats"] = 1, ["procedure_stats"] = 1,
            ["query_store"] = 2, ["query_snapshots"] = 1, ["cpu_utilization"] = 1,
            ["file_io_stats"] = 1, ["memory_stats"] = 1, ["memory_clerks"] = 2,
            ["tempdb_stats"] = 1, ["perfmon_stats"] = 1, ["deadlocks"] = 1,
            ["memory_grant_stats"] = 1, ["waiting_tasks"] = 1,
            ["blocked_process_report"] = 1, ["running_jobs"] = 2
        },
        ["Balanced"] = new(StringComparer.OrdinalIgnoreCase)
        {
            ["wait_stats"] = 1, ["query_stats"] = 1, ["procedure_stats"] = 1,
            ["query_store"] = 5, ["query_snapshots"] = 1, ["cpu_utilization"] = 1,
            ["file_io_stats"] = 1, ["memory_stats"] = 1, ["memory_clerks"] = 5,
            ["tempdb_stats"] = 1, ["perfmon_stats"] = 1, ["deadlocks"] = 1,
            ["memory_grant_stats"] = 1, ["waiting_tasks"] = 1,
            ["blocked_process_report"] = 1, ["running_jobs"] = 5
        },
        ["Low-Impact"] = new(StringComparer.OrdinalIgnoreCase)
        {
            ["wait_stats"] = 5, ["query_stats"] = 10, ["procedure_stats"] = 10,
            ["query_store"] = 30, ["query_snapshots"] = 5, ["cpu_utilization"] = 5,
            ["file_io_stats"] = 10, ["memory_stats"] = 10, ["memory_clerks"] = 30,
            ["tempdb_stats"] = 5, ["perfmon_stats"] = 5, ["deadlocks"] = 5,
            ["memory_grant_stats"] = 5, ["waiting_tasks"] = 5,
            ["blocked_process_report"] = 5, ["running_jobs"] = 30
        }
    };

    private readonly string _schedulePath;
    private readonly ILogger<ScheduleManager>? _logger;
    private List<CollectorSchedule> _schedules;
    private readonly object _lock = new();

    public ScheduleManager(string configDirectory, ILogger<ScheduleManager>? logger = null)
    {
        _schedulePath = Path.Combine(configDirectory, "collection_schedule.json");
        _logger = logger;
        _schedules = new List<CollectorSchedule>();

        LoadSchedules();
    }

    /// <summary>
    /// Gets all configured collector schedules.
    /// </summary>
    public IReadOnlyList<CollectorSchedule> GetAllSchedules()
    {
        lock (_lock)
        {
            return _schedules.ToList();
        }
    }

    /// <summary>
    /// Gets only enabled and scheduled collectors.
    /// </summary>
    public IReadOnlyList<CollectorSchedule> GetEnabledSchedules()
    {
        lock (_lock)
        {
            return _schedules.Where(s => s.Enabled && s.IsScheduled).ToList();
        }
    }

    /// <summary>
    /// Gets collectors that are due to run.
    /// </summary>
    public IReadOnlyList<CollectorSchedule> GetDueCollectors()
    {
        lock (_lock)
        {
            return _schedules.Where(s => s.IsDue).ToList();
        }
    }

    /// <summary>
    /// Gets on-load only collectors (frequency = 0).
    /// </summary>
    public IReadOnlyList<CollectorSchedule> GetOnLoadCollectors()
    {
        lock (_lock)
        {
            return _schedules.Where(s => s.Enabled && !s.IsScheduled).ToList();
        }
    }

    /// <summary>
    /// Gets a specific collector schedule by name.
    /// </summary>
    public CollectorSchedule? GetSchedule(string collectorName)
    {
        lock (_lock)
        {
            return _schedules.FirstOrDefault(s =>
                s.Name.Equals(collectorName, StringComparison.OrdinalIgnoreCase));
        }
    }

    /// <summary>
    /// Marks a collector as having been run.
    /// </summary>
    public void MarkCollectorRun(string collectorName, DateTime runTime)
    {
        lock (_lock)
        {
            var schedule = _schedules.FirstOrDefault(s =>
                s.Name.Equals(collectorName, StringComparison.OrdinalIgnoreCase));

            if (schedule != null)
            {
                schedule.LastRunTime = runTime;

                if (schedule.IsScheduled)
                {
                    schedule.NextRunTime = runTime.AddMinutes(schedule.FrequencyMinutes);
                }

                _logger?.LogDebug("Marked collector '{Name}' as run at {Time}, next run at {NextTime}",
                    collectorName, runTime, schedule.NextRunTime);
            }
        }
    }

    /// <summary>
    /// Updates a collector's schedule settings.
    /// </summary>
    public void UpdateSchedule(string collectorName, bool? enabled = null, int? frequencyMinutes = null, int? retentionDays = null)
    {
        lock (_lock)
        {
            var schedule = _schedules.FirstOrDefault(s =>
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
    /// Detects which preset matches the current intervals, or returns "Custom".
    /// </summary>
    public string GetActivePreset()
    {
        lock (_lock)
        {
            foreach (var (presetName, intervals) in s_presets)
            {
                bool matches = true;
                foreach (var (collector, freq) in intervals)
                {
                    var schedule = _schedules.FirstOrDefault(s =>
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
    }

    /// <summary>
    /// Applies a named preset, changing all scheduled collector frequencies.
    /// Does not modify enabled/disabled state or on-load (frequency=0) collectors.
    /// </summary>
    public void ApplyPreset(string presetName)
    {
        if (!s_presets.TryGetValue(presetName, out var intervals))
        {
            throw new ArgumentException($"Unknown preset: {presetName}");
        }

        lock (_lock)
        {
            foreach (var (collector, freq) in intervals)
            {
                var schedule = _schedules.FirstOrDefault(s =>
                    s.Name.Equals(collector, StringComparison.OrdinalIgnoreCase));
                if (schedule != null)
                {
                    schedule.FrequencyMinutes = freq;
                }
            }

            SaveSchedules();

            _logger?.LogInformation("Applied collection preset '{Preset}'", presetName);
        }
    }

    /// <summary>
    /// Loads schedules from the JSON config file.
    /// </summary>
    private void LoadSchedules()
    {
        if (!File.Exists(_schedulePath))
        {
            _logger?.LogInformation("Schedule file not found, using defaults");
            _schedules = GetDefaultSchedules();
            SaveSchedules();
            return;
        }

        try
        {
            string json = File.ReadAllText(_schedulePath);
            var config = JsonSerializer.Deserialize<ScheduleConfig>(json);
            _schedules = config?.Collectors ?? GetDefaultSchedules();

            /* Create backup of valid config */
            try { File.Copy(_schedulePath, _schedulePath + ".bak", overwrite: true); }
            catch { /* best effort */ }

            _logger?.LogInformation("Loaded {Count} collector schedules from configuration", _schedules.Count);

            /* Add any new default collectors that are missing from the saved config */
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
                    var bakConfig = JsonSerializer.Deserialize<ScheduleConfig>(bakJson);
                    _schedules = bakConfig?.Collectors ?? GetDefaultSchedules();
                    _logger?.LogInformation("Restored schedules from backup file");
                    return;
                }
                catch { /* backup also corrupt, fall through to defaults */ }
            }

            _schedules = GetDefaultSchedules();
            SaveSchedules();
        }
    }

    /// <summary>
    /// Merges any new default collectors that are missing from the loaded config,
    /// and removes any obsolete/renamed collectors that no longer have a dispatch case.
    /// This handles the case where new collectors are added to the code but the user
    /// has an existing config file from an older version.
    /// </summary>
    private void MergeNewDefaults()
    {
        var defaults = GetDefaultSchedules();
        var defaultNames = new HashSet<string>(defaults.Select(s => s.Name), StringComparer.OrdinalIgnoreCase);
        var loadedNames = new HashSet<string>(_schedules.Select(s => s.Name), StringComparer.OrdinalIgnoreCase);
        var changed = false;

        /* Remove obsolete collectors that are no longer in the defaults
           (e.g., blocking_snapshot was renamed to blocked_process_report) */
        var removed = _schedules.RemoveAll(s => !defaultNames.Contains(s.Name));
        if (removed > 0)
        {
            _logger?.LogInformation("Removed {Count} obsolete collector(s) from schedule", removed);
            changed = true;
        }

        /* Add any new default collectors that are missing */
        foreach (var defaultSchedule in defaults)
        {
            if (!loadedNames.Contains(defaultSchedule.Name))
            {
                _schedules.Add(defaultSchedule);
                _logger?.LogInformation("Added missing collector '{Name}' from defaults", defaultSchedule.Name);
                changed = true;
            }
        }

        if (changed)
        {
            SaveSchedules();
        }
    }

    /// <summary>
    /// Saves schedules to the JSON config file.
    /// </summary>
    public void SaveSchedules()
    {
        lock (_lock)
        {
            try
            {
                var config = new ScheduleConfig { Collectors = _schedules };
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
    /// Gets the default collector schedules.
    /// </summary>
    private static List<CollectorSchedule> GetDefaultSchedules()
    {
        return new List<CollectorSchedule>
        {
            new() { Name = "wait_stats", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "Wait statistics from sys.dm_os_wait_stats" },
            new() { Name = "query_stats", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "Query statistics from sys.dm_exec_query_stats" },
            new() { Name = "procedure_stats", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "Stored procedure statistics from sys.dm_exec_procedure_stats" },
            new() { Name = "query_store", Enabled = true, FrequencyMinutes = 5, RetentionDays = 30, Description = "Query Store data (top 100 queries per database)" },
            new() { Name = "query_snapshots", Enabled = true, FrequencyMinutes = 1, RetentionDays = 7, Description = "Currently running queries snapshot" },
            new() { Name = "cpu_utilization", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "CPU utilization from ring buffer" },
            new() { Name = "file_io_stats", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "File I/O statistics from sys.dm_io_virtual_file_stats" },
            new() { Name = "memory_stats", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "Memory statistics from sys.dm_os_sys_memory and performance counters" },
            new() { Name = "memory_clerks", Enabled = true, FrequencyMinutes = 5, RetentionDays = 30, Description = "Memory clerk allocations from sys.dm_os_memory_clerks" },
            new() { Name = "tempdb_stats", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "TempDB space usage from sys.dm_db_file_space_usage" },
            new() { Name = "perfmon_stats", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "Key performance counters from sys.dm_os_performance_counters" },
            new() { Name = "deadlocks", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "Deadlocks from system_health extended event session" },
            new() { Name = "server_config", Enabled = true, FrequencyMinutes = 0, RetentionDays = 30, Description = "Server configuration (on-load only)" },
            new() { Name = "database_config", Enabled = true, FrequencyMinutes = 0, RetentionDays = 30, Description = "Database configuration (on-load only)" },
            new() { Name = "memory_grant_stats", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "Memory grant statistics from sys.dm_exec_query_memory_grants" },
            new() { Name = "waiting_tasks", Enabled = true, FrequencyMinutes = 1, RetentionDays = 7, Description = "Point-in-time waiting tasks from sys.dm_os_waiting_tasks" },
            new() { Name = "blocked_process_report", Enabled = true, FrequencyMinutes = 1, RetentionDays = 30, Description = "Blocked process reports from XE ring buffer session (opt-out)" },
            new() { Name = "database_scoped_config", Enabled = true, FrequencyMinutes = 0, RetentionDays = 30, Description = "Database-scoped configurations (on-load only)" },
            new() { Name = "trace_flags", Enabled = true, FrequencyMinutes = 0, RetentionDays = 30, Description = "Active trace flags via DBCC TRACESTATUS (on-load only)" },
            new() { Name = "running_jobs", Enabled = true, FrequencyMinutes = 5, RetentionDays = 7, Description = "Currently running SQL Agent jobs with duration comparison" },
            new() { Name = "database_size_stats", Enabled = true, FrequencyMinutes = 60, RetentionDays = 90, Description = "Database file sizes for growth trending and capacity planning" },
            new() { Name = "server_properties", Enabled = true, FrequencyMinutes = 0, RetentionDays = 365, Description = "Server edition, licensing, CPU/memory hardware metadata (on-load only)" },
            new() { Name = "session_stats", Enabled = true, FrequencyMinutes = 5, RetentionDays = 30, Description = "Per-application session counts from sys.dm_exec_sessions" }
        };
    }

    /// <summary>
    /// JSON wrapper for schedules list.
    /// </summary>
    private class ScheduleConfig
    {
        [JsonPropertyName("collectors")]
        public List<CollectorSchedule> Collectors { get; set; } = new();
    }
}
