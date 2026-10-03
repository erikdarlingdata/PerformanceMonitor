/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
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

    /// <summary>#4938: what a run time needs to know about each server: the stable id its spread is taken from, the
    /// name a warning shows, and the server's clock (null until a server_properties row has been read, when the run
    /// time reads as UTC). Set by RemoteCollectorService.</summary>
    private readonly Dictionary<string, RunTimeServer> _runTimeServers = new();

    /// <summary>#4938: the ignored run times already warned about, so a bad value logs once, not every cycle.</summary>
    private readonly HashSet<string> _warnedRunTimes = new();

    private sealed record RunTimeServer(int StorageId, string Name, ServerClock? Clock);

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
    public void UpdateSchedule(string collectorName, bool? enabled = null, int? frequencyMinutes = null, int? retentionDays = null,
        string? runAt = null, bool changeRunAt = false)
    {
        lock (_lock)
        {
            var schedule = _defaultSchedule.FirstOrDefault(s =>
                s.Name.Equals(collectorName, StringComparison.OrdinalIgnoreCase));

            if (schedule == null)
            {
                throw new InvalidOperationException($"Collector '{collectorName}' not found");
            }

            /* Refuse before mutating anything, so a bad frequency can't leave a half-applied update. */
            if (frequencyMinutes.HasValue
                && FrequencyError(collectorName, frequencyMinutes.Value) is string frequencyError)
            {
                throw new InvalidOperationException(frequencyError);
            }

            /* #4938: the run time that will stand after this update (the one passed, null clearing it, or the one already
               set) must fit the frequency that will stand, so changing a daily collector to hourly while it has a run
               time is refused with the editor's text instead of leaving a run time that is then ignored. */
            var standingRunAt = changeRunAt ? NormalizeRunAt(runAt) : schedule.RunAt;
            if (RunAtError(collectorName, frequencyMinutes ?? schedule.FrequencyMinutes, standingRunAt) is string runAtError)
            {
                throw new InvalidOperationException(runAtError);
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

            if (changeRunAt)
            {
                schedule.RunAt = standingRunAt;
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
    ///
    /// <para>#3929/#3930: an on-load collector (<c>!IsScheduled</c>, FrequencyMinutes 0) is no longer excluded
    /// here — it becomes due on <see cref="CollectorScheduleDefaults.OnLoadRecaptureMinutes"/> too, the same
    /// substitution Darling's worker makes, so a server tab left open for weeks still re-captures its config
    /// snapshot (#3930) and a trace flag turned off since the last connect eventually clears (#3929) instead of
    /// only on the next reconnect. The tab-open path (<see cref="RemoteCollectorService.RunAllCollectorsForServerAsync"/>)
    /// still runs every on-load collector (the on-connect capture is unchanged), but since #4938 it takes its other
    /// collectors from <see cref="GetCollectorsForTabOpen"/> instead of running them all.</para>
    ///
    /// <para>#4938: a collector with a run time (<see cref="CollectorSchedule.RunAt"/>) is due once a day inside the
    /// 60-minute grace that follows its slot, by <see cref="CollectorRunTime.NextDue"/>, and a daily collector's last
    /// run is the one <see cref="SeedLastRunsForServer"/> read from collection_log at start-up.</para>
    /// </summary>
    public IReadOnlyList<CollectorSchedule> GetDueCollectorsForServer(string serverId)
        => GetDueCollectorsForServer(serverId, DateTime.UtcNow);

    /// <summary>The collectors due at <paramref name="atUtc"/>: the same rule as the one-argument overload, evaluated
    /// at the caller's logical cycle time (#4640). A collector whose recorded run is later than
    /// <paramref name="atUtc"/> is due now (#4732, <see cref="CollectorCadence.ClampDue"/>). That is a wall clock that
    /// stepped backwards since the run was recorded, or a tab-open or refresh run
    /// (<see cref="RemoteCollectorService.RunAllCollectorsForServerAsync"/>) that recorded its own start time after the
    /// cycle's time was taken.</summary>
    public IReadOnlyList<CollectorSchedule> GetDueCollectorsForServer(string serverId, DateTime atUtc)
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
                if (!s.Enabled)
                    continue;

                if (IsDue(serverId, s, runState, atUtc))
                {
                    due.Add(s);
                }
            }

            return due;
        }
    }

    /// <summary>
    /// #4938: whether one collector is due. A collector with a run time (a valid <c>run_at</c> on an interval of whole
    /// days) asks <see cref="CollectorRunTime.NextDue"/> and is due when the answer is at or before
    /// <paramref name="atUtc"/>: from the day's time to the end of the 60-minute grace, once a day. A never-run
    /// collector, or one that missed its day, waits for the next day's time. Every other collector keeps the interval rule.
    /// </summary>
    private bool IsDue(string serverId, CollectorSchedule s, Dictionary<string, DateTime>? runState, DateTime atUtc)
    {
        var intervalMinutes = CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(s.FrequencyMinutes);
        DateTime? lastRun = runState != null && runState.TryGetValue(s.Name, out var last) ? last : null;

        if (ResolveRunAtMinute(serverId, s, intervalMinutes) is int runAtMinute)
        {
            var (spreadId, localToUtc, _) = RunTimeBasis(serverId);
            var next = CollectorRunTime.NextDue(atUtc, lastRun, runAtMinute, intervalMinutes, spreadId, localToUtc);

            /* The clock-step check with the room a run-time stamp needs (an interval, the spread, a 25-hour day). */
            return CollectorCadence.ClampDue(next, atUtc, CollectorRunTime.MaxStampAhead(intervalMinutes)) <= atUtc;
        }

        if (lastRun is not { } ran)
        {
            return true; // never run — due immediately
        }

        /* #4732: due when lastRun plus the interval has come, decided through the shared clamp. A run recorded
           after atUtc is either what a wall clock that stepped backwards leaves behind for every collector (the
           plain elapsed check would hold each one back for as long as the step), or a tab-open or refresh run
           that recorded its own start time after this cycle's time was taken (RemoteCollectorService, only the
           collectors it ran); the clamp counts either as due now. A run one interval or more before atUtc is
           due and one less than an interval before is not, exactly as before. */
        var interval = TimeSpan.FromMinutes(intervalMinutes);
        return CollectorCadence.ClampDue(ran + interval, atUtc, interval) <= atUtc;
    }

    /// <summary>
    /// #4938: the collector's run time in minutes after midnight on the server's clock, or null when it has none that
    /// applies. A value that is not a 24-hour HH:MM time, or that sits on an interval that is not a whole number of
    /// days, is ignored with one warning that names the collector and the server.
    /// </summary>
    private int? ResolveRunAtMinute(string serverId, CollectorSchedule s, int intervalMinutes)
    {
        if (string.IsNullOrWhiteSpace(s.RunAt))
        {
            return null;
        }

        var serverName = _runTimeServers.TryGetValue(serverId, out var server) ? server.Name : serverId;
        if (!CollectorRunTime.TryParse(s.RunAt, out var minute))
        {
            if (_warnedRunTimes.Add($"{serverId}|{s.Name}|{s.RunAt}|format"))
            {
                _logger?.LogWarning("Collector '{Name}' on server '{Server}' has run_at '{RunAt}'. {Message} The run time is ignored.",
                    s.Name, serverName, s.RunAt, CollectorRunTime.InvalidRunAtMessage);
            }

            return null;
        }

        if (!CollectorRunTime.AllowsRunAt(intervalMinutes))
        {
            if (_warnedRunTimes.Add($"{serverId}|{s.Name}|{s.RunAt}|{intervalMinutes}"))
            {
                _logger?.LogWarning("{Message} The run time {RunAt} on server '{Server}' is ignored.",
                    CollectorRunTime.IntervalRefusalMessage(s.Name, intervalMinutes), s.RunAt, serverName);
            }

            return null;
        }

        return minute;
    }

    /// <summary>
    /// #4938: what a run time on this server is computed from. The spread takes an int id: the stable id Lite stores
    /// for the server (the deterministic hash of its storage name), registered by RemoteCollectorService; before one is
    /// registered, a hash of the connection id. The conversion is the server's clock, or UTC until one is known (the
    /// third value says which). Called with the lock held.
    /// </summary>
    private (int SpreadId, Func<DateTime, DateTime> LocalToUtc, ServerClock? Clock) RunTimeBasis(string serverId)
    {
        _runTimeServers.TryGetValue(serverId, out var server);
        var spreadId = server?.StorageId ?? PerformanceMonitor.Common.ServerIdHelper.GetDeterministicHashCode(serverId);
        Func<DateTime, DateTime> localToUtc = server?.Clock is { } clock ? clock.ToUtc : CollectorRunTime.LocalIsUtc;
        return (spreadId, localToUtc, server?.Clock);
    }

    /// <summary>
    /// #4938: one line per enabled collector that has a valid run time, for the schedule editor, so a collector that
    /// waits for its time shows when. With a <paramref name="serverId"/> a line gives the exact next run, in the server's
    /// clock and in UTC; with none (the default schedule, which every server without a custom schedule uses) it gives the
    /// hour the servers spread across. A server Lite has not collected from yet has no clock or run history, and its
    /// line says so. A collector whose time falls inside today's hour and has not run yet shows as due now.
    /// </summary>
    internal IReadOnlyList<string> DescribeRunTimes(string? serverId, IEnumerable<CollectorSchedule> schedules, DateTime atUtc)
    {
        var lines = new List<string>();
        lock (_lock)
        {
            foreach (var s in schedules)
            {
                if (!s.Enabled
                    || RunAtError(s.Name, s.FrequencyMinutes, s.RunAt) is not null
                    || !CollectorRunTime.TryParse(s.RunAt, out var runAtMinute))
                {
                    continue;
                }

                if (serverId is null)
                {
                    var end = CollectorRunTime.Format((runAtMinute + CollectorRunTime.GraceMinutes) % 1440);
                    lines.Add(string.Create(CultureInfo.InvariantCulture,
                        $"{s.Name}: runs between {CollectorRunTime.Format(runAtMinute)} and {end} on each server's clock, each server at its own minute."));
                    continue;
                }

                if (!_runTimeServers.ContainsKey(serverId))
                {
                    lines.Add($"{s.Name}: the next run shows once Lite has collected from this server.");
                    continue;
                }

                var intervalMinutes = CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(s.FrequencyMinutes);
                _serverRunState.TryGetValue(serverId, out var runState);
                DateTime? lastRun = runState != null && runState.TryGetValue(s.Name, out var last) ? last : null;
                var (spreadId, localToUtc, clock) = RunTimeBasis(serverId);
                var next = CollectorRunTime.NextDue(atUtc, lastRun, runAtMinute, intervalMinutes, spreadId, localToUtc);

                if (next <= atUtc)
                {
                    lines.Add($"{s.Name}: due now, inside today's hour.");
                    continue;
                }

                lines.Add(clock is null
                    ? string.Create(CultureInfo.InvariantCulture,
                        $"{s.Name}: next run {next:yyyy-MM-dd HH:mm} UTC (the server's clock is not known yet, so the run time is read as UTC).")
                    : string.Create(CultureInfo.InvariantCulture,
                        $"{s.Name}: next run {clock.ToServerLocal(next):yyyy-MM-dd HH:mm} server time ({next:yyyy-MM-dd HH:mm} UTC)."));
            }
        }

        return lines;
    }

    /// <summary>One collector's run time on one server: the minutes after midnight on the server's clock, the interval
    /// the run time is judged on (a whole number of days), and whether the collector is enabled.</summary>
    internal readonly record struct RunTimeSetting(string Collector, int RunAtMinute, int IntervalMinutes, bool Enabled);

    /// <summary>
    /// #4938: which of <paramref name="collectors"/> have a run time that applies on one server, read from the schedule
    /// the sweep reads (the server's own schedule when it has one, else the default), through the same check
    /// (<see cref="ResolveRunAtMinute"/>): a value that is not an HH:MM time, or that sits on an interval that is not a
    /// whole number of days, is none. The server is the stable storage id the health reads are keyed by
    /// (<see cref="RemoteCollectorService.GetServerId"/>), looked up in <paramref name="servers"/> the way
    /// <see cref="GetFrequencyForStorageServer"/> does; a server the list no longer holds has none. A disabled collector
    /// is listed, so a reader can tell "no run time" from "disabled".
    /// </summary>
    internal IReadOnlyList<RunTimeSetting> GetRunTimeSettingsForStorageServer(
        ServerManager servers, int storageServerId, IEnumerable<string> collectors)
    {
        var settings = new List<RunTimeSetting>();
        var server = servers.GetAllServers().FirstOrDefault(s =>
            RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(s)) == storageServerId);
        if (server is null)
        {
            return settings;
        }

        lock (_lock)
        {
            foreach (var name in collectors)
            {
                if (GetScheduleForServer(server.Id, name) is not { } schedule)
                {
                    continue;
                }

                var intervalMinutes = CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(schedule.FrequencyMinutes);
                if (ResolveRunAtMinute(server.Id, schedule, intervalMinutes) is int minute)
                {
                    settings.Add(new RunTimeSetting(schedule.Name, minute, intervalMinutes, schedule.Enabled));
                }
            }
        }

        return settings;
    }

    /// <summary>
    /// #4938: tells the scheduler what a run time needs about a server: the stable id the spread is taken from
    /// (<c>RemoteCollectorService.GetServerId</c>, the deterministic hash of the storage name), the name warnings show,
    /// and the server's clock from its newest server_properties row. A null clock reads the run time as UTC until one
    /// arrives; every due check uses the clock set last, so a new clock moves the next slot with no other step.
    /// </summary>
    public void SetServerRunContext(string serverId, int storageServerId, string serverName, ServerClock? clock)
    {
        lock (_lock)
        {
            _runTimeServers[serverId] = new RunTimeServer(storageServerId, serverName, clock);
        }
    }

    /// <summary>
    /// #4938: the collectors the tab-open run starts, in schedule order. An on-load collector (frequency 0) always
    /// runs: that is its connect capture, and a run time moves only its daily re-capture. A collector with a run time
    /// does not run, because its time owns it. A collector that runs once a day or less often and is not due does not
    /// run either, so opening a tab no longer re-runs it. Every other enabled collector runs, as before.
    /// </summary>
    public IReadOnlyList<CollectorSchedule> GetCollectorsForTabOpen(string serverId, DateTime atUtc)
    {
        lock (_lock)
        {
            var schedules = _serverOverrides.TryGetValue(serverId, out var over)
                ? over.Collectors
                : _defaultSchedule;

            _serverRunState.TryGetValue(serverId, out var runState);

            var run = new List<CollectorSchedule>();
            foreach (var s in schedules)
            {
                if (!s.Enabled)
                    continue;

                if (s.IsScheduled)
                {
                    var intervalMinutes = CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(s.FrequencyMinutes);
                    if (ResolveRunAtMinute(serverId, s, intervalMinutes) is not null)
                        continue;

                    if (IsDailyOrLonger(intervalMinutes) && !IsDue(serverId, s, runState, atUtc))
                        continue;
                }

                run.Add(s);
            }

            return run;
        }
    }

    private const int DailyIntervalMinutes = 1440;

    /// <summary>
    /// #4938: the one rule for a daily-or-longer collector: an effective interval of a day or more, whole days or not.
    /// The tab-open selection, the start-up read of last runs and a failed attempt all ask it, so a collector one of
    /// them counts is a collector all of them count. It is not <see cref="CollectorRunTime.AllowsRunAt"/>, which is
    /// narrower on purpose: a run time needs a whole number of days, and a 2000-minute collector has none to set.
    /// </summary>
    internal static bool IsDailyOrLonger(int effectiveIntervalMinutes) => effectiveIntervalMinutes >= DailyIntervalMinutes;

    /// <summary>
    /// #4938: the enabled collectors whose effective interval is a day or more (the on-load ones recur daily), with
    /// that interval. These are the ones whose last run is read from collection_log at start-up.
    /// </summary>
    public IReadOnlyDictionary<string, int> GetDailyCollectorIntervalsForServer(string serverId)
    {
        lock (_lock)
        {
            var schedules = _serverOverrides.TryGetValue(serverId, out var over)
                ? over.Collectors
                : _defaultSchedule;

            var daily = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            foreach (var s in schedules.Where(s => s.Enabled))
            {
                var intervalMinutes = CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(s.FrequencyMinutes);
                if (IsDailyOrLonger(intervalMinutes))
                {
                    daily[s.Name] = intervalMinutes;
                }
            }

            return daily;
        }
    }

    /// <summary>
    /// #4938: fills in the last run of collectors from the local collection_log at start-up. The run state lives in
    /// memory, so without this every collector is "never run, due now" after a launch. A run already on record
    /// stays when it is the later of the two.
    /// </summary>
    public void SeedLastRunsForServer(string serverId, IReadOnlyDictionary<string, DateTime> lastRuns)
    {
        lock (_lock)
        {
            if (!_serverRunState.TryGetValue(serverId, out var runState))
            {
                runState = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
                _serverRunState[serverId] = runState;
            }

            foreach (var (name, ran) in lastRuns)
            {
                if (!runState.TryGetValue(name, out var existing) || existing < ran)
                {
                    runState[name] = ran;
                }
            }
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
    /// A server's EFFECTIVE cadence for one collector, keyed the way the analysis pipeline and the alert read
    /// adapter key servers — the deterministic storage-name hash — where this class keys them by connection
    /// GUID. Null for a server the list no longer holds or a collector the schedule does not know, which
    /// every caller reads as "use the shipped default". #3896's analysis lookback and the alert adapter's
    /// snapshot-freshness bounds (#1812/#1839) both resolve through here.
    /// </summary>
    public int? GetFrequencyForStorageServer(ServerManager servers, int serverId, string collectorName)
    {
        var server = servers.GetAllServers().FirstOrDefault(s =>
            RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(s)) == serverId);
        return server is null ? null : GetScheduleForServer(server.Id, collectorName)?.FrequencyMinutes;
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
    /// #4938: records an attempt that did not succeed (an error, a denied permission, a declined sign-in, a lock yield)
    /// as the run of a daily-or-longer collector (an effective interval of a day or more, whole days or not, by
    /// <see cref="IsDailyOrLonger"/>: the rule the start-up read uses), with or without a run time. The attempt wrote its
    /// collection_log row, and the start-up read counts every row as the last run whatever its status, so the session
    /// counts it too: a collector that keeps failing runs once, not on every sweep, and is due again an interval after
    /// the attempt (with a run time, inside the 60-minute hour that follows its next time). A collector that runs more
    /// often than that, one the schedule does not list, and one that is switched off are left alone: they stay due on
    /// the next sweep, as before.
    /// </summary>
    public void MarkCollectorAttemptForServer(string serverId, string collectorName, DateTime attemptTime)
    {
        lock (_lock)
        {
            var schedules = _serverOverrides.TryGetValue(serverId, out var over)
                ? over.Collectors
                : _defaultSchedule;

            var schedule = schedules.FirstOrDefault(s =>
                s.Name.Equals(collectorName, StringComparison.OrdinalIgnoreCase));
            if (schedule is null
                || !schedule.Enabled
                || !IsDailyOrLonger(CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(schedule.FrequencyMinutes)))
            {
                return;
            }

            if (!_serverRunState.TryGetValue(serverId, out var runState))
            {
                runState = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
                _serverRunState[serverId] = runState;
            }

            runState[collectorName] = attemptTime;

            _logger?.LogDebug("Marked collector '{Name}' as attempted for server {ServerId} at {Time}; the attempt ends its period's run",
                collectorName, serverId, attemptTime);
        }
    }

    /// <summary>
    /// Creates or updates a per-server schedule override.
    /// </summary>
    public void SetScheduleForServer(string serverId, List<CollectorSchedule> schedules)
    {
        lock (_lock)
        {
            foreach (var schedule in schedules)
            {
                if (FrequencyError(schedule.Name, schedule.FrequencyMinutes) is string frequencyError)
                {
                    throw new InvalidOperationException(frequencyError);
                }

                /* #4938: a blank run time is none; it is stored as null, so the file carries run_at only where one is set. */
                schedule.RunAt = NormalizeRunAt(schedule.RunAt);
                if (RunAtError(schedule.Name, schedule.FrequencyMinutes, schedule.RunAt) is string runAtError)
                {
                    throw new InvalidOperationException(runAtError);
                }
            }

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
            SanitizeDeltaFrequencies();
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
                        SanitizeDeltaFrequencies();
                        return;
                    }

                    var bakConfig = JsonSerializer.Deserialize<ScheduleConfigV1>(bakJson);
                    _defaultSchedule = bakConfig?.Collectors ?? GetDefaultSchedules();
                    _serverOverrides = new Dictionary<string, ServerScheduleOverride>();
                    _logger?.LogInformation("Restored v1 schedules from backup file");
                    SanitizeDeltaFrequencies();
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
    /// Why a frequency can't be honored for this collector, or null when it can (#3532). Negative is
    /// nonsense on any collector (0 = on-load only), and a delta-family collector past
    /// <see cref="CollectorDeltaCalculator.MaxDeltaFrequencyMinutes"/> would exceed the shared delta gap
    /// policy every cycle and record permanent zeros. The editor shows this message before saving; the
    /// write APIs throw it as a backstop.
    /// </summary>
    internal static string? FrequencyError(string collectorName, int frequencyMinutes)
    {
        if (frequencyMinutes < 0)
        {
            return $"'{collectorName}': frequency (minutes) can't be negative. Use 0 to collect once on server load.";
        }

        return CollectorDeltaCalculator.DeltaFrequencyError(collectorName, frequencyMinutes);
    }

    /// <summary>The note the editor shows beside the run time: Lite collects only while it is open (#4938).</summary>
    internal const string RunAtLiteClosedNote =
        "Lite collects only while it is open. If Lite is closed at this time, that day's run is skipped.";

    /// <summary>
    /// #4938: why a run time can't be honored for this collector, or null when it can. A blank run time is none, and
    /// always fine. Otherwise it must be a 24-hour HH:MM time, and the collector must run once a day or less often:
    /// judged on the effective interval, so an on-load collector's daily re-run counts and an hourly collector is
    /// refused. The texts are the shared ones (<see cref="CollectorRunTime"/>), so the editors and the CLI agree. The
    /// editor shows this message before saving; the write APIs throw it as a backstop.
    /// </summary>
    internal static string? RunAtError(string collectorName, int frequencyMinutes, string? runAt)
    {
        if (string.IsNullOrWhiteSpace(runAt))
        {
            return null;
        }

        if (!CollectorRunTime.TryParse(runAt, out _))
        {
            return CollectorRunTime.InvalidRunAtMessage;
        }

        var intervalMinutes = CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(frequencyMinutes);
        return CollectorRunTime.AllowsRunAt(intervalMinutes)
            ? null
            : CollectorRunTime.IntervalRefusalMessage(collectorName, intervalMinutes);
    }

    /// <summary>#4938: a blank run time is none (null); anything else loses its surrounding spaces.</summary>
    internal static string? NormalizeRunAt(string? runAt) => string.IsNullOrWhiteSpace(runAt) ? null : runAt.Trim();

    /// <summary>
    /// Clamps any loaded delta-family frequency above the gap-policy cap back to the cap (#3532) — the
    /// write APIs refuse such a cadence, but a hand-edited or pre-fix collection_schedule.json can still
    /// carry one, and honoring it would fabricate permanent quiet (every cycle past the gap policy
    /// re-baselines and stores a zero delta). Load-time has no user to bounce the value back to, so it
    /// clamps and logs instead of refusing. Saves when anything changed.
    /// </summary>
    private void SanitizeDeltaFrequencies()
    {
        var changed = false;

        var lists = new List<List<CollectorSchedule>> { _defaultSchedule };
        foreach (var over in _serverOverrides.Values)
        {
            lists.Add(over.Collectors);
        }

        foreach (var list in lists)
        {
            foreach (var schedule in list)
            {
                if (schedule.FrequencyMinutes > CollectorDeltaCalculator.MaxDeltaFrequencyMinutes
                    && CollectorDeltaCalculator.IsDeltaFamily(schedule.Name))
                {
                    _logger?.LogWarning(
                        "Collector '{Name}' was scheduled every {Bad}m, above the {Max}m cap for delta collectors — past the {Policy}s delta gap policy every reading would be discarded and recorded as zero. Clamped to {Max}m.",
                        schedule.Name, schedule.FrequencyMinutes, CollectorDeltaCalculator.MaxDeltaFrequencyMinutes,
                        CollectorDeltaCalculator.DefaultMaxGapSeconds, CollectorDeltaCalculator.MaxDeltaFrequencyMinutes);
                    schedule.FrequencyMinutes = CollectorDeltaCalculator.MaxDeltaFrequencyMinutes;
                    changed = true;
                }
            }
        }

        if (changed)
        {
            SaveSchedules();
        }
    }

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
            Description = s.Description,
            RunAt = s.RunAt
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
            new() { Name = "long_query_completions", Enabled = false, FrequencyMinutes = 1, RetentionDays = 30, Description = "Long-running query completions (rpc_completed/sql_batch_completed >= threshold, plus attention/cancels) from this install's own XE ring buffer session. OPT-IN, default OFF: enabling creates the session on monitored servers, disabling drops it unless another registration of this install keeps it (#1496). On Azure SQL Database the session is created in each monitored database." },
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
