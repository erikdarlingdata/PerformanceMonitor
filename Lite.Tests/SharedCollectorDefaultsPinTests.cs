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
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Identity-pins between Lite's own defaults and the shared constants the Darling service
/// consumes (headless plan M2 slice A): the bundled ignored-wait-types json must equal
/// <see cref="IgnoredWaitDefaults"/>, ScheduleManager's default table must equal
/// <see cref="CollectorScheduleDefaults"/>, and the delta calculator / server-identity idioms
/// must be the shared implementations — so the two SKUs cannot drift apart silently.
/// </summary>
public sealed class SharedCollectorDefaultsPinTests
{
    private static string FindRepoFile(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (int i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (File.Exists(candidate))
            {
                return candidate;
            }
            dir = Path.GetDirectoryName(dir);
        }
        throw new FileNotFoundException($"Could not locate {relativePath} walking up from {AppContext.BaseDirectory}");
    }

    [Fact]
    public void IgnoredWaitDefaults_MatchLiteBundledJson()
    {
        var jsonPath = FindRepoFile(Path.Combine("Lite", "config", "ignored_wait_types.json"));
        using var doc = JsonDocument.Parse(File.ReadAllText(jsonPath));
        var fromJson = doc.RootElement.GetProperty("ignored_waits")
            .EnumerateArray()
            .Select(e => e.GetString()!)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        Assert.Equal(124, fromJson.Count);
        Assert.Equal(fromJson, IgnoredWaitDefaults.All.ToHashSet(StringComparer.OrdinalIgnoreCase));
    }

    [Fact]
    public void CollectorScheduleDefaults_MatchScheduleManagerTable()
    {
        var liteDefaults = ScheduleManager.GetDefaultSchedules();

        /* The SQL Server subset of the shared defaults, not all of it. The catalog is engine-mixed and Lite
           has no PostgreSQL target, so its schedule table correctly does not list the PostgreSQL collectors —
           a phantom row for a collector this SKU can never dispatch would show up in Lite's own schedule UI. */
        var sqlServerDefaults = CollectorCatalog.All
            .Where(d => d.TargetEngine == CollectorTargetEngine.SqlServer)
            .Select(d => d.Name)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        Assert.Equal(liteDefaults.Count, sqlServerDefaults.Count);
        foreach (var schedule in liteDefaults)
        {
            Assert.True(CollectorScheduleDefaults.All.TryGetValue(schedule.Name, out var shared),
                $"shared defaults missing collector '{schedule.Name}'");
            Assert.Equal(schedule.FrequencyMinutes, shared!.FrequencyMinutes);
            Assert.Equal(schedule.RetentionDays, shared.RetentionDays);
            /* The seeded enabled state must match the shared DefaultEnabled — so an opt-in collector
               (long_query_completions, #1496) can't ship enabled in one SKU and disabled in the other. */
            Assert.Equal(schedule.Enabled, shared.DefaultEnabled);
        }
    }

    /// <summary>
    /// #1963, ruled 2026-08-01: deadlocks defaults to the 5-minute tier, beside the other event-buffer
    /// readers. The read costs ~273ms of FIXED serialization regardless of buffer content, the buffer
    /// retains events between polls, and a deadlock is forensic by the time anyone reads it - so the
    /// per-minute cadence bought latency nobody could use at real fleet cost. The identity pins above
    /// only assert the SKUs agree; this pins the ruled VALUE, so a symmetric edit of both tables cannot
    /// silently regress the decision.
    /// </summary>
    [Fact]
    public void Deadlocks_DefaultToTheFiveMinuteTier_Ruled1963()
    {
        Assert.Equal(5, CollectorScheduleDefaults.All["deadlocks"].FrequencyMinutes);
        Assert.Equal(
            CollectorScheduleDefaults.All["system_health_events"].FrequencyMinutes,
            CollectorScheduleDefaults.All["deadlocks"].FrequencyMinutes);
    }

    [Fact]
    public void CollectorScheduleDefaults_CoverEveryCatalogCollector()
    {
        foreach (var definition in CollectorCatalog.All)
        {
            Assert.True(CollectorScheduleDefaults.All.ContainsKey(definition.Name),
                $"no default schedule for catalog collector '{definition.Name}'");
        }
    }

    [Fact]
    public void SchedulePresets_ReferenceOnlyRealCollectors()
    {
        /* Every collector named in a schedule preset must be a real collector in the shared defaults —
           guards against a preset table naming a renamed, removed, or typo'd collector. */
        foreach (var (presetName, intervals) in ScheduleManager.s_presets)
        {
            foreach (var collector in intervals.Keys)
            {
                Assert.True(CollectorScheduleDefaults.All.ContainsKey(collector),
                    $"preset '{presetName}' references unknown collector '{collector}'");
            }
        }
    }

    [Fact]
    public void SchedulePresets_CoverTheSamePinnedCollectorSet()
    {
        /* ScheduleManager.s_presets is now the ONE preset table — the collector-schedule editor window
           reads/applies it instead of holding its own copy (which had drifted ~8 collectors behind).
           Pin the membership: every preset must cover exactly this set, so adding a collector to one
           preset but not the others — or letting the set silently fall behind again — fails here. */
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

        Assert.Equal(3, ScheduleManager.s_presets.Count);
        foreach (var (presetName, intervals) in ScheduleManager.s_presets)
        {
            Assert.True(expected.SetEquals(intervals.Keys),
                $"preset '{presetName}' collector set drifted from the pinned set");
        }
    }

    [Fact]
    public void DeltaCalculator_IsTheSharedCoreImplementation()
    {
        Assert.True(typeof(CollectorDeltaCalculator).IsAssignableFrom(typeof(DeltaCalculator)));

        /* Behavior smoke on the shared core: baseline 0, then delta, counter-reset 0, gap 0. */
        var deltas = new CollectorDeltaCalculator();
        var t0 = new DateTime(2026, 7, 2, 12, 0, 0, DateTimeKind.Utc);
        Assert.Equal(0, deltas.CalculateDelta(1, "c", "k", 100, t0, 300));
        Assert.Equal(50, deltas.CalculateDelta(1, "c", "k", 150, t0.AddSeconds(60), 300));
        Assert.Equal(0, deltas.CalculateDelta(1, "c", "k", 10, t0.AddSeconds(120), 300));   /* reset */
        Assert.Equal(0, deltas.CalculateDelta(1, "c", "k", 500, t0.AddSeconds(120 + 3600), 300)); /* gap */
    }

    [Fact]
    public void ServerStorageName_ForwardsToSharedBuilder()
    {
        Assert.Equal("HOST", PerformanceMonitor.Common.ServerIdHelper.BuildStorageName("HOST", null, false));
        Assert.Equal("HOST:db1", PerformanceMonitor.Common.ServerIdHelper.BuildStorageName("HOST", "db1", false));
        Assert.Equal("HOST:db1:RO", PerformanceMonitor.Common.ServerIdHelper.BuildStorageName("HOST", "db1", true));
        Assert.Equal("HOST:RO", PerformanceMonitor.Common.ServerIdHelper.BuildStorageName("HOST", "", true));
    }
}
