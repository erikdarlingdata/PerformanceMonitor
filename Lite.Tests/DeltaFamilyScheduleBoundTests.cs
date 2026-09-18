/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3532: a delta-family collector scheduled past the shared gap policy takes the reset branch every
/// cycle and fabricates permanent quiet — all (0, 0) deltas, zero facts, a green product that stopped
/// measuring. These tests pin the cadence cap: the census (the delta family equals the calculator's
/// caller set, so a new delta call site can't dodge the bound), the bound's derivation from
/// <see cref="CollectorDeltaCalculator.DefaultMaxGapSeconds"/>, and the three Lite enforcement points
/// (the write APIs refuse, the load path clamps, snapshot collectors stay exempt).
/// </summary>
public sealed class DeltaFamilyScheduleBoundTests : IDisposable
{
    private readonly string _configDir = Directory.CreateTempSubdirectory("pm-lite-schedule-tests-").FullName;

    public void Dispose()
    {
        try { Directory.Delete(_configDir, recursive: true); } catch { /* best effort */ }
    }

    /* ---------------- the census ---------------- */

    /// <summary>
    /// <see cref="CollectorDeltaCalculator.DeltaFamilyCollectors"/> must equal the set of collectors that
    /// actually call the delta calculator (<c>context.Deltas.CalculateDelta*</c>), found by scanning the
    /// shared collector sources. A collector that grows a delta call without joining the family would
    /// accept the poisoned cadence; a listed collector that stopped calling would refuse a cadence that
    /// is now harmless. Both directions fail here.
    /// </summary>
    [Fact]
    public void DeltaFamily_EqualsTheCalculatorCallerSet()
    {
        var collectorsDir = FindRepoDirectory("PerformanceMonitor.Collectors");
        var callSite = new Regex(@"Deltas\s*\.\s*CalculateDelta", RegexOptions.Compiled);
        var namePin = new Regex("override string Name\\s*=>\\s*\"([^\"]+)\"", RegexOptions.Compiled);

        var callers = Directory.EnumerateFiles(collectorsDir, "*.cs", SearchOption.AllDirectories)
            .Select(File.ReadAllText)
            .Where(source => callSite.IsMatch(source))
            .Select(source =>
            {
                var match = namePin.Match(source);
                Assert.True(match.Success, "a file calling Deltas.CalculateDelta has no Name => \"...\" pin — the census can't classify it");
                return match.Groups[1].Value;
            })
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        Assert.True(callers.Count > 0, $"no Deltas.CalculateDelta call sites found under {collectorsDir} — the census regex is broken");
        Assert.Equal(
            CollectorDeltaCalculator.DeltaFamilyCollectors.OrderBy(n => n, StringComparer.OrdinalIgnoreCase),
            callers.OrderBy(n => n, StringComparer.OrdinalIgnoreCase));
    }

    /* ---------------- the bound itself ---------------- */

    [Fact]
    public void Cap_IsHalfTheGapPolicy_SoAMissedCycleStillYieldsARealDelta()
    {
        /* Derived, not coincidental: at the cap, even a gap of two full cadences (one entirely missed
           cycle) is exactly the policy — still inside the strict '>' comparison the reset branch uses. */
        Assert.Equal(CollectorDeltaCalculator.DefaultMaxGapSeconds / 60 / 2, CollectorDeltaCalculator.MaxDeltaFrequencyMinutes);
        Assert.True(CollectorDeltaCalculator.MaxDeltaFrequencyMinutes * 60 * 2 <= CollectorDeltaCalculator.DefaultMaxGapSeconds);
    }

    [Fact]
    public void DeltaFrequencyError_NamesTheCapAndThePolicy()
    {
        var error = CollectorDeltaCalculator.DeltaFrequencyError("wait_stats", 90);

        Assert.NotNull(error);
        Assert.Contains("wait_stats", error);
        Assert.Contains(CollectorDeltaCalculator.MaxDeltaFrequencyMinutes.ToString(), error);
        Assert.Contains($"{CollectorDeltaCalculator.DefaultMaxGapSeconds / 60}-minute delta gap policy", error);
    }

    [Fact]
    public void DeltaFrequencyError_AllowsTheCapOnLoadOnlyAndEveryNonDeltaCadence()
    {
        Assert.Null(CollectorDeltaCalculator.DeltaFrequencyError("wait_stats", CollectorDeltaCalculator.MaxDeltaFrequencyMinutes));
        Assert.Null(CollectorDeltaCalculator.DeltaFrequencyError("wait_stats", 0));
        /* Snapshot collectors keep long cadences — index_object_stats ships at 1440 by default. */
        Assert.Null(CollectorDeltaCalculator.DeltaFrequencyError("index_object_stats", 1440));
        Assert.Null(CollectorDeltaCalculator.DeltaFrequencyError("database_size_stats", 90));
    }

    /* ---------------- Lite's write APIs refuse ---------------- */

    [Fact]
    public void UpdateSchedule_RefusesADeltaCadencePastTheCap_AndNamesThePolicy()
    {
        var manager = new ScheduleManager(_configDir);

        var ex = Assert.Throws<InvalidOperationException>(() => manager.UpdateSchedule("wait_stats", frequencyMinutes: 90));
        Assert.Contains("delta gap policy", ex.Message);
        Assert.Contains(CollectorDeltaCalculator.MaxDeltaFrequencyMinutes.ToString(), ex.Message);

        /* Refused means unchanged — and unsaved. */
        Assert.Equal(1, manager.GetDefaultSchedule().First(s => s.Name == "wait_stats").FrequencyMinutes);
    }

    [Fact]
    public void UpdateSchedule_RefusesNegative_AllowsTheCapAndSnapshotLongCadences()
    {
        var manager = new ScheduleManager(_configDir);

        Assert.Throws<InvalidOperationException>(() => manager.UpdateSchedule("wait_stats", frequencyMinutes: -1));

        manager.UpdateSchedule("wait_stats", frequencyMinutes: CollectorDeltaCalculator.MaxDeltaFrequencyMinutes);
        Assert.Equal(CollectorDeltaCalculator.MaxDeltaFrequencyMinutes,
            manager.GetDefaultSchedule().First(s => s.Name == "wait_stats").FrequencyMinutes);

        /* A snapshot collector is exempt: 90 minutes on database sizes loses nothing. */
        manager.UpdateSchedule("database_size_stats", frequencyMinutes: 90);
        Assert.Equal(90, manager.GetDefaultSchedule().First(s => s.Name == "database_size_stats").FrequencyMinutes);
    }

    [Fact]
    public void SetScheduleForServer_RefusesADeltaCadencePastTheCap()
    {
        var manager = new ScheduleManager(_configDir);

        var schedules = ScheduleManager.GetDefaultSchedules();
        schedules.First(s => s.Name == "latch_stats").FrequencyMinutes = 90;

        var ex = Assert.Throws<InvalidOperationException>(() => manager.SetScheduleForServer("srv1", schedules));
        Assert.Contains("latch_stats", ex.Message);
        Assert.False(manager.HasServerOverride("srv1"));

        /* The same list with the cadence inside the cap saves fine. */
        schedules.First(s => s.Name == "latch_stats").FrequencyMinutes = CollectorDeltaCalculator.MaxDeltaFrequencyMinutes;
        manager.SetScheduleForServer("srv1", schedules);
        Assert.True(manager.HasServerOverride("srv1"));
    }

    /* ---------------- the load path clamps (no user to bounce the value back to) ---------------- */

    [Fact]
    public void LoadSchedules_ClampsAPersistedDeltaCadence_LeavesSnapshotsAlone()
    {
        /* A hand-edited (or pre-fix) collection_schedule.json carrying the poisoned cadence — in the
           default schedule AND in a per-server override. */
        var json = """
        {
          "version": 2,
          "default_schedule": [
            { "name": "wait_stats", "enabled": true, "frequency_minutes": 90, "retention_days": 30 },
            { "name": "database_size_stats", "enabled": true, "frequency_minutes": 90, "retention_days": 90 }
          ],
          "server_overrides": {
            "srv1": {
              "collectors": [
                { "name": "latch_stats", "enabled": true, "frequency_minutes": 240, "retention_days": 30 }
              ]
            }
          }
        }
        """;
        File.WriteAllText(Path.Combine(_configDir, "collection_schedule.json"), json);

        var manager = new ScheduleManager(_configDir);

        Assert.Equal(CollectorDeltaCalculator.MaxDeltaFrequencyMinutes,
            manager.GetDefaultSchedule().First(s => s.Name == "wait_stats").FrequencyMinutes);
        Assert.Equal(CollectorDeltaCalculator.MaxDeltaFrequencyMinutes,
            manager.GetSchedulesForServer("srv1").First(s => s.Name == "latch_stats").FrequencyMinutes);

        /* The snapshot cadence survives untouched, and the clamp was persisted — a fresh load sees it. */
        Assert.Equal(90, manager.GetDefaultSchedule().First(s => s.Name == "database_size_stats").FrequencyMinutes);
        var reloaded = new ScheduleManager(_configDir);
        Assert.Equal(CollectorDeltaCalculator.MaxDeltaFrequencyMinutes,
            reloaded.GetDefaultSchedule().First(s => s.Name == "wait_stats").FrequencyMinutes);
    }

    /* ---------------- presets stay inside the cap ---------------- */

    [Fact]
    public void EveryPreset_KeepsEveryDeltaCollectorInsideTheCap()
    {
        foreach (var (presetName, intervals) in ScheduleManager.s_presets)
        {
            foreach (var (collector, frequency) in intervals)
            {
                Assert.Null(CollectorDeltaCalculator.DeltaFrequencyError(collector, frequency));
            }

            Assert.NotNull(presetName);
        }
    }

    /* ---------------- helpers ---------------- */

    private static string FindRepoDirectory(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (int i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (Directory.Exists(candidate))
            {
                return candidate;
            }
            dir = Path.GetDirectoryName(dir);
        }
        throw new DirectoryNotFoundException($"Could not locate {relativePath} walking up from {AppContext.BaseDirectory}");
    }
}
