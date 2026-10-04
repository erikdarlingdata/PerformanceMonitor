/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// PIN A (parity board §05 A2, round 2): the CROSS-APP schedule-preset VALUE pin. The two per-app preset pins
/// that exist today — Lite's <see cref="SharedCollectorDefaultsPinTests.SchedulePresets_CoverTheSamePinnedCollectorSet"/>
/// and Darling's <c>ViewerControlPlaneStage3bTests.Presets_CoverTheSamePinnedCollectorSet</c> — each check
/// MEMBERSHIP against a LOCAL hardcoded 27-collector set; neither reads the OTHER app's table and neither pins
/// the interval VALUES. So a value edit on one side only (e.g. Darling Aggressive <c>plan_cache_stats</c> 2->3)
/// ships green on both. This closes that hole: per preset (Aggressive / Balanced / Low-Impact) it asserts
/// Lite's <see cref="ScheduleManager.s_presets"/> and Darling's <c>CollectorSchedulePresets.Presets</c> are
/// IDENTICAL — same collector keys AND same minute values. Byte-identical today, so green and guarding forward.
///
/// <para>
/// Lite's table is the real in-memory object (internal, visible to Lite.Tests). Darling's lives in the
/// <c>PerformanceMonitor.Darling.Viewer</c> WPF assembly, which this CI-run project deliberately does not
/// reference; it is read from source (see <see cref="ParitySource"/>). The source parser is self-validated by
/// <see cref="PresetSourceParser_FaithfullyReproducesLiteRealObject"/> — it must reproduce Lite's real
/// <c>s_presets</c> exactly from <c>ScheduleManager.cs</c>, whose inner-entry format is identical to Darling's —
/// so a parser bug fails that self-test rather than silently passing the cross-app comparison.
/// </para>
/// </summary>
[Trait("Reads", "Darling")]
public sealed class CrossAppPresetValuePinTests
{
    private const string LiteSchedulePath = "Lite/Services/ScheduleManager.cs";
    private const string DarlingPresetsPath = "Darling/PerformanceMonitor.Darling.Viewer/CollectorSchedulePresets.cs";

    private static readonly string[] PresetNames = { "Aggressive", "Balanced", "Low-Impact" };

    /* The pinned membership set both apps' presets must cover — the same set each app's own membership pin
       asserts. Kept here so a collector added to the presets without updating this pin fails loudly. */
    private static readonly HashSet<string> ExpectedCollectors = new(StringComparer.OrdinalIgnoreCase)
    {
        "wait_stats", "latch_stats", "spinlock_stats", "cpu_scheduler_stats", "plan_cache_stats",
        "query_stats", "procedure_stats", "query_store", "query_snapshots", "cpu_utilization",
        "file_io_stats", "memory_stats", "memory_clerks", "memory_pressure_events", "tempdb_stats",
        "perfmon_stats", "deadlocks", "memory_grant_stats", "waiting_tasks", "dmv_blocking_snapshot",
        "blocked_process_report", "running_jobs", "session_summary_stats", "system_health_events",
        "default_trace_events", "job_history", "agent_status",
        "ag_replica_states", "ag_database_replica_states", "plan_correction", "database_states"
    };

    [Fact]
    public void DarlingPresetIntervals_AreIdenticalToLite()
    {
        var lite = ScheduleManager.s_presets;                                          // real in-memory Lite table
        var darling = ParsePresetTable(ParitySource.ReadFile(DarlingPresetsPath));     // Darling source-of-truth

        Assert.Equal(PresetNames.Length, darling.Count);

        foreach (var presetName in PresetNames)
        {
            Assert.True(lite.ContainsKey(presetName), $"Lite is missing preset '{presetName}'");
            Assert.True(darling.ContainsKey(presetName), $"Darling is missing preset '{presetName}'");

            var liteIntervals = lite[presetName];
            var darlingIntervals = darling[presetName];

            /* Membership matches the pinned set on BOTH sides (so the value loop below covers every collector). */
            Assert.True(ExpectedCollectors.SetEquals(liteIntervals.Keys),
                $"Lite preset '{presetName}' collector set drifted from the pinned set");
            Assert.True(ExpectedCollectors.SetEquals(darlingIntervals.Keys),
                $"Darling preset '{presetName}' collector set drifted from the pinned set");

            /* Same interval VALUE for every collector — the drift the membership-only per-app pins can't see. */
            foreach (var collector in liteIntervals.Keys)
            {
                Assert.True(darlingIntervals.TryGetValue(collector, out var darlingMinutes),
                    $"Darling preset '{presetName}' is missing collector '{collector}'");
                Assert.True(liteIntervals[collector] == darlingMinutes,
                    $"Preset '{presetName}' interval drift for '{collector}': Lite={liteIntervals[collector]} Darling={darlingMinutes}");
            }
        }
    }

    [Fact]
    public void PresetSourceParser_FaithfullyReproducesLiteRealObject()
    {
        /* Anti-parser-bug guard. Darling's table is read from source, so the whole cross-app pin is only as
           trustworthy as the parser. The parser must reproduce Lite's REAL s_presets object exactly from
           ScheduleManager.cs; Darling's CollectorSchedulePresets.cs uses the identical inner-entry format
           (["collector"] = minutes), so a parser proven faithful on Lite's file is trusted on Darling's. If
           this fails, DarlingPresetIntervals_AreIdenticalToLite's green means nothing — so it is pinned, not
           assumed. */
        var real = ScheduleManager.s_presets;
        var parsed = ParsePresetTable(ParitySource.ReadFile(LiteSchedulePath));

        Assert.Equal(real.Count, parsed.Count);
        foreach (var (presetName, intervals) in real)
        {
            Assert.True(parsed.TryGetValue(presetName, out var parsedIntervals),
                $"parser dropped preset '{presetName}'");
            Assert.Equal(intervals.Count, parsedIntervals!.Count);
            foreach (var (collector, minutes) in intervals)
            {
                Assert.True(parsedIntervals.TryGetValue(collector, out var parsedMinutes),
                    $"parser dropped collector '{collector}' from preset '{presetName}'");
                Assert.Equal(minutes, parsedMinutes);
            }
        }
    }

    /// <summary>
    /// Parses the three preset tables out of a <c>ScheduleManager.cs</c> / <c>CollectorSchedulePresets.cs</c>
    /// source string. Anchors on <c>["PresetName"] = new … {</c> and captures the brace-delimited body — the
    /// inner dictionaries hold no nested braces — then reads every <c>["collector"] = minutes</c> entry. Format
    /// agnostic to the target-typed <c>new(...)</c> (Lite) vs the explicit
    /// <c>new Dictionary&lt;string,int&gt;(...)</c> (Darling); whitespace/newlines/trailing commas are tolerated.
    /// </summary>
    private static Dictionary<string, Dictionary<string, int>> ParsePresetTable(string source)
    {
        var result = new Dictionary<string, Dictionary<string, int>>(StringComparer.OrdinalIgnoreCase);

        foreach (var presetName in PresetNames)
        {
            /* `["<preset>"] = new <anything-but-braces> {` — the opening of this preset's inner dictionary.
               [^{}]* between `new` and `{` consumes the (StringComparer…) / Dictionary<…>(…) noise but cannot
               cross a brace, so it lands on the inner-dictionary open brace, never a later one. */
            var open = Regex.Match(source, @"\[""" + Regex.Escape(presetName) + @"""\]\s*=\s*new\b[^{}]*\{");
            if (!open.Success)
            {
                continue;   // leave absent → the caller's ContainsKey / count assertions report it
            }

            var bodyStart = open.Index + open.Length;
            var bodyEnd = source.IndexOf('}', bodyStart);   // no nested braces inside → first '}' closes it
            Assert.True(bodyEnd > bodyStart, $"unterminated preset body for '{presetName}'");
            var body = source.Substring(bodyStart, bodyEnd - bodyStart);

            var intervals = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            foreach (Match entry in Regex.Matches(body, @"\[""([a-z0-9_]+)""\]\s*=\s*(\d+)"))
            {
                intervals[entry.Groups[1].Value] = int.Parse(entry.Groups[2].Value);
            }

            result[presetName] = intervals;
        }

        return result;
    }
}
