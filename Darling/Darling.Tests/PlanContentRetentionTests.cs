/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The V75 plan-content retention knob (#2316). The payload dimensions' GC horizon is coupled to the
/// widest dim-feeding fact retention so a raised override can never orphan a reader — which also means a
/// store YOUNGER than that horizon has an unbounded plan dimension: measured on the dogfood fleet,
/// <c>query_plan_dim</c> reached 127 GB (63% of the store) in its first 22 days of parameter-sniffing
/// recompile churn (65 distinct XMLs per plan shape per day), with the coupled GC unable to delete a
/// single row until ~a month AFTER the projected disk-full. The knob gives plan CONTENT its own horizon;
/// facts keep theirs. These facts pin the rung, the knob's clamps, the cutoff arithmetic in both
/// directions (bounding when enabled, byte-identical old behavior when disabled), and the viewer probe.
/// </summary>
public sealed class PlanContentRetentionTests
{
    private static readonly DateTime Now = new(2026, 8, 18, 12, 0, 0, DateTimeKind.Unspecified);

    /* ---------------- the rung ---------------- */

    [Fact]
    public void TheRungIsTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(75, versions.Max());
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);

        /* Dense above the one sanctioned historical hole at V45. */
        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    [Fact]
    public void TheRungAddsTheKnobWithThe21DayDefault()
    {
        var rung = PgMigrations.Scripts.Single(s => s.Version == 75);

        Assert.Equal("plan-content-retention-knob", rung.Name);
        Assert.Contains("config.config_service", rung.Sql, StringComparison.Ordinal);
        Assert.Contains("plan_content_retention_days integer NOT NULL DEFAULT 21", rung.Sql, StringComparison.Ordinal);
    }

    /* ---------------- the clamps ---------------- */

    /// <summary>
    /// 0 and below mean DISABLED (the fact-coupled horizon stands alone); an enabled value clamps to
    /// [7,365] — a sub-week horizon would age plan XML out from under the viewer's default history
    /// windows, and clamping a bad stored value beats failing the config load (the V59 knobs' posture).
    /// </summary>
    [Theory]
    [InlineData(0, 0)]
    [InlineData(-5, 0)]
    [InlineData(1, 7)]
    [InlineData(6, 7)]
    [InlineData(7, 7)]
    [InlineData(21, 21)]
    [InlineData(365, 365)]
    [InlineData(9999, 365)]
    public void TheClampDisablesAtZeroAndBoundsEnabledValues(int stored, int effective)
        => Assert.Equal(effective, StoreConfigProvider.ClampPlanContentRetentionDays(stored));

    /* ---------------- the cutoff arithmetic ---------------- */

    /// <summary>
    /// Disabled (0) must be BYTE-IDENTICAL to the pre-knob behavior — every existing caller and test
    /// passes nothing and must observe no change.
    /// </summary>
    [Fact]
    public void Disabled_ReproducesTheFactCoupledCutoffExactly()
    {
        var withoutKnob = DarlingRetention.ComputeDimensionCutoff(Now, 90, Now.AddDays(-40));
        var withZero = DarlingRetention.ComputeDimensionCutoff(Now, 90, Now.AddDays(-40), planContentRetentionDays: 0);

        Assert.Equal(withoutKnob, withZero);
    }

    /// <summary>
    /// The knob's entire point: on a store younger than the fact horizon, the dedicated cutoff is NEWER
    /// than the coupled one and must win — with the same one-day margin as the measured side, covering
    /// the hourly <c>last_seen</c> refresh guard.
    /// </summary>
    [Fact]
    public void Enabled_FloorsTheCutoffAtTheDedicatedHorizon()
    {
        /* The dogfood shape: 90-day fact retention, a dim only ~3 weeks old. Coupled cutoff sits ~92
           days back (nothing ever eligible); a 21-day knob must pull it to now - 22. */
        var cutoff = DarlingRetention.ComputeDimensionCutoff(Now, 90, Now.AddDays(-22), planContentRetentionDays: 21);

        Assert.Equal(Now.AddDays(-22), cutoff);
    }

    /// <summary>
    /// A knob WIDER than the coupled horizon must not widen retention — the coupled cutoff (which is
    /// newer in that case) still governs, so raising the knob past the fact retention is a no-op rather
    /// than a way to keep plan XML no fact can reference.
    /// </summary>
    [Fact]
    public void Enabled_NeverWidensPastTheCoupledCutoff()
    {
        var coupledOnly = DarlingRetention.ComputeDimensionCutoff(Now, 30, oldestSurvivingDigestFact: null);
        var withWideKnob = DarlingRetention.ComputeDimensionCutoff(Now, 30, oldestSurvivingDigestFact: null, planContentRetentionDays: 365);

        Assert.Equal(coupledOnly, withWideKnob);
    }

    /// <summary>The measured-floor clamp still applies underneath the knob: when held facts reach further
    /// back than the assumed horizon, the knob (being newer still) wins over both — held history must not
    /// re-unbound the dimension.</summary>
    [Fact]
    public void Enabled_WinsOverTheMeasuredFloorToo()
    {
        var cutoff = DarlingRetention.ComputeDimensionCutoff(Now, 90, Now.AddDays(-200), planContentRetentionDays: 21);

        Assert.Equal(Now.AddDays(-22), cutoff);
    }

    /* ---------------- the viewer probe ---------------- */

    [Fact]
    public void TheProbeMapsAFullyMigratedStoreTo75()
    {
        Assert.Equal(75, StorageVersion.SchemaVersion);
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        /* 50 positional sentinels then the V75 one by name — the map takes 51 parameters. Present => 75,
           newest-first; absent => the previous arm still answers 74 rather than falling through. */
        var all = Enumerable.Repeat(true, 50).Cast<object>().ToArray();

        Assert.Equal(75, InvokeMap(all, hasPlanContentRetentionKnob: true));
        Assert.Equal(74, InvokeMap(all, hasPlanContentRetentionKnob: false));
    }

    [Fact]
    public void TheProbeAsksForTheColumn_AndTheThreePlacesAgree()
    {
        Assert.Contains("column_name = 'plan_content_retention_days'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var mapParameters = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .GetParameters().Length;

        var viewerSource = ReadViewerSource();

        /* The reader must hand over exactly one argument per map parameter: ordinals are 0-based, so the
           highest is Count - 1, and the next one up must NOT appear. */
        Assert.Contains($"reader.GetBoolean({mapParameters - 1})", viewerSource, StringComparison.Ordinal);
        Assert.DoesNotContain($"reader.GetBoolean({mapParameters})", viewerSource, StringComparison.Ordinal);
    }

    /* ---------------- helpers ---------------- */

    private static int InvokeMap(object[] leading, bool hasPlanContentRetentionKnob)
    {
        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;

        var args = leading.Concat(new object[] { hasPlanContentRetentionKnob }).ToArray();
        Assert.Equal(method.GetParameters().Length, args.Length);

        return (int)method.Invoke(null, args)!;
    }

    private static string ReadViewerSource([System.Runtime.CompilerServices.CallerFilePath] string thisFile = "")
    {
        var dir = System.IO.Path.GetDirectoryName(thisFile)!;
        var relative = System.IO.Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        while (dir is not null && !System.IO.File.Exists(System.IO.Path.Combine(dir, relative)))
        {
            dir = System.IO.Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return System.IO.File.ReadAllText(System.IO.Path.Combine(dir!, relative));
    }
}
