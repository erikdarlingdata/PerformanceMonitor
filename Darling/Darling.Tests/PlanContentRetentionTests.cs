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

        /* #2319 added V76, so this rung is no longer the top — the "I am the top" claim moves to the
           newest rung's own test (QueryStoreHealthStoreTests) and this one keeps the invariants that
           stay true forever: the rung is PRESENT, the ladder is ordered and dense, and the build's
           schema version tracks the maximum. */
        Assert.Contains(75, versions);
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

    /* ---------------- the scoping router (review catch) ---------------- */

    /// <summary>
    /// The dedicated horizon governs the PLAN dimension only — query text keeps the fact-coupled cutoff,
    /// or the knob would quietly break "text stays analyzable for the facts' full retention", which is
    /// half its own justification (and buy ~40 MB for the damage).
    /// </summary>
    [Fact]
    public void TheRouterScopesTheKnobToThePlanDimensionOnly()
    {
        var coupled = Now.AddDays(-92);
        var dedicated = Now.AddDays(-22);

        Assert.Equal(dedicated, DarlingRetention.ComputeDimTableCutoff(PayloadDimensions.QueryPlanDimTable, coupled, dedicated));
        Assert.Equal(coupled, DarlingRetention.ComputeDimTableCutoff(PayloadDimensions.QueryTextDimTable, coupled, dedicated));
    }

    /* ---------------- the map ordering (review catch) ---------------- */

    /// <summary>
    /// The invariant the review caught this PR breaking: the DIMENSION must outlive the MAP under every
    /// knob value, or a live <c>query_store_plan_map</c> row resolves to deleted content — the
    /// silent-missing-plans failure the margin ordering exists to prevent. Swept across every age from
    /// inside retention to well past both horizons, for the shipped default, the clamp edges, disabled,
    /// and a knob wider than the fact horizon.
    /// </summary>
    [Theory]
    [InlineData(30, 0)]
    [InlineData(30, 7)]
    [InlineData(30, 21)]
    [InlineData(90, 21)]
    [InlineData(90, 365)]
    [InlineData(7, 21)]
    [InlineData(1, 7)]
    public void NeitherPruneOrder_CanLeaveAMapRowResolvingToAnAbsentDigest_UnderTheKnob(int factRetentionDays, int knobDays)
    {
        var dimCutoff = DarlingRetention.ComputeDimensionCutoff(Now, factRetentionDays, oldestSurvivingDigestFact: null, planContentRetentionDays: knobDays);
        var mapCutoff = DarlingRetention.ComputeMapCutoff(Now, factRetentionDays, knobDays);

        /* The dim must outlive the map: strictly older cutoff. */
        Assert.True(dimCutoff < mapCutoff,
            $"the dim GC would take content the map still points at: dim cutoff {dimCutoff:o} is not earlier " +
            $"than map cutoff {mapCutoff:o} at {factRetentionDays}d retention / knob {knobDays}");

        /* And the both-orders sweep: no age where the dim row is takeable while its map row survives. */
        for (var age = 0; age <= factRetentionDays + 370; age++)
        {
            var lastSeen = Now.AddDays(-age);
            var mapEligible = lastSeen < mapCutoff;
            var dimEligible = lastSeen < dimCutoff;

            Assert.False(dimEligible && !mapEligible,
                $"at {age}d the dim row is prunable while its map row survives (retention {factRetentionDays}d, knob {knobDays})");
        }
    }

    /// <summary>Disabled must reproduce the pre-knob map cutoff exactly, like the dim's disabled path.</summary>
    [Fact]
    public void MapCutoff_Disabled_ReproducesTheOldBehaviorExactly()
    {
        var old = Now.AddDays(-(30 + QueryStorePlanMap.PruneMarginDays));

        Assert.Equal(old, DarlingRetention.ComputeMapCutoff(Now, 30));
        Assert.Equal(old, DarlingRetention.ComputeMapCutoff(Now, 30, planContentRetentionDays: 0));
    }

    /// <summary>
    /// The destructive-sink clamp (review catch, twice: the first "fix" commit lost the edit to a
    /// failed batch-script assertion and shipped only its comment). PurgeAsync cannot be executed here
    /// without a live store, so this pins the clamp the way the repo pins other unexecutable seams —
    /// at the source: the sink must re-clamp before first use, because on a store-unreachable boot the
    /// worker passes darling.json's RAW value and a file value of 1-6 would prune plan content below
    /// the [7,365] contract. Proven to fail against the unclamped code before the fix landed.
    /// </summary>
    [Fact]
    public void PurgeAsyncClampsTheKnobAtTheDestructiveSink()
    {
        var source = ReadRetentionSource();
        var body = source[source.IndexOf("public static async Task<PurgeSummary> PurgeAsync", StringComparison.Ordinal)..];

        var clampAt = body.IndexOf("planContentRetentionDays = StoreConfigProvider.ClampPlanContentRetentionDays(planContentRetentionDays);", StringComparison.Ordinal);
        Assert.True(clampAt > 0, "PurgeAsync no longer clamps planContentRetentionDays at the destructive sink");

        /* And the clamp must come BEFORE the first use — both cutoff computations. */
        var firstUse = body.IndexOf("ComputeDimensionCutoff(", StringComparison.Ordinal);
        var mapUse = body.IndexOf("ComputeMapCutoff(", StringComparison.Ordinal);
        Assert.True(clampAt < firstUse, "the clamp sits after the dimension cutoff computation");
        Assert.True(clampAt < mapUse, "the clamp sits after the map cutoff computation");
    }

    /* ---------------- the viewer probe ---------------- */

    [Fact]
    public void TheProbeMapsAFullyMigratedStoreTo75()
    {
        /* #2319: no longer the top (that claim lives in QueryStoreHealthStoreTests) — this fact keeps
           pinning that a store at exactly 75 maps to 75 and one at 74 maps to 74, forever. */
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

        /* #2319 appended hasQueryStoreHealth and #2312 appended hasQueryStoreTextHash after this rung's
           parameter — pass both FALSE so these facts keep exercising the V75/V74 arms rather than the
           newer ones. */
        /* Parameters appended by LATER rungs are padded FALSE, so this fact keeps exercising its own
           arm rather than a newer one. Derived from the method's arity rather than listed by hand, so
           a future rung does not have to edit this file -- #2357 (V78) was the fourth that would have. */
        var args = leading.Concat(new object[] { hasPlanContentRetentionKnob }).ToArray();
        args = args
            .Concat(Enumerable.Repeat((object)false, method.GetParameters().Length - args.Length))
            .ToArray();
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

    private static string ReadRetentionSource([System.Runtime.CompilerServices.CallerFilePath] string thisFile = "")
    {
        var dir = System.IO.Path.GetDirectoryName(thisFile)!;
        var relative = System.IO.Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingRetention.cs");
        while (dir is not null && !System.IO.File.Exists(System.IO.Path.Combine(dir, relative)))
        {
            dir = System.IO.Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return System.IO.File.ReadAllText(System.IO.Path.Combine(dir!, relative));
    }
}
