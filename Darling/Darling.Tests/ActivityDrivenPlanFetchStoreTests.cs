/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The V77 rung (#2312 Finding 2) — the schema strokes behind the activity-driven plan/text fetch: the
/// plan map's <c>digest</c> goes nullable (the content-less marker for plans whose XML the engine cannot
/// persist), <c>query_store_text</c> gains <c>query_hash</c> (the Query Store reset detector), and the
/// retired <c>planwm:</c>/<c>textwm:</c> watermark state rows are deleted wholesale. These facts pin the
/// rung's place on the ladder, the viewer probe's newest-first arm, and the migration SQL's load-bearing
/// strokes. The fetch behavior itself is pinned in <c>QueryStorePlanFetchTests</c> and exercised live in
/// the gated Postgres suites.
/// </summary>
public sealed class ActivityDrivenPlanFetchStoreTests
{
    /* ---------------- the rung ---------------- */

    [Fact]
    public void TheRungIsTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        /* #2357 added V78, so this rung is no longer the maximum. What stays true: it is PRESENT, the
           ladder is ordered and dense, and the build's schema version tracks the maximum. */
        Assert.Contains(77, versions);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);

        /* Dense above the one sanctioned historical hole at V45. */
        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);

        Assert.Equal("activity-driven-plan-fetch", PgMigrations.Scripts.Single(s => s.Version == 77).Name);
    }

    /// <summary>The three strokes, each load-bearing and none allowed to drift out of the rung: without
    /// the nullable digest the NULL-XML marker cannot land, without query_hash the reset detector has no
    /// stored baseline, and without the deletes the orphaned watermark rows live forever (collector_state
    /// has no retention, and the prune set no longer owns those prefixes).</summary>
    [Fact]
    public void TheRungCarriesAllThreeStrokes()
    {
        var sql = PgMigrations.Scripts.Single(s => s.Version == 77).Sql;

        Assert.Contains("ALTER TABLE collect.query_store_plan_map ALTER COLUMN digest DROP NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE collect.query_store_text ADD COLUMN IF NOT EXISTS query_hash text", sql, StringComparison.Ordinal);
        Assert.Contains("DELETE FROM collector_state WHERE collector_name = 'query_store_plan_xml' AND state_key LIKE 'planwm:%'", sql, StringComparison.Ordinal);
        Assert.Contains("DELETE FROM collector_state WHERE collector_name = 'query_store_text' AND state_key LIKE 'textwm:%'", sql, StringComparison.Ordinal);
    }

    /* ---------------- the viewer probe ---------------- */

    [Fact]
    public void TheProbeMapsAStoreAtExactly77To77()
    {
        /* #2357 added V78, so this rung is no longer the top — the "I am the top" claim moves to the newest
           rung's own test (ComposeStatementTimeoutStoreTests). What stays true forever is the arm itself: a
           store migrated to EXACTLY 77 must answer 77 rather than falling through to 76. */
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        /* 52 positional sentinels, then this rung's own by name. Anything a LATER rung appends is padded
           by InvokeMap from the method's arity, so this count stays fixed as the ladder grows. */
        var all = Enumerable.Repeat(true, 52).Cast<object>().ToArray();

        Assert.Equal(77, InvokeMap(all, hasQueryStoreTextHash: true));
        Assert.Equal(76, InvokeMap(all, hasQueryStoreTextHash: false));
    }

    [Fact]
    public void TheProbeAsksForTheColumn_AndTheThreePlacesAgree()
    {
        Assert.Contains(
            "table_name = 'query_store_text' AND column_name = 'query_hash'",
            ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

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

    private static int InvokeMap(object[] leading, bool hasQueryStoreTextHash)
    {
        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;

        /* Parameters appended by LATER rungs are padded FALSE, so this fact keeps exercising its own arm
           rather than a newer one. Derived from the method's arity rather than named by hand: listing them
           made every new rung break this file, which is exactly what V79 (#2349) did. */
        var args = leading.Concat(new object[] { hasQueryStoreTextHash }).ToArray();
        args = args
            .Concat(Enumerable.Repeat((object)false, method.GetParameters().Length - args.Length))
            .ToArray();

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
