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
/// The V79 rung (#2349) — the database file-growth alert's settings. No longer the top of the ladder:
/// V80 (#2472) took that, and the assertions that belong to whichever rung is newest moved with it to
/// <see cref="CollectionLogFanoutRollupStoreTests"/>. What stays here is everything that is true of THIS
/// rung forever — that it is registered, that its DDL is what it was, and that a store migrated to exactly
/// 79 still maps to 79 rather than falling through a newer arm.
/// </summary>
public class FileGrowthAlertStoreTests
{
    [Fact]
    public void TheRungIsRegisteredAndIsTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("file-growth-alert", PgMigrations.Scripts.Single(s => s.Version == 79).Name);

        /* Demoted at V80 (#2472): "79 is the maximum" was true of the LADDER while this was its top rung,
           not of this rung, and leaving it here is how a demotion turns into a red build on the next one.
           The density and ordering checks below stay — those are properties of the whole ladder that every
           rung's test may assert. */
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    /// <summary>
    /// The alert ships OFF. A new alert that starts firing on upgrade is a bad citizen — the operator did not
    /// ask for it, and the right thresholds are a property of their fleet rather than of the product.
    /// </summary>
    [Fact]
    public void TheRungAddsTheKnobs_Idempotently_AndTheAlertShipsOff()
    {
        var sql = PgMigrations.Scripts.Single(s => s.Version == 79).Sql;

        Assert.Contains("ALTER TABLE config.config_alert_settings", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS file_growth_enabled boolean NOT NULL DEFAULT false", sql, StringComparison.Ordinal);
        Assert.Contains("file_growth_rise_mb", sql, StringComparison.Ordinal);
        Assert.Contains("file_growth_volume_percent", sql, StringComparison.Ordinal);
        Assert.Contains("file_growth_lookback_minutes", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbeAsksForTheColumn_AndTheReaderStillFetchesItsOrdinal()
    {
        Assert.Contains(
            "table_name = 'config_alert_settings' AND column_name = 'file_growth_enabled'",
            ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        /* Demoted at V81 (#2515), and it should have been at V80. This read `mapParameters - 1`, which is
           the NEWEST sentinel's ordinal — correct only while V79 was the top rung, and from V80 onwards it
           was quietly asserting a later rung's wiring while reading as though it still guarded this one.
           Pinned at 54, this rung's own ordinal, which cannot slide. The "and no more than that" half
           belongs to whichever rung is newest and lives there. */
        Assert.Contains("reader.GetBoolean(54)", ReadViewerSource(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbeMapsAStoreAtExactly79To79()
    {
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* 54 positional sentinels, then this rung's own, then FALSE for anything a later rung appends. The
           leading count is FIXED at this rung's ordinal deliberately: deriving it from arity (`arity - 1`)
           reads identically while this is the top rung, then slides the flag one place right per new rung —
           the assertion keeps passing while quietly testing a newer arm. V80 has since been appended and
           this test needed no edit, which is the fixed count doing exactly what it was written for. */
        var all = Enumerable.Repeat(true, 54).Cast<object>().ToArray();
        object[] Args(bool ownFlag) => all
            .Concat(new object[] { ownFlag })
            .Concat(Enumerable.Repeat((object)false, arity - 55))
            .ToArray();

        Assert.Equal(79, (int)method.Invoke(null, Args(true))!);
        Assert.Equal(78, (int)method.Invoke(null, Args(false))!);
    }

    /// <summary>
    /// The read must select and map every knob. <c>ApplyToConfig</c> replaces <c>config.Alerts</c> wholesale,
    /// so a column selected but not mapped — or mapped but not selected — silently RESETS the operator's
    /// setting on every worker start rather than failing, which is the failure mode the comments on every
    /// appended knob above it warn about.
    /// </summary>
    [Fact]
    public void TheStoreReadSelectsAndMapsEveryKnob()
    {
        var source = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "StoreConfigProvider.cs");

        foreach (var column in new[]
                 {
                     "file_growth_enabled", "file_growth_rise_mb",
                     "file_growth_volume_percent", "file_growth_lookback_minutes",
                 })
        {
            Assert.Contains(column, source, StringComparison.Ordinal);
        }

        Assert.Contains("FileGrowthEnabled = reader.GetBoolean(54)", source, StringComparison.Ordinal);
        Assert.Contains("FileGrowthRiseMb = reader.GetInt32(55)", source, StringComparison.Ordinal);
        Assert.Contains("FileGrowthVolumePercent = reader.GetInt32(56)", source, StringComparison.Ordinal);
        Assert.Contains("FileGrowthLookbackMinutes = reader.GetInt32(57)", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// Both SKUs read the same shape. The Postgres side uses <c>DISTINCT ON</c> and DuckDB has none, so Lite
    /// expresses the same selection with <c>ROW_NUMBER()</c> — different SQL, same rule, and the alert must not
    /// behave differently depending on which product an operator bought.
    /// </summary>
    [Fact]
    public void BothSkusReadTheSameShape()
    {
        var darling = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingAlertReadAdapter.cs");
        var lite = ReadRepoFile("Lite", "Services", "LocalDataService.FileGrowth.cs");

        Assert.Contains("DISTINCT ON (database_name, file_name)", darling, StringComparison.Ordinal);
        Assert.Contains("ROW_NUMBER() OVER (PARTITION BY database_name, file_name", lite, StringComparison.Ordinal);

        /* Both bound the window on collection_time, the partitioning column, so the read prunes rather than
           scanning retention -- and both compute growth against a baseline from inside that window. */
        Assert.Contains("collection_time >= $2", darling, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", lite, StringComparison.Ordinal);
        Assert.Contains("growth_window_minutes", darling, StringComparison.Ordinal);
        Assert.Contains("growth_window_minutes", lite, StringComparison.Ordinal);
    }

    private static string ReadViewerSource() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");

    private static string ReadRepoFile(params string[] parts)
    {
        var relative = System.IO.Path.Combine(parts);
        var dir = System.IO.Path.GetDirectoryName(ThisFile())!;
        while (dir is not null && !System.IO.File.Exists(System.IO.Path.Combine(dir, relative)))
        {
            dir = System.IO.Path.GetDirectoryName(dir);
        }

        return System.IO.File.ReadAllText(System.IO.Path.Combine(dir!, relative));
    }

    private static string ThisFile([System.Runtime.CompilerServices.CallerFilePath] string path = "") => path;
}
