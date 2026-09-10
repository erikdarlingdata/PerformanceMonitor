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
/// The V78 rung (#2357) — the compose <c>statement_timeout</c> knob, and the top of the ladder.
///
/// <para><b>Why it could not just be a raised constant.</b> The timeout is applied in role PROVISIONING DDL,
/// deliberately not a versioned migration: a role's <c>statement_timeout</c> has no probeable schema footprint,
/// so tying it to <c>StorageVersion.SchemaVersion</c> would break the viewer's connect-time version gate. An
/// existing install therefore already has the old value baked into its roles, and bumping a constant in a new
/// build would appear to do nothing.</para>
///
/// <para><b>Why no new machinery was needed to deliver it.</b> That same provisioning SQL is re-run on every
/// managed start — "idempotent + self-healing: re-run every managed start, converging role state" — so reading
/// the knob there means a changed value reaches a running install on its next restart.</para>
/// </summary>
public class ComposeStatementTimeoutStoreTests
{
    [Fact]
    public void TheRungIsRegisteredAndIsTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        /* #2349 added V79, so this rung is no longer the top -- the "I am the top" claim moves to the newest
           rung's own test (FileGrowthAlertStoreTests). What stays true: it is PRESENT, the ladder is ordered
           and dense, and the build's schema version tracks the maximum. */
        Assert.Equal("compose-statement-timeout", PgMigrations.Scripts.Single(s => s.Version == 78).Name);
        Assert.Contains(78, versions);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());

        /* Ordered, and dense above the one sanctioned historical hole at V45. */
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    /// <summary>
    /// The rung adds the column with the default that reproduces today's behaviour, and does it idempotently —
    /// a rung that is not re-runnable turns a retried upgrade into a failed one.
    /// </summary>
    [Fact]
    public void TheRungAddsTheColumn_Idempotently_WithTodaysValueAsTheDefault()
    {
        var sql = PgMigrations.Scripts.Single(s => s.Version == 78).Sql;

        Assert.Contains("ALTER TABLE config.config_service", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS compose_statement_timeout_seconds", sql, StringComparison.Ordinal);
        Assert.Contains("DEFAULT 15", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The probe, the reader ordinals and the map arity are three places that must agree; the reader hands over
    /// exactly one argument per map parameter, so an added sentinel that forgot its ordinal fails here.
    /// </summary>
    [Fact]
    public void TheProbeAsksForTheColumn_AndTheThreePlacesAgree()
    {
        Assert.Contains(
            "table_name = 'config_service' AND column_name = 'compose_statement_timeout_seconds'",
            ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var mapParameters = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .GetParameters().Length;

        var viewerSource = ReadViewerSource();

        Assert.Contains($"reader.GetBoolean({mapParameters - 1})", viewerSource, StringComparison.Ordinal);
        Assert.DoesNotContain($"reader.GetBoolean({mapParameters})", viewerSource, StringComparison.Ordinal);
    }

    /// <summary>
    /// A fully migrated store maps to exactly 78, and the previous arm still answers 77 rather than falling
    /// through — the invariant that keeps the version banner from reporting a mismatch on a current store.
    /// </summary>
    [Fact]
    public void TheProbeMapsAStoreAtExactly78To78()
    {
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* Every parameter appended by a LATER rung is padded FALSE, derived from arity so a future rung does
           not have to edit this file -- the lesson from #2357, where four older rung tests broke at once.

           The LEADING count is fixed at this rung's own ordinal and must NOT be derived from arity: padding
           that side instead would slide ownFlag one position right per new rung, so the assertion would drift
           onto a newer arm while still passing. It read correctly with exactly one rung above it, which is
           the only reason V79 did not catch it here too. */
        object[] Args(bool ownFlag) => Enumerable.Repeat(true, 53).Cast<object>()
            .Concat(new object[] { ownFlag })
            .Concat(Enumerable.Repeat((object)false, arity - 54))
            .ToArray();

        Assert.Equal(78, (int)method.Invoke(null, Args(true))!);
        Assert.Equal(77, (int)method.Invoke(null, Args(false))!);
    }

    /// <summary>
    /// <b>The backstop must survive configuration.</b> A LIMIT bounds OUTPUT; a group-by scans and sorts before
    /// it. Something has to bound WORK, so the knob is clamped rather than trusted — zero or negative would
    /// remove the ceiling entirely, which is the one outcome the whole design leans on not happening.
    /// </summary>
    [Theory]
    [InlineData(0, 15)]
    [InlineData(-1, 15)]
    [InlineData(1, 5)]
    [InlineData(5, 5)]
    [InlineData(15, 15)]
    [InlineData(120, 120)]
    [InlineData(600, 600)]
    [InlineData(9999, 600)]
    public void TheKnobIsClamped(int stored, int effective)
    {
        Assert.Equal(effective, StoreConfigProvider.ClampComposeStatementTimeoutSeconds(stored));
    }

    /// <summary>
    /// The provisioning DDL carries the configured value, and clamps independently — the method is public, and
    /// a caller passing 0 must not be able to remove the ceiling.
    /// </summary>
    [Theory]
    [InlineData(120, "120s")]
    [InlineData(15, "15s")]
    [InlineData(0, "15s")]
    [InlineData(99999, "600s")]
    public void TheProvisioningDdl_AppliesTheConfiguredTimeout(int seconds, string expected)
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql(
            "AdminPassword01", "ViewerPassword02", "McpPassword03", seconds);

        Assert.Contains($"SET statement_timeout = '{expected}'", sql, StringComparison.Ordinal);

        /* Both compose roles, not just one: the mcp role gets viewer's read surface and must get its ceiling. */
        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(sql, @"SET statement_timeout = '").Count);
    }

    /// <summary>
    /// <b>A reload carries the knob into the held config.</b> It was seeded, read back, clamped and
    /// documented as store-authoritative, but <see cref="StoreConfigProvider.ApplyToConfig"/> never
    /// assigned it, so <c>config.ComposeStatementTimeoutSeconds</c> reported the darling.json value for
    /// the life of the process while <c>config_service</c> said otherwise. Pure -- ApplyToConfig does no I/O.
    ///
    /// <para>This pins PROPAGATION, which is not the same as delivery. The effective ceiling is whatever
    /// startup provisioning last wrote onto the roles (see the class summary above); a reload does not
    /// re-assert it. So the held value states the store's DESIRED timeout, and
    /// <see cref="StoreConfigViewPropagationTests"/> holds the same invariant for the whole family.</para>
    /// </summary>
    [Theory]
    [InlineData(120)]
    [InlineData(15)]
    [InlineData(600)]
    public void AReload_CarriesTheKnobIntoTheHeldConfig(int stored)
    {
        /* Deliberately NOT the darling.json default: an assignment that never happens has to be
           distinguishable from one that happens to land on 15. */
        var config = new DarlingConfig { ComposeStatementTimeoutSeconds = 42 };

        StoreConfigProvider.ApplyToConfig(
            config, new StoreConfigView { ComposeStatementTimeoutSeconds = stored });

        Assert.Equal(stored, config.ComposeStatementTimeoutSeconds);
    }

    /// <summary>Omitting it reproduces the constant it replaced, so an untouched install is unchanged.</summary>
    [Fact]
    public void TheDefaultReproducesTheOldConstant()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql("AdminPassword01", "ViewerPassword02", "McpPassword03");

        Assert.Contains("SET statement_timeout = '15s'", sql, StringComparison.Ordinal);
    }

    private static string ReadViewerSource([System.Runtime.CompilerServices.CallerFilePath] string thisFile = "")
    {
        var dir = System.IO.Path.GetDirectoryName(thisFile)!;
        var relative = System.IO.Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        while (dir is not null && !System.IO.File.Exists(System.IO.Path.Combine(dir, relative)))
        {
            dir = System.IO.Path.GetDirectoryName(dir);
        }

        return System.IO.File.ReadAllText(System.IO.Path.Combine(dir!, relative));
    }
}
