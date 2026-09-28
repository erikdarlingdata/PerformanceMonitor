/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using Xunit;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;

namespace Darling.Tests;

/// <summary>
/// Pins Darling rung V152 (#4503): the six Query Store rollups' auto-created two-key group index (first
/// key one of the rung's drop-list columns, second key <c>bucket</c>) is dropped by catalog shape rather
/// than by a built name, and the six rollups now carry <c>create_group_indexes = false</c> so a fresh
/// store never grows the index in the first place. This file is the RUNG (ladder, viewer probe) only —
/// the live schema-after-migrate proof (both the drop on an already-migrated store and a fresh store
/// never creating it) is <c>CaggGroupIndexDropLiveTests</c>.
///
/// <para>This file's "I am the top rung" claim moved to <c>IntervalFirstExecIndexRungTests</c> (V153) now
/// that V153 has landed; this file's own rung/probe facts below keep asserting what stays true forever
/// (present, in-order, gated behind the arm above it) rather than "is exactly the top".</para>
/// </summary>
public sealed class CaggGroupIndexDropRungTests
{
    private const int RungVersion = 152;
    private const int PreviousVersion = 151;

    /// <summary>This rung's sentinel ordinal in the viewer probe — no longer the newest, since V153
    /// landed above it.</summary>
    private const int ProbeOrdinal = 127;

    /// <summary>
    /// The rung is registered, and the ladder stays dense above the historical gap — the claim this class
    /// took over from <c>AgGroupIdRungTests</c> (V151) moved on again to <c>IntervalFirstExecIndexRungTests</c>
    /// (V153) now that V153 has landed.
    /// </summary>
    [Fact]
    public void TheRungIsRegistered_AndTheLadderIsDenseAboveTheHistoricalGap()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("drop-unread-cagg-group-indexes", PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);

        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    /// <summary>
    /// The viewer probe's sentinel carries this rung, and the map treats it as an arm gated below the
    /// current top's arm — a missing arm maps a fully-migrated store one rung short, permanently, because
    /// <see cref="ViewerDataService.RequiredStoreSchemaVersion"/> is <see cref="StorageVersion.SchemaVersion"/>.
    /// </summary>
    [Fact]
    public void TheProbeCarriesThisRungsSentinel_AndTheArmSitsBelowTheCurrentTop()
    {
        var probe = ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains("query_hash", probe, StringComparison.Ordinal);
        Assert.Contains("collect.query_store_stats_hourly", probe, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.Equal("hasCaggGroupIndexDrop", method.GetParameters()[ProbeOrdinal].Name);

        /* Every rung above this one (V153's hasIntervalFirstExecIndexes) must also be false, or the map
           finds the newer arm first and this assertion is checking the wrong rung's fallthrough. */
        var all = Enumerable.Repeat((object)true, arity).ToArray();
        var behind = (object[])all.Clone();
        for (var i = ProbeOrdinal; i < arity; i++)
        {
            behind[i] = false;
        }
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        /* V153 (#4608) is now the top rung, so this arm no longer needs to be the LAST one — it only has
           to sit below the current top's arm, which is what the ladder-dense invariant above already
           guarantees is registered ahead of it. */
        var thisArm = viewer.IndexOf("if (hasCaggGroupIndexDrop)", StringComparison.Ordinal);
        var topArm = viewer.IndexOf("if (hasIntervalFirstExecIndexes)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V152 sentinel arm — a fully-migrated store would map one rung short");
        Assert.True(topArm >= 0 && topArm < thisArm, "the current top rung's arm must sit above the V152 arm");
        Assert.Contains(
            "return " + RungVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..viewer.IndexOf("if (hasAgGroupId)", StringComparison.Ordinal)], StringComparison.Ordinal);
    }
}
