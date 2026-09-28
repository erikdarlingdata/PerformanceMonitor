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
/// <para>This file's "I am the top rung" claim takes over from <c>AgGroupIdRungTests</c> (V151) now that
/// V152 has landed.</para>
/// </summary>
public sealed class CaggGroupIndexDropRungTests
{
    private const int RungVersion = 152;
    private const int PreviousVersion = 151;

    /// <summary>This rung's sentinel ordinal in the viewer probe — the newest, so the last argument.</summary>
    private const int ProbeOrdinal = 127;

    /// <summary>
    /// The rung is registered and is the new top of the ladder — the claim this class takes over from
    /// <c>AgGroupIdRungTests</c> (V151) now that V152 has landed.
    /// </summary>
    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("drop-unread-cagg-group-indexes", PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(RungVersion, StorageVersion.SchemaVersion);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// The viewer probe's sentinel carries this rung, and the map treats it as the TOP arm: a missing top arm
    /// maps a fully-migrated store one rung short, permanently, because
    /// <see cref="ViewerDataService.RequiredStoreSchemaVersion"/> is <see cref="StorageVersion.SchemaVersion"/>.
    /// </summary>
    [Fact]
    public void TheProbeMapsAFullyMigratedStoreToThisTopRung()
    {
        var probe = ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains("query_hash", probe, StringComparison.Ordinal);
        Assert.Contains("collect.query_store_stats_hourly", probe, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain($"reader.GetBoolean({ProbeOrdinal + 1})", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.Equal(ProbeOrdinal, arity - 1);
        Assert.Equal("hasCaggGroupIndexDrop", method.GetParameters()[ProbeOrdinal].Name);

        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        var behind = (object[])all.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        var thisArm = viewer.IndexOf("if (hasCaggGroupIndexDrop)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasAgGroupId)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V152 sentinel arm — a fully-migrated store would map one rung short");
        Assert.True(thisArm < previousArm, "the V152 arm sits below V151's, so a current store maps one rung short");
        Assert.Contains(
            "return " + StorageVersion.SchemaVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..previousArm], StringComparison.Ordinal);
    }
}
