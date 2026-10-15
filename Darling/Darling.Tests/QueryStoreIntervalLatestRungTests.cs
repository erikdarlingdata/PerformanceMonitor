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
using System.Reflection;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling rung V143 (#3953): three new tables, all engine-plain, and nothing on an existing table. The
/// latest Query Store snapshot per interval, its per-server coverage, and the replay record for a batch whose apply
/// failed. The writer, the reads and the retention are pinned where they live
/// (<see cref="QueryStoreIntervalLatestWriterTests"/>, <see cref="PlanRegressionIntervalTableEquivalenceTests"/>);
/// this file is the RUNG: the ladder, the DDL, and the viewer probe.
///
/// <para>This file took over the "I am the top rung" claims that moved off <see cref="CollectionCaveatsRungTests"/>
/// (V141) when this rung landed, and hands them on to <see cref="RawChunkIntervalRungHistoryRungTests"/> (V144,
/// #4211) when that one did.</para>
/// </summary>
[Trait("Stage", "Guard")] // #5798: walks the repo tree, so the guard stage runs it
public sealed class QueryStoreIntervalLatestRungTests
{
    private const int RungVersion = 143;
    private const int PreviousVersion = 142;

    /// <summary>This rung's sentinel ordinal in the viewer probe — the newest, so the last argument.</summary>
    private const int ProbeOrdinal = 118;

    private static PgMigrations.Migration V143 => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("query-store-interval-latest", V143.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        /* Not `RungVersion == SchemaVersion` any more: that asserted this rung is the newest, which stopped
           being true when V144 landed. The invariant that outlives the handoff is that the LADDER's top and
           the declared version agree, which the two lines above already say. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// The viewer's schema map, given every sentinel up to this rung's ordinal, returns this rung's version, and with
    /// this rung's sentinel clear, the previous rung's: a sentinel at the wrong ordinal maps a store one rung off.
    /// </summary>
    [Fact]
    public void TheProbeMapsAStoreStoppedHereToThisRung()
    {
        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.True(ProbeOrdinal < arity - 1);
        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);
        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);
    }
}
