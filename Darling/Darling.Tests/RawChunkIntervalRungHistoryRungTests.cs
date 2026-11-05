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
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling rung V144 (#4211): two new tables, <c>collect.raw_chunk_interval_rung_history</c> and
/// <c>collect.raw_chunk_interval_reconcile_runs</c> — the store <see cref="RawChunkIntervalReconciler"/> writes
/// (it reads back only the rung history).
///
/// <para>This file took over the "I am the top rung" claim that moved off
/// <see cref="QueryStoreIntervalLatestRungTests"/> (V143) when this rung landed, and hands it on to
/// the #3953 B4 wide-interval-table rung (V145) when that one did.</para>
/// </summary>
public sealed class RawChunkIntervalRungHistoryRungTests
{
    private const int RungVersion = 144;

    private const int ProbeOrdinal = 119;

    private const string HistoryTable = "raw_chunk_interval_rung_history";
    private const string RunsTable = "raw_chunk_interval_reconcile_runs";

    private static PgMigrations.Migration V144 => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("raw-chunk-interval-rung-history", V144.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        /* Not `RungVersion == SchemaVersion` any more: that asserted this rung is the newest, which stopped
           being true when V145 landed. The invariant that outlives the handoff is that the LADDER's top and
           the declared version agree, which the two lines above already say. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }
}
