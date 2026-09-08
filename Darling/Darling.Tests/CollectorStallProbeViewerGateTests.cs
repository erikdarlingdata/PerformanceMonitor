/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V112 (#2880)'s half of the viewer's connect-time schema gate.
///
/// <para>Split from <see cref="CollectorStallProbeStoreTests"/> for the reason V111's suite was split: this is
/// the only part of the rung's suite that needs the WPF Viewer assembly, which is <c>net10.0-windows</c>.
/// Everything else in that suite runs against <c>net10.0</c> projects and can therefore be exercised outside
/// the Windows-only harness; keeping this test with it would have made the whole file Windows-only.</para>
/// </summary>
public class CollectorStallProbeViewerGateTests
{
    /// <summary>The probe ordinal this rung's sentinel occupies. Its OWN ordinal, which never moves.</summary>
    internal const int ProbeOrdinal = 87;

    /// <summary>The version a store one rung behind this one reports.</summary>
    private const int PreviousVersion = 111;

    /// <summary>The rung this suite is about. Read from the store-side suite so the two cannot disagree.</summary>
    private const int RungVersion = CollectorStallProbeStoreTests.RungVersion;

    /// <summary>The table the rung creates.</summary>
    private const string TableName = "collector_stall_probes";

    /// <summary>
    /// The connect-time gate. A TABLE sentinel, because the table is the only object the rung creates.
    ///
    /// <para>This rung is no longer the top one — V113 (#2138 phase 1) is — so the "a fully-migrated store
    /// maps to exactly THIS version" clause has moved to that rung's suite, where it is true. Two things
    /// here had to stop assuming it: the sentinel's ordinal is no longer the last one, and the
    /// one-rung-behind check has to switch off every LATER sentinel too, or it measures the newest rung
    /// instead of this one.</para>
    /// </summary>
    [Fact]
    public void TheProbeAsksForTheTable_AndMapsAFullyMigratedStoreToThisRung()
    {
        Assert.Contains(
            $"table_name = '{TableName}'",
            ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = ReadSource("Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasCollectorStallProbes", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* The ordinal has to be a position that exists. It was arity - 1 while this was the top rung; a
           later rung appends a sentinel and that equality would fail for every rung but the newest, which
           is a pin about the ladder's length rather than about this rung. */
        Assert.True(
            ProbeOrdinal < arity,
            $"sentinel ordinal {ProbeOrdinal} is outside the probe's {arity} parameters");

        /* A store migrated to exactly THIS rung: every sentinel up to and including this one true, every
           later one false. Built by reflection so the arity tracks the signature — the literal-true form
           silently defaults a newly added sentinel to false and maps one version low. */
        var throughMine = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, throughMine)!);

        /* One rung behind: this rung's sentinel absent as well must report 111, not 112. Without it the
           arm above could be satisfied by an unconditional return and nothing would notice. */
        var allButMine = Enumerable.Range(0, arity).Select(i => (object)(i < ProbeOrdinal)).ToArray();
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, allButMine)!);
    }

    private static string ReadSource(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        while (dir is not null && !File.Exists(Path.Combine(dir, relativePath)))
        {
            dir = Directory.GetParent(dir)?.FullName;
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relativePath));
    }
}
