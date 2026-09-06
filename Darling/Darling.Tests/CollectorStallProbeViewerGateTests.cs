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

    /// <summary>The table the rung creates.</summary>
    private const string TableName = "collector_stall_probes";

    /// <summary>
    /// The connect-time gate. A TABLE sentinel, because the table is the only object the rung creates. Being
    /// the TOP rung, a fully-migrated store must map to exactly this version or the viewer refuses a store
    /// that is perfectly current — permanently, because no later upgrade changes the answer.
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

        /* The sentinel count and the ordinal have to agree, or the ordinal literal above is pinning a
           position that no longer exists. */
        Assert.Equal(arity - 1, ProbeOrdinal);

        /* Every sentinel true = a fully-migrated store, which must map to THIS rung. As the top rung this is
           also the "and no more than that" guard: a later rung appending a sentinel without its own arm
           would leave this returning 112 for a store that is actually further along. Built by reflection so
           the arity tracks the signature — the literal-true form silently defaults a newly added sentinel to
           false and maps one version low. */
        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        /* One rung behind: every sentinel present EXCEPT this one must report 111, not 112. Without this the
           arm above could be satisfied by an unconditional return and nothing would notice. */
        var allButMine = Enumerable.Repeat((object)true, arity).ToArray();
        allButMine[ProbeOrdinal] = false;
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
