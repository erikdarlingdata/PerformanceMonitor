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
/// V111 (#3021)'s half of the viewer's connect-time schema gate.
///
/// <para>Split from <see cref="StoreLogSelfMonitoringStoreTests"/> for one practical reason worth stating:
/// this is the only part of the rung's suite that needs the WPF Viewer assembly, which is
/// <c>net10.0-windows</c>. Everything else in that suite runs against <c>net10.0</c> projects and can
/// therefore be exercised outside the Windows-only harness; keeping this test with it would have made the
/// whole file Windows-only.</para>
/// </summary>
public class StoreLogViewerGateTests
{
    /// <summary>The probe ordinal this rung's sentinel occupies. Its OWN ordinal, which never moves.</summary>
    internal const int ProbeOrdinal = 86;

    /// <summary>
    /// The connect-time gate. A TABLE sentinel, because all three objects are new at this rung. This rung's
    /// sentinel and ordinal are still pinned here — they never move — while a fully-migrated store maps to
    /// whatever the current top rung is.
    /// </summary>
    [Fact]
    public void TheProbeAsksForTheTable_AndMapsAFullyMigratedStoreToThisRung()
    {
        Assert.Contains(
            "table_name = 'store_log_events'",
            ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = ReadSource("Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasStoreLogSelfMonitoring", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* The ordinal has to still exist in the signature, or the literal above is pinning a position that
           was never there. Not `arity - 1` any more: that form asserted this rung was the LAST sentinel,
           which stopped being true the moment a later rung appended its own. */
        Assert.True(ProbeOrdinal < arity);

        /* Every sentinel true = a fully-migrated store, which must map to the CURRENT top rung. Built by
           reflection so the arity tracks the signature — the literal-true form silently defaults a newly
           added sentinel to false and maps one version low. This rung is no longer the top one, so the
           "one rung behind" half of this check now belongs to whichever rung is (V112's own test carries
           it); keeping a copy here that names 110 would assert this rung is still newest, which is how the
           NEXT rung's build goes red — the note V110's test already left for this one. */
        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);
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
