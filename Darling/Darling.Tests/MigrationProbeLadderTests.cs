/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The viewer's schema probe names a store's rung from the sentinels it finds, newest first
/// (<c>ViewerDataService.MapProbedSchemaVersion</c>). One theory over <see cref="RungRewindArtifacts.Ladder"/> pins the property
/// every rung's own test file used to pin by hand (#5950): with a rung's sentinel and every earlier one present and every later
/// one absent, the probe answers that rung. A sentinel at the wrong place in the signature, or an arm that returns the wrong
/// version, maps a store one rung off. A new rung adds one <see cref="RungRewindArtifacts.Ladder"/> entry and no test here.
/// </summary>
public sealed class MigrationProbeLadderTests
{
    private static readonly MethodInfo Map = typeof(ViewerDataService).GetMethod(
        "MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;

    public static IEnumerable<object[]> Rungs() =>
        RungRewindArtifacts.Ladder.Select(r => new object[] { r.Version, r.ProbeParameter });

    /// <summary>
    /// The probe answers rung k when sentinel k is the newest one present. The ladder's older entries answer the "stopped one
    /// rung behind" cases the per-rung tests used to spell out (sentinel k absent gives k - 1).
    /// </summary>
    [Theory]
    [MemberData(nameof(Rungs))]
    public void TheProbeNamesTheRungOfTheNewestSentinelPresent(int version, string probeParameter)
    {
        var parameters = Map.GetParameters();
        var ordinal = Array.FindIndex(parameters, p => p.Name == probeParameter);
        Assert.True(ordinal >= 0, $"MapProbedSchemaVersion has no parameter {probeParameter}: fix the ladder entry for V{version}");

        var present = Enumerable.Range(0, parameters.Length).Select(i => (object)(i <= ordinal)).ToArray();
        Assert.Equal(version, (int)Map.Invoke(null, present)!);
    }

    /// <summary>
    /// A fully migrated store maps to the store's schema version, and the ladder's last entry is that version: a rung added
    /// without its ladder entry fails here, and the message says what to add.
    /// </summary>
    [Fact]
    public void AFullyMigratedStoreMapsToTheNewestLadderRung()
    {
        var parameters = Map.GetParameters();
        var allPresent = parameters.Select(_ => (object)true).ToArray();
        var newest = RungRewindArtifacts.Ladder.Max(r => r.Version);

        Assert.Equal(StorageVersion.SchemaVersion, (int)Map.Invoke(null, allPresent)!);
        Assert.True(
            newest == StorageVersion.SchemaVersion,
            $"RungRewindArtifacts.Ladder ends at V{newest}, but the store is at V{StorageVersion.SchemaVersion}: add the new rung's entry (version, probe parameter, drop SQL)");
    }
}
