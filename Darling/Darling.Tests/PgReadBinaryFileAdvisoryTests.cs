/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="PgReadBinaryFileAdvisory"/>'s once-a-day gate (#4046), and
/// <see cref="DarlingCollectorRunner.WithReadBinaryFileAdvisoryNote"/>'s use of it alongside
/// <see cref="PgReadBinaryFileCapability"/>'s cache.
/// </summary>
[Collection("pg-read-binary-file-statics")]
public sealed class PgReadBinaryFileAdvisoryTests : IDisposable
{
    public PgReadBinaryFileAdvisoryTests()
    {
        PgReadBinaryFileAdvisory.Reset();
        PgReadBinaryFileAdvisory.NoteInterval = TimeSpan.FromHours(24);
        PgReadBinaryFileAdvisory.ClockForTests = null;
        PgReadBinaryFileCapability.Reset();
    }

    public void Dispose()
    {
        PgReadBinaryFileAdvisory.Reset();
        PgReadBinaryFileAdvisory.NoteInterval = TimeSpan.FromHours(24);
        PgReadBinaryFileAdvisory.ClockForTests = null;
        PgReadBinaryFileCapability.Reset();
    }

    [Fact]
    public void TheFirstCycleNotes()
    {
        Assert.True(PgReadBinaryFileAdvisory.ShouldNote("target-a"));
    }

    [Fact]
    public void ASecondCycleInsideTheIntervalDoesNotNote()
    {
        Assert.True(PgReadBinaryFileAdvisory.ShouldNote("target-a"));
        Assert.False(PgReadBinaryFileAdvisory.ShouldNote("target-a"));
    }

    /// <summary>The gate is judged on the advisory's own clock: a fake clock steps 19 ms (inside a 20 ms interval, no
    /// note) and then 2 ms more (past it, notes again), so no wall-clock pause between two calls can cross the
    /// interval on its own.</summary>
    [Fact]
    public void AfterTheIntervalItNotesAgain()
    {
        var now = new DateTime(2026, 10, 14, 12, 0, 0, DateTimeKind.Utc);
        PgReadBinaryFileAdvisory.ClockForTests = () => now;
        PgReadBinaryFileAdvisory.NoteInterval = TimeSpan.FromMilliseconds(20);

        Assert.True(PgReadBinaryFileAdvisory.ShouldNote("target-a"));

        now = now.AddMilliseconds(19);
        Assert.False(PgReadBinaryFileAdvisory.ShouldNote("target-a"));

        now = now.AddMilliseconds(2);
        Assert.True(PgReadBinaryFileAdvisory.ShouldNote("target-a"));
    }

    [Fact]
    public void PerTargetGatesAreIndependent()
    {
        Assert.True(PgReadBinaryFileAdvisory.ShouldNote("target-a"));
        Assert.True(PgReadBinaryFileAdvisory.ShouldNote("target-b"));
    }

    private static CollectorRunResult PlainResult() => new(0, 0, 0, CollectorContext.NoMeasurements);

    /// <summary>A granted target never notes: <c>WithReadBinaryFileAdvisoryNote</c> reads the SAME cache
    /// <see cref="PgReadBinaryFileCapability"/> keeps, so a target the capability probed as granted must
    /// never carry the nudge meant for one that has not granted it yet.</summary>
    [Fact]
    public async Task AGrantedTargetNeverNotes()
    {
        var connection = new PgReadBinaryFileCapabilityTests.FakeScalarConnection { Scalar = "UTF8:true" };
        await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", default);

        var result = DarlingCollectorRunner.WithReadBinaryFileAdvisoryNote(PlainResult(), PgReadBinaryFileCapabilityTests.Runtime("target-a"));

        Assert.Null(result.HostNote);
    }

    /// <summary>An ungranted target notes once, using the SAME cache the collector run itself already
    /// populated this cycle — no second round trip.</summary>
    [Fact]
    public async Task AnUngrantedTargetNotesOncePerInterval()
    {
        var connection = new PgReadBinaryFileCapabilityTests.FakeScalarConnection { Scalar = "UTF8:false" };
        await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", default);

        var first = DarlingCollectorRunner.WithReadBinaryFileAdvisoryNote(PlainResult(), PgReadBinaryFileCapabilityTests.Runtime("target-a"));
        Assert.Contains("pg_read_binary_file", first.HostNote, StringComparison.Ordinal);
        Assert.Contains("#4046", first.HostNote, StringComparison.Ordinal);

        var second = DarlingCollectorRunner.WithReadBinaryFileAdvisoryNote(PlainResult(), PgReadBinaryFileCapabilityTests.Runtime("target-a"));
        Assert.Null(second.HostNote);
    }

    /// <summary>A target this capability was never checked for (a managed target, which reaches its log
    /// through the RDS API) never notes — <c>TryGetCachedVerdict</c> finds nothing to key off.</summary>
    [Fact]
    public void ANeverCheckedTargetNeverNotes()
    {
        var result = DarlingCollectorRunner.WithReadBinaryFileAdvisoryNote(PlainResult(), PgReadBinaryFileCapabilityTests.Runtime("managed-target"));

        Assert.Null(result.HostNote);
    }
}
