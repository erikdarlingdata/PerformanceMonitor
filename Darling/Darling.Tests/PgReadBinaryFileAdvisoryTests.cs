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
        PgReadBinaryFileCapability.Reset();
    }

    public void Dispose()
    {
        PgReadBinaryFileAdvisory.Reset();
        PgReadBinaryFileAdvisory.NoteInterval = TimeSpan.FromHours(24);
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

    [Fact]
    public async Task AfterTheIntervalItNotesAgain()
    {
        PgReadBinaryFileAdvisory.NoteInterval = TimeSpan.FromMilliseconds(20);

        Assert.True(PgReadBinaryFileAdvisory.ShouldNote("target-a"));
        Assert.False(PgReadBinaryFileAdvisory.ShouldNote("target-a"));

        await Task.Delay(60);

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
        var connection = new PgReadBinaryFileCapabilityTests.FakeScalarConnection { Scalar = true };
        await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", default);

        var result = DarlingCollectorRunner.WithReadBinaryFileAdvisoryNote(PlainResult(), "target-a");

        Assert.Null(result.HostNote);
    }

    /// <summary>An ungranted target notes once, using the SAME cache the collector run itself already
    /// populated this cycle — no second round trip.</summary>
    [Fact]
    public async Task AnUngrantedTargetNotesOncePerInterval()
    {
        var connection = new PgReadBinaryFileCapabilityTests.FakeScalarConnection { Scalar = false };
        await PgReadBinaryFileCapability.IsGrantedAsync(connection, "target-a", default);

        var first = DarlingCollectorRunner.WithReadBinaryFileAdvisoryNote(PlainResult(), "target-a");
        Assert.Contains("pg_read_binary_file", first.HostNote, StringComparison.Ordinal);
        Assert.Contains("#4046", first.HostNote, StringComparison.Ordinal);

        var second = DarlingCollectorRunner.WithReadBinaryFileAdvisoryNote(PlainResult(), "target-a");
        Assert.Null(second.HostNote);
    }

    /// <summary>A target this capability was never checked for (a managed target, which reaches its log
    /// through the RDS API) never notes — <c>TryGetCachedVerdict</c> finds nothing to key off.</summary>
    [Fact]
    public void ANeverCheckedTargetNeverNotes()
    {
        var result = DarlingCollectorRunner.WithReadBinaryFileAdvisoryNote(PlainResult(), "managed-target");

        Assert.Null(result.HostNote);
    }
}
