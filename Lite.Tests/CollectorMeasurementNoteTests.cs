/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3161, Lite half: this SKU's run note is COMPOSED from the runner's own note and the counts the
/// DEFINITION measured, and there is no way to read one without the other.
///
/// <para><b>Why this half exists at all.</b> Lite has no PostgreSQL target, so the collectors that
/// motivated #3161 are Darling-only — but the seam is engine-agnostic and the SQL Server collectors have
/// the identical silence: <c>blocked_process_report</c>, <c>dmv_blocking_snapshot</c> and
/// <c>deadlocks</c> all record SUCCESS with zero rows and no note on a quiet fleet. A shared member wired
/// into one runner reads as a permanently-empty value in the other SKU and NOTHING FAILS TO BUILD, which
/// is exactly the parity failure CONTRIBUTING's two-store rules name — so it is both SKUs or neither, and
/// the consumer #3161 shipped with is a SQL Server collector that runs on both.</para>
///
/// <para>Darling's twin pins are in <c>Darling.Tests/CollectorMeasurementSeamTests.cs</c>, which also
/// carries the engine-neutral half: the label grammar, the no-rendered-conclusion guard, and the source
/// walk over every shipped label. This file asserts against the LIVE telemetry object, which that project
/// cannot reference.</para>
/// </summary>
public class CollectorMeasurementNoteTests
{
    [Fact]
    public void The_Run_Note_Carries_Both_Halves_Host_First()
    {
        var telemetry = CreateService().TelemetryFor(1);

        telemetry.HostNote = EnumeratedCollectorDriver.EmptyEnumerationMessage;
        telemetry.Measurements.Add(new CollectorMeasurement("events_read", 40));
        telemetry.Measurements.Add(new CollectorMeasurement("events_stored", 0));

        /* Host note first, because both hosts truncate error_message and the classified half is the one
           that has to survive a cut. Same order, same separator and the same shared Compose that Darling's
           CollectorRunResult.Note computes through, so the two SKUs cannot come to disagree about what a
           run note contains. */
        Assert.Equal(
            EnumeratedCollectorDriver.EmptyEnumerationMessage + "; events_read=40 events_stored=0",
            telemetry.Note);
    }

    [Fact]
    public void The_Definition_Half_Reaches_The_Row_With_No_Host_Note_At_All()
    {
        /* THE case #3161 was filed about: a run that succeeded, stored nothing, and had no runner-authored
           note to hang anything off. Before this seam the column was NULL here, which is what made 85,549
           empty SUCCESS runs indistinguishable from an idle fleet (#3030). */
        var telemetry = CreateService().TelemetryFor(1);

        telemetry.Measurements.Add(new CollectorMeasurement("events_read", 0));
        telemetry.Measurements.Add(new CollectorMeasurement("events_stored", 0));

        Assert.Null(telemetry.HostNote);
        Assert.Equal("events_read=0 events_stored=0", telemetry.Note);
    }

    [Fact]
    public void A_Collector_That_Measures_Nothing_Still_Leaves_The_Column_Null()
    {
        /* 69 collectors measure nothing and none of them may start writing a note. */
        var telemetry = CreateService().TelemetryFor(1);

        Assert.Null(telemetry.Note);
    }

    [Fact]
    public void The_Note_Cannot_Be_Assigned_So_The_Host_Cannot_Drop_The_Definition_Half()
    {
        /* The parity guarantee, asserted rather than described: there is no spelling of "the host note
           alone" for the read site to reach for. While Note was settable, a runner could assign the host
           half and the definition's counts would never reach the column — with nothing failing to build,
           in either SKU. Making it computed is also what made the compiler enumerate the six sites that
           used to assign it. */
        var note = typeof(RemoteCollectorService.RunTelemetry)
            .GetProperty(nameof(RemoteCollectorService.RunTelemetry.Note));

        Assert.NotNull(note);
        Assert.Null(note!.SetMethod);
    }

    [Fact]
    public void Resetting_Clears_Both_Halves_So_One_Run_Cannot_Annotate_The_Next()
    {
        /* The note rides the same per-run slot as the sql/storage timings, so BOTH halves have to clear at
           the top of every definition run. Clearing only the host half would leave one collector's counts
           on the next collector's row — and the counts are the half that looks authoritative, because it
           looks like a measurement of the collector whose row it is sitting on. */
        var telemetry = CreateService().TelemetryFor(1);

        telemetry.HostNote = EnumeratedCollectorDriver.EmptyEnumerationMessage;
        telemetry.Measurements.Add(new CollectorMeasurement("events_read", 40));
        Assert.NotNull(telemetry.Note);

        telemetry.ResetNote();

        Assert.Null(telemetry.HostNote);
        Assert.Empty(telemetry.Measurements);
        Assert.Null(telemetry.Note);
    }

    [Fact]
    public void One_Servers_Counts_Cannot_Land_On_Another_Servers_Row()
    {
        /* A collection cycle runs the monitored servers in PARALLEL on one service instance, and the
           measurement list is now part of what the per-server slot holds. A shared list would blend two
           servers' counts into one figure that describes neither — and unlike a blended note, a blended
           COUNT reads as a valid measurement. */
        var service = CreateService();

        var serverA = service.TelemetryFor(1);
        var serverB = service.TelemetryFor(2);

        Assert.NotSame(serverA, serverB);
        Assert.NotSame(serverA.Measurements, serverB.Measurements);

        serverA.Measurements.Add(new CollectorMeasurement("events_read", 40));
        serverB.ResetNote();

        Assert.Equal("events_read=40", serverA.Note);
        Assert.Null(serverB.Note);
        Assert.Same(serverA, service.TelemetryFor(1));
    }

    /* ── helpers ── */

    private static RemoteCollectorService CreateService() =>
        new(duckDb: null!, serverManager: null!, scheduleManager: null!);
}
