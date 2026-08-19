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
using PerformanceMonitor.Alerting;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2362: every fingerprinted alert accumulates a monotonic total, not just blocking and deadlocks.
///
/// <para><b>Why the observation list must be UNCAPPED.</b> Each context builder renders a capped subset — 3 for
/// long-running queries and anomalous jobs, 5 for volumes, PVS and failed jobs — because a card with fifty
/// entries helps nobody. The render cap is a display budget. A fingerprint outside it still has a live incident,
/// and observing only the displayed subset would reset the total of anything that fell out of the top N the
/// moment it surfaced again: a subtler version of the undercount #2216 exists to fix, reintroduced by the fix
/// for it. <c>BlockingIncidents</c> already carries that reasoning; these five now share it.</para>
/// </summary>
public class IncidentObservationCoverageTests
{
    private const string Server = "SQLPROD01";

    /* Render caps, from each context builder's own GetRange(0, Math.Min(N, ...)). The point of every test
       below is that the OBSERVATION list is larger than these. */
    private const int LongRunningRenderCap = 3;
    private const int VolumeRenderCap = 5;
    private const int PvsRenderCap = 5;
    private const int AnomalousJobRenderCap = 3;
    private const int FailedJobRenderCap = 5;

    /// <summary>
    /// The whole point, stated once per alert: hand each builder MORE items than its card renders and get an
    /// incident for every one of them. A regression that reintroduced the cap would fail here by count.
    /// </summary>
    [Fact]
    public void EveryBuilder_ObservesBeyondItsRenderCap()
    {
        var lrq = AlertContextBuilders.LongRunningQueryIncidents(Server,
            Enumerable.Range(0, LongRunningRenderCap + 4)
                .Select(i => new LongRunningQueryInfo { QueryHash = $"0xHASH{i}", DatabaseName = "OrdersDB" }).ToList());
        Assert.Equal(LongRunningRenderCap + 4, lrq.Count);

        var vol = AlertContextBuilders.VolumeFreeSpaceIncidents(Server,
            Enumerable.Range(0, VolumeRenderCap + 4)
                .Select(i => new VolumeFreeSpaceInfo { MountPoint = $"{(char)('D' + i)}:\\" }).ToList());
        Assert.Equal(VolumeRenderCap + 4, vol.Count);

        var pvs = AlertContextBuilders.PvsPressureIncidents(Server,
            Enumerable.Range(0, PvsRenderCap + 4)
                .Select(i => new PvsPressureInfo { DatabaseName = $"DB{i}" }).ToList());
        Assert.Equal(PvsRenderCap + 4, pvs.Count);

        var anomalous = AlertContextBuilders.AnomalousJobIncidents(Server,
            Enumerable.Range(0, AnomalousJobRenderCap + 4)
                .Select(i => new AnomalousJobInfo { JobName = $"Job{i}" }).ToList());
        Assert.Equal(AnomalousJobRenderCap + 4, anomalous.Count);

        var failed = AlertContextBuilders.FailedJobIncidents(Server,
            Enumerable.Range(0, FailedJobRenderCap + 4)
                .Select(i => new FailedJobInfo { JobName = $"Job{i}" }).ToList());
        Assert.Equal(FailedJobRenderCap + 4, failed.Count);
    }

    /// <summary>
    /// Distinct fingerprints, because the accumulator keys on <c>DedupKey</c>: if two different volumes or
    /// databases collapsed to one key their totals would merge, which is worse than not counting them.
    /// </summary>
    [Fact]
    public void Incidents_CarryDistinctDedupKeys()
    {
        var vol = AlertContextBuilders.VolumeFreeSpaceIncidents(Server,
            new List<VolumeFreeSpaceInfo>
            {
                new() { MountPoint = @"C:\" }, new() { MountPoint = @"D:\" }, new() { MountPoint = @"E:\" },
            });

        Assert.Equal(3, vol.Select(i => i.DedupKey).Distinct(StringComparer.Ordinal).Count());
        Assert.DoesNotContain(vol, i => string.IsNullOrEmpty(i.DedupKey));
    }

    /// <summary>
    /// A null or empty source is an empty list, never a throw. These run on every sweep, including the ones
    /// where a fetch came back with nothing, and an observation path that throws would take the check down.
    /// </summary>
    [Fact]
    public void EveryBuilder_ToleratesNullAndEmpty()
    {
        Assert.Empty(AlertContextBuilders.LongRunningQueryIncidents(Server, null));
        Assert.Empty(AlertContextBuilders.VolumeFreeSpaceIncidents(Server, null));
        Assert.Empty(AlertContextBuilders.PvsPressureIncidents(Server, null));
        Assert.Empty(AlertContextBuilders.AnomalousJobIncidents(Server, null));
        Assert.Empty(AlertContextBuilders.FailedJobIncidents(Server, null));

        Assert.Empty(AlertContextBuilders.LongRunningQueryIncidents(Server, new List<LongRunningQueryInfo>()));
        Assert.Empty(AlertContextBuilders.VolumeFreeSpaceIncidents(Server, new List<VolumeFreeSpaceInfo>()));
        Assert.Empty(AlertContextBuilders.PvsPressureIncidents(Server, new List<PvsPressureInfo>()));
        Assert.Empty(AlertContextBuilders.AnomalousJobIncidents(Server, new List<AnomalousJobInfo>()));
        Assert.Empty(AlertContextBuilders.FailedJobIncidents(Server, new List<FailedJobInfo>()));
    }

    /// <summary>
    /// A long-running query with no <c>query_hash</c> produces no incident rather than an incident keyed on the
    /// empty string — every hashless query would otherwise share one fingerprint and pool its total.
    /// </summary>
    [Fact]
    public void ALongRunningQueryWithNoHash_ProducesNoIncident()
    {
        var incidents = AlertContextBuilders.LongRunningQueryIncidents(Server,
            new List<LongRunningQueryInfo>
            {
                new() { QueryHash = null, DatabaseName = "OrdersDB" },
                new() { QueryHash = "0xABC", DatabaseName = "OrdersDB" },
            });

        Assert.Single(incidents);
    }

    /// <summary>
    /// The render path and the observation path must agree on what a fingerprint IS, or a total would attach to
    /// a key the card never shows. Same builder, same server, the capped input the card renders: the keys it
    /// produces are a prefix of the uncapped set.
    /// </summary>
    [Fact]
    public void TheRenderedSubset_UsesTheSameFingerprintsAsTheObservation()
    {
        var all = Enumerable.Range(0, PvsRenderCap + 3)
            .Select(i => new PvsPressureInfo { DatabaseName = $"DB{i}" }).ToList();

        var observed = AlertContextBuilders.PvsPressureIncidents(Server, all);
        var rendered = AlertContextBuilders.PvsPressureIncidents(Server, all.GetRange(0, PvsRenderCap));

        Assert.Equal(PvsRenderCap, rendered.Count);
        Assert.True(observed.Count > rendered.Count, "the observation must be wider than the render");
        Assert.Equal(
            rendered.Select(i => i.DedupKey).ToList(),
            observed.Take(PvsRenderCap).Select(i => i.DedupKey).ToList());
    }
}
