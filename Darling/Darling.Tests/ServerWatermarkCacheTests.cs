/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4197 part b — pins for the pure <see cref="ServerWatermarkCache"/> class DarlingCollectorRunner now
/// wires in. These are net10.0-windows and cannot execute on this machine; each states the RED case
/// against pre-fix dev (where <c>ServerWatermarkCache.cs</c> does not exist, so every one of these types
/// fails to resolve and the whole file fails to compile) rather than a runtime assertion failure. Proven
/// GREEN against this branch's build via a throwaway net10.0 reflection harness (not committed) before
/// this file was added — see the lane report for the 13/13 result.
/// </summary>
public sealed class ServerWatermarkCacheTests
{
    /// <summary>RED on dev: ServerWatermarkCache does not exist there, so this fails to compile.</summary>
    [Fact]
    public void Miss_ReturnsNull()
    {
        var cache = new ServerWatermarkCache();
        Assert.Null(cache.TryGet(1, "job_history"));
    }

    /// <summary>RED on dev: same reason — no type to seed.</summary>
    [Fact]
    public void Seed_RoundTripsValueAndNumericTwin()
    {
        var cache = new ServerWatermarkCache();
        var t = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        cache.Seed(1, "job_history", t, fromUtcColumn: false, numericValue: 42L);

        var hit = cache.TryGet(1, "job_history");
        Assert.NotNull(hit);
        Assert.Equal(t, hit!.Value.Value);
        Assert.Equal(42L, hit.Value.NumericValue);
        Assert.False(hit.Value.FromUtcColumn);
    }

    /// <summary>
    /// Monotonic: advancing with an OLDER batch max leaves the cached value unchanged. RED on dev: no
    /// Advance to call, and — if it existed under a naive "last write wins" implementation — this would
    /// fail by replacing the newer cached value with the older one just supplied.
    /// </summary>
    [Fact]
    public void Advance_WithOlderValue_LeavesCacheUnchanged()
    {
        var cache = new ServerWatermarkCache();
        var newer = new DateTime(2026, 1, 2, 0, 0, 0, DateTimeKind.Utc);
        var older = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        cache.Seed(1, "job_history", newer, fromUtcColumn: false, numericValue: 100L);
        cache.Advance(1, "job_history", older, fromUtcColumn: false, batchMaxNumericValue: 50L);

        var hit = cache.TryGet(1, "job_history");
        Assert.Equal(newer, hit!.Value.Value);
        Assert.Equal(100L, hit.Value.NumericValue);
    }

    /// <summary>Advance with a NEWER value moves the cache forward. RED on dev: no Advance to call.</summary>
    [Fact]
    public void Advance_WithNewerValue_MovesForward()
    {
        var cache = new ServerWatermarkCache();
        var older = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var newer = new DateTime(2026, 1, 2, 0, 0, 0, DateTimeKind.Utc);

        cache.Seed(1, "job_history", older, fromUtcColumn: false, numericValue: 50L);
        cache.Advance(1, "job_history", newer, fromUtcColumn: false, batchMaxNumericValue: 100L);

        var hit = cache.TryGet(1, "job_history");
        Assert.Equal(newer, hit!.Value.Value);
        Assert.Equal(100L, hit.Value.NumericValue);
    }

    /// <summary>
    /// Per-server isolation: server A's advance never changes server B's entry for the same collector
    /// name. RED on dev: no type to construct two independent keys against.
    /// </summary>
    [Fact]
    public void PerServerIsolation_AdvanceOnOneServer_LeavesTheOtherUntouched()
    {
        var cache = new ServerWatermarkCache();
        var t1 = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var t2 = new DateTime(2026, 1, 5, 0, 0, 0, DateTimeKind.Utc);

        cache.Seed(1, "job_history", t1, fromUtcColumn: false, numericValue: 10L);
        cache.Seed(2, "job_history", t1, fromUtcColumn: false, numericValue: 10L);

        cache.Advance(1, "job_history", t2, fromUtcColumn: false, batchMaxNumericValue: 99L);

        Assert.Equal(t2, cache.TryGet(1, "job_history")!.Value.Value);
        Assert.Equal(t1, cache.TryGet(2, "job_history")!.Value.Value);
    }

    /// <summary>
    /// Invalidate drops exactly the one (server, collector) entry — a fault in that run. RED on dev: no
    /// Invalidate to call.
    /// </summary>
    [Fact]
    public void Invalidate_DropsOnlyThatEntry()
    {
        var cache = new ServerWatermarkCache();
        var t = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        cache.Seed(1, "job_history", t, fromUtcColumn: false, numericValue: 1L);
        cache.Seed(1, "default_trace_events", t, fromUtcColumn: false, numericValue: null);

        cache.Invalidate(1, "job_history");

        Assert.Null(cache.TryGet(1, "job_history"));
        Assert.NotNull(cache.TryGet(1, "default_trace_events"));
    }

    /// <summary>
    /// InvalidateServer drops every collector entry for that server (reconnect / re-add) and leaves other
    /// servers' entries alone. RED on dev: no InvalidateServer to call.
    /// </summary>
    [Fact]
    public void InvalidateServer_DropsEveryCollectorForThatServer_LeavesOthers()
    {
        var cache = new ServerWatermarkCache();
        var t = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        cache.Seed(1, "job_history", t, fromUtcColumn: false, numericValue: 1L);
        cache.Seed(1, "default_trace_events", t, fromUtcColumn: false, numericValue: null);
        cache.Seed(2, "job_history", t, fromUtcColumn: false, numericValue: 1L);

        cache.InvalidateServer(1);

        Assert.Null(cache.TryGet(1, "job_history"));
        Assert.Null(cache.TryGet(1, "default_trace_events"));
        Assert.NotNull(cache.TryGet(2, "job_history"));
    }

    /// <summary>
    /// #3778 UTC-twin frame flag rides WITH the value it describes: advancing to a newer value adopts
    /// that value's frame rather than keeping the old frame beside a new value. RED on dev: no
    /// ServerWatermarkEntry.FromUtcColumn to assert against.
    /// </summary>
    [Fact]
    public void Advance_NewerValueCarriesItsOwnFrameFlag()
    {
        var cache = new ServerWatermarkCache();
        var older = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var newer = new DateTime(2026, 1, 2, 0, 0, 0, DateTimeKind.Utc);

        cache.Seed(1, "cpu_utilization", older, fromUtcColumn: false, numericValue: null);
        cache.Advance(1, "cpu_utilization", newer, fromUtcColumn: true, batchMaxNumericValue: null);

        var hit = cache.TryGet(1, "cpu_utilization");
        Assert.Equal(newer, hit!.Value.Value);
        Assert.True(hit.Value.FromUtcColumn);
    }

    /// <summary>
    /// A definition that declares no <c>WatermarkValueAccessor</c> (the default, and pg_cpu_utilization's
    /// permanent state per #4197 part b's census — <c>Targets/RdsCpuIngestor.cs</c> is a second writer)
    /// opts out at the definition level: <c>CollectorDefinitionBase&lt;TRow&gt;.WatermarkValueAccessor</c>
    /// is null by default. RunAsync's <c>watermarkCacheEligible</c> gate reads this directly, so a
    /// collector that never overrides it can never seed, hit, or advance the cache — this pins the
    /// DEFAULT rather than the runner wiring (which needs a live/counting store and is
    /// lane-4197b-measure's live equivalence pin). RED on dev: CollectorDefinitionBase has no
    /// WatermarkValueAccessor member at all.
    /// </summary>
    [Fact]
    public void PgCpuUtilization_DeclaresNoAccessor_SoItStaysOutOfTheCacheByDefault()
    {
        Assert.Null(PgCpuUtilizationCollector.Instance.WatermarkValueAccessor);
        Assert.Null(PgCpuUtilizationCollector.Instance.NumericWatermarkValueAccessor);
    }

    /// <summary>
    /// The four collectors this lane put IN the cache declare a live accessor. RED on dev: none of these
    /// members exist.
    /// </summary>
    [Fact]
    public void TheFourCachedCollectors_DeclareALiveWatermarkAccessor()
    {
        Assert.NotNull(JobHistoryCollector.Instance.WatermarkValueAccessor);
        Assert.NotNull(JobHistoryCollector.Instance.NumericWatermarkValueAccessor);
        Assert.NotNull(DefaultTraceEventsCollector.Instance.WatermarkValueAccessor);
        Assert.NotNull(SystemHealthEventsCollector.Instance.WatermarkValueAccessor);
        Assert.NotNull(MemoryPressureEventsCollector.Instance.WatermarkValueAccessor);
    }

    /// <summary>
    /// cpu_utilization (the #3778 UTC-twin pair) is excluded from this lane's cache wiring — this lane's
    /// call, stated in the report — and stays that way until the twin's batch-max wiring is done. RED on
    /// dev: WatermarkValueAccessor does not exist to assert Null against (though the value is the same
    /// either way — this pin exists to CATCH a future accidental accessor addition without the twin
    /// wiring, not to prove today's dev state).
    /// </summary>
    [Fact]
    public void CpuUtilization_StaysExcludedUntilTheTwinWiringIsDone()
    {
        Assert.Null(CpuUtilizationCollector.Instance.WatermarkValueAccessor);
    }
}
