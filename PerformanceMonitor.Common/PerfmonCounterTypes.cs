/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Common;

/// <summary>
/// What a stored <c>perfmon_stats.cntr_type</c> says the row's number IS — the three-way reading every
/// consumer of the perfmon family makes since Darling V132 / Lite v62 (#3653 A7).
/// </summary>
public enum PerfmonCounterKind
{
    /// <summary>A monotonically increasing count (<c>PERF_COUNTER_BULK_COUNT</c>, <c>PERF_COUNTER_COUNTER</c>):
    /// the per-second value is the stored delta over the stored interval. <c>Batch Requests/sec</c>.</summary>
    Rate,

    /// <summary>A level read at collection time (<c>PERF_COUNTER_LARGE_RAWCOUNT</c>, <c>PERF_COUNTER_RAWCOUNT</c>):
    /// the stored <c>cntr_value</c> IS the value, a delta is noise, and a falling level is not a counter reset.
    /// <c>Total Server Memory (KB)</c>, <c>Memory Grants Pending</c>, <c>Processes blocked</c>.</summary>
    Gauge,

    /// <summary>Anything else the engine reports: the numerator of an average (<c>PERF_AVERAGE_BULK</c>), a
    /// fraction (<c>PERF_LARGE_RAW_FRACTION</c>), the base a numerator divides by (<c>PERF_LARGE_RAW_BASE</c>),
    /// or an id this vocabulary has never seen. The stored delta is a real per-interval change of the raw
    /// value; it is neither a per-second rate nor a level, and no reader may call it either.</summary>
    Other,
}

/// <summary>
/// The <c>sys.dm_os_performance_counters.cntr_type</c> vocabulary — the Windows performance-counter type ids
/// SQL Server reports — and the one classification every reader of <c>perfmon_stats</c> applies to a stored
/// type (#3653 A7, Darling V132 / Lite v62).
///
/// <para><b>Why the type is stored at all.</b> The collector differenced every counter it read and stored the
/// delta beside the raw value; the store held nothing that said which rows were counts and which were
/// levels, so <c>Total Server Memory (KB)</c> was delta'd like <c>Batch Requests/sec</c>, and a target that
/// released memory — a FALLING level — presented to the shared delta calculator as a counter reset: the
/// (0, 0) "no delta knowable" marker, at exactly the moment the operator would want to see the drop (#3540
/// A7: "a falling gauge = fake counter reset"). Until the rung the viewers classified by a NAME-SUFFIX PROXY
/// (<c>/sec</c> → rate, everything else per interval — <see cref="DeltaSeriesShaping.BasisFor(string?)"/>),
/// which could label a level honestly as "Δ per interval" but could never plot it as the level it is. With
/// the type stored, the write path stops differencing gauges (the collector writes the level with NULL
/// delta and NULL interval — <c>PerfmonStatsCollector</c>) and the read path plots a gauge as its value.</para>
///
/// <para><b>The ids.</b> The DMV column is the raw Windows <c>PERF_*</c> type word. SQL Server's own counters
/// use the 64-bit ("LARGE") variants exclusively; the 32-bit siblings are listed because the vocabulary
/// should not fall to <see cref="PerfmonCounterKind.Other"/> on an id whose meaning is documented, and a
/// user-supplied counter list (<c>perfmon_counters.json</c>) can name anything the DMV reports.
/// <list type="bullet">
/// <item><description><c>272696576</c> <c>PERF_COUNTER_BULK_COUNT</c> and <c>272696320</c> <c>PERF_COUNTER_COUNTER</c>
/// — rates. The value accumulates; the number an operator wants is Δvalue / Δseconds.</description></item>
/// <item><description><c>65792</c> <c>PERF_COUNTER_LARGE_RAWCOUNT</c> and <c>65536</c> <c>PERF_COUNTER_RAWCOUNT</c>
/// — gauges. The value is the reading.</description></item>
/// <item><description><c>1073874176</c> <c>PERF_AVERAGE_BULK</c> — the numerator of an average whose denominator is a
/// sibling row of type <c>1073939712</c> <c>PERF_LARGE_RAW_BASE</c>; the honest figure is Δnumerator / Δbase,
/// which needs both rows and a join this store does not make. <c>537003264</c> <c>PERF_LARGE_RAW_FRACTION</c>
/// (and <c>537003008</c> <c>PERF_RAW_FRACTION</c>) — a value over a base, same join. All
/// <see cref="PerfmonCounterKind.Other"/>: their stored delta is a per-interval change of the raw value and is
/// plotted as exactly that, under a label that says so. The default counter list carries ten such rows per
/// collection — the <c>Average wait time (ms)</c> instance of each <c>SQLServer:Wait Statistics</c> counter —
/// and plotting their Δnumerator as "Δ per interval" is true (it is the wait milliseconds that accrued) while
/// their instance name promises an average this store cannot compute without the base. That is a stated
/// finding of the rung that stored the type, not something it fixes.</description></item>
/// </list></para>
///
/// <para><b>One vocabulary, two projects.</b> The COLLECTOR decides the write shape from the same gauge set
/// (<c>PerfmonStatsCollector.GaugeCounterTypes</c>), but <c>PerformanceMonitor.Collectors</c> and this
/// assembly reference neither each other nor a third that could hold the ids, so the collector spells its
/// set itself and <c>Lite.Tests/PerfmonCounterTypeTests</c> asserts the two sets equal — the twin-constant
/// idiom the repo uses wherever an assembly boundary forbids one declaration.</para>
/// </summary>
public static class PerfmonCounterTypes
{
    /// <summary><c>PERF_COUNTER_BULK_COUNT</c> — a 64-bit rate counter. The type of almost every <c>/sec</c> counter.</summary>
    public const int PerfCounterBulkCount = 272696576;

    /// <summary><c>PERF_COUNTER_COUNTER</c> — the 32-bit rate counter.</summary>
    public const int PerfCounterCounter = 272696320;

    /// <summary><c>PERF_COUNTER_LARGE_RAWCOUNT</c> — a 64-bit gauge. The type of every memory, queue and
    /// "currently" counter SQL Server reports.</summary>
    public const int PerfCounterLargeRawCount = 65792;

    /// <summary><c>PERF_COUNTER_RAWCOUNT</c> — the 32-bit gauge.</summary>
    public const int PerfCounterRawCount = 65536;

    /// <summary><c>PERF_AVERAGE_BULK</c> — the numerator of an average; divide its delta by the delta of its
    /// <see cref="PerfLargeRawBase"/> sibling for the figure the name promises.</summary>
    public const int PerfAverageBulk = 1073874176;

    /// <summary><c>PERF_LARGE_RAW_BASE</c> — the denominator row an average or a fraction divides by.</summary>
    public const int PerfLargeRawBase = 1073939712;

    /// <summary><c>PERF_LARGE_RAW_FRACTION</c> — a value over a <see cref="PerfLargeRawBase"/> (<c>Buffer cache hit ratio</c>).</summary>
    public const int PerfLargeRawFraction = 537003264;

    /// <summary><c>PERF_RAW_FRACTION</c> — the 32-bit fraction.</summary>
    public const int PerfRawFraction = 537003008;

    /// <summary>The ids that are rates: the per-second figure is the stored delta over the stored interval.</summary>
    public static readonly IReadOnlySet<int> RateTypes = new HashSet<int> { PerfCounterBulkCount, PerfCounterCounter };

    /// <summary>The ids that are gauges: the stored <c>cntr_value</c> is the reading and no delta is written for
    /// them. The collector's <c>GaugeCounterTypes</c> is this set, spelled on its side of the assembly boundary
    /// and pinned equal.</summary>
    public static readonly IReadOnlySet<int> GaugeTypes = new HashSet<int> { PerfCounterLargeRawCount, PerfCounterRawCount };

    /// <summary>The three-way reading of a stored type. An id this vocabulary has never seen is
    /// <see cref="PerfmonCounterKind.Other"/>: the one reading under which the stored delta is neither
    /// divided nor mistaken for a level.</summary>
    public static PerfmonCounterKind Kind(int cntrType)
    {
        if (RateTypes.Contains(cntrType)) return PerfmonCounterKind.Rate;
        if (GaugeTypes.Contains(cntrType)) return PerfmonCounterKind.Gauge;
        return PerfmonCounterKind.Other;
    }

    /// <summary><see cref="Kind(int)"/> for a stored column that may be NULL — a row written before the rung
    /// stored the type. NULL in, null out: the caller decides what "unknown" means for its surface (the viewers
    /// fall back to the name proxy; the MCP tools publish null and say so).</summary>
    public static PerfmonCounterKind? Kind(int? cntrType) => cntrType is int t ? Kind(t) : null;

    /// <summary>The wire spelling of a kind for the MCP perfmon tools — <c>rate</c>, <c>gauge</c>, <c>other</c> —
    /// or null for a pre-rung row. Lower-case single words, one spelling on both SKUs.</summary>
    public static string? Word(int? cntrType) => Kind(cntrType) switch
    {
        PerfmonCounterKind.Rate => "rate",
        PerfmonCounterKind.Gauge => "gauge",
        PerfmonCounterKind.Other => "other",
        _ => null,
    };
}
