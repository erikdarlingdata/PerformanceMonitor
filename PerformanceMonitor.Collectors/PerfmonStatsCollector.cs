/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Key performance counters from sys.dm_os_performance_counters. Extracted verbatim from Lite's
/// RemoteCollectorService.Perfmon.cs — the curated default counter list is parity brain and lives
/// HERE; hosts may supply an override via <see cref="CollectorContext.PerfmonCounterOverride"/>
/// (Lite: perfmon_counters.json). Counter names interpolate as escaped N'...' literals; one delta
/// group ("perfmon") keyed "{object}|{counter}|{instance}" with the shared gap; the sample interval
/// written per row is the MEASURED gap since the previous sweep, not the configured cadence (#2234).
///
/// <para><b>The delta is computed for RATE counters only</b> (#3653 A7, Darling V132 / Lite v62). The DMV's
/// <c>cntr_type</c> is selected and stored beside every row, and a row whose type is in
/// <see cref="GaugeCounterTypes"/> — a level, not a count — is written as its raw value with NULL delta
/// and NULL interval, never handed to the delta calculator. Before the rung every counter went through
/// the same <c>CalculateDeltaWithInterval</c> call, so <c>Total Server Memory (KB)</c> was differenced like
/// <c>Batch Requests/sec</c>, and a target that released memory — a FALLING level — read to the calculator
/// as a counter reset and stored the (0, 0) "no delta knowable" marker at exactly the moment the drop
/// mattered. Everything that is not a gauge — the rate types and the average/fraction/base family alike —
/// keeps the pre-rung write: delta and measured interval beside the raw value, the type telling the reader
/// which of them to divide. The read-side vocabulary is <c>PerformanceMonitor.Common.PerfmonCounterTypes</c>;
/// the gauge set is spelled on both sides of that assembly boundary and pinned equal.</para>
/// </summary>
public sealed class PerfmonStatsCollector : CollectorDefinitionBase<PerfmonStatsCollector.Row>
{
    public static PerfmonStatsCollector Instance { get; } = new();

    private PerfmonStatsCollector()
    {
    }

    /// <summary>One DMV row: the three-part key, the raw <c>cntr_value</c>, and its <c>cntr_type</c> — the
    /// Windows performance-counter type id the engine reports for the row, which decides whether the raw
    /// value is a count to difference or a level to store as read.</summary>
    public readonly record struct Row(string ObjectName, string CounterName, string InstanceName, long CntrValue, int CntrType);

    /// <summary>
    /// The <c>cntr_type</c> ids for which this collector writes NO delta: <c>PERF_COUNTER_LARGE_RAWCOUNT</c>
    /// (65792) and its 32-bit sibling <c>PERF_COUNTER_RAWCOUNT</c> (65536) — the gauges. A gauge's
    /// <c>cntr_value</c> is the reading; differencing it produces noise, and a falling reading produces the
    /// calculator's reset marker in place of the number the operator wanted.
    ///
    /// <para>This is the write half of one rule whose read half is
    /// <c>PerformanceMonitor.Common.PerfmonCounterTypes.GaugeTypes</c>. The two live in assemblies that
    /// reference neither each other nor a common third (Collectors is kept free of the MCP SDK and the
    /// credential store that Common carries), so the set is spelled twice and
    /// <c>Lite.Tests/PerfmonCounterTypeTests</c> asserts the spellings equal — a type added to one side
    /// without the other fails there, not in a chart.</para>
    /// </summary>
    public static readonly IReadOnlySet<int> GaugeCounterTypes = new HashSet<int> { 65792, 65536 };

    /// <summary>True when the row's stored type is a gauge and the row is written as its level: no delta call,
    /// NULL <c>delta_cntr_value</c>, NULL <c>sample_interval_seconds</c>.</summary>
    public static bool IsGauge(int cntrType) => GaugeCounterTypes.Contains(cntrType);

    public static readonly IReadOnlyList<string> DefaultCounters = new[]
    {
        /* I/O counters */
        "Forwarded Records/sec",
        "Page reads/sec",
        "Page writes/sec",
        "Checkpoint pages/sec",
        "Page lookups/sec",
        "Readahead pages/sec",
        "Background writer pages/sec",
        "Lazy writes/sec",
        "Full Scans/sec",
        "Index Searches/sec",
        "Page Splits/sec",
        "Free list stalls/sec",
        "Non-Page latch waits",
        "Page IO latch waits",
        "Page latch waits",
        /* Transaction counters */
        "Transactions/sec",
        "Longest Transaction Running Time",
        /* Locking counters */
        "Table Lock Escalations/sec",
        "Lock Requests/sec",
        "Lock Wait Time (ms)",
        "Lock Waits/sec",
        "Number of Deadlocks/sec",
        "Lock waits",
        "Processes blocked",
        "Lock Timeouts/sec",
        /* Memory counters */
        "Granted Workspace Memory (KB)",
        "Lock Memory (KB)",
        "Memory Grants Pending",
        "SQL Cache Memory (KB)",
        "Stolen Server Memory (KB)",
        "Target Server Memory (KB)",
        "Total Server Memory (KB)",
        "Memory grant queue waits",
        "Thread-safe memory objects waits",
        /* Compilation counters */
        "SQL Compilations/sec",
        "SQL Re-Compilations/sec",
        "Query optimizations/sec",
        "Reduced memory grants/sec",
        /* Batch and request counters */
        "Batch Requests/sec",
        "Requests completed/sec",
        "Active requests",
        "Queued requests",
        "Blocked tasks",
        "Active parallel threads",
        /* Log counters */
        "Log Flushes/sec",
        "Log Bytes Flushed/sec",
        "Log Flush Write Time (ms)",
        "Log buffer waits",
        "Log write waits",
        /* TempDB counters */
        "Version Store Size (KB)",
        "Free Space in tempdb (KB)",
        "Active Temp Tables",
        "Version Generation rate (KB/s)",
        "Version Cleanup rate (KB/s)",
        "Temp Tables Creation Rate",
        "Workfiles Created/sec",
        "Worktables Created/sec",
        /* Wait counters */
        "Network IO waits",
        "Wait for the worker",
        /* Availability Group counters (SQLServer:Database Replica object, #991). Both names are unique
           to that object, so the collector's counter_name-only filter picks them up without an
           object_name predicate. Transaction Delay / Mirrored Write Transactions/sec is the average
           primary-side commit delay per mirrored transaction — the primary-side half of the AG latency
           picture the ag_database_replica_states gauges cover from the secondary side. Absent on an
           instance without AGs, which is simply fewer rows. */
        "Transaction Delay",
        "Mirrored Write Transactions/sec",
    };

    public override string Name => "perfmon_stats";

    public override string TargetTable => "perfmon_stats";

    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        var counters = context.PerfmonCounterOverride is { Count: > 0 }
            ? context.PerfmonCounterOverride
            : DefaultCounters;
        var counterList = string.Join(",\n    ", counters.Select(c => $"N'{c.Replace("'", "''", StringComparison.Ordinal)}'"));

        var query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    object_name = RTRIM(pc.object_name),
    counter_name = RTRIM(pc.counter_name),
    instance_name = RTRIM(pc.instance_name),
    cntr_value = pc.cntr_value,
    cntr_type = pc.cntr_type
FROM sys.dm_os_performance_counters AS pc
WHERE pc.counter_name IN (
    {counterList}
)
OPTION(RECOMPILE);";

        return new CollectorQuery(query);
    }

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("object_name", CollectorColumnType.Varchar),
        new CollectorColumn("counter_name", CollectorColumnType.Varchar),
        new CollectorColumn("instance_name", CollectorColumnType.Varchar),
        new CollectorColumn("cntr_value", CollectorColumnType.BigInt),
        new CollectorColumn("delta_cntr_value", CollectorColumnType.BigInt),
        new CollectorColumn("sample_interval_seconds", CollectorColumnType.Integer),
        /* Appended LAST (V132 / v62): both stores' writers are positional and an upgraded store receives
           the column by ALTER TABLE ADD COLUMN, which can only land at the end. NULL on every row written
           before the rung. */
        new CollectorColumn("cntr_type", CollectorColumnType.Integer),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                reader.GetInt64(3),
                reader.GetInt32(4)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* A GAUGE is written as its level and never differenced (#3653 A7). The (delta, interval) pair is
           NULL, not (0, 0): 0 is the calculator's "no delta was knowable" marker, a claim about a count,
           and a gauge has no delta to know. NULL/NULL beside a stored gauge type says "this row's number is
           cntr_value" and nothing else. The calculator is not called, so its per-key cache never holds a
           gauge key from this pass — the seeders still restore every perfmon row's cntr_value into it on a
           restart (they read no type), which is harmless: an unused baseline under a key no delta call will
           ever present. The pass window rolls on the rate rows of the same sweep, and perfmon passes no
           series age, so nothing that reads the window can miss the skipped keys. */
        long? deltaCntrValue = null;
        int? sampleIntervalSeconds = null;

        if (!IsGauge(row.CntrType))
        {
            /* Delta for counters. Group/key/gap are the parity contract. Rate types (PERF_COUNTER_BULK_COUNT
               and its 32-bit sibling) are the reason this branch exists; the average/fraction/base family
               takes the same write — its delta is a real per-interval change of the raw value, and the
               stored type is what tells a reader not to divide it into a rate the name never promised.

               The interval is MEASURED, not assumed. This wrote a literal 60 — the configured one-minute
               cadence — on every row, while the fleet's actual gap between perfmon sweeps runs a median of
               299 s (p99 830 s, max 2,514 s over 99,717 samples), so anyone deriving a rate from it was up
               to 5x high on a denominator the collector had invented. Nothing in-product divided by THIS
               column at the time (the perfmon MCP tools handed it to the caller and the Viewer carried it
               unplotted; since #3702 both viewers plot a rate counter's delta THROUGH it); the
               NULLIF(sample_interval_seconds, 0) idiom guards query_stats' interval, which
               QueryStatsCollector has always measured. Perfmon was the outlier (#2234). A 0 here means no
               delta was knowable — first sighting, counter reset, or a gap past the policy — so callers must
               treat 0 as unknown rather than dividing by it. */
            var deltaKey = $"{row.ObjectName}|{row.CounterName}|{row.InstanceName}";
            deltaCntrValue = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "perfmon", deltaKey,
                row.CntrValue, out var measuredInterval,
                collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
            sampleIntervalSeconds = measuredInterval;
        }

        writer
            .Value(row.ObjectName)          /* object_name VARCHAR */
            .Value(row.CounterName)         /* counter_name VARCHAR */
            .Value(row.InstanceName)        /* instance_name VARCHAR */
            .Value(row.CntrValue)           /* cntr_value BIGINT — the reading itself for a gauge */
            .Value(deltaCntrValue)          /* delta_cntr_value BIGINT — NULL for a gauge */
            .Value(sampleIntervalSeconds)   /* sample_interval_seconds — measured, not the cadence; NULL for a gauge */
            .Value(row.CntrType);           /* cntr_type INTEGER — the DMV's type id, the reader's classifier */
    }
}
