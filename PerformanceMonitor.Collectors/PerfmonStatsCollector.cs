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
/// </summary>
public sealed class PerfmonStatsCollector : CollectorDefinitionBase<PerfmonStatsCollector.Row>
{
    public static PerfmonStatsCollector Instance { get; } = new();

    private PerfmonStatsCollector()
    {
    }

    public readonly record struct Row(string ObjectName, string CounterName, string InstanceName, long CntrValue);

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
    cntr_value = pc.cntr_value
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
                reader.GetInt64(3)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* Delta for per-second counters. Group/key/gap are the parity contract.

           The interval is MEASURED, not assumed. This wrote a literal 60 — the configured one-minute
           cadence — on every row, while the fleet's actual gap between perfmon sweeps runs a median of
           299 s (p99 830 s, max 2,514 s over 99,717 samples), so anyone deriving a rate from it was up
           to 5x high on a denominator the collector had invented. Nothing in-product divided by THIS
           column (the perfmon MCP tools hand it to the caller and the Viewer carries it unplotted); the
           NULLIF(sample_interval_seconds, 0) idiom guards query_stats' interval, which
           QueryStatsCollector has always measured. Perfmon was the outlier (#2234). A 0 here means no
           delta was knowable — first sighting, counter reset, or a gap past the policy — so callers must
           treat 0 as unknown rather than dividing by it. */
        var deltaKey = $"{row.ObjectName}|{row.CounterName}|{row.InstanceName}";
        var deltaCntrValue = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "perfmon", deltaKey,
            row.CntrValue, out var sampleIntervalSeconds,
            collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);

        writer
            .Value(row.ObjectName)          /* object_name VARCHAR */
            .Value(row.CounterName)         /* counter_name VARCHAR */
            .Value(row.InstanceName)        /* instance_name VARCHAR */
            .Value(row.CntrValue)           /* cntr_value BIGINT */
            .Value(deltaCntrValue)          /* delta_cntr_value BIGINT */
            .Value(sampleIntervalSeconds);  /* sample_interval_seconds — measured, not the cadence */
    }
}
