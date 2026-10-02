/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Instance-level CPU utilization for a managed PostgreSQL/Aurora target (#2719).
///
/// <para><b>There is no SQL for this collector, ever.</b> Unlike every other definition in this catalog,
/// PostgreSQL has no DMV or extension that reports instance-level CPU-percent-of-capacity —
/// <c>pg_stat_kcache</c> measures per-QUERY kernel time, not "is this instance CPU-saturated right now", and
/// core PostgreSQL exposes nothing that answers that question at all. The only source that does is AWS RDS
/// Performance Insights' <c>os.cpuUtilization.total.avg</c> OS counter, reached over the RDS/PI API rather
/// than a database connection — the same "reach the target a different way" shape #2538's log capture
/// already established for Aurora/RDS. <see cref="BuildQuery"/> and <see cref="ReadAsync"/> are therefore
/// unreachable: the worker dispatches this collector's name straight to
/// <c>DarlingCollectorRunner.IngestPgCpuAsync</c> (mirroring <c>pg_deadlocks</c>/<c>pg_plan_capture</c>'s RDS
/// branch), which never calls <see cref="ICollectorDefinition{TRow}.BuildQuery"/> — they exist only to
/// satisfy the interface contract and to keep <see cref="PgSchemaGenerator"/> able to generate this table's
/// DDL from the same column metadata every other collector uses.</para>
///
/// <para><b>Why Performance Insights over CloudWatch's <c>AWS/RDS</c>/<c>CPUUtilization</c>.</b> Measured
/// live against the same instance and window the two disagree: CloudWatch read ~6.8% where PI's
/// <c>os.cpuUtilization.total.avg</c> read ~16.8%. PI is the one kept, because it is the counter the
/// instance's own OS reports rather than a figure CloudWatch derives, and because it is universally
/// available — every monitored Aurora Postgres instance already has it enabled.</para>
///
/// <para><b>That reading is percent of the capacity CURRENTLY ALLOCATED, not of a fixed ceiling</b>
/// (#3281), so it is not "true OS-level utilization" in contrast to CloudWatch's capacity-relative
/// figure: both denominators move on a serverless instance class, just differently. On Aurora Serverless
/// v2 the allocation is re-sized
/// continuously, so a one-vCPU instance reads exactly <c>100.0</c> with <c>idle</c> exactly <c>0.0</c>
/// whenever one core stays busy for a whole minute, which is the routine trigger for scaling UP rather than
/// a saturation incident. Measured at one such minute: 4 of 12 configured ACUs in use — 33% of the
/// configured ceiling, two thirds of it unused.</para>
///
/// <para><b>So both are collected, and they answer different questions.</b> <c>cpu_percent</c> stays
/// because "was a core pinned, and by what" is a real question it answers.
/// <c>acu_utilization_percent</c> — Performance Insights'
/// <c>os.general.acuUtilization.avg</c> — is the one a saturation BAND may read, because 100% of it means
/// the CONFIGURED ceiling is reached, which is an incident. See
/// <c>ServerHealthClassifier.CpuSeverity</c> for the banding rule and
/// <c>RdsCpuIngestor</c> for why all four metrics arrive on one API call.</para>
///
/// <para><b>Since V136 the row also carries the host's MEMORY (#3691, design §4b)</b> — six columns after
/// the capacity four, from the same Performance Insights response: <c>os.memory.total / free / cached /
/// buffers / active</c> as BYTES (PI publishes them in kilobytes; stored ×1024 so every byte column in the
/// store means one thing), and <c>configured_memory_bytes</c>, the Serverless v2 capacity of that minute ×
/// <see cref="BytesPerAcu"/> — what the instance is entitled to right now, as distinct from the host's RAM,
/// which does not move with the capacity; NULL on a provisioned instance class. Memory is a column family on
/// THIS row rather than a table of its own for two reasons. It IS this row: one <c>GetResourceMetrics</c>
/// response, one minute stamp, one watermark, one COPY — a sibling table would have re-stored the same
/// identity with its own resume point and its own hypertable for the privilege of a second name. And the
/// second name had a cost the plan did not see: every collector table is a hypertable, the compression grid
/// spreads them at <c>CompressionPhaseMaxPerMinute</c> per minute, and a 73rd hypertable widened the band
/// past what the hour can pay — the heaviest refresh's watch line would have dropped below its recorded
/// ceiling (896 s), which <c>TimescaleSupport</c> documents as a scheduling decision, not a renumbering. Six
/// nullable columns cost none of that (V130 made the same choice for the log-event numbers).</para>
///
/// <para><b>A stock PostgreSQL target has no row here at all, and that absence is the memory series'
/// honesty arm.</b> There is no host-memory figure inside the engine — <c>pg_settings</c> says what was
/// configured, <c>pg_stat_*</c> what was used of it, neither what the host HAS — so a consumer that finds no
/// row must read <c>unavailable</c>, never "no memory"; and a row with the memory columns NULL is a minute PI
/// had no memory sample for (or a target whose Performance Insights does not publish <c>os.memory.*</c>, in
/// which case the ingestor's fallback asked for the pre-V136 four and these stay NULL forever, truthfully).
/// Nothing reads the six yet; the composition checks are #3691's later slices.</para>
/// </summary>
public sealed class PgCpuUtilizationCollector : PostgresCollectorDefinitionBase<PgCpuUtilizationCollector.Row>
{
    public static PgCpuUtilizationCollector Instance { get; } = new();

    private PgCpuUtilizationCollector()
    {
    }

    /// <summary>
    /// Bytes of memory per Aurora capacity unit: 2 GiB. AWS's definition of the unit — "each ACU is a
    /// combination of approximately 2 gibibytes (GiB) of memory, corresponding CPU, and networking" (the
    /// Aurora Serverless v2 capacity documentation) — so this is vendor-defined, not a measurement, and it is
    /// the factor behind <c>configured_memory_bytes</c>. Public so the ingestor that derives the column and
    /// the test that pins the arithmetic share one spelling.
    /// </summary>
    public const long BytesPerAcu = 2L * 1024 * 1024 * 1024;

    /// <param name="SampleTime">The Performance Insights data point's own timestamp — distinct from the
    /// prefix <c>collection_time</c> (when the ingestor's cycle ran), exactly like SQL Server's
    /// <see cref="CpuUtilizationCollector.Row.SampleTime"/>. PI returns one point per minute regardless of
    /// how often the ingestor asks, so requesting a short lookback window and dedupping client-side against
    /// this column survives a missed cycle the same way the ring-buffer route does.</param>
    /// <param name="CpuPercent"><c>os.cpuUtilization.total.avg</c>, 0-100 — percent of the capacity
    /// CURRENTLY ALLOCATED to the instance, which on Aurora Serverless v2 moves (see the class doc
    /// comment). Nullable because PI can return a data point with a null value for a period it has no
    /// sample for.</param>
    /// <param name="AcuUtilizationPercent"><c>os.general.acuUtilization.avg</c>, 0-100 — percent of the
    /// CONFIGURED ACU ceiling in use (#3281), the figure a saturation band reads. Nullable for
    /// <paramref name="CpuPercent"/>'s reason, and a null here bands Unknown rather than Healthy: it means
    /// the headroom was never measured for this minute, not that there was headroom.</param>
    /// <param name="ServerlessCapacityAcu"><c>os.general.serverlessDatabaseCapacity.avg</c> — the ACU
    /// actually allocated at this minute, the numerator behind
    /// <paramref name="AcuUtilizationPercent"/>. Stored so a reader can state "4 of 12" rather than only a
    /// percentage.</param>
    /// <param name="MaxConfiguredAcu"><c>os.general.maxConfiguredAcu.avg</c> — the cluster's configured ACU
    /// ceiling, the denominator. Recorded per sample rather than looked up when read, because it is a
    /// setting someone can change and a ratio taken against today's ceiling would misdescribe last week's
    /// samples.</param>
    /// <param name="MemoryTotalBytes"><c>os.memory.total.avg</c> × 1024 — the host's RAM (V136).</param>
    /// <param name="MemoryFreeBytes"><c>os.memory.free.avg</c> × 1024 — unassigned memory (V136).</param>
    /// <param name="MemoryCachedBytes"><c>os.memory.cached.avg</c> × 1024 — the file-system cache, PostgreSQL's
    /// second buffer tier and the figure <c>effective_cache_size</c> is supposed to describe (V136).</param>
    /// <param name="MemoryBuffersBytes"><c>os.memory.buffers.avg</c> × 1024 — I/O buffering ahead of the
    /// storage device (V136).</param>
    /// <param name="MemoryActiveBytes"><c>os.memory.active.avg</c> × 1024 — assigned memory (V136).</param>
    /// <param name="ConfiguredMemoryBytes">Aurora Serverless v2 only: <paramref name="ServerlessCapacityAcu"/> ×
    /// <see cref="BytesPerAcu"/>, the memory the instance is entitled to at this minute. NULL on a provisioned
    /// instance class and wherever the capacity metric had no sample (V136).</param>
    public readonly record struct Row(
        System.DateTime SampleTime,
        double? CpuPercent,
        double? AcuUtilizationPercent,
        double? ServerlessCapacityAcu,
        double? MaxConfiguredAcu,
        long? MemoryTotalBytes = null,
        long? MemoryFreeBytes = null,
        long? MemoryCachedBytes = null,
        long? MemoryBuffersBytes = null,
        long? MemoryActiveBytes = null,
        long? ConfiguredMemoryBytes = null);

    public override string Name => "pg_cpu_utilization";

    public override string TargetTable => "pg_cpu_utilization";

    public override string? WatermarkColumn => "sample_time";

    /// <summary>Aurora only, matching <c>PgWaitStatsCollector</c>'s own gate exactly — Performance Insights
    /// reaches every monitored Aurora target today. A plain (non-Aurora) RDS Postgres instance would still
    /// be tried at runtime by <c>RdsCpuIngestor</c> (it dispatches on <c>RdsEndpoint.TryParse</c> succeeding,
    /// not on this flag), but nothing in the fleet is that shape, and gating on <c>IsAurora</c> alone is what
    /// the engine-capability sweep (<c>CollectorEngineCapability.TargetsWithEngineKind</c>) already varies
    /// correctly for PostgreSQL — unlike <c>CollectorTargetInfo.IsAwsRds</c>, which is documented as a
    /// SQL-Server-only fact and is never varied for a Postgres target.</summary>
    public override bool AppliesTo(CollectorTargetInfo target) => target.IsAurora;

    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("sample_time", CollectorColumnType.Timestamp),
        new CollectorColumn("cpu_percent", CollectorColumnType.Double),
        new CollectorColumn("acu_utilization_percent", CollectorColumnType.Double),
        new CollectorColumn("serverless_capacity_acu", CollectorColumnType.Double),
        new CollectorColumn("max_configured_acu", CollectorColumnType.Double),
        /* V136 (#3691): the host's memory, BYTES, appended LAST so every pre-V136 row and every positional
           reader keeps its ordinals. bigint rather than double: PI's kilobyte averages are rounded to whole
           bytes by the ingestor, and a byte count is an integer. */
        new CollectorColumn("memory_total_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("memory_free_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("memory_cached_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("memory_buffers_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("memory_active_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("configured_memory_bytes", CollectorColumnType.BigInt),
    };

    public override CollectorQuery BuildQuery(CollectorContext context) =>
        throw new System.NotSupportedException(
            $"{Name} has no SQL route — it is always dispatched through the RDS/Performance Insights "
            + "ingestor (DarlingCollectorRunner.IngestPgCpuAsync). This method exists only to satisfy "
            + "ICollectorDefinition<TRow> and PgSchemaGenerator's DDL generation.");

    public override ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken) =>
        throw new System.NotSupportedException(
            $"{Name} has no SQL route — see {nameof(BuildQuery)}.");

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            /* Naive UTC, per the store contract: PI returns Kind=Utc DateTime and Npgsql refuses one
               against a `timestamp` column. */
            .Value(System.DateTime.SpecifyKind(row.SampleTime, System.DateTimeKind.Unspecified))
            .Value(row.CpuPercent)
            /* Each of the three independently nullable, written in PayloadColumns order. A metric PI had no
               sample for this minute stays NULL rather than becoming 0 — a 0 here would read as "no
               capacity in use" / "no ceiling configured", which are measurements nobody took. */
            .Value(row.AcuUtilizationPercent)
            .Value(row.ServerlessCapacityAcu)
            .Value(row.MaxConfiguredAcu)
            /* V136: the six memory columns, each independently nullable for the same reason — a 0 here would
               read as "no memory free" / "no memory at all", which are measurements nobody took. */
            .Value(row.MemoryTotalBytes)
            .Value(row.MemoryFreeBytes)
            .Value(row.MemoryCachedBytes)
            .Value(row.MemoryBuffersBytes)
            .Value(row.MemoryActiveBytes)
            .Value(row.ConfiguredMemoryBytes);
    }
}
