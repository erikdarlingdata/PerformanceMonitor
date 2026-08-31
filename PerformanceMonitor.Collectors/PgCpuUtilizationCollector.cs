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
/// live against the same instance and window: CloudWatch's figure reads capacity-relative and runs roomy on
/// a Serverless v2 target (~6.8%), while PI's <c>os.cpuUtilization.total.avg</c> reads true OS-level
/// utilization on the identical window (~16.8%). PI is the honest signal for this fleet, and it is
/// universally available — every monitored Aurora Postgres instance already has it enabled.</para>
/// </summary>
public sealed class PgCpuUtilizationCollector : PostgresCollectorDefinitionBase<PgCpuUtilizationCollector.Row>
{
    public static PgCpuUtilizationCollector Instance { get; } = new();

    private PgCpuUtilizationCollector()
    {
    }

    /// <param name="SampleTime">The Performance Insights data point's own timestamp — distinct from the
    /// prefix <c>collection_time</c> (when the ingestor's cycle ran), exactly like SQL Server's
    /// <see cref="CpuUtilizationCollector.Row.SampleTime"/>. PI returns one point per minute regardless of
    /// how often the ingestor asks, so requesting a short lookback window and dedupping client-side against
    /// this column survives a missed cycle the same way the ring-buffer route does.</param>
    /// <param name="CpuPercent"><c>os.cpuUtilization.total.avg</c>, 0-100. Nullable because PI can return a
    /// data point with a null value for a period it has no sample for.</param>
    public readonly record struct Row(System.DateTime SampleTime, double? CpuPercent);

    public override string Name => "pg_cpu_utilization";

    public override string TargetTable => "pg_cpu_utilization";

    public override string? WatermarkColumn => "sample_time";

    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("sample_time", CollectorColumnType.Timestamp),
        new CollectorColumn("cpu_percent", CollectorColumnType.Double),
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
            .Value(row.CpuPercent);
    }
}
