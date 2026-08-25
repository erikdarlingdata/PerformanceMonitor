/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Operating-system CPU and disk per query — <c>pg_stat_kcache</c> (#2603).
///
/// <para><b>What it adds over <c>pg_stat_statements</c>.</b> That view reports elapsed time, which answers
/// "this query took a while" and nothing else. This reports what the KERNEL charged the backend: user CPU,
/// system CPU, and bytes that actually reached the disk. A query with high total time and near-zero CPU was
/// waiting; one with high CPU and no I/O is burning processor. Same statement identity — <c>queryid</c>
/// joins <c>pg_statement_stats</c> — so the two halves compose.</para>
///
/// <para><b><c>read_bytes = 0</c> does NOT mean the query read nothing.</b> This is the reading most likely
/// to be got wrong, and it was zero across every row on the rig while writes were not. The counters come
/// from <c>getrusage</c>, so they measure I/O that reached the DEVICE — a read served from the OS page
/// cache is genuinely zero here and is the healthy case. Zero reads with high CPU is a cached workload
/// working properly; large read bytes is a workload missing cache. Neither is comparable to a logical-read
/// figure, and this collector does not pretend otherwise.</para>
///
/// <para><b>Only top-level statements are collected.</b> The function reports a <c>top</c> flag, and with
/// <c>pg_stat_statements.track = 'all'</c> a nested statement appears BOTH on its own row and inside its
/// caller's. Summing the two double-counts every function body on the server. The measured rig ran
/// <c>track = 'top'</c>, where the filter is inert — which is exactly why it has to be written down rather
/// than discovered later on a customer who sets <c>track = 'all'</c>.</para>
///
/// <para><b>Times are stored in milliseconds and the columns say so.</b> The function reports seconds as a
/// double; the product's convention is milliseconds everywhere, so the conversion happens once here rather
/// than in each reader that might forget.</para>
///
/// <para><b>Cumulative, so the read deltas it</b> — same shape as <c>pg_wait_sampling</c> and
/// <c>pg_statement_stats</c>. <c>stats_since</c> travels with each row so a reset is visible as a fact
/// rather than inferred from a counter that went backwards.</para>
///
/// <para><b>Cluster-wide, but legitimately per-database.</b> Unlike <c>pg_wait_sampling</c>, the function
/// exposes <c>dbid</c>, and <c>pg_database</c> is a SHARED catalog — so the name resolves correctly from
/// whichever database the collector connected to. That is why this one carries <c>database_name</c> and
/// that one deliberately does not (#2599 is about not claiming attribution the catalog cannot support; here
/// it can).</para>
/// </summary>
public sealed class PgKernelStatsCollector : PostgresCollectorDefinitionBase<PgKernelStatsCollector.Row>
{
    public static readonly PgKernelStatsCollector Instance = new();

    private PgKernelStatsCollector()
    {
    }

    /// <param name="QueryId">Joins <c>pg_statement_stats.queryid</c>. Zero is a statement
    /// <c>pg_stat_statements</c> did not track — a utility command — kept rather than dropped so the totals
    /// still reconcile with the server's own.</param>
    /// <param name="ExecReadBytes">Bytes that reached the DEVICE. Zero means the page cache served the
    /// read, not that nothing was read. See the type header.</param>
    /// <param name="StatsSince">When these counters were last reset, per the extension itself — so a reset
    /// is a recorded fact rather than something a reader infers from a counter going backwards.</param>
    public readonly record struct Row(
        string? DatabaseName,
        long QueryId,
        double ExecUserTimeMs,
        double ExecSystemTimeMs,
        double PlanCpuTimeMs,
        long ExecReadBytes,
        long ExecWriteBytes,
        long MinorFaults,
        long MajorFaults,
        DateTime? StatsSince);

    /* public.pg_stat_kcache(), not pg_catalog: it is an extension function and lives in the schema the
       extension was created in - the same correction pgstatindex needed in #2561, where the pg_catalog
       qualification does not resolve at all.

       ORDER BY CPU, not by bytes. CPU is the figure this collector exists to add, and ranking by I/O would
       put a cache-missing report at the top of a list whose question is "what is burning processor". */
    private const string QueryText = @"
SELECT
    d.datname::text                                              AS database_name,
    k.queryid::bigint                                            AS query_id,
    (sum(k.exec_user_time) * 1000.0)::double precision           AS exec_user_time_ms,
    (sum(k.exec_system_time) * 1000.0)::double precision         AS exec_system_time_ms,
    (sum(k.plan_user_time + k.plan_system_time) * 1000.0)::double precision AS plan_cpu_time_ms,
    sum(k.exec_reads)::bigint                                    AS exec_read_bytes,
    sum(k.exec_writes)::bigint                                   AS exec_write_bytes,
    sum(k.exec_minflts)::bigint                                  AS minor_faults,
    sum(k.exec_majflts)::bigint                                  AS major_faults,
    min(k.stats_since)                                           AS stats_since
FROM public.pg_stat_kcache() AS k
JOIN pg_catalog.pg_database AS d
  ON d.oid = k.dbid
WHERE k.top
GROUP BY d.datname, k.queryid
ORDER BY sum(k.exec_user_time + k.exec_system_time) DESC, k.queryid
LIMIT 500";

    public override string Name => "pg_kernel_stats";

    public override string TargetTable => "pg_kernel_stats";

    /// <summary>
    /// Any PostgreSQL target. An absent module raises <c>42883</c> (undefined function), which the host
    /// classifies as <c>ObjectMissing</c> and records as a named non-fatal skip — the same degradation
    /// <c>pg_index_bloat</c> takes without pgstattuple.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    /// <summary>
    /// Cluster-wide. The function reports every database's statements from one connection, and
    /// <c>pg_database</c> resolves the names, so running per database would collect the same rows once per
    /// database.
    /// </summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("query_id", CollectorColumnType.BigInt),
        new CollectorColumn("exec_user_time_ms", CollectorColumnType.Double),
        new CollectorColumn("exec_system_time_ms", CollectorColumnType.Double),
        new CollectorColumn("plan_cpu_time_ms", CollectorColumnType.Double),
        /* Named _bytes because that is what they are, and because a column called exec_reads next to a
           SQL Server codebase would be read as a logical-read count, which it emphatically is not. */
        new CollectorColumn("exec_read_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("exec_write_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("minor_faults", CollectorColumnType.BigInt),
        new CollectorColumn("major_faults", CollectorColumnType.BigInt),
        new CollectorColumn("stats_since", CollectorColumnType.Timestamp),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                DatabaseName: reader.IsDBNull(0) ? null : reader.GetString(0),
                QueryId: reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                ExecUserTimeMs: reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                ExecSystemTimeMs: reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                PlanCpuTimeMs: reader.IsDBNull(4) ? 0 : reader.GetDouble(4),
                ExecReadBytes: reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                ExecWriteBytes: reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                MinorFaults: reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                MajorFaults: reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                /* Read as UTC and stored naive, the convention every PostgreSQL timestamp takes here:
                   stats_since is timestamptz, and letting it land as Local would shift every reset stamp
                   by the store host's offset. */
                StatsSince: reader.IsDBNull(9)
                    ? null
                    : DateTime.SpecifyKind(reader.GetDateTime(9).ToUniversalTime(), DateTimeKind.Unspecified)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No deltas. Cumulative counters are stored as the server reports them and differenced by the read,
           which can recognise a reset - stats_since says when one happened - where a delta computed here
           would publish a large negative CPU figure the moment somebody reset the extension. */
        writer
            .Value(row.DatabaseName)
            .Value(row.QueryId)
            .Value(row.ExecUserTimeMs)
            .Value(row.ExecSystemTimeMs)
            .Value(row.PlanCpuTimeMs)
            .Value(row.ExecReadBytes)
            .Value(row.ExecWriteBytes)
            .Value(row.MinorFaults)
            .Value(row.MajorFaults)
            .Value(row.StatsSince);
    }
}
