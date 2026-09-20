/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetFactCollector
{
    /// <summary>
    /// <c>CONFIG_PG_MEMORY_OVERCOMMIT</c> from the config family's knob facts already in the list (the §4b arithmetic
    /// — <c>shared_buffers + max_connections × work_mem</c> plus <c>maintenance_work_mem × autovacuum_max_workers</c> —
    /// reads <c>CONFIG_PG_*</c> facts by key where they exist and <c>pg_server_config</c> for the knobs no v1 fact
    /// carries) against the host's <c>memory_total_bytes</c>, which rides <c>pg_cpu_utilization</c>'s row since V136
    /// (lane R5: six memory columns on the existing hypertable — there is NO <c>pg_host_memory</c> table; a row older
    /// than V136 has them NULL); and <c>PG_HOST_MEMORY_PRESSURE</c>, the window's <c>memory_free_bytes +
    /// memory_cached_bytes</c> over <c>memory_total_bytes</c> from the same rows, Aurora only. No row with a non-NULL
    /// total in the window means NO composition fact (the arithmetic has no denominator), stated as
    /// coverage rather than guessed. The knob values the advice states travel on the fact's metadata as doubles.
    /// <para>/* filled by lane 32 — returns immediately until then.
    /// The SQL it will run is declared as a
    /// <c>public const string …Sql</c> in THIS file so <see cref="AllSql"/> picks it up by reflection; every
    /// command sets <c>CommandTimeout = FactCommandTimeoutSeconds</c>, every store call passes
    /// <c>context.CancellationToken</c>, <c>$N</c> positional, no bare <c>now()</c> / <c>CURRENT_TIMESTAMP</c>
    /// (StoreSqlClockDisciplineTests), the catch is the <c>when (!AnalysisShutdown.IsExpectedAbandon(ex,
    /// context.CancellationToken))</c> shape that calls <see cref="ReportCollectionFailure"/>, and every rate divides
    /// by <see cref="AnalysisContext.ObservedDurationMs"/>, never the nominal window.
    /// pg_server_config and pg_cpu_utilization are CollectorCatalog targets already (the second is lane 9's v1 read),
    /// so the FROM/JOIN census admits both with no edit. */</para>
    /// </summary>
    private partial Task CollectMemoryFactsAsync(AnalysisContext context, List<Fact> facts) => Task.CompletedTask;
}
