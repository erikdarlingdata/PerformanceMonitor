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
    /// <c>PG_CPU_BURN_CORES</c> from the window's <c>pg_kernel_stats</c> — the reset-aware differences of
    /// <c>exec_user_time_ms + exec_system_time_ms</c> (<c>stats_since</c> is the reset witness) summed across statements
    /// per collection, divided by the collection's wall ms: cores busy — and <c>PG_CPU_DECOMPOSITION</c>, that CPU time
    /// against the wait facts already in the list. Availability of <c>pg_stat_kcache</c> is read from
    /// <c>pg_extension_availability</c> and its absence is a SILENT no-fact, never a finding (the extension's advisory
    /// is the config family's); a present extension with no rows in the window is a coverage statement, not zero CPU.
    /// <para>/* filled by lane 28 — returns immediately until then.
    /// The SQL it will run is declared as a
    /// <c>public const string …Sql</c> in THIS file so <see cref="AllSql"/> picks it up by reflection; every
    /// command sets <c>CommandTimeout = FactCommandTimeoutSeconds</c>, every store call passes
    /// <c>context.CancellationToken</c>, <c>$N</c> positional, no bare <c>now()</c> / <c>CURRENT_TIMESTAMP</c>
    /// (StoreSqlClockDisciplineTests), the catch is the <c>when (!AnalysisShutdown.IsExpectedAbandon(ex,
    /// context.CancellationToken))</c> shape that calls <see cref="ReportCollectionFailure"/>, and every rate divides
    /// by <see cref="AnalysisContext.ObservedDurationMs"/>, never the nominal window.
    /// pg_kernel_stats and pg_extension_availability are CollectorCatalog targets already, so the FROM/JOIN census
    /// admits them (the first pinned by name in PgTargetFactCollectorTests; the second is a v1 read). */</para>
    /// </summary>
    private partial Task CollectKernelFactsAsync(AnalysisContext context, List<Fact> facts) => Task.CompletedTask;
}
