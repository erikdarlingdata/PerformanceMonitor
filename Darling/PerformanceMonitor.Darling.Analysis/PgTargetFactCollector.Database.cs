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
    /// The ONE <c>pg_database_stats</c> read, emitting <c>PG_TPS</c>, <c>PG_HIT_RATIO</c>, <c>PG_DEADLOCK_RATE</c>
    /// and <c>PG_TEMP_SPILL</c>. Reset-aware differencing verbatim from <c>DarlingPgDatabaseReader.PgDatabaseSql</c>
    /// (<c>stats_reset IS DISTINCT FROM LAG(stats_reset)</c>, <c>GREATEST(raw, 0)</c>) — a <c>pg_stat_reset()</c>
    /// mid-window otherwise reads as a negative delta clamped to zero. The deadlock RATE comes from this counter,
    /// never from a <c>pg_deadlocks</c> row count (the log tail is lossy on exactly the busiest servers).
    /// <para>/* filled by lane 2 — returns immediately until then. The SQL it will run is declared as a
    /// <c>public const string …Sql</c> in THIS file so <see cref="AllSql"/> picks it up by reflection; every
    /// command sets <c>CommandTimeout = FactCommandTimeoutSeconds</c>, every store call passes
    /// <c>context.CancellationToken</c>, the catch is the <c>when (!AnalysisShutdown.IsExpectedAbandon(ex,
    /// context.CancellationToken))</c> shape that calls <see cref="ReportCollectionFailure"/>, and every
    /// rate divides by <see cref="AnalysisContext.ObservedDurationMs"/>, never the nominal window. */</para>
    /// </summary>
    private partial Task CollectDatabaseFactsAsync(AnalysisContext context, List<Fact> facts) => Task.CompletedTask;
}
