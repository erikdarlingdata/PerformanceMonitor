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
    /// <c>PG_AUTOVACUUM_BACKLOG</c> (per table, dead/threshold ratio and slope over ≥ 3 consecutive hourly
    /// samples), <c>PG_WRAPAROUND_TREND</c> (XID and MultiXact graded separately, never collapsed) and
    /// <c>PG_XMIN_HOLD</c> (per-source attribution). Patterns: <c>DarlingPgAutovacuumReader</c>,
    /// <c>DarlingPgWraparoundReader</c>, <c>DarlingPgXminReader</c>.
    /// <para>/* filled by lane 4 — returns immediately until then. The SQL it will run is declared as a
    /// <c>public const string …Sql</c> in THIS file so <see cref="AllSql"/> picks it up by reflection; every
    /// command sets <c>CommandTimeout = FactCommandTimeoutSeconds</c>, every store call passes
    /// <c>context.CancellationToken</c>, the catch is the <c>when (!AnalysisShutdown.IsExpectedAbandon(ex,
    /// context.CancellationToken))</c> shape that calls <see cref="ReportCollectionFailure"/>, and every
    /// rate divides by <see cref="AnalysisContext.ObservedDurationMs"/>, never the nominal window. */</para>
    /// </summary>
    private partial Task CollectVacuumFactsAsync(AnalysisContext context, List<Fact> facts) => Task.CompletedTask;
}
