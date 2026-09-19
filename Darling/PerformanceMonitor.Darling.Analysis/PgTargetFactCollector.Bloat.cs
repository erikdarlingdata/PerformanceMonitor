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
    /// <c>PG_BLOAT_TREND</c> over the hourly <c>pg_table_bloat_stats</c> samples and <c>PG_INDEX_BLOAT_TREND</c> over the daily <c>pg_index_bloat</c> samples. The pattern to copy is <c>PgTargetFactCollector.Vacuum.cs</c> — per-object composition; a trend over a sparse series states its own sample count in metadata.
    /// <para>/* filled by lane 13 — returns immediately until then. The SQL it will run is declared as a
    /// <c>public const string …Sql</c> in THIS file so <see cref="AllSql"/> picks it up by reflection; every
    /// command sets <c>CommandTimeout = FactCommandTimeoutSeconds</c>, every store call passes
    /// <c>context.CancellationToken</c>, the catch is the <c>when (!AnalysisShutdown.IsExpectedAbandon(ex,
    /// context.CancellationToken))</c> shape that calls <see cref="ReportCollectionFailure"/>, and every
    /// rate divides by <see cref="AnalysisContext.ObservedDurationMs"/>, never the nominal window. */</para>
    /// </summary>
    private partial Task CollectBloatFactsAsync(AnalysisContext context, List<Fact> facts) => Task.CompletedTask;
}
