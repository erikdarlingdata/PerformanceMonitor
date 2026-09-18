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
    /// <c>PG_BAD_ACTOR_&lt;queryid&gt;</c> from <c>pg_statement_stats</c> collect-time deltas joined to
    /// <c>pg_statement_text</c>, keyed through <see cref="PgTargetFactKeys.BadActorKey"/>. Lane 6 adds the temp
    /// (<c>temp_blks_written</c>) offenders to the same read. Pattern: <c>DarlingPgStatementReader.PgTopQueriesSql</c>.
    /// <para>/* filled by lane 7 — returns immediately until then. The SQL it will run is declared as a
    /// <c>public const string …Sql</c> in THIS file so <see cref="AllSql"/> picks it up by reflection; every
    /// command sets <c>CommandTimeout = FactCommandTimeoutSeconds</c>, every store call passes
    /// <c>context.CancellationToken</c>, the catch is the <c>when (!AnalysisShutdown.IsExpectedAbandon(ex,
    /// context.CancellationToken))</c> shape that calls <see cref="ReportCollectionFailure"/>, and every
    /// rate divides by <see cref="AnalysisContext.ObservedDurationMs"/>, never the nominal window. */</para>
    /// </summary>
    private partial Task CollectQueryFactsAsync(AnalysisContext context, List<Fact> facts) => Task.CompletedTask;
}
