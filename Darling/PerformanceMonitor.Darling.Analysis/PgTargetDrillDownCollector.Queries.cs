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

public sealed partial class PgTargetDrillDownCollector
{
    /// <summary>
    /// The statement leaf: for a <c>PG_BAD_ACTOR_*</c> root the statement's own <c>pg_statement_stats</c> deltas
    /// and its normalised text from <c>pg_statement_text</c>; for a <c>PG_TEMP_SPILL</c> root the top
    /// <c>temp_blks_written</c> offenders over the window. Pattern: <c>PgDrillDownCollector.Queries.cs</c> /
    /// <c>DarlingPgStatementReader.PgTopQueriesSql</c>. Unused parameters until then are the contract, not
    /// leftovers.
    /// <para>/* filled by lane 7 (bad actor) and lane 6 (temp offenders) — returns immediately until then. Every
    /// command sets <c>CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds</c> and every store
    /// call passes <c>context.CancellationToken</c>; the window binds naive-UTC via <c>AsNaive</c>. */</para>
    /// </summary>
    private partial Task CollectTopStatementsAsync(AnalysisFinding finding, AnalysisContext context, HashSet<string> pathKeys)
        => Task.CompletedTask;
}
