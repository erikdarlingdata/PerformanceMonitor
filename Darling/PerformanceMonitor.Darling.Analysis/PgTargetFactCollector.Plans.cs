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
    /// <c>PG_PLAN_REGRESSION</c> from the window's <c>pg_statement_stats</c> (the reset-aware per-statement mean-ms
    /// difference — copy <c>PgTargetFactCollector.Queries.cs</c>'s differencing, never re-derive the NULL→timestamp
    /// first-reset trap) joined to <c>pg_plan_capture</c> by <c>query_id</c> for the <c>plan_hash</c> flip;
    /// <c>PG_PARAMETER_SENSITIVITY</c> from <c>plan_hash</c> variance per <c>query_id</c> beside <c>pg_column_stats</c>'
    /// <c>top_value_frequency</c>; and (lane 30) <c>PG_SEQ_SCAN_ADVISORY</c> from <c>plan_json</c> Seq Scan nodes beside
    /// <c>pg_predicate_stats</c>. The statement travels through the <c>ObjectName</c> seam (<c>Fact.Metadata</c> is
    /// doubles-only); a fact naming an index candidate carries evidence, never DDL (D8).
    /// <para>/* filled by lane 27 (lane 30 adds the Seq-Scan read in the same partial) — returns immediately until then.
    /// The SQL it will run is declared as a
    /// <c>public const string …Sql</c> in THIS file so <see cref="AllSql"/> picks it up by reflection; every
    /// command sets <c>CommandTimeout = FactCommandTimeoutSeconds</c>, every store call passes
    /// <c>context.CancellationToken</c>, <c>$N</c> positional, no bare <c>now()</c> / <c>CURRENT_TIMESTAMP</c>
    /// (StoreSqlClockDisciplineTests), the catch is the <c>when (!AnalysisShutdown.IsExpectedAbandon(ex,
    /// context.CancellationToken))</c> shape that calls <see cref="ReportCollectionFailure"/>, and every rate divides
    /// by <see cref="AnalysisContext.ObservedDurationMs"/>, never the nominal window.
    /// The four tables are CollectorCatalog targets already, so the FROM/JOIN census admits them (pinned by name in
    /// PgTargetFactCollectorTests). */</para>
    /// </summary>
    private partial Task CollectPlanFactsAsync(AnalysisContext context, List<Fact> facts) => Task.CompletedTask;
}
