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
    /// <c>PG_BLOCKING_CHAIN</c> from the window's <c>pg_blocking_edges</c> (reconstructed per capture — the <c>BlockingChainReconstructor</c> port from the <c>get_pg_blocking</c> reader, root attributed, recurrence counted across captures), <c>PG_LOCK_WAIT_EVENTS</c> from <c>pg_log_events</c> where <c>family = 'lock_wait'</c> (event grain, rated over observed time), and <c>PG_LONG_RUNNING_QUERY</c> from <c>pg_session_states</c>' active rows. The pattern to copy is <c>PgTargetFactCollector.Vacuum.cs</c> — per-object composition; the chain fact states its capture count so a sample is never read as an event log.
    /// <para>/* filled by lane 17 — returns immediately until then. The SQL it will run is declared as a
    /// <c>public const string …Sql</c> in THIS file so <see cref="AllSql"/> picks it up by reflection; every
    /// command sets <c>CommandTimeout = FactCommandTimeoutSeconds</c>, every store call passes
    /// <c>context.CancellationToken</c>, the catch is the <c>when (!AnalysisShutdown.IsExpectedAbandon(ex,
    /// context.CancellationToken))</c> shape that calls <see cref="ReportCollectionFailure"/>, and every
    /// rate divides by <see cref="AnalysisContext.ObservedDurationMs"/>, never the nominal window. The three
    /// tables are CollectorCatalog targets already, so the FROM/JOIN census admits them (pinned by name in
    /// PgTargetFactCollectorTests). */</para>
    /// </summary>
    private partial Task CollectBlockingFactsAsync(AnalysisContext context, List<Fact> facts) => Task.CompletedTask;
}
