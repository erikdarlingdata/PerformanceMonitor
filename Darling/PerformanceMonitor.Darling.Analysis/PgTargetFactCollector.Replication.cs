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
    /// <c>PG_REPLICATION_LAG</c> from <c>pg_replication_stats</c>, <c>PG_SLOT_RETENTION</c> / <c>PG_SLOT_XMIN</c> from <c>pg_replication_slot_stats</c>. The pattern to copy is <c>PgTargetFactCollector.Vacuum.cs</c> — per-object composition naming the worst standby / slot; the 5-minute cadence means the fact carries its own sample count and never the one-minute coverage fraction.
    /// <para>/* filled by lane 12 — returns immediately until then. The SQL it will run is declared as a
    /// <c>public const string …Sql</c> in THIS file so <see cref="AllSql"/> picks it up by reflection; every
    /// command sets <c>CommandTimeout = FactCommandTimeoutSeconds</c>, every store call passes
    /// <c>context.CancellationToken</c>, the catch is the <c>when (!AnalysisShutdown.IsExpectedAbandon(ex,
    /// context.CancellationToken))</c> shape that calls <see cref="ReportCollectionFailure"/>, and every
    /// rate divides by <see cref="AnalysisContext.ObservedDurationMs"/>, never the nominal window. */</para>
    /// </summary>
    private partial Task CollectReplicationFactsAsync(AnalysisContext context, List<Fact> facts) => Task.CompletedTask;
}
