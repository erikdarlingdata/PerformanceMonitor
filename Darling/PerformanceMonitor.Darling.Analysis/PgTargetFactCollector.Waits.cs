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
    /// The wait profile: type rollups and named standouts keyed through <see cref="PgTargetFactKeys.WaitKey"/>.
    /// Reads BOTH <c>pg_wait_stats</c> (Aurora deltas) and <c>pg_wait_sampling</c> (stock sampling estimate, marked
    /// <c>is_sampled = 1</c> with <c>estimate_resolution_ms</c>) and emits from whichever has rows — they are
    /// platform-exclusive by construction, so both non-empty is itself an ERROR-worthy finding. Intervals via
    /// <c>LAG(collection_time)</c> (the table has no stored interval), a first-sighting collection is not a sample,
    /// and fractions divide by the wait source's OWN observed time (<c>wait_source_observed_ms</c>).
    /// <para>/* filled by lane 5 — returns immediately until then. The SQL it will run is declared as a
    /// <c>public const string …Sql</c> in THIS file so <see cref="AllSql"/> picks it up by reflection; every
    /// command sets <c>CommandTimeout = FactCommandTimeoutSeconds</c>, every store call passes
    /// <c>context.CancellationToken</c>, the catch is the <c>when (!AnalysisShutdown.IsExpectedAbandon(ex,
    /// context.CancellationToken))</c> shape that calls <see cref="ReportCollectionFailure"/>, and every
    /// rate divides by <see cref="AnalysisContext.ObservedDurationMs"/>, never the nominal window. */</para>
    /// </summary>
    private partial Task CollectWaitFactsAsync(AnalysisContext context, List<Fact> facts) => Task.CompletedTask;
}
