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
    /// The latest <c>pg_server_config</c> snapshot → <c>CONFIG_PG_*</c> facts, with the unit normalisation
    /// (<c>8kB</c> pages, <c>kB</c>/<c>MB</c>/<c>GB</c>, <c>ms</c>/<c>s</c>/<c>min</c>, <c>-1</c> sentinels preserved)
    /// this read owes because <c>pg_settings</c> reports values in per-setting units. Pattern:
    /// <c>DarlingPgServerConfigReader.CurrentConfigSql</c> with its three-value <c>source</c> exclusion
    /// (<c>client</c>, <c>session</c>, <c>override</c>). Also emits the context facts lane 3 reads at score time
    /// (<c>CONFIG_PG_MAX_CONNECTIONS</c>, <c>CONFIG_PG_SUPERUSER_RESERVED</c>) and the keys lanes 4 and 6 score.
    /// <para>/* filled by lane 2 — returns immediately until then. The SQL it will run is declared as a
    /// <c>public const string …Sql</c> in THIS file so <see cref="AllSql"/> picks it up by reflection; every
    /// command sets <c>CommandTimeout = FactCommandTimeoutSeconds</c>, every store call passes
    /// <c>context.CancellationToken</c>, the catch is the <c>when (!AnalysisShutdown.IsExpectedAbandon(ex,
    /// context.CancellationToken))</c> shape that calls <see cref="ReportCollectionFailure"/>, and every
    /// rate divides by <see cref="AnalysisContext.ObservedDurationMs"/>, never the nominal window. */</para>
    /// </summary>
    private partial Task CollectConfigFactsAsync(AnalysisContext context, List<Fact> facts) => Task.CompletedTask;
}
