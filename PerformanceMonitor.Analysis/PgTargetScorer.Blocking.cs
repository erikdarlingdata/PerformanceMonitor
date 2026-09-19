/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// <c>pg_blocking</c> — the blocking / active-query family (filled by lane 17 of #3691, design §2a): <c>PG_BLOCKING_CHAIN</c>
/// from the window's sampled <c>pg_blocking_edges</c> reconstructed into chains with the root attributed (the
/// <c>BlockingChainReconstructor</c> port), <c>PG_LOCK_WAIT_EVENTS</c> from the engine-written <c>lock_wait</c> family
/// of <c>pg_log_events</c> (event grain — what the sample between two captures cannot see), and
/// <c>PG_LONG_RUNNING_QUERY</c> from <c>pg_session_states</c>' active rows. Bars carry their lineage marker within six
/// lines; unmeasured ones carry <c>threshold_lineage = 0</c>; gates are rates or fractions of OBSERVED time, never
/// absolute totals. The chain fact's grade must state that its source is a SAMPLE (a chain shorter than the capture
/// interval was never seen) — the collector's own caveat, carried into the finding.
/// </summary>
public static partial class PgTargetScorer
{
    /* filled by lane 17 */
    private static partial double ScoreBlockingFact(Fact fact) => 0.0;

    /* filled by lane 17 */
    private static partial List<AmplifierDefinition> BlockingAmplifiers(string key) => [];
}
