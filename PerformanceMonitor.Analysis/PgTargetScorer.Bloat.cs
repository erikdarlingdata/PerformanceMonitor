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
/// <c>pg_bloat</c> — table and index bloat TRENDS (filled by lane 13 of #3691, design §3.12): <c>PG_BLOAT_TREND</c>
/// over the hourly <c>pg_table_bloat_stats</c> samples, <c>PG_INDEX_BLOAT_TREND</c> over the daily
/// <c>pg_index_bloat</c> samples. A trend, never a point estimate — the bar is on growth across the window's
/// samples, and a fact over so sparse a series states its own sample count in metadata. Bars carry their lineage
/// marker within six lines; unmeasured ones carry <c>threshold_lineage = 0</c>.
/// </summary>
public static partial class PgTargetScorer
{
    /* filled by lane 13 */
    private static partial double ScoreBloatFact(Fact fact) => 0.0;

    /* filled by lane 13 */
    private static partial List<AmplifierDefinition> BloatAmplifiers(string key) => [];
}
