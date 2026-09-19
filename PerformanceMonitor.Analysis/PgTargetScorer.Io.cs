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
/// <c>pg_io</c> — data-file I/O latency (filled by lane 11 of #3691, design §3.9): <c>PG_IO_READ_LATENCY_MS</c> /
/// <c>PG_IO_WRITE_LATENCY_MS</c> composed from the reset-aware <c>pg_io_stats</c> counter differences
/// (<c>pg_stat_io</c>, PostgreSQL 16+; absent below). The pattern to copy is <c>PgTargetScorer.Temp.cs</c>
/// (baseline-relative with a floor); every bar carries its lineage marker within six lines
/// (<c>PgTargetThresholdLineageTests</c>) and, until a fleet read exists, <c>threshold_lineage = 0</c>.
/// </summary>
public static partial class PgTargetScorer
{
    /* filled by lane 11 */
    private static partial double ScoreIoFact(Fact fact) => 0.0;

    /* filled by lane 11 */
    private static partial List<AmplifierDefinition> IoAmplifiers(string key) => [];
}
