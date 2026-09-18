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
/// <c>pg_temp</c> — temp-file spill (lane 6). <c>CONFIG_PG_WORK_MEM</c> is scored in the config partial and
/// reaches ≥ 0.5 ONLY on a <c>PG_TEMP_SPILL</c> co-fire (D5); the spill fact itself grades bytes/sec of
/// observed time.
/// </summary>
public static partial class PgTargetScorer
{
    /* filled by lane 6 */
    private static partial double ScoreTempFact(Fact fact) => 0.0;

    /* filled by lane 6 */
    private static partial List<AmplifierDefinition> TempAmplifiers(string key) => [];
}
