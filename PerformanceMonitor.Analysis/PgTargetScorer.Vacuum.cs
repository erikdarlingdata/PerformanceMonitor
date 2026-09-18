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
/// <c>pg_vacuum</c> — autovacuum backlog, wraparound trend, xmin hold (lane 4). The wraparound and xmin
/// bars are the SAME symbols the Tier-0 alert evaluator grades on (D9) — engine-defined lineage, referenced,
/// never retyped; a source pin holds that neither file carries the other's literal.
/// </summary>
public static partial class PgTargetScorer
{
    /* filled by lane 4 */
    private static partial double ScoreVacuumFact(Fact fact) => 0.0;

    /* filled by lane 4 */
    private static partial List<AmplifierDefinition> VacuumAmplifiers(string key) => [];
}
