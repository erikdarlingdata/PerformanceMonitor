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
/// <c>pg_queries</c> — top statements (lane 7), keyed through <see cref="PgTargetFactKeys.BadActorKey"/>.
/// Pattern: <c>FactScorer.ScoreBadActorFact</c> tiering by execution count, with PostgreSQL-measured bars.
/// </summary>
public static partial class PgTargetScorer
{
    /* filled by lane 7 */
    private static partial double ScoreQueriesFact(Fact fact) => 0.0;

    /* filled by lane 7 */
    private static partial List<AmplifierDefinition> QueriesAmplifiers(string key) => [];
}
