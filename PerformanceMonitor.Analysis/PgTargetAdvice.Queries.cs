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
/// Advice for the top statements (lane 7): the <c>ScoreBadActorFact</c> tiering stated, and the caveat that
/// <c>queryid</c> is not stable across major upgrades, so occurrence tracking restarts at one.
/// </summary>
public static partial class PgTargetAdvice
{
    /* filled by lane 7 */
    private static partial AdviceBlock? ComposeQueries(string key, IReadOnlyDictionary<string, Fact> factsByKey) => null;
}
