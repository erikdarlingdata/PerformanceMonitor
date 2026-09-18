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
/// Advice for the <c>ANOMALY_PG_*</c> facts (lane 9): the observed value, sigmas above the hour-of-week baseline
/// and the baseline itself, the <c>ComposeAnomaly</c> pattern; the deadlock-rate ratio states its own baseline.
/// </summary>
public static partial class PgTargetAdvice
{
    /* filled by lane 9 */
    private static partial AdviceBlock? ComposeAnomaly(string key, IReadOnlyDictionary<string, Fact> factsByKey) => null;
}
