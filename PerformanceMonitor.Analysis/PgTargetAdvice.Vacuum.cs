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
/// Advice for the vacuum family (lane 4): dead/threshold ratio and slope per table, time-to-wall arithmetic with
/// its "not computable" branch, and the xmin holder named per <c>holder_source</c>.
/// </summary>
public static partial class PgTargetAdvice
{
    /* filled by lane 4 */
    private static partial AdviceBlock? ComposeVacuum(string key, IReadOnlyDictionary<string, Fact> factsByKey) => null;
}
