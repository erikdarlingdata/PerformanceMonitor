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
/// Advice for the wait profile (lane 5). A sampled fact's prose contains "estimated from sampling" and never
/// "spent" — the estimate is per-backend-sample time summed over tasks, not a share of the server's clock.
/// </summary>
public static partial class PgTargetAdvice
{
    /* filled by lane 5 */
    private static partial AdviceBlock? ComposeWait(string key, IReadOnlyDictionary<string, Fact> factsByKey) => null;
}
