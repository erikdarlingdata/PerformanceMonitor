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
/// Advice for the durability posture facts (lane 8). Durability only: what the setting protects against and what is
/// lost while it is off. NONE of "faster", "performance", "throughput", "speed" — pinned by
/// <c>PgTargetPostureIsolationTests</c> (D6).
/// </summary>
public static partial class PgTargetAdvice
{
    /* filled by lane 8 */
    private static partial AdviceBlock? ComposePosture(string key, IReadOnlyDictionary<string, Fact> factsByKey) => null;
}
