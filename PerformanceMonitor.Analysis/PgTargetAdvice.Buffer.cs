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
/// Advice for the buffer-cache composite (lane 2): per-component evidence (hit ratio, evictions when tracked,
/// bgwriter), saying which arms were structurally absent on this flavour / major (D6).
/// </summary>
public static partial class PgTargetAdvice
{
    /* filled by lane 2 */
    private static partial AdviceBlock? ComposeBuffer(string key, IReadOnlyDictionary<string, Fact> factsByKey) => null;
}
