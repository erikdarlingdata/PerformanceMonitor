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
/// Advice for temp spill and its <c>work_mem</c> co-fire (lane 6): bytes/sec of observed time and the offending
/// statements from the drill-down.
/// </summary>
public static partial class PgTargetAdvice
{
    /* filled by lane 6 */
    private static partial AdviceBlock? ComposeTemp(string key, IReadOnlyDictionary<string, Fact> factsByKey) => null;
}
