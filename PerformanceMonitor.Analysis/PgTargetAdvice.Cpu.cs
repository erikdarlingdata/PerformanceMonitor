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
/// Advice for instance CPU (lane 9; Aurora only). Instance CPU, not process CPU — there is no SQL-vs-other split.
/// </summary>
public static partial class PgTargetAdvice
{
    /* filled by lane 9 */
    private static partial AdviceBlock? ComposeCpu(string key, IReadOnlyDictionary<string, Fact> factsByKey) => null;
}
