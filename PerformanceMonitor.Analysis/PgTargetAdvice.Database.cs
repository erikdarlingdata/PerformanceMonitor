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
/// Advice for the per-database counter facts (lane 2): the deadlock rate states "the engine counted N; M were
/// captured from the log", because the counter is complete and the log tail is lossy.
/// </summary>
public static partial class PgTargetAdvice
{
    /* filled by lane 2 */
    private static partial AdviceBlock? ComposeDatabase(string key, IReadOnlyDictionary<string, Fact> factsByKey) => null;
}
