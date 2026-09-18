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
/// Advice for checkpoint / WAL pressure (lane 2): the requested-vs-timed ratio and <c>wal_bytes</c> per
/// interval the engine measured, and the <c>max_wal_size</c> the ratio argues for.
/// </summary>
public static partial class PgTargetAdvice
{
    /* filled by lane 2 */
    private static partial AdviceBlock? ComposeWrite(string key, IReadOnlyDictionary<string, Fact> factsByKey) => null;
}
