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
/// Advice for replication lag and slot retention (design §3.10). Value-stated from the replication facts — the worst standby by name, bytes of lag, WAL retained by the slot against <c>max_slot_wal_keep_size</c> — and honest that dropping a slot is a data-loss decision the operator owns, never a performance win (filled by lane 12 of #3691).
/// </summary>
public static partial class PgTargetAdvice
{
    /* filled by lane 12 */
    private static partial AdviceBlock? ComposeReplication(string key, IReadOnlyDictionary<string, Fact> factsByKey) => null;
}
