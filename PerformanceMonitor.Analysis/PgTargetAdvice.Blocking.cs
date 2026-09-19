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
/// Advice for the blocking / active-query family (design §2a). Value-stated from the facts — the root blocker's state, application and fingerprint, how many sessions were behind it and in how many captures, the lock-wait event rate, the long-running statement's duration against the window's norm — and a remedy per ROOT STATE, because an <c>idle in transaction</c> root is an application defect while an <c>active</c> root is a query-tuning problem and the two need opposite responses (the <c>get_pg_blocking</c> tool's own rule). Never a <c>CREATE INDEX</c> statement (D8); the sample caveat is stated on every chain card (filled by lane 17 of #3691).
/// </summary>
public static partial class PgTargetAdvice
{
    /* filled by lane 17 */
    private static partial AdviceBlock? ComposeBlocking(string key, IReadOnlyDictionary<string, Fact> factsByKey) => null;

    /* filled by lane 17 — the ANOMALY_PG_BLOCKING arm of ComposeAnomaly delegates here (PgTargetAdvice.Anomaly.cs), so
       the anomaly's prose lives with its family and the shared prefix routing never reaches a SQL Server composer. */
    private static partial AdviceBlock? ComposeBlockingAnomaly(IReadOnlyDictionary<string, Fact> factsByKey) => null;
}
