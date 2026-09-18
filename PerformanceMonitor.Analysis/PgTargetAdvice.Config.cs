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
/// Advice for the <c>CONFIG_PG_*</c> checks (lane 2; lanes 3/4/6 for the keys their families own): state the
/// current setting in its own unit, the measured evidence that fired beside it, and the value the evidence
/// argues for — the <c>ComposeConfigMaxdop</c> pattern.
/// </summary>
public static partial class PgTargetAdvice
{
    /* filled by lane 2 */
    private static partial AdviceBlock? ComposeConfig(string key, IReadOnlyDictionary<string, Fact> factsByKey) => null;
}
