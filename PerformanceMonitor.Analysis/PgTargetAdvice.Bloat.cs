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
/// Advice for table and index bloat trends (design §3.12). Value-stated from the trend facts — the object, the growth across the window's samples and the sample count — and never a <c>CREATE INDEX</c> or <c>REINDEX</c> DDL statement (D8); the card points at the maintenance conversation (filled by lane 13 of #3691).
/// </summary>
public static partial class PgTargetAdvice
{
    /* filled by lane 13 */
    private static partial AdviceBlock? ComposeBloat(string key, IReadOnlyDictionary<string, Fact> factsByKey) => null;
}
