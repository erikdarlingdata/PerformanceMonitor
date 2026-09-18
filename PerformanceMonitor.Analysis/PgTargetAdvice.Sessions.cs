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
/// Advice for connection saturation (lane 3): <c>sessions / (max_connections − superuser_reserved)</c> stated with
/// the three numbers; <c>PG_MONITORING_PERMISSIONS</c> says what the monitoring role could not see.
/// </summary>
public static partial class PgTargetAdvice
{
    /* filled by lane 3 */
    private static partial AdviceBlock? ComposeSessions(string key, IReadOnlyDictionary<string, Fact> factsByKey) => null;
}
