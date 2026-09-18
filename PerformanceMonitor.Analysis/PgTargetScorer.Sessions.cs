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
/// <c>pg_sessions</c> — connection saturation (lane 3): <c>sessions / (max_connections − superuser_reserved)</c>,
/// read from the lane-2 context facts at score time. The ceiling is engine-defined; the bands below it are stated.
/// </summary>
public static partial class PgTargetScorer
{
    /* filled by lane 3 */
    private static partial double ScoreSessionsFact(Fact fact) => 0.0;

    /* filled by lane 3; lane 5 adds the Lock-type wait confirmer */
    private static partial List<AmplifierDefinition> SessionsAmplifiers(string key) => [];
}
