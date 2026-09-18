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
/// <c>pg_config</c> — the <c>CONFIG_PG_*</c> setting checks (lane 2 fills the two knobs and the convention /
/// meta checks; lanes 3, 4 and 6 fill the keys their families own). Pattern: <c>FactScorer.ScoreConfigFact</c>
/// — 0.4 ONLY when the setting is bad, so a convention check roots an advisory card and never an incident
/// (D5). <see cref="PgTargetFactKeys.ServerMajorVersion"/> is context and stays 0. Every bar carries its
/// lineage marker (see the class summary in <c>PgTargetScorer.cs</c>).
/// </summary>
public static partial class PgTargetScorer
{
    /* filled by lane 2 (knobs), lane 3 (max_connections context), lane 4 (autovacuum off), lane 6 (work_mem co-fire) */
    private static partial double ScoreConfigFact(Fact fact) => 0.0;

    /* filled by lane 2 — CONFIG_PG_SHARED_BUFFERS reaches >= 0.5 ONLY when PG_BUFFER_CACHE_PRESSURE co-fires (D5) */
    private static partial List<AmplifierDefinition> ConfigAmplifiers(string key) => [];
}
