/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The saturation chain (lane 3): <c>PG_CONNECTION_SATURATION</c> → <c>Lock</c>-type waits (parked holders
/// block; lane 5 adds the wait edge); saturation ↔ <c>PG_IDLE_IN_TRANSACTION</c> is a v2 hook and stays a
/// comment until that fact exists.
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    /* filled by lane 3 */
    private partial void BuildSaturationEdges()
    {
    }
}
