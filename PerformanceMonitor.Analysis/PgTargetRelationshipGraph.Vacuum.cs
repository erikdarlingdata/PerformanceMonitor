/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The vacuum chain (lane 4): <c>PG_WRAPAROUND_TREND</c> → <c>PG_AUTOVACUUM_BACKLOG</c> → <c>PG_XMIN_HOLD</c>, with
/// <c>CONFIG_PG_AUTOVACUUM_OFF</c> as the config leaf. <c>PG_XMIN_HOLD</c> → idle-in-transaction / slot xmin are
/// v2 leaves; in v1 the hold fact carries <c>holder_source</c> metadata and the advice names the fix per source.
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    /* filled by lane 4 */
    private partial void BuildVacuumEdges()
    {
    }
}
