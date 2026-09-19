/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The replication chain (filled by lane 12 of #3691, design §3.10): <c>PG_SLOT_XMIN</c> → <c>PG_XMIN_HOLD</c> /
/// <c>PG_AUTOVACUUM_BACKLOG</c> (a slot's horizon is one of the causes the vacuum family names), and
/// <c>PG_REPLICATION_LAG</c> ↔ <c>PG_WAL_VOLUME_SHIFT</c> (a standby falls behind what the primary writes).
/// Predicates read the destination fact's verdict, never a bar of their own.
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    /* filled by lane 12 */
    private partial void BuildReplicationEdges()
    {
    }
}
