/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The write chain (lane 2): <c>PG_WAL_VOLUME_SHIFT</c> → <c>PG_CHECKPOINT_PRESSURE</c> → <c>CONFIG_PG_MAX_WAL_SIZE</c>;
/// lane 5 adds <c>IO:WALSync</c> / <c>LWLock:WALWrite</c> waits into the same chain.
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    /* filled by lane 2 */
    private partial void BuildWriteEdges()
    {
    }
}
