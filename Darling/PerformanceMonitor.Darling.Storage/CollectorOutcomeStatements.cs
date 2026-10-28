/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #6861. The read statement for a collector's latest run, as ONE definition for the service and the viewer. The service's get_*
/// tools read it for their <c>precondition</c> answer (<c>DarlingRuntimePrecondition.LatestCollectorOutcomeSql</c> aliases it,
/// and its remarks hold the ordering rationale) and the viewer reads it for the note an empty tab shows. Two copies pinned
/// equal to each other fail only after both have drifted, so there is one.
/// </summary>
public static class CollectorOutcomeStatements
{
    /// <summary>
    /// The latest run of one collector for one server: its status, the explanation the runner stored and when it ran. Ordered
    /// by <c>collection_time DESC</c> first so <c>idx_collection_log_watermark (server_id, collector_name, collection_time DESC)</c>
    /// serves the walk, with <c>log_id DESC</c> only as the tiebreak. $1 server_id, $2 collector.
    /// </summary>
    public const string LatestCollectorOutcomeSql = @"
SELECT status, error_message, collection_time
FROM collection_log
WHERE server_id = $1
AND   collector_name = $2
ORDER BY collection_time DESC, log_id DESC
LIMIT 1";
}
