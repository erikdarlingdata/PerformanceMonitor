/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// When a server was first collected (#7386): the oldest <c>wait_stats</c> row for a SQL Server target, the oldest
/// <c>pg_database_stats</c> row for a PostgreSQL one. The two tables <c>analyze_server</c> measures its history depth on, so the
/// charts and that gate agree on what "the server has this much history" means. The desktop viewer and the web viewer's
/// <c>/api/server-data-start</c> route run these two statements, so the two charts agree on where the data starts.
///
/// <para>Each reads the oldest row in time order, not a <c>MIN</c>, so the ordered read can stop in the oldest chunk. One row.
/// $1 server_id.</para>
/// </summary>
public static class ServerDataStart
{
    /// <summary>The SQL Server target's statement.</summary>
    public const string SqlServerSql = "SELECT collection_time FROM wait_stats WHERE server_id = $1 ORDER BY collection_time ASC LIMIT 1";

    /// <summary>The PostgreSQL-target twin of <see cref="SqlServerSql"/>.</summary>
    public const string PostgresSql = "SELECT collection_time FROM pg_database_stats WHERE server_id = $1 ORDER BY collection_time ASC LIMIT 1";
}
