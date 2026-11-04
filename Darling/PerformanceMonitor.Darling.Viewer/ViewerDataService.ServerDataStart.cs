/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Viewer;

public sealed partial class ViewerDataService
{
    /// <summary>
    /// When the server was first collected (#7386): the oldest <c>wait_stats</c> row for a SQL Server target, the oldest
    /// <c>pg_database_stats</c> row for a PostgreSQL one. The two tables <c>analyze_server</c> measures its history depth on, so the
    /// charts and that gate agree on what "the server has this much history" means. Read as the oldest row in time order, not as a
    /// <c>MIN</c>, so the ordered read can stop in the oldest chunk. One row, one read per server; the tab caches it.
    /// $1 server_id.
    /// </summary>
    public const string SqlServerDataStartSql = "SELECT collection_time FROM wait_stats WHERE server_id = $1 ORDER BY collection_time ASC LIMIT 1";

    /// <summary>The PostgreSQL-target twin of <see cref="SqlServerDataStartSql"/>.</summary>
    public const string PgDataStartSql = "SELECT collection_time FROM pg_database_stats WHERE server_id = $1 ORDER BY collection_time ASC LIMIT 1";

    /// <summary>The first time the server was collected, as naive UTC, or null when it has no row yet. A read that fails throws; the
    /// caller keeps what it had.</summary>
    public async Task<DateTime?> GetServerDataStartAsync(int serverId, bool isPostgres, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(isPostgres ? PgDataStartSql : SqlServerDataStartSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is DateTime first ? DateTime.SpecifyKind(first, DateTimeKind.Utc) : null;
    }
}
