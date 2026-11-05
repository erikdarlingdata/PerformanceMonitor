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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

public sealed partial class ViewerDataService
{
    /// <summary>The SQL Server target's data start (#7386): the oldest <c>wait_stats</c> row. The one copy is
    /// <see cref="ServerDataStart.SqlServerSql"/>, which the web viewer's route runs too. One row, one read per server; the tab
    /// caches it. $1 server_id.</summary>
    public const string SqlServerDataStartSql = ServerDataStart.SqlServerSql;

    /// <summary>The PostgreSQL-target twin of <see cref="SqlServerDataStartSql"/>: the oldest <c>pg_database_stats</c> row.</summary>
    public const string PgDataStartSql = ServerDataStart.PostgresSql;

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
