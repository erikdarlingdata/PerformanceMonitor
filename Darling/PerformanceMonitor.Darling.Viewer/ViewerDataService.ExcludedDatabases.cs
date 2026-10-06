/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Store reads backing the Excluded Databases picker. The viewer never connects to a monitored SQL
/// Server (so it can't enumerate its LIVE database list the way Lite's dialog does), but the Darling
/// store already HAS the databases the service collected — <c>v_database_config</c> (the sys.databases
/// snapshot) and <c>v_database_size_stats</c> (per-file sizes). The picker offers those collected user
/// databases as checkboxes instead of a free-text editor, keeping a manual-add fallback for a
/// not-yet-collected database. System databases (master/model/msdb/tempdb) are always excluded from the
/// list — they mirror Lite's <c>database_id &gt; 4</c> user-database filter and are never per-database
/// collector targets anyway.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>
    /// The distinct user databases the store has collected for one server (empty when nothing has been
    /// collected yet). Feeds the Excluded Databases picker's checkbox list.
    /// </summary>
    public async Task<List<string>> GetCollectedDatabaseNamesAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var names = new List<string>();

        await using var command = _dataSource.CreateCommand(CollectedDatabases.NamesSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }
}
