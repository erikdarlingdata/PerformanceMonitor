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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The store reads behind <c>get_object_usage</c> (#8784): one database's index and object stats snapshots over the window (the shared
/// <see cref="ObjectUsageWindowRead"/> statement and mapper), fed to the shared <see cref="ObjectUsageWindow"/>; its module executions from the per-day summary (<c>collect.object_executions_daily</c>); and its newest
/// object list (<c>collect.object_inventory</c>). Stored reads only, no live monitored-server hit.
/// </summary>
internal static class DarlingObjectUsageReader
{
    /// <summary>
    /// The window's module executions for one database from the per-day summary. $1 server_id, $2 database_name, $3 the first day (a date).
    /// </summary>
    internal const string ExecutionsSql = """
        SELECT
            e.module_name,
            sum(e.execution_count)::bigint,
            max(e.last_execution_time)
        FROM collect.object_executions_daily AS e
        WHERE e.server_id = $1
        AND   e.database_name = $2
        AND   e.day >= $3
        GROUP BY
            e.module_name
        """;

    /// <summary>
    /// The days of the window on which the daily Query Store summary held anything for THIS database, whether or not a module ran: the oldest,
    /// the newest and how many. Per database and not per server, because Query Store is per database: a database with Query Store off (or
    /// read-only, or skipped by the collector) has no such day while its neighbours on the same server do, and reading the server's days would
    /// report full coverage for it with every module "Not executed in window". The count shows a hole between the oldest and newest day.
    /// $1 server_id, $2 database_name, $3 the first day.
    /// </summary>
    internal static readonly string ExecutionDaysSql = $"""
        SELECT min(d.day), max(d.day), count(*)
        FROM {ObjectExecutionsDaily.DatabaseDaysTable} AS d
        WHERE d.server_id = $1
        AND   d.database_name = $2
        AND   d.day >= $3
        """;

    /// <summary>The database's newest object list capture. $1 server_id, $2 database_name.</summary>
    internal const string InventorySql = """
        SELECT
            i.collection_time,
            i.schema_name,
            i.object_name,
            i.type_desc,
            i.create_date,
            i.modify_date,
            COALESCE(i.is_natively_compiled, false)
        FROM collect.object_inventory AS i
        WHERE i.server_id = $1
        AND   i.database_name = $2
        AND   i.collection_time =
              (
                  SELECT max(l.collection_time)
                  FROM collect.object_inventory AS l
                  WHERE l.server_id = $1
                  AND   l.database_name = $2
              )
        """;

    /// <summary>Reads the window and runs it through <see cref="ObjectUsageWindow"/>.</summary>
    public static async Task<ObjectUsageWindowResult> ReadTablesAsync(
        NpgsqlDataSource postgres, int serverId, string databaseName, DateTime windowStartUtc, CancellationToken cancellationToken)
    {
        var window = new ObjectUsageWindow();
        await using var command = postgres.CreateCommand(ObjectUsageWindowRead.Sql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddText(command, databaseName);
        DarlingMcpReadParameters.AddTimestamp(command, windowStartUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            window.Add(ObjectUsageWindowRead.Map(reader));
        }

        return window.Complete();
    }

    /// <summary>Reads the module executions. The covered range is the days the summary holds for this database in the window.</summary>
    public static async Task<ObjectExecutionWindow> ReadExecutionsAsync(
        NpgsqlDataSource postgres, int serverId, string databaseName, DateTime windowStartUtc, CancellationToken cancellationToken)
    {
        var firstDay = DateOnly.FromDateTime(windowStartUtc);
        DateTime? from = null;
        DateTime? to = null;
        var daysWithData = 0;
        await using (var days = postgres.CreateCommand(ExecutionDaysSql))
        {
            days.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(days, serverId);
            DarlingMcpReadParameters.AddText(days, databaseName);
            days.Parameters.Add(new NpgsqlParameter<DateOnly> { TypedValue = firstDay });
            await using var reader = await days.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken) && !reader.IsDBNull(0))
            {
                from = reader.GetFieldValue<DateOnly>(0).ToDateTime(TimeOnly.MinValue);
                to = reader.GetFieldValue<DateOnly>(1).ToDateTime(TimeOnly.MinValue).AddDays(1);
                daysWithData = Convert.ToInt32(reader.GetValue(2), System.Globalization.CultureInfo.InvariantCulture);
            }
        }

        var modules = new List<ObjectExecutionTotals>();
        await using (var command = postgres.CreateCommand(ExecutionsSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            DarlingMcpReadParameters.AddText(command, databaseName);
            command.Parameters.Add(new NpgsqlParameter<DateOnly> { TypedValue = firstDay });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                modules.Add(new ObjectExecutionTotals(
                    reader.GetString(0),
                    reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                    reader.IsDBNull(2) ? null : reader.GetDateTime(2)));
            }
        }

        return new ObjectExecutionWindow(
            from,
            to,
            "Darling's per-day module summary, copied from the daily Query Store summary and kept as long as index_object_stats. A day is "
            + "summarised after it ends, so the newest day or two are not in it yet, and a day changed after it was summarised can be off. "
            + "Covered days are the days Query Store held anything for this database.",
            modules,
            daysWithData);
    }

    /// <summary>Reads the database's newest object list, or null when the store has no capture of it (the collector is off, or has not run).</summary>
    public static async Task<ObjectInventory?> ReadInventoryAsync(
        NpgsqlDataSource postgres, int serverId, string databaseName, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(InventorySql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddText(command, databaseName);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        DateTime? captured = null;
        var objects = new List<ObjectInventoryEntry>();
        while (await reader.ReadAsync(cancellationToken))
        {
            captured ??= reader.GetDateTime(0);
            objects.Add(new ObjectInventoryEntry(
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                reader.IsDBNull(3) ? "" : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetDateTime(4),
                reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                reader.GetBoolean(6)));
        }

        return captured is { } at ? new ObjectInventory(at, objects) : null;
    }
}
