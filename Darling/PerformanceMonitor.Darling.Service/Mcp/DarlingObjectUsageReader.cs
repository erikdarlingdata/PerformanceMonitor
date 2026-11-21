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

    /// <summary>
    /// The database's Query Store capture mode in the window, from the stored health snapshots (V137's <c>query_capture_mode</c>): the newest
    /// one that is not <c>ALL</c> where there is one, otherwise the newest. A mode other than <c>ALL</c> at any point in the window means rare
    /// or cheap statements may have gone unrecorded, so it wins over a later <c>ALL</c>. No row when none was stored. A day of slack before
    /// the window start keeps a snapshot just before it. $1 server_id, $2 database_name, $3 the window start.
    /// </summary>
    internal const string CaptureModeSql = """
        SELECT h.query_capture_mode
        FROM collect.query_store_health AS h
        WHERE h.server_id = $1
        AND   h.database_name = $2
        AND   h.capture_time >= $3 - interval '1 day'
        AND   h.query_capture_mode IS NOT NULL
        ORDER BY
            upper(btrim(h.query_capture_mode)) = 'ALL',
            h.capture_time DESC
        LIMIT 1
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

    /// <summary>
    /// The names Query Store statements referenced over the window that can be objects of THIS database (#8784 part 2): names statements run
    /// in it wrote with no database part (or its own), and names statements run in any other database of the server wrote with its name as
    /// the database part, so a view read only from another database counts. The database part is compared ignoring case, as SQL Server
    /// compares database names on a default collation. $1 server_id, $2 database_name, $3 the first day.
    /// </summary>
    internal static readonly string ReferencesSql = $"""
        SELECT
            r.referenced_schema,
            r.referenced_name,
            r.is_function,
            sum(r.execution_count),
            max(r.last_execution_time),
            bool_and(r.text_matched)
        FROM {ObjectReferencesDaily.Table} AS r
        WHERE r.server_id = $1
        AND   r.day >= $3
        AND   (
                  (r.database_name = $2 AND (r.referenced_database = '' OR lower(r.referenced_database) = lower($2)))
               OR (r.database_name <> $2 AND lower(r.referenced_database) = lower($2))
              )
        GROUP BY
            r.referenced_schema,
            r.referenced_name,
            r.is_function
        """;

    /// <summary>
    /// The days of the window on which the summary looked at statement text for THIS database: the oldest, the newest, how many, and the
    /// statements looked at, with no text and unparsed. $1 server_id, $2 database_name, $3 the first day.
    /// </summary>
    internal static readonly string ReferenceDaysSql = $"""
        SELECT
            min(d.day),
            max(d.day),
            count(*),
            COALESCE(sum(d.statements), 0)::bigint,
            COALESCE(sum(d.statements_without_text), 0)::bigint,
            COALESCE(sum(d.statements_unparsed), 0)::bigint
        FROM {ObjectReferencesDaily.DatabaseDaysTable} AS d
        WHERE d.server_id = $1
        AND   d.database_name = $2
        AND   d.day >= $3
        """;

    /// <summary>The database's newest dependency list capture (#8784 part 2). $1 server_id, $2 database_name.</summary>
    internal const string DependenciesSql = """
        SELECT
            d.collection_time,
            d.referencing_schema,
            d.referencing_name,
            d.referencing_type,
            d.referenced_server,
            d.referenced_database,
            d.referenced_schema,
            d.referenced_name,
            COALESCE(d.is_bound, false),
            COALESCE(d.is_caller_dependent, false),
            COALESCE(d.is_ambiguous, false)
        FROM collect.object_dependencies AS d
        WHERE d.server_id = $1
        AND   d.database_name = $2
        AND   d.collection_time =
              (
                  SELECT max(l.collection_time)
                  FROM collect.object_dependencies AS l
                  WHERE l.server_id = $1
                  AND   l.database_name = $2
              )
        """;

    /// <summary>
    /// The rows of the other databases' newest dependency lists on the same server that name the asked database by a three-part name
    /// (#8784 review): a reporting database's <c>A.dbo.vReport</c> reading <c>B.dbo.vCustomer</c> is read for B here, so <c>vCustomer</c> is
    /// not called unused when only <c>vReport</c> is ever queried. $1 server id, $2 database name. The database part is matched ignoring case,
    /// as SQL Server does on a case-insensitive server; a linked-server row is never the same server's database.
    /// </summary>
    internal const string InboundDependenciesSql = """
        SELECT
            d.database_name,
            d.referencing_schema,
            d.referencing_name,
            d.referencing_type,
            d.referenced_server,
            d.referenced_database,
            d.referenced_schema,
            d.referenced_name,
            COALESCE(d.is_bound, false),
            COALESCE(d.is_caller_dependent, false),
            COALESCE(d.is_ambiguous, false)
        FROM collect.object_dependencies AS d
        JOIN
        (
            SELECT
                l.database_name,
                max(l.collection_time) AS collection_time
            FROM collect.object_dependencies AS l
            WHERE l.server_id = $1
            AND   l.database_name <> $2
            GROUP BY
                l.database_name
        ) AS newest
          ON  newest.database_name = d.database_name
          AND newest.collection_time = d.collection_time
        WHERE d.server_id = $1
        AND   lower(d.referenced_database) = lower($2)
        AND   COALESCE(d.referenced_server, '') = ''
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

        string? captureMode = null;
        await using (var mode = postgres.CreateCommand(CaptureModeSql))
        {
            mode.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(mode, serverId);
            DarlingMcpReadParameters.AddText(mode, databaseName);
            DarlingMcpReadParameters.AddTimestamp(mode, windowStartUtc);
            await using var reader = await mode.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken) && !reader.IsDBNull(0))
            {
                captureMode = reader.GetString(0);
            }
        }

        return new ObjectExecutionWindow(
            from,
            to,
            "Darling's per-day module summary, copied from the daily Query Store summary and kept as long as index_object_stats. A day is "
            + "summarised after it ends, so the newest day or two are not in it yet, and a day changed after it was summarised can be off. "
            + "Covered days are the days Query Store held anything for this database.",
            modules,
            daysWithData,
            captureMode);
    }

    /// <summary>Reads the names Query Store statements referenced over the window. The covered range is the days the summary looked at
    /// statement text for this database.</summary>
    public static async Task<ObjectReferenceWindow> ReadReferencesAsync(
        NpgsqlDataSource postgres, int serverId, string databaseName, DateTime windowStartUtc, CancellationToken cancellationToken)
    {
        var firstDay = DateOnly.FromDateTime(windowStartUtc);
        DateTime? from = null;
        DateTime? to = null;
        var daysWithData = 0;
        long statements = 0, withoutText = 0, unparsed = 0;
        await using (var days = postgres.CreateCommand(ReferenceDaysSql))
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
                statements = reader.GetInt64(3);
                withoutText = reader.GetInt64(4);
                unparsed = reader.GetInt64(5);
            }
        }

        var references = new List<ObjectReferenceTotals>();
        await using (var command = postgres.CreateCommand(ReferencesSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            DarlingMcpReadParameters.AddText(command, databaseName);
            command.Parameters.Add(new NpgsqlParameter<DateOnly> { TypedValue = firstDay });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var executions = reader.IsDBNull(3) ? 0m : reader.GetDecimal(3);
                references.Add(new ObjectReferenceTotals(
                    reader.GetString(0),
                    reader.GetString(1),
                    executions >= long.MaxValue ? long.MaxValue : (long)executions,
                    reader.IsDBNull(4) ? null : reader.GetDateTime(4),
                    TextMatched: !reader.IsDBNull(5) && reader.GetBoolean(5),
                    IsFunction: reader.GetBoolean(2)));
            }
        }

        return new ObjectReferenceWindow(
            from,
            to,
            daysWithData,
            "Darling's per-day summary of the names Query Store statements referenced, parsed from the stored statement text and kept as long "
            + "as index_object_stats. A day is summarised after it ends, so the newest day or two are not in it yet. Covered days are the days "
            + "statement text was looked at for this database.",
            references,
            statements,
            withoutText,
            unparsed);
    }

    /// <summary>Reads the database's newest dependency list, the rows that never name a user object of the database left out
    /// (<see cref="ObjectDependencyFilter"/>), or null when the store has no capture of it.</summary>
    public static async Task<ObjectDependencyList?> ReadDependenciesAsync(
        NpgsqlDataSource postgres, int serverId, string databaseName, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(DependenciesSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddText(command, databaseName);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        DateTime? captured = null;
        var rows = new List<RawObjectDependency>();
        while (await reader.ReadAsync(cancellationToken))
        {
            captured ??= reader.GetDateTime(0);
            rows.Add(new RawObjectDependency(
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                reader.IsDBNull(3) ? "" : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.IsDBNull(7) ? "" : reader.GetString(7),
                reader.GetBoolean(8),
                reader.GetBoolean(9),
                reader.GetBoolean(10)));
        }

        return captured is { } at ? new ObjectDependencyList(at, ObjectDependencyFilter.ToEdges(rows, databaseName)) : null;
    }

    /// <summary>Reads the edges from the other databases' newest dependency lists on the same server into this database
    /// (<see cref="InboundDependenciesSql"/>, filtered by <see cref="ObjectDependencyFilter.ToInboundEdge"/>).</summary>
    public static async Task<IReadOnlyList<ObjectDependency>> ReadInboundDependenciesAsync(
        NpgsqlDataSource postgres, int serverId, string databaseName, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(InboundDependenciesSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddText(command, databaseName);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var edges = new List<ObjectDependency>();
        while (await reader.ReadAsync(cancellationToken))
        {
            var source = reader.GetString(0);
            var row = new RawObjectDependency(
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                reader.IsDBNull(3) ? "" : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.IsDBNull(7) ? "" : reader.GetString(7),
                reader.GetBoolean(8),
                reader.GetBoolean(9),
                reader.GetBoolean(10));
            if (ObjectDependencyFilter.ToInboundEdge(row, source, databaseName) is { } edge)
            {
                edges.Add(edge);
            }
        }

        return edges;
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
