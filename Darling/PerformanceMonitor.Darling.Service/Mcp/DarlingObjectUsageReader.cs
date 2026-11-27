/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
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
    /// compares database names on a default collation: the summary stores it folded by <c>ObjectReferenceAggregator.FoldName</c>, and $4 is
    /// the asked name folded the same way, so the read and the build fold in one place (#8784 review). The schema and name come back as
    /// first seen where the build kept that spelling, and folded otherwise. $1 server_id, $2 database_name, $3 the first day, $4 the database
    /// name folded.
    ///
    /// <para><b>Two arms, each on its own index</b> (#8784 review). One <c>OR</c> over both could seek only on <c>server_id</c>, so every call
    /// read every database's rows for the server across the window. The same-database arm seeks the primary key on
    /// <c>(server_id, database_name, referenced_database)</c>; the cross-database arm seeks <c>idx_object_references_daily_cross_database</c>
    /// <c>(server_id, referenced_database, day) WHERE referenced_database &lt;&gt; ''</c>, and repeats that predicate so the planner can prove
    /// the partial index applies.</para>
    /// </summary>
    internal static readonly string ReferencesSql = $"""
        SELECT
            COALESCE(min(r.referenced_schema_as_seen), r.referenced_schema),
            COALESCE(min(r.referenced_name_as_seen), r.referenced_name),
            r.is_function,
            sum(r.execution_count),
            max(r.last_execution_time),
            bool_and(r.text_matched)
        FROM
        (
            SELECT
                s.referenced_schema,
                s.referenced_name,
                s.is_function,
                s.execution_count,
                s.last_execution_time,
                s.text_matched,
                s.referenced_schema_as_seen,
                s.referenced_name_as_seen
            FROM {ObjectReferencesDaily.Table} AS s
            WHERE s.server_id = $1
            AND   s.database_name = $2
            AND   s.referenced_database IN ('', $4)
            AND   s.day >= $3
            UNION ALL
            SELECT
                x.referenced_schema,
                x.referenced_name,
                x.is_function,
                x.execution_count,
                x.last_execution_time,
                x.text_matched,
                x.referenced_schema_as_seen,
                x.referenced_name_as_seen
            FROM {ObjectReferencesDaily.Table} AS x
            WHERE x.server_id = $1
            AND   x.referenced_database = $4
            AND   x.referenced_database <> ''
            AND   x.day >= $3
            AND   x.database_name <> $2
        ) AS r
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
    ///
    /// <para><b>Only each database's newest capture is read</b> (#8784 review). A <c>max(collection_time) ... GROUP BY database_name</c>
    /// reads every row the server has kept over the 30 days, since PostgreSQL has no skip scan: about 13.5 million rows a call for 150
    /// databases of 3,000 rows each. Instead the database names are found by a recursive skip scan over
    /// <c>idx_object_dependencies_database</c> <c>(server_id, database_name, collection_time)</c>, one index seek per name, then each
    /// database's newest <c>collection_time</c> is one more seek (a <c>LATERAL</c> top-1 on the same index), and only those captures' rows
    /// are read.</para>
    ///
    /// <para><b>Only the rows that name another database</b> (#8784 review). The second <c>LATERAL</c> seeks
    /// <c>idx_object_dependencies_cross_database</c>, partial on <c>COALESCE(referenced_database, '') &lt;&gt; ''</c>, and repeats that
    /// predicate so the planner can prove the index applies: a newest capture's same-database rows, nearly all of it, are never read.</para>
    /// </summary>
    internal const string InboundDependenciesSql = """
        WITH RECURSIVE
            databases AS
        (
            (
                SELECT
                    l.database_name
                FROM collect.object_dependencies AS l
                WHERE l.server_id = $1
                ORDER BY
                    l.database_name
                LIMIT 1
            )
            UNION ALL
            SELECT
                (
                    SELECT
                        l.database_name
                    FROM collect.object_dependencies AS l
                    WHERE l.server_id = $1
                    AND   l.database_name > databases.database_name
                    ORDER BY
                        l.database_name
                    LIMIT 1
                )
            FROM databases
            WHERE databases.database_name IS NOT NULL
        )
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
        FROM databases
        CROSS JOIN LATERAL
        (
            SELECT
                l.collection_time
            FROM collect.object_dependencies AS l
            WHERE l.server_id = $1
            AND   l.database_name = databases.database_name
            ORDER BY
                l.collection_time DESC
            LIMIT 1
        ) AS newest
        CROSS JOIN LATERAL
        (
            SELECT
                c.*
            FROM collect.object_dependencies AS c
            WHERE c.server_id = $1
            AND   c.database_name = databases.database_name
            AND   c.collection_time = newest.collection_time
            AND   COALESCE(c.referenced_database, '') <> ''
            AND   lower(c.referenced_database) = lower($2)
            AND   COALESCE(c.referenced_server, '') = ''
            /* A fence: without it the planner pulls this subquery up and, the referenced_database filter looking selective, reads the
               whole table in one sequential scan instead of each newest capture through the index. */
            OFFSET 0
        ) AS d
        WHERE databases.database_name IS NOT NULL
        AND   databases.database_name <> $2
        """;

    /// <summary>
    /// The window's baseline and the newest raw snapshot of one database. The baseline is the snapshot <see cref="ObjectUsageWindowRead.Sql"/>
    /// starts from: <see cref="ObjectUsageWindowRead.BaselineCte"/> itself, else the first snapshot at or after the start. $1 server_id, $2 database_name, $3 the window start.
    /// </summary>
    internal static readonly string BoundsSql = ObjectUsageWindowRead.BaselineCte + """

        SELECT
            (
                SELECT min(s.collection_time)
                FROM v_index_object_stats AS s
                WHERE s.server_id = $1
                AND   s.database_name = $2
                AND   s.collection_time >= COALESCE((SELECT collection_time FROM baseline), $3)
            ),
            (
                SELECT max(n.collection_time)
                FROM v_index_object_stats AS n
                WHERE n.server_id = $1
                AND   n.database_name = $2
            )
        """;

    /// <summary>The chain's per-snapshot facts of one database from the baseline to the newest snapshot, oldest first.
    /// $1 server_id, $2 database_name, $3 the baseline, $4 the newest snapshot.</summary>
    internal const string FactsSql = """
        SELECT
            f.collection_time,
            f.step_version,
            f.answering_server_name,
            f.sqlserver_start_time,
            f.compared_with,
            f.reasons,
            f.index_resets,
            f.blind_from
        FROM collect.object_usage_snapshots AS f
        WHERE f.server_id = $1
        AND   f.database_name = $2
        AND   f.collection_time >= $3
        AND   f.collection_time <= $4
        ORDER BY
            f.collection_time
        """;

    /// <summary>The stored deltas of the window summed per object, the baseline and the boundary steps left out. $1 server_id,
    /// $2 database_name, $3 the baseline, $4 the newest snapshot, $5 the boundary steps' times.</summary>
    internal const string DeltasSql = """
        SELECT
            d.object_id,
            sum(d.reads)::bigint                                               AS reads,
            COALESCE(sum(d.updates) FILTER (WHERE d.index_id <= 1), 0)::bigint AS writes,
            max(d.last_read)                                                   AS last_read,
            max(d.last_update)                                                 AS last_write
        FROM collect.object_usage_deltas AS d
        WHERE d.server_id = $1
        AND   d.database_name = $2
        AND   d.collection_time > $3
        AND   d.collection_time <= $4
        AND   d.collection_time <> ALL ($5)
        GROUP BY
            d.object_id
        """;

    /// <summary>The runs of one database that overlap the window. $1 server_id, $2 database_name, $3 the baseline, $4 the newest snapshot.</summary>
    internal const string RunsSql = """
        SELECT
            r.object_id,
            r.index_id,
            r.first_time,
            r.last_time,
            r.schema_name,
            r.object_name,
            r.is_indexed_view,
            r.is_memory_optimized
        FROM collect.object_usage_runs AS r
        WHERE r.server_id = $1
        AND   r.database_name = $2
        AND   r.first_time <= $4
        AND   (r.last_time IS NULL OR r.last_time >= $3)
        ORDER BY
            r.first_time,
            r.index_id
        """;

    /// <summary>The raw rows of the snapshots the replay recomputes: <see cref="ObjectUsageWindowRead.Columns"/>, the list <see cref="ObjectUsageWindowRead.Sql"/> selects,
    /// so <see cref="ObjectUsageWindowRead.Map"/> reads them. $1 server_id, $2 database_name, $3 the collection times.</summary>
    internal static readonly string BoundaryRowsSql = "SELECT " + ObjectUsageWindowRead.Columns + """

        FROM v_index_object_stats AS s
        WHERE s.server_id = $1
        AND   s.database_name = $2
        AND   s.collection_time = ANY ($3)
        """;

    private static ILogger? _sharedLogger;

    /// <summary>Gives the reader the host's logger, for the line it writes when it takes the full fold.</summary>
    internal static void UseSharedLogger(ILogger logger) => Interlocked.CompareExchange(ref _sharedLogger, logger, null);

    /// <summary>
    /// Reads the window (#9349). The persisted path replays the window over the chain's per-snapshot facts and sums the stored deltas, and the
    /// answer is record-equal to the full fold's. Whenever it cannot prove that, it takes the full fold (<see cref="ReadTablesFullAsync"/>) and
    /// logs the reason at Debug level.
    /// </summary>
    public static async Task<ObjectUsageWindowResult> ReadTablesAsync(
        NpgsqlDataSource postgres, int serverId, string databaseName, DateTime windowStartUtc, CancellationToken cancellationToken,
        ILogger? logger = null)
    {
        ObjectUsageWindowResult? result;
        string? reason;
        var failedStep = "";
        try
        {
            (result, reason) = await TryReadPersistedAsync(postgres, serverId, databaseName, windowStartUtc, cancellationToken, step => failedStep = step);
        }
        catch (Exception ex) when (ex is PostgresException or NpgsqlException && !cancellationToken.IsCancellationRequested)
        {
            /* A chain table that cannot be read (dropped, renamed, a permission lost) must not fail the tool: the full fold answers from the raw
               snapshots alone. The caller's own cancellation is not an error and still propagates. */
            var sqlState = (ex as PostgresException)?.SqlState ?? "none";
            reason = $"the {failedStep} statement failed (SQLSTATE {sqlState})";
            (logger ?? _sharedLogger)?.LogWarning(ex, "get_object_usage reads every snapshot of the window: {Reason}", reason);
            return await ReadTablesFullAsync(postgres, serverId, databaseName, windowStartUtc, cancellationToken);
        }

        if (result is not null)
        {
            return result;
        }

        (logger ?? _sharedLogger)?.LogDebug("get_object_usage reads every snapshot of the window: {Reason}", reason);
        return await ReadTablesFullAsync(postgres, serverId, databaseName, windowStartUtc, cancellationToken);
    }

    /// <summary>The persisted path. Null with the reason when the window cannot be answered from the chain.</summary>
    internal static async Task<(ObjectUsageWindowResult? Result, string? Reason)> TryReadPersistedAsync(
        NpgsqlDataSource postgres, int serverId, string databaseName, DateTime windowStartUtc, CancellationToken cancellationToken,
        Action<string>? onStep = null)
    {
        onStep?.Invoke("bounds");
        DateTime? baseline = null;
        DateTime? newest = null;
        await using (var command = postgres.CreateCommand(BoundsSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            DarlingMcpReadParameters.AddText(command, databaseName);
            DarlingMcpReadParameters.AddTimestamp(command, windowStartUtc);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                baseline = reader.IsDBNull(0) ? null : reader.GetDateTime(0);
                newest = reader.IsDBNull(1) ? null : reader.GetDateTime(1);
            }
        }

        if (baseline is not { } from || newest is not { } to)
        {
            return (null, "the window holds no snapshot");
        }

        var facts = new List<ObjectUsageSnapshotFact>();
        onStep?.Invoke("facts");
        await using (var command = postgres.CreateCommand(FactsSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            DarlingMcpReadParameters.AddText(command, databaseName);
            DarlingMcpReadParameters.AddTimestamp(command, from);
            DarlingMcpReadParameters.AddTimestamp(command, to);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                facts.Add(new ObjectUsageSnapshotFact(
                    reader.GetDateTime(0),
                    Convert.ToInt16(reader.GetValue(1), CultureInfo.InvariantCulture),
                    reader.IsDBNull(2) ? null : reader.GetString(2),
                    reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                    reader.IsDBNull(4) ? null : reader.GetDateTime(4),
                    (ObjectUsageResetReasons)Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture),
                    Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture),
                    reader.IsDBNull(7) ? null : reader.GetDateTime(7)));
            }
        }

        var replay = ObjectUsageWindowReplay.Run(facts, from, to, ObjectUsageChainVersion.Current);
        if (replay.FallbackReason is not null)
        {
            return (null, replay.FallbackReason);
        }

        var boundaryTimes = replay.Boundaries.Select(b => b.Time).ToArray();
        var deltas = new List<ObjectUsageDeltaTotals>();
        onStep?.Invoke("deltas");
        await using (var command = postgres.CreateCommand(DeltasSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            DarlingMcpReadParameters.AddText(command, databaseName);
            DarlingMcpReadParameters.AddTimestamp(command, from);
            DarlingMcpReadParameters.AddTimestamp(command, to);
            AddTimestampArray(command, boundaryTimes);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                deltas.Add(new ObjectUsageDeltaTotals(
                    Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture),
                    reader.GetInt64(1),
                    reader.GetInt64(2),
                    reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                    reader.IsDBNull(4) ? null : reader.GetDateTime(4)));
            }
        }

        var runs = new List<ObjectUsageRun>();
        onStep?.Invoke("runs");
        await using (var command = postgres.CreateCommand(RunsSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            DarlingMcpReadParameters.AddText(command, databaseName);
            DarlingMcpReadParameters.AddTimestamp(command, from);
            DarlingMcpReadParameters.AddTimestamp(command, to);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                runs.Add(new ObjectUsageRun(
                    Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture),
                    Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture),
                    reader.GetDateTime(2),
                    reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                    reader.IsDBNull(4) ? "" : reader.GetString(4),
                    reader.IsDBNull(5) ? "" : reader.GetString(5),
                    reader.GetBoolean(6),
                    reader.IsDBNull(7) ? null : reader.GetBoolean(7)));
            }
        }

        var raw = new List<ObjectUsageSample>();
        onStep?.Invoke("boundary rows");
        await using (var command = postgres.CreateCommand(BoundaryRowsSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            DarlingMcpReadParameters.AddText(command, databaseName);
            AddTimestampArray(command, replay.RawTimes.ToArray());
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                raw.Add(ObjectUsageWindowRead.Map(reader));
            }
        }

        var result = replay.Assemble(deltas, runs, raw, out var reason);
        return (result, reason);
    }

    /// <summary>Binds a naive-UTC <c>timestamp[]</c>.</summary>
    private static void AddTimestampArray(NpgsqlCommand command, DateTime[] values) =>
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp,
            Value = values.Select(v => DateTime.SpecifyKind(v, DateTimeKind.Unspecified)).ToArray(),
        });

    /// <summary>The full fold: every index row of every snapshot in the window through <see cref="ObjectUsageWindow"/>. Lite's read, and the
    /// persisted path's fallback.</summary>
    internal static async Task<ObjectUsageWindowResult> ReadTablesFullAsync(
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
            DarlingMcpReadParameters.AddText(command, PerformanceMonitor.Analysis.ObjectReferences.ObjectReferenceAggregator.FoldName(databaseName));
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
