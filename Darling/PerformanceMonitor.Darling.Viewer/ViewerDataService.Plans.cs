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

/// <summary>
/// On-demand execution-plan reads for the Plan Viewer host (headless-plan wave): the plan XML the
/// Darling service captures alongside the query/query-store/procedure stats (PR #1349 for query &amp;
/// query-store, #1368 / V7 for procedure_stats, gated on <c>CollectorContext.CapturePlanXml</c> — Darling
/// sets it true, TOAST compresses the text). Unlike Lite — which fetches plans live from the monitored
/// server's plan cache — the viewer never touches SQL Server, so it reads the stored plan text straight out
/// of Postgres by the grid row's key columns. The keyed grids (Top Queries / Query Store / Top Procedures)
/// carry only a cheap presence flag or fetch on click (no multi-KB XML per row); the blocked-process /
/// deadlock grids instead carry their best-effort plan XML in-row (see ViewerDataService.Blocking.cs /
/// .Deadlock.cs), like they already carry the report / graph XML. These keyed lookups are keyed (server +
/// object identity), not windowed, so there is no timestamp parameter and no naive-UTC concern —
/// <c>ORDER BY collection_time DESC LIMIT 1</c> returns the most recently captured plan for that key. Plans
/// can be large; they are read as text with no size games (the task's rule: TOAST handles storage, reading
/// is fine).
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>
    /// The latest captured query_stats plan for a Top-Queries row, keyed by (server, database, query_hash) —
    /// the grid groups by (database_name, query_hash), so the same key fetches the plan the row represents.
    /// $1 server_id, $2 database_name, $3 query_hash.
    /// </summary>
    public const string QueryStatsPlanXmlSql = """
        SELECT query_plan_xml, query_plan_gz
        FROM v_query_stats
        WHERE server_id = $1
        AND   database_name = $2
        AND   query_hash = $3
        /* #2069: plans written since V54 live as gzip bytes (query_plan_gz) with the text column
           NULL, so the presence guard and the projection must carry BOTH forms — the C# side
           resolves text-else-gz (PayloadDimensions.ResolveContent). */
        AND   (query_plan_xml IS NOT NULL OR query_plan_gz IS NOT NULL)
        ORDER BY collection_time DESC
        LIMIT 1
        """;

    /// <summary>
    /// The stored execution plan for a Top-Queries grid row, or null when no plan was captured for that
    /// (database, query_hash). Read as text — no length cap (the collector stored the whole plan).
    /// </summary>
    public async Task<string?> GetQueryStatsPlanXmlAsync(
        int serverId, string databaseName, string queryHash, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(queryHash))
        {
            return null;
        }

        await using var command = _dataSource.CreateCommand(QueryStatsPlanXmlSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName ?? "" });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = queryHash });
        return await ReadPlanTextOrGzipAsync(command, cancellationToken);
    }

    /// <summary>
    /// The latest captured query_store_stats plan for a Query-Store row, keyed by (server, database,
    /// query_id, plan_id) — a Query Store plan_id names one specific compiled plan, so (query_id, plan_id)
    /// uniquely identifies it. $1 server_id, $2 database_name, $3 query_id, $4 plan_id.
    /// </summary>
    public const string QueryStorePlanTextSql = """
        SELECT query_plan_text
        FROM query_store_stats
        WHERE server_id = $1
        AND   database_name = $2
        AND   query_id = $3
        AND   plan_id = $4
        AND   query_plan_text IS NOT NULL
        ORDER BY collection_time DESC
        LIMIT 1
        """;

    /// <summary>
    /// The stored Query Store execution plan for a grid row, or null when no plan text was captured for
    /// that (database, query_id, plan_id). Query Store already stores plans as ShowPlanXML text.
    /// </summary>
    public async Task<string?> GetQueryStorePlanTextAsync(
        int serverId, string databaseName, long queryId, long planId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(QueryStorePlanTextSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName ?? "" });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = queryId });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = planId });
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is string s ? s : null;
    }

    /// <summary>
    /// The latest captured procedure_stats plan for a Top-Procedures row, keyed by (server, database,
    /// schema, object) — the grid groups by (database_name, schema_name, object_name, object_type), so the
    /// same object identity fetches the plan the row represents (Top Queries' keyed-fetch pattern, not the
    /// in-row carry the aggregate grid can't do). procedure_stats.query_plan_xml is captured CLEANLY from the
    /// cached plan (dm_exec_procedure_stats.plan_handle → dm_exec_text_query_plan) whenever the host sets
    /// CapturePlanXml — Darling does, Lite always writes NULL. Read from the base <c>procedure_stats</c>
    /// table (there is no v_procedure_stats view; TopProceduresSql already reads the base table), which also
    /// avoids the V7-column / pinned-view issue the blocked/deadlock reads sidestep — so unlike the
    /// query_stats twin above, this read resolves the #1767 plan dimension itself. $1 server_id, $2
    /// database_name, $3 schema_name, $4 object_name.
    /// </summary>
    public const string ProcedureStatsPlanXmlSql = """
        SELECT COALESCE(ps.query_plan_xml, qpd.query_plan_xml), qpd.query_plan_gz
        FROM procedure_stats AS ps
        LEFT JOIN query_plan_dim AS qpd
          ON qpd.digest = ps.query_plan_digest
        WHERE ps.server_id = $1
        AND   ps.database_name = $2
        AND   ps.schema_name = $3
        AND   ps.object_name = $4
        /* The guard rides the COALESCED expression, not the bare inline column: rows written since
           #1767 leave query_plan_xml NULL and carry the plan in the dimension, so testing the inline
           column would discard every new row before the join could resolve it. The gz arm (#2069)
           extends the same reasoning one step: dim rows written since V54 carry gzip bytes with the
           dim TEXT column NULL too, so the coalesced text alone would discard every post-V54 plan. */
        AND   (COALESCE(ps.query_plan_xml, qpd.query_plan_xml) IS NOT NULL OR qpd.query_plan_gz IS NOT NULL)
        ORDER BY ps.collection_time DESC
        LIMIT 1
        """;

    /// <summary>
    /// The stored execution plan for a Top-Procedures grid row, or null when no plan was captured for that
    /// (database, schema, object). Read as text — no length cap (the collector stored the whole plan).
    /// </summary>
    public async Task<string?> GetProcedureStatsPlanXmlAsync(
        int serverId, string databaseName, string schemaName, string objectName, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(objectName))
        {
            return null;
        }

        await using var command = _dataSource.CreateCommand(ProcedureStatsPlanXmlSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName ?? "" });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = schemaName ?? "" });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = objectName });
        return await ReadPlanTextOrGzipAsync(command, cancellationToken);
    }

    /// <summary>
    /// Executes a two-column (text, gzip bytea) plan read and resolves the form the row carries —
    /// the #2069 read seam shared by both plan-dimension reads above. One row max (LIMIT 1).
    /// </summary>
    private static async Task<string?> ReadPlanTextOrGzipAsync(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return PayloadDimensions.ResolveContent(
            reader.IsDBNull(0) ? null : reader.GetString(0),
            reader.IsDBNull(1) ? null : reader.GetFieldValue<byte[]>(1));
    }
}
