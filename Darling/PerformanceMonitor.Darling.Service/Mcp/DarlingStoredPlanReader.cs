/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Service-side stored-plan reads for the plan-analysis MCP tools — the SAME stored-plan-XML the
/// collectors persist alongside the query/query-store/procedure stats (query_stats.query_plan_xml,
/// query_store_stats.query_plan_text, procedure_stats.query_plan_xml; captured when
/// <c>CollectorContext.CapturePlanXml</c> is set — Darling sets it true, TOAST compresses the text).
///
/// <para>
/// This mirrors the viewer's <c>ViewerDataService.Plans.cs</c> reads (adapted DuckDB/Dashboard →
/// Postgres) but lives service-side so the MCP host never touches the WPF viewer project: the MCP
/// server reads the STORE exactly like <c>analyze_server</c> does, never the live monitored SQL
/// Server — consistent with Darling's read-only-from-collected-data posture (contrast
/// <c>PgPlanFetcher</c>, which the analysis engine uses to pull a live plan from the cache on demand).
/// </para>
///
/// <para>
/// Each read keys on server_id + the object identity the Dashboard's McpPlanTools use (query_hash /
/// sql_handle / query_id) so the MCP contract matches the Dashboard's. Two of the reads accept an
/// OPTIONAL finer key the Postgres store carries (database_name for the query-stats read, plan_id for
/// the query-store read); when null the read behaves exactly like the Dashboard's (most-recently
/// collected plan for the coarse key), and when supplied it pins the exact row the viewer's grids key
/// on. <c>ORDER BY collection_time DESC LIMIT 1</c> returns the most recently captured plan; the
/// <c>IS NOT NULL</c> guard skips rows where no plan was captured. If a change here alters a stored
/// column, the viewer's twin read must move with it.
/// </para>
///
/// <para><b>The oversized-plan fallback (#3392).</b> A plan whose XML measured over
/// <c>QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes</c> ships NULL for content, so it produces no
/// plan-dimension row and the <c>IS NOT NULL</c> guards above skip it — correctly, since there is nothing
/// there. When every collected row for a key is in that state the primary read returns nothing, and the
/// answer is in <c>collect.oversized_plan_backlog</c> instead: the same plan, fetched later by the backlog
/// sweep on its own connection. So each stored-plan read here tries the collected content first and the
/// backlog second, and a caller sees one answer either way. Without this half nothing a user can see changes
/// — the recording half alone just moves the blind spot into a table.</para>
/// </summary>
internal static class DarlingStoredPlanReader
{
    /// <summary>
    /// The latest captured query_stats plan for a query, keyed by (server, query_hash) with an OPTIONAL
    /// database filter — the Dashboard's GetPlanXmlByQueryHashAsync keys on query_hash alone (most recent
    /// wins); the $3 null-guard adds the viewer's (database, query_hash) precision when the caller knows the
    /// database. $1 server_id, $2 query_hash, $3 database_name (NULL = no database filter).
    /// </summary>
    public const string QueryStatsPlanXmlByHashSql = """
        SELECT query_plan_xml, query_plan_gz
        FROM v_query_stats
        WHERE server_id = $1
        AND   query_hash = $2
        AND   ($3::text IS NULL OR database_name = $3)
        /* #2069: plans written since V54 live as gzip bytes (query_plan_gz) with the text column
           NULL, so the presence guard and the projection must carry BOTH forms — the C# side
           resolves text-else-gz (PayloadDimensions.ResolveContent). */
        AND   (query_plan_xml IS NOT NULL OR query_plan_gz IS NOT NULL)
        ORDER BY collection_time DESC
        LIMIT 1
        """;

    /// <summary>
    /// The latest captured procedure_stats plan for a procedure, keyed by (server, sql_handle) — the
    /// Dashboard's GetProcedurePlanXmlBySqlHandleAsync key. procedure_stats carries sql_handle as the
    /// '0x...' hex string the collector stamps (CONVERT(varchar(130), ..., 1)), so the match is a direct
    /// text compare (no varbinary CONVERT the SQL-Server side needs). There is no v_procedure_stats to
    /// resolve the #1767 plan dimension, so this read joins it itself. $1 server_id, $2 sql_handle.
    /// </summary>
    public const string ProcedurePlanXmlBySqlHandleSql = """
        SELECT COALESCE(ps.query_plan_xml, qpd.query_plan_xml), qpd.query_plan_gz
        FROM procedure_stats AS ps
        LEFT JOIN query_plan_dim AS qpd
          ON qpd.digest = ps.query_plan_digest
        WHERE ps.server_id = $1
        AND   ps.sql_handle = $2
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
    /// The unbounded statement plus exactly one predicate, <c>AND ps.collection_time &gt; $3</c>, for a handle the
    /// module map does not know while the map is fresh. A bare, un-OR'd comparison on the hypertable's time
    /// column lets the planner exclude chunks older than the bound. $1 server_id, $2 sql_handle, $3 the bound
    /// (<c>timestamp</c>).
    /// </summary>
    public static readonly string ProcedurePlanXmlBySqlHandleBoundedSql = ProcedurePlanXmlBySqlHandleSql.Replace(
        "ORDER BY ps.collection_time DESC",
        "AND   ps.collection_time > $3\nORDER BY ps.collection_time DESC", StringComparison.Ordinal);

    /// <summary>
    /// Whether the handle is mapped for the server and how far the map reaches. The server name is the one the
    /// map's refresh writes: <see cref="DarlingModuleMap.RefreshSql"/> copies <c>server_name</c> from
    /// procedure_stats, so the newest procedure_stats row for the server_id is the same spelling. $1 server_id,
    /// $2 sql_handle. <c>known</c> is NULL and <c>horizon</c> is NULL when the map holds nothing for the server.
    /// </summary>
    public const string ModuleMapProbeSql = """
        WITH sn AS (SELECT server_name FROM collect.procedure_stats
                    WHERE server_id = $1 ORDER BY collection_time DESC LIMIT 1)
        SELECT bool_or(m.sql_handle = $2) AS known, max(m.last_seen) AS horizon
        FROM collect.module_map AS m
        JOIN sn USING (server_name)
        """;

    /// <summary>
    /// A module map whose newest entry is older than this is treated as not refreshing: the refresh reads the
    /// last two days of procedure_stats, so a horizon older than that means rows newer than the horizon may
    /// never have been mapped, and a miss cannot be bounded safely.
    /// </summary>
    public static readonly TimeSpan ModuleMapStaleAfter = TimeSpan.FromDays(2);

    /// <summary>
    /// Margin below the map's horizon for the bounded scan, covering rows that committed after the refresh with
    /// a collection_time a little below the horizon.
    /// </summary>
    public static readonly TimeSpan ModuleMapHorizonMargin = TimeSpan.FromHours(1);

    private static readonly ConcurrentDictionary<int, byte> s_staleMapLogged = new();

    /// <summary>Test seam: forgets which servers already logged the stale-map message.</summary>
    internal static void ResetStaleMapLogForTests() => s_staleMapLogged.Clear();

    /// <summary>
    /// The latest captured query_store_stats plan for a query, keyed by (server, database, query_id) with an
    /// OPTIONAL plan_id filter — the Dashboard's GetQueryStorePlanXmlAsync keys on (database, query_id); the
    /// $4 null-guard adds the viewer's (query_id, plan_id) precision (a plan_id names one specific compiled
    /// plan) when the caller knows it. $1 server_id, $2 database_name, $3 query_id, $4 plan_id (NULL = latest
    /// plan for the query).
    /// </summary>
    public const string QueryStorePlanTextSql = """
        SELECT query_plan_text
        FROM query_store_stats
        WHERE server_id = $1
        AND   database_name = $2
        AND   query_id = $3
        AND   ($4::bigint IS NULL OR plan_id = $4)
        AND   query_plan_text IS NOT NULL
        ORDER BY collection_time DESC
        LIMIT 1
        """;

    /// <summary>
    /// The stored execution plan XML for a query (query_stats), or null when no plan was captured for the
    /// key. Read as text — no length cap (the collector stored the whole plan; the MCP tool truncates for
    /// transport).
    /// </summary>
    public static async Task<string?> GetQueryStatsPlanXmlByHashAsync(
        NpgsqlDataSource postgres, int serverId, string queryHash, string? databaseName,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(queryHash))
        {
            return null;
        }

        await using (var command = postgres.CreateCommand(QueryStatsPlanXmlByHashSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = queryHash });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)databaseName ?? DBNull.Value });

            var captured = await ReadPlanTextOrGzipAsync(command, cancellationToken);
            if (captured is not null)
            {
                return captured;
            }
        }

        /* #3392: nothing collected for this key carries a plan. That is also what an over-cap plan looks
           like from here, so ask the backlog — the same statement's plan, fetched later out of band. Second,
           never first: collected content is the fresher of the two, and the backlog is keyed on a handle
           that only ever held one document. */
        await using var backlog = postgres.CreateCommand(OversizedPlanBacklog.QueryStatsFallbackSql);
        backlog.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        backlog.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        backlog.Parameters.Add(new NpgsqlParameter<string> { TypedValue = queryHash });
        backlog.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)databaseName ?? DBNull.Value });
        return await ReadBacklogPlanXmlAsync(backlog, cancellationToken);
    }

    /// <summary>
    /// The stored execution plan XML for a procedure (procedure_stats), or null when no plan was captured
    /// for the sql_handle.
    /// </summary>
    public static async Task<string?> GetProcedurePlanXmlBySqlHandleAsync(
        NpgsqlDataSource postgres, int serverId, string sqlHandle,
        CancellationToken cancellationToken = default, ILogger? logger = null)
    {
        if (string.IsNullOrEmpty(sqlHandle))
        {
            return null;
        }

        /* An absent handle used to scan the server's whole raw history. The module map is keyed (server,
           handle): a mapped handle, an empty map, or a map that has stopped refreshing keeps the unbounded
           read; otherwise the handle can only be newer than the map's horizon, so the scan starts there. */
        DateTime? bound = null;
        var known = false;
        DateTime? horizon = null;
        try
        {
            await using var probe = postgres.CreateCommand(ModuleMapProbeSql);
            probe.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            probe.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            probe.Parameters.Add(new NpgsqlParameter<string> { TypedValue = sqlHandle });
            await using var probeReader = await probe.ExecuteReaderAsync(cancellationToken);
            if (await probeReader.ReadAsync(cancellationToken))
            {
                known = !probeReader.IsDBNull(0) && probeReader.GetBoolean(0);
                horizon = probeReader.IsDBNull(1) ? null : probeReader.GetDateTime(1);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* The map is runtime setup and may not exist: the unbounded read is always correct. */
            known = false;
            horizon = null;
            System.Diagnostics.Trace.TraceWarning($"#4954 module_map probe failed, reading unbounded: {ex.GetType().Name}: {ex.Message}");
        }

        if (!known && horizon is { } h)
        {
            var nowUtc = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            if (nowUtc - h > ModuleMapStaleAfter)
            {
                if (s_staleMapLogged.TryAdd(serverId, 0))
                {
                    var message = $"#4954 module_map refresh looks stale for server {serverId} (newest entry {h:u}); the bounded procedure-plan miss is off and an absent sql_handle scans the full history.";
                    if (logger is not null)
                    {
                        logger.LogWarning("{Message}", message);
                    }
                    else
                    {
                        System.Diagnostics.Trace.TraceWarning(message);
                    }
                }
            }
            else
            {
                bound = h - ModuleMapHorizonMargin;
            }
        }

        await using (var command = postgres.CreateCommand(bound is null ? ProcedurePlanXmlBySqlHandleSql : ProcedurePlanXmlBySqlHandleBoundedSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = sqlHandle });
            if (bound is { } b)
            {
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(b, DateTimeKind.Unspecified) });
            }

            var captured = await ReadPlanTextOrGzipAsync(command, cancellationToken);
            if (captured is not null)
            {
                return captured;
            }
        }

        /* #3392, the same second look as the query_stats read above — a module-grain plan is the shape most
           likely to cross the capture cap, so this arm carries more of the population than its sibling. */
        await using var backlog = postgres.CreateCommand(OversizedPlanBacklog.ProcedureStatsFallbackBySqlHandleSql);
        backlog.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        backlog.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        backlog.Parameters.Add(new NpgsqlParameter<string> { TypedValue = sqlHandle });
        return await ReadBacklogPlanXmlAsync(backlog, cancellationToken);
    }

    /// <summary>
    /// The stored Query Store execution plan for a query (query_store_stats.query_plan_text — Query Store
    /// already stores plans as ShowPlanXML text), or null when no plan text was captured for the key.
    /// </summary>
    public static async Task<string?> GetQueryStorePlanTextAsync(
        NpgsqlDataSource postgres, int serverId, string databaseName, long queryId, long? planId,
        CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(QueryStorePlanTextSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName ?? "" });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = queryId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = (object?)planId ?? DBNull.Value });
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is string s ? s : null;
    }

    /// <summary>
    /// Executes a one-column backlog read (#3392). Plain text, never gzip: the backlog holds the content the
    /// sweep fetched, and it does not route through the plan dimension — a capped row produced no dim row to
    /// compress into.
    /// </summary>
    private static async Task<string?> ReadBacklogPlanXmlAsync(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is string plan ? plan : null;
    }

    /// <summary>
    /// Executes a two-column (text, gzip bytea) plan read and resolves the form the row carries —
    /// the #2069 read seam shared by both dimension-resolving reads above. One row max (LIMIT 1).
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
