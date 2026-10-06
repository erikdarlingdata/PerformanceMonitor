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
    /// The stored estimated execution plan for one Active Queries snapshot row, keyed by its natural key
    /// (server, collection_time, session_id, request_id). BYTE-EQUAL to the Viewer's
    /// <c>ViewerDataService.QuerySnapshotEstimatedPlanSql</c> (a test pins the equality), so the desktop and
    /// the web fetch the same row the same way. <c>COALESCE(request_id, 0)</c> matches a row collected with
    /// no request_id, which the grids carry as 0. $1 server_id, $2 collection_time (naive UTC,
    /// microsecond-exact), $3 session_id, $4 request_id.
    /// </summary>
    public const string QuerySnapshotPlanSql = """
        SELECT query_plan
        FROM query_snapshots
        WHERE server_id = $1
        AND   collection_time = $2
        AND   session_id = $3
        AND   COALESCE(request_id, 0) = $4
        AND   query_plan IS NOT NULL
        LIMIT 1
        """;

    /// <summary>The live/actual plan twin of <see cref="QuerySnapshotPlanSql"/>; byte-equal to the Viewer's
    /// <c>ViewerDataService.QuerySnapshotLivePlanSql</c>.</summary>
    public const string QuerySnapshotLivePlanSql = """
        SELECT live_query_plan
        FROM query_snapshots
        WHERE server_id = $1
        AND   collection_time = $2
        AND   session_id = $3
        AND   COALESCE(request_id, 0) = $4
        AND   live_query_plan IS NOT NULL
        LIMIT 1
        """;

    /// <summary>
    /// The plan the BLOCKED process of one blocked-process report ran under (#5236), by the report's own key as
    /// get_blocking lists it: (server, event_time, both spids, both ecids). The plan is the V7 inline text column the
    /// collector stamps with the report, best-effort (a plan that had left the cache by then is NULL), and no plan
    /// dimension or backlog is involved. The presence predicate is <c>IS NOT NULL AND &lt;&gt; ''</c> on purpose and
    /// is the SAME predicate the list's <c>has_blocked_plan</c> flag uses, so a flagged row can never answer "no plan".
    /// <para>
    /// No key makes the report unique, so the same event can be stored as more than one copy, and only some copies carry
    /// a plan. This takes the EARLIEST copy that has one, ties broken by <c>blocked_report_id</c>: the collector resolves
    /// the plan from the live plan cache when it collects the report, so the copy collected nearest the event is the
    /// one whose plan was most likely still cached. It is not <c>DESC</c>. $7 is the
    /// <see cref="EventWindowFloor"/> for the event_time, a partition-column bound with no upper limit, so a
    /// late-collected report is still found while the chunks older than the event are never opened.
    /// </para>
    /// <para>
    /// The answer is per EVENT, while the list's presence flag is per stored copy. Two copies of one event can carry
    /// different plans (a recompile between the two collections), and then both rows show a button and both open the
    /// earlier copy's plan, where the desktop viewer shows each row its own copy's plan. The web page gives those rows
    /// one panel key, so opening one opens both. That is accepted: the earlier copy is the one whose plan was most likely
    /// still cached when the event ran, and nothing here tells the two copies apart.
    /// </para>
    /// <para>
    /// An Azure master target collects several databases under one server_id, and each database numbers its own
    /// sessions, so two databases' reports can share every other part of the key. The optional $8 narrows the read to the
    /// row's own database, in the same NULL-or-equal form as the other optional database filters in this class; a caller
    /// that sends none gets the answer it got before the filter existed.
    /// </para>
    /// $1 server_id, $2 event_time (naive UTC, microsecond-exact), $3 blocked_spid, $4 blocked_ecid, $5 blocking_spid,
    /// $6 blocking_ecid, $7 collection_time floor, $8 database_name (NULL = no database filter). Byte-equal to
    /// <see cref="BlockingPlanSql"/> except for the column.
    /// </summary>
    public const string BlockedPlanSql = """
        SELECT blocked_query_plan_xml
        FROM blocked_process_reports
        WHERE server_id = $1
        AND   event_time = $2
        AND   blocked_spid = $3
        AND   blocked_ecid = $4
        AND   blocking_spid = $5
        AND   blocking_ecid = $6
        AND   collection_time >= $7
        AND   ($8::text IS NULL OR database_name = $8)
        AND   blocked_query_plan_xml IS NOT NULL
        AND   blocked_query_plan_xml <> ''
        ORDER BY collection_time, blocked_report_id
        LIMIT 1
        """;

    /// <summary>The BLOCKING process's plan for the same report key: <see cref="BlockedPlanSql"/> over
    /// <c>blocking_query_plan_xml</c> (a test pins that the two differ only in the column).</summary>
    public const string BlockingPlanSql = """
        SELECT blocking_query_plan_xml
        FROM blocked_process_reports
        WHERE server_id = $1
        AND   event_time = $2
        AND   blocked_spid = $3
        AND   blocked_ecid = $4
        AND   blocking_spid = $5
        AND   blocking_ecid = $6
        AND   collection_time >= $7
        AND   ($8::text IS NULL OR database_name = $8)
        AND   blocking_query_plan_xml IS NOT NULL
        AND   blocking_query_plan_xml <> ''
        ORDER BY collection_time, blocked_report_id
        LIMIT 1
        """;

    /// <summary>
    /// The victim's plan for one deadlock (#5236), by the row's own (collection_time, deadlock_time) as get_deadlocks
    /// lists them. That pair is NOT unique (no key enforces it, and one monitor pass can report two deadlocks whose
    /// stamps are equal to the millisecond), so the optional <c>victim_process_id</c> narrows it to the one the row
    /// names, and the optional <c>database_name</c> to the row's own database (the same reason, and the same
    /// NULL-or-equal form, as <see cref="BlockedPlanSql"/>'s $8). The presence predicate matches the list's
    /// <c>has_victim_plan</c> flag (see <see cref="BlockedPlanSql"/>). A NULL deadlock_time never lists, so it is never
    /// asked for here.
    /// <para>
    /// It reads up to TWO rows, <c>deadlock_id</c> first, and selects <c>victim_process_id</c> beside the plan so the
    /// caller can tell whether the one it takes was the only candidate. When no victim is sent and the two rows name
    /// different victims, the stamps do not say which deadlock was meant, and the plan of the wrong deadlock is worse
    /// than none, so <see cref="GetDeadlockVictimPlanXmlAsync"/> reports the read as ambiguous and takes neither. Two
    /// rows that name the same victim are copies of one deadlock, and the lower <c>deadlock_id</c> answers.
    /// </para>
    /// $1 server_id, $2 collection_time, $3 deadlock_time (both naive UTC, microsecond-exact), $4 victim_process_id
    /// (NULL = no victim filter), $5 database_name (NULL = no database filter).
    /// </summary>
    public const string DeadlockVictimPlanSql = """
        SELECT victim_query_plan_xml, victim_process_id
        FROM deadlocks
        WHERE server_id = $1
        AND   collection_time = $2
        AND   deadlock_time = $3
        AND   ($4::text IS NULL OR victim_process_id = $4)
        AND   ($5::text IS NULL OR database_name = $5)
        AND   victim_query_plan_xml IS NOT NULL
        AND   victim_query_plan_xml <> ''
        ORDER BY deadlock_id
        LIMIT 2
        """;

    /// <summary>
    /// The Query Store plan for ONE plan_id as it is stored since #2210: the fact rows carry no plan text (the
    /// collector ships a NULL placeholder), and a plan lives ONCE in <c>query_plan_dim</c>, reached through
    /// <c>collect.query_store_plan_map</c> on its primary key (server_id, database_name, plan_id). Two index
    /// lookups, no fact-table scan: a plan_id belongs to exactly one query_id in its database, so the fact
    /// table adds nothing once the plan_id is known.
    /// <para>
    /// The dimension join is a LEFT join on purpose, so the read distinguishes the two ways of coming back
    /// empty. NO row means the map has never heard of the plan (a pre-cutover plan, or one not yet fetched),
    /// and the caller falls back to the inline column. A row whose columns are NULL means the map knows the
    /// plan and has nothing to show: a NULL digest is the content-less marker for a plan the engine could
    /// not persist, and a digest whose dimension row the dimension GC removed reads the same. That plan is
    /// absent, and the inline column is not asked, because a post-cutover plan has nothing there.
    /// </para>
    /// The dimension row carries text, gzip bytes or (for the oldest rows) text only, so both columns come
    /// back and the C# side resolves text-else-gz. $1 server_id, $2 database_name, $3 plan_id.
    /// </summary>
    public const string QueryStorePlanViaMapSql = """
        SELECT d.query_plan_xml, d.query_plan_gz
        FROM collect.query_store_plan_map AS m
        LEFT JOIN query_plan_dim AS d
          ON d.digest = m.digest
        WHERE m.server_id = $1
        AND   m.database_name = $2
        AND   m.plan_id = $3
        """;

    /// <summary>
    /// The plan_ids a query ran under in the last <see cref="UnpinnedLookbackDays"/> days, newest first (ties
    /// broken by plan_id so the answer is deterministic). Used when the caller gave no plan_id: each candidate
    /// is then resolved through <see cref="QueryStorePlanViaMapSql"/>. The time bound is what keeps this off
    /// the cold history: <c>collection_time</c> is the hypertable's partitioning column, so chunk pruning keeps
    /// the read on the newest chunks, and <c>idx_query_store_stats_server_db_query_plan_time</c> (server_id,
    /// database_name, query_id, plan_id, collection_time) answers it there. That index covers the uncompressed
    /// chunks only (a compressed chunk keeps an empty index shell, see PgTableTuning), so an unbounded read
    /// would decompress every batch of the server. $1 server_id, $2 database_name, $3 query_id, $4 the
    /// lower bound on collection_time.
    /// </summary>
    public const string QueryStorePlanCandidatesSql = """
        SELECT plan_id, MAX(collection_time) AS last_collected
        FROM query_store_stats
        WHERE server_id = $1
        AND   database_name = $2
        AND   query_id = $3
        AND   collection_time >= $4
        AND   plan_id IS NOT NULL
        GROUP BY plan_id
        ORDER BY last_collected DESC, plan_id DESC
        """;

    /// <summary>
    /// The same candidates with no time bound. Run ONCE, and only when the bounded pass resolved nothing: a
    /// query that has not run in <see cref="UnpinnedLookbackDays"/> days still has a plan worth showing, and
    /// this is the old behaviour for it. $1 server_id, $2 database_name, $3 query_id.
    /// </summary>
    public const string QueryStorePlanCandidatesUnboundedSql = """
        SELECT plan_id, MAX(collection_time) AS last_collected
        FROM query_store_stats
        WHERE server_id = $1
        AND   database_name = $2
        AND   query_id = $3
        AND   plan_id IS NOT NULL
        GROUP BY plan_id
        ORDER BY last_collected DESC, plan_id DESC
        """;

    /// <summary>
    /// How far back the unpinned Query Store plan read looks before it gives up on the newest chunks.
    /// </summary>
    public const int UnpinnedLookbackDays = 7;

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
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(sqlHandle))
        {
            return null;
        }

        await using (var command = postgres.CreateCommand(ProcedurePlanXmlBySqlHandleSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = sqlHandle });

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
    /// The stored Query Store execution plan for a query, or null when none was captured for the key. See
    /// <see cref="ResolveQueryStorePlanAsync"/> for how the plan is found; this keeps only the text.
    /// </summary>
    public static async Task<string?> GetQueryStorePlanTextAsync(
        NpgsqlDataSource postgres, int serverId, string databaseName, long queryId, long? planId,
        DateTime? asOf = null, CancellationToken cancellationToken = default)
    {
        var read = await ResolveQueryStorePlanAsync(postgres, serverId, databaseName, queryId, planId, asOf, cancellationToken);
        return read?.PlanXml;
    }

    /// <summary>
    /// The stored Query Store plan for a query and the plan_id it belongs to, or null when none was captured.
    /// Since #2210 a plan is stored once in <c>query_plan_dim</c> and reached through
    /// <c>collect.query_store_plan_map</c> (<see cref="QueryStorePlanViaMapSql"/>); rows written before that
    /// carry the text inline on <c>query_store_stats.query_plan_text</c> (<see cref="QueryStorePlanTextSql"/>).
    /// <para>
    /// <b>Pinned</b> (a plan_id is given): the map is read by its primary key. The inline column is read ONLY
    /// when the map has no row for that plan_id. A map row without content (a NULL-digest marker, or a digest
    /// whose dimension row is gone) is absent, with no second look.
    /// </para>
    /// <para>
    /// <b>Unpinned</b>: the plan_ids the query ran under in the last <see cref="UnpinnedLookbackDays"/> days
    /// (counted back from <paramref name="asOf"/>, default now), newest first, each resolved as above until one
    /// has content. Only if nothing resolves inside the bound does ONE unbounded pass run, for a query that
    /// has not run for longer than that. The bound is what keeps the read on the newest hypertable chunks; see
    /// <see cref="QueryStorePlanCandidatesSql"/>.
    /// </para>
    /// </summary>
    public static async Task<QueryStorePlanRead?> ResolveQueryStorePlanAsync(
        NpgsqlDataSource postgres, int serverId, string databaseName, long queryId, long? planId,
        DateTime? asOf = null, CancellationToken cancellationToken = default)
    {
        databaseName ??= "";

        if (planId is long pinned)
        {
            var plan = await ReadQueryStorePlanByIdAsync(postgres, serverId, databaseName, queryId, pinned, cancellationToken);
            return plan is null ? null : new QueryStorePlanRead(plan, pinned);
        }

        var since = DateTime.SpecifyKind((asOf ?? DateTime.UtcNow).AddDays(-UnpinnedLookbackDays), DateTimeKind.Unspecified);
        var tried = new HashSet<long>();
        foreach (var candidate in await ReadCandidatePlanIdsAsync(postgres, QueryStorePlanCandidatesSql, serverId, databaseName, queryId, since, cancellationToken))
        {
            tried.Add(candidate);
            var plan = await ReadQueryStorePlanByIdAsync(postgres, serverId, databaseName, queryId, candidate, cancellationToken);
            if (plan is not null)
            {
                return new QueryStorePlanRead(plan, candidate);
            }
        }

        foreach (var candidate in await ReadCandidatePlanIdsAsync(postgres, QueryStorePlanCandidatesUnboundedSql, serverId, databaseName, queryId, since: null, cancellationToken))
        {
            if (!tried.Add(candidate))
            {
                continue;
            }

            var plan = await ReadQueryStorePlanByIdAsync(postgres, serverId, databaseName, queryId, candidate, cancellationToken);
            if (plan is not null)
            {
                return new QueryStorePlanRead(plan, candidate);
            }
        }

        return null;
    }

    /// <summary>
    /// One plan_id's plan: the map by its primary key, then (only when the map has no row) the inline column.
    /// </summary>
    public static async Task<string?> ReadQueryStorePlanByIdAsync(
        NpgsqlDataSource postgres, int serverId, string databaseName, long queryId, long planId,
        CancellationToken cancellationToken = default)
    {
        await using (var viaMap = postgres.CreateCommand(QueryStorePlanViaMapSql))
        {
            viaMap.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            viaMap.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            viaMap.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName ?? "" });
            viaMap.Parameters.Add(new NpgsqlParameter<long> { TypedValue = planId });
            await using var reader = await viaMap.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                /* The map knows this plan_id: whatever it holds is the answer, including nothing. */
                return PayloadDimensions.ResolveContent(
                    reader.IsDBNull(0) ? null : reader.GetString(0),
                    reader.IsDBNull(1) ? null : reader.GetFieldValue<byte[]>(1));
            }
        }

        await using var command = postgres.CreateCommand(QueryStorePlanTextSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName ?? "" });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = queryId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = planId });
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is string s ? s : null;
    }

    private static async Task<List<long>> ReadCandidatePlanIdsAsync(
        NpgsqlDataSource postgres, string sql, int serverId, string databaseName, long queryId, DateTime? since,
        CancellationToken cancellationToken)
    {
        var planIds = new List<long>();
        await using var command = postgres.CreateCommand(sql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = queryId });
        if (since is DateTime bound)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = bound });
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            planIds.Add(reader.GetInt64(0));
        }

        return planIds;
    }

    /// <summary>
    /// The stored plan for one Active Queries snapshot row (<see cref="QuerySnapshotPlanSql"/> /
    /// <see cref="QuerySnapshotLivePlanSql"/>), or null when that request captured none.
    /// <paramref name="collectionTimeUtc"/> must be the row's collection_time exactly — naive UTC, with the
    /// microseconds the store keeps — and is relabelled <c>Unspecified</c> here so Npgsql binds it as a
    /// <c>timestamp</c> and not a <c>timestamptz</c> (the #1969 trap).
    /// </summary>
    public static async Task<string?> GetQuerySnapshotPlanXmlAsync(
        NpgsqlDataSource postgres, int serverId, DateTime collectionTimeUtc, int sessionId, int requestId, bool live,
        CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(live ? QuerySnapshotLivePlanSql : QuerySnapshotPlanSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = sessionId });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = requestId });
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is string s ? s : null;
    }

    /// <summary>
    /// The stored plan of one side of one blocked-process report (<see cref="BlockedPlanSql"/> /
    /// <see cref="BlockingPlanSql"/>), or null when no copy of that report carries a plan for that side.
    /// <paramref name="eventTimeUtc"/> must be the row's event_time exactly — naive UTC, with the microseconds the
    /// store keeps — and is relabelled <c>Unspecified</c> by the binder so it is a <c>timestamp</c> and not a
    /// <c>timestamptz</c> (the #1969 trap). The ecids and spids are the row's own: both sides of the key, whichever
    /// plan is asked for. A null or empty <paramref name="databaseName"/> adds no database filter.
    /// </summary>
    public static async Task<string?> GetBlockingPlanXmlAsync(
        NpgsqlDataSource postgres, int serverId, DateTime eventTimeUtc, int blockedSpid, int blockedEcid,
        int blockingSpid, int blockingEcid, bool blockingSide, string? databaseName = null, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(blockingSide ? BlockingPlanSql : BlockedPlanSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddTimestamp(command, eventTimeUtc);
        DarlingMcpReadParameters.AddInt(command, blockedSpid);
        DarlingMcpReadParameters.AddInt(command, blockedEcid);
        DarlingMcpReadParameters.AddInt(command, blockingSpid);
        DarlingMcpReadParameters.AddInt(command, blockingEcid);
        DarlingMcpReadParameters.AddTimestamp(command, EventWindowFloor.For(eventTimeUtc));
        DarlingMcpReadParameters.AddNullableText(command, string.IsNullOrEmpty(databaseName) ? null : databaseName);
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is string s ? s : null;
    }

    /// <summary>
    /// The stored victim plan of one deadlock (<see cref="DeadlockVictimPlanSql"/>), with a null plan when that deadlock
    /// captured none, or <see cref="DeadlockVictimPlanRead.Ambiguous"/> when no victim was named and the two deadlocks the
    /// stamps match name different victims. Both times are the row's own, exactly: naive UTC with the store's
    /// microseconds. A null or empty <paramref name="victimProcessId"/> adds no victim filter, and a null or empty
    /// <paramref name="databaseName"/> no database filter.
    /// </summary>
    public static async Task<DeadlockVictimPlanRead> GetDeadlockVictimPlanXmlAsync(
        NpgsqlDataSource postgres, int serverId, DateTime collectionTimeUtc, DateTime deadlockTimeUtc, string? victimProcessId,
        string? databaseName = null, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(DeadlockVictimPlanSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddTimestamp(command, collectionTimeUtc);
        DarlingMcpReadParameters.AddTimestamp(command, deadlockTimeUtc);
        DarlingMcpReadParameters.AddNullableText(command, string.IsNullOrEmpty(victimProcessId) ? null : victimProcessId);
        DarlingMcpReadParameters.AddNullableText(command, string.IsNullOrEmpty(databaseName) ? null : databaseName);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return new DeadlockVictimPlanRead(null, false);
        }

        var plan = reader.GetString(0);
        var victim = reader.IsDBNull(1) ? null : reader.GetString(1);
        if (await reader.ReadAsync(cancellationToken))
        {
            /* A second candidate. Two copies of one deadlock name the same victim and the lower deadlock_id answers; two
               different victims are two deadlocks the stamps cannot tell apart (only reachable when no victim was sent,
               since a sent victim is in the WHERE), so neither plan is taken. */
            var other = reader.IsDBNull(1) ? null : reader.GetString(1);
            if (!string.Equals(victim, other, StringComparison.Ordinal))
            {
                return new DeadlockVictimPlanRead(null, true);
            }
        }

        return new DeadlockVictimPlanRead(plan, false);
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

/// <summary>A Query Store plan and the plan_id it was stored under.</summary>
internal sealed record QueryStorePlanRead(string PlanXml, long PlanId);

/// <summary>The victim-plan read's answer (#5236): the plan, or null when no deadlock the key matches captured one.
/// <see cref="Ambiguous"/> is true, with no plan, when no victim was named and two deadlocks that share both stamps name
/// different victims, so the stamps do not say which one was meant.</summary>
internal sealed record DeadlockVictimPlanRead(string? PlanXml, bool Ambiguous);
