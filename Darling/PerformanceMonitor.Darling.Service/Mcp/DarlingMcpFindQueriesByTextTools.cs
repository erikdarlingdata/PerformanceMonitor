/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #7610: <c>find_queries_by_text</c>, which statements on one server contain a piece of text. An agent that holds part of the SQL
/// (a table name, a fragment of an ad-hoc statement) had no read that took text: the other query reads take a hash or an id. This one
/// searches two stores of statement text and hands back the keys the other reads take (<c>query_id</c> for
/// <c>get_query_store_query_history</c>, <c>query_hash</c> for <c>get_query_trend</c>).
///
/// <para><b>Two sources, each with its own rows and its own horizon.</b> Query Store text (<c>collect.query_store_text</c>, one row per
/// database and <c>query_id</c>) and the plan-cache text of the raw tier (<c>query_stats</c>, whose text sits in <c>query_text_dim</c>
/// behind a digest since #1767; one row per <c>query_hash</c>). <c>query_snapshots</c> is not a third source: it carries a
/// <c>query_hash</c> but one row per in-flight session per sample, and its retention is whatever the operator set for that collector,
/// so no single horizon can be stated for an empty answer.</para>
///
/// <para><b>No index, so a time budget.</b> The text has no index and a rare pattern reads every row in scope. On the largest server of
/// a 6.1 million row store (238,395 rows) that took 24.9 s cold and 1.9 s warm, and one server plus one database took 0.69 s. Each
/// source's statement runs in its own transaction under <c>SET LOCAL statement_timeout</c> (<see cref="BudgetMilliseconds"/>), so the
/// read stays under about ten seconds; a source that runs out is reported <c>stopped</c> and the other source is unaffected.</para>
///
/// <para><b>#7674: the all-servers scope, only on an index.</b> A search that names no server reads the plan-cache text of every enabled server, through
/// the opt-in trigram index on <c>query_text_dim.query_text</c> (<see cref="QueryTextTrigramIndex"/>, option <c>queryTextTrigramIndexEnabled</c>).
/// It is offered only when the index is <c>Ready</c> (it exists and is valid): in every other state the search is refused with a sentence that names the option
/// and the state, because without the index the text match would read the whole text table and run out its budget. The index serves the text match only.
/// Once the matching texts are known, the samples of every enabled server in the raw window are read to find where those texts ran, and
/// <c>query_stats</c> has no index on <c>query_text_digest</c>, so on a large store this scope can still reach the budget and the reply says <c>stopped</c>
/// (no timing of it has been taken). Query Store text has no index and is still searched one server at a time. With no <c>server_name</c> the front door
/// routes this tool as a fan-out (the default for a tool it does not list), so each store answers with its own index state. The one-server scope keeps the plain path whether or not the index exists: <see cref="PlanCacheSql"/> writes its match so the index
/// cannot serve it, which the issue measured to be faster than the index for that scope.</para>
///
/// <para><b>The judge runs on the whole text, then the excerpt is cut (#5320).</b> The SQL hands back the whole stored text of the at most
/// <c>limit + 1</c> matched rows, and the C# side judges it with <see cref="SensitiveStatements.Text"/> before it cuts anything, the same
/// order <c>McpHelpers.TruncateStatement</c> and <c>StatementPreview</c> keep. A cut taken in SQL would hand the judge only the piece: a rule
/// that needs context (a <c>bcp</c> name more than 100 characters before its <c>-P</c> value) loses its anchor in the piece, and the
/// host's <c>SensitiveStatementOutputFilter</c> judges only what the reply carries. A statement the judge names is listed with the withheld
/// marker in place of the excerpt, with no length and no excerpt of its text, like any other withheld statement. The match itself
/// (<c>ILIKE</c>) is a match, not the judge: it only chooses which rows are read.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpFindQueriesByTextTools
{
    internal const string ToolName = "find_queries_by_text";

    public const int DefaultLimit = 20;
    public const int MaxLimit = 50;
    public const int MinPatternLength = 3;

    /// <summary>The time each source's statement may run: 4 s, so two sources stay under 10 s.</summary>
    public const int BudgetMilliseconds = 4000;

    /// <summary>Characters of text returned around the first match.</summary>
    public const int ExcerptLength = 300;

    /// <summary>Characters of the excerpt that come before the first match.</summary>
    public const int ExcerptLead = 100;

    /// <summary>
    /// Test seam: the statement time each source may run, and a delay a test can force. The delay is its own <c>SELECT pg_sleep(...)</c>,
    /// run in the same transaction after <c>SET LOCAL statement_timeout</c> and before the search, so the timeout cancels it and the source
    /// stops deterministically. The tool itself always uses <see cref="Default"/>.
    /// </summary>
    internal sealed record SourceBudget(int Milliseconds, double TestDelaySeconds = 0)
    {
        public static SourceBudget Default { get; } = new(BudgetMilliseconds);
    }

    /// <summary>Query Store text. $1 server_id, $2 database or null, $3 the escaped pattern, $4 row cap + 1. The whole text comes back so the judge
    /// sees all of it before the excerpt is cut (see the class summary); the cap keeps that to at most $4 texts.
    /// Primary-key order, so a common pattern stops at the cap instead of reading the scope.</summary>
    internal const string QueryStoreSql = @"
SELECT t.database_name,
       t.query_id,
       t.query_hash,
       t.last_seen,
       t.query_sql_text
FROM query_store_text AS t
WHERE t.server_id = $1
AND   ($2::text IS NULL OR t.database_name = $2)
AND   t.query_sql_text ILIKE '%' || $3 || '%' ESCAPE '\'
ORDER BY t.database_name, t.query_id
LIMIT $4";

    /// <summary>Plan-cache text of the raw tier. $1 server_id, $2 database or null, $3 the escaped pattern, $4 row cap + 1,
    /// $5 the raw-tier floor (naive UTC). The server's distinct (hash, database, digest) in the window come first, so the pattern is tested
    /// once per distinct text instead of once per sample; one row per query_hash and database, the newest sighting of each (#7905: a statement
    /// that runs in several databases lists once per database, each with the <c>database_name</c> that <c>get_query_trend</c> takes). The match and the page carry the digest
    /// only: the whole text (<c>query_text_dim.query_text</c> has no size cap) is read by primary key for the at most $4 rows of the page, so a
    /// common pattern never sorts every matching text.
    /// <para>#7674: the match is <c>(d.query_text || '') ILIKE</c>, not <c>d.query_text ILIKE</c>, on purpose. With the opt-in trigram index on
    /// <c>query_text_dim.query_text</c> the planner would otherwise run a bitmap scan of that index over the whole table and join it to this server's
    /// digests, which the issue measured at 1.7 to 2.0 s cold for one server and one database against 0.2 to 0.7 s without the index. The concatenation is
    /// not the indexed column, so no index can serve it and this scope keeps the plain path whether or not the index exists. The all-servers scope is
    /// <see cref="PlanCacheAllServersSql"/>, which keeps the bare column so the index serves it.</para></summary>
    internal const string PlanCacheSql = @"
WITH seen AS MATERIALIZED (
    SELECT query_hash, database_name, query_text_digest, MAX(collection_time) AS last_collected
    FROM query_stats
    WHERE server_id = $1
    AND   collection_time >= $5
    AND   query_text_digest IS NOT NULL
    AND   ($2::text IS NULL OR database_name = $2)
    GROUP BY query_hash, database_name, query_text_digest
),
hits AS (
    SELECT DISTINCT ON (s.query_hash, s.database_name)
           s.query_hash, s.database_name, s.last_collected, s.query_text_digest
    FROM seen AS s
    JOIN query_text_dim AS d ON d.digest = s.query_text_digest
    WHERE (d.query_text || '') ILIKE '%' || $3 || '%' ESCAPE '\'
    ORDER BY s.query_hash, s.database_name, s.last_collected DESC
),
page AS (
    SELECT query_hash, database_name, last_collected, query_text_digest
    FROM hits
    ORDER BY last_collected DESC, query_hash, database_name
    LIMIT $4
)
SELECT p.query_hash, p.database_name, p.last_collected, d.query_text
FROM page AS p
JOIN query_text_dim AS d ON d.digest = p.query_text_digest
ORDER BY p.last_collected DESC, p.query_hash, p.database_name";

    /// <summary>#7674: the plan-cache text of every enabled server, served by the trigram index on <c>query_text_dim.query_text</c>. Only offered when
    /// <see cref="QueryTextTrigramIndex"/> reports <c>Ready</c>. $1 the escaped pattern, $2 row cap + 1, $3 the raw-tier floor (naive UTC). The match is
    /// the bare column, so the planner may use the index for the text match. The index does not reach the sample side: <c>query_stats</c> has no index on
    /// <c>query_text_digest</c>, so the samples of every enabled server in the raw window are read and joined to the matched digests, and on a large store
    /// this can reach the budget (the reply then says <c>stopped</c>); one row per server, query_hash and database, the newest sighting of each (#7905). The page carries the digest and the whole text is read by primary key for the
    /// at most $2 rows of it, as in <see cref="PlanCacheSql"/>.</summary>
    internal const string PlanCacheAllServersSql = @"
WITH matched AS MATERIALIZED (
    SELECT digest
    FROM query_text_dim
    WHERE query_text ILIKE '%' || $1 || '%' ESCAPE '\'
),
hits AS (
    SELECT DISTINCT ON (s.server_id, s.query_hash, s.database_name)
           s.server_id, s.query_hash, s.database_name, s.collection_time AS last_collected, s.query_text_digest
    FROM query_stats AS s
    JOIN matched AS m ON m.digest = s.query_text_digest
    JOIN servers AS v ON v.server_id = s.server_id AND v.is_enabled
    WHERE s.collection_time >= $3
    ORDER BY s.server_id, s.query_hash, s.database_name, s.collection_time DESC
),
page AS (
    SELECT server_id, query_hash, database_name, last_collected, query_text_digest
    FROM hits
    ORDER BY last_collected DESC, server_id, query_hash, database_name
    LIMIT $2
)
SELECT p.server_id, p.query_hash, p.database_name, p.last_collected, d.query_text
FROM page AS p
JOIN query_text_dim AS d ON d.digest = p.query_text_digest
ORDER BY p.last_collected DESC, p.server_id, p.query_hash, p.database_name";

    /// <summary>Escapes the LIKE metacharacters (<c>\</c>, <c>%</c>, <c>_</c>) so the pattern matches the text as typed.</summary>
    internal static string EscapeLike(string pattern) =>
        pattern.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("%", "\\%", StringComparison.Ordinal).Replace("_", "\\_", StringComparison.Ordinal);

    /// <summary>
    /// What the reply carries for one stored text (#5320): the judge runs on the WHOLE text first, then the excerpt is cut from the judged
    /// text, around the first match. A text the judge names is the withheld marker (<see cref="SensitiveStatements.PlaceholderText"/>) with no
    /// length, so a withheld row says no more than a withheld statement says anywhere else. A text that is clean carries its whole length and
    /// <see cref="ExcerptLength"/> characters starting <see cref="ExcerptLead"/> characters before the match; when the match cannot be found
    /// again here (the SQL fold of case and this one differ on an unusual letter) the excerpt starts at the head of the text.
    /// </summary>
    internal static (int? TextLength, string? Excerpt) Shape(string? stored, string pattern)
    {
        if (stored is null) return (null, null);

        var judged = SensitiveStatements.Text(stored);
        if (judged is null || string.Equals(judged, SensitiveStatements.PlaceholderText, StringComparison.Ordinal))
        {
            return (null, SensitiveStatements.PlaceholderText);
        }

        var at = judged.IndexOf(pattern, StringComparison.OrdinalIgnoreCase);
        var start = Math.Max(0, at - ExcerptLead);
        if (start > 0 && char.IsLowSurrogate(judged[start])) start--;
        var tail = judged.AsSpan(start);
        return (judged.Length, tail[..McpHelpers.TextElementCutLength(tail, ExcerptLength)].ToString());
    }

    /// <summary>What each source covers, for the reply and for an empty answer.</summary>
    internal static string QueryStoreHorizon { get; } =
        "Query Store text the store holds (collect.query_store_text): one row per database and query_id, kept until the statement has gone unseen for the widest fact retention plus "
        + QueryStoreTextStore.PruneMarginDays.ToString(CultureInfo.InvariantCulture)
        + " days. A statement Query Store has since evicted, or one this server's Query Store never recorded, is not here.";

    internal static string PlanCacheHorizon { get; } =
        "Plan-cache text of the raw tier (query_stats): the last "
        + ((int)TimescaleSupport.RawRetentionSpan.TotalDays).ToString(CultureInfo.InvariantCulture)
        + " days, one row per query_hash and database. A statement that left the plan cache before that, or ran only before the server was monitored, is not here.";

    internal static string PlanCacheAllServersHorizon { get; } =
        "Plan-cache text of the raw tier (query_stats) for every enabled server (the text is matched through the trigram index, then the samples in the raw window are read): the last "
        + ((int)TimescaleSupport.RawRetentionSpan.TotalDays).ToString(CultureInfo.InvariantCulture)
        + " days, one row per server, query_hash and database. A statement that left the plan cache before that, or ran only before the server was monitored, is not here.";

    [McpServerTool(Name = "find_queries_by_text"), Description(
        "Finds statements whose text contains a piece of text (a table name, a fragment of SQL), and returns the keys the other query reads take: query_id and database for get_query_store_query_history, query_hash for get_query_trend. pattern is a literal substring, case-insensitive (% and _ match themselves), at least 3 characters. Name a server with server_name: database_name narrows it and makes the search much faster. Two sources, each reported separately with its own horizon: Query Store text and the plan-cache text of the raw tier. There is no index on the text unless the operator turned one on, so each source has a 4 second budget; a source that runs out is reported stopped (name a database to narrow it). Leave server_name out to search the plan-cache text of every server: that is served only when the opt-in trigram index is ready (darling.json queryTextTrigramIndexEnabled; get_store_metrics query_text_index says its state), is refused with the state otherwise, and does not search Query Store text, which is searched one server at a time. The index finds the matching texts only; the samples of every enabled server in the raw window are then read to find where they ran, so on a large store the all-servers search can reach the 4 second budget and report stopped. Each source in the reply carries a status: ok (rows found), empty (searched, nothing matched), stopped (the budget ran out, so the rows listed may be incomplete) or not_searched (the all-servers scope does not search that source, Query Store text). Refused when mcp.dataDetail hides statement text, because a search on hidden text would reveal it. Rows carry a ~300 character excerpt around the first match, never the whole text; a statement the sensitive-statement filter withholds is listed with the withheld marker in place of the excerpt.")]
    public static Task<string> FindQueriesByText(
        NpgsqlDataSource postgres,
        [Description("Text to find in the statement. A literal substring, matched ignoring case; % and _ match themselves. At least 3 characters.")] string pattern,
        [Description("Server name or display name. Omit to search the plan-cache text of every server, which needs the trigram index to be ready.")] string? server_name = null,
        [Description("Limit to one database of the named server. Omit for all databases on the server; naming one is much faster. Needs server_name.")] string? database_name = null,
        [Description("Maximum rows per source. Default 20, maximum 50.")] int limit = DefaultLimit,
        McpDataDetailSetting? maskSetting = null,
        CancellationToken cancellationToken = default)
        => FindQueriesByText(postgres, pattern, server_name, database_name, limit, maskSetting?.Mode ?? McpDataDetail.Full, SourceBudget.Default, SourceBudget.Default, cancellationToken, null);

    internal static async Task<string> FindQueriesByText(
        NpgsqlDataSource postgres,
        string? pattern,
        string? server_name,
        string? database_name,
        int limit,
        McpDataDetail detail,
        SourceBudget queryStoreBudget,
        SourceBudget planCacheBudget,
        CancellationToken cancellationToken,
        QueryTextTrigramIndexTracker? tracker = null)
    {
        if (detail != McpDataDetail.Full)
        {
            /* maskLiterals hides the literals in a statement; a search on them would tell the caller what they are. metadataOnly hides the text. */
            return McpHelpers.Refusal(
                "pattern",
                $"find_queries_by_text is not served at mcp.dataDetail '{McpDataDetailModes.Word(detail)}': it searches statement text, and a search on text this level hides would tell the caller what the text holds. Use a tool that returns query_hash or query_id, or ask the operator for the 'full' level.");
        }

        var text = pattern ?? string.Empty;
        if (text.Trim().Length < MinPatternLength)
        {
            return McpHelpers.Refusal("pattern", $"pattern must be at least {MinPatternLength} characters: a shorter one matches most statements.");
        }

        var limitError = McpHelpers.ValidateTop(limit, MaxLimit);
        if (limitError != null) return limitError;

        if (string.IsNullOrWhiteSpace(server_name))
        {
            if (!string.IsNullOrWhiteSpace(database_name))
            {
                return McpHelpers.Refusal(
                    "server_name",
                    "database_name narrows one server's search, so it needs server_name. Use list_servers for the names, or leave both out to search the plan-cache text of every server once the trigram index is ready (queryTextTrigramIndexEnabled in darling.json).");
            }

            return await FindInAllServersAsync(postgres, text, limit, planCacheBudget, tracker ?? QueryTextTrigramIndexTracker.Shared, cancellationToken);
        }

        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            var database = string.IsNullOrWhiteSpace(database_name) ? null : database_name.Trim();
            var escaped = EscapeLike(text);
            var floor = DateTime.SpecifyKind(DateTime.UtcNow - TimescaleSupport.RawRetentionSpan, DateTimeKind.Unspecified);

            var queryStore = await RunSourceAsync(
                postgres, QueryStoreSql, queryStoreBudget, limit, p => BindOneServer(p, resolved.ServerId, database, escaped, limit, null), QueryStoreHorizon, StoppedAdvice(database),
                r => (Database: r.GetString(0), QueryId: r.GetInt64(1), Hash: r.IsDBNull(2) ? null : r.GetString(2), LastSeen: r.GetDateTime(3),
                    Stored: r.IsDBNull(4) ? null : r.GetString(4)),
                raw =>
                {
                    var (textLength, excerpt) = Shape(raw.Stored, text);
                    return new
                    {
                        database_name = raw.Database,
                        query_id = raw.QueryId,
                        query_hash = raw.Hash,
                        last_seen_at = Utc(raw.LastSeen),
                        text_length = textLength,
                        excerpt,
                    };
                }, cancellationToken);

            var planCache = await RunSourceAsync(
                postgres, PlanCacheSql, planCacheBudget, limit, p => BindOneServer(p, resolved.ServerId, database, escaped, limit, floor), PlanCacheHorizon, StoppedAdvice(database),
                r => (Hash: r.IsDBNull(0) ? null : r.GetString(0), Database: r.IsDBNull(1) ? null : r.GetString(1), LastCollected: r.GetDateTime(2),
                    Stored: r.IsDBNull(3) ? null : r.GetString(3)),
                raw =>
                {
                    var (textLength, excerpt) = Shape(raw.Stored, text);
                    return new
                    {
                        query_hash = raw.Hash,
                        database_name = raw.Database,
                        last_collected_at = Utc(raw.LastCollected),
                        text_length = textLength,
                        excerpt,
                    };
                }, cancellationToken);

            return JsonSerializer.Serialize(new
            {
                server = resolved.DisplayName,
                pattern = text,
                database_name = database,
                limit,
                match = "literal substring, case-insensitive",
                scope = "one_server",
                sources = new { query_store = queryStore, plan_cache = planCache },
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(ToolName, ex);
        }
    }

    /// <summary>#7674: the all-servers scope. Offered only when the trigram index is <c>Ready</c>; otherwise <see cref="AllServersRefusal"/>. The plan-cache text
    /// is searched through the index across every enabled server, and the Query Store text is reported as not searched, because it has no index.</summary>
    private static async Task<string> FindInAllServersAsync(
        NpgsqlDataSource postgres, string text, int limit, SourceBudget planCacheBudget, QueryTextTrigramIndexTracker tracker, CancellationToken cancellationToken)
    {
        TrigramIndexHealth? health;
        try
        {
            await using var healthConnection = await postgres.OpenConnectionAsync(cancellationToken);
            health = await QueryTextTrigramIndex.ReadHealthAsync(healthConnection, tracker, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            health = null;
        }

        if (health?.Status != TrigramIndexStatus.Ready)
        {
            return McpHelpers.Refusal("server_name", AllServersRefusal(health));
        }

        try
        {
            var (servers, fault) = await DarlingServerResolver.LoadEnabledOrFaultAsync(postgres, cancellationToken);
            if (fault != null) return fault;

            var escaped = EscapeLike(text);
            var floor = DateTime.SpecifyKind(DateTime.UtcNow - TimescaleSupport.RawRetentionSpan, DateTimeKind.Unspecified);
            var planCache = await RunSourceAsync(
                postgres, PlanCacheAllServersSql, planCacheBudget, limit,
                p =>
                {
                    p.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = escaped });
                    p.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = limit + 1 });
                    p.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = floor });
                },
                PlanCacheAllServersHorizon,
                "Use a longer, more specific pattern, or name a server with server_name (and database_name) to search it directly.",
                r => (ServerId: r.GetInt32(0), Hash: r.IsDBNull(1) ? null : r.GetString(1), Database: r.IsDBNull(2) ? null : r.GetString(2), LastCollected: r.GetDateTime(3),
                    Stored: r.IsDBNull(4) ? null : r.GetString(4)),
                raw =>
                {
                    var (textLength, excerpt) = Shape(raw.Stored, text);
                    return new
                    {
                        server = DarlingServerResolver.AnswerNameOf(servers, servers.Find(x => x.ServerId == raw.ServerId)),
                        query_hash = raw.Hash,
                        database_name = raw.Database,
                        last_collected_at = Utc(raw.LastCollected),
                        text_length = textLength,
                        excerpt,
                    };
                }, cancellationToken);

            return JsonSerializer.Serialize(new
            {
                server = (string?)null,
                pattern = text,
                database_name = (string?)null,
                limit,
                match = "literal substring, case-insensitive",
                scope = "all_servers",
                sources = new
                {
                    query_store = new
                    {
                        status = "not_searched",
                        message = "Query Store text has no index, so an all-servers search does not read it. Name a server with server_name (and database_name to make it faster) to search its Query Store text.",
                        horizon = QueryStoreHorizon,
                        returned = 0,
                        truncated = false,
                        rows = Array.Empty<object>(),
                    },
                    plan_cache = planCache,
                },
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(ToolName, ex);
        }
    }

    /// <summary>#7674: the refusal of the all-servers scope when the trigram index is not <c>Ready</c>. One sentence plus the way forward, naming the option and the
    /// state. Null <paramref name="health"/> is a catalog read that failed. Pure, so each state is tested without a store.</summary>
    internal static string AllServersRefusal(TrigramIndexHealth? health)
    {
        var lead = "Searching every server at once needs the opt-in trigram index on the plan-cache text (queryTextTrigramIndexEnabled in darling.json), and ";
        var tail = " Until then, name one server with server_name (database_name makes it faster).";
        if (health is null)
        {
            return lead + "its state could not be read (the store's catalog read failed)." + tail;
        }

        var minutes = ((int)QueryTextTrigramIndex.StartDelay.TotalMinutes).ToString(CultureInfo.InvariantCulture);
        var state = health.Status switch
        {
            TrigramIndexStatus.Unconfigured =>
                "the service has not read darling.json yet, so the option is not known (state: not configured yet). Try again in a moment.",
            TrigramIndexStatus.Off =>
                "the option is off (state: off). Set queryTextTrigramIndexEnabled to true in darling.json and restart the service: the index is built in the background about "
                + minutes + " minutes after start and can take many minutes on a large store.",
            TrigramIndexStatus.DropPending or TrigramIndexStatus.Dropping =>
                "the option is off and the service is dropping the index (state: being dropped). Set queryTextTrigramIndexEnabled to true in darling.json and restart the service to keep it.",
            TrigramIndexStatus.Pending =>
                "the option is on but the index is not built yet (state: pending). The service builds it in the background about " + minutes + " minutes after start.",
            TrigramIndexStatus.Building =>
                "the index is building (state: building). Try again when get_store_metrics reports query_text_index.status Ready.",
            TrigramIndexStatus.Failed =>
                "the last build failed (state: failed). The service tries again after a wait: get_store_metrics query_text_index says why and when.",
            TrigramIndexStatus.WaitingOnExtension =>
                "it is waiting on the pg_trgm extension (state: waiting on the pg_trgm extension). An operator runs "
                + (health.OperatorStatement ?? QueryTextTrigramIndex.OperatorStatement) + " in the store database as a role that may create it.",
            _ => "it is not ready (state: " + health.Status + ").",
        };
        return lead + state + tail;
    }

    private static string StoppedAdvice(string? database) =>
        database is null
            ? "Name a database with database_name to narrow the search; one server and one database is much faster."
            : "The database named is large; use a longer, more specific pattern.";

    private static void BindOneServer(NpgsqlParameterCollection parameters, int serverId, string? database, string escaped, int limit, DateTime? floor)
    {
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)database ?? DBNull.Value });
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = escaped });
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = limit + 1 });
        if (floor is not null)
        {
            parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = floor.Value });
        }
    }

    private static string Utc(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);

    /// <summary>One source: the statement under <c>SET LOCAL statement_timeout</c> in its own transaction. A statement that runs out is
    /// <c>stopped</c> (SQLSTATE 57014), not an error, so the other source still answers. <paramref name="read"/> takes the raw row (whole
    /// text included) while the statement runs; <paramref name="shape"/> judges and cuts it after the connection is back in the pool, for
    /// the rows kept (the row past the cap is dropped unjudged).</summary>
    private static async Task<object> RunSourceAsync<TRaw, TRow>(
        NpgsqlDataSource postgres, string sql, SourceBudget budget, int limit, Action<NpgsqlParameterCollection> bind,
        string horizon, string stoppedAdvice, Func<NpgsqlDataReader, TRaw> read, Func<TRaw, TRow> shape, CancellationToken cancellationToken)
    {
        var rows = new List<TRaw>();
        var started = DateTime.UtcNow;
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            var setSql = $"SET LOCAL statement_timeout = {budget.Milliseconds.ToString(CultureInfo.InvariantCulture)}";
            await using (var set = new NpgsqlCommand(setSql, connection, transaction) { CommandTimeout = McpCommandDeadlines.ReadSeconds })
            {
                await set.ExecuteNonQueryAsync(cancellationToken);
            }

            if (budget.TestDelaySeconds > 0)
            {
                var delaySql = "SELECT pg_sleep(" + budget.TestDelaySeconds.ToString(CultureInfo.InvariantCulture) + ")";
                await using var delay = new NpgsqlCommand(delaySql, connection, transaction) { CommandTimeout = McpCommandDeadlines.ReadSeconds };
                await delay.ExecuteNonQueryAsync(cancellationToken);
            }

            await using (var command = new NpgsqlCommand(sql, connection, transaction))
            {
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                bind(command.Parameters);

                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    rows.Add(read(reader));
                }
            }
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.QueryCanceled)
        {
            var seconds = (DateTime.UtcNow - started).TotalSeconds;
            return new
            {
                status = "stopped",
                message = $"Stopped after {seconds.ToString("0.0", CultureInfo.InvariantCulture)} s: the {budget.Milliseconds / 1000.0:0.#} s budget for this source ran out before the search finished, "
                    + "so the rows below (none) are not a complete answer. "
                    + stoppedAdvice,
                horizon,
                returned = 0,
                truncated = false,
                rows = Array.Empty<TRow>(),
            };
        }

        var truncated = rows.Count > limit;
        if (truncated) rows.RemoveAt(rows.Count - 1);
        var shaped = rows.ConvertAll(r => shape(r));
        return new
        {
            status = shaped.Count == 0 ? "empty" : "ok",
            message = shaped.Count == 0 ? "No statement in this source contains the pattern. Only what the horizon covers was searched." : null,
            horizon,
            returned = shaped.Count,
            truncated,
            rows = shaped,
        };
    }
}
