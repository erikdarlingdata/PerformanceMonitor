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
/// read stays under about ten seconds; a source that runs out is reported <c>stopped</c> and the other source is unaffected. A
/// trigram index is a later, opt-in change, so the search stays scoped to one server.</para>
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
    /// Test seam: the statement time each source may run, and a delay run first inside the same timed statement so a test can
    /// force a stop deterministically. The tool itself always uses <see cref="Default"/>.
    /// </summary>
    internal sealed record SourceBudget(int Milliseconds, double TestDelaySeconds = 0)
    {
        public static SourceBudget Default { get; } = new(BudgetMilliseconds);
    }

    /// <summary>Query Store text. $1 server_id, $2 database or null, $3 the escaped pattern, $4 the raw pattern (for the excerpt position), $5 row cap + 1.
    /// Primary-key order, so a common pattern stops at the cap instead of reading the scope.</summary>
    internal const string QueryStoreSql = @"
SELECT t.database_name,
       t.query_id,
       t.query_hash,
       t.last_seen,
       length(t.query_sql_text) AS text_length,
       substr(t.query_sql_text, greatest(1, p.pos - 100), 300) AS excerpt
FROM query_store_text AS t
CROSS JOIN LATERAL (SELECT strpos(lower(t.query_sql_text), lower($4)) AS pos) AS p
WHERE t.server_id = $1
AND   ($2::text IS NULL OR t.database_name = $2)
AND   t.query_sql_text ILIKE '%' || $3 || '%' ESCAPE '\'
ORDER BY t.database_name, t.query_id
LIMIT $5";

    /// <summary>Plan-cache text of the raw tier. $1 server_id, $2 database or null, $3 the escaped pattern, $4 the raw pattern, $5 row cap + 1,
    /// $6 the raw-tier floor (naive UTC). The server's distinct (hash, database, digest) in the window come first, so the pattern is tested
    /// once per distinct text instead of once per sample; one row per query_hash, the newest sighting.</summary>
    internal const string PlanCacheSql = @"
WITH seen AS MATERIALIZED (
    SELECT query_hash, database_name, query_text_digest, MAX(collection_time) AS last_collected
    FROM query_stats
    WHERE server_id = $1
    AND   collection_time >= $6
    AND   query_text_digest IS NOT NULL
    AND   ($2::text IS NULL OR database_name = $2)
    GROUP BY query_hash, database_name, query_text_digest
),
hits AS (
    SELECT DISTINCT ON (s.query_hash)
           s.query_hash, s.database_name, s.last_collected,
           length(d.query_text) AS text_length,
           substr(d.query_text, greatest(1, strpos(lower(d.query_text), lower($4)) - 100), 300) AS excerpt
    FROM seen AS s
    JOIN query_text_dim AS d ON d.digest = s.query_text_digest
    WHERE d.query_text ILIKE '%' || $3 || '%' ESCAPE '\'
    ORDER BY s.query_hash, s.last_collected DESC
)
SELECT query_hash, database_name, last_collected, text_length, excerpt
FROM hits
ORDER BY last_collected DESC, query_hash
LIMIT $5";

    /// <summary>Escapes the LIKE metacharacters (<c>\</c>, <c>%</c>, <c>_</c>) so the pattern matches the text as typed.</summary>
    internal static string EscapeLike(string pattern) =>
        pattern.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("%", "\\%", StringComparison.Ordinal).Replace("_", "\\_", StringComparison.Ordinal);

    /// <summary>What each source covers, for the reply and for an empty answer.</summary>
    internal static string QueryStoreHorizon { get; } =
        "Query Store text the store holds (collect.query_store_text): one row per database and query_id, kept until the statement has gone unseen for the widest fact retention plus "
        + QueryStoreTextStore.PruneMarginDays.ToString(CultureInfo.InvariantCulture)
        + " days. A statement Query Store has since evicted, or one this server's Query Store never recorded, is not here.";

    internal static string PlanCacheHorizon { get; } =
        "Plan-cache text of the raw tier (query_stats): the last "
        + ((int)TimescaleSupport.RawRetentionSpan.TotalDays).ToString(CultureInfo.InvariantCulture)
        + " days, one row per query_hash. A statement that left the plan cache before that, or ran only before the server was monitored, is not here.";

    [McpServerTool(Name = ToolName), Description(
        "Finds statements on ONE server whose text contains a piece of text (a table name, a fragment of SQL), and returns the keys the other query reads take: query_id and database for get_query_store_query_history, query_hash for get_query_trend. pattern is a literal substring, case-insensitive (% and _ match themselves), at least 3 characters. server_name is required; database_name narrows it and makes the search much faster. Two sources, each reported separately with its own horizon: Query Store text and the plan-cache text of the raw tier. There is no index on the text, so each source has a 4 second budget; a source that runs out is reported stopped (name a database to narrow it). Refused when mcp.dataDetail hides statement text, because a search on hidden text would reveal it. Rows carry a ~300 character excerpt around the first match, never the whole text.")]
    public static Task<string> FindQueriesByText(
        NpgsqlDataSource postgres,
        [Description("Text to find in the statement. A literal substring, matched ignoring case; % and _ match themselves. At least 3 characters.")] string pattern,
        [Description("Server name or display name. Required.")] string server_name,
        [Description("Limit to one database. Omit for all databases on the server; naming one is much faster.")] string? database_name = null,
        [Description("Maximum rows per source. Default 20, maximum 50.")] int limit = DefaultLimit,
        McpDataDetailSetting? maskSetting = null,
        CancellationToken cancellationToken = default)
        => FindQueriesByText(postgres, pattern, server_name, database_name, limit, maskSetting?.Mode ?? McpDataDetail.Full, SourceBudget.Default, SourceBudget.Default, cancellationToken);

    internal static async Task<string> FindQueriesByText(
        NpgsqlDataSource postgres,
        string? pattern,
        string? server_name,
        string? database_name,
        int limit,
        McpDataDetail detail,
        SourceBudget queryStoreBudget,
        SourceBudget planCacheBudget,
        CancellationToken cancellationToken)
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

        if (string.IsNullOrWhiteSpace(server_name))
        {
            return McpHelpers.Refusal("server_name", "server_name is required: the search is scoped to one server. Use list_servers for the names.");
        }

        if (limit <= 0 || limit > MaxLimit)
        {
            return McpHelpers.Refusal("limit", $"limit must be between 1 and {MaxLimit} (default {DefaultLimit}).");
        }

        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            var database = string.IsNullOrWhiteSpace(database_name) ? null : database_name.Trim();
            var escaped = EscapeLike(text);
            var floor = DateTime.SpecifyKind(DateTime.UtcNow - TimescaleSupport.RawRetentionSpan, DateTimeKind.Unspecified);

            var queryStore = await RunSourceAsync(
                postgres, QueryStoreSql, queryStoreBudget, resolved.ServerId, database, escaped, text, limit, null, QueryStoreHorizon, database,
                r => new
                {
                    database_name = r.GetString(0),
                    query_id = r.GetInt64(1),
                    query_hash = r.IsDBNull(2) ? null : r.GetString(2),
                    last_seen_at = Utc(r.GetDateTime(3)),
                    text_length = r.GetInt32(4),
                    excerpt = r.IsDBNull(5) ? null : r.GetString(5),
                }, cancellationToken);

            var planCache = await RunSourceAsync(
                postgres, PlanCacheSql, planCacheBudget, resolved.ServerId, database, escaped, text, limit, floor, PlanCacheHorizon, database,
                r => new
                {
                    query_hash = r.IsDBNull(0) ? null : r.GetString(0),
                    database_name = r.IsDBNull(1) ? null : r.GetString(1),
                    last_collected_at = Utc(r.GetDateTime(2)),
                    text_length = r.GetInt32(3),
                    excerpt = r.IsDBNull(4) ? null : r.GetString(4),
                }, cancellationToken);

            return JsonSerializer.Serialize(new
            {
                server = resolved.DisplayName,
                pattern = text,
                database_name = database,
                limit,
                match = "literal substring, case-insensitive",
                sources = new { query_store = queryStore, plan_cache = planCache },
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(ToolName, ex);
        }
    }

    private static string Utc(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);

    /// <summary>One source: the statement under <c>SET LOCAL statement_timeout</c> in its own transaction. A statement that runs out is
    /// <c>stopped</c> (SQLSTATE 57014), not an error, so the other source still answers.</summary>
    private static async Task<object> RunSourceAsync<T>(
        NpgsqlDataSource postgres, string sql, SourceBudget budget, int serverId, string? database, string escaped, string raw, int limit, DateTime? floor,
        string horizon, string? databaseScope, Func<NpgsqlDataReader, T> map, CancellationToken cancellationToken)
    {
        var rows = new List<T>();
        var started = DateTime.UtcNow;
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            await using (var set = new NpgsqlCommand($"SET LOCAL statement_timeout = {budget.Milliseconds.ToString(CultureInfo.InvariantCulture)}", connection, transaction))
            {
                await set.ExecuteNonQueryAsync(cancellationToken);
            }

            if (budget.TestDelaySeconds > 0)
            {
                await using var delay = new NpgsqlCommand("SELECT pg_sleep(" + budget.TestDelaySeconds.ToString(CultureInfo.InvariantCulture) + ")", connection, transaction);
                await delay.ExecuteNonQueryAsync(cancellationToken);
            }

            await using (var command = new NpgsqlCommand(sql, connection, transaction))
            {
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)database ?? DBNull.Value });
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = escaped });
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = raw });
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = limit + 1 });
                if (floor is not null)
                {
                    command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = floor.Value });
                }

                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    rows.Add(map(reader));
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
                    + (databaseScope is null
                        ? "Name a database with database_name to narrow the search; one server and one database is much faster."
                        : "The database named is large; use a longer, more specific pattern."),
                horizon,
                returned = 0,
                truncated = false,
                rows = Array.Empty<T>(),
            };
        }

        var truncated = rows.Count > limit;
        if (truncated) rows.RemoveAt(rows.Count - 1);
        return new
        {
            status = rows.Count == 0 ? "empty" : "ok",
            message = rows.Count == 0 ? "No statement in this source contains the pattern. Only what the horizon covers was searched." : null,
            horizon,
            returned = rows.Count,
            truncated,
            rows,
        };
    }
}
