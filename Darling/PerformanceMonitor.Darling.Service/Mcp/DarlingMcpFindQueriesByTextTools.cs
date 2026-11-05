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
    /// once per distinct text instead of once per sample; one row per query_hash, the newest sighting. The match and the page carry the digest
    /// only: the whole text (<c>query_text_dim.query_text</c> has no size cap) is read by primary key for the at most $4 rows of the page, so a
    /// common pattern never sorts every matching text.</summary>
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
    SELECT DISTINCT ON (s.query_hash)
           s.query_hash, s.database_name, s.last_collected, s.query_text_digest
    FROM seen AS s
    JOIN query_text_dim AS d ON d.digest = s.query_text_digest
    WHERE d.query_text ILIKE '%' || $3 || '%' ESCAPE '\'
    ORDER BY s.query_hash, s.last_collected DESC
),
page AS (
    SELECT query_hash, database_name, last_collected, query_text_digest
    FROM hits
    ORDER BY last_collected DESC, query_hash
    LIMIT $4
)
SELECT p.query_hash, p.database_name, p.last_collected, d.query_text
FROM page AS p
JOIN query_text_dim AS d ON d.digest = p.query_text_digest
ORDER BY p.last_collected DESC, p.query_hash";

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
        + " days, one row per query_hash. A statement that left the plan cache before that, or ran only before the server was monitored, is not here.";

    [McpServerTool(Name = "find_queries_by_text"), Description(
        "Finds statements on ONE server whose text contains a piece of text (a table name, a fragment of SQL), and returns the keys the other query reads take: query_id and database for get_query_store_query_history, query_hash for get_query_trend. pattern is a literal substring, case-insensitive (% and _ match themselves), at least 3 characters. server_name is required; database_name narrows it and makes the search much faster. Two sources, each reported separately with its own horizon: Query Store text and the plan-cache text of the raw tier. There is no index on the text, so each source has a 4 second budget; a source that runs out is reported stopped (name a database to narrow it). Refused when mcp.dataDetail hides statement text, because a search on hidden text would reveal it. Rows carry a ~300 character excerpt around the first match, never the whole text; a statement the sensitive-statement filter withholds is listed with the withheld marker in place of the excerpt.")]
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

        var limitError = McpHelpers.ValidateTop(limit, MaxLimit);
        if (limitError != null) return limitError;

        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            var database = string.IsNullOrWhiteSpace(database_name) ? null : database_name.Trim();
            var escaped = EscapeLike(text);
            var floor = DateTime.SpecifyKind(DateTime.UtcNow - TimescaleSupport.RawRetentionSpan, DateTimeKind.Unspecified);

            var queryStore = await RunSourceAsync(
                postgres, QueryStoreSql, queryStoreBudget, resolved.ServerId, database, escaped, limit, null, QueryStoreHorizon, database,
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
                postgres, PlanCacheSql, planCacheBudget, resolved.ServerId, database, escaped, limit, floor, PlanCacheHorizon, database,
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
    /// <c>stopped</c> (SQLSTATE 57014), not an error, so the other source still answers. <paramref name="read"/> takes the raw row (whole
    /// text included) while the statement runs; <paramref name="shape"/> judges and cuts it after the connection is back in the pool, for
    /// the rows kept (the row past the cap is dropped unjudged).</summary>
    private static async Task<object> RunSourceAsync<TRaw, TRow>(
        NpgsqlDataSource postgres, string sql, SourceBudget budget, int serverId, string? database, string escaped, int limit, DateTime? floor,
        string horizon, string? databaseScope, Func<NpgsqlDataReader, TRaw> read, Func<TRaw, TRow> shape, CancellationToken cancellationToken)
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
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)database ?? DBNull.Value });
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = escaped });
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = limit + 1 });
                if (floor is not null)
                {
                    command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = floor.Value });
                }

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
                    + (databaseScope is null
                        ? "Name a database with database_name to narrow the search; one server and one database is much faster."
                        : "The database named is large; use a longer, more specific pattern."),
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
