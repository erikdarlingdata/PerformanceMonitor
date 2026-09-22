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
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The store's OWN per-statement timings as an MCP read (#3899): which statements the monitoring store spends
/// its time on, attributed to the role that ran them. It exists for one question the product could not answer
/// before, "the web viewer / MCP tools are slow — which query?", and its role split is what answers it:
/// <c>viewer</c> is the web viewer, <c>mcp</c> is MCP tools, <c>admin</c> is the Darling Viewer desktop app,
/// and the owner is the service itself. Reads the two SECURITY DEFINER functions
/// <see cref="StoreStatementStats"/> creates; a store without them answers <c>unavailable</c> with the remedy.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpStoreQueryStatsTools
{
    /// <summary>The default page size.</summary>
    public const int DefaultTop = 20;

    /// <summary>How much of a statement's text the default response carries.</summary>
    public const int PreviewLength = 240;

    /// <summary>The <c>order_by</c> values, mapped to the reader function's columns. The map IS the whitelist:
    /// a value not in it is refused, and only a value from it is ever interpolated into SQL.</summary>
    internal static readonly IReadOnlyDictionary<string, string> OrderColumns =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["total_time"] = "total_exec_ms",
            ["mean_time"] = "mean_exec_ms",
            ["max_time"] = "max_exec_ms",
            ["calls"] = "calls",
            ["rows"] = "rows_returned",
            ["shared_blks_read"] = "shared_blks_read",
        };

    /// <summary>The <c>role</c> values, and who each one is.</summary>
    internal static readonly IReadOnlyDictionary<string, string> RoleIdentities =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["owner"] = "the service itself: collection, maintenance, alerting",
            [DarlingManagedPostgres.AdminRoleName] = "the Darling Viewer desktop app and its Settings window",
            [DarlingManagedPostgres.ViewerRoleName] = "the web viewer and read-only Viewer seats",
            [DarlingManagedPostgres.McpRoleName] = "MCP tools",
        };

    /// <summary>The database owner's statements report as <c>owner</c> rather than under its login name, so the
    /// label means the same thing on a managed store and on one whose owner role is called something else.</summary>
    internal const string RoleKeySql =
        "CASE WHEN f.role_name = (SELECT pg_catalog.pg_get_userbyid(d.datdba) FROM pg_catalog.pg_database AS d WHERE d.datname = pg_catalog.current_database()) THEN 'owner' ELSE f.role_name END";

    /// <summary>When the counters were last reset and how many entries the view has evicted since.</summary>
    public static readonly string InfoSql =
        $"SELECT i.stats_reset, i.dealloc FROM {PgSchemaGenerator.ConfigSchema}.{StoreStatementStats.InfoFunctionName}() AS i";

    /// <summary>Each role's share of the store's statement time. The whole store's total is a window over the
    /// same grouping, so the share's denominator is the statement's own and never a sum of rows the reader
    /// happened to fetch (the page contract's rule, #3613).</summary>
    public static readonly string ByRoleSql = $@"
SELECT
    {RoleKeySql} AS role_key,
    pg_catalog.count(*) AS statements,
    pg_catalog.sum(f.calls)::bigint AS calls,
    pg_catalog.sum(f.total_exec_ms) AS total_exec_ms,
    pg_catalog.sum(pg_catalog.sum(f.total_exec_ms)) OVER () AS store_total_exec_ms
FROM {PgSchemaGenerator.ConfigSchema}.{StoreStatementStats.FunctionName}() AS f
GROUP BY 1
ORDER BY 4 DESC NULLS LAST";

    /// <summary>The ranked statements. <c>$1</c> the role key or null for every role, <c>$2</c> the fetch size
    /// (the caller's cap plus one, so truncation is observed rather than inferred). <paramref name="orderColumn"/>
    /// comes from <see cref="OrderColumns"/> and nowhere else.</summary>
    public static string BuildStatementsSql(string orderColumn) => $@"
SELECT
    ranked.role_key,
    ranked.queryid,
    ranked.calls,
    ranked.total_exec_ms,
    ranked.mean_exec_ms,
    ranked.max_exec_ms,
    ranked.rows_returned,
    ranked.shared_blks_hit,
    ranked.shared_blks_read,
    ranked.temp_blks_written,
    ranked.query
FROM
(
    SELECT
        {RoleKeySql} AS role_key,
        f.*
    FROM {PgSchemaGenerator.ConfigSchema}.{StoreStatementStats.FunctionName}() AS f
) AS ranked
WHERE $1::text IS NULL
OR    ranked.role_key = $1
ORDER BY ranked.{orderColumn} DESC NULLS LAST, ranked.queryid
LIMIT $2";

    [McpServerTool(Name = "get_store_query_stats"), Description(
        "Ranks the monitoring STORE's own SQL statements by their server-side cost, from pg_stat_statements in the store — not a monitored server's queries. It answers 'the web viewer / MCP tools are slow: which query?'. Every statement is attributed to the role that ran it: viewer (the web viewer), mcp (MCP tools), admin (the Darling Viewer desktop app and Settings), owner (the service itself: collection, maintenance, alerting). by_role gives each role's statement count, calls, total execution ms and share of the total; statements lists the top statements with calls, total/mean/max execution ms, rows, and shared blocks read and hit, plus temp blocks written. Figures are CUMULATIVE since stats_since, the last counter reset (a store restart does not reset them), and are server-side execution only: network transfer and result serialization are not in them. entries_evicted above zero means pg_stat_statements dropped its least-used entries, so a rarely-run statement can be missing from the ranking. The text is PostgreSQL's normalized form (constants replaced by $1, $2 ...) with comments stripped, cut to a preview unless full_text is true. Answers status unavailable, with the remedy, on a store where pg_stat_statements is not installed or not yet loaded (it loads on a store restart). Takes no server_name: the store is the subject.")]
    public static async Task<string> GetStoreQueryStats(
        NpgsqlDataSource postgres,
        [Description("Only statements run by this role: owner, admin, viewer or mcp. Omit for every role.")] string? role = null,
        [Description("Rank by total_time (default: where the store's time went), mean_time (slowest per call), max_time (worst single call), calls, rows, or shared_blks_read (disk reads).")] string order_by = "total_time",
        [Description("How many statements. Default 20, max 1000.")] int top = DefaultTop,
        [Description("Return each statement's full normalized text instead of a 240-character preview. Default false.")] bool full_text = false)
    {
        var invalidTop = McpHelpers.ValidateTop(top, "top");
        if (invalidTop != null)
        {
            return invalidTop;
        }

        if (!OrderColumns.TryGetValue(order_by ?? "", out var orderColumn))
        {
            return McpHelpers.Refusal("order_by",
                $"Invalid order_by '{order_by}'. Use one of: {string.Join(", ", OrderColumns.Keys)}.");
        }

        string? roleKey = null;
        if (!string.IsNullOrWhiteSpace(role))
        {
            if (!RoleIdentities.ContainsKey(role.Trim()))
            {
                return McpHelpers.Refusal("role",
                    $"Invalid role '{role}'. Use one of: {string.Join(", ", RoleIdentities.Keys)}, or omit it for every role.");
            }

            roleKey = role.Trim().ToLowerInvariant();
        }

        try
        {
            DateTime? statsSince = null;
            long? evicted = null;
            await using (var info = postgres.CreateCommand(InfoSql))
            {
                info.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                await using var reader = await info.ExecuteReaderAsync();
                if (await reader.ReadAsync())
                {
                    statsSince = reader.IsDBNull(0) ? null : reader.GetDateTime(0).ToUniversalTime();
                    evicted = reader.IsDBNull(1) ? null : reader.GetInt64(1);
                }
            }

            var byRole = new List<(string Role, long Statements, long Calls, double TotalMs, double StoreTotalMs)>();
            await using (var command = postgres.CreateCommand(ByRoleSql))
            {
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                await using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    byRole.Add((
                        reader.GetString(0),
                        reader.GetInt64(1),
                        reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                        reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                        reader.IsDBNull(4) ? 0 : reader.GetDouble(4)));
                }
            }

            var fetched = new List<StatementRow>();
            await using (var command = postgres.CreateCommand(BuildStatementsSql(orderColumn)))
            {
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                command.Parameters.Add(new NpgsqlParameter { Value = (object?)roleKey ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
                command.Parameters.AddWithValue(top + 1);
                await using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var text = reader.IsDBNull(10) ? "" : reader.GetString(10);
                    fetched.Add(new StatementRow(
                        Role: reader.GetString(0),
                        QueryId: reader.IsDBNull(1) ? null : reader.GetInt64(1),
                        Calls: reader.GetInt64(2),
                        TotalExecMs: Round(reader.GetDouble(3)),
                        MeanExecMs: Round(reader.GetDouble(4)),
                        MaxExecMs: Round(reader.GetDouble(5)),
                        Rows: reader.GetInt64(6),
                        SharedBlksHit: reader.GetInt64(7),
                        SharedBlksRead: reader.GetInt64(8),
                        TempBlksWritten: reader.GetInt64(9),
                        Query: full_text ? text : CompactSql(text, PreviewLength)));
                }
            }

            var (page, truncated) = McpHelpers.BoundPage(fetched, top);

            return JsonSerializer.Serialize(new
            {
                stats_since = statsSince?.ToString("O", CultureInfo.InvariantCulture),
                entries_evicted = evicted,
                role = roleKey,
                order_by = order_by!.ToLowerInvariant(),
                top,
                statements_returned = page.Count,
                truncated,
                by_role = byRole.Select(r => new
                {
                    role = r.Role,
                    identity = RoleIdentities.TryGetValue(r.Role, out var who) ? who : null,
                    statements = r.Statements,
                    calls = r.Calls,
                    total_exec_ms = Round(r.TotalMs),
                    share_pct = r.StoreTotalMs > 0 ? Math.Round(r.TotalMs / r.StoreTotalMs * 100, 1) : 0,
                }),
                statements = page.Select(s => new
                {
                    role = s.Role,
                    query_id = s.QueryId,
                    calls = s.Calls,
                    total_exec_ms = s.TotalExecMs,
                    mean_exec_ms = s.MeanExecMs,
                    max_exec_ms = s.MaxExecMs,
                    rows = s.Rows,
                    shared_blks_read = s.SharedBlksRead,
                    shared_blks_hit = s.SharedBlksHit,
                    temp_blks_written = s.TempBlksWritten,
                    query = s.Query,
                }),
                note = "Cumulative since stats_since, server-side execution only. by_role shares are of the store's total statement time across every role, so a role's share is where the store's own effort went, not how slow that role's calls felt. A statement that is slow per call ranks under mean_time or max_time; one that is cheap but constant ranks under total_time.",
            }, McpHelpers.JsonOptions);
        }
        catch (PostgresException ex) when (UnavailableReason(ex.SqlState) is { } reason)
        {
            return McpHelpers.Status("unavailable", reason);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_store_query_stats", ex);
        }
    }

    /// <summary>The remedy for each way a store can lack statement statistics, keyed by the SQLSTATE the reader
    /// functions raise; null for anything else, which is a real error and goes out as one.</summary>
    internal static string? UnavailableReason(string? sqlState) => sqlState switch
    {
        /* undefined_function: the reader functions were never created. */
        "42883" =>
            "Statement statistics are not set up on this store. The service creates them at start and on its hourly store-maintenance pass once pg_stat_statements is installed. A managed store preloads it from its first service-owned start of a build carrying #3899; a store you run yourself needs pg_stat_statements in shared_preload_libraries, a restart, and CREATE EXTENSION pg_stat_statements run by a superuser in this database.",
        /* object_not_in_prerequisite_state: installed, but the library is not loaded. */
        "55000" =>
            "pg_stat_statements is installed but not loaded. shared_preload_libraries takes effect only on a store restart: a managed store loads it on its next service-owned start (the service log records when the preload was added); a store you run yourself needs it in shared_preload_libraries and a restart.",
        /* insufficient_privilege: a role the service does not grant. */
        "42501" =>
            "This role may not read the store's statement statistics. The service grants them to the admin, viewer and mcp roles at start and hourly; any other role has no grant by design.",
        _ => null,
    };

    /// <summary>
    /// A statement's text as a one-line preview: comments removed, whitespace collapsed, cut at
    /// <paramref name="maxLength"/> with "..." when it was longer. The product's SQL carries long comment
    /// blocks, which is most of what this removes. A quoted literal is copied through untouched, so a
    /// <c>--</c> inside one survives; normalized text rarely has any, since constants are already <c>$n</c>.
    /// </summary>
    internal static string CompactSql(string? text, int maxLength)
    {
        if (string.IsNullOrEmpty(text))
        {
            return "";
        }

        var builder = new StringBuilder(Math.Min(text.Length, maxLength + 64));
        var inQuote = false;
        var pendingSpace = false;
        for (var i = 0; i < text.Length; i++)
        {
            var c = text[i];
            if (inQuote)
            {
                builder.Append(c);
                inQuote = c != '\'';
                continue;
            }

            if (c == '-' && i + 1 < text.Length && text[i + 1] == '-')
            {
                var newline = text.IndexOf('\n', i);
                i = newline < 0 ? text.Length : newline;
                pendingSpace = true;
                continue;
            }

            if (c == '/' && i + 1 < text.Length && text[i + 1] == '*')
            {
                var close = text.IndexOf("*/", i + 2, StringComparison.Ordinal);
                i = close < 0 ? text.Length : close + 1;
                pendingSpace = true;
                continue;
            }

            if (char.IsWhiteSpace(c))
            {
                pendingSpace = true;
                continue;
            }

            if (pendingSpace && builder.Length > 0)
            {
                builder.Append(' ');
            }

            pendingSpace = false;
            builder.Append(c);
            inQuote = c == '\'';
            if (builder.Length > maxLength)
            {
                break;
            }
        }

        return builder.Length > maxLength
            ? builder.ToString(0, maxLength).TrimEnd() + "..."
            : builder.ToString();
    }

    private static double Round(double value) => Math.Round(value, 2);

    private sealed record StatementRow(
        string Role,
        long? QueryId,
        long Calls,
        double TotalExecMs,
        double MeanExecMs,
        double MaxExecMs,
        long Rows,
        long SharedBlksHit,
        long SharedBlksRead,
        long TempBlksWritten,
        string Query);
}
