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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The store's OWN per-statement timings as an MCP read (#3899): which statements the monitoring store spends
/// its time on, attributed to the role that ran them. It exists for one question the product could not answer
/// before, "the web viewer / MCP tools are slow — which query?", and its role split is what answers it on a
/// managed or compose store, where each surface has its own login (<see cref="RoleIdentities"/> says which is
/// which). A bring-your-own store runs a web or MCP host with no login of its own (#3914:
/// <c>postgres.webConnectionString</c> / <c>postgres.mcpConnectionString</c>) as the owner, so the answer says so
/// rather than filing its statements under the service. Reads the two SECURITY DEFINER functions
/// <see cref="StoreStatementStats"/> creates; a store without them, or without the library loaded, answers
/// <c>precondition</c> with the remedy, decided from the catalog before the read rather than from an error
/// after it.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpStoreQueryStatsTools
{
    /// <summary>The default page size.</summary>
    public const int DefaultTop = 20;

    /// <summary>How much of a statement's text the default response carries.</summary>
    public const int PreviewLength = 240;

    /// <summary>What the module shows in place of a statement's text when the reader's definer may not read
    /// another role's.</summary>
    internal const string InsufficientPrivilegeText = StoreStatementStats.InsufficientPrivilegeText;

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

    /// <summary>The <c>role</c> values, and who connects as each on a managed or compose store. The viewer login is not
    /// only the web viewer: remote Darling Viewer seats default to it, and the service runs every custom-alert
    /// rule's metric on a viewer-role pool.</summary>
    internal static readonly IReadOnlyDictionary<string, string> RoleIdentities =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["owner"] = "the service: collection, maintenance and alerting (and a bring-your-own store's web viewer or MCP tools when they have no login of their own)",
            [DarlingManagedPostgres.AdminRoleName] = "the Darling Viewer on the service's machine and its Settings window, and any remote seat given the admin role",
            [DarlingManagedPostgres.ViewerRoleName] = "the web viewer, remote read-only Darling Viewer seats, and the service's custom-alert rule evaluation",
            [DarlingManagedPostgres.McpRoleName] = "MCP tools",
        };

    /// <summary>The database owner's statements report as <c>owner</c> rather than under its login name, so the
    /// label means the same thing on a managed store and on one whose owner role is called something else.</summary>
    internal const string RoleKeySql =
        "CASE WHEN f.role_name = (SELECT pg_catalog.pg_get_userbyid(d.datdba) FROM pg_catalog.pg_database AS d WHERE d.datname = pg_catalog.current_database()) THEN 'owner' ELSE f.role_name END";

    /// <summary>
    /// What the tool needs to know before it reads, from the catalog alone, so every state a store can be in is
    /// named rather than raised: whether the reader exists and this role may run it, the installed extension
    /// version (none when it is not installed, or was dropped after the reader was built), whether the library
    /// is loaded, whether utility statements are tracked, and whether this connection IS the store owner (a web
    /// or MCP host on the owner login, #3914). Loaded is read from <c>pg_settings</c>, where the
    /// module's own settings appear only once it is loaded, readable by any role (see
    /// <see cref="StoreStatementStats.ProbeSql"/>). <c>has_function_privilege</c> over a missing function is
    /// null, not an error, because <c>to_regprocedure</c> answers null and the function is strict.
    /// </summary>
    public static readonly string StateSql = $@"
SELECT
    pg_catalog.to_regprocedure('{PgSchemaGenerator.ConfigSchema}.{StoreStatementStats.FunctionName}()') IS NOT NULL AS reader_exists,
    COALESCE(pg_catalog.has_function_privilege(pg_catalog.to_regprocedure('{PgSchemaGenerator.ConfigSchema}.{StoreStatementStats.FunctionName}()'), 'EXECUTE'), false) AS may_read,
    (
        SELECT e.extversion
        FROM pg_catalog.pg_extension AS e
        WHERE e.extname = 'pg_stat_statements'
    ) AS extension_version,
    EXISTS
    (
        SELECT 1
        FROM pg_catalog.pg_settings AS s
        WHERE s.name = 'pg_stat_statements.max'
    ) AS loaded,
    (
        SELECT s.setting
        FROM pg_catalog.pg_settings AS s
        WHERE s.name = 'pg_stat_statements.track_utility'
    ) AS track_utility,
    current_user =
    (
        SELECT pg_catalog.pg_get_userbyid(d.datdba)
        FROM pg_catalog.pg_database AS d
        WHERE d.datname = pg_catalog.current_database()
    ) AS connected_as_owner";

    /// <summary>When the counters were last reset and how many eviction passes the module has run since.</summary>
    public static readonly string InfoSql =
        $"SELECT i.stats_reset, i.dealloc FROM {PgSchemaGenerator.ConfigSchema}.{StoreStatementStats.InfoFunctionName}() AS i";

    /// <summary>Each role's share of the store's statement time. The whole store's total is a window over the
    /// same grouping, so the share's denominator is the statement's own and never a sum of rows the reader
    /// happened to fetch (the page contract's rule, #3613). The hidden-text count rides the same window: how
    /// many statements the reader's definer may not read the text of.</summary>
    public static readonly string ByRoleSql = $@"
SELECT
    {RoleKeySql} AS role_key,
    pg_catalog.count(*) AS statements,
    pg_catalog.sum(f.calls)::bigint AS calls,
    pg_catalog.sum(f.total_exec_ms) AS total_exec_ms,
    pg_catalog.sum(pg_catalog.sum(f.total_exec_ms)) OVER () AS store_total_exec_ms,
    (pg_catalog.sum(pg_catalog.count(*) FILTER (WHERE f.query = '{InsufficientPrivilegeText}')) OVER ())::bigint AS hidden_text
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
        "Ranks the STORE's own SQL statements by server-side cost (pg_stat_statements), not a monitored " +
        "server's queries. No server_name: the store is the subject. Each " +
        "statement carries its role (owner, admin, viewer, mcp); by_role is each role's TIME SHARE, " +
        "not how slow it felt. Figures are cumulative since stats_since, server-side only: ms, rows, block " +
        "counts. Zero matches: a normal payload with empty arrays, not status empty. Gated: status " +
        "precondition, with the remedy, when pg_stat_statements is missing, not loaded or too old, its reader " +
        "isn't built yet, or this role has no grant. " +
        "<<GUIDE>> Ranks the monitoring STORE's own SQL statements by server-side cost, from pg_stat_statements, not a monitored server's queries. Answers 'the web viewer / MCP tools are slow: which query?'. Each statement is attributed to the role that ran it; on a managed or compose store viewer = the web viewer, remote read-only Viewer seats and custom-alert rule evaluation, mcp = MCP tools, admin = the local Darling Viewer, owner = the service (and a bring-your-own store's web viewer and MCP tools without logins of their own). by_role gives each role's share of the recorded time; statements lists the top statements with calls, total/mean/max ms, rows and block I/O, the text normalized ($1, $2) and cut to a preview unless full_text. Figures are cumulative since stats_since and server-side only; the note says what they leave out. Answers status precondition, with the remedy, when pg_stat_statements is missing, not loaded, too old or not granted. No server_name: the store is the subject.")]
    public static async Task<string> GetStoreQueryStats(
        NpgsqlDataSource postgres,
        [Description("Only statements run by this role: owner, admin, viewer or mcp. Omit for every role.")] string? role = null,
        [Description("Rank by total_time (default: where the store's time went), mean_time (slowest per call), max_time (worst single call), calls, rows, or shared_blks_read (disk reads).")] string order_by = "total_time",
        [Description("How many statements. Default 20, max 1000.")] int top = DefaultTop,
        [Description("Return each statement's full normalized text instead of a 240-character preview. Default false.")] bool full_text = false,
        CancellationToken cancellationToken = default)
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
            StatsState state;
            await using (var command = postgres.CreateCommand(StateSql))
            {
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                await reader.ReadAsync(cancellationToken);
                state = new StatsState(
                    ReaderExists: reader.GetBoolean(0),
                    MayRead: reader.GetBoolean(1),
                    ExtensionVersion: reader.IsDBNull(2) ? null : reader.GetString(2),
                    Loaded: reader.GetBoolean(3),
                    TrackUtility: reader.IsDBNull(4) ? null : reader.GetString(4),
                    ConnectedAsOwner: !reader.IsDBNull(5) && reader.GetBoolean(5));
            }

            if (PreconditionReason(state) is { } reason)
            {
                return McpHelpers.Status("precondition", reason);
            }

            DateTime? statsSince = null;
            long? evictionPasses = null;
            await using (var info = postgres.CreateCommand(InfoSql))
            {
                info.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                await using var reader = await info.ExecuteReaderAsync(cancellationToken);
                if (await reader.ReadAsync(cancellationToken))
                {
                    statsSince = reader.IsDBNull(0) ? null : reader.GetDateTime(0).ToUniversalTime();
                    evictionPasses = reader.IsDBNull(1) ? null : reader.GetInt64(1);
                }
            }

            var byRole = new List<(string Role, long Statements, long Calls, double TotalMs, double StoreTotalMs)>();
            long hiddenText = 0;
            await using (var command = postgres.CreateCommand(ByRoleSql))
            {
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    byRole.Add((
                        reader.GetString(0),
                        reader.GetInt64(1),
                        reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                        reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                        reader.IsDBNull(4) ? 0 : reader.GetDouble(4)));
                    hiddenText = reader.IsDBNull(5) ? 0 : reader.GetInt64(5);
                }
            }

            var fetched = new List<StatementRow>();
            await using (var command = postgres.CreateCommand(BuildStatementsSql(orderColumn)))
            {
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                command.Parameters.Add(new NpgsqlParameter { Value = (object?)roleKey ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
                command.Parameters.AddWithValue(top + 1);
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    var text = ShownText(reader.IsDBNull(10) ? "" : reader.GetString(10));
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
                        Query: full_text ? text : StoreStatementStats.CompactStatementText(text, PreviewLength)));
                }
            }

            var (page, truncated) = McpHelpers.BoundPage(fetched, top);
            var utilityTracked = string.Equals(state.TrackUtility, "on", StringComparison.OrdinalIgnoreCase);

            return JsonSerializer.Serialize(new
            {
                stats_since = statsSince?.ToString("O", CultureInfo.InvariantCulture),
                eviction_passes = evictionPasses,
                extension_version = state.ExtensionVersion,
                utility_statements_tracked = utilityTracked,
                connected_as_owner = state.ConnectedAsOwner,
                hidden_text_statements = hiddenText,
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
                    /* int8 goes out as a STRING (#2548's rule, as get_pg_top_queries does): most query ids exceed
                       2^53, and a JSON number is rounded by every JavaScript consumer, the web viewer's panels
                       included, into an id that matches nothing. */
                    query_id = s.QueryId?.ToString(CultureInfo.InvariantCulture),
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
                note = BuildNote(state, utilityTracked, hiddenText, evictionPasses),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_store_query_stats", ex);
        }
    }

    /// <summary>What <see cref="StateSql"/> read.</summary>
    internal sealed record StatsState(
        bool ReaderExists,
        bool MayRead,
        string? ExtensionVersion,
        bool Loaded,
        string? TrackUtility,
        bool ConnectedAsOwner);

    /// <summary>
    /// The remedy for each way a store can lack statement statistics, in the order they have to be fixed in, or
    /// null when the read can go ahead. Pure, so every branch is pinned without a server. The setup runs at every
    /// service start and again hourly (#3913: on every store shape), and each remedy says so.
    /// </summary>
    internal static string? PreconditionReason(StatsState state)
    {
        const string Cadence = "The service builds the store's statement statistics at every start and again every hour.";
        const string ByoSetup = "A store you run yourself needs pg_stat_statements in shared_preload_libraries with pg_stat_statements.track_utility = off (a restart loads it), then CREATE EXTENSION pg_stat_statements run by a superuser in this database; Darling/tools/provision-roles.sql carries the steps.";

        if (state.ExtensionVersion is null)
        {
            return state.ReaderExists
                ? "pg_stat_statements is no longer installed in this database: it was dropped after the service built its reader. The service creates it again on its next pass when it connects as a superuser; otherwise run CREATE EXTENSION pg_stat_statements as a superuser in this database. " + Cadence
                : "Statement statistics are not set up on this store: pg_stat_statements is not installed in this database. A managed store installs it itself once its conf preloads it (from its first service-owned start of a build carrying #3899). " + ByoSetup + " " + Cadence;
        }

        if (!StoreStatementStats.ExtensionVersionAtLeast(state.ExtensionVersion, StoreStatementStats.MinimumReaderVersion))
        {
            return $"pg_stat_statements {state.ExtensionVersion} is installed, older than {StoreStatementStats.MinimumReaderVersion}, the oldest version the store's reader can serve (pg_upgrade never updates an extension). Run ALTER EXTENSION pg_stat_statements UPDATE as a superuser in this database. " + Cadence;
        }

        if (!state.ReaderExists)
        {
            return "pg_stat_statements is installed, but the service has not built its reader in this database yet. " + Cadence;
        }

        if (!state.MayRead)
        {
            return "This role may not read the store's statement statistics. The service grants them to the admin, viewer and mcp roles when it builds them; any other role has no grant by design. " + Cadence;
        }

        if (!state.Loaded)
        {
            return "pg_stat_statements is installed but not loaded. shared_preload_libraries takes effect only on a store restart: a managed store loads it on its next service-owned start (the service log records when its conf preloaded it); a store you run yourself needs it in shared_preload_libraries and a restart.";
        }

        return null;
    }

    /// <summary>The note beside the figures: what they cover and, when it applies, what they cannot show.</summary>
    internal static string BuildNote(StatsState state, bool utilityTracked, long hiddenText, long? evictionPasses)
    {
        var parts = new List<string>
        {
            "Cumulative since stats_since, server-side execution only. by_role shares are of the time pg_stat_statements recorded, so a role's share is where the recorded time went, not how slow that role's calls felt. A statement that is slow per call ranks under mean_time or max_time; one that is cheap but constant ranks under total_time.",
        };

        parts.Add(utilityTracked
            ? "Utility statements are tracked on this store (pg_stat_statements.track_utility is on), so COPY, CALL, VACUUM and DDL are in these figures, each with its text withheld: a utility statement keeps its literals as typed, a password among them, so only normalized SELECT, INSERT, UPDATE, DELETE and MERGE text is shown. The store should set track_utility off."
            : "Utility statements are not tracked (pg_stat_statements.track_utility is off, as the store sets it), so COPY, CALL, VACUUM and DDL are not in these figures: the collectors' bulk ingest is COPY, so the owner's share understates collection. get_collector_cost has the collection side.");

        if (state.ConnectedAsOwner)
        {
            parts.Add("This read connected as the store owner, as a bring-your-own store's web viewer and MCP tools do until postgres.webConnectionString and postgres.mcpConnectionString give them logins of their own: their statements are counted under owner, together with the service's own.");
        }

        if (hiddenText > 0)
        {
            parts.Add($"{hiddenText.ToString(CultureInfo.InvariantCulture)} statement(s) read as {InsufficientPrivilegeText}, with no query_id: the store owner, whose rights the reader runs with, is neither a superuser nor a member of pg_read_all_stats. Granting pg_read_all_stats to the owner role shows their text, and also lets that login read every session's query text and every database's statements on the cluster, and a bring-your-own store's web viewer and MCP tools run as that login unless they have logins of their own (#3914), so on a cluster shared with other applications leave it hidden.");
        }

        if (state.ExtensionVersion is { } version
            && !StoreStatementStats.ExtensionVersionAtLeast(version, StoreStatementStats.InfoViewVersion))
        {
            parts.Add($"pg_stat_statements {version} predates pg_stat_statements_info, so stats_since and eviction_passes are unknown; ALTER EXTENSION pg_stat_statements UPDATE, run by a superuser, adds them.");
        }
        else if (evictionPasses > 0)
        {
            parts.Add("eviction_passes counts the times the module dropped its least-used entries to make room (each pass drops about 5% of pg_stat_statements.max), so a rarely-run statement may be missing and a statement re-admitted after an eviction counts only from then.");
        }

        return string.Join(" ", parts);
    }

    private static double Round(double value) => Math.Round(value, 2);

    /// <summary>
    /// The text shown for a reader row (#3920's fourth review): the two sentinels as they are, and every other
    /// text masked by the SQL lexer (<see cref="PgLogTextRedactor.RedactStoredStatement"/>), withheld when it
    /// cannot be read to its end. pg_stat_statements normally keeps a normalized text ($1 for every constant),
    /// which comes through as it was, bar a bare number (<c>ORDER BY 1</c>) and collapsed whitespace. But it keeps
    /// the RAW text, literals and all, when a statement's entry is gone at executor end: evicted during a long
    /// execution on a busy store, or reset between a protocol Parse and its Execute (<c>pgss_store</c> with no
    /// jumble state, PostgreSQL 18's pg_stat_statements.c). Such a text passes the reader's allowlist like any
    /// SELECT. A caller of the reader function in SQL still reads it unmasked.
    /// </summary>
    internal static string ShownText(string text) =>
        text.Length == 0 || text == InsufficientPrivilegeText || text == StoreStatementStats.WithheldText
            ? text
            : PgLogTextRedactor.RedactStoredStatement(text) ?? StoreStatementStats.WithheldText;

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
