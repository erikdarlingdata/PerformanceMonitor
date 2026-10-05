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
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The store's OWN hourly statement history as an MCP read (#5097): what <c>collect.store_statement_history</c> holds,
/// the per-hour change in each statement's calls and time that the service snapshots from the store's
/// <c>pg_stat_statements</c>. <c>get_store_query_stats</c> answers "what is expensive since the counters were last reset";
/// this answers "what changed, and when", across a store restart or a reset. Two modes: without <c>query_id</c> the top
/// statements over the window by the time they spent, with the mean of the newest quarter of their hours against the
/// rest; with <c>query_id</c> that statement's hourly series, oldest first. Both carry the window's capture summary, so
/// a missing hour reads as "not captured" and not as "did not run".
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpStoreQueryHistoryTools
{
    /// <summary>The default window, in hours.</summary>
    public const int DefaultHours = 24;

    /// <summary>The longest window: the history's retention.</summary>
    public const int MaxHours = StoreStatementHistory.RetentionDays * 24;

    /// <summary>The default page size of the ranked mode.</summary>
    public const int DefaultTop = 20;

    /// <summary>The share of a statement's hours (the newest) that the "recent" mean covers.</summary>
    public const double RecentShare = 0.25;

    /// <summary>The fewest hours (first_seen hours not counted) a statement needs for the recent and earlier means to be given.</summary>
    public const int MinHoursForSplit = 4;

    private const string Table = "store_statement_history";

    /// <summary>The coverage start: the earliest capture that read the extension, over all time, not only the window.</summary>
    public const string EarliestCaptureSql = @"
SELECT min(capture_time)
FROM collect.store_statement_captures
WHERE outcome IN ('" + StoreStatementHistory.OutcomeOk + "', '" + StoreStatementHistory.OutcomeRebaselined + "')";

    /// <summary>Whether the rung exists, and what the capture table holds: all captures, and those that read the extension.</summary>
    public const string ShapeSql = @"
SELECT to_regclass('collect.store_statement_history') IS NOT NULL
   AND to_regclass('collect.store_statement_captures') IS NOT NULL";

    /// <summary>All captures, and those that actually read the extension.</summary>
    public const string CaptureCountsSql = @"
SELECT count(*)::bigint,
       count(*) FILTER (WHERE outcome <> '" + StoreStatementHistory.OutcomePrecondition + @"')::bigint
FROM collect.store_statement_captures";

    /// <summary>The window's captures. <c>$1</c> the window start.</summary>
    public const string CaptureSummarySql = @"
SELECT count(*)::bigint,
       count(*) FILTER (WHERE outcome = '" + StoreStatementHistory.OutcomeRebaselined + @"')::bigint,
       count(*) FILTER (WHERE outcome = '" + StoreStatementHistory.OutcomePrecondition + @"')::bigint,
       COALESCE(sum(dealloc_delta), 0)::bigint,
       count(*) FILTER (WHERE statements_seen > statements_kept)::bigint
FROM collect.store_statement_captures
WHERE capture_time >= $1";

    /// <summary>The ranked statements. <c>$1</c> window start, <c>$2</c> the role key or null, <c>$3</c> the fetch size.</summary>
    public static readonly string RankedSql = $@"
WITH h AS
(
    SELECT {DarlingMcpStoreQueryStatsTools.RoleKeySql} AS role_key,
           f.queryid, f.capture_time, f.delta_calls, f.delta_total_exec_ms, f.delta_rows,
           f.delta_shared_blks_hit, f.delta_shared_blks_read, f.delta_temp_blks_written, f.max_exec_ms,
           f.first_seen, f.entry_restarted, f.reset_in_interval,
           /* The recent-vs-earlier split ranks only the hours that are NOT first_seen: a first_seen hour can credit a statement's whole earlier life as one hour. */
           row_number() OVER (PARTITION BY f.role_name, f.queryid, f.first_seen ORDER BY f.capture_time DESC) AS newest_rank,
           count(*) OVER (PARTITION BY f.role_name, f.queryid, f.first_seen) AS split_count
    FROM collect.store_statement_history AS f
    WHERE f.capture_time >= $1
),
w AS
(
    SELECT h.*, (NOT h.first_seen AND h.newest_rank <= ceil(h.split_count * {RecentShare.ToString(CultureInfo.InvariantCulture)})) AS recent
    FROM h
    WHERE $2::text IS NULL OR h.role_key = $2
)
SELECT w.role_key,
       w.queryid,
       sum(w.delta_calls)::bigint AS calls,
       sum(w.delta_total_exec_ms)::double precision AS total_exec_ms,
       sum(w.delta_rows)::bigint AS rows_returned,
       sum(w.delta_shared_blks_hit)::bigint AS shared_blks_hit,
       sum(w.delta_shared_blks_read)::bigint AS shared_blks_read,
       sum(w.delta_temp_blks_written)::bigint AS temp_blks_written,
       max(w.max_exec_ms)::double precision AS max_exec_ms,
       count(*)::integer AS hours_present,
       sum(w.delta_calls) FILTER (WHERE w.recent)::bigint AS recent_calls,
       sum(w.delta_total_exec_ms) FILTER (WHERE w.recent)::double precision AS recent_total_ms,
       sum(w.delta_calls) FILTER (WHERE NOT w.recent AND NOT w.first_seen)::bigint AS earlier_calls,
       sum(w.delta_total_exec_ms) FILTER (WHERE NOT w.recent AND NOT w.first_seen)::double precision AS earlier_total_ms,
       (count(*) FILTER (WHERE w.first_seen))::integer AS first_seen_hours,
       (count(*) FILTER (WHERE w.entry_restarted))::integer AS restarted_hours,
       (count(*) FILTER (WHERE w.reset_in_interval))::integer AS reset_hours,
       (count(*) FILTER (WHERE NOT w.first_seen))::integer AS split_hours
FROM w
GROUP BY w.role_key, w.queryid
ORDER BY 4 DESC, w.queryid, w.role_key
LIMIT $3";

    /// <summary>One statement's series, oldest first. <c>$1</c> window start, <c>$2</c> the role key or null, <c>$3</c> the query id.</summary>
    public static readonly string SeriesSql = $@"
SELECT f.capture_time,
       {DarlingMcpStoreQueryStatsTools.RoleKeySql} AS role_key,
       f.interval_seconds,
       f.delta_calls,
       f.delta_total_exec_ms,
       f.delta_rows,
       f.delta_shared_blks_hit,
       f.delta_shared_blks_read,
       f.delta_temp_blks_written,
       f.max_exec_ms,
       f.first_seen,
       f.entry_restarted,
       f.reset_in_interval
FROM collect.store_statement_history AS f
WHERE f.queryid = $3
AND   f.capture_time >= $1
AND   ($2::text IS NULL OR ({DarlingMcpStoreQueryStatsTools.RoleKeySql}) = $2)
ORDER BY f.capture_time, 2";

    /// <summary>
    /// The live text for some query ids. <c>$1</c> the ids. The shared sensitive-statement predicate sits on top of the
    /// reader function's own filter (#4348): a second layer for a store whose function body is older than the pattern,
    /// kept here because the diagnostics bundle reads statement text through this tool and its own text read applied it.
    /// </summary>
    /* max(query) GROUP BY queryid assumes a queryid carries the same text under every role (the same normalized statement); if two roles ever disagreed, one text is shown. The predicate wraps the chosen text, not each role's, so it is the shown text that is tested. */
    public static readonly string TextSql = @"
SELECT f.queryid, " + PgSensitiveStatementFilter.SqlPredicate("max(f.query)") + @"
FROM " + PgSchemaGenerator.ConfigSchema + "." + StoreStatementStats.FunctionName + @"() AS f
WHERE f.queryid = ANY($1)
GROUP BY f.queryid";

    /// <summary>What a statement's text reads when the reader is not usable now: the history is answered, only the text is withheld.</summary>
    public const string TextUnavailable = "text unavailable";

    /// <summary>What a statement's text reads when the extension no longer holds it.</summary>
    public const string TextGone = "text no longer in pg_stat_statements";

    [McpServerTool(Name = "get_store_query_history"), Description(
        "The STORE's own SQL statements hour by hour (pg_stat_statements deltas the service snapshots), not a " +
        "monitored server's queries. No server_name. Without query_id: top statements over hours_back by time " +
        "spent, each with calls, mean ms and the newest quarter of its hours vs the rest. With query_id: that " +
        "statement's hourly series, oldest first. Gated: status precondition before the first snapshot. " +
        "<<GUIDE>> Reads collect.store_statement_history: once an hour the service records each statement's CHANGE in calls, total time, rows and blocks since the last snapshot (the 100 that spent the most time), so a statement's mean ms per call can be read hour by hour across a store restart, a pg_stat_statements_reset() or the nightly upgrade. get_store_query_stats answers what is expensive since the counters were last reset; this answers what changed and when. Without query_id: statements ranks by total time over hours_back, with calls, mean_ms (total/calls), the mean in the newest quarter of the hours the statement appears in (recent_mean_ms) against the earlier hours (earlier_mean_ms), and hours_present; truncated says more statements exist than top. With query_id (a string, as get_store_query_stats prints it): series, one row per hour, oldest first, with interval_seconds, calls, mean_ms, total_ms, rows, shared block hits and reads, temp blocks written, max_exec_ms and three flags: first_seen (the statement's first hour in the counters; can be an upper bound, so its figures may include earlier life), entry_restarted (the extension evicted and re-admitted the entry, so the hour is a lower bound) and reset_in_interval (the counters were reset inside the hour). max_exec_ms is the extension's cumulative maximum, not the hour's. captures says how many snapshots the window holds, how many were a fresh baseline (rebaselined: no history written for that hour), how many could not read the extension (precondition), the evictions in the window (dealloc_delta_total) and the hours whose cap hid statements (capped_hours): an hour with no row may be an hour not captured. role is owner, admin, viewer or mcp; the text is the live normalized text, or says it is gone. In ranked mode first_seen_hours, restarted_hours and reset_hours count the flagged hours behind each statement's figures (calls and total_ms include them; restarted hours are lower bounds); first_seen hours are left out of recent_mean_ms and earlier_mean_ms, which are null under 4 other hours. effective_start, window_truncated and truncation_note say where the history starts relative to hours_back. History is kept 90 days and taken hourly. Answers status precondition, with the remedy, when the store predates the history, holds no snapshot yet, or when every snapshot so far could not read the extension. When history exists but the reader is not usable now, the history is still answered and only the text is withheld: query reads 'text unavailable' and text_note says why. No server_name: the store is the subject.")]
    public static Task<string> GetStoreQueryHistory(
        NpgsqlDataSource postgres,
        [Description("One statement's id, as get_store_query_stats prints it. Omit to rank the statements.")] string? query_id = null,
        [Description("Only statements run by this role: owner, admin, viewer or mcp. Omit for every role.")] string? role = null,
        [Description("Hours of history. Default 24, max 2160 (90 days).")] int hours_back = DefaultHours,
        [Description("How many statements when ranking. Default 20, max 1000.")] int top = DefaultTop,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        GetStoreQueryHistoryCoreAsync(postgres, query_id, role, hours_back, top, uncutText: false, logger, cancellationToken);

    /// <summary>
    /// The diagnostics bundle's read (#5097): the ranked answer, every statement's <c>query</c> whole. The bundle aliases the
    /// text first and cuts it afterwards, so a name that straddles the tool's own cut is never left as a prefix. Not a tool:
    /// the MCP surface, its parameters and its output are the ones <see cref="GetStoreQueryHistory"/> has.
    /// </summary>
    internal static Task<string> GetStoreQueryHistoryUncut(NpgsqlDataSource postgres, int hours_back, int top, CancellationToken cancellationToken) =>
        GetStoreQueryHistoryCoreAsync(postgres, query_id: null, role: null, hours_back, top, uncutText: true, logger: null, cancellationToken);

    private static async Task<string> GetStoreQueryHistoryCoreAsync(
        NpgsqlDataSource postgres,
        string? query_id,
        string? role,
        int hours_back,
        int top,
        bool uncutText,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        var invalidTop = McpHelpers.ValidateTop(top, "top");
        if (invalidTop != null)
        {
            return invalidTop;
        }

        /* Checked here, not by McpHelpers.ValidateHoursBack: the shared validator caps at 7 days and this history reaches back 90. */
        if (hours_back <= 0 || hours_back > MaxHours)
        {
            return McpHelpers.Refusal("hours_back",
                $"Invalid hours_back value '{hours_back}'. Must be a positive integer (1-{MaxHours.ToString(CultureInfo.InvariantCulture)}).");
        }

        string? roleKey = null;
        if (!string.IsNullOrWhiteSpace(role))
        {
            if (!DarlingMcpStoreQueryStatsTools.RoleIdentities.ContainsKey(role.Trim()))
            {
                return McpHelpers.Refusal("role",
                    $"Invalid role '{role}'. Use one of: {string.Join(", ", DarlingMcpStoreQueryStatsTools.RoleIdentities.Keys)}, or omit it for every role.");
            }

            roleKey = role.Trim().ToLowerInvariant();
        }

        long? queryId = null;
        if (!string.IsNullOrWhiteSpace(query_id))
        {
            if (!long.TryParse(query_id.Trim(), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var parsed))
            {
                return McpHelpers.Refusal("query_id",
                    $"Invalid query_id '{query_id}'. Use the query_id get_store_query_history or get_store_query_stats printed (a 64-bit integer, as text).");
            }

            queryId = parsed;
        }

        try
        {
            var windowStart = DateTime.SpecifyKind(DateTime.UtcNow.AddHours(-hours_back), DateTimeKind.Unspecified);

            var gate = await PreconditionAsync(postgres, cancellationToken);
            if (gate.Message is { } message)
            {
                return McpHelpers.Status("precondition", message);
            }

            var captures = await ReadCapturesAsync(postgres, windowStart, cancellationToken);

            if (queryId is long id)
            {
                var series = await ReadSeriesAsync(postgres, windowStart, roleKey, id, cancellationToken);
                if (series.Count == 0)
                {
                    return await EmptyAsync(postgres, "No history for this query_id in the window: it was not among the statements that spent the most time in any captured hour, or was not run.", windowStart, captures, logger, cancellationToken);
                }

                var text = gate.TextUsable ? await ReadTextAsync(postgres, [id], cancellationToken) : null;
                var notice = await ReadNoticeAsync(postgres, windowStart, emptyAnswer: false, logger, cancellationToken);
                var json = JsonSerializer.Serialize(new
                {
                    mode = "series",
                    hours_back,
                    effective_start = notice.EffectiveStart,
                    window_truncated = notice.WindowTruncated,
                    truncation_note = notice.TruncationNote,
                    role = roleKey,
                    query_id = id.ToString(CultureInfo.InvariantCulture),
                    query = ShownQuery(text, id, gate.TextUsable, uncutText),
                    text_note = gate.TextNote,
                    hours_returned = series.Count,
                    captures = CapturesShape(captures),
                    series = series.Select(s => new
                    {
                        capture_time = s.CaptureTime.ToString("O", CultureInfo.InvariantCulture),
                        role = s.Role,
                        interval_seconds = s.IntervalSeconds,
                        calls = s.Calls,
                        mean_ms = Mean(s.TotalMs, s.Calls),
                        total_ms = Round(s.TotalMs),
                        rows = s.Rows,
                        shared_blks_hit = s.BlksHit,
                        shared_blks_read = s.BlksRead,
                        temp_blks_written = s.TempWritten,
                        max_exec_ms = s.MaxMs is double m ? Round(m) : (double?)null,
                        first_seen = s.FirstSeen,
                        entry_restarted = s.EntryRestarted,
                        reset_in_interval = s.ResetInInterval,
                    }),
                    note = Note,
                }, McpHelpers.JsonOptions);
                return notice.IsUnavailable ? DarlingMcpWindowNotice.WithoutKeys(json) : json;
            }

            var fetched = await ReadRankedAsync(postgres, windowStart, roleKey, top + 1, cancellationToken);
            if (fetched.Count == 0)
            {
                return await EmptyAsync(postgres, "No statement history in the window.", windowStart, captures, logger, cancellationToken);
            }

            var (page, truncated) = McpHelpers.BoundPage(fetched, top);
            var texts = gate.TextUsable ? await ReadTextAsync(postgres, page.Select(p => p.QueryId).Distinct().ToArray(), cancellationToken) : null;
            var rankedNotice = await ReadNoticeAsync(postgres, windowStart, emptyAnswer: false, logger, cancellationToken);
            var rankedJson = JsonSerializer.Serialize(new
            {
                mode = "ranked",
                hours_back,
                effective_start = rankedNotice.EffectiveStart,
                window_truncated = rankedNotice.WindowTruncated,
                truncation_note = rankedNotice.TruncationNote,
                role = roleKey,
                top,
                text_note = gate.TextNote,
                statements_returned = page.Count,
                truncated,
                captures = CapturesShape(captures),
                statements = page.Select(s => new
                {
                    role = s.Role,
                    /* int8 goes out as a STRING (the rule get_store_query_stats follows): a JSON number is rounded by JavaScript consumers. */
                    query_id = s.QueryId.ToString(CultureInfo.InvariantCulture),
                    calls = s.Calls,
                    total_ms = Round(s.TotalMs),
                    mean_ms = Mean(s.TotalMs, s.Calls),
                    recent_mean_ms = s.SplitHours < MinHoursForSplit ? null : Mean(s.RecentTotalMs, s.RecentCalls),
                    earlier_mean_ms = s.SplitHours < MinHoursForSplit ? null : Mean(s.EarlierTotalMs, s.EarlierCalls),
                    hours_present = s.HoursPresent,
                    first_seen_hours = s.FirstSeenHours,
                    restarted_hours = s.RestartedHours,
                    reset_hours = s.ResetHours,
                    rows = s.Rows,
                    shared_blks_hit = s.BlksHit,
                    shared_blks_read = s.BlksRead,
                    temp_blks_written = s.TempWritten,
                    max_exec_ms = s.MaxMs is double m ? Round(m) : (double?)null,
                    query = ShownQuery(texts, s.QueryId, gate.TextUsable, uncutText),
                }),
                note = Note,
            }, McpHelpers.JsonOptions);
            return rankedNotice.IsUnavailable ? DarlingMcpWindowNotice.WithoutKeys(rankedJson) : rankedJson;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_store_query_history", ex);
        }
    }

    private const string Note =
        "Each row is the CHANGE since the previous hourly snapshot, so it survives a restart or a reset; the extension keeps at most the 100 statements that spent the most time in an hour, so a cheap statement can be missing from an hour it ran in. max_exec_ms is the extension's cumulative maximum, not the hour's. captures.rebaselined counts hours with no history because the counters could not be trusted. History is kept 90 days and taken hourly.";

    /// <summary>
    /// A statement's text as the answer shows it. <paramref name="uncut"/> keeps it whole (redacted, not compacted) for the
    /// diagnostics bundle, which aliases it before it cuts it (<see cref="GetStoreQueryHistoryUncut"/>).
    /// </summary>
    private static string ShownQuery(IReadOnlyDictionary<long, string>? texts, long id, bool textUsable, bool uncut)
    {
        if (!textUsable)
        {
            return TextUnavailable;
        }

        if (texts is null || !texts.TryGetValue(id, out var text) || text.Length == 0)
        {
            return TextGone;
        }

        var shown = DarlingMcpStoreQueryStatsTools.ShownText(text);
        return uncut ? shown : StoreStatementStats.CompactStatementText(shown, DarlingMcpStoreQueryStatsTools.PreviewLength);
    }

    private static double Round(double value) => Math.Round(value, 2);

    private static double? Mean(double? total, long? calls) =>
        total is double t && calls is long c && c > 0 ? Math.Round(t / c, 3) : null;

    private static object CapturesShape(CaptureSummary c) => new
    {
        count = c.Count,
        rebaselined = c.Rebaselined,
        precondition = c.Precondition,
        dealloc_delta_total = c.DeallocDelta,
        capped_hours = c.CappedHours,
    };

    /// <summary>The state of the rung: a message when the read cannot go ahead, whether the live text can be joined, and why not when it cannot.</summary>
    private static async Task<(string? Message, bool TextUsable, string? TextNote)> PreconditionAsync(NpgsqlDataSource postgres, CancellationToken ct)
    {
        await using (var shape = postgres.CreateCommand(ShapeSql))
        {
            shape.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            if (!(bool)(await shape.ExecuteScalarAsync(ct))!)
            {
                return ("This store has no statement history yet: it predates the history table (migration V163). The service migrates the store at its next start; the first snapshot is taken within an hour after that.", false, null);
            }
        }

        long all, readCaptures;
        await using (var counts = postgres.CreateCommand(CaptureCountsSql))
        {
            counts.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            await using var reader = await counts.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            all = reader.GetInt64(0);
            readCaptures = reader.GetInt64(1);
        }

        string? reason = null;
        await using (var command = postgres.CreateCommand(DarlingMcpStoreQueryStatsTools.StateSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            await using var reader = await command.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            reason = DarlingMcpStoreQueryStatsTools.PreconditionReason(new DarlingMcpStoreQueryStatsTools.StatsState(
                ReaderExists: reader.GetBoolean(0),
                MayRead: reader.GetBoolean(1),
                ExtensionVersion: reader.IsDBNull(2) ? null : reader.GetString(2),
                Loaded: reader.GetBoolean(3),
                TrackUtility: reader.IsDBNull(4) ? null : reader.GetString(4),
                ConnectedAsOwner: !reader.IsDBNull(5) && reader.GetBoolean(5)));
        }

        if (all == 0)
        {
            return ("No statement snapshot has been taken yet: the first one is taken within an hour of the service starting.", false, null);
        }

        if (readCaptures == 0)
        {
            return ("Every snapshot so far could not read pg_stat_statements, so no history exists. " + (reason ?? "The service records the reason in its log; the reader functions or the loaded library are missing for its login."), false, null);
        }

        return (null, reason is null, reason);
    }

    private static async Task<CaptureSummary> ReadCapturesAsync(NpgsqlDataSource postgres, DateTime windowStart, CancellationToken ct)
    {
        await using var command = postgres.CreateCommand(CaptureSummarySql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter { Value = windowStart, NpgsqlDbType = NpgsqlDbType.Timestamp });
        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return new CaptureSummary(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3), reader.GetInt64(4));
    }

    private static async Task<List<SeriesRow>> ReadSeriesAsync(NpgsqlDataSource postgres, DateTime windowStart, string? roleKey, long id, CancellationToken ct)
    {
        var rows = new List<SeriesRow>();
        await using var command = postgres.CreateCommand(SeriesSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter { Value = windowStart, NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)roleKey ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter { Value = id, NpgsqlDbType = NpgsqlDbType.Bigint });
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new SeriesRow(
                DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc),
                reader.GetString(1),
                reader.GetInt32(2),
                reader.GetInt64(3),
                reader.GetDouble(4),
                reader.GetInt64(5),
                reader.GetInt64(6),
                reader.GetInt64(7),
                reader.GetInt64(8),
                reader.IsDBNull(9) ? null : reader.GetDouble(9),
                reader.GetBoolean(10),
                reader.GetBoolean(11),
                reader.GetBoolean(12)));
        }

        return rows;
    }

    private static async Task<List<RankedRow>> ReadRankedAsync(NpgsqlDataSource postgres, DateTime windowStart, string? roleKey, int fetch, CancellationToken ct)
    {
        var rows = new List<RankedRow>();
        await using var command = postgres.CreateCommand(RankedSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter { Value = windowStart, NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)roleKey ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter { Value = fetch, NpgsqlDbType = NpgsqlDbType.Integer });
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new RankedRow(
                reader.GetString(0),
                reader.GetInt64(1),
                reader.GetInt64(2),
                reader.GetDouble(3),
                reader.GetInt64(4),
                reader.GetInt64(5),
                reader.GetInt64(6),
                reader.GetInt64(7),
                reader.IsDBNull(8) ? null : reader.GetDouble(8),
                reader.GetInt32(9),
                reader.IsDBNull(10) ? null : reader.GetInt64(10),
                reader.IsDBNull(11) ? null : reader.GetDouble(11),
                reader.IsDBNull(12) ? null : reader.GetInt64(12),
                reader.IsDBNull(13) ? null : reader.GetDouble(13),
                reader.GetInt32(14),
                reader.GetInt32(15),
                reader.GetInt32(16),
                reader.GetInt32(17)));
        }

        return rows;
    }

    private static async Task<IReadOnlyDictionary<long, string>> ReadTextAsync(NpgsqlDataSource postgres, long[] ids, CancellationToken ct)
    {
        var texts = new Dictionary<long, string>();
        await using var command = postgres.CreateCommand(TextSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter { Value = ids, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint });
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (!reader.IsDBNull(1))
            {
                texts[reader.GetInt64(0)] = reader.GetString(1);
            }
        }

        return texts;
    }

    /// <summary>
    /// The notice from the history's OWN start (the earliest capture that read the extension, over all time), not a collector
    /// table's probe. A failed read answers <see cref="McpWindowNotice.Unavailable"/>: it costs the keys, never the answer.
    /// </summary>
    private static readonly AsyncLocal<bool> s_failEarliestRead = new();

    /// <summary>A test's switch that makes the earliest-capture read fail, per async flow.</summary>
    internal static bool TestOnlyFailEarliestRead
    {
        get => s_failEarliestRead.Value;
        set => s_failEarliestRead.Value = value;
    }

    private static async Task<McpWindowNotice> ReadNoticeAsync(
        NpgsqlDataSource postgres, DateTime windowStart, bool emptyAnswer, ILogger? logger, CancellationToken ct)
    {
        try
        {
            DateTime? floor;
            if (s_failEarliestRead.Value)
            {
                throw new InvalidOperationException("test-only: the earliest-capture read failed");
            }

            await using (var command = postgres.CreateCommand(EarliestCaptureSql))
            {
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                var value = await command.ExecuteScalarAsync(ct);
                floor = value is DateTime d ? DateTime.SpecifyKind(d, DateTimeKind.Utc) : null;
            }

            return DarlingMcpWindowNotice.Build(
                floor, windowStart, Table, "History is kept 90 days and taken hourly.", emptyAnswer, storeSubject: true);
        }
        catch (Exception ex) when (!(ex is OperationCanceledException && ct.IsCancellationRequested))
        {
            logger?.LogWarning(ex, "The coverage read of {Table} failed; the answer goes without its window-floor notice.", Table);
            return McpWindowNotice.Unavailable;
        }
    }

    private static async Task<string> EmptyAsync(
        NpgsqlDataSource postgres, string message, DateTime windowStart, CaptureSummary captures, ILogger? logger, CancellationToken ct)
    {
        var notice = await ReadNoticeAsync(postgres, windowStart, emptyAnswer: true, logger, ct);
        return McpHelpers.Status(
            "empty",
            message,
            notice.IsUnavailable
                ? (object)new { captures = CapturesShape(captures) }
                : new { effective_start = notice.EffectiveStart, window_truncated = notice.WindowTruncated, truncation_note = notice.TruncationNote, captures = CapturesShape(captures) });
    }

    private sealed record CaptureSummary(long Count, long Rebaselined, long Precondition, long DeallocDelta, long CappedHours);

    private sealed record SeriesRow(
        DateTime CaptureTime, string Role, int IntervalSeconds, long Calls, double TotalMs, long Rows,
        long BlksHit, long BlksRead, long TempWritten, double? MaxMs, bool FirstSeen, bool EntryRestarted, bool ResetInInterval);

    private sealed record RankedRow(
        string Role, long QueryId, long Calls, double TotalMs, long Rows, long BlksHit, long BlksRead, long TempWritten,
        double? MaxMs, int HoursPresent, long? RecentCalls, double? RecentTotalMs, long? EarlierCalls, double? EarlierTotalMs,
        int FirstSeenHours, int RestartedHours, int ResetHours, int SplitHours);
}
