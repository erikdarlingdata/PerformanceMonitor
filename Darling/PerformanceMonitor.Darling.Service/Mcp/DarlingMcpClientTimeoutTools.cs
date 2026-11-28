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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The Client Timeouts read (#5640) — get_client_timeouts — over Darling's PostgreSQL store. The twin of Lite's
/// <c>McpClientTimeoutTools</c>: both run the one shared <see cref="AttentionWindowSql"/> text and judge it with the one shared
/// <see cref="ClientTimeoutsRead"/>, which applies the alert's own freshness and span rule, so the tool and the alert cannot
/// disagree about how many timeouts a window held. A STORED read (no live monitored-server hit). A PostgreSQL target answers
/// <c>not_collected</c> through the engine-capability question; a missing or denied capture session answers <c>precondition</c>.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpClientTimeoutTools
{
    private const string Collector = "attention";

    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "get_client_timeouts"), Description("Gets client timeouts (queries a client cancelled or timed out) for one server over a time range: exact count and rate per minute, plus sampled top statements, databases, client apps and recent events. Samples are a subset, never the total. empty = collector ran, none counted; unavailable = no recent run; not_collected = engine without the collector; precondition = capture session missing. hours_back is whole hours (minimum 1): for a short alert window set as_of to its end and read sampled_events' event_time. <<GUIDE>> Reading guide for get_client_timeouts. count is exact: it is the sum of the attention collector's event_counter deltas, one per run, so it holds every client cancel or timeout the capture session counted, however many events the ring buffer kept. top_statements, top_databases, top_client_apps and sampled_events come from the ring buffer, which keeps at most 1,000 events and can miss some: they are a subset, they show what the timeouts looked like and never how many there were (samples_note says so on every answer). limit cuts only sampled_events and matched_calls (sampled_events_truncated and matched_calls_truncated say it did), never count or the top lists. A reply over the 32 KB response budget lists fewer sampled_events than limit and, when matched_calls alone are over it, fewer matched_calls (budget_truncated true, budget_note names the block and says what to do). A sampled attention that lasted under 1 s used to be left out of the rate as too short to be a client timeout. It is now matched to a captured call that ended aborted (same session and database, the call ending within 100 ms of the attention): that call is the command the client cancelled, and the attention only timed the cancel, so its length is not the timeout's length. A matched sample is judged and shows matched_call: true, matched_calls in excluded_from_alert counts them, and the top-level matched_calls lists each call with how long it ran (elapsed_seconds) and its object or statement. below_duration_floor_events now counts only the samples no call matched. rate_per_minute is count divided by minutes_covered, the minutes between the oldest and newest collector run the read used, not the whole hours_back window, so minutes the collector did not cover do not dilute the rate. Status words: empty means the collector ran across the window and its counter did not move, a true zero; unavailable means no recent successful run (the newest is outside the freshness window, or fewer than two runs), so no count is claimed, because an absent row is not a zero, and get_collection_health shows why; not_collected means the collector does not run on this server's engine, which is permanent and needs no fix; precondition means the collector's latest run recorded a missing or denied capture session, and the message names the fix. An attention is a client cancel or timeout, so a high rate is a client-side symptom (the client gave up waiting), not proof the server failed: look at top_statements and their waits next (get_wait_stats, get_waiting_tasks, get_active_queries).")]
    public static async Task<string> GetClientTimeouts(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Whole hours, minimum 1, no fractions. Default 1. For an alert's few minutes set as_of to its end and read event_time in sampled_events.")] int hours_back = 1,
        [Description("Sampled events to return, newest first. Default 20. A reply can list fewer for size (budget_truncated). Never cuts the count or top lists.")] int limit = 20,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = DarlingReadWindow.Validate(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var unmet = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.DisplayName, Collector, cancellationToken);
            if (unmet != null)
            {
                return unmet;
            }

            var windowStart = windowEnd.AddHours(-hours_back);
            var (window, samples) = await ReadRangeAsync(postgres, resolved.ServerId, windowStart, windowEnd, limit, cancellationToken);
            var (kind, reading) = ClientTimeoutsRead.Classify(window, windowEnd);

            /* #7010: the window-floor notice of attention_events (Lite's twin reads the same relation). A precondition carries none, it says its own
               cause; the generic unavailable and the quiet answers carry it under hints, because a window the store does not reach back to is not a
               true negative. The unmet-engine answer above stays bare, so a collector that never ran here is not read as a quiet window. */
            Task<McpWindowNotice> NoticeAsync(bool emptyAnswer) => DarlingMcpWindowNotice.ReadAsync(
                () => DarlingMcpWindowNotice.Probe(postgres, "attention_events", resolved.ServerName, windowStart, windowEnd, cancellationToken),
                windowStart, windowEnd, "attention_events", emptyAnswer: emptyAnswer, logger: logger, cancellationToken: cancellationToken);

            if (kind == ClientTimeoutsKind.NotObserved)
            {
                /* A collector that recorded a denial or a missing session on its latest run has a specific sentence, which is better
                   evidence than the generic one. */
                var precondition = await DarlingRuntimePrecondition.StatusAsync(postgres, resolved.ServerId, resolved.DisplayName, Collector, cancellationToken);
                if (precondition is not null)
                {
                    return precondition;
                }

                var unavailableNotice = await NoticeAsync(emptyAnswer: true);
                return DarlingMcpWindowNotice.FinishEmpty(
                    McpHelpers.Status("unavailable", ClientTimeoutsRead.UnavailableMessage(resolved.DisplayName, window, windowEnd, hours_back), unavailableNotice.AsHints()),
                    unavailableNotice);
            }

            if (kind == ClientTimeoutsKind.Quiet)
            {
                var quietNotice = await NoticeAsync(emptyAnswer: true);
                return await DarlingMcpWindowNotice.FinishEmptyAsync(
                    postgres, resolved.ServerId, resolved.DisplayName, windowEnd,
                    McpHelpers.Status("empty", ClientTimeoutsRead.EmptyMessage(resolved.DisplayName, reading!, hours_back), quietNotice.AsHints()),
                    quietNotice, cancellationToken, windowStart);
            }

            /* #9593: the newest sampled events that fit the response budget (the oldest are left off whole), the same cut Lite's twin makes; the window notice
               is inside the measured reply. #9884: when the reply is still over the budget with none of them, the oldest matched calls are left off whole too,
               and count, the rates and the top lists stay whole. */
            var notice = await NoticeAsync(emptyAnswer: false);
            string Reply(ClientTimeoutsFit fit) => JsonSerializer.Serialize(new
            {
                server = resolved.DisplayName,
                hours_back,
                effective_start = notice.EffectiveStart,
                window_truncated = notice.WindowTruncated,
                truncation_note = notice.TruncationNote,
                client_timeouts = ClientTimeoutsRead.Payload(reading!, window, samples, limit, 2000, fit),
            }, McpHelpers.JsonOptions);

            return DarlingMcpWindowNotice.Finish(ClientTimeoutsRead.FitToBudget(ClientTimeoutsRead.EventsReturned(samples, limit), ClientTimeoutsRead.MatchedCallsReturned(window, limit), 2000, Reply), notice);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_client_timeouts", ex);
        }
    }

    /// <summary>
    /// The reads on ONE connection, in two steps. Step one reads the runs, the counts and the sample groups with the ceiling on
    /// ($3 = the window end plus the row skew) and the evaluator cuts them to the span. Step two reads the top lists and the newest
    /// <paramref name="sampleLimit"/> + 1 sampled events over exactly that span (<see cref="ClientTimeoutsRead.DetailSpan"/>: $2 = the
    /// exclusive span boundary, $3 = the newest run plus the skew), so they never hold the base run's samples from before the span or
    /// anything past the newest run (#5640). $1 server_id, $2/$3 naive UTC (unspecified kind, so Npgsql binds <c>timestamp</c> and never
    /// converts through the session's time zone). Every command carries the MCP read deadline (#2874).
    /// </summary>
    internal static async Task<(AttentionWindow Window, List<ClientTimeoutSample> Samples)> ReadRangeAsync(
        NpgsqlDataSource postgres, int serverId, DateTime windowStart, DateTime windowEnd, int sampleLimit, CancellationToken cancellationToken)
    {
        var floor = DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified);
        var ceiling = DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified);

        /* The collection log stamps a run when it starts; its rows carry the time the runner took a moment later. A range that ends
           between the two must still count that run's rows, the allowance AttentionEvaluator.ReadSpan itself makes (RowSkewSeconds).
           Only the run list stops at the range end; the rows get the skew past it, and ReadSpan cuts them at the newest run + skew. */
        var rowCeiling = ceiling.AddSeconds(AttentionEvaluator.RowSkewSeconds);

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);

        /* The reads that match the captured calls take them as arrays after $1 to $3 (#9881): the calls of the range are read once, as
           their own small statement, and the planner then knows how many there are. */
        const int CallsAt = 4;

        async Task<List<T>> Query<T>(string sql, DateTime lower, DateTime upper, Func<NpgsqlDataReader, T> map, Action<NpgsqlCommand>? more = null)
        {
            await using var command = new NpgsqlCommand(sql, connection);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, lower);
            command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, upper);
            more?.Invoke(command);

            var rows = new List<T>();
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                rows.Add(map(reader));
            }

            return rows;
        }

        var runs = await Query(AttentionWindowSql.Runs("collection_log", bounded: true), floor, ceiling, r => r.GetDateTime(0));
        /* #9024: while the store 277 copy runs, the older rows are in the legacy table, so the events read through the relation that counts each row once. */
        return await AttentionReadRelation.ReadAsync<(AttentionWindow Window, List<ClientTimeoutSample> Samples)>(connection, async events =>
        {
            var counts = await Query(AttentionWindowSql.Counts(events, bounded: true), floor, rowCeiling,
                r => new AttentionCountRow(r.GetDateTime(0), r.GetInt64(1)));
            var windowCalls = new AttentionCallArrays();
            await Query<long>(AttentionWindowSql.Calls(events, bounded: true), floor, rowCeiling, windowCalls.Add);
            var groups = await Query(AttentionWindowSql.SampleGroups(events, bounded: true, callsAt: CallsAt), floor, rowCeiling,
                r => new AttentionSampleGroup(
                    r.GetDateTime(0),
                    r.IsDBNull(1) ? null : r.GetString(1),
                    r.IsDBNull(2) ? null : r.GetString(2),
                    r.GetInt64(3),
                    r.GetInt64(4),
                    r.GetInt64(5),
                    r.GetInt64(6),
                    r.GetInt64(7)), windowCalls.AddTo);

            /* No span (fewer than two runs) is "not observed": the answer never shows the detail, so it is not read. */
            var span = ClientTimeoutsRead.DetailSpan(runs);
            if (span is null)
            {
                return (new AttentionWindow(runs, counts, groups, [], [], []), []);
            }

            var spanFloor = DateTime.SpecifyKind(span.Value.Floor, DateTimeKind.Unspecified);
            var spanCeiling = DateTime.SpecifyKind(span.Value.Ceiling, DateTimeKind.Unspecified);

            var statements = await Query(AttentionWindowSql.TopStatements(events, bounded: true), spanFloor, spanCeiling,
                r => new AttentionTopItem(r.GetString(0), r.GetInt64(1), r.IsDBNull(2) ? null : r.GetString(2)));
            var databases = await Query(AttentionWindowSql.TopBy(events, "database_name", bounded: true), spanFloor, spanCeiling,
                r => new AttentionTopItem(r.GetString(0), r.GetInt64(1)));
            var applications = await Query(AttentionWindowSql.TopBy(events, "client_app_name", bounded: true), spanFloor, spanCeiling,
                r => new AttentionTopItem(r.GetString(0), r.GetInt64(1)));
            var hosts = await Query(AttentionWindowSql.TopBy(events, "client_hostname", bounded: true), spanFloor, spanCeiling,
                r => new AttentionTopItem(r.GetString(0), r.GetInt64(1)));
            /* The samples and the matched calls read the calls of the span, the range their own calls CTE reads. */
            var spanCalls = new AttentionCallArrays();
            await Query<long>(AttentionWindowSql.Calls(events, bounded: true), spanFloor, spanCeiling, spanCalls.Add);
            var samples = await Query(AttentionWindowSql.Samples(events, sampleLimit + 1, callsAt: CallsAt), spanFloor, spanCeiling,
                r => new ClientTimeoutSample(
                    r.IsDBNull(0) ? null : r.GetDateTime(0),
                    r.GetDateTime(1),
                    r.IsDBNull(2) ? null : r.GetString(2),
                    r.IsDBNull(3) ? null : r.GetString(3),
                    r.IsDBNull(4) ? null : r.GetString(4),
                    r.IsDBNull(5) ? null : r.GetInt32(5),
                    r.IsDBNull(6) ? null : r.GetInt64(6),
                    r.IsDBNull(7) ? null : r.GetString(7),
                    r.IsDBNull(8) ? null : r.GetString(8),
                    r.GetBoolean(9),
                    r.GetBoolean(10),
                    r.GetBoolean(11),
                    r.GetBoolean(12)), spanCalls.AddTo);
            /* The matched calls in two steps: the ids of the calls a sample matches, then those calls by id, so no join has to guess how many match. */
            var matchedIds = await Query(AttentionWindowSql.MatchedCallIds(events, CallsAt), spanFloor, spanCeiling, r => r.GetInt64(0), spanCalls.AddTo);
            var matchedCalls = await Query(AttentionWindowSql.MatchedCallRows(events, sampleLimit + 1, idsAt: CallsAt), spanFloor, spanCeiling,
                r => new ClientTimeoutMatchedCall(
                    r.IsDBNull(0) ? null : r.GetDateTime(0),
                    r.GetDateTime(1),
                    r.IsDBNull(2) ? null : r.GetString(2),
                    r.IsDBNull(3) ? null : r.GetString(3),
                    r.IsDBNull(4) ? null : r.GetString(4),
                    r.IsDBNull(5) ? null : r.GetInt32(5),
                    r.IsDBNull(6) ? null : r.GetInt64(6),
                    r.IsDBNull(7) ? null : r.GetString(7)), command => AttentionCallArrays.AddIds(command, matchedIds));

            return (new AttentionWindow(runs, counts, groups, statements, databases, applications, TopHosts: hosts, MatchedCallList: matchedCalls), samples);
        }, cancellationToken);
    }
}
