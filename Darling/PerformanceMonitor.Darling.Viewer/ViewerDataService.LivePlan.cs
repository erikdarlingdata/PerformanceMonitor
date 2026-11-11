/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's typed wrapper over the Stage-2 <c>fetch_plan</c> command — the missing live-plan path. The
/// viewer holds no SqlClient connection to a monitored server, but the SERVICE keeps a live runtime connection
/// to every one, so a "fetch this process's plan from the live cache" request rides the SAME command channel
/// <c>test_connect</c> / <c>snapshot_now</c> use: the viewer enqueues a <c>fetch_plan</c> row carrying the
/// handle to fetch (a plan_handle for a query-grid row, or a sql_handle + statement offsets for a deadlock-graph
/// / blocked-process process), the service reads the plan off the target server's plan cache, and the viewer
/// polls for the plan XML in <c>result_json</c>.
///
/// <para>Like every other command it is a WRITE (enqueue), so a read-only <c>viewer</c> seat throws
/// <see cref="ViewerReadOnlyException"/> — correct, and consistent with Pause / Snapshot: enqueuing a command is
/// a config write a read-only seat cannot make. The command row is DELETED after its terminal result is read
/// (like <c>test_connect</c>): the plan XML in <c>result_json</c> can be large and there is no reason to retain
/// it in <c>config_command</c> once the viewer has it.</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>The command type the viewer enqueues for a live plan fetch (must match the service executor's case).</summary>
    public const string CommandFetchPlan = "fetch_plan";

    private static readonly JsonSerializerOptions s_planFetchArgsOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
    };

    private static readonly JsonSerializerOptions s_planFetchResultOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    /// <summary>
    /// Builds the <c>args_json</c> for a fetch BY PLAN_HANDLE (the query-grids path — the currently-cached plan
    /// for a query/procedure row, distinct from the stored one). Pure — pinned by Darling.Tests against the
    /// service's <c>PlanFetchRequest.TryParse</c>.
    /// </summary>
    public static string BuildPlanFetchArgsByPlanHandle(string planHandle, string? databaseName)
        => JsonSerializer.Serialize(
            new PlanFetchArgs { PlanHandle = planHandle, DatabaseName = NullIfBlank(databaseName) },
            s_planFetchArgsOptions);

    /// <summary>
    /// Builds the <c>args_json</c> for a fetch BY SQL_HANDLE + statement offsets (the deadlock-graph /
    /// blocked-process path — the plan for a process whose only key is a sql_handle from an executionStack
    /// frame). Pure — pinned by Darling.Tests against the service's <c>PlanFetchRequest.TryParse</c>.
    /// </summary>
    public static string BuildPlanFetchArgsBySqlHandle(
        string sqlHandle, int statementStartOffset, int statementEndOffset, string? databaseName)
        => JsonSerializer.Serialize(
            new PlanFetchArgs
            {
                SqlHandle = sqlHandle,
                StatementStartOffset = statementStartOffset,
                StatementEndOffset = statementEndOffset,
                DatabaseName = NullIfBlank(databaseName),
            },
            s_planFetchArgsOptions);

    /// <summary>
    /// Enqueues a <c>fetch_plan</c> with the given <paramref name="argsJson"/> (built by one of the Build*
    /// helpers), polls for its terminal result, deletes the (possibly large) command row, and maps the outcome
    /// to a <see cref="LivePlanFetchResult"/>. A read-only seat throws <see cref="ViewerReadOnlyException"/> from
    /// the enqueue (a read-only seat cannot command the service). Returns <see cref="LivePlanFetchStatus.TimedOut"/>
    /// when the service does not report within the budget (the caller shows "still fetching / try again").
    /// </summary>
    public async Task<LivePlanFetchResult> FetchLivePlanAsync(
        int serverId, string argsJson, CancellationToken cancellationToken = default)
    {
        var commandId = await EnqueueCommandAsync(CommandFetchPlan, serverId, argsJson, RequestedBy(), cancellationToken);
        var result = await PollCommandResultAsync(commandId, DefaultCommandTimeout, cancellationToken);

        if (result is not null)
        {
            try
            {
                await DeleteCommandAsync(commandId, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* The plan-bearing row could not be cleaned up now; the service-side reaper is the backstop.
                   Never let cleanup hide the plan the user is waiting on. */
            }
        }

        return ParsePlanFetchResult(result);
    }

    /// <summary>
    /// Maps a polled command result to a <see cref="LivePlanFetchResult"/> (pure — pinned by Darling.Tests):
    /// a null poll → <see cref="LivePlanFetchStatus.TimedOut"/>; a <c>succeeded</c> row → the plan XML from
    /// <c>result_json</c> (or <see cref="LivePlanFetchStatus.NotInCache"/> when it is null); a <c>failed</c> row →
    /// <see cref="LivePlanFetchStatus.Failed"/> with the service's error / status message.
    /// </summary>
    public static LivePlanFetchResult ParsePlanFetchResult(CommandResult? result)
    {
        if (result is null)
        {
            return new LivePlanFetchResult(LivePlanFetchStatus.TimedOut, null,
                "The service did not return the plan in time. It may still be fetching. Try again.");
        }

        if (result.Status == StatusSucceeded)
        {
            var planXml = ReadStringField(result.ResultJson, "planXml");
            return string.IsNullOrEmpty(planXml)
                ? new LivePlanFetchResult(LivePlanFetchStatus.NotInCache, null,
                    "The plan is no longer in the server's plan cache (it was evicted or recompiled since it was captured).")
                : new LivePlanFetchResult(LivePlanFetchStatus.Fetched, planXml, null);
        }

        var error = ReadStringField(result.ResultJson, "error");
        return new LivePlanFetchResult(LivePlanFetchStatus.Failed, null,
            !string.IsNullOrEmpty(error) ? error
            : !string.IsNullOrEmpty(result.ResultStatus) ? result.ResultStatus
            : "The service could not fetch the plan.");
    }

    private static string? ReadStringField(string? json, string field)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return null;
        }

        try
        {
            using var document = JsonDocument.Parse(json);
            if (document.RootElement.ValueKind == JsonValueKind.Object
                && document.RootElement.TryGetProperty(field, out var element)
                && element.ValueKind == JsonValueKind.String)
            {
                return element.GetString();
            }
        }
        catch (JsonException)
        {
            /* Malformed result_json — degrade to null. */
        }

        return null;
    }

    private static string? NullIfBlank(string? value) => string.IsNullOrWhiteSpace(value) ? null : value;

    /// <summary>The wire shape of a <c>fetch_plan</c> <c>args_json</c> (camelCase to match the service's parser).</summary>
    private sealed class PlanFetchArgs
    {
        public string? PlanHandle { get; set; }
        public string? SqlHandle { get; set; }
        public int? StatementStartOffset { get; set; }
        public int? StatementEndOffset { get; set; }
        public string? DatabaseName { get; set; }
    }
}

/// <summary>The terminal state of a live plan fetch — the viewer's plan handlers switch on it.</summary>
public enum LivePlanFetchStatus
{
    /// <summary>The plan XML was read from the live cache.</summary>
    Fetched,

    /// <summary>The fetch ran but the plan is no longer cached (evicted / recompiled).</summary>
    NotInCache,

    /// <summary>The service reported a failure (server not connected, connect / SQL error).</summary>
    Failed,

    /// <summary>The service did not report a terminal result within the poll budget.</summary>
    TimedOut,
}

/// <summary>A live plan fetch's outcome: the plan XML on success, else a message to surface.</summary>
public sealed record LivePlanFetchResult(LivePlanFetchStatus Status, string? PlanXml, string? Message);
