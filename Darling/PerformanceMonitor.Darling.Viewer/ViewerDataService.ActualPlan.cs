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
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's typed wrapper over the Stage-2 <c>execute_actual_plan</c> command — the "Get Actual Plan"
/// path. Where <c>fetch_plan</c> (<see cref="FetchLivePlanAsync"/>) READS a cached plan, this RE-EXECUTES the
/// stored query on the target (SET STATISTICS XML ON) so the plan comes back with runtime stats. The viewer holds
/// no SqlClient connection, so — exactly like <c>fetch_plan</c> — the request rides the command channel to the
/// SERVICE, which holds the live connection.
///
/// <para><b>Identifier-only payload (security):</b> the args carry ONLY the stored-row key (query_hash +
/// database_name), never the SQL text. The service resolves the text + estimated plan from its own store before
/// re-executing, so a command writer can never make the service run arbitrary SQL against a target (see
/// <c>ActualPlanRequest</c>). <b>Read-only seats:</b> enqueuing a command is a config write, so a read-only
/// <c>viewer</c> seat throws <see cref="ViewerReadOnlyException"/> from the enqueue (same rule as Pause / Snapshot
/// / Fetch-Live-Plan). The command row is DELETED after its terminal result is read (the plan XML can be large).</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>The command type the viewer enqueues for an actual-plan capture (must match the service executor's case).</summary>
    public const string CommandExecuteActualPlan = "execute_actual_plan";

    /// <summary>
    /// Builds the <c>args_json</c> for a <c>query_stats</c> actual-plan capture — the stored-row key
    /// (query_hash + database_name) plus the SHA-256 digest of the FULL query text the row holds, NO query text
    /// itself. Serves Top Queries, Query-Stats history, and FinOps High Impact. The service resolves the latest
    /// stored text for the hash and runs it only when its digest equals this one (#5639), so the statement that
    /// runs is the statement the prompt showed. Pure — pinned by Darling.Tests against the service's
    /// <c>ActualPlanRequest.TryParse</c> and asserted to carry no SQL text.
    /// </summary>
    public static string BuildActualPlanArgs(string queryHash, string? databaseName, string queryText)
        => JsonSerializer.Serialize(
            new ActualPlanArgs
            {
                QueryHash = queryHash,
                DatabaseName = NullIfBlank(databaseName),
                ConfirmedSha256 = QueryTextDigest.Compute(queryText),
            },
            s_planFetchArgsOptions);

    /// <summary>
    /// The viewer-side gate for a <c>query_stats</c> actual-plan request (#5639): builds the args only for a row
    /// whose hash names a particular query (not empty, not all zeros) and that holds query text to confirm.
    /// Otherwise returns false with the message to show, and no request is made.
    /// </summary>
    public static bool TryBuildActualPlanArgs(
        string? queryHash, string? databaseName, string? queryText, out string argsJson, out string refusal)
    {
        argsJson = "";
        if (!QueryModificationDetector.IsUsableQueryHash(queryHash))
        {
            refusal = "This row has no usable query hash (it is empty or all zeros), so the stored text cannot be tied to "
                + "the statement you would confirm. Nothing was run.";
            return false;
        }

        if (string.IsNullOrEmpty(queryText))
        {
            refusal = "This row has no query text to confirm. Nothing was run.";
            return false;
        }

        argsJson = BuildActualPlanArgs(queryHash!, databaseName, queryText);
        refusal = "";
        return true;
    }

    /// <summary>
    /// Builds the <c>args_json</c> for a Query Store actual-plan capture — the stored-row key ONLY (query_id +
    /// database_name), NO query text. The service resolves the text from <c>query_store_stats</c>. Pure.
    /// </summary>
    public static string BuildActualPlanArgsForQueryStore(long queryId, string? databaseName)
        => JsonSerializer.Serialize(
            new ActualPlanArgs { QueryId = queryId, DatabaseName = NullIfBlank(databaseName) },
            s_planFetchArgsOptions);

    /// <summary>
    /// Builds the <c>args_json</c> for a Wait drill-down (query snapshot) actual-plan capture — the stored-row key
    /// ONLY (collection_time + session_id, plus database for the USE context), NO query text. The service resolves
    /// the text from <c>query_snapshots</c> by the exact capture time + session. Pure.
    /// </summary>
    public static string BuildActualPlanArgsForSnapshot(DateTime collectionTime, int sessionId, string? databaseName)
        => JsonSerializer.Serialize(
            new ActualPlanArgs
            {
                SnapshotCollectionTime = DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified),
                SnapshotSessionId = sessionId,
                DatabaseName = NullIfBlank(databaseName),
            },
            s_planFetchArgsOptions);

    /// <summary>
    /// Enqueues an <c>execute_actual_plan</c> with the given <paramref name="argsJson"/> (built by
    /// <see cref="BuildActualPlanArgs"/>), polls for its terminal result (the re-execution can take a while, so
    /// the imperative 3-minute budget), deletes the (possibly large) command row, and maps the outcome. A
    /// read-only seat throws <see cref="ViewerReadOnlyException"/> from the enqueue.
    /// </summary>
    public async Task<ActualPlanResult> ExecuteActualPlanAsync(
        int serverId, string argsJson, CancellationToken cancellationToken = default)
    {
        var commandId = await EnqueueCommandAsync(CommandExecuteActualPlan, serverId, argsJson, RequestedBy(), cancellationToken);
        var result = await PollCommandResultAsync(commandId, ImperativeCommandTimeout, cancellationToken);

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

        return ParseActualPlanResult(result);
    }

    /// <summary>
    /// Maps a polled command result to an <see cref="ActualPlanResult"/> (pure — pinned by Darling.Tests):
    /// a null poll → <see cref="ActualPlanStatus.TimedOut"/>; a <c>succeeded</c> row → the plan XML from
    /// <c>result_json</c> (or <see cref="ActualPlanStatus.NoPlanCaptured"/> when it is null); a <c>failed</c> row →
    /// <see cref="ActualPlanStatus.Failed"/> with the service's legible error (permission / timeout / SQL error).
    /// </summary>
    public static ActualPlanResult ParseActualPlanResult(CommandResult? result)
    {
        if (result is null)
        {
            return new ActualPlanResult(ActualPlanStatus.TimedOut, null,
                "The service did not return the actual plan in time. The query may still be running — try again.");
        }

        if (result.Status == StatusSucceeded)
        {
            var planXml = ReadStringField(result.ResultJson, "planXml");
            return string.IsNullOrEmpty(planXml)
                ? new ActualPlanResult(ActualPlanStatus.NoPlanCaptured, null,
                    "The query executed but no actual execution plan was captured.")
                : new ActualPlanResult(ActualPlanStatus.Captured, planXml, null);
        }

        var error = ReadStringField(result.ResultJson, "error");
        return new ActualPlanResult(ActualPlanStatus.Failed, null,
            !string.IsNullOrEmpty(error) ? error
            : !string.IsNullOrEmpty(result.ResultStatus) ? result.ResultStatus
            : "The service could not capture the actual plan.");
    }

    /// <summary>The wire shape of an <c>execute_actual_plan</c> <c>args_json</c> — the stored-row key only (one
    /// identifier kind per request; null fields are omitted). There is deliberately NO query-text field: the
    /// service resolves the text from its own store.</summary>
    private sealed class ActualPlanArgs
    {
        public string? QueryHash { get; set; }
        public long? QueryId { get; set; }
        public DateTime? SnapshotCollectionTime { get; set; }
        public int? SnapshotSessionId { get; set; }
        public string? DatabaseName { get; set; }
        public string? ConfirmedSha256 { get; set; }
    }
}

/// <summary>The terminal state of an actual-plan capture — the viewer's handler switches on it.</summary>
public enum ActualPlanStatus
{
    /// <summary>The query was re-executed and its actual plan XML captured.</summary>
    Captured,

    /// <summary>The query ran but SQL Server returned no plan (e.g. a statement that produced none).</summary>
    NoPlanCaptured,

    /// <summary>The service reported a failure (permission denied, SQL error, not connected).</summary>
    Failed,

    /// <summary>The service did not report a terminal result within the poll budget (still running).</summary>
    TimedOut,
}

/// <summary>An actual-plan capture's outcome: the plan XML on success, else a message to surface.</summary>
public sealed record ActualPlanResult(ActualPlanStatus Status, string? PlanXml, string? Message);
