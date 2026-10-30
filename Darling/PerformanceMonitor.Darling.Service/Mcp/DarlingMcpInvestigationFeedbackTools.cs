/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The rest of the investigation admin writes over MCP (#6400, part 2): <c>start_practice_request</c>, <c>save_environment_profile</c> and
/// <c>record_investigation_feedback</c>. Each calls the store method the web route and the Viewer call
/// (<see cref="InvestigationPracticeStore.OpenAsync"/>, <see cref="InvestigationRequestStore.SaveProfileAsync"/>,
/// <see cref="InvestigationFeedbackStore.RecordAsync"/>), so the checks, the refusals and the journal rows are one copy. Served only with
/// <c>mcp.allowWrites</c> and a token, and on <see cref="DarlingMcpWritePolicy.NeverAgentTools"/>: an investigation agent never starts a
/// practice request for itself, never edits the profile it is briefed with and never marks its own report right. The caller is the
/// token (<see cref="DarlingMcpInvestigationAdminTools.Actor"/>), never an argument.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpInvestigationFeedbackTools
{
    [McpServerTool(Name = "start_practice_request"), Description(
        "Starts a practice request: a replay of one judged case (a request that has a report and feedback on it) that an investigation agent works as if it were new, so its report can be scored against what people said of the original (the web's Start practice). Changes the monitoring store only. " +
        "<<GUIDE>>" +
        "request_id is the case's id (a GUID, from list_investigation_requests). Refused, writing nothing, for an unknown id, a case that is itself a practice request, a case with no report or no feedback, or one that already has a live practice request. " +
        "Answer: {status:\"opened\", request_id (the new practice request), source_request_id, opened_by, request: {before: null, after: {...}}, undo: null}. There is no undo: a practice request cannot be deleted; close_investigation_request closes it so no agent works it. " +
        "Needs mcp.allowWrites; an agent investigation can never call it.")]
    public static async Task<string> StartPracticeRequest(
        NpgsqlDataSource postgres,
        [Description("The id of the judged case to replay (a GUID).")] string request_id,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (!Guid.TryParse(request_id, out var source))
            {
                return McpHelpers.Refusal("request_id", "The request id is not a request id. Nothing was changed.");
            }

            var actor = DarlingMcpInvestigationAdminTools.Actor();
            if (InvestigationPersonWrites.ValidateActor(actor) is { } actorRefusal)
            {
                return McpHelpers.Refusal("request_id", actorRefusal + " Nothing was changed.");
            }

            var (outcome, practiceId, message) = await new InvestigationPracticeStore(postgres, McpCommandDeadlines.ReadSeconds)
                .OpenAsync(source, actor, cancellationToken);
            if (outcome != InvestigationPracticeOutcome.Opened)
            {
                var sentence = (message ?? "The practice request was not opened.")
                    + (practiceId is { } existing ? " Its id is " + existing.ToString("D", CultureInfo.InvariantCulture) + "." : "");
                return McpHelpers.Refusal("request_id", sentence + " Nothing was changed.");
            }

            var after = await new InvestigationRequestStore(postgres, McpCommandDeadlines.ReadSeconds, () => DateTime.UtcNow)
                .ReadRequestCloseStateAsync(practiceId!.Value, cancellationToken);
            logger?.LogInformation("MCP write start_practice_request by {Identity}: practice {PracticeId} of {Source}", ReadCaller.WriteIdentity(), practiceId, source);
            return JsonSerializer.Serialize(new
            {
                status = "opened",
                request_id = practiceId.ToString(),
                source_request_id = source.ToString(),
                opened_by = actor,
                request = new { before = (object?)null, after = after is null ? null : new { state = after.State } },
                undo = (object?)null,
                undo_note = "There is no undo: a practice request cannot be deleted. close_investigation_request closes it so no agent works it.",
            }, McpHelpers.JsonOptions);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            return McpHelpers.Status("precondition", DarlingMcpInvestigationAdminTools.PreconditionMessage);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("start_practice_request", ex);
        }
    }

    [McpServerTool(Name = "save_environment_profile"), Description(
        "Saves the environment profile of one scope: the free text every investigation brief of that scope carries about how the environment is run (the web's Save on the profile page). One profile per scope: the whole fleet, one server or one tag. A blank profile_text removes the scope's profile. Changes the monitoring store only. " +
        "<<GUIDE>>" +
        "Scope: leave server_name and tag_id out for the fleet's profile, give server_name (the name list_servers shows) for one server, or tag_id (from get_fleet_overview tags) for one tag; not both. profile_text is up to 4000 characters. " +
        "expected_modified_at guards against overwriting someone else's edit: give the modified_at of the profile you read (the 'before' of an earlier answer or the profile page), or leave it out when the scope has no profile yet. A profile changed, removed or newly created since is refused and nothing is written; the refusal names its modified_at. " +
        "Answer: {status:\"saved\"|\"removed\"|\"unchanged\", scope, profile: {before, after}, undo: null}; before and after are {profile_text, modified_at, modified_by} or null. To undo, save the 'before' profile_text again with expected_modified_at set to the 'after' modified_at (or blank it if before is null). " +
        "Needs mcp.allowWrites; an agent investigation can never call it.")]
    public static async Task<string> SaveEnvironmentProfile(
        NpgsqlDataSource postgres,
        [Description("The profile text, up to 4000 characters. Blank removes the scope's profile.")] string? profile_text,
        [Description("Server name (the name list_servers shows), or a unique part of it. Leave out, with tag_id, for the fleet's profile.")] string? server_name = null,
        [Description("A tag id (from get_fleet_overview tags). Leave out, with server_name, for the fleet's profile.")] int? tag_id = null,
        [Description("The modified_at (UTC, ISO 8601) of the profile you read for this scope. Leave out when the scope has no profile yet.")] string? expected_modified_at = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            DateTime? expected = null;
            if (!string.IsNullOrWhiteSpace(expected_modified_at))
            {
                if (!DateTime.TryParse(expected_modified_at, CultureInfo.InvariantCulture,
                        DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed))
                {
                    return McpHelpers.Refusal("expected_modified_at", "expected_modified_at must be the modified_at the profile you read showed. Nothing was changed.");
                }

                expected = parsed;
            }

            var hasServer = !string.IsNullOrWhiteSpace(server_name);
            /* Any positive id stands for "a server is named" here: the real id is resolved below, and SaveProfileAsync checks it. */
            if (InvestigationProfiles.Validate(hasServer ? 1 : null, tag_id, profile_text) is { } profileRefusal)
            {
                return McpHelpers.Refusal(tag_id is { } tag && (hasServer || tag <= 0) ? "tag_id" : "profile_text", profileRefusal + " Nothing was changed.");
            }

            var actor = DarlingMcpInvestigationAdminTools.Actor();
            if (InvestigationPersonWrites.ValidateActor(actor) is { } actorRefusal)
            {
                return McpHelpers.Refusal("profile_text", actorRefusal + " Nothing was changed.");
            }

            int? serverId = null;
            string? serverDisplay = null;
            if (hasServer)
            {
                var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
                if (error is not null)
                {
                    return error;
                }

                serverId = resolved.ServerId;
                serverDisplay = resolved.DisplayName;
            }

            var store = new InvestigationRequestStore(postgres, McpCommandDeadlines.ReadSeconds, () => DateTime.UtcNow);
            var before = await ReadProfileAsync(store, serverId, tag_id, cancellationToken);
            bool changed;
            try
            {
                /* The login is the person actor without its "person:" prefix: SaveProfileAsync builds the actor from it, as the web and the Viewer do. */
                changed = await store.SaveProfileAsync(serverId, tag_id, profile_text, actor["person:".Length..], expected, cancellationToken);
            }
            catch (InvestigationProfileConflictException ex)
            {
                var now = await ReadProfileAsync(store, serverId, tag_id, cancellationToken);
                return McpHelpers.Refusal("expected_modified_at", ex.Message
                    + (now is null ? " The scope has no profile now." : " It now stands at modified_at " + Utc(now.ModifiedAtUtc) + ".") + " Nothing was changed.");
            }
            catch (ArgumentException ex)
            {
                return McpHelpers.Refusal(ex.Message.StartsWith("Tag ", StringComparison.Ordinal) ? "tag_id" : ex.Message.StartsWith("Server ", StringComparison.Ordinal) ? "server_name" : "profile_text", ex.Message.Split('\n')[0].Replace(" (Parameter 'profileText')", "", StringComparison.Ordinal) + " Nothing was changed.");
            }

            var after = await ReadProfileAsync(store, serverId, tag_id, cancellationToken);
            var status = !changed ? "unchanged" : string.IsNullOrWhiteSpace(profile_text) ? "removed" : "saved";
            logger?.LogInformation("MCP write save_environment_profile by {Identity}: {Scope} {Status}", ReadCaller.WriteIdentity(), InvestigationProfiles.ScopeReason(serverId, tag_id), status);
            return JsonSerializer.Serialize(new
            {
                status,
                scope = new { server = serverDisplay, tag_id },
                saved_by = actor,
                profile = new { before = ProfileImage(before), after = ProfileImage(after) },
                undo = (object?)null,
                undo_note = "Save the 'before' profile_text again with expected_modified_at set to the 'after' modified_at to restore it (blank profile_text removes a profile that had no 'before').",
            }, McpHelpers.JsonOptions);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            return McpHelpers.Status("precondition", DarlingMcpInvestigationAdminTools.PreconditionMessage);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("save_environment_profile", ex);
        }
    }

    [McpServerTool(Name = "record_investigation_feedback"), Description(
        "Records feedback on an investigation report: whether it was right, partly right or wrong, what the right verdict was and what the agent missed (the web's feedback form). Feedback is what a practice request is scored against. One feedback per author per report: recording again as the same token replaces it. Changes the monitoring store only. " +
        "<<GUIDE>>" +
        "request_id is the reported request's id. verdict is right, partly or wrong. corrected_verdict (optional) is real, false_positive or needs_person. missed_step and comment (optional) are up to 4000 characters each. A request that is unknown or has no report is refused and nothing is written. " +
        "Answer: {status:\"recorded\"|\"replaced\", request_id, author, feedback: {before, after}, undo: null}. The author is the token's name, never an argument. To undo a replacement, record the 'before' values again; a first record cannot be removed from the web or the Viewer. " +
        "Needs mcp.allowWrites; an agent investigation can never call it.")]
    public static async Task<string> RecordInvestigationFeedback(
        NpgsqlDataSource postgres,
        [Description("The reported request's id (a GUID).")] string request_id,
        [Description("right, partly or wrong.")] string verdict,
        [Description("Optional corrected verdict: real, false_positive or needs_person.")] string? corrected_verdict = null,
        [Description("Optional: the step the agent missed (up to 4000 characters).")] string? missed_step = null,
        [Description("Optional comment (up to 4000 characters).")] string? comment = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (!Guid.TryParse(request_id, out var request))
            {
                return McpHelpers.Refusal("request_id", "The request id is not a request id. Nothing was changed.");
            }

            /* The caller is the token (named, or the legacy token), shaped to the author rule as a sign-in name is on the web. */
            var author = InvestigationFeedbackRules.AuthorFromIdentity("mcp " + ReadCaller.StoreTokenIdentity()) ?? "mcp";
            var candidate = new InvestigationFeedbackSubmission(
                request, verdict ?? "", string.IsNullOrWhiteSpace(corrected_verdict) ? null : corrected_verdict,
                string.IsNullOrWhiteSpace(missed_step) ? null : missed_step, string.IsNullOrWhiteSpace(comment) ? null : comment, author);
            var problems = InvestigationFeedbackRules.Validate(candidate);
            if (problems.Count > 0)
            {
                var parameter = problems.Any(p => p.Contains("corrected", StringComparison.OrdinalIgnoreCase)) ? "corrected_verdict"
                    : problems.Any(p => p.Contains("missed", StringComparison.OrdinalIgnoreCase)) ? "missed_step"
                    : problems.Any(p => p.Contains("comment", StringComparison.OrdinalIgnoreCase)) ? "comment" : "verdict";
                return McpHelpers.Refusal(parameter, InvestigationFeedbackRules.RefusalMessage(problems) + " Nothing was changed.");
            }

            var store = new InvestigationFeedbackStore(postgres, McpCommandDeadlines.ReadSeconds);
            var before = (await store.ReadAsync([request], cancellationToken))
                .FirstOrDefault(r => string.Equals(r.Author, author, StringComparison.OrdinalIgnoreCase));
            var result = await store.RecordAsync(candidate, cancellationToken);
            if (result.Outcome is not (InvestigationFeedbackOutcome.Recorded or InvestigationFeedbackOutcome.Replaced))
            {
                return McpHelpers.Refusal("request_id", (result.Message ?? "The feedback was not recorded.") + " Nothing was changed.");
            }

            var after = (await store.ReadAsync([request], cancellationToken))
                .FirstOrDefault(r => string.Equals(r.Author, author, StringComparison.OrdinalIgnoreCase));
            logger?.LogInformation("MCP write record_investigation_feedback by {Identity}: request {RequestId} {Outcome}", ReadCaller.WriteIdentity(), request, result.Outcome);
            return JsonSerializer.Serialize(new
            {
                status = result.Outcome == InvestigationFeedbackOutcome.Recorded ? "recorded" : "replaced",
                request_id = request.ToString(),
                author,
                feedback = new { before = FeedbackImage(before), after = FeedbackImage(after) },
                undo = (object?)null,
                undo_note = before is null
                    ? "There is no undo: a first feedback record cannot be removed from the web or the Viewer."
                    : "To undo, record the 'before' values again (record_investigation_feedback replaces this author's feedback).",
            }, McpHelpers.JsonOptions);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            return McpHelpers.Status("precondition", DarlingMcpInvestigationAdminTools.PreconditionMessage);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("record_investigation_feedback", ex);
        }
    }

    private static async Task<InvestigationProfileRow?> ReadProfileAsync(InvestigationRequestStore store, int? serverId, int? tagId, CancellationToken ct) =>
        (await store.ListProfilesAsync(ct)).FirstOrDefault(p => p.ScopeServerId == serverId && p.ScopeTagId == tagId);

    private static object? ProfileImage(InvestigationProfileRow? row) =>
        row is null ? null : new { profile_text = row.ProfileText, modified_at = Utc(row.ModifiedAtUtc), modified_by = row.ModifiedBy };

    private static object? FeedbackImage(InvestigationFeedbackRow? row) =>
        row is null ? null : new
        {
            verdict = row.Verdict,
            corrected_verdict = row.CorrectedVerdict,
            missed_step = row.MissedStep,
            comment = row.Comment,
            recorded_at = Utc(row.CreatedAtUtc),
        };

    private static string Utc(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Utc).ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture);
}
