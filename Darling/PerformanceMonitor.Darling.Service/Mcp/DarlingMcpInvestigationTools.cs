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
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The Level 1 investigation tools (#5632 Phase A, plan v2 section 6): an agent lists the requests alerts opened, claims
/// one (the brief is composed and stored at claim time), and returns one report under its claim; a person reads the
/// requests, reports and journal back. The two reads are ordinary reads (also on the web's <c>/api/read</c>); claim and
/// submit write only investigation state and are served only with a bearer token and <c>mcp.allowWrites</c> off
/// (<see cref="DarlingMcpWritePolicy.InvestigationTools"/>, ruling R2). Every answer passes the host's
/// <c>mcp.dataDetail</c> filter; the brief is composed masked at that level before it is stored.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpInvestigationTools
{
    /// <summary>The request states a list can filter on.</summary>
    internal static readonly string[] States = ["open", "claimed", "reported", "closed", "expired"];

    /// <summary>The investigation families (V184's check constraint).</summary>
    internal static readonly string[] Families =
        ["blocking", "query_performance", "resources", "availability", "jobs", "self_monitor", "other"];

    /// <summary>The verdicts a report may give (V184's check constraint).</summary>
    internal static readonly string[] Verdicts = ["real", "false_positive", "needs_person"];

    /// <summary>The longest proposed note (V184's check constraint).</summary>
    internal const int MaxNoteChars = 2000;

    /// <summary>The longest summary (V184's check constraint).</summary>
    internal const int MaxSummaryChars = 4000;

    /// <summary>The most requests one list returns.</summary>
    internal const int MaxLimit = 100;

    /// <summary>The most journal rows one report read returns.</summary>
    internal const int JournalLimit = 200;

    /// <summary>The agent a submit refusal is journaled under when its claim_id resolves to no claim holder.</summary>
    internal const string UnclaimedAgent = "unclaimed";

    private static InvestigationRequestStore Store(NpgsqlDataSource postgres) =>
        new(postgres, ServiceCommandDeadlines.SerialLoopSeconds);

    [McpServerTool(Name = "list_investigation_requests"), Description(
        "Lists the investigation requests alerts opened for your agent, newest first, with state, lease and expiry. Writes " +
        "nothing. status ok or empty. Claim one with claim_investigation_request. <<GUIDE>> " +
        "Each row: id, server, family, first metric, alert count, state, lease and expiry. The state is computed from the " +
        "lease and expiry at read time (a lapsed lease reads open), and the read writes nothing. " +
        "A request is opened when an alert matches a notification route whose agent_level is 1 (get_notification_routes), " +
        "within the route's hourly and daily limits; alerts of one server and family join one request inside a 30-minute " +
        "window. States: open (claimable), claimed (a live lease), reported, closed, expired (past its expiry, or out of its " +
        "three claims). Filters are optional: state, server_name (exact, then partial), family (blocking, query_performance, " +
        "resources, availability, jobs, self_monitor, other), limit (1-100, default 20).")]
    public static async Task<string> ListInvestigationRequests(
        NpgsqlDataSource postgres,
        [Description("open, claimed, reported, closed or expired. Omit for all.")] string? state = null,
        [Description("Server name. Omit for all.")] string? server_name = null,
        [Description("Alert family (see the guide). Omit for all.")] string? family = null,
        [Description("1-100. Default 20.")] int limit = 20,
        CancellationToken cancellationToken = default)
    {
        var refusal = McpHelpers.ValidateChoice(state, States, "state")
                      ?? McpHelpers.ValidateChoice(family, Families, "family");
        if (refusal is not null)
            return refusal;
        if (limit < 1 || limit > MaxLimit)
            return McpHelpers.Refusal("limit", $"Invalid limit value '{limit}'. Must be an integer from 1 to {MaxLimit}.");

        try
        {
            int? serverId = null;
            if (!string.IsNullOrWhiteSpace(server_name))
            {
                var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
                if (error != null)
                    return error;
                serverId = resolved.ServerId;
            }

            var rows = await Store(postgres).ListAsync(state, serverId, family, limit, cancellationToken);
            return JsonSerializer.Serialize(new
            {
                status = rows.Count == 0 ? "empty" : "ok",
                message = rows.Count == 0 ? "No investigation request matches these filters." : null,
                count = rows.Count,
                requests = rows.Select(RequestPayload),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("list_investigation_requests", ex);
        }
    }

    [McpServerTool(Name = "claim_investigation_request"), Description(
        "Claims an investigation request and returns its brief, claim_id and lease_until. Writes only investigation state. " +
        "Pass your claim_id again to renew. status claimed, renewed, already_claimed, not_open, not_found or " +
        "unsupported_format. <<GUIDE>> " +
        "The brief says what fired, what changed, earlier reports and the next calls to make. " +
        "A claim is one conditional update, so two agents claiming at once cannot both win; a request takes at most three " +
        "claims. The lease is the route's agent_lease_minutes; a renewal moves it on by that much, never past the request's " +
        "expiry, and only while your claim still holds a live lease (otherwise the call claims afresh, with a new claim_id). " +
        "The brief is composed at claim time, masked at mcp.dataDetail, and stored; if it cannot be composed the claim is " +
        "released (it stays counted) and the answer is status error. brief_format: omit, or 1. agent_id names your agent " +
        "(at most 64 characters) in the journal. Return your findings with submit_investigation_report under this claim_id.")]
    public static Task<string> ClaimInvestigationRequest(
        NpgsqlDataSource postgres,
        InvestigationBriefComposer composer,
        [Description("From list_investigation_requests.")] string request_id,
        [Description("Your agent's name, at most 64 characters.")] string agent_id,
        [Description("Your claim_id, to renew. Omit to claim.")] string? claim_id = null,
        [Description("Omit, or 1.")] int? brief_format = null,
        CancellationToken cancellationToken = default) =>
        ClaimAsync(Store(postgres), composer, request_id, agent_id, claim_id, brief_format, cancellationToken);

    /// <summary>The claim over a given store and composer, so a test can drive a composer that fails.</summary>
    internal static async Task<string> ClaimAsync(InvestigationRequestStore store, InvestigationBriefComposer composer,
        string request_id, string agent_id, string? claim_id, int? brief_format, CancellationToken cancellationToken)
    {
        if (!Guid.TryParse(request_id, out var requestId))
            return Status("not_found", $"request_id '{request_id}' is not a request id.");
        if (string.IsNullOrWhiteSpace(agent_id))
            return McpHelpers.Refusal("agent_id", "agent_id is required: name your agent, at most 64 characters.");

        try
        {
            if (brief_format is not null && brief_format != InvestigationFormats.BriefFormat)
            {
                await store.RecordRefusalAsync(requestId, agent_id, "claim", "unsupported_format", null, cancellationToken);
                return Status("unsupported_format",
                    $"brief_format {brief_format} is not supported. Darling composes brief_format {InvestigationFormats.BriefFormat}.");
            }

            if (!string.IsNullOrWhiteSpace(claim_id) && Guid.TryParse(claim_id, out var held))
            {
                var renewed = await store.RenewAsync(requestId, held, cancellationToken);
                if (renewed.Renewed)
                {
                    return JsonSerializer.Serialize(new
                    {
                        status = "renewed",
                        request_id = requestId,
                        claim_id = held,
                        lease_until = renewed.LeaseUntilUtc,
                    }, McpHelpers.JsonOptions);
                }
            }

            var claim = await store.ClaimAsync(requestId, agent_id, cancellationToken);
            if (claim.Outcome != InvestigationClaimOutcome.Claimed)
                return Status(ClaimStatus(claim.Outcome), ClaimMessage(claim));

            var claimId = claim.ClaimId!.Value;
            string? brief;
            try
            {
                brief = await composer.ComposeAsync(requestId, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* Fix-B ruling: the claim stays counted, so a brief that keeps failing cannot claim forever. */
                await store.ReleaseAsync(requestId, claimId, CancellationToken.None);
                await store.RecordRefusalAsync(requestId, agent_id, "claim", "brief_failed", null, CancellationToken.None);
                return McpHelpers.FormatError("claim_investigation_request",
                    new InvalidOperationException("the brief could not be composed, so the claim was released: " + ex.Message, ex));
            }

            if (brief is null || !await store.SetBriefAsync(requestId, claimId, InvestigationFormats.BriefFormat, brief, cancellationToken))
            {
                await store.ReleaseAsync(requestId, claimId, CancellationToken.None);
                return McpHelpers.FormatError("claim_investigation_request",
                    new InvalidOperationException("the brief could not be stored under this claim, so the claim was released."));
            }

            return JsonSerializer.Serialize(new
            {
                status = "claimed",
                request_id = requestId,
                claim_id = claimId,
                lease_until = claim.LeaseUntilUtc,
                claim_count = claim.ClaimCount,
                brief_format = InvestigationFormats.BriefFormat,
                report_formats = InvestigationFormats.AcceptedReportFormats,
                brief = JsonNode.Parse(brief),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("claim_investigation_request", ex);
        }
    }

    [McpServerTool(Name = "submit_investigation_report"), Description(
        "Returns your report on a request you claimed. Writes only investigation state. status reported, invalid, " +
        "unsupported_format, lease_lost, already_reported or not_found. <<GUIDE>> " +
        "A report is verdict (real, false_positive, needs_person), root_cause, evidence (the calls you made and the numbers " +
        "they showed) and summary, with an optional proposed_note for a person to approve. " +
        "evidence is a JSON array of at least one {\"tool\":\"...\",\"arguments\":{...},\"key_numbers\":\"...\"}. invalid " +
        "lists every missing or wrong field at once. Level 1 investigates and proposes nothing: any action_proposals is " +
        "refused (invalid) and journaled. A report is accepted under the current claim_id until a newer claim supersedes " +
        "it (then lease_lost), even after the lease lapsed. report_format: 1. summary at most 4,000 characters, " +
        "proposed_note at most 2,000; the note waits as pending until a person approves it, and only then reaches a later " +
        "brief. Refusals are journaled once per request and reason.")]
    public static Task<string> SubmitInvestigationReport(
        NpgsqlDataSource postgres,
        [Description("The request you claimed.")] string request_id,
        [Description("From claim_investigation_request.")] string claim_id,
        [Description("1.")] int report_format,
        [Description("real, false_positive or needs_person.")] string verdict,
        [Description("What caused the alert, or that you could not tell.")] string root_cause,
        [Description("JSON array of {tool, arguments, key_numbers}; see the guide.")] string evidence,
        [Description("At most 4,000 characters.")] string summary,
        [Description("Optional note for a person to approve, at most 2,000 characters.")] string? proposed_note = null,
        [Description("Refused at Level 1. Omit.")] string? action_proposals = null,
        CancellationToken cancellationToken = default) =>
        SubmitAsync(Store(postgres), request_id, claim_id, report_format, verdict, root_cause, evidence, summary,
            proposed_note, action_proposals, cancellationToken);

    /// <summary>The submit over a given store.</summary>
    internal static async Task<string> SubmitAsync(InvestigationRequestStore store, string request_id, string claim_id,
        int report_format, string verdict, string root_cause, string evidence, string summary, string? proposed_note,
        string? action_proposals, CancellationToken cancellationToken)
    {
        var problems = ValidateReport(request_id, claim_id, verdict, root_cause, evidence, summary, proposed_note,
            action_proposals, out var requestId, out var claimId, out var evidenceJson);
        try
        {
            var agent = requestId != Guid.Empty && claimId != Guid.Empty
                ? await store.ClaimHolderAsync(requestId, claimId, cancellationToken) ?? UnclaimedAgent
                : UnclaimedAgent;

            if (report_format < short.MinValue || report_format > short.MaxValue
                || !InvestigationFormats.IsAcceptedReportFormat((short)report_format))
            {
                if (requestId != Guid.Empty)
                    await store.RecordRefusalAsync(requestId, agent, "submit", "unsupported_format", null, cancellationToken);
                return Status("unsupported_format", InvestigationFormats.UnsupportedReportFormatMessage(
                    (short)Math.Clamp(report_format, short.MinValue, short.MaxValue)));
            }

            if (problems.Count > 0)
            {
                if (requestId != Guid.Empty)
                {
                    var reason = problems.Any(p => p.StartsWith("action_proposals", StringComparison.Ordinal))
                        ? "action_proposals" : "invalid";
                    var detail = JsonSerializer.Serialize(new { fields = problems.Select(p => p.Split(':')[0]) }, McpHelpers.JsonOptions);
                    await store.RecordRefusalAsync(requestId, agent, "submit", reason, detail, cancellationToken);
                }

                return McpHelpers.Refusal(problems[0].Split(':')[0], InvalidReportMessage(problems));
            }

            var outcome = await store.SubmitAsync(new InvestigationReportSubmission(
                requestId, claimId, agent, (short)report_format, verdict, root_cause.Trim(),
                RootCauseFound: verdict != "needs_person", summary.Trim(), evidenceJson!,
                string.IsNullOrWhiteSpace(proposed_note) ? null : proposed_note.Trim()), cancellationToken);

            return outcome switch
            {
                InvestigationSubmitOutcome.Reported => JsonSerializer.Serialize(new
                {
                    status = "reported",
                    request_id = requestId,
                    proposed_note = string.IsNullOrWhiteSpace(proposed_note) ? null : "pending",
                }, McpHelpers.JsonOptions),
                InvestigationSubmitOutcome.LeaseLost => Status("lease_lost",
                    "This claim_id no longer holds the request: a newer claim superseded it, or it never held one. The report was not stored."),
                InvestigationSubmitOutcome.AlreadyReported => Status("already_reported", "The request already holds a report."),
                InvestigationSubmitOutcome.NotOpen => Status("not_open", "The request is closed."),
                InvestigationSubmitOutcome.NotFound => Status("not_found", $"No investigation request has id {requestId}."),
                InvestigationSubmitOutcome.AgentIdTooLong => Status("agent_id_too_long",
                    $"The claim's agent id is over {InvestigationRequestStore.MaxAgentIdLength} characters."),
                _ => Status("journal_capped",
                    $"The request already holds {InvestigationRequestStore.MaxAgentJournalRows} agent journal rows; the report was refused."),
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("submit_investigation_report", ex);
        }
    }

    /// <summary>The <c>invalid</c> answer's message: every problem at once, each as <c>field: sentence</c>, so an agent
    /// fixes the report in one round; the refusal's <c>hints.parameter</c> names the first field.</summary>
    internal static string InvalidReportMessage(IReadOnlyList<string> problems) =>
        "The report was not stored. Fix every field listed: " + string.Join(" ", problems);

    /// <summary>
    /// Every problem with a report, as <c>field: sentence</c>, in parameter order: an empty list is a valid report. Lists
    /// them all at once, so an agent fixes the report in one round. Any action proposal is refused at Level 1.
    /// </summary>
    internal static List<string> ValidateReport(string? request_id, string? claim_id, string? verdict, string? root_cause,
        string? evidence, string? summary, string? proposed_note, string? action_proposals,
        out Guid requestId, out Guid claimId, out string? evidenceJson)
    {
        var problems = new List<string>();
        evidenceJson = null;
        if (!Guid.TryParse(request_id, out requestId))
            problems.Add("request_id: missing or not a request id.");
        if (!Guid.TryParse(claim_id, out claimId))
            problems.Add("claim_id: missing or not a claim id.");
        if (string.IsNullOrWhiteSpace(verdict))
            problems.Add("verdict: missing; give real, false_positive or needs_person.");
        else if (!Verdicts.Contains(verdict, StringComparer.Ordinal))
            problems.Add($"verdict: '{verdict}' is not real, false_positive or needs_person.");
        if (string.IsNullOrWhiteSpace(root_cause))
            problems.Add("root_cause: missing; say what caused the alert, or that you could not tell.");
        if (string.IsNullOrWhiteSpace(summary))
            problems.Add("summary: missing.");
        else if (summary.Trim().Length > MaxSummaryChars)
            problems.Add($"summary: {summary.Trim().Length} characters, over {MaxSummaryChars}.");
        if (!string.IsNullOrWhiteSpace(proposed_note) && proposed_note.Trim().Length > MaxNoteChars)
            problems.Add($"proposed_note: {proposed_note.Trim().Length} characters, over {MaxNoteChars}.");

        var evidenceProblem = ValidateEvidence(evidence, out evidenceJson);
        if (evidenceProblem is not null)
            problems.Add("evidence: " + evidenceProblem);

        if (!string.IsNullOrWhiteSpace(action_proposals) && action_proposals.Trim() is not ("[]" or "null"))
            problems.Add("action_proposals: refused at Level 1; an investigation proposes no action. Omit it.");

        return problems;
    }

    private static string? ValidateEvidence(string? evidence, out string? evidenceJson)
    {
        evidenceJson = null;
        if (string.IsNullOrWhiteSpace(evidence))
            return "missing; give a JSON array of at least one {tool, arguments, key_numbers}.";

        JsonNode? root;
        try
        {
            root = JsonNode.Parse(evidence);
        }
        catch (JsonException ex)
        {
            return "not valid JSON: " + ex.Message;
        }

        if (root is not JsonArray items || items.Count == 0)
            return "must be a JSON array of at least one {tool, arguments, key_numbers}.";

        var missing = new List<string>();
        for (var i = 0; i < items.Count; i++)
        {
            if (items[i] is not JsonObject item)
            {
                missing.Add($"item {i} is not an object");
                continue;
            }

            if (item["tool"] is not JsonValue tool || string.IsNullOrWhiteSpace(tool.ToString()))
                missing.Add($"item {i} has no tool");
            if (item["arguments"] is null)
                missing.Add($"item {i} has no arguments");
            if (item["key_numbers"] is null)
                missing.Add($"item {i} has no key_numbers");
        }

        if (missing.Count > 0)
            return string.Join("; ", missing) + ".";

        evidenceJson = items.ToJsonString();
        return null;
    }

    [McpServerTool(Name = "get_investigation_reports"), Description(
        "Reads investigation requests with their reports and journal, by request_id or over the last hours_back hours. " +
        "status ok or empty. <<GUIDE>> " +
        "One request_id also returns its stored brief. Briefs and reports are stored masked at mcp.dataDetail. " +
        "Each request carries its computed state (as list_investigation_requests computes it), each report its verdict, " +
        "root_cause, evidence and summary as the agent returned them: unreviewed and self-attributed, so read them as an " +
        "agent's claim, not a finding. The journal lists what happened to each request, newest first (opened, joined, " +
        "claimed, refused with its reason, lease_expired, reported, expired, claims_exhausted), at most 200 rows. " +
        "hours_back 1-168, default 24.")]
    public static async Task<string> GetInvestigationReports(
        NpgsqlDataSource postgres,
        [Description("Server name. Omit for all.")] string? server_name = null,
        [Description("One request, with its brief. Omit for the window.")] string? request_id = null,
        [Description("1-168. Default 24. Ignored with request_id.")] int hours_back = 24,
        CancellationToken cancellationToken = default)
    {
        var windowRefusal = McpHelpers.ValidateHoursBack(hours_back);
        if (windowRefusal is not null)
            return windowRefusal;

        Guid? requestId = null;
        if (!string.IsNullOrWhiteSpace(request_id))
        {
            if (!Guid.TryParse(request_id, out var parsed))
                return Status("not_found", $"request_id '{request_id}' is not a request id.");
            requestId = parsed;
        }

        try
        {
            int? serverId = null;
            if (!string.IsNullOrWhiteSpace(server_name))
            {
                var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
                if (error != null)
                    return error;
                serverId = resolved.ServerId;
            }

            var read = await Store(postgres).ReadReportsAsync(
                requestId, serverId, DateTime.UtcNow.AddHours(-hours_back), MaxLimit, JournalLimit, cancellationToken);
            if (read.Requests.Count == 0)
            {
                return requestId is not null
                    ? Status("not_found", $"No investigation request has id {requestId}.")
                    : Status("empty", $"No investigation request opened in the last {hours_back} hours.");
            }

            return JsonSerializer.Serialize(new
            {
                status = "ok",
                hours_back = requestId is null ? hours_back : (int?)null,
                request_count = read.Requests.Count,
                requests = read.Requests.Select(RequestPayload),
                reports = read.Reports.Select(r => new
                {
                    request_id = r.RequestId,
                    report_format = r.ReportFormat,
                    agent_id = r.AgentId,
                    submitted_at = r.SubmittedAtUtc,
                    verdict = r.Verdict,
                    root_cause = r.RootCause,
                    root_cause_found = r.RootCauseFound,
                    summary = r.Summary,
                    evidence = JsonNode.Parse(r.EvidenceJson),
                    labels = InvestigationBriefComposer.EarlierReportLabels,
                }),
                journal = read.Journal.Select(j => new
                {
                    event_time = j.EventTimeUtc,
                    request_id = j.RequestId,
                    server_id = j.ServerId,
                    @event = j.Event,
                    actor = j.Actor,
                    reason = j.Reason,
                    detail = j.DetailJson is null ? null : JsonNode.Parse(j.DetailJson),
                }),
                brief = read.Brief is null ? null : JsonNode.Parse(read.Brief),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_investigation_reports", ex);
        }
    }

    private static object RequestPayload(InvestigationRequestRow r) => new
    {
        request_id = r.RequestId,
        route_id = r.RouteId,
        server_id = r.ServerId,
        server_name = r.ServerName,
        family = r.Family,
        first_metric = r.FirstMetric,
        first_alert_at = r.FirstAlertAtUtc,
        last_alert_at = r.LastAlertAtUtc,
        alert_count = r.AlertCount,
        state = r.State,
        opened_at = r.OpenedAtUtc,
        expires_at = r.ExpiresAtUtc,
        claimed_by = r.ClaimedBy,
        lease_until = r.LeaseUntilUtc,
        claim_count = r.ClaimCount,
        reported_at = r.ReportedAtUtc,
    };

    internal static string ClaimStatus(InvestigationClaimOutcome outcome) => outcome switch
    {
        InvestigationClaimOutcome.Claimed => "claimed",
        InvestigationClaimOutcome.AlreadyClaimed => "already_claimed",
        InvestigationClaimOutcome.NotOpen => "not_open",
        InvestigationClaimOutcome.NotFound => "not_found",
        InvestigationClaimOutcome.AgentIdTooLong => "agent_id_too_long",
        _ => "journal_capped",
    };

    private static string ClaimMessage(InvestigationClaimResult claim) => claim.Outcome switch
    {
        InvestigationClaimOutcome.AlreadyClaimed => "Another claim holds a live lease on this request.",
        InvestigationClaimOutcome.NotOpen => claim.Reason switch
        {
            "claim_limit" => $"The request has used its {InvestigationRequestStore.MaxClaims} claims.",
            { } reason => $"The request is not open to a claim: {reason}.",
            _ => "The request is not open to a claim.",
        },
        InvestigationClaimOutcome.NotFound => "No investigation request has that id.",
        InvestigationClaimOutcome.AgentIdTooLong => $"agent_id is over {InvestigationRequestStore.MaxAgentIdLength} characters.",
        _ => $"The request already holds {InvestigationRequestStore.MaxAgentJournalRows} agent journal rows; the claim was refused.",
    };

    private static string Status(string status, string message) =>
        JsonSerializer.Serialize(new { status, message }, McpHelpers.JsonOptions);
}
