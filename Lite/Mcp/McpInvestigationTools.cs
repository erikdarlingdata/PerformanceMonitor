/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Text.Json;
using System.Text.Json.Nodes;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The Level 1 investigation tools on Lite (#5820): the twin of Darling's <c>DarlingMcpInvestigationTools</c>, with the same
/// names, parameters, status words and answers. An agent lists the requests alerts opened, claims one (the brief is
/// composed and stored at claim time), and returns one report under its claim; a person reads the requests, reports and
/// journal back. The four are served only with <c>mcp_allow_investigations</c> on and the write tools off
/// (<see cref="McpInvestigationPolicy"/>, by tool name). The shared <see cref="InvestigationToolCore"/> does the work; these
/// are thin wrappers over the registered <see cref="IInvestigationRequestStore"/> (Lite's DuckDB store).
/// </summary>
[McpServerToolType]
public sealed class McpInvestigationTools
{
    /// <summary>The product word the unsupported-format refusals name.</summary>
    internal const string Product = "Lite";

    [McpServerTool(Name = "list_investigation_requests"), Description(
        "Lists the investigation requests alerts opened for your agent, newest first, with state, lease and expiry. Writes " +
        "nothing. status ok or empty. Claim one with claim_investigation_request. <<GUIDE>> " +
        "Each row: id, server, family, first metric, alert count, state, lease and expiry. The state is computed from the " +
        "lease and expiry at read time (a lapsed lease reads open), and the read writes nothing. " +
        "A request is opened when an alert matches a notification route whose agent_level is 1 (get_notification_routes), " +
        "within the route's hourly and daily limits; alerts of one server and family join one request while they keep firing (each within 30 minutes of the request's latest alert, up to 4 hours after its first). " +
        "States: open (claimable), claimed (a live lease), reported, closed, expired (past its expiry, or out of its " +
        "three claims). Filters are optional: state, server_name (exact, then partial), family (blocking, query_performance, " +
        "resources, availability, jobs, self_monitor, other), limit (1-100, default 20). requests_returned is how many rows " +
        "you got; truncated says more requests matched than limit (raise limit or narrow the filters).")]
    public static async Task<string> ListInvestigationRequests(
        ServerManager serverManager,
        IInvestigationRequestStore store,
        [Description("open, claimed, reported, closed or expired. Omit for all.")] string? state = null,
        [Description("Server name. Omit for all.")] string? server_name = null,
        [Description("Alert family (see the guide). Omit for all.")] string? family = null,
        [Description("1-100. Default 20.")] int limit = 20,
        CancellationToken cancellationToken = default)
    {
        var refusal = McpHelpers.ValidateChoice(state, InvestigationToolCore.States, "state")
                      ?? McpHelpers.ValidateChoice(family, InvestigationToolCore.Families, "family");
        if (refusal is not null)
            return refusal;
        if (limit < 1 || limit > InvestigationToolCore.MaxLimit)
            return McpHelpers.Refusal("limit", $"Invalid limit value '{limit}'. Must be an integer from 1 to {InvestigationToolCore.MaxLimit}.");

        try
        {
            int? serverId = null;
            if (!string.IsNullOrWhiteSpace(server_name))
            {
                var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
                if (error != null)
                    return error;
                serverId = resolved.ServerId;
            }

            /* The #3699 shape: read one row past the limit, return the page, and say whether more matched. */
            var fetched = await store.ListAsync(state, serverId, family, limit + 1, cancellationToken);
            var truncated = fetched.Count > limit;
            IReadOnlyList<InvestigationRequestRow> rows = truncated ? fetched.Take(limit).ToList() : fetched;
            return JsonSerializer.Serialize(new
            {
                status = rows.Count == 0 ? "empty" : "ok",
                message = rows.Count == 0 ? "No investigation request matches these filters." : null,
                requests_returned = rows.Count,
                truncated,
                requests = rows.Select(InvestigationToolCore.RequestPayload),
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
        "The brief is composed at claim time, masked at mcp_data_detail, and stored; if it cannot be composed the claim is " +
        "released (it stays counted) and the answer is status error. brief_format: omit, or 1. agent_id names your agent " +
        "in the journal: 1-64 letters, digits, '.', '_', ':' or '-'. Return your findings with submit_investigation_report under this claim_id.")]
    public static Task<string> ClaimInvestigationRequest(
        IInvestigationRequestStore store,
        InvestigationBriefComposer composer,
        [Description("From list_investigation_requests.")] string request_id,
        [Description("Your agent name: 1-64 of A-Za-z0-9._:-")] string agent_id,
        [Description("Your claim_id, to renew. Omit to claim.")] string? claim_id = null,
        [Description("Omit, or 1.")] int? brief_format = null,
        CancellationToken cancellationToken = default) =>
        ClaimAsync(store, composer, request_id, agent_id, claim_id, brief_format, cancellationToken);

    /// <summary>The claim over a given store and composer, so a test can drive a composer that fails. The brief is composed
    /// UNLOCKED, between the claim and storing it; a compose failure releases the claim (it stays counted).</summary>
    internal static async Task<string> ClaimAsync(IInvestigationRequestStore store, InvestigationBriefComposer composer,
        string request_id, string agent_id, string? claim_id, int? brief_format, CancellationToken cancellationToken)
    {
        if (!Guid.TryParse(request_id, out var requestId))
            return InvestigationToolCore.Status("not_found", $"request_id '{request_id}' is not a request id.");

        try
        {
            /* An agent id is a name, not text, so it cannot pose as another journal line. */
            if (!InvestigationRules.IsValidAgentId(agent_id))
            {
                await store.RecordRefusalAsync(requestId, InvestigationRules.InvalidAgentId, "claim", "agent_id_invalid", null,
                    cancellationToken);
                return McpHelpers.Refusal("agent_id", InvestigationRules.AgentIdRule);
            }

            if (brief_format is not null && brief_format != InvestigationFormats.BriefFormat)
            {
                await store.RecordRefusalAsync(requestId, agent_id, "claim", "unsupported_format", null, cancellationToken);
                return InvestigationToolCore.Status("unsupported_format",
                    $"brief_format {brief_format} is not supported. {Product} composes brief_format {InvestigationFormats.BriefFormat}.");
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
                return InvestigationToolCore.Status(InvestigationToolCore.ClaimStatus(claim.Outcome), InvestigationToolCore.ClaimMessage(claim));

            var claimId = claim.ClaimId!.Value;
            string? brief;
            try
            {
                brief = await composer.ComposeAsync(requestId, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* The claim stays counted, so a brief that keeps failing cannot claim forever. */
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
        "evidence is a JSON array of at least one {\"tool\":\"...\",\"arguments\":{...},\"key_numbers\":\"...\",\"basis\":\"" + InvestigationReportFields.BasesAlternation + "\"}: " +
        "basis says whether you checked the claim against data (verified) or reasoned to it (inferred), and a reader sees the two apart. invalid " +
        "lists every missing or wrong field at once. Level 1 investigates and proposes nothing: any action_proposals is " +
        "refused (invalid) and journaled. A report is accepted under the current claim_id until a newer claim supersedes " +
        "it (then lease_lost), even after the lease lapsed. report_format: 1. summary at most 4,000 characters, " +
        "proposed_note at most 2,000, root_cause at most 8,000; evidence at most 50 items, each string value at most 1,000 " +
        "characters, 32 KB in all (over a cap: status refused, naming the field and the cap). The note waits as pending until a person approves it, and only then reaches a later " +
        "brief. Refusals are journaled once per request and reason. " +
        "links (optional) is a JSON array of at most 10 {\"kind\":\"" + InvestigationReportLinks.KindsAlternation + "\",\"url\":\"https://...\",\"key\":\"OPS-123\"}: a ticket, " +
        "pull request, bug report, commit or release you opened or found. A url must be https with no user name or password; http, file and UNC " +
        "(\\\\server\\share, //server/share) links are refused (invalid). Each URL is journaled once per request as link_recorded. " +
        "Lite stores the link and never calls Jira or git. " +
        "family_of (optional) is the request_id of an earlier request on this store, which need not have a report yet; it must name a request (else invalid). " +
        "queries_run (optional) is a JSON array of the statements you ran, " + InvestigationReportFields.QueriesRunRule + " (over a cap: refused, never cut); " +
        "it is masked as statement text. " +
        "follow_up_at (optional) asks Lite to open a new request for this case later: " + InvestigationReportFields.FollowUpRule + ". " +
        "When the time comes the new request names this one (follow_up_of), its brief carries this report, and one follow_up event is " +
        "journaled; Lite sends no event, so find it in list_investigation_requests. Lite takes a time only, no deploy condition. " +
        "A time in the past, more than 30 days ahead, or a route already holding 20 pending follow-ups is refused (invalid), and nothing is stored.")]
    public static Task<string> SubmitInvestigationReport(
        IInvestigationRequestStore store,
        [Description("The request you claimed.")] string request_id,
        [Description("From claim_investigation_request.")] string claim_id,
        [Description("1.")] int report_format,
        [Description("real, false_positive or needs_person.")] string verdict,
        [Description("What caused the alert, or that you could not tell.")] string root_cause,
        [Description("JSON array of {tool, arguments, key_numbers, basis}; basis (" + InvestigationReportFields.BasesAlternation + ") is required on each item; see the guide.")] string evidence,
        [Description("At most 4,000 characters.")] string summary,
        [Description("Optional note for a person to approve, at most 2,000 characters.")] string? proposed_note = null,
        [Description("Refused at Level 1. Omit.")] string? action_proposals = null,
        [Description("Optional JSON array of at most 10 {kind, url, key}; https only. See the guide.")] string? links = null,
        [Description("Optional. The request_id of an earlier request on this store; it need not have a report yet.")] string? family_of = null,
        [Description("Optional JSON array of statements you ran, " + InvestigationReportFields.QueriesRunRule + ".")] string? queries_run = null,
        [Description("Optional. When to open a follow-up request: " + InvestigationReportFields.FollowUpRule + ".")] string? follow_up_at = null,
        CancellationToken cancellationToken = default) =>
        SubmitAsync(store, request_id, claim_id, report_format, verdict, root_cause, evidence, summary,
            proposed_note, action_proposals, cancellationToken, links, family_of, queries_run, follow_up_at);

    /// <summary>The <c>reported</c> answer: <c>links_recorded</c> only when the report carried a link (Darling's answer, #5914).</summary>
    private static string ReportedAnswer(Guid requestId, string? proposedNote, int linksRecorded)
    {
        var answer = new JsonObject
        {
            ["status"] = "reported",
            ["request_id"] = requestId.ToString(),
            ["proposed_note"] = string.IsNullOrWhiteSpace(proposedNote) ? null : "pending",
        };
        if (linksRecorded > 0)
            answer["links_recorded"] = linksRecorded;
        return answer.ToJsonString(McpHelpers.JsonOptions);
    }

    /// <summary>The submit over a given store.</summary>
    internal static async Task<string> SubmitAsync(IInvestigationRequestStore store, string request_id, string claim_id,
        int report_format, string verdict, string root_cause, string evidence, string summary, string? proposed_note,
        string? action_proposals, CancellationToken cancellationToken, string? links = null, string? family_of = null,
        string? queries_run = null, string? follow_up_at = null)
    {
        var problems = InvestigationToolCore.ValidateReport(request_id, claim_id, verdict, root_cause, evidence, summary, proposed_note,
            action_proposals, out var requestId, out var claimId, out var evidenceJson, requireBasis: true);
        /* #5914/#6004 (ported by #6011): links, family_of and queries_run ride after the other fields, in Darling's order. */
        if (InvestigationReportLinks.Validate(links, out var reportLinks) is { } linkProblem)
            problems.Add("links: " + linkProblem);
        if (InvestigationReportFields.ValidateFamilyOf(family_of, out var familyOf) is { } familyProblem)
            problems.Add("family_of: " + familyProblem);

        if (InvestigationReportFields.Validate(queries_run, out var queriesRunJson) is { } queriesProblem)
            problems.Add("queries_run: " + queriesProblem);
        /* #6012 (Darling's #6008): follow_up_at after queries_run, for the same reason. The route's pending cap is the store's. */
        if (InvestigationReportFields.ValidateFollowUp(follow_up_at, DateTime.UtcNow, out var followUpAtUtc) is { } followUpProblem)
            problems.Add("follow_up_at: " + followUpProblem);
        try
        {
            var agent = requestId != Guid.Empty && claimId != Guid.Empty
                ? await store.ClaimHolderAsync(requestId, claimId, cancellationToken) ?? InvestigationToolCore.UnclaimedAgent
                : InvestigationToolCore.UnclaimedAgent;

            if (report_format < short.MinValue || report_format > short.MaxValue
                || !InvestigationFormats.IsAcceptedReportFormat((short)report_format))
            {
                if (requestId != Guid.Empty)
                    await store.RecordRefusalAsync(requestId, agent, "submit", "unsupported_format", null, cancellationToken);
                return InvestigationToolCore.Status("unsupported_format", InvestigationFormats.UnsupportedReportFormatMessage(
                    (short)Math.Clamp(report_format, short.MinValue, short.MaxValue), Product));
            }

            /* The caps, before any write, journaled once per request and reason. */
            var oversize = InvestigationToolCore.CapProblems(root_cause, evidence, queries_run);
            if (oversize.Count > 0)
            {
                if (requestId != Guid.Empty)
                {
                    var detail = JsonSerializer.Serialize(new { fields = oversize.Select(p => p.Field).Distinct() }, McpHelpers.JsonOptions);
                    await store.RecordRefusalAsync(requestId, agent, "submit", "too_large", detail, cancellationToken);
                }

                return McpHelpers.Status("refused",
                    "The report was not stored: it is over a size cap. " + string.Join(" ", oversize.Select(p => p.Problem)),
                    new { parameter = oversize[0].Field, cap = oversize[0].Cap });
            }

            if (familyOf is { } family && !await store.RequestExistsAsync(family, cancellationToken))
                problems.Add($"family_of: no investigation request has id {family}; give the request_id of an earlier request on this store.");

            if (problems.Count > 0)
            {
                if (requestId != Guid.Empty)
                {
                    var reason = problems.Any(p => p.StartsWith("action_proposals", StringComparison.Ordinal))
                        ? "action_proposals" : "invalid";
                    var detail = JsonSerializer.Serialize(new { fields = problems.Select(p => p.Split(':')[0]) }, McpHelpers.JsonOptions);
                    await store.RecordRefusalAsync(requestId, agent, "submit", reason, detail, cancellationToken);
                }

                return McpHelpers.Refusal(problems[0].Split(':')[0], InvestigationToolCore.InvalidReportMessage(problems));
            }

            var outcome = await store.SubmitAsync(new InvestigationReportSubmission(
                requestId, claimId, agent, (short)report_format, verdict, root_cause.Trim(),
                RootCauseFound: verdict != "needs_person", summary.Trim(), evidenceJson!,
                string.IsNullOrWhiteSpace(proposed_note) ? null : proposed_note.Trim(), reportLinks.Count > 0 ? reportLinks : null,
                familyOf, queriesRunJson, followUpAtUtc), cancellationToken);

            return outcome switch
            {
                InvestigationSubmitOutcome.Reported => ReportedAnswer(requestId, proposed_note, reportLinks.Count),
                InvestigationSubmitOutcome.LeaseLost => InvestigationToolCore.Status("lease_lost",
                    "This claim_id no longer holds the request: a newer claim superseded it, or it never held one. The report was not stored."),
                InvestigationSubmitOutcome.AlreadyReported => InvestigationToolCore.Status("already_reported", "The request already holds a report."),
                InvestigationSubmitOutcome.NotOpen => InvestigationToolCore.Status("not_open", "The request is closed."),
                InvestigationSubmitOutcome.NotFound => InvestigationToolCore.Status("not_found", $"No investigation request has id {requestId}."),
                InvestigationSubmitOutcome.AgentIdInvalid => InvestigationToolCore.Status("agent_id_invalid", "The claim's agent id is not valid. " + InvestigationRules.AgentIdRule),
                InvestigationSubmitOutcome.FollowUpLimit => McpHelpers.Status("refused",
                    $"The report was not stored: this request's route already holds {InvestigationReportFields.MaxPendingFollowUpsPerRoute} pending follow-ups. Submit without follow_up_at, or wait for one to open.",
                    new { parameter = "follow_up_at", cap = InvestigationReportFields.MaxPendingFollowUpsPerRoute }),
                _ => InvestigationToolCore.Status("journal_capped",
                    $"The request already holds {InvestigationRules.MaxAgentJournalRows} agent journal rows; the report was refused."),
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("submit_investigation_report", ex);
        }
    }

    [McpServerTool(Name = "get_investigation_reports"), Description(
        "Reads investigation requests with their reports and journal, by request_id or over the last hours_back hours. " +
        "status ok or empty. <<GUIDE>> " +
        "One request_id also returns its stored brief. Reports are stored as submitted and masked at answer time at " +
        "mcp_data_detail, as is a brief stored at a weaker level. " +
        "Each request carries its computed state (as list_investigation_requests computes it), each report its verdict, " +
        "root_cause, evidence and summary as the agent returned them: unreviewed and self-attributed, so read them as an " +
        "agent's claim, not a finding. The journal lists what happened to each request, newest first (opened, joined, " +
        "claimed, refused with its reason, lease_expired, reported, expired, claims_exhausted), at most 200 rows. " +
        "THE WINDOW IS BOUNDED AT 100 REQUESTS, newest first: requests_returned is how many you got, truncated says the " +
        "window held more (the reports and journal are those of the returned requests only; narrow hours_back or pass " +
        "server_name or request_id), and journal_truncated says their journal held more than 200 rows. " +
        "hours_back 1-168, default 24.")]
    public static async Task<string> GetInvestigationReports(
        ServerManager serverManager,
        IInvestigationRequestStore store,
        InvestigationBriefComposer composer,
        [Description("Server name. Omit for all.")] string? server_name = null,
        [Description("One request, with its brief. Omit for the window.")] string? request_id = null,
        [Description("1-168. Default 24. Ignored with request_id.")] int hours_back = 24,
        CancellationToken cancellationToken = default) =>
        await ReportsAsync(serverManager, store, composer.Mode, server_name, request_id, hours_back, cancellationToken, await composer.KeyAsync(cancellationToken));

    /// <summary>The report read at <paramref name="mode"/>.</summary>
    internal static async Task<string> ReportsAsync(ServerManager serverManager, IInvestigationRequestStore store,
        McpDataDetail mode, string? server_name, string? request_id, int hours_back, CancellationToken cancellationToken, PseudonymKey? key = null)
    {
        Guid? requestId = null;
        if (!string.IsNullOrWhiteSpace(request_id))
        {
            if (!Guid.TryParse(request_id, out var parsed))
                return InvestigationToolCore.Status("not_found", $"request_id '{request_id}' is not a request id.");
            requestId = parsed;
        }
        else
        {
            /* Only the window arm reads hours_back, so only it validates it. One request_id reads that request whatever its
               age: hours_back is ignored, never evaluated. */
            var windowRefusal = McpHelpers.ValidateHoursBack(hours_back);
            if (windowRefusal is not null)
                return windowRefusal;
        }

        try
        {
            int? serverId = null;
            if (!string.IsNullOrWhiteSpace(server_name))
            {
                var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
                if (error != null)
                    return error;
                serverId = resolved.ServerId;
            }

            var read = await store.ReadReportsAsync(
                requestId, serverId, requestId is null ? DateTime.UtcNow.AddHours(-hours_back) : DateTime.UtcNow,
                InvestigationToolCore.MaxLimit, InvestigationToolCore.JournalLimit, cancellationToken);
            if (read.Requests.Count == 0)
            {
                return requestId is not null
                    ? InvestigationToolCore.Status("not_found", $"No investigation request has id {requestId}.")
                    : InvestigationToolCore.Status("empty", $"No investigation request opened in the last {hours_back} hours.");
            }

            return InvestigationToolCore.ReportsAnswer(read, requestId is null ? hours_back : null, mode, key);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_investigation_reports", ex);
        }
    }
}
