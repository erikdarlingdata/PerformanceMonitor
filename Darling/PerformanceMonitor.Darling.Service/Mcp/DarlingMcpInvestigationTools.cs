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
using System.Diagnostics;
using System.Globalization;
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
    /// <summary>The long-poll tool's name (#5822).</summary>
    internal const string WaitToolName = "wait_for_investigation_requests";

    /// <summary>How long a wait lasts when the call names no timeout, in seconds.</summary>
    internal const int DefaultWaitSeconds = 20;

    /// <summary>The longest wait, in seconds: under the 30 seconds a proxy or a client's request timeout commonly allows, so
    /// neither cuts the call before it answers.</summary>
    internal const int MaxWaitSeconds = 25;

    /// <summary>The most requests one wait answers; the cursor moves past only what it answered.</summary>
    internal const int MaxWaitRows = 100;

    /// <summary>The most rows one catch-up read takes (the cursor's already-answered rows included).</summary>
    internal const int WaitReadLimit = 1000;

    /// <summary>
    /// How far behind its newest answered request a cursor reads again (#5822). A request's <c>opened_at</c> is taken before
    /// its transaction waits on the open-or-join locks, so it can commit after a request with a later <c>opened_at</c> was
    /// already answered. The cursor keeps the ids it answered inside this window and skips them, so a late commit is answered
    /// once and an early one is not answered twice. Each lock wait is capped at
    /// <see cref="InvestigationDispatcher.LockTimeoutMilliseconds"/>.
    /// </summary>
    internal static readonly TimeSpan CursorOverlap = TimeSpan.FromMinutes(5);

    /// <summary>The store read a wait catches up with: requests opened after a time on some routes, oldest first.</summary>
    internal delegate Task<IReadOnlyList<InvestigationOpenedRow>> OpenedReader(
        DateTime sinceUtc, IReadOnlyCollection<int>? routeIds, int limit, CancellationToken cancellationToken);

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
        "resources, availability, jobs, self_monitor, other), limit (1-100, default 20). requests_returned is how many rows " +
        "you got; truncated says more requests matched than limit (raise limit or narrow the filters).")]
    public static async Task<string> ListInvestigationRequests(
        NpgsqlDataSource postgres,
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
                var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
                if (error != null)
                    return error;
                serverId = resolved.ServerId;
            }

            /* #5834, the #3699 shape: read one row past the limit, return the page, and say whether more matched. */
            var fetched = await Store(postgres).ListAsync(state, serverId, family, limit + 1, cancellationToken);
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

    [McpServerTool(Name = "wait_for_investigation_requests"), Description(
        "Waits up to 25 seconds for an investigation request to open on your agent routes and answers its id, server and " +
        "family. Writes nothing. status ok or empty. Call again with the cursor it answers; claim with " +
        "claim_investigation_request. <<GUIDE>> " +
        "A long poll (#5822): the MCP transport is stateless, so the server cannot push a notification; this call is how an " +
        "agent hears of a new request without listing on a timer. It answers at once when a request opened on route_ids after " +
        "the cursor; otherwise it waits up to timeout_seconds (0-25, default 20; capped at 25 so a proxy or a client's request " +
        "timeout never cuts the call) for one to open, and at the timeout answers empty with the same cursor. Each row: " +
        "request_id, route_id, server_name, family and opened_at, nothing more: no lease and no brief. The claim stays pull: " +
        "the first claim_investigation_request call wins, exactly as for a request found by listing, so one request can wake " +
        "several agents and only one claims it. route_ids: the agent routes to wait on (get_notification_routes, agent_level " +
        "1); omit them for every route. There is one shared bearer token, so every token holder can wait on every route; a " +
        "route that is not an agent route opens no request, so a wait on it only times out. after: the opaque cursor the last " +
        "call answered. Omit it to start now: requests already open are not answered (list them with " +
        "list_investigation_requests). Nothing is queued for an agent: the store is the queue, so a cursor catches up on every " +
        "request opened since its last answer (at most 100 per answer, oldest first), including one that opened while no call " +
        "was waiting, and answers each request once.")]
    public static Task<string> WaitForInvestigationRequests(
        NpgsqlDataSource postgres,
        InvestigationOpenSignal signal,
        [Description("Agent route ids to wait on. Omit for every route.")] int[]? route_ids = null,
        [Description("The cursor the last call answered. Omit to start now.")] string? after = null,
        [Description("0-25. Default 20.")] int timeout_seconds = DefaultWaitSeconds,
        CancellationToken cancellationToken = default) =>
        WaitAsync(Store(postgres).OpenedSinceAsync, signal, () => DateTime.UtcNow, route_ids, after, timeout_seconds,
            cancellationToken);

    /// <summary>
    /// The long poll behind <see cref="WaitForInvestigationRequests"/> (#5822). Rearms its waiter BEFORE each catch-up read,
    /// so a request that commits between the read and the wait still wakes it; answers what the read found past the cursor,
    /// or waits for the signal and reads again, until the timeout. A cancellation removes the waiter and throws.
    /// </summary>
    internal static async Task<string> WaitAsync(OpenedReader read, InvestigationOpenSignal signal, Func<DateTime> utcNow,
        int[]? route_ids, string? after, int timeout_seconds, CancellationToken cancellationToken)
    {
        if (timeout_seconds < 0 || timeout_seconds > MaxWaitSeconds)
            return McpHelpers.Refusal("timeout_seconds",
                $"Invalid timeout_seconds value '{timeout_seconds}'. Must be an integer from 0 to {MaxWaitSeconds}.");
        if (route_ids is not null && route_ids.Any(id => id < 1))
            return McpHelpers.Refusal("route_ids", "Invalid route_ids: every route id is a positive integer (get_notification_routes).");

        WaitCursor? cursor = null;
        if (after is not null && !WaitCursor.TryParse(after, out cursor))
            return McpHelpers.Refusal("after",
                $"Invalid after value. Pass the cursor a previous {WaitToolName} call answered, or omit it to start now.");

        IReadOnlyCollection<int>? routes = route_ids is { Length: > 0 } ? route_ids.Distinct().ToArray() : null;
        var timeout = TimeSpan.FromSeconds(timeout_seconds);
        var started = Stopwatch.GetTimestamp();

        try
        {
            using var waiter = signal.Register(routes);
            while (true)
            {
                waiter.Rearm();
                IReadOnlyList<InvestigationOpenedRow> fresh;
                if (cursor is null)
                {
                    var now = utcNow();
                    var rows = await read(now - CursorOverlap, routes, WaitReadLimit, cancellationToken);
                    (fresh, cursor) = WaitCursor.Start(now, rows).Advance(rows);
                }
                else
                {
                    var rows = await read(cursor.Watermark - CursorOverlap, routes, WaitReadLimit, cancellationToken);
                    (fresh, cursor) = cursor.Advance(rows);
                }

                if (fresh.Count > 0)
                    return WaitAnswer(fresh, cursor, timeout_seconds);

                var remaining = timeout - Stopwatch.GetElapsedTime(started);
                if (remaining <= TimeSpan.Zero || !await waiter.WaitAsync(remaining, cancellationToken))
                    return WaitAnswer(fresh, cursor, timeout_seconds);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(WaitToolName, ex);
        }
    }

    private static string WaitAnswer(IReadOnlyList<InvestigationOpenedRow> rows, WaitCursor cursor, int timeoutSeconds) =>
        JsonSerializer.Serialize(new
        {
            status = rows.Count == 0 ? "empty" : "ok",
            message = rows.Count == 0
                ? $"No investigation request opened on these routes in {timeoutSeconds} seconds. Call again with this cursor."
                : null,
            count = rows.Count,
            requests = rows.Select(r => new
            {
                request_id = r.RequestId,
                route_id = r.RouteId,
                server_name = r.ServerName,
                family = r.Family,
                opened_at = r.OpenedAtUtc,
            }),
            cursor = cursor.Encode(),
        }, McpHelpers.JsonOptions);

    /// <summary>
    /// The long poll's cursor (#5822): the newest <c>opened_at</c> it has answered (the watermark) and the ids it answered
    /// within <see cref="CursorOverlap"/> of it. Opaque to the agent: <c>v1.&lt;ticks&gt;.&lt;id&gt;,&lt;id&gt;</c>.
    /// </summary>
    internal sealed class WaitCursor
    {
        private const string Version = "v1";

        private WaitCursor(DateTime watermark, IReadOnlySet<Guid> seen)
        {
            Watermark = watermark;
            Seen = seen;
        }

        /// <summary>The newest opened_at answered, UTC.</summary>
        internal DateTime Watermark { get; }

        /// <summary>The ids answered inside the overlap window: skipped when the read meets them again.</summary>
        internal IReadOnlySet<Guid> Seen { get; }

        /// <summary>The cursor of a call that named none: everything opened by <paramref name="nowUtc"/> counts as seen.</summary>
        internal static WaitCursor Start(DateTime nowUtc, IReadOnlyList<InvestigationOpenedRow> rows) =>
            new(DateTime.SpecifyKind(nowUtc, DateTimeKind.Utc),
                rows.Where(r => r.OpenedAtUtc <= nowUtc).Select(r => r.RequestId).ToHashSet());

        /// <summary>
        /// The rows of a catch-up read (every request opened after <see cref="Watermark"/> minus the overlap, oldest first)
        /// this cursor has not answered, at most <see cref="MaxWaitRows"/>, and the cursor past them.
        /// </summary>
        internal (IReadOnlyList<InvestigationOpenedRow> Fresh, WaitCursor Next) Advance(IReadOnlyList<InvestigationOpenedRow> rows)
        {
            var fresh = new List<InvestigationOpenedRow>();
            var known = new List<InvestigationOpenedRow>();
            foreach (var row in rows)
            {
                if (Seen.Contains(row.RequestId))
                {
                    known.Add(row);
                }
                else if (fresh.Count < MaxWaitRows)
                {
                    fresh.Add(row);
                    known.Add(row);
                }
            }

            var watermarkTicks = known.Count == 0 ? Watermark.Ticks : Math.Max(Watermark.Ticks, known.Max(r => r.OpenedAtUtc.Ticks));
            var watermark = new DateTime(watermarkTicks, DateTimeKind.Utc);
            var floor = watermark - CursorOverlap;
            var seen = known.Where(r => r.OpenedAtUtc > floor).Select(r => r.RequestId).ToHashSet();
            return (fresh, new WaitCursor(watermark, seen));
        }

        internal string Encode() =>
            Version + "." + Watermark.Ticks.ToString(CultureInfo.InvariantCulture) + "." +
            string.Join(",", Seen.OrderBy(id => id).Select(id => id.ToString("N")));

        /// <summary>
        /// Reads a cursor an earlier call answered. Bounded on both sides (#5856 review round 1): every read subtracts
        /// <see cref="CursorOverlap"/> from the watermark, so a watermark at or under the overlap (<c>v1.0.</c>) is
        /// refused here as an <c>invalid</c> cursor instead of throwing below <see cref="DateTime.MinValue"/> in the read.
        /// </summary>
        internal static bool TryParse(string text, out WaitCursor? cursor)
        {
            cursor = null;
            var parts = text.Split('.');
            if (parts.Length != 3 || parts[0] != Version
                || !long.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var ticks)
                || ticks > DateTime.MaxValue.Ticks
                || ticks <= CursorOverlap.Ticks)
            {
                return false;
            }

            var seen = new HashSet<Guid>();
            if (parts[2].Length > 0)
            {
                foreach (var id in parts[2].Split(','))
                {
                    if (!Guid.TryParseExact(id, "N", out var guid))
                    {
                        return false;
                    }

                    seen.Add(guid);
                }
            }

            cursor = new WaitCursor(new DateTime(ticks, DateTimeKind.Utc), seen);
            return true;
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
        "in the journal: 1-64 letters, digits, '.', '_', ':' or '-'. Return your findings with submit_investigation_report under this claim_id.")]
    public static Task<string> ClaimInvestigationRequest(
        NpgsqlDataSource postgres,
        InvestigationBriefComposer composer,
        [Description("From list_investigation_requests.")] string request_id,
        [Description("Your agent name: 1-64 of A-Za-z0-9._:-")] string agent_id,
        [Description("Your claim_id, to renew. Omit to claim.")] string? claim_id = null,
        [Description("Omit, or 1.")] int? brief_format = null,
        CancellationToken cancellationToken = default) =>
        ClaimAsync(Store(postgres), composer, request_id, agent_id, claim_id, brief_format, cancellationToken);

    /// <summary>The claim over a given store and composer, so a test can drive a composer that fails.</summary>
    internal static async Task<string> ClaimAsync(InvestigationRequestStore store, InvestigationBriefComposer composer,
        string request_id, string agent_id, string? claim_id, int? brief_format, CancellationToken cancellationToken)
    {
        if (!Guid.TryParse(request_id, out var requestId))
            return InvestigationToolCore.Status("not_found", $"request_id '{request_id}' is not a request id.");

        try
        {
            /* #5632 review round 2, L4: an agent id is a name, not text, so it cannot pose as another journal line. */
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
                return InvestigationToolCore.Status(InvestigationToolCore.ClaimStatus(claim.Outcome), InvestigationToolCore.ClaimMessage(claim));

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
        "proposed_note at most 2,000, root_cause at most 8,000; evidence at most 50 items, each string value at most 1,000 " +
        "characters, 32 KB in all (over a cap: status refused, naming the field and the cap). The note waits as pending until a person approves it, and only then reaches a later " +
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
        var problems = InvestigationToolCore.ValidateReport(request_id, claim_id, verdict, root_cause, evidence, summary, proposed_note,
            action_proposals, out var requestId, out var claimId, out var evidenceJson);
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
                    (short)Math.Clamp(report_format, short.MinValue, short.MaxValue)));
            }

            /* #5632 review round 2, M2: the caps, before any write, journaled once per request and reason. Kestrel's
               request limit stays as it is: the plan-XML tools need large bodies. */
            var oversize = InvestigationToolCore.CapProblems(root_cause, evidence);
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
                string.IsNullOrWhiteSpace(proposed_note) ? null : proposed_note.Trim()), cancellationToken);

            return outcome switch
            {
                InvestigationSubmitOutcome.Reported => JsonSerializer.Serialize(new
                {
                    status = "reported",
                    request_id = requestId,
                    proposed_note = string.IsNullOrWhiteSpace(proposed_note) ? null : "pending",
                }, McpHelpers.JsonOptions),
                InvestigationSubmitOutcome.LeaseLost => InvestigationToolCore.Status("lease_lost",
                    "This claim_id no longer holds the request: a newer claim superseded it, or it never held one. The report was not stored."),
                InvestigationSubmitOutcome.AlreadyReported => InvestigationToolCore.Status("already_reported", "The request already holds a report."),
                InvestigationSubmitOutcome.NotOpen => InvestigationToolCore.Status("not_open", "The request is closed."),
                InvestigationSubmitOutcome.NotFound => InvestigationToolCore.Status("not_found", $"No investigation request has id {requestId}."),
                InvestigationSubmitOutcome.AgentIdInvalid => InvestigationToolCore.Status("agent_id_invalid", "The claim's agent id is not valid. " + InvestigationRules.AgentIdRule),
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
        "mcp.dataDetail, as is a brief stored at a weaker level. " +
        "Each request carries its computed state (as list_investigation_requests computes it), each report its verdict, " +
        "root_cause, evidence and summary as the agent returned them: unreviewed and self-attributed, so read them as an " +
        "agent's claim, not a finding. The journal lists what happened to each request, newest first (opened, joined, " +
        "claimed, refused with its reason, lease_expired, reported, expired, claims_exhausted), at most 200 rows. " +
        "THE WINDOW IS BOUNDED AT 100 REQUESTS, newest first: requests_returned is how many you got, truncated says the " +
        "window held more (the reports and journal are those of the returned requests only; narrow hours_back or pass " +
        "server_name or request_id), and journal_truncated says their journal held more than 200 rows. " +
        "Each report also carries the feedback people gave it (verdict right, partly or wrong, corrected_verdict, " +
        "missed_step, comment, author), and a window read returns family_counts: per alert family, reported, " +
        "with_feedback, marked_wrong, agent_false_positives (reports that say false_positive) and false_positives " +
        "(the same after people's corrected verdicts). " +
        "hours_back 1-168, default 24.")]
    public static Task<string> GetInvestigationReports(
        NpgsqlDataSource postgres,
        InvestigationBriefComposer composer,
        [Description("Server name. Omit for all.")] string? server_name = null,
        [Description("One request, with its brief. Omit for the window.")] string? request_id = null,
        [Description("1-168. Default 24. Ignored with request_id.")] int hours_back = 24,
        CancellationToken cancellationToken = default) =>
        ReportsAsync(postgres, composer.Mode, server_name, request_id, hours_back, cancellationToken);

    /// <summary>The report read at <paramref name="mode"/>. The web panel passes <see cref="McpDataDetail.Full"/>: its role
    /// can SELECT these tables itself, so the setting guards the MCP side (#5632 review round 2, M1).</summary>
    internal static async Task<string> ReportsAsync(NpgsqlDataSource postgres, McpDataDetail mode, string? server_name,
        string? request_id, int hours_back, CancellationToken cancellationToken)
    {
        Guid? requestId = null;
        if (!string.IsNullOrWhiteSpace(request_id))
        {
            ReadScope.NoteNoWindow();
            if (!Guid.TryParse(request_id, out var parsed))
                return InvestigationToolCore.Status("not_found", $"request_id '{request_id}' is not a request id.");
            requestId = parsed;
        }
        else
        {
            /* Only the window arm reads hours_back, so only it validates it and notes the window on the slow-read row
               (#5844, the #5814 rule). One request_id reads that request whatever its age: hours_back is ignored, never
               evaluated (an absurd value must not throw into an error envelope), and the read notes it had no window. */
            var windowRefusal = DarlingReadWindow.ValidateHoursBack(hours_back);
            if (windowRefusal is not null)
                return windowRefusal;
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
                requestId, serverId, requestId is null ? DateTime.UtcNow.AddHours(-hours_back) : DateTime.UtcNow,
                InvestigationToolCore.MaxLimit, InvestigationToolCore.JournalLimit, cancellationToken);
            if (read.Requests.Count == 0)
            {
                return requestId is not null
                    ? InvestigationToolCore.Status("not_found", $"No investigation request has id {requestId}.")
                    : InvestigationToolCore.Status("empty", $"No investigation request opened in the last {hours_back} hours.");
            }

            return InvestigationToolCore.ReportsAnswer(read, requestId is null ? hours_back : null, mode);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_investigation_reports", ex);
        }
    }
}
