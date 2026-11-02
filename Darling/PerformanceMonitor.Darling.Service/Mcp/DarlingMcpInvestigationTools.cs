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
public sealed partial class DarlingMcpInvestigationTools
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
        "Each row: id, server, family, first metric, alert count, state, lease and expiry; a Change Watch request also carries detail_text, " +
        "its changes and the measures that moved, each query measure with its database. The state is computed from the " +
        "lease and expiry at read time (a lapsed lease reads open), and the read writes nothing. " +
        "A request is opened when an alert matches a notification route whose agent_level is 1 (get_notification_routes), " +
        "within the route's hourly and daily limits; alerts of one server and family join one request while they keep firing (each within 30 minutes of the request's latest alert, up to 4 hours after its first). " +
        "States: open (claimable), claimed (a live lease), reported, closed, expired (past its expiry, or out of its " +
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
        "Waits up to 25 seconds for an investigation request event on your agent routes (opened, joined, resolved, expired, " +
        "claims_exhausted, evidence_ready, follow_up, deadlock_digest) and answers each with its request's id, server and " +
        "family. Writes nothing. status ok or empty. Call again with the cursor it answers; claim with " +
        "claim_investigation_request. <<GUIDE>> " +
        "A long poll (#5822): the MCP transport is stateless, so the server cannot push a notification; this call is how an " +
        "agent hears of a new request, and of what happens to one, without listing on a timer. It answers at once when an " +
        "event was journaled on route_ids after the cursor; otherwise it waits up to timeout_seconds (0-25, default 20; capped " +
        "at 25 so a proxy or a client's request timeout never cuts the call) for one, and at the timeout answers empty with the " +
        "same cursor. Each row: request_id, route_id, server_name, family, opened_at, event, event_id, event_time and the " +
        "event's detail, and detail_text for a Change Watch request; no lease and no brief. joined comes only for a new metric or a higher severity. evidence_ready's " +
        "detail names the evidence sections ready, not_collected and pending: the brief is composed at claim, so evidence that " +
        "lands later reaches a claimed agent only through this detail. The claim stays pull: " +
        "the first claim_investigation_request call wins, exactly as for a request found by listing, so one request can wake " +
        "several agents and only one claims it. route_ids: the agent routes to wait on (get_notification_routes, agent_level " +
        "1); omit them for every route. There is one shared bearer token, so every token holder can wait on every route; a " +
        "route that is not an agent route opens no request, so a wait on it only times out. after: the opaque cursor the last " +
        "call answered. Omit it to start now: events already journaled are not answered (list requests with " +
        "list_investigation_requests). Nothing is queued for an agent: the journal is the queue, so a cursor catches up on " +
        "every event since its last answer (at most 100 per answer, oldest first), including one journaled while no call " +
        "was waiting, and answers each event once.")]
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

    /// <summary>One request the wait answers. #5999: <c>display_name</c> and <c>instance_name</c> are the registry's names, read when
    /// served. On this store <c>display_name</c> is always present (it falls back to the name the request was opened with); only
    /// <c>instance_name</c> is absent, when the server has left the registry (the shape <c>list_investigation_requests</c> keeps).</summary>
    private static Dictionary<string, object?> WaitRequest(InvestigationOpenedRow r)
    {
        var node = new Dictionary<string, object?>
        {
            ["request_id"] = r.RequestId,
            ["route_id"] = r.RouteId,
            ["server_name"] = r.ServerName,
        };
        if (r.DisplayName is not null) node["display_name"] = r.DisplayName;
        if (r.InstanceName is not null) node["instance_name"] = r.InstanceName;
        node["family"] = r.Family;
        /* #6551: a change-watch request's detail, as list_investigation_requests answers it. */
        if (r.DetailText is not null) node["detail_text"] = r.DetailText;
        node["opened_at"] = r.OpenedAtUtc;
        /* #6003: the event this row answers. An evidence_ready detail names the sections that are ready, not collected and
           still pending: the agent re-reads the request's evidence by its id. */
        if (r.Event is not null)
        {
            node["event"] = r.Event;
            node["event_id"] = r.EventId;
            node["event_time"] = r.EventTimeUtc;
            if (r.DetailJson is not null)
            {
                try
                {
                    node["detail"] = JsonSerializer.Deserialize<JsonElement>(r.DetailJson);
                }
                catch (JsonException)
                {
                    /* A detail that is not JSON is left out: the event still answers. */
                }
            }
        }

        return node;
    }

    private static string WaitAnswer(IReadOnlyList<InvestigationOpenedRow> rows, WaitCursor cursor, int timeoutSeconds) =>
        JsonSerializer.Serialize(new
        {
            status = rows.Count == 0 ? "empty" : "ok",
            message = rows.Count == 0
                ? $"No investigation request event on these routes in {timeoutSeconds} seconds. Call again with this cursor."
                : null,
            count = rows.Count,
            requests = rows.Select(WaitRequest),
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
                rows.Where(r => r.CursorTimeUtc <= nowUtc).Select(r => r.CursorKey).ToHashSet());

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
                if (Seen.Contains(row.CursorKey))
                {
                    known.Add(row);
                }
                else if (fresh.Count < MaxWaitRows)
                {
                    fresh.Add(row);
                    known.Add(row);
                }
            }

            /* #6003: a row is keyed by its event (CursorKey, CursorTimeUtc), so a request's later events are answered too and
               none twice. */
            var watermarkTicks = known.Count == 0 ? Watermark.Ticks : Math.Max(Watermark.Ticks, known.Max(r => r.CursorTimeUtc.Ticks));
            var watermark = new DateTime(watermarkTicks, DateTimeKind.Utc);
            var floor = watermark - CursorOverlap;
            var seen = known.Where(r => r.CursorTimeUtc > floor).Select(r => r.CursorKey).ToHashSet();
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
        "Two more statuses can answer where the host serves named agent tokens: claim_not_token (a named token passed a claim_id another " +
        "token made) and agent_id_is_named_token (the legacy token passed an agent_id that is a named token's name, or a claim_id that " +
        "named token holds). " +
        "The brief says what fired, what changed, earlier reports and the next calls to make. " +
        "A claim is one conditional update, so two agents with different agent_ids claiming at once cannot both win (agents that share one agent_id on a named token are one agent); a request takes at most three " +
        "claims. The lease is the route's agent_lease_minutes; a renewal moves it on by that much, never past the request's " +
        "expiry, and only while your claim still holds a live lease (otherwise the call claims afresh, with a new claim_id). " +
        "On a named agent token, if you lost your claim_id, claim again while your lease is live: status renewed, reclaimed true, " +
        "your existing claim_id and a renewed lease (the token's name is your agent_id; any other agent still gets already_claimed). " +
        "On the legacy token a second claim while a lease is live answers already_claimed, even for the same agent_id. " +
        "The brief is composed at claim time, masked at your data detail level, and stored; if it cannot be composed the claim is " +
        "released (it stays counted) and the answer is status error. brief_format: omit, or 1. agent_id names your agent " +
        "in the journal: 1-64 letters, digits, '.', '_', ':' or '-'. Return your findings with submit_investigation_report under this claim_id.")]
    public static Task<string> ClaimInvestigationRequest(
        NpgsqlDataSource postgres,
        InvestigationBriefComposer composer,
        [Description("From list_investigation_requests.")] string request_id,
        [Description("Your agent name: 1-64 of A-Za-z0-9._:-")] string agent_id,
        [Description("Your claim_id, to renew. Omit to claim.")] string? claim_id = null,
        [Description("Omit, or 1.")] int? brief_format = null,
        DarlingMcpServedPolicy? served = null,
        CancellationToken cancellationToken = default) =>
        ClaimAsync(Store(postgres), composer, request_id, agent_id, claim_id, brief_format, cancellationToken, served?.Policy.AgentTokenName,
            served?.NamedAgentNames);

    /// <summary>The claim over a given store and composer, so a test can drive a composer that fails. <paramref name="tokenName"/> is
    /// the name of the named agent token the call came with (#6542), or null for the legacy token: with a name, the agent is that
    /// name, and an <paramref name="agent_id"/> that differs from it is refused. A later call under a claim_id must come from the token
    /// that claimed it. <paramref name="namedAgentNames"/> is the names of the named agent tokens the host serves: the legacy token
    /// (<paramref name="tokenName"/> null) may not claim under one of them.</summary>
    internal static async Task<string> ClaimAsync(InvestigationRequestStore store, InvestigationBriefComposer composer,
        string request_id, string agent_id, string? claim_id, int? brief_format, CancellationToken cancellationToken, string? tokenName = null,
        IReadOnlySet<string>? namedAgentNames = null)
    {
        if (!Guid.TryParse(request_id, out var requestId))
            return InvestigationToolCore.Status("not_found", $"request_id '{request_id}' is not a request id.");

        try
        {
            /* #6542: a named agent token is the agent's identity. An agent_id that names another agent is refused and journaled
               under the token's name, so one token cannot claim as another agent. */
            if (tokenName is not null)
            {
                if (!string.Equals(agent_id, tokenName, StringComparison.Ordinal))
                {
                    await store.RecordRefusalAsync(requestId, tokenName, "claim", AgentIdNotToken, null, cancellationToken);
                    return McpHelpers.Refusal("agent_id", AgentIdNotTokenMessage(tokenName));
                }
            }
            else if (DarlingAgentActionGate.IsRefusedLegacyAgentId(namedAgentNames, agent_id))
            {
                /* The legacy token's agent_id is free text. One that is a named agent's name would put the claim, and the plan changes after it,
                   in that agent's journal, and one that is a reserved placeholder (invalid, unclaimed) would make the claim's rows read as a
                   refusal's, so it is refused and journaled under no agent's name. */
                await store.RecordRefusalAsync(requestId, InvestigationRules.InvalidAgentId, "claim", AgentIdIsNamedToken, null, cancellationToken);
                return InvestigationToolCore.Status(AgentIdIsNamedToken, AgentIdIsNamedTokenMessage(agent_id));
            }

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
                if (ClaimTokenRefusal(tokenName, namedAgentNames, await store.ClaimHolderAsync(requestId, held, cancellationToken)) is { } notYours)
                {
                    await store.RecordRefusalAsync(requestId, tokenName ?? InvestigationRules.InvalidAgentId, "claim", notYours.Status, null, cancellationToken);
                    return InvestigationToolCore.Status(notYours.Status, notYours.Message);
                }

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

            /* #6586: only a named token proves who agent_id is; on the legacy token it is free text, so no reclaim. */
            var claim = await store.ClaimAsync(requestId, agent_id, allowReclaim: tokenName is not null, cancellationToken);
            if (claim.Outcome == InvestigationClaimOutcome.Reclaimed)
            {
                /* #6576: the same agent_id already holds the live lease. Its claim_id comes back and the lease is renewed. */
                return JsonSerializer.Serialize(new
                {
                    status = "renewed",
                    reclaimed = true,
                    request_id = requestId,
                    claim_id = claim.ClaimId,
                    lease_until = claim.LeaseUntilUtc,
                }, McpHelpers.JsonOptions);
            }

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

    [McpServerTool(Name = "get_investigation_evidence"), Description(
        "Reads the frozen evidence sections of one investigation request, reported ones too. Writes nothing. status ok, empty, not_found or " +
        "already_claimed. <<GUIDE>> " +
        "Where the host serves named agent tokens, claim_not_token (a named token passed another token's claim_id) and " +
        "agent_id_is_named_token (the legacy token passed a claim_id a named token holds) can also answer. " +
        "The brief is composed once, at claim time; evidence that freezes later reaches you only as an evidence_ready event " +
        "from wait_for_investigation_requests. Call this with that event's request_id to read it. Each section carries its " +
        "status (ready, pending, empty, unavailable or not_collected) and its content as stored, masked at your data detail level the " +
        "way the brief masks it. A pending section comes back as pending with no content: it never waits, so read it again " +
        "after a later evidence_ready. sections (optional) names the sections to return; omit it for all. A request that " +
        "another agent holds a live claim on is refused (already_claimed) unless you pass its claim_id; an unclaimed or " +
        "finished request is served to any caller.")]
    public static async Task<string> GetInvestigationEvidence(
        NpgsqlDataSource postgres,
        InvestigationBriefComposer composer,
        [Description("From list_investigation_requests.")] string request_id,
        [Description("Section names to return. Omit for all.")] string[]? sections = null,
        [Description("Your claim_id, when you hold the claim.")] string? claim_id = null,
        DarlingMcpServedPolicy? served = null,
        CancellationToken cancellationToken = default) =>
        await EvidenceAsync(Store(postgres), composer.Mode, request_id, sections, claim_id, cancellationToken, await composer.KeyAsync(cancellationToken),
            served?.Policy.AgentTokenName, served?.NamedAgentNames);

    /// <summary>The evidence read over a given store at <paramref name="mode"/> (#6033).</summary>
    internal static async Task<string> EvidenceAsync(InvestigationRequestStore store, McpDataDetail mode, string request_id,
        string[]? sections, string? claim_id, CancellationToken cancellationToken, PseudonymKey? key = null, string? tokenName = null,
        IReadOnlySet<string>? namedAgentNames = null)
    {
        if (!Guid.TryParse(request_id, out var requestId))
            return InvestigationToolCore.Status("not_found", $"request_id '{request_id}' is not a request id.");

        try
        {
            var request = await store.ReadRequestAsync(requestId, cancellationToken);
            if (request is null)
                return InvestigationToolCore.Status("not_found", "No investigation request has that id.");

            if (request.Value.Row.State == "claimed")
            {
                var holder = Guid.TryParse(claim_id, out var claim) ? await store.ClaimHolderAsync(requestId, claim, cancellationToken) : null;
                /* A claim_id is the claiming token's: a named token that presents another token's claim_id is refused. The read journals
                   nothing (it is a read, and the write policy serves it as one): a journaled refusal counts toward MaxAgentJournalRows,
                   and at the cap the claiming agent's next journaled call on the request is refused. */
                if (ClaimTokenRefusal(tokenName, namedAgentNames, holder) is { } notYours)
                    return InvestigationToolCore.Status(notYours.Status, notYours.Message);

                if (holder is null)
                    return InvestigationToolCore.Status("already_claimed", "Another claim holds a live lease on this request.");
            }

            var wanted = sections is { Length: > 0 } ? new HashSet<string>(sections, StringComparer.Ordinal) : null;
            var rows = (await store.ReadEvidenceAsync(requestId, cancellationToken))
                .Where(r => wanted is null || wanted.Contains(r.Section))
                .Select(r => InvestigationBriefComposer.EvidenceSection(
                    new InvestigationBriefEvidence(r.Section, r.State, r.CapturedAtUtc, r.ContentJson), mode, key))
                .ToList();

            return JsonSerializer.Serialize(new
            {
                status = rows.Count == 0 ? "empty" : "ok",
                request_id = requestId,
                request_state = request.Value.Row.State,
                sections = new JsonArray(rows.Select(n => (JsonNode?)n).ToArray()),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_investigation_evidence", ex);
        }
    }

    [McpServerTool(Name = "submit_investigation_report"), Description(
        "Returns your report on a request you claimed. Writes only investigation state. status reported, invalid, " +
        "unsupported_format, lease_lost, already_reported or not_found. <<GUIDE>> " +
        "Where the host serves named agent tokens, a named token reports only under its own claim_id: another token's claim_id answers " +
        "claim_not_token, and the legacy token passing a claim_id a named token holds answers agent_id_is_named_token. " +
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
        "(\\\\server\\share, //server/share) links are refused (invalid). Each URL is journaled once per request as link_recorded, and " +
        "later briefs for the same server and family list it under prior_links. Darling stores the link and never calls Jira or git. " +
        "family_of (optional) is the request_id of an earlier request on this store, which need not have a report yet; it must name a request (else invalid). " +
        "queries_run (optional) is a JSON array of the statements you ran, " + InvestigationReportFields.QueriesRunRule + " (over a cap: refused, never cut); " +
        "it is masked as statement text. " +
        "follow_up_at (optional) asks Darling to open a new request for this case later: " + InvestigationReportFields.FollowUpRule + ". " +
        "When the time comes the new request names this one (follow_up_of), its brief carries this report, and one follow_up event is " +
        "journaled. follow_up_condition (optional) is module:<database>.<schema>.<object>, with follow_up_at as its latest date: the follow-up opens once when that module's modify_date changes (as procedure stats record it), with its procedure-stats counts before and after in a deploy_follow_up evidence section, or at follow_up_at if the module has not changed. A time in the past, more than 30 days ahead, or a route already holding 20 pending follow-ups is refused (invalid), and nothing is stored.")]
    public static Task<string> SubmitInvestigationReport(
        NpgsqlDataSource postgres,
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
        [Description("Optional. Open the follow-up when a module is deployed: " + InvestigationReportFields.FollowUpConditionRule + ".")] string? follow_up_condition = null,
        DarlingMcpServedPolicy? served = null,
        CancellationToken cancellationToken = default) =>
        SubmitAsync(Store(postgres), request_id, claim_id, report_format, verdict, root_cause, evidence, summary,
            proposed_note, action_proposals, cancellationToken, links, family_of, queries_run, follow_up_at, follow_up_condition,
            served?.Policy.AgentTokenName, served?.NamedAgentNames);

    /// <summary>The <c>reported</c> answer: <c>links_recorded</c> (the distinct URLs stored, #5914) only when the report
    /// carried a link, so a report without one answers exactly as before.</summary>
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
    internal static async Task<string> SubmitAsync(InvestigationRequestStore store, string request_id, string claim_id,
        int report_format, string verdict, string root_cause, string evidence, string summary, string? proposed_note,
        string? action_proposals, CancellationToken cancellationToken, string? links = null, string? family_of = null,
        string? queries_run = null, string? follow_up_at = null, string? follow_up_condition = null, string? tokenName = null,
        IReadOnlySet<string>? namedAgentNames = null)
    {
        var problems = InvestigationToolCore.ValidateReport(request_id, claim_id, verdict, root_cause, evidence, summary, proposed_note,
            action_proposals, out var requestId, out var claimId, out var evidenceJson, requireBasis: true, serverActionTools: true);
        /* #5914: links ride after the other fields, so a report's field order in invalid's message is unchanged. */
        if (InvestigationReportLinks.Validate(links, out var reportLinks) is { } linkProblem)
            problems.Add("links: " + linkProblem);
        /* #6004: family_of and queries_run after links, for the same reason. A family_of that parses is checked against the store below. */
        if (InvestigationReportFields.ValidateFamilyOf(family_of, out var familyOf) is { } familyProblem)
            problems.Add("family_of: " + familyProblem);

        if (InvestigationReportFields.Validate(queries_run, out var queriesRunJson) is { } queriesProblem)
            problems.Add("queries_run: " + queriesProblem);
        /* #6008: follow_up_at after queries_run, for the same reason. The route's pending cap is the store's, under the route lock. */
        if (InvestigationReportFields.ValidateFollowUp(follow_up_at, DateTime.UtcNow, out var followUpAtUtc) is { } followUpProblem)
            problems.Add("follow_up_at: " + followUpProblem);
        /* #6009: follow_up_condition after follow_up_at, for the same reason. It needs the time as its latest date. */
        if (InvestigationReportFields.ValidateFollowUpCondition(follow_up_condition, followUpAtUtc is not null, out var followUpCondition) is { } conditionProblem)
            problems.Add("follow_up_condition: " + conditionProblem);
        try
        {
            var holder = requestId != Guid.Empty && claimId != Guid.Empty
                ? await store.ClaimHolderAsync(requestId, claimId, cancellationToken)
                : null;
            var agent = holder ?? InvestigationToolCore.UnclaimedAgent;

            /* A report under a claim_id comes from the token that claimed it. A named token that presents another token's claim_id is
               refused, and every refusal here is journaled as the caller, not as the claim's holder. */
            var journalAs = tokenName ?? agent;
            if (ClaimTokenRefusal(tokenName, namedAgentNames, holder) is { } notYours)
            {
                if (requestId != Guid.Empty)
                    await store.RecordRefusalAsync(requestId, tokenName ?? InvestigationRules.InvalidAgentId, "submit", notYours.Status, null, cancellationToken);
                return InvestigationToolCore.Status(notYours.Status, notYours.Message);
            }

            if (report_format < short.MinValue || report_format > short.MaxValue
                || !InvestigationFormats.IsAcceptedReportFormat((short)report_format))
            {
                if (requestId != Guid.Empty)
                    await store.RecordRefusalAsync(requestId, journalAs, "submit", "unsupported_format", null, cancellationToken);
                return InvestigationToolCore.Status("unsupported_format", InvestigationFormats.UnsupportedReportFormatMessage(
                    (short)Math.Clamp(report_format, short.MinValue, short.MaxValue)));
            }

            /* #5632 review round 2, M2: the caps, before any write, journaled once per request and reason. Kestrel's
               request limit stays as it is: the plan-XML tools need large bodies. */
            var oversize = InvestigationToolCore.CapProblems(root_cause, evidence, queries_run);
            if (oversize.Count > 0)
            {
                if (requestId != Guid.Empty)
                {
                    var detail = JsonSerializer.Serialize(new { fields = oversize.Select(p => p.Field).Distinct() }, McpHelpers.JsonOptions);
                    await store.RecordRefusalAsync(requestId, journalAs, "submit", "too_large", detail, cancellationToken);
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
                    await store.RecordRefusalAsync(requestId, journalAs, "submit", reason, detail, cancellationToken);
                }

                return McpHelpers.Refusal(problems[0].Split(':')[0], InvestigationToolCore.InvalidReportMessage(problems));
            }

            var outcome = await store.SubmitAsync(new InvestigationReportSubmission(
                requestId, claimId, journalAs, (short)report_format, verdict, root_cause.Trim(),
                RootCauseFound: verdict != "needs_person", summary.Trim(), evidenceJson!,
                string.IsNullOrWhiteSpace(proposed_note) ? null : proposed_note.Trim(), reportLinks.Count > 0 ? reportLinks : null,
                familyOf, queriesRunJson, followUpAtUtc, followUpCondition), cancellationToken);

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
        "While a practice request (practice_of on its row) is open or claimed, the case it replays is read without its report, " +
        "feedback and journal, so the practice is not answered from them. " +
        "hours_back 1-168, default 24.")]
    public static async Task<string> GetInvestigationReports(
        NpgsqlDataSource postgres,
        InvestigationBriefComposer composer,
        InvestigationReadPolicy readPolicy,
        [Description("Server name. Omit for all.")] string? server_name = null,
        [Description("One request, with its brief. Omit for the window.")] string? request_id = null,
        [Description("1-168. Default 24. Ignored with request_id.")] int hours_back = 24,
        CancellationToken cancellationToken = default) =>
        await ReportsAsync(postgres, composer.Mode, server_name, request_id, hours_back, cancellationToken, readPolicy.HideLivePracticeAnswers,
            await composer.KeyAsync(cancellationToken));

    /// <summary>The report read at <paramref name="mode"/>. The web panel passes <see cref="McpDataDetail.Full"/>: its role
    /// can SELECT these tables itself, so the setting guards the MCP side (#5632 review round 2, M1).</summary>
    internal static async Task<string> ReportsAsync(NpgsqlDataSource postgres, McpDataDetail mode, string? server_name,
        string? request_id, int hours_back, CancellationToken cancellationToken, bool hideLivePracticeAnswers = false,
        PseudonymKey? key = null)
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

            var store = Store(postgres);
            var read = await store.ReadReportsAsync(
                requestId, serverId, requestId is null ? DateTime.UtcNow.AddHours(-hours_back) : DateTime.UtcNow,
                InvestigationToolCore.MaxLimit, InvestigationToolCore.JournalLimit, cancellationToken);
            /* #5918: under the investigations arm, a case a live practice request replays has no report and no feedback to
               read (the practice would be answered by the answer key). The request row stays. */
            if (hideLivePracticeAnswers && read.Requests.Count > 0)
                read = WithoutReplayedAnswers(read, await store.LivePracticeReplaysAsync(cancellationToken));
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

    /// <summary>
    /// <paramref name="read"/> without the reports, the feedback and the journal rows of the requests in
    /// <paramref name="replayed"/> (#5918): the cases a live practice request replays. Their request rows stay, so the agent
    /// still sees the case exists; the family counts are windows over many cases and are left as they are. A stored brief loses the replayed cases from its earlier reports and prior links.
    /// </summary>
    internal static InvestigationReportsRead WithoutReplayedAnswers(InvestigationReportsRead read, IReadOnlySet<Guid> replayed)
    {
        if (replayed.Count == 0)
            return read;
        return read with
        {
            Reports = read.Reports.Where(r => !replayed.Contains(r.RequestId)).ToList(),
            Feedback = read.Feedback?.Where(f => !replayed.Contains(f.RequestId)).ToList(),
            Journal = read.Journal.Where(j => j.RequestId is not { } id || !replayed.Contains(id)).ToList(),
            /* A brief stored before the practice started lists the replayed case's report under earlier_reports (#6236). */
            Brief = InvestigationReplayFilter.WithoutReplayed(read.Brief, replayed),
        };
    }
}

/// <summary>
/// What the investigation reads may show, by the arm the host serves (#5918). <see cref="HideLivePracticeAnswers"/> is true
/// under the investigations arm (<see cref="DarlingMcpWritePolicy.Effective.Investigations"/>): while a practice request is
/// live, the case it replays has no report and no feedback in <c>get_investigation_reports</c>. The writes arm (operators) and
/// a host serving neither arm see everything. Registered by <c>ConfigureMcpServices</c> from the effective policy.
/// </summary>
public sealed record InvestigationReadPolicy(bool HideLivePracticeAnswers);

public sealed partial class DarlingMcpInvestigationTools
{
    /// <summary>The refusal reason for an agent_id that is not the named token's name (#6542).</summary>
    internal const string AgentIdNotToken = "agent_id_not_token";

    /// <summary>The sentence for <see cref="AgentIdNotToken"/>. Names the token's name, never its value.</summary>
    internal static string AgentIdNotTokenMessage(string tokenName) =>
        $"This token is the agent {tokenName}: pass agent_id {tokenName}. An agent token claims and acts only as its own name.";

    /// <summary>A call under a claim_id came from a token other than the one that claimed it (#6542). Later calls under a claim must come
    /// from the token that made it.</summary>
    internal const string ClaimNotToken = "claim_not_token";

    internal static string ClaimNotTokenMessage(string tokenName) =>
        $"This token is the agent {tokenName}, and that claim_id was not made by this token. Use the claim_id from your own claim.";

    /// <summary>A legacy-token claim named an agent_id that is a named agent token's name (#6542).</summary>
    internal const string AgentIdIsNamedToken = "agent_id_is_named_token";

    /// <summary>Whether a call under a claim_id may go on, given who made the claim (<paramref name="holder"/>, null when no claim holds it).
    /// A named token acts only under a claim it made; the legacy token acts only under a claim no served named token made, since the
    /// journal would credit that named agent with what the legacy token did. Null means it may.</summary>
    internal static (string Status, string Message)? ClaimTokenRefusal(string? tokenName, IReadOnlySet<string>? namedAgentNames, string? holder)
    {
        if (holder is null)
            return null;
        if (tokenName is not null)
            return string.Equals(holder, tokenName, StringComparison.Ordinal) ? null : (ClaimNotToken, ClaimNotTokenMessage(tokenName));
        return namedAgentNames is { Count: > 0 } && namedAgentNames.Contains(holder) ? (AgentIdIsNamedToken, ClaimHeldByNamedTokenMessage(holder)) : null;
    }

    internal static string ClaimHeldByNamedTokenMessage(string holder) =>
        $"That claim_id belongs to the named agent token {holder}, which this host serves. Only that token can act under it; claim the request again with a different agent_id.";

    internal static string AgentIdIsNamedTokenMessage(string agentId) =>
        DarlingMcpTokenSet.IsReservedName(agentId)
            ? $"agent_id {agentId} is a reserved name the journal uses when no agent made a call. Use a different agent_id on this token."
            : $"agent_id {agentId} is the name of a named agent token this host serves. Use a different agent_id on this token.";
}
