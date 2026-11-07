/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service.Chat;

/// <summary>
/// #7656: the bot's posts, with the token read from the opened settings at each post. A token saved after start is used at the next
/// post; a blank token means the pass is not ready and posts nothing, so every row waits unmarked for one. The client is rebuilt only
/// when the token changes.
/// </summary>
public sealed class SettingsSlackBotPosts(Func<string> token, string? apiBase = null) : ISlackBotPosts
{
    private readonly object _lock = new();
    private string _tokenInUse = "";
    private SlackBotClient? _client;

    /// <summary>Whether a bot token is set now.</summary>
    public bool HasToken() => !string.IsNullOrWhiteSpace(token());

    /// <inheritdoc />
    public Task<SlackPostResult> PostAsync(string channel, string text, IReadOnlyList<object>? blocks, string? threadTs, CancellationToken cancellationToken)
    {
        var current = (token() ?? "").Trim();
        if (current.Length == 0)
        {
            return Task.FromResult(new SlackPostResult(false, null, null, "no bot token"));
        }

        SlackBotClient client;
        lock (_lock)
        {
            if (_client is null || !string.Equals(_tokenInUse, current, StringComparison.Ordinal))
            {
                _client = new SlackBotClient(current, apiBase);
                _tokenInUse = current;
            }

            client = _client;
        }

        return client.PostAsync(channel, text, blocks, threadTs, cancellationToken);
    }
}

/// <summary>
/// #7656: one pass of the Slack thread posts, after the other request-event sends. It reads the journal rows of the kinds in
/// <see cref="PostedEvents"/> from the last <see cref="ReadWindow"/> that have no post mark (a mark says what was posted; the window
/// bounds what each pass reads, so a pass costs the window's rows and not the journal's), in journal
/// order, so a request's <c>opened</c> row posts first and opens its thread, and every later row replies in it. A follow-up's request
/// post replies in its <c>follow_up_of</c> parent's thread and inherits it, one level deep: the parent's stored thread is used when it
/// exists and sits in the route's current bot channel, else the follow-up opens a thread of its own. A change whose result never came
/// posts once as result unknown after <see cref="UnknownAfter"/>.
///
/// <para>A request gets its parent post only when it opened at or after the time its route got its bot channel
/// (<see cref="NotificationRoute.SlackBotChannelSetAt"/>), so a route that gets a channel later does not post every open request's
/// history. The stamp gates the parent post alone: a request with a stored thread keeps posting its later rows into it when the channel
/// changes. A request opened before the stamp gets no thread, its rows are marked with the gate as the reason and never posted,
/// whichever pass reads them (each row carries its request's opened time, so a later row read in a later pass is gated the same
/// way as its parent), and a follow-up of it starts a thread of its own.</para>
///
/// <para>When a parent post fails, the request has no thread, so its later posts are skipped with one log line and marked, never
/// posted at the top level. A 429 ends the pass: the post's mark is released, the channel waits for Retry-After in the shared
/// <see cref="SlackPostThrottle"/>, and no pass posts to it before then. Every other failure keeps its mark.</para>
/// </summary>
public sealed class SlackThreadPass
{
    /// <summary>The most rows one pass takes.</summary>
    public const int BatchSize = 100;

    /// <summary>How far back a pass reads. A row older than this is neither read nor marked, so the read walks the journal's time index
    /// over a week of rows rather than the whole journal when nothing is pending. A week holds a 429's wait many times over, and a bot
    /// token set within a week of the channel posts that week's requests.</summary>
    public static readonly TimeSpan ReadWindow = TimeSpan.FromDays(7);

    /// <summary>How long a change's <c>action_called</c> row stands with no result row before the thread says the result is unknown.
    /// The same wait as a proposal's unknown state.</summary>
    public static readonly TimeSpan UnknownAfter = TimeSpan.FromMinutes(10);

    /// <summary>The kind the pass gives a change whose result never came. Not a journal kind: the row is the <c>action_called</c> row.</summary>
    public const string ActionUnknown = "action_unknown";

    /// <summary>The reason on the mark of a row that is not posted because its request opened before the route got its bot channel.</summary>
    public const string OpenedBeforeChannel = "opened before the route's bot channel was set";

    /// <summary>
    /// The journal kinds that post. The parent from <c>opened</c>; the claim; the report; each change's result (<c>action_done</c>,
    /// <c>action_failed</c>, a person's <c>action_undone</c>); a proposal when made and at each final state; and the end:
    /// <c>resolved</c>, <c>closed</c>, <c>expired</c> and <c>claims_exhausted</c>. Not posted: <c>joined</c> (the alert webhook
    /// already posts the new alert), <c>action_called</c> (its result posts, else result unknown), <c>proposal_first_press</c> and
    /// <c>proposal_approved</c> (the run or failure follows in seconds and names the approver), and every agent-facing or delivery
    /// mark.
    /// </summary>
    public static readonly IReadOnlyList<string> PostedEvents =
    [
        InvestigationEvents.Opened, "claimed", "reported",
        "action_done", "action_failed", "action_undone",
        "proposal_created", "proposal_run", "proposal_failed",
        "proposal_refused", "proposal_expired", "proposal_superseded",
        "proposal_voided", "proposal_unknown",
        InvestigationEvents.Resolved, "closed", InvestigationEvents.Expired, InvestigationEvents.ClaimsExhausted,
    ];

    private static readonly HashSet<string> s_posted = new(PostedEvents, StringComparer.Ordinal);

    private readonly ISlackThreadRowSource _rows;
    private readonly ISlackRequestThreadStore _threads;
    private readonly SlackThreadPoster _poster;
    private readonly SlackPostThrottle _throttle;
    private readonly Func<IReadOnlyList<NotificationRoute>> _routes;
    private readonly ILogger _logger;
    private readonly McpDataDetail _dataDetail;
    private readonly Func<string> _baseUrl;
    private readonly Func<DateTime> _utcNow;
    private readonly Func<bool> _ready;

    /// <param name="ready">Whether the bot can post now (a token is set). False leaves every row unmarked and posts nothing.</param>
    public SlackThreadPass(ISlackThreadRowSource rows, ISlackRequestThreadStore threads, SlackThreadPoster poster, SlackPostThrottle throttle,
        Func<IReadOnlyList<NotificationRoute>> routes, ILogger logger, McpDataDetail dataDetail, Func<string> baseUrl,
        Func<DateTime>? utcNow = null, Func<bool>? ready = null)
    {
        _rows = rows ?? throw new ArgumentNullException(nameof(rows));
        _threads = threads ?? throw new ArgumentNullException(nameof(threads));
        _poster = poster ?? throw new ArgumentNullException(nameof(poster));
        _throttle = throttle ?? throw new ArgumentNullException(nameof(throttle));
        _routes = routes ?? throw new ArgumentNullException(nameof(routes));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _dataDetail = dataDetail;
        _baseUrl = baseUrl ?? throw new ArgumentNullException(nameof(baseUrl));
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        _ready = ready ?? (() => true);
    }

    /// <summary>One pass. Answers how many posts Slack took. Throws what the store throws.</summary>
    public async Task<int> RunOnceAsync(CancellationToken cancellationToken)
    {
        if (!_ready())
        {
            return 0;
        }

        var now = DateTime.SpecifyKind(_utcNow(), DateTimeKind.Utc);
        var routes = _routes();
        var rows = await _rows.PendingSlackThreadRowsAsync(PostedEvents, now - ReadWindow, now - UnknownAfter, BatchSize, cancellationToken);
        var posted = 0;
        var skippedRequests = new HashSet<Guid>();
        var gatedRequests = new HashSet<Guid>();
        var blockedChannels = new HashSet<string>(StringComparer.Ordinal);
        foreach (var row in rows)
        {
            if (!s_posted.Contains(row.Event) && !string.Equals(row.Event, ActionUnknown, StringComparison.Ordinal))
            {
                continue;
            }

            /* The route list is the service's own; a route gone from it, or with its channel cleared since the read, waits unmarked. */
            var route = routes.FirstOrDefault(r => r.RouteId == row.RouteId);
            if (route is null || !route.Enabled || string.IsNullOrWhiteSpace(route.SlackBotChannel))
            {
                continue;
            }

            var channel = route.SlackBotChannel.Trim();
            if (_throttle.BlockedUntil(channel, now) is DateTime until)
            {
                if (blockedChannels.Add(channel))
                {
                    _logger.LogInformation("Slack thread posts to the bot channel of route {RouteId} wait until {Until:O} after a 429", row.RouteId, until);
                }

                continue;
            }

            var isParent = string.Equals(row.PostKey, SlackThreadPosts.RequestKey, StringComparison.Ordinal);
            var threadRequest = row.RequestId;
            if (isParent && row.FollowUpOf is Guid parent)
            {
                var parentThread = await _threads.FindAsync(parent, cancellationToken);
                if (parentThread is not null && string.Equals(parentThread.ChannelId, channel, StringComparison.Ordinal))
                {
                    threadRequest = parent;
                }
            }

            /* The gate: a parent that would open a thread of its own posts only when the request opened at or after the route got its
               channel. A gated request's rows are marked with the gate as the reason, so the next pass does not read them; a stored
               thread (the channel changed under a request in flight) is not gated, since its parent is already marked and its later
               rows reply in that thread. */
            var openedBeforeChannel = route.SlackBotChannelSetAt is DateTime setAt && row.OpenedAtUtc < setAt;
            if (isParent && threadRequest == row.RequestId && openedBeforeChannel)
            {
                await MarkGatedAsync(row, gatedRequests, cancellationToken);
                continue;
            }

            var target = new SlackThreadTarget(row.RequestId, threadRequest, row.RouteId, row.ServerId, channel, Practice: false);
            var changes = string.Equals(row.Event, "reported", StringComparison.Ordinal)
                ? await _rows.ChangesForRequestAsync(row.RequestId, cancellationToken)
                : [];
            var content = Content(row, changes, _dataDetail, _baseUrl() ?? "");
            var outcome = await _poster.PostAsync(target, row.PostKey, content, cancellationToken);
            switch (outcome)
            {
                case SlackThreadPostOutcome.Posted:
                    posted++;
                    break;
                case SlackThreadPostOutcome.RateLimited:
                    /* The mark is released and the channel blocked by the poster; the rest of the rows wait for the next pass. */
                    return posted;
                case SlackThreadPostOutcome.Failed when isParent:
                    if (skippedRequests.Add(row.RequestId))
                    {
                        _logger.LogWarning("Slack thread parent for investigation request {RequestId} was not delivered; its later posts are skipped", row.RequestId);
                    }

                    break;
                case SlackThreadPostOutcome.NoThread when openedBeforeChannel:
                    /* A later row of a request the gate skipped, in this pass or an earlier one (a request's rows are read 100 at a time
                       across requests, so its parent and its later rows often land in different passes): the request has no thread and
                       opened before the channel, so the row is marked with the gate as the reason and never posted at the top level. */
                    await MarkGatedAsync(row, gatedRequests, cancellationToken);
                    break;
                case SlackThreadPostOutcome.NoThread:
                    /* The parent post failed in this pass or an earlier one: the row is marked so it is not read again, and never posted at the top level. */
                    await _threads.MarkOnceAsync(row.RequestId, row.RouteId, row.ServerId, row.PostKey, SlackThreadPosts.OnceKey(row.RequestId, row.PostKey), cancellationToken);
                    if (skippedRequests.Add(row.RequestId))
                    {
                        _logger.LogWarning("Slack thread posts for investigation request {RequestId} are skipped: its parent post was not delivered", row.RequestId);
                    }

                    break;
                default:
                    break;
            }
        }

        return posted;
    }

    /* The gate's mark: the same mark row and once key a sent post gets, with the gate as the reason, so the read never returns the row
       again and the journal says why it was not posted. One log line per request per pass. */
    private async Task MarkGatedAsync(SlackThreadJournalRow row, HashSet<Guid> gatedRequests, CancellationToken cancellationToken)
    {
        await _threads.MarkSkippedAsync(row.RequestId, row.RouteId, row.ServerId, row.PostKey, SlackThreadPosts.OnceKey(row.RequestId, row.PostKey), OpenedBeforeChannel, cancellationToken);
        if (gatedRequests.Add(row.RequestId))
        {
            _logger.LogInformation("Slack thread posts for investigation request {RequestId} are skipped: it opened at {OpenedAt:O}, before the route's bot channel was set",
                row.RequestId, row.OpenedAtUtc);
        }
    }

    /// <summary>PURE: the post of one journal row, at <paramref name="mode"/>. Every collected value goes through the builders' one escape.</summary>
    internal static SlackThreadPostContent Content(SlackThreadJournalRow row, IReadOnlyList<RequestChangeRow> changes, McpDataDetail mode, string baseUrl)
    {
        ArgumentNullException.ThrowIfNull(row);
        var detail = ParseObject(row.DetailJson);
        switch (row.Event)
        {
            case InvestigationEvents.Opened:
                return SlackThreadPosts.RequestPost(
                    new SlackRequestFacts(row.RequestId, row.ServerName, row.Metric, row.Severity,
                        row.FollowUpOf is null ? (string.IsNullOrWhiteSpace(row.Trigger) ? "alert" : row.Trigger) : InvestigationEvents.FollowUp, baseUrl),
                    mode);
            case "claimed":
                return SlackThreadPosts.ClaimPost(row.Actor, row.ClaimNo);
            case "reported":
                return SlackThreadPosts.ReportPost(
                    new ReportNoticeContent(row.RequestId, row.Verdict ?? row.Reason ?? "", row.ServerName, row.Metric, row.RootCause,
                        changes.Select(c => new ReportNoticeChange(c.Tool, InvestigationEventDelivery.ChangeObject(c.TargetId, c.Database, mode), ResultWord(c.Result))).ToList(),
                        baseUrl),
                    mode);
            case "action_done":
            case "action_failed":
            case "action_undone":
            case ActionUnknown:
                {
                    var called = ParseObject(row.CalledDetailJson) ?? (string.Equals(row.Event, ActionUnknown, StringComparison.Ordinal) ? detail : null);
                    var target = called?["target"] as JsonObject;
                    var id = Text(target?["id"]) ?? "";
                    var database = Text(target?["database"]);
                    var objectText = string.IsNullOrWhiteSpace(database) ? id : id + " in " + database;
                    var word = row.Event switch
                    {
                        "action_done" => "done",
                        "action_failed" => "failed",
                        "action_undone" => "undone" + ByWord(row.Actor),
                        _ => "result unknown",
                    };
                    return SlackThreadPosts.ChangePost(Text(called?["tool"]) ?? "change", objectText, word, mode);
                }

            case InvestigationEvents.Resolved:
                return SlackThreadPosts.ResolutionPost(string.IsNullOrWhiteSpace(row.Reason) ? row.Metric : row.Reason);
            case "closed":
                return SlackThreadPosts.EndPost("Closed", row.Actor, row.Reason);
            case InvestigationEvents.Expired:
                return SlackThreadPosts.EndPost("Expired", null, Unclaimed(detail) ? "no agent claimed it" : "the time ran out with no report");
            case InvestigationEvents.ClaimsExhausted:
                return SlackThreadPosts.EndPost("Claims exhausted", null, "every claim lapsed with no report");
            default:
                {
                    /* A proposal row: the state is the kind's last word, the deciding person when the row names one. */
                    var state = row.Event.StartsWith("proposal_", StringComparison.Ordinal) ? row.Event["proposal_".Length..] : row.Event;
                    var decidedBy = Text(detail?["by"]) ?? Text(detail?["approved_by"]) ?? Text(detail?["approver"])
                        ?? (string.Equals(row.Actor, "service", StringComparison.Ordinal) ? null : row.Actor);
                    var stateText = state + (decidedBy is null ? "" : " by " + decidedBy)
                        + (!string.IsNullOrWhiteSpace(row.Reason) && !string.Equals(row.Event, "proposal_created", StringComparison.Ordinal)
                            ? ": " + row.Reason
                            : "");
                    var proposalUrl = Guid.TryParse(Text(detail?["proposal_id"]), out var proposalId) ? ProposalNotice.LinkFor(baseUrl, proposalId) : null;
                    /* The card says when the proposal expires: its row's time plus the one proposal lifetime, so no second read is needed. */
                    var expiresUtc = string.Equals(row.Event, "proposal_created", StringComparison.Ordinal)
                        ? row.EventTimeUtc.AddMinutes(ProposalRules.ExpiryMinutes)
                        : (DateTime?)null;
                    return SlackThreadPosts.ProposalPost(Text(detail?["tool"]) ?? "action", row.ServerName, stateText, null, proposalUrl, mode, expiresUtc);
                }
        }
    }

    private static string ByWord(string actor) =>
        string.IsNullOrWhiteSpace(actor) || string.Equals(actor, "service", StringComparison.Ordinal) ? "" : " by " + actor;

    private static string ResultWord(AgentChangeResult result) => result switch
    {
        AgentChangeResult.Done => "done",
        AgentChangeResult.Failed => "failed",
        _ => "result unknown",
    };

    private static bool Unclaimed(JsonObject? detail) =>
        detail?["unclaimed"] is JsonValue value && value.TryGetValue<bool>(out var unclaimed) && unclaimed;

    private static string? Text(JsonNode? node) => node is JsonValue value ? value.ToString() : null;

    private static JsonObject? ParseObject(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return null;
        }

        try
        {
            return JsonNode.Parse(json) as JsonObject;
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
