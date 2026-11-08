/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// #8244: how the service's own posts meet an endpoint that paces them. Two things, both keyed by the destination (the
/// URL, or the PagerDuty routing key) rather than by the channel, because two routes can point one channel at two places
/// and the endpoint's limit is the place's.
///
/// <para><b>HTTP 429.</b> A 429 says the endpoint is pacing the sender, not refusing the message. When its
/// <c>Retry-After</c> asks for no more than what is left of the send's bound (<see cref="DefaultRetryAfter"/> when the
/// header is missing or unusable), the post is not a failed send yet: the destination is held for that long, the post
/// waits it out and is sent once more, and only the retry's answer is recorded, so a 429 that clears on the retry moves no
/// failure count and raises no "Notification Channel Failing"; a 429 on the retry is the destination's failure, body
/// logged. A <c>Retry-After</c> over what is left is not waited out: the post fails as any other failed post does, body
/// logged, and the destination is held for what was asked (at most <see cref="MaxHold"/>), so the posts that follow
/// inside the hold do not hammer it.</para>
///
/// <para><b>The bound.</b> One send waits at most <see cref="MaxRetryAfter"/> in all: its waits for the destination's hold,
/// for its Per-event slot and for a 429's <c>Retry-After</c> add up to no more than that. A turn further off than what is
/// left of the bound is not waited for: the post fails without a request and moves no slot. That is what keeps a wait out
/// of the alert passes: Darling evaluates alerts a few servers at a time, and Lite holds a server's alert gate while it
/// sends, so one send on a busy URL holds either for at most the bound, however many posts are queued ahead of it.</para>
///
/// <para><b>The hold.</b> The hold is the destination's own: one place's 429 never holds another. A post with no spacing
/// (a Summary card, a notice, a self-alert) waits for the hold alone; a Per-event post waits for the later of the hold and
/// its slot.</para>
///
/// <para><b>Per-event spacing.</b> A Per-event delivery posts one message per incident to one destination, back to back,
/// and an incoming webhook that allows about one message a second answers the burst with 429s. Each destination has a next
/// slot: a Per-event post takes the slot and moves it <see cref="PerEventPostSpacing"/> on, so the second post in a burst
/// waits for the first's second to pass. A post with no spacing neither takes a slot nor waits for one, so a Summary card
/// or a "Collection Stopped" self-alert never queues behind the slots a burst of Per-event posts has reserved.</para>
///
/// <para>The clock and the wait are seams (<see cref="PacingClock"/>, <see cref="Delay"/>), so a test can run a burst
/// against a fixed clock and read the waits it asked for; the pooled HTTP clients are the same, through
/// <see cref="HttpHandlerOverride"/>. Production construction sites set none of them.</para>
/// </summary>
public partial class WebhookAlertService
{
    /// <summary>The most one send waits in all: for the destination's hold, for its Per-event slot and for a 429's
    /// <c>Retry-After</c> together. A turn or an ask beyond what is left of it is not waited: the post fails, and a 429's
    /// ask still holds the destination.</summary>
    internal static readonly TimeSpan MaxRetryAfter = TimeSpan.FromSeconds(10);

    /// <summary>The wait a 429 asks for when it carries no <c>Retry-After</c>, or one that is not a positive delay.</summary>
    internal static readonly TimeSpan DefaultRetryAfter = TimeSpan.FromSeconds(1);

    /// <summary>The least time between two Per-event posts to one destination: Slack's incoming-webhook rate, and under
    /// every other channel's documented limit.</summary>
    internal static readonly TimeSpan PerEventPostSpacing = TimeSpan.FromSeconds(1);

    /// <summary>The longest a 429 holds a destination, whatever its <c>Retry-After</c> asked for. A broken endpoint that
    /// answers with a date in the wrong year, or a day, silences its destination for this long and not for the day.</summary>
    internal static readonly TimeSpan MaxHold = TimeSpan.FromMinutes(5);

    /// <summary>Test seam: the clock the per-destination slots and holds are read against. Not the payload stamp clock,
    /// which a caller may pin for byte-identity without wanting every post to wait.</summary>
    internal Func<DateTime> PacingClock { get; set; } = static () => DateTime.UtcNow;

    /// <summary>Test seam: the wait a post performs for its slot or a 429's <c>Retry-After</c>.</summary>
    internal Func<TimeSpan, CancellationToken, Task> Delay { get; set; } = static (wait, ct) => Task.Delay(wait, ct);

    /// <summary>Test seam: a handler every post of this instance goes through instead of the pooled clients.</summary>
    internal HttpMessageHandler? HttpHandlerOverride
    {
        get => _handlerOverride;
        set
        {
            _handlerOverride = value;
            _clientOverride = value is null ? null : new HttpClient(value, disposeHandler: false) { Timeout = TimeSpan.FromSeconds(30) };
        }
    }

    private HttpMessageHandler? _handlerOverride;
    private HttpClient? _clientOverride;

    /// <summary>One lock over both tables: a turn reads the hold and moves the slot as one step, so two concurrent posts to
    /// one place get two slots and see the same hold.</summary>
    private readonly object _pacingLock = new();

    /// <summary>Each destination's next free Per-event slot.</summary>
    private readonly Dictionary<string, DateTime> _nextPostSlot = new(StringComparer.Ordinal);

    /// <summary>When each destination's 429 hold ends. A destination with no entry, or one in the past, is not held.</summary>
    private readonly Dictionary<string, DateTime> _holdUntil = new(StringComparer.Ordinal);

    /// <summary>What <see cref="TakeTurnAsync"/> did: the error text to record when the turn was too far off to wait for,
    /// else null, and the wait it performed.</summary>
    private readonly record struct Turn(string? Refusal, TimeSpan Waited);

    /// <summary>
    /// The service's own post to <paramref name="webhookUrl"/>: waits for <paramref name="destination"/>'s turn, posts, and
    /// on a 429 within what is left of the bound holds the destination for the endpoint's <c>Retry-After</c> and posts once
    /// more. Null when delivered, the error text when not. The same contract as the static post for everything but the 429.
    /// </summary>
    /// <param name="destination">What the pacing is keyed by: the URL for the token channels, the routing key for PagerDuty.</param>
    /// <param name="spacing"><see cref="PerEventPostSpacing"/> for a Per-event post, zero for any other.</param>
    private async Task<string?> PostPacedAsync(
        string channel,
        string destination,
        string webhookUrl,
        string jsonPayload,
        string? proxyAddress,
        IReadOnlyDictionary<string, string>? headers,
        WebhookSendSigning? signing,
        TimeSpan spacing,
        CancellationToken cancellationToken)
    {
        /* What this send may still wait: every wait below is taken from it, and a turn or an ask beyond it is not waited. */
        var budget = MaxRetryAfter;

        var turn = await TakeTurnAsync(destination, spacing, budget, cancellationToken);
        if (turn.Refusal is { } refused)
        {
            return refused;
        }

        budget -= turn.Waited;

        var outcome = await PostOnceAsync(
            _clientOverride, webhookUrl, jsonPayload, proxyAddress, headers, WebhookPostTimeout, signing, _logger, bodyInResult: false, cancellationToken,
            retryBound: budget, clock: PacingClock);
        if (!outcome.RateLimited)
        {
            return outcome.Error;
        }

        var until = HoldDestination(destination, outcome.RetryAfter);
        var asked = Seconds(outcome.RetryAfter);
        if (outcome.RetryAfter > budget)
        {
            /* The body has gone to the log already, as for any other failed post. The post is not tried again. */
            _logger.LogWarning(
                $"{channel.ToUpperInvariant()} WEBHOOK RATE LIMITED (HTTP 429): the endpoint asked for a {asked} s wait, over the {Seconds(budget)} s this send may still wait of its {Seconds(MaxRetryAfter)} s; posts to it are held until {until.ToString("HH:mm:ss", CultureInfo.InvariantCulture)} UTC");
            return outcome.Error;
        }

        _logger.LogInformation($"{channel.ToUpperInvariant()} WEBHOOK RATE LIMITED (HTTP 429): posting again in {asked} s");
        turn = await TakeTurnAsync(destination, TimeSpan.Zero, budget, cancellationToken);
        if (turn.Refusal is { } refusedMeanwhile)
        {
            return refusedMeanwhile;
        }

        /* The retry's answer is final: a 429 here is read and logged like any failed post, and its Retry-After holds the
           destination so the posts that follow do not hammer it. */
        outcome = await PostOnceAsync(
            _clientOverride, webhookUrl, jsonPayload, proxyAddress, headers, WebhookPostTimeout, signing, _logger, bodyInResult: false, cancellationToken,
            retryBound: null, clock: PacingClock);
        if (outcome.RateLimited)
        {
            HoldDestination(destination, outcome.RetryAfter);
        }

        return outcome.Error;
    }

    /// <summary>
    /// What a 429's <c>Retry-After</c> asks for: its delay, or its date read against <paramref name="nowUtc"/>;
    /// <see cref="DefaultRetryAfter"/> when there is no header or it is not a positive wait. Not bounded here: the caller
    /// decides what an ask over what is left of <see cref="MaxRetryAfter"/> means.
    /// </summary>
    internal static TimeSpan RetryAfterWait(RetryConditionHeaderValue? retryAfter, DateTime nowUtc)
    {
        TimeSpan? asked = retryAfter?.Delta;
        if (asked is null && retryAfter?.Date is { } date)
        {
            asked = date.UtcDateTime - nowUtc;
        }

        return asked is { } wait && wait > TimeSpan.Zero ? wait : DefaultRetryAfter;
    }

    /// <summary>
    /// Waits for <paramref name="destination"/>'s turn: the later of the hold a 429 left and, for a post with spacing, its
    /// slot, which it then moves on by <paramref name="spacing"/>. A turn further off than <paramref name="budget"/> is
    /// refused: nothing is posted, no slot moves, and the error text to record comes back instead.
    /// </summary>
    private async Task<Turn> TakeTurnAsync(string destination, TimeSpan spacing, TimeSpan budget, CancellationToken cancellationToken)
    {
        TimeSpan wait;
        lock (_pacingLock)
        {
            var now = PacingClock();
            var start = _holdUntil.TryGetValue(destination, out var heldUntil) && heldUntil > now ? heldUntil : now;
            if (spacing > TimeSpan.Zero && _nextPostSlot.TryGetValue(destination, out var slot) && slot > start)
            {
                start = slot;
            }

            wait = start - now;
            if (wait > budget)
            {
                return new Turn(RefusedText(start, wait), TimeSpan.Zero);
            }

            if (spacing > TimeSpan.Zero)
            {
                _nextPostSlot[destination] = start + spacing;
            }
        }

        if (wait > TimeSpan.Zero)
        {
            await Delay(wait, cancellationToken);
        }

        return new Turn(null, wait);
    }

    /// <summary>A 429 told the sender to wait <paramref name="asked"/>: no post to <paramref name="destination"/> goes before
    /// then, bounded by <see cref="MaxHold"/>. A hold already further out stays where it is. Returns when the hold ends.</summary>
    private DateTime HoldDestination(string destination, TimeSpan asked)
    {
        var hold = asked > MaxHold ? MaxHold : asked;
        lock (_pacingLock)
        {
            var until = PacingClock() + hold;
            if (_holdUntil.TryGetValue(destination, out var current) && current > until)
            {
                until = current;
            }

            _holdUntil[destination] = until;
            return until;
        }
    }

    /// <summary>The error a post records when its turn is too far off to wait for: when the turn comes and how far off it
    /// is. Stored and served like any send error, so it names no destination.</summary>
    private static string RefusedText(DateTime turn, TimeSpan away) =>
        $"not posted: the destination is held or queued until {turn.ToString("HH:mm:ss", CultureInfo.InvariantCulture)} UTC, {Seconds(away)} s away, over the {Seconds(MaxRetryAfter)} s one send waits in all";

    private static string Seconds(TimeSpan span) => span.TotalSeconds.ToString("0.###", CultureInfo.InvariantCulture);
}
