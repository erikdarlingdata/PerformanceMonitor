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
/// <para><b>HTTP 429.</b> A 429 says the endpoint is pacing the sender, not refusing the message, so it is not a failed
/// send yet: the post waits what <c>Retry-After</c> asks for (a delay in seconds or an HTTP date, bounded by
/// <see cref="MaxRetryAfter"/>; <see cref="DefaultRetryAfter"/> when the header is missing or unusable) and is sent once
/// more. Only the retry's answer is recorded, so a 429 that clears on the retry moves no failure count and raises no
/// "Notification Channel Failing"; a 429 on the retry is the destination's failure, with the status as its error.</para>
///
/// <para><b>Per-event spacing.</b> A Per-event delivery posts one message per incident to one destination, back to back, and
/// an incoming webhook that allows about one message a second answers the burst with 429s. Each destination has a next
/// slot: a post takes the slot and moves it <see cref="PerEventPostSpacing"/> on, so the second post in a burst waits
/// for the first's second to pass. A Summary post takes the slot without moving it, so a single card never waits on this,
/// but it does wait out a hold a 429 left behind. One instance per service, so concurrent deliveries to one place queue on
/// the same slot.</para>
///
/// <para>The clock and the wait are seams (<see cref="PacingClock"/>, <see cref="Delay"/>), so a test can run a burst
/// against a fixed clock and read the waits it asked for; the pooled HTTP clients are the same, through
/// <see cref="HttpHandlerOverride"/>. Production construction sites set none of them.</para>
/// </summary>
public partial class WebhookAlertService
{
    /// <summary>The longest a 429's <c>Retry-After</c> holds a post. A longer one is waited this long and then tried.</summary>
    internal static readonly TimeSpan MaxRetryAfter = TimeSpan.FromSeconds(60);

    /// <summary>The wait before the retry when a 429 carries no <c>Retry-After</c>, or one that is not a positive delay.</summary>
    internal static readonly TimeSpan DefaultRetryAfter = TimeSpan.FromSeconds(1);

    /// <summary>The least time between two Per-event posts to one destination: Slack's incoming-webhook rate, and under
    /// every other channel's documented limit.</summary>
    internal static readonly TimeSpan PerEventPostSpacing = TimeSpan.FromSeconds(1);

    /// <summary>Test seam: the clock the per-destination slots are read against. Not the payload stamp clock, which a
    /// caller may pin for byte-identity without wanting every post to wait.</summary>
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

    /// <summary>Each destination's next free slot, by destination. Guarded by its own lock: a reservation reads and moves
    /// the slot as one step, so two concurrent posts to one place get two slots.</summary>
    private readonly Dictionary<string, DateTime> _nextPostSlot = new(StringComparer.Ordinal);

    /// <summary>
    /// The service's own post to <paramref name="webhookUrl"/>: waits for <paramref name="destination"/>'s slot, posts, and on
    /// a 429 holds the destination for the endpoint's <c>Retry-After</c> and posts once more. Null when delivered, the error
    /// text when not. The same contract as the static post for everything but the 429.
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
        await WaitForSlotAsync(destination, spacing, cancellationToken);
        var outcome = await PostOnceAsync(
            _clientOverride, webhookUrl, jsonPayload, proxyAddress, headers, WebhookPostTimeout, signing, _logger, bodyInResult: false, cancellationToken);
        if (!outcome.RateLimited)
        {
            return outcome.Error;
        }

        var wait = RetryAfterWait(outcome.RetryAfter, PacingClock());
        _logger.LogInformation(
            $"{channel.ToUpperInvariant()} WEBHOOK RATE LIMITED (HTTP 429): posting again in {wait.TotalSeconds.ToString("0.###", CultureInfo.InvariantCulture)} s");
        HoldDestination(destination, wait);

        await WaitForSlotAsync(destination, spacing, cancellationToken);
        outcome = await PostOnceAsync(
            _clientOverride, webhookUrl, jsonPayload, proxyAddress, headers, WebhookPostTimeout, signing, _logger, bodyInResult: false, cancellationToken);
        return outcome.Error;
    }

    /// <summary>
    /// How long a 429 holds the destination: <paramref name="retryAfter"/>'s delay, or its date read against
    /// <paramref name="nowUtc"/>, bounded by <see cref="MaxRetryAfter"/>; <see cref="DefaultRetryAfter"/> when there is no
    /// header or it is not a positive wait.
    /// </summary>
    internal static TimeSpan RetryAfterWait(RetryConditionHeaderValue? retryAfter, DateTime nowUtc)
    {
        TimeSpan? asked = retryAfter?.Delta;
        if (asked is null && retryAfter?.Date is { } date)
        {
            asked = date.UtcDateTime - nowUtc;
        }

        if (asked is not { } wait || wait <= TimeSpan.Zero)
        {
            return DefaultRetryAfter;
        }

        return wait > MaxRetryAfter ? MaxRetryAfter : wait;
    }

    /// <summary>Takes <paramref name="destination"/>'s next slot, moving it on by <paramref name="spacing"/>, and waits until
    /// the slot comes round. A destination with no slot, or one in the past, posts now.</summary>
    private async Task WaitForSlotAsync(string destination, TimeSpan spacing, CancellationToken cancellationToken)
    {
        TimeSpan wait;
        lock (_nextPostSlot)
        {
            var now = PacingClock();
            var slot = _nextPostSlot.TryGetValue(destination, out var next) && next > now ? next : now;
            wait = slot - now;
            _nextPostSlot[destination] = slot + spacing;
        }

        if (wait > TimeSpan.Zero)
        {
            await Delay(wait, cancellationToken);
        }
    }

    /// <summary>A 429 told the sender to wait <paramref name="wait"/>: no post to <paramref name="destination"/> goes before
    /// then, the retry included. A slot already further out stays where it is.</summary>
    private void HoldDestination(string destination, TimeSpan wait)
    {
        lock (_nextPostSlot)
        {
            var until = PacingClock() + wait;
            if (!_nextPostSlot.TryGetValue(destination, out var next) || next < until)
            {
                _nextPostSlot[destination] = until;
            }
        }
    }
}
