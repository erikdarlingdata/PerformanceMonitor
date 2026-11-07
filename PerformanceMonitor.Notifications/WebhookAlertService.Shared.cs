/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Notifications;

public partial class WebhookAlertService
{
    /// <summary>
    /// Splits a synthesized advice paragraph ("Investigation: ..." / "Remediation: ...") into a
    /// (label, value) pair on the first ": ". Falls back to ("Detail", paragraph) when there is no
    /// leading label. Depends on advice prose containing no interior blank-line break so each
    /// Body chunk is a single labelled paragraph (audited safe for the current FactAdvice blocks);
    /// a future block with a paragraph break degrades to the "Detail" fallback rather than breaking.
    /// </summary>
    private static (string Label, string Value) SplitProseLabel(string paragraph)
    {
        var idx = paragraph.IndexOf(": ", StringComparison.Ordinal);
        if (idx > 0)
        {
            return (paragraph.Substring(0, idx), paragraph.Substring(idx + 2));
        }
        return ("Detail", paragraph);
    }

    /* Reuse HttpClients instead of newing one (plus a handler) per send. A fresh
       HttpClient/handler per request leaks sockets into TIME_WAIT and can exhaust
       the ephemeral port range when many servers alert. One pooled client covers the
       no-proxy case; proxied clients are cached per proxy address. #6231: the two direct clients connect through
       WebhookConnectGuard, which refuses link-local and cloud instance addresses (a proxied client's connection to its proxy goes through it too, #6392). */
    private static readonly HttpClient s_defaultClient =
        new(WebhookConnectGuard.NewHandler())
        { Timeout = TimeSpan.FromSeconds(30) };

    private static readonly ConcurrentDictionary<string, HttpClient> s_proxyClients = new();

    /* #5366: a send that carries the generic headers does not follow redirects, so the headers go to the URL they
       were set up for and nowhere else. These two clients serve only those sends; sends without headers keep the
       clients above. */
    private static readonly HttpClient s_defaultNoRedirectClient =
        new(WebhookConnectGuard.NewHandler(allowAutoRedirect: false))
        { Timeout = TimeSpan.FromSeconds(30) };

    private static readonly ConcurrentDictionary<string, HttpClient> s_proxyNoRedirectClients = new();

    /* #4752: how long ONE webhook post may take, from the send to the last byte of the response. The pooled
       clients' 30-second Timeout stays as the outer bound and is not what a post normally meets. A delivery
       posts to up to four channels one after another, so an endpoint that accepts the connection and never
       answers used to hold that delivery for 30 seconds per channel, and nothing could cancel it. Ten seconds
       is generous for Teams, Slack, PagerDuty or an automation endpoint to acknowledge one small JSON body; a
       slower one is reported as failed, with the timeout as the reason, instead of being waited on. */
    internal static readonly TimeSpan WebhookPostTimeout = TimeSpan.FromSeconds(10);

    private static HttpClient GetHttpClient(string? proxyAddress, bool followRedirects = true)
    {
        if (string.IsNullOrWhiteSpace(proxyAddress))
            return followRedirects ? s_defaultClient : s_defaultNoRedirectClient;

        return (followRedirects ? s_proxyClients : s_proxyNoRedirectClients).GetOrAdd(proxyAddress, addr =>
            new HttpClient(NewProxyHandler(addr, followRedirects))
            { Timeout = TimeSpan.FromSeconds(30) });
    }

    /* #6392: the proxied handler's connect goes through WebhookConnectGuard too, so the connection to the proxy itself is
       checked: a proxy that resolves to a link-local or cloud instance address is refused, and a proxy on loopback or a
       private address still works. */
    internal static SocketsHttpHandler NewProxyHandler(string proxyAddress, bool followRedirects) => new()
    {
        Proxy = new WebProxy(proxyAddress),
        UseProxy = true,
        PooledConnectionLifetime = TimeSpan.FromMinutes(5),
        AllowAutoRedirect = followRedirects,
        ConnectCallback = WebhookConnectGuard.ConnectAsync,
    };

    /// <summary>The health entry of one destination: a channel, and the route that supplies the destination (null for
    /// the settings row's own). Created on first use.</summary>
    private DestinationHealth HealthOf(string channel, int? routeId) =>
        _destinationHealth.GetOrAdd((channel, routeId), _ => new DestinationHealth());

    /// <summary>
    /// The health entries a send to <paramref name="destination"/> is counted against, on every send path (the fan-out's
    /// four channels, a resolution, a notice, a report notice, a PagerDuty resolve): every entry that names the
    /// destination. The settings row's own (null) when the destination is its URL or key for <paramref name="channel"/>,
    /// and each route's, enabled or disabled, whose destination it is: an alert that fired through a route is resolved
    /// after the operator turns the route off, and its outcome is still the route's. A send to one place is a send from
    /// every configured place that names it, so a failure raises each of these counts and a delivery clears each
    /// (<see cref="ClearDestination"/>), and each reaches <see cref="FailingChannelThreshold"/> at the cadence of the
    /// sends to the place, not a share of it. Empty for a destination that no configured channel names (a route's URL
    /// that was edited away): its outcome is logged and counted nowhere, because no entry a host could announce belongs
    /// to it.
    /// </summary>
    private static List<int?> EntriesNaming(IAlertSettings delivery, string channel, string destination)
    {
        var entries = new List<int?>();
        if (IsDestination(SettingsRowDestination(delivery, channel), destination))
        {
            entries.Add(null);
        }

        foreach (var route in delivery.NotificationRoutes)
        {
            if (route is not null && IsDestination(RouteDestination(route, channel), destination))
            {
                entries.Add(route.RouteId);
            }
        }

        return entries;
    }

    private static string? SettingsRowDestination(IAlertSettings settings, string channel) => channel switch
    {
        NotificationRouter.TeamsChannel => settings.TeamsWebhookUrl,
        NotificationRouter.SlackChannel => settings.SlackWebhookUrl,
        NotificationRouter.GenericChannel => settings.GenericWebhookUrl,
        NotificationRouter.PagerDutyChannel => settings.PagerDutyRoutingKey,
        _ => null,
    };

    private static string? RouteDestination(NotificationRoute route, string channel) => channel switch
    {
        NotificationRouter.TeamsChannel => route.TeamsUrl,
        NotificationRouter.SlackChannel => route.SlackUrl,
        NotificationRouter.GenericChannel => route.GenericUrl,
        NotificationRouter.PagerDutyChannel => route.PagerDutyRoutingKey,
        _ => null,
    };

    /// <summary>Whether <paramref name="configured"/>, the settings row's or a route's destination, is
    /// <paramref name="destination"/>. The router hands a route's destination out trimmed and the settings row's as it
    /// is stored, so both forms match.</summary>
    private static bool IsDestination(string? configured, string destination) =>
        !string.IsNullOrWhiteSpace(configured)
        && (string.Equals(configured, destination, StringComparison.Ordinal)
            || string.Equals(configured.Trim(), destination, StringComparison.Ordinal));

    /// <summary>
    /// A send that came back with an error text: count it against every entry that names its destination
    /// (<paramref name="entries"/>, from <see cref="EntriesNaming"/>), keep it as each one's last error, and log each
    /// entry loudly for its first <see cref="FailingChannelThreshold"/> failures in a row, then on every 50th (#7225: one
    /// copy for the four webhook channels). The log line names the route, never the URL. A destination no entry names
    /// is logged once and counted nowhere.
    /// </summary>
    private void RecordChannelFailure(string channel, List<int?> entries, string error)
    {
        if (entries.Count == 0)
        {
            _logger.LogError($"{channel.ToUpperInvariant()} WEBHOOK FAILED (a destination no channel names, not counted): {error}");
            return;
        }

        foreach (var routeId in entries)
        {
            var health = HealthOf(channel, routeId);
            var failures = Interlocked.Increment(ref health.Failures);
            health.LastError = error;
            var where = routeId is { } id ? $", route {id}" : "";

            if (failures <= FailingChannelThreshold)
                _logger.LogError($"{channel.ToUpperInvariant()} WEBHOOK FAILED ({failures}x{where}): {error}");
            else if (failures % 50 == 0)
                _logger.LogError($"{channel.ToUpperInvariant()} WEBHOOK STILL FAILING: {failures} failures{where}. Last: {error}");
        }
    }

    /// <summary>A send that threw: count it against every entry that names its destination and keep the message as each
    /// one's last error, logging every one (no throttle). A destination no entry names is logged and counted nowhere.</summary>
    private void RecordChannelException(string channel, List<int?> entries, Exception ex)
    {
        if (entries.Count == 0)
        {
            _logger.LogError(ex, $"{channel} (a destination no channel names) webhook error: {ex.Message}");
            return;
        }

        foreach (var routeId in entries)
        {
            var health = HealthOf(channel, routeId);
            Interlocked.Increment(ref health.Failures);
            health.LastError = ex.Message;
            _logger.LogError(ex, $"{WebhookChannelFailureCount.LabelOf(channel, routeId)} webhook error: {ex.Message}");
        }
    }

    /// <summary>A send that succeeded: clear every entry that names its destination (<see cref="ClearDestination"/>),
    /// and say so for each one that ends a run of failures. Other destinations of the channel are not touched, and a
    /// destination with no entry has nothing to clear. The log line names the route, never the URL.</summary>
    private void RecordChannelRecovered(string channel, List<int?> entries)
    {
        foreach (var (label, failures) in ClearDestination(channel, entries))
        {
            _logger.LogInformation($"{label} webhook recovered after {failures} failure(s)");
        }
    }

    /// <summary>
    /// A send was delivered: clear the count and last error of every entry that names its destination
    /// (<paramref name="entries"/>, from <see cref="EntriesNaming"/>), creating each so a destination a send has gone to
    /// is listed. They are the entries a failure to the destination is counted against, so no count survives a working
    /// delivery to its place, which would raise the failing signal across it. Returns the label and the count of each
    /// cleared entry that was failing. A destination with no entry is named by nothing, so nothing is cleared.
    /// </summary>
    private List<(string Label, int Failures)> ClearDestination(string channel, List<int?> entries)
    {
        var cleared = new List<(string Label, int Failures)>();
        foreach (var routeId in entries)
        {
            var health = HealthOf(channel, routeId);
            var failures = Interlocked.Exchange(ref health.Failures, 0);
            health.LastError = null;
            if (failures > 0)
            {
                cleared.Add((WebhookChannelFailureCount.LabelOf(channel, routeId), failures));
            }
        }

        return cleared;
    }

    /// <summary>
    /// Posts a JSON payload to a webhook URL, giving up after <see cref="WebhookPostTimeout"/>. Returns null on
    /// success, error message on failure; a timeout is an error message, and a cancelled
    /// <paramref name="cancellationToken"/> is not one — it propagates as the cancellation it is.
    /// </summary>
    /// <param name="headers">
    /// The generic channel's operator-authored request headers. <c>null</c> — what Teams/Slack pass — sends
    /// exactly today's request (no custom headers, no User-Agent); those two endpoints need neither.
    /// </param>
    /// <param name="cancellationToken">
    /// #4752: the caller's token, so a service that is stopping can end a post in flight.
    /// </param>
    private static Task<string?> PostWebhookAsync(
        string webhookUrl,
        string jsonPayload,
        string? proxyAddress,
        IReadOnlyDictionary<string, string>? headers = null,
        ILogger? bodyLog = null,
        bool bodyInResult = false,
        CancellationToken cancellationToken = default) =>
        PostWebhookAsync(
            webhookUrl, jsonPayload, proxyAddress, headers, WebhookPostTimeout, signing: null, cancellationToken, bodyLog, bodyInResult);

    /// <summary>
    /// The post with its timeout stated, so a test can hang an endpoint and wait 200 ms for the answer instead
    /// of <see cref="WebhookPostTimeout"/>. Everything else is the private overload's contract.
    /// </summary>
    internal static Task<string?> PostWebhookAsync(
        string webhookUrl,
        string jsonPayload,
        string? proxyAddress,
        IReadOnlyDictionary<string, string>? headers,
        TimeSpan postTimeout,
        CancellationToken cancellationToken)
        => PostWebhookAsync(webhookUrl, jsonPayload, proxyAddress, headers, postTimeout, signing: null, cancellationToken);

    /// <summary>
    /// The post with a signature (#5998): <paramref name="signing"/> null sends exactly the unsigned post.
    /// </summary>
    internal static async Task<string?> PostWebhookAsync(
        string webhookUrl,
        string jsonPayload,
        string? proxyAddress,
        IReadOnlyDictionary<string, string>? headers,
        TimeSpan postTimeout,
        WebhookSendSigning? signing,
        CancellationToken cancellationToken,
        ILogger? bodyLog = null,
        bool bodyInResult = false)
    {
        var outcome = await PostOnceAsync(
            client: null, webhookUrl, jsonPayload, proxyAddress, headers, postTimeout, signing, bodyLog, bodyInResult, cancellationToken);
        return outcome.Error;
    }

    /// <summary>
    /// What one post came back with (#8244): the error text to record (null when delivered), and whether the endpoint
    /// answered HTTP 429, with the <c>Retry-After</c> it sent when it sent one. A 429 is reported, not logged, so the
    /// caller can hold the destination and post once more before it counts anything.
    /// </summary>
    private readonly record struct WebhookPostOutcome(string? Error, bool RateLimited, RetryConditionHeaderValue? RetryAfter);

    /// <summary>
    /// One post, with the client stated: <paramref name="client"/> null picks the pooled client for the proxy and the
    /// redirect rule; a caller that holds its own client (the rate-limit test seam) passes it. Everything else is
    /// <see cref="PostWebhookAsync(string, string, string?, IReadOnlyDictionary{string, string}?, TimeSpan, WebhookSendSigning?, CancellationToken, ILogger?, bool)"/>'s
    /// contract, except that an HTTP 429 comes back as <see cref="WebhookPostOutcome.RateLimited"/> with its status text
    /// as the error and nothing logged.
    /// </summary>
    private static async Task<WebhookPostOutcome> PostOnceAsync(
        HttpClient? client,
        string webhookUrl,
        string jsonPayload,
        string? proxyAddress,
        IReadOnlyDictionary<string, string>? headers,
        TimeSpan postTimeout,
        WebhookSendSigning? signing,
        ILogger? bodyLog,
        bool bodyInResult,
        CancellationToken cancellationToken)
    {
        var sendsHeaders = headers is { Count: > 0 };
        /* #6003 review: a signed send is held to #5366's rule too. A followed 307/308 would carry the body and a valid
           webhook-signature to whatever host the endpoint names. */
        var noRedirects = sendsHeaders || signing is not null;
        client ??= GetHttpClient(proxyAddress, followRedirects: !noRedirects);

        /* #5998: a signed send puts these exact bytes on the wire and signs these same bytes, so no re-encoding sits
           between the signature and the body. An unsigned send is the StringContent it always was. */
        var bodyBytes = signing is null ? null : Encoding.UTF8.GetBytes(jsonPayload);
        using HttpContent content = bodyBytes is null
            ? new StringContent(jsonPayload, Encoding.UTF8, "application/json")
            : new ByteArrayContent(bodyBytes);
        if (bodyBytes is not null)
        {
            content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json") { CharSet = "utf-8" };
        }

        using var request = new HttpRequestMessage(HttpMethod.Post, webhookUrl) { Content = content };

        if (headers != null)
        {
            ApplyHeaders(request, content, headers);
        }

        if (signing is not null && bodyBytes is not null)
        {
            /* Set after the operator's headers, so an operator header of the same name cannot replace a signature. */
            foreach (var name in new[] { WebhookSigning.IdHeader, WebhookSigning.TimestampHeader, WebhookSigning.SignatureHeader })
            {
                request.Headers.Remove(name);
            }

            request.Headers.TryAddWithoutValidation(WebhookSigning.IdHeader, signing.MessageId);
            request.Headers.TryAddWithoutValidation(WebhookSigning.TimestampHeader, signing.Timestamp.ToString(CultureInfo.InvariantCulture));
            request.Headers.TryAddWithoutValidation(
                WebhookSigning.SignatureHeader, WebhookSigning.Sign(signing.Keys, signing.MessageId, signing.Timestamp, bodyBytes));
        }

        /* #4752: one token bounds the send and the read of the response. It is linked to the caller's, so
           either ends the post, and the catch below tells the two apart: the timeout is this post's own
           failure and becomes its error text, but the caller's cancel is a stop request and must reach the
           caller as one, not be reported as an endpoint that was slow. */
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(postTimeout);

        try
        {
            using var response = await client.SendAsync(request, timeout.Token);

            if (response.IsSuccessStatusCode)
                return new WebhookPostOutcome(null, false, null);

            /* #5366: a redirect is not followed on a send with headers or a signature. The message names no URL and no header. */
            if (noRedirects && (int)response.StatusCode is >= 300 and < 400)
            {
                return new WebhookPostOutcome(string.Create(CultureInfo.InvariantCulture,
                    $"HTTP {(int)response.StatusCode}: the endpoint answered with a redirect. Set the webhook URL to the final address."), false, null);
            }

            /* #8244: the endpoint is pacing the sender, not refusing the message. The body is not read: nothing from a
               429 is stored or logged here, and the caller decides whether this attempt is the last. */
            if (response.StatusCode == HttpStatusCode.TooManyRequests)
            {
                return new WebhookPostOutcome(HttpStatusText(response.StatusCode), true, response.Headers.RetryAfter);
            }

            /* #6231: the text this returns is what the alert log's send_error and the channel health getter store and
               serve, and a read-only MCP client can read both. A destination can be any address the host can reach,
               so what it answers with must not be stored: the stored text keeps the status and its reason, and the
               first 2 KB of the body goes to the log only (capped, because an unbounded read lets an endpoint bloat
               the log). The interactive Test buttons (bodyInResult) show the operator the body, because nothing from
               them is stored. */
            var body = await response.Content.ReadAsStringAsync(timeout.Token);
            if (body.Length > 2048)
                body = string.Concat(body.AsSpan(0, 2048), "…(truncated)");

            var statusText = HttpStatusText(response.StatusCode);
            if (bodyInResult)
                return new WebhookPostOutcome($"{statusText}: {body}", false, null);

            if (bodyLog is not null)
            {
                var oneLine = body.Replace('\r', ' ').Replace('\n', ' ');
                bodyLog.LogError($"Webhook post failed with {statusText}; the endpoint's response: {oneLine}");
            }

            return new WebhookPostOutcome(statusText, false, null);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return new WebhookPostOutcome(
                string.Create(CultureInfo.InvariantCulture, $"timed out after {postTimeout.TotalSeconds:0.###} seconds"), false, null);
        }
    }

    /// <summary>
    /// "HTTP 500 Internal Server Error": the status and its standard name, never the reason phrase the endpoint sent,
    /// which is free text the endpoint chooses.
    /// </summary>
    internal static string HttpStatusText(HttpStatusCode status)
    {
        var code = (int)status;
        return Enum.IsDefined(status)
            ? string.Create(CultureInfo.InvariantCulture, $"HTTP {code} {Regex.Replace(status.ToString(), "(?<=[a-z])(?=[A-Z])", " ")}")
            : string.Create(CultureInfo.InvariantCulture, $"HTTP {code}");
    }

    /// <summary>
    /// Applies the operator's headers to the request, routing content headers (a Content-Type override) onto
    /// the content — setting one on <see cref="HttpRequestMessage.Headers"/> is rejected, so a naive add would
    /// silently drop it. Values are added without validation: they are the operator's own, and a strict parse
    /// would reject perfectly good tokens (CR/LF is already rejected upstream in <see cref="TryParseHeaders"/>).
    /// </summary>
    internal static void ApplyHeaders(HttpRequestMessage request, HttpContent content, IReadOnlyDictionary<string, string> headers)
    {
        foreach (var (name, value) in headers)
        {
            if (request.Headers.TryAddWithoutValidation(name, value))
            {
                continue;
            }

            /* A content header (e.g. Content-Type) — StringContent already set one, so replace it. */
            content.Headers.Remove(name);
            content.Headers.TryAddWithoutValidation(name, value);
        }

        if (!request.Headers.Contains("User-Agent"))
        {
            request.Headers.TryAddWithoutValidation("User-Agent", DefaultUserAgent);
        }
    }
}
