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
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Service.Chat;

/// <summary>#7656: how one Slack post ended. <see cref="Ts"/> and <see cref="Channel"/> are Slack's for a delivered post, the
/// thread's handle for every reply that follows. <see cref="Error"/> is Slack's error word, an HTTP status or an exception's text,
/// and never carries the bot token. <see cref="RateLimited"/> is a 429, the one answer that says the post definitely did not go out,
/// with Slack's <c>Retry-After</c> in <see cref="RetryAfter"/> when the header was there.</summary>
public sealed record SlackPostResult(bool Ok, string? Ts, string? Channel, string? Error, bool RateLimited = false, TimeSpan? RetryAfter = null);

/// <summary>#7656: posts one message to a Slack channel or thread as the bot. The one seam the thread poster sends through, so a
/// test drives it with a recorder and the product with <see cref="SlackBotClient"/>.</summary>
public interface ISlackBotPosts
{
    /// <summary>Posts <paramref name="text"/> (and <paramref name="blocks"/>, when given) to <paramref name="channel"/>, as a reply when
    /// <paramref name="threadTs"/> names a thread parent. Never throws for a failed post: the result says so.</summary>
    Task<SlackPostResult> PostAsync(string channel, string text, IReadOnlyList<object>? blocks, string? threadTs, CancellationToken cancellationToken);
}

/// <summary>#7658: the two calls a press answer needs. <see cref="UpdateAsync"/> edits a card the bot posted (<c>chat.update</c>, the bot token);
/// <see cref="RespondAsync"/> answers the presser alone through the payload's <c>response_url</c>, which needs no token.</summary>
public interface ISlackCardApi
{
    /// <summary>Replaces the text and blocks of the message at <paramref name="channel"/> and <paramref name="ts"/>. Never throws for a failed edit.</summary>
    Task<SlackPostResult> UpdateAsync(string channel, string ts, string text, IReadOnlyList<object>? blocks, CancellationToken cancellationToken);

    /// <summary>Posts an ephemeral answer to <paramref name="responseUrl"/>: only the presser sees it, and the card stays. Never throws for a failed post.</summary>
    Task<SlackPostResult> RespondAsync(Uri responseUrl, string text, IReadOnlyList<object>? blocks, CancellationToken cancellationToken);
}

/// <summary>
/// #7656: the Slack Web API client of the bot: <c>chat.postMessage</c> with the bot token as a bearer header. Every post sets
/// <c>unfurl_links</c> and <c>unfurl_media</c> to false, so Slack fetches no URL an agent wrote (the design's security points).
/// The token is held here and goes only into the Authorization header: it is never logged, never put in a result, and every
/// text that could echo it (a response body, an exception message) passes through <see cref="Redact"/> first.
/// </summary>
public sealed class SlackBotClient : ISlackBotPosts, ISlackCardApi
{
    /// <summary>The host a <c>response_url</c> must be on: Slack's own. A payload that names another host is not answered.</summary>
    public const string ResponseUrlHost = "hooks.slack.com";

    /// <summary>Slack's Web API root. A test points the client at a loopback listener instead.</summary>
    public const string DefaultApiBase = "https://slack.com/api/";

    /// <summary>What a token reads as once redacted from any text that leaves this class.</summary>
    public const string RedactedToken = "[bot token]";

    /// <summary>How long one post may take. Slack answers in well under a second; a hung post must not hold the delivery pass.</summary>
    public static readonly TimeSpan PostTimeout = TimeSpan.FromSeconds(15);

    private static readonly JsonSerializerOptions s_jsonOptions = new() { PropertyNamingPolicy = null };

    private readonly string _token;
    private readonly Uri _postUri;
    private readonly Uri _updateUri;
    private readonly string _responseUrlHost;
    private readonly HttpClient _http;

    /// <param name="botToken">The opened bot token. The caller opens the sealed value; this class never sees the store.</param>
    /// <param name="apiBase">The Web API root, <see cref="DefaultApiBase"/> by default.</param>
    /// <param name="handler">An HTTP handler for a test; the product uses the default.</param>
    /// <param name="responseUrlHost">The host a <c>response_url</c> must be on; <see cref="ResponseUrlHost"/> by default, a loopback host in a test.</param>
    public SlackBotClient(string botToken, string? apiBase = null, HttpMessageHandler? handler = null, string? responseUrlHost = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(botToken);
        _token = botToken.Trim();
        var root = string.IsNullOrWhiteSpace(apiBase) ? DefaultApiBase : apiBase.Trim();
        if (!root.EndsWith('/'))
        {
            root += "/";
        }

        _postUri = new Uri(new Uri(root, UriKind.Absolute), "chat.postMessage");
        _updateUri = new Uri(new Uri(root, UriKind.Absolute), "chat.update");
        _responseUrlHost = string.IsNullOrWhiteSpace(responseUrlHost) ? ResponseUrlHost : responseUrlHost.Trim();
        _http = handler is null ? new HttpClient() : new HttpClient(handler, disposeHandler: false);
        _http.Timeout = PostTimeout;
    }

    /// <inheritdoc />
    public async Task<SlackPostResult> PostAsync(string channel, string text, IReadOnlyList<object>? blocks, string? threadTs, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(channel);
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, _postUri);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _token);
            request.Content = new StringContent(BuildBody(channel, text, blocks, threadTs), Encoding.UTF8, "application/json");
            using var response = await _http.SendAsync(request, cancellationToken);
            var body = await response.Content.ReadAsStringAsync(cancellationToken);
            return ParseResponse((int)response.StatusCode, body, _token, RetryAfterOf(response));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return new SlackPostResult(false, null, null, Redact(ex.GetBaseException().Message, _token));
        }
    }

    /// <inheritdoc />
    public async Task<SlackPostResult> UpdateAsync(string channel, string ts, string text, IReadOnlyList<object>? blocks, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(channel);
        ArgumentException.ThrowIfNullOrWhiteSpace(ts);
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, _updateUri);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _token);
            request.Content = new StringContent(BuildUpdateBody(channel, ts, text, blocks), Encoding.UTF8, "application/json");
            using var response = await _http.SendAsync(request, cancellationToken);
            var body = await response.Content.ReadAsStringAsync(cancellationToken);
            return ParseResponse((int)response.StatusCode, body, _token, RetryAfterOf(response));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return new SlackPostResult(false, null, null, Redact(ex.GetBaseException().Message, _token));
        }
    }

    /// <inheritdoc />
    public async Task<SlackPostResult> RespondAsync(Uri responseUrl, string text, IReadOnlyList<object>? blocks, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(responseUrl);
        if (!IsAllowedResponseUrl(responseUrl, _responseUrlHost))
        {
            return new SlackPostResult(false, null, null, "response_url is not on " + _responseUrlHost);
        }

        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, responseUrl);
            request.Content = new StringContent(BuildResponseBody(text, blocks), Encoding.UTF8, "application/json");
            using var response = await _http.SendAsync(request, cancellationToken);
            var body = await response.Content.ReadAsStringAsync(cancellationToken);
            /* A response_url answers "ok" as plain text, not JSON: a 200 is a delivered answer. */
            if ((int)response.StatusCode == 200)
            {
                return new SlackPostResult(true, null, null, null);
            }

            return ParseResponse((int)response.StatusCode, body, _token, RetryAfterOf(response));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return new SlackPostResult(false, null, null, Redact(ex.GetBaseException().Message, _token));
        }
    }

    /// <summary>PURE: whether <paramref name="responseUrl"/> may be posted to: HTTPS on <paramref name="host"/> (or any scheme on a loopback host, for a test).</summary>
    internal static bool IsAllowedResponseUrl(Uri responseUrl, string host)
    {
        if (responseUrl is null || !responseUrl.IsAbsoluteUri)
        {
            return false;
        }

        if (responseUrl.IsLoopback && string.Equals(responseUrl.Host, host, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return string.Equals(responseUrl.Scheme, "https", StringComparison.OrdinalIgnoreCase)
            && string.Equals(responseUrl.Host, host, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>PURE: the <c>chat.update</c> body: the channel, the ts and the new text and blocks.</summary>
    internal static string BuildUpdateBody(string channel, string ts, string text, IReadOnlyList<object>? blocks)
    {
        var body = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["channel"] = channel,
            ["ts"] = ts,
            ["text"] = text ?? "",
            ["blocks"] = blocks is { Count: > 0 } ? blocks : Array.Empty<object>(),
        };
        return JsonSerializer.Serialize(body, s_jsonOptions);
    }

    /// <summary>PURE: the <c>response_url</c> body: ephemeral, the card kept (<c>replace_original</c> false), the text and blocks.</summary>
    internal static string BuildResponseBody(string text, IReadOnlyList<object>? blocks)
    {
        var body = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["response_type"] = "ephemeral",
            ["replace_original"] = false,
            ["text"] = text ?? "",
        };
        if (blocks is { Count: > 0 })
        {
            body["blocks"] = blocks;
        }

        return JsonSerializer.Serialize(body, s_jsonOptions);
    }

    /// <summary>PURE: the <c>chat.postMessage</c> body. <c>thread_ts</c> only when given, so a parent post opens a thread and a
    /// reply joins one. <c>unfurl_links</c> and <c>unfurl_media</c> are always false.</summary>
    internal static string BuildBody(string channel, string text, IReadOnlyList<object>? blocks, string? threadTs)
    {
        var body = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["channel"] = channel,
            ["text"] = text ?? "",
            ["unfurl_links"] = false,
            ["unfurl_media"] = false,
        };
        if (blocks is { Count: > 0 })
        {
            body["blocks"] = blocks;
        }

        if (!string.IsNullOrWhiteSpace(threadTs))
        {
            body["thread_ts"] = threadTs.Trim();
        }

        return JsonSerializer.Serialize(body, s_jsonOptions);
    }

    /// <summary>Slack's <c>Retry-After</c> on a 429: a delay, or the gap to a date; null when the header is absent.</summary>
    private static TimeSpan? RetryAfterOf(HttpResponseMessage response)
    {
        var header = response.Headers.RetryAfter;
        if (header is null)
        {
            return null;
        }

        if (header.Delta is TimeSpan delta)
        {
            return delta < TimeSpan.Zero ? TimeSpan.Zero : delta;
        }

        if (header.Date is DateTimeOffset date)
        {
            var gap = date - DateTimeOffset.UtcNow;
            return gap < TimeSpan.Zero ? TimeSpan.Zero : gap;
        }

        return null;
    }

    /// <summary>PURE: Slack's answer. A 200 with <c>ok:true</c> delivers and carries <c>ts</c> and <c>channel</c>; <c>ok:false</c> names
    /// the error; a 429 is <see cref="SlackPostResult.RateLimited"/> with <paramref name="retryAfter"/>; any other status is an HTTP
    /// error. A body that is not JSON is reported by its status alone, never quoted, and every error text is redacted of
    /// <paramref name="token"/>.</summary>
    internal static SlackPostResult ParseResponse(int statusCode, string body, string token, TimeSpan? retryAfter = null)
    {
        if (statusCode == 429)
        {
            return new SlackPostResult(false, null, null, "HTTP 429", RateLimited: true, RetryAfter: retryAfter);
        }

        if (statusCode != 200)
        {
            return new SlackPostResult(false, null, null, "HTTP " + statusCode.ToString(CultureInfo.InvariantCulture));
        }

        try
        {
            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;
            if (root.ValueKind == JsonValueKind.Object && root.TryGetProperty("ok", out var ok) && ok.ValueKind == JsonValueKind.True)
            {
                var ts = root.TryGetProperty("ts", out var tsNode) && tsNode.ValueKind == JsonValueKind.String ? tsNode.GetString() : null;
                var channel = root.TryGetProperty("channel", out var chNode) && chNode.ValueKind == JsonValueKind.String ? chNode.GetString() : null;
                return new SlackPostResult(true, ts, channel, null);
            }

            var error = root.ValueKind == JsonValueKind.Object && root.TryGetProperty("error", out var errorNode) && errorNode.ValueKind == JsonValueKind.String
                ? errorNode.GetString()
                : null;
            return new SlackPostResult(false, null, null, Redact(string.IsNullOrWhiteSpace(error) ? "not ok" : error, token));
        }
        catch (JsonException)
        {
            return new SlackPostResult(false, null, null, "HTTP 200 with a body that is not JSON");
        }
    }

    /// <summary>PURE: <paramref name="text"/> with every copy of <paramref name="token"/> replaced by <see cref="RedactedToken"/>.</summary>
    internal static string Redact(string? text, string token)
    {
        if (string.IsNullOrEmpty(text) || string.IsNullOrEmpty(token))
        {
            return text ?? "";
        }

        return text.Replace(token, RedactedToken, StringComparison.Ordinal);
    }

    /// <summary>Names the endpoint, never the token.</summary>
    public override string ToString() => "SlackBotClient(" + _postUri + ")";
}
