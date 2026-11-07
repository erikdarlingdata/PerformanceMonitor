/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Service.Chat;

/// <summary>
/// #7657: the product's <see cref="ISlackSocketModeTransport"/>: <c>auth.test</c> and <c>apps.connections.open</c> over the Slack Web
/// API with the token as a bearer header, and a <see cref="ClientWebSocket"/> for the Socket Mode connection, straight to Slack with no
/// proxy. A token goes only into the Authorization header of its one call; every text that could echo it (a response body, an
/// exception message) passes through <see cref="SlackBotClient.Redact"/> first. The API root is injectable, so a test drives the two
/// Web API calls with a loopback listener.
/// </summary>
public sealed class SlackSocketModeWebApi : ISlackSocketModeTransport
{
    /// <summary>How long one Web API call or the WebSocket handshake may take.</summary>
    public static readonly TimeSpan CallTimeout = TimeSpan.FromSeconds(15);

    /// <summary>The largest text frame the socket reads. A Socket Mode envelope is a few kilobytes; a frame past this is a
    /// broken peer, and the socket is closed rather than buffered without bound.</summary>
    public const int MaxFrameBytes = 1024 * 1024;

    private readonly Uri _authTestUri;
    private readonly Uri _openUri;
    private readonly HttpClient _http;

    /// <param name="apiBase">The Web API root, <see cref="SlackBotClient.DefaultApiBase"/> by default.</param>
    /// <param name="handler">An HTTP handler for a test; the product uses the default.</param>
    public SlackSocketModeWebApi(string? apiBase = null, HttpMessageHandler? handler = null)
    {
        var root = string.IsNullOrWhiteSpace(apiBase) ? SlackBotClient.DefaultApiBase : apiBase.Trim();
        if (!root.EndsWith('/'))
        {
            root += "/";
        }

        var rootUri = new Uri(root, UriKind.Absolute);
        _authTestUri = new Uri(rootUri, "auth.test");
        _openUri = new Uri(rootUri, "apps.connections.open");
        _http = handler is null ? new HttpClient() : new HttpClient(handler, disposeHandler: false);
        _http.Timeout = CallTimeout;
    }

    /// <inheritdoc />
    public async Task<SlackAuthTest> AuthTestAsync(string botToken, CancellationToken cancellationToken)
    {
        var token = (botToken ?? "").Trim();
        if (token.Length == 0)
        {
            return new SlackAuthTest(false, null, null, null, "no bot token");
        }

        var (status, body, error) = await PostAsync(_authTestUri, token, cancellationToken);
        if (error is not null)
        {
            return new SlackAuthTest(false, null, null, null, error);
        }

        if (status != 200)
        {
            return new SlackAuthTest(false, null, null, null, "HTTP " + status.ToString(CultureInfo.InvariantCulture));
        }

        try
        {
            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;
            if (root.ValueKind == JsonValueKind.Object && root.TryGetProperty("ok", out var ok) && ok.ValueKind == JsonValueKind.True)
            {
                return new SlackAuthTest(true, StringOf(root, "team_id"), StringOf(root, "user_id"), StringOf(root, "bot_id"), null);
            }

            return new SlackAuthTest(false, null, null, null, SlackBotClient.Redact(ErrorWord(root), token));
        }
        catch (JsonException)
        {
            return new SlackAuthTest(false, null, null, null, "HTTP 200 with a body that is not JSON");
        }
    }

    /// <inheritdoc />
    public async Task<SlackConnectionOpen> OpenConnectionAsync(string appToken, CancellationToken cancellationToken)
    {
        var token = (appToken ?? "").Trim();
        if (token.Length == 0)
        {
            return new SlackConnectionOpen(false, null, "no app token");
        }

        var (status, body, error) = await PostAsync(_openUri, token, cancellationToken);
        if (error is not null)
        {
            return new SlackConnectionOpen(false, null, error);
        }

        if (status != 200)
        {
            return new SlackConnectionOpen(false, null, "HTTP " + status.ToString(CultureInfo.InvariantCulture));
        }

        try
        {
            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;
            if (root.ValueKind == JsonValueKind.Object && root.TryGetProperty("ok", out var ok) && ok.ValueKind == JsonValueKind.True)
            {
                var url = StringOf(root, "url");
                if (Uri.TryCreate(url, UriKind.Absolute, out var uri)
                    && (string.Equals(uri.Scheme, "wss", StringComparison.OrdinalIgnoreCase) || string.Equals(uri.Scheme, "ws", StringComparison.OrdinalIgnoreCase)))
                {
                    return new SlackConnectionOpen(true, uri, null);
                }

                return new SlackConnectionOpen(false, null, "ok without a WebSocket url");
            }

            return new SlackConnectionOpen(false, null, SlackBotClient.Redact(ErrorWord(root), token));
        }
        catch (JsonException)
        {
            return new SlackConnectionOpen(false, null, "HTTP 200 with a body that is not JSON");
        }
    }

    /// <inheritdoc />
    public async Task<ISlackSocket> ConnectAsync(Uri url, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(url);
        var socket = new ClientWebSocket();
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(CallTimeout);
            await socket.ConnectAsync(url, timeout.Token);
            return new ClientSocket(socket);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    private async Task<(int Status, string Body, string? Error)> PostAsync(Uri uri, string token, CancellationToken cancellationToken)
    {
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, uri);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            request.Content = new StringContent("", Encoding.UTF8, "application/x-www-form-urlencoded");
            using var response = await _http.SendAsync(request, cancellationToken);
            var body = await response.Content.ReadAsStringAsync(cancellationToken);
            return ((int)response.StatusCode, body, null);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return (0, "", SlackBotClient.Redact(ex.GetBaseException().Message, token));
        }
    }

    private static string? StringOf(JsonElement root, string name) =>
        root.TryGetProperty(name, out var node) && node.ValueKind == JsonValueKind.String ? node.GetString() : null;

    private static string ErrorWord(JsonElement root)
    {
        var error = root.ValueKind == JsonValueKind.Object ? StringOf(root, "error") : null;
        return string.IsNullOrWhiteSpace(error) ? "not ok" : error;
    }

    /// <summary>Names the endpoints, never a token.</summary>
    public override string ToString() => "SlackSocketModeWebApi(" + _openUri + ")";

    /// <summary>A <see cref="ClientWebSocket"/> as the listener's socket: whole text frames in, acknowledgements out.</summary>
    private sealed class ClientSocket(ClientWebSocket socket) : ISlackSocket
    {
        private readonly byte[] _buffer = new byte[16 * 1024];

        public async Task<string?> ReceiveAsync(CancellationToken cancellationToken)
        {
            using var frame = new MemoryStream();
            while (true)
            {
                ValueWebSocketReceiveResult result;
                try
                {
                    result = await socket.ReceiveAsync(new Memory<byte>(_buffer), cancellationToken);
                }
                catch (WebSocketException)
                {
                    return null;
                }

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    return null;
                }

                if (result.MessageType == WebSocketMessageType.Binary)
                {
                    /* Socket Mode is text only. A binary frame is skipped whole. */
                    if (result.EndOfMessage)
                    {
                        frame.SetLength(0);
                    }

                    continue;
                }

                frame.Write(_buffer, 0, result.Count);
                if (frame.Length > MaxFrameBytes)
                {
                    throw new InvalidDataException("a Socket Mode frame exceeded " + MaxFrameBytes.ToString(CultureInfo.InvariantCulture) + " bytes");
                }

                if (result.EndOfMessage)
                {
                    return Encoding.UTF8.GetString(frame.GetBuffer(), 0, (int)frame.Length);
                }
            }
        }

        public async Task SendAsync(string text, CancellationToken cancellationToken)
        {
            var bytes = Encoding.UTF8.GetBytes(text ?? "");
            await socket.SendAsync(new ReadOnlyMemory<byte>(bytes), WebSocketMessageType.Text, endOfMessage: true, cancellationToken);
        }

        public async Task CloseAsync(CancellationToken cancellationToken)
        {
            if (socket.State is not (WebSocketState.Open or WebSocketState.CloseReceived))
            {
                return;
            }

            try
            {
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeout.CancelAfter(TimeSpan.FromSeconds(5));
                await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "reconnect", timeout.Token);
            }
            catch (Exception ex) when (ex is WebSocketException or OperationCanceledException or ObjectDisposedException)
            {
                /* The peer is gone or the close took too long: the socket is disposed either way. */
            }
        }

        public void Dispose() => socket.Dispose();
    }
}
