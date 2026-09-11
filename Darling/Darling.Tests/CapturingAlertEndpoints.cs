/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;

namespace Darling.Tests;

/* The loopback channel endpoints and alert-history doubles the delivery suites drive. Shared rather than
   nested per test class because the SMTP one in particular is the only interception point email has short of
   the protocol (see its own remarks), and a second copy of a protocol responder is a second thing to get
   subtly wrong. Driven by AlertDeliveryChannelTests (#3169/#3297/#3303) and IncidentDeliveryFilterTests
   (#3313). A block comment rather than an XML doc block: a file-level summary with no member of its own
   stacks against the first type's, which DocCommentHygieneTests.NoMemberCarriesTwoStackedSummaryBlocks
   forbids and which CI caught here. */
/// <summary>
/// A loopback endpoint that records the bodies posted to it. <see cref="System.Net.Sockets.TcpListener"/>
/// rather than <c>HttpListener</c> on purpose: HttpListener wants a URL ACL on Windows, and the four
/// lines of HTTP a webhook POST needs are cheaper than that dependency. The same choice
/// <c>NpgsqlRootCertificateValidationTests</c> and <c>DarlingStoreUpgradeTests</c> already make.
/// </summary>
internal sealed class CapturingWebhookEndpoint : IDisposable
{
    private readonly TcpListener _listener;
    private readonly List<string> _bodies = new();
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _accepting;

    public CapturingWebhookEndpoint()
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        Url = $"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}/hook";
        _accepting = Task.Run(AcceptLoopAsync);
    }

    public string Url { get; }

    /// <summary>
    /// Safe to read without synchronization once the send has been awaited: each body is appended before
    /// its response is written, and the sender awaits every response in turn.
    /// </summary>
    public IReadOnlyList<string> Bodies => _bodies;

    private async Task AcceptLoopAsync()
    {
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                using var stream = client.GetStream();
                _bodies.Add(await ReadRequestBodyAsync(stream));

                var response = Encoding.ASCII.GetBytes(
                    "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                await stream.WriteAsync(response, _stop.Token);
                await stream.FlushAsync(_stop.Token);
            }
        }
        catch (OperationCanceledException) { /* Dispose */ }
        catch (System.Net.Sockets.SocketException) { /* listener stopped */ }
        catch (ObjectDisposedException) { /* listener stopped */ }
    }

    /// <summary>Reads headers to the blank line, then exactly Content-Length bytes of body.</summary>
    private static async Task<string> ReadRequestBodyAsync(NetworkStream stream)
    {
        var buffer = new byte[16 * 1024];
        var received = new List<byte>(capacity: 16 * 1024);
        int headerEnd;

        while ((headerEnd = IndexOfHeaderEnd(received)) < 0)
        {
            var read = await stream.ReadAsync(buffer);
            if (read == 0)
            {
                return Encoding.UTF8.GetString(received.ToArray());
            }

            received.AddRange(new ArraySegment<byte>(buffer, 0, read));
        }

        var headers = Encoding.ASCII.GetString(received.ToArray(), 0, headerEnd);
        var contentLength = ParseContentLength(headers);
        var bodyStart = headerEnd + 4;

        while (received.Count - bodyStart < contentLength)
        {
            var read = await stream.ReadAsync(buffer);
            if (read == 0)
            {
                break;
            }

            received.AddRange(new ArraySegment<byte>(buffer, 0, read));
        }

        var body = received.ToArray();
        var available = Math.Min(contentLength, body.Length - bodyStart);
        return Encoding.UTF8.GetString(body, bodyStart, Math.Max(0, available));
    }

    private static int IndexOfHeaderEnd(List<byte> bytes)
    {
        for (int i = 0; i + 3 < bytes.Count; i++)
        {
            if (bytes[i] == (byte)'\r' && bytes[i + 1] == (byte)'\n' &&
                bytes[i + 2] == (byte)'\r' && bytes[i + 3] == (byte)'\n')
            {
                return i;
            }
        }

        return -1;
    }

    private static int ParseContentLength(string headers)
    {
        foreach (var line in headers.Split("\r\n", StringSplitOptions.RemoveEmptyEntries))
        {
            if (line.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase) &&
                int.TryParse(line.AsSpan("Content-Length:".Length).Trim(), out var length))
            {
                return length;
            }
        }

        return 0;
    }

    public void Dispose()
    {
        _stop.Cancel();
        _listener.Stop();
        try
        {
            _accepting.Wait(TimeSpan.FromSeconds(5));
        }
        catch (AggregateException) { /* the cancellation above */ }

        _stop.Dispose();
    }
}

/// <summary>
/// A loopback SMTP sink that records the DATA of each message posted to it, quoted-printable soft line
/// breaks removed so an assertion on a phrase cannot fail because MIME wrapped it at column 76.
/// <para>Exists because email is the only channel with no interception point short of the protocol:
/// there is no builder-returns-the-payload seam between <c>EmailSendCore</c> and the wire. Without it a
/// mutation that drops the detail on the way to the email template alone passes every other pin — which
/// is exactly the defect #3296 reported, so leaving that one hop unpinned was not an option.</para>
/// </summary>
internal sealed class CapturingSmtpEndpoint : IDisposable
{
    private readonly TcpListener _listener;
    private readonly List<string> _messages = new();
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _accepting;

    public CapturingSmtpEndpoint()
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _accepting = Task.Run(AcceptLoopAsync);
    }

    public int Port { get; }

    /// <summary>Safe to read once the send has been awaited; see the webhook endpoint's note.</summary>
    public IReadOnlyList<string> Messages => _messages;

    private async Task AcceptLoopAsync()
    {
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                using var stream = client.GetStream();
                using var reader = new StreamReader(stream, Encoding.UTF8, false, 8192, leaveOpen: true);
                using var writer = new StreamWriter(stream, new UTF8Encoding(false), 8192, leaveOpen: true) { AutoFlush = true, NewLine = "\r\n" };

                await writer.WriteLineAsync("220 localhost ESMTP capture");

                var data = new StringBuilder();
                var inData = false;
                string? line;
                while ((line = await reader.ReadLineAsync(_stop.Token)) is not null)
                {
                    if (inData)
                    {
                        if (line == ".")
                        {
                            _messages.Add(DecodeMimeText(data.ToString()));
                            data.Clear();
                            inData = false;
                            await writer.WriteLineAsync("250 OK queued");
                            continue;
                        }

                        /* Transparency: a body line starting with '.' arrives doubled. CRLF explicitly,
                           not AppendLine: MIME line endings are CRLF by the spec the parsing below
                           depends on, and Environment.NewLine is LF on the platform this is written on. */
                        data.Append(line.StartsWith("..", StringComparison.Ordinal) ? line.Substring(1) : line).Append("\r\n");
                        continue;
                    }

                    if (line.StartsWith("EHLO", StringComparison.OrdinalIgnoreCase) ||
                        line.StartsWith("HELO", StringComparison.OrdinalIgnoreCase))
                    {
                        /* No extensions advertised, so the client never tries STARTTLS or AUTH. */
                        await writer.WriteLineAsync("250 localhost");
                    }
                    else if (line.StartsWith("DATA", StringComparison.OrdinalIgnoreCase))
                    {
                        inData = true;
                        await writer.WriteLineAsync("354 End data with <CRLF>.<CRLF>");
                    }
                    else if (line.StartsWith("QUIT", StringComparison.OrdinalIgnoreCase))
                    {
                        await writer.WriteLineAsync("221 Bye");
                        break;
                    }
                    else
                    {
                        await writer.WriteLineAsync("250 OK");
                    }
                }
            }
        }
        catch (OperationCanceledException) { /* Dispose */ }
        catch (IOException) { /* client went away */ }
        catch (SocketException) { /* listener stopped */ }
        catch (ObjectDisposedException) { /* listener stopped */ }
    }

    /// <summary>
    /// Returns the DECODED text of every MIME part, concatenated. Both transfer encodings this message
    /// actually uses are handled — .NET picks base64 for the utf-8 plain-text alternate view and
    /// quoted-printable for the us-ascii HTML one — because an assertion against the raw DATA would be
    /// asserting against an encoding choice rather than against the delivered words.
    /// </summary>
    private static string DecodeMimeText(string raw)
    {
        var boundary = System.Text.RegularExpressions.Regex.Match(raw, @"boundary=(\S+)");
        if (!boundary.Success)
        {
            return DecodeQuotedPrintable(raw);
        }

        var parts = raw.Split("--" + boundary.Groups[1].Value, StringSplitOptions.None);
        var decoded = new StringBuilder();

        foreach (var part in parts.Skip(1))
        {
            var split = part.IndexOf("\r\n\r\n", StringComparison.Ordinal);
            if (split < 0)
            {
                continue;
            }

            var headers = part.Substring(0, split);
            var body = part.Substring(split + 4);

            decoded.Append(headers.Contains("base64", StringComparison.OrdinalIgnoreCase)
                ? Encoding.UTF8.GetString(Convert.FromBase64String(
                    new string(body.Where(c => !char.IsWhiteSpace(c)).ToArray())))
                : DecodeQuotedPrintable(body)).Append("\r\n");
        }

        return decoded.ToString();
    }

    /// <summary>Drops soft line breaks ("=" at end of line) and decodes "=XX" octets.</summary>
    private static string DecodeQuotedPrintable(string raw)
    {
        var unfolded = raw.Replace("=\r\n", "", StringComparison.Ordinal).Replace("=\n", "", StringComparison.Ordinal);
        var sb = new StringBuilder(unfolded.Length);

        for (int i = 0; i < unfolded.Length; i++)
        {
            if (unfolded[i] == '=' && i + 2 < unfolded.Length &&
                Uri.IsHexDigit(unfolded[i + 1]) && Uri.IsHexDigit(unfolded[i + 2]))
            {
                sb.Append((char)Convert.ToInt32(unfolded.Substring(i + 1, 2), 16));
                i += 2;
                continue;
            }

            sb.Append(unfolded[i]);
        }

        return sb.ToString();
    }

    public void Dispose()
    {
        _stop.Cancel();
        _listener.Stop();
        try
        {
            _accepting.Wait(TimeSpan.FromSeconds(5));
        }
        catch (AggregateException) { /* the cancellation above */ }

        _stop.Dispose();
    }
}

/// <summary>
/// <see cref="DiscardingHistoryStore"/> with the records kept, so a test can read what the row would
/// have held. Safe to read after the send has been awaited: <c>SendFindingAlertAsync</c> awaits its
/// own <c>RecordAlertAsync</c>.
/// </summary>
internal sealed class CapturingHistoryStore : IAlertHistoryStore
{
    public List<AlertHistoryRecord> Records { get; } = new();

    public Task RecordAlertAsync(AlertHistoryRecord record)
    {
        Records.Add(record);
        return Task.CompletedTask;
    }

    public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
        Task.FromResult<DateTime?>(null);

    public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
        Task.FromResult<DateTime?>(null);

    public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) =>
        Task.FromResult<DateTime?>(null);
}

internal sealed class DiscardingHistoryStore : IAlertHistoryStore
{
    public Task RecordAlertAsync(AlertHistoryRecord record) => Task.CompletedTask;

    public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
        Task.FromResult<DateTime?>(null);

    public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
        Task.FromResult<DateTime?>(null);

    public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) =>
        Task.FromResult<DateTime?>(null);
}
