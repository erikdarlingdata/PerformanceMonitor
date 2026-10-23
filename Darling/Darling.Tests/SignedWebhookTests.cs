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
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5998: the Standard Webhooks signature itself, and the receiver check the docs' starter sample describes
/// (docs/signed-webhooks.md). <see cref="SampleReceiver"/> is that check written in C#: it recomputes the HMAC over the
/// bytes it received, enforces the 5-minute timestamp window and dedupes on the id.
/// </summary>
public sealed class SignedWebhookSignatureTests
{
    [Fact]
    public void TheSignature_MatchesTheStandardWebhooksTestVector()
    {
        /* The example Standard Webhooks publishes: its secret, id, timestamp and body, and the signature it lists. */
        var signature = WebhookSigning.Sign(
            new[] { "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw" }, "msg_p5jXN8AQM9LWM0D4loKWxJek", 1614265330,
            Encoding.UTF8.GetBytes("{\"test\": 2432232314}"));

        Assert.Equal("v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=", signature);
    }

    [Fact]
    public void TheSignature_IsAnHmacSha256OverIdTimestampAndBody_ComputedHereWithoutTheSigner()
    {
        var keyBytes = Enumerable.Range(1, 32).Select(i => (byte)i).ToArray();
        var key = "whsec_" + Convert.ToBase64String(keyBytes);
        var body = Encoding.UTF8.GetBytes("{\"metric\":\"CPU\",\"server\":\"sql-01\",\"note\":\"café\"}");

        var expected = "v1," + Convert.ToBase64String(
            HMACSHA256.HashData(keyBytes, Encoding.UTF8.GetBytes("msg_fixed.1700000000.").Concat(body).ToArray()));

        Assert.Equal(expected, WebhookSigning.Sign(new[] { key }, "msg_fixed", 1700000000, body));
    }

    [Fact]
    public void TheSampleReceiver_AcceptsASignedBody_AndRejectsOneChangedByte()
    {
        var key = WebhookSigning.NewKey();
        var now = new DateTime(2026, 10, 16, 12, 0, 0, DateTimeKind.Utc);
        var body = Encoding.UTF8.GetBytes("{\"metric\":\"Blocking Detected\"}");
        var ts = WebhookSigning.UnixSeconds(now);
        var signature = WebhookSigning.Sign(new[] { key }, "msg_1", ts, body);

        Assert.Equal(SampleReceiver.Result.Accepted, new SampleReceiver(key).Verify("msg_1", ts.ToString(CultureInfo.InvariantCulture), signature, body, now));

        var changed = (byte[])body.Clone();
        changed[5] ^= 0x01;
        Assert.Equal(SampleReceiver.Result.BadSignature, new SampleReceiver(key).Verify("msg_1", ts.ToString(CultureInfo.InvariantCulture), signature, changed, now));
    }

    [Fact]
    public void TheSampleReceiver_RejectsASixMinuteOldTimestamp_AndAReplayedId()
    {
        var key = WebhookSigning.NewKey();
        var now = new DateTime(2026, 10, 16, 12, 0, 0, DateTimeKind.Utc);
        var body = Encoding.UTF8.GetBytes("{}");
        var old = WebhookSigning.UnixSeconds(now.AddMinutes(-6));
        var receiver = new SampleReceiver(key);

        Assert.Equal(SampleReceiver.Result.TooOld,
            receiver.Verify("msg_old", old.ToString(CultureInfo.InvariantCulture), WebhookSigning.Sign(new[] { key }, "msg_old", old, body), body, now));

        var fresh = WebhookSigning.UnixSeconds(now);
        var signature = WebhookSigning.Sign(new[] { key }, "msg_2", fresh, body);
        Assert.Equal(SampleReceiver.Result.Accepted, receiver.Verify("msg_2", fresh.ToString(CultureInfo.InvariantCulture), signature, body, now));
        Assert.Equal(SampleReceiver.Result.Replayed, receiver.Verify("msg_2", fresh.ToString(CultureInfo.InvariantCulture), signature, body, now));
    }

    [Fact]
    public void TheSampleReceiver_StillRejectsAReplay_FromASenderWhoseClockRunsFourMinutesAhead()
    {
        /* The id must outlive its own timestamp window, not five minutes from arrival: at arrival + 5:02 the post
           (stamped arrival + 4:00) is only 1:02 from this clock and still passes the window check. */
        var key = WebhookSigning.NewKey();
        var arrival = new DateTime(2026, 10, 16, 12, 0, 0, DateTimeKind.Utc);
        var body = Encoding.UTF8.GetBytes("{}");
        var ts = WebhookSigning.UnixSeconds(arrival.AddMinutes(4));
        var tsText = ts.ToString(CultureInfo.InvariantCulture);
        var signature = WebhookSigning.Sign(new[] { key }, "msg_skew", ts, body);
        var receiver = new SampleReceiver(key);

        Assert.Equal(SampleReceiver.Result.Accepted, receiver.Verify("msg_skew", tsText, signature, body, arrival));
        Assert.Equal(SampleReceiver.Result.Replayed, receiver.Verify("msg_skew", tsText, signature, body, arrival.AddMinutes(5).AddSeconds(2)));
        Assert.Equal(SampleReceiver.Result.Replayed, receiver.Verify("msg_skew", tsText, signature, body, arrival.AddMinutes(9)));
        Assert.Equal(SampleReceiver.Result.TooOld, receiver.Verify("msg_skew", tsText, signature, body, arrival.AddMinutes(9).AddSeconds(1)));
    }

    [Fact]
    public void TheSampleReceiver_ReturnsFalseOnMalformedHeaders_InsteadOfThrowing()
    {
        var key = WebhookSigning.NewKey();
        var now = new DateTime(2026, 10, 16, 12, 0, 0, DateTimeKind.Utc);
        var body = Encoding.UTF8.GetBytes("{}");
        var ts = WebhookSigning.UnixSeconds(now);
        var tsText = ts.ToString(CultureInfo.InvariantCulture);
        var good = WebhookSigning.Sign(new[] { key }, "msg_m", ts, body);
        var receiver = new SampleReceiver(key);

        Assert.Equal(SampleReceiver.Result.Malformed, receiver.Verify("msg_m", "not-a-number", good, body, now));
        Assert.Equal(SampleReceiver.Result.Malformed, receiver.Verify("msg_m", "99999999999999999999", good, body, now));
        Assert.Equal(SampleReceiver.Result.Malformed, receiver.Verify("msg_m", "-5", good, body, now));
        Assert.Equal(SampleReceiver.Result.Malformed, receiver.Verify("msg_m", tsText, null!, body, now));
        Assert.Equal(SampleReceiver.Result.Malformed, receiver.Verify("", tsText, good, body, now));
        Assert.Equal(SampleReceiver.Result.Malformed, receiver.Verify(null!, tsText, good, body, now));
        Assert.Equal(SampleReceiver.Result.BadSignature, receiver.Verify("msg_m", tsText, "v1,!!!not base64!!!", body, now));
        Assert.Equal(SampleReceiver.Result.BadSignature, receiver.Verify("msg_m", tsText, "", body, now));
        Assert.Equal(SampleReceiver.Result.Accepted, receiver.Verify("msg_m", tsText, "v1,%%% " + good, body, now));
    }

    [Fact]
    public void DuringTheOverlap_BothKeysSign_AndAfterSevenDays_OnlyTheNewKey()
    {
        /* One clock value; every instant below is derived from it (#3676). */
        var rotatedAt = new DateTime(2026, 10, 16, 12, 0, 0, DateTimeKind.Utc);
        var oldKey = WebhookSigning.NewKey();
        var newKey = WebhookSigning.NewKey();
        var keys = new WebhookSigningKeys(newKey, oldKey, rotatedAt + WebhookSigning.RotationOverlap);
        var body = Encoding.UTF8.GetBytes("{}");

        var during = rotatedAt.AddDays(6);
        var duringTs = WebhookSigning.UnixSeconds(during);
        var both = WebhookSigning.Sign(keys.LiveKeys(during), "msg_r", duringTs, body);
        Assert.Equal(2, both.Split(' ').Length);
        Assert.Equal(SampleReceiver.Result.Accepted, new SampleReceiver(oldKey).Verify("msg_r", duringTs.ToString(CultureInfo.InvariantCulture), both, body, during));
        Assert.Equal(SampleReceiver.Result.Accepted, new SampleReceiver(newKey).Verify("msg_r", duringTs.ToString(CultureInfo.InvariantCulture), both, body, during));

        var after = rotatedAt.AddDays(WebhookSigning.RotationOverlapDays);
        var afterTs = WebhookSigning.UnixSeconds(after);
        var one = WebhookSigning.Sign(keys.LiveKeys(after), "msg_s", afterTs, body);
        Assert.Single(one.Split(' '));
        Assert.Equal(SampleReceiver.Result.BadSignature, new SampleReceiver(oldKey).Verify("msg_s", afterTs.ToString(CultureInfo.InvariantCulture), one, body, after));
        Assert.Equal(SampleReceiver.Result.Accepted, new SampleReceiver(newKey).Verify("msg_s", afterTs.ToString(CultureInfo.InvariantCulture), one, body, after));
    }

    [Fact]
    public void TheKeysToString_PrintsNoKey()
    {
        var key = WebhookSigning.NewKey();
        var previous = WebhookSigning.NewKey();
        var keys = new WebhookSigningKeys(key, previous, DateTime.UtcNow);
        var route = new NotificationRoute(3, "CPU", "", "", "https://r.example.test/x", "", "", true) { GenericSigning = keys };

        foreach (var text in new[] { keys.ToString(), route.ToString() })
        {
            Assert.DoesNotContain(key[WebhookSigning.KeyPrefix.Length..], text, StringComparison.Ordinal);
            Assert.DoesNotContain(previous[WebhookSigning.KeyPrefix.Length..], text, StringComparison.Ordinal);
        }
    }

    /// <summary>The receiver check of docs/signed-webhooks.md, in C#. Keep it identical to the sample there.</summary>
    internal sealed class SampleReceiver
    {
        public enum Result { Accepted, BadSignature, TooOld, Replayed, Malformed }

        private static readonly TimeSpan Tolerance = TimeSpan.FromMinutes(5);
        private readonly byte[] _key;
        private readonly object _gate = new();
        private readonly Dictionary<string, DateTime> _seen = new(StringComparer.Ordinal);

        public SampleReceiver(string secret)
        {
            _key = Convert.FromBase64String(secret.StartsWith("whsec_", StringComparison.Ordinal) ? secret[6..] : secret);
        }

        public Result Verify(string id, string timestamp, string signatureHeader, byte[] body, DateTime nowUtc)
        {
            if (string.IsNullOrEmpty(id) || signatureHeader is null) return Result.Malformed;

            if (!long.TryParse(timestamp, NumberStyles.None, CultureInfo.InvariantCulture, out var seconds)
                || seconds > 253402300799)
            {
                return Result.Malformed;
            }

            var sent = DateTimeOffset.FromUnixTimeSeconds(seconds).UtcDateTime;
            if ((nowUtc - sent).Duration() > Tolerance)
            {
                return Result.TooOld;
            }

            var expected = HMACSHA256.HashData(_key, Encoding.UTF8.GetBytes(id + "." + timestamp + ".").Concat(body).ToArray());
            var match = false;
            foreach (var part in signatureHeader.Split(' ', StringSplitOptions.RemoveEmptyEntries))
            {
                var signature = new byte[64];
                if (part.StartsWith("v1,", StringComparison.Ordinal)
                    && Convert.TryFromBase64String(part[3..], signature, out var written)
                    && CryptographicOperations.FixedTimeEquals(signature.AsSpan(0, written), expected))
                {
                    match = true;
                }
            }

            if (!match)
            {
                return Result.BadSignature;
            }

            lock (_gate)
            {
                foreach (var old in _seen.Where(kv => nowUtc - kv.Value > Tolerance).Select(kv => kv.Key).ToList())
                {
                    _seen.Remove(old);
                }

                return _seen.TryAdd(id, sent) ? Result.Accepted : Result.Replayed;
            }
        }
    }
}

/// <summary>
/// #5998: what goes on the wire. The generic webhook is signed with its destination's keys over the exact bytes sent;
/// Teams and Slack carry no signing header; a route's own URL is signed with the route's key; and a key that will not
/// open turns the destination off rather than sending it unsigned, with a health text and logs that never carry a key.
/// </summary>
public sealed class SignedWebhookSendTests
{
    private static readonly PasswordPrivateKey RingKey = NewRingKey();

    private static PasswordPrivateKey NewRingKey()
    {
        using var rsa = RSA.Create(DarlingPasswordKeyFile.KeyBits);
        return PasswordPrivateKey.FromPkcs8(rsa.ExportPkcs8PrivateKey());
    }

    private static string Seal(IPasswordKeyRing ring, string slot, string row, string value, string? proxy, string? boundUrl) =>
        ring.Seal(value, PasswordBinding.ForWebhook(slot, row, proxy, boundUrl));

    private static WebhookSigningKeys SealedKeys(IPasswordKeyRing ring, string row, string storedUrl, string key) =>
        new(Seal(ring, PasswordBinding.SigningKeySlot, row, key, "", storedUrl), "", null);

    private static async Task<WebhookFanoutResult> Fire(DarlingAlertSettings settings, string metric = "CPU High")
    {
        var service = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance);
        return await service.TrySendWebhookAlertsAsync(metric, "sql-01", "95", "80", serverId: "1");
    }

    [Fact]
    public async Task TheGenericSend_IsSignedOverTheBytesSent_AndTeamsAndSlackCarryNoSigningHeader()
    {
        using var generic = new RawEndpoint();
        using var teams = new RawEndpoint();
        using var slack = new RawEndpoint();
        var ring = DarlingPasswordKey.FromPrivateKey(RingKey);
        var key = WebhookSigning.NewKey();
        var config = new DarlingConfig();
        config.Webhooks.GenericUrl = Seal(ring, "generic", PasswordBinding.WebhookSettingsRow, generic.Url, "", null);
        config.Webhooks.GenericSigning = SealedKeys(ring, PasswordBinding.WebhookSettingsRow, config.Webhooks.GenericUrl, key);
        config.Webhooks.TeamsUrl = teams.Url;
        config.Webhooks.SlackUrl = slack.Url;

        await Fire(new DarlingAlertSettings(config, ring: ring));

        var request = Assert.Single(generic.Requests);
        Assert.Equal(SignedWebhookSignatureTests.SampleReceiver.Result.Accepted,
            new SignedWebhookSignatureTests.SampleReceiver(key).Verify(
                request.Header(WebhookSigning.IdHeader)!, request.Header(WebhookSigning.TimestampHeader)!,
                request.Header(WebhookSigning.SignatureHeader)!, request.Body, DateTime.UtcNow));

        foreach (var other in teams.Requests.Concat(slack.Requests))
        {
            Assert.Null(other.Header(WebhookSigning.IdHeader));
            Assert.Null(other.Header(WebhookSigning.TimestampHeader));
            Assert.Null(other.Header(WebhookSigning.SignatureHeader));
        }

        Assert.Single(teams.Requests);
        Assert.Single(slack.Requests);
    }

    [Fact]
    public async Task ARouteUrl_IsSignedWithTheRoutesKey_NotTheSettingsRowsKey()
    {
        using var parent = new RawEndpoint();
        using var routed = new RawEndpoint();
        var ring = DarlingPasswordKey.FromPrivateKey(RingKey);
        var parentKey = WebhookSigning.NewKey();
        var routeKey = WebhookSigning.NewKey();
        var config = new DarlingConfig();
        config.Webhooks.GenericUrl = Seal(ring, "generic", PasswordBinding.WebhookSettingsRow, parent.Url, "", null);
        config.Webhooks.GenericSigning = SealedKeys(ring, PasswordBinding.WebhookSettingsRow, config.Webhooks.GenericUrl, parentKey);
        var routeRow = PasswordBinding.WebhookRouteRow(7);
        var routeUrl = Seal(ring, "generic", routeRow, routed.Url, "", null);
        config.NotificationRoutes = new[]
        {
            new NotificationRoute(7, "CPU High", "", "", routeUrl, "", "", true) { GenericSigning = SealedKeys(ring, routeRow, routeUrl, routeKey) },
        };

        await Fire(new DarlingAlertSettings(config, ring: ring));

        Assert.Empty(parent.Requests);
        var request = Assert.Single(routed.Requests);
        string Check(string k) => new SignedWebhookSignatureTests.SampleReceiver(k).Verify(
            request.Header(WebhookSigning.IdHeader)!, request.Header(WebhookSigning.TimestampHeader)!,
            request.Header(WebhookSigning.SignatureHeader)!, request.Body, DateTime.UtcNow).ToString();
        Assert.Equal("Accepted", Check(routeKey));
        Assert.Equal("BadSignature", Check(parentKey));
    }

    /// <summary>
    /// #6456: an investigation event post carries the same <c>User-Agent</c> an alert post to the same receiver does, beside its
    /// three <c>webhook-*</c> headers.
    /// </summary>
    [Fact]
    public async Task AnInvestigationEventPost_CarriesTheSameUserAgentAnAlertPostDoes()
    {
        using var receiver = new RawEndpoint();
        var ring = DarlingPasswordKey.FromPrivateKey(RingKey);
        var routeKey = WebhookSigning.NewKey();
        var config = new DarlingConfig();
        var routeRow = PasswordBinding.WebhookRouteRow(7);
        var routeUrl = Seal(ring, "generic", routeRow, receiver.Url, "", null);
        config.NotificationRoutes = new[]
        {
            new NotificationRoute(7, "CPU High", "", "", routeUrl, "", "", true) { GenericSigning = SealedKeys(ring, routeRow, routeUrl, routeKey) },
        };
        var settings = new DarlingAlertSettings(config, ring: ring);
        var service = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance);

        /* The route owns the "CPU High" alert, so the alert and the event reach the same receiver: the alert first. */
        await service.TrySendWebhookAlertsAsync("CPU High", "sql-01", "95", "80", serverId: "1");
        var error = await service.TrySendInvestigationEventAsync(
            7, receiver.Url, "{\"event\":\"claimed\"}", "evt-6456", TestContext.Current.CancellationToken);

        Assert.Null(error);
        Assert.Equal(2, receiver.Requests.Count);
        var alert = receiver.Requests[0];
        var evt = receiver.Requests[1];
        Assert.False(string.IsNullOrWhiteSpace(alert.Header("User-Agent")));
        Assert.Equal(alert.Header("User-Agent"), evt.Header("User-Agent"));
        Assert.Equal("evt-6456", evt.Header(WebhookSigning.IdHeader));
        Assert.NotNull(evt.Header(WebhookSigning.SignatureHeader));
    }

    [Fact]
    public async Task AKeyThatWillNotOpen_TurnsTheChannelOff_SendsNothingUnsigned_AndNoTextOrLogCarriesTheKey()
    {
        using var generic = new RawEndpoint();
        using var routed = new RawEndpoint();
        var ring = DarlingPasswordKey.FromPrivateKey(RingKey);
        var key = WebhookSigning.NewKey();
        var routeKey = WebhookSigning.NewKey();
        var config = new DarlingConfig();
        config.Webhooks.GenericUrl = Seal(ring, "generic", PasswordBinding.WebhookSettingsRow, generic.Url, "", null);

        /* Sealed for another URL: it will not open for this row. */
        config.Webhooks.GenericSigning = SealedKeys(ring, PasswordBinding.WebhookSettingsRow, "sealed:another-url", key);
        var routeRow = PasswordBinding.WebhookRouteRow(4);
        var routeUrl = Seal(ring, "generic", routeRow, routed.Url, "", null);
        config.NotificationRoutes = new[]
        {
            new NotificationRoute(4, "Deadlocks Detected", "", "", routeUrl, "", "", true)
            {
                GenericSigning = new WebhookSigningKeys(Seal(ring, PasswordBinding.SigningKeySlot, PasswordBinding.WebhookRouteRow(5), routeKey, "", routeUrl), "", null),
            },
        };

        var logs = new CapturingLoggerProvider();
        using var factory = LoggerFactory.Create(b => b.AddProvider(logs).SetMinimumLevel(LogLevel.Trace));
        var settings = new DarlingAlertSettings(config, factory.CreateLogger("alerts"), ring);
        var service = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, factory.CreateLogger<WebhookAlertService>());

        await service.TrySendWebhookAlertsAsync("CPU High", "sql-01", "95", "80", serverId: "1");
        await service.TrySendWebhookAlertsAsync("Deadlocks Detected", "sql-01", "3", "1", serverId: "1");

        Assert.Empty(generic.Requests);
        Assert.Empty(routed.Requests);
        Assert.True(((IAlertSettings)settings).GenericSigningKeyUnreadable);

        var problems = settings.WebhookChannelProblems;
        Assert.Equal(2, problems.Count);
        Assert.All(problems, p => Assert.Contains("signing key", p, StringComparison.Ordinal));

        var everything = string.Join("\n", problems) + "\n" + string.Join("\n", logs.Lines) + "\n" + settings.NotificationRoutes[0];
        foreach (var secret in new[] { key, routeKey })
        {
            Assert.DoesNotContain(secret[WebhookSigning.KeyPrefix.Length..], everything, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheDiagnosticsBundle_AliasesAnOpenedSigningKey_InLogText()
    {
        var ring = DarlingPasswordKey.FromPrivateKey(RingKey);
        var key = WebhookSigning.NewKey();
        var config = new DarlingConfig();
        config.Webhooks.GenericUrl = Seal(ring, "generic", PasswordBinding.WebhookSettingsRow, "https://generic.example.test/hook", "", null);
        config.Webhooks.GenericSigning = SealedKeys(ring, PasswordBinding.WebhookSettingsRow, config.Webhooks.GenericUrl, key);
        var aliaser = new BundleAliaser();

        var notes = DiagnosticsBundle.SeedNotificationIdentifiers(aliaser, config, ring);
        var text = aliaser.Alias($"a line that leaked {key} and {key[WebhookSigning.KeyPrefix.Length..]}");

        Assert.Empty(notes);
        Assert.DoesNotContain(key[WebhookSigning.KeyPrefix.Length..], text, StringComparison.Ordinal);
    }

    /// <summary>A loopback endpoint that answers every POST 200 and keeps each request's head and exact body bytes.</summary>
    private sealed class RawEndpoint : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new();
        private readonly ConcurrentQueue<Captured> _requests = new();

        public RawEndpoint()
        {
            _listener.Start();
            Url = $"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}/hook";
            _ = Task.Run(AcceptLoopAsync);
        }

        public string Url { get; }

        public IReadOnlyList<Captured> Requests => _requests.ToArray();

        private async Task AcceptLoopAsync()
        {
            while (!_stop.IsCancellationRequested)
            {
                TcpClient client;
                try
                {
                    client = await _listener.AcceptTcpClientAsync(_stop.Token);
                }
                catch (Exception)
                {
                    return;
                }

                using (client)
                {
                    var stream = client.GetStream();
                    var buffer = new MemoryStream();
                    var chunk = new byte[8192];
                    int headEnd = -1, length = 0;
                    while (true)
                    {
                        var read = await stream.ReadAsync(chunk, _stop.Token);
                        if (read == 0)
                        {
                            break;
                        }

                        buffer.Write(chunk, 0, read);
                        var all = buffer.ToArray();
                        if (headEnd < 0)
                        {
                            headEnd = IndexOf(all, "\r\n\r\n"u8.ToArray());
                            if (headEnd >= 0)
                            {
                                var head = Encoding.ASCII.GetString(all, 0, headEnd);
                                var line = head.Split("\r\n").FirstOrDefault(l => l.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase));
                                length = line is null ? 0 : int.Parse(line[15..].Trim(), CultureInfo.InvariantCulture);
                            }
                        }

                        if (headEnd >= 0 && all.Length >= headEnd + 4 + length)
                        {
                            _requests.Enqueue(new Captured(Encoding.ASCII.GetString(all, 0, headEnd), all.AsSpan(headEnd + 4, length).ToArray()));
                            await stream.WriteAsync("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"u8.ToArray(), _stop.Token);
                            break;
                        }
                    }
                }
            }
        }

        private static int IndexOf(byte[] haystack, byte[] needle)
        {
            for (var i = 0; i <= haystack.Length - needle.Length; i++)
            {
                if (haystack.AsSpan(i, needle.Length).SequenceEqual(needle))
                {
                    return i;
                }
            }

            return -1;
        }

        public void Dispose()
        {
            _stop.Cancel();
            _listener.Stop();
        }
    }

    private sealed record Captured(string Head, byte[] Body)
    {
        public string? Header(string name) => Head.Split("\r\n")
            .Where(l => l.StartsWith(name + ":", StringComparison.OrdinalIgnoreCase))
            .Select(l => l[(name.Length + 1)..].Trim())
            .FirstOrDefault();
    }

    private sealed class CapturingLoggerProvider : ILoggerProvider
    {
        public ConcurrentQueue<string> Lines { get; } = new();

        public ILogger CreateLogger(string categoryName) => new Logger(Lines);

        public void Dispose()
        {
        }

        private sealed class Logger(ConcurrentQueue<string> lines) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
                lines.Enqueue(formatter(state, exception) + " " + exception);
        }
    }
}

/// <summary>
/// #5998: the previous key's window. A previous key past its window is never opened, so an unreadable one cannot turn the channel off;
/// one that fails inside its window is dropped with a warning that names the route while the current key still signs; and the window
/// end, written on the database's clock, is compared on that clock.
/// </summary>
public sealed class SignedWebhookWindowTests
{
    private static readonly DateTime Now = new(2026, 10, 16, 12, 0, 0, DateTimeKind.Utc);
    private const string RouteUrl = "https://route.test/hook-not-real";
    private static readonly PasswordPrivateKey RingKey = NewRingKey();

    private static PasswordPrivateKey NewRingKey()
    {
        using var rsa = RSA.Create(DarlingPasswordKeyFile.KeyBits);
        return PasswordPrivateKey.FromPkcs8(rsa.ExportPkcs8PrivateKey());
    }

    private static string Seal(IPasswordKeyRing ring, string slot, string row, string value, string? storedUrl) =>
        ring.Seal(value, PasswordBinding.ForWebhook(slot, row, "", storedUrl));

    /// <summary>A route whose current key opens with <paramref name="ring"/> and whose previous key was sealed by <paramref name="previousSealedBy"/>.</summary>
    private static (OpenedWebhooks Opened, string Key, string Previous) OpenRoute(IPasswordKeyRing ring, IPasswordKeyRing previousSealedBy, DateTime until)
    {
        var key = WebhookSigning.NewKey();
        var previous = WebhookSigning.NewKey();
        var row = PasswordBinding.WebhookRouteRow(7);
        var url = Seal(ring, "generic", row, RouteUrl, null);
        var route = new NotificationRoute(7, "CPU", "", "", url, "", "", true)
        {
            GenericSigning = new WebhookSigningKeys(
                Seal(ring, PasswordBinding.SigningKeySlot, row, key, url),
                Seal(previousSealedBy, PasswordBinding.SigningKeySlot, row, previous, url),
                until),
        };
        return (DarlingWebhookSecrets.Open(new WebhooksConfig(), new[] { route }, ring, Now), key, previous);
    }

    [Fact]
    public void APreviousKeyPastItsWindow_IsNeverOpened_SoAnUnreadableOneDoesNotTurnTheChannelOff()
    {
        var ring = DarlingPasswordKey.FromPrivateKey(RingKey);
        var other = DarlingPasswordKey.FromPrivateKey(NewRingKey());

        var (opened, key, _) = OpenRoute(ring, other, Now.AddSeconds(-1));

        Assert.Empty(opened.Failures);
        Assert.Empty(opened.Warnings);
        var route = Assert.Single(opened.Routes);
        Assert.False(route.IsOffFor(NotificationRouter.GenericChannel));
        Assert.NotEmpty(route.GenericUrl);
        Assert.Equal(new[] { key }, route.GenericSigning!.LiveKeys(Now));
    }

    [Fact]
    public void APreviousKeyThatWillNotOpenInsideItsWindow_IsDropped_WithAWarningNamingTheRoute_AndTheCurrentKeyStillSigns()
    {
        var ring = DarlingPasswordKey.FromPrivateKey(RingKey);
        var other = DarlingPasswordKey.FromPrivateKey(NewRingKey());

        var (opened, key, previous) = OpenRoute(ring, other, Now.AddDays(1));

        Assert.Empty(opened.Failures);
        var warning = Assert.Single(opened.Warnings);
        Assert.Contains("route 7", warning, StringComparison.Ordinal);
        Assert.DoesNotContain(previous[WebhookSigning.KeyPrefix.Length..], warning, StringComparison.Ordinal);
        Assert.DoesNotContain(key[WebhookSigning.KeyPrefix.Length..], warning, StringComparison.Ordinal);
        var route = Assert.Single(opened.Routes);
        Assert.False(route.IsOffFor(NotificationRouter.GenericChannel));
        Assert.NotEmpty(route.GenericUrl);
        Assert.Equal(new[] { key }, route.GenericSigning!.LiveKeys(Now));
    }

    [Fact]
    public void APreviousKeyThatOpensInsideItsWindow_StillSignsBesideTheCurrentKey()
    {
        var ring = DarlingPasswordKey.FromPrivateKey(RingKey);

        var (opened, key, previous) = OpenRoute(ring, ring, Now.AddDays(1));

        Assert.Empty(opened.Warnings);
        Assert.Equal(new[] { key, previous }, Assert.Single(opened.Routes).GenericSigning!.LiveKeys(Now));
    }

    [Fact]
    public void TwoReadsOfAnUnchangedRow_AreEqual_WhateverClockOffsetEachMeasured()
    {
        var until = Now;
        var a = new WebhookSigningKeys("k", "p", until, TimeSpan.FromMilliseconds(3));
        var b = new WebhookSigningKeys("k", "p", until, TimeSpan.FromMilliseconds(9));

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, new WebhookSigningKeys("k", "p", until.AddSeconds(1), TimeSpan.FromMilliseconds(3)));
    }

    [Fact]
    public void TheWindowEnd_IsComparedOnTheDatabaseClock_NotTheServiceClock()
    {
        /* One instant: the window end. The service clock is five minutes behind it; the database's is one second past it. */
        var until = Now;
        var serviceNow = until.AddMinutes(-5);
        var current = WebhookSigning.NewKey();
        var previous = WebhookSigning.NewKey();

        var databaseOneSecondPast = new WebhookSigningKeys(current, previous, until, TimeSpan.FromMinutes(5) + TimeSpan.FromSeconds(1));
        var databaseOneSecondBefore = new WebhookSigningKeys(current, previous, until, TimeSpan.FromMinutes(5) - TimeSpan.FromSeconds(1));
        var clocksAgree = new WebhookSigningKeys(current, previous, until);

        Assert.Equal(new[] { current }, databaseOneSecondPast.LiveKeys(serviceNow));
        Assert.Equal(new[] { current, previous }, databaseOneSecondBefore.LiveKeys(serviceNow));
        Assert.Equal(new[] { current, previous }, clocksAgree.LiveKeys(serviceNow));
    }
}
