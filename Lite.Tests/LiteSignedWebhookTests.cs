/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
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
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6012 part B, Lite's twins of Darling's #5998 pins. Lite is the sender, so a generic webhook to a destination with a key
/// carries the three Standard Webhooks headers over the exact bytes sent, and a receiver that recomputes the HMAC accepts it
/// (and refuses one changed byte); during a rotation both keys verify. The key is kept in the secret store (an in-memory
/// pair here, never the user's Credential Manager) and in no table, settings text or status line; and a saved key that will
/// not open sends nothing to that destination. In the <c>app-alert-statics</c> collection: the settings row's destination is
/// the App statics, and the route cache is static.
/// </summary>
[Collection("app-alert-statics")]
public sealed class LiteSignedWebhookTests : IDisposable
{
    private readonly string _tempDir;
    private readonly DuckDbInitializer _db;
    private readonly FakeRouteSecrets _secrets = new();
    private readonly DuckDbNotificationRouteStore _store;
    private readonly bool _enabled = App.GenericWebhookEnabled;
    private readonly string _url = App.GenericWebhookUrl;
    private readonly string _headers = App.GenericWebhookHeadersJson;
    private readonly bool _signingSet = App.GenericSigningSet;
    private readonly DateTime? _signingUntil = App.GenericSigningUntilUtc;

    public LiteSignedWebhookTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteSignedWebhookTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _db = new DuckDbInitializer(Path.Combine(_tempDir, "lite.duckdb"));
        _db.InitializeAsync().GetAwaiter().GetResult();
        _store = new DuckDbNotificationRouteStore(_db, () => new[] { 7101 }, _secrets.Access);
        App.GenericWebhookEnabled = false;
        App.GenericWebhookUrl = "";
        App.GenericWebhookHeadersJson = "";
        App.GenericSigningSet = false;
        App.GenericSigningUntilUtc = null;
    }

    public void Dispose()
    {
        NotificationRouteCache.Configure(null);
        App.GenericWebhookEnabled = _enabled;
        App.GenericWebhookUrl = _url;
        App.GenericWebhookHeadersJson = _headers;
        App.GenericSigningSet = _signingSet;
        App.GenericSigningUntilUtc = _signingUntil;
        _db.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static int s_metric;

    /// <summary>One alert through the real sender, the settings Lite really builds; a fresh metric name each time so no cooldown holds it.</summary>
    private async Task FireAsync()
    {
        var service = new WebhookAlertService(new AppAlertSettings(_secrets.Access), EmailAlertService.Branding, new AppLoggerAdapter<WebhookAlertService>());
        var metric = "High CPU " + Interlocked.Increment(ref s_metric).ToString(CultureInfo.InvariantCulture);
        await service.TrySendWebhookAlertsAsync(metric, "example-server", "97%", "90%", serverId: "7101", cancellationToken: Ct);
    }

    /// <summary>The receiver check of Darling's docs/signed-webhooks.md, reduced to the HMAC: true when any signature in the header
    /// equals the HMAC-SHA256 of id.timestamp.body under <paramref name="key"/>.</summary>
    private static bool Verifies(string key, Captured request)
    {
        var id = request.Header(WebhookSigning.IdHeader);
        var timestamp = request.Header(WebhookSigning.TimestampHeader);
        var header = request.Header(WebhookSigning.SignatureHeader);
        if (id is null || timestamp is null || header is null)
        {
            return false;
        }

        var secret = Convert.FromBase64String(key[WebhookSigning.KeyPrefix.Length..]);
        var signed = Encoding.UTF8.GetBytes(id + "." + timestamp + ".").Concat(request.Body).ToArray();
        var expected = "v1," + Convert.ToBase64String(HMACSHA256.HashData(secret, signed));
        return header.Split(' ').Contains(expected, StringComparer.Ordinal);
    }

    private static async Task<int> SaveRouteAsync(DuckDbNotificationRouteStore store, string url) =>
        await store.InsertAsync(new NotificationRoute(0, NotificationRoute.WildcardMatch, "", "", url, "", "", true) { ScopeServerId = 7101 });

    private void LoadRoutesIntoTheCache() =>
        NotificationRouteCache.Configure(() => Task.Run(() => _store.ListAsync()).GetAwaiter().GetResult());

    // ---- pin 1: the send is signed ---------------------------------------------------------------

    [Fact]
    public async Task TheSettingsRowsGenericSend_CarriesTheThreeHeaders_AReceiverAccepts_AndRefusesOneChangedByte()
    {
        using var endpoint = new RawEndpoint();
        var made = LiteWebhookSigningKeys.Generate(_secrets.Access, LiteWebhookSigningKeys.SettingsName(false), LiteWebhookSigningKeys.SettingsName(true), LiteSigningState.None, DateTime.UtcNow);
        Assert.NotNull(made.NewKey);
        App.GenericWebhookEnabled = true;
        App.GenericWebhookUrl = endpoint.Url;
        App.GenericSigningSet = made.State.HasKey;
        App.GenericSigningUntilUtc = made.State.PreviousUntilUtc;

        await FireAsync();

        var request = Assert.Single(endpoint.Requests);
        Assert.NotNull(request.Header(WebhookSigning.IdHeader));
        Assert.NotNull(request.Header(WebhookSigning.TimestampHeader));
        Assert.StartsWith("v1,", request.Header(WebhookSigning.SignatureHeader), StringComparison.Ordinal);
        Assert.True(Verifies(made.NewKey!, request));
        var changed = (byte[])request.Body.Clone();
        changed[3] ^= 0x01;
        Assert.False(Verifies(made.NewKey!, request with { Body = changed }));
    }

    [Fact]
    public async Task ARoutesOwnUrl_IsSignedWithTheRoutesKey_AndDuringARotationBothKeysVerify()
    {
        using var endpoint = new RawEndpoint();
        var id = await SaveRouteAsync(_store, endpoint.Url);
        var (first, firstError) = await _store.GenerateSigningKeyAsync(id);
        Assert.Null(firstError);
        var (second, secondError) = await _store.GenerateSigningKeyAsync(id);
        Assert.Null(secondError);
        Assert.NotEqual(first, second);
        LoadRoutesIntoTheCache();

        await FireAsync();

        var request = Assert.Single(endpoint.Requests);
        Assert.True(Verifies(second!, request), "the new key signs");
        Assert.True(Verifies(first!, request), "the previous key signs during the 7 days");
        Assert.Equal(2, request.Header(WebhookSigning.SignatureHeader)!.Split(' ').Length);
    }

    // ---- pin 2: where the key is kept ------------------------------------------------------------

    [Fact]
    public async Task TheKey_IsInTheSecretStore_AndInNoTableStatusTextOrErrorAfterItIsMade()
    {
        using var endpoint = new RawEndpoint();
        var id = await SaveRouteAsync(_store, endpoint.Url);

        var (key, error) = await _store.GenerateSigningKeyAsync(id);

        Assert.Null(error);
        Assert.Equal(key, _secrets.Read(LiteWebhookSigningKeys.RouteName(id, false)));
        var bare = key![WebhookSigning.KeyPrefix.Length..];

        /* Every column of the route's row, as text: the flag and the rotation end are there, the key is not. */
        using (var connection = _db.CreateConnection())
        {
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT CAST(r AS VARCHAR) FROM notification_routes AS r";
            var row = Convert.ToString(command.ExecuteScalar(), CultureInfo.InvariantCulture)!;
            Assert.Contains("true", row, StringComparison.Ordinal);
            Assert.DoesNotContain(bare, row, StringComparison.Ordinal);
        }

        /* The route as the alert path reads it prints no key, and the text the screens show names none. */
        var route = Assert.Single(await _store.ListAsync(Ct));
        Assert.True(route.GenericSigning!.HasKey);
        Assert.DoesNotContain(bare, route.GenericSigning.ToString(), StringComparison.Ordinal);
        var state = new LiteSigningState(true, DateTime.UtcNow.AddDays(7));
        Assert.DoesNotContain(bare, LiteWebhookSigningKeys.StatusText(state, DateTime.UtcNow), StringComparison.Ordinal);
        Assert.DoesNotContain(bare, LiteWebhookSigningKeys.StatusText(state, DateTime.UtcNow, unreadable: true), StringComparison.Ordinal);

        /* A write Credential Manager refuses reports why and never carries the key. */
        _secrets.WriteGate = (_, _) => false;
        var refused = LiteWebhookSigningKeys.Generate(_secrets.Access, "a", "b", LiteSigningState.None, DateTime.UtcNow);
        Assert.Null(refused.NewKey);
        Assert.NotNull(refused.Error);
        Assert.DoesNotContain(bare, refused.Error + refused, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSettingsRowsTwoPlainFacts_LoadFromSettingsJson_AndNothingElseAboutTheKeyDoes()
    {
        var configDirectory = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDirectory);
        var until = new DateTime(2026, 11, 1, 12, 0, 0, DateTimeKind.Utc);
        File.WriteAllText(Path.Combine(configDirectory, "settings.json"),
            "{\"generic_signing_set\":true,\"generic_signing_until\":\"" + until.ToString("o", CultureInfo.InvariantCulture) + "\"}");

        App.LoadAlertSettings(configDirectory, _ => "", (key, _) => Assert.Fail($"LoadAlertSettings wrote credential '{key}'."));

        Assert.True(App.GenericSigningSet);
        Assert.Equal(until, App.GenericSigningUntilUtc);
    }

    // ---- pin 3: a saved key that will not open sends nothing -------------------------------------

    [Fact]
    public async Task ASettingsRowWithAKeyFlagAndNoReadableKey_SendsNothing()
    {
        using var endpoint = new RawEndpoint();
        App.GenericWebhookEnabled = true;
        App.GenericWebhookUrl = endpoint.Url;
        App.GenericSigningSet = true;

        await FireAsync();

        Assert.Empty(endpoint.Requests);
        Assert.True(new AppAlertSettings(_secrets.Access).GenericSigningKeyUnreadable);
    }

    [Fact]
    public async Task ARouteWithAKeyFlagAndNoReadableKey_SendsNothingToItsUrl_AndNothingFallsToTheAppWideUrl()
    {
        using var routed = new RawEndpoint();
        using var appWide = new RawEndpoint();
        var id = await SaveRouteAsync(_store, routed.Url);
        var (key, _) = await _store.GenerateSigningKeyAsync(id);
        Assert.NotNull(key);
        App.GenericWebhookEnabled = true;
        App.GenericWebhookUrl = appWide.Url;
        Assert.True(_secrets.Delete(LiteWebhookSigningKeys.RouteName(id, false)));
        LoadRoutesIntoTheCache();

        await FireAsync();

        Assert.Empty(routed.Requests);
        Assert.Empty(appWide.Requests);
        Assert.True(Assert.Single(await _store.ListAsync(Ct)).IsOffFor(NotificationRouter.GenericChannel));
    }

    [Fact]
    public async Task ADestinationWithNoKey_IsSentUnsigned_AsItAlwaysWas()
    {
        using var endpoint = new RawEndpoint();
        App.GenericWebhookEnabled = true;
        App.GenericWebhookUrl = endpoint.Url;

        await FireAsync();

        var request = Assert.Single(endpoint.Requests);
        Assert.Null(request.Header(WebhookSigning.SignatureHeader));
    }

    internal sealed record Captured(string Head, byte[] Body)
    {
        public string? Header(string name) => Head.Split("\r\n")
            .Select(l => l.Split(':', 2))
            .Where(p => p.Length == 2 && string.Equals(p[0].Trim(), name, StringComparison.OrdinalIgnoreCase))
            .Select(p => p[1].Trim())
            .FirstOrDefault();
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
                    var headEnd = -1;
                    var length = 0;
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
            _stop.Dispose();
        }
    }
}
