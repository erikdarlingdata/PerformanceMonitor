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
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5998 part 3, the viewer's key block without a store: what a key is sealed under (the service opens it), what the block says,
/// and which saves it refuses. The key text comes from <see cref="WebhookSigning.NewKey"/> and is never in a state's text.
/// </summary>
public sealed class ViewerSigningKeyTests
{
    private const string Url = "https://receiver.test/hook-not-real";
    private static readonly DateTime Now = new(2026, 10, 16, 12, 0, 0, DateTimeKind.Utc);

    private static string SealedUrl(PasswordPrivateKey key, string row, string proxy = "") =>
        DarlingPasswordKey.FromPrivateKey(key).Seal(Url, PasswordBinding.ForWebhook("generic", row, proxy, null));

    [Fact]
    public void AGeneratedKey_IsSealedUnderTheBindingTheServiceOpensIt_ForTheSettingsRowAndForARoute()
    {
        using var private_ = PasswordPrivateKey.Generate();
        var ring = DarlingPasswordKey.FromPrivateKey(private_);
        var sealer = new ViewerPasswordSealer(private_.PublicKey);

        var settingsUrl = SealedUrl(private_, PasswordBinding.WebhookSettingsRow);
        var (settingsKey, settingsSealed) = ViewerWebhookSigningKeys.MakeKey(
            PasswordBinding.WebhookSettingsRow, new ViewerSigningKeyState("", "", null, settingsUrl, ""), sealer);
        var routeUrl = SealedUrl(private_, PasswordBinding.WebhookRouteRow(7));
        var (routeKey, routeSealed) = ViewerWebhookSigningKeys.MakeKey(
            PasswordBinding.WebhookRouteRow(7), new ViewerSigningKeyState("", "", null, routeUrl, ""), sealer);

        var webhooks = new WebhooksConfig { GenericUrl = settingsUrl, GenericSigning = new WebhookSigningKeys(settingsSealed, "", null) };
        var route = new NotificationRoute(7, "High CPU", "", "", routeUrl, "", "", true)
        {
            GenericSigning = new WebhookSigningKeys(routeSealed, "", null),
        };
        var opened = DarlingWebhookSecrets.Open(webhooks, new[] { route }, ring);

        Assert.Empty(opened.Failures);
        Assert.Equal(settingsKey, opened.Webhooks.GenericSigning!.Key);
        Assert.Equal(routeKey, Assert.Single(opened.Routes).GenericSigning!.Key);
        Assert.StartsWith(WebhookSigning.KeyPrefix, settingsKey, StringComparison.Ordinal);
        Assert.True(ViewerWebhookSealing.IsSaved(settingsSealed));
        Assert.DoesNotContain(settingsKey, settingsSealed, StringComparison.Ordinal);
    }

    [Fact]
    public void AKeyCannotBeMade_ForAUrlThatIsNotSavedAndSealed()
    {
        using var private_ = PasswordPrivateKey.Generate();
        var sealer = new ViewerPasswordSealer(private_.PublicKey);

        var refused = Assert.Throws<ViewerPasswordRefusedException>(
            () => ViewerWebhookSigningKeys.MakeKey(PasswordBinding.WebhookSettingsRow, new ViewerSigningKeyState("", "", null, "", ""), sealer));
        Assert.Equal(ViewerWebhookSigningKeys.SaveUrlFirstText, refused.Message);
        Assert.Throws<ViewerPasswordRefusedException>(
            () => ViewerWebhookSigningKeys.MakeKey(PasswordBinding.WebhookSettingsRow, new ViewerSigningKeyState("", "", null, Url, ""), sealer));
    }

    [Fact]
    public void TheBlockReadsSaved_WhenAKeyIsSealed_AndSaysWhenTheOldKeyStops_AndCarriesNoKeyText()
    {
        using var private_ = PasswordPrivateKey.Generate();
        var sealer = new ViewerPasswordSealer(private_.PublicKey);
        var state = new ViewerSigningKeyState("", "", null, SealedUrl(private_, PasswordBinding.WebhookSettingsRow), "");
        Assert.Equal(ViewerWebhookSigningKeys.NoKeyText, ViewerWebhookSigningKeys.StatusText(state, Now));
        Assert.Equal("Generate key", ViewerWebhookSigningKeys.ActionText(state));

        var (plain, sealedKey) = ViewerWebhookSigningKeys.MakeKey(PasswordBinding.WebhookSettingsRow, state, sealer);
        var (oldPlain, oldSealed) = ViewerWebhookSigningKeys.MakeKey(PasswordBinding.WebhookSettingsRow, state, sealer);
        var saved = state with { Key = sealedKey };
        Assert.StartsWith("Saved.", ViewerWebhookSigningKeys.StatusText(saved, Now), StringComparison.Ordinal);
        Assert.Equal("Rotate key", ViewerWebhookSigningKeys.ActionText(saved));

        var until = Now.AddDays(WebhookSigning.RotationOverlapDays);
        var rotating = saved with { PreviousKey = oldSealed, PreviousUntilUtc = until };
        var rotatingText = ViewerWebhookSigningKeys.StatusText(rotating, Now);
        Assert.Contains("2026-10-23 12:00 UTC", rotatingText, StringComparison.Ordinal);
        Assert.DoesNotContain("also sent", ViewerWebhookSigningKeys.StatusText(rotating, until.AddSeconds(1)), StringComparison.Ordinal);

        foreach (var text in new[] { rotatingText, rotating.ToString(), ViewerWebhookSigningKeys.StatusText(saved, Now) })
        {
            Assert.DoesNotContain(plain, text, StringComparison.Ordinal);
            Assert.DoesNotContain(oldPlain, text, StringComparison.Ordinal);
            Assert.DoesNotContain(sealedKey, text, StringComparison.Ordinal);
            Assert.DoesNotContain(oldSealed, text, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ASaveThatChangesTheUrlTextOrTheProxy_IsRefusedWhileAKeyIsStored_AndOtherwiseAllowed()
    {
        var withKey = new ViewerSigningKeyState("seal:v1:a", "", null, "seal:v1:url", "http://proxy.test:8080");
        Assert.Null(ViewerWebhookSigningKeys.SaveRefusal(withKey, "seal:v1:url", "http://proxy.test:8080", checkProxy: true));
        Assert.Equal(ViewerWebhookSigningKeys.RemoveKeyFirstText, ViewerWebhookSigningKeys.SaveRefusal(withKey, "seal:v1:other", "http://proxy.test:8080", true));
        Assert.Equal(ViewerWebhookSigningKeys.RemoveKeyFirstText, ViewerWebhookSigningKeys.SaveRefusal(withKey, "", "http://proxy.test:8080", true));
        Assert.Equal(ViewerWebhookSigningKeys.RemoveKeyFirstText, ViewerWebhookSigningKeys.SaveRefusal(withKey, "seal:v1:url", "", true));

        var noKey = withKey with { Key = "" };
        Assert.Null(ViewerWebhookSigningKeys.SaveRefusal(noKey, "seal:v1:other", "", true));
        Assert.Null(ViewerWebhookSigningKeys.SaveRefusal(null, "seal:v1:other", "", true));

        Assert.Null(ViewerWebhookSigningKeys.RouteKeyRefusal(Array.Empty<int>(), "a", "b"));
        Assert.Null(ViewerWebhookSigningKeys.RouteKeyRefusal(new[] { 4 }, "a", "a"));
        Assert.Contains("4, 9", ViewerWebhookSigningKeys.RouteKeyRefusal(new[] { 9, 4 }, "a", "b")!, StringComparison.Ordinal);
    }
}

//// <summary>
/// #5998 part 3, the web status answer: <c>GET /api/webhook-signing</c> carries whether a key is set (it opened), whether a stored key
/// would not open, whether a rotation is in its window and when it ends, and never a key, sealed or opened. One clock: the same
/// instant is published and asked.
/// </summary>
public sealed class WebhookSigningStatusTests
{
    private static readonly DateTime Now = new(2026, 10, 16, 12, 0, 0, DateTimeKind.Utc);
    private static readonly PasswordPrivateKey RingKey = NewRingKey();
    private const string SettingsUrl = "https://settings.test/hook-not-real";
    private const string RouteUrl = "https://route.test/hook-not-real";

    private static PasswordPrivateKey NewRingKey()
    {
        using var rsa = System.Security.Cryptography.RSA.Create(DarlingPasswordKeyFile.KeyBits);
        return PasswordPrivateKey.FromPkcs8(rsa.ExportPkcs8PrivateKey());
    }

    private static string Seal(IPasswordKeyRing ring, string slot, string row, string value, string? storedUrl) =>
        ring.Seal(value, PasswordBinding.ForWebhook(slot, row, "", storedUrl));

    private static WebhookSigningKeys Keys(IPasswordKeyRing ring, string row, string storedUrl, string key, string? previous, DateTime? until) =>
        new(Seal(ring, PasswordBinding.SigningKeySlot, row, key, storedUrl),
            previous is null ? "" : Seal(ring, PasswordBinding.SigningKeySlot, row, previous, storedUrl), until);

    private static async Task<(int Status, JsonObject Body, string Raw)> GetAsync(CollectorRuntimeState state, DateTime now)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions());
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        await using var app = builder.Build();
        DarlingWebEndpoints.MapWebhookSigningStatus(app, state, () => now);
        await app.StartAsync(TestContext.Current.CancellationToken);
        using var server = app.GetTestServer();
        var context = await server.SendAsync(request =>
        {
            request.Request.Method = "GET";
            request.Request.Path = "/api/webhook-signing";
            request.Request.Headers.Host = "localhost";
        });
        using var reader = new StreamReader(context.Response.Body, Encoding.UTF8);
        var raw = await reader.ReadToEndAsync(TestContext.Current.CancellationToken);
        return (context.Response.StatusCode, JsonNode.Parse(raw)!.AsObject(), raw);
    }

    [Fact]
    public async Task TheAnswer_CarriesPresenceAndDates_ForTheSettingsRowAndRoutesWithKeys_AndNeverTheKey()
    {
        var ring = DarlingPasswordKey.FromPrivateKey(RingKey);
        var until = Now.AddDays(3);
        var current = WebhookSigning.NewKey();
        var previous = WebhookSigning.NewKey();
        var routeKey = WebhookSigning.NewKey();
        var settingsUrl = Seal(ring, "generic", PasswordBinding.WebhookSettingsRow, SettingsUrl, null);
        var routeRow = PasswordBinding.WebhookRouteRow(4);
        var routeUrl = Seal(ring, "generic", routeRow, RouteUrl, null);
        var state = new CollectorRuntimeState();
        state.SigningStatus.Publish(
            new WebhooksConfig
            {
                GenericUrl = settingsUrl,
                GenericSigning = Keys(ring, PasswordBinding.WebhookSettingsRow, settingsUrl, current, previous, until),
            },
            new[]
            {
                new NotificationRoute(4, "Deadlocks Detected", "", "", routeUrl, "", "", true) { GenericSigning = Keys(ring, routeRow, routeUrl, routeKey, null, null) },
                new NotificationRoute(5, "High CPU", "", "", "u", "", "", true),
            },
            Now, ring);

        var (status, body, raw) = await GetAsync(state, Now);

        Assert.Equal(200, status);
        Assert.Equal("ok", (string?)body["status"]);
        var settings = body["settings"]!.AsObject();
        Assert.True((bool)settings["key_set"]!);
        Assert.False((bool)settings["open_failed"]!);
        Assert.True((bool)settings["rotating"]!);
        Assert.Equal(until, DateTime.Parse((string)settings["rotation_ends_utc"]!, null, System.Globalization.DateTimeStyles.RoundtripKind).ToUniversalTime());
        Assert.Equal(until - WebhookSigning.RotationOverlap, DateTime.Parse((string)settings["last_rotated_utc"]!, null, System.Globalization.DateTimeStyles.RoundtripKind).ToUniversalTime());
        var route = Assert.Single(body["routes"]!.AsArray())!.AsObject();
        Assert.Equal(4, (int)route["route_id"]!);
        Assert.True((bool)route["key_set"]!);
        Assert.False((bool)route["open_failed"]!);
        Assert.False((bool)route["rotating"]!);
        Assert.Null(route["rotation_ends_utc"]);

        foreach (var secret in new[] { current, previous, routeKey })
        {
            Assert.DoesNotContain(secret[WebhookSigning.KeyPrefix.Length..], raw, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("seal:v1", raw, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AKeyThatWillNotOpen_IsNotShownAsSet_AndTheAnswerSaysOpenFailed()
    {
        /* The key was sealed for another ring, as after a lost password key: the channel is off, so the page must not say it is signed. */
        var other = DarlingPasswordKey.FromPrivateKey(NewRingKey());
        var ring = DarlingPasswordKey.FromPrivateKey(RingKey);
        var key = WebhookSigning.NewKey();
        var settingsUrl = Seal(ring, "generic", PasswordBinding.WebhookSettingsRow, SettingsUrl, null);
        var routeRow = PasswordBinding.WebhookRouteRow(6);
        var routeUrl = Seal(ring, "generic", routeRow, RouteUrl, null);
        var state = new CollectorRuntimeState();
        state.SigningStatus.Publish(
            new WebhooksConfig { GenericUrl = settingsUrl, GenericSigning = Keys(other, PasswordBinding.WebhookSettingsRow, settingsUrl, key, null, null) },
            new[] { new NotificationRoute(6, "CPU", "", "", routeUrl, "", "", true) { GenericSigning = Keys(other, routeRow, routeUrl, key, null, null) } },
            Now, ring);

        var (_, body, raw) = await GetAsync(state, Now);

        var settings = body["settings"]!.AsObject();
        Assert.False((bool)settings["key_set"]!);
        Assert.True((bool)settings["open_failed"]!);
        var route = Assert.Single(body["routes"]!.AsArray())!.AsObject();
        Assert.Equal(6, (int)route["route_id"]!);
        Assert.False((bool)route["key_set"]!);
        Assert.True((bool)route["open_failed"]!);
        Assert.DoesNotContain(key[WebhookSigning.KeyPrefix.Length..], raw, StringComparison.Ordinal);
        Assert.DoesNotContain("seal:v1", raw, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AfterTheWindow_NoRotationIsShown_AndAnUnpublishedBoardSaysSo()
    {
        var ring = DarlingPasswordKey.FromPrivateKey(RingKey);
        var until = Now.AddDays(1);
        var settingsUrl = Seal(ring, "generic", PasswordBinding.WebhookSettingsRow, SettingsUrl, null);
        var state = new CollectorRuntimeState();
        var unpublished = await GetAsync(state, Now);
        Assert.Equal("unavailable", (string?)unpublished.Body["status"]);

        state.SigningStatus.Publish(
            new WebhooksConfig
            {
                GenericUrl = settingsUrl,
                GenericSigning = Keys(ring, PasswordBinding.WebhookSettingsRow, settingsUrl, WebhookSigning.NewKey(), WebhookSigning.NewKey(), until),
            },
            Array.Empty<NotificationRoute>(), Now, ring);
        var after = (await GetAsync(state, until.AddSeconds(1))).Body["settings"]!.AsObject();
        Assert.True((bool)after["key_set"]!);
        Assert.False((bool)after["rotating"]!);
        Assert.Null(after["rotation_ends_utc"]);

        state.SigningStatus.Publish(new WebhooksConfig(), Array.Empty<NotificationRoute>(), Now, ring);
        var none = (await GetAsync(state, Now)).Body["settings"]!.AsObject();
        Assert.False((bool)none["key_set"]!);
        Assert.False((bool)none["open_failed"]!);
        Assert.False((bool)none["rotating"]!);
        Assert.Null(none["last_rotated_utc"]);
    }
}

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #5998 part 3 against a real store: generate seals a key the service opens and the column reads back as saved; rotate keeps the
/// previous key and a window of seven days; remove clears both; a route save that changes the URL is refused while a key is stored.
/// </summary>
public sealed class ViewerSigningKeyLiveTests
{
    private const string Url = "https://receiver.test/hook-not-real";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<T> ScalarAsync<T>(ScratchPostgres scratch, string sql, System.Threading.CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<StoreConfigView> ViewAsync(ScratchPostgres scratch, System.Threading.CancellationToken ct)
    {
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        return (await new StoreConfigProvider(postgres).LoadViewAsync(new DarlingConfig(), ct))!;
    }

    [Fact]
    public async Task Generate_ShowsTheKeyOnce_TheColumnReadsSaved_AndRotateKeepsThePreviousKeyForSevenDays_AndRemoveClearsBoth()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live signing key checks.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var private_ = PasswordPrivateKey.Generate();
        var ring = DarlingPasswordKey.FromPrivateKey(private_);
        var bodySucceeded = false;
        try
        {
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
            }

            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var sealer = new ViewerPasswordSealer(private_.PublicKey);

            /* A key needs the URL saved and sealed first. */
            await Assert.ThrowsAsync<ViewerPasswordRefusedException>(() => viewer.GenerateSettingsSigningKeyAsync(sealer, ct));
            var row = NotificationRow.Defaults();
            row.GenericUrl = Url;
            ViewerWebhookSealing.ResolveSettingsRow(row, null, sealer, null);
            await viewer.UpsertNotificationAsync(row, ct);

            var first = await viewer.GenerateSettingsSigningKeyAsync(sealer, ct);
            var afterGenerate = (await viewer.GetSettingsSigningKeyStateAsync(ct))!;
            Assert.StartsWith(WebhookSigning.KeyPrefix, first, StringComparison.Ordinal);
            Assert.True(ViewerWebhookSealing.IsSaved(afterGenerate.Key));
            Assert.DoesNotContain(first, afterGenerate.Key, StringComparison.Ordinal);
            Assert.Equal("", afterGenerate.PreviousKey);
            Assert.Null(afterGenerate.PreviousUntilUtc);

            var view = await ViewAsync(scratch, ct);
            var opened = DarlingWebhookSecrets.Open(view.Webhooks, view.NotificationRoutes, ring);
            Assert.Empty(opened.Failures);
            Assert.Equal(first, opened.Webhooks.GenericSigning!.Key);

            /* Rotate: the new key is current, the old sealed text moves to the previous slot untouched, the window is seven days.
               The database clock is the only clock: the window is read back as an interval against the same now(). */
            var second = await viewer.GenerateSettingsSigningKeyAsync(sealer, ct);
            var afterRotate = (await viewer.GetSettingsSigningKeyStateAsync(ct))!;
            Assert.NotEqual(first, second);
            Assert.Equal(afterGenerate.Key, afterRotate.PreviousKey);
            Assert.NotEqual(afterGenerate.Key, afterRotate.Key);
            var windowDays = await ScalarAsync<double>(
                scratch, "SELECT (extract(epoch FROM (previous_signing_key_until - now())) / 86400.0)::double precision FROM config.config_notification WHERE id = 1", ct);
            Assert.InRange(windowDays, WebhookSigning.RotationOverlapDays - 0.01, WebhookSigning.RotationOverlapDays);

            var rotatedView = await ViewAsync(scratch, ct);
            var rotatedOpened = DarlingWebhookSecrets.Open(rotatedView.Webhooks, rotatedView.NotificationRoutes, ring);
            Assert.Empty(rotatedOpened.Failures);
            Assert.Equal(second, rotatedOpened.Webhooks.GenericSigning!.Key);
            Assert.Equal(first, rotatedOpened.Webhooks.GenericSigning.PreviousKey);
            Assert.Contains("also sent until", ViewerWebhookSigningKeys.StatusText(afterRotate, DateTime.UtcNow), StringComparison.Ordinal);

            /* A Settings save that would change the URL text is refused while the key is stored. */
            Assert.Equal(
                ViewerWebhookSigningKeys.RemoveKeyFirstText,
                ViewerWebhookSigningKeys.SaveRefusal(afterRotate, "seal:v1:another", afterRotate.Proxy, checkProxy: true));

            await viewer.RemoveSettingsSigningKeyAsync(ct);
            var removed = (await viewer.GetSettingsSigningKeyStateAsync(ct))!;
            Assert.False(removed.HasKey);
            Assert.Equal("", removed.PreviousKey);
            Assert.Null(removed.PreviousUntilUtc);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task ASettingsSave_LocksTheKeyRowBetweenItsCheckAndItsWrite_SoAGenerateCannotLandInBetween()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live signing key checks.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var private_ = PasswordPrivateKey.Generate();
        var ring = DarlingPasswordKey.FromPrivateKey(private_);
        var bodySucceeded = false;
        try
        {
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
            }

            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            await using var other = new ViewerDataService(scratch.ConnectionString);
            var sealer = new ViewerPasswordSealer(private_.PublicKey);
            var first = NotificationRow.Defaults();
            first.GenericUrl = Url;
            ViewerWebhookSealing.ResolveSettingsRow(first, null, sealer, null);
            await viewer.UpsertNotificationAsync(first, ct);

            /* A save that changes the URL, with no key stored yet so the check passes. Inside the check-then-write window (the refusal
               callback runs after the check's reads and before the write) another viewer generates a key for the old URL. With the
               reads under FOR UPDATE that Generate waits for the save, then finds the URL changed and is refused; with plain reads it
               would complete inside the window and leave a key sealed for the old URL. */
            var second = NotificationRow.Defaults();
            second.GenericUrl = Url + "-moved";
            ViewerWebhookSealing.ResolveSettingsRow(second, null, sealer, null);
            Task<string>? generate = null;
            var finishedInsideTheWindow = false;
            await viewer.UpsertNotificationCheckingSigningKeysAsync(second, (state, ids) =>
            {
                generate = Task.Run(() => other.GenerateSettingsSigningKeyAsync(sealer, ct), ct);
                finishedInsideTheWindow = Task.WhenAny(generate, Task.Delay(TimeSpan.FromSeconds(2), ct)).GetAwaiter().GetResult() == generate;
                return null;
            }, ct);

            Assert.False(finishedInsideTheWindow);
            await Assert.ThrowsAsync<ViewerPasswordRefusedException>(() => generate!);
            Assert.False((await viewer.GetSettingsSigningKeyStateAsync(ct))!.HasKey);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task ASettingsSaveThatChangesTheProxy_BlocksARouteGenerate_SoNoKeySealedForTheOldProxyIsStored()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live signing key checks.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var private_ = PasswordPrivateKey.Generate();
        var bodySucceeded = false;
        try
        {
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
            }

            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            await using var other = new ViewerDataService(scratch.ConnectionString);
            var sealer = new ViewerPasswordSealer(private_.PublicKey);
            var first = NotificationRow.Defaults();
            first.GenericUrl = Url;
            first.GenericProxy = "http://proxy-one:8080";
            ViewerWebhookSealing.ResolveSettingsRow(first, null, sealer, null);
            await viewer.UpsertNotificationAsync(first, ct);

            /* A route with no key yet: the save's own locks cover only routes that already hold one, so they do not cover this route. */
            var routeId = await viewer.SaveNotificationRouteSealedAsync(
                new NotificationRouteRow { MetricMatch = "High CPU", GenericUrl = Url }, null, first, sealer, null, ct);

            /* A save that moves the generic proxy. Inside its check-then-write window (the callback runs after the save has read and
               locked the settings row and before it writes) another viewer generates the route's first key. The route write checks
               the proxy under FOR SHARE on the settings row, so it waits for the save, then sees the new proxy and is refused. With
               a plain subquery it would read the old proxy, finish inside the window, and store a key sealed for the old proxy. */
            var second = NotificationRow.Defaults();
            second.GenericUrl = Url;
            second.GenericProxy = "http://proxy-two:8080";
            ViewerWebhookSealing.ResolveSettingsRow(second, null, sealer, null);
            Task<string>? generate = null;
            var finishedInsideTheWindow = false;
            await viewer.UpsertNotificationCheckingSigningKeysAsync(second, (state, ids) =>
            {
                generate = Task.Run(() => other.GenerateRouteSigningKeyAsync(routeId, sealer, ct), ct);
                finishedInsideTheWindow = Task.WhenAny(generate, Task.Delay(TimeSpan.FromSeconds(2), ct)).GetAwaiter().GetResult() == generate;
                return null;
            }, ct);

            Assert.False(finishedInsideTheWindow);
            await Assert.ThrowsAsync<ViewerPasswordRefusedException>(() => generate!);
            Assert.False((await viewer.GetRouteSigningKeyStateAsync(routeId, ct))!.HasKey);
            Assert.Equal("http://proxy-two:8080", (await viewer.GetSettingsSigningKeyStateAsync(ct))!.Proxy);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task TheConfigRead_CarriesTheDatabaseClockOffset_OnEachDestinationsKeys()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live signing key checks.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var private_ = PasswordPrivateKey.Generate();
        var bodySucceeded = false;
        try
        {
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
            }

            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var sealer = new ViewerPasswordSealer(private_.PublicKey);
            var row = NotificationRow.Defaults();
            row.GenericUrl = Url;
            ViewerWebhookSealing.ResolveSettingsRow(row, null, sealer, null);
            await viewer.UpsertNotificationAsync(row, ct);
            await viewer.GenerateSettingsSigningKeyAsync(sealer, ct);
            var routeId = await viewer.SaveNotificationRouteSealedAsync(
                new NotificationRouteRow { MetricMatch = "High CPU", GenericUrl = Url }, null, NotificationRow.Defaults(), sealer, null, ct);
            await viewer.GenerateRouteSigningKeyAsync(routeId, sealer, ct);

            /* The database is on this machine, so the offset is only the round trip's jitter. */
            var view = await ViewAsync(scratch, ct);
            Assert.InRange(view.Webhooks.GenericSigning!.DatabaseClockAhead, TimeSpan.FromSeconds(-30), TimeSpan.FromSeconds(30));
            Assert.InRange(Assert.Single(view.NotificationRoutes).GenericSigning!.DatabaseClockAhead, TimeSpan.FromSeconds(-30), TimeSpan.FromSeconds(30));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task ARoutesKey_IsSealedForTheRoute_AndASaveThatChangesItsUrlIsRefusedWhileTheKeyIsStored()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live signing key checks.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var private_ = PasswordPrivateKey.Generate();
        var ring = DarlingPasswordKey.FromPrivateKey(private_);
        var bodySucceeded = false;
        try
        {
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
            }

            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var sealer = new ViewerPasswordSealer(private_.PublicKey);
            var parent = NotificationRow.Defaults();
            var routeId = await viewer.SaveNotificationRouteSealedAsync(
                new NotificationRouteRow { MetricMatch = "High CPU", GenericUrl = Url }, null, parent, sealer, null, ct);
            var stored = (await viewer.GetNotificationRoutesAsync(ct)).Single(r => r.RouteId == routeId);

            var key = await viewer.GenerateRouteSigningKeyAsync(routeId, sealer, ct);
            Assert.Equal(new[] { routeId }, await viewer.GetRouteIdsHoldingSigningKeysAsync(ct));
            Assert.True(ViewerWebhookSealing.IsSaved((await viewer.GetRouteSigningKeyStateAsync(routeId, ct))!.Key));

            var view = await ViewAsync(scratch, ct);
            var opened = DarlingWebhookSecrets.Open(view.Webhooks, view.NotificationRoutes, ring);
            Assert.Empty(opened.Failures);
            Assert.Equal(key, Assert.Single(opened.Routes, r => r.RouteId == routeId).GenericSigning!.Key);

            /* The same sealed URL kept: allowed, and the key still opens. A different URL: refused, and nothing changes. */
            var kept = stored.Clone();
            kept.GenericUrl = stored.GenericUrl;
            await viewer.SaveNotificationRouteSealedAsync(kept, stored, parent, null, null, ct);
            /* The save above is a newer version of the route (#6274): the next screen reads it again, as the Viewer's does after a save. */
            stored = (await viewer.GetNotificationRoutesAsync(ct)).Single(r => r.RouteId == routeId);
            var changed = stored.Clone();
            changed.GenericUrl = "https://another.test/hook-not-real";
            var refused = await Assert.ThrowsAsync<ViewerPasswordRefusedException>(
                () => viewer.SaveNotificationRouteSealedAsync(changed, stored, parent, sealer, null, ct));
            Assert.Equal(ViewerWebhookSigningKeys.RemoveKeyFirstText, refused.Message);
            Assert.Equal(stored.GenericUrl, (await viewer.GetNotificationRoutesAsync(ct)).Single(r => r.RouteId == routeId).GenericUrl);

            await viewer.RemoveRouteSigningKeyAsync(routeId, ct);
            Assert.Empty(await viewer.GetRouteIdsHoldingSigningKeysAsync(ct));
            var removed = (await viewer.GetRouteSigningKeyStateAsync(routeId, ct))!;
            Assert.False(removed.HasKey);
            Assert.Equal("", removed.PreviousKey);
            Assert.Null(removed.PreviousUntilUtc);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }
}
