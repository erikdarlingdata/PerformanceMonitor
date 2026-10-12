/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5366: what the viewer stores for the webhook URLs, the PagerDuty routing key and the generic headers. A typed value is
/// sealed for its slot, row, proxy and (headers) generic URL; a blank box keeps the saved sealed value; a lone dash removes it.
/// </summary>
public sealed class ViewerWebhookSealingTests
{
    private const string TeamsUrl = "https://example.test/teams-not-real";

    private static string Seal(PasswordPrivateKey key, string value, string slot, string row, string proxy = "", string url = "") =>
        PasswordSeal.Seal(value, key.PublicKey, PasswordBinding.ForWebhook(slot, row, proxy, url));

    [Fact]
    public void ATypedValue_IsSealedForTheSettingsRowAndItsProxy_AndOpensOnlyThere()
    {
        using var key = PasswordPrivateKey.Generate();
        var row = new NotificationRow { TeamsUrl = TeamsUrl, TeamsProxy = "http://proxy.example:8080" };

        ViewerWebhookSealing.ResolveSettingsRow(row, null, new ViewerPasswordSealer(key.PublicKey), null);

        Assert.True(PasswordSeal.IsSealed(row.TeamsUrl));
        Assert.DoesNotContain("example.test", row.TeamsUrl, StringComparison.Ordinal);
        Assert.Equal(TeamsUrl, PasswordSeal.Open(row.TeamsUrl, key, PasswordBinding.ForWebhook("teams", "notification", "http://proxy.example:8080", null)));
        Assert.Throws<PasswordSealException>(() => PasswordSeal.Open(row.TeamsUrl, key, PasswordBinding.ForWebhook("teams", "route:1", "http://proxy.example:8080", null)));
        Assert.Throws<PasswordSealException>(() => PasswordSeal.Open(row.TeamsUrl, key, PasswordBinding.ForWebhook("teams", "notification", "", null)));
    }

    [Fact]
    public void TheGenericHeaders_AreSealedOverTheStoredGenericUrlText()
    {
        using var key = PasswordPrivateKey.Generate();
        var row = new NotificationRow { GenericUrl = "https://example.test/generic-not-real", GenericHeaders = "{\"X-Key\":\"not-real\"}" };

        ViewerWebhookSealing.ResolveSettingsRow(row, null, new ViewerPasswordSealer(key.PublicKey), null);

        Assert.Equal(
            "{\"X-Key\":\"not-real\"}",
            PasswordSeal.Open(row.GenericHeaders, key, PasswordBinding.ForWebhook("generic_headers", "notification", "", row.GenericUrl)));
    }

    [Fact]
    public void ABlankBox_KeepsTheSavedValue_AndNeedsNoKey()
    {
        using var key = PasswordPrivateKey.Generate();
        var saved = Seal(key, TeamsUrl, "teams", "notification");
        var stored = new NotificationRow { TeamsUrl = saved };
        var row = new NotificationRow { TeamsUrl = ViewerWebhookSealing.CarryKept("  ", stored.TeamsUrl) };

        Assert.False(ViewerWebhookSealing.NeedsKey(row, stored));
        ViewerWebhookSealing.ResolveSettingsRow(row, stored, null, "No key.");

        Assert.Equal(saved, row.TeamsUrl);
        Assert.Equal("", ViewerWebhookSealing.ShownText(saved));
    }

    [Fact]
    public void ALoneDash_RemovesTheSavedValue()
    {
        using var key = PasswordPrivateKey.Generate();
        var stored = new NotificationRow { SlackUrl = Seal(key, "https://example.test/slack", "slack", "notification") };
        var row = new NotificationRow { SlackUrl = ViewerWebhookSealing.CarryKept("-", stored.SlackUrl) };

        ViewerWebhookSealing.ResolveSettingsRow(row, stored, null, null);

        Assert.Equal("", row.SlackUrl);
    }

    [Fact]
    public void ALegacyPlaintextValue_IsShownAndSealedOnSave()
    {
        using var key = PasswordPrivateKey.Generate();
        var stored = new NotificationRow { PagerDutyRoutingKey = "pd-key-not-real" };
        Assert.Equal("pd-key-not-real", ViewerWebhookSealing.ShownText(stored.PagerDutyRoutingKey));
        var row = new NotificationRow { PagerDutyRoutingKey = ViewerWebhookSealing.CarryKept("pd-key-not-real", stored.PagerDutyRoutingKey) };

        ViewerWebhookSealing.ResolveSettingsRow(row, stored, new ViewerPasswordSealer(key.PublicKey), null);

        Assert.True(PasswordSeal.IsSealed(row.PagerDutyRoutingKey));
    }

    [Fact]
    public void AChangedProxy_RefusesToKeepTheSavedValue()
    {
        using var key = PasswordPrivateKey.Generate();
        var stored = new NotificationRow { TeamsUrl = Seal(key, TeamsUrl, "teams", "notification", "http://a.example:1"), TeamsProxy = "http://a.example:1" };
        var row = new NotificationRow { TeamsUrl = stored.TeamsUrl, TeamsProxy = "http://b.example:2" };

        var ex = Assert.Throws<ViewerPasswordRefusedException>(() => ViewerWebhookSealing.ResolveSettingsRow(row, stored, null, null));

        Assert.Equal("Enter the Teams webhook URL again: its proxy changed.", ex.Message);
    }

    [Fact]
    public void ANewGenericUrl_RefusesToKeepTheSavedHeaders()
    {
        using var key = PasswordPrivateKey.Generate();
        var url = Seal(key, "https://example.test/generic", "generic", "notification");
        var stored = new NotificationRow { GenericUrl = url, GenericHeaders = Seal(key, "{}", "generic_headers", "notification", "", url) };
        var row = new NotificationRow { GenericUrl = "https://example.test/other", GenericHeaders = stored.GenericHeaders };

        var ex = Assert.Throws<ViewerPasswordRefusedException>(
            () => ViewerWebhookSealing.ResolveSettingsRow(row, stored, new ViewerPasswordSealer(key.PublicKey), null));

        Assert.Equal("Enter the generic webhook headers again: the generic webhook URL changed.", ex.Message);
    }

    [Fact]
    public void ALoneSurrogate_IsRefusedBeforeSealing()
    {
        using var key = PasswordPrivateKey.Generate();
        var row = new NotificationRow { SlackUrl = "https://example.test/\ud800" };

        var ex = Assert.Throws<ViewerPasswordRefusedException>(
            () => ViewerWebhookSealing.ResolveSettingsRow(row, null, new ViewerPasswordSealer(key.PublicKey), null));

        Assert.Contains("cannot be stored", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WithoutAKey_ATypedValueIsRefusedWithTheReason()
    {
        var row = new NotificationRow { SlackUrl = "https://example.test/slack" };

        var ex = Assert.Throws<ViewerPasswordRefusedException>(() => ViewerWebhookSealing.ResolveSettingsRow(row, null, null, "No key here."));

        Assert.Equal("No key here.", ex.Message);
    }

    [Fact]
    public void ARouteValue_IsSealedForItsRowAndTheParentsProxy()
    {
        using var key = PasswordPrivateKey.Generate();
        var parent = new NotificationRow { SlackProxy = "http://proxy.example:8080" };
        var route = new NotificationRouteRow { SlackUrl = "https://example.test/slack-route", SmtpRecipients = "" };

        ViewerWebhookSealing.ResolveRoute(route, null, parent, 9, new ViewerPasswordSealer(key.PublicKey), null);

        Assert.Equal(
            "https://example.test/slack-route",
            PasswordSeal.Open(route.SlackUrl, key, PasswordBinding.ForWebhook("slack", "route:9", "http://proxy.example:8080", null)));
        Assert.Throws<PasswordSealException>(() => PasswordSeal.Open(route.SlackUrl, key, PasswordBinding.ForWebhook("slack", "route:8", "http://proxy.example:8080", null)));
    }

    [Fact]
    public void ARouteThatKeepsItsValue_NeedsNoKey()
    {
        using var key = PasswordPrivateKey.Generate();
        var stored = new NotificationRouteRow { RouteId = 4, TeamsUrl = Seal(key, TeamsUrl, "teams", "route:4") };
        var route = stored.Clone();
        route.TeamsUrl = ViewerWebhookSealing.CarryKept("", stored.TeamsUrl);

        Assert.False(ViewerWebhookSealing.NeedsKey(route, stored));
        ViewerWebhookSealing.ResolveRoute(route, stored, new NotificationRow(), 4, null, null);

        Assert.Equal(stored.TeamsUrl, route.TeamsUrl);
    }

    [Fact]
    public void AChangedProxy_WithARouteValueSealedForTheOldProxy_SaysSoOnSave()
    {
        using var key = PasswordPrivateKey.Generate();
        var stored = new NotificationRow { SlackProxy = "http://proxy.example:8080" };
        var row = new NotificationRow { SlackProxy = "http://other-proxy.example:3128" };
        var routes = new[]
        {
            new NotificationRouteRow { RouteId = 4, MetricMatch = "High CPU", SlackUrl = Seal(key, TeamsUrl, "slack", "route:4", "http://proxy.example:8080") },
        };

        Assert.True(ViewerWebhookSealing.RouteValuesNeedEnteringAgain(stored, row, routes));
        Assert.Equal(
            "Route webhook values were saved for the old proxy. Enter them again on each route.",
            ViewerWebhookSealing.RouteValuesNeedEnteringAgainText);
    }

    [Fact]
    public void AProxyChange_WithNoSealedRouteValueForThatChannel_SaysNothing()
    {
        using var key = PasswordPrivateKey.Generate();
        var stored = new NotificationRow { SlackProxy = "http://proxy.example:8080", TeamsProxy = "" };
        var row = new NotificationRow { SlackProxy = "http://other-proxy.example:3128", TeamsProxy = "" };
        var routes = new[]
        {
            /* A Teams value (its proxy did not change) and a legacy plaintext Slack value (not bound to a proxy). */
            new NotificationRouteRow { RouteId = 4, MetricMatch = "High CPU", TeamsUrl = Seal(key, TeamsUrl, "teams", "route:4") },
            new NotificationRouteRow { RouteId = 5, MetricMatch = "Low Memory", SlackUrl = "https://example.test/slack-legacy" },
        };

        Assert.False(ViewerWebhookSealing.RouteValuesNeedEnteringAgain(stored, row, routes));
        Assert.False(ViewerWebhookSealing.RouteValuesNeedEnteringAgain(null, row, routes));
        Assert.False(ViewerWebhookSealing.RouteValuesNeedEnteringAgain(stored, stored, routes));
    }

    [Fact]
    public void ABlankTestBox_ForASavedValue_SaysItCannotBeTested_AndATypedOrUnsavedOneGoesAhead()
    {
        using var key = PasswordPrivateKey.Generate();
        var saved = Seal(key, TeamsUrl, "teams", "notification");

        var text = ViewerWebhookSealing.CannotTestSavedValueText("", saved, "Teams webhook URL");

        Assert.Equal(
            "The Teams webhook URL is saved, and a saved value cannot be tested from here. Type the Teams webhook URL to test it.",
            text);
        Assert.DoesNotContain("sealed:", text, StringComparison.Ordinal);
        Assert.Null(ViewerWebhookSealing.CannotTestSavedValueText(TeamsUrl, saved, "Teams webhook URL"));
        Assert.Null(ViewerWebhookSealing.CannotTestSavedValueText("", "", "Teams webhook URL"));
        Assert.Null(ViewerWebhookSealing.CannotTestSavedValueText("", "https://example.test/legacy-plaintext", "Teams webhook URL"));
    }

    [Fact]
    public void NewGenericHeaders_WithABlankUrlBoxAndASavedUrl_AskForTheUrlAgain()
    {
        using var key = PasswordPrivateKey.Generate();
        var savedUrl = Seal(key, "https://example.test/generic-not-real", "generic", "notification");
        var savedHeaders = Seal(key, "{\"X-Key\":\"not-real\"}", "generic_headers", "notification", url: savedUrl);

        Assert.True(ViewerWebhookSealing.HeadersNeedUrlRetyped("{\"X-Key\":\"changed-not-real\"}", "", savedHeaders, savedUrl));
        Assert.True(ViewerWebhookSealing.HeadersNeedUrlRetyped("{\"X-Key\":\"new-not-real\"}", "  ", null, savedUrl));
        Assert.False(ViewerWebhookSealing.HeadersNeedUrlRetyped("{\"X-Key\":\"changed-not-real\"}", "https://example.test/generic-not-real", savedHeaders, savedUrl));
        Assert.False(ViewerWebhookSealing.HeadersNeedUrlRetyped("", "", savedHeaders, savedUrl));
        Assert.False(ViewerWebhookSealing.HeadersNeedUrlRetyped(ViewerWebhookSealing.ClearMarker, "", savedHeaders, savedUrl));
        Assert.False(ViewerWebhookSealing.HeadersNeedUrlRetyped("{\"X-Key\":\"new-not-real\"}", "", null, null));
        Assert.Equal("Type the generic webhook URL again to change its headers.", ViewerWebhookSealing.RetypeUrlForHeadersText);
    }
}
