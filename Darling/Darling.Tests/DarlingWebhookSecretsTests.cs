/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #5366: the service opens the webhook URLs, the PagerDuty routing key and the generic headers that the store holds
/// sealed. A value opens only in the slot, row, proxy and (for the headers) generic URL it was sealed for. A value that
/// will not open turns its channel off and says so in the channel's health text, without the value in it.
/// </summary>
public sealed class DarlingWebhookSecretsTests
{
    private const string TeamsUrl = "https://example.test/teams-not-real";
    private const string GenericUrl = "https://example.test/generic-not-real";
    private const string Headers = "{\"Authorization\":\"Bearer not-real\"}";
    private const string Proxy = "http://proxy.example:8080";

    private static string Seal(PasswordPrivateKey key, string value, string slot, string row, string proxy = "", string url = "") =>
        PasswordSeal.Seal(value, key.PublicKey, PasswordBinding.ForWebhook(slot, row, proxy, url));

    private static DarlingAlertSettings Settings(DarlingConfig config, PasswordPrivateKey? key) =>
        new(config, null, key is null ? DarlingPasswordKey.Refusing("The password key is not available here.") : DarlingPasswordKey.FromPrivateKey(key));

    [Fact]
    public void SealedValuesOnTheSettingsRow_OpenForTheirSlot()
    {
        using var key = PasswordPrivateKey.Generate();
        var generic = Seal(key, GenericUrl, "generic", "notification", Proxy);
        var config = new DarlingConfig();
        config.Webhooks.TeamsUrl = Seal(key, TeamsUrl, "teams", "notification");
        config.Webhooks.GenericUrl = generic;
        config.Webhooks.GenericProxy = Proxy;
        config.Webhooks.GenericHeaders = Seal(key, Headers, "generic_headers", "notification", Proxy, generic);
        config.Webhooks.PagerDutyRoutingKey = Seal(key, "pd-key-not-real", "pagerduty", "notification");

        var settings = Settings(config, key);

        Assert.True(settings.TeamsWebhookEnabled);
        Assert.Equal(TeamsUrl, settings.TeamsWebhookUrl);
        Assert.Equal(GenericUrl, settings.GenericWebhookUrl);
        Assert.Equal(Headers, settings.GenericWebhookHeadersJson);
        Assert.Equal("pd-key-not-real", settings.PagerDutyRoutingKey);
        Assert.False(settings.SlackWebhookEnabled);
        Assert.Empty(settings.WebhookChannelProblems);
    }

    [Fact]
    public void LegacyPlaintextValues_StillRead_EvenWhenTheKeyIsNotAvailable()
    {
        var config = new DarlingConfig();
        config.Webhooks.SlackUrl = "https://example.test/slack-not-real";
        config.NotificationRoutes = new[] { new NotificationRoute(1, "High CPU", "https://example.test/route-teams", "", "", "", "", true) };

        var settings = Settings(config, null);

        Assert.Equal("https://example.test/slack-not-real", settings.SlackWebhookUrl);
        Assert.Equal("https://example.test/route-teams", settings.NotificationRoutes.Single().TeamsUrl);
        Assert.Empty(settings.WebhookChannelProblems);
    }

    [Fact]
    public void ASealedTeamsUrlCopiedToARoute_DoesNotOpen_AndTheRouteSendsNothingToTeams()
    {
        using var key = PasswordPrivateKey.Generate();
        var sealedForSettings = Seal(key, TeamsUrl, "teams", "notification");
        var config = new DarlingConfig();
        config.Webhooks.TeamsUrl = sealedForSettings;
        config.NotificationRoutes = new[] { new NotificationRoute(3, "High CPU", sealedForSettings, "", "", "", "", true) };

        var settings = Settings(config, key);

        Assert.Equal(TeamsUrl, settings.TeamsWebhookUrl);
        Assert.Equal("", settings.NotificationRoutes.Single().TeamsUrl);
        /* The route's own Teams destination is off: the alert is not sent to the settings row's Teams URL instead (#5366). */
        var decision = NotificationRouter.Resolve("High CPU", settings.NotificationRoutes, settings);
        Assert.False(decision.Teams.IsDelivered);
        Assert.Null(decision.Teams.Destination);
        var problem = Assert.Single(settings.WebhookChannelProblems);
        Assert.Equal(
            "The Teams webhook URL of route 3 could not be opened: it was saved for a different place or proxy, or was changed. Enter it again in Settings.",
            problem);
        Assert.DoesNotContain("example.test", problem, StringComparison.Ordinal);
    }

    [Fact]
    public void ARouteValueThatWillNotOpen_TurnsOffOnlyThatChannelOfThatRoute()
    {
        using var key = PasswordPrivateKey.Generate();
        var config = new DarlingConfig();
        config.Webhooks.TeamsUrl = Seal(key, TeamsUrl, "teams", "notification");
        config.Webhooks.SlackUrl = "https://example.test/slack-parent-not-real";
        config.NotificationRoutes = new[]
        {
            new NotificationRoute(3, "High CPU", Seal(key, TeamsUrl, "teams", "notification"), "", "", "", "", true),
            new NotificationRoute(4, "Low Memory", "", "https://example.test/slack-route-not-real", "", "", "", true),
        };

        var settings = Settings(config, key);

        var off = NotificationRouter.Resolve("High CPU", settings.NotificationRoutes, settings);
        Assert.False(off.Teams.IsDelivered);
        /* A channel the route never set still inherits the parent. */
        Assert.Equal("https://example.test/slack-parent-not-real", off.Slack.Destination);
        /* The other route is unaffected, and a metric the broken route does not match still reaches the parent. */
        Assert.Equal("https://example.test/slack-route-not-real", NotificationRouter.Resolve("Low Memory", settings.NotificationRoutes, settings).Slack.Destination);
        Assert.Equal(TeamsUrl, NotificationRouter.Resolve("Low Memory", settings.NotificationRoutes, settings).Teams.Destination);
    }

    [Fact]
    public void ARouteValueThatWillNotOpen_IsLoggedOnce_AcrossRepeatedReads()
    {
        using var key = PasswordPrivateKey.Generate();
        var config = new DarlingConfig();
        config.NotificationRoutes = new[]
        {
            new NotificationRoute(3, "High CPU", Seal(key, TeamsUrl, "teams", "notification"), "", "", "", "", true),
        };
        var logger = new CapturingLogger();
        var settings = new DarlingAlertSettings(config, logger, DarlingPasswordKey.FromPrivateKey(key));

        _ = settings.NotificationRoutes;
        _ = settings.NotificationRoutes;
        _ = settings.WebhookChannelProblems;

        Assert.Equal(1, logger.Errors.Count(m => m.Contains("route 3", StringComparison.Ordinal)));
    }

    private sealed class CapturingLogger : Microsoft.Extensions.Logging.ILogger
    {
        public System.Collections.Generic.List<string> Errors { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == Microsoft.Extensions.Logging.LogLevel.Error)
            {
                Errors.Add(formatter(state, exception));
            }
        }
    }

    [Fact]
    public void ARouteValueSealedForItsOwnRow_Opens()
    {
        using var key = PasswordPrivateKey.Generate();
        var config = new DarlingConfig();
        config.Webhooks.SlackProxy = Proxy;
        config.NotificationRoutes = new[]
        {
            new NotificationRoute(5, "High CPU", "", Seal(key, "https://example.test/slack-route", "slack", "route:5", Proxy), "", "", "", true),
        };

        var settings = Settings(config, key);

        Assert.Equal("https://example.test/slack-route", settings.NotificationRoutes.Single().SlackUrl);
        Assert.Empty(settings.WebhookChannelProblems);
    }

    [Fact]
    public void TheGenericHeaders_StopOpening_WhenTheGenericUrlChanges()
    {
        using var key = PasswordPrivateKey.Generate();
        var firstUrl = Seal(key, GenericUrl, "generic", "notification");
        var headers = Seal(key, Headers, "generic_headers", "notification", "", firstUrl);
        var config = new DarlingConfig();
        config.Webhooks.GenericUrl = Seal(key, "https://example.test/other-not-real", "generic", "notification");
        config.Webhooks.GenericHeaders = headers;
        config.NotificationRoutes = new[] { new NotificationRoute(2, "High CPU", "", "", "https://example.test/route-generic", "", "", true) };

        var settings = Settings(config, key);

        Assert.False(settings.GenericWebhookEnabled);
        Assert.Equal("", settings.GenericWebhookUrl);
        Assert.Equal("", settings.GenericWebhookHeadersJson);
        Assert.Equal("", settings.NotificationRoutes.Single().GenericUrl);
        Assert.Equal(
            "The generic webhook headers could not be opened: it was saved for a different place or proxy, or was changed. Enter it again in Settings.",
            Assert.Single(settings.WebhookChannelProblems));
    }

    [Fact]
    public void AChangedProxy_NeedsTheValueAgain()
    {
        using var key = PasswordPrivateKey.Generate();
        var config = new DarlingConfig();
        config.Webhooks.TeamsUrl = Seal(key, TeamsUrl, "teams", "notification", Proxy);
        config.Webhooks.TeamsProxy = "http://other-proxy.example:3128";

        var settings = Settings(config, key);

        Assert.False(settings.TeamsWebhookEnabled);
        Assert.StartsWith("The Teams webhook URL could not be opened: ", Assert.Single(settings.WebhookChannelProblems), StringComparison.Ordinal);
    }

    [Fact]
    public void AValueSealedToAnotherKey_TurnsTheChannelOffAndNamesTheKey()
    {
        using var other = PasswordPrivateKey.Generate();
        using var key = PasswordPrivateKey.Generate();
        var config = new DarlingConfig();
        config.Webhooks.PagerDutyRoutingKey = Seal(other, "pd-key-not-real", "pagerduty", "notification");

        var settings = Settings(config, key);

        Assert.False(settings.PagerDutyEnabled);
        var problem = Assert.Single(settings.WebhookChannelProblems);
        Assert.StartsWith("The PagerDuty routing key could not be opened: it was sealed to password key ", problem, StringComparison.Ordinal);
        Assert.EndsWith(", which this service does not have. Enter it again in Settings.", problem, StringComparison.Ordinal);
    }

    [Fact]
    public void WithoutAKey_ASealedValueTurnsItsChannelOff_AndTheReasonIsTheKeysOwn()
    {
        using var key = PasswordPrivateKey.Generate();
        var config = new DarlingConfig();
        config.Webhooks.SlackUrl = Seal(key, "https://example.test/slack-not-real", "slack", "notification");

        var settings = Settings(config, null);

        Assert.False(settings.SlackWebhookEnabled);
        Assert.Equal(
            "The Slack webhook URL could not be opened: The password key is not available here.",
            Assert.Single(settings.WebhookChannelProblems));
    }

    [Fact]
    public void TheOpenedValues_FollowAChangeToTheStoredSettings()
    {
        using var key = PasswordPrivateKey.Generate();
        var config = new DarlingConfig();
        var settings = Settings(config, key);
        Assert.False(settings.TeamsWebhookEnabled);

        config.Webhooks.TeamsUrl = Seal(key, TeamsUrl, "teams", "notification");

        Assert.True(settings.TeamsWebhookEnabled);
        Assert.Equal(TeamsUrl, settings.TeamsWebhookUrl);
    }

    [Fact]
    public void AnEmptyRoutesListAndWebhookSettings_OpenToNothing()
    {
        using var key = PasswordPrivateKey.Generate();
        var opened = DarlingWebhookSecrets.Open(new WebhooksConfig(), Array.Empty<NotificationRoute>(), DarlingPasswordKey.FromPrivateKey(key));

        Assert.Empty(opened.Failures);
        Assert.Empty(opened.Routes);
        Assert.Equal("", opened.Webhooks.TeamsUrl);
    }
}
