/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitorLite;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Behavioural-invariance tests for <see cref="AppAlertSettings"/> (Plan D §8).
/// The adapter must be a pure, live pass-through to the App.* statics — reading
/// the current value on every access with no caching/snapshotting — so that a
/// runtime settings reload (App.LoadAlertSettings) is reflected immediately,
/// exactly as the pre-refactor direct App.* reads behaved.
///
/// These tests deliberately exercise only the SMTP / webhook / cooldown statics.
/// They avoid App.AnalysisNotifySeverity / AnalysisNotifyCooldownMinutes because
/// AnalysisNotificationTests mutates those, and xUnit runs separate test classes
/// in parallel — touching the shared analysis statics here could flake that class.
/// The pass-through contract is type-uniform (every member is `=> App.X`), so the
/// SMTP/webhook coverage proves the invariant for the analysis members too.
/// </summary>
public class AppAlertSettingsTests
{
    /// <summary>
    /// Snapshot pass-through: set each App.* static to a sentinel, then assert the
    /// adapter returns that exact value for every member (bool / string / int).
    /// </summary>
    [Fact]
    public void Adapter_ReturnsLiveAppValues_ForEveryMember()
    {
        App.SmtpEnabled = true;
        App.SmtpServer = "smtp.sentinel.test";
        App.SmtpPort = 2525;
        App.SmtpUseSsl = false;
        App.SmtpUsername = "sentinel-user";
        App.SmtpFromAddress = "from@sentinel.test";
        App.SmtpRecipients = "to@sentinel.test";
        App.EmailCooldownMinutes = 42;
        App.TeamsWebhookEnabled = true;
        App.TeamsWebhookUrl = "https://teams.sentinel.test/hook";
        App.TeamsProxyAddress = "proxy-teams.sentinel.test";
        App.SlackWebhookEnabled = true;
        App.SlackWebhookUrl = "https://slack.sentinel.test/hook";
        App.SlackProxyAddress = "proxy-slack.sentinel.test";

        IAlertSettings adapter = new AppAlertSettings();

        Assert.Equal(App.SmtpEnabled, adapter.SmtpEnabled);
        Assert.Equal(App.SmtpServer, adapter.SmtpServer);
        Assert.Equal(App.SmtpPort, adapter.SmtpPort);
        Assert.Equal(App.SmtpUseSsl, adapter.SmtpUseSsl);
        Assert.Equal(App.SmtpUsername, adapter.SmtpUsername);
        Assert.Equal(App.SmtpFromAddress, adapter.SmtpFromAddress);
        Assert.Equal(App.SmtpRecipients, adapter.SmtpRecipients);
        Assert.Equal(App.EmailCooldownMinutes, adapter.EmailCooldownMinutes);
        Assert.Equal(App.TeamsWebhookEnabled, adapter.TeamsWebhookEnabled);
        Assert.Equal(App.TeamsWebhookUrl, adapter.TeamsWebhookUrl);
        Assert.Equal(App.TeamsProxyAddress, adapter.TeamsProxyAddress);
        Assert.Equal(App.SlackWebhookEnabled, adapter.SlackWebhookEnabled);
        Assert.Equal(App.SlackWebhookUrl, adapter.SlackWebhookUrl);
        Assert.Equal(App.SlackProxyAddress, adapter.SlackProxyAddress);
    }

    /// <summary>
    /// No caching: mutate App.* AFTER constructing the adapter and assert the
    /// adapter reflects the new value live. This is the load-bearing contract —
    /// App.* setters are public and written at runtime (tests + App.LoadAlertSettings),
    /// so a snapshotting adapter would diverge the moment any write happens.
    /// </summary>
    [Fact]
    public void Adapter_ReflectsMutationsAfterConstruction_NoCaching()
    {
        App.SmtpEnabled = false;
        App.SmtpServer = "before.sentinel.test";
        App.SmtpPort = 25;

        IAlertSettings adapter = new AppAlertSettings();

        /* Mutate after the adapter already exists. */
        App.SmtpEnabled = true;
        App.SmtpServer = "after.sentinel.test";
        App.SmtpPort = 587;

        Assert.True(adapter.SmtpEnabled);                       // bool, live
        Assert.Equal("after.sentinel.test", adapter.SmtpServer); // string, live
        Assert.Equal(587, adapter.SmtpPort);                     // numeric, live
    }

    /// <summary>
    /// GetSmtpPassword() delegates to App.GetSmtpPassword() (Credential Manager I/O).
    /// Asserts it does not throw and returns the same value App exposes, without
    /// requiring any stored credential state (both sides return null when unset).
    /// </summary>
    [Fact]
    public void GetSmtpPassword_DelegatesToApp()
    {
        IAlertSettings adapter = new AppAlertSettings();

        var fromApp = App.GetSmtpPassword();
        var fromAdapter = adapter.GetSmtpPassword();

        Assert.Equal(fromApp, fromAdapter);
    }
}
