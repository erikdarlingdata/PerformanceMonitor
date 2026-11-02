/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Collections.Generic;
using System.Net.Mail;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7184: the default email recipients saved from the Viewer's and Lite's settings were never checked, so one entry the send
/// cannot parse (an Outlook-style <c>a@x.com;b@x.com</c>) made <c>MailAddressCollection.Add</c> throw and every email on the
/// default route failed, valid recipients included (<c>a@x.com; b@x.com</c> does not throw: it sends to <c>b@x.com</c> only). The settings windows now refuse such a list with
/// <see cref="EmailSendCore.RecipientsRefusal"/>, the one rule the route window and the MCP tools already use.
/// </summary>
public sealed class DefaultRecipientsRefusalTests
{
    [Theory]
    [InlineData("a@x.com; b@x.com")]
    [InlineData("a@x.com;b@x.com")]
    [InlineData("dba.corp.com")]
    [InlineData("a@x.com, dba.corp.com")]
    [InlineData("Ops Team <ops@example.com>")]
    public void AnEntryTheSendCannotParse_IsRefused_AndIsTheOneThatBreaksTheSend(string recipients)
    {
        var refusal = EmailSendCore.RecipientsRefusal(recipients);

        Assert.NotNull(refusal);
        Assert.DoesNotContain(recipients, refusal, StringComparison.Ordinal);
    }

    [Fact]
    public void ASemicolonWithASpace_IsRefused_BecauseTheSendKeepsOnlyTheSecondAddress()
    {
        using var message = new MailMessage();
        message.To.Add("a@x.com; b@x.com");

        // No throw, but one recipient: the first address became a display name and is never sent to.
        var recipient = Assert.Single(message.To);
        Assert.Equal("b@x.com", recipient.Address);
    }

    [Theory]
    [InlineData("a@x.com;b@x.com")]
    [InlineData("dba.corp.com")]
    public void TheListsTheSettingsRefuse_AreTheOnesTheSendThrowsOn(string recipients)
    {
        // The send adds each comma-split entry to MailMessage.To; this is the throw the refusal keeps off the default route.
        using var message = new MailMessage();
        Assert.Throws<FormatException>(() =>
        {
            foreach (var entry in recipients.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                message.To.Add(entry);
            }
        });
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("ops@example.com")]
    [InlineData("a@x.com, b@x.com")]
    [InlineData("a@x.com,b@x.com,")]
    public void BlankOrPlainAddressLists_AreAccepted_AndTheSendAddsThemAll(string recipients)
    {
        Assert.Null(EmailSendCore.RecipientsRefusal(recipients));

        using var message = new MailMessage();
        foreach (var entry in recipients.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            message.To.Add(entry);
        }
    }

    [Fact]
    public void MoreThanTheMostRecipients_IsRefused()
    {
        var many = string.Join(",", Enumerable.Range(0, EmailSendCore.MaxRecipients + 1).Select(i => $"u{i}@example.com"));
        var atMost = string.Join(",", Enumerable.Range(0, EmailSendCore.MaxRecipients).Select(i => $"u{i}@example.com"));

        Assert.NotNull(EmailSendCore.RecipientsRefusal(many));
        Assert.Null(EmailSendCore.RecipientsRefusal(atMost));
    }

    [Theory]
    [InlineData("a@x.com; b@x.com")]
    [InlineData("a@x.com;b@x.com")]
    public async Task TheSend_RefusesABadStoredDefaultList_WithTheSettingsWindowsSentence(string stored)
    {
        // A list stored before the settings windows checked it: the send re-checks it and fails with the same sentence,
        // before any connection (the refusal is thrown ahead of SendMailAsync, so no server is needed).
        var error = await EmailSendCore.SendTestEmailAsync(new StubSettings(stored, "127.0.0.1", 1), new AlertBranding("Test", null));

        Assert.Equal(EmailSendCore.RecipientsRefusal(stored), error);
    }

    [Fact]
    public async Task TheSend_LetsAGoodStoredDefaultListReachTheSendStep()
    {
        // Port 0 is never connectable, so the send step fails on the connection, not on the recipient rule.
        var error = await EmailSendCore.SendTestEmailAsync(new StubSettings("a@x.com, b@x.com", "127.0.0.1", 0), new AlertBranding("Test", null));

        Assert.NotNull(error);
        Assert.NotEqual(EmailSendCore.RecipientsRefusal("a@x.com; b@x.com"), error);
        Assert.DoesNotContain("plain addresses", error, StringComparison.Ordinal);
    }

    private sealed class StubSettings(string recipients, string server, int port) : IAlertSettings
    {
        public bool SmtpEnabled => true;
        public string SmtpServer => server;
        public int SmtpPort => port;
        public bool SmtpUseSsl => false;
        public string SmtpUsername => "";
        public string SmtpFromAddress => "alerts@x.com";
        public string SmtpRecipients => recipients;
        public string? GetSmtpPassword() => null;
        public int EmailCooldownMinutes => 15;
        public bool TeamsWebhookEnabled => false;
        public string TeamsWebhookUrl => "";
        public string TeamsProxyAddress => "";
        public bool SlackWebhookEnabled => false;
        public string SlackWebhookUrl => "";
        public string SlackProxyAddress => "";
        public bool GenericWebhookEnabled => false;
        public string GenericWebhookUrl => "";
        public string GenericWebhookHeadersJson => "";
        public string GenericWebhookBodyTemplate => "";
        public string GenericWebhookProxyAddress => "";
        public bool PagerDutyEnabled => false;
        public string PagerDutyRoutingKey => "";
        public bool PagerDutyUseEuRegion => false;
        public string PagerDutyProxyAddress => "";
        public double AnalysisNotifySeverity => 1.5;
        public int AnalysisNotifyCooldownMinutes => 360;
        public int AnalysisPageCap => 5;
        public string TriageBaseUrl => "";
        public IReadOnlyList<NotificationRoute> NotificationRoutes { get; } = Array.Empty<NotificationRoute>();
    }
}
