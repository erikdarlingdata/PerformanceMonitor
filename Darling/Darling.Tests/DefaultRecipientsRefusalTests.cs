/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Net.Mail;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7184: the default email recipients saved from the Viewer's and Lite's settings were never checked, so one entry the send
/// cannot parse (an Outlook-style <c>a@x.com; b@x.com</c>) made <c>MailAddressCollection.Add</c> throw and every email on the
/// default route failed, valid recipients included. The settings windows now refuse such a list with
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

    [Theory]
    [InlineData("a@x.com; b@x.com")]
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
}
