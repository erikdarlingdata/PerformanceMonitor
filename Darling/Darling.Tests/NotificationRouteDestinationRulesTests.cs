/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>The save-time rules for a route's own destinations (#6231), shared by the Viewer's route window and the MCP route tools.</summary>
public sealed class NotificationRouteDestinationRulesTests
{
    [Theory]
    [InlineData("ops@example.com")]
    [InlineData("ops@example.com, dba@example.org")]
    [InlineData("ops@example.com,dba@example.org,")]
    [InlineData("")]
    [InlineData("   ")]
    public void PlainAddresses_AndABlankList_AreAccepted(string recipients) =>
        Assert.Null(NotificationRouteWriter.RecipientsRefusal(recipients));

    [Theory]
    [InlineData("not an address")]
    [InlineData("Ops Team <ops@example.com>")]
    [InlineData("ops@example.com; dba@example.org")]
    [InlineData("ops@")]
    public void ADisplayName_AGroupOrAMalformedAddress_IsRefused_WithoutEchoingTheValue(string recipients)
    {
        var refusal = NotificationRouteWriter.RecipientsRefusal(recipients);
        Assert.NotNull(refusal);
        Assert.DoesNotContain(recipients, refusal, StringComparison.Ordinal);
    }

    [Fact]
    public void MoreThanTheMostRecipients_IsRefused()
    {
        var many = string.Join(",", Enumerable.Range(0, NotificationRouteWriter.MaxRecipients + 1).Select(i => $"u{i}@example.com"));
        Assert.NotNull(NotificationRouteWriter.RecipientsRefusal(many));
    }

    [Fact]
    public void TheRecipientCountAndDomains_DescribeAListWithoutTheAddresses()
    {
        var (count, domains) = NotificationRouteWriter.DescribeRecipients("a@example.com, b@Example.com, c@example.org");
        Assert.Equal(3, count);
        Assert.Equal(["example.com", "example.org"], domains);
    }

    [Theory]
    [InlineData("http://169.254.169.254/latest/meta-data")]
    [InlineData("https://[fe80::1]/hook")]
    [InlineData("http://168.63.129.16/hook")]
    [InlineData("http://100.100.100.200/hook")]
    [InlineData("http://[fd00:ec2::254]/hook")]
    public void ALinkLocalOrCloudInstanceAddress_IsRefusedAtSave(string url)
    {
        var refusal = WebhookUrlShape.Refusal(url, "Slack webhook URL");
        Assert.NotNull(refusal);
        Assert.DoesNotContain("169.254.169.254", refusal, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("https://hooks.example.test/services/x")]
    [InlineData("http://127.0.0.1:8080/hook")]
    [InlineData("http://10.1.2.3/hook")]
    [InlineData("http://receiver.internal/hook")]
    public void AHostNameOrALoopbackOrPrivateAddress_IsAccepted(string url) =>
        Assert.Null(WebhookUrlShape.Refusal(url, "Slack webhook URL"));
}
