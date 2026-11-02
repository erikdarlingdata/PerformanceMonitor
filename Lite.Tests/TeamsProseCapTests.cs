/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text;
using System.Text.Json;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7202: a flat alert detail that names hundreds of databases must not push a Teams card past the connector's
/// ~28 KB ceiling, where the post is rejected again on every repeat.
/// </summary>
public class TeamsProseCapTests
{
    private static readonly AlertBranding Branding = new("Performance Monitor Lite", null);

    private static string DetailsText(string payload)
    {
        using var doc = JsonDocument.Parse(payload);
        return doc.RootElement.GetProperty("sections").EnumerateArray()
            .Single(s => s.TryGetProperty("activityTitle", out var t) && t.GetString() == "Details")
            .GetProperty("text").GetString()!;
    }

    private static string Build(string detail) =>
        WebhookAlertService.BuildTeamsPayload("Log Backup Overdue", "SQL01", "900 overdue", "older than 2 hours", Branding, detailText: detail);

    [Fact]
    public void AHugeDetail_IsCutToTheProseCeiling_AtASpace_AndSaysSo()
    {
        var names = string.Join(", ", Enumerable.Range(0, 3000).Select(i => $"Tenant_{i:D4} (12 days old)"));
        var payload = Build("The last log backup is older than 2 hours on 3000 database(s): " + names + ".");

        Assert.True(Encoding.UTF8.GetByteCount(payload) < 28_000, "the whole card stays under the connector ceiling");
        var text = DetailsText(payload);
        Assert.True(Encoding.UTF8.GetByteCount(text) <= WebhookAlertService.TeamsProseMaxBytes);
        Assert.Contains("cut to fit the Teams size limit", text, StringComparison.Ordinal);
        Assert.Contains("Tenant", text, StringComparison.Ordinal);
        Assert.DoesNotContain("2999", text, StringComparison.Ordinal);
        /* The cut lands between two entries, never inside "Tenant_0123 (12 days old)". */
        var kept = text[..text.IndexOf("\n\n_The rest", StringComparison.Ordinal)];
        Assert.EndsWith("days old)", kept.TrimEnd(',', ' '), StringComparison.Ordinal);
    }

    [Fact]
    public void ADetailInsideTheCeiling_IsSentAsItIs()
    {
        var text = DetailsText(Build("Short detail on two database(s): A (3 days old), B (4 days old)."));
        Assert.DoesNotContain("cut to fit", text, StringComparison.Ordinal);
        Assert.Contains("B (4 days old)", text, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCut_NeverLeavesAHalfEscapeOrASplitPair()
    {
        /* Escaped characters (& becomes &amp;, a backtick becomes \`) and emoji (surrogate pairs) all along the text. */
        var detail = string.Concat(Enumerable.Repeat("a&b `c` \U0001F600 ", 5000));
        var text = DetailsText(Build(detail));

        Assert.True(Encoding.UTF8.GetByteCount(text) <= WebhookAlertService.TeamsProseMaxBytes);
        var kept = text[..text.IndexOf("\n\n_The rest", StringComparison.Ordinal)];
        Assert.False(kept.EndsWith('\\'), "no trailing backslash escape");
        Assert.False(kept.LastIndexOf('&') > kept.LastIndexOf(';'), "no entity cut short");
        Assert.False(char.IsHighSurrogate(kept[^1]), "no lone high surrogate");
        Assert.Equal(0, (kept.Split("\\`").Length - 1) % 2);
    }

    [Fact]
    public void AHugeDetailWithNoBreak_IsDroppedForTheNotice_NotCutMidToken()
    {
        var text = DetailsText(Build(new string('x', 40_000)));
        Assert.StartsWith("\n\n_The rest", text, StringComparison.Ordinal);
        Assert.DoesNotContain("xxx", text, StringComparison.Ordinal);
    }
}
