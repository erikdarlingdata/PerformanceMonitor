using System;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Identical-output invariant for Plan E Stage E3c: the shared WebhookAlertService payload
/// builders, fed Lite's wired AlertBranding (via EmailAlertService.Branding), must reproduce
/// Lite's pre-E3c Teams/Slack payloads — edition string "Performance Monitor Lite" and the
/// snooze hint present (Lite has a SnoozeHint; Dashboard does not).
/// </summary>
public class WebhookPayloadBrandingTests
{
    [Fact]
    public void BuildTeamsPayload_LiteBranding_IncludesEditionAndSnooze()
    {
        var branding = EmailAlertService.Branding;
        Assert.Equal("Performance Monitor Lite", branding.EditionName);
        Assert.NotNull(branding.SnoozeHint);

        var payload = WebhookAlertService.BuildTeamsPayload(
            "High CPU", "TestServer", "95%", "90%", branding);

        Assert.Contains("Performance Monitor Lite", payload);
        Assert.DoesNotContain("Performance Monitor Dashboard", payload);
        // ASCII tail of the snooze hint — robust to System.Text.Json escaping the "→" chars.
        Assert.Contains("Manage Mute Rules", payload);
    }

    [Fact]
    public void BuildSlackPayload_LiteBranding_IncludesEditionAndSnooze()
    {
        var branding = EmailAlertService.Branding;

        var payload = WebhookAlertService.BuildSlackPayload(
            "High CPU", "TestServer", "95%", "90%", branding);

        Assert.Contains("Sent by Performance Monitor Lite", payload);
        Assert.Contains("Manage Mute Rules", payload);
    }

    [Fact]
    public void TestPayloads_OmitSnooze_EvenWithLiteBranding()
    {
        var branding = EmailAlertService.Branding;

        var teams = WebhookAlertService.BuildTeamsPayload("Test Notification", "", "Webhook configuration verified", "", branding, isTest: true);
        var slack = WebhookAlertService.BuildSlackPayload("Test Notification", "", "Webhook configuration verified", "", branding, isTest: true);

        // The snooze section is alert-only; test notifications never render it.
        Assert.DoesNotContain("Manage Mute Rules", teams);
        Assert.DoesNotContain("Manage Mute Rules", slack);
    }

    /* ---------------- #3297: the alert's prose detail reaches the webhook payloads ---------------- */

    private const string Prose =
        "Store query_stats retention [1072] is HELD PAUSED by the rollup-coverage gate. Run the " +
        "--backfill-rollups operator action, then RESTART the service.";

    /// <summary>
    /// #3297: <c>WebhookAlertService</c> held ZERO references to the alert's detail text across all of its
    /// channels, so a Teams card carried a metric name, a value and a threshold with the remedy discarded —
    /// the same defect #3296 reported by email. A MessageCard section with <c>text</c> rather than a fact,
    /// because a multi-paragraph remedy in a label/value fact renders as an unreadable column.
    /// </summary>
    [Fact]
    public void BuildTeamsPayload_ProseDetail_BecomesItsOwnSection()
    {
        var payload = WebhookAlertService.BuildTeamsPayload(
            "Retention Held", "Monitor Store", "9.6x its 4 days horizon", "2.0x",
            EmailAlertService.Branding, detailText: Prose);

        using var doc = JsonDocument.Parse(payload);
        var section = Assert.Single(
            doc.RootElement.GetProperty("sections").EnumerateArray().ToList(),
            s => s.TryGetProperty("activityTitle", out var t) && t.GetString() == "Details");

        Assert.Equal(Prose, section.GetProperty("text").GetString());
    }

    /// <summary>
    /// #3297 for Slack — the channel being stood up, and therefore the one where the first alert an operator
    /// ever sees would otherwise have had its remedy thrown away. A Block Kit <c>section</c> with mrkdwn text
    /// is the prose affordance; it sits under the field block and above the per-incident dividers.
    /// </summary>
    [Fact]
    public void BuildSlackPayload_ProseDetail_BecomesAMrkdwnSection()
    {
        var payload = WebhookAlertService.BuildSlackPayload(
            "Retention Held", "Monitor Store", "9.6x its 4 days horizon", "2.0x",
            EmailAlertService.Branding, detailText: Prose);

        using var doc = JsonDocument.Parse(payload);
        var carried = doc.RootElement.GetProperty("attachments")[0].GetProperty("blocks")
            .EnumerateArray()
            .Any(b => b.GetProperty("type").GetString() == "section" &&
                      b.TryGetProperty("text", out var t) &&
                      t.GetProperty("type").GetString() == "mrkdwn" &&
                      t.GetProperty("text").GetString()!.Contains(Prose, StringComparison.Ordinal));

        Assert.True(carried, "no mrkdwn section carried the alert's detail text");
    }

    /// <summary>
    /// Prose-free alerts keep the pre-#3297 payloads: no Details section on Teams, and no section block on
    /// Slack beyond the ones the structured context already produced.
    /// </summary>
    [Fact]
    public void ChannelPayloads_OmitTheDetailSection_WhenTheAlertCarriesNoProse()
    {
        var teams = WebhookAlertService.BuildTeamsPayload(
            "High CPU", "SRV", "97%", "90%", EmailAlertService.Branding);
        Assert.DoesNotContain(
            JsonDocument.Parse(teams).RootElement.GetProperty("sections").EnumerateArray().ToList(),
            s => s.TryGetProperty("activityTitle", out var t) && t.GetString() == "Details");

        var slack = WebhookAlertService.BuildSlackPayload(
            "High CPU", "SRV", "97%", "90%", EmailAlertService.Branding);
        Assert.DoesNotContain("*Details*", slack, StringComparison.Ordinal);
    }
}
