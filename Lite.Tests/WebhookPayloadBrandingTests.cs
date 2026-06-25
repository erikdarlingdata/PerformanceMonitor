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
}
