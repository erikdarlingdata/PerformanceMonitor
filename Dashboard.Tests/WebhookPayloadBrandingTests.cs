using PerformanceMonitor.Notifications;
using PerformanceMonitorDashboard.Services;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// Identical-output invariant for Plan E Stage E3c: the shared WebhookAlertService payload
/// builders, fed Dashboard's wired AlertBranding (via EmailAlertService.Branding), must
/// reproduce Dashboard's pre-E3c Teams/Slack payloads — edition string "Performance Monitor
/// Dashboard" and NO snooze hint (Dashboard's AlertBranding.SnoozeHint is null).
/// </summary>
public class WebhookPayloadBrandingTests
{
    [Fact]
    public void BuildTeamsPayload_DashboardBranding_IncludesEditionAndOmitsSnooze()
    {
        var branding = EmailAlertService.Branding;
        Assert.Equal("Performance Monitor Dashboard", branding.EditionName);
        Assert.Null(branding.SnoozeHint);

        var payload = WebhookAlertService.BuildTeamsPayload(
            "High CPU", "TestServer", "95%", "90%", branding);

        Assert.Contains("Performance Monitor Dashboard", payload);
        Assert.DoesNotContain("Performance Monitor Lite", payload);
        // Lite's snooze hint must never appear under Dashboard branding.
        Assert.DoesNotContain("Manage Mute Rules", payload);
    }

    [Fact]
    public void BuildSlackPayload_DashboardBranding_IncludesEditionAndOmitsSnooze()
    {
        var branding = EmailAlertService.Branding;

        var payload = WebhookAlertService.BuildSlackPayload(
            "High CPU", "TestServer", "95%", "90%", branding);

        Assert.Contains("Sent by Performance Monitor Dashboard", payload);
        Assert.DoesNotContain("Manage Mute Rules", payload);
    }
}
