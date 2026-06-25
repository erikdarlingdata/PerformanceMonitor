using PerformanceMonitor.Notifications;
using PerformanceMonitorDashboard.Services;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// Render invariant for Plan E Stage E3a: the shared EmailTemplateBuilder, fed
/// Dashboard's wired AlertBranding (via EmailAlertService.Branding), must reproduce
/// Dashboard's pre-E3a alert email exactly — edition string "Performance Monitor
/// Dashboard" and NO snooze hint (Dashboard's branding carries a null hint, so the
/// snooze section is omitted from both the HTML and plain-text bodies).
/// </summary>
public class EmailTemplateBrandingTests
{
    [Fact]
    public void BuildAlertEmail_DashboardBranding_IncludesEditionAndOmitsSnoozeFromBothBodies()
    {
        var branding = EmailAlertService.Branding;
        Assert.Equal("Performance Monitor Dashboard", branding.EditionName);
        Assert.Null(branding.SnoozeHint);

        var (html, plain) = EmailTemplateBuilder.BuildAlertEmail(
            "High CPU", "TestServer", "95%", "90%", 15, branding);

        // Edition string present, Lite's absent.
        Assert.Contains("Performance Monitor Dashboard", html);
        Assert.DoesNotContain("Performance Monitor Lite", html);

        // No snooze hint in either body.
        Assert.DoesNotContain("To silence this alert", html);
        Assert.DoesNotContain("To silence this alert", plain);
    }
}
