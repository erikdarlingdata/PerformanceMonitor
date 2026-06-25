using System.Net;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Render invariant for Plan E Stage E3a: the shared EmailTemplateBuilder, fed
/// Lite's wired AlertBranding (via EmailAlertService.Branding), must reproduce
/// Lite's pre-E3a alert email exactly — edition string "Performance Monitor Lite"
/// and the snooze hint present in BOTH the HTML and plain-text bodies.
/// </summary>
public class EmailTemplateBrandingTests
{
    [Fact]
    public void BuildAlertEmail_LiteBranding_IncludesEditionAndSnoozeInBothBodies()
    {
        var branding = EmailAlertService.Branding;
        Assert.Equal("Performance Monitor Lite", branding.EditionName);
        Assert.NotNull(branding.SnoozeHint);

        var (html, plain) = EmailTemplateBuilder.BuildAlertEmail(
            "High CPU", "TestServer", "95%", "90%", 15, branding);

        // Edition string present, Dashboard's absent.
        Assert.Contains("Performance Monitor Lite", html);
        Assert.DoesNotContain("Performance Monitor Dashboard", html);

        // Snooze hint present in BOTH bodies (HTML is encoded; plain text is raw).
        Assert.Contains(WebUtility.HtmlEncode(branding.SnoozeHint), html);
        Assert.Contains(branding.SnoozeHint!, plain);
    }
}
