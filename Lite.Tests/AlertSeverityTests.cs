using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Guards the runtime-graded severity for "Volume Free Space" (#1136). The metric used to be absent
/// from the <see cref="AlertSeverity"/> map and fell through to INFO-blue (the lowest tier) for every
/// breach; it now renders WARNING by default and CRITICAL when the alert site sets
/// <see cref="AlertContext.SeverityOverride"/> for a critically-low volume. The email body, Teams card,
/// and Slack sidebar all key severity off the shared map, so this one suite covers both apps' output.
/// </summary>
public class AlertSeverityTests
{
    private static readonly AlertBranding Branding = new("Test Edition", null);

    [Fact]
    public void VolumeFreeSpace_NoOverride_IsWarningNotInfo()
    {
        var (hex, badge, _) = AlertSeverity.ForMetric("Volume Free Space");
        Assert.Equal("WARNING", badge);
        Assert.Equal("#D97706", hex);
    }

    [Fact]
    public void VolumeFreeSpace_CriticalOverride_IsCritical()
    {
        var (hex, badge, _) = AlertSeverity.ForMetric("Volume Free Space", AlertSeverityLevel.Critical);
        Assert.Equal("CRITICAL", badge);
        Assert.Equal("#DC2626", hex);
    }

    [Fact]
    public void Override_IsAuthoritativeOverMetricMap()
    {
        // The override wins regardless of the metric's own mapped tier.
        var (_, badge, _) = AlertSeverity.ForMetric("High CPU", AlertSeverityLevel.Critical);
        Assert.Equal("CRITICAL", badge);
    }

    [Fact]
    public void EmailBody_CriticalOverride_RendersCriticalNotInfo()
    {
        var ctx = new AlertContext { SeverityOverride = AlertSeverityLevel.Critical };
        var (html, _) = EmailTemplateBuilder.BuildAlertEmail(
            "Volume Free Space", "S1", "E:\\ 2% free (1.0 GB)", "10% / 5 GB", 15, Branding, ctx);

        Assert.Contains("CRITICAL", html);
        Assert.Contains("#DC2626", html);
        Assert.DoesNotContain("INFO", html);
    }

    [Fact]
    public void EmailBody_NoOverride_RendersWarningNotInfo()
    {
        var (html, _) = EmailTemplateBuilder.BuildAlertEmail(
            "Volume Free Space", "S1", "E:\\ 8% free (40.0 GB)", "10% / 5 GB", 15, Branding, context: null);

        Assert.Contains("WARNING", html);
        Assert.DoesNotContain("INFO", html);
    }

    [Fact]
    public void TeamsPayload_CriticalOverride_UsesCriticalColorAndBadge()
    {
        var ctx = new AlertContext { SeverityOverride = AlertSeverityLevel.Critical };
        var payload = WebhookAlertService.BuildTeamsPayload(
            "Volume Free Space", "S1", "E:\\ 2% free (1.0 GB)", "10% / 5 GB", Branding, context: ctx);

        Assert.Contains("CRITICAL", payload);
        Assert.Contains("DC2626", payload); // themeColor renders without the leading '#'
    }

    [Fact]
    public void SlackPayload_CriticalOverride_UsesCriticalColor()
    {
        var ctx = new AlertContext { SeverityOverride = AlertSeverityLevel.Critical };
        var payload = WebhookAlertService.BuildSlackPayload(
            "Volume Free Space", "S1", "E:\\ 2% free (1.0 GB)", "10% / 5 GB", Branding, context: ctx);

        Assert.Contains("CRITICAL", payload);
        Assert.Contains("#DC2626", payload);
    }
}
