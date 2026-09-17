using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3303: the shared email/webhook render builders take an optional human display name (a custom alert
/// rule's name) that replaces the metric name in the VISIBLE subject/title/heading, while severity, dedup,
/// and cooldown keep keying on the metric name. A null/empty display name renders the metric name exactly as
/// before, so built-in alerts (which never pass one) are unchanged — the existing WebhookPayloadBranding /
/// AlertSeverity / EmailTemplateBranding suites pin that byte-for-byte; these add the present/absent cases.
/// </summary>
public class CustomAlertRenderTests
{
    private static AlertBranding Branding => EmailAlertService.Branding;

    /* A custom fire arrives with metric_name "Custom:<id>" plus a SeverityOverride context (the deliverer
       folds AlertOutcome.Severity into it), so ForMetric still resolves a real badge from the override. */
    private static AlertContext Warn => new() { SeverityOverride = AlertSeverityLevel.Warning };

    // ─────────────────────────── email ───────────────────────────

    [Fact]
    public void EmailBody_WithDisplayName_ShowsHumanName_NotTheCustomKey()
    {
        var (html, plain) = EmailTemplateBuilder.BuildAlertEmail(
            "Custom:42", "PROD01", "1500", ">= 1000", 15, Branding, Warn, displayName: "Signal wait % high");

        Assert.Contains("Signal wait % high", html);
        Assert.DoesNotContain("Custom:42", html);
        Assert.Contains("Signal wait % high", plain);
        Assert.DoesNotContain("Custom:42", plain);
    }

    [Fact]
    public void EmailBody_NoDisplayName_FallsBackToMetricName()
    {
        var (html, plain) = EmailTemplateBuilder.BuildAlertEmail(
            "High CPU", "PROD01", "95%", "90%", 15, Branding);

        Assert.Contains("High CPU", html);
        Assert.Contains("High CPU", plain);
    }

    [Fact]
    public void EmailBody_EmptyDisplayName_FallsBackToMetricName()
    {
        var (html, _) = EmailTemplateBuilder.BuildAlertEmail(
            "High CPU", "PROD01", "95%", "90%", 15, Branding, context: null, displayName: "");

        Assert.Contains("High CPU", html);
    }

    // ─────────────────────────── Teams ───────────────────────────

    [Fact]
    public void TeamsPayload_WithDisplayName_TitleShowsHumanName_NotTheCustomKey()
    {
        var payload = WebhookAlertService.BuildTeamsPayload(
            "Custom:42", "PROD01", "1500", ">= 1000", Branding, context: Warn, displayName: "Signal wait % high");

        Assert.Contains("Signal wait % high", payload);
        Assert.DoesNotContain("Custom:42", payload);
    }

    [Fact]
    public void TeamsPayload_NoDisplayName_TitleShowsMetricName()
    {
        var payload = WebhookAlertService.BuildTeamsPayload(
            "High CPU", "PROD01", "95%", "90%", Branding);

        Assert.Contains("High CPU", payload);
    }

    // ─────────────────────────── Slack ───────────────────────────

    [Fact]
    public void SlackPayload_WithDisplayName_TitleShowsHumanName_NotTheCustomKey()
    {
        var payload = WebhookAlertService.BuildSlackPayload(
            "Custom:42", "PROD01", "1500", ">= 1000", Branding, context: Warn, displayName: "Signal wait % high");

        Assert.Contains("Signal wait % high", payload);
        Assert.DoesNotContain("Custom:42", payload);
    }

    [Fact]
    public void SlackPayload_NoDisplayName_TitleShowsMetricName()
    {
        var payload = WebhookAlertService.BuildSlackPayload(
            "High CPU", "PROD01", "95%", "90%", Branding);

        Assert.Contains("High CPU", payload);
    }

    // ─────────────────────────── PagerDuty ───────────────────────────

    [Fact]
    public void PagerDutyPayload_WithDisplayName_SummaryShowsHumanName_ButDedupKeepsTheCustomKey()
    {
        var payload = WebhookAlertService.BuildPagerDutyPayload(
            "Custom:42", "PROD01", "1500", ">= 1000", Branding, "routing-key",
            context: Warn, displayName: "Signal wait % high");

        // The human-facing summary carries the name...
        Assert.Contains("Signal wait % high", payload);
        // ...but the dedup_key stays the immutable metric key so PagerDuty correlation is rename-safe.
        Assert.Contains("Custom:42", payload);
    }

    [Fact]
    public void PagerDutyPayload_NoDisplayName_SummaryShowsMetricName()
    {
        var payload = WebhookAlertService.BuildPagerDutyPayload(
            "High CPU", "PROD01", "95%", "90%", Branding, "routing-key");

        Assert.Contains("High CPU", payload);
    }
}
