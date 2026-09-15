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
    public void CaptureDown_IsCriticalNotInfo()
    {
        // Capture Down is emailed/webhooked and fires as an Error toast — it must not fall through
        // to INFO-blue like an unmapped metric (same gap class as Volume Free Space, #1136).
        var (hex, badge, _) = AlertSeverity.ForMetric("Capture Down");
        Assert.Equal("CRITICAL", badge);
        Assert.Equal("#DC2626", hex);
    }

    [Fact]
    public void FailedAgentJob_IsWarningNotInfo()
    {
        var (hex, badge, _) = AlertSeverity.ForMetric("Failed Agent Job");
        Assert.Equal("WARNING", badge);
        Assert.Equal("#D97706", hex);
    }

    /// <summary>#3443's one deliberate INFO arm. The collector-cost digest renders identically to the
    /// unmapped fall-through on purpose - it is a report to read, not a condition to act on - but its arm
    /// is DECLARED in the map so the next #1136/#2090-style sweep does not read the INFO rendering as an
    /// accident and promote it to WARNING. This pin is what fails if that promotion happens. The digest
    /// stays out of <see cref="EveryFiredMetricName_HasASeverityArm_NotTheInfoFallThrough"/>'s inventory
    /// for the same reason: that list asserts fired metrics DIFFER from the fall-through, and this one
    /// matches it by design.</summary>
    [Fact]
    public void CollectorCostDigest_IsInfoBlueOnPurpose()
    {
        var (hex, badge, _) = AlertSeverity.ForMetric("Collector Cost Digest");
        Assert.Equal("INFO", badge);
        Assert.Equal("#2eaef1", hex);
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

    /// <summary>
    /// The #1136 fall-through tripwire (#2090): every metric name any fire site uses must have an arm in
    /// <see cref="AlertSeverity.ForMetric"/>, or an alert-history replay (the renderer with no context)
    /// silently renders INFO-blue for a WARNING/CRITICAL condition. This gap SHIPPED four separate times
    /// (blocking wait, capture down, failed agent job, database state) and #2090 found six more — because
    /// nothing forced a new alert's author to visit the map. This inventory is that forcing function:
    /// firing a NEW metric name means adding it here, and adding it here fails until the map has its arm.
    /// </summary>
    [Fact]
    public void EveryFiredMetricName_HasASeverityArm_NotTheInfoFallThrough()
    {
        var infoFallThrough = AlertSeverity.ForMetric("no-such-metric-name");

        foreach (var metric in new[]
        {
            /* The alert engine's thresholds. */
            "Blocking Detected", "Blocking Wait Time", "Deadlocks Detected", "High CPU", "Poison Wait",
            "Long-Running Query", "tempdb Space", "Long-Running Job", "Failed Agent Job",
            "Database State", "Volume Free Space", "Version Store (PVS)",
            /* Connection-state family. */
            "Server Unreachable", "Server Restored",
            /* Availability Groups (#991). */
            "AG Failover", "AG Replica Disconnected", "AG Replica Reconnected",
            "AG Sync Fell Behind", "AG Database Suspended",
            /* Darling self-alerts (#2090's batch — all fire Critical at their sites). */
            "Capture Down", "Collection Stopped", "Agent Not Running",
            "Store Disk Pressure", "Store Runtime Upgrade", "Compression Job Stuck",
        })
        {
            var (hex, badge, _) = AlertSeverity.ForMetric(metric);
            Assert.False(
                hex == infoFallThrough.HexColor && badge == infoFallThrough.BadgeText,
                $"'{metric}' hits the INFO fall-through — add its arm to AlertSeverity.ForMetric " +
                "(and if this is a brand-new alert, its fire site should also pass an explicit severity).");
        }
    }

    /// <summary>#2090's root cause, pinned end-to-end: a self-alert-shaped send (context null at the fire
    /// site, severity on the outcome) must NOT render INFO once the deliverer folds the severity in.</summary>
    [Fact]
    public void SelfAlertSeverity_FoldedIntoContext_RendersCriticalNotInfo()
    {
        var ctx = new AlertContext();
        ctx.SeverityOverride ??= AlertSeverityLevel.Critical; /* the deliverer's #2090 fold */
        var payload = WebhookAlertService.BuildTeamsPayload(
            "Collection Stopped", "S1", "5 runs failing", "collecting", Branding, context: ctx);

        Assert.Contains("CRITICAL", payload);
        Assert.DoesNotContain("2eaef1", payload);
    }

}
