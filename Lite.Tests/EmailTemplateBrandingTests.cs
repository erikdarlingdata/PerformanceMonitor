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

    /* ---------------- #3297: the alert's prose detail reaches both bodies ---------------- */

    /// <summary>The #3296 alert, shortened: a self-alert's prose detail with no structured context at all.</summary>
    private const string Prose =
        "Store query_stats retention [1072] is HELD PAUSED by the rollup-coverage gate. The policy arms " +
        "ITSELF once its consumer covers everything raw holds, but it arms at STARTUP: run the " +
        "--backfill-rollups operator action, then RESTART the service.";

    /// <summary>
    /// #3297, the defect itself. A self-alert fires with <c>context: null</c>, so the Details section — which
    /// was gated on <see cref="AlertContext.Details"/>, a STRUCTURED collection — never rendered, and the
    /// delivered email was a metric name, a current value, a threshold and two timestamps with the remedy
    /// discarded. Reported from the field on #3296 by the only operator who has ever had email configured;
    /// there is no dogfooding path to this, so this assertion is the whole guard.
    /// <para>Both bodies, separately: they are built by two different methods
    /// (<c>BuildHtmlBody</c> / <c>BuildPlainTextBody</c>) and a repair to one says nothing about the other.</para>
    /// </summary>
    [Fact]
    public void BuildAlertEmail_ProseDetail_ReachesBothBodies_OnAContextFreeSelfAlert()
    {
        var (html, plain) = EmailTemplateBuilder.BuildAlertEmail(
            "Retention Held", "Monitor Store", "9.6x its 4 days horizon", "2.0x", 15,
            EmailAlertService.Branding, context: null, detailText: Prose);

        Assert.Contains("DETAILS", html);
        Assert.Contains(WebUtility.HtmlEncode(Prose), html);

        Assert.Contains("--- Details ---", plain);
        Assert.Contains(Prose, plain);
    }

    /// <summary>
    /// The other half of the same gate: an alert with neither prose nor structured detail must still render
    /// no Details section, so the pre-#3297 email is unchanged wherever there was nothing to add.
    /// </summary>
    [Fact]
    public void BuildAlertEmail_NoProseAndNoContext_RendersNoDetailsSection()
    {
        var (html, plain) = EmailTemplateBuilder.BuildAlertEmail(
            "High CPU", "TestServer", "95%", "90%", 15, EmailAlertService.Branding);

        Assert.DoesNotContain("DETAILS", html);
        Assert.DoesNotContain("--- Details ---", plain);
    }

    /// <summary>
    /// The #2109 AG-database shape: a structured context AND independent prose naming the remedy. Suppressing
    /// the prose whenever a context is present would have dropped exactly this, which is why the gate compares
    /// the prose against the context's own flattening instead.
    /// </summary>
    [Fact]
    public void BuildAlertEmail_StructuredContextAndIndependentProse_RendersBoth()
    {
        var context = AgAlertContexts.ForDatabase("SalesDB", "ag-primary", "replica-1");

        var (html, plain) = EmailTemplateBuilder.BuildAlertEmail(
            "AG Database Suspended", "SRV", "Suspended", "Online", 15,
            EmailAlertService.Branding, context: context, detailText: "Resume it with ALTER DATABASE SET HADR RESUME.");

        Assert.Contains("SET HADR RESUME", html);
        Assert.Contains("Availability Group", html);
        Assert.Contains("SET HADR RESUME", plain);
        Assert.Contains("Availability Group", plain);
    }
}
