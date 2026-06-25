using System;
using System.Linq;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Guards <see cref="AlertIncidentRenderer"/> (#1140): incidents project into <see cref="AlertContext.Details"/>
/// so the dedup fingerprint renders on every surface (Teams / Slack / email HTML + plaintext) via the
/// shared detail-item path, without touching any individual renderer.
/// </summary>
public class AlertIncidentRenderTests
{
    private static readonly AlertBranding Branding = new("Test Edition", null);

    [Fact]
    public void Apply_SetsIncidentsAndAppendsDetailWithoutDisturbingExistingOrder()
    {
        var ctx = new AlertContext();
        ctx.Details.Add(new AlertDetailItem { Heading = "Diagnosis" });

        AlertIncidentRenderer.Apply(ctx, new[] { new AlertIncident("key1", new[] { "SalesDB.dbo.Orders" }) });

        Assert.NotNull(ctx.Incidents);
        Assert.Single(ctx.Incidents!);
        Assert.Equal("Diagnosis", ctx.Details[0].Heading); // existing item untouched
        var incident = ctx.Details.Single(d => d.Heading == "Incident");
        Assert.Contains(incident.Fields, f => f.Label == "Dedup Key" && f.Value == "key1");
        Assert.Contains(incident.Fields, f => f.Label == "Involved Objects" && f.Value == "SalesDB.dbo.Orders");
    }

    [Fact]
    public void Apply_NoIncidents_IsNoOp()
    {
        var ctx = new AlertContext();
        AlertIncidentRenderer.Apply(ctx, null);
        AlertIncidentRenderer.Apply(ctx, Array.Empty<AlertIncident>());
        Assert.Null(ctx.Incidents);
        Assert.Empty(ctx.Details);
    }

    [Fact]
    public void Apply_MultipleIncidents_NumbersHeadingsShowsOccurrencesAndWaitRange()
    {
        var ctx = new AlertContext();
        AlertIncidentRenderer.Apply(ctx, new[]
        {
            new AlertIncident("k1", new[] { "db.dbo.A" }, OccurrenceCount: 8, WaitRange: "80-90 s"),
            new AlertIncident("k2", new[] { "db.dbo.B" })
        });

        var first = ctx.Details.Single(d => d.Heading == "Incident 1 of 2");
        var second = ctx.Details.Single(d => d.Heading == "Incident 2 of 2");
        Assert.Contains(first.Fields, f => f.Label == "Occurrences" && f.Value == "8");
        Assert.Contains(first.Fields, f => f.Label == "Wait Range" && f.Value == "80-90 s");
        Assert.DoesNotContain(second.Fields, f => f.Label == "Occurrences"); // count 1 is omitted
    }

    [Fact]
    public void Apply_UnresolvedObjects_RendersPlaceholder()
    {
        var ctx = new AlertContext();
        AlertIncidentRenderer.Apply(ctx, new[] { new AlertIncident("k", Array.Empty<string>()) });
        var incident = ctx.Details.Single(d => d.Heading == "Incident");
        Assert.Contains(incident.Fields, f => f.Label == "Involved Objects" && f.Value == "(unresolved)");
    }

    [Fact]
    public void DedupKey_RendersOnTeamsSlackAndBothEmailBodies()
    {
        var ctx = new AlertContext();
        AlertIncidentRenderer.Apply(ctx, new[] { new AlertIncident("fingerprint-abc", new[] { "SalesDB.dbo.Orders" }) });

        var teams = WebhookAlertService.BuildTeamsPayload("Blocking Detected", "S1", "8", "n/a", Branding, context: ctx);
        var slack = WebhookAlertService.BuildSlackPayload("Blocking Detected", "S1", "8", "n/a", Branding, context: ctx);
        var (html, plain) = EmailTemplateBuilder.BuildAlertEmail("Blocking Detected", "S1", "8", "n/a", 15, Branding, ctx);

        Assert.Contains("fingerprint-abc", teams);
        Assert.Contains("fingerprint-abc", slack);
        Assert.Contains("fingerprint-abc", html);
        Assert.Contains("fingerprint-abc", plain);
    }
}
