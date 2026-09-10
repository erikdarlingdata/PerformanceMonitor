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
    public void TeamsPayload_EachFieldsItem_GetsItsOwnLabeledSection()
    {
        /* #2108: the association between an item's fields IS the item boundary — a flat fact list
           loses it. Each Fields-carrying detail item becomes its own MessageCard section, titled by
           its heading and carrying ONLY its own facts; advice prose stays folded into the lead
           section, because it is commentary on the whole alert. */
        var ctx = new AlertContext();
        ctx.Details.Add(new AlertDetailItem
        {
            Heading = "Deadlock 1 of 2",
            Fields = new() { ("Database", "SalesDB"), ("Dedup Key", "aaa") }
        });
        ctx.Details.Add(new AlertDetailItem
        {
            Heading = "Deadlock 2 of 2",
            Fields = new() { ("Database", "OtherDb"), ("Dedup Key", "bbb") }
        });
        ctx.Details.Add(new AlertDetailItem { Heading = "Check the graph", Body = "Advice prose." });

        var payload = WebhookAlertService.BuildTeamsPayload("Deadlocks Detected", "S1", "2", "n/a", Branding, context: ctx);

        using var doc = System.Text.Json.JsonDocument.Parse(payload);
        var sections = doc.RootElement.GetProperty("sections").EnumerateArray().ToList();
        /* lead + one per Fields item + snooze-hint text section (Branding has none here → 3). */
        Assert.Equal(3, sections.Count);

        var lead = sections[0];
        Assert.Contains("Deadlocks Detected", lead.GetProperty("activityTitle").GetString());
        Assert.Contains(lead.GetProperty("facts").EnumerateArray(),
            f => f.GetProperty("name").GetString() == "Advice");

        var first = sections[1];
        Assert.Equal("Deadlock 1 of 2", first.GetProperty("activityTitle").GetString());
        var firstFacts = first.GetProperty("facts").EnumerateArray().ToList();
        Assert.Equal(2, firstFacts.Count);
        Assert.Equal("SalesDB", firstFacts[0].GetProperty("value").GetString());
        Assert.Equal("aaa", firstFacts[1].GetProperty("value").GetString());

        var second = sections[2];
        Assert.Equal("Deadlock 2 of 2", second.GetProperty("activityTitle").GetString());
        Assert.Contains(second.GetProperty("facts").EnumerateArray(),
            f => f.GetProperty("value").GetString() == "bbb");
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

    /// <summary>
    /// #2710: the Datadog-parity resource_name/env tags — promoted to the LEAD section/fields
    /// (not just nested inside the per-incident "Incident" item #1140 already renders), so a
    /// reader/automation scanning only the top-level facts sees what the alert is about, matching
    /// Datadog's <c>resource_name:</c> tag. First incident wins on a multi-incident alert, same
    /// anchor <c>DerivePagerDutyDedupKey</c> already uses.
    /// </summary>
    [Fact]
    public void ResourceNameAndDatabase_PromotedToTopLevel_OnTeamsAndSlack()
    {
        var ctx = new AlertContext();
        AlertIncidentRenderer.Apply(ctx, new[]
        {
            new AlertIncident("k1", new[] { "SalesDB.dbo.Orders" }, Database: "SalesDB"),
            new AlertIncident("k2", new[] { "OtherDb.dbo.Widgets" }, Database: "OtherDb"),
        });

        var teams = WebhookAlertService.BuildTeamsPayload("Deadlocks Detected", "S1", "2", "n/a", Branding, context: ctx);
        using var teamsDoc = System.Text.Json.JsonDocument.Parse(teams);
        var leadFacts = teamsDoc.RootElement.GetProperty("sections")[0].GetProperty("facts").EnumerateArray().ToList();
        Assert.Contains(leadFacts, f => f.GetProperty("name").GetString() == "Resource" && f.GetProperty("value").GetString() == "SalesDB.dbo.Orders");
        Assert.Contains(leadFacts, f => f.GetProperty("name").GetString() == "Database" && f.GetProperty("value").GetString() == "SalesDB");

        var slack = WebhookAlertService.BuildSlackPayload("Deadlocks Detected", "S1", "2", "n/a", Branding, context: ctx);
        Assert.Contains("*Resource:*\\nSalesDB.dbo.Orders", slack);
        Assert.Contains("*Database:*\\nSalesDB", slack);
    }

    [Fact]
    public void ResourceNameAndDatabase_Absent_WhenAlertHasNoIncident()
    {
        var teams = WebhookAlertService.BuildTeamsPayload("High CPU", "S1", "97%", "90%", Branding);
        var slack = WebhookAlertService.BuildSlackPayload("High CPU", "S1", "97%", "90%", Branding);

        Assert.DoesNotContain("\"name\":\"Resource\"", teams);
        Assert.DoesNotContain("Resource:", slack);
    }

    /// <summary>
    /// Review catch (#2710): a caller can pass a blank display object through
    /// <see cref="AlertFingerprint.ForKey"/> without going through the object-filtering
    /// <see cref="AlertFingerprint.ForObjects"/> callers get — the joined resource_name must not
    /// render as a labeled-but-empty tag in that case, the exact failure the design otherwise avoids
    /// by omitting the tag entirely on a null.
    /// </summary>
    [Fact]
    public void ResourceName_Absent_WhenInvolvedObjectsIsBlank()
    {
        var ctx = new AlertContext();
        AlertIncidentRenderer.Apply(ctx, new[] { new AlertIncident("k", new[] { "" }) });

        var teams = WebhookAlertService.BuildTeamsPayload("Low Disk Space", "S1", "5%", "10%", Branding, context: ctx);
        Assert.DoesNotContain("\"name\":\"Resource\"", teams);
    }
}
