using System;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9611: Darling's Teams alert as an Adaptive Card. Darling has a triage page, so its card carries the link to it as an
/// <c>Action.OpenUrl</c> button, and its branding has no snooze footer. These tests read the payload the shared builder
/// makes with Darling's own branding.
/// </summary>
public class TeamsAdaptiveCardDarlingTests
{
    private static readonly DateTime Fired = new(2026, 11, 25, 12, 0, 0, DateTimeKind.Utc);

    private static string TriageUrl => TriageLink.Build("https://darling.example", "SQL01", "High CPU", Fired, "k")!;

    private static AlertContext External() => new() { External = new AlertExternalOrigin("billing", "queue-depth", 7) };

    [Fact]
    public void ABuiltInAlert_CarriesTheTriageLinkAsAnOpenUrlButton_AndNoSnoozeFooter()
    {
        var payload = WebhookAlertService.BuildTeamsPayload(
            "High CPU", "SQL01", "97%", "90%", DarlingAlertDeliverer.Branding,
            triageUrl: TriageUrl, detailText: "CPU at 97% for 5 minutes", nowUtc: Fired);
        var card = TeamsCard.Parse(payload);

        Assert.Equal("Performance Monitor Darling — SQL01", card.Subtitle);
        Assert.EndsWith(" — High CPU", card.Title, StringComparison.Ordinal);
        Assert.Equal(["Server", "Current Value", "Threshold", "Time (UTC)", "Time (Local)"], card.Facts.Select(f => f.Title));
        Assert.Equal(["CPU at 97% for 5 minutes"], card.Section("Details").Texts);
        Assert.Single(card.Sections);

        /* The button: one action, of the kind a webhook card supports, with exactly a type, a title and the link. */
        var action = Assert.Single(card.Content.GetProperty("actions").EnumerateArray());
        Assert.Equal(["type", "title", "url"], action.EnumerateObject().Select(p => p.Name));
        Assert.Equal(("Action.OpenUrl", "Open triage page", TriageUrl), card.Action);
        Assert.StartsWith("https://darling.example/", card.Action!.Value.Url, StringComparison.Ordinal);
        /* The link is the button's alone: no text of the card repeats it. */
        Assert.DoesNotContain(card.AllText, text => text.Contains("darling.example", StringComparison.Ordinal));
    }

    [Fact]
    public void AnExternalAlert_KeepsItsMarkAndItsLines_AndCarriesTheButtonToo()
    {
        var payload = WebhookAlertService.BuildTeamsPayload(
            "External:billing:queue-depth", "SQL01", "7", "", DarlingAlertDeliverer.Branding,
            context: External(), triageUrl: TriageUrl, detailText: "Nightly report\nfailed: 3\nretries left: 5", displayName: "Nightly report", nowUtc: Fired);
        var card = TeamsCard.Parse(payload);

        Assert.EndsWith(@" — \[External: billing\] Nightly report", card.Title, StringComparison.Ordinal);
        Assert.Equal(["Nightly report\n\nfailed: 3\n\nretries left: 5"], card.Section("Details").Texts);
        Assert.Equal(("Action.OpenUrl", "Open triage page", TriageUrl), card.Action);
    }

    [Fact]
    public void AResolution_IsGreen_WithNoThresholdFact_AndStillOpensTheTriagePage()
    {
        var payload = WebhookAlertService.BuildTeamsPayload(
            "High CPU Resolved", "SQL01", "41%", "", DarlingAlertDeliverer.Branding,
            triageUrl: TriageUrl, detailText: "Alert: High CPU", nowUtc: Fired);
        var card = TeamsCard.Parse(payload);

        Assert.Equal("good", card.Style);
        Assert.Contains("RESOLVED", card.Title, StringComparison.Ordinal);
        Assert.DoesNotContain(card.Facts, f => f.Title == "Threshold");
        Assert.Equal("Action.OpenUrl", card.Action!.Value.Type);
    }

    [Fact]
    public void WithNoTriageLink_TheCardHasNoActions()
    {
        var payload = WebhookAlertService.BuildTeamsPayload("High CPU", "SQL01", "97%", "90%", DarlingAlertDeliverer.Branding, nowUtc: Fired);

        Assert.Null(TeamsCard.Parse(payload).Action);
        Assert.DoesNotContain("actions", payload, StringComparison.Ordinal);
        Assert.Equal(1, payload.Split("null").Length - 1);
    }
}
