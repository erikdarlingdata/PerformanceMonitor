using System;
using System.Text.Json;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The #2710 triage-page link: the pure URL builder, and the wiring that puts a computed link into every
/// webhook payload — and, just as load-bearing, OMITS it (leaving the payload shaped exactly as before)
/// whenever no base URL is configured, so alerts can never break because the knob is unset. Lite always
/// supplies an empty base, so these are also the pins that keep Lite's payloads link-free.
/// </summary>
public class TriageLinkTests
{
    private static readonly AlertBranding Branding = new("Test Edition", null);
    private static readonly DateTime Fired = new(2026, 8, 31, 8, 0, 0, DateTimeKind.Utc);

    /* ---------------- TriageLink.Build (pure) ---------------- */

    [Fact]
    public void Build_ComposesTheHashRoute_WithEscapedQueryValues()
    {
        var url = TriageLink.Build("http://box:5153", "SRV 01&x", "High CPU", Fired, "aa/bb+cc");

        Assert.Equal(
            "http://box:5153/#/triage?server=SRV%2001%26x&metric=High%20CPU&at=2026-08-31T08%3A00%3A00Z&dedup=aa%2Fbb%2Bcc",
            url);
    }

    [Fact]
    public void Build_TrimsATrailingSlash_SoTheRouteIsNotDoubled()
    {
        var url = TriageLink.Build("https://box:5153/", "S", "M", Fired);
        Assert.StartsWith("https://box:5153/#/triage?", url, StringComparison.Ordinal);
    }

    [Fact]
    public void Build_OmitsTheDedupPair_WhenTheAlertHasNoFingerprint()
    {
        var url = TriageLink.Build("http://box:5153", "S", "M", Fired, dedupKey: null);
        Assert.NotNull(url);
        Assert.DoesNotContain("dedup=", url, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("box:5153")]            // not absolute
    [InlineData("ftp://box:5153")]      // not http(s)
    [InlineData("not a url")]
    public void Build_ReturnsNull_ForAnUnsetOrInvalidBase(string? baseUrl)
    {
        Assert.Null(TriageLink.Build(baseUrl, "S", "M", Fired, "k"));
    }

    /* ---------------- per-channel wiring: link present when passed ---------------- */

    [Fact]
    public void TeamsPayload_CarriesAnOpenUriPotentialAction_WhenATriageUrlIsPassed()
    {
        var url = TriageLink.Build("http://box:5153", "SRV", "High CPU", Fired, "k")!;
        var payload = WebhookAlertService.BuildTeamsPayload("High CPU", "SRV", "97%", "90%", Branding, triageUrl: url);

        using var doc = JsonDocument.Parse(payload);
        var action = doc.RootElement.GetProperty("potentialAction")[0];
        Assert.Equal("OpenUri", action.GetProperty("@type").GetString());
        Assert.Equal(url, action.GetProperty("targets")[0].GetProperty("uri").GetString());
    }

    [Fact]
    public void SlackPayload_CarriesALinkButton_WhenATriageUrlIsPassed()
    {
        var url = TriageLink.Build("http://box:5153", "SRV", "High CPU", Fired, "k")!;
        var payload = WebhookAlertService.BuildSlackPayload("High CPU", "SRV", "97%", "90%", Branding, triageUrl: url);

        using var doc = JsonDocument.Parse(payload);
        var blocks = doc.RootElement.GetProperty("attachments")[0].GetProperty("blocks");
        var found = false;
        foreach (var block in blocks.EnumerateArray())
        {
            if (block.GetProperty("type").GetString() == "actions")
            {
                var button = block.GetProperty("elements")[0];
                Assert.Equal("button", button.GetProperty("type").GetString());
                Assert.Equal(url, button.GetProperty("url").GetString());
                found = true;
            }
        }

        Assert.True(found, "no actions block with the triage link button was found");
    }

    [Fact]
    public void PagerDutyPayload_CarriesTheLinkInLinksAndCustomDetails_WhenATriageUrlIsPassed()
    {
        var url = TriageLink.Build("http://box:5153", "SRV", "High CPU", Fired, "k")!;
        var payload = WebhookAlertService.BuildPagerDutyPayload(
            "High CPU", "SRV", "97%", "90%", Branding, "routing-key", triageUrl: url);

        using var doc = JsonDocument.Parse(payload);
        var link = doc.RootElement.GetProperty("links")[0];
        Assert.Equal(url, link.GetProperty("href").GetString());
        Assert.Equal(url,
            doc.RootElement.GetProperty("payload").GetProperty("custom_details").GetProperty("Triage").GetString());
    }

    [Fact]
    public void GenericPayload_TriageUrlToken_SubstitutesTheLink_AndEmptyWhenUnset()
    {
        const string template = """{"link": "{{triage_url}}"}""";
        var url = TriageLink.Build("http://box:5153", "SRV", "High CPU", Fired, "k")!;

        var withLink = WebhookAlertService.BuildGenericPayload(
            "High CPU", "SRV", "97%", "90%", Branding, bodyTemplate: template, triageUrl: url);
        Assert.Equal(url, JsonDocument.Parse(withLink).RootElement.GetProperty("link").GetString());

        /* Unset base ⇒ null triageUrl ⇒ the token renders empty and the template stays well-formed. */
        var without = WebhookAlertService.BuildGenericPayload(
            "High CPU", "SRV", "97%", "90%", Branding, bodyTemplate: template);
        Assert.Equal("", JsonDocument.Parse(without).RootElement.GetProperty("link").GetString());
    }

    /* ---------------- per-channel wiring: null keeps the pre-#2710 shape ---------------- */

    [Fact]
    public void AllChannelPayloads_OmitEveryLinkAffordance_WhenNoTriageUrlIsPassed()
    {
        var teams = WebhookAlertService.BuildTeamsPayload("High CPU", "SRV", "97%", "90%", Branding);
        Assert.False(JsonDocument.Parse(teams).RootElement.TryGetProperty("potentialAction", out _));

        var slack = WebhookAlertService.BuildSlackPayload("High CPU", "SRV", "97%", "90%", Branding);
        Assert.DoesNotContain("\"actions\"", slack, StringComparison.Ordinal);

        var pagerDuty = WebhookAlertService.BuildPagerDutyPayload("High CPU", "SRV", "97%", "90%", Branding, "rk");
        using var pdDoc = JsonDocument.Parse(pagerDuty);
        Assert.False(pdDoc.RootElement.TryGetProperty("links", out _));
        Assert.False(pdDoc.RootElement.GetProperty("payload").GetProperty("custom_details").TryGetProperty("Triage", out _));
    }
}
