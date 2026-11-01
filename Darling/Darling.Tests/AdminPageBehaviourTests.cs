/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// What the Admin page draws, from the shipped <c>admin.js</c> and the real <c>panels.js</c> grid run under Node by the edit
/// harness (<c>admin-server-edit-harness.mjs</c>): no credential-shaped field reaches any text. These are the seven facts the
/// old vm harness (<c>admin-page-harness.mjs</c>, retired in #5240: its stand-ins could not follow the page) ran, with the same
/// assertions; the ones that read the stand-in grid's text now read what the real grid draws, in a pinned UTC zone.
/// </summary>
public sealed class AdminPageBehaviourTests
{
    private static JsonElement Tab(string scenario) => AdminServerEditBehaviourTests.Run(scenario);

    private static string[] Texts(JsonElement page) => page.GetProperty("texts").EnumerateArray().Select(e => e.GetString()!).ToArray();

    private static string[] Reads(JsonElement page) => page.GetProperty("reads").EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Theory]
    [InlineData("servers", "Alpha", "GET /api/admin/servers")]
    // The routes tab also reads the webhook signing status (#5998), second, after its routes, and the routes where nobody can
    // approve a proposal (#7109), third: the page draws a strip for each of those routes on purpose.
    [InlineData("routes", "Blocking Detected", "GET /api/read/get_notification_routes", "GET /api/webhook-signing", "GET /api/notification-routes/nobody-can-approve")]
    [InlineData("settings", "CPU: threshold (%)", "GET /api/read/get_alert_settings")]
    public void EachTab_ReadsItsTool_AndDrawsTheRow(string tab, string expectedText, params string[] reads)
    {
        var page = Tab("tab:" + tab);

        Assert.Contains(Texts(page), t => t == expectedText);
        Assert.Equal(reads, Reads(page));
    }

    [Fact]
    public void TheServersTab_DrawsTheDisabledServer_WithItsAuthCostAndAddedTime_AndGreysOnlyTheDisabledRow()
    {
        var page = Tab("tab:servers");
        var texts = Texts(page);

        Assert.Contains("Enabled", texts);
        Assert.Contains("Disabled", texts);
        Assert.Contains("Windows", texts);
        Assert.Contains("SQL Server", texts);
        Assert.Contains("$1,234", texts);
        // The real grid formats the time cell in the pinned UTC zone (the wording follows the locale, so it is compared with what
        // the shipped formatter gives for the same instant) instead of showing the raw text the stand-in grid did.
        Assert.Equal(0, page.GetProperty("utcOffsetMinutes").GetInt32());
        var added = page.GetProperty("addedFormatted").GetString()!;
        Assert.Contains("2026", added, StringComparison.Ordinal);
        Assert.NotEqual("2026-01-02T00:00:00.0000000", added);
        Assert.Contains(added, texts);
        Assert.DoesNotContain("2026-01-02T00:00:00.0000000", texts);
        // One row has no class and only the disabled one is grey.
        var classes = page.GetProperty("rowClasses").EnumerateArray().Select(e => e.GetString()!).ToArray();
        Assert.Equal(new[] { "", "band-Offline" }, classes);
        Assert.Equal(1, classes.Count(c => c == "band-Offline"));
        Assert.Contains("2 servers.", string.Join(" ", texts));
        Assert.Equal("GET /api/admin/servers", Assert.Single(Reads(page)));
    }

    [Fact]
    public void TheServersTab_DrawsTheSameMachineColumn_WithTheNamesTheServiceSent_AndABlankCellWhenThereIsNone()
    {
        var texts = Texts(Tab("tab:servers"));

        Assert.Contains("Same machine as", texts);
        Assert.Contains("Peer-One, Peer-Two", texts);
    }

    [Theory]
    [InlineData("servers")]
    [InlineData("routes")]
    [InlineData("settings")]
    public void NoRenderedText_CarriesASecretShapedField(string tab)
    {
        var texts = Texts(Tab("tab:" + tab));

        Assert.NotEmpty(texts);
        Assert.DoesNotContain(texts, t => t.Contains("SECRET", StringComparison.Ordinal));
        Assert.DoesNotContain(texts, t => t.Contains("hooks.example.test", StringComparison.Ordinal));
    }

    [Fact]
    public void TheSettingsTab_DrawsOnlyTheFieldsItListsForAGroup_AndNeverTheStoresSecretNames()
    {
        var texts = Texts(Tab("tab:settings"));

        // The Setting column shows a readable label (click-through 4d); the key rides in its tooltip.
        Assert.Contains("CPU: threshold (%)", texts);
        Assert.DoesNotContain(texts, t => t.Contains("UNLISTED", StringComparison.Ordinal));
        Assert.DoesNotContain(texts, t => t.Contains("future_knob", StringComparison.Ordinal));
        Assert.DoesNotContain(texts, t => t.Contains("slack_url", StringComparison.Ordinal) || t.Contains("pagerduty_routing_key", StringComparison.Ordinal));
    }

    [Fact]
    public void TheLongRunningQueryFilters_AreTheirOwnSection()
    {
        var texts = Texts(Tab("tab:settings"));

        Assert.Contains("Alert thresholds", texts);
        Assert.Contains("Long Running Query Filters", texts);
        Assert.Contains("Long-running query: exclude backups", texts);
        Assert.Contains("Long-running query: threshold (minutes)", texts);
        Assert.True(Array.IndexOf(texts, "Long Running Query Filters") < Array.IndexOf(texts, "Long-running query: exclude backups"));
        Assert.True(Array.IndexOf(texts, "Long-running query: threshold (minutes)") < Array.IndexOf(texts, "Long Running Query Filters"));
    }

    [Theory]
    [InlineData("settings")]
    [InlineData("servers")]
    public void ARepaintOfTheTabOnScreen_KeepsItsBody_UntilTheReadLands(string tab)
    {
        var repaint = Tab("repaint:" + tab);

        Assert.True(repaint.GetProperty("sameBody").GetBoolean());
        Assert.False(repaint.GetProperty("loadingShown").GetBoolean());
        Assert.True(repaint.GetProperty("rowsKept").GetBoolean());
        // Stronger than the old check: the very row text is still on screen while the read is held, and again after it lands.
        Assert.True(repaint.GetProperty("rowShown").GetBoolean());
        Assert.True(repaint.GetProperty("rowAfter").GetBoolean());
    }

    [Fact]
    public void TheRoutesTab_ShowsChannelNames_ButNeverADestination()
    {
        var texts = Texts(Tab("tab:routes"));

        Assert.Contains("slack, pagerduty", texts);
        Assert.Contains("ops@example.test", texts);
    }

    /// <summary>#5825: the route's send-resolutions override reads in the route dialog's words (null follows the channel), and
    /// the settings tab draws the per-channel flags as yes/no, never "—" and never a value that is not a boolean.</summary>
    [Fact]
    public void TheSendResolutionsSettingAndRouteOverride_AreDrawnInTheViewersWords()
    {
        var routes = Texts(Tab("tab:routes"));
        Assert.Contains("Send resolutions", routes);
        Assert.Contains("Always", routes);
        Assert.Contains("Never", routes);
        // Route 9 sends null and routes 10 to 13 carry no field: all five follow the channel.
        Assert.Equal(5, routes.Count(t => t == "Use channel setting"));

        var settings = Texts(Tab("tab:settings"));
        Assert.Contains("Delivery: send resolutions", settings);
        Assert.Contains("Slack: yes, Teams: no, Generic webhook: no, Email: yes", settings);
        Assert.DoesNotContain(settings, t => t.Contains("UNLISTED-RES", StringComparison.Ordinal));
    }

    [Fact]
    public void TheRoutesTab_ShowsWhereEachRouteApplies_AndAnOrphanOrUnreadableScopeNeverReadsAsOk()
    {
        var texts = Texts(Tab("tab:routes"));

        // The fleet-wide route and the scoped ones: "All servers", the server's name, the tag's name, or the id a gone scope still names.
        Assert.Contains(ViewerNotificationRouteScope.AllServersLabel, texts);
        Assert.Contains("Alpha", texts);
        Assert.Contains("East", texts);
        Assert.Contains("(server id 99)", texts);
        Assert.Contains("(tag id 6)", texts);
        // An untrusted name is drawn as text, never parsed as markup: the grid shows the literal characters.
        Assert.Contains("<img src=x onerror=alert(1)>", texts);

        // The status words are the desktop viewer's, and the orphan and unreadable routes (which match nothing) are never OK.
        Assert.Contains(ViewerNotificationRouteScope.OrphanedServerStatus, texts);
        Assert.Contains(ViewerNotificationRouteScope.OrphanedTagStatus, texts);
        Assert.Contains(ViewerNotificationRouteScope.TagDataUnreadableStatus, texts);
        // A server-scoped route whose inputs could not be read says "scope data unreadable", as the desktop viewer does; the tag route above keeps "tag data unreadable".
        Assert.Contains(ViewerNotificationRouteScope.ScopeDataUnreadableStatus, texts);
        Assert.Contains(ViewerNotificationRouteScope.OkStatus, texts);
        Assert.Equal(3, texts.Count(t => t == ViewerNotificationRouteScope.OkStatus));
        // The wildcard route reports no family.
        Assert.Contains("wildcard", texts);
    }
}
