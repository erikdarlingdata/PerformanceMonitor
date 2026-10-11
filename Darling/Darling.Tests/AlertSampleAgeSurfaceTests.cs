/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5642: the newest-sample age line on the Darling surfaces. The engine stamps the carrier ONCE at fire time;
/// the deliverer persists it with the history row, and the web pages, the MCP <c>get_alert_history</c> read and
/// the Viewer's alert detail read it back from the persisted context and never measure it again.
/// </summary>
public sealed class AlertSampleAgeSurfaceTests
{
    private const string Line = "Newest CPU sample: 15:43:29 UTC, 5m 20s old";
    private const string LinePrefix = "Newest CPU sample:";

    private const string NotebookPath = "Darling/PerformanceMonitor.Darling.Service/AlertNotebookEndpoint.cs";
    private const string TriagePath = "Darling/PerformanceMonitor.Darling.Service/DarlingTriageEndpoint.cs";
    private const string McpPath = "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs";
    private const string AlertsJsPath = "Darling/PerformanceMonitor.Darling.Service/wwwroot/js/pages/alerts.js";
    private const string ViewerDetailPath = "Darling/PerformanceMonitor.Darling.Viewer/AlertDetailWindow.xaml.cs";
    private const string ViewerMainPath = "Darling/PerformanceMonitor.Darling.Viewer/MainWindow.xaml.cs";

    private static AlertNewestSample Sample() =>
        new("CPU", new DateTime(2026, 10, 11, 15, 43, 29, DateTimeKind.Utc), TimeSpan.FromSeconds(320));

    private static int Count(string haystack, string needle)
    {
        var n = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0;
             i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
            n++;
        return n;
    }

    private static DarlingAlertReader.AlertHistoryReadRow Row(string? contextJson, string? detailText = null) =>
        new(new DateTime(2026, 10, 11, 15, 48, 49, DateTimeKind.Utc), 1, "SRV1", "High CPU", 97, 90, true, "email",
            null, false, detailText, false, contextJson);

    private static string ContextJsonWith(AlertNewestSample? sample)
    {
        var ctx = new AlertContext { NewestSample = sample };
        var item = new AlertDetailItem { Heading = "Diagnosis" };
        item.Fields.Add(("Wait Type", "LCK_M_X"));
        ctx.Details.Add(item);
        return AlertContextSerializer.Serialize(ctx);
    }

    /* ---------------- the deliverer's history row and its PagerDuty post ---------------- */

    private static async Task<(AlertHistoryRecord Record, List<string> PagerDutyBodies)> DeliverAsync(
        AlertContext context, string? detailText)
    {
        var config = new DarlingConfig();
        config.Webhooks.PagerDutyRoutingKey = "rk-test";
        config.Smtp.Host = "";
        config.Smtp.To = "";

        var bodies = new List<string>();
        var settings = new DarlingAlertSettings(config);
        var history = new CapturingHistoryStore();
        var webhooks = new WebhookAlertService(
            settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        webhooks.PostPagerDutyAsyncOverride = (_, payload) =>
        {
            bodies.Add(payload);
            return Task.FromResult<string?>(null);
        };
        var deliverer = new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance);

        var outcome = new AlertOutcome(
            "13", "PROD01", "High CPU", "97", "90",
            Context: context, DetailText: detailText, NumericCurrentValue: 97, NumericThresholdValue: 90,
            Muted: false, Severity: AlertSeverityLevel.Critical);
        await deliverer.DeliverAsync(outcome, TestContext.Current.CancellationToken);

        return (Assert.Single(history.Records), bodies);
    }

    [Fact]
    public async Task TheDeliverer_PersistsTheLineOnce_WhenTheEngineBuiltTheTextWithoutIt()
    {
        var (record, bodies) = await DeliverAsync(
            new AlertContext { NewestSample = Sample() }, "CPU is at 97% over three readings.");

        Assert.Equal(1, Count(record.DetailText!, LinePrefix));
        Assert.EndsWith(Line, record.DetailText!, StringComparison.Ordinal);
        Assert.Equal(1, Count(Assert.Single(bodies), LinePrefix));
    }

    [Fact]
    public async Task TheDeliverer_PersistsTheLineOnce_WhenTheEngineBuiltTheTextWithIt()
    {
        var (record, bodies) = await DeliverAsync(
            new AlertContext { NewestSample = Sample() }, "CPU is at 97% over three readings.\n\n" + Line);

        Assert.Equal(1, Count(record.DetailText!, LinePrefix));
        Assert.Equal(1, Count(Assert.Single(bodies), LinePrefix));
    }

    [Fact]
    public async Task TheDeliverer_PersistsTheCarrierInTheHistoryRowsContextJson()
    {
        var (record, _) = await DeliverAsync(new AlertContext { NewestSample = Sample() }, "CPU is at 97%.");

        var read = AlertContextSerializer.TryReadNewestSample(record.ContextJson);
        Assert.Equal(Sample(), read);
    }

    [Fact]
    public async Task TheDeliverer_AddsNoLine_WhenTheCarrierIsNull()
    {
        var (record, bodies) = await DeliverAsync(new AlertContext(), "CPU is at 97%.");

        Assert.Equal("CPU is at 97%.", record.DetailText);
        Assert.DoesNotContain("sample:", Assert.Single(bodies));
        Assert.Null(AlertContextSerializer.TryReadNewestSample(record.ContextJson));
    }

    /* ---------------- the web: notebook and triage rows ---------------- */

    [Fact]
    public void TheNotebookRow_CarriesNewestSample_ReadFromThePersistedContext()
    {
        var node = AlertNotebookEndpoint.AlertRowNode(Row(ContextJsonWith(Sample())), incident: null);

        var sample = Assert.IsType<JsonObject>(node["newest_sample"]);
        Assert.Equal("CPU", (string?)sample["source"]);
        Assert.Equal("2026-10-11T15:43:29.0000000Z", (string?)sample["instant_utc"]);
        Assert.Equal(320d, (double?)sample["age_seconds"]);
        Assert.Equal(Line, (string?)sample["line"]);
    }

    [Fact]
    public void TheTriageRow_CarriesTheSameNewestSample()
    {
        var json = ContextJsonWith(Sample());
        var triage = DarlingTriageEndpoint.AlertRowNode(Row(json));
        var notebook = AlertNotebookEndpoint.AlertRowNode(Row(json), incident: null);

        Assert.Equal(notebook["newest_sample"]!.ToJsonString(), triage["newest_sample"]!.ToJsonString());
        Assert.Equal(Line, (string?)triage["newest_sample"]!["line"]);
    }

    [Fact]
    public void TheWebRows_HaveANullNewestSample_WhenTheRowCarriesNoAge_OrNoContext()
    {
        foreach (var json in new[] { ContextJsonWith(null), null })
        {
            Assert.Null(DarlingTriageEndpoint.AlertRowNode(Row(json))["newest_sample"]);
            Assert.Null(AlertNotebookEndpoint.AlertRowNode(Row(json), incident: null)["newest_sample"]);
        }
    }

    [Fact]
    public void TheWebRows_DoNotRecomputeTheAge_FromTheClock()
    {
        // The carrier was 320 s at fire time; a row read years later still says so.
        var node = AlertNotebookEndpoint.AlertRowNode(Row(ContextJsonWith(Sample())), incident: null);
        Assert.Equal(320d, (double?)node["newest_sample"]!["age_seconds"]);

        foreach (var path in new[] { NotebookPath, TriagePath, McpPath })
        {
            var source = ReadRepoFile(path);
            Assert.DoesNotContain("UtcNow - sample", source, StringComparison.Ordinal);
            Assert.DoesNotContain("SampleAgeLine(new AlertNewestSample", source, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheAlertsPage_ShowsTheLineFromTheStructuredBranch_ExactlyOnce()
    {
        var js = ReadRepoFile(AlertsJsPath);

        // Inside the structured-items branch only; the detail_text branches already hold the line in the text.
        var items = js.IndexOf("if (items) {", StringComparison.Ordinal);
        var next = js.IndexOf("} else if (fields) {", items, StringComparison.Ordinal);
        Assert.True(items >= 0 && next > items);
        var branch = js[items..next];
        Assert.Contains("a.newest_sample", branch, StringComparison.Ordinal);
        Assert.Equal(1, Count(js, "class: \"detail-sample-age\""));
        Assert.Contains("detail-sample-age", branch, StringComparison.Ordinal);
    }

    /* ---------------- MCP get_alert_history ---------------- */

    [Fact]
    public void TheMcpHistoryRow_PublishesNewestSample_FromThePersistedContext()
    {
        var source = ReadRepoFile(McpPath);

        Assert.Contains("newest_sample = AlertNewestSampleWire.FromContextJson(r.ContextJson),", source, StringComparison.Ordinal);
        Assert.Equal(1, Count(source, "newest_sample ="));

        // The same field serializes with the names the web rows use, because both read one wire type.
        var json = JsonSerializer.Serialize(new
        {
            newest_sample = AlertNewestSampleWire.FromContextJson(ContextJsonWith(Sample()))
        });
        using var doc = JsonDocument.Parse(json);
        var sample = doc.RootElement.GetProperty("newest_sample");
        Assert.Equal("CPU", sample.GetProperty("source").GetString());
        Assert.Equal(320d, sample.GetProperty("age_seconds").GetDouble());
        Assert.Equal(Line, sample.GetProperty("line").GetString());
        Assert.Equal(JsonValueKind.Null, JsonDocument.Parse(JsonSerializer.Serialize(new
        {
            newest_sample = AlertNewestSampleWire.FromContextJson(ContextJsonWith(null))
        })).RootElement.GetProperty("newest_sample").ValueKind);
    }

    /* ---------------- the Viewer's alert detail ---------------- */

    [Fact]
    public void TheViewerDetail_ReadsThePersistedCarrier_ForTheStructuredViewAndTheFlatText()
    {
        var json = ContextJsonWith(Sample());

        // Structured view up: detail_text is not printed, so the line comes from the carrier.
        Assert.Equal(Line, AlertDetailText.SampleAgeLineFromJson(json, null));
        // Flat text that already carries the line: nothing added, so it shows once.
        Assert.Null(AlertDetailText.SampleAgeLineFromJson(json, "CPU is at 97%.\n\n" + Line));
        // No age on the row: no line.
        Assert.Null(AlertDetailText.SampleAgeLineFromJson(ContextJsonWith(null), "CPU is at 97%."));

        var source = ReadRepoFile(ViewerDetailPath);
        Assert.Contains("AlertDetailText.SampleAgeLineFromJson(", source, StringComparison.Ordinal);
        Assert.Contains("DetailItemsScroll.Visibility == Visibility.Visible ? null : item.DetailText", source, StringComparison.Ordinal);
        Assert.Equal(1, Count(source, "SampleAgeText.Text ="));
    }

    [Fact]
    public void TheViewerToast_DoesNotSummariseOnTheAgeLine()
    {
        var source = ReadRepoFile(ViewerMainPath);

        Assert.Contains("AlertDetailText.StripSampleAge(row.DetailText ?? string.Empty)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("row.DetailText.Split(", source, StringComparison.Ordinal);

        // A detail text that is only the age line strips to nothing, so the toast falls to the formatted value.
        Assert.Equal(string.Empty, AlertDetailText.StripSampleAge(Line));
        Assert.Equal("CPU is at 97%.", AlertDetailText.StripSampleAge("CPU is at 97%.\n\n" + Line));
    }
}
