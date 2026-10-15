/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The generic webhook's <c>{{investigation_json}}</c> token (#5821), end to end through <see cref="DarlingAlertDeliverer"/>
/// to a capturing endpoint: the request the alert opened or joined is in the posted body (so it was opened before the post),
/// its brief masked at <c>mcp.dataDetail</c>, and the token is empty for an alert that opened none. The store is stood in
/// by the dispatcher's seam; its own reads are <c>InvestigationRequestStoreLiveTests</c>.
/// </summary>
public sealed class InvestigationWebhookTokenTests
{
    private const string BareTemplate = "{\"metric\": \"{{metric}}\", \"investigation\": {{investigation_json}}}";
    private const string QuotedTemplate = "{\"metric\": \"{{metric}}\", \"investigation\": \"{{investigation_json}}\"}";
    private const string Secret = "WHERE CustomerName = 'Alice Liddell'";

    private static readonly Guid RequestId = Guid.Parse("6f1c2a8e-0d4b-4c39-9b7a-3e5f1d2c4a10");

    private static NotificationRoute Route(int routeId, string match, int? level = NotificationRoute.AgentLevelInvestigate) =>
        new(routeId, match, "", "", "", "", "", Enabled: true) { AgentLevel = level };

    private static AlertOutcome Outcome(string metric, bool muted = false) =>
        new("42", "srv", metric, "3", "1", null, "detail",
            NumericCurrentValue: 3, NumericThresholdValue: 1, Muted: muted, Severity: null);

    private static InvestigationRequestRow Row(Guid id, string state = "open") => new(
        id, 5, 42, "srv", "blocking", "Deadlocks Detected",
        new DateTime(2026, 10, 15, 12, 0, 0, DateTimeKind.Utc), new DateTime(2026, 10, 15, 12, 5, 0, DateTimeKind.Utc), 2,
        state, new DateTime(2026, 10, 15, 12, 0, 1, DateTimeKind.Utc), new DateTime(2026, 10, 15, 16, 0, 1, DateTimeKind.Utc),
        null, null, 0, null);

    /// <summary>A brief stored at <c>full</c> whose finding story quotes collected text.</summary>
    private static string StoredBrief() =>
        "{\"brief_format\":1,\"data_detail\":\"full\",\"findings\":[{\"category\":\"blocking\",\"story\":"
        + JsonSerializer.Serialize(Secret) + "}]}";

    private static async Task<(string? Body, List<InvestigationAlert> Asked)> DeliverAsync(
        string template, AlertOutcome outcome, NotificationRoute[] routes, InvestigationOpenResult? open = null,
        string? brief = null, McpDataDetail mode = McpDataDetail.MetadataOnly, List<Guid>? reads = null)
    {
        using var generic = new CapturingWebhookEndpoint();
        var config = new DarlingConfig();
        config.Webhooks.GenericUrl = generic.Url;
        config.Webhooks.GenericBodyTemplate = template;
        config.NotificationRoutes = routes;
        var settings = new DarlingAlertSettings(config);
        var history = new NullHistoryStore();
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        var asked = new List<InvestigationAlert>();
        var result = open ?? new InvestigationOpenResult(InvestigationOpenOutcome.Opened, RequestId);
        var dispatcher = new InvestigationDispatcher(
            (alert, _) =>
            {
                asked.Add(alert);
                return Task.FromResult(result);
            },
            settings, NullLogger.Instance,
            (id, _) =>
            {
                reads?.Add(id);
                return Task.FromResult<(InvestigationRequestRow Row, string? Brief)?>((Row(id), brief));
            },
            mode);
        var deliverer = new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance, investigationDispatcher: dispatcher);

        await deliverer.DeliverAsync(outcome, TestContext.Current.CancellationToken);
        return (generic.Bodies.SingleOrDefault(), asked);
    }

    private static JsonElement Investigation(string? body)
    {
        Assert.NotNull(body);
        return JsonDocument.Parse(body!).RootElement.GetProperty("investigation").Clone();
    }

    [Fact]
    public async Task AnAlertThatOpensARequest_PostsTheRequest_WithItsBriefMaskedAtTheConfiguredLevel()
    {
        var (body, asked) = await DeliverAsync(
            BareTemplate, Outcome("Deadlocks Detected"), [Route(5, "Deadlocks Detected")], brief: StoredBrief());

        Assert.Single(asked);
        var investigation = Investigation(body);
        Assert.Equal(RequestId.ToString(), investigation.GetProperty("request_id").GetString());
        Assert.Equal("opened", investigation.GetProperty("outcome").GetString());
        Assert.Equal("open", investigation.GetProperty("state").GetString());
        Assert.Equal("blocking", investigation.GetProperty("family").GetString());
        Assert.Equal("srv", investigation.GetProperty("server_name").GetString());
        Assert.Equal(JsonValueKind.String, investigation.GetProperty("expires_at").ValueKind);
        Assert.Equal("claim_investigation_request", investigation.GetProperty("claim_tool").GetString());
        Assert.Equal("submit_investigation_report", investigation.GetProperty("report_tool").GetString());

        /* The brief was stored at full; served at metadataOnly its quoted text does not leave the service. */
        var brief = investigation.GetProperty("brief");
        Assert.Equal(JsonValueKind.Object, brief.ValueKind);
        Assert.DoesNotContain("Alice Liddell", body!, StringComparison.Ordinal);
        Assert.Equal("blocking", brief.GetProperty("findings")[0].GetProperty("category").GetString());
    }

    /// <summary>
    /// #5821 review round 1: the request is read back only for a page whose generic body template places the token. The
    /// token is in no shipped template, so an agent-routed alert on the default template opens its request and makes no
    /// second store call in front of its page.
    /// </summary>
    [Theory]
    [InlineData("{\"metric\": \"{{metric}}\"}")]
    [InlineData("")]
    public async Task ATemplateWithoutTheToken_OpensTheRequest_AndReadsNothingBack(string template)
    {
        var reads = new List<Guid>();
        var (body, asked) = await DeliverAsync(
            template, Outcome("Deadlocks Detected"), [Route(5, "Deadlocks Detected")], brief: StoredBrief(), reads: reads);

        Assert.NotNull(body);
        Assert.Single(asked);
        Assert.Empty(reads);
    }

    [Fact]
    public async Task ATemplateWithTheToken_ReadsTheRequestBackOnce()
    {
        var reads = new List<Guid>();
        await DeliverAsync(BareTemplate, Outcome("Deadlocks Detected"), [Route(5, "Deadlocks Detected")], reads: reads);

        Assert.Equal(RequestId, Assert.Single(reads));
    }

    /// <summary>
    /// #5821 review round 1: a stored brief the token cannot parse or mask does not stop the page. It still posts, carrying
    /// the request id, and the brief goes as null.
    /// </summary>
    [Fact]
    public async Task ABriefThatCannotBeRendered_StillPostsTheRequestId()
    {
        var (body, asked) = await DeliverAsync(
            BareTemplate, Outcome("Deadlocks Detected"), [Route(5, "Deadlocks Detected")], brief: "{\"findings\": [");

        Assert.Single(asked);
        var investigation = Investigation(body);
        Assert.Equal(RequestId.ToString(), investigation.GetProperty("request_id").GetString());
        Assert.Equal(JsonValueKind.Null, investigation.GetProperty("brief").ValueKind);
    }

    [Fact]
    public async Task AtFull_TheBriefGoesAsStored()
    {
        var (body, _) = await DeliverAsync(
            BareTemplate, Outcome("Deadlocks Detected"), [Route(5, "Deadlocks Detected")], brief: StoredBrief(),
            mode: McpDataDetail.Full);

        Assert.Equal(Secret, Investigation(body).GetProperty("brief").GetProperty("findings")[0].GetProperty("story").GetString());
    }

    [Fact]
    public async Task ARequestWithNoBriefYet_PostsANullBrief()
    {
        var (body, _) = await DeliverAsync(BareTemplate, Outcome("Deadlocks Detected"), [Route(5, "Deadlocks Detected")]);

        Assert.Equal(JsonValueKind.Null, Investigation(body).GetProperty("brief").ValueKind);
    }

    [Fact]
    public async Task ASecondAlertInTheWindow_PostsTheRequestItJoined()
    {
        var joined = Guid.Parse("0a9b8c7d-6e5f-4a3b-8c2d-1e0f9a8b7c6d");
        var (body, _) = await DeliverAsync(
            BareTemplate, Outcome("Deadlocks Detected"), [Route(5, "Deadlocks Detected")],
            open: new InvestigationOpenResult(InvestigationOpenOutcome.Joined, joined));

        var investigation = Investigation(body);
        Assert.Equal(joined.ToString(), investigation.GetProperty("request_id").GetString());
        Assert.Equal("joined", investigation.GetProperty("outcome").GetString());
    }

    [Fact]
    public async Task InsideQuotes_TheTokenIsTheRequestsEscapedText_AndTheBodyStaysWellFormed()
    {
        var (body, _) = await DeliverAsync(QuotedTemplate, Outcome("Deadlocks Detected"), [Route(5, "Deadlocks Detected")]);

        var text = Investigation(body).GetString();
        Assert.NotNull(text);
        Assert.Equal(RequestId.ToString(), JsonDocument.Parse(text!).RootElement.GetProperty("request_id").GetString());
    }

    [Theory]
    [InlineData(BareTemplate)]
    [InlineData(QuotedTemplate)]
    public async Task ARouteWithNoAgentLevel_RendersTheTokenEmpty(string template)
    {
        var (body, asked) = await DeliverAsync(template, Outcome("Deadlocks Detected"), [Route(5, "Deadlocks Detected", level: null)]);

        Assert.Empty(asked);
        Assert.Equal("", Investigation(body).GetString());
    }

    [Theory]
    [InlineData("Collector Cost Digest")] // a report
    [InlineData("Server Restored")] // a recovery
    public async Task AReportOrARecovery_RendersTheTokenEmpty(string metric)
    {
        var (body, asked) = await DeliverAsync(
            BareTemplate, Outcome(metric), [Route(2, AlertFamily.Reports), Route(3, "Server Restored")]);

        Assert.Empty(asked);
        Assert.Equal("", Investigation(body).GetString());
    }

    [Fact]
    public async Task ARouteAtItsLimit_RendersTheTokenEmpty()
    {
        var (body, asked) = await DeliverAsync(
            BareTemplate, Outcome("Deadlocks Detected"), [Route(5, "Deadlocks Detected")],
            open: new InvestigationOpenResult(InvestigationOpenOutcome.LimitReached, null));

        Assert.Single(asked);
        Assert.Equal("", Investigation(body).GetString());
    }

    [Fact]
    public async Task AMutedAlert_OpensNothing_AndRendersEmpty()
    {
        /* A muted alert posts nothing at all; the token it would have carried is empty because no request was asked for. */
        var settings = new DarlingAlertSettings(new DarlingConfig { NotificationRoutes = [Route(5, "Deadlocks Detected")] });
        var asked = 0;
        var dispatcher = new InvestigationDispatcher(
            (_, _) =>
            {
                asked++;
                return Task.FromResult(new InvestigationOpenResult(InvestigationOpenOutcome.Opened, RequestId));
            },
            settings, NullLogger.Instance);

        var token = await dispatcher.OpenForWebhookAsync(
            "42", "srv", "Deadlocks Detected", muted: true, DateTime.UtcNow, null, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Null(token);
        Assert.Equal(0, asked);
        var rendered = WebhookAlertService.BuildGenericPayload(
            "Deadlocks Detected", "srv", "3", "1", DarlingAlertDeliverer.Branding, bodyTemplate: BareTemplate, investigationJson: token);
        Assert.Equal("", Investigation(rendered).GetString());
    }

    [Theory]
    [InlineData("{\"note\": \"say \\\"hi\\\" {{investigation_json}}\", \"after\": {{investigation_json}}}")]
    [InlineData("{\"a\\\\\": {{investigation_json}}, \"b\": \"{{investigation_json}}\"}")]
    public void EscapedQuotesAndBackslashesBeforeTheToken_DoNotMisplaceIt(string template)
    {
        const string Json = "{\"request_id\":\"x\",\"brief\":null}";

        var rendered = WebhookAlertService.BuildGenericPayload(
            "Deadlocks Detected", "srv", "3", "1", DarlingAlertDeliverer.Branding, bodyTemplate: template, investigationJson: Json);
        var root = JsonDocument.Parse(rendered).RootElement;

        var properties = root.EnumerateObject().ToList();
        Assert.Equal(JsonValueKind.String, properties[0].Name == "note" ? properties[0].Value.ValueKind : properties[1].Value.ValueKind);
        Assert.Equal(JsonValueKind.Object, properties[0].Name == "note" ? properties[1].Value.ValueKind : properties[0].Value.ValueKind);
        Assert.Null(WebhookAlertService.ValidateGenericConfig(null, template));
    }

    private sealed class NullHistoryStore : IAlertHistoryStore
    {
        public Task RecordAlertAsync(AlertHistoryRecord record) => Task.CompletedTask;

        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) =>
            Task.FromResult<DateTime?>(null);
    }
}
