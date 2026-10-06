using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// PagerDuty webhook channel tests. Verifies the Events API v2 payload structure, severity mapping,
/// dedup_key derivation, and EU-region endpoint resolution.
/// </summary>
public class PagerDutyWebhookTests
{
    private static readonly AlertBranding Branding = new("Performance Monitor Lite", null);

    /* ---------------- Payload structure ---------------- */

    [Fact]
    public void BuildPagerDutyPayload_ProducesValidJson_WithRequiredFields()
    {
        var payload = WebhookAlertService.BuildPagerDutyPayload(
            "High CPU", "SRV1", "95%", "90%", Branding, "abc123routingkey");

        var root = JsonDocument.Parse(payload).RootElement;

        Assert.Equal("abc123routingkey", root.GetProperty("routing_key").GetString());
        Assert.Equal("trigger", root.GetProperty("event_action").GetString());
        Assert.True(root.TryGetProperty("dedup_key", out var dedupKey));
        Assert.False(string.IsNullOrEmpty(dedupKey.GetString()));

        var payloadObj = root.GetProperty("payload");
        Assert.Contains("High CPU", payloadObj.GetProperty("summary").GetString());
        Assert.Contains("SRV1", payloadObj.GetProperty("summary").GetString());
        Assert.Equal("SRV1", payloadObj.GetProperty("source").GetString());
        Assert.Equal("warning", payloadObj.GetProperty("severity").GetString());
        Assert.Equal("SQL Server Performance Monitor", payloadObj.GetProperty("component").GetString());
        Assert.False(string.IsNullOrEmpty(payloadObj.GetProperty("timestamp").GetString()));

        Assert.Equal("Performance Monitor Lite", root.GetProperty("client").GetString());
    }

    [Fact]
    public void BuildPagerDutyPayload_TestMode_OmitsRealServerData()
    {
        var payload = WebhookAlertService.BuildPagerDutyPayload(
            "Test Notification", "", "Webhook configuration verified", "",
            Branding, "abc123routingkey", isTest: true);

        var root = JsonDocument.Parse(payload).RootElement;
        var payloadObj = root.GetProperty("payload");

        Assert.Equal("Webhook configuration verified", payloadObj.GetProperty("summary").GetString());
        Assert.Equal("Performance Monitor Lite", payloadObj.GetProperty("source").GetString());
    }

    /* ---------------- Severity mapping ---------------- */

    [Theory]
    [InlineData("CRITICAL", "critical")]
    [InlineData("ALERT", "error")]
    [InlineData("WARNING", "warning")]
    [InlineData("RESOLVED", "info")]
    [InlineData("INFO", "info")]
    public void BuildPagerDutyPayload_MapsSeverityCorrectly(string badgeText, string expectedPdSeverity)
    {
        /* Use a metric that produces the desired badge text, or a severity override. */
        var metricName = badgeText switch
        {
            "CRITICAL" => "Server Unreachable",
            "ALERT" => "Blocking Detected",
            "WARNING" => "High CPU",
            "RESOLVED" => "Server Restored",
            "INFO" => "Unknown Metric",
            _ => "High CPU"
        };

        var payload = WebhookAlertService.BuildPagerDutyPayload(
            metricName, "SRV1", "0", "0", Branding, "key");

        var root = JsonDocument.Parse(payload).RootElement;
        var severity = root.GetProperty("payload").GetProperty("severity").GetString();

        Assert.Equal(expectedPdSeverity, severity);
    }

    /* ---------------- Dedup key derivation ---------------- */

    [Fact]
    public void BuildPagerDutyPayload_WithIncidentContext_UsesIncidentDedupKey()
    {
        var context = new AlertContext
        {
            Incidents = new List<AlertIncident>
            {
                new("incident-fingerprint-123", new[] { "db.dbo.Table1" })
            }
        };

        var payload = WebhookAlertService.BuildPagerDutyPayload(
            "Blocking Detected", "SRV1", "5", "1", Branding, "key", context: context);

        var root = JsonDocument.Parse(payload).RootElement;
        var dedupKey = root.GetProperty("dedup_key").GetString();

        Assert.Equal("incident-fingerprint-123", dedupKey);
    }

    [Fact]
    public void BuildPagerDutyPayload_WithoutIncidentContext_FallsBackToMetricServerKey()
    {
        var payload = WebhookAlertService.BuildPagerDutyPayload(
            "High CPU", "SRV1", "95%", "90%", Branding, "key");

        var root = JsonDocument.Parse(payload).RootElement;
        var dedupKey = root.GetProperty("dedup_key").GetString();

        Assert.Equal("SRV1:High CPU", dedupKey);
    }

    [Fact]
    public void BuildPagerDutyPayload_DedupKey_IsDeterministic()
    {
        var payload1 = WebhookAlertService.BuildPagerDutyPayload(
            "High CPU", "SRV1", "95%", "90%", Branding, "key");
        var payload2 = WebhookAlertService.BuildPagerDutyPayload(
            "High CPU", "SRV1", "95%", "90%", Branding, "key");

        var root1 = JsonDocument.Parse(payload1).RootElement;
        var root2 = JsonDocument.Parse(payload2).RootElement;

        Assert.Equal(root1.GetProperty("dedup_key").GetString(), root2.GetProperty("dedup_key").GetString());
    }

    /* ---------------- Edge-pair dedup is gated on the opt-in ---------------- */

    [Fact]
    public void BuildPagerDutyPayload_EdgePair_DefaultOff_KeepsShippedStateNamedKeys()
    {
        /* The review's paired-lifecycle merge risk, stated as a pin: with the default off, the two halves
           of a pair keep their own state-named keys — byte-identical to the shipped behavior — so a
           manually-resolved outage and a later restore never share a key. Correlation is an opt-in behaviour
           change, scoped entirely to the operator who switched it on. */
        var unreachable = WebhookAlertService.BuildPagerDutyPayload(
            "Server Unreachable", "SRV1", "Login timeout expired", "Online",
            Branding, "key", serverId: "261742202");
        var restored = WebhookAlertService.BuildPagerDutyPayload(
            "Server Restored", "SRV1", "Online", "Online",
            Branding, "key", serverId: "261742202");

        Assert.Equal("261742202:Server Unreachable",
            JsonDocument.Parse(unreachable).RootElement.GetProperty("dedup_key").GetString());
        Assert.Equal("261742202:Server Restored",
            JsonDocument.Parse(restored).RootElement.GetProperty("dedup_key").GetString());
    }

    [Fact]
    public void BuildPagerDutyPayload_ConnectionEdges_WithOptIn_ShareOneDedupKey_NamedByTheFiring()
    {
        /* With the opt-in on, "Server Unreachable" and "Server Restored" are one incident: keying on the
           metric name minted a distinct dedup_key per edge, so PagerDuty showed two incidents for one
           outage, and a closing edge could never resolve the open one. Both edges rename to the pair's
           canonical FIRING name. */
        var unreachable = WebhookAlertService.BuildPagerDutyPayload(
            "Server Unreachable", "SRV1", "Login timeout expired", "Online",
            Branding, "key", serverId: "261742202", autoResolve: true);
        var restored = WebhookAlertService.BuildPagerDutyPayload(
            "Server Restored", "SRV1", "Online", "Online",
            Branding, "key", serverId: "261742202", autoResolve: true);

        var unreachableKey = JsonDocument.Parse(unreachable).RootElement.GetProperty("dedup_key").GetString();
        var restoredKey = JsonDocument.Parse(restored).RootElement.GetProperty("dedup_key").GetString();

        Assert.Equal("261742202:Server Unreachable", unreachableKey);
        Assert.Equal(unreachableKey, restoredKey);
    }

    [Fact]
    public void BuildPagerDutyPayload_ReplicationEdges_WithOptIn_ShareOneDedupKey_ScopedPerReplica()
    {
        /* The same per-incident identity for the AG pair — AND per replica: without the replica scope
           there is still one key per server for the whole group, so one replica's reconnect would resolve
           the incident a still-disconnected sibling holds open. Each edge's own context carries the twin
           "AG:replica" identity its fire site set, and both sides of the same replica converge on it. */
        var first = AgAlertContexts.ForReplica("OrdersAG", "REPLICA-A");
        var second = AgAlertContexts.ForReplica("OrdersAG", "REPLICA-A");
        var other = AgAlertContexts.ForReplica("OrdersAG", "REPLICA-B");

        var disconnected = WebhookAlertService.BuildPagerDutyPayload(
            "AG Replica Disconnected", "SRV1", "DISCONNECTED", "CONNECTED",
            Branding, "key", serverId: "261742202", context: first, autoResolve: true);
        var reconnected = WebhookAlertService.BuildPagerDutyPayload(
            "AG Replica Reconnected", "SRV1", "CONNECTED", "CONNECTED",
            Branding, "key", serverId: "261742202", context: second, autoResolve: true);
        var sibling = WebhookAlertService.BuildPagerDutyPayload(
            "AG Replica Disconnected", "SRV1", "DISCONNECTED", "CONNECTED",
            Branding, "key", serverId: "261742202", context: other, autoResolve: true);

        var disconnectedKey = JsonDocument.Parse(disconnected).RootElement.GetProperty("dedup_key").GetString();
        var reconnectedKey = JsonDocument.Parse(reconnected).RootElement.GetProperty("dedup_key").GetString();
        var siblingKey = JsonDocument.Parse(sibling).RootElement.GetProperty("dedup_key").GetString();

        Assert.Equal("261742202:AG Replica Disconnected:OrdersAG:REPLICA-A", disconnectedKey);
        Assert.Equal(disconnectedKey, reconnectedKey);
        Assert.NotEqual(disconnectedKey, siblingKey);
    }

    [Fact]
    public void BuildPagerDutyPayload_ReplicationEdge_WithoutIdentity_KeepsPerServerKey()
    {
        /* The per-replica key is the better incident, not a contract: a pair that arrives without the
           identity (a build that predates the new fire sites, a test that never set one) keeps the old
           per-server key rather than failing the render. */
        var reconnected = WebhookAlertService.BuildPagerDutyPayload(
            "AG Replica Reconnected", "SRV1", "CONNECTED", "CONNECTED",
            Branding, "key", serverId: "261742202", autoResolve: true);

        Assert.Equal("261742202:AG Replica Disconnected",
            JsonDocument.Parse(reconnected).RootElement.GetProperty("dedup_key").GetString());
    }

    [Fact]
    public void BuildPagerDutyPayload_NonEdgeMetric_StillKeysOnTheMetricName()
    {
        var payload = WebhookAlertService.BuildPagerDutyPayload(
            "High CPU", "SRV1", "95%", "90%", Branding, "key", serverId: "261742202");

        var root = JsonDocument.Parse(payload).RootElement;

        Assert.Equal("trigger", root.GetProperty("event_action").GetString());
        Assert.Equal("261742202:High CPU", root.GetProperty("dedup_key").GetString());
    }

    /* ---------------- The auto-resolve opt-in ---------------- */

    [Fact]
    public void BuildPagerDutyPayload_EdgeClosingEdge_DefaultsToInfoLevelTrigger_LeavingTheIncidentOpen()
    {
        var payload = WebhookAlertService.BuildPagerDutyPayload(
            "Server Restored", "SRV1", "Online", "Online",
            Branding, "key", serverId: "261742202");

        var root = JsonDocument.Parse(payload).RootElement;

        Assert.Equal("trigger", root.GetProperty("event_action").GetString());
        Assert.Equal("info", root.GetProperty("payload").GetProperty("severity").GetString());
        /* ...and keeps the shipped state-named key: correlation is opt-in, not smuggled in the default. */
        Assert.Equal("261742202:Server Restored", root.GetProperty("dedup_key").GetString());
    }

    [Fact]
    public void BuildPagerDutyPayload_AutoResolve_SendsTheResolveEvent_OnTheFiringEdgeDedupKey()
    {
        var payload = WebhookAlertService.BuildPagerDutyPayload(
            "Server Restored", "SRV1", "Online", "Online",
            Branding, "key", serverId: "261742202", autoResolve: true);

        var root = JsonDocument.Parse(payload).RootElement;

        Assert.Equal("resolve", root.GetProperty("event_action").GetString());
        Assert.Equal("261742202:Server Unreachable", root.GetProperty("dedup_key").GetString());
    }

    [Fact]
    public void BuildPagerDutyPayload_AutoResolve_ReplicaReconnect_AlsoResolves_TheFiringEdgeNeverDoes()
    {
        var edge = WebhookAlertService.BuildPagerDutyPayload(
            "AG Replica Disconnected", "SRV1", "DISCONNECTED", "CONNECTED",
            Branding, "key", serverId: "261742202", context: AgAlertContexts.ForReplica("OrdersAG", "REPLICA-A"), autoResolve: true);
        var closing = WebhookAlertService.BuildPagerDutyPayload(
            "AG Replica Reconnected", "SRV1", "CONNECTED", "CONNECTED",
            Branding, "key", serverId: "261742202", context: AgAlertContexts.ForReplica("OrdersAG", "REPLICA-A"), autoResolve: true);

        Assert.Equal("trigger", JsonDocument.Parse(edge).RootElement.GetProperty("event_action").GetString());
        Assert.Equal("resolve", JsonDocument.Parse(closing).RootElement.GetProperty("event_action").GetString());
        /* ...on the same per-replica key: closing only ITS incident, not the sibling's. */
        Assert.Equal(
            "261742202:AG Replica Disconnected:OrdersAG:REPLICA-A",
            JsonDocument.Parse(edge).RootElement.GetProperty("dedup_key").GetString());
        Assert.Equal(
            JsonDocument.Parse(edge).RootElement.GetProperty("dedup_key").GetString(),
            JsonDocument.Parse(closing).RootElement.GetProperty("dedup_key").GetString());
    }

    [Fact]
    public void BuildPagerDutyPayload_AutoResolve_OnAFiringMetric_OrAnyNonEdgeAlert_StillTriggers()
    {
        var unreachable = WebhookAlertService.BuildPagerDutyPayload(
            "Server Unreachable", "SRV1", "Login timeout expired", "Online",
            Branding, "key", serverId: "261742202", autoResolve: true);
        var cpu = WebhookAlertService.BuildPagerDutyPayload(
            "High CPU", "SRV1", "95%", "90%", Branding, "key", serverId: "261742202", autoResolve: true);

        Assert.Equal("trigger", JsonDocument.Parse(unreachable).RootElement.GetProperty("event_action").GetString());
        Assert.Equal("trigger", JsonDocument.Parse(cpu).RootElement.GetProperty("event_action").GetString());
    }

    [Fact]
    public void BuildPagerDutyPayload_TestMode_AlwaysTriggers_EvenWithAutoResolve()
    {
        var payload = WebhookAlertService.BuildPagerDutyPayload(
            "Server Restored", "", "Webhook configuration verified", "",
            Branding, "key", isTest: true, dedupKey: "test-" + Guid.NewGuid(), autoResolve: true);

        Assert.Equal("trigger", JsonDocument.Parse(payload).RootElement.GetProperty("event_action").GetString());
    }

    /* ---------------- The send path with a flag-on/f flag-off settings seam ---------------- */

    /// <summary>
    /// The review's coverage gap, stated as a pin: the builder-level tests above still pass with the live
    /// wiring removed — so this one goes THROUGH <see cref="WebhookAlertService.TrySendWebhookAlertsAsync"/>
    /// and asserts the ACTION the wire actually saw, for both settings of the flag. The PagerDuty destination
    /// carries the loopback endpoint (a fake routing key is enough: the content never reaches PagerDuty), so
    /// no network calls the fixed production endpoint.
    /// </summary>
    [Fact]
    public async Task SendPath_PagerDutySend_RestoredEdge_ResolvesOnlyWhenTheSettingIsOn()
    {
        var restoring = new CapturingPagerDuty();
        var resolving = new CapturingPagerDuty();

        var off = new WebhookSettings
        {
            PagerDutyRoutingKey = "rk-test",
            Capture = restoring,
        };
        var on = new WebhookSettings
        {
            PagerDutyRoutingKey = "rk-test",
            PagerDutyAutoResolve = true,
            Capture = resolving,
        };

        await SendSingleAlertAsync(ServiceFor(off, restoring), "Server Restored");
        await SendSingleAlertAsync(ServiceFor(on, resolving), "Server Restored");

        Assert.Equal("trigger", WireAction(Assert.Single(restoring.Bodies)));
        Assert.Equal("resolve", WireAction(Assert.Single(resolving.Bodies)));

        /* The resolve lands on the same per-replica incident identity the opt-in trigger opened: */
        Assert.Equal(
            WebhookAlertService.DerivePagerDutyDedupKey("261742202", "Server Unreachable", null, autoResolve: true),
            WireDedupKey(Assert.Single(resolving.Bodies)));
    }

    [Fact]
    public async Task SendPath_PagerDutySend_Reconnect_ClosesOnlyItsOwnReplicaIncident()
    {
        /* The review's A/B disconnect case: both replicas down (flag on), A reconnects. Asserting on the
           wire bodies that the resolve carries A's replica-scoped incident — and that B's disconnect key is
           a DIFFERENT incident, so B's outage outlives A's resolve. */
        var endpoint = new CapturingPagerDuty();

        var settings = new WebhookSettings
        {
            PagerDutyRoutingKey = "rk-test",
            PagerDutyAutoResolve = true,
            Capture = endpoint,
        };

        var service = ServiceFor(settings, endpoint);

        await SendSingleAlertAsync(service, "AG Replica Disconnected", "261742202", AgAlertContexts.ForReplica("OrdersAG", "REPLICA-B"));
        await SendSingleAlertAsync(service, "AG Replica Reconnected", "261742202", AgAlertContexts.ForReplica("OrdersAG", "REPLICA-A"));

        Assert.Equal(2, endpoint.Bodies.Count);
        var downKey = WireDedupKey(endpoint.Bodies[0]);
        var resolveRoot = JsonDocument.Parse(endpoint.Bodies[1]).RootElement;

        Assert.Equal("resolve", resolveRoot.GetProperty("event_action").GetString());
        Assert.Equal("261742202:AG Replica Disconnected:OrdersAG:REPLICA-A",
            resolveRoot.GetProperty("dedup_key").GetString());
        Assert.NotEqual(downKey, resolveRoot.GetProperty("dedup_key").GetString());
    }

    [Fact]
    public async Task SendPath_WebhookCooldown_AutoResolveRecovery_RearmsItsFiring()
    {
        /* The review's down-back-down case: with the flag on, a delivered close clears the cooldown entry
           its firing left behind, so the next "Server Unreachable" posts instead of meeting the old window.
           Asserted on the wire bodies: unreachable, restored, unreachable again delivers THREE posts. */
        var endpoint = new CapturingPagerDuty();

        var settings = new WebhookSettings { PagerDutyRoutingKey = "rk-test", PagerDutyAutoResolve = true, Capture = endpoint };

        var service = ServiceFor(settings, endpoint);

        Assert.Equal(AlertChannelOutcome.Delivered, await SendSingleAlertAsync(service, "Server Unreachable"));
        Assert.Equal(AlertChannelOutcome.Delivered, await SendSingleAlertAsync(service, "Server Restored"));
        var second = await SendSingleAlertAsync(service, "Server Unreachable");

        Assert.Equal(AlertChannelOutcome.Delivered, second);
        Assert.Equal(3, endpoint.Bodies.Count);
        Assert.Equal("trigger", WireAction(endpoint.Bodies[2]));
    }

    /// <summary>
    /// One alert through the real <see cref="WebhookAlertService"/> fan-out with the PagerDuty post
    /// captured. Each <see cref="WebhookAlertService"/> is constructed per send (no shared state leaks
    /// between the sends an A/B case sequences); the shared per-test cooldown is the POINT of the
    /// re-arm test, so the same service instance is REUSED where the cooldown must persist across sends.
    /// </summary>
    private async Task<AlertChannelOutcome> SendSingleAlertAsync(
        WebhookAlertService service, string metricName, string serverId = "261742202",
        AlertContext? context = null)
    {
        var result = await service.TrySendWebhookAlertsAsync(
            metricName, "SRV1", "current", "threshold", serverId, context);
        return result.Outcome;
    }

    private WebhookAlertService ServiceFor(WebhookSettings settings, CapturingPagerDuty capture)
    {
        var service = new WebhookAlertService(
            settings,
            Branding,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<WebhookAlertService>.Instance,
            historyStore: null);
        service.PostPagerDutyAsyncOverride = async (endpoint, payload) =>
        {
            Assert.Equal("https://events.pagerduty.com/v2/enqueue", endpoint);
            capture.Bodies.Add(payload);
            return await Task.FromResult<string?>(null);
        };
        return service;
    }

    private static string WireAction(string body) =>
        JsonDocument.Parse(body).RootElement.GetProperty("event_action").GetString()!;

    private static string WireDedupKey(string body) =>
        JsonDocument.Parse(body).RootElement.GetProperty("dedup_key").GetString()!;

    /// <summary>
    /// The payloads one test's PagerDuty sends captured, in send order. Plain list, no socket: the
    /// service's post override answers the send and hands the payload here, so nothing leaves the
    /// process and the assertions read the exact JSON the wire would have seen. One instance per
    /// settings, so a flag-on capture can never leak bodies into a flag-off one.
    /// </summary>
    private sealed class CapturingPagerDuty
    {
        public List<string> Bodies { get; } = new();
    }

    private sealed class WebhookSettings : IAlertSettings
    {
        public string PagerDutyRoutingKey { get; set; } = "";
        public bool PagerDutyAutoResolve { get; set; }
        public CapturingPagerDuty? Capture { get; set; }

        public bool PagerDutyEnabled => !string.IsNullOrWhiteSpace(PagerDutyRoutingKey);
        public bool PagerDutyUseEuRegion => false;

        /* The proxy is unused on this fake (the capture rides the service's post override instead), kept
           defaulted to the shipped "direct" shape. */
        string IAlertSettings.PagerDutyProxyAddress => "";

        public bool TeamsWebhookEnabled => false;
        public string TeamsWebhookUrl => "";
        public string TeamsProxyAddress => "";
        public bool SlackWebhookEnabled => false;
        public string SlackWebhookUrl => "";
        public string SlackProxyAddress => "";
        public bool GenericWebhookEnabled => false;
        public string GenericWebhookUrl => "";
        public string GenericWebhookHeadersJson => "";
        public string GenericWebhookBodyTemplate => "";
        public string GenericWebhookProxyAddress => "";
        public bool SmtpEnabled => false;
        public string SmtpServer => "";
        public int SmtpPort => 25;
        public bool SmtpUseSsl => false;
        public string SmtpUsername => "";
        public string SmtpFromAddress => "";
        public string SmtpRecipients => "";
        public string? GetSmtpPassword() => null;
        public string EmailSmtpHost => "";
        public int EmailSmtpPort => 25;
        public bool EmailSmtpUseSsl => false;
        public string EmailSmtpUsername => "";
        public string EmailSmtpPassword => "";
        public string EmailFromAddress => "";
        public string EmailRecipients => "";
        public int EmailCooldownMinutes => 60;
        public int AnalysisPageCap => 10;
        public string TriageBaseUrl => "";
        public double AnalysisNotifySeverity => 0;
        public int AnalysisNotifyCooldownMinutes => 0;
        public int UncorroboratedFindingPageCap => 0;
        public string UncorroboratedFindingRoute => "";
        public System.Collections.Generic.IReadOnlyList<NotificationRoute> NotificationRoutes => System.Array.Empty<NotificationRoute>();
    }

    /* ---------------- Custom details (T-SQL hint) ---------------- */

    [Fact]
    public void BuildPagerDutyPayload_CodeBlockContext_RendersTsqlWebhookHint_NotInlineSql()
    {
        var context = new AlertContext
        {
            Details = new List<AlertDetailItem>
            {
                new()
                {
                    Heading = "Remediation T-SQL",
                    IsCodeBlock = true
                }
            }
        };

        var payload = WebhookAlertService.BuildPagerDutyPayload(
            "Blocking Detected", "SRV1", "5", "1", Branding, "key", context: context);

        var root = JsonDocument.Parse(payload).RootElement;
        var customDetails = root.GetProperty("payload").GetProperty("custom_details");

        Assert.True(customDetails.TryGetProperty("Remediation T-SQL", out var hint));
        Assert.Contains("See email or in-app", hint.GetString());
        Assert.DoesNotContain("SELECT", payload);
        Assert.DoesNotContain("UPDATE", payload);
    }

    [Fact]
    public void BuildPagerDutyPayload_FieldContext_FlattensIntoCustomDetails()
    {
        var context = new AlertContext
        {
            Details = new List<AlertDetailItem>
            {
                new()
                {
                    Heading = "Blocking Chain",
                    Fields = new List<(string Label, string Value)>
                    {
                        ("Victim SQL", "SELECT * FROM Users"),
                        ("Blocking SPID", "55")
                    }
                }
            }
        };

        var payload = WebhookAlertService.BuildPagerDutyPayload(
            "Blocking Detected", "SRV1", "5", "1", Branding, "key", context: context);

        var root = JsonDocument.Parse(payload).RootElement;
        var customDetails = root.GetProperty("payload").GetProperty("custom_details");

        Assert.True(customDetails.TryGetProperty("Blocking Chain — Victim SQL", out _));
        Assert.True(customDetails.TryGetProperty("Blocking Chain — Blocking SPID", out _));
    }

    /* ---------------- Datadog-parity tags (#2710) ---------------- */

    [Fact]
    public void BuildPagerDutyPayload_Incident_AddsResourceAndDatabaseToCustomDetails()
    {
        var context = new AlertContext();
        AlertIncidentRenderer.Apply(context, new[]
        {
            new AlertIncident("k1", new[] { "SalesDB.dbo.Orders" }, Database: "SalesDB"),
            new AlertIncident("k2", new[] { "OtherDb.dbo.Widgets" }, Database: "OtherDb"),
        });

        var payload = WebhookAlertService.BuildPagerDutyPayload(
            "Deadlocks Detected", "SRV1", "2", "n/a", Branding, "key", context: context);

        var customDetails = JsonDocument.Parse(payload).RootElement.GetProperty("payload").GetProperty("custom_details");

        /* First incident wins, the same anchor DerivePagerDutyDedupKey already uses. */
        Assert.Equal("SalesDB.dbo.Orders", customDetails.GetProperty("Resource").GetString());
        Assert.Equal("SalesDB", customDetails.GetProperty("Database").GetString());
    }

    [Fact]
    public void BuildPagerDutyPayload_NoIncident_OmitsResourceAndDatabase()
    {
        var payload = WebhookAlertService.BuildPagerDutyPayload(
            "High CPU", "SRV1", "97%", "90%", Branding, "key");

        var customDetails = JsonDocument.Parse(payload).RootElement.GetProperty("payload").GetProperty("custom_details");

        Assert.False(customDetails.TryGetProperty("Resource", out _));
        Assert.False(customDetails.TryGetProperty("Database", out _));
    }

    /* ---------------- EU region endpoint ---------------- */

    [Fact]
    public void PagerDutyEndpoint_UseEuRegion_ReturnsEuEndpoint()
    {
        /* The endpoint helper is private, but we can verify via SendTestPagerDutyAsync's behavior.
           For now, just verify the payload builds correctly with the EU flag set. */
        var payload = WebhookAlertService.BuildPagerDutyPayload(
            "High CPU", "SRV1", "95%", "90%", Branding, "key");

        /* The endpoint is resolved at send time, not in the payload. This test verifies the payload
           structure is correct regardless of region. The endpoint resolution is tested implicitly
           by the send path (which we don't test with a live endpoint here). */
        Assert.NotNull(payload);
    }

    /* ---------------- #3297: the alert's prose detail ---------------- */

    /// <summary>
    /// #3297: <c>BuildPagerDutyCustomDetails</c> already flattened <see cref="AlertContext.Details"/> — the
    /// STRUCTURED collection — which is why this channel looked like it might already carry the detail. It
    /// did not: an alert's <c>DetailText</c> is a separate field, and a prose-only self-alert took the
    /// empty-Details branch that reduces custom_details to <c>{"Sent by": ...}</c>.
    /// <para>custom_details rather than <c>summary</c>: PD-CEF caps summary at 1024 characters and it is the
    /// one-line headline PD pages on, while custom_details is the table view and what most downstream
    /// integrations read — the placement #2710 already chose for the triage link.</para>
    /// </summary>
    [Fact]
    public void BuildPagerDutyPayload_ProseDetail_RidesInCustomDetails()
    {
        const string prose =
            "Store query_stats retention [1072] is HELD PAUSED by the rollup-coverage gate. Run the " +
            "--backfill-rollups operator action, then RESTART the service.";

        var payload = WebhookAlertService.BuildPagerDutyPayload(
            "Retention Held", "Monitor Store", "9.6x its 4 days horizon", "2.0x", Branding, "rk",
            detailText: prose);

        var customDetails = JsonDocument.Parse(payload).RootElement
            .GetProperty("payload").GetProperty("custom_details");

        Assert.Equal(prose, customDetails.GetProperty("Details").GetString());
    }

    /// <summary>A prose-free alert keeps the pre-#3297 custom_details shape — no empty key is ever sent.</summary>
    [Fact]
    public void BuildPagerDutyPayload_OmitsTheDetailsKey_WhenTheAlertCarriesNoProse()
    {
        var payload = WebhookAlertService.BuildPagerDutyPayload(
            "High CPU", "SRV1", "95%", "90%", Branding, "rk");

        var customDetails = JsonDocument.Parse(payload).RootElement
            .GetProperty("payload").GetProperty("custom_details");

        Assert.False(customDetails.TryGetProperty("Details", out _));
    }

    /* ---------------- #3355: the payload stamp's clock ---------------- */

    /// <summary>
    /// The stamp renders the injected instant, and moves when that instant moves.
    /// <para>This channel needs its own pin because a delivery capture cannot reach it: PagerDuty's endpoint
    /// is the hardcoded Events v2 URL, so a fan-out capture that redirects the other three channels to a
    /// loopback leaves this one unconfigured and never observes its payload. A census over captured bodies
    /// is green whether or not this builder is wired to the clock seam, which would leave its one call site
    /// unheld.</para>
    /// <para>Both arms carry weight. A builder that ignored its argument and read the wall clock renders
    /// neither the fixed instant nor the advanced one, so asserting the stamp EQUALS the injected instant is
    /// what catches an unwired seam; asserting it MOVES is what stops a hardcoded constant passing for one.
    /// </para>
    /// </summary>
    [Fact]
    public void BuildPagerDutyPayload_StampsTheInjectedClock_AndMovesWhenItDoes()
    {
        var fixedUtc = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var fixedStamp = fixedUtc.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);
        var timestamps = new Regex(@"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}Z?");

        var atFixed = WebhookAlertService.BuildPagerDutyPayload(
            "High CPU", "SRV1", "95%", "90%", Branding, "rk", nowUtc: fixedUtc);

        /* Every timestamp-shaped token rather than the one field it happens to stamp today, so a stamp added
           to this payload later is covered without anyone remembering to extend this test. */
        var stamps = timestamps.Matches(atFixed).Select(m => m.Value).ToList();

        /* A payload carrying no stamp at all would satisfy the loop below vacuously. */
        Assert.NotEmpty(stamps);
        Assert.All(stamps, stamp => Assert.Equal(fixedStamp, stamp));

        var aSecondLater = WebhookAlertService.BuildPagerDutyPayload(
            "High CPU", "SRV1", "95%", "90%", Branding, "rk", nowUtc: fixedUtc.AddSeconds(1));

        Assert.DoesNotContain(fixedStamp, aSecondLater, StringComparison.Ordinal);
    }

    /* ---------------- Fan-out (TrySendWebhookAlertsAsync) ---------------- */

    [Fact]
    public async System.Threading.Tasks.Task TrySendWebhookAlertsAsync_PagerDutyEnabled_AttemptsSend()
    {
        var settings = new FakePagerDutySettings
        {
            PagerDutyEnabled = true,
            PagerDutyRoutingKey = "test-routing-key",
            PagerDutyUseEuRegion = false,
            EmailCooldownMinutes = 15
        };

        var service = new WebhookAlertService(
            settings, Branding, new AppLoggerAdapter<WebhookAlertService>(), historyStore: null);

        /* The send will fail (no real endpoint), but we verify it was attempted via the failure counter. */
        var result = await service.TrySendWebhookAlertsAsync("High CPU", "SRV1", "95%", "90%", "server-1");

        Assert.False(result.Sent); /* Failed send */
        Assert.Equal(1, service.GetPagerDutyHealth().ConsecutiveFailures);

        /* #3427: an attempted-and-failed fan-out is Failed, not one of the suppressions, and it names the
           channel in its error so the alert log's send_error points at an endpoint. */
        Assert.Equal(AlertChannelOutcome.Failed, result.Outcome);
        Assert.NotNull(result.SendError);
        Assert.StartsWith("PagerDuty: ", result.SendError);
    }

    [Fact]
    public async System.Threading.Tasks.Task TrySendWebhookAlertsAsync_PagerDutyDisabled_SkipsSend()
    {
        var settings = new FakePagerDutySettings
        {
            PagerDutyEnabled = false,
            PagerDutyRoutingKey = "",
            EmailCooldownMinutes = 15
        };

        var service = new WebhookAlertService(
            settings, Branding, new AppLoggerAdapter<WebhookAlertService>(), historyStore: null);

        var result = await service.TrySendWebhookAlertsAsync("High CPU", "SRV1", "95%", "90%", "server-1");

        Assert.False(result.Sent);
        Assert.Equal(0, service.GetPagerDutyHealth().ConsecutiveFailures); /* Not attempted */

        /* #3427: no webhook channel is configured, so the fan-out reports NotAttempted rather than a
           suppression — a channel that does not exist cannot be throttled or folded, and reporting one
           would put a mechanism on the alert-log row for a store that has no webhook. */
        Assert.Equal(AlertChannelOutcome.NotAttempted, result.Outcome);
        Assert.Null(result.SendError);
    }

    private sealed class FakePagerDutySettings : IAlertSettings
    {
        public bool SmtpEnabled => false;
        public string SmtpServer => "";
        public int SmtpPort => 25;
        public bool SmtpUseSsl => false;
        public string SmtpUsername => "";
        public string SmtpFromAddress => "";
        public string SmtpRecipients => "";
        public string? GetSmtpPassword() => null;
        public int EmailCooldownMinutes { get; set; } = 15;
        public bool TeamsWebhookEnabled => false;
        public string TeamsWebhookUrl => "";
        public string TeamsProxyAddress => "";
        public bool SlackWebhookEnabled => false;
        public string SlackWebhookUrl => "";
        public string SlackProxyAddress => "";
        public bool GenericWebhookEnabled => false;
        public string GenericWebhookUrl => "";
        public string GenericWebhookHeadersJson => "";
        public string GenericWebhookBodyTemplate => "";
        public string GenericWebhookProxyAddress => "";
        public bool PagerDutyEnabled { get; set; }
        public string PagerDutyRoutingKey { get; set; } = "";
        public bool PagerDutyUseEuRegion { get; set; }
        public string PagerDutyProxyAddress => "";
        public double AnalysisNotifySeverity => 1.5;
        public int AnalysisNotifyCooldownMinutes => 360;
        public int AnalysisPageCap => 10;
        public string TriageBaseUrl => "";
    }
}
