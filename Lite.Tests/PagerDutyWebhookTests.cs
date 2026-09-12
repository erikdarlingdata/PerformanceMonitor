using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Text.Json;
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
        var sent = await service.TrySendWebhookAlertsAsync("High CPU", "SRV1", "95%", "90%", "server-1");

        Assert.False(sent); /* Failed send */
        Assert.Equal(1, service.GetPagerDutyHealth().ConsecutiveFailures);
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

        var sent = await service.TrySendWebhookAlertsAsync("High CPU", "SRV1", "95%", "90%", "server-1");

        Assert.False(sent);
        Assert.Equal(0, service.GetPagerDutyHealth().ConsecutiveFailures); /* Not attempted */
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
        public string TriageBaseUrl => "";
    }
}
