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
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3330: <c>AlertContext.AttachmentXml</c> was one string for a whole alert while the cards carrying it are
/// per-incident, and nothing mapped a captured graph to the fingerprint it belonged to. Per-event mode
/// (#1141) copied that one value onto every split message, so an alert carrying N distinct deadlock
/// fingerprints sent N emails that all attached the FIRST graph in the window — N-1 of them documenting a
/// deadlock their own card did not mention. #3324's delivery filter then made the same mismatch reachable in
/// Summary mode, where the card can now render only its fresh incidents while the attachment belongs to a
/// suppressed one.
///
/// <para>Pinned at the WIRE rather than on the context, for the reason #3297's pin gives: email is the only
/// consumer of the attachment and the only channel with no interception point short of the protocol, so a
/// context-level assertion is green on a send path that hands the template the wrong object. These read the
/// bytes that left the process, attachment part included.</para>
///
/// <para>Every assertion pairs a graph to the fingerprint on the same card. "Each email has an attachment"
/// is true of the defect — that is what made it survive #1146, which put the graph there in the first
/// place.</para>
/// </summary>
public sealed class IncidentAttachmentDeliveryTests
{
    /* Fingerprint shapes from #3313's measured case, so the fixture is a reported shape. */
    private const string Stale = "bf7e29b4cooldowninwindow";
    private const string Fresh = "9d5ead0aoutsideitswindow";

    /* Markers chosen to appear NOWHERE else in a rendered card — not in a dedup key, an involved object, a
       heading or a fact label — so DoesNotContain cannot pass on a near-miss. */
    private const string StaleGraph = "<deadlock marker=\"GRAPH-FOR-ORDERS\"/>";
    private const string FreshGraph = "<deadlock marker=\"GRAPH-FOR-SHIPMENTS\"/>";

    /// <summary>
    /// <b>The pin that matters.</b> An alert carrying two distinct fingerprints, in per-event mode, sends two
    /// emails whose attachments are two DIFFERENT graphs, each the one belonging to its own card.
    /// <para>The alert-level attachment is deliberately a third graph no incident owns, so a message that
    /// still copies it fails rather than coincidentally matching whichever incident came first.</para>
    /// </summary>
    [Fact]
    public async Task PerEventMode_SendsOneEmailPerIncident_EachAttachingItsOwnGraph()
    {
        using var endpoint = new CapturingWebhookEndpoint();
        using var smtp = new CapturingSmtpEndpoint();

        var deliverer = BuildDeliverer(
            endpoint, smtp, new SeedingHistoryStore(null, DateTime.UtcNow),
            config => config.Alerts.DeliveryMode = AlertNotificationMode.PerEvent);

        var context = TwoIncidents(alertLevelXml: "<deadlock marker=\"GRAPH-FOR-NEITHER\"/>");
        await deliverer.DeliverAsync(Outcome(context), TestContext.Current.CancellationToken);

        Assert.Equal(2, smtp.Messages.Count);

        var forOrders = Assert.Single(smtp.Messages, m => m.Contains(Stale, StringComparison.Ordinal));
        var forShipments = Assert.Single(smtp.Messages, m => m.Contains(Fresh, StringComparison.Ordinal));

        /* Each card carries its own graph and NOT the other's — the half the defect got wrong. */
        Assert.Contains(StaleGraph, forOrders, StringComparison.Ordinal);
        Assert.DoesNotContain(FreshGraph, forOrders, StringComparison.Ordinal);
        Assert.Contains(FreshGraph, forShipments, StringComparison.Ordinal);
        Assert.DoesNotContain(StaleGraph, forShipments, StringComparison.Ordinal);

        /* And neither carries the alert-level one, which under the defect was the only thing either had. */
        Assert.All(smtp.Messages, m => Assert.DoesNotContain("GRAPH-FOR-NEITHER", m, StringComparison.Ordinal));

        /* The attachments are real MIME parts, named, not just words in the body. */
        Assert.Equal(2, smtp.RawMessages.Count);
        Assert.All(smtp.RawMessages, raw => Assert.Contains(
            AlertIncidentAttachment.DeadlockGraphFileName, raw, StringComparison.Ordinal));
    }

    /// <summary>
    /// #3330's Summary-mode instance, the one #3324 made reachable: a partially-suppressed batch renders only
    /// its fresh incident while the alert-level attachment belongs to the suppressed one. The email attaches
    /// the graph for the incident the card actually describes.
    /// </summary>
    [Fact]
    public async Task AFilteredSummaryCard_AttachesTheKeptIncidentsGraph_NotTheSuppressedOnes()
    {
        using var endpoint = new CapturingWebhookEndpoint();
        using var smtp = new CapturingSmtpEndpoint();

        var history = new SeedingHistoryStore(Stale, DateTime.UtcNow - TimeSpan.FromMinutes(1));
        var deliverer = BuildDeliverer(endpoint, smtp, history);

        /* The alert-level value the builder would have set: the suppressed incident's graph, because its
           row was first in the window. */
        await deliverer.DeliverAsync(
            Outcome(TwoIncidents(alertLevelXml: StaleGraph)), TestContext.Current.CancellationToken);

        var message = Assert.Single(smtp.Messages);
        Assert.Contains(Fresh, message, StringComparison.Ordinal);
        Assert.DoesNotContain(Stale, message, StringComparison.Ordinal);
        Assert.Contains(FreshGraph, message, StringComparison.Ordinal);
        Assert.DoesNotContain(StaleGraph, message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The control. An UNFILTERED Summary card lists every incident, so the alert-level attachment belongs to
    /// something it describes and is delivered unchanged — this path is not the defect and must not move.
    /// <para>Without this pin, "attach the kept incident's graph" is satisfied by a change that also rewrites
    /// the common single-card send, which is the majority of deliveries.</para>
    /// </summary>
    [Fact]
    public async Task AnUnfilteredSummaryCard_KeepsTheAlertLevelAttachment()
    {
        using var endpoint = new CapturingWebhookEndpoint();
        using var smtp = new CapturingSmtpEndpoint();

        var deliverer = BuildDeliverer(endpoint, smtp, new SeedingHistoryStore(null, DateTime.UtcNow));

        await deliverer.DeliverAsync(
            Outcome(TwoIncidents(alertLevelXml: StaleGraph)), TestContext.Current.CancellationToken);

        var message = Assert.Single(smtp.Messages);
        Assert.Contains(Stale, message, StringComparison.Ordinal);
        Assert.Contains(Fresh, message, StringComparison.Ordinal);
        Assert.Contains(StaleGraph, message, StringComparison.Ordinal);
    }

    /// <summary>
    /// An incident with no forensic file of its own gets no attachment, and the email does not ANNOUNCE one.
    /// <see cref="EmailTemplateBuilder"/> prints "Attached: &lt;name&gt;" from the filename alone while the
    /// send core attaches only when both halves are set, so carrying a filename forward without its XML
    /// would produce a card claiming an attachment it does not have.
    /// <para>A blocking chain seen only by the DMV-snapshot fallback is exactly this case in production, so
    /// it is a routine outcome and not a gap.</para>
    /// </summary>
    [Fact]
    public async Task AnIncidentWithNoGraph_SendsNoAttachmentAndAnnouncesNone()
    {
        using var endpoint = new CapturingWebhookEndpoint();
        using var smtp = new CapturingSmtpEndpoint();

        var deliverer = BuildDeliverer(
            endpoint, smtp, new SeedingHistoryStore(null, DateTime.UtcNow),
            config => config.Alerts.DeliveryMode = AlertNotificationMode.PerEvent);

        var context = new AlertContext
        {
            AttachmentXml = StaleGraph,
            AttachmentFileName = AlertIncidentAttachment.DeadlockGraphFileName
        };
        AlertIncidentRenderer.Apply(context, new[] { new AlertIncident(Fresh, new[] { "SalesDb.dbo.Shipments" }) });

        await deliverer.DeliverAsync(Outcome(context), TestContext.Current.CancellationToken);

        var message = Assert.Single(smtp.Messages);
        Assert.DoesNotContain("GRAPH-FOR-ORDERS", message, StringComparison.Ordinal);
        Assert.DoesNotContain("Attached:", message, StringComparison.Ordinal);
        Assert.DoesNotContain(
            AlertIncidentAttachment.DeadlockGraphFileName,
            Assert.Single(smtp.RawMessages),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// Teams, Slack, PagerDuty and the generic webhook do not consume the attachment, and their payloads are
    /// byte-identical across this change. Established by DELIVERING TWICE — once with per-incident
    /// attachments populated, once with them absent — and comparing the bytes each channel posted, rather
    /// than by grepping the builders for a member they do not currently read.
    /// <para>A grep answers "does any builder name this today"; this answers "can the attachment influence a
    /// webhook payload at all", which is the claim. The two runs' EMAILS differ (one carries files, one does
    /// not), so a run that produced identical bodies by delivering nothing would fail the email half.</para>
    /// <para>PagerDuty's endpoint is the hardcoded Events v2 URL and cannot be redirected to a loopback
    /// listener; it takes the same context from the same call in the fan-out as the three checked here, and
    /// the property under test is "the attachment reaches no webhook builder", which is a property of what
    /// the fan-out HANDS them.</para>
    /// </summary>
    [Fact]
    public async Task TheFourNonEmailChannels_PostIdenticalBytes_WithAndWithoutIncidentAttachments()
    {
        /* Per-event mode, because that is where the delivered attachment actually varies: an unfiltered
           Summary card carries the ALERT-level attachment, which is the same (absent) in both arms — so a
           Summary comparison would find the emails identical too and prove nothing. */
        var withAttachments = await CaptureAsync(TwoIncidents(alertLevelXml: null), perEvent: true);
        var withoutAttachments = await CaptureAsync(TwoIncidentsWithNoAttachments(), perEvent: true);

        /* Two split messages x three redirectable channels. Ordered before comparing: the property is that
           the SET of bytes each run posted is the same, and the fan-out's ordering is not under test. */
        Assert.Equal(6, withAttachments.Webhooks.Count);
        Assert.Equal(
            withAttachments.Webhooks.OrderBy(b => b, StringComparer.Ordinal).ToList(),
            withoutAttachments.Webhooks.OrderBy(b => b, StringComparer.Ordinal).ToList());

        /* And the direct reading of the same claim, independent of that comparison: no webhook body names
           an attachment at all, on either arm. */
        Assert.All(withAttachments.Webhooks, body =>
        {
            Assert.DoesNotContain("GRAPH-FOR-", body, StringComparison.Ordinal);
            Assert.DoesNotContain(AlertIncidentAttachment.DeadlockGraphFileName, body, StringComparison.Ordinal);
        });

        /* The control on the control: the EMAILS did differ, so the comparison above was not comparing two
           runs that both delivered nothing. */
        Assert.Equal(2, withAttachments.Emails.Count);
        Assert.Contains(withAttachments.Emails, e => e.Contains(FreshGraph, StringComparison.Ordinal));
        Assert.All(withoutAttachments.Emails, e => Assert.DoesNotContain("GRAPH-FOR-", e, StringComparison.Ordinal));
    }

    /* ─────────────── helpers ─────────────── */

    private sealed record Captured(IReadOnlyList<string> Webhooks, IReadOnlyList<string> Emails);

    private static async Task<Captured> CaptureAsync(AlertContext context, bool perEvent = false)
    {
        using var endpoint = new CapturingWebhookEndpoint();
        using var smtp = new CapturingSmtpEndpoint();

        var deliverer = BuildDeliverer(
            endpoint, smtp, new SeedingHistoryStore(null, DateTime.UtcNow),
            perEvent ? config => config.Alerts.DeliveryMode = AlertNotificationMode.PerEvent : null);
        await deliverer.DeliverAsync(Outcome(context), TestContext.Current.CancellationToken);

        return new Captured(endpoint.Bodies.ToList(), smtp.Messages.ToList());
    }

    /// <summary>
    /// Two fingerprinted deadlock incidents, each carrying its own graph, on an alert whose alert-level
    /// attachment is <paramref name="alertLevelXml"/> (null for none) — the shape both builders produce.
    /// </summary>
    private static AlertContext TwoIncidents(string? alertLevelXml)
    {
        var context = new AlertContext
        {
            AttachmentXml = alertLevelXml,
            AttachmentFileName = alertLevelXml is null ? null : AlertIncidentAttachment.DeadlockGraphFileName
        };
        AlertIncidentRenderer.Apply(context, new[]
        {
            new AlertIncident(Stale, new[] { "SalesDb.dbo.Orders" }, TotalOccurrences: 1,
                Attachment: new AlertIncidentAttachment(StaleGraph, AlertIncidentAttachment.DeadlockGraphFileName)),
            new AlertIncident(Fresh, new[] { "SalesDb.dbo.Shipments" }, TotalOccurrences: 1,
                Attachment: new AlertIncidentAttachment(FreshGraph, AlertIncidentAttachment.DeadlockGraphFileName))
        });
        return context;
    }

    /// <summary>The same two incidents with no attachment anywhere — the webhook comparison's other arm.</summary>
    private static AlertContext TwoIncidentsWithNoAttachments()
    {
        var context = new AlertContext();
        AlertIncidentRenderer.Apply(context, new[]
        {
            new AlertIncident(Stale, new[] { "SalesDb.dbo.Orders" }, TotalOccurrences: 1),
            new AlertIncident(Fresh, new[] { "SalesDb.dbo.Shipments" }, TotalOccurrences: 1)
        });
        return context;
    }

    /* The engine's own shape: DetailText is ContextToDetailText of the context, which is what makes
       ProseForDelivery recognise it as a restatement and suppress it. */
    private static AlertOutcome Outcome(AlertContext context) => new(
        "7", "PROD01", "Deadlocks Detected", "2", "1",
        context, AlertContextBuilders.ContextToDetailText(context),
        NumericCurrentValue: 2, NumericThresholdValue: 1, Muted: false, Severity: null);

    private static DarlingAlertDeliverer BuildDeliverer(
        CapturingWebhookEndpoint endpoint,
        CapturingSmtpEndpoint smtp,
        IAlertHistoryStore history,
        Action<DarlingConfig>? configure = null)
    {
        var config = new DarlingConfig();
        config.Webhooks.TeamsUrl = endpoint.Url;
        config.Webhooks.SlackUrl = endpoint.Url;
        config.Webhooks.GenericUrl = endpoint.Url;
        config.Smtp.Host = "127.0.0.1";
        config.Smtp.Port = smtp.Port;
        config.Smtp.UseSsl = false;
        config.Smtp.From = "monitor@example.invalid";
        config.Smtp.To = "operator@example.invalid";
        configure?.Invoke(config);

        var settings = new DarlingAlertSettings(config);
        var webhooks = new WebhookAlertService(
            settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        return new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance);
    }

    /// <summary>
    /// Reports one named fingerprint as sent at <paramref name="sentUtc"/> on BOTH channels, and null for
    /// everything else — the restart-seed path (#981/#1145) is how a cooldown learns an incident was already
    /// delivered, and the only way to put a key inside its window without sleeping through one.
    /// </summary>
    private sealed class SeedingHistoryStore : IAlertHistoryStore
    {
        private readonly string? _seededDedupKey;
        private readonly DateTime _sentUtc;

        public SeedingHistoryStore(string? seededDedupKey, DateTime sentUtc)
        {
            _seededDedupKey = seededDedupKey;
            _sentUtc = sentUtc;
        }

        public List<AlertHistoryRecord> Records { get; } = new();

        public Task RecordAlertAsync(AlertHistoryRecord record)
        {
            Records.Add(record);
            return Task.CompletedTask;
        }

        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Seed(dedupKey);

        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Seed(dedupKey);

        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) =>
            Seed(dedupKey);

        private Task<DateTime?> Seed(string? dedupKey) =>
            Task.FromResult<DateTime?>(
                _seededDedupKey is not null && string.Equals(dedupKey, _seededDedupKey, StringComparison.Ordinal)
                    ? _sentUtc
                    : null);
    }
}
