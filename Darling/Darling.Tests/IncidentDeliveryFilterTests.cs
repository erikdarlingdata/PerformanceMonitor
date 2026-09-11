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
/// #3313: the per-fingerprint <see cref="IncidentCooldown"/> governed WHETHER a card posted and nothing
/// governed WHICH incidents it contained, so a single fresh fingerprint dragged every co-resident stale one
/// back into the channel with it. Measured on a 42-server store: a card delivered fingerprint
/// <c>bf7e29b4</c> alone, then 41 minutes later re-delivered it as "Deadlock 2 of 2" riding on a new
/// fingerprint, with <c>Total Occurrences: 1</c> proving it had not recurred.
///
/// <para>Both halves are pinned here because either alone is satisfiable by something wrong: "only the
/// fresh incident is delivered" is satisfied by dropping everything, and "the row carries both" is
/// satisfied by not filtering at all.</para>
///
/// <para>The wire-level pins drive the whole Darling path — <see cref="DarlingAlertDeliverer.DeliverAsync"/>
/// through the shared send core, the fan-out and each channel's payload builder — and read the bytes that
/// left the process, for the reason #3297's pin in <see cref="AlertDeliveryChannelTests"/> gives: the defect
/// is in what a builder is HANDED, and a builder-level assertion is green on a fan-out that hands it the
/// wrong thing. Lite runs the same <c>PerformanceMonitor.Notifications</c> send core, so the filter is one
/// fix for both SKUs; <c>Lite.Tests.IncidentCooldownTests</c> pins the decision side of the seam.</para>
/// </summary>
public sealed class IncidentDeliveryFilterTests
{
    /* Fingerprint shapes from the measured case, truncated the way the report quotes them. */
    private const string Stale = "bf7e29b4cooldowninwindow";
    private const string Fresh = "9d5ead0aoutsideitswindow";

    private const string Footer = "1 other incident(s) still open (already reported)";

    private static readonly AlertBranding Branding = new("Test Edition", null);

    /* ─────────────── the filter itself ─────────────── */

    /// <summary>
    /// The defect, at the seam. Two fingerprints, one of them already delivered and still inside its own
    /// window: the render carries the fresh one, no item names the stale one, and the count of what was held
    /// back is stated rather than silently dropped.
    /// </summary>
    [Fact]
    public void AMixedBatch_RendersOnlyTheDeliverableIncident_AndFootnotesTheRest()
    {
        var context = TwoIncidents();

        var render = IncidentDeliveryFilter.ForDelivery(context, null, new[] { Fresh });

        Assert.Equal(1, render.SuppressedIncidentCount);
        Assert.NotSame(context, render.Context);

        var incident = Assert.Single(render.Context!.Incidents!);
        Assert.Equal(Fresh, incident.DedupKey);

        Assert.DoesNotContain(render.Context.Details, DedupKeyIs(Stale));
        Assert.Contains(render.Context.Details, DedupKeyIs(Fresh));

        var footer = Assert.Single(
            render.Context.Details, d => d.Heading == IncidentDeliveryFilter.OtherIncidentsHeading);
        Assert.Contains(footer.Fields, f => f.Value == Footer);

        /* The same batch with the stale fingerprint LAST. Position is not identity, and a filter that
           reduced over the ordered list rather than testing set membership would be right on exactly one
           of the two orders — a mutation of the cooldown's own ordered reduction survived the single-order
           form of this fixture, which is why both are asserted here too. */
        var flipped = new AlertContext();
        AlertIncidentRenderer.Apply(flipped, Incidents().Reverse().ToList());

        var flippedRender = IncidentDeliveryFilter.ForDelivery(flipped, null, new[] { Fresh });

        Assert.Equal(1, flippedRender.SuppressedIncidentCount);
        Assert.Equal(Fresh, Assert.Single(flippedRender.Context!.Incidents!).DedupKey);
        Assert.DoesNotContain(flippedRender.Context.Details, DedupKeyIs(Stale));
    }

    /// <summary>
    /// The control, and #1154's whole point: a batch whose every fingerprint is outside its window delivers
    /// all of them. Asserted by REFERENCE — an unfiltered alert renders the very instance the caller built,
    /// so the four channels and the email template see byte-for-byte what they saw before #3313, and a
    /// filter that rebuilt the context unconditionally (and could therefore drop something on the common
    /// path) fails here rather than in whichever channel assertion happened to notice.
    /// </summary>
    [Fact]
    public void ABatchWhoseEveryFingerprintIsFresh_IsReturnedUnfiltered()
    {
        var context = TwoIncidents();

        var render = IncidentDeliveryFilter.ForDelivery(context, null, new[] { Stale, Fresh });

        Assert.Same(context, render.Context);
        Assert.Equal(0, render.SuppressedIncidentCount);
        Assert.DoesNotContain(
            render.Context!.Details, d => d.Heading == IncidentDeliveryFilter.OtherIncidentsHeading);
    }

    /// <summary>
    /// Delivery filters; persistence does not. The caller writes its <c>config_alert_log</c> row from the
    /// context it still holds AFTER the send, so a filter that mutated in place would silently strip the
    /// history row, the context JSON, the triage page, the Viewer's detail pane and
    /// <c>AlertMuteContext.PopulateFromDetailText</c> of everything it held back.
    /// </summary>
    [Fact]
    public void TheSourceContext_IsNeverMutated()
    {
        var context = TwoIncidents();
        var itemCount = context.Details.Count;

        IncidentDeliveryFilter.ForDelivery(context, null, new[] { Fresh });

        Assert.Equal(2, context.Incidents!.Count);
        Assert.Equal(itemCount, context.Details.Count);
        Assert.Contains(context.Details, DedupKeyIs(Stale));
        Assert.DoesNotContain(context.Details, d => d.Heading == IncidentDeliveryFilter.OtherIncidentsHeading);
    }

    /// <summary>
    /// An alert with no fingerprintable incident is evaluated on the metric-level fallback key, so its
    /// deliverable set is <c>null</c> — "nothing to filter", not "nothing to show". That is the shape of
    /// every CPU / memory / poison-wait / tempdb / failed-job alert AND of the #2109 AG database alerts,
    /// whose <c>SET HADR RESUME</c> prose is the one thing the delivery gate was deliberately built not to
    /// drop: <c>AgAlertContexts.ForDatabase</c> builds one plain detail item and never touches
    /// <see cref="AlertIncidentRenderer"/>, so the filter cannot reach it even in principle. Pinned
    /// anyway — that is a property of a file this change does not touch, and nothing else would notice if
    /// it changed.
    /// </summary>
    [Fact]
    public void AnAlertWithNoFingerprints_IsUnfiltered_AndKeepsItsRemediationProse()
    {
        const string Remedy = "Fix the underlying cause, then resume it with ALTER DATABASE [Sales] SET HADR RESUME.";
        var context = AgAlertContexts.ForDatabase("Sales", "ag-primary", "replica-a", ("Suspend Reason", "SUSPEND_FROM_USER"));

        var render = IncidentDeliveryFilter.ForDelivery(context, Remedy, deliverableDedupKeys: null);

        Assert.Same(context, render.Context);
        Assert.Equal(Remedy, render.Prose);
        Assert.Equal(0, render.SuppressedIncidentCount);
    }

    /// <summary>
    /// Only an incident's OWN item is dropped. Every other item describes the whole alert — the builder's
    /// data section, advice prose, the remediation T-SQL block, and the standalone "Deadlock Victim" item
    /// <c>BuildDeadlockContext</c> keeps for a deadlock whose objects would not parse — and none of them
    /// belongs to a fingerprint, so none of them is a fingerprint's to take away.
    /// </summary>
    [Fact]
    public void ItemsThatBelongToNoIncident_AreAlwaysKept()
    {
        var context = new AlertContext();
        context.Details.Add(new AlertDetailItem { Heading = "Deadlock Victim", Fields = new() { ("Victim SQL", "SELECT 1") } });
        context.Details.Add(new AlertDetailItem { Heading = "What to check", Body = "Investigation: look at the graph." });
        context.Details.Add(new AlertDetailItem { Heading = "Remediation", IsCodeBlock = true, Body = "-- script" });
        AlertIncidentRenderer.Apply(context, Incidents());

        var render = IncidentDeliveryFilter.ForDelivery(context, null, new[] { Fresh });

        Assert.Contains(render.Context!.Details, d => d.Heading == "Deadlock Victim");
        Assert.Contains(render.Context.Details, d => d.Heading == "What to check");
        Assert.Contains(render.Context.Details, d => d.Heading == "Remediation" && d.IsCodeBlock);
    }

    /// <summary>
    /// An incident with a blank fingerprint produces no cooldown key at all (<c>BuildKeys</c> excludes it),
    /// so it can never have been throttled — and must never be filtered out on a key it does not have. It
    /// would otherwise vanish from every card the moment a fingerprinted sibling went into cooldown, which
    /// is the same class of silent loss #3313 is about, pointed the other way.
    /// </summary>
    [Fact]
    public void AnIncidentWithNoFingerprint_IsNeverFilteredOut()
    {
        var context = new AlertContext();
        AlertIncidentRenderer.Apply(context, new[]
        {
            new AlertIncident(Stale, new[] { "SalesDb.dbo.Orders" }),
            new AlertIncident("", new[] { "SalesDb.dbo.Unparsed" }),
            new AlertIncident(Fresh, new[] { "SalesDb.dbo.Shipments" })
        });

        var render = IncidentDeliveryFilter.ForDelivery(context, null, new[] { Fresh });

        Assert.Equal(2, render.Context!.Incidents!.Count);
        Assert.Contains(render.Context.Incidents!, i => i.DedupKey == "");
        Assert.Contains(render.Context.Incidents!, i => i.DedupKey == Fresh);
    }

    /// <summary>
    /// A deliverable set naming none of the alert's fingerprints renders the WHOLE alert rather than an
    /// empty card. It cannot arise from a <c>ShouldSend</c> decision — that says at least one key was fresh
    /// — so it means a caller paired a context with another evaluation's key set, and the two ways to be
    /// wrong there are not symmetric: repeating an incident is noise, while posting a card with no incident
    /// on it destroys the alert.
    /// </summary>
    [Fact]
    public void ADeliverableSetMatchingNothing_RendersTheWholeAlert()
    {
        var context = TwoIncidents();

        var render = IncidentDeliveryFilter.ForDelivery(context, null, Array.Empty<string>());

        Assert.Same(context, render.Context);
        Assert.Equal(0, render.SuppressedIncidentCount);
    }

    /// <summary>
    /// The filter finds an incident's item by the fact name <see cref="AlertIncidentRenderer.BuildItem"/>
    /// emits, so the two must be reading and writing the same string. Read off the SHIPPED item rather than
    /// compared to a literal: a renamed fact with both sides renamed together is fine, while a literal in
    /// the filter that drifted from the renderer would leave every item unmatched and the filter silently
    /// dropping nothing at all.
    /// </summary>
    [Fact]
    public void TheFilterMatchesOnTheFactNameTheRendererEmits()
    {
        var item = AlertIncidentRenderer.BuildItem(
            new AlertIncident(Stale, new[] { "SalesDb.dbo.Orders" }), "Deadlock", includeDetailFields: false);

        Assert.Contains(item.Fields, f => f.Label == AlertIncidentRenderer.DedupKeyFactName && f.Value == Stale);
    }

    /* ─────────────── the wire ─────────────── */

    /// <summary>
    /// <b>The pin that matters.</b> A Summary-mode batch carrying one fingerprint inside its window and one
    /// outside delivers only the fresh one on every channel, while the history row carries both.
    ///
    /// <para>Teams, Slack and the generic channel all point at one loopback endpoint; PagerDuty's endpoint
    /// is the hardcoded Events v2 URL and cannot be redirected, and it takes the same filtered context from
    /// the same single call in the fan-out as the three checked here. Email is asserted too, because it
    /// resolves its own decision against its own key space and its own filter call — a fix applied to the
    /// webhook fan-out alone would pass every webhook assertion here.</para>
    /// </summary>
    [Fact]
    public async Task ASummaryBatch_DeliversOnlyTheFreshFingerprint_WhileTheHistoryRowCarriesBoth()
    {
        using var endpoint = new CapturingWebhookEndpoint();
        using var smtp = new CapturingSmtpEndpoint();

        var history = new SeedingHistoryStore(Stale, DateTime.UtcNow - TimeSpan.FromMinutes(1));
        var deliverer = BuildDeliverer(endpoint, smtp, history);

        var context = TwoIncidents();
        await deliverer.DeliverAsync(Outcome(context), TestContext.Current.CancellationToken);

        var bodies = endpoint.Bodies;
        Assert.Equal(3, bodies.Count);
        Assert.All(bodies, body => Assert.Contains(Fresh, body, StringComparison.Ordinal));
        Assert.All(bodies, body => Assert.DoesNotContain(Stale, body, StringComparison.Ordinal));
        Assert.All(bodies, body => Assert.Contains(Footer, body, StringComparison.Ordinal));

        var message = Assert.Single(smtp.Messages);
        Assert.Contains(Fresh, message, StringComparison.Ordinal);
        Assert.DoesNotContain(Stale, message, StringComparison.Ordinal);
        Assert.Contains(Footer, message, StringComparison.Ordinal);

        /* And the row is whole: both fingerprints in the flat detail AND in the context JSON, which are two
           separately-written columns read by different consumers. */
        var record = Assert.Single(history.Records);
        Assert.Contains(Stale, record.DetailText!, StringComparison.Ordinal);
        Assert.Contains(Fresh, record.DetailText!, StringComparison.Ordinal);
        Assert.Contains(Stale, record.ContextJson!, StringComparison.Ordinal);
        Assert.Contains(Fresh, record.ContextJson!, StringComparison.Ordinal);
        Assert.DoesNotContain(Footer, record.DetailText!, StringComparison.Ordinal);
    }

    /// <summary>
    /// The control on the wire: a batch whose every fingerprint is fresh still delivers all of them, with no
    /// footer. Without it, "deliver only the fresh one" is satisfied by a filter that keeps exactly one
    /// incident per card forever.
    /// </summary>
    [Fact]
    public async Task ABatchWhoseEveryFingerprintIsFresh_DeliversAllOfThem()
    {
        using var endpoint = new CapturingWebhookEndpoint();
        using var smtp = new CapturingSmtpEndpoint();

        var history = new SeedingHistoryStore(seededDedupKey: null, DateTime.UtcNow);
        var deliverer = BuildDeliverer(endpoint, smtp, history);

        await deliverer.DeliverAsync(Outcome(TwoIncidents()), TestContext.Current.CancellationToken);

        var bodies = endpoint.Bodies;
        Assert.Equal(3, bodies.Count);
        Assert.All(bodies, body => Assert.Contains(Stale, body, StringComparison.Ordinal));
        Assert.All(bodies, body => Assert.Contains(Fresh, body, StringComparison.Ordinal));
        Assert.All(bodies, body => Assert.DoesNotContain(
            IncidentDeliveryFilter.OtherIncidentsHeading, body, StringComparison.Ordinal));

        var message = Assert.Single(smtp.Messages);
        Assert.Contains(Stale, message, StringComparison.Ordinal);
        Assert.Contains(Fresh, message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A prose detail that is NOT a flattening of the alert's own context still reaches the wire on a
    /// FILTERED card — the #2109 <c>SET HADR RESUME</c> case, put on the one shape no existing pin covers
    /// (prose and fingerprinted incidents on the same alert).
    ///
    /// <para>This is the interaction worth pinning rather than arguing. Five channel builders re-run
    /// <c>AlertDetailText.ProseForDelivery</c> against whatever context they are handed; the prose decision
    /// is therefore made once, against the UNFILTERED context, since a comparison against the filtered copy
    /// would find them unequal and print the whole flattened alert — resurrecting in prose exactly the
    /// incidents just removed from the structure.</para>
    /// </summary>
    [Fact]
    public async Task AnIndependentProse_StillReachesTheWire_OnAFilteredCard()
    {
        const string Remedy = "Fix the underlying cause, then resume it with ALTER DATABASE [Sales] SET HADR RESUME.";

        using var endpoint = new CapturingWebhookEndpoint();
        using var smtp = new CapturingSmtpEndpoint();

        var history = new SeedingHistoryStore(Stale, DateTime.UtcNow - TimeSpan.FromMinutes(1));
        var deliverer = BuildDeliverer(endpoint, smtp, history);

        var context = TwoIncidents();
        await deliverer.DeliverAsync(
            Outcome(context) with { DetailText = Remedy }, TestContext.Current.CancellationToken);

        Assert.Equal(3, endpoint.Bodies.Count);
        Assert.All(endpoint.Bodies, body => Assert.Contains("SET HADR RESUME", body, StringComparison.Ordinal));
        Assert.All(endpoint.Bodies, body => Assert.DoesNotContain(Stale, body, StringComparison.Ordinal));
        Assert.Contains("SET HADR RESUME", Assert.Single(smtp.Messages), StringComparison.Ordinal);
    }

    /// <summary>
    /// Per-event mode (#1141) does not have this defect and is not changed, established here rather than
    /// asserted from reading: <c>PerEventNotification.Split</c> gives each message a single-incident
    /// context, so an in-window fingerprint fails its OWN <c>ShouldSend</c> and never posts at all. One card
    /// set goes out for the fresh incident and none for the stale one — while both still get a history row,
    /// because per-event records once per split message whether or not a channel accepted it.
    /// </summary>
    [Fact]
    public async Task PerEventMode_PostsTheFreshFingerprintOnly_AndRecordsBoth()
    {
        using var endpoint = new CapturingWebhookEndpoint();
        using var smtp = new CapturingSmtpEndpoint();

        var history = new SeedingHistoryStore(Stale, DateTime.UtcNow - TimeSpan.FromMinutes(1));
        var deliverer = BuildDeliverer(
            endpoint, smtp, history, config => config.Alerts.DeliveryMode = AlertNotificationMode.PerEvent);

        await deliverer.DeliverAsync(Outcome(TwoIncidents()), TestContext.Current.CancellationToken);

        Assert.Equal(3, endpoint.Bodies.Count);
        Assert.All(endpoint.Bodies, body => Assert.Contains(Fresh, body, StringComparison.Ordinal));
        Assert.All(endpoint.Bodies, body => Assert.DoesNotContain(Stale, body, StringComparison.Ordinal));

        /* No footer: a single-incident card held nothing back — the cooldown refused the other message
           outright, which is a different and already-correct mechanism. */
        Assert.All(endpoint.Bodies, body => Assert.DoesNotContain(
            IncidentDeliveryFilter.OtherIncidentsHeading, body, StringComparison.Ordinal));

        Assert.Equal(2, history.Records.Count);
        Assert.Contains(history.Records, r => r.DetailText!.Contains(Stale, StringComparison.Ordinal));
        Assert.Contains(history.Records, r => r.DetailText!.Contains(Fresh, StringComparison.Ordinal));
    }

    /* ─────────────── helpers ─────────────── */

    private static IReadOnlyList<AlertIncident> Incidents() => new[]
    {
        /* TotalOccurrences: 1 on both — the measured card's proof that the repeat was a re-render and not a
           recurrence, carried here so the fixture is the reported shape rather than a convenient one. */
        new AlertIncident(Stale, new[] { "SalesDb.dbo.Orders" }, TotalOccurrences: 1),
        new AlertIncident(Fresh, new[] { "SalesDb.dbo.Shipments" }, TotalOccurrences: 1)
    };

    private static AlertContext TwoIncidents()
    {
        var context = new AlertContext();
        AlertIncidentRenderer.Apply(context, Incidents());
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

    private static Predicate<AlertDetailItem> DedupKeyIs(string dedupKey) =>
        item => item.Fields.Any(
            f => f.Label == AlertIncidentRenderer.DedupKeyFactName && f.Value == dedupKey);

    /// <summary>
    /// Reports one named fingerprint as sent at <paramref name="sentUtc"/> on BOTH channels, and null for
    /// everything else — the restart-seed path (#981/#1145) is how a cooldown learns an incident was already
    /// delivered, and it is also the only way to put a key in the window without sleeping through one.
    /// Both channels, because email and webhook hold separate key spaces and seeding one would leave the
    /// other's filter with nothing to do.
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
