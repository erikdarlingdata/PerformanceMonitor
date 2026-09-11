/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3169: <c>config_alert_log.alert_sent</c> meant "delivered" on a fired row and "no send channel
/// applies" on a resolution row, so <c>false</c> could not be told from never-attempted and no aggregate
/// over the column meant anything.
///
/// <para>These pin the Darling half: the resolution record states its channel instead of claiming a
/// delivery, the headless service never writes a <c>tray</c> row, and the Viewer's status column renders
/// through the one shared describer both SKUs use. Lite's half — including that its stored values are
/// UNCHANGED — is <c>Lite.Tests.AlertDeliveryChannelTests</c>.</para>
/// </summary>
public sealed class AlertDeliveryChannelTests
{
    /* ─────────────── the resolution record: a stated channel, not a claimed delivery ─────────────── */

    /// <summary>
    /// The defect itself. <c>BuildResolutionRecord</c> was the only construction site in non-test code that
    /// named <c>AlertSent:</c> explicitly, and it said <c>true</c> — documented as meaning "a resolution has
    /// no send channel", not "something was delivered". Measured across three live stores on 2026-09-08:
    /// 32,546 stored rows, 5,246 of them <c>alert_sent = true</c>, and no send channel configured on any of
    /// the three — so not one of those <c>true</c>s was a delivery. In the seven-day window where the census
    /// was broken down per metric (11,521 rows, 2,391 of them <c>true</c>), every single <c>true</c> row
    /// carried a resolution title. The per-metric breakdown is what supports that last claim, so it is
    /// stated for the window it was measured in rather than for all 5,246.
    /// </summary>
    [Fact]
    public void TheResolutionRecord_StatesNoChannel_RatherThanClaimingDelivery()
    {
        var record = DarlingSelfAlertEvaluator.BuildResolutionRecord(
            new AlertResolution("7", "Srv", "High CPU", "CPU Resolved", "back under the threshold"));

        Assert.False(record.AlertSent);
        Assert.Equal(AlertDelivery.ChannelNotApplicable, record.NotificationType);
        Assert.Null(record.SendError);
    }

    /// <summary>
    /// The pairing <c>BuildResolutionRecord</c> exists for, pinned separately from the boolean so a repair
    /// of the boolean cannot quietly cost it: an operator reviewing history must still see "Detected" then
    /// "Cleared" as a pair, which means the row is still written, still carries the resolution title, and
    /// still classifies as resolved.
    /// </summary>
    [Fact]
    public void TheResolutionRecord_StillPairsWithItsDetection()
    {
        var record = DarlingSelfAlertEvaluator.BuildResolutionRecord(
            new AlertResolution("7", "Srv", "Blocking Detected", "Blocking Cleared", "no blocking"));

        Assert.Equal("Blocking Cleared", record.MetricName);
        Assert.True(AlertMetricClassifier.IsResolution(record.MetricName));
        Assert.Equal("resolved", record.CurrentValueText);
        Assert.Equal("no blocking", record.DetailText);
    }

    /// <summary>A row with no channel is never a delivered row, whatever else is true of it.</summary>
    [Fact]
    public void NoChannelApplies_IsNeverSent()
    {
        var delivery = AlertDelivery.NoChannelApplies();

        Assert.False(delivery.Sent);
        Assert.Equal(AlertDelivery.ChannelNotApplicable, delivery.Channel);
        Assert.Null(delivery.SendError);
    }

    /* ─────────────── the invariant that makes alert_sent decodable again ─────────────── */

    /// <summary>
    /// <b>Sent implies a named delivering channel</b>, over the WHOLE input domain rather than the
    /// combinations the send core happens to produce. Enumerating every route to a violation is the point:
    /// <c>EmailFanoutResult</c> carries four independent bools, and an <c>EmailSent</c> without an
    /// <c>EmailAttempted</c> — which the send core never emits but the type freely represents — would
    /// otherwise report a delivery on a channel that never named itself, putting a <c>true</c> back
    /// alongside <c>tray</c> and re-breaking the legacy decode in
    /// <see cref="TheLegacyResolutionSignature_DecodesToNoChannel"/>.
    /// </summary>
    [Fact]
    public void EverySentDisposition_NamesADeliveringChannel()
    {
        var delivering = new[]
        {
            AlertDelivery.ChannelEmail, AlertDelivery.ChannelWebhook, AlertDelivery.ChannelEmailAndWebhook,
        };

        var checked_ = 0;
        foreach (var (result, muted, tray) in EveryFanoutCase())
        {
            var delivery = AlertDelivery.FromFanout(result, muted, tray);
            checked_++;

            /* One-directional. The converse is false by design: an attempted email that threw is
               ChannelEmail with Sent false and its error attached. */
            if (delivery.Sent)
            {
                Assert.Contains(delivery.Channel, delivering);
            }
        }

        /* The count IS the claim that the enumeration is the whole domain: 2^4 result bools x send_error
           present-or-not x muted x trayChannelPresent. A shrunken loop would otherwise pass by covering
           less. */
        Assert.Equal(128, checked_);
    }

    /// <summary>
    /// The decode-critical half of the invariant, stated on its own rather than left as a corollary: no
    /// <c>Sent</c> row lands on a channel that did not deliver. This is precisely what makes
    /// <c>alert_sent = true</c> with <c>notification_type = 'tray'</c> unreachable for a new row, and so
    /// what licenses <see cref="TheLegacyResolutionSignature_DecodesToNoChannel"/> to read that signature
    /// as the resolution builder's old constant.
    /// </summary>
    [Fact]
    public void NoSentRow_LandsOnANonDeliveringChannel()
    {
        string[] nonDelivering =
        {
            AlertDelivery.ChannelTray, AlertDelivery.ChannelNotApplicable,
            AlertDelivery.ChannelNoneConfigured, AlertDelivery.ChannelMuted, AlertDelivery.ChannelUndelivered,
        };

        foreach (var (result, muted, tray) in EveryFanoutCase())
        {
            var delivery = AlertDelivery.FromFanout(result, muted, tray);

            if (delivery.Sent)
            {
                Assert.DoesNotContain(delivery.Channel, nonDelivering);
            }
        }
    }

    /// <summary><c>Sent</c> is exactly "a channel delivered", with no other contributor.</summary>
    [Fact]
    public void Sent_IsEmailSentOrWebhookSent_AndNothingElse()
    {
        foreach (var (result, muted, tray) in EveryFanoutCase())
        {
            Assert.Equal(
                result.EmailSent || result.WebhookSent,
                AlertDelivery.FromFanout(result, muted, tray).Sent);
        }
    }

    /// <summary>
    /// <b>No state-carrying channel can carry a <c>send_error</c>.</b> Over the whole input domain, a
    /// non-null error implies <see cref="AlertDelivery.DeliveringChannels"/> — because the error can only
    /// come from the SMTP attempt, so its presence is itself email involvement.
    ///
    /// <para>This is what licenses two surfaces to check in different orders. The web dashboard tests
    /// <c>send_error</c> before its state lookup; <see cref="AlertDeliveryStatus.Describe"/> tests the state
    /// channels first. Equivalent, but only because the two cases can never co-occur — which the review of
    /// this change correctly noted was true of the reachable subset rather than of the type. It is now true
    /// of the type.</para>
    ///
    /// <para>Residual, stated rather than papered over: a row PERSISTED by an earlier version could still
    /// hold a state channel beside an error, and the two surfaces would label it differently. No producer
    /// can write one, and none of the 32,546 rows on the three live stores carries a non-null
    /// <c>send_error</c> at all.</para>
    /// </summary>
    [Fact]
    public void NoStateCarryingChannel_CanCarryASendError()
    {
        foreach (var (result, muted, tray) in EveryFanoutCase())
        {
            var delivery = AlertDelivery.FromFanout(result, muted, tray);

            if (delivery.SendError is not null)
            {
                Assert.Contains(delivery.Channel, AlertDelivery.DeliveringChannels);
                Assert.DoesNotContain(delivery.Channel, AlertDelivery.StateCarryingChannels);
            }
        }
    }

    /* ─────────────── the three states that used to be one `false` ─────────────── */

    /// <summary>
    /// The state every fired alert on Erik's three live stores is actually in, and the one the column could
    /// not express: no SMTP and no webhook configured, so nothing was attempted. Measured 2026-09-08 —
    /// all nine channel flags empty in <c>config_notification</c> on all three, never edited since seed —
    /// which is why these rows' <c>false</c> was correct all along and still told a reader nothing.
    /// </summary>
    [Fact]
    public void WithNoChannelConfigured_TheDispositionSaysSo()
    {
        var delivery = AlertDelivery.FromFanout(
            Fanout(anyChannelConfigured: false), muted: false, trayChannelPresent: false);

        Assert.False(delivery.Sent);
        Assert.Equal(AlertDelivery.ChannelNoneConfigured, delivery.Channel);
    }

    /// <summary>
    /// Configured, consulted, nothing delivered — a throttled send or a webhook post that came back
    /// unsuccessful. Distinct from <see cref="AlertDelivery.ChannelNoneConfigured"/>, which is the whole
    /// point: the two want different operator responses and used to arrive identically.
    /// </summary>
    [Fact]
    public void WithAChannelConfiguredAndNothingDelivered_TheDispositionIsUndelivered()
    {
        var delivery = AlertDelivery.FromFanout(
            Fanout(anyChannelConfigured: true), muted: false, trayChannelPresent: false);

        Assert.False(delivery.Sent);
        Assert.Equal(AlertDelivery.ChannelUndelivered, delivery.Channel);
    }

    /// <summary>An attempt that threw keeps its error, and reads as a failure rather than as an absence.</summary>
    [Fact]
    public void AnAttemptThatFailed_KeepsItsErrorAndReadsAsFailed()
    {
        var delivery = AlertDelivery.FromFanout(
            Fanout(emailAttempted: true, sendError: "relay refused", anyChannelConfigured: true),
            muted: false, trayChannelPresent: false);

        Assert.False(delivery.Sent);
        Assert.Equal(AlertDelivery.ChannelEmail, delivery.Channel);
        Assert.Equal("relay refused", delivery.SendError);
        Assert.Equal(AlertDeliveryStatus.Failed, Describe(delivery));
    }

    /// <summary>A muted alert is still recorded, and says the channels were suppressed rather than absent.</summary>
    [Fact]
    public void AMutedAlert_SaysSuppressed_NotUnconfigured()
    {
        var delivery = AlertDelivery.FromFanout(
            Fanout(anyChannelConfigured: true), muted: true, trayChannelPresent: false);

        Assert.Equal(AlertDelivery.ChannelMuted, delivery.Channel);
        Assert.Equal(AlertDeliveryStatus.Muted, Describe(delivery));
    }

    /* ─────────────── the deliberate per-SKU divergence, asserted ─────────────── */

    /// <summary>
    /// <b>The headless service has no tray, so it never writes a tray row.</b> Not a style choice: there is
    /// no tray icon and no toast code anywhere under the Darling service, yet all 32,546 rows across the
    /// three live stores carried <c>notification_type = 'tray'</c> — a UI event that cannot occur —
    /// because both Darling producers had copied Lite's taxonomy comment along with its tray fallback.
    /// Asserted here rather than left to the comment, so the answer is remade if a producer is added.
    /// </summary>
    [Fact]
    public void TheHeadlessService_NeverWritesATrayRow()
    {
        foreach (var (result, muted, _) in EveryFanoutCase())
        {
            Assert.NotEqual(
                AlertDelivery.ChannelTray,
                AlertDelivery.FromFanout(result, muted, trayChannelPresent: false).Channel);
        }
    }

    /// <summary>
    /// Both Darling producers state <c>trayChannelPresent: false</c>. The named argument is what makes a
    /// transposition a compile error rather than a silent flip, and this is what makes ADDING a producer
    /// that forgets the answer a red test rather than another storeful of untrue rows.
    /// </summary>
    [Fact]
    public void BothDarlingProducers_DeclareNoTrayChannel()
    {
        string[] producers =
        {
            "Darling/PerformanceMonitor.Darling.Service/DarlingAlertDeliverer.cs",
            "Darling/PerformanceMonitor.Darling.Service/DarlingFindingAlertSender.cs",
        };

        foreach (var relative in producers)
        {
            var text = File.ReadAllText(RepoPath(relative));
            Assert.Contains("trayChannelPresent: false", text, StringComparison.Ordinal);
            Assert.DoesNotContain("trayChannelPresent: true", text, StringComparison.Ordinal);
        }

        /* Every FromFanout call under the Darling tree is one of those two, so the pair above is the
           whole population rather than a sample of it. */
        var callers = SourceFilesUnder("Darling")
            .Where(f => File.ReadAllText(f).Contains("AlertDelivery.FromFanout(", StringComparison.Ordinal))
            .Where(f => !f.Contains("Darling.Tests", StringComparison.Ordinal))
            .Select(f => f.Replace('\\', '/'))
            .ToList();

        Assert.Equal(2, callers.Count);
        Assert.All(callers, c => Assert.Contains("PerformanceMonitor.Darling.Service", c, StringComparison.Ordinal));
    }

    /* ─────────────── the read side: one vocabulary, and old rows left alone ─────────────── */

    /// <summary>
    /// Every disposition renders as something an operator can act on, and no two states that want different
    /// responses collapse onto one label.
    /// </summary>
    [Theory]
    [InlineData(false, AlertDelivery.ChannelNotApplicable, null, AlertDeliveryStatus.NoChannel)]
    [InlineData(false, AlertDelivery.ChannelNoneConfigured, null, AlertDeliveryStatus.NoChannelConfigured)]
    [InlineData(false, AlertDelivery.ChannelUndelivered, null, AlertDeliveryStatus.NotSent)]
    [InlineData(false, AlertDelivery.ChannelMuted, null, AlertDeliveryStatus.Muted)]
    [InlineData(false, AlertDelivery.ChannelEmail, "relay refused", AlertDeliveryStatus.Failed)]
    [InlineData(false, AlertDelivery.ChannelEmail, null, AlertDeliveryStatus.NotSent)]
    [InlineData(true, AlertDelivery.ChannelEmail, null, AlertDeliveryStatus.Delivered)]
    [InlineData(true, AlertDelivery.ChannelWebhook, null, AlertDeliveryStatus.Delivered)]
    [InlineData(true, AlertDelivery.ChannelEmailAndWebhook, null, AlertDeliveryStatus.Delivered)]
    [InlineData(true, AlertDelivery.ChannelTray, null, AlertDeliveryStatus.NoChannel)]
    [InlineData(false, AlertDelivery.ChannelTray, null, AlertDeliveryStatus.Logged)]
    public void Describe_OnADarlingStore_DiscriminatesEveryDisposition(
        bool sent, string channel, string? sendError, string expected)
        => Assert.Equal(expected, AlertDeliveryStatus.Describe(sent, channel, sendError, producerHadTrayChannel: false));

    /// <summary>
    /// <b>The one legacy signature that can be decoded.</b> <c>alert_sent = true</c> with
    /// <c>notification_type = 'tray'</c> is unreachable for a new row —
    /// <see cref="EverySentDisposition_NamesADeliveringChannel"/> is what holds that — and it is what the
    /// resolution builder's hardcoded constant wrote, on either SKU, because that builder is shared. So it
    /// renders as what it always meant rather than as a delivery that never happened.
    /// </summary>
    [Fact]
    public void TheLegacyResolutionSignature_DecodesToNoChannel()
    {
        Assert.Equal(AlertDeliveryStatus.NoChannel,
            AlertDeliveryStatus.Describe(true, AlertDelivery.ChannelTray, null, producerHadTrayChannel: false));
        Assert.Equal(AlertDeliveryStatus.NoChannel,
            AlertDeliveryStatus.Describe(true, AlertDelivery.ChannelTray, null, producerHadTrayChannel: true));
    }

    /// <summary>
    /// <b>And the one that is NOT decoded.</b> A legacy <c>false</c> + <c>tray</c> covers no-channel,
    /// throttled and failed-webhook alike, and nothing in the row separates them, so it gets an
    /// outcome-only label that makes no claim about configuration. Reading it as
    /// <see cref="AlertDeliveryStatus.NoChannelConfigured"/> would be asserting the new semantics over rows
    /// written before them — every existing row on all three stores predates this change, and on the busiest
    /// of them 8,085 rows in a single seven-day window are exactly this shape.
    /// </summary>
    [Fact]
    public void TheLegacyDetectedSignature_IsNotReinterpreted()
    {
        var status = AlertDeliveryStatus.Describe(false, AlertDelivery.ChannelTray, null, producerHadTrayChannel: false);

        /* #2781's word for it, adopted rather than replaced with a second one. It claims nothing in either
           direction, which is the only honest reading of a row whose state cannot be recovered. */
        Assert.Equal(AlertDeliveryStatus.Logged, status);
        Assert.NotEqual(AlertDeliveryStatus.NoChannelConfigured, status);
        Assert.NotEqual(AlertDeliveryStatus.Shown, status);
        Assert.NotEqual(AlertDeliveryStatus.Delivered, status);
    }

    /// <summary>
    /// The Darling Viewer said "Shown" for every fired alert and "Delivered" for every resolution, because
    /// its copy of the renderer branched on <c>notification_type == "email"</c> and, with no SMTP
    /// configured, that arm never ran. Nothing a Darling store can hold may read as a UI event now.
    /// </summary>
    [Fact]
    public void Describe_OnADarlingStore_NeverSaysShown()
    {
        foreach (var channel in EveryStoredChannelValue())
        {
            foreach (var sent in new[] { false, true })
            {
                foreach (var error in new[] { null, "relay refused" })
                {
                    Assert.NotEqual(
                        AlertDeliveryStatus.Shown,
                        AlertDeliveryStatus.Describe(sent, channel, error, producerHadTrayChannel: false));
                }
            }
        }
    }

    /// <summary>
    /// The Viewer's grid column IS the shared describer's answer, not a copy that agrees today. If a local
    /// copy is reintroduced it drifts the moment the describer changes, and this goes red.
    /// </summary>
    [Fact]
    public void TheViewerRow_RendersThroughTheSharedDescriber()
    {
        var cases = new (bool Sent, string Channel, string? Error)[]
        {
            (false, AlertDelivery.ChannelNotApplicable, null),
            (false, AlertDelivery.ChannelNoneConfigured, null),
            (false, AlertDelivery.ChannelUndelivered, null),
            (false, AlertDelivery.ChannelMuted, null),
            (false, AlertDelivery.ChannelEmail, "relay refused"),
            (true, AlertDelivery.ChannelEmailAndWebhook, null),
            (true, AlertDelivery.ChannelTray, null),
            (false, AlertDelivery.ChannelTray, null),
        };

        foreach (var (sent, channel, error) in cases)
        {
            var row = new ViewerAlertRow
            {
                AlertTime = new DateTime(2026, 9, 8, 1, 0, 0, DateTimeKind.Utc),
                MetricName = "High CPU",
                CurrentValue = 92,
                ThresholdValue = 90,
                AlertSent = sent,
                NotificationType = channel,
                SendError = error,
                Muted = false,
            };

            Assert.Equal(
                AlertDeliveryStatus.Describe(sent, channel, error, producerHadTrayChannel: false),
                row.StatusDisplay);
        }
    }

    /// <summary>
    /// <b>The per-SKU divergence is exactly one cell, and this says which.</b> Over every
    /// (channel, sent, send_error) combination the column can hold, the two SKUs' renderings differ for
    /// <c>tray</c> with <c>alert_sent = false</c> and nothing else — "Shown" where a toast really was shown,
    /// #2781's "Logged" where the producer had no tray.
    ///
    /// <para>Stated as a count rather than as prose because "there is a deliberate divergence" is not a
    /// falsifiable claim and this is: widening it, or collapsing it, fails here. A test that merely checked
    /// the one known cell would pass while a second divergence appeared beside it.</para>
    /// </summary>
    [Fact]
    public void TheTwoSkus_DivergeOnExactlyOneStoredCombination()
    {
        var diverging = new List<string>();

        foreach (var channel in EveryStoredChannelValue())
        {
            foreach (var sent in new[] { false, true })
            {
                foreach (var error in new string?[] { null, "relay refused" })
                {
                    var headless = AlertDeliveryStatus.Describe(sent, channel, error, producerHadTrayChannel: false);
                    var withTray = AlertDeliveryStatus.Describe(sent, channel, error, producerHadTrayChannel: true);

                    if (headless != withTray)
                    {
                        diverging.Add($"{channel}/sent={sent}/error={(error is null ? "null" : "set")}");
                    }
                }
            }
        }

        Assert.Equal(new[] { $"{AlertDelivery.ChannelTray}/sent=False/error=null" }, diverging.ToArray());
    }

    /* ─────────────── the hatch stays where it was put ─────────────── */

    /// <summary>
    /// <c>FromLegacyStoredColumns</c> is the only way left to hand-write a disposition, and it exists for
    /// the deprecated Dashboard SKU, whose 26 callers pass an already-decided pair as method parameters.
    /// It is <c>internal</c>, but <c>InternalsVisibleTo</c> on the Notifications project also admits Lite
    /// and both SKUs' test projects — so the boundary is asserted rather than assumed. A live producer
    /// reaching for it fails here instead of quietly reintroducing #3169.
    /// </summary>
    [Fact]
    public void TheLegacyHatch_HasExactlyOneProductionCaller()
    {
        var root = RepoRoot();
        var callers = new List<string>();

        foreach (var file in Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories))
        {
            if (file.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                || file.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            {
                continue;
            }

            var text = File.ReadAllText(file);
            if (!text.Contains("FromLegacyStoredColumns", StringComparison.Ordinal))
            {
                continue;
            }

            var relative = Path.GetRelativePath(root, file).Replace('\\', '/');

            /* Its own declaration, and test code deliberately storing an arbitrary column triple, are not
               producers. Everything else is. */
            if (relative == "PerformanceMonitor.Notifications/AlertDelivery.cs"
                || relative.Contains(".Tests/", StringComparison.Ordinal))
            {
                continue;
            }

            callers.Add(relative);
        }

        Assert.Equal(new[] { "deprecated/Dashboard/Services/EmailAlertService.cs" }, callers.OrderBy(c => c).ToArray());
    }

    /* ─────────────── helpers ─────────────── */

    /// <summary>
    /// Every value <c>notification_type</c> can hold: the declared taxonomy, plus the two shapes a stored
    /// row can carry that the taxonomy does not name — an empty string and a value written by a version
    /// that had a channel this one does not. The reading code must be total over the column, not just over
    /// the constants.
    /// </summary>
    private static IEnumerable<string> EveryStoredChannelValue()
        => AlertDelivery.StateCarryingChannels
            .Concat(AlertDelivery.DeliveringChannels)
            .Concat(new[] { "", "toast" });

    private static string Describe(AlertDelivery delivery)
        => AlertDeliveryStatus.Describe(delivery.Sent, delivery.Channel, delivery.SendError, producerHadTrayChannel: false);

    private static EmailFanoutResult Fanout(
        bool emailAttempted = false, bool emailSent = false, string? sendError = null,
        bool webhookSent = false, bool anyChannelConfigured = false)
        => new(emailAttempted, emailSent, sendError, webhookSent, anyChannelConfigured);

    /// <summary>
    /// Every representable <c>EmailFanoutResult</c> shape crossed with both callers' answers — 128 cases.
    /// <c>SendError</c> is in the cross-product because an arm DOES branch on it: it counts as email
    /// involvement, which is what makes
    /// <see cref="NoStateCarryingChannel_CanCarryASendError"/> hold over the whole domain.
    /// </summary>
    private static IEnumerable<(EmailFanoutResult Result, bool Muted, bool Tray)> EveryFanoutCase()
    {
        foreach (var emailAttempted in new[] { false, true })
        foreach (var emailSent in new[] { false, true })
        foreach (var webhookSent in new[] { false, true })
        foreach (var anyConfigured in new[] { false, true })
        foreach (var sendError in new string?[] { null, "relay refused" })
        foreach (var muted in new[] { false, true })
        foreach (var tray in new[] { false, true })
        {
            yield return (
                new EmailFanoutResult(emailAttempted, emailSent, sendError, webhookSent, anyConfigured),
                muted, tray);
        }
    }

    private static IEnumerable<string> SourceFilesUnder(string relativeDirectory)
        => Directory.EnumerateFiles(Path.Combine(RepoRoot(), relativeDirectory), "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal));

    private static string RepoPath(string relative) => Path.Combine(RepoRoot(), relative);

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));

    /* ─────────────── #3297: the detail text reaches the WIRE, not just the payload builder ─────────────── */

    /// <summary>
    /// #3297: <see cref="AlertOutcome.DetailText"/> reached the history store, the Viewer, the MCP reader and
    /// the triage endpoint — and no delivery channel. <c>WebhookAlertService</c> held zero references to it
    /// across all 1,534 lines, and the email template's detail section was gated on
    /// <see cref="AlertContext.Details"/>, a different and STRUCTURED field, which a self-alert leaves null.
    /// So an operator whose only channel was email received a metric name, a value, a threshold and two
    /// timestamps, with the remedy discarded — reported from the field on #3296.
    ///
    /// <para>This drives the WHOLE Darling path — <see cref="DarlingAlertDeliverer.DeliverAsync"/> through the
    /// shared send core, the fan-out, and each channel's payload builder — and asserts against the bytes that
    /// actually left the process. Deliberately not a payload-builder assertion: the defect was never in a
    /// builder, it was that nothing passed the text TO one, and a builder-level pin is green on a fan-out
    /// that drops the argument. There is no dogfooding path here (no email configured on any of the three
    /// stores, and no webhook channel until one is stood up), so a test is the only instrument.</para>
    ///
    /// <para>Three requests: Teams, Slack and the generic channel all point at the one loopback endpoint.
    /// PagerDuty is absent because its endpoint is the hardcoded Events v2 URL and cannot be redirected —
    /// its builder is pinned in <c>Lite.Tests.PagerDutyWebhookTests</c> instead, and it takes the same
    /// <c>prose</c> from the same single resolution in the fan-out as the three checked here.</para>
    /// </summary>
    [Fact]
    public async Task TheDeliverer_PutsDetailTextOnTheWire_OnEmailAndEveryRedirectableWebhookChannel()
    {
        /* The #3296 alert's own detail, shortened. The marker is the operator action the reporter had to
           work out for themselves because no channel delivered it. */
        const string Prose =
            "Store query_stats retention [1072] is HELD PAUSED by the rollup-coverage gate. Run the " +
            "--backfill-rollups operator action, then RESTART the service.";

        using var endpoint = new CapturingWebhookEndpoint();
        using var smtp = new CapturingSmtpEndpoint();

        var config = new DarlingConfig();
        config.Webhooks.TeamsUrl = endpoint.Url;
        config.Webhooks.SlackUrl = endpoint.Url;
        config.Webhooks.GenericUrl = endpoint.Url;
        config.Smtp.Host = "127.0.0.1";
        config.Smtp.Port = smtp.Port;
        config.Smtp.UseSsl = false;
        config.Smtp.From = "monitor@example.invalid";
        config.Smtp.To = "operator@example.invalid";

        var settings = new DarlingAlertSettings(config);
        var history = new DiscardingHistoryStore();
        var webhooks = new WebhookAlertService(
            settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        var deliverer = new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance);

        /* A self-alert: Context null, prose in DetailText. The shape the defect hid in. */
        await deliverer.DeliverAsync(
            new AlertOutcome(
                "retentionhold:1072", "Monitor Store", "Retention Held", "9.6x its 4 days horizon", "2.0x",
                Context: null, DetailText: Prose, NumericCurrentValue: 9.6, NumericThresholdValue: 2.0,
                Muted: false, Severity: AlertSeverityLevel.Critical),
            TestContext.Current.CancellationToken);

        var bodies = endpoint.Bodies;
        Assert.Equal(3, bodies.Count);
        Assert.All(bodies, body => Assert.Contains("--backfill-rollups", body, StringComparison.Ordinal));
        Assert.All(bodies, body => Assert.Contains("RESTART the service", body, StringComparison.Ordinal));

        /* And email, which is the channel #3296 actually reported and therefore the one that must not be
           covered only at the payload builder. Both MIME parts: the HTML and plain-text bodies are built by
           two different methods and a repair to one says nothing about the other. */
        var message = Assert.Single(smtp.Messages);
        Assert.Contains("--backfill-rollups", message, StringComparison.Ordinal);
        Assert.Contains("RESTART the service", message, StringComparison.Ordinal);
        /* TWICE: once in the text/plain part, once in the text/html one. A single occurrence would mean one
           of the two bodies lost it, which the pre-#3297 code would have reported as a clean pass on the
           other. */
        Assert.Equal(2, CountOccurrences(message, "--backfill-rollups"));
    }

    /// <summary>
    /// #3297 and #3303 arrived at this seam from opposite directions in the same week — the alert's prose
    /// detail and a custom rule's human display name — and every signature from
    /// <see cref="DarlingAlertDeliverer"/> down to each payload builder gained one parameter from each. Both
    /// are <c>string?</c>, so nothing about losing one, or transposing the pair, is a compile error.
    ///
    /// <para>Each side's own suite covers its payload BUILDERS. Neither covers the three hops between the
    /// deliverer and those builders with the OTHER field also present, because neither side had both fields
    /// to pass. This drives the whole path once with both set and reads the bytes that left the process, so
    /// "both survived" is measured at the hops the merge conflict was actually in rather than inferred from
    /// the builders being intact.</para>
    ///
    /// <para>Bodies are identified by a channel-native marker, never by arrival order. And the generic
    /// channel's exemption is asserted here rather than assumed: it carries the prose (alert content) but
    /// keeps the immutable metric name, because <c>{{metric}}</c> is the key an automation correlates on and
    /// that channel renders no title.</para>
    /// </summary>
    [Fact]
    public async Task TheDeliverer_CarriesBothTheProseAndTheDisplayName_ThroughTheOneFanOut()
    {
        const string Prose =
            "Signal wait time has exceeded its ceiling for 15 minutes. Check for a runaway parallel query "
            + "before raising MAXDOP.";
        const string DisplayName = "Signal wait % high";
        const string MetricKey = "Custom:42";

        using var endpoint = new CapturingWebhookEndpoint();
        using var smtp = new CapturingSmtpEndpoint();

        var config = new DarlingConfig();
        config.Webhooks.TeamsUrl = endpoint.Url;
        config.Webhooks.SlackUrl = endpoint.Url;
        config.Webhooks.GenericUrl = endpoint.Url;
        config.Smtp.Host = "127.0.0.1";
        config.Smtp.Port = smtp.Port;
        config.Smtp.UseSsl = false;
        config.Smtp.From = "monitor@example.invalid";
        config.Smtp.To = "operator@example.invalid";

        var settings = new DarlingAlertSettings(config);
        var history = new DiscardingHistoryStore();
        var webhooks = new WebhookAlertService(
            settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        var deliverer = new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance);

        /* A custom-rule fire, the shape CustomAlertEvaluator.BuildFireOutcome produces: Context null, the
           rule's prose in DetailText, the rule's name in DisplayName, "Custom:<id>" as the metric key. */
        await deliverer.DeliverAsync(
            new AlertOutcome(
                "custom:42", "PROD01", MetricKey, "1500", ">= 1000",
                Context: null, DetailText: Prose, NumericCurrentValue: 1500, NumericThresholdValue: 1000,
                Muted: false, Severity: AlertSeverityLevel.Warning,
                ShortMessage: null, DisplayName: DisplayName),
            TestContext.Current.CancellationToken);

        var bodies = endpoint.Bodies;
        Assert.Equal(3, bodies.Count);

        /* The prose is alert CONTENT, so every channel carries it. */
        Assert.All(bodies, body => Assert.Contains(Prose, body, StringComparison.Ordinal));

        /* The display name is a TITLE concern, so the two card channels carry it. */
        var teams = Assert.Single(bodies, b => b.Contains("themeColor", StringComparison.Ordinal));
        Assert.Contains(DisplayName, teams, StringComparison.Ordinal);
        Assert.DoesNotContain(MetricKey, teams, StringComparison.Ordinal);

        var slack = Assert.Single(bodies, b => b.Contains("\"blocks\"", StringComparison.Ordinal));
        Assert.Contains(DisplayName, slack, StringComparison.Ordinal);
        Assert.DoesNotContain(MetricKey, slack, StringComparison.Ordinal);

        /* And the generic channel keeps the machine key instead. */
        var generic = Assert.Single(bodies, b => b.Contains("\"metric\"", StringComparison.Ordinal));
        Assert.Contains(MetricKey, generic, StringComparison.Ordinal);
        Assert.DoesNotContain(DisplayName, generic, StringComparison.Ordinal);

        /* Email is the channel #3296 was reported from, and its subject is where #3303's name shows. Both
           MIME parts again, so losing either body fails. */
        var message = Assert.Single(smtp.Messages);
        Assert.Contains(DisplayName, message, StringComparison.Ordinal);
        Assert.Contains("before raising MAXDOP", message, StringComparison.Ordinal);
        Assert.Equal(2, CountOccurrences(message, "before raising MAXDOP"));
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        int count = 0, index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }

        return count;
    }

    /// <summary>
    /// A loopback endpoint that records the bodies posted to it. <see cref="System.Net.Sockets.TcpListener"/>
    /// rather than <c>HttpListener</c> on purpose: HttpListener wants a URL ACL on Windows, and the four
    /// lines of HTTP a webhook POST needs are cheaper than that dependency. The same choice
    /// <c>NpgsqlRootCertificateValidationTests</c> and <c>DarlingStoreUpgradeTests</c> already make.
    /// </summary>
    private sealed class CapturingWebhookEndpoint : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly List<string> _bodies = new();
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _accepting;

        public CapturingWebhookEndpoint()
        {
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            Url = $"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}/hook";
            _accepting = Task.Run(AcceptLoopAsync);
        }

        public string Url { get; }

        /// <summary>
        /// Safe to read without synchronization once the send has been awaited: each body is appended before
        /// its response is written, and the sender awaits every response in turn.
        /// </summary>
        public IReadOnlyList<string> Bodies => _bodies;

        private async Task AcceptLoopAsync()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    using var stream = client.GetStream();
                    _bodies.Add(await ReadRequestBodyAsync(stream));

                    var response = Encoding.ASCII.GetBytes(
                        "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    await stream.WriteAsync(response, _stop.Token);
                    await stream.FlushAsync(_stop.Token);
                }
            }
            catch (OperationCanceledException) { /* Dispose */ }
            catch (System.Net.Sockets.SocketException) { /* listener stopped */ }
            catch (ObjectDisposedException) { /* listener stopped */ }
        }

        /// <summary>Reads headers to the blank line, then exactly Content-Length bytes of body.</summary>
        private static async Task<string> ReadRequestBodyAsync(NetworkStream stream)
        {
            var buffer = new byte[16 * 1024];
            var received = new List<byte>(capacity: 16 * 1024);
            int headerEnd;

            while ((headerEnd = IndexOfHeaderEnd(received)) < 0)
            {
                var read = await stream.ReadAsync(buffer);
                if (read == 0)
                {
                    return Encoding.UTF8.GetString(received.ToArray());
                }

                received.AddRange(new ArraySegment<byte>(buffer, 0, read));
            }

            var headers = Encoding.ASCII.GetString(received.ToArray(), 0, headerEnd);
            var contentLength = ParseContentLength(headers);
            var bodyStart = headerEnd + 4;

            while (received.Count - bodyStart < contentLength)
            {
                var read = await stream.ReadAsync(buffer);
                if (read == 0)
                {
                    break;
                }

                received.AddRange(new ArraySegment<byte>(buffer, 0, read));
            }

            var body = received.ToArray();
            var available = Math.Min(contentLength, body.Length - bodyStart);
            return Encoding.UTF8.GetString(body, bodyStart, Math.Max(0, available));
        }

        private static int IndexOfHeaderEnd(List<byte> bytes)
        {
            for (int i = 0; i + 3 < bytes.Count; i++)
            {
                if (bytes[i] == (byte)'\r' && bytes[i + 1] == (byte)'\n' &&
                    bytes[i + 2] == (byte)'\r' && bytes[i + 3] == (byte)'\n')
                {
                    return i;
                }
            }

            return -1;
        }

        private static int ParseContentLength(string headers)
        {
            foreach (var line in headers.Split("\r\n", StringSplitOptions.RemoveEmptyEntries))
            {
                if (line.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase) &&
                    int.TryParse(line.AsSpan("Content-Length:".Length).Trim(), out var length))
                {
                    return length;
                }
            }

            return 0;
        }

        public void Dispose()
        {
            _stop.Cancel();
            _listener.Stop();
            try
            {
                _accepting.Wait(TimeSpan.FromSeconds(5));
            }
            catch (AggregateException) { /* the cancellation above */ }

            _stop.Dispose();
        }
    }

    /// <summary>
    /// A loopback SMTP sink that records the DATA of each message posted to it, quoted-printable soft line
    /// breaks removed so an assertion on a phrase cannot fail because MIME wrapped it at column 76.
    /// <para>Exists because email is the only channel with no interception point short of the protocol:
    /// there is no builder-returns-the-payload seam between <c>EmailSendCore</c> and the wire. Without it a
    /// mutation that drops the detail on the way to the email template alone passes every other pin — which
    /// is exactly the defect #3296 reported, so leaving that one hop unpinned was not an option.</para>
    /// </summary>
    private sealed class CapturingSmtpEndpoint : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly List<string> _messages = new();
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _accepting;

        public CapturingSmtpEndpoint()
        {
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _accepting = Task.Run(AcceptLoopAsync);
        }

        public int Port { get; }

        /// <summary>Safe to read once the send has been awaited; see the webhook endpoint's note.</summary>
        public IReadOnlyList<string> Messages => _messages;

        private async Task AcceptLoopAsync()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    using var stream = client.GetStream();
                    using var reader = new StreamReader(stream, Encoding.UTF8, false, 8192, leaveOpen: true);
                    using var writer = new StreamWriter(stream, new UTF8Encoding(false), 8192, leaveOpen: true) { AutoFlush = true, NewLine = "\r\n" };

                    await writer.WriteLineAsync("220 localhost ESMTP capture");

                    var data = new StringBuilder();
                    var inData = false;
                    string? line;
                    while ((line = await reader.ReadLineAsync(_stop.Token)) is not null)
                    {
                        if (inData)
                        {
                            if (line == ".")
                            {
                                _messages.Add(DecodeMimeText(data.ToString()));
                                data.Clear();
                                inData = false;
                                await writer.WriteLineAsync("250 OK queued");
                                continue;
                            }

                            /* Transparency: a body line starting with '.' arrives doubled. CRLF explicitly,
                               not AppendLine: MIME line endings are CRLF by the spec the parsing below
                               depends on, and Environment.NewLine is LF on the platform this is written on. */
                            data.Append(line.StartsWith("..", StringComparison.Ordinal) ? line.Substring(1) : line).Append("\r\n");
                            continue;
                        }

                        if (line.StartsWith("EHLO", StringComparison.OrdinalIgnoreCase) ||
                            line.StartsWith("HELO", StringComparison.OrdinalIgnoreCase))
                        {
                            /* No extensions advertised, so the client never tries STARTTLS or AUTH. */
                            await writer.WriteLineAsync("250 localhost");
                        }
                        else if (line.StartsWith("DATA", StringComparison.OrdinalIgnoreCase))
                        {
                            inData = true;
                            await writer.WriteLineAsync("354 End data with <CRLF>.<CRLF>");
                        }
                        else if (line.StartsWith("QUIT", StringComparison.OrdinalIgnoreCase))
                        {
                            await writer.WriteLineAsync("221 Bye");
                            break;
                        }
                        else
                        {
                            await writer.WriteLineAsync("250 OK");
                        }
                    }
                }
            }
            catch (OperationCanceledException) { /* Dispose */ }
            catch (IOException) { /* client went away */ }
            catch (SocketException) { /* listener stopped */ }
            catch (ObjectDisposedException) { /* listener stopped */ }
        }

        /// <summary>
        /// Returns the DECODED text of every MIME part, concatenated. Both transfer encodings this message
        /// actually uses are handled — .NET picks base64 for the utf-8 plain-text alternate view and
        /// quoted-printable for the us-ascii HTML one — because an assertion against the raw DATA would be
        /// asserting against an encoding choice rather than against the delivered words.
        /// </summary>
        private static string DecodeMimeText(string raw)
        {
            var boundary = System.Text.RegularExpressions.Regex.Match(raw, @"boundary=(\S+)");
            if (!boundary.Success)
            {
                return DecodeQuotedPrintable(raw);
            }

            var parts = raw.Split("--" + boundary.Groups[1].Value, StringSplitOptions.None);
            var decoded = new StringBuilder();

            foreach (var part in parts.Skip(1))
            {
                var split = part.IndexOf("\r\n\r\n", StringComparison.Ordinal);
                if (split < 0)
                {
                    continue;
                }

                var headers = part.Substring(0, split);
                var body = part.Substring(split + 4);

                decoded.Append(headers.Contains("base64", StringComparison.OrdinalIgnoreCase)
                    ? Encoding.UTF8.GetString(Convert.FromBase64String(
                        new string(body.Where(c => !char.IsWhiteSpace(c)).ToArray())))
                    : DecodeQuotedPrintable(body)).Append("\r\n");
            }

            return decoded.ToString();
        }

        /// <summary>Drops soft line breaks ("=" at end of line) and decodes "=XX" octets.</summary>
        private static string DecodeQuotedPrintable(string raw)
        {
            var unfolded = raw.Replace("=\r\n", "", StringComparison.Ordinal).Replace("=\n", "", StringComparison.Ordinal);
            var sb = new StringBuilder(unfolded.Length);

            for (int i = 0; i < unfolded.Length; i++)
            {
                if (unfolded[i] == '=' && i + 2 < unfolded.Length &&
                    Uri.IsHexDigit(unfolded[i + 1]) && Uri.IsHexDigit(unfolded[i + 2]))
                {
                    sb.Append((char)Convert.ToInt32(unfolded.Substring(i + 1, 2), 16));
                    i += 2;
                    continue;
                }

                sb.Append(unfolded[i]);
            }

            return sb.ToString();
        }

        public void Dispose()
        {
            _stop.Cancel();
            _listener.Stop();
            try
            {
                _accepting.Wait(TimeSpan.FromSeconds(5));
            }
            catch (AggregateException) { /* the cancellation above */ }

            _stop.Dispose();
        }
    }

    private sealed class DiscardingHistoryStore : IAlertHistoryStore
    {
        public Task RecordAlertAsync(AlertHistoryRecord record) => Task.CompletedTask;

        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);
    }
}
