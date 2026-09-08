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
using System.Runtime.CompilerServices;
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

        /* The count IS the claim that the enumeration is the whole domain: 2^4 result bools x muted x
           trayChannelPresent. A shrunken loop would otherwise pass by covering less. */
        Assert.Equal(64, checked_);
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
    /// Every representable <c>EmailFanoutResult</c> shape crossed with both callers' answers — 64 cases.
    /// <c>SendError</c> is not part of the cross-product because no arm branches on it; the failure path is
    /// covered by <see cref="AnAttemptThatFailed_KeepsItsErrorAndReadsAsFailed"/>.
    /// </summary>
    private static IEnumerable<(EmailFanoutResult Result, bool Muted, bool Tray)> EveryFanoutCase()
    {
        foreach (var emailAttempted in new[] { false, true })
        foreach (var emailSent in new[] { false, true })
        foreach (var webhookSent in new[] { false, true })
        foreach (var anyConfigured in new[] { false, true })
        foreach (var muted in new[] { false, true })
        foreach (var tray in new[] { false, true })
        {
            yield return (
                new EmailFanoutResult(emailAttempted, emailSent, null, webhookSent, anyConfigured),
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
}
