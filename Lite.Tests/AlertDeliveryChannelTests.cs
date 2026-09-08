/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3169, Lite's half. Darling's is <c>Darling.Tests.AlertDeliveryChannelTests</c>, which pins the
/// resolution record and the headless service's no-tray answer.
///
/// <para><b>Lite is not the defective SKU here, and the load-bearing claim is that nothing about what it
/// stores changed.</b> The taxonomy was written out by hand in three producers, of which Lite's was the
/// original and the only truthful one: its deliverer really does show a styled balloon for every non-muted
/// alert on the same call that records the row, so a stored <c>tray</c> means what it says. Collapsing the
/// three copies into one shared derivation therefore has to leave Lite's output byte-identical, and
/// <see cref="LiteDispositions_AreUnchangedFromTheHandWrittenDerivation"/> is what says so — over the whole
/// input domain, against the derivation it replaced, rather than at a handful of sampled points.</para>
/// </summary>
public sealed class AlertDeliveryChannelTests
{
    /* ─────────────── the parity claim: Lite's stored values did not move ─────────────── */

    /// <summary>
    /// The pre-#3169 derivation, transcribed from the three copies it existed in (Lite's
    /// <c>EmailAlertService</c>, Darling's <c>DarlingAlertDeliverer</c> and <c>DarlingFindingAlertSender</c>
    /// — identical but for the muted arm the finding sender does not need). Kept here as the reference
    /// Lite's new output is compared against.
    /// </summary>
    private static (bool Sent, string NotificationType) HandWrittenDerivation(EmailFanoutResult result, bool muted)
    {
        var notificationType = muted ? "muted" : "tray";
        if (result.EmailAttempted)
        {
            notificationType = "email";
        }

        var sent = result.EmailSent;
        if (result.WebhookSent)
        {
            notificationType = notificationType == "email" ? "email+webhook" : "webhook";
            sent = true;
        }

        return (sent, notificationType);
    }

    /// <summary>
    /// Over every representable send outcome, Lite's shared derivation agrees with the hand-written one it
    /// replaced — so no Lite row's <c>alert_sent</c> or <c>notification_type</c> changes value.
    ///
    /// <para>The one combination excluded is <c>EmailSent</c> without <c>EmailAttempted</c>, which the send
    /// core cannot produce and where the two derivations deliberately DISAGREE: the old one reported a send
    /// on <c>tray</c>, and the new one names the email channel, which is what
    /// <c>Darling.Tests.AlertDeliveryChannelTests.EverySentDisposition_NamesADeliveringChannel</c> requires.
    /// Excluding it is a stated carve-out rather than a silent one — <see cref="TheOneDisagreement_IsTheImpossibleCombination"/>
    /// pins that it is the ONLY disagreement, so this test cannot be weakened by widening the exclusion.</para>
    /// </summary>
    [Fact]
    public void LiteDispositions_AreUnchangedFromTheHandWrittenDerivation()
    {
        var compared = 0;

        foreach (var (result, muted) in EveryFanoutCase())
        {
            if (result.EmailSent && !result.EmailAttempted)
            {
                continue;
            }

            var expected = HandWrittenDerivation(result, muted);
            var actual = AlertDelivery.FromFanout(result, muted, trayChannelPresent: true);
            compared++;

            Assert.Equal(expected.Sent, actual.Sent);
            Assert.Equal(expected.NotificationType, actual.Channel);
        }

        /* 2^4 result bools x muted = 32, less the 8 cases carrying the impossible EmailSent-without-attempt. */
        Assert.Equal(24, compared);
    }

    /// <summary>
    /// The carve-out above is exactly the impossible combination and nothing else. Without this, widening
    /// the <c>continue</c> would make the parity claim pass by comparing less.
    /// </summary>
    [Fact]
    public void TheOneDisagreement_IsTheImpossibleCombination()
    {
        var disagreements = new List<EmailFanoutResult>();

        foreach (var (result, muted) in EveryFanoutCase())
        {
            var expected = HandWrittenDerivation(result, muted);
            var actual = AlertDelivery.FromFanout(result, muted, trayChannelPresent: true);

            if (expected.Sent != actual.Sent || expected.NotificationType != actual.Channel)
            {
                disagreements.Add(result);
            }
        }

        Assert.NotEmpty(disagreements);
        Assert.All(disagreements, r => Assert.True(r.EmailSent && !r.EmailAttempted));
    }

    /// <summary>
    /// Lite has a tray, so it writes one — and its status column still reads "Shown", which is a true
    /// statement there. The Darling Viewer's twin pin requires the opposite for the same stored value.
    /// </summary>
    [Fact]
    public void LiteWritesTray_BecauseItHasOne()
    {
        var delivery = AlertDelivery.FromFanout(
            new EmailFanoutResult(false, false, null, false, AnyChannelConfigured: false),
            muted: false, trayChannelPresent: true);

        Assert.Equal(AlertDelivery.ChannelTray, delivery.Channel);
        Assert.False(delivery.Sent);
        Assert.Equal(
            AlertDeliveryStatus.Shown,
            AlertDeliveryStatus.Describe(delivery.Sent, delivery.Channel, delivery.SendError, trayChannelPresent: true));
    }

    /// <summary>
    /// Lite's producer states <c>trayChannelPresent: true</c> — the named argument, so the answer cannot be
    /// flipped by a positional edit, and the divergence from Darling is declared at the call site rather
    /// than inferred.
    /// </summary>
    [Fact]
    public void TheLiteProducer_DeclaresItsTrayChannel()
    {
        var text = File.ReadAllText(RepoPath("Lite/Services/EmailAlertService.cs"));

        Assert.Contains("trayChannelPresent: true", text, StringComparison.Ordinal);
        Assert.DoesNotContain("trayChannelPresent: false", text, StringComparison.Ordinal);

        /* And it is the only FromFanout caller under Lite, so that one file is the whole population. */
        var callers = Directory
            .EnumerateFiles(Path.Combine(RepoRoot(), "Lite"), "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Where(f => File.ReadAllText(f).Contains("AlertDelivery.FromFanout(", StringComparison.Ordinal))
            .Select(f => Path.GetRelativePath(RepoRoot(), f).Replace('\\', '/'))
            .ToArray();

        Assert.Equal(new[] { "Lite/Services/EmailAlertService.cs" }, callers);
    }

    /* ─────────────── the read side ─────────────── */

    /// <summary>
    /// Lite's grid column IS the shared describer's answer. The copy it replaces rendered the same
    /// <c>false</c> as "Not sent" on its email arm and "Shown" on the other, which is how one boolean came
    /// to have two vocabularies.
    /// </summary>
    [Fact]
    public void TheLiteRow_RendersThroughTheSharedDescriber()
    {
        var cases = new (bool Sent, string Channel, string? Error)[]
        {
            (false, AlertDelivery.ChannelNotApplicable, null),
            (false, AlertDelivery.ChannelNoneConfigured, null),
            (false, AlertDelivery.ChannelUndelivered, null),
            (false, AlertDelivery.ChannelMuted, null),
            (false, AlertDelivery.ChannelEmail, "relay refused"),
            (false, AlertDelivery.ChannelEmail, null),
            (true, AlertDelivery.ChannelEmailAndWebhook, null),
            (true, AlertDelivery.ChannelTray, null),
            (false, AlertDelivery.ChannelTray, null),
        };

        foreach (var (sent, channel, error) in cases)
        {
            var row = new AlertHistoryRow
            {
                AlertTime = new DateTime(2026, 9, 8, 1, 0, 0, DateTimeKind.Utc),
                MetricName = "High CPU",
                CurrentValue = 92,
                ThresholdValue = 90,
                AlertSent = sent,
                NotificationType = channel,
                SendError = error,
            };

            Assert.Equal(
                AlertDeliveryStatus.Describe(sent, channel, error, trayChannelPresent: true),
                row.StatusDisplay);
        }
    }

    /// <summary>
    /// The legacy resolution signature decodes the same on a Lite store, because the resolution builder is
    /// shared code: <c>true</c> alongside <c>tray</c> was that builder's constant on either SKU, and Lite's
    /// own tray rows are <c>false</c>.
    /// </summary>
    [Fact]
    public void TheLegacyResolutionSignature_DecodesToNoChannel_OnALiteStoreToo()
    {
        Assert.Equal(AlertDeliveryStatus.NoChannel,
            AlertDeliveryStatus.Describe(true, AlertDelivery.ChannelTray, null, trayChannelPresent: true));
        Assert.Equal(AlertDeliveryStatus.Shown,
            AlertDeliveryStatus.Describe(false, AlertDelivery.ChannelTray, null, trayChannelPresent: true));
    }

    /* ─────────────── where AnyChannelConfigured comes from ─────────────── */

    /// <summary>
    /// Each webhook channel on its own makes the deployment "configured". The disjunction and the fan-out's
    /// own four if-conditions are the same four expressions, so a channel added to one is added to both —
    /// but each is pinned individually here, because a disjunction that silently dropped a term would
    /// report "no channel configured" for a deployment that has one, and that is the reading an operator
    /// would act on.
    /// </summary>
    [Theory]
    [InlineData("teams")]
    [InlineData("slack")]
    [InlineData("generic")]
    [InlineData("pagerduty")]
    public void AnyWebhookConfigured_IsTrueForEachChannelAlone(string channel)
    {
        var settings = new OneChannelSettings(channel);
        var service = new WebhookAlertService(
            settings, new AlertBranding("Lite", null), NullLogger<WebhookAlertService>.Instance);

        Assert.True(service.AnyWebhookConfigured);
    }

    /// <summary>And false when nothing is set — the state all three of Erik's live stores are in.</summary>
    [Fact]
    public void AnyWebhookConfigured_IsFalseWithNothingSet()
    {
        var service = new WebhookAlertService(
            new OneChannelSettings("none"), new AlertBranding("Lite", null), NullLogger<WebhookAlertService>.Instance);

        Assert.False(service.AnyWebhookConfigured);
    }

    /* ─────────────── helpers ─────────────── */

    private static IEnumerable<(EmailFanoutResult Result, bool Muted)> EveryFanoutCase()
    {
        foreach (var emailAttempted in new[] { false, true })
        foreach (var emailSent in new[] { false, true })
        foreach (var webhookSent in new[] { false, true })
        foreach (var anyConfigured in new[] { false, true })
        foreach (var muted in new[] { false, true })
        {
            yield return (new EmailFanoutResult(emailAttempted, emailSent, null, webhookSent, anyConfigured), muted);
        }
    }

    private static string RepoPath(string relative) => Path.Combine(RepoRoot(), relative);

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));

    /// <summary>An <see cref="IAlertSettings"/> with exactly one channel populated.</summary>
    private sealed class OneChannelSettings : IAlertSettings
    {
        private readonly string _channel;

        public OneChannelSettings(string channel) => _channel = channel;

        public bool SmtpEnabled => false;
        public string SmtpServer => "";
        public int SmtpPort => 25;
        public bool SmtpUseSsl => false;
        public string SmtpUsername => "";
        public string SmtpFromAddress => "";
        public string SmtpRecipients => "";
        public string? GetSmtpPassword() => null;
        public int EmailCooldownMinutes => 15;

        public bool TeamsWebhookEnabled => _channel == "teams";
        public string TeamsWebhookUrl => _channel == "teams" ? "https://example.invalid/teams" : "";
        public string TeamsProxyAddress => "";

        public bool SlackWebhookEnabled => _channel == "slack";
        public string SlackWebhookUrl => _channel == "slack" ? "https://example.invalid/slack" : "";
        public string SlackProxyAddress => "";

        public bool GenericWebhookEnabled => _channel == "generic";
        public string GenericWebhookUrl => _channel == "generic" ? "https://example.invalid/generic" : "";
        public string GenericWebhookHeadersJson => "";
        public string GenericWebhookBodyTemplate => "";
        public string GenericWebhookProxyAddress => "";

        public bool PagerDutyEnabled => _channel == "pagerduty";
        public string PagerDutyRoutingKey => _channel == "pagerduty" ? "routing-key-placeholder" : "";
        public bool PagerDutyUseEuRegion => false;
        public string PagerDutyProxyAddress => "";

        public double AnalysisNotifySeverity => 1.5;
        public int AnalysisNotifyCooldownMinutes => 360;
        public string TriageBaseUrl => "";
    }

}
