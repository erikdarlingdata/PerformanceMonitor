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
///
/// <para><b>One class of Lite row does now change value</b> (#3427): a webhook post that came back
/// unsuccessful stored <c>tray</c>, so an install whose Slack posts were all failing recorded a delivery
/// for every one of them. It stores <see cref="AlertDelivery.ChannelFailed"/> instead.
/// <see cref="TheOnlyLiteRowsThatChangeValue_AreFailedWebhookPosts"/> enumerates the complete disagreement
/// set, so the parity claim above still holds everywhere else and a second change cannot ride along with
/// this one.</para>
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
    /// The <c>EmailFanoutResult</c> shapes <c>EmailSendCore</c> cannot emit. All three come from the same
    /// place — <c>attemptChannels: !muted</c> gates every attempt, and both <c>EmailSent</c> and
    /// <c>SendError</c> are set only inside the branch that has already set <c>EmailAttempted</c>:
    ///
    /// <list type="bullet">
    /// <item>a send without an attempt</item>
    /// <item>an error without an attempt</item>
    /// <item>a muted alert carrying any channel outcome or error</item>
    /// </list>
    ///
    /// <para>All are representable, so the shared derivation must still be well-defined on them, and it
    /// deliberately differs from the derivation it replaced there — the old one would put a <c>Sent</c> row
    /// onto <c>tray</c> or <c>muted</c>, or an error onto a state-carrying channel, which
    /// <c>Darling.Tests.AlertDeliveryChannelTests</c>'s domain-wide pins forbid. None is reachable, so
    /// excluding them from the parity comparison costs nothing real, and
    /// <see cref="EveryDisagreement_IsUnreachableOrTheDeliberateChange"/> holds the exclusion to exactly
    /// this predicate plus the one deliberate change, <see cref="WebhookFailureNowNamed"/>.</para>
    /// </summary>
    private static bool Unreachable(EmailFanoutResult result, bool muted)
        => (result.SendError is not null) != (result.EmailOutcome == AlertChannelOutcome.Failed)
        || (result.WebhookSendError is not null) != (result.WebhookOutcome == AlertChannelOutcome.Failed)
        || (muted && (result.EmailOutcome != AlertChannelOutcome.NotAttempted
                      || result.WebhookOutcome != AlertChannelOutcome.NotAttempted))
        || (!result.AnyChannelConfigured && (result.EmailOutcome != AlertChannelOutcome.NotAttempted
                                             || result.WebhookOutcome != AlertChannelOutcome.NotAttempted))
        || (result.AnyChannelConfigured && !muted
            && result.EmailOutcome == AlertChannelOutcome.NotAttempted
            && result.WebhookOutcome == AlertChannelOutcome.NotAttempted);

    /// <summary>
    /// Whether email was involved at all — <see cref="AlertDelivery.FromFanout"/>'s own predicate,
    /// restated here because the one class of Lite row whose stored value DOES move is characterised by
    /// it: a webhook post that was attempted and failed, on an alert where email contributed nothing.
    /// </summary>
    private static bool EmailInvolved(EmailFanoutResult result)
        => result.EmailAttempted || result.EmailSent || result.SendError is not null;

    /// <summary>
    /// <b>The one Lite disposition #3427 deliberately changes.</b> A webhook post that came back
    /// unsuccessful used to be stored as <c>tray</c> on this SKU, because the tray arm answered first and a
    /// toast really had been shown — so a Lite install whose Slack posts were all failing recorded a
    /// successful delivery for every one of them. A failure is now named above the tray arm.
    ///
    /// <para>A throttled or folded send is NOT in here, and still stores <c>tray</c>: the toast is a real,
    /// completed delivery, so "nothing reached anyone" would be false. A failure is different in kind —
    /// nothing else on this SKU records it per alert.</para>
    /// </summary>
    private static bool WebhookFailureNowNamed(EmailFanoutResult result)
        => result.WebhookOutcome == AlertChannelOutcome.Failed && !EmailInvolved(result);

    /// <summary>
    /// Over every send outcome the core can actually produce, outside the one deliberate change, Lite's
    /// shared derivation agrees with the hand-written one it replaced — so no other Lite row's
    /// <c>alert_sent</c> or <c>notification_type</c> changes value.
    /// <see cref="EveryDisagreement_IsUnreachableOrTheDeliberateChange"/> pins that the exclusion is
    /// exactly <see cref="Unreachable"/> plus <see cref="WebhookFailureNowNamed"/> and nothing more, so
    /// this cannot be weakened by widening either.
    /// </summary>
    [Fact]
    public void LiteDispositions_AreUnchangedFromTheHandWrittenDerivation()
    {
        var compared = 0;

        foreach (var (result, muted) in EveryFanoutCase())
        {
            if (Unreachable(result, muted) || WebhookFailureNowNamed(result))
            {
                continue;
            }

            var expected = HandWrittenDerivation(result, muted);
            var actual = AlertDelivery.FromFanout(result, muted, trayChannelPresent: true);
            compared++;

            Assert.Equal(expected.Sent, actual.Sent);
            Assert.Equal(expected.NotificationType, actual.Channel);
        }

        /* 400 representable (5 outcomes per channel x each channel's error present-or-not x
           AnyChannelConfigured x muted), of which 27 are reachable and 3 of those are the deliberate
           change. The figures are derived from the two predicates rather than asserted independently — see
           TheReachableCount_FollowsFromThePredicates, which is what stops them drifting into magic
           numbers. */
        Assert.Equal(UnchangedCaseCount(), compared);
    }

    /// <summary>
    /// <b>The change to Lite's stored values is exactly three shapes, and this says which.</b> All three
    /// are a failed webhook post with email uninvolved, all three used to store <c>tray</c>, and all three
    /// now store <see cref="AlertDelivery.ChannelFailed"/>. Stated as the complete disagreement set rather
    /// than as "the failed case also changed", because a second unintended change would otherwise sit
    /// beside it unnoticed — the same reason the Darling suite counts its SKU divergence instead of
    /// checking the one cell it knows about.
    /// </summary>
    [Fact]
    public void TheOnlyLiteRowsThatChangeValue_AreFailedWebhookPosts()
    {
        var changed = new List<(string Was, string Now)>();

        foreach (var (result, muted) in EveryFanoutCase())
        {
            if (Unreachable(result, muted))
            {
                continue;
            }

            var expected = HandWrittenDerivation(result, muted);
            var actual = AlertDelivery.FromFanout(result, muted, trayChannelPresent: true);

            if (expected.Sent == actual.Sent && expected.NotificationType == actual.Channel)
            {
                continue;
            }

            changed.Add((expected.NotificationType, actual.Channel));

            /* Each one is the characterised shape, so the exclusion above is not a licence to change
               anything else. */
            Assert.True(WebhookFailureNowNamed(result));

            /* alert_sent does not move — nothing delivered before and nothing delivers now. Only the
               reason does. */
            Assert.Equal(expected.Sent, actual.Sent);
        }

        Assert.Equal(3, changed.Count);
        Assert.All(changed, c => Assert.Equal(AlertDelivery.ChannelTray, c.Was));
        Assert.All(changed, c => Assert.Equal(AlertDelivery.ChannelFailed, c.Now));
    }

    /// <summary>
    /// <b>The both-SKUs answer, measured rather than asserted in prose.</b> Over the whole reachable
    /// domain, Lite can store <see cref="AlertDelivery.ChannelFailed"/> — so the new vocabulary is not
    /// Darling-only — and it cannot store <see cref="AlertDelivery.ChannelThrottled"/> or
    /// <see cref="AlertDelivery.ChannelFolded"/>, because the tray arm answers first for a suppression and
    /// a toast really was shown. Neither SKU can store
    /// <see cref="AlertDelivery.ChannelUndelivered"/> any more.
    ///
    /// <para>Written as two exact sets, not as membership checks, so a value that quietly becomes
    /// unreachable on one SKU — a permanently-empty column an operator would read as "this never happens"
    /// — fails here. <c>unconfigured</c>'s absence from the Lite set is pre-existing and for the same
    /// reason as the suppressions': #3169 put the tray arm above the configuration arms so a Lite row's
    /// value does not depend on whether SMTP happens to be set up.</para>
    /// </summary>
    [Fact]
    public void TheStoredVocabulary_DiffersBetweenTheSkus_ExactlyHere()
    {
        var lite = new SortedSet<string>(StringComparer.Ordinal);
        var headless = new SortedSet<string>(StringComparer.Ordinal);

        foreach (var (result, muted) in EveryFanoutCase())
        {
            if (Unreachable(result, muted))
            {
                continue;
            }

            lite.Add(AlertDelivery.FromFanout(result, muted, trayChannelPresent: true).Channel);
            headless.Add(AlertDelivery.FromFanout(result, muted, trayChannelPresent: false).Channel);
        }

        Assert.Equal(
            new[]
            {
                AlertDelivery.ChannelEmail, AlertDelivery.ChannelEmailAndWebhook,
                AlertDelivery.ChannelFailed, AlertDelivery.ChannelMuted, AlertDelivery.ChannelTray,
                AlertDelivery.ChannelWebhook,
            }.OrderBy(c => c, StringComparer.Ordinal).ToArray(),
            lite.ToArray());

        Assert.Equal(
            new[]
            {
                AlertDelivery.ChannelEmail, AlertDelivery.ChannelEmailAndWebhook,
                AlertDelivery.ChannelFailed, AlertDelivery.ChannelFolded, AlertDelivery.ChannelMuted,
                AlertDelivery.ChannelNoneConfigured, AlertDelivery.ChannelThrottled,
                AlertDelivery.ChannelWebhook,
            }.OrderBy(c => c, StringComparer.Ordinal).ToArray(),
            headless.ToArray());

        /* And the legacy value is gone from both, which is what makes a stored `undelivered` row
           unambiguously pre-#3427 history rather than something a current build might still be writing. */
        Assert.DoesNotContain(AlertDelivery.ChannelUndelivered, lite);
        Assert.DoesNotContain(AlertDelivery.ChannelUndelivered, headless);
    }

    /// <summary>
    /// Every disagreement with the old derivation is either an unreachable shape or the one deliberate
    /// change. Without this, widening either exclusion would make the parity claim pass by comparing less.
    /// </summary>
    [Fact]
    public void EveryDisagreement_IsUnreachableOrTheDeliberateChange()
    {
        var disagreements = new List<(EmailFanoutResult Result, bool Muted)>();

        foreach (var (result, muted) in EveryFanoutCase())
        {
            var expected = HandWrittenDerivation(result, muted);
            var actual = AlertDelivery.FromFanout(result, muted, trayChannelPresent: true);

            if (expected.Sent != actual.Sent || expected.NotificationType != actual.Channel)
            {
                disagreements.Add((result, muted));
            }
        }

        Assert.NotEmpty(disagreements);
        Assert.All(disagreements, c =>
            Assert.True(Unreachable(c.Result, c.Muted) || WebhookFailureNowNamed(c.Result)));
    }

    /// <summary>
    /// Lite has a tray, so it writes one — and its status column still reads "Shown", which is a true
    /// statement there. The Darling Viewer's twin pin requires the opposite for the same stored value.
    /// </summary>
    [Fact]
    public void LiteWritesTray_BecauseItHasOne()
    {
        var delivery = AlertDelivery.FromFanout(
            new EmailFanoutResult(
                AlertChannelOutcome.NotAttempted, null, AlertChannelOutcome.NotAttempted, null,
                AnyChannelConfigured: false),
            muted: false, trayChannelPresent: true);

        Assert.Equal(AlertDelivery.ChannelTray, delivery.Channel);
        Assert.False(delivery.Sent);
        Assert.Equal(
            AlertDeliveryStatus.Shown,
            AlertDeliveryStatus.Describe(delivery.Sent, delivery.Channel, delivery.SendError, producerHadTrayChannel: true));
    }

    /// <summary>
    /// Lite's producer states its tray channel with the named argument, so the answer cannot be flipped by
    /// a positional edit, and the divergence from Darling is declared at the call site rather than inferred.
    /// <para>#3916 moved this pin: the answer is now <c>trayShown</c>, TRUE by default for the engine path
    /// (LiteAlertDeliverer shows a balloon on the same call) and FALSE on exactly one caller — the analysis
    /// finding arm, because Lite wires no tray sink into AnalysisNotificationService and a finding row stored
    /// <c>tray</c> read "Shown" for a toast nobody saw. Neither literal answer is hard-coded any more.</para>
    /// </summary>
    [Fact]
    public void TheLiteProducer_DeclaresItsTrayChannel()
    {
        var text = File.ReadAllText(RepoPath("Lite/Services/EmailAlertService.cs"));

        Assert.Contains("trayChannelPresent: trayShown)", text, StringComparison.Ordinal);
        Assert.Contains("bool trayShown = true)", text, StringComparison.Ordinal);
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(text, @"trayShown: false\)"));
        Assert.DoesNotContain("trayChannelPresent: false", text, StringComparison.Ordinal);
        Assert.DoesNotContain("trayChannelPresent: true)", text, StringComparison.Ordinal);

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
            (false, AlertDelivery.ChannelThrottled, null),
            (false, AlertDelivery.ChannelFolded, null),
            (false, AlertDelivery.ChannelFailed, "Slack: 500 Internal Server Error"),
            (false, AlertDelivery.ChannelFailed, null),
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
                AlertDeliveryStatus.Describe(sent, channel, error, producerHadTrayChannel: true),
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
            AlertDeliveryStatus.Describe(true, AlertDelivery.ChannelTray, null, producerHadTrayChannel: true));
        Assert.Equal(AlertDeliveryStatus.Shown,
            AlertDeliveryStatus.Describe(false, AlertDelivery.ChannelTray, null, producerHadTrayChannel: true));
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
        foreach (var email in Enum.GetValues<AlertChannelOutcome>())
        foreach (var webhook in Enum.GetValues<AlertChannelOutcome>())
        foreach (var anyConfigured in new[] { false, true })
        foreach (var sendError in new string?[] { null, "relay refused" })
        foreach (var webhookError in new string?[] { null, "Slack: 500 Internal Server Error" })
        foreach (var muted in new[] { false, true })
        {
            yield return (
                new EmailFanoutResult(email, sendError, webhook, webhookError, anyConfigured),
                muted);
        }
    }

    private static int ReachableCaseCount()
        => EveryFanoutCase().Count(c => !Unreachable(c.Result, c.Muted));

    private static int UnchangedCaseCount()
        => EveryFanoutCase().Count(c => !Unreachable(c.Result, c.Muted) && !WebhookFailureNowNamed(c.Result));

    /// <summary>
    /// 27 reachable of 400 representable, of which 24 keep their old value and 3 are the deliberate change.
    /// Pinned separately from the parity comparison so a widened <see cref="Unreachable"/> or
    /// <see cref="WebhookFailureNowNamed"/> cannot make that comparison pass by excluding more: the parity
    /// test asserts it compared <see cref="UnchangedCaseCount"/> cases, and this asserts what that number
    /// is and that the two predicates partition the reachable set as claimed.
    /// </summary>
    [Fact]
    public void TheReachableCount_FollowsFromThePredicates()
    {
        Assert.Equal(400, EveryFanoutCase().Count());
        Assert.Equal(27, ReachableCaseCount());
        Assert.Equal(24, UnchangedCaseCount());

        /* The two exclusions do not overlap, so "27 = 24 + 3" is a partition rather than an arithmetic
           coincidence — an unreachable shape counted as a deliberate change would otherwise hide one. */
        Assert.Equal(
            3,
            EveryFanoutCase().Count(c => !Unreachable(c.Result, c.Muted) && WebhookFailureNowNamed(c.Result)));
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
