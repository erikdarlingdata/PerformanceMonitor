/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// What was attempted for one alert, on which channel, with what outcome — the value
/// <see cref="AlertHistoryRecord"/> persists into <c>alert_sent</c> / <c>notification_type</c> /
/// <c>send_error</c>.
///
/// <para><b>Why a value and not three fields.</b> <c>alert_sent</c> alone answers two different questions
/// depending on which producer wrote the row: on a fired alert it is a delivery measurement from the
/// deliverer, and on a resolution row it was a hardcoded <c>true</c> standing for "no send channel applies
/// to a resolution". A reader given the boolean cannot tell which sense it holds, so no aggregate over the
/// column means anything — the delivered fraction reads healthy in exact proportion to how many resolutions
/// occurred. <see cref="Channel"/> states which channel the row is about, so "no channel applies" is a
/// named state rather than a <c>true</c> meaning something else, and <see cref="Sent"/> is only ever a
/// measurement.</para>
///
/// <para><b>Factory-only, on purpose.</b> The constructor is private and neither property is settable, so
/// there is no <c>with</c> expression and no way to hand-write a <see cref="Sent"/> of <c>true</c>. Every
/// row's disposition comes from <see cref="FromFanout"/> (a real send attempt) or
/// <see cref="NoChannelApplies"/> (a row that has no channel by design). Before this type the derivation
/// was written out by hand in three producers — Lite's <c>EmailAlertService</c>, Darling's
/// <c>DarlingAlertDeliverer</c> and Darling's <c>DarlingFindingAlertSender</c>, each carrying a comment
/// pointing at one of the others as the definition — and nothing held the three copies equal.</para>
/// </summary>
public sealed record AlertDelivery
{
    /// <summary>No send channel applies to this row at all — a resolution row, which exists to pair with
    /// its "Detected" entry in history and was never a candidate for delivery. Distinct from
    /// <see cref="ChannelNoneConfigured"/>, which means a channel WOULD have applied and none is set up.</summary>
    public const string ChannelNotApplicable = "none";

    /// <summary>A channel applied to this alert, and no channel is configured on this store, so nothing
    /// was attempted. The honest reading of a headless deployment with empty SMTP and webhook settings.</summary>
    public const string ChannelNoneConfigured = "unconfigured";

    /// <summary>A mute rule suppressed the channels. The row is still recorded, flagged muted.</summary>
    public const string ChannelMuted = "muted";

    /// <summary>The Lite tray toast — a real channel that really received the alert, and the reason
    /// <c>tray</c> exists in this taxonomy at all. Only a SKU that has a tray may write it; see
    /// <see cref="FromFanout"/>'s <c>trayChannelPresent</c>.</summary>
    public const string ChannelTray = "tray";

    /// <summary>At least one channel was configured and consulted, and none of them delivered — a
    /// cooldown-throttled send, or a webhook post that came back unsuccessful. Replaces the reading that
    /// used to fall into <see cref="ChannelTray"/> on a SKU with no tray.</summary>
    public const string ChannelUndelivered = "undelivered";

    public const string ChannelEmail = "email";
    public const string ChannelWebhook = "webhook";
    public const string ChannelEmailAndWebhook = "email+webhook";

    /// <summary>
    /// The channel values that state a delivery STATE rather than naming a channel that carried the alert.
    /// Declared rather than left implicit in the reading code, because three surfaces need the answer — the
    /// two WPF grids through <see cref="AlertDeliveryStatus"/>, and the headless web dashboard, which
    /// restates it in <c>util.js</c> and is pinned against this list.
    /// </summary>
    public static IReadOnlyList<string> StateCarryingChannels { get; } = new[]
    {
        ChannelNotApplicable, ChannelNoneConfigured, ChannelMuted, ChannelUndelivered, ChannelTray,
    };

    /// <summary>
    /// The channel values that name a channel the alert actually went out on. <see cref="Sent"/> implies
    /// one of these; the converse does not hold, because an attempted email that threw is
    /// <see cref="ChannelEmail"/> with <see cref="Sent"/> false.
    /// </summary>
    public static IReadOnlyList<string> DeliveringChannels { get; } = new[]
    {
        ChannelEmail, ChannelWebhook, ChannelEmailAndWebhook,
    };

    private AlertDelivery(bool sent, string channel, string? sendError)
    {
        Sent = sent;
        Channel = channel;
        SendError = sendError;
    }

    /// <summary>Whether a channel actually delivered this alert. A measurement on every row — never a
    /// stand-in for anything else. Persisted as <c>alert_sent</c>.</summary>
    public bool Sent { get; }

    /// <summary>Which channel this row is about, from the constants above. Persisted as
    /// <c>notification_type</c>. The field that carries "no channel applies" so <see cref="Sent"/> does
    /// not have to.</summary>
    public string Channel { get; }

    /// <summary>The email send error, when the SMTP attempt threw. Null on every other disposition —
    /// including a configured webhook whose post failed, which <c>EmailFanoutResult</c> does not report
    /// (see <see cref="FromFanout"/>).</summary>
    public string? SendError { get; }

    /// <summary>
    /// The row has no send channel by design. Used by the resolution-record builder, whose rows exist so
    /// an operator reviewing history sees "Detected" then "Cleared" as a pair and which were never
    /// candidates for email or webhook.
    ///
    /// <para><see cref="Sent"/> is <c>false</c> here, where the hand-written record it replaces said
    /// <c>true</c>. The <c>true</c> was not a claim that anything was delivered; the state it meant is now
    /// in <see cref="Channel"/>, which is where a reader can act on it.</para>
    /// </summary>
    public static AlertDelivery NoChannelApplies() => new(false, ChannelNotApplicable, null);

    /// <summary>
    /// A disposition rebuilt from three already-computed column values. <b>The deprecated Dashboard SKU's
    /// only.</b>
    ///
    /// <para>It exists because that SKU's <c>EmailAlertService.RecordAlert</c> takes the pair as method
    /// PARAMETERS from 26 call sites, each hand-computing it — which is this issue's defect, in a SKU that
    /// is not shipped, writes to its own JSON store, and has its own display. Converting those 26 sites
    /// would be churn in dead code and would fix nothing an operator can see. Deliberately
    /// <c>internal</c>, so no live producer can reach it, and
    /// <c>Darling.Tests.AlertDeliveryChannelTests.TheLegacyHatch_HasExactlyOneProductionCaller</c> pins
    /// that it stays that way — an <c>internal</c> member is visible to Lite as well as to the deprecated
    /// SKU, so the boundary is asserted rather than assumed.</para>
    /// </summary>
    [Obsolete("Deprecated Dashboard SKU only. Live producers use FromFanout or NoChannelApplies.", error: false)]
    internal static AlertDelivery FromLegacyStoredColumns(bool sent, string channel, string? sendError) =>
        new(sent, channel, sendError);

    /// <summary>
    /// The disposition of a real send attempt, from what the shared send core reports.
    /// </summary>
    /// <param name="result">What <c>EmailSendCore.TrySendAsync</c> did.</param>
    /// <param name="muted">Whether a mute rule suppressed the channels — the caller passed
    /// <c>attemptChannels: !muted</c>, so a muted result is all-false by construction.</param>
    /// <param name="trayChannelPresent">
    /// Whether this SKU has a tray channel that received the alert. <b>Lite: true</b> — its deliverer shows
    /// a styled balloon for every non-muted alert on the same call that records the row, so
    /// <see cref="ChannelTray"/> is a truthful statement there. <b>Darling: false</b> — the headless service
    /// has no tray and no toast code at all, so a <c>tray</c> row on a Darling store asserted a UI event
    /// that cannot occur. This is a deliberate per-SKU divergence, not an oversight, and each SKU's tests
    /// pin its own answer.
    /// </param>
    /// <remarks>
    /// <para><b>Invariant:</b> <see cref="Sent"/> implies <see cref="Channel"/> is one of
    /// <see cref="ChannelEmail"/>, <see cref="ChannelWebhook"/>, <see cref="ChannelEmailAndWebhook"/> — a
    /// channel that delivered names itself. One-directional on purpose: the converse is false and should
    /// be, because an attempted email that threw is <see cref="ChannelEmail"/> with <see cref="Sent"/>
    /// false and its error attached.</para>
    ///
    /// <para>It holds over the whole <c>EmailFanoutResult</c> domain rather than over the shapes the send
    /// core emits, which is what the ordering below is for and why the email arm reads <c>EmailSent</c> as
    /// well as <c>EmailAttempted</c>: the result's four bools are independent, and the two combinations the
    /// send core cannot produce (an <c>EmailSent</c> without an <c>EmailAttempted</c>, and a muted result
    /// carrying any channel outcome) would each otherwise put a <c>Sent</c> row onto a non-delivering
    /// channel. The invariant is what lets a reader decode the one legacy signature this change leaves
    /// behind — see <c>AlertDeliveryStatus.Describe</c>.</para>
    ///
    /// <para><b>A configured webhook that failed reads as <see cref="ChannelUndelivered"/>, not
    /// <see cref="ChannelEmail"/>-style failure.</b> <c>EmailFanoutResult.SendError</c> tracks the EMAIL
    /// channel only — <c>WebhookAlertService.TrySendWebhookAlertsAsync</c> collapses every per-channel
    /// outcome into one bool — so a webhook post that came back unsuccessful is reported here as
    /// "configured, nothing delivered" with a null error. That is honest but coarse, and it is a live
    /// delivery gap on any store that configures a webhook.</para>
    /// </remarks>
    public static AlertDelivery FromFanout(EmailFanoutResult result, bool muted, bool trayChannelPresent)
    {
        var sent = result.EmailSent || result.WebhookSent;

        /* The channels that carry an outcome are tested FIRST, so the invariant holds over the whole input
           domain and not merely over the shapes the send core emits. Ordering muted ahead of them read
           naturally — a muted alert attempts nothing — but it let a Sent row be labelled "muted", which
           puts a true back onto a non-delivering channel and breaks the legacy decode. Both routes to that
           are unreachable in practice (attemptChannels: !muted gates every attempt), and neither is
           unrepresentable, which is the difference that matters.

           Below the delivering arms the order preserves the derivation this replaces: muted beats tray
           (Lite shows no toast for a muted alert), and tray beats the configuration arms so a Lite row's
           stored value is unchanged whether or not SMTP happens to be set up. */
        var emailInvolved = result.EmailAttempted || result.EmailSent;

        var channel =
            result.WebhookSent && emailInvolved ? ChannelEmailAndWebhook
            : result.WebhookSent ? ChannelWebhook
            : emailInvolved ? ChannelEmail
            : muted ? ChannelMuted
            : trayChannelPresent ? ChannelTray
            : !result.AnyChannelConfigured ? ChannelNoneConfigured
            : ChannelUndelivered;

        return new AlertDelivery(sent, channel, result.SendError);
    }
}

/// <summary>
/// The one rendering of a stored alert-history row's delivery status, shared by Lite's
/// <c>AlertHistoryRow</c> and the Darling Viewer's, which each carried their own copy of it.
///
/// <para>The copies branched on <c>notification_type == "email"</c> and rendered the same <c>false</c> as
/// "Not sent" on one arm and "Shown" on the other. With no SMTP configured the email arm never ran, so
/// every fired alert rendered "Shown" and every resolution rendered "Delivered" — a UI event that cannot
/// happen on a headless service, and a delivery that never occurred.</para>
/// </summary>
public static class AlertDeliveryStatus
{
    /// <summary>What to show an operator for a stored row.</summary>
    /// <param name="sent">The row's <c>alert_sent</c>.</param>
    /// <param name="channel">The row's <c>notification_type</c>.</param>
    /// <param name="sendError">The row's <c>send_error</c>.</param>
    /// <param name="producerHadTrayChannel">
    /// Whether the SKU that WROTE this store records a tray channel — the read side of
    /// <see cref="AlertDelivery.FromFanout"/>'s <c>trayChannelPresent</c>, and required for the same
    /// reason: a stored <c>tray</c> is a truthful "a toast was shown" on a Lite store and means nothing at
    /// all on a headless Darling one, so one renderer cannot label it without being told which store it is
    /// reading. <b>Lite: true.</b> <b>The Darling Viewer: false.</b>
    ///
    /// <para>Named for the PRODUCER rather than the reader on purpose. The Darling Viewer has an
    /// <c>AlertToastCoordinator</c> and does raise its own toasts, so "does this SKU have a tray" is true
    /// of the reader and false of the writer, and the value here is about the writer. Getting that backwards
    /// would make the Viewer render "Shown" for 32,546 rows recorded by a service with no tray, which is the
    /// exact reading this change removes.</para>
    /// </param>
    /// <remarks>
    /// <para><b>Rows written before this taxonomy are read conservatively, not reinterpreted.</b> The one
    /// legacy signature that can be decoded is <c>alert_sent = true</c> with
    /// <c>notification_type = 'tray'</c>: <see cref="AlertDelivery.FromFanout"/>'s invariant says a
    /// delivering channel names itself, and every producer that ever wrote this column derived the pair the
    /// same way, so a <c>true</c> alongside <c>tray</c> can only have come from the resolution builder's
    /// hardcoded constant. It renders as <see cref="NoChannel"/>, which is what it always meant — on either
    /// SKU, because that builder is shared code.</para>
    ///
    /// <para>A legacy <c>alert_sent = false</c> with <c>tray</c> is NOT decoded on a store whose producer had no
    /// tray. It covers a store with no channel configured, a throttled send, and a failed webhook post, and
    /// nothing in the row separates them — the whole reason this change exists. It renders
    /// <see cref="Logged"/>, which is #2781's word for it and claims nothing in either direction. New rows
    /// say which of those they are.</para>
    /// </remarks>
    public static string Describe(bool sent, string? channel, string? sendError, bool producerHadTrayChannel)
    {
        if (channel == AlertDelivery.ChannelNotApplicable)
        {
            return NoChannel;
        }

        /* The legacy resolution signature. Unreachable for a new row: Sent implies a named delivering
           channel, so nothing can write true alongside tray again. */
        if (sent && channel == AlertDelivery.ChannelTray)
        {
            return NoChannel;
        }

        if (sent)
        {
            return Delivered;
        }

        if (!string.IsNullOrEmpty(sendError))
        {
            return Failed;
        }

        if (channel == AlertDelivery.ChannelMuted)
        {
            return Muted;
        }

        if (channel == AlertDelivery.ChannelNoneConfigured)
        {
            return NoChannelConfigured;
        }

        /* Lite's tray toast is a real delivery to a real channel. On a store whose producer had no tray
           the same stored value carries no information at all, so it gets #2781's neutral "Logged" — the
           label the web surface already chose for this exact row, rather than a second word for it. */
        if (channel == AlertDelivery.ChannelTray)
        {
            return producerHadTrayChannel ? Shown : Logged;
        }

        return NotSent;
    }

    public const string NoChannel = "No channel";
    public const string NoChannelConfigured = "No channel configured";
    public const string Delivered = "Delivered";
    public const string Failed = "Failed";
    public const string Muted = "Muted";
    public const string Shown = "Shown";

    /// <summary>#2781/#2814's label for a stored <c>tray</c> row on a surface with no tray: a history row
    /// was written and no channel was involved, with no claim either way. Reached only by rows written
    /// before this taxonomy, since a SKU without a tray no longer records one.</summary>
    public const string Logged = "Logged";

    public const string NotSent = "Not sent";
}
