// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

namespace PerformanceMonitor.Notifications;

/// <summary>
/// An evaluator's decision to hold one fire back from paging (#7086): the fire is recorded as a digest row
/// (<see cref="AlertDelivery.RoutedToDigest"/>) and nothing is sent, opened for a webhook or toasted. Carried on
/// <c>AlertOutcome.Hold</c>.
/// <para><b>What the deliverers do.</b> A held fire writes its history row with the <c>digest</c> disposition and
/// <see cref="AlertContext.Routing"/> set to <c>("digest", Reason)</c>, so "why didn't this page" is answered by the
/// row. A muted fire wins over a held one: a row a mute rule matched is recorded as muted.</para>
/// <para><b>What the evaluator owes.</b> A held fire is not a page. The family does not stamp its page cooldown and
/// does not record an escalation tier for it, so a real page for the same key right after it goes out at once. The
/// standing-fire reads of both history stores leave digest rows out, so a restart never seeds a tier or an open
/// fire from a held row.</para>
/// </summary>
/// <param name="Reason">One sentence saying why the fire was held (what the bar is and how far the condition got),
/// stored on the row as the routing reason and shown as the status tooltip on the web.</param>
public sealed record AlertHold(string Reason)
{
    /// <summary>The routing record a held fire's history row carries: route <c>digest</c> and the hold's reason.</summary>
    public AlertRoutingDto ToRouting() => new(FindingRouting.DigestText, Reason);
}
