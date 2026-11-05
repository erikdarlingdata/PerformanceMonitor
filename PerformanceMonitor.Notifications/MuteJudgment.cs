// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// What the mute rules say about one fire (#7083): muted, held for the daily digest, or neither. Every fire site asks
/// through <see cref="Judge"/>, so the order is one rule everywhere: a mute rule wins, then a digest rule's window, else the fire pages.
/// <para>A window hold stamps state the way a mute does (<see cref="Silenced"/>): the family's cooldown and edge memory move as
/// they do under a mute, only the history row's disposition differs (a digest row, not a muted one).</para>
/// </summary>
/// <param name="Muted">A mute rule matched. The host records the alert as muted.</param>
/// <param name="Hold">A digest rule's window is open and no mute rule matched. The host records a digest row and sends nothing.</param>
public readonly record struct MuteJudgment(bool Muted, AlertHold? Hold)
{
    /// <summary>The fire sends nothing, for either reason. State that a mute stamps, a window hold stamps too.</summary>
    public bool Silenced => Muted || Hold is not null;

    /// <summary>Asks <paramref name="isAlertMuted"/> first, then <paramref name="digestWindowReason"/> (null for a host with no digest
    /// rules, such as Lite). A muted fire is never also held.</summary>
    public static MuteJudgment Judge(
        Func<AlertMuteContext, bool> isAlertMuted, Func<AlertMuteContext, string?>? digestWindowReason, AlertMuteContext context)
    {
        if (isAlertMuted(context)) return new MuteJudgment(true, null);
        var reason = digestWindowReason?.Invoke(context);
        return reason is null ? default : new MuteJudgment(false, new AlertHold(reason));
    }
}
