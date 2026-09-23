/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The once-a-day nudge for a self-hosted target still on the text route (#4046): a target that has NOT
/// granted <c>pg_read_binary_file</c> yet has not necessarily hit the 22021 byte either — the fault message
/// <c>DarlingWorker.PostgresFaultOutcome</c> carries only fires once a bad byte actually lands in the
/// window — so without this an operator who has simply never granted it gets no signal at all until the day
/// a failed login plants one. This is that earlier signal, throttled so it does not repeat every cycle.
///
/// <para><b>Process-wide and static, like <see cref="PgReadBinaryFileCapability"/>'s cache</b>: "a restart
/// notes once more" is then simply what a fresh process starts with — an empty dictionary — needing no
/// explicit action. <see cref="Reset"/> exists for tests, which share this process-wide state across cases
/// and must not leak a "already noted" verdict between them.</para>
///
/// <para><b>Only <c>pg_log_events</c> applies this</b> — the caller in <c>DarlingWorker.RunOneAsync</c>
/// gates on the collector name, not this type, so a target with all three log-tail collectors scheduled
/// gets ONE note a day rather than whichever of the three happens to win the 24-hour gate on a given cycle.
/// <c>pg_deadlocks</c> and <c>pg_plan_capture</c> still get the fault message's own recommendation the day a
/// byte actually blinds them; this is the earlier, quieter nudge before that day arrives.</para>
/// </summary>
public static class PgReadBinaryFileAdvisory
{
    /// <summary>
    /// The three facts the sentence carries (#4046): the readers are on the text route today, one invalid
    /// byte a failed login can plant blinds all three of them for as long as it sits in the 4 MB window, and
    /// the exact grant that switches this target onto the route with no such failure.
    /// </summary>
    public const string Sentence =
        "This target's three log readers (pg_log_events, pg_deadlocks, pg_plan_capture) are on the text "
        + "route, pg_read_file: one byte that is not valid UTF-8, which a client can plant with nothing but "
        + "a failed login, blinds all three until it leaves the 4 MB tail window (#4046). Grant EXECUTE ON "
        + "FUNCTION pg_read_binary_file(text, bigint, bigint) to the monitoring role and the collector "
        + "switches to the binary route on its own, which carries no such failure.";

    /// <summary>How often a target may be noted again. Settable for tests; a real process never sets it.</summary>
    public static TimeSpan NoteInterval { get; set; } = TimeSpan.FromHours(24);

    /* Process-wide and static for the reason the type header gives. Ordinal keys: targetKey is
       CollectorContext.ServerName / ServerRuntime.StorageName, compared exactly as PgReadBinaryFileCapability
       already compares it. */
    private static readonly ConcurrentDictionary<string, DateTime> s_lastNotedUtc = new(StringComparer.Ordinal);

    /// <summary>
    /// True the first time <paramref name="targetKey"/> is asked, and again once <see cref="NoteInterval"/>
    /// has passed since the last true answer; false in between. Recording happens on the SAME call that
    /// returns true, so a caller must actually use the true answer — asking twice in a row without noting
    /// is not this type's contract, but every real caller (<c>DarlingWorker.RunOneAsync</c>) does use it
    /// immediately, once per cycle, per target.
    /// </summary>
    public static bool ShouldNote(string targetKey)
    {
        ArgumentNullException.ThrowIfNull(targetKey);

        var now = DateTime.UtcNow;

        if (s_lastNotedUtc.TryGetValue(targetKey, out var last) && now - last < NoteInterval)
        {
            return false;
        }

        s_lastNotedUtc[targetKey] = now;
        return true;
    }

    /// <summary>Drops every recorded "already noted" verdict. Tests only.</summary>
    public static void Reset() => s_lastNotedUtc.Clear();
}
