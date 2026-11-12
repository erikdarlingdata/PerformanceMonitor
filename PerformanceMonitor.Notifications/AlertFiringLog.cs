/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Notifications;

/// <summary>
/// The one definition of how an alert reads in an operator's log (#1681).
///
/// <para>The bug this exists to close: resolutions were logged and FIRINGS were not, so a log showed
/// "… Recovered" with nothing before it — a spontaneous recovery from a condition that never happened,
/// which is worse than logging neither half, because anyone tailing or grepping sees half a story and
/// cannot tell whether they are missing the other half or it never occurred.</para>
///
/// <para>Both halves are formatted here rather than at each site so the pair is genuinely symmetric and
/// greppable: same shape, same field order, and an explicit TRIGGERED / RESOLVED marker so the two are
/// distinguishable without parsing severity. There are several delivery paths — the shared alert engine's
/// nine families, Darling's self-alert evaluator, and Lite's connection and Availability Group senders,
/// which bypass both — and they must not each invent their own wording.</para>
///
/// <para>Deliberately returns the message and leaves the actual logging to the caller: the callers use
/// three different logging stacks (<c>ILogger</c> in the engine and Darling, Lite's <c>AppLogger</c>), and
/// forcing one of them in here would drag a logging dependency in here for no benefit.</para>
/// </summary>
public static class AlertFiringLog
{
    /// <summary>
    /// The line for an alert that just FIRED. Severity is spelled out rather than left to the log level,
    /// because a reader filtering on text should not have to know how each app maps severity to level.
    /// A muted alert is still logged, flagged: muting suppresses the notification CHANNELS, not the
    /// operator's ability to find the event afterwards — suppressing both would make a muted condition
    /// invisible everywhere at once, which is how a muted-and-forgotten alert becomes an outage.
    /// </summary>
    /// <param name="shortMessage">
    /// Optional, because <c>AlertOutcome.ShortMessage</c> is (<c>string? ShortMessage = null</c>) and most
    /// conditions never set one. Handled here rather than at the call sites: a null forced through a
    /// non-nullable parameter is a warning at every caller and an interpolated <c>": "</c> with nothing
    /// after it in the log — so the shape a reader greps for would end in a dangling colon precisely when
    /// there is nothing to read. When it is absent the line simply stops after the server name.
    /// </param>
    public static string Fired(
        string serverName, string metricName, string severity, string? shortMessage, bool muted, bool held = false) =>
        $"ALERT TRIGGERED [{severity}] {metricName} on {serverName}"
        + (string.IsNullOrWhiteSpace(shortMessage) ? string.Empty : $": {shortMessage}")
        + (muted ? " [muted]" : held ? " [held for digest]" : "");

    /// <summary>
    /// The line for a SCHEDULED REPORT that was produced and handed to the channels (#6215): the collector-cost
    /// digest, the fleet-sweep rollup and the analysis singles digest. A report states a measurement of a
    /// period; it is not a condition that went wrong, so it must not read as <c>ALERT TRIGGERED</c> and must
    /// not be logged at Warning, which is the level the Windows event log keeps. Same shape as
    /// <see cref="Fired"/> after the marker, so a reader greps the two the same way, and the same muted flag:
    /// muting suppresses the channels, not the operator's ability to find the report in the log.
    /// </summary>
    public static string Report(string serverName, string metricName, string? shortMessage, bool muted) =>
        $"SCHEDULED REPORT {metricName} on {serverName}"
        + (string.IsNullOrWhiteSpace(shortMessage) ? string.Empty : $": {shortMessage}")
        + (muted ? " [muted]" : "");

    /// <summary>
    /// The line for an alert that CLEARED — the other half of the pair, in the same shape so the two grep
    /// together and read as one story.
    /// </summary>
    public static string Resolved(string serverName, string metricName, string message) =>
        $"ALERT RESOLVED {metricName} on {serverName}: {message}";
}
