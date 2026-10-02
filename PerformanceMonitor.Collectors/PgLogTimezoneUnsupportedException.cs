/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The PostgreSQL server log is timestamped in a zone that is not UTC, so the deadlock reports in it cannot
/// be stored (#2993).
///
/// <para><b>Log events throw it too, not only deadlocks.</b> <see cref="PgLogEntryAssembler"/> is the one
/// throw site, and both <see cref="PgDeadlockLogParser"/> (pg_deadlocks) and <see cref="PgLogEventClassifier"/>
/// (pg_log_events) read through it, so the message names the log rather than either reader's rows. It said
/// "deadlock reports" and "NOT 'no deadlocks were detected'" until 2026-09-23, when DARLING01 showed that
/// sentence on a pg_log_events row.</para>
///
/// <para><b>Why this refuses rather than converting.</b> The log prefix carries the zone as an
/// ABBREVIATION, and abbreviations are ambiguous: <c>CST</c> is US Central, China Standard and Cuba
/// Standard — three offsets and two hemispheres of daylight saving. There is no correct conversion to make
/// from one, which is presumably why <see cref="PgDeadlockLogParser"/> captured the zone and never read
/// it. What the token DOES answer exactly is whether the offset is zero, and that is the only question
/// asked of it here: <c>occurred_at</c> is a naive UTC column, as every other collection on the server is.
/// </para>
///
/// <para><b>And why it is not simply skipped.</b> The parser tolerates everything else a block can be wrong
/// about, because the <c>pg_read_file</c> transport re-reads an overlapping window and a report cut at its
/// edge is whole on the next pass. This is different in both directions: it does not clear itself, and the
/// block PARSES — the
/// timestamp is well formed, the graph is intact, and the only thing wrong with the row is the moment it
/// claims. Stored, it puts every deadlock in the wrong trend bucket and misaligns it against every
/// UTC-keyed collection beside it, with nothing erroring anywhere. Dropped silently, it reads as a server
/// with no deadlocks. Thrown, the runner records the store's non-fatal degradation status with a message
/// naming <c>log_timezone</c> — a setting an operator can change, and on a managed fleet a parameter-group
/// change.</para>
/// </summary>
public sealed class PgLogTimezoneUnsupportedException : Exception
{
    /// <summary>
    /// The setting to change, named in one place so the message and any caller inspecting this cannot
    /// disagree about which GUC is at fault.
    /// </summary>
    public const string SettingName = "log_timezone";

    public PgLogTimezoneUnsupportedException(string? observedZone)
        : base(BuildMessage(observedZone))
        => ObservedZone = observedZone;

    /// <summary>
    /// The zone token the log prefix actually carried, kept alongside the message so a caller can log or
    /// group on it without parsing prose back out.
    /// </summary>
    public string? ObservedZone { get; }

    private static string BuildMessage(string? observedZone)
    {
        var zone = string.IsNullOrWhiteSpace(observedZone) ? "(none)" : observedZone.Trim();

        return "The collector stored nothing from the PostgreSQL server log on this target this cycle, "
            + "and this is NOT an empty log. "
            + $"The log is timestamped '{zone}', which is not a zero-offset zone. So {SettingName} is not "
            + "UTC, and every timestamp in the log is local time. "
            + "The collector refuses these lines instead of converting them. The zone in a log prefix is an "
            + "abbreviation, and one abbreviation can name several zones. For example, CST names three. "
            + "A conversion is a guess, and a guess stores rows that look correct but have the wrong time. "
            + $"Set {SettingName} = 'UTC' on the target. On managed PostgreSQL, change it in the parameter "
            + "group. The change takes effect on a reload, so no restart is necessary. The next collection "
            + "cycle then stores normally.";
    }
}
