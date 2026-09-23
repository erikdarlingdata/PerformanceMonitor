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
/// One ASSEMBLED PostgreSQL server-log entry (#3601): the primary line plus every companion field line
/// PostgreSQL wrote for the same message — <c>DETAIL</c>, <c>HINT</c>, <c>STATEMENT</c>, <c>CONTEXT</c>,
/// <c>QUERY</c>, <c>LOCATION</c> — with their tab-indented continuations folded in.
///
/// <para><b>This is the seam every log family parser consumes, on both transports.</b> The deadlock and
/// plan parsers each matched their block out of raw text with a regex that re-derived the log prefix —
/// timestamp, zone, pid — for themselves, which is why <see cref="PgDeadlockLogParser"/> carries a page on
/// the two prefix families. The assembler derives that ONCE and hands every parser the same record, so a
/// family parser (#3602's <c>log_temp_files</c>, #3603's autovacuum completions) reads <see cref="Message"/>
/// and the companion fields and never sees a prefix at all.</para>
///
/// <para><b><see cref="OccurredAtUtc"/> is already checked.</b> The assembler has applied
/// <see cref="PgDeadlockLogParser.IsZeroOffsetLogZone"/> to the prefix zone before this record exists, and
/// refused the whole read (<see cref="PgLogTimezoneUnsupportedException"/>) where it was not zero — the
/// same rule, the same exception, for the same reason (#2993). A parser does not have to think about
/// <c>log_timezone</c>.</para>
///
/// <para><b>Nothing here is normalized.</b> This is the RAW entry, and it is what a parser needs to extract
/// structure from — a lock wait's pid and mode, a spill's byte count. The SQL in it is normalized where it is
/// about to be STORED, in <see cref="PgLogTextRedactor"/> inside <see cref="PgLogEvent.From"/>, and the
/// statement is only fingerprinted. An entry's SQL must not leave the collector process as written, and the
/// type boundary is what says so: <see cref="PgLogEntry"/> is in-process only, <see cref="PgLogEvent"/> is
/// what gets written.</para>
/// </summary>
/// <param name="TimestampText">The prefix's <c>%m</c> or <c>%t</c> stamp as written.</param>
/// <param name="ZoneText">The prefix's zone token as written — <c>log_timezone</c> rendered for THIS line.</param>
/// <param name="OccurredAtUtc">The stamp, read as UTC. Kind is Utc.</param>
/// <param name="Pid">The backend's <c>%p</c>.</param>
/// <param name="PrefixRest">Whatever the prefix carried between the pid and the severity label — user@db
/// under a <c>%u@%d</c> prefix, a query id under <c>%Q</c>, a SQLSTATE under <c>%e</c>. Best-effort
/// extraction from it lives on the assembler; the raw text is kept so a parser can ask for more.</param>
/// <param name="Severity">PostgreSQL's own label: LOG, INFO, NOTICE, WARNING, ERROR, FATAL, PANIC (or a
/// DEBUGn, which nothing stores).</param>
/// <param name="Message">The primary line's text after <c>SEVERITY:  </c>, continuations joined with
/// newlines and their leading tab removed.</param>
/// <param name="Detail">The <c>DETAIL:</c> companion, or null.</param>
/// <param name="Hint">The <c>HINT:</c> companion, or null.</param>
/// <param name="Statement">The <c>STATEMENT:</c> companion — the user's SQL, with literals — or null.</param>
/// <param name="Context">The <c>CONTEXT:</c> companion, or null.</param>
/// <param name="UserName">From the prefix where it carried <c>%u@%d</c>; null otherwise. A family parser
/// may override from the message (connection lines name the user themselves).</param>
/// <param name="DatabaseName">From the prefix where it carried <c>%u@%d</c>; null otherwise.</param>
/// <param name="SqlState">From the prefix where it carried <c>%e</c>; null otherwise. The default stderr
/// prefix does not, so this is null on most self-hosted targets.</param>
/// <param name="RawText">The whole entry as it appeared in the log — every line, verbatim. Hashed for
/// identity across overlapping tail reads; never stored.</param>
public readonly record struct PgLogEntry(
    string TimestampText,
    string ZoneText,
    DateTime OccurredAtUtc,
    int Pid,
    string PrefixRest,
    string Severity,
    string Message,
    string? Detail,
    string? Hint,
    string? Statement,
    string? Context,
    string? UserName,
    string? DatabaseName,
    string? SqlState,
    string RawText)
{
    /// <summary>
    /// PostgreSQL's severity labels ranked by SERIOUSNESS, which is the order a reader filtering on
    /// "at least WARNING" means. Deliberately NOT <c>log_min_messages</c>' order, where LOG sits ABOVE
    /// ERROR because that setting ranks by how hard a message is to suppress rather than by how bad it is
    /// — a SQL Server DBA reading "LOG outranks ERROR" would draw exactly the wrong conclusion.
    /// <see cref="PgLogEvent.SeverityRank"/> and the read's <c>min_severity</c> use this one ordering.
    /// </summary>
    public static int RankOf(string? severity) => severity switch
    {
        "PANIC" => 6,
        "FATAL" => 5,
        "ERROR" => 4,
        "WARNING" => 3,
        "NOTICE" => 2,
        "INFO" => 1,
        "LOG" => 1,
        _ => 0,
    };
}
