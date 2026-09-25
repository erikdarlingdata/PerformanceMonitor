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
/// The PostgreSQL target's logging collector is on, but <c>log_destination</c> does not include
/// <c>stderr</c> (#4019), or <c>log_directory</c> holds no stderr-format log file anyway (#3997). Either way
/// there is no current file for the three log-reading collectors to parse, only <c>csvlog</c> and/or
/// <c>jsonlog</c> output.
///
/// <para><b>Why this is a third state and not "logging is off".</b> <see cref="PgLoggingCollectorOffException"/>
/// covers <c>logging_collector = off</c>: no server-managed log file exists anywhere. This is the opposite
/// shape — the collector process is running and IS writing a current file — but <c>log_destination</c> does
/// not include <c>stderr</c>, so every file it writes is one <see cref="PgServerLogTail.TailCteSql"/>'s
/// <c>newest</c> CTE deliberately excludes. A <c>csvlog</c>-only target is the ordinary way to reach this:
/// nothing is broken, the operator chose CSV-only logging, most likely for a log shipper that wants
/// structured rows and never expected a stderr-format reader downstream.</para>
///
/// <para><b>Why it refuses rather than parsing the csvlog/jsonlog file as stderr, or returning zero rows.</b>
/// A csvlog record is comma-delimited and a jsonlog record opens with <c>{</c> — neither matches the
/// stderr <c>log_line_prefix</c>-plus-severity shape every parser here anchors on, so reading either as
/// stderr finds nothing and reads as a quiet target, the #3030 failure shape. Worse, the issue this type
/// closes found a sharper edge: a csvlog record whose text happens to contain a bracketed number followed by
/// a label can satisfy the stderr prefix regex's second alternative by accident, and the parser then stores
/// the rest of that CSV row — statement column included — as if it were an ordinary log line. Refusing named
/// beats either silent failure.</para>
///
/// <para><b>Why it refuses rather than falling back to the <c>.csv</c>/<c>.json</c> file it just excluded.</b>
/// That file's records are real, but reading them as stderr text is the defect this closes, not a fallback
/// worth having — a correct csvlog reader is a distinct, unbuilt feature (its own field-splitting, its own
/// quoting rules), not a few lines added to a stderr tailer under a different name.</para>
///
/// <para><b>The remedy is a reload, not a restart</b> — the fact that tells an operator this is cheap to fix.
/// Unlike <c>logging_collector</c>, which is postmaster-context, <c>log_destination</c> is <c>sighup</c>
/// (measured against a live 18.6 target): adding <c>stderr</c> to it and reloading is enough, and the
/// existing <c>csvlog</c>/<c>jsonlog</c> output keeps flowing to whatever already reads it — this does not
/// ask anyone to give that up.</para>
///
/// <para><b>Where it comes from.</b> All three collectors' queries open with
/// <see cref="PgServerLogTail.TailCteSql"/> and append <see cref="PgServerLogTail.NoStderrLogFileMarkerSql"/>
/// as a second <c>UNION ALL</c> arm, in their own column shape, beside the existing
/// <see cref="PgServerLogTail.LoggingCollectorOffMarkerSql"/> arm; their <c>ReadAsync</c> recognises
/// <see cref="Marker"/> and throws this. Only the <c>pg_read_file</c> route can produce it — the RDS log API
/// resolves its own newest file independently (<c>RdsLogSource.NewestLogFileAsync</c>, filtered the same
/// way) and raises its own named failure when nothing opens.</para>
///
/// <para><b>Decided by the setting, not the directory (#4019).</b> The first version fired only when the
/// directory held literally ZERO non-csvlog/jsonlog files, which almost never happens. PostgreSQL's
/// syslogger writes one small stderr-format file the moment it determines stderr is not among the final
/// destinations: measured at 170 bytes on a fresh 18.6 instance started with <c>log_destination = 'csvlog'</c>
/// from its very first start, opening with <c>LOG:  ending log output to stderr</c> and never appended to
/// again. A target that ever had stderr keeps its old <c>.log</c> files as well. <c>newest</c> found that
/// file and the collectors read it every cycle as a quiet target. The marker now fires whenever
/// <c>log_destination</c> lacks <c>stderr</c>
/// (<see cref="PgServerLogTail.NoStderrLogFileMarkerSql"/>), which is exact. A byte-size rule would have
/// misread a quiet stderr target, and a match on the file's text goes blind under a translated
/// <c>lc_messages</c>.</para>
/// </summary>
public sealed class PgNoStderrLogFileException : Exception
{
    /// <summary>
    /// The setting whose value decides this state, named in one place so the message, the queries' marker
    /// and any caller inspecting this cannot disagree about which GUC is at fault.
    /// </summary>
    public const string SettingName = "log_destination";

    /// <summary>
    /// The marker value the collectors' queries return in place of log rows when <c>logging_collector</c> is
    /// on but no stderr-format file exists, and the value their <c>ReadAsync</c> recognises. Distinct from
    /// <see cref="PgLoggingCollectorOffException.Marker"/> so the two named skips cannot be confused with
    /// one another downstream.
    /// </summary>
    public const string Marker = "no_stderr_log_file";

    public PgNoStderrLogFileException()
        : base(BuildMessage())
    {
    }

    private static string BuildMessage() =>
        $"logging_collector is on, but {SettingName} on this target does not include stderr, so there is no "
        + "server-managed stderr-format log file to read for deadlocks or plan captures — only its csvlog "
        + "and/or jsonlog output, if any, which this product does not parse as stderr text (a csvlog row is "
        + "comma-delimited and a jsonlog row opens with '{', so a reader built for the stderr shape would "
        + "either find nothing or, worse, misread a CSV field as a log line). pg_log_events is read from the "
        + $"csvlog file directly when {SettingName} includes csvlog (#4053), so log events keep flowing even "
        + "without stderr — this refusal is deadlocks' and plan captures' only, and this is NOT 'the log held "
        + $"nothing'. To capture deadlocks and plans from this server's log too, add stderr to {SettingName} "
        + "(for example 'stderr,csvlog' to keep csvlog too) and reload the configuration — it is a SIGHUP "
        + "setting, so a reload is enough and this does NOT need a server restart. Recorded as a named "
        + "non-fatal skip rather than an error so it does not fill the log every cycle; the collector retries "
        + $"every cycle and starts collecting on the first one after {SettingName} carries stderr.";
}
