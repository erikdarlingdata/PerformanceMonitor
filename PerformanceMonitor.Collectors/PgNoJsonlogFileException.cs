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
/// The jsonlog route's own "nothing to read yet" state (#4053 part a2): <c>logging_collector</c> is on and
/// <see cref="CollectorContext.PgLogUsesJsonlog"/> found <c>jsonlog</c> in <c>log_destination</c>, but
/// <see cref="PgServerLogTail.TailJsonCteSql"/>'s <c>newest</c> came back empty anyway — no <c>.json</c>
/// file has appeared under <c>log_directory</c> yet, most likely because the target only just added
/// <c>jsonlog</c> to its destinations and the syslogger has not rolled a file since.
///
/// <para><b>Why this is not <see cref="PgNoCsvlogFileException"/> or <see cref="PgNoStderrLogFileException"/>.</b>
/// Those types' names and messages are about the CSVLOG and STDERR routes finding no file of their own
/// format; firing either here, on the jsonlog route finding no JSON file, would name the wrong setting to
/// an operator whose target already has <c>jsonlog</c> configured and IS being read by it. The predicate
/// this type's marker rides is the SAME shape as its siblings' — <c>logging_collector = 'on' AND NOT
/// EXISTS (SELECT 1 FROM newest)</c>, read off the jsonlog statement's own <c>newest</c> CTE rather than
/// the stderr or csvlog one — but the marker literal and the exception it throws are distinct, so
/// <c>PgLogEventsCollector.ReadAsync</c> can never confuse the three "no file yet" states and no fault
/// message can name the wrong destination.</para>
///
/// <para><b>Where it comes from.</b> <c>PgLogEventsCollector</c>'s jsonlog statement appends this marker as
/// a second <c>UNION ALL</c> arm, beside <see cref="PgLoggingCollectorOffException.Marker"/>'s arm, in its
/// own column shape; its <c>ReadAsync</c> recognises <see cref="Marker"/> and throws this. Only the
/// jsonlog route can produce it — the csvlog route's own marker is <see cref="PgNoCsvlogFileException"/>'s
/// and the stderr route's is <see cref="PgNoStderrLogFileException"/>'s.</para>
/// </summary>
public sealed class PgNoJsonlogFileException : Exception
{
    /// <summary>
    /// The marker value the jsonlog statement returns in place of log rows when <c>logging_collector</c> is
    /// on but no <c>.json</c> file exists yet, and the value <c>PgLogEventsCollector.ReadAsync</c>
    /// recognises. Distinct from <see cref="PgNoCsvlogFileException.Marker"/> and
    /// <see cref="PgNoStderrLogFileException.Marker"/> so the three named skips cannot be confused with one
    /// another downstream.
    /// </summary>
    public const string Marker = "no_jsonlog_file";

    public PgNoJsonlogFileException()
        : base(BuildMessage())
    {
    }

    private static string BuildMessage() =>
        "logging_collector is on and log_destination includes jsonlog on this target, but no .json file has "
        + "appeared under log_directory yet — most likely jsonlog was only just added to log_destination and "
        + "the syslogger has not rolled a file since the reload. There is no server-managed jsonlog file to "
        + "read, so nothing was read this cycle, and this is NOT 'the log held nothing'. Recorded as a named "
        + "non-fatal skip rather than an error so it does not fill the log every cycle; the collector retries "
        + "every cycle and starts collecting on the first one after a .json file exists.";
}
