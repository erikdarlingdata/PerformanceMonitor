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
/// The csvlog route's own "nothing to read yet" state (#4053 part a1b): <c>logging_collector</c> is on and
/// <see cref="CollectorContext.PgLogUsesCsvlog"/> found <c>csvlog</c> in <c>log_destination</c>, but
/// <see cref="PgServerLogTail.TailCsvCteSql"/>'s <c>newest</c> came back empty anyway — no <c>.csv</c> file
/// has appeared under <c>log_directory</c> yet, most likely because the target only just added <c>csvlog</c>
/// to its destinations and the syslogger has not rolled a file since.
///
/// <para><b>Why this is not <see cref="PgNoStderrLogFileException"/>.</b> That type's name and message are
/// about the STDERR route finding no stderr-format file; firing it here, on the csvlog route finding no CSV
/// file, would tell an operator to add <c>stderr</c> to a target that already has <c>csvlog</c> configured
/// and IS being read by it — the wrong remedy for a target that is not actually broken. The predicate this
/// type's marker rides is the SAME shape as that type's — <c>logging_collector = 'on' AND NOT EXISTS (SELECT
/// 1 FROM newest)</c>, read off the csvlog statement's own <c>newest</c> CTE rather than the stderr one — but
/// the marker literal and the exception it throws are distinct, so <c>PgLogEventsCollector.ReadAsync</c> can
/// never confuse "no stderr file" with "no csvlog file yet" and neither fault message can name the wrong
/// setting.</para>
///
/// <para><b>Where it comes from.</b> <c>PgLogEventsCollector</c>'s csvlog statement appends this marker as a
/// second <c>UNION ALL</c> arm, beside <see cref="PgLoggingCollectorOffException.Marker"/>'s arm, in its own
/// column shape; its <c>ReadAsync</c> recognises <see cref="Marker"/> and throws this. Only the csvlog route
/// can produce it — the stderr route's own marker is <see cref="PgNoStderrLogFileException"/>'s.</para>
/// </summary>
public sealed class PgNoCsvlogFileException : Exception
{
    /// <summary>
    /// The marker value the csvlog statement returns in place of log rows when <c>logging_collector</c> is on
    /// but no <c>.csv</c> file exists yet, and the value <c>PgLogEventsCollector.ReadAsync</c> recognises.
    /// Distinct from <see cref="PgNoStderrLogFileException.Marker"/> so the two named skips cannot be confused
    /// with one another downstream.
    /// </summary>
    public const string Marker = "no_csvlog_file";

    public PgNoCsvlogFileException()
        : base(BuildMessage())
    {
    }

    private static string BuildMessage() =>
        "logging_collector is on and log_destination includes csvlog on this target, but no .csv file has "
        + "appeared under log_directory yet — most likely csvlog was only just added to log_destination and "
        + "the syslogger has not rolled a file since the reload. There is no server-managed csvlog file to "
        + "read, so nothing was read this cycle, and this is NOT 'the log held nothing'. Recorded as a named "
        + "non-fatal skip rather than an error so it does not fill the log every cycle; the collector retries "
        + "every cycle and starts collecting on the first one after a .csv file exists.";
}
