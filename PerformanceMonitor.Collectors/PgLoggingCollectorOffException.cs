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
/// The PostgreSQL server's logging collector is off, so there is no server-managed log file for the two
/// log-reading collectors to read (#3410).
///
/// <para><b>Why this is a third state and not a flavour of failure.</b> With <c>logging_collector = off</c>
/// the server writes its log to stderr and whatever started the server captures it — systemd's journal, a
/// container runtime, a redirect — where no SQL function can follow. That is neither <c>42501</c>'s missing
/// grant nor <c>58P01</c>'s missing file: no grant and no path change can produce a file the server is not
/// writing. Before this type existed the condition surfaced as whatever error the directory listing happened
/// to raise — <c>58P01</c> where the log directory was never created — and recorded ERROR every cycle
/// forever, which reads as a broken collector on a server that is configured, deliberately and legitimately,
/// to log somewhere else.</para>
///
/// <para><b>Why it refuses rather than returning zero rows.</b> Zero rows is the healthy resting state for
/// the deadlock read, so a silent zero here is a server that logs to stderr reading as a server that does
/// not deadlock — the #3030 failure shape from yet another cause. The store's own log read already made this
/// call the same way: <c>StoreLogSweep</c> deliberately writes no heartbeat when the directory listing comes
/// back empty, so <c>get_store_log</c> reports NOT-COLLECTED with the reason instead of a series of
/// successful empty reads. This is that shape on the monitored-server side. Thrown, the runner records the
/// store's non-fatal degradation status with this message — never ERROR, so it sits outside every error
/// count that feeds collector health, the daily band and the collection-failure self-alerts — and the
/// precondition read quotes it back to whoever asks why there is no data.</para>
///
/// <para><b>Where it comes from.</b> Both collectors' queries gate the directory listing on
/// <c>current_setting('logging_collector')</c> and return a marker row instead of listing when it is off;
/// their <c>ReadAsync</c> recognises the marker and throws this. Only the <c>pg_read_file</c> route can
/// produce it — the RDS log API dispatches to its own ingestors, and on managed PostgreSQL the logging
/// collector is on by the platform's own hand.</para>
/// </summary>
public sealed class PgLoggingCollectorOffException : Exception
{
    /// <summary>
    /// The setting that decides this state, named in one place so the message, the queries' marker and any
    /// caller inspecting this cannot disagree about which GUC is at fault.
    /// </summary>
    public const string SettingName = "logging_collector";

    /// <summary>
    /// The marker value the collectors' queries return in place of log rows when the setting is off, and the
    /// value their <c>ReadAsync</c> recognises. A constant rather than a computed string because the setting
    /// is a boolean GUC: <c>off</c> is the only value that is not <c>on</c>, so there is nothing variable to
    /// carry. Spliced into both queries from here, so the SQL and the C# check are one spelling.
    /// </summary>
    public const string Marker = SettingName + "=off";

    public PgLoggingCollectorOffException()
        : base(BuildMessage())
    {
    }

    private static string BuildMessage() =>
        $"{SettingName} is off on this target, so PostgreSQL writes its log to stderr and whatever started "
        + "the server captures it — systemd's journal, a container runtime, a redirect — where no SQL "
        + "function can follow. There are no server-managed log files to read, so nothing was read this "
        + "cycle, and this is NOT 'the log held nothing'. Neither a grant nor a path change helps: this is "
        + "not 42501's missing privilege and not 58P01's missing file, and anything a leftover log "
        + "directory still holds predates the setting being switched off, which is why it is deliberately "
        + $"not read. To capture deadlocks and plans from this server's log, set {SettingName} = on — it "
        + "is a postmaster setting, so it takes a server RESTART rather than a reload. Recorded as a named "
        + "non-fatal skip rather than an error so it does not fill the log every cycle; the collector "
        + "retries every cycle and starts collecting on the first one after the setting is on.";
}
