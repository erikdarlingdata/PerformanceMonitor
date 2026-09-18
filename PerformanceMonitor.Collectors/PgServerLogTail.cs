/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The self-hosted route's log TAILER, made explicit (#3601): the two CTEs every <c>pg_read_file</c> log
/// reader opens with — find the CURRENT log file, read its last <see cref="TailBytes"/>.
///
/// <para><b>Why this is a type and not three copies.</b> <see cref="PgPlanCaptureCollector"/> and
/// <see cref="PgDeadlocksCollector"/> each carried this text inline and identical, and the log-event
/// pipeline would have been the third copy. The shared part of every log reader on this route IS these two
/// CTEs — which file, how much of it, gated on what — and the per-family part is only what each reader does
/// with <c>tail.body</c> afterwards: a <c>regexp_matches</c> for one block shape, or the whole body handed
/// to the classifier. Spelling the shared part once is what lets a fourth family arrive as an addition
/// rather than as a fourth copy of a paragraph about <c>log_directory</c>.</para>
///
/// <para><b>Constants, not a method, so every consumer's <c>QueryText</c> stays a compile-time constant</b>
/// — which the collector base relies on. The SQL each collector ships is therefore byte-for-byte what it
/// shipped before this type existed; the pins that read <c>BuildQuery</c>'s text see no change.</para>
///
/// <para><b>What the two CTEs decide, in order.</b> <c>pg_ls_logdir()</c> finds the CURRENT file rather than
/// a configured name, because <c>log_filename</c> is a strftime pattern and the real name is only knowable
/// by asking. The DIRECTORY is asked for on the same grounds: <c>pg_ls_logdir()</c> returns bare names
/// relative to <c>log_directory</c>, and <c>pg_read_file</c> resolves a relative path against the data
/// directory, so <c>current_setting('log_directory')</c> is right in both regimes — a relative setting
/// concatenates to a path under the data directory, and an absolute one resolves as itself and is readable,
/// because <c>pg_read_file</c> admits an absolute path under <c>log_directory</c> even when that sits
/// outside the data directory. Hardcoding <c>'log/'</c> is correct only where the setting holds its default,
/// and elsewhere raises 58P01 for the file this same query just listed (#3410). The tail is read from a
/// negative offset via <c>greatest(size - TailBytes, 0)</c>, so a fresh small log is read whole and a large
/// one from its end.</para>
///
/// <para><b>The listing is GATED on <c>logging_collector</c></b> (#3410): off means the server logs to
/// stderr, the log directory may legitimately not exist, and 58P01 every cycle on a deliberate
/// configuration is the wrong report. The predicate is pseudoconstant, so the planner enforces it as a
/// one-time filter and <c>pg_ls_logdir()</c> never runs when it is false. The gate carries a MARKER ROW out
/// the other side — <see cref="LoggingCollectorOffMarkerSql"/> is the <c>UNION ALL</c> arm each consumer
/// appends in its own column shape — and the consumer's <c>ReadAsync</c> turns it into
/// <see cref="PgLoggingCollectorOffException"/>, the named non-fatal skip that keeps "off" from reading as
/// a quiet server.</para>
///
/// <para><b>It is a WINDOW, not a resume marker, and that is what decides coverage.</b> The read is always
/// the last <see cref="TailBytes"/> of the current file, so consecutive cycles see overlapping text only
/// while the log grows by less than that between them — about 14 KB/s at a five-minute cadence, 1.2 KB/s
/// at sixty. Above it the windows do not meet and the span between them is read by no cycle: events lost,
/// not deferred. Nothing here measures the write rate, so no consumer can tell a quiet server from a
/// truncated view of a loud one; <see cref="PgDeadlockLogParser"/> carries the arithmetic and the transport
/// split. The managed route (<c>RdsLogSource</c>) keeps a resume marker and has no equivalent exposure.
/// Every consumer of this tail re-reads the overlap on purpose, so every consumer needs an identity column
/// its reads can dedupe on — <c>deadlock_hash</c>, <c>plan_hash</c>, <c>raw_line_hash</c>.</para>
/// </summary>
public static class PgServerLogTail
{
    /// <summary>
    /// How much of the log tail to read per cycle. Bounded because the file can reach hundreds of megabytes
    /// — #2565 measured 772 MB in twenty seconds at capture-everything — and reading it whole would turn a
    /// monitoring collector into the server's biggest reader. See the type header for what the bound costs.
    /// </summary>
    public const int TailBytes = 4 * 1024 * 1024;

    /// <summary>
    /// <see cref="TailBytes"/> spelled for splicing into SQL. A const string keeps every consumer's query a
    /// compile-time constant and keeps the number in ONE place; <c>LogTailOverlapThresholdPinTests</c>
    /// asserts the two agree.
    /// </summary>
    public const string TailBytesLiteral = "4194304";

    /// <summary>
    /// The two opening CTEs, <c>newest</c> and <c>tail</c>. A consumer's query is
    /// <c>WITH</c> + this + its own <c>SELECT ... FROM tail</c>. Ends without a trailing comma or newline,
    /// so the consumer's text follows as <c>)\nSELECT</c> exactly as it did inline.
    /// </summary>
    public const string TailCteSql = @"
WITH newest AS (
    SELECT name, size
    FROM pg_catalog.pg_ls_logdir()
    WHERE pg_catalog.current_setting('logging_collector') = 'on'
    ORDER BY modification DESC
    LIMIT 1
),
tail AS (
    SELECT pg_catalog.pg_read_file(
               pg_catalog.current_setting('log_directory') || '/' || n.name,
               greatest(n.size - " + TailBytesLiteral + @", 0),
               " + TailBytesLiteral + @") AS body
    FROM newest AS n
)";

    /// <summary>
    /// The predicate the marker arm carries: true exactly when the gate above has refused to list. Each
    /// consumer appends <c>UNION ALL SELECT &lt;marker in its own columns&gt; WHERE</c> + this, because the
    /// marker row has to match the consumer's column list and there is no column-agnostic way to say so.
    /// </summary>
    public const string LoggingCollectorOffMarkerSql =
        "pg_catalog.current_setting('logging_collector') <> 'on'";
}
