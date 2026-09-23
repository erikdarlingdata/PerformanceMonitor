/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data.Common;

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
/// <para><b><c>newest</c> excludes <c>.csv</c> and <c>.json</c> siblings</b> (#3997). A target whose
/// <c>log_destination</c> includes <c>csvlog</c> or <c>jsonlog</c> writes every message to a second file
/// beside the stderr one — measured live, same second, same content, name unchanged but for the suffix
/// (<c>postgresql-2026-09-23_070937.log</c> and <c>...csv</c>). <c>pg_ls_logdir()</c>'s <c>modification</c>
/// is a stat mtime with no ordering guarantee between two files written the same instant, so
/// <c>ORDER BY modification DESC LIMIT 1</c> alone can return either sibling — measured: both files above
/// carry the identical mtime. Every consumer's parser matches the stderr line shape (<c>log_line_prefix</c>
/// followed by a severity label); a csvlog record is comma-delimited and a jsonlog record opens with
/// <c>{</c>, so a cycle that picked either sibling read nothing a parser recognises and reported a quiet
/// target instead of a wrong one. The filter mirrors <c>StoreLogSweep.IsStderrLogFile</c> — the store's own
/// log sweep made the identical call for the identical reason — so there is one rule for "is this file
/// stderr-format" rather than two that could drift.</para>
///
/// <para><b>And <c>newest</c> lists nothing unless <c>log_destination</c> includes <c>stderr</c></b> (#4019).
/// Without stderr as a destination, whatever <c>.log</c> file the directory still holds is stale: the
/// syslogger's one-off "ending log output to stderr" file, or output from before the setting changed. Reading
/// it every cycle showed a quiet target, or a refusal about that stale file's timestamps. An empty
/// <c>newest</c> sends the query to <see cref="NoStderrLogFileMarkerSql"/>'s named refusal instead. The value
/// is compared case-insensitively with spaces removed, since PostgreSQL accepts <c>'Stderr, CSVlog'</c>.</para>
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
/// <para><b>A second, narrower gap gets the same treatment</b> (#3997): <c>logging_collector</c> can be ON
/// with <c>log_destination</c> configured as <c>csvlog</c> alone, with <c>newest</c> coming back with
/// literally nothing in it. <see cref="NoStderrLogFileMarkerSql"/> is the second <c>UNION ALL</c> arm every
/// consumer appends, true exactly when that happens while the collector is otherwise on, and each consumer's
/// <c>ReadAsync</c> turns it into <see cref="PgNoStderrLogFileException"/> — the same named-skip treatment as
/// the collector-off case, and for the same reason: silently reading zero rows here is indistinguishable from
/// a target with nothing to report. Never satisfied at the same time as
/// <see cref="LoggingCollectorOffMarkerSql"/> — one requires the setting to be off, the other requires it to
/// be on — so a consumer's two marker checks are mutually exclusive by construction, not by convention. Its
/// own remarks measure exactly how narrow "literally nothing in it" is in practice, and #4019 is the open
/// question that measurement raised.</para>
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
      AND name !~* '\.(csv|json)$'
      AND 'stderr' = ANY (pg_catalog.string_to_array(pg_catalog.lower(pg_catalog.replace(pg_catalog.current_setting('log_destination'), ' ', '')), ','))
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
    /// The binary-route twin of <see cref="TailCteSql"/> (#4046 part 1c): the same file, the same offsets,
    /// the same gates — <c>pg_read_binary_file</c> in place of <c>pg_read_file</c>, returning <c>bytea</c>
    /// instead of <c>text</c>. A consumer sends this text INSTEAD OF <see cref="TailCteSql"/>, never both
    /// in one statement — <see cref="PgReadBinaryFileCapability"/>'s doc comment says why a <c>CASE</c>
    /// cannot pick between them at runtime — and only once <see cref="PgReadBinaryFileCapability.IsGrantedAsync"/>
    /// finds the grant. Kept as an independent literal rather than built from a shared fragment with
    /// <see cref="TailCteSql"/>, so a change here can never alter the byte-for-byte pin on that constant.
    /// </summary>
    public const string TailCteBinarySql = @"
WITH newest AS (
    SELECT name, size
    FROM pg_catalog.pg_ls_logdir()
    WHERE pg_catalog.current_setting('logging_collector') = 'on'
      AND name !~* '\.(csv|json)$'
      AND 'stderr' = ANY (pg_catalog.string_to_array(pg_catalog.lower(pg_catalog.replace(pg_catalog.current_setting('log_destination'), ' ', '')), ','))
    ORDER BY modification DESC
    LIMIT 1
),
tail AS (
    SELECT pg_catalog.pg_read_binary_file(
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

    /// <summary>
    /// The predicate the second marker arm carries (#3997): true exactly when <c>logging_collector</c> is on
    /// — so <see cref="LoggingCollectorOffMarkerSql"/> is false — and <c>newest</c> came back empty.
    ///
    /// <para><b>The setting decides, not the directory (#4019).</b> <c>newest</c> is empty whenever
    /// <c>log_destination</c> lacks <c>stderr</c>, because <see cref="TailCteSql"/> checks the setting before it
    /// lists anything. A directory alone can't answer it: the syslogger writes one small "ending log output to
    /// stderr" file the moment it determines stderr is not a destination (170 bytes on a fresh 18.6 target
    /// started with <c>log_destination = 'csvlog'</c>), and a target that ever had stderr keeps its old
    /// <c>.log</c> files. When only the directory decided, <c>newest</c> found that stale file, this marker
    /// almost never fired, and the collectors read the file every cycle as a quiet target, or refused it for its
    /// old timestamps' zone. Reading the setting is exact where a file size or a match on the file's text would
    /// guess, and the text is also translated under a non-English <c>lc_messages</c>. Emptiness still covers a
    /// directory that holds nothing but csvlog output while <c>stderr</c> is configured.</para>
    ///
    /// <para>References <c>newest</c> rather than re-listing the directory, so a target where
    /// <c>pg_ls_logdir()</c> is itself expensive is not asked twice, and so the two markers read from the exact
    /// same listing. Each consumer appends <c>UNION ALL SELECT &lt;marker in its own columns&gt; WHERE</c> +
    /// this, the same shape as the marker above.</para>
    /// </summary>
    public const string NoStderrLogFileMarkerSql =
        "pg_catalog.current_setting('logging_collector') = 'on' AND NOT EXISTS (SELECT 1 FROM newest)";

    /// <summary>
    /// The target's own <c>log_timezone</c>, which a consumer that reads each line's zone selects beside the text
    /// (#4046), in the same statement, so the setting and the tail are one read. When it renders UTC
    /// (<see cref="PgDeadlockLogParser.IsUtcLogTimezoneSetting"/>), a line stamped in another zone is not the server's
    /// own, and the reader skips and counts it instead of refusing the target. The marker arms carry NULL in its
    /// place, and a NULL keeps today's refusal. Plan capture reads no zone and does not select it.
    /// </summary>
    public const string LogTimezoneSql = "pg_catalog.current_setting('log_timezone')";

    /// <summary>
    /// Whether the current row's <see cref="LogTimezoneSql"/> column, at <paramref name="ordinal"/>, renders UTC
    /// (#4046). False for a NULL (a marker row) and for a reader without the column, which keeps today's refusal.
    /// </summary>
    public static bool LogTimezoneIsUtc(DbDataReader reader, int ordinal)
    {
        ArgumentNullException.ThrowIfNull(reader);
        return reader.FieldCount > ordinal
            && !reader.IsDBNull(ordinal)
            && PgDeadlockLogParser.IsUtcLogTimezoneSetting(reader.GetString(ordinal));
    }

    /// <summary>
    /// The count a consumer records on its collection-log row when a read skipped lines as not the server's own
    /// (#4046, <see cref="PgLogEntryAssembler.Assemble(string?, bool, out int)"/>); <see cref="ForeignZoneLinesNote"/>
    /// says what it counts.
    /// </summary>
    public const string ForeignZoneLinesMeasurement = "foreign_zone_lines_skipped";

    /// <summary>
    /// The sentence the Darling runner puts beside <see cref="ForeignZoneLinesMeasurement"/> on the row (#4046): a
    /// count label cannot name the setting or the issue, and an operator who finds the count needs both.
    /// </summary>
    public const string ForeignZoneLinesNote =
        "Skipped log lines stamped in a zone other than UTC, which this target's UTC log_timezone did not write: a "
        + "client can plant one with a failed login, since %u and %d in log_line_prefix echo its role and database "
        + "names unescaped, and lines from before a log_timezone change look the same. The read went on without "
        + "them (#4046)";

    /// <summary>
    /// Records the lines a read skipped as not the server's own (#4046) on this run's collection-log row. Nothing
    /// for zero, so an ordinary run's row stays as it was.
    /// </summary>
    public static void MeasureForeignZoneLines(CollectorContext context, int foreignZoneLines)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (foreignZoneLines > 0)
        {
            context.Measure(ForeignZoneLinesMeasurement, foreignZoneLines);
        }
    }
}
