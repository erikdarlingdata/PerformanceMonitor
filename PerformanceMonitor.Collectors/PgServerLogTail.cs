/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;

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
/// <para><b>It is a resume marker with a line-aligned overlap</b> (#4699). A consumer that keeps a marker
/// under <see cref="ResumeStateKey"/> passes the file and byte offset the previous read reached; this read finishes that
/// file from the offset (the last <see cref="TailBytes"/> when the backlog is larger), then reads the newest file, so
/// lines written before a rotation are collected. The next offset is the first line start inside the last
/// <see cref="ResumeOverlapBytes"/> read, so a cut entry is whole in the next read; the identity hashes dedupe the
/// overlap. Loss-free up to (<see cref="TailBytes"/> minus the overlap) of log per interval; beyond that, and when the
/// marked file is gone or recycled, the read says so on the collection-log row (<see cref="BytesSkippedMeasurement"/>,
/// <see cref="FilesSkippedByRotationMeasurement"/>, <see cref="ResumeFileMissingMeasurement"/>,
/// <see cref="ResumeFileRecycledMeasurement"/>). With no marker (first contact, or a consumer that has not adopted
/// one) the read is the last <see cref="TailBytes"/> of the newest file. The csvlog and jsonlog twins do the same under their own keys
/// (<see cref="ResumeStateKeyCsv"/>, <see cref="ResumeStateKeyJson"/>). The managed route (<c>RdsLogSource</c>) keeps its own resume marker.
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

    /// <summary>The state key a consumer keeps its resume marker under: <c>"&lt;offset&gt;|&lt;file name&gt;"</c>.</summary>
    public const string ResumeStateKey = "log_resume";

    /// <summary>
    /// How much of what a read returned the next read covers again, line-aligned. The overlap keeps the guarantee
    /// that an entry cut at the end of one read is whole in the next; the identity hashes dedupe it at read time.
    /// </summary>
    public const int ResumeOverlapBytes = 1024 * 1024;

    /// <summary><see cref="ResumeOverlapBytes"/> spelled for splicing into SQL.</summary>
    public const string ResumeOverlapBytesLiteral = "1048576";

    /// <summary>The text every resume row starts with.</summary>
    public const string ResumeRowPrefix = "pm-log-resume|";

    /// <summary>
    /// The resume row's text: next offset, files skipped, bytes skipped, fallback reason, then the file name last so
    /// a <c>|</c> inside <c>log_filename</c> cannot break the parse.
    /// </summary>
    public const string ResumeRowSql =
        "'pm-log-resume|' || r.next_offset || '|' || r.skipped_files || '|' || r.skipped_bytes || '|' || r.fallback || '|' || r.name";

    /// <summary>The csvlog route's marker key: a target that switches <c>log_destination</c> never reads another format's file name as a missing file.</summary>
    public const string ResumeStateKeyCsv = "log_resume_csv";

    /// <summary>The jsonlog route's marker key.</summary>
    public const string ResumeStateKeyJson = "log_resume_json";

    /// <summary>The state keys a resuming consumer declares: one per log format.</summary>
    public static IReadOnlyList<string> ResumeStateKeys { get; } = new[] { ResumeStateKey, ResumeStateKeyCsv, ResumeStateKeyJson };

    /// <summary>The marker key for the route <paramref name="context"/> selects: jsonlog wins over csvlog, then stderr.</summary>
    public static string ResumeStateKeyFor(CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return context.PgLogUsesJsonlog ? ResumeStateKeyJson : context.PgLogUsesCsvlog ? ResumeStateKeyCsv : ResumeStateKey;
    }

    /// <summary>Count of log files between the marked file and the newest that no read opened.</summary>
    public const string FilesSkippedByRotationMeasurement = "log_files_skipped_by_rotation";

    /// <summary>Bytes before the read window that no read covered.</summary>
    public const string BytesSkippedMeasurement = "log_bytes_skipped";

    /// <summary>1 when the file the marker names is gone from the log directory.</summary>
    public const string ResumeFileMissingMeasurement = "log_resume_file_missing";

    /// <summary>1 when the file the marker names is smaller than the offset already read.</summary>
    public const string ResumeFileRecycledMeasurement = "log_resume_file_recycled";

    /// <summary>The sentence beside <see cref="ResumeFileMissingMeasurement"/> and <see cref="ResumeFileRecycledMeasurement"/>.</summary>
    public const string LogResumeLostNote =
        "The log file this collector had read up to is gone from log_directory, or is now smaller than the offset "
        + "already read (recycled under the same name), so this read fell back to the newest file's last 4 MB and "
        + "whatever the old file received after the previous read was not collected (#4699)";

    /// <summary>The sentence beside <see cref="FilesSkippedByRotationMeasurement"/>.</summary>
    public const string LogFilesSkippedNote =
        "More than one log rotation happened between two reads: this read finished the file the previous read stopped "
        + "in and read the newest one, and log_files_skipped_by_rotation counts the files between them that no read "
        + "opened (#4699)";

    /// <summary>The sentence beside <see cref="BytesSkippedMeasurement"/>.</summary>
    public const string LogBytesSkippedNote =
        "The log grew by more than the 4 MB read window since the previous read, so this read took the last 4 MB and "
        + "log_bytes_skipped counts the bytes before it that no read covered (#4699)";

    /// <summary>
    /// The query for a consumer of the stderr tail: the text plus the two resume parameters, bound from
    /// <see cref="CollectorContext.State"/> (NULL on first contact).
    /// </summary>
    public static CollectorQuery WithResume(string text, CollectorContext context, string stateKey = ResumeStateKey)
    {
        ArgumentNullException.ThrowIfNull(context);

        string? file = null;
        long? offset = null;

        if (context.State.TryGetValue(stateKey, out var value) && TryParseResumeState(value, out var f, out var o))
        {
            file = f;
            offset = o;
        }

        return new CollectorQuery(text, new[]
        {
            new CollectorParameter("@log_resume_file", file, CollectorParameterType.NVarChar260),
            new CollectorParameter("@log_resume_offset", offset, CollectorParameterType.BigInt),
        });
    }

    /// <summary>Parses <c>"&lt;offset&gt;|&lt;file name&gt;"</c>; false for anything else.</summary>
    internal static bool TryParseResumeState(string? value, out string file, out long offset)
    {
        file = string.Empty;
        offset = 0;

        if (string.IsNullOrEmpty(value))
        {
            return false;
        }

        var bar = value.IndexOf('|', StringComparison.Ordinal);

        if (bar <= 0 || bar == value.Length - 1)
        {
            return false;
        }

        if (!long.TryParse(value.AsSpan(0, bar), NumberStyles.None, CultureInfo.InvariantCulture, out offset))
        {
            return false;
        }

        file = value[(bar + 1)..];
        return true;
    }

    /// <summary>
    /// True when the row is the resume row (consumed, never parsed as log). Stages the marker under <see cref="ResumeStateKeyFor"/> (the route's own key) only when well-formed
    /// and <paramref name="mayAdvance"/>; the runner persists <see cref="CollectorContext.PendingState"/> only after
    /// the COPY returned.
    /// </summary>
    public static bool TryConsumeResumeRow(string? text, bool fillColumnIsNull, bool mayAdvance, CollectorContext context)
    {
        if (!TryConsumeResumeRow(text, fillColumnIsNull, context, out var next))
        {
            return false;
        }

        if (mayAdvance && next is not null)
        {
            context.PendingState[ResumeStateKeyFor(context)] = next;
        }

        return true;
    }

    /// <summary>
    /// The same recognition and measurements as the four-argument overload, but the marker is RETURNED in
    /// <paramref name="nextMarker"/> (null when the row was malformed) instead of staged, so a reader that only
    /// learns after its loop whether the row limit cut the match set can stage it then. The marker text is
    /// the <see cref="ResumeStateKey"/> value format, in one place.
    /// </summary>
    public static bool TryConsumeResumeRow(string? text, bool fillColumnIsNull, CollectorContext context, out string? nextMarker)
    {
        ArgumentNullException.ThrowIfNull(context);
        nextMarker = null;

        if (!fillColumnIsNull || text is null || !text.StartsWith(ResumeRowPrefix, StringComparison.Ordinal))
        {
            return false;
        }

        var p = text[ResumeRowPrefix.Length..].Split('|', 5);

        if (p.Length != 5 || p[4].Length == 0
            || !long.TryParse(p[0], NumberStyles.None, CultureInfo.InvariantCulture, out var next)
            || !long.TryParse(p[1], NumberStyles.None, CultureInfo.InvariantCulture, out var files)
            || !decimal.TryParse(p[2], NumberStyles.None, CultureInfo.InvariantCulture, out var bytes))
        {
            return true;
        }

        nextMarker = next.ToString(CultureInfo.InvariantCulture) + "|" + p[4];

        if (files > 0)
        {
            context.Measure(FilesSkippedByRotationMeasurement, files);
        }

        if (bytes > 0)
        {
            context.Measure(BytesSkippedMeasurement, (long)Math.Min(bytes, long.MaxValue));
        }

        if (p[3] == "missing")
        {
            context.Measure(ResumeFileMissingMeasurement, 1);
        }

        if (p[3] == "recycled")
        {
            context.Measure(ResumeFileRecycledMeasurement, 1);
        }

        return true;
    }

    /// <summary>The C# twin of the resume CTE's <c>next_offset</c> rule, for pins.</summary>
    public static long NextResumeOffset(ReadOnlySpan<byte> body, long readFrom)
    {
        var cut = Math.Max(body.Length - ResumeOverlapBytes, 0);

        if (cut == 0)
        {
            return readFrom;
        }

        var nl = body[cut..].IndexOf((byte)'\n');
        return nl < 0 ? readFrom : readFrom + cut + nl + 1;
    }

    /// <summary>
    /// The opening CTEs (<c>params</c> through <c>resume</c>, ending in <c>tail</c> and <c>resume</c>). A consumer's query is
    /// <c>WITH</c> + this + its own <c>SELECT ... FROM tail</c>. Ends without a trailing comma or newline,
    /// so the consumer's text follows as <c>)\nSELECT</c> exactly as it did inline.
    /// </summary>
    public const string TailCteSql = @"
WITH params AS (
    SELECT CAST(@log_resume_file AS text) AS file,
           CAST(@log_resume_offset AS bigint) AS off
),
listing AS MATERIALIZED (
    SELECT name, size, modification
    FROM pg_catalog.pg_ls_logdir()
    WHERE pg_catalog.current_setting('logging_collector') = 'on'
      AND name !~* '\.(csv|json)$'
      AND 'stderr' = ANY (pg_catalog.string_to_array(pg_catalog.lower(pg_catalog.replace(pg_catalog.current_setting('log_destination'), ' ', '')), ','))
),
newest AS (
    SELECT name, size, modification
    FROM listing
    ORDER BY modification DESC
    LIMIT 1
),
marked AS (
    SELECT l.name, l.size, l.modification, p.off
    FROM listing AS l
    JOIN params AS p ON l.name = p.file
),
ranges AS (
    SELECT 1 AS part, m.name,
           CASE WHEN m.size - m.off > " + TailBytesLiteral + @" THEN m.size - " + TailBytesLiteral + @" ELSE m.off END AS read_from,
           greatest(m.size - " + TailBytesLiteral + @" - m.off, 0) AS skipped_bytes
    FROM marked AS m
    JOIN newest AS nw ON m.name <> nw.name
    WHERE m.size >= m.off
    UNION ALL
    SELECT 2, nw.name,
           CASE WHEN m.name = nw.name AND nw.size >= m.off THEN greatest(m.off, nw.size - " + TailBytesLiteral + @")
                ELSE greatest(nw.size - " + TailBytesLiteral + @", 0) END,
           CASE WHEN m.name = nw.name AND nw.size >= m.off THEN greatest(nw.size - " + TailBytesLiteral + @" - m.off, 0)
                WHEN m.size >= m.off THEN greatest(nw.size - " + TailBytesLiteral + @", 0)
                ELSE 0 END
    FROM newest AS nw
    LEFT JOIN marked AS m ON true
),
tail AS (
    SELECT n.part, n.name, n.read_from, n.skipped_bytes,
           pg_catalog.pg_read_file(
               pg_catalog.current_setting('log_directory') || '/' || n.name,
               n.read_from,
               " + TailBytesLiteral + @") AS body
    FROM ranges AS n
),
resume AS (
    SELECT t.name,
           CASE WHEN c.cut = 0 OR s.nl = 0 THEN t.read_from ELSE t.read_from + c.cut + s.nl END AS next_offset,
           (SELECT pg_catalog.count(*) FROM listing AS l, marked AS m
             WHERE m.name <> t.name AND l.name <> m.name AND l.name <> t.name
               AND l.modification >= m.modification) AS skipped_files,
           (SELECT pg_catalog.sum(x.skipped_bytes) FROM tail AS x) AS skipped_bytes,
           CASE WHEN p.file IS NULL THEN ''
                WHEN NOT EXISTS (SELECT 1 FROM marked) THEN 'missing'
                WHEN EXISTS (SELECT 1 FROM marked AS m WHERE m.size < m.off) THEN 'recycled'
                ELSE '' END AS fallback
    FROM tail AS t
    CROSS JOIN params AS p
    CROSS JOIN LATERAL (SELECT greatest(pg_catalog.octet_length(t.body) - " + ResumeOverlapBytesLiteral + @", 0) AS cut) AS c
    CROSS JOIN LATERAL (SELECT pg_catalog.position(pg_catalog.substring(
               pg_catalog.convert_to(t.body, pg_catalog.current_setting('server_encoding')), c.cut + 1), '\x0a'::bytea) AS nl) AS s
    WHERE t.part = 2
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
WITH params AS (
    SELECT CAST(@log_resume_file AS text) AS file,
           CAST(@log_resume_offset AS bigint) AS off
),
listing AS MATERIALIZED (
    SELECT name, size, modification
    FROM pg_catalog.pg_ls_logdir()
    WHERE pg_catalog.current_setting('logging_collector') = 'on'
      AND name !~* '\.(csv|json)$'
      AND 'stderr' = ANY (pg_catalog.string_to_array(pg_catalog.lower(pg_catalog.replace(pg_catalog.current_setting('log_destination'), ' ', '')), ','))
),
newest AS (
    SELECT name, size, modification
    FROM listing
    ORDER BY modification DESC
    LIMIT 1
),
marked AS (
    SELECT l.name, l.size, l.modification, p.off
    FROM listing AS l
    JOIN params AS p ON l.name = p.file
),
ranges AS (
    SELECT 1 AS part, m.name,
           CASE WHEN m.size - m.off > " + TailBytesLiteral + @" THEN m.size - " + TailBytesLiteral + @" ELSE m.off END AS read_from,
           greatest(m.size - " + TailBytesLiteral + @" - m.off, 0) AS skipped_bytes
    FROM marked AS m
    JOIN newest AS nw ON m.name <> nw.name
    WHERE m.size >= m.off
    UNION ALL
    SELECT 2, nw.name,
           CASE WHEN m.name = nw.name AND nw.size >= m.off THEN greatest(m.off, nw.size - " + TailBytesLiteral + @")
                ELSE greatest(nw.size - " + TailBytesLiteral + @", 0) END,
           CASE WHEN m.name = nw.name AND nw.size >= m.off THEN greatest(nw.size - " + TailBytesLiteral + @" - m.off, 0)
                WHEN m.size >= m.off THEN greatest(nw.size - " + TailBytesLiteral + @", 0)
                ELSE 0 END
    FROM newest AS nw
    LEFT JOIN marked AS m ON true
),
tail AS (
    SELECT n.part, n.name, n.read_from, n.skipped_bytes,
           pg_catalog.pg_read_binary_file(
               pg_catalog.current_setting('log_directory') || '/' || n.name,
               n.read_from,
               " + TailBytesLiteral + @") AS body
    FROM ranges AS n
),
resume AS (
    SELECT t.name,
           CASE WHEN c.cut = 0 OR s.nl = 0 THEN t.read_from ELSE t.read_from + c.cut + s.nl END AS next_offset,
           (SELECT pg_catalog.count(*) FROM listing AS l, marked AS m
             WHERE m.name <> t.name AND l.name <> m.name AND l.name <> t.name
               AND l.modification >= m.modification) AS skipped_files,
           (SELECT pg_catalog.sum(x.skipped_bytes) FROM tail AS x) AS skipped_bytes,
           CASE WHEN p.file IS NULL THEN ''
                WHEN NOT EXISTS (SELECT 1 FROM marked) THEN 'missing'
                WHEN EXISTS (SELECT 1 FROM marked AS m WHERE m.size < m.off) THEN 'recycled'
                ELSE '' END AS fallback
    FROM tail AS t
    CROSS JOIN params AS p
    CROSS JOIN LATERAL (SELECT greatest(pg_catalog.octet_length(t.body) - " + ResumeOverlapBytesLiteral + @", 0) AS cut) AS c
    CROSS JOIN LATERAL (SELECT pg_catalog.position(pg_catalog.substring(t.body, c.cut + 1), '\x0a'::bytea) AS nl) AS s
    WHERE t.part = 2
)";

    /// <summary>
    /// The csvlog twin of <see cref="TailCteSql"/> (#4053 part a1b): the same file, the same offsets, the
    /// same <c>logging_collector</c> gate — but <c>newest</c> requires <c>'csvlog' = ANY(...)</c> in
    /// <c>log_destination</c> instead of <c>'stderr'</c>, and selects the newest <c>name ~* '\.csv$'</c>
    /// sibling instead of excluding it. Sent by <see cref="PgLogEventsCollector"/> alone, in place of
    /// <see cref="TailCteSql"/>, once <see cref="CollectorContext.PgLogUsesCsvlog"/> says the target's
    /// <c>log_destination</c> includes <c>csvlog</c> — the deadlock and plan-capture collectors still
    /// open only with the stderr twin above, and this constant is theirs to ignore. Kept as an
    /// independent literal rather than built from a shared fragment with <see cref="TailCteSql"/>, for the
    /// same reason <see cref="TailCteBinarySql"/> is: a change here can never alter the byte-for-byte pin
    /// on the stderr twins.
    /// </summary>
    public const string TailCsvCteSql = @"
WITH params AS (
    SELECT CAST(@log_resume_file AS text) AS file,
           CAST(@log_resume_offset AS bigint) AS off
),
listing AS MATERIALIZED (
    SELECT name, size, modification
    FROM pg_catalog.pg_ls_logdir()
    WHERE pg_catalog.current_setting('logging_collector') = 'on'
      AND name ~* '\.csv$'
      AND 'csvlog' = ANY (pg_catalog.string_to_array(pg_catalog.lower(pg_catalog.replace(pg_catalog.current_setting('log_destination'), ' ', '')), ','))
),
newest AS (
    SELECT name, size, modification
    FROM listing
    ORDER BY modification DESC
    LIMIT 1
),
marked AS (
    SELECT l.name, l.size, l.modification, p.off
    FROM listing AS l
    JOIN params AS p ON l.name = p.file
),
ranges AS (
    SELECT 1 AS part, m.name,
           CASE WHEN m.size - m.off > " + TailBytesLiteral + @" THEN m.size - " + TailBytesLiteral + @" ELSE m.off END AS read_from,
           greatest(m.size - " + TailBytesLiteral + @" - m.off, 0) AS skipped_bytes
    FROM marked AS m
    JOIN newest AS nw ON m.name <> nw.name
    WHERE m.size >= m.off
    UNION ALL
    SELECT 2, nw.name,
           CASE WHEN m.name = nw.name AND nw.size >= m.off THEN greatest(m.off, nw.size - " + TailBytesLiteral + @")
                ELSE greatest(nw.size - " + TailBytesLiteral + @", 0) END,
           CASE WHEN m.name = nw.name AND nw.size >= m.off THEN greatest(nw.size - " + TailBytesLiteral + @" - m.off, 0)
                WHEN m.size >= m.off THEN greatest(nw.size - " + TailBytesLiteral + @", 0)
                ELSE 0 END
    FROM newest AS nw
    LEFT JOIN marked AS m ON true
),
tail AS (
    SELECT n.part, n.name, n.read_from, n.skipped_bytes,
           pg_catalog.pg_read_file(
               pg_catalog.current_setting('log_directory') || '/' || n.name,
               n.read_from,
               " + TailBytesLiteral + @") AS body
    FROM ranges AS n
),
resume AS (
    SELECT t.name,
           CASE WHEN c.cut = 0 OR s.nl = 0 THEN t.read_from ELSE t.read_from + c.cut + s.nl END AS next_offset,
           (SELECT pg_catalog.count(*) FROM listing AS l, marked AS m
             WHERE m.name <> t.name AND l.name <> m.name AND l.name <> t.name
               AND l.modification >= m.modification) AS skipped_files,
           (SELECT pg_catalog.sum(x.skipped_bytes) FROM tail AS x) AS skipped_bytes,
           CASE WHEN p.file IS NULL THEN ''
                WHEN NOT EXISTS (SELECT 1 FROM marked) THEN 'missing'
                WHEN EXISTS (SELECT 1 FROM marked AS m WHERE m.size < m.off) THEN 'recycled'
                ELSE '' END AS fallback
    FROM tail AS t
    CROSS JOIN params AS p
    CROSS JOIN LATERAL (SELECT greatest(pg_catalog.octet_length(t.body) - " + ResumeOverlapBytesLiteral + @", 0) AS cut) AS c
    CROSS JOIN LATERAL (SELECT pg_catalog.position(pg_catalog.substring(
               pg_catalog.convert_to(t.body, pg_catalog.current_setting('server_encoding')), c.cut + 1), '\x0a'::bytea) AS nl) AS s
    WHERE t.part = 2
)";

    /// <summary>
    /// The binary-route twin of <see cref="TailCsvCteSql"/> (#4053 part a1b), the same shape
    /// <see cref="TailCteBinarySql"/> is to <see cref="TailCteSql"/>: <c>pg_read_binary_file</c> in place
    /// of <c>pg_read_file</c>, sent only once <see cref="PgReadBinaryFileCapability.IsGrantedAsync"/> finds
    /// the grant.
    /// </summary>
    public const string TailCsvCteBinarySql = @"
WITH params AS (
    SELECT CAST(@log_resume_file AS text) AS file,
           CAST(@log_resume_offset AS bigint) AS off
),
listing AS MATERIALIZED (
    SELECT name, size, modification
    FROM pg_catalog.pg_ls_logdir()
    WHERE pg_catalog.current_setting('logging_collector') = 'on'
      AND name ~* '\.csv$'
      AND 'csvlog' = ANY (pg_catalog.string_to_array(pg_catalog.lower(pg_catalog.replace(pg_catalog.current_setting('log_destination'), ' ', '')), ','))
),
newest AS (
    SELECT name, size, modification
    FROM listing
    ORDER BY modification DESC
    LIMIT 1
),
marked AS (
    SELECT l.name, l.size, l.modification, p.off
    FROM listing AS l
    JOIN params AS p ON l.name = p.file
),
ranges AS (
    SELECT 1 AS part, m.name,
           CASE WHEN m.size - m.off > " + TailBytesLiteral + @" THEN m.size - " + TailBytesLiteral + @" ELSE m.off END AS read_from,
           greatest(m.size - " + TailBytesLiteral + @" - m.off, 0) AS skipped_bytes
    FROM marked AS m
    JOIN newest AS nw ON m.name <> nw.name
    WHERE m.size >= m.off
    UNION ALL
    SELECT 2, nw.name,
           CASE WHEN m.name = nw.name AND nw.size >= m.off THEN greatest(m.off, nw.size - " + TailBytesLiteral + @")
                ELSE greatest(nw.size - " + TailBytesLiteral + @", 0) END,
           CASE WHEN m.name = nw.name AND nw.size >= m.off THEN greatest(nw.size - " + TailBytesLiteral + @" - m.off, 0)
                WHEN m.size >= m.off THEN greatest(nw.size - " + TailBytesLiteral + @", 0)
                ELSE 0 END
    FROM newest AS nw
    LEFT JOIN marked AS m ON true
),
tail AS (
    SELECT n.part, n.name, n.read_from, n.skipped_bytes,
           pg_catalog.pg_read_binary_file(
               pg_catalog.current_setting('log_directory') || '/' || n.name,
               n.read_from,
               " + TailBytesLiteral + @") AS body
    FROM ranges AS n
),
resume AS (
    SELECT t.name,
           CASE WHEN c.cut = 0 OR s.nl = 0 THEN t.read_from ELSE t.read_from + c.cut + s.nl END AS next_offset,
           (SELECT pg_catalog.count(*) FROM listing AS l, marked AS m
             WHERE m.name <> t.name AND l.name <> m.name AND l.name <> t.name
               AND l.modification >= m.modification) AS skipped_files,
           (SELECT pg_catalog.sum(x.skipped_bytes) FROM tail AS x) AS skipped_bytes,
           CASE WHEN p.file IS NULL THEN ''
                WHEN NOT EXISTS (SELECT 1 FROM marked) THEN 'missing'
                WHEN EXISTS (SELECT 1 FROM marked AS m WHERE m.size < m.off) THEN 'recycled'
                ELSE '' END AS fallback
    FROM tail AS t
    CROSS JOIN params AS p
    CROSS JOIN LATERAL (SELECT greatest(pg_catalog.octet_length(t.body) - " + ResumeOverlapBytesLiteral + @", 0) AS cut) AS c
    CROSS JOIN LATERAL (SELECT pg_catalog.position(pg_catalog.substring(t.body, c.cut + 1), '\x0a'::bytea) AS nl) AS s
    WHERE t.part = 2
)";

    /// <summary>
    /// The jsonlog twin of <see cref="TailCteSql"/> (#4053 part a2): the same file, the same offsets, the
    /// same <c>logging_collector</c> gate — but <c>newest</c> requires <c>'jsonlog' = ANY(...)</c> in
    /// <c>log_destination</c> instead of <c>'stderr'</c>, and selects the newest <c>name ~* '\.json$'</c>
    /// sibling instead of excluding it. Sent by <see cref="PgLogEventsCollector"/> alone, in place of
    /// <see cref="TailCteSql"/> and <see cref="TailCsvCteSql"/> both, once
    /// <see cref="CollectorContext.PgLogUsesJsonlog"/> says the target's <c>log_destination</c> includes
    /// <c>jsonlog</c> — jsonlog wins over csvlog when both are configured (<see cref="PgLogFormatCapability"/>'s
    /// own remarks say why). Kept as an independent literal rather than built from a shared fragment with
    /// <see cref="TailCteSql"/> or <see cref="TailCsvCteSql"/>, for the same reason <see cref="TailCteBinarySql"/>
    /// is: a change here can never alter the byte-for-byte pin on the other twins.
    /// </summary>
    public const string TailJsonCteSql = @"
WITH params AS (
    SELECT CAST(@log_resume_file AS text) AS file,
           CAST(@log_resume_offset AS bigint) AS off
),
listing AS MATERIALIZED (
    SELECT name, size, modification
    FROM pg_catalog.pg_ls_logdir()
    WHERE pg_catalog.current_setting('logging_collector') = 'on'
      AND name ~* '\.json$'
      AND 'jsonlog' = ANY (pg_catalog.string_to_array(pg_catalog.lower(pg_catalog.replace(pg_catalog.current_setting('log_destination'), ' ', '')), ','))
),
newest AS (
    SELECT name, size, modification
    FROM listing
    ORDER BY modification DESC
    LIMIT 1
),
marked AS (
    SELECT l.name, l.size, l.modification, p.off
    FROM listing AS l
    JOIN params AS p ON l.name = p.file
),
ranges AS (
    SELECT 1 AS part, m.name,
           CASE WHEN m.size - m.off > " + TailBytesLiteral + @" THEN m.size - " + TailBytesLiteral + @" ELSE m.off END AS read_from,
           greatest(m.size - " + TailBytesLiteral + @" - m.off, 0) AS skipped_bytes
    FROM marked AS m
    JOIN newest AS nw ON m.name <> nw.name
    WHERE m.size >= m.off
    UNION ALL
    SELECT 2, nw.name,
           CASE WHEN m.name = nw.name AND nw.size >= m.off THEN greatest(m.off, nw.size - " + TailBytesLiteral + @")
                ELSE greatest(nw.size - " + TailBytesLiteral + @", 0) END,
           CASE WHEN m.name = nw.name AND nw.size >= m.off THEN greatest(nw.size - " + TailBytesLiteral + @" - m.off, 0)
                WHEN m.size >= m.off THEN greatest(nw.size - " + TailBytesLiteral + @", 0)
                ELSE 0 END
    FROM newest AS nw
    LEFT JOIN marked AS m ON true
),
tail AS (
    SELECT n.part, n.name, n.read_from, n.skipped_bytes,
           pg_catalog.pg_read_file(
               pg_catalog.current_setting('log_directory') || '/' || n.name,
               n.read_from,
               " + TailBytesLiteral + @") AS body
    FROM ranges AS n
),
resume AS (
    SELECT t.name,
           CASE WHEN c.cut = 0 OR s.nl = 0 THEN t.read_from ELSE t.read_from + c.cut + s.nl END AS next_offset,
           (SELECT pg_catalog.count(*) FROM listing AS l, marked AS m
             WHERE m.name <> t.name AND l.name <> m.name AND l.name <> t.name
               AND l.modification >= m.modification) AS skipped_files,
           (SELECT pg_catalog.sum(x.skipped_bytes) FROM tail AS x) AS skipped_bytes,
           CASE WHEN p.file IS NULL THEN ''
                WHEN NOT EXISTS (SELECT 1 FROM marked) THEN 'missing'
                WHEN EXISTS (SELECT 1 FROM marked AS m WHERE m.size < m.off) THEN 'recycled'
                ELSE '' END AS fallback
    FROM tail AS t
    CROSS JOIN params AS p
    CROSS JOIN LATERAL (SELECT greatest(pg_catalog.octet_length(t.body) - " + ResumeOverlapBytesLiteral + @", 0) AS cut) AS c
    CROSS JOIN LATERAL (SELECT pg_catalog.position(pg_catalog.substring(
               pg_catalog.convert_to(t.body, pg_catalog.current_setting('server_encoding')), c.cut + 1), '\x0a'::bytea) AS nl) AS s
    WHERE t.part = 2
)";

    /// <summary>
    /// The binary-route twin of <see cref="TailJsonCteSql"/> (#4053 part a2), the same shape
    /// <see cref="TailCteBinarySql"/> is to <see cref="TailCteSql"/>: <c>pg_read_binary_file</c> in place
    /// of <c>pg_read_file</c>, sent only once <see cref="PgReadBinaryFileCapability.IsGrantedAsync"/> finds
    /// the grant.
    /// </summary>
    public const string TailJsonCteBinarySql = @"
WITH params AS (
    SELECT CAST(@log_resume_file AS text) AS file,
           CAST(@log_resume_offset AS bigint) AS off
),
listing AS MATERIALIZED (
    SELECT name, size, modification
    FROM pg_catalog.pg_ls_logdir()
    WHERE pg_catalog.current_setting('logging_collector') = 'on'
      AND name ~* '\.json$'
      AND 'jsonlog' = ANY (pg_catalog.string_to_array(pg_catalog.lower(pg_catalog.replace(pg_catalog.current_setting('log_destination'), ' ', '')), ','))
),
newest AS (
    SELECT name, size, modification
    FROM listing
    ORDER BY modification DESC
    LIMIT 1
),
marked AS (
    SELECT l.name, l.size, l.modification, p.off
    FROM listing AS l
    JOIN params AS p ON l.name = p.file
),
ranges AS (
    SELECT 1 AS part, m.name,
           CASE WHEN m.size - m.off > " + TailBytesLiteral + @" THEN m.size - " + TailBytesLiteral + @" ELSE m.off END AS read_from,
           greatest(m.size - " + TailBytesLiteral + @" - m.off, 0) AS skipped_bytes
    FROM marked AS m
    JOIN newest AS nw ON m.name <> nw.name
    WHERE m.size >= m.off
    UNION ALL
    SELECT 2, nw.name,
           CASE WHEN m.name = nw.name AND nw.size >= m.off THEN greatest(m.off, nw.size - " + TailBytesLiteral + @")
                ELSE greatest(nw.size - " + TailBytesLiteral + @", 0) END,
           CASE WHEN m.name = nw.name AND nw.size >= m.off THEN greatest(nw.size - " + TailBytesLiteral + @" - m.off, 0)
                WHEN m.size >= m.off THEN greatest(nw.size - " + TailBytesLiteral + @", 0)
                ELSE 0 END
    FROM newest AS nw
    LEFT JOIN marked AS m ON true
),
tail AS (
    SELECT n.part, n.name, n.read_from, n.skipped_bytes,
           pg_catalog.pg_read_binary_file(
               pg_catalog.current_setting('log_directory') || '/' || n.name,
               n.read_from,
               " + TailBytesLiteral + @") AS body
    FROM ranges AS n
),
resume AS (
    SELECT t.name,
           CASE WHEN c.cut = 0 OR s.nl = 0 THEN t.read_from ELSE t.read_from + c.cut + s.nl END AS next_offset,
           (SELECT pg_catalog.count(*) FROM listing AS l, marked AS m
             WHERE m.name <> t.name AND l.name <> m.name AND l.name <> t.name
               AND l.modification >= m.modification) AS skipped_files,
           (SELECT pg_catalog.sum(x.skipped_bytes) FROM tail AS x) AS skipped_bytes,
           CASE WHEN p.file IS NULL THEN ''
                WHEN NOT EXISTS (SELECT 1 FROM marked) THEN 'missing'
                WHEN EXISTS (SELECT 1 FROM marked AS m WHERE m.size < m.off) THEN 'recycled'
                ELSE '' END AS fallback
    FROM tail AS t
    CROSS JOIN params AS p
    CROSS JOIN LATERAL (SELECT greatest(pg_catalog.octet_length(t.body) - " + ResumeOverlapBytesLiteral + @", 0) AS cut) AS c
    CROSS JOIN LATERAL (SELECT pg_catalog.position(pg_catalog.substring(t.body, c.cut + 1), '\x0a'::bytea) AS nl) AS s
    WHERE t.part = 2
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
    /// The target's own <c>log_line_prefix</c>, selected beside the tail body the same way
    /// <see cref="LogTimezoneSql"/> already is (#4501): a consumer that reads each line's label passes it to
    /// <see cref="PgLogEntryAssembler.ForgeryCheckFor"/> so the forgery rule can use the prefix's own
    /// separator instead of the fallback. <c>true</c> as the second argument to <c>current_setting</c> is
    /// belt-and-suspenders — <c>log_line_prefix</c> is a core GUC that always exists — but costs nothing and
    /// matches the readiness collector's own read of the same setting.
    /// </summary>
    public const string LogLinePrefixSql = "pg_catalog.current_setting('log_line_prefix', true)";

    /// <summary>
    /// The current row's <see cref="LogLinePrefixSql"/> column, at <paramref name="ordinal"/>, or null when the
    /// row is a marker row (NULL there), the setting itself is unset, or the reader carries no such column at
    /// all — the last case is every fixture and test double built before #4501, which this keeps working:
    /// <see cref="PgLogEntryAssembler.ForgeryCheckFor"/> treats a null prefix as "not collected" and falls back
    /// to the rule with no separator check.
    /// </summary>
    public static string? LogLinePrefix(DbDataReader reader, int ordinal)
    {
        ArgumentNullException.ThrowIfNull(reader);
        return reader.FieldCount > ordinal && !reader.IsDBNull(ordinal)
            ? reader.GetString(ordinal)
            : null;
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
