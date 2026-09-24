/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// PostgreSQL deadlock reports, read out of the server log (#2661).
///
/// <para><b>We had the count and nothing else.</b> <c>pg_stat_database.deadlocks</c> says a number went up;
/// this says which sessions, holding what, running what SQL. Measured against a real 17.11 target, the log
/// report carries the full wait graph, every participant's complete statement text, and the relation and
/// tuple the conflict landed on.</para>
///
/// <para><b>Nothing has to be ENABLED on the target</b>, which is what makes this different from plan
/// capture. <c>auto_explain</c> needs a preload and a restart — on a managed fleet that is a
/// parameter-group change and a reboot — while a deadlock report is written at default settings, and
/// <c>log_lock_waits</c> governs ordinary lock waits rather than this.</para>
///
/// <para><b>Two preconditions, though, not one.</b> The log has to be READABLE, which
/// <see cref="PgPlanCaptureCollector"/> already established and <c>pg_plan_capture_readiness</c> already
/// reports on. It also has to be verbose enough to carry the report: <c>log_error_verbosity = terse</c>
/// drops the DETAIL field, which is the entire graph, leaving the <c>ERROR</c> line and nothing for the
/// parser to match. A perfectly readable log at terse verbosity yields zero rows and reads as a server
/// that does not deadlock (#3030).</para>
///
/// <para>Reads the same bounded tail of the same file as plan capture, by the same two routes: this
/// definition where there is a filesystem, and the RDS log API at a managed target, chosen at dispatch.
/// Every row carries a hash of its graph and the store dedupes on it, because both routes hand the same
/// report over more than once — but NOT by the same mechanism, and the difference decides what a
/// truncated report costs. This definition's window OVERLAPS between cycles on purpose, so a report cut
/// in half at the edge of one read is whole in the next. The RDS route is consume-once: its resume marker
/// advances past everything a successful cycle CONSUMED, not merely what that cycle stored, so a report
/// cut at one of its chunk boundaries is not completed while the marker lives, and its own repeats come
/// from a restart discarding that in-process marker or from a write that did not land (#3008, #3009).</para>
/// </summary>
public sealed class PgDeadlocksCollector : PostgresCollectorDefinitionBase<PgDeadlocksCollector.Row>
{
    public static PgDeadlocksCollector Instance { get; } = new();

    private PgDeadlocksCollector()
    {
    }

    public readonly record struct Row(
        System.DateTime OccurredAtUtc,
        int VictimPid,
        int ParticipantCount,
        string DeadlockHash,
        string? LockModes,
        string? Resources,
        string? VictimStatement,
        string GraphText);

    /* Same tail size as plan capture, for the same reason and with the same trade: large enough that a busy
       server's reports survive between cycles, small enough not to move the whole file every time. ONE
       spelling, PgServerLogTail's, shared by every pg_read_file reader (#3601). */
    private const string TailBytesLiteral = PgServerLogTail.TailBytesLiteral;

    /* The tailer — pg_ls_logdir() for the CURRENT file, current_setting('log_directory') for where it is
       (#3410), the logging_collector gate — is PgServerLogTail.TailCteSql, shared byte-for-byte with
       PgPlanCaptureCollector and PgLogEventsCollector (#3601); its header carries the full argument, and
       StoreLogSweep.ReadFileSql builds the store's own log path the same way. What is THIS collector's is
       everything after the CTEs:

       The extraction mirrors PgPlanCaptureCollector's: regexp_matches over the tail rather than a
       line-by-line walk, because the block is recognisable as a unit. Two things in this pattern are load
       bearing and were measured rather than assumed:

         [^\n]* between the pid and ERROR: — %Q writes the query id with NO separator before the severity
         (`[1549] 322048460535975151ERROR:  deadlock detected`), so requiring whitespace there matches
         nothing, and matches nothing in the way that looks like "this server has no deadlocks".

         (?:\t[^\n]*\n)* for the DETAIL body — the block runs to the next line carrying a log prefix, and a
         participant's statement is arbitrary user SQL that can contain newlines, each arriving
         tab-indented. A blank-line or line-count rule truncates multi-line SQL silently.

         ([^ \n]+) for the prefix's ZONE, returned as its own column (#2993). It was matched and discarded
         here, and the parser could then only assume the stamp beside it was UTC. [^ \n]+ rather than \w+
         because a zone with no abbreviation renders as a numeric offset (+07) that \w+ cannot match, so
         the block matched nothing and the server reported no deadlocks.

         The line after the DETAIL block, when it carries a prefix and does not open another report
         (#4005): DeadLockReport always writes a HINT there, and that line is the proof the DETAIL arrived
         whole, which the query normalization needs to trust a query after one that does not read to its end.

       The 'n' flag makes ^ match at line starts. The pattern finds CANDIDATES and returns each one's text
       whole; what a candidate is, is decided in C# by PgDeadlockLogParser.FromReport, through the log reader
       every family shares (PgLogEntryAssembler). So the RDS transport — which receives log TEXT and runs no
       SQL — shares it, both routes get the same zone refusal from the same code, and a statement's literal
       holding `ERROR:  deadlock detected` is never read as a report (#4005). The pattern is narrowed to the
       same rule as PgDeadlockLogParser's C# twin (#4014): the gaps before ERROR: and DETAIL: may not cross a
       field label's ":  ", so the ERROR: matched is the line's own. Measured on PostgreSQL 18.6: the old
       pattern matched a STATEMENT line echoing a report (the assembler then refused it); this one does not.

       The prefix clause is PgDeadlockLogParser's own (#4041), with '[^[\n]' as ARE's spelling of its '[^\[\n]':
       the fraction optional, fields allowed between the zone and the pid, and the managed family beside the
       space one. The pattern
       carried only '%m [%p] ' with its fraction required, so a self-hosted target logging under '%t' (pgBadger's
       '%t [%p]: user=%u,db=%d,...'), under '%m %u@%d [%p] ' or under a colon-delimited prefix offered no
       candidate and read as a server with no deadlocks, while the assembler behind it would have read every one.
       Its groups are non-capturing, so m[1] is still the whole candidate. PgDeadlockCandidatePatternLiveTests
       runs it through PostgreSQL's own engine.

       The listing is GATED on logging_collector, and the gate carries a marker row out the other side
       (#3410). With the setting off the server writes to stderr and there may be no log directory at all,
       so pg_ls_logdir() raises 58P01 for a directory that legitimately does not exist — an error every
       cycle forever, on a server configured to log somewhere else on purpose. The predicate is
       pseudoconstant (no column references, current_setting is stable), so the planner enforces it as a
       one-time filter ABOVE the function scan and pg_ls_logdir() is never called when it is false; a
       leftover directory full of files from before the setting was switched off is deliberately not read
       either, because everything in it is stale. The marker row is what stops off from reading as a quiet
       server: ReadAsync recognises it and throws PgLoggingCollectorOffException, which the runner records
       as a named non-fatal skip — the same not-collected-with-reason answer the store's own log read gives
       for an empty directory, rather than a failure or a silent zero. The marker is spelled here in this
       query's own one column, because a UNION ALL arm has to match the column list it joins.

       A second marker arm (#3997) is the narrower gap: logging_collector on, but every file the shared tail
       saw was a csvlog/jsonlog sibling (log_destination carries no stderr), so newest came back empty for a
       different reason than the setting being off. ReadAsync throws PgNoStderrLogFileException for that one;
       the two predicates cannot both hold, since one needs the setting off and the other needs it on.

       The second column is the target's log_timezone, read in this statement with the tail (#4046). Under a
       setting that renders UTC, a candidate in another zone is not the server's own (a client plants one
       through %u or %d with a failed login), so ReadAsync skips and counts it instead of refusing the read.
       The marker arms carry NULL there. */
    private const string QueryText = PgServerLogTail.TailCteSql + @"
SELECT
    m[1]    AS report_text,
    " + PgServerLogTail.LogTimezoneSql + @" AS log_timezone
FROM tail,
     regexp_matches(
         tail.body,
         '^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d+)? (?:[^ \n]+ (?:(?!:  )[^[\n])*\[\d+\]|[^ :\n]+:[^[\n]*\[\d+\])(?:(?!:  )[^\n])*" + DeadlockMarkerLiteral + @"\s*\n(?:(?!:  )[^\n])*DETAIL:  (?:[^\n]*\n)(?:\t[^\n]*\n)*(?:(?![^\n]*" + DeadlockMarkerLiteral + @")\d{4}-\d\d-\d\d [^\n]*\n)?)',
         'gn') AS m
UNION ALL
SELECT '" + PgLoggingCollectorOffException.Marker + @"', NULL
WHERE " + PgServerLogTail.LoggingCollectorOffMarkerSql + @"
UNION ALL
SELECT '" + PgNoStderrLogFileException.Marker + @"', NULL
WHERE " + PgServerLogTail.NoStderrLogFileMarkerSql + @"
LIMIT 500";

    /* The marker text the regexp anchors on — 'ERROR:  deadlock detected' — spliced as its own literal
       so the amplification guard below (item 1, #4058) and the pattern's own literal stay ONE spelling. */
    private const string DeadlockMarkerLiteral = "ERROR:  deadlock detected";

    /* The binary-route twin (#4046 part 1c), sent instead of QueryText once PgReadBinaryFileCapability
       finds the grant: the same tailer, in its bytea form, with tail.body wrapped in
       encode(..., 'escape') before the SAME pattern runs over it — verified on the rig (PostgreSQL 18.6)
       that escape() leaves every byte the pattern matches on literally (newline, tab, the digits and
       punctuation in a timestamp) and only backslash-doubles and NUL/high-byte-octal-escapes, so the
       pattern needs no change; see PgBinaryTailText.UnescapeAndDecode for the reversal ReadAsync applies
       to m[1] afterward. The marker arms are untouched: m[1] is text on both routes, so there is no
       bytea/text mismatch here the way there is in PgLogEventsCollector's whole-body column.

       #4058 item 1: encode(tail.body, 'escape') turns every byte at or above 0x80 into a four-character
       octal escape, so a tail a failed login has stuffed with high bytes (the startup packet's role or
       database name lands in the FATAL line unescaped) can grow the 4 MB tail to roughly 16 MB of text
       before the regex engine ever sees it — about 64 MB of internal engine state, on EVERY cycle, whether
       or not the tail holds a deadlock report at all. The WHERE clause below is evaluated on tail.body
      (bytea) BEFORE encode()/regexp_matches() run: its qual references only tail.body, and
       pg_read_binary_file is volatile, so the CTE stays materialized and this filter runs at the CTE scan,
       below the lateral regexp_matches call — not "the SELECT list", and not because encode()/regexp_matches
       are "volatile-cost"; encode() and regexp_matches() are in fact IMMUTABLE, and the conclusion holds for
       the reason just given, not that one. A refactor that referenced m in this WHERE would silently break
       the ordering the guard depends on (proved on the rig: a PL/pgSQL function standing in for encode()
       that RAISEs unconditionally is never invoked when the marker is absent, and fires normally when it
       is present). position() on bytea is a byte-for-byte substring search — no encoding, no regex — so it
       costs a single pass over the SAME 4 MB the tail already is, not a second 4 MB. It cannot skip a real
       report: DeadLockReport's ERROR line always contains this exact literal verbatim before %Q or anything
       else is glued to it, so the marker is a NECESSARY substring of every candidate the pattern beneath it
       could ever match, and the guard changes nothing about which rows the query returns or their order
       when it IS present.

       This is an OPTIMIZATION, not a bound: position() needs only the substring somewhere in the 4 MB, not
       at a line start, so an attacker can plant the marker the same way the deadlock report's own bytes can
       be planted — a failed login whose role name IS "ERROR:  deadlock detected" defeats this guard for as
       long as that line stays in the tail. See the type header's #4058 M1 remarks (still open on the
       issue) for the remaining exposure and why it is bounded rather than closed. */
    private const string BinaryQueryText = PgServerLogTail.TailCteBinarySql + @"
SELECT
    m[1]    AS report_text,
    " + PgServerLogTail.LogTimezoneSql + @" AS log_timezone
FROM tail,
     regexp_matches(
         pg_catalog.encode(tail.body, 'escape'),
         '^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d+)? (?:[^ \n]+ (?:(?!:  )[^[\n])*\[\d+\]|[^ :\n]+:[^[\n]*\[\d+\])(?:(?!:  )[^\n])*" + DeadlockMarkerLiteral + @"\s*\n(?:(?!:  )[^\n])*DETAIL:  (?:[^\n]*\n)(?:\t[^\n]*\n)*(?:(?![^\n]*" + DeadlockMarkerLiteral + @")\d{4}-\d\d-\d\d [^\n]*\n)?)',
         'gn') AS m
WHERE pg_catalog.position(tail.body, '" + DeadlockMarkerLiteral + @"'::bytea) > 0
UNION ALL
SELECT '" + PgLoggingCollectorOffException.Marker + @"', NULL
WHERE " + PgServerLogTail.LoggingCollectorOffMarkerSql + @"
UNION ALL
SELECT '" + PgNoStderrLogFileException.Marker + @"', NULL
WHERE " + PgServerLogTail.NoStderrLogFileMarkerSql + @"
LIMIT 500";

    /* The csvlog pair (#4053 part b1), sent instead of the two above once context.PgLogUsesCsvlog says the
       target's log_destination includes csvlog — the same flag PgLogEventsCollector reads, extended to this
       collector by DarlingCollectorRunner.ResolvePgReadBinaryFileGrantAsync. Unlike QueryText/BinaryQueryText,
       this pair runs NO server-side regexp_matches: PgServerLogCsvParser resyncs and shape-checks the whole
       tail into typed PgLogEntry records in C#, and ReadAsync picks the deadlock ones out by calling
       PgDeadlockLogParser.FromEntry on each. The marker arms are PgLogEventsCollector's own shape: the
       collector-off arm is shared, and the no-file-yet arm is PgNoCsvlogFileException.Marker — never
       PgNoStderrLogFileException's — so the fault message names csvlog rather than stderr. */
    private const string CsvQueryText = PgServerLogTail.TailCsvCteSql + @"
SELECT tail.body AS log_body,
       " + PgServerLogTail.LogTimezoneSql + @" AS log_timezone
FROM tail
UNION ALL
SELECT '" + PgLoggingCollectorOffException.Marker + @"', NULL
WHERE " + PgServerLogTail.LoggingCollectorOffMarkerSql + @"
UNION ALL
SELECT '" + PgNoCsvlogFileException.Marker + @"', NULL
WHERE " + PgServerLogTail.NoStderrLogFileMarkerSql;

    /* The binary-route twin of CsvQueryText (#4053 part b1), the same shape BinaryQueryText is to QueryText:
       pg_read_binary_file in place of pg_read_file, and the marker arms cast through convert_to for the
       reason BinaryQueryText's own remarks give — a bytea/text UNION mismatch would ask PostgreSQL to parse
       the marker text as bytea input rather than hand back its own UTF-8 bytes. */
    private const string CsvBinaryQueryText = PgServerLogTail.TailCsvCteBinarySql + @"
SELECT tail.body AS log_body,
       " + PgServerLogTail.LogTimezoneSql + @" AS log_timezone
FROM tail
UNION ALL
SELECT pg_catalog.convert_to('" + PgLoggingCollectorOffException.Marker + @"', 'UTF8'), NULL
WHERE " + PgServerLogTail.LoggingCollectorOffMarkerSql + @"
UNION ALL
SELECT pg_catalog.convert_to('" + PgNoCsvlogFileException.Marker + @"', 'UTF8'), NULL
WHERE " + PgServerLogTail.NoStderrLogFileMarkerSql;

    /// <summary>
    /// The count a consumer records on its collection-log row when the csvlog parser discarded a record
    /// (#4053 part b1), the same measurement <see cref="PgLogEventsCollector.CsvRecordsDiscardedMeasurement"/>
    /// spells — ONE label, so an operator reads the same name whichever collector's row carries it.
    /// </summary>
    /* A literal, not a reference to PgLogEventsCollector.CsvRecordsDiscardedMeasurement: the measurement-label gate
       (CollectorMeasurementSeamTests) reads labels only as string-literal consts in the collector's own file. It
       must stay equal to PgLogEventsCollector's label. */
    private const string CsvRecordsDiscardedMeasurement = "csv_records_discarded";

    /// <summary>
    /// #4058 item 1: deadlock-shaped entries the csvlog route skipped because they were not actually written
    /// by PostgreSQL's own <c>DeadLockReport</c> — a database client's <c>RAISE</c> built to imitate a
    /// deadlock, caught by <see cref="PgDeadlockLogParser.IsRaiseShaped"/> before <see cref="PgDeadlockLogParser.FromEntry"/>
    /// ever runs. Counted rather than silently dropped, following <see cref="PgPlanCaptureCollector.ForgedCaptureMeasurement"/>'s
    /// pattern for the plan-capture twin of this same issue.
    /// </summary>
    public const string RaiseShapedDeadlocksSkippedMeasurement = "raise_shaped_deadlocks_skipped";

    /// <summary>
    /// The sentence the Darling runner puts beside <see cref="RaiseShapedDeadlocksSkippedMeasurement"/> on the
    /// row, following <see cref="PgPlanCaptureCollector.ForgedCaptureNote"/>'s exact pattern.
    /// </summary>
    public const string RaiseShapedDeadlocksSkippedNote =
        "Skipped deadlock-shaped log entries that were not written by PostgreSQL's own deadlock detector: a "
        + "client can RAISE an ERROR whose message and DETAIL imitate a deadlock report (#4058). The read "
        + "went on without them";

    public override string Name => "pg_deadlocks";

    public override string TargetTable => "pg_deadlocks";

    /// <summary>
    /// Every PostgreSQL target, for the reason <see cref="PgPlanCaptureCollector"/> gives: gating on the
    /// engine would report deadlock capture as a PERMANENT gap on Aurora, which is false — those targets
    /// reach the same table through the RDS log API. The route is chosen at dispatch, so this definition
    /// never executes against a managed target.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    /// <summary>Server-wide: one log holds every database's deadlocks.</summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override CollectorQuery BuildQuery(CollectorContext context) =>
        new(context.PgLogUsesCsvlog
            ? (context.PgReadBinaryFileGranted ? CsvBinaryQueryText : CsvQueryText)
            : (context.PgReadBinaryFileGranted ? BinaryQueryText : QueryText));

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("occurred_at", CollectorColumnType.Timestamp),
        /* The session PostgreSQL chose to cancel. It is the one whose transaction was rolled back, and the
           one whose application saw an error - which is usually the only end anybody notices. */
        new CollectorColumn("victim_pid", CollectorColumnType.Integer),
        new CollectorColumn("participant_count", CollectorColumnType.Integer),
        /* Identity across repeated reads. This route re-reads the tail every cycle, so without this the
           same deadlock is stored once per cycle for as long as it stays inside the window; the RDS route
           repeats for its own reasons rather than by overlapping (see the class remarks). */
        new CollectorColumn("deadlock_hash", CollectorColumnType.Varchar),
        new CollectorColumn("lock_modes", CollectorColumnType.Varchar),
        new CollectorColumn("resources", CollectorColumnType.Varchar),
        new CollectorColumn("victim_statement", CollectorColumnType.Varchar),
        /* The DETAIL block, its queries normalized (#4005). The parsed columns are an interpretation; this is
           the evidence, and a report shape the parser does not recognise yet is still readable by a person. */
        new CollectorColumn("graph_text", CollectorColumnType.Varchar),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            if (context.PgLogUsesCsvlog)
            {
                ReadCsvRow(reader, context, rows);
                continue;
            }

            var firstColumn = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);

            /* The marker row the query returns instead of listing the log directory when
               logging_collector is off (#3410). Thrown rather than skipped for the reason the zone refusal
               below is: skipped, a server that logs to stderr reads as a server with no deadlocks, and the
               runner never learns the one fact that explains every empty cycle. It cannot collide with a
               real row — the first column is a regexp capture of a literal-digit timestamp. */
            if (string.Equals(firstColumn, PgLoggingCollectorOffException.Marker, System.StringComparison.Ordinal))
            {
                throw new PgLoggingCollectorOffException();
            }

            /* The second marker row (#3997): logging_collector is on but the tail excluded every file as a
               csvlog/jsonlog sibling, so there is no stderr-format file this cycle. Same reasoning as above;
               a regexp capture of a literal-digit timestamp cannot equal this marker text by accident. */
            if (string.Equals(firstColumn, PgNoStderrLogFileException.Marker, System.StringComparison.Ordinal))
            {
                throw new PgNoStderrLogFileException();
            }

            /* #4046 part 1c: on the binary route the candidate came back through encode(..., 'escape'), so
               it is reversed here before the parser sees it — after the marker checks above, since a marker
               is never escaped text and must be compared to the literal constant first. */
            if (context.PgReadBinaryFileGranted)
            {
                firstColumn = PgBinaryTailText.UnescapeAndDecode(firstColumn, context.PgLogEncoding ?? System.Text.Encoding.UTF8);
            }

            /* One column, the candidate's text (#4005). Reaching the parser is what makes the stamp's meaning
               checked rather than assumed: a non-zero-offset zone throws out of here, and the worker records
               the refusal against log_timezone instead of storing a shifted occurred_at. The throw abandons
               rows already read in this batch, which is the trade PgDeadlockLogParser.Extract's remarks argue
               for: a partial history from a target declared unreadable is worse for the reader than a
               refusal that says one thing. Unless the target's own log_timezone renders UTC: then a candidate
               in another zone is not the server's, and it is skipped and counted instead (#4046). */
            var parsed = PgDeadlockLogParser.FromReport(
                firstColumn, PgServerLogTail.LogTimezoneIsUtc(reader, 1), out var foreignZoneLines);
            PgServerLogTail.MeasureForeignZoneLines(context, foreignZoneLines);

            /* A block that will not parse is skipped rather than reported. The window is bounded, so a
               report cut in half at its edge is ordinary and is read whole on the next overlapping pass. */
            if (parsed is null)
            {
                continue;
            }

            rows.Add(ToRow(parsed.Value));
        }

        return rows;
    }

    /// <summary>
    /// The csvlog route's own row read (#4053 part b1): decode the whole tail body, check its two marker
    /// values, resync it into typed entries with <see cref="PgServerLogCsvParser"/>, apply the same
    /// foreign-zone rule the stderr path applies through <see cref="PgDeadlockLogParser.FromReport(string?, bool, out int)"/>'s
    /// assembler, then keep only the entries that are deadlock reports (<see cref="PgDeadlockLogParser.FromEntry"/>
    /// returns non-null for those and null for every other row the tail carries — most of it). Column 0 is the
    /// whole tail body here, never a regexp candidate, so it is decoded exactly as <c>PgLogEventsCollector</c>'s
    /// own csvlog branch decodes it.
    /// </summary>
    private static void ReadCsvRow(DbDataReader reader, CollectorContext context, List<Row> rows)
    {
        var body = reader.IsDBNull(0)
            ? null
            : context.PgReadBinaryFileGranted
                ? PgBinaryTailText.DecodeWhole(reader.GetFieldValue<byte[]>(0), context.PgLogEncoding ?? System.Text.Encoding.UTF8)
                : reader.GetString(0);

        if (string.Equals(body, PgLoggingCollectorOffException.Marker, System.StringComparison.Ordinal))
        {
            throw new PgLoggingCollectorOffException();
        }

        /* The csvlog route's own "no file yet" marker (#4053 part b1) — distinct from the stderr route's
           PgNoStderrLogFileException, so the fault message names csvlog, never stderr. */
        if (string.Equals(body, PgNoCsvlogFileException.Marker, System.StringComparison.Ordinal))
        {
            throw new PgNoCsvlogFileException();
        }

        var entries = PgServerLogCsvParser.Parse(body ?? string.Empty, out var recordsDiscarded);

        if (recordsDiscarded > 0)
        {
            context.Measure(CsvRecordsDiscardedMeasurement, recordsDiscarded);
        }

        var logTimezoneIsUtc = PgServerLogTail.LogTimezoneIsUtc(reader, 1);
        var kept = PgLogEventsCollector.FilterForeignZoneEntries(entries, logTimezoneIsUtc, out var foreignZoneLines);
        PgServerLogTail.MeasureForeignZoneLines(context, foreignZoneLines);

        var raiseShapedSkipped = 0;

        foreach (var entry in kept)
        {
            /* #4058 item 1: IsRaiseShaped is checked, and counted, only for an entry that already matches
               the deadlock candidate shape FromEntry itself tests (severity ERROR, the deadlock marker
               message, a non-empty DETAIL) — the same restriction FromEntry applies internally. Checking
               every entry the tail carries would count an unrelated RAISE-based ERROR row toward a label
               that is supposed to mean "looked like a deadlock and was not one". */
            if (entry.Severity == "ERROR"
                && entry.Message.TrimEnd() == "deadlock detected"
                && !string.IsNullOrWhiteSpace(entry.Detail)
                && PgDeadlockLogParser.IsRaiseShaped(entry))
            {
                raiseShapedSkipped++;
                continue;
            }

            var parsed = PgDeadlockLogParser.FromEntry(entry);

            if (parsed is not null)
            {
                rows.Add(ToRow(parsed.Value));
            }
        }

        if (raiseShapedSkipped > 0)
        {
            context.Measure(RaiseShapedDeadlocksSkippedMeasurement, raiseShapedSkipped);
        }
    }

    private static Row ToRow(PgDeadlockLogParser.ParsedDeadlock parsed) => new(
        OccurredAtUtc: parsed.OccurredAtUtc,
        VictimPid: parsed.VictimPid,
        ParticipantCount: parsed.ParticipantCount,
        DeadlockHash: parsed.DeadlockHash,
        LockModes: parsed.LockModes,
        Resources: parsed.Resources,
        VictimStatement: parsed.VictimStatement,
        GraphText: parsed.GraphText);

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No deltas: a deadlock is an EVENT, not a counter. The rate is a count of these rows over a window,
           which the read does, and pg_stat_database's cumulative deadlock counter is the honest denominator
           for whether this log window saw all of them. */
        writer
            /* Naive UTC, per the store contract: the parser returns a Kind=Utc DateTime and Npgsql refuses
               one against a `timestamp` column. */
            .Value(System.DateTime.SpecifyKind(row.OccurredAtUtc, System.DateTimeKind.Unspecified))
            .Value(row.VictimPid)
            .Value(row.ParticipantCount)
            .Value(row.DeadlockHash)
            .Value(row.LockModes)
            .Value(row.Resources)
            .Value(row.VictimStatement)
            .Value(row.GraphText);
    }
}
