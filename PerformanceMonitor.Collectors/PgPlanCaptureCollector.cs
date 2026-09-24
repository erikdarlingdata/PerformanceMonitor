/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Execution plans captured by <c>auto_explain</c>, read out of the server log (#2566, part of #2538).
///
/// <para><b>Why the log at all.</b> <c>auto_explain</c> has no view, no function and no table — it writes to
/// the server log and nowhere else. #2565 settled that it is nonetheless the right mechanism: on-demand
/// <c>EXPLAIN ANALYZE</c> was rejected because it EXECUTES the statement (demonstrated there against a
/// DELETE), and plain <c>EXPLAIN</c> returns estimates with no actual rows, which is the information a plan
/// is opened for.</para>
///
/// <para><b>This is a SELF-HOSTED capability and the collector says so rather than failing vaguely.</b>
/// Reading the log needs <c>pg_read_server_files</c> AND an explicit
/// <c>GRANT EXECUTE ON FUNCTION pg_read_file</c> — measured: the role alone is NOT enough, because
/// <c>pg_read_file</c>'s ACL is <c>postgres=X/postgres</c> and the role does not carry EXECUTE. On Aurora and
/// RDS there is no filesystem and the role is not grantable at all; logs come from the RDS API, which is a
/// different integration entirely (#2538). Where the grant is absent this degrades to a named non-fatal
/// skip, which is the honest outcome — not a hidden failure.</para>
///
/// <para><b>No query text, and no literals anywhere. This is the part to not undo.</b> <c>auto_explain</c>
/// emits <c>Query Text</c> verbatim — <c>WHERE datname = 'postgres'</c> — and
/// <c>auto_explain.log_parameter_max_length = 0</c> does NOT suppress it (that setting only covers bind
/// parameters; measured on #2565). Literals also appear INSIDE the plan tree, in <c>Filter</c> and its
/// relatives: <c>(datname = 'postgres'::name)</c>. So <c>Query Text</c> is dropped entirely — the statement
/// identity is <c>query_id</c>, and its normalised text already lives in <c>pg_statement_stats</c> — and
/// every remaining string is redacted before it reaches the store. That matches what this product does
/// everywhere else: <c>pg_session_states</c> carries no query text and <c>pg_column_stats</c> drops
/// <c>most_common_vals</c>, both for this reason.</para>
///
/// <para><b>Redaction is deliberately asymmetric.</b> Quoted literals are stripped from EVERY string, which
/// is safe because relation and alias names are not quoted. Bare numbers are stripped only inside known
/// condition fields, because a blanket numeric strip would rewrite a table genuinely named
/// <c>transactionitems1</c> into <c>transactionitems?</c> — mangling identity to hide a value that was
/// never there.</para>
/// </summary>
public sealed class PgPlanCaptureCollector : PostgresCollectorDefinitionBase<PgPlanCaptureCollector.Row>
{
    public static readonly PgPlanCaptureCollector Instance = new();

    private PgPlanCaptureCollector()
    {
    }

    /// <param name="QueryId">From <c>%Q</c> in <c>log_line_prefix</c>, which is the ONLY place auto_explain
    /// exposes it — the plan JSON contains no identifier of its own, even with <c>compute_query_id</c> on.
    /// Joins <c>pg_statement_stats</c>. Zero means the prefix was not configured, and the plan is an orphan
    /// (<c>pg_plan_capture_readiness</c> reports this as the <c>plan_attribution</c> facet).</param>
    /// <param name="PlanHash">Of the REDACTED plan, so the same shape recurs to the same hash regardless of
    /// the values it ran with — which is what makes dedup work at all.</param>
    /// <param name="PlanJson">Redacted. See the type header.</param>
    public readonly record struct Row(
        long QueryId,
        string PlanHash,
        double DurationMs,
        int NodeCount,
        string? TopNodeType,
        string PlanJson);

    /// <summary>
    /// How much of the log tail to read per cycle — <see cref="PgServerLogTail.TailBytes"/>, shared with
    /// every other <c>pg_read_file</c> reader (#3601). Bounded because the file can reach hundreds of
    /// megabytes — #2565 measured 772 MB in twenty seconds at capture-everything — and reading it whole
    /// would turn a monitoring collector into the server's biggest reader.
    ///
    /// <para><b>It is a window, not a resume marker, and that is what decides coverage.</b> The read is
    /// always the last <c>TailBytes</c> of the current file, so consecutive cycles see overlapping text
    /// only while the log grows by less than this between them. At the <c>pg_plan_capture</c> default of
    /// sixty minutes that threshold is about 1.2 KB/s. Above it the two windows do not meet and the span
    /// between them is read by no cycle: those plans are lost, not deferred, and the result reads
    /// downstream as a target with nothing slow on it. The measurement quoted above is exactly a rate that
    /// clears the threshold by four orders of magnitude, so this is the ordinary case on a target with
    /// <c>auto_explain.log_min_duration</c> set low rather than a pathological one.</para>
    ///
    /// <para>Raising this number moves the threshold without establishing one, which is why it has not
    /// been raised: at 772 MB in twenty seconds no fixed tail covers an hour. The gap is that nothing
    /// reports the shortfall — the query already selects the file's <c>size</c>, so comparing it against
    /// the previous cycle's would turn a silent skip into a number. An operator on a busy self-hosted
    /// target lowers the interval for that server in the meantime; the managed route reaches this table
    /// through the RDS log API instead, which keeps a resume marker and has no equivalent exposure.</para>
    /// </summary>
    private const int TailBytes = PgServerLogTail.TailBytes;

    /* Spliced into the query text as a literal. A const string keeps QueryText a compile-time constant,
       which every other collector here relies on, and keeps the number in ONE place — PgServerLogTail's. */
    private const string TailBytesLiteral = PgServerLogTail.TailBytesLiteral;

    /* The tailer — which file, how much of it, gated on what — is PgServerLogTail.TailCteSql, shared
       byte-for-byte with PgDeadlocksCollector and PgLogEventsCollector (#3601); its header carries the
       full argument for pg_ls_logdir(), current_setting('log_directory') (#3410) and the logging_collector
       gate. What is THIS collector's is everything after the CTEs:

       Plans are extracted with regexp_matches rather than parsed line by line because auto_explain writes
       the JSON tab-indented under its LOG line, so the block is recognisable as a unit. The tabs are
       stripped to make it valid JSON.

       ANCHORED to '^' with the 'n' (newline-sensitive) flag, and the timestamp is REQUIRED (#4008). The
       pre-fix pattern matched '[digits] digits LOG:  duration: ... plan:' ANYWHERE in the tail, with no
       timestamp check at all, so a statement's own author could write that text into their own SQL and
       have PostgreSQL echo it back verbatim in the STATEMENT: companion after a syntax error — tab-indented
       continuation lines and all — forging a plan, with any query id and duration, onto any real query's
       history. A '^\d{4}-\d\d-\d\d ...' timestamp required at a genuine line start closes that: forged
       text is never the first character of a raw physical line, because a real line always opens with
       log_line_prefix and a continuation always opens with a tab, and 'n' makes '^' match only right after
       a newline or at the very start of the tail. Self-hosted's own log_line_prefix is whatever the
       operator configured, but the space-separated family is PostgreSQL's own default and the only one this
       route has ever recognised — PgPlanLogParser.s_planBlock carries the colon-separated (managed-prefix)
       alternative too, because that family is the norm on the RDS log-API route this SQL never runs on.

       The marker row the gate emits instead of log rows when logging_collector is off (#3410) is spelled
       in this query's own three columns; ReadAsync turns it into PgLoggingCollectorOffException, and the
       runner records the named non-fatal skip — not-collected with the reason, never a silent zero that
       reads as a target with nothing slow on it. A second marker arm (#3997) covers the narrower gap where
       logging_collector is on but log_destination carries no stderr format — csvlog/jsonlog only — so the
       shared tail's newest CTE excludes every file it sees; ReadAsync turns that one into
       PgNoStderrLogFileException, and the two arms are mutually exclusive by construction (one needs the
       setting off, the other needs it on). */
    /* The marker text the regexp anchors on, spliced as its own literal so the binary-route amplification
       guard below (item 1, #4058) and the pattern's own literal stay ONE spelling. */
    private const string PlanMarkerLiteral = "LOG:  duration: ";

    /* The csvlog twin of PlanMarkerLiteral (#4053 part b2): csvlog's own "message" field never carries the
       severity label — that is its own column — so a captured record's message reads "duration: N ms
       plan:\n{json}" with no "LOG:  " head, verified live on a pg18 rig with logging_collector=on and
       log_destination=csvlog. */
    private const string PlanMarkerCsvLiteral = "duration: ";

    /* #4058 item 3: (m[1])::bigint and (m[2])::double precision throw when a forged capture ("LOG:  duration:"
       with any query id, planted the same way #4008 planted a whole block) exceeds the target type — a
       19-plus-digit query id, or a duration with hundreds of digits — and PostgreSQL raises the cast error
       OUT OF THE SELECT LIST, which aborts the whole statement and blinds every OTHER real capture in the
       same 4 MB tail, not just the forged one. The guard is a CASE chain, evaluated in order, that only
       casts a capture already shaped like the target type: a query id of at most 19 digits whose numeric
       value is inside bigint's range, and a duration of at most 15 integer and 9 fractional digits (the
       ([0-9.]+) capture also admits '1.2.3', which fails the cast as invalid input, not only as overflow).
       No pg_input_is_valid: it is PostgreSQL 16+, and the TEXT route serves 14 and 15 targets too. So a
       forged row is nulled rather than aborting capture for every real row beside it — the parser already treats query_id = 0 as "the prefix carried no %Q" and DurationMs
       is not identity, so NULL reads the same as a block this bounded tail cut in half. */
    private const string QueryText = PgServerLogTail.TailCteSql + @"
SELECT
    CASE WHEN m[1] !~ '^-?[0-9]{1,19}$' THEN NULL
         WHEN (m[1])::numeric BETWEEN -9223372036854775808 AND 9223372036854775807 THEN (m[1])::bigint END AS query_id,
    CASE WHEN m[2] !~ '^[0-9]{1,15}(\.[0-9]{1,9})?$' THEN NULL ELSE (m[2])::double precision END AS duration_ms,
    replace(m[3], chr(9), '')                        AS plan_json
FROM tail,
     regexp_matches(
         tail.body,
         '^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d+)? [^ [\n]+ [^[\n]*\[\d+\] (-?\d+) " + PlanMarkerLiteral + @"([0-9.]+) ms  plan:\s*\n((?:\t[^\n]*\n)+)',
         'gn') AS m
UNION ALL
SELECT NULL::bigint, NULL::double precision, '" + PgLoggingCollectorOffException.Marker + @"'
WHERE " + PgServerLogTail.LoggingCollectorOffMarkerSql + @"
UNION ALL
SELECT NULL::bigint, NULL::double precision, '" + PgNoStderrLogFileException.Marker + @"'
WHERE " + PgServerLogTail.NoStderrLogFileMarkerSql + @"
LIMIT 2000";

    /* The csvlog pair (#4053 part b2), sent instead of QueryText/BinaryQueryText once
       context.PgLogUsesCsvlog says the target's log_destination includes csvlog — the same flag
       PgLogEventsCollector reads for its own csvlog pair (#4053 part a1b). Opened on
       PgServerLogTail.TailCsvCteSql/TailCsvCteBinarySql instead of the stderr twins, this collector's own
       regexp_matches is dropped entirely: a csvlog record already carries the plan JSON quoted whole in
       its own "message" field (see PgServerLogCsvParser's type header for the 26-column shape), so there
       is no block to extract with SQL — ReadAsync gets the raw body and calls PgServerLogCsvParser.Parse
       itself, the same shape PgLogEventsCollector's csv branch takes. The marker arms carry
       PgNoCsvlogFileException.Marker, not PgNoStderrLogFileException.Marker, so the fault this route
       throws names csvlog rather than stderr — PgLogEventsCollector's csv branch makes the identical
       choice. */
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

    /* The binary-route twin of CsvQueryText (#4053 part b2), the same shape BinaryQueryText is to
       QueryText: pg_read_binary_file in place of pg_read_file, sent only once
       PgReadBinaryFileCapability.IsGrantedAsync finds the grant. tail.body is bytea here, so the marker
       arms are cast through convert_to, the same reason PgLogEventsCollector's own binary-route csv pair
       gives. */
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

    /* The guarded duration shape, in C# (#4053 part b2): the same "^[0-9]{1,15}(\.[0-9]{1,9})?$" the two
       stderr statements above cast under a CASE chain, so a forged duration text — csvlog quotes it inside
       the "message" field the same as any other value, so nothing server-side can guard it the way the
       stderr regex does — is rejected before double.TryParse ever sees it rather than trusted to fail
       parsing on every malformed shape ("1.2.3" parses as 1.2 under some cultures' TryParse otherwise). */
    private static readonly Regex s_csvDurationShape = new(
        @"^[0-9]{1,15}(\.[0-9]{1,9})?$", RegexOptions.Compiled, TimeSpan.FromSeconds(1));

    /* The binary-route twin (#4046 part 1c) — same parity gap as the deadlock reader, since both filter
       tail.body with a server-side regexp_matches: encode(..., 'escape') feeds the identical pattern, and
       ReadAsync reverses it on plan_json via PgBinaryTailText.UnescapeAndDecode. The marker arms are
       untouched: their third column is a plain text literal on both routes.

       #4058 item 1: the same amplification the deadlock reader's remarks measure — encode(tail.body,
       'escape') can quadruple a high-byte-stuffed tail before the regex engine sees it, on every cycle,
       whether or not this tail holds a plan capture at all. The WHERE clause filters tail.body (bytea)
       BEFORE encode()/regexp_matches() run, for the identical reason PgDeadlocksCollector's remarks give:
       a one-row FROM tail with volatile-cost expressions in the SELECT list evaluates WHERE against that
       row first, so an absent marker skips both calls entirely rather than running them and discarding the
       result. The marker cannot skip a real capture: auto_explain's LOG line always contains this exact
       literal before the query id or duration is glued around it, so it is a NECESSARY substring of every
       row the pattern beneath it could match, present or not present makes no difference to which rows
       come back or in what order. Its qual references only tail.body, and pg_read_binary_file is volatile,
       so the CTE stays materialized and this filter runs at the CTE scan, below the lateral regexp_matches
       call — a refactor that referenced m in this WHERE would silently break that ordering.

       #4058 item 3: the same guarded casts as the text route, immediately above — a forged capture on the
       binary route is the identical shape after encode()/decode, so it gets the identical treatment.

       This is an OPTIMIZATION, not a bound: an attacker can plant the marker the same way #4008 planted a
       whole block, and plan capture needs no plant at all — the marker is present on every cycle wherever
       auto_explain logs anything. See the type header's #4058 M1 remarks (still open on the issue). */
    private const string BinaryQueryText = PgServerLogTail.TailCteBinarySql + @"
SELECT
    CASE WHEN m[1] !~ '^-?[0-9]{1,19}$' THEN NULL
         WHEN (m[1])::numeric BETWEEN -9223372036854775808 AND 9223372036854775807 THEN (m[1])::bigint END AS query_id,
    CASE WHEN m[2] !~ '^[0-9]{1,15}(\.[0-9]{1,9})?$' THEN NULL ELSE (m[2])::double precision END AS duration_ms,
    replace(m[3], chr(9), '')                        AS plan_json
FROM tail,
     regexp_matches(
         pg_catalog.encode(tail.body, 'escape'),
         '^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d+)? [^ [\n]+ [^[\n]*\[\d+\] (-?\d+) " + PlanMarkerLiteral + @"([0-9.]+) ms  plan:\s*\n((?:\t[^\n]*\n)+)',
         'gn') AS m
WHERE pg_catalog.position(tail.body, '" + PlanMarkerLiteral + @"'::bytea) > 0
UNION ALL
SELECT NULL::bigint, NULL::double precision, '" + PgLoggingCollectorOffException.Marker + @"'
WHERE " + PgServerLogTail.LoggingCollectorOffMarkerSql + @"
UNION ALL
SELECT NULL::bigint, NULL::double precision, '" + PgNoStderrLogFileException.Marker + @"'
WHERE " + PgServerLogTail.NoStderrLogFileMarkerSql + @"
LIMIT 2000";

    public override string Name => "pg_plan_capture";

    public override string TargetTable => "pg_plan_capture";

    /// <summary>
    /// Every PostgreSQL target — including Aurora and RDS, which reach the same table by a different road.
    ///
    /// <para><b>This deliberately does NOT gate on the engine, and the reason is worth stating.</b> Gating
    /// here would make the capability model report plan capture as a PERMANENT GAP on Aurora, which is a
    /// lie: those targets do capture plans, through the RDS log API (<c>RdsPlanIngestor</c>, #2538). The
    /// route is chosen at dispatch, so this definition never actually executes against a managed target and
    /// cannot produce the permission failure that gating was meant to avoid.</para>
    ///
    /// <para>An absent grant, an unloaded module or an unlistable log directory all raise errors the host
    /// classifies as non-fatal skips, and <c>pg_plan_capture_readiness</c> already reports which
    /// precondition is missing.</para>
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    /// <summary>Server-wide: one log holds every database's plans.</summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override CollectorQuery BuildQuery(CollectorContext context) =>
        new(context.PgLogUsesCsvlog
            ? (context.PgReadBinaryFileGranted ? CsvBinaryQueryText : CsvQueryText)
            : (context.PgReadBinaryFileGranted ? BinaryQueryText : QueryText));

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("query_id", CollectorColumnType.BigInt),
        new CollectorColumn("plan_hash", CollectorColumnType.Varchar),
        new CollectorColumn("duration_ms", CollectorColumnType.Double),
        new CollectorColumn("node_count", CollectorColumnType.Integer),
        new CollectorColumn("top_node_type", CollectorColumnType.Varchar),
        /* No database_name and no query_text, both deliberately. The log line prefix is not guaranteed to
           carry %d, so a database column would be a claim the source cannot support (#2599); the text is
           dropped for the reason in the type header. */
        new CollectorColumn("plan_json", CollectorColumnType.Varchar),
    };

    /// <summary>
    /// The count a consumer records on its collection-log row when a capture's query id OR duration came
    /// back NULL from the guarded CASE chain — a forged capture (#4058 L1), never a real one: <c>%Q</c>
    /// always prints an in-range signed int64 (0 when <c>compute_query_id</c> is off) and the duration is
    /// always <c>%.3f</c>, so neither can fail the CASE's shape check on a genuine auto_explain line. Before
    /// this, a forged row's NULL columns were passed to <see cref="PgPlanLogParser.FromBlock"/> as literal
    /// 0s, which stored the forgery under query id 0 with a 0 ms duration instead of dropping it — the same
    /// pattern <see cref="PgServerLogTail.ForeignZoneLinesMeasurement"/> follows for its own skip.
    /// </summary>
    public const string ForgedCaptureMeasurement = "forged_captures_skipped";

    /// <summary>
    /// The sentence the Darling runner puts beside <see cref="ForgedCaptureMeasurement"/> on the row: a count
    /// label alone cannot say why the row is forged or why it matters, and an operator who finds the count
    /// needs both.
    /// </summary>
    public const string ForgedCaptureNote =
        "Skipped auto_explain captures whose query id or duration did not match the guarded shape a real "
        + "capture always has: a client can plant one through a syntax error whose STATEMENT: companion "
        + "echoes attacker-chosen text back into the log (#4058). The read went on without them";

    /// <summary>
    /// The count a consumer records on its collection-log row when the csvlog parser discarded a record
    /// (#4053 part b2), following <see cref="PgLogEventsCollector.CsvRecordsDiscardedMeasurement"/>'s exact
    /// pattern — the same underlying parser, the same discard reasons (a resync fragment or a bad shape).
    /// </summary>
    /* A literal on purpose: the measurement-label gate (CollectorMeasurementSeamTests) reads labels only as
       string-literal consts in the collector's own file. It must stay equal to PgLogEventsCollector's label. */
    public const string CsvRecordsDiscardedMeasurement = "csv_records_discarded";

    /// <summary>
    /// The csvlog branch (#4053 part b2): <c>PgServerLogCsvParser.Parse</c> on the raw body — the same call
    /// <see cref="PgLogEventsCollector"/>'s own csv branch makes — then keeps only the records whose
    /// <c>Message</c> is an auto_explain duration-plan message, verified on the rig: csvlog quotes the
    /// entire "duration: N ms  plan:\n{...}" text, tabs and all, inside the record's own <c>message</c>
    /// field, and PostgreSQL's own <c>query_id</c> column (PG14+) carries the identity — preferred over
    /// parsing it out of any prefix text, because csvlog has no <c>%Q</c>-in-prefix text to parse at all.
    /// The duration is taken from the message with the SAME guarded shape the stderr SQL casts under a CASE
    /// chain (<see cref="s_csvDurationShape"/>), so a forged duration in a planted message is skipped and
    /// counted under <see cref="ForgedCaptureMeasurement"/> exactly as the stderr route's guarded cast
    /// would null it, rather than parsed by double.TryParse's own more permissive grammar.
    /// </summary>
    private async ValueTask<List<Row>> ReadCsvAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();
        var forgedCaptures = 0;
        var totalRecordsDiscarded = 0;

        while (await reader.ReadAsync(cancellationToken))
        {
            /* #4046 part 1c: on the binary route column 0 is bytea, real tail data and the convert_to'd
               marker rows alike — the identical decode PgLogEventsCollector's own csv branch applies. */
            var body = reader.IsDBNull(0)
                ? null
                : context.PgReadBinaryFileGranted
                    ? PgBinaryTailText.DecodeWhole(reader.GetFieldValue<byte[]>(0), context.PgLogEncoding ?? System.Text.Encoding.UTF8)
                    : reader.GetString(0);

            if (string.Equals(body, PgLoggingCollectorOffException.Marker, StringComparison.Ordinal))
            {
                throw new PgLoggingCollectorOffException();
            }

            if (string.Equals(body, PgNoCsvlogFileException.Marker, StringComparison.Ordinal))
            {
                throw new PgNoCsvlogFileException();
            }

            var entries = PgServerLogCsvParser.Parse(body ?? string.Empty, out var recordsDiscarded);
            totalRecordsDiscarded += recordsDiscarded;

            rows.AddRange(PlanRowsFromCsvEntries(entries, out var forgedInThisBatch));
            forgedCaptures += forgedInThisBatch;
        }

        if (forgedCaptures > 0)
        {
            context.Measure(ForgedCaptureMeasurement, forgedCaptures);
        }

        if (totalRecordsDiscarded > 0)
        {
            context.Measure(CsvRecordsDiscardedMeasurement, totalRecordsDiscarded);
        }

        return rows;
    }

    /// <summary>
    /// The per-entry half of <see cref="ReadCsvAsync"/> (#4053 part c3): classify csvlog entries into plan
    /// rows, with no reader and no context — a shared step the RDS/Aurora plan ingestor can call over
    /// entries the RDS log API handed it, rather than a second copy of the marker check, the LOG-severity
    /// gate, the query-id and duration guards and <see cref="PgPlanLogParser.FromBlock"/>. Internal, visible
    /// to the Darling service (that caller's assembly) and nothing else (#4053 c3 review).
    /// <para><b>Precondition: csv-parser entries only.</b> The query id is read from the text after the LAST
    /// comma of <see cref="PgLogEntry.RawText"/>. That is the unquoted <c>query_id</c> column only because
    /// <see cref="PgServerLogCsvParser"/> admits a record only with exactly 26 fields. An entry from the stderr
    /// assembler or the jsonlog parser carries raw line text, whose last comma can sit in client-written
    /// message text, so a client could choose the query id. Never pass those entries here.</para>
    /// <para>No foreign-zone filter, on either route: plan rows are stamped with the collection time, never a
    /// log timestamp, and the stderr plan routes never filtered either.</para>
    /// </summary>
    /// <param name="forgedCaptures">The count this batch of entries added to
    /// <see cref="ForgedCaptureMeasurement"/> — a query id or duration that did not match the guarded shape
    /// a real capture always has (#4058 L1).</param>
    internal static List<Row> PlanRowsFromCsvEntries(IEnumerable<PgLogEntry> entries, out int forgedCaptures)
    {
        ArgumentNullException.ThrowIfNull(entries);

        var rows = new List<Row>();
        forgedCaptures = 0;

        foreach (var entry in entries)
        {
            /* csvlog carries severity in its own field ("error_severity"), never glued onto Message the
               way the stderr line's log_line_prefix glues "LOG:  " on — verified on the rig: the
               captured record's message field is "duration: N ms  plan:\n{json}", not
               "LOG:  duration: ...". PlanMarkerCsvLiteral is that literal minus the stderr-only
               "LOG:  " head. A record whose Message does not start with it is not an auto_explain
               capture and is not this collector's concern — PgLogEventsCollector's own classifier
               reads the same tail for every OTHER family. */
            if (entry.Message is null || !entry.Message.StartsWith(PlanMarkerCsvLiteral, StringComparison.Ordinal))
            {
                continue;
            }

            /* auto_explain writes at LOG. The stderr route's regex requires the LOG label, so the csv route
               does too (review round 1): without it, a client's RAISE NOTICE or WARNING whose message
               starts with the marker would reach the parse, which the stderr route never allows. */
            if (!string.Equals(entry.Severity, "LOG", StringComparison.Ordinal))
            {
                continue;
            }

            var rest = entry.Message[PlanMarkerCsvLiteral.Length..];
            var msIndex = rest.IndexOf(" ms  plan:", StringComparison.Ordinal);

            /* No plan text: a genuine log_min_duration_statement or log_duration record starts the same way.
               It is not a capture, and not forged, so it is skipped WITHOUT counting (review round 1);
               counting it would grow forged_captures_skipped with every slow statement. */
            if (msIndex < 0)
            {
                continue;
            }

            var durationText = rest[..msIndex];
            var planJson = rest[(msIndex + " ms  plan:".Length)..].TrimStart('\r', '\n');

            /* The query_id column (PG14+, 0-based index 25) — preferred over any prefix text, because
               csvlog carries no %Q-rendered prefix at all: the identity is PostgreSQL's own column. A
               query id that is not a real value — %Q renders 0 when compute_query_id is off, never an
               unparseable string — marks this record as one this collector cannot attribute. */
            if (!long.TryParse(
                    entry.RawText.Length > 0 ? QueryIdFromRawText(entry.RawText) : null,
                    NumberStyles.Integer | NumberStyles.AllowLeadingSign,
                    CultureInfo.InvariantCulture,
                    out var queryId))
            {
                forgedCaptures++;
                continue;
            }

            if (!s_csvDurationShape.IsMatch(durationText)
                || !double.TryParse(durationText, NumberStyles.Float, CultureInfo.InvariantCulture, out var durationMs))
            {
                forgedCaptures++;
                continue;
            }

            var parsed = PgPlanLogParser.FromBlock(queryId, durationMs, planJson);

            if (parsed is not null)
            {
                rows.Add(new Row(
                    QueryId: parsed.Value.QueryId,
                    PlanHash: parsed.Value.PlanHash,
                    DurationMs: parsed.Value.DurationMs,
                    NodeCount: parsed.Value.NodeCount,
                    TopNodeType: parsed.Value.TopNodeType,
                    PlanJson: parsed.Value.PlanJson));
            }
        }

        return rows;
    }

    /// <summary>
    /// Reads the <c>query_id</c> field — the last of the 26 csvlog columns (#4053 part b2) — straight off
    /// the record's own raw text rather than re-splitting it through <c>PgServerLogCsvParser</c>'s private
    /// field splitter, which <see cref="PgLogEntry"/> does not expose past field 19 (its own consumers never
    /// needed the trailing columns). <c>query_id</c> is PostgreSQL's own bigint rendering — never quoted —
    /// so it is the text after the LAST comma in the record.
    /// </summary>
    private static string? QueryIdFromRawText(string rawText)
    {
        var lastComma = rawText.LastIndexOf(',');
        return lastComma < 0 ? null : rawText[(lastComma + 1)..].TrimEnd('\r', '\n');
    }

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();
        var forgedCaptures = 0;

        if (context.PgLogUsesCsvlog)
        {
            return await ReadCsvAsync(reader, context, cancellationToken);
        }

        while (await reader.ReadAsync(cancellationToken))
        {
            /* The marker row the query returns instead of listing the log directory when
               logging_collector is off (#3410). A real row always carries a query id — the regexp capture
               is literal digits cast to bigint — so a NULL first column plus the marker text is the gate's
               row and nothing else's. Thrown so the runner records the named skip; skipped, a server that
               logs to stderr reads as a target with no slow statements. */
            if (reader.IsDBNull(0)
                && !reader.IsDBNull(2)
                && string.Equals(reader.GetString(2), PgLoggingCollectorOffException.Marker, StringComparison.Ordinal))
            {
                throw new PgLoggingCollectorOffException();
            }

            /* The second marker row (#3997): logging_collector is on but every file in the directory was a
               csvlog/jsonlog sibling, so the shared tail found no stderr-format file this cycle. Same shape
               as the check above — a NULL query id plus this marker text cannot be a real capture. */
            if (reader.IsDBNull(0)
                && !reader.IsDBNull(2)
                && string.Equals(reader.GetString(2), PgNoStderrLogFileException.Marker, StringComparison.Ordinal))
            {
                throw new PgNoStderrLogFileException();
            }

            /* #4046 part 1c: on the binary route plan_json came back through encode(..., 'escape'),
               reversed here before the parser sees it — after both marker checks above, which compare
               against the literal (never-escaped) marker constants. */
            var planJson = reader.IsDBNull(2) ? null : reader.GetString(2);
            if (planJson is not null && context.PgReadBinaryFileGranted)
            {
                planJson = PgBinaryTailText.UnescapeAndDecode(planJson, context.PgLogEncoding ?? System.Text.Encoding.UTF8);
            }

            /* #4058 L1: a NULL query id or duration here is the guarded CASE chain's own refusal — a
               capture whose shape does not fit the target type, which a real auto_explain line can never
               produce. Passing 0 for either NULL (as the parser's other call site below still does for a
               genuine mid-block cut) would store the forgery under query id 0 with a 0 ms duration instead
               of dropping it. Counted rather than silently dropped, the same way
               PgServerLogTail.MeasureForeignZoneLines counts its own skip. */
            if (reader.IsDBNull(0) || reader.IsDBNull(1))
            {
                forgedCaptures++;
                continue;
            }

            /* Extraction, redaction and hashing live in PgPlanLogParser, shared with the RDS log-API
               transport (#2538). Two implementations of the redaction would eventually disagree, and the
               cost of THAT divergence is customer data rather than a wrong number. */
            var parsed = PgPlanLogParser.FromBlock(
                reader.GetInt64(0),
                reader.GetDouble(1),
                planJson);

            /* Null is a block the bounded tail read cut in half, which is ordinary rather than
               exceptional: the window can begin mid-plan. Skipped, not stored half-parsed. */
            if (parsed is not null)
            {
                rows.Add(new Row(
                    QueryId: parsed.Value.QueryId,
                    PlanHash: parsed.Value.PlanHash,
                    DurationMs: parsed.Value.DurationMs,
                    NodeCount: parsed.Value.NodeCount,
                    TopNodeType: parsed.Value.TopNodeType,
                    PlanJson: parsed.Value.PlanJson));
            }
        }

        if (forgedCaptures > 0)
        {
            context.Measure(ForgedCaptureMeasurement, forgedCaptures);
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.QueryId)
            .Value(row.PlanHash)
            .Value(row.DurationMs)
            .Value(row.NodeCount)
            .Value(row.TopNodeType)
            .Value(row.PlanJson);
    }
}
