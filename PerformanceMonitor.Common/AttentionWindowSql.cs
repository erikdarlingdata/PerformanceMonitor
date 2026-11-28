/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;
using System.Linq;

namespace PerformanceMonitor.Common;

/// <summary>
/// The Client Timeouts alert's read (#5640), written ONCE for both stores: the PostgreSQL store (Darling) and the
/// DuckDB store (Lite) run the same text over their own relation names, so the two reads cannot drift. Every statement binds
/// <c>$1</c> = server_id and <c>$2</c> = the naive-UTC floor (now minus the engine's lookback; both stores keep
/// microseconds in <c>collection_time</c>, so the floor is bound as a full-precision <see cref="System.DateTime"/>).
///
/// <para><b>The count model.</b> <c>attention_events</c> holds one <c>count</c> row per collector run, written only when the
/// event_counter moved, with the exact attentions since the prior run in <c>attention_count</c>; every other row is a
/// <c>sample</c> (one per kept ring-buffer event, <c>attention_count</c> 0) with database, application, fingerprint and
/// text. Samples are detail, never the total, and a quiet run writes no row, so the runs come from the collection log.</para>
///
/// <para><b>What counts as a run.</b> A collection_log row with <c>collector_name = 'attention'</c> and
/// <c>status = 'SUCCESS'</c>, in both stores. A run that found the session missing (<c>SESSION_MISSING</c>), failed, or was
/// skipped did not read the counter, so it is not proof the span was observed.</para>
/// </summary>
public static class AttentionWindowSql
{
    /// <summary>The collector's name in <c>collection_log.collector_name</c>, in both stores.</summary>
    public const string CollectorName = "attention";

    /// <summary>The collection log status of a run that read the counter.</summary>
    public const string SuccessStatus = "SUCCESS";

    /// <summary>The <c>event_kind</c> of a run's counter row.</summary>
    public const string KindCount = "count";

    /// <summary>The <c>event_kind</c> of a kept ring-buffer event.</summary>
    public const string KindSample = "sample";

    /// <summary>The <c>event_kind</c> of one captured call that ended Abort, or Error after at least 0.9 s (#9507). A command the
    /// client cancelled while it still ran has that shape, and its <c>interrupt_duration_us</c> is the call's elapsed time, which is
    /// the length of the command timeout. A batch-aborting error after 0.9 s or more (a conversion error, a deadlock victim that
    /// waited) ends with result Error too, on SQL Server 2022 as well as 2025. Such a row has no attention partner, so every read
    /// leaves it out. The same value as <c>AttentionCollector.KindAborted</c>.</summary>
    public const string KindAborted = "aborted";

    /// <summary>How far BEFORE an attention sample's event_time a captured call's event_time may fall and still match it (#9507), in
    /// microseconds. On SQL Server 2022 and 2025 the captured call and the attention carry the same millisecond timestamp, the call first,
    /// in 20 of 20 real timeouts. A field pairing measured every real cancel within 20 ms of its attention, from the client side. 100 ms
    /// leaves margin under load. A wider window would let an error that ends a call (a deadlock victim), followed by a reader cleanup
    /// cancel on the same session, count as a timeout.</summary>
    public const long CapturedCallMatchBeforeUs = 100_000;

    /// <summary>How far AFTER an attention sample's event_time a captured call's event_time may fall and still match it (#9507), in
    /// microseconds. The captured call is written at or before the attention (see <see cref="CapturedCallMatchBeforeUs"/>), so this is
    /// margin only, and it is 100 ms for the same reason: a wider window would pair a call that ended in an error with a later cleanup
    /// cancel on the same session.</summary>
    public const long CapturedCallMatchAfterUs = 100_000;

    private static readonly string MatchCallToleranceBeforeSql = $"INTERVAL '{CapturedCallMatchBeforeUs} microseconds'";

    private static readonly string MatchCallWindowAfterSql = $"INTERVAL '{CapturedCallMatchAfterUs} microseconds'";

    /// <summary>The baseline read's collection_time cut: <c>$2</c> inclusive, <c>$3</c> exclusive.</summary>
    private const string BaselineRange = "collection_time >= $2 AND collection_time < $3";

    /// <summary>How many entries each top list holds.</summary>
    public const int TopCount = 5;

    /// <summary>The duration floor (#7061, raised from 1 ms to 1 s in #9030), in microseconds: a sampled attention that lasted under 1 s is
    /// left out of the judged rate and the baseline. The shortest are the SqlClient driver cancelling a command that has already finished, with
    /// no statement text and 20 to 40 microseconds. Busy servers also show cancels of a few milliseconds that no command timeout can explain:
    /// a client command timeout under 1 s is very rare, and a real one lasts about as long as the command timeout, often 30 s. Cancels of long
    /// queries (minutes or hours) are few, still client aborts, and keep counting. An event of exactly 1 s counts. A sample with NO recorded
    /// duration (NULL) is not under the floor and still counts, the way every sample did before the floor.</summary>
    public const long MinInterruptDurationUs = 1_000_000;

    /// <summary>The shortest call, in microseconds, that both the session keeps and the read counts as a captured call (#9538). A command
    /// timeout is whole seconds in every driver, so a real one runs about 1 s and can be recorded a little under it: the session keeps calls
    /// from 0.9 s, and the read of the captured calls uses the same value, so a call that was kept is never thrown away by the read. It is
    /// below <see cref="MinInterruptDurationUs"/> on purpose: that floor judges an attention sample, this one judges a call.</summary>
    public const long CapturedCallMinDurationUs = 900_000;

    /// <summary>The floor as it reads in a message: "1 s" for a whole number of seconds, "N ms" for a whole number of milliseconds, else
    /// microseconds. Follows <see cref="MinInterruptDurationUs"/>, so no message carries its own copy of the value (#9030).</summary>
    public static string MinInterruptDurationText => DurationText(MinInterruptDurationUs);

    /// <summary>A microsecond count as "N s" (whole seconds), "N ms" (whole milliseconds) or "N µs".</summary>
    public static string DurationText(long microseconds) => microseconds switch
    {
        > 0 when microseconds % 1_000_000 == 0 => $"{microseconds / 1_000_000} s",
        > 0 when microseconds % 1000 == 0 => $"{microseconds / 1000} ms",
        _ => $"{microseconds} µs",
    };

    /// <summary>The client application Azure Data Studio connects under. Its SQL Tools Service names every connection <c>azdata</c>, the query
    /// editor's too (connection pooling is on by default, and with it the service adds no purpose suffix to the name), so the name alone cannot
    /// be left out without hiding a real timeout from the query editor (#7061). The metadata statements are told apart by shape, see
    /// <see cref="AzdataMetadataPredicate"/>.</summary>
    public const string AzdataClientApp = "azdata";

    /// <summary>The client application the dbatools PowerShell module connects under (#8877). The same name runs people's own scripts and
    /// queries (<c>Invoke-DbaQuery</c>), so the name alone cannot be left out without hiding a real timeout. The SMO catalog reads are told apart by
    /// the parameter marker, see <see cref="DbatoolsSmoMetadataPredicate"/>.</summary>
    public const string DbatoolsClientApp = "dbatools PowerShell module - dbatools.io";

    /// <summary>The parameter marker SMO puts in the statements it generates (<c>@_msparam_0</c>, <c>@_msparam_1</c>, ...) (#8877).</summary>
    public const string SmoParameterMarker = "@_msparam_";

    /// <summary>What an SSMS IntelliSense client application name starts with (#6552, #7061). The C# rule
    /// (<c>AttentionEvaluator.IsSsmsIntelliSense</c>) and the SQL rule (<see cref="ExcludedAppPredicate"/>) both read this, so they cannot drift.</summary>
    public const string SsmsIntelliSensePrefix = "Microsoft SQL Server Management Studio";

    /// <summary>What an SSMS IntelliSense client application name ends with, ignoring trailing whitespace (#6552, #7061).</summary>
    public const string SsmsIntelliSenseSuffix = "IntelliSense";

    /// <summary>The VS Code mssql extension's metadata connections (#6856). The SQL Tools Service it runs appends the connection purpose to
    /// the application name "vscode-mssql" (ConnectionService.GetApplicationNameWithFeature), and the language service adds "-languageService".
    /// Metadata work is the General connection and Object Explorer, each with or without that suffix, plus the language service alone.
    /// Four of the five names were seen in the field, in the client application names a production store recorded (the report in #6856),
    /// the two double-suffix names among them. Only "vscode-mssql-languageService" rests on the extension's source alone. No source line
    /// names the query editor's application, so the rule is a closed list: any name not on it, "vscode-mssql" included, stays counted,
    /// the way SSMS "- Query" does, and shows in the alert's top client apps. Matched ignoring case and surrounding whitespace, in C# and in
    /// <see cref="ExcludedAppPredicate"/>.</summary>
    public static readonly string[] VsCodeMetadataApps =
    {
        "vscode-mssql-GeneralConnection",
        "vscode-mssql-GeneralConnection-languageService",
        "vscode-mssql-ObjectExplorer",
        "vscode-mssql-ObjectExplorer-languageService",
        "vscode-mssql-languageService",
    };

    /// <summary>A string as a SQL literal body: every quote doubled.</summary>
    private static string SqlQuote(string value) => value.Replace("'", "''");

    /// <summary>A string as a LIKE pattern body: the backslash escape character, <c>%</c> and <c>_</c> made literal. Used with <c>ESCAPE '\'</c>.</summary>
    private static string LikeEscape(string value) =>
        SqlQuote(value.Replace("\\", "\\\\").Replace("%", "\\%").Replace("_", "\\_"));

    /// <summary>True for a sample whose recorded duration is under <see cref="MinInterruptDurationUs"/>. NULL when the duration is NULL, so
    /// it is never true for a missing duration. Written once for both stores (PostgreSQL and DuckDB run it as is).</summary>
    public static readonly string ShortDurationPredicate = $"interrupt_duration_us < {MinInterruptDurationUs}";

    /// <summary>
    /// True for a sample of Azure Data Studio's own metadata work (#7061): the client application is <c>azdata</c> AND the statement is one
    /// of the shapes its object explorer and dashboard send through SMO, either the server-info temp table (<c>create table #SVer</c>) or a
    /// SMO-generated column read (the <c>clmns</c> alias SMO gives <c>sys.all_columns</c> / <c>sys.columns</c>). The query editor connects under
    /// the same name, so the statement shape is the only thing that tells them apart; a query from the editor that does not look like SMO still
    /// counts. A sample with no statement text (the statement filter withheld it) is never matched. ILIKE and lower/trim/ltrim behave alike in
    /// PostgreSQL and DuckDB. NULL when the application or the text is NULL.
    /// </summary>
    public static readonly string AzdataMetadataPredicate =
        $"(lower(trim(client_app_name)) = '{AzdataClientApp}' AND (ltrim(statement_text) ILIKE 'create table #sver%' OR statement_text ILIKE '% as clmns%'))";

    /// <summary>
    /// True for a sample of dbatools' SMO catalog reads (#8877): the client application is <c>dbatools PowerShell module - dbatools.io</c> AND the
    /// statement text holds SMO's parameter marker (<c>@_msparam_</c>, underscores escaped so they match literally). The same application name runs
    /// people's own queries, so a statement without the marker still counts. A sample with no statement text (the statement filter withheld it) is
    /// never matched. NULL when the application or the text is NULL. The application cannot be <c>azdata</c> too, so this and
    /// <see cref="AzdataMetadataPredicate"/> never overlap.
    /// </summary>
    public static readonly string DbatoolsSmoMetadataPredicate =
        $"(lower(trim(client_app_name)) = '{SqlQuote(DbatoolsClientApp.ToLowerInvariant())}' AND statement_text ILIKE '%{LikeEscape(SmoParameterMarker)}%' ESCAPE '\\')";

    /// <summary>
    /// True for a sample whose client application is SSMS IntelliSense or one of the VS Code mssql metadata connections (#6552, #6856), the
    /// SQL twin of <c>AttentionEvaluator.IsExcludedApp</c>, generated from the same names (<see cref="SsmsIntelliSensePrefix"/>,
    /// <see cref="SsmsIntelliSenseSuffix"/>, <see cref="VsCodeMetadataApps"/>). SSMS: starts with the prefix and, trailing whitespace ignored, ends with
    /// the suffix, ignoring case. VS Code: the name, trimmed, equals one name on the list, ignoring case. Only the BASELINE read uses it
    /// (<see cref="BaselineMinutes"/>), so the baseline is scaled by the same events the evaluator leaves out of the rate; the rate path leaves
    /// those groups out in C# and does not run it. ILIKE with an explicit ESCAPE, lower, trim and rtrim behave alike in PostgreSQL and DuckDB.
    /// NULL when the application is NULL.
    /// </summary>
    public static readonly string ExcludedAppPredicate =
        $"((client_app_name ILIKE '{LikeEscape(SsmsIntelliSensePrefix)}%' ESCAPE '\\' AND rtrim(client_app_name) ILIKE '%{LikeEscape(SsmsIntelliSenseSuffix)}' ESCAPE '\\')"
        + $" OR lower(trim(client_app_name)) IN ({string.Join(", ", VsCodeMetadataApps.Select(a => $"'{SqlQuote(a.ToLowerInvariant())}'"))}))";

    /// <summary>
    /// The captured calls (#9507): one row per <c>aborted</c> event with what a match needs (session, database, time) and whether the call is
    /// Azure Data Studio or dbatools metadata work, judged on the CALL's own statement (the attention row carries none, so the call is the
    /// only place the statement can be told apart). <paramref name="range"/> is the collection_time cut, written with the prefix passed in.
    /// A call is collected with its attention event or just after it, so it takes the same floor and ceiling as the samples.
    /// A call that ran under the capture floor (<see cref="CapturedCallMinDurationUs"/>) is not a call here (#9538): an application cancel
    /// a few milliseconds into a call, such as a data layer cancelling before it closes a reader, is no command timeout, and a driver's
    /// command timeout runs about a second. The floor is the one the session captures from, so nothing it kept is thrown away here. A call with no recorded duration is left out too, as the collector always writes one.
    /// </summary>
    private static string CallsSelect(string eventsRelation, string range) => $"""
SELECT
        attention_event_id AS id,
        session_id,
        database_name,
        event_time,
        COALESCE({AzdataMetadataPredicate}, FALSE) AS azdata_metadata,
        COALESCE({DbatoolsSmoMetadataPredicate}, FALSE) AS dbatools_metadata
    FROM {eventsRelation}
    WHERE server_id = $1
    AND   {range}
    AND   event_kind = '{KindAborted}'
    AND   interrupt_duration_us >= {CapturedCallMinDurationUs}
    AND   session_id IS NOT NULL
    AND   event_time IS NOT NULL
""";

    /// <summary>
    /// The captured calls of a window as one statement (#9881), for a caller that reads them first and hands them to the reads below as
    /// arrays (<c>callsAt</c>). Columns: id (bigint), session_id (integer), database_name, event_time, azdata_metadata, dbatools_metadata.
    /// <c>$1</c> = server_id, <c>$2</c> = the inclusive floor, and with <paramref name="bounded"/> <c>$3</c> = the inclusive ceiling: the
    /// range the reads give their own calls, so the rows are the ones their <c>calls</c> CTE holds.
    /// </summary>
    public static string Calls(string eventsRelation, bool bounded = false) => CallsSelect(eventsRelation, "collection_time >= $2" + Ceiling(bounded));

    /// <summary>The captured calls of the baseline range (<c>$2</c> inclusive, <c>$3</c> exclusive), the calls <see cref="BaselineMinutes"/> matches. Same columns as <see cref="Calls"/>.</summary>
    public static string BaselineCalls(string eventsRelation) => CallsSelect(eventsRelation, BaselineRange);

    /// <summary>How many array parameters a read takes from <c>callsAt</c> on: one per column of <see cref="Calls"/>, in that order.</summary>
    public const int CallArrayCount = 6;

    /// <summary>
    /// The calls a caller read with <see cref="Calls"/>, as a relation over <see cref="CallArrayCount"/> array parameters starting at
    /// <paramref name="at"/> (PostgreSQL). The planner counts the elements of a bound array, so it knows how many calls there are; read from
    /// the compressed table inside the statement it guesses thousands for a few dozen and sorts every sample to meet them (#9881).
    /// </summary>
    private static string CallsUnnest(int at) =>
        $"unnest(${at}::bigint[], ${at + 1}::integer[], ${at + 2}::text[], ${at + 3}::timestamp[], ${at + 4}::boolean[], ${at + 5}::boolean[])"
        + " AS u(id, session_id, database_name, event_time, azdata_metadata, dbatools_metadata)";

    private static int WholeSecondsUp(long microseconds) => (int)((microseconds + 999_999) / 1_000_000);

    /// <summary>
    /// The whole-second shifts that put a call in the second of every sample it can match (#9881): a call ends at most
    /// <see cref="CapturedCallMatchAfterUs"/> after the sample and at most <see cref="CapturedCallMatchBeforeUs"/> before it, so the sample's
    /// second is the call's second moved by no more than those tolerances rounded up to whole seconds. Generated from the two constants, so a
    /// wider tolerance widens the list with it.
    /// </summary>
    private static readonly string MatchSecondShifts = string.Join(", ",
        Enumerable.Range(-WholeSecondsUp(CapturedCallMatchAfterUs), WholeSecondsUp(CapturedCallMatchAfterUs) + WholeSecondsUp(CapturedCallMatchBeforeUs) + 1)
            .Select(seconds => $"(INTERVAL '{seconds.ToString(CultureInfo.InvariantCulture)} seconds')"));

    /// <summary>
    /// The captured calls as a CTE named <c>calls</c>. MATERIALIZED: the CTE is read once, however many samples look at it. Left to inline,
    /// the sample list (<see cref="Samples"/>) runs the whole call scan again for every listed sample (#9507: 72 ms against 4 ms for a page of
    /// samples on a week at 60 samples a minute). With <paramref name="callsAt"/> the rows come from the caller's arrays, not the table.
    /// </summary>
    private static string CallsCte(string eventsRelation, string range, int? callsAt = null) => callsAt is int at
        ? $"""
calls AS MATERIALIZED (
    SELECT
        u.id,
        u.session_id,
        u.database_name,
        u.event_time,
        u.azdata_metadata,
        u.dbatools_metadata
    FROM {CallsUnnest(at)}
)
"""
        : $"""
calls AS MATERIALIZED (
    {CallsSelect(eventsRelation, range)}
)
""";

    /// <summary>The <c>calls</c> CTE and its comma, for a read that joins every sample to the calls. Empty with <paramref name="callsAt"/>: that join reads the arrays itself (<see cref="JoinCalls"/>).</summary>
    private static string CallsLead(string eventsRelation, string range, int? callsAt) => callsAt is null
        ? $"""
{CallsCte(eventsRelation, range)},

"""
        : "";

    /// <summary>
    /// The join of every under-floor sample (alias <paramref name="sample"/>) to the calls, alias <c>c</c>, on <see cref="MatchOn"/>.
    /// Without <paramref name="callsAt"/> it joins the <c>calls</c> CTE. With it (#9881) it joins the caller's arrays, each call once per
    /// second a matching sample can sit in (<see cref="MatchSecondShifts"/>), and adds that second to the join key. A matching pair meets on
    /// exactly one shift, the sample's second minus the call's, so the rows are the same; the key lets the join hash on (session, second),
    /// where the session alone makes every sample meet every call of its session, and with no CTE scan in it the pass over the samples can
    /// run in parallel workers.
    /// </summary>
    private static string JoinCalls(string sample, int? callsAt) => callsAt is int at
        ? $"""
INNER JOIN (
        SELECT
            u.id,
            u.session_id,
            u.database_name,
            u.event_time,
            u.azdata_metadata,
            u.dbatools_metadata,
            date_trunc('second', u.event_time) + o.shift AS matched_second
        FROM {CallsUnnest(at)}
        CROSS JOIN (VALUES {MatchSecondShifts}) AS o(shift)
    ) AS c
        ON c.matched_second = date_trunc('second', {sample}.event_time)
    AND   {MatchOn("c", sample)}
"""
        : $"""
INNER JOIN calls AS c
        ON {MatchOn("c", sample)}
""";

    /// <summary>
    /// The match rule (#9507): a call belongs to an attention event when it is the same session and database and the call ended from the
    /// tolerance before the attention to the window after it. <paramref name="call"/> and <paramref name="sample"/> are the aliases.
    /// A NULL database matches a NULL database.
    /// </summary>
    private static string MatchOn(string call, string sample) => $"""
{call}.session_id = {sample}.session_id
    AND   {call}.database_name IS NOT DISTINCT FROM {sample}.database_name
    AND   {call}.event_time >= {sample}.event_time - {MatchCallToleranceBeforeSql}
    AND   {call}.event_time <= {sample}.event_time + {MatchCallWindowAfterSql}
""";

    /// <summary>
    /// The under-floor samples that match a captured call, one row per sample (#9507). A join, never a correlated EXISTS: the baseline reads
    /// seven days of samples. Only samples under the floor are looked at, because a sample at or over the floor is judged anyway. Columns: id,
    /// the sample's own collection_time, database_name and client_app_name (what the readers group it by), and azdata_metadata and
    /// dbatools_metadata: the sample is that tool's metadata work, by its own statement or by the statement of a call it matches. On a store
    /// with no <c>aborted</c> rows the join is empty and the read gives the numbers of the samples alone.
    /// <para>The readers never join this back to the samples row by row (#9881). They count the samples from each row's own columns, which runs
    /// in parallel and sorts nothing wide, then correct the few groups that hold a matched sample with the counts of this CTE. A row-by-row
    /// LEFT JOIN of this CTE into the sample stream made the 7-day grouping serial, with every sample's statement text in an on-disk sort.</para>
    /// </summary>
    /* The id is unique in the table, so grouping by the sample's own columns beside it adds no group. The relation is read inside a derived
       table before it takes an alias: the Darling legacy-table read passes a derived table that already ends in "AS attention_events"
       (AttentionReadRelation.DerivedRelation), and a second alias on it is a syntax error (42601). */
    private static string MatchedCte(string eventsRelation, string range, int? callsAt = null) => $"""
matched AS (
    SELECT
        s.attention_event_id AS id,
        s.collection_time,
        s.database_name,
        s.client_app_name,
        bool_or(s.azdata_own OR c.azdata_metadata) AS azdata_metadata,
        bool_or(s.dbatools_own OR c.dbatools_metadata) AS dbatools_metadata
    FROM (
        SELECT
            attention_event_id,
            collection_time,
            database_name,
            client_app_name,
            session_id,
            event_time,
            COALESCE({AzdataMetadataPredicate}, FALSE) AS azdata_own,
            COALESCE({DbatoolsSmoMetadataPredicate}, FALSE) AS dbatools_own
        FROM {eventsRelation}
        WHERE server_id = $1
        AND   {range}
        AND   event_kind = '{KindSample}'
        AND   interrupt_duration_us < {MinInterruptDurationUs}
    ) AS s
    {JoinCalls("s", callsAt)}
    GROUP BY s.attention_event_id, s.collection_time, s.database_name, s.client_app_name
)
""";

    /// <summary>A sample under the floor, judged on the row alone: <see cref="ShortDurationPredicate"/>, FALSE for a missing duration.</summary>
    private static readonly string ShortOwnPredicate = $"COALESCE({ShortDurationPredicate}, FALSE)";

    /// <summary>A sample of Azure Data Studio metadata work by its own statement, FALSE when the application or the text is NULL.</summary>
    private static readonly string AzdataOwnPredicate = $"COALESCE({AzdataMetadataPredicate}, FALSE)";

    /// <summary>A sample of dbatools SMO metadata work by its own statement, FALSE when the application or the text is NULL.</summary>
    private static readonly string DbatoolsOwnPredicate = $"COALESCE({DbatoolsSmoMetadataPredicate}, FALSE)";

    /// <summary>The read-surface ceiling (#5640, get_client_timeouts): <c>$3</c> = the naive-UTC end of the window, so an <c>as_of</c> read
    /// does not count what happened after it. The alert's counts and sample groups pass <c>bounded: false</c> and have no ceiling; its top lists pass <c>true</c> since the final round of #5714 (L2).</summary>
    public static string Ceiling(bool bounded) => bounded ? "\nAND   collection_time <= $3" : "";

    /// <summary>The floor of the top lists. The alert's counts and sample groups (<c>bounded: false</c>) keep <c>collection_time &gt;= $2</c>, the lookback floor. The
    /// read surface and, since the final round of #5714 (L2), the alert's top lists (<c>bounded: true</c>) binds the SPAN boundary into <c>$2</c> and reads <c>collection_time &gt; $2</c>, with <c>$3</c> the
    /// inclusive row ceiling: the very cut <c>AttentionEvaluator.ReadSpan</c> makes of the counts and the sample
    /// groups (#5640), so a top list or a sampled event can never come from outside the rows the count came from.</summary>
    public static string SpanFloor(bool bounded) => bounded ? "collection_time > $2" : "collection_time >= $2";

    /// <summary>
    /// Successful run times, newest first. Column 0 = collection_time.
    /// <para><b>The guarantee this read leans on</b>: the attention collector runs per database on Azure SQL Database, but both runners write ONE
    /// blended <c>collection_log</c> row for the whole collector run, not one per database (Darling: <c>DarlingWorker</c> logs the run's single
    /// <c>CollectorRunResult</c>, the per-database cost rides on that row's fan-out rollup; Lite: <c>RemoteCollectorService</c> logs once after
    /// <c>RunDefinitionAsync</c> returns). So the run list is one time per cycle even on an Azure SQL DB server. If per-database runs ever logged separate
    /// rows, the times would differ by the database loop's seconds, <c>AttentionEvaluator.OldestTaken</c> would collapse the span to those seconds and
    /// the rate would inflate; <c>AttentionRunLogCardinalityPinTests</c> pins the one-row-per-run shape on both runners.</para>
    /// <para><b>The DISTINCT is not that guarantee</b>: with one row per run it removes nothing. It is kept as a free guard against two rows stamped
    /// with the exact same time (it cannot merge rows with different times, so it would not save the span from per-database rows).</para>
    /// </summary>
    public static string Runs(string logRelation, bool bounded = false) => $"""
SELECT DISTINCT collection_time
FROM {logRelation}
WHERE server_id = $1
AND   collection_time >= $2{Ceiling(bounded)}
AND   collector_name = '{CollectorName}'
AND   status = '{SuccessStatus}'
ORDER BY collection_time DESC
""";

    /// <summary>
    /// The per-minute buckets the Client Timeouts baseline is made of (#5736): one row per calendar minute in which the attention collector
    /// had a SUCCESSFUL run, with the sum of that minute's <c>count</c> rows (zero when the run counted nothing). Columns: minute (timestamp),
    /// attentions (bigint), oldest first. <c>$1</c> = server_id, <c>$2</c> = the inclusive naive-UTC start, <c>$3</c> = the exclusive end.
    /// It is the bucket definition of <c>tools/attention-rate-measure.sql</c> (Darling and Lite twins): a minute with no successful run is no
    /// bucket, a count row in such a minute is not read, the sample rows are never the total. #7061: the exact count of a minute is scaled by the
    /// share of that minute's sampled events that are not left out (under the duration floor, Azure Data Studio metadata work, or SSMS IntelliSense and VS Code mssql metadata applications), the proportion the
    /// alert applies to the judged rate, so the baseline is made of the same events; a minute with no samples keeps its full count. Both reads lean on the existing
    /// <c>(server_id, collection_time)</c> indexes of <c>collection_log</c> and <c>attention_events</c>, so the 7-day read is a bounded range scan
    /// of this one server.
    /// </summary>
    /// <remarks><paramref name="callsAt"/> is for a caller that read the calls first with <see cref="BaselineCalls"/> (#9881, PostgreSQL): the number of the
    /// first of the <see cref="CallArrayCount"/> array parameters that hold them. Left out, the statement reads the calls itself. The rows are the same either way.</remarks>
    public static string BaselineMinutes(string logRelation, string eventsRelation, int? callsAt = null) => $"""
WITH {CallsLead(eventsRelation, BaselineRange, callsAt)}{MatchedCte(eventsRelation, BaselineRange, callsAt)},
runs AS (
    SELECT date_trunc('minute', collection_time) AS minute
    FROM {logRelation}
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time < $3
    AND   collector_name = '{CollectorName}'
    AND   status = '{SuccessStatus}'
    GROUP BY date_trunc('minute', collection_time)
),
counts AS (
    SELECT
        date_trunc('minute', collection_time) AS minute,
        CAST(SUM(attention_count) AS BIGINT) AS attentions
    FROM {eventsRelation}
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time < $3
    AND   event_kind = '{KindCount}'
    GROUP BY date_trunc('minute', collection_time)
),
sample_minutes AS (
    SELECT
        date_trunc('minute', collection_time) AS minute,
        CAST(COUNT(*) AS BIGINT) AS kept,
        CAST(COUNT(*) FILTER (WHERE {ShortOwnPredicate} OR {AzdataOwnPredicate} OR {DbatoolsOwnPredicate} OR COALESCE({ExcludedAppPredicate}, FALSE)) AS BIGINT) AS left_out_alone
    FROM {eventsRelation}
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time < $3
    AND   event_kind = '{KindSample}'
    GROUP BY date_trunc('minute', collection_time)
),
matched_minutes AS (
    SELECT
        date_trunc('minute', collection_time) AS minute,
        CAST(COUNT(*) AS BIGINT) AS judged
    FROM matched
    WHERE NOT azdata_metadata
    AND   NOT dbatools_metadata
    AND   NOT COALESCE({ExcludedAppPredicate}, FALSE)
    GROUP BY date_trunc('minute', collection_time)
),
samples AS (
    SELECT
        g.minute,
        g.kept,
        CAST(g.left_out_alone - COALESCE(j.judged, 0) AS BIGINT) AS left_out
    FROM sample_minutes AS g
    LEFT JOIN matched_minutes AS j
        ON j.minute = g.minute
)
SELECT
    r.minute,
    CASE
        WHEN COALESCE(s.kept, 0) = 0 OR COALESCE(s.left_out, 0) = 0
            THEN CAST(COALESCE(c.attentions, 0) AS BIGINT)
        ELSE CAST(ROUND(CAST(COALESCE(c.attentions, 0) AS FLOAT8) * (1.0 - CAST(s.left_out AS FLOAT8) / CAST(s.kept AS FLOAT8))) AS BIGINT)
    END AS attentions
FROM runs AS r
LEFT JOIN counts AS c
    ON c.minute = r.minute
LEFT JOIN samples AS s
    ON s.minute = r.minute
ORDER BY r.minute
""";

    /// <summary>The count rows, summed per collection_time. Column 0 = collection_time, 1 = attentions (bigint).</summary>
    public static string Counts(string eventsRelation, bool bounded = false) => $"""
SELECT
    collection_time,
    CAST(SUM(attention_count) AS BIGINT) AS attentions
FROM {eventsRelation}
WHERE server_id = $1
AND   collection_time >= $2{Ceiling(bounded)}
AND   event_kind = '{KindCount}'
GROUP BY collection_time
ORDER BY collection_time DESC
""";

    /// <summary>The sample rows counted per (run, database, application). Columns: collection_time, database_name, client_app_name, samples (bigint),
    /// short_samples (bigint: the part under the duration floor, <see cref="ShortDurationPredicate"/>, that NO captured call explains, #9507), azdata_metadata_samples (bigint: the part
    /// that is Azure Data Studio metadata work and NOT already short, <see cref="AzdataMetadataPredicate"/> or the matched call's own statement), dbatools_metadata_samples (bigint: the part that is
    /// dbatools SMO metadata work and NOT already short, <see cref="DbatoolsSmoMetadataPredicate"/> or the matched call's own statement; the three parts never overlap), matched_samples (bigint, #9507: the part under the floor
    /// that matches a captured call, <see cref="MatchOn"/>, and is not Azure Data Studio or dbatools metadata work. Such a sample is judged, not left out: the attention event timed the cancel, the call is what ran).</summary>
    /// <remarks><paramref name="callsAt"/> is for a caller that read the calls first with <see cref="Calls"/> (#9881, PostgreSQL): the number of the
    /// first of the <see cref="CallArrayCount"/> array parameters that hold them. Left out, the statement reads the calls itself. The rows are the same either way.</remarks>
    public static string SampleGroups(string eventsRelation, bool bounded = false, int? callsAt = null) => $"""
WITH {CallsLead(eventsRelation, "collection_time >= $2" + Ceiling(bounded), callsAt)}{MatchedCte(eventsRelation, "collection_time >= $2" + Ceiling(bounded), callsAt)},
matched_groups AS (
    SELECT
        collection_time,
        database_name,
        client_app_name,
        CAST(COUNT(*) AS BIGINT) AS matched,
        CAST(COUNT(*) FILTER (WHERE azdata_metadata) AS BIGINT) AS azdata_metadata,
        CAST(COUNT(*) FILTER (WHERE dbatools_metadata) AS BIGINT) AS dbatools_metadata,
        CAST(COUNT(*) FILTER (WHERE NOT azdata_metadata AND NOT dbatools_metadata) AS BIGINT) AS judged
    FROM matched
    GROUP BY collection_time, database_name, client_app_name
),
sample_groups AS (
    SELECT
        collection_time,
        database_name,
        client_app_name,
        CAST(COUNT(*) AS BIGINT) AS samples,
        CAST(COUNT(*) FILTER (WHERE {ShortOwnPredicate}) AS BIGINT) AS short_alone,
        CAST(COUNT(*) FILTER (WHERE NOT {ShortOwnPredicate} AND {AzdataOwnPredicate}) AS BIGINT) AS azdata_alone,
        CAST(COUNT(*) FILTER (WHERE NOT {ShortOwnPredicate} AND {DbatoolsOwnPredicate}) AS BIGINT) AS dbatools_alone
    FROM {eventsRelation}
    WHERE server_id = $1
    AND   collection_time >= $2{Ceiling(bounded)}
    AND   event_kind = '{KindSample}'
    GROUP BY collection_time, database_name, client_app_name
)
SELECT
    g.collection_time,
    g.database_name,
    g.client_app_name,
    g.samples,
    CAST(g.short_alone - COALESCE(m.matched, 0) AS BIGINT) AS short_samples,
    CAST(g.azdata_alone + COALESCE(m.azdata_metadata, 0) AS BIGINT) AS azdata_metadata_samples,
    CAST(g.dbatools_alone + COALESCE(m.dbatools_metadata, 0) AS BIGINT) AS dbatools_metadata_samples,
    CAST(COALESCE(m.judged, 0) AS BIGINT) AS matched_samples
FROM sample_groups AS g
LEFT JOIN matched_groups AS m
    ON  m.collection_time = g.collection_time
    AND m.database_name IS NOT DISTINCT FROM g.database_name
    AND m.client_app_name IS NOT DISTINCT FROM g.client_app_name
ORDER BY g.collection_time DESC, g.database_name, g.client_app_name
""";

    /// <summary>The top <see cref="TopCount"/> statement fingerprints by sampled events (count ties break by name in binary order,
    /// <c>COLLATE "C"</c>, which PostgreSQL honors and DuckDB's default already is, so both stores list the same fifth entry), each with the newest non-NULL sample
    /// text, read for the five winners only (#9148: the text is never read for the rows that lose; ties on collection_time and event_time break on the unique id, so the pick is stable). Columns: fingerprint, samples (bigint), sample_text. Rows without a fingerprint (the statement filter withheld the
    /// text) are not a statement.</summary>
    public static string TopStatements(string eventsRelation, bool bounded = false) => $"""
WITH winners AS (
    SELECT
        statement_fingerprint AS fingerprint,
        CAST(COUNT(*) AS BIGINT) AS samples
    FROM {eventsRelation}
    WHERE server_id = $1
    AND   {SpanFloor(bounded)}{Ceiling(bounded)}
    AND   event_kind = '{KindSample}'
    AND   statement_fingerprint IS NOT NULL
    GROUP BY statement_fingerprint
    ORDER BY COUNT(*) DESC, statement_fingerprint COLLATE "C"
    LIMIT {TopCount}
)
SELECT
    w.fingerprint AS statement_fingerprint,
    w.samples,
    x.statement_text AS sample_text
FROM winners AS w
LEFT JOIN LATERAL (
    SELECT statement_text
    FROM {eventsRelation}
    WHERE server_id = $1
    AND   {SpanFloor(bounded)}{Ceiling(bounded)}
    AND   event_kind = '{KindSample}'
    AND   statement_fingerprint = w.fingerprint
    AND   statement_text IS NOT NULL
    ORDER BY collection_time DESC, event_time DESC NULLS LAST, attention_event_id DESC
    LIMIT 1
) AS x ON TRUE
ORDER BY w.samples DESC, w.fingerprint COLLATE "C"
""";

    /// <summary>The top <see cref="TopCount"/> values of <paramref name="column"/> (database_name or client_app_name) by
    /// sampled events. Columns: name, samples (bigint). NULL names are not listed.</summary>
    public static string TopBy(string eventsRelation, string column, bool bounded = false) => $"""
SELECT
    {column},
    CAST(COUNT(*) AS BIGINT) AS samples
FROM {eventsRelation}
WHERE server_id = $1
AND   {SpanFloor(bounded)}{Ceiling(bounded)}
AND   event_kind = '{KindSample}'
AND   {column} IS NOT NULL
GROUP BY {column}
ORDER BY COUNT(*) DESC, {column} COLLATE "C"
LIMIT {TopCount}
""";

    /// <summary>
    /// The newest kept ring-buffer events, for the read surface only (get_client_timeouts): one row per <c>sample</c>, newest first.
    /// The listed metadata flags come from ALL the calls a sample matches, with <c>bool_or</c>, the rule <see cref="MatchedCte"/> uses for the
    /// groups, so a sample that matches a metadata call and an ordinary one is listed as metadata, the way the groups count it (#9507). The
    /// aggregate returns one row for a sample that matches nothing, with NULL flags, which is what <c>matched_call</c> tests.
    /// Columns: event_time, collection_time, database_name, client_app_name, client_hostname, session_id, interrupt_duration_us,
    /// statement_fingerprint, statement_text, below_duration_floor (boolean), azdata_metadata (boolean), dbatools_metadata (boolean). Always bounded: <c>$2</c> is the EXCLUSIVE span boundary and <c>$3</c> the inclusive row ceiling (see <see cref="SpanFloor"/>). Newest first, ties broken by the table's unique id so a page edge is stable. <paramref name="limit"/> is a validated integer, never caller text.
    /// </summary>
    /// <remarks><paramref name="callsAt"/> is for a caller that read the calls first with <see cref="Calls"/> (#9881, PostgreSQL): the number of the
    /// first of the <see cref="CallArrayCount"/> array parameters that hold them. Left out, the statement reads the calls itself. The rows are the same either way.</remarks>
    public static string Samples(string eventsRelation, int limit, int? callsAt = null) => $"""
WITH {CallsCte(eventsRelation, "collection_time >= $2" + Ceiling(true), callsAt)},
newest AS (
    SELECT
        attention_event_id,
        event_time,
        collection_time,
        database_name,
        client_app_name,
        client_hostname,
        session_id,
        interrupt_duration_us,
        statement_fingerprint,
        statement_text,
        COALESCE({ShortDurationPredicate}, FALSE) AS short_duration,
        COALESCE({AzdataMetadataPredicate}, FALSE) AS azdata_own,
        COALESCE({DbatoolsSmoMetadataPredicate}, FALSE) AS dbatools_own
    FROM {eventsRelation}
    WHERE server_id = $1
    AND   collection_time > $2
    AND   collection_time <= $3
    AND   event_kind = '{KindSample}'
    ORDER BY collection_time DESC, event_time DESC NULLS LAST, attention_event_id DESC
    LIMIT {limit}
)
SELECT
    n.event_time,
    n.collection_time,
    n.database_name,
    n.client_app_name,
    n.client_hostname,
    n.session_id,
    n.interrupt_duration_us,
    n.statement_fingerprint,
    n.statement_text,
    (n.short_duration AND m.azdata_metadata IS NULL) AS below_duration_floor,
    (n.azdata_own OR COALESCE(m.azdata_metadata, FALSE)) AS azdata_metadata,
    (n.dbatools_own OR COALESCE(m.dbatools_metadata, FALSE)) AS dbatools_metadata,
    (m.azdata_metadata IS NOT NULL) AS matched_call
FROM newest AS n
LEFT JOIN LATERAL (
    SELECT
        bool_or(c.azdata_metadata) AS azdata_metadata,
        bool_or(c.dbatools_metadata) AS dbatools_metadata
    FROM calls AS c
    WHERE n.short_duration
    AND   {MatchOn("c", "n")}
) AS m ON TRUE
ORDER BY n.collection_time DESC, n.event_time DESC NULLS LAST, n.attention_event_id DESC
""";

    /// <summary>
    /// The captured calls that match an under-floor sample in the span, for the read surface only (#9507): one row per call, newest first, with
    /// what the call itself says about the timeout. Columns: event_time, collection_time, database_name, client_app_name, client_hostname,
    /// session_id, interrupt_duration_us (the call's elapsed time), statement_text (the object name, then a colon and the statement for an RPC).
    /// A call that is Azure Data Studio or dbatools metadata work is left out, as its sample is not judged. Always bounded, the way <see cref="Samples"/>
    /// is: <c>$2</c> is the EXCLUSIVE span boundary and <c>$3</c> the inclusive ceiling.
    /// </summary>
    public static string MatchedCalls(string eventsRelation, int limit) => $"""
WITH {CallsCte(eventsRelation, "collection_time >= $2" + Ceiling(true))},
hit AS (
    SELECT DISTINCT c.id
    FROM (SELECT * FROM {eventsRelation}) AS s
    INNER JOIN calls AS c
        ON {MatchOn("c", "s")}
    WHERE s.server_id = $1
    AND   s.collection_time > $2
    AND   s.collection_time <= $3
    AND   s.event_kind = '{KindSample}'
    AND   s.interrupt_duration_us < {MinInterruptDurationUs}
    AND   NOT c.azdata_metadata
    AND   NOT c.dbatools_metadata
)
SELECT
    e.event_time,
    e.collection_time,
    e.database_name,
    e.client_app_name,
    e.client_hostname,
    e.session_id,
    e.interrupt_duration_us,
    e.statement_text
FROM (SELECT * FROM {eventsRelation}) AS e
INNER JOIN hit AS h
    ON h.id = e.attention_event_id
WHERE e.server_id = $1
AND   e.collection_time >= $2
AND   e.collection_time <= $3
AND   e.event_kind = '{KindAborted}'
ORDER BY e.collection_time DESC, e.event_time DESC NULLS LAST, e.attention_event_id DESC
LIMIT {limit}
""";

    /// <summary>
    /// The first half of <see cref="MatchedCalls"/> for a caller that read the calls first with <see cref="Calls"/> (#9881, PostgreSQL):
    /// the ids of the calls that match an under-floor sample in the span and are not Azure Data Studio or dbatools metadata work, one row
    /// per call. Column: id (bigint). <c>$2</c> is the EXCLUSIVE span boundary, <c>$3</c> the inclusive ceiling, and the
    /// <see cref="CallArrayCount"/> array parameters start at <paramref name="callsAt"/>. The caller hands the ids to
    /// <see cref="MatchedCallRows"/>. Two statements, because joined in one the planner must guess how many ids match: on a week with 70
    /// calls it expected one and read the window's calls again for each of the 68 it found.
    /// </summary>
    public static string MatchedCallIds(string eventsRelation, int callsAt) => $"""
SELECT DISTINCT c.id
FROM (SELECT * FROM {eventsRelation}) AS s
{JoinCalls("s", callsAt)}
WHERE s.server_id = $1
AND   s.collection_time > $2
AND   s.collection_time <= $3
AND   s.event_kind = '{KindSample}'
AND   s.interrupt_duration_us < {MinInterruptDurationUs}
AND   NOT c.azdata_metadata
AND   NOT c.dbatools_metadata
""";

    /// <summary>
    /// The second half of <see cref="MatchedCalls"/> (#9881, PostgreSQL): the calls whose id is in the bigint array parameter
    /// <paramref name="idsAt"/>, the ids <see cref="MatchedCallIds"/> gave, with the columns, order and <paramref name="limit"/> of
    /// <see cref="MatchedCalls"/>. <c>$2</c> and <c>$3</c> are the inclusive range of the calls, the one <see cref="Calls"/> reads.
    /// </summary>
    public static string MatchedCallRows(string eventsRelation, int limit, int idsAt) => $"""
SELECT
    e.event_time,
    e.collection_time,
    e.database_name,
    e.client_app_name,
    e.client_hostname,
    e.session_id,
    e.interrupt_duration_us,
    e.statement_text
FROM (SELECT * FROM {eventsRelation}) AS e
WHERE e.server_id = $1
AND   e.collection_time >= $2
AND   e.collection_time <= $3
AND   e.event_kind = '{KindAborted}'
AND   e.attention_event_id = ANY (${idsAt}::bigint[])
ORDER BY e.collection_time DESC, e.event_time DESC NULLS LAST, e.attention_event_id DESC
LIMIT {limit}
""";
}
