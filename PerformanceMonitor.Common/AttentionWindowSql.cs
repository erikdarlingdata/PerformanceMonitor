/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

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

    /// <summary>How many entries each top list holds.</summary>
    public const int TopCount = 5;

    /// <summary>The duration floor (#7061), in microseconds: a sampled attention that lasted under 1 ms is left out of the judged rate and the
    /// baseline. Such events have no statement text and last 20 to 40 microseconds: the SqlClient driver cancelling a command that has already
    /// finished, which no user saw. A real client timeout lasts about as long as the command timeout, often 30 s. A sample with NO recorded
    /// duration (NULL) is not under the floor and still counts, the way every sample did before the floor.</summary>
    public const long MinInterruptDurationUs = 1000;

    /// <summary>The client application Azure Data Studio connects under. Its SQL Tools Service names every connection <c>azdata</c>, the query
    /// editor's too (connection pooling is on by default, and with it the service adds no purpose suffix to the name), so the name alone cannot
    /// be left out without hiding a real timeout from the query editor (#7061). The metadata statements are told apart by shape, see
    /// <see cref="AzdataMetadataPredicate"/>.</summary>
    public const string AzdataClientApp = "azdata";

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
    public static string BaselineMinutes(string logRelation, string eventsRelation) => $"""
WITH runs AS (
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
samples AS (
    SELECT
        date_trunc('minute', collection_time) AS minute,
        CAST(COUNT(*) AS BIGINT) AS kept,
        CAST(COUNT(*) FILTER (WHERE COALESCE({ShortDurationPredicate}, FALSE) OR COALESCE({AzdataMetadataPredicate}, FALSE) OR COALESCE({ExcludedAppPredicate}, FALSE)) AS BIGINT) AS left_out
    FROM {eventsRelation}
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time < $3
    AND   event_kind = '{KindSample}'
    GROUP BY date_trunc('minute', collection_time)
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
    /// short_samples (bigint: the part under the duration floor, <see cref="ShortDurationPredicate"/>), azdata_metadata_samples (bigint: the part
    /// that is Azure Data Studio metadata work and NOT already short, <see cref="AzdataMetadataPredicate"/>; the two parts never overlap).</summary>
    public static string SampleGroups(string eventsRelation, bool bounded = false) => $"""
SELECT
    collection_time,
    database_name,
    client_app_name,
    CAST(COUNT(*) AS BIGINT) AS samples,
    CAST(COUNT(*) FILTER (WHERE {ShortDurationPredicate}) AS BIGINT) AS short_samples,
    CAST(COUNT(*) FILTER (WHERE NOT COALESCE({ShortDurationPredicate}, FALSE) AND COALESCE({AzdataMetadataPredicate}, FALSE)) AS BIGINT) AS azdata_metadata_samples
FROM {eventsRelation}
WHERE server_id = $1
AND   collection_time >= $2{Ceiling(bounded)}
AND   event_kind = '{KindSample}'
GROUP BY collection_time, database_name, client_app_name
ORDER BY collection_time DESC, database_name, client_app_name
""";

    /// <summary>The top <see cref="TopCount"/> statement fingerprints by sampled events (count ties break by name in binary order,
    /// <c>COLLATE "C"</c>, which PostgreSQL honors and DuckDB's default already is, so both stores list the same fifth entry), each with the newest non-NULL sample
    /// text (ties on collection_time and event_time break on the unique id, so the pick is stable). Columns: fingerprint, samples (bigint), sample_text. Rows without a fingerprint (the statement filter withheld the
    /// text) are not a statement.</summary>
    public static string TopStatements(string eventsRelation, bool bounded = false) => $"""
SELECT
    statement_fingerprint,
    CAST(COUNT(*) AS BIGINT) AS samples,
    (array_agg(statement_text ORDER BY collection_time DESC, event_time DESC NULLS LAST, attention_event_id DESC) FILTER (WHERE statement_text IS NOT NULL))[1] AS sample_text
FROM {eventsRelation}
WHERE server_id = $1
AND   {SpanFloor(bounded)}{Ceiling(bounded)}
AND   event_kind = '{KindSample}'
AND   statement_fingerprint IS NOT NULL
GROUP BY statement_fingerprint
ORDER BY COUNT(*) DESC, statement_fingerprint COLLATE "C"
LIMIT {TopCount}
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
    /// Columns: event_time, collection_time, database_name, client_app_name, client_hostname, session_id, interrupt_duration_us,
    /// statement_fingerprint, statement_text, below_duration_floor (boolean), azdata_metadata (boolean). Always bounded: <c>$2</c> is the EXCLUSIVE span boundary and <c>$3</c> the inclusive row ceiling (see <see cref="SpanFloor"/>). Newest first, ties broken by the table's unique id so a page edge is stable. <paramref name="limit"/> is a validated integer, never caller text.
    /// </summary>
    public static string Samples(string eventsRelation, int limit) => $"""
SELECT
    event_time,
    collection_time,
    database_name,
    client_app_name,
    client_hostname,
    session_id,
    interrupt_duration_us,
    statement_fingerprint,
    statement_text,
    COALESCE({ShortDurationPredicate}, FALSE) AS below_duration_floor,
    COALESCE({AzdataMetadataPredicate}, FALSE) AS azdata_metadata
FROM {eventsRelation}
WHERE server_id = $1
AND   collection_time > $2
AND   collection_time <= $3
AND   event_kind = '{KindSample}'
ORDER BY collection_time DESC, event_time DESC NULLS LAST, attention_event_id DESC
LIMIT {limit}
""";
}
