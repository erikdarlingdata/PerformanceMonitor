/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #7610: <c>find_queries_by_text</c>, which statements on one server contain a piece of text. An agent that holds part of the SQL
/// (a table name, a fragment of an ad-hoc statement) had no read that took text: the other query reads take a hash or an id. This one
/// searches two stores of statement text and hands back the keys the other reads take (<c>query_id</c> for
/// <c>get_query_store_query_history</c>, <c>query_hash</c> for <c>get_query_trend</c>).
///
/// <para><b>Two sources, each with its own rows and its own horizon.</b> Query Store text (<c>collect.query_store_text</c>, one row per
/// database and <c>query_id</c>) and the plan-cache text of the raw tier (<c>query_stats</c>, whose text sits in <c>query_text_dim</c>
/// behind a digest since #1767; one row per <c>query_hash</c>). <c>query_snapshots</c> is not a third source: it carries a
/// <c>query_hash</c> but one row per in-flight session per sample, and its retention is whatever the operator set for that collector,
/// so no single horizon can be stated for an empty answer.</para>
///
/// <para><b>A time budget, because the text is not indexed by default.</b> Without an index a rare pattern reads every row in scope. On the
/// largest server of a 6.1 million row store (238,395 rows) that took 24.9 s cold and 1.9 s warm, and one server plus one database took
/// 0.69 s. Each source's statement runs in its own transaction under <c>SET LOCAL statement_timeout</c> (<see cref="BudgetMilliseconds"/>),
/// so the read stays under about ten seconds; a source that runs out is reported <c>stopped</c> and the other source is unaffected.</para>
///
/// <para><b>#8570: the Query Store source on an index.</b> The opt-in trigram index on <c>query_store_text (server_id, query_sql_text)</c>
/// (<see cref="QueryStoreTextTrigramIndex"/>, option <c>queryStoreTextTrigramIndexEnabled</c>) serves <see cref="QueryStoreSql"/> as it is
/// written: the planner reads the server's matches through the index, with the server applied inside it beside the pattern's trigrams, so
/// only that server's candidates are rechecked (a no-match search of one server of a seeded 1,000,000 row store read 340 buffers in 0.7 ms,
/// against 1,301 buffers in 34 ms through the primary key), and keeps the primary-key walk for a pattern most of the server's texts
/// contain, which stops at the row cap. The statement, its order, its cap and its excerpt are the same with or
/// without the index, so the rows are too; an index that is building or INVALID is never planned. The budget stays, for a store whose
/// index is off or not yet built.</para>
///
/// <para><b>#7674: the all-servers scope, only on an index.</b> A search that names no server reads the plan-cache text of every enabled server, through
/// the opt-in trigram index on <c>query_text_dim.query_text</c> (<see cref="QueryTextTrigramIndex"/>, option <c>queryTextTrigramIndexEnabled</c>).
/// It is offered only when the index is <c>Ready</c> (it exists and is valid): in every other state the search is refused with a sentence that names the option
/// and the state, because without the index the text match would read the whole text table and run out its budget. The index serves the text match.
/// The all-servers scope does not read Query Store text, which is searched one server at a time. With no <c>server_name</c> the front door
/// routes this tool as a fan-out (the default for a tool it does not list), so each store answers with its own index state. The one-server scope answers with the index off and reads through it when it is <c>Ready</c> (see #8570 below).</para>
///
/// <para><b>#7849: the sample side is a capped probe, and an opt-in index makes it an index read.</b> Once the matching texts are known, the samples of
/// every enabled server in the raw window have to be read to find where those texts ran, and no index a migration ships has <c>query_text_digest</c>
/// as a key. The matched digests are read (<see cref="MatchedDigestsSql"/>, at most <see cref="ProbeDigestCap"/> + 1 of them) and, when they fit
/// under the cap, bound to the sample read as an array (<see cref="PlanCacheAllServersProbeSql"/>, <c>= ANY($1::bytea[])</c>): the planner then has
/// the digests in hand, takes any index on the column on the uncompressed chunks and applies the digest min/max metadata per batch on the compressed
/// ones. The sample index the same option builds on <c>query_stats (query_text_digest, collection_time DESC)</c>
/// (<see cref="QueryTextTrigramIndex.SampleIndexName"/>) is what the probe reaches on the uncompressed chunks; without it the probe is a filtered scan
/// of the window, so the search is served as soon as the trigram index is valid and gets faster when the sample index is.</para>
///
/// <para><b>#9605: the all-servers search reads the newest samples first, and every statement of it is bounded by a row count.</b> The first
/// statement is a lookup span (<see cref="PlanCacheAllServersLookupSpanSql"/>): the newest <see cref="AllServersSpanRows"/> samples, each sighting's
/// text tested by its digest, newest first. A pattern many texts contain fills the page there, and a window that holds fewer samples than that is
/// read whole by it: either way the search ends without reading the matched digests at all, which on a large store is the costly read for a
/// common pattern (the trigram index hands back every candidate text before the cap can stop it). A page still short, with samples left to
/// read, reads the matched digests next, inside a savepoint and under a timeout of its own
/// (<see cref="AllServersMatchShare"/>). At most <see cref="ProbeDigestCap"/> of them: the probe reads the rest of the window by them, in the fixed
/// spans (<see cref="RawTierSpans"/>). More, or a match read its timeout cut (the savepoint is rolled back and the rows found are kept): the search
/// goes on in lookup spans, each sized by a row count and by nothing else (<see cref="AllServersSpanRowParts"/>), so no statement reads a whole
/// span of a large store or matches the whole text table, and a small store is read in as few statements as its samples fill. The spans
/// partition the window newest first and a server, query_hash and database an earlier statement found is
/// not listed again, so a search that finishes lists what the whole-window join (<see cref="PlanCacheAllServersSql"/>) lists, in its order, and a
/// budget that runs out leaves the rows of the part searched. The reply's <c>plan_cache</c> source says which read served it (<c>sample_read</c>:
/// <c>lookup</c>, <c>probe</c> or <c>join</c>, see <see cref="SampleReadLookup"/>) and, for the probe, how many texts matched
/// (<c>matched_texts</c>).</para>
///
/// <para><b>#8570: one server's plan-cache text through the same probe.</b> When the plan-cache text index is <c>Ready</c>, the one-server search reads the
/// matched digests (<see cref="MatchedDigestsSql"/>, under its own timeout, <see cref="MatchBudgetMilliseconds"/>) on a connection of its own while the
/// Query Store source runs, and, when they fit under <see cref="ProbeDigestCap"/>, reads each span's samples of the server by them
/// (<see cref="PlanCacheProbeSql"/>), so no span tests a text. Past the cap, or when the match read runs out of its timeout or fails, each span tests its
/// distinct texts against the pattern (<see cref="PlanCacheSql"/>), which is also the read with the index off. Measured on a seeded store (350,000 texts, 5.7 million
/// samples, three of five chunks compressed), one server: 0.30 s cold and about 0.2 s warm through the probe, against 0.73 s cold and 0.37 to 0.53 s warm
/// through the span read, of which the match read took 18 to 42 ms. With the index ready the reply's <c>plan_cache</c> source says which read served it
/// (<c>sample_read</c>: <c>probe</c> or <c>plain</c>) and, under the cap, how many texts matched (<c>matched_texts</c>); with it off the reply carries neither.</para>
///
/// <para><b>The judge runs on the whole text, then the excerpt is cut (#5320).</b> The SQL hands back the whole stored text of the at most
/// <c>limit + 1</c> matched rows, and the C# side judges it with <see cref="SensitiveStatements.Text"/> before it cuts anything, the same
/// order <c>McpHelpers.TruncateStatement</c> and <c>StatementPreview</c> keep. A cut taken in SQL would hand the judge only the piece: a rule
/// that needs context (a <c>bcp</c> name more than 100 characters before its <c>-P</c> value) loses its anchor in the piece, and the
/// host's <c>SensitiveStatementOutputFilter</c> judges only what the reply carries. A statement the judge names is listed with the withheld
/// marker in place of the excerpt, with no length and no excerpt of its text, like any other withheld statement. The match itself
/// (<c>ILIKE</c>) is a match, not the judge: it only chooses which rows are read.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpFindQueriesByTextTools
{
    internal const string ToolName = "find_queries_by_text";

    public const int DefaultLimit = 20;
    public const int MaxLimit = 50;
    public const int MinPatternLength = 3;

    /// <summary>The time each source's statement may run: 4 s, so two sources stay under 10 s.</summary>
    public const int BudgetMilliseconds = 4000;

    /// <summary>#8570: the time the one-server search's match read may take before the search falls back to the span read. #9150: the whole
    /// <see cref="BudgetMilliseconds"/>, as the all-servers search's match read has. At 1 s a match read that was only slow (a cold index) gave up on
    /// the digests and sent a rare pattern to the span read, which tests every text the server has and, on a server with many, ends at its budget
    /// before the newest span is done. The match read runs while the Query Store source does, on a connection of its own, so its time is not
    /// taken from the plan-cache source's budget, and the search still ends within one match read and one span read.</summary>
    public const int MatchBudgetMilliseconds = BudgetMilliseconds;

    /// <summary>Characters of text returned around the first match.</summary>
    public const int ExcerptLength = 300;

    /// <summary>Characters of the excerpt that come before the first match.</summary>
    public const int ExcerptLead = 100;

    /// <summary>
    /// Test seam: the statement time each source may run, and a delay a test can force. The delay is its own <c>SELECT pg_sleep(...)</c>,
    /// run in the same transaction after <c>SET LOCAL statement_timeout</c> and before the search, so the timeout cancels it and the source
    /// stops deterministically. The tool itself always uses <see cref="Default"/>.
    /// </summary>
    internal sealed record SourceBudget(int Milliseconds, double TestDelaySeconds = 0)
    {
        public static SourceBudget Default { get; } = new(BudgetMilliseconds);

        /// <summary>#7849 test seam: the clock the budget is spent against, as the time elapsed since the source's first statement could start. Null is
        /// a real stopwatch. A test hands in a clock it sets by hand, so the remainder the sample statement runs under is a fixed number, not a race.</summary>
        public Func<TimeSpan>? ElapsedClock { get; init; }

        /// <summary>#7849 test seam: called on the source's connection, in its transaction, just before the statement that follows a read in the same
        /// budget (the all-servers sample statement), once its <c>statement_timeout</c> is the remainder, so a test can read the setting back. It runs
        /// after <see cref="StatementBound"/>, so a test that models a slow statement can hold this one by the span it binds.</summary>
        public Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task>? BeforeStatement { get; init; }

        /// <summary>#8408 test seam: called with the step and the statement's bound parameters just before it runs, so a test can read what
        /// one step carries into the next (the one-server search binds the digests it has judged so far and their verdicts).</summary>
        public Action<int, NpgsqlParameterCollection>? StatementBound { get; init; }

        /// <summary>#9219: how many sample rows an all-servers lookup span reads for each of its parts (one for the first two spans, two for the
        /// third, <see cref="AllServersSpanRowParts"/> after it); <see cref="AllServersSpanRows"/> unless a test sets it, so a small store can
        /// model a store of more samples than one statement reads.</summary>
        public int SpanRows { get; init; } = AllServersSpanRows;

        /// <summary>#9605: the all-servers match read's own timeout, in milliseconds; null is the source's budget divided by
        /// <see cref="AllServersMatchShare"/>. A test sets it small and adds <see cref="MatchTestDelaySeconds"/>, so the timeout cancels the read
        /// at a fixed point.</summary>
        public int? AllServersMatchMilliseconds { get; init; }

        /// <summary>#8570: the one-server match read's own timeout; <see cref="MatchBudgetMilliseconds"/> unless a test sets it.</summary>
        public int MatchMilliseconds { get; init; } = MatchBudgetMilliseconds;

        /// <summary>#8570 test seam: a <c>SELECT pg_sleep(...)</c> run in the match read's transaction (the all-servers search: in its savepoint),
        /// under its timeout, so a test makes the match read run out deterministically and the search fall back to the span read.</summary>
        public double MatchTestDelaySeconds { get; init; }

        /// <summary>#8570 test seam: called once the match read's timeout is set, just before its statements run, so a test can hold the Query Store
        /// source until the match read has started and so show the two run together.</summary>
        public Action? MatchStarting { get; init; }
    }

    /// <summary>Query Store text. $1 server_id, $2 database or null, $3 the escaped pattern, $4 row cap + 1. The whole text comes back so the judge
    /// sees all of it before the excerpt is cut (see the class summary); the cap keeps that to at most $4 texts.
    /// Primary-key order, so a common pattern stops at the cap instead of reading the scope.
    /// <para>The match is the inner statement, on the text table alone, and its cap is applied there. <c>last_seen</c> is the liveness stamp
    /// the fetch probe keeps in <c>query_store_text_seen</c> (<see cref="QueryStoreTextSeen"/>), read by primary key for the at most $4 rows
    /// of the page, or the text row's own stamp where that is the newer one or the text has no row there.</para></summary>
    internal const string QueryStoreSql = @"
SELECT m.database_name,
       m.query_id,
       m.query_hash,
       GREATEST(s.last_seen, m.last_seen) AS last_seen,
       m.query_sql_text
FROM
(
    SELECT t.server_id,
           t.database_name,
           t.query_id,
           t.query_hash,
           t.last_seen,
           t.query_sql_text
    FROM query_store_text AS t
    WHERE t.server_id = $1
    AND   ($2::text IS NULL OR t.database_name = $2)
    AND   t.query_sql_text ILIKE '%' || $3 || '%'
    ORDER BY t.database_name, t.query_id
    LIMIT $4
) AS m
LEFT JOIN query_store_text_seen AS s
  ON  s.server_id = m.server_id
  AND s.database_name = m.database_name
  AND s.query_id = m.query_id
ORDER BY m.database_name, m.query_id";

    /// <summary>Plan-cache text of one span of the raw tier. $1 server_id, $2 database or null, $3 the escaped pattern, $4 the rows still wanted
    /// (the page cap + 1 less the rows earlier spans found), $5 and $6 the span's floor and ceiling (naive UTC, floor inclusive), $7 and $8 the
    /// query_hash and database_name of every row an earlier span already found, so a statement seen again here is not listed twice. The
    /// one-server search runs this once per span of the raw window, newest span first (<see cref="RawTierSpans"/>), inside the source's one
    /// transaction and one budget: a span is one bounded statement, so a budget that runs out between spans leaves the rows the earlier spans
    /// found. The server's distinct (hash, database, digest) in the span come first; one row per query_hash and database, the newest sighting
    /// of each (#7905: a statement that runs in several databases lists once per database, each with the <c>database_name</c> that
    /// <c>get_query_trend</c> takes), and a statement's first span, the newest it ran in, carries its newest sighting. The match and the page
    /// carry the digest only: the whole text (<c>query_text_dim.query_text</c> has no size cap) is read by primary key for the at most $4 rows
    /// of the page, so a common pattern never sorts every matching text.
    /// <para>Each text is judged once per search, whichever spans it runs in, and the search carries the verdicts itself: the transaction is a
    /// pure read, with no temp table, because the <c>mcp</c> role the read runs as has CONNECT on the managed store's database and nothing
    /// else at the database level, TEMPORARY included. $9 and $10 are the digests the earlier spans judged and their verdicts (<c>known</c>),
    /// bound the way $7 and $8 carry the found keys. The digests this span sees for the first time (<c>judged</c>) are tested against the
    /// pattern, and the span's hits take their verdict from this span's judgement or from <c>known</c>. The statement hands back two kinds of
    /// row, told apart by the first column: the page's <c>hit</c> rows first, in page order, then one <c>judged</c> row per digest this span
    /// judged, with its verdict, which the search appends to what it binds to the next span. A statement sampled in every span, which is most
    /// procedure and parameterized text, therefore has its text tested in its newest span only, and a span whose distinct texts number in the
    /// tens of thousands, where the planner tests the pattern by scanning every text rather than reading each by key, pays that scan once per
    /// search instead of once per span. The carried arrays grow to the server's distinct digests in the window, 32 bytes and a boolean each.
    /// Measured: see <see cref="RawTierSpanParts"/>. A digest with no text row is judged unmatched.</para>
    /// <para>#7674: the match is <c>(d.query_text || '') ILIKE</c>, not <c>d.query_text ILIKE</c>, on purpose. With the opt-in trigram index on
    /// <c>query_text_dim.query_text</c> the planner would otherwise run a bitmap scan of that index over the whole table and join it to this server's
    /// digests, which the issue measured at 1.7 to 2.0 s cold for one server and one database against 0.2 to 0.7 s without the index. The concatenation is
    /// not the indexed column, so no index can serve it. With the index <c>Ready</c> the search reads the matched digests through it first and takes
    /// <see cref="PlanCacheProbeSql"/> (#8570); this statement is the read with the index off and the one a pattern past <see cref="ProbeDigestCap"/>
    /// matches, or a match read that runs out, falls back to. The all-servers scope matches the
    /// bare column (<see cref="MatchedDigestsSql"/>), so the index serves it.</para></summary>
    internal const string PlanCacheSql = @"
WITH seen AS MATERIALIZED (
    SELECT query_hash, database_name, query_text_digest, MAX(collection_time) AS last_collected
    FROM query_stats
    WHERE server_id = $1
    AND   collection_time >= $5
    AND   collection_time < $6
    AND   query_text_digest IS NOT NULL
    AND   ($2::text IS NULL OR database_name = $2)
    GROUP BY query_hash, database_name, query_text_digest
),
known AS MATERIALIZED (
    SELECT digest, matched
    FROM unnest($9::bytea[], $10::boolean[]) AS k(digest, matched)
),
judged AS MATERIALIZED (
    SELECT f.digest, COALESCE((d.query_text || '') ILIKE '%' || $3 || '%', FALSE) AS matched
    FROM (SELECT DISTINCT s.query_text_digest AS digest
          FROM seen AS s
          WHERE NOT EXISTS (SELECT 1 FROM known AS k WHERE k.digest = s.query_text_digest)) AS f
    LEFT JOIN query_text_dim AS d ON d.digest = f.digest
),
hits AS (
    SELECT DISTINCT ON (s.query_hash, s.database_name)
           s.query_hash, s.database_name, s.last_collected, s.query_text_digest
    FROM seen AS s
    WHERE (EXISTS (SELECT 1 FROM judged AS j WHERE j.digest = s.query_text_digest AND j.matched)
           OR EXISTS (SELECT 1 FROM known AS k WHERE k.digest = s.query_text_digest AND k.matched))
    AND   NOT EXISTS (SELECT 1
                      FROM unnest($7::text[], $8::text[]) AS k(query_hash, database_name)
                      WHERE k.query_hash IS NOT DISTINCT FROM s.query_hash
                      AND   k.database_name IS NOT DISTINCT FROM s.database_name)
    ORDER BY s.query_hash, s.database_name, s.last_collected DESC
),
page AS (
    SELECT query_hash, database_name, last_collected, query_text_digest
    FROM hits
    ORDER BY last_collected DESC, query_hash, database_name
    LIMIT $4
)
SELECT 'hit' AS kind, p.query_hash, p.database_name, p.last_collected, d.query_text, NULL::bytea AS digest, NULL::boolean AS matched
FROM page AS p
JOIN query_text_dim AS d ON d.digest = p.query_text_digest
UNION ALL
SELECT 'judged', NULL, NULL, NULL, NULL, j.digest, j.matched
FROM judged AS j
ORDER BY kind, last_collected DESC, query_hash, database_name";

    /// <summary>The first column of a <see cref="PlanCacheSql"/> row: <c>hit</c> is a row of the page, <c>judged</c> a digest this span judged
    /// and its verdict, carried to the spans after it.</summary>
    internal const string PlanCacheJudgedRowKind = "judged";

    /// <summary>#7674: the plan-cache text of every enabled server over the whole raw window in one statement: the texts the pattern matches
    /// (the bare column, so the trigram index on <c>query_text_dim.query_text</c> may serve the match), joined to every enabled server's samples.
    /// $1 the escaped pattern, $2 row cap + 1, $3 the raw-tier floor (naive UTC). One row per server, query_hash and database, the newest sighting of
    /// each (#7905), newest first. The page carries the digest and the whole text is read by primary key for the at most $2 rows of it, as in
    /// <see cref="PlanCacheSql"/>. The tool does not run this statement: it matches every text and reads every sample of the window, and a budget
    /// that cuts one statement leaves no rows. It is the reference the reads the tool runs (<see cref="PlanCacheAllServersLookupSpanSql"/>,
    /// <see cref="PlanCacheAllServersProbeSql"/>) are tested equal to.</summary>
    internal const string PlanCacheAllServersSql = @"
WITH matched AS MATERIALIZED (
    SELECT digest
    FROM query_text_dim
    WHERE query_text ILIKE '%' || $1 || '%'
),
hits AS (
    SELECT DISTINCT ON (s.server_id, s.query_hash, s.database_name)
           s.server_id, s.query_hash, s.database_name, s.collection_time AS last_collected, s.query_text_digest
    FROM query_stats AS s
    JOIN matched AS m ON m.digest = s.query_text_digest
    JOIN servers AS v ON v.server_id = s.server_id AND v.is_enabled
    WHERE s.collection_time >= $3
    ORDER BY s.server_id, s.query_hash, s.database_name, s.collection_time DESC
),
page AS (
    SELECT server_id, query_hash, database_name, last_collected, query_text_digest
    FROM hits
    ORDER BY last_collected DESC, server_id, query_hash, database_name
    LIMIT $2
)
SELECT p.server_id, p.query_hash, p.database_name, p.last_collected, d.query_text
FROM page AS p
JOIN query_text_dim AS d ON d.digest = p.query_text_digest
ORDER BY p.last_collected DESC, p.server_id, p.query_hash, p.database_name";

    /// <summary>#9150: <see cref="PlanCacheAllServersSql"/> over one span of the raw window. $1 the escaped pattern, then $2 to $7 as in
    /// <see cref="PlanCacheAllServersProbeSql"/>: the rows still wanted, the span's floor and ceiling (naive UTC), and the server ids, query_hashes and
    /// databases of the rows earlier spans found, which this span does not list again. The tool does not run this statement (#9605): it repeats the
    /// whole text match and reads every sample of its span, which on a large store is more than the budget for one span. It is the reference a lookup
    /// span (<see cref="PlanCacheAllServersLookupSpanSql"/>) is tested equal to, span for span.</summary>
    internal const string PlanCacheAllServersSpanSql = @"
WITH matched AS MATERIALIZED (
    SELECT digest
    FROM query_text_dim
    WHERE query_text ILIKE '%' || $1 || '%'
),
hits AS (
    SELECT DISTINCT ON (s.server_id, s.query_hash, s.database_name)
           s.server_id, s.query_hash, s.database_name, s.collection_time AS last_collected, s.query_text_digest
    FROM query_stats AS s
    JOIN matched AS m ON m.digest = s.query_text_digest
    JOIN servers AS v ON v.server_id = s.server_id AND v.is_enabled
    WHERE s.collection_time >= $3
    AND   s.collection_time < $4
    ORDER BY s.server_id, s.query_hash, s.database_name, s.collection_time DESC
),
page AS (
    SELECT h.server_id, h.query_hash, h.database_name, h.last_collected, h.query_text_digest
    FROM hits AS h
    WHERE NOT EXISTS (
        SELECT 1
        FROM unnest($5::integer[], $6::text[], $7::text[]) AS k(server_id, query_hash, database_name)
        WHERE k.server_id = h.server_id
        AND   k.query_hash IS NOT DISTINCT FROM h.query_hash
        AND   k.database_name IS NOT DISTINCT FROM h.database_name
    )
    ORDER BY h.last_collected DESC, h.server_id, h.query_hash, h.database_name
    LIMIT $2
)
SELECT p.server_id, p.query_hash, p.database_name, p.last_collected, d.query_text
FROM page AS p
JOIN query_text_dim AS d ON d.digest = p.query_text_digest
ORDER BY p.last_collected DESC, p.server_id, p.query_hash, p.database_name";

    /// <summary>#9219: one lookup span of the all-servers search: <see cref="PlanCacheAllServersSpanSql"/>'s parameters ($1 to $7) and, with $8 (the
    /// most texts to find matching before the statement stops), its rows, but the pattern is tested only on the texts the span's own samples carry,
    /// newest sample first, and the statement stops as soon as it has found $8 matching ones. The digests are probed in the order the page is listed
    /// in (newest sighting, then server, query_hash, database), through a lateral <c>LIMIT 1</c> read by primary key that the planner cannot flatten
    /// into a join with the whole table, so <c>LIMIT $8</c> ends the probing in the plan, not in an estimate. A statement is one sighting (server,
    /// query_hash, database, digest), and a statement text can be found under several digests, so the matching ones are reduced to the newest per
    /// server, query_hash and database after the limit. The last column says the limit was reached: when the reduced page is still short of $2, the
    /// caller runs the span again with the same limit (sized to the rows still wanted), for the keys not yet found, until the page is full, the span
    /// is done or the budget ends. A lookup is one random read of the text table per digest, so the caller bounds the samples a span holds by a row
    /// count (<see cref="AllServersSpanRows"/>).</summary>
    internal const string PlanCacheAllServersLookupSpanSql = @"
WITH combos AS (
    SELECT s.server_id, s.query_hash, s.database_name, s.query_text_digest, max(s.collection_time) AS last_collected
    FROM query_stats AS s
    JOIN servers AS v ON v.server_id = s.server_id AND v.is_enabled
    WHERE s.collection_time >= $3
    AND   s.collection_time < $4
    AND   s.query_text_digest IS NOT NULL
    AND   NOT EXISTS (
        SELECT 1
        FROM unnest($5::integer[], $6::text[], $7::text[]) AS k(server_id, query_hash, database_name)
        WHERE k.server_id = s.server_id
        AND   k.query_hash IS NOT DISTINCT FROM s.query_hash
        AND   k.database_name IS NOT DISTINCT FROM s.database_name
    )
    GROUP BY s.server_id, s.query_hash, s.database_name, s.query_text_digest
),
matched AS MATERIALIZED (
    SELECT c.server_id, c.query_hash, c.database_name, c.last_collected, c.query_text_digest
    FROM (
        SELECT cb.server_id, cb.query_hash, cb.database_name, cb.last_collected, cb.query_text_digest
        FROM combos AS cb
        ORDER BY cb.last_collected DESC, cb.server_id, cb.query_hash, cb.database_name, cb.query_text_digest
    ) AS c
    CROSS JOIN LATERAL (SELECT 1 AS hit
                        FROM query_text_dim AS d
                        WHERE d.digest = c.query_text_digest
                        AND   d.query_text ILIKE '%' || $1 || '%'
                        LIMIT 1) AS t
    LIMIT $8
),
newest AS (
    SELECT DISTINCT ON (m.server_id, m.query_hash, m.database_name)
           m.server_id, m.query_hash, m.database_name, m.last_collected, m.query_text_digest
    FROM matched AS m
    ORDER BY m.server_id, m.query_hash, m.database_name, m.last_collected DESC
),
page AS (
    SELECT n.server_id, n.query_hash, n.database_name, n.last_collected, n.query_text_digest
    FROM newest AS n
    ORDER BY n.last_collected DESC, n.server_id, n.query_hash, n.database_name
    LIMIT $2
)
SELECT p.server_id, p.query_hash, p.database_name, p.last_collected, d.query_text, (SELECT count(*) FROM matched) >= $8 AS capped
FROM page AS p
JOIN query_text_dim AS d ON d.digest = p.query_text_digest
ORDER BY p.last_collected DESC, p.server_id, p.query_hash, p.database_name";

    /// <summary>#9219: the sample rows a lookup span of the all-servers search reads for each of its parts. A span's width in time says nothing
    /// about its work: on a large store the newest hour held about 972,000 rows and 30,000 digests, each digest one cold random read of a text
    /// table of 1.5 million pages, more than the whole budget. A span ends at the sample this many rows, times its parts, back from its newest
    /// edge, or at the window's floor when fewer are left, so the work of each statement is bounded by a count, the search widens while time
    /// remains, and a store that holds less than one allotment is read whole by the first statement.</summary>
    internal const int AllServersSpanRows = 20000;

    /// <summary>#9605: the most parts a lookup span reads <see cref="AllServersSpanRows"/> rows for. The spans read 1, 1 and 2 parts and then this
    /// many each, so no statement reads more than 80,000 samples (and those that share the oldest one's collection time) whatever the store
    /// holds, and the row count alone sizes a span.</summary>
    internal const int AllServersSpanRowParts = 4;

    /// <summary>#9605: the all-servers match read's share of the source's budget: its timeout is the budget divided by this (2 s of 4 s), or what
    /// is left of the budget when that is less. The read is fast when few texts match, which is when the probe it enables is worth having, and
    /// slow on a cold index for a pattern many texts contain, where the lookup spans answer without it; the rest of the budget is the spans'.</summary>
    internal const int AllServersMatchShare = 2;

    /// <summary>#9219: the number of matching sightings the lookup statement finds before it stops probing, as a multiple of the rows still wanted. A
    /// statement text can be found under several digests of one server, query_hash and database, which count once, so the limit is not the page size.</summary>
    internal const int AllServersLookupMatchFactor = 8;

    /// <summary>
    /// #7849: how many matched texts the all-servers sample read takes as an array (<see cref="PlanCacheAllServersProbeSql"/>); past it the search
    /// goes on in lookup spans (<see cref="PlanCacheAllServersLookupSpanSql"/>). Measured on a seeded large store (40 servers, 3.0 M sample rows over
    /// four days, 300,000 texts, TimescaleDB with the older half of the window compressed): the array probe ran in about 0.3 s with 100 and with 1,000
    /// digests, 1.25 s with 10,000, and with 100,000 digests (a pattern a third of the texts contain) the backend ran out of memory and was killed, so
    /// an uncapped probe is not safe. Under the cap the probe is the faster read with or without the sample index: a pattern matching one text took
    /// 0.33 s through the probe with no index on the digest, and 0.11 s with the sample index.
    /// </summary>
    public const int ProbeDigestCap = 1000;

    /// <summary>
    /// #9219: how many equal parts the raw window is measured in for the spans of a plan-cache search (<see cref="RawTierSpans"/>): the newest span
    /// is one part (an hour of a four-day tier) and the spans after it widen, so the first statement of a search is small whatever the store
    /// holds. Each span is one statement (<see cref="PlanCacheSql"/> for one server) under what is left of the source's budget, run newest span
    /// first, so a budget that runs out leaves the rows the earlier spans found and the reply says how much of the window they cover. Under
    /// sixteen equal six-hour spans the first statement read a sixteenth of the raw tier, and on a large store that statement alone could miss
    /// the whole budget (the search then stopped with nothing searched, and which searches did so changed with the limit, because the limit
    /// changes the plan). Each text is judged once per search whichever spans it runs in (see <see cref="PlanCacheSql"/>), which keeps the
    /// statements of a finished one-server search at about the cost of one. The spans stop widening at <see cref="RawTierSpanMaxParts"/> parts,
    /// so the whole window is twelve spans.
    /// </summary>
    public const int RawTierSpanParts = 96;

    /// <summary>#9219: the widest a span grows, in <see cref="RawTierSpanParts"/> (twelve hours of a four-day tier). The spans run 1, 1, 2, 4, 8 parts
    /// and then this many each, the oldest cut short at the floor.</summary>
    public const int RawTierSpanMaxParts = 12;

    /// <summary>
    /// The spans of the raw window <paramref name="floor"/> to <paramref name="now"/>, newest first: each one's floor the next one's ceiling, the
    /// oldest starting exactly at the floor and the newest open a day past <paramref name="now"/>, so a sample stamped slightly ahead of this
    /// host's clock is still in the first span. Their lengths are 1, 1, 2, 4, 8 and then <see cref="RawTierSpanMaxParts"/> parts of
    /// <see cref="RawTierSpanParts"/> (#9219), so the first statement reads the newest slice only and the search widens while time remains.
    /// </summary>
    internal static IReadOnlyList<(DateTime Floor, DateTime Ceiling)> RawTierSpans(DateTime floor, DateTime now)
    {
        var part = (now - floor) / RawTierSpanParts;
        var spans = new List<(DateTime Floor, DateTime Ceiling)>();
        var ceiling = now.AddDays(1);
        var reached = now;
        var width = 1;
        var next = 1;
        while (reached > floor)
        {
            /* The oldest span takes whatever is left, including the few ticks the division by the parts dropped. */
            var spanFloor = reached - floor < part * (width + 1) ? floor : reached - part * width;

            spans.Add((spanFloor, ceiling));
            ceiling = spanFloor;
            reached = spanFloor;

            /* 1, 1, 2, 4, 8, 12, 12 ...: the width doubles from the second span on, up to the cap. */
            (width, next) = (Math.Min(next, RawTierSpanMaxParts), next * 2);
        }

        return spans;
    }

    /// <summary>How much of the raw window the spans a search finished cover, for the stopped message: "6 hours", "1 day", "1.5 days". The
    /// number is rendered first and the unit's word follows the rendered number, so the newest day of the tier, a hair over 24 hours once the
    /// Query Store source's time is in the window, reads "1 day".</summary>
    internal static string DescribeSearched(int completedSpans, DateTime floor, DateTime now)
    {
        return DescribeSearchedFrom(RawTierSpans(floor, now)[completedSpans - 1].Floor, now);
    }

    /// <summary>#9219: <see cref="DescribeSearched"/> for a search whose spans were not the fixed ones (the all-servers search ends its lookup
    /// spans by a row count), given the floor of the part it covered.</summary>
    internal static string DescribeSearchedFrom(DateTime coveredFloor, DateTime now)
    {
        var searched = now - coveredFloor;
        var (value, unit) = searched.TotalHours < 24 ? (searched.TotalHours, "hour") : (searched.TotalDays, "day");
        var rendered = value.ToString("0.#", CultureInfo.InvariantCulture);
        return rendered + " " + unit + (rendered == "1" ? string.Empty : "s");
    }

    /// <summary>#9150: what a plan-cache source's reply says it searched: the servers, the raw window the search covers (<c>window_from</c> to
    /// <c>window_to</c>) and the part of it the page is the exact answer for (<c>searched_from</c> to <c>searched_to</c>), with <c>complete</c> saying
    /// whether the page is the whole-window answer. A search that finished is: it read every span, or it stopped reading once the page was full,
    /// and spans run newest first, so the older spans it never read could not have changed the page (<c>truncated</c> says more matches exist). A
    /// stopped search that read in spans (<paramref name="completedSpans"/> of them, newest first) is the exact answer for the newest part of the
    /// window only, and one that stopped before its first span, or before the matching texts were known, for none of it: both times are null.
    /// <paramref name="completedSpans"/> is null for a search that was not in spans. #9219: the all-servers search cuts its lookup spans by a row
    /// count, so it hands in the floor of what it finished (<paramref name="coveredIsGiven"/>, null when it finished nothing).</summary>
    internal static object SearchedWindow(
        object servers, int? serverCount, string status, int? completedSpans, DateTime floor, DateTime now, bool coveredIsGiven = false, DateTime? coveredFloor = null)
    {
        var stopped = string.Equals(status, "stopped", StringComparison.Ordinal);
        DateTime? searchedFrom = !stopped
            ? floor
            : coveredIsGiven ? coveredFloor
            : completedSpans is > 0 ? RawTierSpans(floor, now)[completedSpans.Value - 1].Floor : null;
        return new
        {
            servers,
            server_count = serverCount,
            window_from = Utc(floor),
            window_to = Utc(now),
            searched_from = searchedFrom is { } from ? Utc(from) : null,
            searched_to = searchedFrom is null ? null : Utc(now),
            complete = !stopped,
        };
    }

    /// <summary>#7849: the matched digests, read first so the sample read can take them as an array. $1 the escaped pattern, $2 the cap + 1: a result
    /// that fills the cap + 1 rows says the pattern matched more texts than the probe takes, and the search goes on in lookup spans instead.</summary>
    internal const string MatchedDigestsSql = @"
SELECT digest
FROM query_text_dim
WHERE query_text ILIKE '%' || $1 || '%'
LIMIT $2";

    /// <summary>#7849: the sample read for a pattern that matched at most <see cref="ProbeDigestCap"/> texts, one span of the raw window (see
    /// <see cref="RawTierSpans"/>), run newest span first. $1 the matched digests, $2 the rows still wanted (row cap + 1 less the rows earlier spans
    /// found), $3 and $4 the span's floor and ceiling (naive UTC), and $5, $6 and $7 the server ids, query_hashes and databases of the rows earlier
    /// spans found, which this span does not list again. With the digests bound as an array the planner reads <c>query_stats</c> by them: through the
    /// sample index (<see cref="QueryTextTrigramIndex.SampleIndexName"/>) on an uncompressed chunk when the option built it, by a filtered scan
    /// otherwise, and on a compressed chunk by the digest's per-batch metadata, and the span's bounds leave out the chunks outside it. The equality is
    /// strict, so a row with no digest is never a hit and the partial sample index serves it. One row per server, query_hash and database, the newest
    /// sighting of each, paged and read back as <see cref="PlanCacheAllServersSql"/> does: the spans partition the window and the newest span runs
    /// first, so the spans' rows together are the rows that read lists, in its order.</summary>
    internal const string PlanCacheAllServersProbeSql = @"
WITH hits AS (
    SELECT DISTINCT ON (s.server_id, s.query_hash, s.database_name)
           s.server_id, s.query_hash, s.database_name, s.collection_time AS last_collected, s.query_text_digest
    FROM query_stats AS s
    JOIN servers AS v ON v.server_id = s.server_id AND v.is_enabled
    WHERE s.query_text_digest = ANY($1::bytea[])
    AND   s.collection_time >= $3
    AND   s.collection_time < $4
    ORDER BY s.server_id, s.query_hash, s.database_name, s.collection_time DESC
),
page AS (
    SELECT h.server_id, h.query_hash, h.database_name, h.last_collected, h.query_text_digest
    FROM hits AS h
    WHERE NOT EXISTS (
        SELECT 1
        FROM unnest($5::integer[], $6::text[], $7::text[]) AS k(server_id, query_hash, database_name)
        WHERE k.server_id = h.server_id
        AND   k.query_hash IS NOT DISTINCT FROM h.query_hash
        AND   k.database_name IS NOT DISTINCT FROM h.database_name
    )
    ORDER BY h.last_collected DESC, h.server_id, h.query_hash, h.database_name
    LIMIT $2
)
SELECT p.server_id, p.query_hash, p.database_name, p.last_collected, d.query_text
FROM page AS p
JOIN query_text_dim AS d ON d.digest = p.query_text_digest
ORDER BY p.last_collected DESC, p.server_id, p.query_hash, p.database_name";

    /// <summary>#8570: one server's sample read for a pattern that matched at most <see cref="ProbeDigestCap"/> texts, one span of the raw window, run
    /// newest span first in place of <see cref="PlanCacheSql"/> when the plan-cache text index is <c>Ready</c>. $1 server_id, $2 database or null, $3 the
    /// matched digests, $4 and $5 the span's floor and ceiling (naive UTC, floor inclusive), $6 the rows still wanted, $7 and $8 the query_hash and
    /// database_name of the rows earlier spans found. The texts were matched once, before the first span, so a span tests no text: it reads the server's
    /// samples by the digests, as <see cref="PlanCacheAllServersProbeSql"/> does for every server. One row per query_hash and database, the newest
    /// sighting of each, paged and read back in <see cref="PlanCacheSql"/>'s order; the first column is <c>hit</c>, so the rows read as that
    /// statement's page rows.</summary>
    internal const string PlanCacheProbeSql = @"
WITH hits AS (
    SELECT DISTINCT ON (s.query_hash, s.database_name)
           s.query_hash, s.database_name, s.collection_time AS last_collected, s.query_text_digest
    FROM query_stats AS s
    WHERE s.server_id = $1
    AND   ($2::text IS NULL OR s.database_name = $2)
    AND   s.query_text_digest = ANY($3::bytea[])
    AND   s.collection_time >= $4
    AND   s.collection_time < $5
    ORDER BY s.query_hash, s.database_name, s.collection_time DESC
),
page AS (
    SELECT h.query_hash, h.database_name, h.last_collected, h.query_text_digest
    FROM hits AS h
    WHERE NOT EXISTS (
        SELECT 1
        FROM unnest($7::text[], $8::text[]) AS k(query_hash, database_name)
        WHERE k.query_hash IS NOT DISTINCT FROM h.query_hash
        AND   k.database_name IS NOT DISTINCT FROM h.database_name
    )
    ORDER BY h.last_collected DESC, h.query_hash, h.database_name
    LIMIT $6
)
SELECT 'hit' AS kind, p.query_hash, p.database_name, p.last_collected, d.query_text
FROM page AS p
JOIN query_text_dim AS d ON d.digest = p.query_text_digest
ORDER BY p.last_collected DESC, p.query_hash, p.database_name";

    /// <summary>#7849: the <c>sample_read</c> words of the plan-cache source. <see cref="SampleReadProbe"/>: the matched digests were bound to the
    /// sample read. <see cref="SampleReadJoin"/> (all servers): more than <see cref="ProbeDigestCap"/> texts matched, so each span's samples were
    /// joined to their own texts, span by span. <see cref="SampleReadLookup"/> (all servers, #9605): the matched texts were not counted, because the
    /// newest samples filled the page before they were read, the budget ended first, or the match read ran out of its timeout, so each span's samples
    /// were joined to their own texts. <see cref="SampleReadPlain"/> (one server, #8570): more than <see cref="ProbeDigestCap"/> texts matched, or the
    /// match read ran out of its timeout or was refused, so each span tested its texts against the pattern.</summary>
    public const string SampleReadProbe = "probe";

    /// <inheritdoc cref="SampleReadProbe"/>
    public const string SampleReadJoin = "join";

    /// <inheritdoc cref="SampleReadProbe"/>
    public const string SampleReadLookup = "lookup";

    /// <inheritdoc cref="SampleReadProbe"/>
    public const string SampleReadPlain = "plain";

    /// <summary>Escapes the LIKE metacharacters (<c>\</c>, <c>%</c>, <c>_</c>) so the pattern matches the text as typed.</summary>
    internal static string EscapeLike(string pattern) =>
        pattern.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("%", "\\%", StringComparison.Ordinal).Replace("_", "\\_", StringComparison.Ordinal);

    /// <summary>
    /// What the reply carries for one stored text (#5320): the judge runs on the WHOLE text first, then the excerpt is cut from the judged
    /// text, around the first match. A text the judge names is the withheld marker (<see cref="SensitiveStatements.PlaceholderText"/>) with no
    /// length, so a withheld row says no more than a withheld statement says anywhere else. A text that is clean carries its whole length and
    /// <see cref="ExcerptLength"/> characters starting <see cref="ExcerptLead"/> characters before the match; when the match cannot be found
    /// again here (the SQL fold of case and this one differ on an unusual letter) the excerpt starts at the head of the text.
    /// </summary>
    internal static (int? TextLength, string? Excerpt) Shape(string? stored, string pattern)
    {
        if (stored is null) return (null, null);

        var judged = SensitiveStatements.Text(stored);
        if (judged is null || string.Equals(judged, SensitiveStatements.PlaceholderText, StringComparison.Ordinal))
        {
            return (null, SensitiveStatements.PlaceholderText);
        }

        var at = judged.IndexOf(pattern, StringComparison.OrdinalIgnoreCase);
        var start = Math.Max(0, at - ExcerptLead);
        if (start > 0 && char.IsLowSurrogate(judged[start])) start--;
        var tail = judged.AsSpan(start);
        return (judged.Length, tail[..McpHelpers.TextElementCutLength(tail, ExcerptLength)].ToString());
    }

    /// <summary>What each source covers, for the reply and for an empty answer.</summary>
    internal static string QueryStoreHorizon { get; } =
        "Query Store text the store holds (collect.query_store_text): one row per database and query_id, kept until the statement has gone unseen for the widest fact retention plus "
        + QueryStoreTextStore.PruneMarginDays.ToString(CultureInfo.InvariantCulture)
        + " days. A statement Query Store has since evicted, or one this server's Query Store never recorded, is not here.";

    internal static string PlanCacheHorizon { get; } =
        "Plan-cache text of the raw tier (query_stats): the last "
        + ((int)TimescaleSupport.RawRetentionSpan.TotalDays).ToString(CultureInfo.InvariantCulture)
        + " days, one row per query_hash and database. A statement that left the plan cache before that, or ran only before the server was monitored, is not here.";

    internal static string PlanCacheAllServersHorizon { get; } =
        "Plan-cache text of the raw tier (query_stats) for every enabled server (the newest samples are read first and each one's text tested, then the text is matched through the trigram index and the samples in the raw window are read by the matched digests, or span by span with each sample's text tested when more than "
        + ProbeDigestCap.ToString(CultureInfo.InvariantCulture) + " texts matched): the last "
        + ((int)TimescaleSupport.RawRetentionSpan.TotalDays).ToString(CultureInfo.InvariantCulture)
        + " days, one row per server, query_hash and database. A statement that left the plan cache before that, or ran only before the server was monitored, is not here.";

    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "find_queries_by_text"), Description(
        "Finds statements whose text contains a piece of text (a table name, a fragment of SQL), and returns the keys the other query reads take: query_id and database for get_query_store_query_history, query_hash for get_query_trend. pattern is a literal substring, case-insensitive (% and _ match themselves), at least 3 characters. Name a server with server_name: database_name narrows it and makes the search much faster. Two sources, each reported separately with its own horizon: Query Store text and the plan-cache text of the raw tier. The text is not indexed unless the operator turns an index on, so each source has a 4 second budget. One server's Query Store text is read through its opt-in trigram index when that index is ready (darling.json queryStoreTextTrigramIndexEnabled; get_store_metrics query_store_text_index says its state), which makes a rare pattern fast. One server's plan-cache text is read through the plan-cache text index when that one is ready (queryTextTrigramIndexEnabled): its source then says sample_read probe (the matching texts were found first; matched_texts says how many) or plain (more than 1000 texts matched, or finding them was slow, so each span's texts were tested). A source that runs out is reported stopped with the rows it had found by then (the plan-cache text is searched newest first, in spans of the raw window, so those rows are the newest matches and the message says how much of the window was searched; name a server, and a database, to narrow it). The plan-cache source also carries searched: the servers, the raw window it covers (window_from to window_to) and the part of it the page is the exact answer for (searched_from to searched_to, null when none is; complete says whether the page is the whole-window answer): a search that finished is, even when it stopped reading because the page was full (spans run newest first, so older spans cannot change a full page, and truncated says more matches exist), and a search the budget cut is the answer for the newest spans it finished only. Leave server_name out to search the plan-cache text of every server: that is served only when the opt-in trigram index is ready (darling.json queryTextTrigramIndexEnabled; get_store_metrics query_text_index says its state), is refused with the state otherwise, and does not search Query Store text, which is searched one server at a time. It reads the newest samples first and tests each one's text: a page that fills there, or a raw tier with no more samples than that read takes, is the answer (sample_read lookup, matched_texts null). Otherwise the trigram index finds the matching texts and the samples are read by those texts' digests (sample_read probe, with matched_texts saying how many matched), which a second index the same option builds on query_stats serves on uncompressed chunks. A pattern more than 1000 texts contain (sample_read join), or one whose matching texts were not found in time (sample_read lookup), is searched on newest first in spans of a bounded number of samples, each sample's text tested, which on a large store can reach the 4 second budget and report stopped with the newest part searched. Each source in the reply carries a status: ok (rows found), empty (searched, nothing matched), stopped (the budget ran out, so the rows listed may be incomplete), not_searched (the all-servers scope does not search that source, Query Store text) or not_collected (a PostgreSQL target has neither SQL Server source: both sources say so and nothing was searched; read PostgreSQL statement text with get_pg_top_queries, full_text true). Refused when mcp.dataDetail hides statement text, because a search on hidden text would reveal it. Rows carry a ~300 character excerpt around the first match, never the whole text; a statement the sensitive-statement filter withholds is listed with the withheld marker in place of the excerpt.")]
    public static Task<string> FindQueriesByText(
        NpgsqlDataSource postgres,
        [Description("Text to find in the statement. A literal substring, matched ignoring case; % and _ match themselves. At least 3 characters.")] string pattern,
        [Description("Server name or display name. Omit to search the plan-cache text of every server, which needs the trigram index to be ready.")] string? server_name = null,
        [Description("Limit to one database of the named server. Omit for all databases on the server; naming one is much faster. Needs server_name.")] string? database_name = null,
        [Description("Maximum rows per source. Default 20, maximum 50.")] int limit = DefaultLimit,
        McpDataDetailSetting? maskSetting = null,
        CancellationToken cancellationToken = default)
        => FindQueriesByText(postgres, pattern, server_name, database_name, limit, maskSetting?.Mode ?? McpDataDetail.Full, SourceBudget.Default, SourceBudget.Default, cancellationToken, null);

    internal static async Task<string> FindQueriesByText(
        NpgsqlDataSource postgres,
        string? pattern,
        string? server_name,
        string? database_name,
        int limit,
        McpDataDetail detail,
        SourceBudget queryStoreBudget,
        SourceBudget planCacheBudget,
        CancellationToken cancellationToken,
        QueryTextTrigramIndexTracker? tracker = null,
        QueryTextTrigramIndexTracker? queryStoreTextTracker = null)
    {
        if (detail != McpDataDetail.Full)
        {
            /* maskLiterals hides the literals in a statement; a search on them would tell the caller what they are. metadataOnly hides the text. */
            return McpHelpers.Refusal(
                "pattern",
                $"find_queries_by_text is not served at mcp.dataDetail '{McpDataDetailModes.Word(detail)}': it searches statement text, and a search on text this level hides would tell the caller what the text holds. Use a tool that returns query_hash or query_id, or ask the operator for the 'full' level.");
        }

        var text = pattern ?? string.Empty;
        if (text.Trim().Length < MinPatternLength)
        {
            return McpHelpers.Refusal("pattern", $"pattern must be at least {MinPatternLength} characters: a shorter one matches most statements.");
        }

        var limitError = McpHelpers.ValidateTop(limit, MaxLimit);
        if (limitError != null) return limitError;

        if (string.IsNullOrWhiteSpace(server_name))
        {
            if (!string.IsNullOrWhiteSpace(database_name))
            {
                return McpHelpers.Refusal(
                    "server_name",
                    "database_name narrows one server's search, so it needs server_name. Use list_servers for the names, or leave both out to search the plan-cache text of every server once the trigram index is ready (queryTextTrigramIndexEnabled in darling.json).");
            }

            return await FindInAllServersAsync(postgres, text, limit, planCacheBudget, tracker ?? QueryTextTrigramIndexTracker.Shared, cancellationToken);
        }

        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        /* #8683: both sources read SQL Server tables (query_store, query_stats), so on a PostgreSQL target they would answer "empty" for a
           statement the store does hold text for. Decided before either read runs. A registry read that fails answers null: search as before. */
        string? engineKind;
        try
        {
            (_, engineKind, _, _) = await DarlingEngineCapability.ReadServerEngineAsync(postgres, resolved.ServerId, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            engineKind = null;
        }

        if (PostgresTargetReply(resolved.DisplayName, engineKind, text, string.IsNullOrWhiteSpace(database_name) ? null : database_name.Trim(), limit) is { } postgresReply)
        {
            return postgresReply;
        }

        try
        {
            var database = string.IsNullOrWhiteSpace(database_name) ? null : database_name.Trim();
            var escaped = EscapeLike(text);
            var floor = DateTime.SpecifyKind(DateTime.UtcNow - TimescaleSupport.RawRetentionSpan, DateTimeKind.Unspecified);

            /* #8570: with the plan-cache text index Ready the matched digests are read first and each span reads the server's samples by them. The
               index state and the match read run on a connection of their own while the Query Store source runs, so the match read's time is spent
               alongside that source and not out of the plan-cache source's budget. The index state is read only when the option is on, so a store
               with it off pays nothing for it. */
            using var matchCancel = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var matchAhead = ReadMatchAheadAsync(postgres, tracker ?? QueryTextTrigramIndexTracker.Shared, escaped, planCacheBudget, matchCancel.Token);
            object queryStore;
            try
            {
                queryStore = await RunSourceAsync(
                    postgres, QueryStoreSql, queryStoreBudget, limit, p => BindOneServer(p, resolved.ServerId, database, escaped, limit, null), QueryStoreHorizon,
                    QueryStoreStoppedAdvice(database, (queryStoreTextTracker ?? QueryStoreTextTrigramIndex.SharedTracker).Read()),
                    r => (Database: r.GetString(0), QueryId: r.GetInt64(1), Hash: r.IsDBNull(2) ? null : r.GetString(2), LastSeen: r.GetDateTime(3),
                        Stored: r.IsDBNull(4) ? null : r.GetString(4)),
                    raw =>
                    {
                        var (textLength, excerpt) = Shape(raw.Stored, text);
                        return new
                        {
                            database_name = raw.Database,
                            query_id = raw.QueryId,
                            query_hash = raw.Hash,
                            last_seen_at = Utc(raw.LastSeen),
                            text_length = textLength,
                            excerpt,
                        };
                    }, cancellationToken);
            }
            catch
            {
                /* The Query Store source threw, so nothing reads the match read's answer: it is cancelled and waited out before the error goes on. */
                await matchCancel.CancelAsync();
                await Task.WhenAny(matchAhead);
                throw;
            }

            var (probeWanted, matched) = await matchAhead;

            /* The raw window newest span first, one statement per span inside the one transaction and budget (see RawTierSpanParts): the rows
               a span finds are kept whatever happens to the spans after it, and a span already-found statements ran in again does not list them
               twice. On the span read the digests each span judged, with their verdicts, are carried to the spans after it, so a text is judged once. */
            var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            var spans = RawTierSpans(floor, now);
            var judgedDigests = new List<byte[]>();
            var judgedVerdicts = new List<bool>();

            /* The match read's answer: the probe under the cap, the span read past it or when the match read gave up. */
            int? matchedTexts = null;
            string? sampleRead = null;
            byte[][]? digests = null;
            if (probeWanted)
            {
                if (matched is null || matched.Count > ProbeDigestCap)
                {
                    sampleRead = SampleReadPlain;
                }
                else
                {
                    sampleRead = SampleReadProbe;
                    matchedTexts = matched.Count;
                    digests = matched.ToArray();
                }
            }

            var planCacheOutcome = await RunSourceAsync<PlanCacheHit, object>(
                postgres, planCacheBudget, limit,
                (_, _, step, collected, _, _) =>
                {
                    /* No text matched, so no sample statement runs. */
                    if (digests is { Length: 0 } || step >= spans.Count || collected.Count > limit)
                    {
                        return Task.FromResult<SourceStatement?>(null);
                    }

                    var span = spans[step];
                    var wanted = limit + 1 - collected.Count;
                    if (digests is { } probed)
                    {
                        return Task.FromResult<SourceStatement?>(
                            new SourceStatement(PlanCacheProbeSql, p => BindPlanCacheProbeSpan(p, resolved.ServerId, database, probed, wanted, span, collected)));
                    }

                    return Task.FromResult<SourceStatement?>(new SourceStatement(
                        PlanCacheSql, p => BindPlanCacheSpan(p, resolved.ServerId, database, escaped, wanted, span, collected, judgedDigests, judgedVerdicts)));
                },
                StoppedAdvice(database),
                completedSpans => completedSpans == 0
                    ? $"before the newest span of the {RawTierDays} raw tier had been searched"
                    : $"after the newest {DescribeSearched(completedSpans, floor, now)} of the {RawTierDays} raw tier had been searched, newest first, and the rest had not",
                r => new PlanCacheHit(r.IsDBNull(1) ? null : r.GetString(1), r.IsDBNull(2) ? null : r.GetString(2), r.GetDateTime(3), r.IsDBNull(4) ? null : r.GetString(4)),
                r =>
                {
                    /* A judged row is a digest this span judged and its verdict, for the spans after it; it is not a row of the page. */
                    if (!string.Equals(r.GetString(0), PlanCacheJudgedRowKind, StringComparison.Ordinal))
                    {
                        return false;
                    }

                    judgedDigests.Add((byte[])r[5]);
                    judgedVerdicts.Add(r.GetBoolean(6));
                    return true;
                },
                raw =>
                {
                    var (textLength, excerpt) = Shape(raw.Stored, text);
                    return new
                    {
                        query_hash = raw.Hash,
                        database_name = raw.Database,
                        last_collected_at = Utc(raw.LastCollected),
                        text_length = textLength,
                        excerpt,
                    };
                }, cancellationToken);
            object planCache = probeWanted
                ? new
                {
                    status = planCacheOutcome.Status,
                    message = planCacheOutcome.Message,
                    horizon = PlanCacheHorizon,
                    /* #8570: how the samples were found with the index ready. matched_texts is the count under the cap, null when the span read served it. */
                    matched_texts = matchedTexts,
                    sample_read = sampleRead,
                    searched = SearchedWindow(new[] { resolved.DisplayName }, 1, planCacheOutcome.Status, planCacheOutcome.CompletedSteps, floor, now),
                    returned = planCacheOutcome.Returned,
                    truncated = planCacheOutcome.Truncated,
                    rows = planCacheOutcome.Rows,
                }
                : new
                {
                    status = planCacheOutcome.Status,
                    message = planCacheOutcome.Message,
                    horizon = PlanCacheHorizon,
                    searched = SearchedWindow(new[] { resolved.DisplayName }, 1, planCacheOutcome.Status, planCacheOutcome.CompletedSteps, floor, now),
                    returned = planCacheOutcome.Returned,
                    truncated = planCacheOutcome.Truncated,
                    rows = planCacheOutcome.Rows,
                };

            return JsonSerializer.Serialize(new
            {
                server = resolved.DisplayName,
                pattern = text,
                database_name = database,
                limit,
                match = "literal substring, case-insensitive",
                scope = "one_server",
                sources = new { query_store = queryStore, plan_cache = planCache },
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(ToolName, ex);
        }
    }

    /// <summary>#8683: the one-server reply for a PostgreSQL target, or <c>null</c> for any other engine kind. Both sources are <c>not_collected</c>: they
    /// read SQL Server tables a PostgreSQL target never fills, so <c>empty</c> (a true negative) would be false. The message names the engine and points at
    /// get_pg_top_queries, the read that holds PostgreSQL statement text. Pure, so it is tested without a store.</summary>
    internal static string? PostgresTargetReply(string serverName, string? engineKind, string pattern, string? database, int limit)
    {
        if (!MonitoredEngineKind.IsPostgres(engineKind))
        {
            return null;
        }

        string Message(string source) =>
            $"{serverName} runs {MonitoredEngineKind.DescribeEngineKind(engineKind)}. {source} is a SQL Server signal that this engine does not collect, so " +
            "this server has no text there and never will: a search of it would answer nothing, which says nothing about the server's statements. " +
            "find_queries_by_text searches SQL Server targets only. Statement text for a PostgreSQL server is in get_pg_top_queries (set full_text to true " +
            "for the whole statement, not the 400-character preview). " +
            "This is a permanent engine capability gap, not a collection outage: checking collection health, enabling a collector or starting a capture cannot change it.";

        object Source(string source) => new
        {
            status = "not_collected",
            message = Message(source),
            returned = 0,
            truncated = false,
            rows = Array.Empty<object>(),
        };

        return JsonSerializer.Serialize(new
        {
            server = serverName,
            pattern,
            database_name = database,
            limit,
            match = "literal substring, case-insensitive",
            scope = "one_server",
            sources = new { query_store = Source("Query Store text"), plan_cache = Source("Plan-cache text (query_stats)") },
        }, McpHelpers.JsonOptions);
    }

    /// <summary>#7674: the all-servers scope. Offered only when the trigram index is <c>Ready</c>; otherwise <see cref="AllServersRefusal"/>. The plan-cache text
    /// is searched through the index across every enabled server, and the Query Store text is reported as not searched, because it has no index.</summary>
    private static async Task<string> FindInAllServersAsync(
        NpgsqlDataSource postgres, string text, int limit, SourceBudget planCacheBudget, QueryTextTrigramIndexTracker tracker, CancellationToken cancellationToken)
    {
        TrigramIndexHealth? health;
        try
        {
            await using var healthConnection = await postgres.OpenConnectionAsync(cancellationToken);
            health = await QueryTextTrigramIndex.ReadHealthAsync(healthConnection, tracker, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            health = null;
        }

        if (health?.Status != TrigramIndexStatus.Ready)
        {
            return McpHelpers.Refusal("server_name", AllServersRefusal(health));
        }

        try
        {
            var (servers, fault) = await DarlingServerResolver.LoadEnabledOrFaultAsync(postgres, cancellationToken);
            if (fault != null) return fault;

            var escaped = EscapeLike(text);
            var floor = DateTime.SpecifyKind(DateTime.UtcNow - TimescaleSupport.RawRetentionSpan, DateTimeKind.Unspecified);
            int? matchedTexts = null;

            /* The word the reply carries: the lookup spans until the matched texts have been read, then the read their count chose. */
            var sampleRead = SampleReadLookup;
            byte[][]? digests = null;
            var matchTried = false;
            var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            var spans = RawTierSpans(floor, now);

            /* The spans are cut as the search goes (a lookup span by a row count, see AllServersSpanRows), so what the search has finished is the
               floor of the last span it completed, not an index into the fixed spans. Each span's ceiling is the floor of the one before it, so
               the spans partition the window whichever statement reads them. */
            var reached = now;
            var ceiling = now.AddDays(1);
            var lookupIndex = 0;
            (DateTime Floor, DateTime Ceiling)? pending = null;
            var lookupCapped = false;
            DateTime? covered = null;

            var planCache = await RunSourceAsync<AllServersHit, object>(
                postgres, planCacheBudget, limit,
                async (connection, transaction, step, collected, elapsed, ct) =>
                {
                    /* What is left of the budget for a read this call makes itself, by the source's own clock: the time the source's gate
                       reads after this call, so the two cannot disagree about whether anything is left. */
                    long Left() => planCacheBudget.Milliseconds - (long)elapsed().TotalMilliseconds;

                    /* The span the last statement ran is done, except that a lookup span that hit its match limit without filling the page runs
                       again with the same limit, for the keys not yet found (the keys it found are excluded, so each rerun makes progress, and an
                       unlimited rerun would probe every sighting of the span: a query_hash with many digests fills the first limit alone). */
                    if (pending is { } done && !(lookupCapped && collected.Count <= limit))
                    {
                        covered = done.Floor;
                        reached = done.Floor;
                        ceiling = done.Floor;
                        lookupIndex++;
                        pending = null;
                    }

                    lookupCapped = false;
                    if (pending is null && (reached <= floor || collected.Count > limit))
                    {
                        return null;
                    }

                    /* #9605: the newest lookup span is done and the page is still short, so the matched digests are worth reading, once: at most
                       ProbeDigestCap of them and the probe reads the rest of the window by them, more and the lookup spans go on. The read has a
                       timeout of its own inside a savepoint, so one that runs out costs its share of the budget and nothing else: the rows found
                       stay and the lookup spans go on under what is left. With nothing left by the source's clock it is not run, and the source,
                       which reads that clock at its gate after this call, stops there. */
                    if (pending is null && !matchTried && covered is not null && Left() is > 0 and var left)
                    {
                        matchTried = true;
                        var share = planCacheBudget.AllServersMatchMilliseconds ?? planCacheBudget.Milliseconds / AllServersMatchShare;
                        var matched = await ReadMatchedDigestsInSavepointAsync(
                            connection, transaction, escaped, Math.Max(1, Math.Min(share, left)), planCacheBudget.MatchTestDelaySeconds, ct);
                        if (matched is not null)
                        {
                            if (matched.Count > ProbeDigestCap)
                            {
                                sampleRead = SampleReadJoin;
                            }
                            else
                            {
                                sampleRead = SampleReadProbe;
                                matchedTexts = matched.Count;
                                if (matched.Count == 0)
                                {
                                    return null;
                                }

                                digests = matched.ToArray();
                            }
                        }
                    }

                    var wanted = limit + 1 - collected.Count;
                    if (digests is { } probed)
                    {
                        /* #7849: the probe reads the rest of the window by the matched digests, one statement per fixed span (RawTierSpans), newest
                           first: the first one is what the lookup span left of its fixed span, and each after it a whole one. */
                        var probeFloor = floor;
                        foreach (var fixedSpan in spans)
                        {
                            if (fixedSpan.Floor < reached)
                            {
                                probeFloor = fixedSpan.Floor;
                                break;
                            }
                        }

                        var probeSpan = (probeFloor, ceiling);
                        pending = probeSpan;
                        return new SourceStatement(PlanCacheAllServersProbeSql, p => BindAllServersSpan(
                            p, new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bytea, Value = probed }, wanted, probeSpan, collected));
                    }

                    if (pending is null)
                    {
                        /* #9605: a lookup span is sized by rows alone. It ends at the sample SpanRows rows back from its newest edge, times its
                           parts (1, 1, 2 and then AllServersSpanRowParts), or at the window's floor when fewer samples than that are left, so a
                           store that holds less than one allotment is read whole by its first statement and the search ends there. A width in
                           time is no bound on a statement's work and no help on a small store, where it only repeats the statement for each
                           slice of the window. The cut-off read runs inside this call, before the source arms the statement timeout for the
                           statement that follows, so it is armed here to what is left of the budget. With nothing left it is not run and the
                           span is not cut: the source reads the same elapsed time at its gate after this call and stops there, so that
                           statement never runs. */
                        var parts = lookupIndex == 0 ? 1 : Math.Min(1 << Math.Min(lookupIndex - 1, 8), AllServersSpanRowParts);
                        var spanFloor = floor;
                        if (Left() is > 0 and var floorLeft)
                        {
                            spanFloor = await ReadRowFloorAsync(connection, transaction, floor, ceiling, planCacheBudget.SpanRows * parts, floorLeft, ct);
                        }

                        pending = (spanFloor, ceiling);
                    }

                    var span = pending!.Value;
                    var matchLimit = (int)Math.Min((long)wanted * AllServersLookupMatchFactor, int.MaxValue);
                    return new SourceStatement(PlanCacheAllServersLookupSpanSql, p =>
                    {
                        BindAllServersSpan(p, new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = escaped }, wanted, span, collected);
                        p.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = matchLimit });
                    });
                },
                "Use a longer, more specific pattern, or name a server with server_name (and database_name) to search it directly.",
                _ => covered is null
                    ? $"before the newest span of the {RawTierDays} raw tier had been searched through"
                    : $"after the newest {DescribeSearchedFrom(covered.Value, now)} of the {RawTierDays} raw tier had been searched, newest first, and the rest had not",
                r =>
                {
                    /* #9219: the lookup statement's last column says it stopped at its match limit. */
                    if (r.FieldCount > 5 && r.GetBoolean(5))
                    {
                        lookupCapped = true;
                    }

                    return new AllServersHit(r.GetInt32(0), r.IsDBNull(1) ? null : r.GetString(1), r.IsDBNull(2) ? null : r.GetString(2), r.GetDateTime(3),
                        r.IsDBNull(4) ? null : r.GetString(4));
                },
                null,
                raw =>
                {
                    var (textLength, excerpt) = Shape(raw.Stored, text);
                    return new
                    {
                        server = DarlingServerResolver.AnswerNameOf(servers, servers.Find(x => x.ServerId == raw.ServerId)),
                        query_hash = raw.Hash,
                        database_name = raw.Database,
                        last_collected_at = Utc(raw.LastCollected),
                        text_length = textLength,
                        excerpt,
                    };
                }, cancellationToken);

            return JsonSerializer.Serialize(new
            {
                server = (string?)null,
                pattern = text,
                database_name = (string?)null,
                limit,
                match = "literal substring, case-insensitive",
                scope = "all_servers",
                sources = new
                {
                    query_store = new
                    {
                        status = "not_searched",
                        message = "An all-servers search does not read Query Store text. Name a server with server_name (and database_name to make it faster) to search its Query Store text.",
                        horizon = QueryStoreHorizon,
                        returned = 0,
                        truncated = false,
                        rows = Array.Empty<object>(),
                    },
                    plan_cache = new
                    {
                        status = planCache.Status,
                        message = planCache.Message,
                        horizon = PlanCacheAllServersHorizon,
                        /* #7849: how the samples were found. matched_texts is the count the probe read by, null for the other reads. */
                        matched_texts = matchedTexts,
                        sample_read = sampleRead,
                        searched = SearchedWindow(
                            "every enabled server", servers.Count, planCache.Status, planCache.CompletedSteps, floor, now, true, covered),
                        returned = planCache.Returned,
                        truncated = planCache.Truncated,
                        rows = planCache.Rows,
                    },
                },
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(ToolName, ex);
        }
    }

    /// <summary>#7674: the refusal of the all-servers scope when the trigram index is not <c>Ready</c>. One sentence plus the way forward, naming the option and the
    /// state. Null <paramref name="health"/> is a catalog read that failed. Pure, so each state is tested without a store.</summary>
    internal static string AllServersRefusal(TrigramIndexHealth? health)
    {
        var lead = "Searching every server at once needs the opt-in trigram index on the plan-cache text (update_service_flags query_text_trigram_index_enabled, or queryTextTrigramIndexEnabled in darling.json), and ";
        var tail = " Until then, name one server with server_name (database_name makes it faster).";
        if (health is null)
        {
            return lead + "its state could not be read (the store's catalog read failed)." + tail;
        }

        var minutes = ((int)QueryTextTrigramIndex.StartDelay.TotalMinutes).ToString(CultureInfo.InvariantCulture);
        var passMinutes = ((int)QueryTextTrigramIndex.PassInterval.TotalMinutes).ToString(CultureInfo.InvariantCulture);
        var state = health.Status switch
        {
            TrigramIndexStatus.Unconfigured =>
                "the service has not read darling.json yet, so the option is not known (state: not configured yet). Try again in a moment.",
            TrigramIndexStatus.Off =>
                "the option is off (state: off). Set update_service_flags query_text_trigram_index_enabled true (no restart; the next index pass, at most "
                + passMinutes + " minutes away, builds it), or, while the store holds no value for it, queryTextTrigramIndexEnabled true in darling.json and restart "
                + "the service. The build runs in the background and can take many minutes on a large store.",
            TrigramIndexStatus.DropPending or TrigramIndexStatus.Dropping =>
                "the option is off and the service is dropping the index (state: being dropped). Set update_service_flags query_text_trigram_index_enabled true, or queryTextTrigramIndexEnabled "
                + "true in darling.json and restart the service while the store holds no value for it, to build it again.",
            TrigramIndexStatus.Pending =>
                "the option is on but the index is not built yet (state: pending). The service builds it in the background about " + minutes + " minutes after start.",
            TrigramIndexStatus.Building =>
                "the index is building (state: building). Try again when get_store_metrics reports query_text_index.status Ready.",
            TrigramIndexStatus.Failed =>
                "the last build failed (state: failed). The service tries again after a wait: get_store_metrics query_text_index says why and when.",
            TrigramIndexStatus.WaitingOnExtension =>
                "it is waiting on the pg_trgm extension (state: waiting on the pg_trgm extension). An operator runs "
                + (health.OperatorStatement ?? QueryTextTrigramIndex.OperatorStatement) + " in the store database as a role that may create it.",
            _ => "it is not ready (state: " + health.Status + ").",
        };
        return lead + state + tail;
    }

    private static string StoppedAdvice(string? database) =>
        database is null
            ? "Name a database with database_name to narrow the search; one server and one database is much faster."
            : "The database named is large; use a longer, more specific pattern.";

    /// <summary>#8570: the Query Store source's stopped advice, which also names the Query Store text index: how to turn it on when the option is
    /// off, and where its state is when the option is on (a stopped search with the option on is one the index was not ready for, or a pattern
    /// common enough that the planner walks the key). Reads only this process's tracker, never the store. Pure.</summary>
    internal static string QueryStoreStoppedAdvice(string? database, QueryTextTrigramIndexTracker.Snapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        var index = !snapshot.Configured
            ? ""
            : snapshot.Enabled
                ? " The trigram index on the Query Store text is turned on; get_store_metrics query_store_text_index says whether it is ready."
                : " The opt-in trigram index on the Query Store text makes this search an index read: update_service_flags query_store_text_trigram_index_enabled, or queryStoreTextTrigramIndexEnabled in darling.json.";
        return StoppedAdvice(database) + index;
    }

    /// <summary>One plan-cache row as the statement hands it back, before the judge sees its text.</summary>
    private sealed record PlanCacheHit(string? Hash, string? Database, DateTime LastCollected, string? Stored);

    /// <summary>The raw tier's length, for the stopped message: "4-day".</summary>
    private static string RawTierDays { get; } = ((int)TimescaleSupport.RawRetentionSpan.TotalDays).ToString(CultureInfo.InvariantCulture) + "-day";

    /// <summary><see cref="PlanCacheSql"/>'s parameters for one span: the server, the database filter, the pattern, the rows still wanted, the
    /// span's floor and ceiling, the keys of the rows earlier spans found, and the digests earlier spans judged with their verdicts.</summary>
    private static void BindPlanCacheSpan(
        NpgsqlParameterCollection parameters, int serverId, string? database, string escaped, int wanted, (DateTime Floor, DateTime Ceiling) span,
        IReadOnlyList<PlanCacheHit> found, List<byte[]> judgedDigests, List<bool> judgedVerdicts)
    {
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)database ?? DBNull.Value });
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = escaped });
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = wanted });
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = span.Floor });
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = span.Ceiling });
        AddFoundKeys(parameters, found);
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bytea, Value = judgedDigests.ToArray() });
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Boolean, Value = judgedVerdicts.ToArray() });
    }

    /// <summary>#8570: <see cref="PlanCacheProbeSql"/>'s parameters for one span: the server, the database filter, the matched digests, the span's
    /// floor and ceiling, the rows still wanted, and the keys of the rows earlier spans found.</summary>
    private static void BindPlanCacheProbeSpan(
        NpgsqlParameterCollection parameters, int serverId, string? database, byte[][] digests, int wanted, (DateTime Floor, DateTime Ceiling) span,
        IReadOnlyList<PlanCacheHit> found)
    {
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)database ?? DBNull.Value });
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bytea, Value = digests });
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = span.Floor });
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = span.Ceiling });
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = wanted });
        AddFoundKeys(parameters, found);
    }

    /// <summary>The query_hash and database_name of every one-server row earlier spans found, as two text arrays.</summary>
    private static void AddFoundKeys(NpgsqlParameterCollection parameters, IReadOnlyList<PlanCacheHit> found)
    {
        var hashes = new string?[found.Count];
        var databases = new string?[found.Count];
        for (var i = 0; i < found.Count; i++)
        {
            hashes[i] = found[i].Hash;
            databases[i] = found[i].Database;
        }

        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = hashes });
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = databases });
    }

    /// <summary>One all-servers plan-cache row as the probe or a lookup span hands it back.</summary>
    private sealed record AllServersHit(int ServerId, string? Hash, string? Database, DateTime LastCollected, string? Stored);

    /// <summary>The parameters of one span of an all-servers sample read, <see cref="PlanCacheAllServersProbeSql"/> or
    /// <see cref="PlanCacheAllServersLookupSpanSql"/> (which takes its match limit after them): <paramref name="first"/> (the matched digests, or the escaped pattern), the rows still wanted, the
    /// span's floor and ceiling, and the server ids, query_hashes and databases of the rows earlier spans found.</summary>
    private static void BindAllServersSpan(
        NpgsqlParameterCollection parameters, NpgsqlParameter first, int wanted, (DateTime Floor, DateTime Ceiling) span, IReadOnlyList<AllServersHit> found)
    {
        parameters.Add(first);
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = wanted });
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = span.Floor });
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = span.Ceiling });
        var serverIds = new int[found.Count];
        var hashes = new string?[found.Count];
        var databases = new string?[found.Count];
        for (var i = 0; i < found.Count; i++)
        {
            serverIds[i] = found[i].ServerId;
            hashes[i] = found[i].Hash;
            databases[i] = found[i].Database;
        }

        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = serverIds });
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = hashes });
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = databases });
    }

    private static void BindOneServer(NpgsqlParameterCollection parameters, int serverId, string? database, string escaped, int limit, DateTime? floor)
    {
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)database ?? DBNull.Value });
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = escaped });
        parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = limit + 1 });
        if (floor is not null)
        {
            parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = floor.Value });
        }
    }

    private static string Utc(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);

    /// <summary>One source: the statement under <c>SET LOCAL statement_timeout</c> in its own transaction. A statement that runs out is
    /// <c>stopped</c> (SQLSTATE 57014), not an error, so the other source still answers. <paramref name="read"/> takes the raw row (whole
    /// text included) while the statement runs; <paramref name="shape"/> judges and cuts it after the connection is back in the pool, for
    /// the rows kept (the row past the cap is dropped unjudged).</summary>
    private static async Task<object> RunSourceAsync<TRaw, TRow>(
        NpgsqlDataSource postgres, string sql, SourceBudget budget, int limit, Action<NpgsqlParameterCollection> bind,
        string horizon, string stoppedAdvice, Func<NpgsqlDataReader, TRaw> read, Func<TRaw, TRow> shape, CancellationToken cancellationToken)
    {
        var outcome = await RunSourceAsync(
            postgres, budget, limit, (_, _, step, _, _, _) => Task.FromResult(step == 0 ? new SourceStatement(sql, bind) : null), stoppedAdvice, null, read, null, shape, cancellationToken);
        return new
        {
            status = outcome.Status,
            message = outcome.Message,
            horizon,
            returned = outcome.Returned,
            truncated = outcome.Truncated,
            rows = outcome.Rows,
        };
    }

    /// <summary>A source's statement and its parameters.</summary>
    private sealed record SourceStatement(string Sql, Action<NpgsqlParameterCollection> Bind);

    /// <summary>What one source's read came to, before the reply names its horizon.</summary>
    private sealed record SourceOutcome<TRow>(string Status, string? Message, int Returned, bool Truncated, IReadOnlyList<TRow> Rows, int CompletedSteps);

    /// <summary>The source read as a sequence of statements inside one transaction and one budget. <paramref name="next"/> is asked for the
    /// statement of each step in turn, with the rows the earlier steps found and the source's clock (the time spent of the budget, the one the
    /// gate before each statement reads), and may read first (#7849: the all-servers scope reads the matched digests, then hands back the sample
    /// statement that fits them), under a timeout it takes from that clock; null ends the source. <c>statement_timeout</c> starts at the budget, and each
    /// statement runs under what the steps before it left, so the source takes at most the budget before it is <c>stopped</c>, whichever
    /// statement runs out and even when a read spent it all. A source that stops keeps the rows the completed steps found: the stopped message
    /// counts them and, when <paramref name="searched"/> describes the completed steps (it is absent or returns null when they are not spans), says
    /// what they covered. <paramref name="aside"/>, when given,
    /// sees each row first and takes the rows that are not rows of the page (the one-server search's judged digests, which it carries to the
    /// next step itself); a row it returns true for is not read as a page row.</summary>
    private static async Task<SourceOutcome<TRow>> RunSourceAsync<TRaw, TRow>(
        NpgsqlDataSource postgres, SourceBudget budget, int limit,
        Func<NpgsqlConnection, NpgsqlTransaction, int, IReadOnlyList<TRaw>, Func<TimeSpan>, CancellationToken, Task<SourceStatement?>> next,
        string stoppedAdvice, Func<int, string?>? searched, Func<NpgsqlDataReader, TRaw> read, Func<NpgsqlDataReader, bool>? aside, Func<TRaw, TRow> shape,
        CancellationToken cancellationToken)
    {
        var rows = new List<TRaw>();
        var completed = 0;
        var stopwatch = new System.Diagnostics.Stopwatch();
        TimeSpan Elapsed() => budget.ElapsedClock?.Invoke() ?? stopwatch.Elapsed;
        string? stoppedMessage = null;
        void Stop()
        {
            var count = rows.Count == 0 ? "none" : Math.Min(rows.Count, limit).ToString(CultureInfo.InvariantCulture);
            var head = $"Stopped after {Elapsed().TotalSeconds.ToString("0.0", CultureInfo.InvariantCulture)} s: the {budget.Milliseconds / 1000.0:0.#} s budget for this source ran out ";
            var said = searched?.Invoke(completed);
            stoppedMessage = said is null
                ? head + "before the search finished, so the rows below (" + count + ") are not a complete answer. " + stoppedAdvice
                : head + said + ", so the rows below (" + count + ") are "
                    + (completed == 0 ? "not an answer: nothing was searched. " : "the matches in the part searched and not a complete answer. ")
                    + stoppedAdvice;
        }

        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            var setSql = $"SET LOCAL statement_timeout = {budget.Milliseconds.ToString(CultureInfo.InvariantCulture)}";
            await using (var set = new NpgsqlCommand(setSql, connection, transaction) { CommandTimeout = McpCommandDeadlines.ReadSeconds })
            {
                await set.ExecuteNonQueryAsync(cancellationToken);
            }

            stopwatch.Start();
            if (budget.TestDelaySeconds > 0)
            {
                var delaySql = "SELECT pg_sleep(" + budget.TestDelaySeconds.ToString(CultureInfo.InvariantCulture) + ")";
                await using var delay = new NpgsqlCommand(delaySql, connection, transaction) { CommandTimeout = McpCommandDeadlines.ReadSeconds };
                await delay.ExecuteNonQueryAsync(cancellationToken);
            }

            for (var step = 0; ; step++)
            {
                var statement = await next(connection, transaction, step, rows, Elapsed, cancellationToken);
                if (statement is null)
                {
                    break;
                }

                /* Each statement gets what the steps before it left of the budget, not the budget again. */
                var remaining = budget.Milliseconds - (long)Elapsed().TotalMilliseconds;
                if (remaining <= 0)
                {
                    Stop();
                    break;
                }

                var armSql = "SET LOCAL statement_timeout = " + remaining.ToString(CultureInfo.InvariantCulture);
                await using (var arm = new NpgsqlCommand(armSql, connection, transaction) { CommandTimeout = McpCommandDeadlines.ReadSeconds })
                {
                    await arm.ExecuteNonQueryAsync(cancellationToken);
                }

                await using var command = new NpgsqlCommand(statement.Sql, connection, transaction);
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                statement.Bind(command.Parameters);
                budget.StatementBound?.Invoke(step, command.Parameters);
                if (budget.BeforeStatement is not null)
                {
                    await budget.BeforeStatement(connection, transaction, cancellationToken);
                }


                await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
                {
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        if (aside is not null && aside(reader))
                        {
                            continue;
                        }

                        rows.Add(read(reader));
                    }
                }

                completed++;
            }
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.QueryCanceled)
        {
            /* The statement the timeout cancelled handed back nothing; the rows are the completed steps' own. */
            Stop();
        }

        var truncated = rows.Count > limit;
        if (truncated) rows.RemoveRange(limit, rows.Count - limit);
        var shaped = rows.ConvertAll(r => shape(r));
        if (stoppedMessage is not null)
        {
            return new SourceOutcome<TRow>("stopped", stoppedMessage, shaped.Count, truncated, shaped, completed);
        }

        return new SourceOutcome<TRow>(
            shaped.Count == 0 ? "empty" : "ok",
            shaped.Count == 0 ? "No statement in this source contains the pattern. Only what the horizon covers was searched." : null,
            shaped.Count,
            truncated,
            shaped,
            completed);
    }

    /// <summary>#9219: the time a lookup span ends at when it is cut by a row count: the collection time of the sample <paramref name="rows"/> rows
    /// back from <paramref name="ceiling"/> among those that carry a digest (of the text or of the plan), no earlier than <paramref name="floor"/>, or
    /// <paramref name="floor"/> when fewer samples than that are in the span. A read of the span's newest samples in time order
    /// (<see cref="RowFloorSql"/>). The span keeps every sample at that time, so it holds at least <paramref name="rows"/> such rows and its floor is
    /// strictly below its ceiling. <paramref name="statementTimeoutMs"/> is armed first as the statement timeout.</summary>
    private static async Task<DateTime> ReadRowFloorAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, DateTime floor, DateTime ceiling, int rows, long statementTimeoutMs,
        CancellationToken cancellationToken)
    {
        /* The cut-off read runs before the source arms the statement timeout for the statement that follows, so it is armed here to what
           is left of the budget (the caller passes it only when something is left). */
        var armSql = "SET LOCAL statement_timeout = " + statementTimeoutMs.ToString(CultureInfo.InvariantCulture);
        await using (var arm = new NpgsqlCommand(armSql, connection, transaction) { CommandTimeout = McpCommandDeadlines.ReadSeconds })
        {
            await arm.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var command = new NpgsqlCommand(RowFloorSql, connection, transaction) { CommandTimeout = McpCommandDeadlines.ReadSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = floor });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = ceiling });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Math.Max(rows - 1, 0) });
        var found = await command.ExecuteScalarAsync(cancellationToken);
        return found is DateTime at ? at : floor;
    }

    /// <summary>#9219: <see cref="ReadRowFloorAsync"/>'s statement. $1 the floor, $2 the ceiling (naive UTC), $3 the rows to skip. #9605: it counts
    /// the samples that carry a digest, of the text or of the plan. That predicate is, word for word, the predicate of the time-ordered partial
    /// index a migration ships (<c>ix_query_stats_digest_floor</c>), so the planner walks that index backward from the ceiling, stops after $3
    /// entries and answers from the index alone: an entry needs no visit to its row to be tested. A narrower predicate (the text digest only) is
    /// served by the same index with one row visit per entry, and with no digest predicate the only index that covers the range leads with
    /// <c>server_id</c>, so the read is every index entry of the span and a sort. The answer only places a span's edge, so counting the few
    /// samples that carry a plan digest and no text digest moves the edge slightly newer and changes no row: the lookup statement tests the text
    /// digest itself (<see cref="PlanCacheAllServersLookupSpanSql"/>), and one time is one span's floor and the next span's ceiling.</summary>
    internal const string RowFloorSql = @"
SELECT s.collection_time
FROM query_stats AS s
WHERE s.collection_time >= $1
AND   s.collection_time < $2
AND   (s.query_text_digest IS NOT NULL OR s.query_plan_digest IS NOT NULL)
ORDER BY s.collection_time DESC
OFFSET $3
LIMIT 1";

    /// <summary>#7849: the digests of the texts the pattern matches, at most <see cref="ProbeDigestCap"/> + 1 of them, in the caller's transaction: the
    /// all-servers source's (<see cref="ReadMatchedDigestsInSavepointAsync"/>) or the one-server match read's own.</summary>
    private static async Task<List<byte[]>> ReadMatchedDigestsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string escaped, CancellationToken cancellationToken)
    {
        var digests = new List<byte[]>();
        await using var command = new NpgsqlCommand(MatchedDigestsSql, connection, transaction) { CommandTimeout = McpCommandDeadlines.ReadSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = escaped });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ProbeDigestCap + 1 });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            digests.Add((byte[])reader[0]);
        }

        return digests;
    }

    /// <summary>#9605: the all-servers match read: <see cref="ReadMatchedDigestsAsync"/> inside a savepoint of the source's transaction, under a
    /// statement timeout of its own (<paramref name="timeoutMs"/>). Null when that timeout cancelled it: the savepoint is rolled back, which
    /// takes the transaction out of its failed state, so the statements after it run and the rows the source has found are kept.
    /// <paramref name="testDelaySeconds"/> is <see cref="SourceBudget.MatchTestDelaySeconds"/>.</summary>
    private static async Task<List<byte[]>?> ReadMatchedDigestsInSavepointAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, string escaped, long timeoutMs, double testDelaySeconds, CancellationToken cancellationToken)
    {
        const string savepoint = "find_queries_match";
        await transaction.SaveAsync(savepoint, cancellationToken);
        try
        {
            var armSql = "SET LOCAL statement_timeout = " + timeoutMs.ToString(CultureInfo.InvariantCulture);
            await using (var arm = new NpgsqlCommand(armSql, connection, transaction) { CommandTimeout = McpCommandDeadlines.ReadSeconds })
            {
                await arm.ExecuteNonQueryAsync(cancellationToken);
            }

            if (testDelaySeconds > 0)
            {
                var delaySql = "SELECT pg_sleep(" + testDelaySeconds.ToString(CultureInfo.InvariantCulture) + ")";
                await using var delay = new NpgsqlCommand(delaySql, connection, transaction) { CommandTimeout = McpCommandDeadlines.ReadSeconds };
                await delay.ExecuteNonQueryAsync(cancellationToken);
            }

            var matched = await ReadMatchedDigestsAsync(connection, transaction, escaped, cancellationToken);
            await transaction.ReleaseAsync(savepoint, cancellationToken);
            return matched;
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.QueryCanceled)
        {
            await transaction.RollbackAsync(savepoint, cancellationToken);
            return null;
        }
    }

    /// <summary>#8570: the one-server search's match read, started before the Query Store source on a connection of its own so the two run together.
    /// The plan-cache text index state comes first, read only when the option is on. When the index is <c>Ready</c>, <see cref="ReadMatchedDigestsAsync"/>
    /// runs in a transaction under its own timeout, the smaller of <see cref="SourceBudget.MatchMilliseconds"/> and the source's budget. <c>Probe</c> is
    /// false when the index is not <c>Ready</c> or its state could not be read, and the search reads as it does with the index off. <c>Matched</c> is
    /// null when the match read ran out of its timeout or failed, and the search takes the span read. A caller's cancellation is not caught.</summary>
    private static async Task<(bool Probe, List<byte[]>? Matched)> ReadMatchAheadAsync(
        NpgsqlDataSource postgres, QueryTextTrigramIndexTracker tracker, string escaped, SourceBudget budget, CancellationToken cancellationToken)
    {
        var option = tracker.Read();
        if (!option.Configured || !option.Enabled)
        {
            return (false, null);
        }

        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            var health = await QueryTextTrigramIndex.ReadHealthAsync(connection, tracker, cancellationToken);
            if (health.Status != TrigramIndexStatus.Ready)
            {
                return (false, null);
            }

            try
            {
                await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
                var timeout = Math.Max(1, Math.Min(budget.MatchMilliseconds, budget.Milliseconds));
                var setSql = "SET LOCAL statement_timeout = " + timeout.ToString(CultureInfo.InvariantCulture);
                await using (var set = new NpgsqlCommand(setSql, connection, transaction) { CommandTimeout = McpCommandDeadlines.ReadSeconds })
                {
                    await set.ExecuteNonQueryAsync(cancellationToken);
                }

                budget.MatchStarting?.Invoke();
                if (budget.MatchTestDelaySeconds > 0)
                {
                    var delaySql = "SELECT pg_sleep(" + budget.MatchTestDelaySeconds.ToString(CultureInfo.InvariantCulture) + ")";
                    await using var delay = new NpgsqlCommand(delaySql, connection, transaction) { CommandTimeout = McpCommandDeadlines.ReadSeconds };
                    await delay.ExecuteNonQueryAsync(cancellationToken);
                }

                var matched = await ReadMatchedDigestsAsync(connection, transaction, escaped, cancellationToken);
                await transaction.CommitAsync(cancellationToken);
                return (true, matched);
            }
            catch (Exception ex) when (ex is not OperationCanceledException && !cancellationToken.IsCancellationRequested)
            {
                return (true, null);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException && !cancellationToken.IsCancellationRequested)
        {
            return (false, null);
        }
    }
}
