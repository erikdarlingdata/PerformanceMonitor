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
/// One CLASSIFIED log event, in the shape <c>collect.pg_log_events</c> stores (#3601). Built only by a
/// family parser from a <see cref="PgLogEntry"/>, and only through <see cref="From"/>, which is where the SQL
/// PostgreSQL writes into the entry meets <see cref="PgLogTextRedactor"/> — a parser cannot construct a row that
/// keeps a statement's text, an auto_explain plan, or a literal of the SQL frames PostgreSQL writes into a DETAIL
/// or CONTEXT. The prose is stored as PostgreSQL wrote it (#3944).
///
/// <para><b>Generic first, structured where a family has structure.</b> The leading columns are the ones
/// EVERY family has — when, who, how bad, what it said — plus a statement fingerprint and an identity
/// hash. #3601 planned a family's own numbers (a spill's byte count, an autovacuum run's page counts) as a
/// sibling TABLE fed by a sibling parser; #3602 / #3603 landed them instead as NULLABLE COLUMNS on this one
/// row (<see cref="PgLogEventMetrics"/>, V130), and the reason is the reader's question rather than the
/// schema's tidiness: "everything that happened at 03:07" and "what did the 03:07 spill cost" are one
/// query over one table with one identity, one dedupe and one retention, where a sibling table would have
/// been a second cursor's worth of overlap to reconcile and a join on a hash for every page. The family
/// column still says which of these columns can be non-null: <c>bytes</c> for a spill, the relation and
/// the run figures for an autovacuum, nothing for the rest. A nullable no-default column on a compressed
/// hypertable is catalog-only in PostgreSQL and TimescaleDB, so the widening cost the store nothing.</para>
/// </summary>
/// <param name="OccurredAtUtc">When PostgreSQL wrote the line. Kind is Utc.</param>
/// <param name="Family">One of <see cref="PgLogFamilies.All"/>.</param>
/// <param name="Severity">PostgreSQL's label as written.</param>
/// <param name="SqlState">From a <c>%e</c> prefix, or null. The default prefix carries none.</param>
/// <param name="DatabaseName">From the prefix or the message, or null.</param>
/// <param name="UserName">From the prefix or the message, or null.</param>
/// <param name="ApplicationName">From the message where it names one (connection authorized), or null.</param>
/// <param name="Pid">The backend that wrote the line.</param>
/// <param name="Message">PostgreSQL's prose, as written (#3944); an auto_explain plan report keeps only its duration
/// line, and a syntax error's quoted SQL is normalized (#3996's review).</param>
/// <param name="Detail">As written, with the SQL PostgreSQL writes into a DETAIL (a deadlock's queries, a crash's
/// query) normalized; or null.</param>
/// <param name="Context">As written, with the statement an SQL frame quotes normalized; or null — the
/// <c>CONTEXT:</c> companion: for a lock wait the tuple
/// and relation the waiter was on (<c>while updating tuple (0,7) in relation "orders"</c>), for an error
/// inside a function the frame (<c>PL/pgSQL function f() line 3 at RAISE</c>). Review caught the first
/// draft parsing it and dropping it while a doc comment claimed it was stored.</param>
/// <param name="StatementFingerprint">Hash of the REDACTED statement, or null where the entry had none.</param>
/// <param name="RawLineHash">Identity across sightings. See <see cref="PgLogTextRedactor.RawLineHash"/>.</param>
/// <param name="Metrics">The family's lifted numbers (#3602 spill bytes, #3603 autovacuum run figures), or
/// <see cref="PgLogEventMetrics.None"/> for a family with none. Every member nullable; the store columns are.</param>
public readonly record struct PgLogEvent(
    DateTime OccurredAtUtc,
    string Family,
    string Severity,
    string? SqlState,
    string? DatabaseName,
    string? UserName,
    string? ApplicationName,
    int Pid,
    string Message,
    string? Detail,
    string? Context,
    string? StatementFingerprint,
    string RawLineHash,
    PgLogEventMetrics Metrics)
{
    /// <summary><see cref="PgLogEntry.RankOf"/> over <see cref="Severity"/>: seriousness order, not <c>log_min_messages</c>'.</summary>
    public int SeverityRank => PgLogEntry.RankOf(Severity);

    /// <summary>
    /// The ONE constructor path. Takes the raw entry and the family's structured additions, normalizes the SQL
    /// in its DETAIL and CONTEXT, fingerprints the statement, hashes the raw entry. A parser supplies the family
    /// name and whatever it extracted that the prefix did not carry; it never supplies stored text of its own.
    /// </summary>
    /// <param name="entry">The assembled, unredacted entry.</param>
    /// <param name="family">The family this parser claims it for.</param>
    /// <param name="databaseName">Overrides the prefix's, where the message named one (connection lines).</param>
    /// <param name="userName">Overrides the prefix's, where the message named one.</param>
    /// <param name="applicationName">From the message, where it named one.</param>
    /// <param name="metrics">The family's lifted numbers, where it has any (#3602, #3603).
    /// <see cref="PgLogEventMetrics.RelationName"/> is an identifier PostgreSQL wrote after the noun
    /// <c>table</c>, lifted out of the <c>message</c> that is stored as written anyway — the same standing
    /// <paramref name="databaseName"/> and <paramref name="userName"/> have. Every source clause is engine prose
    /// with engine numbers.</param>
    public static PgLogEvent From(
        in PgLogEntry entry,
        string family,
        string? databaseName = null,
        string? userName = null,
        string? applicationName = null,
        PgLogEventMetrics metrics = default)
    {
        var redactedStatement = PgLogTextRedactor.RedactStatement(entry.Statement);

        return new PgLogEvent(
            OccurredAtUtc: entry.OccurredAtUtc,
            Family: family,
            Severity: entry.Severity,
            SqlState: entry.SqlState,
            DatabaseName: databaseName ?? entry.DatabaseName,
            UserName: userName ?? entry.UserName,
            ApplicationName: applicationName,
            Pid: entry.Pid,
            /* #3944: the message, and the prose of the detail and context, as PostgreSQL wrote them. #3920: a
               DETAIL can carry other sessions' SQL (a deadlock's `Process N: query` lines, a crash's `Failed
               process was running: query`), and a CONTEXT the statement a function was running (`SQL statement
               "UPDATE ... WHERE id = 42"`); that SQL is normalized like a stored statement, and so is the token a
               syntax error quotes, in any catalogue's words (#3996's reviews, #4006). A deadlock report's later
               queries are read past one cut inside a literal only when the entry proves its DETAIL whole. The HINT
               companion is deliberately NOT stored: it is advice text, never evidence, and nothing here claims
               otherwise. */
            Message: PgLogTextRedactor.RedactMessage(entry.Message) ?? string.Empty,
            Detail: PgLogTextRedactor.RedactDetail(entry.Detail, entry.DetailComplete),
            Context: PgLogTextRedactor.RedactContext(entry.Context),
            StatementFingerprint: PgLogTextRedactor.Fingerprint(redactedStatement),
            RawLineHash: PgLogTextRedactor.RawLineHash(entry.RawText),
            Metrics: metrics);
    }
}

/// <summary>
/// The numbers a family parser lifts out of its line (#3602, #3603) — the V130 columns of
/// <c>collect.pg_log_events</c>, every one nullable, every one null for a family that has none.
///
/// <para><b>Two families populate it, disjointly.</b> A <c>temp_file</c> event carries <see cref="Bytes"/>
/// and nothing else: <c>log_temp_files</c> writes one line per file with its size, and the statement that
/// spilled is already fingerprinted on the generic row, so (fingerprint, bytes, time) is the per-execution
/// attribution the issue asked for. An <c>autovacuum</c> event carries <see cref="RelationName"/>,
/// <see cref="IsAnalyze"/>, <see cref="DurationMs"/> and whichever of the page / tuple / buffer / WAL
/// figures the line had — an autoanalyze line has no pages or tuples clause, and an autovacuum line before
/// PostgreSQL 18 has no WAL <i>buffers full</i> term (not stored; the four members here are the ones every
/// version from 16 writes). A null member on an autovacuum row therefore means "this line did not carry
/// the clause", never zero; the parsers do not fabricate a figure a version did not print.</para>
///
/// <para><b>What is deliberately NOT lifted.</b> <c>avg read rate</c> / <c>avg write rate</c> — derived
/// figures (bytes over elapsed) a reader can recompute from what IS stored (misses, dirtied, duration) and
/// that PostgreSQL itself prints as <c>0.000</c> for any run under a second; the CPU user/system split,
/// which is a detail of the worker rather than of the table; the per-index lines, which are one row per
/// index and belong to an index table if anything ever wants them; the <c>removable cutoff</c> and
/// <c>relfrozenxid</c> lines, which <c>pg_wraparound_stats</c> already samples from the catalog. Everything
/// lifted here is what a per-table cost history needs and no more.</para>
/// </summary>
/// <param name="RelationName"><c>schema.table</c> for an autovacuum / autoanalyze run — the database part
/// of PostgreSQL's <c>"db.schema.table"</c> goes to the row's <c>database_name</c>. Null for a spill: the
/// path in a <c>log_temp_files</c> line names a pgsql_tmp file, not a relation.</param>
/// <param name="Bytes">The spilled file's size in bytes (<c>size N</c>). Null on every other family.</param>
/// <param name="DurationMs">The run's wall-clock time, from <c>elapsed: N.NN s</c> — centisecond precision is
/// all PostgreSQL prints, stored as whole milliseconds.</param>
/// <param name="PagesRemoved"><c>pages: N removed</c> — heap pages truncated off the end of the relation.</param>
/// <param name="PagesRemaining"><c>pages: …, N remain</c> — the relation's size in pages after the run.</param>
/// <param name="TuplesRemoved"><c>tuples: N removed</c> — dead tuples actually reclaimed.</param>
/// <param name="TuplesRemaining"><c>tuples: …, N remain</c> — live tuples the run estimated it left.</param>
/// <param name="BufferHits"><c>buffer usage: N hits</c>.</param>
/// <param name="BufferMisses"><c>buffer usage: …, N misses</c> on 16 and 17; PostgreSQL 18 renamed the term
/// <c>reads</c> and this member reads either — one column, because it is one quantity.</param>
/// <param name="BufferDirtied"><c>buffer usage: …, N dirtied</c>.</param>
/// <param name="WalRecords"><c>WAL usage: N records</c>.</param>
/// <param name="WalBytes"><c>WAL usage: …, N bytes</c>.</param>
/// <param name="IsAnalyze">False for <c>automatic vacuum of table</c> (including the aggressive and
/// to-prevent-wraparound variants), true for <c>automatic analyze of table</c>. Null on every other family.</param>
public readonly record struct PgLogEventMetrics(
    string? RelationName = null,
    long? Bytes = null,
    long? DurationMs = null,
    long? PagesRemoved = null,
    long? PagesRemaining = null,
    long? TuplesRemoved = null,
    long? TuplesRemaining = null,
    long? BufferHits = null,
    long? BufferMisses = null,
    long? BufferDirtied = null,
    long? WalRecords = null,
    long? WalBytes = null,
    bool? IsAnalyze = null)
{
    /// <summary>Every member null — the value of every family without numbers of its own.</summary>
    public static PgLogEventMetrics None => default;
}
