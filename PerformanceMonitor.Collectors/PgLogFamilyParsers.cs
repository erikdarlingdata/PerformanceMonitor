/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The <c>error</c> family (#3601): any entry at WARNING or worse, whatever it says.
///
/// <para><b>Severity is the whole recogniser, on purpose.</b> A SQL Server DBA's "check the error log"
/// means "show me everything the engine thought was wrong", and PostgreSQL's severity label is exactly
/// that judgment, made by the server. Classifying on message text instead would enumerate error shapes
/// and miss the one that mattered. The cost is that a deadlock's <c>ERROR:  deadlock detected</c> lands
/// here too, beside its full graph in <c>pg_deadlocks</c> — one event, two tables, and the second is the
/// evidence. Correct, and said in the read's description.</para>
///
/// <para><b>First in the registration order</b>, so a <c>FATAL:  password authentication failed</c> is an
/// error carrying its SQLSTATE and user, not a connection event. What the prefix did not carry (no
/// <c>%u@%d</c>) the message sometimes does — <c>for user "app_rw"</c> — and that is lifted.</para>
/// </summary>
public sealed class PgErrorEventParser : IPgLogFamilyParser
{
    /* `for user "name"` / `role "name"` / `database "name" does not exist` — the identifiers PostgreSQL
       double-quotes in authentication and startup failures, lifted here so a FATAL connection storm groups by
       who was refused. */
    private static readonly Regex s_forUser = new(
        @"\b(?:for user|role) ""(?<user>[^""]+)""", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_forDatabase = new(
        @"\bdatabase ""(?<db>[^""]+)""", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public string Family => PgLogFamilies.Error;

    public bool TryParse(in PgLogEntry entry, out PgLogEvent logEvent)
    {
        if (PgLogEntry.RankOf(entry.Severity) < PgLogEntry.RankOf("WARNING"))
        {
            logEvent = default;
            return false;
        }

        var user = s_forUser.Match(entry.Message);
        var database = s_forDatabase.Match(entry.Message);

        logEvent = PgLogEvent.From(
            entry,
            Family,
            databaseName: database.Success ? database.Groups["db"].Value : null,
            userName: user.Success ? user.Groups["user"].Value : null);
        return true;
    }
}

/// <summary>
/// The <c>connection</c> family (#3601): what <c>log_connections</c> and <c>log_disconnections</c> write.
///
/// <para>Five shapes across PostgreSQL 16–18, all at LOG: <c>connection received: host=... port=...</c>,
/// <c>connection authenticated: identity="..." method=...</c>, <c>connection authorized: user=... database=...
/// application_name=...</c> (with an SSL clause where TLS is on), 18's <c>connection ready: setup total=...
/// ms, ...</c>, and <c>disconnection: session time: H:MM:SS.fff user=... database=... host=...</c>. Plus
/// <c>replication connection authorized</c>, which is a connection too. Recognised by the message's opening
/// words; user, database and application name are lifted from the <c>key=value</c> pairs where present,
/// which is how this family carries an identity the default prefix does not.</para>
///
/// <para><b>The volume family.</b> A pool that reconnects per statement writes three of these per query,
/// which is why <c>pg_log_events</c>' retention is thirty days rather than the deadlock table's ninety and
/// why the read filters by family — see <c>CollectorScheduleDefaults</c>. The session time on a
/// disconnection stays in the message text; a per-session duration column is a shape for a sibling table
/// if churn analysis ever wants to aggregate it.</para>
/// </summary>
public sealed class PgConnectionEventParser : IPgLogFamilyParser
{
    private static readonly Regex s_shape = new(
        @"^(?:replication )?(?:connection (?:received|authenticated|authorized|ready)|disconnection):",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_user = new(@"\buser=(?<v>\S+)", RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly Regex s_database = new(@"\bdatabase=(?<v>\S+)", RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly Regex s_application = new(@"\bapplication_name=(?<v>.+?)(?: SSL enabled| GSS |$)", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public string Family => PgLogFamilies.Connection;

    public bool TryParse(in PgLogEntry entry, out PgLogEvent logEvent)
    {
        if (!string.Equals(entry.Severity, "LOG", StringComparison.Ordinal) || !s_shape.IsMatch(entry.Message))
        {
            logEvent = default;
            return false;
        }

        var user = s_user.Match(entry.Message);
        var database = s_database.Match(entry.Message);
        var application = s_application.Match(entry.Message);

        logEvent = PgLogEvent.From(
            entry,
            Family,
            databaseName: database.Success ? database.Groups["v"].Value : null,
            userName: user.Success ? user.Groups["v"].Value : null,
            applicationName: application.Success ? application.Groups["v"].Value.Trim() : null);
        return true;
    }
}

/// <summary>
/// The <c>lock_wait</c> family (#3601): what <c>log_lock_waits</c> writes once a wait outlives
/// <c>deadlock_timeout</c> — the one SQL Server DBAs ask for first, because it is the blocked-process report
/// they already know, written by the engine rather than sampled.
///
/// <para>Four shapes, all at LOG, all opening <c>process N</c>: <c>still waiting for &lt;mode&gt; on
/// &lt;resource&gt; after N ms</c> (the report), <c>acquired &lt;mode&gt; on &lt;resource&gt; after N ms</c>
/// (the same wait ending — pair them by pid to get the wait's length), <c>avoided deadlock for ... by
/// rearranging queue order after N ms</c>, and <c>detected deadlock while waiting for ... after N ms</c>.
/// The <c>DETAIL</c> names the holder and the queue (<c>Process holding the lock: 4321. Wait queue:
/// 1234.</c>), the <c>STATEMENT</c> is the waiter's SQL, and the <c>CONTEXT</c> — where present — names the
/// tuple and relation (<c>while updating tuple (0,7) in relation "orders"</c>). The message, detail and
/// context are stored as PostgreSQL wrote them (#3944); the statement is fingerprinted and never stored. (The
/// first draft claimed the context survived while the row had no column for it — review caught the gap, and
/// the column exists because of it.)</para>
///
/// <para><b>What this is beside <c>pg_blocking</c>.</b> That collector SAMPLES <c>pg_blocking_pids()</c> on
/// a cadence and can miss a wait that starts and ends between samples; this is the server's own record of
/// every wait past the timeout, complete but only as detailed as one line. Read both: the sample has the
/// blocker's statement, the log has the count.</para>
/// </summary>
public sealed class PgLockWaitEventParser : IPgLogFamilyParser
{
    private static readonly Regex s_shape = new(
        @"^process \d+ (?:still waiting for|acquired|avoided deadlock for|detected deadlock while waiting for) ",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public string Family => PgLogFamilies.LockWait;

    public bool TryParse(in PgLogEntry entry, out PgLogEvent logEvent)
    {
        if (!string.Equals(entry.Severity, "LOG", StringComparison.Ordinal) || !s_shape.IsMatch(entry.Message))
        {
            logEvent = default;
            return false;
        }

        logEvent = PgLogEvent.From(entry, Family);
        return true;
    }
}

/// <summary>
/// The <c>temp_file</c> family (#3602): what <c>log_temp_files</c> writes when a sort, hash, or
/// materialisation outgrows <c>work_mem</c> and its spill file is deleted — one line per FILE, with the
/// exact size, and the <c>STATEMENT:</c> companion naming the statement that spilled.
///
/// <para><b>One shape, stable since 8.3, locale-safe:</b> <c>temporary file: path "base/pgsql_tmp/pgsql_tmp4102.0",
/// size 4294967296</c> (<c>fd.c</c>, <c>ReportTemporaryFileUsage</c>: <c>"temporary file: path \"%s\",
/// size %lu"</c>, identical in 16, 17 and 18). The size is an unsigned long of bytes with no separators —
/// PostgreSQL's own <c>snprintf</c> never applies a locale — so it parses invariantly. The path names a
/// pgsql_tmp file, not a relation: it carries the pid and a sequence number and nothing about the data,
/// which is why it stays in <c>message</c> and is NOT lifted into
/// <c>relation_name</c>. One statement can write several files (a parallel hash, a multi-batch sort), and
/// each is its own event with its own bytes; summing per fingerprint per second is a reader's job.</para>
///
/// <para><b>What this closes beside the counters.</b> <c>pg_stat_database.temp_bytes</c> (per database,
/// <c>pg_database_stats</c>) and <c>pg_stat_statements.temp_blks_written</c> (per statement shape,
/// <c>pg_statement_stats</c>) both know THAT spilling happened; only this line knows that THIS execution
/// at 03:07 wrote 4.2 GB — and the statement fingerprint on the row is the join back to the shape's
/// counters. The per-event grain is also what an honest spill-storm alert needs: a counter delta cannot
/// tell one 10 GB spill from ten 1 GB ones. That rule is a custom-alert source for later, not this
/// parser.</para>
///
/// <para><b>Recognised on the header, lifted where it parses.</b> Acceptance is the opening
/// <c>temporary file: </c>, the same recogniser <see cref="PgRecognisedFamilyParser"/> carried for this
/// family before #3602 — so every line that was stored as a recognised-only event yesterday is stored today,
/// with <c>bytes</c> where the size clause is read and null where a future variant is not.</para>
/// </summary>
public sealed class PgTempFileEventParser : IPgLogFamilyParser
{
    private static readonly Regex s_shape = new(@"^temporary file: ", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /* `size N` at the end of the line — the one number in the message. Anchored on the keyword rather
       than on the path's closing quote, because a path is an operating-system string and can carry a quote
       of its own. */
    private static readonly Regex s_size = new(@"\bsize (?<n>\d+)\s*$", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public string Family => PgLogFamilies.TempFile;

    public bool TryParse(in PgLogEntry entry, out PgLogEvent logEvent)
    {
        if (!string.Equals(entry.Severity, "LOG", StringComparison.Ordinal) || !s_shape.IsMatch(entry.Message))
        {
            logEvent = default;
            return false;
        }

        var size = s_size.Match(entry.Message);
        long? bytes = size.Success && long.TryParse(size.Groups["n"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : null;

        logEvent = PgLogEvent.From(entry, Family, metrics: new PgLogEventMetrics(Bytes: bytes));
        return true;
    }
}

/// <summary>
/// The <c>autovacuum</c> family (#3603): the completion report <c>log_autovacuum_min_duration</c> writes
/// for every automatic vacuum and analyze that ran at least that long — per run, what it COST, which is
/// the question <c>get_pg_autovacuum_health</c>'s catalog sample (dead tuples, last-run stamps, counts)
/// cannot answer.
///
/// <para><b>The line, per version, read from the source rather than remembered.</b> One entry: a header
/// line and tab-indented continuations, which the assembler folds into <see cref="PgLogEntry.Message"/>
/// separated by newlines. The header is <c>automatic [aggressive ]vacuum[ to prevent wraparound] of table
/// "db.schema.table": index scans: N</c> (<c>vacuumlazy.c</c>) or <c>automatic analyze of table
/// "db.schema.table"</c> (<c>analyze.c</c>). The clauses lifted, and how they moved:</para>
/// <list type="bullet">
/// <item><c>pages: N removed, N remain, N scanned (P% of total)</c> on 16 and 17; <b>18 appends
/// <c>, N eagerly scanned</c></b>. Vacuum only. The first two terms are lifted; the rest is not.</item>
/// <item><c>tuples: N removed, N remain, N are dead but not yet removable</c> — unchanged 16–18, vacuum
/// only. First two terms lifted.</item>
/// <item><c>buffer usage: N hits, N misses, N dirtied</c> on 16 and 17; <b>18 renamed the middle term
/// <c>reads</c></b>. Both spellings land in <c>buffer_misses</c> — one quantity, one column. Vacuum and
/// analyze.</item>
/// <item><c>WAL usage: N records, N full page images, N bytes</c> on 16 and 17; <b>18 appends <c>, N buffers
/// full</c></b> and <b>18 is the first to write the clause on an analyze line at all</b>. Records and
/// bytes lifted; full-page images and buffers-full are not.</item>
/// <item><c>system usage: CPU: user: N.NN s, system: N.NN s, elapsed: N.NN s</c> (<c>pg_rusage.c</c>,
/// unchanged) — <c>elapsed</c> becomes <c>duration_ms</c>. The one clause every version writes on every
/// line, so <c>duration_ms</c> is the one figure a reader can rely on across the fleet.</item>
/// </list>
/// <para>Clauses this leaves in <c>message</c> and does not lift: <c>index scans</c>, <c>removable cutoff</c>,
/// <c>new relfrozenxid</c>, <c>frozen</c>, <c>visibility map</c> (18), the per-index lines, <c>I/O timings</c>
/// (only with <c>track_io_timing</c>), <c>avg read rate</c> / <c>avg write rate</c> (derived, and
/// <c>0.000</c> on any sub-second run), <c>delay time</c> (18, only with <c>track_cost_delay_timing</c>).
/// Each is matched by keyword and read independently, so a clause a version does not write, or a future
/// version reorders, leaves its columns null rather than failing the line — and a line at PostgreSQL 15's
/// one-line autoanalyze shape (<c>automatic analyze of table "…" system usage: CPU: …</c>) still yields
/// its relation and duration.</para>
///
/// <para><b>Numbers are invariant.</b> PostgreSQL prints every figure through its own <c>snprintf</c>,
/// which knows no locale: <c>.</c> is the decimal point and there are no grouping separators, whatever the
/// server's <c>lc_numeric</c>. Parsing is <see cref="CultureInfo.InvariantCulture"/> for that reason, and
/// pinned under a comma-decimal culture so a Windows host in de-DE reads <c>elapsed: 1.23 s</c> as 1230 ms
/// rather than 123000. (<c>lc_messages</c> is a different matter: a translated line matches no keyword and
/// the whole pipeline already requires English — the readiness read's <c>message_locale</c> facet.)</para>
///
/// <para><b>The relation is split, once.</b> PostgreSQL writes <c>"db.schema.table"</c>; the database goes
/// to the row's <c>database_name</c> (an autovacuum worker's prefix carries no <c>%d</c> the reader could
/// otherwise use) and <c>schema.table</c> to <c>relation_name</c>, which is the key
/// <c>get_pg_autovacuum_health</c> joins its per-table rows on. The split is on the FIRST dot only: a
/// schema or table name may itself contain a dot, and PostgreSQL does not quote the parts.</para>
///
/// <para><b>Recognised on the header, lifted where it parses</b> — the same header recogniser
/// <see cref="PgRecognisedFamilyParser"/> carried for this family before #3603, so no line stored yesterday
/// is dropped today; what changed is that the numbers come out of the prose into columns.</para>
/// </summary>
public sealed class PgAutovacuumEventParser : IPgLogFamilyParser
{
    /* The header: the recogniser, plus the two facts it carries — which kind of run, and of what. */
    private static readonly Regex s_header = new(
        @"^automatic (?:aggressive )?(?<kind>vacuum|analyze)(?: to prevent wraparound)? of table ""(?<rel>[^""]+)""",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /* Each clause by keyword, each independent; `(?m)` so `^` is a continuation line's head after the
       assembler's fold. Terms beyond the ones lifted are left to the line — see the class remarks. */
    private static readonly Regex s_pages = new(
        @"(?m)^pages: (?<removed>\d+) removed, (?<remain>\d+) remain\b",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_tuples = new(
        @"(?m)^tuples: (?<removed>\d+) removed, (?<remain>\d+) remain\b",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /* `misses` (16, 17) or `reads` (18): one quantity under two names. */
    private static readonly Regex s_buffers = new(
        @"(?m)^buffer usage: (?<hits>\d+) hits, (?<misses>\d+) (?:misses|reads), (?<dirtied>\d+) dirtied\b",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_wal = new(
        @"(?m)^WAL usage: (?<records>\d+) records, \d+ full page images, (?<bytes>\d+) bytes\b",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /* `elapsed: N.NN s` — the tail of pg_rusage_show, on every version's last line. Not anchored to the
       line head: on PostgreSQL 15 the whole analyze report is ONE line and this clause follows the
       relation on it. */
    private static readonly Regex s_elapsed = new(
        @"\belapsed: (?<s>\d+(?:\.\d+)?) s\b",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public string Family => PgLogFamilies.Autovacuum;

    public bool TryParse(in PgLogEntry entry, out PgLogEvent logEvent)
    {
        if (!string.Equals(entry.Severity, "LOG", StringComparison.Ordinal))
        {
            logEvent = default;
            return false;
        }

        var header = s_header.Match(entry.Message);

        if (!header.Success)
        {
            logEvent = default;
            return false;
        }

        var isAnalyze = string.Equals(header.Groups["kind"].Value, "analyze", StringComparison.Ordinal);
        var (database, relation) = SplitRelation(header.Groups["rel"].Value);

        var pages = s_pages.Match(entry.Message);
        var tuples = s_tuples.Match(entry.Message);
        var buffers = s_buffers.Match(entry.Message);
        var wal = s_wal.Match(entry.Message);
        var elapsed = s_elapsed.Match(entry.Message);

        var metrics = new PgLogEventMetrics(
            RelationName: relation,
            DurationMs: elapsed.Success ? ElapsedMilliseconds(elapsed.Groups["s"].Value) : null,
            PagesRemoved: Whole(pages, "removed"),
            PagesRemaining: Whole(pages, "remain"),
            TuplesRemoved: Whole(tuples, "removed"),
            TuplesRemaining: Whole(tuples, "remain"),
            BufferHits: Whole(buffers, "hits"),
            BufferMisses: Whole(buffers, "misses"),
            BufferDirtied: Whole(buffers, "dirtied"),
            WalRecords: Whole(wal, "records"),
            WalBytes: Whole(wal, "bytes"),
            IsAnalyze: isAnalyze);

        logEvent = PgLogEvent.From(entry, Family, databaseName: database, metrics: metrics);
        return true;
    }

    /// <summary><c>"db.schema.table"</c> → (<c>db</c>, <c>schema.table</c>); split on the first dot only. A
    /// value with no dot (not a shape PostgreSQL writes) is kept whole as the relation with no database.</summary>
    public static (string? Database, string Relation) SplitRelation(string qualified)
    {
        var dot = qualified.IndexOf('.', StringComparison.Ordinal);
        return dot <= 0 || dot == qualified.Length - 1
            ? (null, qualified)
            : (qualified[..dot], qualified[(dot + 1)..]);
    }

    /// <summary><c>elapsed: 12.34 s</c> → 12340. Invariant parse; rounded to whole milliseconds because
    /// centiseconds is all PostgreSQL prints and a fractional millisecond would be a claim it never made.</summary>
    public static long? ElapsedMilliseconds(string seconds)
        => double.TryParse(seconds, NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out var s)
            ? (long)Math.Round(s * 1000.0, MidpointRounding.AwayFromZero)
            : null;

    private static long? Whole(Match clause, string group)
        => clause.Success && long.TryParse(clause.Groups[group].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var n)
            ? n
            : null;
}

/// <summary>
/// The RECOGNISED-ONLY family (#3601): <c>checkpoint</c>. Recognised by message shape, stored as a generic
/// event under its own family name with nothing lifted, so the read can filter on it today. #3601 shipped
/// three families here; <c>temp_file</c> and <c>autovacuum</c> left the list when #3602 and #3603 gave them
/// classes of their own (<see cref="PgTempFileEventParser"/>, <see cref="PgAutovacuumEventParser"/>),
/// registered ahead of this arm. The rows those two families stored while they were recognised-only keep
/// their family value and need no relabelling; only their V130 columns are null.
///
/// <para>The shape: <c>checkpoint starting: ...</c> / <c>checkpoint complete: ...</c> and their
/// <c>restartpoint</c> twins on a standby (<c>log_checkpoints</c>, on by default since 15). A checkpoint's
/// structure — buffers written, WAL files added / removed / recycled, write / sync / total seconds,
/// distance and estimate — is a family for a later issue on the same seam, and it would leave this list
/// the same way.</para>
/// </summary>
public sealed class PgRecognisedFamilyParser : IPgLogFamilyParser
{
    private static readonly Regex s_checkpoint = new(@"^(?:checkpoint|restartpoint) (?:starting|complete):", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>Reported as the family it would emit for a given entry, or null; the classifier reads the family off the event.</summary>
    public string Family => PgLogFamilies.Other;

    /// <summary>Which recognised family this LOG message is, or null when it is none of them.</summary>
    public static string? FamilyOf(in PgLogEntry entry)
    {
        if (!string.Equals(entry.Severity, "LOG", StringComparison.Ordinal))
        {
            return null;
        }

        if (s_checkpoint.IsMatch(entry.Message)) return PgLogFamilies.Checkpoint;
        return null;
    }

    public bool TryParse(in PgLogEntry entry, out PgLogEvent logEvent)
    {
        var family = FamilyOf(entry);

        if (family is null)
        {
            logEvent = default;
            return false;
        }

        logEvent = PgLogEvent.From(entry, family);
        return true;
    }
}
