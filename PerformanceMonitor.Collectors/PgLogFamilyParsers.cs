/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
       double-quotes in authentication and startup failures. Identifiers, not values; kept by the redactor
       and lifted here so a FATAL connection storm groups by who was refused. */
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
/// context are stored redacted, and survive it because they are pids, modes and identifiers after a noun;
/// the statement is fingerprinted and never stored. (The first draft claimed the context survived while
/// the row had no column for it — review caught the gap, and the column exists because of it.)</para>
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
/// The RECOGNISED-ONLY families (#3601): <c>temp_file</c>, <c>autovacuum</c>, <c>checkpoint</c>. One
/// parser, three message shapes, no structure lifted — each is stored as a generic event under its own
/// family name so the read can filter on it today, and so #3602 / #3603 land their structured parsers and
/// tables as additions. When one of those lands, its family leaves this list and gains a class of its own;
/// the rows already stored keep their family value and need no relabelling.
///
/// <para>The shapes: <c>temporary file: path "base/pgsql_tmp/pgsql_tmp123.0", size 1234567</c>
/// (<c>log_temp_files</c>); <c>automatic vacuum of table "db.schema.t": ...</c> and <c>automatic analyze of
/// table ...</c>, including the <c>aggressive</c> and <c>to prevent wraparound</c> variants
/// (<c>log_autovacuum_min_duration</c>); <c>checkpoint starting: ...</c> / <c>checkpoint complete: ...</c>
/// and their <c>restartpoint</c> twins on a standby (<c>log_checkpoints</c>, on by default since 15).</para>
/// </summary>
public sealed class PgRecognisedFamilyParser : IPgLogFamilyParser
{
    private static readonly Regex s_tempFile = new(@"^temporary file: ", RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly Regex s_autovacuum = new(@"^automatic (?:aggressive )?(?:vacuum|analyze)(?: to prevent wraparound)? of table ", RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly Regex s_checkpoint = new(@"^(?:checkpoint|restartpoint) (?:starting|complete):", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>Reported as the family it would emit for a given entry, or null; the classifier reads the family off the event.</summary>
    public string Family => PgLogFamilies.Other;

    /// <summary>Which recognised family this LOG message is, or null when it is none of the three.</summary>
    public static string? FamilyOf(in PgLogEntry entry)
    {
        if (!string.Equals(entry.Severity, "LOG", StringComparison.Ordinal))
        {
            return null;
        }

        if (s_tempFile.IsMatch(entry.Message)) return PgLogFamilies.TempFile;
        if (s_autovacuum.IsMatch(entry.Message)) return PgLogFamilies.Autovacuum;
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
