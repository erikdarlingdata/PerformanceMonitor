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
/// family parser from a <see cref="PgLogEntry"/>, and only through <see cref="From"/>, which is where every
/// text column meets <see cref="PgLogTextRedactor"/> — a parser cannot construct an unredacted row.
///
/// <para><b>Generic by design.</b> The columns are the ones EVERY family has — when, who, how bad, what it
/// said — plus a statement fingerprint and an identity hash. A family with structure of its own (a spill's
/// byte count, an autovacuum run's page counts) is a sibling TABLE fed by a sibling parser off the same
/// assembled entries (#3602, #3603); this row is the event log those sit beside, and the family column is
/// what lets a reader ask "everything that happened at 03:07" across all of them.</para>
/// </summary>
/// <param name="OccurredAtUtc">When PostgreSQL wrote the line. Kind is Utc.</param>
/// <param name="Family">One of <see cref="PgLogFamilies.All"/>.</param>
/// <param name="Severity">PostgreSQL's label as written.</param>
/// <param name="SqlState">From a <c>%e</c> prefix, or null. The default prefix carries none.</param>
/// <param name="DatabaseName">From the prefix or the message, or null.</param>
/// <param name="UserName">From the prefix or the message, or null.</param>
/// <param name="ApplicationName">From the message where it names one (connection authorized), or null.</param>
/// <param name="Pid">The backend that wrote the line.</param>
/// <param name="Message">REDACTED prose.</param>
/// <param name="Detail">REDACTED prose, or null.</param>
/// <param name="Context">REDACTED prose, or null — the <c>CONTEXT:</c> companion: for a lock wait the tuple
/// and relation the waiter was on (<c>while updating tuple (0,7) in relation "orders"</c>), for an error
/// inside a function the frame (<c>PL/pgSQL function f() line 3 at RAISE</c>). Review caught the first
/// draft parsing it and dropping it while a doc comment claimed it was stored.</param>
/// <param name="StatementFingerprint">Hash of the REDACTED statement, or null where the entry had none.</param>
/// <param name="RawLineHash">Identity across sightings. See <see cref="PgLogTextRedactor.RawLineHash"/>.</param>
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
    string RawLineHash)
{
    /// <summary><see cref="PgLogEntry.RankOf"/> over <see cref="Severity"/>: seriousness order, not <c>log_min_messages</c>'.</summary>
    public int SeverityRank => PgLogEntry.RankOf(Severity);

    /// <summary>
    /// The ONE constructor path. Takes the raw entry and the family's structured additions, redacts every
    /// text column, fingerprints the statement, hashes the raw entry. A parser supplies the family name and
    /// whatever it extracted that the prefix did not carry; it never supplies stored text of its own.
    /// </summary>
    /// <param name="entry">The assembled, unredacted entry.</param>
    /// <param name="family">The family this parser claims it for.</param>
    /// <param name="databaseName">Overrides the prefix's, where the message named one (connection lines).</param>
    /// <param name="userName">Overrides the prefix's, where the message named one.</param>
    /// <param name="applicationName">From the message, where it named one.</param>
    public static PgLogEvent From(
        in PgLogEntry entry,
        string family,
        string? databaseName = null,
        string? userName = null,
        string? applicationName = null)
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
            Message: PgLogTextRedactor.RedactMessage(entry.Message) ?? string.Empty,
            Detail: PgLogTextRedactor.RedactMessage(entry.Detail),
            /* Prose-strength redaction, like Detail: a CONTEXT can carry an inner statement — `SQL statement
               "UPDATE … WHERE id = 42"` — and that double-quoted run follows no identifier noun, so the
               allowlist takes it whole. The HINT companion is deliberately NOT stored: it is advice text,
               never evidence, and nothing here claims otherwise. */
            Context: PgLogTextRedactor.RedactMessage(entry.Context),
            StatementFingerprint: PgLogTextRedactor.Fingerprint(redactedStatement),
            RawLineHash: PgLogTextRedactor.RawLineHash(entry.RawText));
    }
}
