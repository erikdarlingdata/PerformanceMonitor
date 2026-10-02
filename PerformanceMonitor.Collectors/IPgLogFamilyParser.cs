/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// One log family's parser (#3601): the per-family seam of the log-event pipeline. A family is a KIND of
/// line PostgreSQL writes — an error, a connection, a lock-wait report — and its parser is the one place
/// that knows how to recognise that kind and what structure to lift out of it.
///
/// <para><b>What implementing one costs, and does not.</b> A parser sees a <see cref="PgLogEntry"/> — the
/// prefix already read, the zone already checked, the companion lines already attached — and returns a
/// <see cref="PgLogEvent"/> through <see cref="PgLogEvent.From"/>, which normalizes the SQL. It does not know
/// which transport the entry came from, does not touch a cursor, and cannot store a statement's text. #3602
/// (<c>log_temp_files</c>) and #3603 (<c>log_autovacuum_min_duration</c>) each added ONE class implementing
/// this and one line in <see cref="PgLogEventClassifier.DefaultParsers"/>, exactly as planned; the
/// structure they lift rides the same row as nullable columns (<see cref="PgLogEventMetrics"/>, V130)
/// rather than sibling tables — the seam did not change for them.</para>
///
/// <para><b>Order matters and the classifier owns it.</b> Parsers are consulted in registration order and
/// the first to accept an entry wins. The error family goes first, on severity, so a <c>FATAL</c>
/// connection failure is an error event carrying its SQLSTATE rather than a connection event that
/// happens to be fatal; the LOG-level families follow, on message shape.</para>
/// </summary>
public interface IPgLogFamilyParser
{
    /// <summary>The family name this parser emits — one of <see cref="PgLogFamilies.All"/>.</summary>
    string Family { get; }

    /// <summary>
    /// Whether this entry is one of this family's, and if so the event to store. False means "not mine";
    /// the classifier asks the next parser. A parser must not return true for an entry it did not
    /// recognise merely because it could build a row from it — that is how a generic family swallows a
    /// specific one registered after it.
    /// </summary>
    bool TryParse(in PgLogEntry entry, out PgLogEvent logEvent);
}

/// <summary>
/// The family vocabulary of <c>collect.pg_log_events</c> (#3601): the closed set the <c>family</c> column
/// takes, the read's <c>family</c> filter validates against, and the census of which are PARSED and which
/// are RECOGNISED-ONLY tonight.
///
/// <para><b>Five parsed, one recognised, one reserved.</b> <see cref="Error"/>, <see cref="Connection"/>
/// and <see cref="LockWait"/> shipped with parsers in #3601 — three implementers of the seam, so the seam
/// was proven before a sibling added a fourth. <see cref="TempFile"/> (#3602) and <see cref="Autovacuum"/>
/// (#3603) were REGISTERED families the classifier recognised by message shape and stored as generic events
/// until their parsers landed; they now lift their numbers into the V130 columns, and the rows stored while
/// they were recognised-only keep their family value with those columns null — nothing was relabelled.
/// <see cref="Checkpoint"/> is still recognised-only: stored under its own name, nothing lifted, its
/// structure a family for a later issue on the same seam. <see cref="Other"/> is reserved for an entry a
/// parser accepts without naming a family; no parser does, so it is a vocabulary word rather than a row.</para>
///
/// <para><b>What is NOT a family, deliberately.</b> An unrecognised LOG / INFO / NOTICE line — "database
/// system is ready", a checkpoint's siblings, the thousand shapes a server writes at default settings —
/// is DROPPED, not stored as <c>other</c>. Storing it would make this table a copy of the log, at a volume
/// the retention below was not sized for, and the whole point of classifying is to keep what a reader can
/// ask a question about. A line that matters and is not here is a family to add, not a firehose to open.</para>
/// </summary>
public static class PgLogFamilies
{
    /// <summary>WARNING or worse, whatever the message — the "check the error log" family.</summary>
    public const string Error = "error";

    /// <summary><c>log_connections</c> / <c>log_disconnections</c>: received, authorized, disconnection with session time.</summary>
    public const string Connection = "connection";

    /// <summary><c>log_lock_waits</c>: "process N still waiting for ..." and the "acquired ... after" that ends it.</summary>
    public const string LockWait = "lock_wait";

    /// <summary><c>log_temp_files</c>: one event per spilled file, <c>bytes</c> lifted, the statement fingerprinted (#3602).</summary>
    public const string TempFile = "temp_file";

    /// <summary><c>log_autovacuum_min_duration</c>: one event per completed run — relation, duration, pages, tuples, buffers, WAL (#3603).</summary>
    public const string Autovacuum = "autovacuum";

    /// <summary><c>log_checkpoints</c>: recognised, stored generic.</summary>
    public const string Checkpoint = "checkpoint";

    /// <summary>Reserved: a parser that accepts an entry without a family of its own. Unused tonight.</summary>
    public const string Other = "other";

    /// <summary>The closed set, in the order the read's description lists them.</summary>
    public static IReadOnlyList<string> All { get; } = new[]
    {
        Error, Connection, LockWait, TempFile, Autovacuum, Checkpoint, Other,
    };

    /// <summary>The families with a structured parser — #3601's three, then #3602's and #3603's.</summary>
    public static IReadOnlyList<string> Parsed { get; } = new[] { Error, Connection, LockWait, TempFile, Autovacuum };

    /// <summary>The families recognised by message shape and stored generic, awaiting an issue of their own.</summary>
    public static IReadOnlyList<string> RecognisedOnly { get; } = new[] { Checkpoint };

    /// <summary>
    /// Whether a caller-supplied family name is one a reader can ask for. Ordinal, lower-case: the column
    /// is. <see cref="Other"/> is deliberately NOT known here even though it is in <see cref="All"/>: no
    /// parser emits it, so a filter on it would answer "no events" for a family that cannot have any, which
    /// is the silent shape a typo gets refused for — review caught the asymmetry with the read's own error
    /// message, which lists the six askable families and not this one.
    /// </summary>
    public static bool IsKnown(string? family)
    {
        if (string.IsNullOrWhiteSpace(family) || string.Equals(family, Other, StringComparison.Ordinal))
        {
            return false;
        }

        foreach (var known in All)
        {
            if (string.Equals(known, family, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
