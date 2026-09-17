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
       server's reports survive between cycles, small enough not to move the whole file every time. */
    private const string TailBytesLiteral = "4194304";

    /* pg_ls_logdir() for the CURRENT file rather than a configured name — log_filename is a strftime
       pattern, so the real name is only knowable by asking.

       The DIRECTORY is asked for on the same grounds. pg_ls_logdir() returns bare names relative to
       log_directory, and pg_read_file resolves a relative path against the data directory, so
       current_setting('log_directory') is right in both regimes: a relative setting concatenates to a path
       under the data directory, and an absolute one — which some installers pick, to keep logs on their own
       volume — resolves as itself and is readable, because pg_read_file admits an absolute path under
       log_directory even when log_directory sits outside the data directory. Hardcoding 'log/' is correct
       only where log_directory holds its default, and elsewhere raises 58P01 for the file this same query
       just listed (#3410). StoreLogSweep.ReadFileSql builds the store's own log path the same way.

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

       The 'n' flag makes ^ match at line starts. Parsing of the block itself is C#, in
       PgDeadlockLogParser, so the RDS transport — which receives log TEXT and runs no SQL — shares it, and
       both routes get the same zone refusal from the same code.

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
       for an empty directory, rather than a failure or a silent zero. */
    private const string QueryText = @"
WITH newest AS (
    SELECT name, size
    FROM pg_catalog.pg_ls_logdir()
    WHERE pg_catalog.current_setting('logging_collector') = 'on'
    ORDER BY modification DESC
    LIMIT 1
),
tail AS (
    SELECT pg_catalog.pg_read_file(
               pg_catalog.current_setting('log_directory') || '/' || n.name,
               greatest(n.size - " + TailBytesLiteral + @", 0),
               " + TailBytesLiteral + @") AS body
    FROM newest AS n
)
SELECT
    m[1]    AS occurred_at_text,
    m[2]    AS log_zone_text,
    m[3]    AS victim_pid_text,
    m[4]    AS detail_body
FROM tail,
     regexp_matches(
         tail.body,
         '^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d+) ([^ \n]+) \[(\d+)\][^\n]*ERROR:  deadlock detected\s*\n[^\n]*DETAIL:  ((?:[^\n]*\n)(?:\t[^\n]*\n)*)',
         'gn') AS m
UNION ALL
SELECT '" + PgLoggingCollectorOffException.Marker + @"', NULL, NULL, NULL
WHERE pg_catalog.current_setting('logging_collector') <> 'on'
LIMIT 500";

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

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

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
        /* The DETAIL block verbatim. The parsed columns are an interpretation; this is the evidence, and a
           report shape the parser does not recognise yet is still readable by a person. */
        new CollectorColumn("graph_text", CollectorColumnType.Varchar),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
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

            /* Ordinals track the query's four columns: stamp, zone, victim pid, DETAIL. The zone is
               ordinal 1 and reaching the parser is what makes the stamp's meaning checked rather than
               assumed; a non-zero-offset one throws out of here, and the worker records the refusal
               against log_timezone instead of storing a shifted occurred_at. The throw abandons rows
               already read in this batch, which is the trade PgDeadlockLogParser.Extract's remarks argue
               for: a partial history from a target declared unreadable is worse for the reader than a
               refusal that says one thing. */
            var parsed = PgDeadlockLogParser.FromBlock(
                firstColumn,
                reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3));

            /* A block that will not parse is skipped rather than reported. The window is bounded, so a
               report cut in half at its edge is ordinary and is read whole on the next overlapping pass. */
            if (parsed is null)
            {
                continue;
            }

            rows.Add(new Row(
                OccurredAtUtc: parsed.Value.OccurredAtUtc,
                VictimPid: parsed.Value.VictimPid,
                ParticipantCount: parsed.Value.ParticipantCount,
                DeadlockHash: parsed.Value.DeadlockHash,
                LockModes: parsed.Value.LockModes,
                Resources: parsed.Value.Resources,
                VictimStatement: parsed.Value.VictimStatement,
                GraphText: parsed.Value.GraphText));
        }

        return rows;
    }

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
