/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Classified PostgreSQL server-log events, read out of the log on the self-hosted route (#3601): errors,
/// connections, lock waits, temp-file spills with their bytes (#3602), autovacuum runs with their cost
/// (#3603), and the recognised-only checkpoint family, into <c>collect.pg_log_events</c>.
///
/// <para><b>The gap this closes.</b> The log is the engine's primary event record and Darling read exactly
/// two shapes out of it. A burst of <c>FATAL</c> connection failures, a statement cancelled by its client,
/// a lock wait past <c>deadlock_timeout</c> — each lands in the log and nowhere else, and each was
/// invisible between two samples of a cumulative counter. The SQL Server DBA's "check the error log" had
/// no answer here; <c>get_pg_log_events</c> is that answer.</para>
///
/// <para><b>Same tail, same gate, same refusals as the two readers before it.</b> The query opens with
/// <see cref="PgServerLogTail.TailCteSql"/> — the same file, the same 4 MB, the same
/// <c>logging_collector</c> gate and marker row (#3410), the same <c>log_directory</c> resolution — and the
/// zone check is <see cref="PgDeadlockLogParser.IsZeroOffsetLogZone"/> through the assembler (#2993). An
/// operator who has made deadlock capture work on a target has made this work too; the readiness facets
/// are the same three (readable, verbose enough, English).</para>
///
/// <para><b>What this collector deliberately does differently: the BODY crosses the wire.</b> The plan and
/// deadlock readers run their block regex server-side so only matched blocks return. This one returns
/// <c>tail.body</c> whole and classifies in C#, because the classifier IS the thing being shared — a
/// server-side pre-filter would be a second spelling of every family's recogniser, in SQL, kept in step by
/// hand, which is the defect this pipeline exists to end. The cost is up to 4 MB per cycle per target on
/// the monitoring connection, at the deadlock cadence; the target-side READ cost is unchanged, because
/// the deadlock collector already pulls the same 4 MB through <c>pg_read_file</c> every five minutes and
/// this is the same call. A busy target's operator lowers the cadence per server as for deadlocks.</para>
///
/// <para><b>What the target has to have on for each family to carry anything.</b> <c>error</c> needs only
/// <c>log_min_messages</c> at WARNING or lower (the default); <c>connection</c> needs
/// <c>log_connections</c> / <c>log_disconnections</c>; <c>lock_wait</c> needs <c>log_lock_waits</c>;
/// <c>temp_file</c> needs <c>log_temp_files</c>; <c>autovacuum</c> needs <c>log_autovacuum_min_duration</c>;
/// <c>checkpoint</c> needs <c>log_checkpoints</c> (on by default since 15). This collector does NOT audit
/// those settings — #3607 is that audit, running against <c>pg_server_config</c>'s snapshot — so an
/// all-off target here reads as quiet, and the read's empty branch names the settings.</para>
///
/// <para><b>SQL normalization is not optional and not here.</b> The SQL PostgreSQL writes into a DETAIL or a
/// CONTEXT passes through <see cref="PgLogTextRedactor"/> inside <see cref="PgLogEvent.From"/>, the only
/// constructor path, and the prose around it is stored as PostgreSQL wrote it (#3944). The <c>STATEMENT</c>
/// companion — the user's SQL with its literals — is never stored at all, only fingerprinted.</para>
/// </summary>
public sealed class PgLogEventsCollector : PostgresCollectorDefinitionBase<PgLogEvent>
{
    public static PgLogEventsCollector Instance { get; } = new();

    private PgLogEventsCollector()
    {
    }

    /* The tailer, shared byte-for-byte with PgPlanCaptureCollector and PgDeadlocksCollector — see
       PgServerLogTail for the full argument. This query's own part is one column: the body, whole. Both
       marker rows ride their own UNION ALL arm, spelled in this query's one column (#3997: the second arm
       is the csvlog/jsonlog-only gap, mutually exclusive with the first by construction). The second column is
       the target's log_timezone, read with the body (#4046): see ReadAsync. */
    private const string QueryText = PgServerLogTail.TailCteSql + @"
SELECT tail.body AS log_body,
       " + PgServerLogTail.LogTimezoneSql + @" AS log_timezone
FROM tail
UNION ALL
SELECT '" + PgLoggingCollectorOffException.Marker + @"', NULL
WHERE " + PgServerLogTail.LoggingCollectorOffMarkerSql + @"
UNION ALL
SELECT '" + PgNoStderrLogFileException.Marker + @"', NULL
WHERE " + PgServerLogTail.NoStderrLogFileMarkerSql;

    /* The binary-route twin (#4046 part 1c), sent instead of QueryText once PgReadBinaryFileCapability
       finds the grant. tail.body is bytea here, so the marker arms are cast through convert_to rather than
       left as a bare text literal — a UNION ALL between bytea and an "unknown"-typed string literal would
       ask PostgreSQL to parse the marker text AS bytea input, which it is not. convert_to produces the
       marker's own UTF-8 bytes instead, and ReadAsync decodes column 0 the same way whichever arm produced
       it, so the marker comparison downstream never has to know which route ran. */
    private const string BinaryQueryText = PgServerLogTail.TailCteBinarySql + @"
SELECT tail.body AS log_body,
       " + PgServerLogTail.LogTimezoneSql + @" AS log_timezone
FROM tail
UNION ALL
SELECT pg_catalog.convert_to('" + PgLoggingCollectorOffException.Marker + @"', 'UTF8'), NULL
WHERE " + PgServerLogTail.LoggingCollectorOffMarkerSql + @"
UNION ALL
SELECT pg_catalog.convert_to('" + PgNoStderrLogFileException.Marker + @"', 'UTF8'), NULL
WHERE " + PgServerLogTail.NoStderrLogFileMarkerSql;

    /* The csvlog pair (#4053 part a1b), sent instead of the two above once context.PgLogUsesCsvlog says the
       target's log_destination includes csvlog. Same shape as QueryText/BinaryQueryText, opened on
       PgServerLogTail.TailCsvCteSql/TailCsvCteBinarySql instead: the marker arms carry
       PgLoggingCollectorOffException.Marker for the shared "collector is off" state, and
       PgNoCsvlogFileException.Marker — not PgNoStderrLogFileException.Marker — for "on, but no .csv file
       yet", so the fault message this route throws names csvlog, never stderr. */
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

    public override string Name => "pg_log_events";

    public override string TargetTable => "pg_log_events";

    /// <summary>
    /// Every PostgreSQL target, for the reason its two siblings give: gating on the engine would report
    /// the log as a PERMANENT gap on Aurora and RDS, which is false — those targets reach the same table
    /// through the RDS log API (<c>RdsLogEventIngestor</c>). The route is chosen at dispatch.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    /// <summary>Server-wide: one log holds every database's events.</summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    /// <summary>
    /// The tail query, or a refusal when the host has no log-hash key (#4004): refused HERE, before the query runs, so a
    /// service whose key file could not be used never pulls 4 MB of log it could only hash without a key.
    /// </summary>
    /// <exception cref="InvalidOperationException">The context carries no <see cref="CollectorContext.LogHashKey"/>;
    /// the message is <see cref="PgLogHashKey.UnavailableMessage"/>, which the run records.</exception>
    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        _ = RequireKey(context);
        return new(context.PgLogUsesCsvlog
            ? (context.PgReadBinaryFileGranted ? CsvBinaryQueryText : CsvQueryText)
            : (context.PgReadBinaryFileGranted ? BinaryQueryText : QueryText));
    }

    private static PgLogHashKey RequireKey(CollectorContext context) =>
        context.LogHashKey ?? throw new InvalidOperationException(PgLogHashKey.UnavailableMessage);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("occurred_at", CollectorColumnType.Timestamp),
        /* One of PgLogFamilies.All. Text rather than an enum column because the vocabulary grows with the
           sibling issues and a CHECK would make each a rung. */
        new CollectorColumn("family", CollectorColumnType.Varchar),
        /* PostgreSQL's own label, as written. Rank it with PgLogEntry.RankOf, never lexically. */
        new CollectorColumn("severity", CollectorColumnType.Varchar),
        /* Null on the default prefix; the read says so. */
        new CollectorColumn("sqlstate", CollectorColumnType.Varchar),
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("user_name", CollectorColumnType.Varchar),
        new CollectorColumn("application_name", CollectorColumnType.Varchar),
        new CollectorColumn("pid", CollectorColumnType.Integer),
        /* As PostgreSQL wrote it (#3944). */
        new CollectorColumn("message", CollectorColumnType.Varchar),
        /* As written, with the SQL in it (a deadlock's queries, a crash's query) normalized. */
        new CollectorColumn("detail", CollectorColumnType.Varchar),
        /* As written, with an SQL frame's statement normalized. The CONTEXT companion — for a lock wait, the
           tuple and relation. */
        new CollectorColumn("context", CollectorColumnType.Varchar),
        /* Keyed hash of the REDACTED statement (#4004); the statement itself is never stored, and no read surface
           returns the hash. */
        new CollectorColumn("statement_fingerprint", CollectorColumnType.Varchar),
        /* Identity across sightings, keyed (#4004) — this route re-reads the tail every cycle, so the reads dedupe on
           it as the deadlock reads do on deadlock_hash. */
        new CollectorColumn("raw_line_hash", CollectorColumnType.Varchar),
        /* V130 (#3602, #3603): the family-specific numbers, appended AFTER the identity column so the V129
           column order is undisturbed and the ALTER an upgraded store ran lands them in the same positions
           the generator gives a fresh one. All nullable; which can be non-null is the family's business —
           see PgLogEventMetrics. */
        /* schema.table of an autovacuum / autoanalyze run; null for a spill (a pgsql_tmp path is not a relation). */
        new CollectorColumn("relation_name", CollectorColumnType.Varchar),
        /* The spilled file's size, from `size N`. */
        new CollectorColumn("bytes", CollectorColumnType.BigInt),
        /* `elapsed: N.NN s` as whole milliseconds. */
        new CollectorColumn("duration_ms", CollectorColumnType.BigInt),
        new CollectorColumn("pages_removed", CollectorColumnType.BigInt),
        new CollectorColumn("pages_remaining", CollectorColumnType.BigInt),
        new CollectorColumn("tuples_removed", CollectorColumnType.BigInt),
        new CollectorColumn("tuples_remaining", CollectorColumnType.BigInt),
        new CollectorColumn("buffer_hits", CollectorColumnType.BigInt),
        /* `misses` on 16/17, `reads` on 18 — one quantity. */
        new CollectorColumn("buffer_misses", CollectorColumnType.BigInt),
        new CollectorColumn("buffer_dirtied", CollectorColumnType.BigInt),
        new CollectorColumn("wal_records", CollectorColumnType.BigInt),
        new CollectorColumn("wal_bytes", CollectorColumnType.BigInt),
        /* false = automatic vacuum (incl. aggressive / wraparound), true = automatic analyze, null = not this family. */
        new CollectorColumn("is_analyze", CollectorColumnType.Boolean),
    };

    public override async ValueTask<List<PgLogEvent>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<PgLogEvent>();
        var classifier = new PgLogEventClassifier(RequireKey(context));

        while (await reader.ReadAsync(cancellationToken))
        {
            /* #4046 part 1c: on the binary route column 0 is bytea (real tail data and the convert_to'd
               marker rows alike), decoded leniently so a byte a failed login planted becomes U+FFFD instead
               of the 22021 the text route would have thrown before this row ever reached C#. */
            var body = reader.IsDBNull(0)
                ? null
                : context.PgReadBinaryFileGranted
                    ? PgBinaryTailText.DecodeWhole(reader.GetFieldValue<byte[]>(0), context.PgLogEncoding ?? System.Text.Encoding.UTF8)
                    : reader.GetString(0);

            /* The marker row (#3410). It cannot collide with a real body: a log tail that is exactly the
               marker text and nothing else is not a log. Thrown so the runner records the named skip. */
            if (string.Equals(body, PgLoggingCollectorOffException.Marker, StringComparison.Ordinal))
            {
                throw new PgLoggingCollectorOffException();
            }

            var logTimezoneIsUtc = PgServerLogTail.LogTimezoneIsUtc(reader, 1);

            if (context.PgLogUsesCsvlog)
            {
                /* #4053 part a1b: the csvlog route's own "no file yet" marker — distinct from the stderr
                   route's PgNoStderrLogFileException, so the fault message names csvlog, not stderr. */
                if (string.Equals(body, PgNoCsvlogFileException.Marker, StringComparison.Ordinal))
                {
                    throw new PgNoCsvlogFileException();
                }

                var entries = PgServerLogCsvParser.Parse(body ?? string.Empty, out var recordsDiscarded);

                /* The same foreign-zone rule the stderr path applies through
                   PgLogEntryAssembler.Assemble(body, logTimezoneIsUtc, out foreignZoneLines): under a UTC
                   log_timezone a record in another zone is not the server's own and is skipped and counted
                   rather than refusing the whole read; otherwise a foreign zone refuses the read whole, the
                   #2993 trade. */
                var kept = FilterForeignZoneEntries(entries, logTimezoneIsUtc, out var foreignZoneLines);
                PgServerLogTail.MeasureForeignZoneLines(context, foreignZoneLines);

                if (recordsDiscarded > 0)
                {
                    context.Measure(CsvRecordsDiscardedMeasurement, recordsDiscarded);
                }

                rows.AddRange(classifier.Classify(kept));
                continue;
            }

            /* The second marker row (#3997): logging_collector is on but the tail excluded every file as a
               csvlog/jsonlog sibling, so there is no stderr-format file this cycle. Same reasoning as above. */
            if (string.Equals(body, PgNoStderrLogFileException.Marker, StringComparison.Ordinal))
            {
                throw new PgNoStderrLogFileException();
            }

            /* The whole pipeline, shared with the RDS transport. A non-UTC zone throws out of here and
               abandons the batch, which is the trade the deadlock parser argues for (#2993) — unless the
               target's own log_timezone renders UTC, when a line in another zone is not the server's and is
               skipped and counted instead (#4046). */
            rows.AddRange(classifier.Classify(body, logTimezoneIsUtc, out var stderrForeignZoneLines));
            PgServerLogTail.MeasureForeignZoneLines(context, stderrForeignZoneLines);
        }

        return rows;
    }

    /// <summary>
    /// The count a consumer records on its collection-log row when the csvlog parser discarded a record
    /// (a resync fragment or a bad shape) (#4053 part a1b): reported only when &gt; 0, following
    /// <see cref="PgPlanCaptureCollector.ForgedCaptureMeasurement"/>'s exact pattern.
    /// </summary>
    public const string CsvRecordsDiscardedMeasurement = "csv_records_discarded";

    /// <summary>
    /// Filters <paramref name="entries"/> the same way <see cref="PgLogEntryAssembler.Assemble(string?, bool, out int)"/>
    /// filters stderr lines (#4053 part a1b): under a UTC <c>log_timezone</c> a record in another zone is not the
    /// server's own and is dropped and counted in <paramref name="foreignZoneLines"/>; otherwise a foreign zone
    /// throws <see cref="PgLogTimezoneUnsupportedException"/> and abandons the whole batch, the same #2993 trade the
    /// stderr assembler makes.
    ///
    /// <para>Public (#4053 part c1) so <c>RdsLogEventIngestor</c>, in the Darling assembly, can share it instead of
    /// keeping its own byte-identical copy — the AWS-log transport applies the exact same rule to the exact same
    /// <see cref="PgLogEntry"/> shape, just without a <c>CollectorContext</c> of its own to route the measurement
    /// through until after its caller returns.</para>
    /// </summary>
    public static List<PgLogEntry> FilterForeignZoneEntries(List<PgLogEntry> entries, bool logTimezoneIsUtc, out int foreignZoneLines)
    {
        foreignZoneLines = 0;

        if (!logTimezoneIsUtc)
        {
            foreach (var entry in entries)
            {
                if (!PgDeadlockLogParser.IsZeroOffsetLogZone(entry.ZoneText))
                {
                    throw new PgLogTimezoneUnsupportedException(entry.ZoneText);
                }
            }

            return entries;
        }

        var kept = new List<PgLogEntry>(entries.Count);

        foreach (var entry in entries)
        {
            if (PgDeadlockLogParser.IsZeroOffsetLogZone(entry.ZoneText))
            {
                kept.Add(entry);
            }
            else
            {
                foreignZoneLines++;
            }
        }

        return kept;
    }

    /// <exception cref="InvalidOperationException">The event was never stamped with the store's keyed identities
    /// (#4004): built by <see cref="PgLogEvent.From"/> but not passed through a <see cref="PgLogEventClassifier"/>.
    /// Both transports write through here, so no row reaches the store without its keyed <c>raw_line_hash</c>.</exception>
    public override void WritePayload(PgLogEvent row, ICollectorRowWriter writer, CollectorContext context)
    {
        if (string.IsNullOrEmpty(row.RawLineHash))
        {
            throw new InvalidOperationException(
                "A PostgreSQL log event reached the store writer without its keyed identity (#4004); every event must pass through PgLogEventClassifier.");
        }

        writer
            /* Naive UTC, per the store contract: Kind=Utc against a `timestamp` column is refused by Npgsql. */
            .Value(DateTime.SpecifyKind(row.OccurredAtUtc, DateTimeKind.Unspecified))
            .Value(row.Family)
            .Value(row.Severity)
            .Value(row.SqlState)
            .Value(row.DatabaseName)
            .Value(row.UserName)
            .Value(row.ApplicationName)
            .Value(row.Pid)
            .Value(row.Message)
            .Value(row.Detail)
            .Value(row.Context)
            .Value(row.StatementFingerprint)
            .Value(row.RawLineHash)
            .Value(row.Metrics.RelationName)
            .Value(row.Metrics.Bytes)
            .Value(row.Metrics.DurationMs)
            .Value(row.Metrics.PagesRemoved)
            .Value(row.Metrics.PagesRemaining)
            .Value(row.Metrics.TuplesRemoved)
            .Value(row.Metrics.TuplesRemaining)
            .Value(row.Metrics.BufferHits)
            .Value(row.Metrics.BufferMisses)
            .Value(row.Metrics.BufferDirtied)
            .Value(row.Metrics.WalRecords)
            .Value(row.Metrics.WalBytes)
            .Value(row.Metrics.IsAnalyze);
    }
}
