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
/// connections, lock waits, and the recognised-only families, into <c>collect.pg_log_events</c>.
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
/// <para><b>Redaction is not optional and not here.</b> Every text column passes through
/// <see cref="PgLogTextRedactor"/> inside <see cref="PgLogEvent.From"/>, the only constructor path; this
/// collector never sees an unredacted row. The <c>STATEMENT</c> companion — the user's SQL with its
/// literals — is never stored at all, only fingerprinted.</para>
/// </summary>
public sealed class PgLogEventsCollector : PostgresCollectorDefinitionBase<PgLogEvent>
{
    public static PgLogEventsCollector Instance { get; } = new();

    private PgLogEventsCollector()
    {
    }

    /* The tailer, shared byte-for-byte with PgPlanCaptureCollector and PgDeadlocksCollector — see
       PgServerLogTail for the full argument. This query's own part is one column: the body, whole. The
       marker row rides the same UNION ALL arm as its siblings, spelled in this query's one column. */
    private const string QueryText = PgServerLogTail.TailCteSql + @"
SELECT tail.body AS log_body
FROM tail
UNION ALL
SELECT '" + PgLoggingCollectorOffException.Marker + @"'
WHERE " + PgServerLogTail.LoggingCollectorOffMarkerSql;

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

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

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
        /* REDACTED. */
        new CollectorColumn("message", CollectorColumnType.Varchar),
        /* REDACTED. */
        new CollectorColumn("detail", CollectorColumnType.Varchar),
        /* Hash of the REDACTED statement; the statement itself is never stored. */
        new CollectorColumn("statement_fingerprint", CollectorColumnType.Varchar),
        /* Identity across sightings — this route re-reads the tail every cycle, so the reads dedupe on it
           as the deadlock reads do on deadlock_hash. */
        new CollectorColumn("raw_line_hash", CollectorColumnType.Varchar),
    };

    public override async ValueTask<List<PgLogEvent>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<PgLogEvent>();

        while (await reader.ReadAsync(cancellationToken))
        {
            var body = reader.IsDBNull(0) ? null : reader.GetString(0);

            /* The marker row (#3410). It cannot collide with a real body: a log tail that is exactly the
               marker text and nothing else is not a log. Thrown so the runner records the named skip. */
            if (string.Equals(body, PgLoggingCollectorOffException.Marker, StringComparison.Ordinal))
            {
                throw new PgLoggingCollectorOffException();
            }

            /* The whole pipeline, shared with the RDS transport. A non-UTC zone throws out of here and
               abandons the batch, which is the trade the deadlock parser argues for (#2993). */
            rows.AddRange(PgLogEventClassifier.Default.Classify(body));
        }

        return rows;
    }

    public override void WritePayload(PgLogEvent row, ICollectorRowWriter writer, CollectorContext context)
    {
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
            .Value(row.StatementFingerprint)
            .Value(row.RawLineHash);
    }
}
