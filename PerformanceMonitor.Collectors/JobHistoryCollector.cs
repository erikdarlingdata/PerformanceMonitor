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
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Retained SQL Agent job-run history — every step row AND the job-outcome (step_id 0) row from
/// <c>msdb.dbo.sysjobhistory</c>, snapshotted incrementally into a retained <c>job_history</c> table so
/// the fleet-wide "Job History" tab can show up to a year of runs with per-step results, retries,
/// durations, and color-coded failures/long-runtimes (issue #1433). The run_datetime / run_duration
/// HHMMSS-integer decode idioms and the sysjobs/syscategories joins are the proven ones from the
/// live-msdb <c>FailedJobsQuery</c> and <c>RunningJobsCollector</c>, ported into the shared collector
/// library so both SKUs (portable Lite → DuckDB, Darling → Postgres) collect it from one definition.
///
/// <para><b>STATEFUL dedup on a numeric high-water mark (must-handle):</b> unlike the timestamp-watermark
/// event collectors, this dedups on <c>sysjobhistory.instance_id</c> — a unique monotonic IDENTITY that
/// survives server-side <c>sp_purge_jobhistory</c>. The host reads MAX(instance_id) already collected for
/// the server (<see cref="CollectorContext.NumericWatermark"/>) and this filters
/// <c>jh.instance_id &gt; @last_instance_id</c>, so each run's rows are collected exactly once — no gaps,
/// no duplicates — even as msdb prunes its own history. The FIRST run backfills everything currently in
/// sysjobhistory (bounded by msdb's own history cap), the same first-run-collects-all choice
/// <see cref="DefaultTraceEventsCollector"/> makes.</para>
///
/// <para><b>Lite archival-emptied edge (must-handle):</b> a quiet server's hot <c>job_history</c> table can
/// be emptied by Lite's parquet archival while the parquet tier still holds the rows, so MAX(instance_id)
/// comes back null even though history exists. Re-collecting all of sysjobhistory then would re-insert rows
/// already in parquet, and <c>v_job_history</c> UNIONs hot + parquet with no dedup, so those rows would
/// DOUBLE-COUNT. This declares <c>run_datetime</c> as its <see cref="WatermarkColumn"/> too, so the host
/// sets <see cref="CollectorContext.HasCollectedBefore"/> when the numeric watermark is null: on an
/// archival-emptied (necessarily quiet) server the query falls back to a BOUNDED recent
/// <see cref="ArchivalEmptyFallbackHours"/>-hour run_datetime window (computed server-side against
/// GETDATE(), since run_datetime is the server's LOCAL wall clock) — far smaller than any retention
/// horizon, so it re-reads only genuinely-recent runs not yet archived and never re-scans parquet. Darling
/// has no parquet tier, so this branch never triggers there. Ports the Dashboard-collector idiom
/// <see cref="DefaultTraceEventsCollector.ArchivalEmptyFallbackHours"/> uses.</para>
///
/// <para><b>Azure SQL Database (edition 5): does NOT apply</b> — there is no SQL Agent on Azure SQL DB
/// (<c>msdb.dbo.sysjobhistory</c> is unreachable there), so the collector is gated off via
/// <see cref="AppliesTo"/> = <c>!IsAzureSqlDb</c>, exactly like <see cref="RunningJobsCollector"/> and the
/// failed-jobs alert path. Managed Instance (edition 8), on-prem, and AWS RDS all have Agent and collect
/// normally.</para>
/// </summary>
public sealed class JobHistoryCollector : CollectorDefinitionBase<JobHistoryCollector.Row>
{
    public static JobHistoryCollector Instance { get; } = new();

    private JobHistoryCollector()
    {
    }

    /// <summary>
    /// The BOUNDED fallback window (hours) used when the numeric watermark is null but the collector HAS
    /// succeeded before (<see cref="CollectorContext.HasCollectedBefore"/>) — i.e. Lite's hot store was
    /// emptied by parquet archival, not a true first run. Re-collecting all sysjobhistory here would
    /// re-insert rows already aged into parquet, and <c>v_job_history</c> UNIONs hot + parquet with no
    /// dedup, so those rows would DOUBLE-COUNT and recur every archival cycle. This window is far smaller
    /// than the retention horizon, so on an archival-emptied (necessarily quiet) server it re-reads only
    /// genuinely-recent runs — which by definition are not yet archived — and never re-scans parquet.
    /// Mirrors <see cref="DefaultTraceEventsCollector.ArchivalEmptyFallbackHours"/>.
    /// </summary>
    public const int ArchivalEmptyFallbackHours = 24;

    public sealed class Row
    {
        public long InstanceId { get; set; }
        public string JobId { get; set; } = "";
        public string JobName { get; set; } = "";
        public bool JobEnabled { get; set; }
        public string? CategoryName { get; set; }
        public int StepId { get; set; }
        public string? StepName { get; set; }
        public int RunStatus { get; set; }
        public string? RunStatusDesc { get; set; }
        public DateTime? RunDateTime { get; set; }
        public long RunDurationSeconds { get; set; }
        public int RetriesAttempted { get; set; }
        public string? Message { get; set; }
    }

    /* Every step row AND the job-outcome (step_id 0) row, joined to sysjobs (job name/enabled) and
       syscategories (category), with the HHMMSS-integer decode idioms from FailedJobsQuery /
       RunningJobsCollector. A {0} placeholder is spliced with the per-cycle incremental filter (see
       BuildQuery). READ UNCOMMITTED like every collector; OPTION(RECOMPILE) because the filter selectivity
       varies wildly between the all-history first run and the tiny steady-state instance_id windows. */
    /* Parsed once — the template is re-formatted every collection cycle (CA1863). */
    private static readonly System.Text.CompositeFormat QueryTemplateFormat =
        System.Text.CompositeFormat.Parse(QueryTemplate);

    private const string QueryTemplate = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    instance_id = jh.instance_id,
    job_id = CONVERT(varchar(36), jh.job_id),
    job_name = j.name,
    job_enabled = j.enabled,
    category_name = c.name,
    step_id = jh.step_id,
    step_name = jh.step_name,
    run_status = jh.run_status,
    run_status_desc =
        CASE jh.run_status
            WHEN 0 THEN N'Failed'
            WHEN 1 THEN N'Succeeded'
            WHEN 2 THEN N'Retry'
            WHEN 3 THEN N'Canceled'
            WHEN 4 THEN N'In Progress'
            ELSE N'Unknown'
        END,
    run_datetime =
        DATEADD
        (
            SECOND,
            (jh.run_time / 10000) * 3600 +
            ((jh.run_time / 100) % 100) * 60 +
            (jh.run_time % 100),
            CONVERT(datetime, CONVERT(varchar(8), jh.run_date))
        ),
    run_duration_seconds =
        (jh.run_duration / 10000) * 3600 +
        ((jh.run_duration / 100) % 100) * 60 +
        (jh.run_duration % 100),
    retries_attempted = jh.retries_attempted,
    message = jh.message
FROM msdb.dbo.sysjobhistory AS jh
JOIN msdb.dbo.sysjobs AS j
  ON j.job_id = jh.job_id
LEFT JOIN msdb.dbo.syscategories AS c
  ON c.category_id = j.category_id
WHERE 1 = 1{0}
ORDER BY
    jh.instance_id
OPTION(RECOMPILE);";

    /// <summary>
    /// The archival-emptied fallback predicate: a bounded recent run_datetime window computed SERVER-SIDE
    /// against GETDATE() (run_datetime is the server's local wall clock, so a host-supplied UTC cutoff would
    /// be timezone-skewed). A cheap sargable run_date pre-filter plus the exact decoded-run_datetime bound,
    /// mirroring FailedJobsQuery's dual run_date / run_datetime lookback. Built once from
    /// <see cref="ArchivalEmptyFallbackHours"/>.
    /// </summary>
    private static readonly string ArchivalEmptyFilter = string.Format(
        CultureInfo.InvariantCulture,
        @"
AND   jh.run_date >= CONVERT(integer, CONVERT(varchar(8), DATEADD(HOUR, -{0}, GETDATE()), 112))
AND   DATEADD
      (
          SECOND,
          (jh.run_time / 10000) * 3600 +
          ((jh.run_time / 100) % 100) * 60 +
          (jh.run_time % 100),
          CONVERT(datetime, CONVERT(varchar(8), jh.run_date))
      ) >= DATEADD(HOUR, -{0}, GETDATE())",
        ArchivalEmptyFallbackHours);

    /// <summary>
    /// The archival-emptied window as a bare CONJUNCTION — <see cref="ArchivalEmptyFilter"/> with its
    /// leading <c>AND</c> stripped — so the identity-regression arm of
    /// <see cref="IdentityGuardedWatermarkFilter"/> can nest the SAME window inside an OR without a second
    /// copy of the predicate drifting from the first. Derived, never re-typed: one definition of "the
    /// bounded recent window" in this file, and the archival branch's own text stays byte-for-byte what it
    /// was before the guard existed.
    /// </summary>
    private static readonly string ArchivalEmptyWindowPredicate =
        ArchivalEmptyFilter.StartsWith(ArchivalEmptyFilterPrefix, StringComparison.Ordinal)
            ? ArchivalEmptyFilter[ArchivalEmptyFilterPrefix.Length..]
            : ArchivalEmptyFilter.TrimStart();

    /// <summary>The <c>AND</c> lead-in <see cref="ArchivalEmptyFilter"/> opens with, stripped to derive <see cref="ArchivalEmptyWindowPredicate"/>.</summary>
    private const string ArchivalEmptyFilterPrefix = "\r\nAND   ";

    /// <summary>
    /// The target's OWN current high-water mark, read on the same round trip as the rows (a scalar
    /// aggregate over <c>sysjobhistory</c>'s clustered <c>instance_id</c> key — a one-row backward top,
    /// not a scan). <c>ISNULL(..., -1)</c> because a freshly purged <c>sysjobhistory</c> with nothing
    /// written since returns NULL, and a NULL on either side of a comparison makes BOTH arms of the guard
    /// unknown — which is exactly the silent-zero-rows failure this fix exists to end.
    /// </summary>
    private const string TargetMaxInstanceIdScalar =
        "ISNULL((SELECT MAX(h2.instance_id) FROM msdb.dbo.sysjobhistory AS h2), -1)";

    /// <summary>
    /// The steady-state filter, SELF-GUARDING against an identity regression in ONE statement (#3885).
    ///
    /// <para><b>The failure it ends.</b> <c>instance_id</c> is an IDENTITY, monotonic only while the
    /// identity itself stands. It does NOT survive a reseed: a weekly cleanup window that purges
    /// <c>sysjobhistory</c> and reseeds the identity drops the target's <c>MAX(instance_id)</c> into the
    /// thousands while the store still remembers millions, and <c>jh.instance_id &gt; @last_instance_id</c>
    /// then matches nothing FOREVER. The query stays valid and the run records SUCCESS with zero rows, so
    /// no failure arm fires and the health surface bands the server healthy while its record-keeping is
    /// dark — #3754's shape, one collector over. An <c>msdb</c> restore, an AG failover to a replica whose
    /// <c>msdb</c> carries a lower identity, and a registration re-pointed at a different instance all
    /// starve it the same way.</para>
    ///
    /// <para><b>Why one statement and no second round trip.</b> The guard is a T-SQL comparison of the
    /// target's own max against the host-supplied watermark, evaluated inside the filter, so the decision
    /// is made against the instance the connection ACTUALLY reaches at the instant the rows are read. A
    /// host-side probe would be a second round trip, a second connection's worth of failure modes, and a
    /// window between the probe and the read in which a failover could move the answer.</para>
    ///
    /// <para><b>What the regressed arm collects.</b> NOT everything: the same bounded
    /// <see cref="ArchivalEmptyFallbackHours"/>-hour window the archival-emptied branch takes
    /// (<see cref="ArchivalEmptyWindowPredicate"/>, composed from the one definition). The reseed is what
    /// caused the regression, so what remains on the target is recent by construction; the window bounds
    /// the re-read anyway, and on Lite it keeps the run from re-inserting rows already aged into parquet
    /// that <c>v_job_history</c> would double-count.</para>
    ///
    /// <para><b>Self-healing, no operator action.</b> The regressed run stores rows carrying the NEW
    /// epoch's ids, so the host's <c>SELECT MAX(instance_id)</c> for this server is the new max on the very
    /// next run and the watermark arm is honest again — one run of fallback per reseed, not a mode the
    /// collector stays in. Pinned as two consecutive BuildQuery calls in
    /// <c>JobHistoryIdentityEpochTests</c>.</para>
    /// </summary>
    private static readonly string IdentityGuardedWatermarkFilter = string.Format(
        CultureInfo.InvariantCulture,
        @"
AND   (
          (
              {0} >= @last_instance_id
              AND jh.instance_id > @last_instance_id
          )
          OR
          (
              {0} < @last_instance_id
              AND {1}
          )
      )",
        TargetMaxInstanceIdScalar,
        ArchivalEmptyWindowPredicate);

    /// <summary>
    /// Count of job-history identity regressions this run observed (0 or 1) — the store-side marker for a
    /// reseed, on the run's <c>collection_log</c> note (#3161's measurement seam). A count, never a verdict.
    /// </summary>
    public const string IdentityRegressionsMeasurement = "job_history_identity_regressions";

    /// <summary>The store watermark the regressed target fell below, on the run that detected it.</summary>
    public const string IdentityWatermarkMeasurement = "job_history_identity_store_watermark";

    /// <summary>The first target <c>instance_id</c> read on the run that detected the regression — the new epoch's floor.</summary>
    public const string IdentityTargetRowMeasurement = "job_history_identity_target_row";

    public override string Name => "job_history";

    public override string TargetTable => "job_history";

    /// <summary>Lite names this table's prefix id "job_history_id"; Darling mirrors it.</summary>
    public override string PrefixIdColumnName => "job_history_id";

    /// <summary>
    /// Dedup is primarily on the numeric <c>instance_id</c> watermark (see
    /// <see cref="NumericWatermarkColumn"/>); <c>run_datetime</c> is declared here too so the host sets
    /// <see cref="CollectorContext.HasCollectedBefore"/> when the numeric watermark comes back null,
    /// distinguishing a TRUE first run (collect all) from a Lite hot store merely emptied by archival
    /// (bounded recent window — see the class remarks).
    /// </summary>
    public override string? WatermarkColumn => "run_datetime";

    /// <summary>
    /// Exact-and-complete dedup key: <c>sysjobhistory.instance_id</c>, a unique monotonic IDENTITY that
    /// survives server-side history purges. The host reads MAX(instance_id) already collected for the
    /// server and this filters newer rows only.
    /// </summary>
    public override string? NumericWatermarkColumn => "instance_id";

    /// <summary>#4197 part b: lets the host cache this collector's server-scoped watermark in memory.</summary>
    public override Func<Row, DateTime?>? WatermarkValueAccessor => static row => row.RunDateTime;

    /// <summary>#4197 part b: the numeric twin, for the same reason.</summary>
    public override Func<Row, long?>? NumericWatermarkValueAccessor => static row => row.InstanceId;

    /// <summary>
    /// Collects on SQL Server, Azure SQL Managed Instance, and AWS RDS — everywhere SQL Agent exists. NOT
    /// Azure SQL Database (edition 5): there is no Agent / <c>msdb.dbo.sysjobhistory</c> there. NOT a login
    /// without msdb access: every table this reads (sysjobhistory, sysjobs) lives in msdb. It is NOT gated on
    /// AWS RDS — unlike <see cref="RunningJobsCollector"/> it never touches <c>syssessions</c>, so retained
    /// history reads fine there. Gated here in the shared AppliesTo so Lite and Darling skip identically.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => !target.IsAzureSqlDb;

    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        /* Incremental filter selection (the numeric high-water mark, with the Lite archival-emptied fallback):
             - numeric watermark present             -> steady state, collect instance_id newer than it,
                                                        UNLESS the target's own MAX(instance_id) is BELOW
                                                        that watermark, in which case the identity regressed
                                                        (a purge-and-reseed cleanup window, an msdb restore,
                                                        an AG failover to a lower-identity replica, a
                                                        re-pointed registration) and the same statement
                                                        collapses to the bounded window instead of matching
                                                        nothing forever - see
                                                        IdentityGuardedWatermarkFilter (#3885).
             - watermark null, never succeeded        -> TRUE first run, collect ALL of sysjobhistory.
             - watermark null, HAS succeeded (Lite)   -> hot store emptied by archival, use a BOUNDED recent
                                                         run_datetime window so we never re-scan rows already
                                                         in the parquet archive (which v_job_history would
                                                         double-count). */
        string filter;
        IReadOnlyList<CollectorParameter> parameters;

        if (context.NumericWatermark.HasValue)
        {
            filter = IdentityGuardedWatermarkFilter;
            parameters = new[]
            {
                new CollectorParameter("@last_instance_id", context.NumericWatermark.Value, CollectorParameterType.BigInt),
            };
        }
        else if (context.HasCollectedBefore)
        {
            filter = ArchivalEmptyFilter;
            parameters = Array.Empty<CollectorParameter>();
        }
        else
        {
            filter = string.Empty;
            parameters = Array.Empty<CollectorParameter>();
        }

        var text = string.Format(CultureInfo.InvariantCulture, QueryTemplateFormat, filter);
        return new CollectorQuery(text, parameters);
    }

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("instance_id", CollectorColumnType.BigInt),
        new CollectorColumn("job_id", CollectorColumnType.Varchar),
        new CollectorColumn("job_name", CollectorColumnType.Varchar),
        new CollectorColumn("job_enabled", CollectorColumnType.Boolean),
        new CollectorColumn("category_name", CollectorColumnType.Varchar),
        new CollectorColumn("step_id", CollectorColumnType.Integer),
        new CollectorColumn("step_name", CollectorColumnType.Varchar),
        new CollectorColumn("run_status", CollectorColumnType.Integer),
        new CollectorColumn("run_status_desc", CollectorColumnType.Varchar),
        new CollectorColumn("run_datetime", CollectorColumnType.Timestamp),
        new CollectorColumn("run_duration_seconds", CollectorColumnType.BigInt),
        new CollectorColumn("retries_attempted", CollectorColumnType.Integer),
        new CollectorColumn("message", CollectorColumnType.Varchar),
    };

    /// <summary>
    /// Reads the rows and, on the way past, notices an identity REGRESSION (#3885): a row whose
    /// <c>instance_id</c> is at or below <see cref="CollectorContext.NumericWatermark"/> can only arrive on
    /// the guarded filter's bounded-window arm, because the watermark arm's own predicate is
    /// <c>jh.instance_id &gt; @last_instance_id</c>. So the rows themselves ARE the detection - no
    /// projected guard column, no second scalar for the reader to consume, no extra bytes on every
    /// ordinary run: the cheaper of the two shapes, and the one that cannot disagree with the filter that
    /// produced the rows.
    ///
    /// <para><b>Once per run, not once per row.</b> A reseed leaves hundreds of low-id rows in the window;
    /// the account is composed on the FIRST one and the flag suppresses the rest. Once per EPOCH follows
    /// from the fix being self-healing: this run stores the new epoch's ids, so the host's
    /// <c>MAX(instance_id)</c> is the new max next run and the watermark arm is honest again. A regressed
    /// target with nothing inside the window returns no rows and nothing is said - true, and the next run
    /// that does find a row says it.</para>
    ///
    /// <para><b>The carrier.</b> The count rides the run's <c>collection_log</c> note
    /// (<see cref="IdentityRegressionsMeasurement"/>, #3161's measurement seam) and the sentence rides the
    /// calculator's discontinuity queue, which both hosts drain and log after the run (#3653 A5). It is NOT
    /// routed through <see cref="ServerEpoch.ObserveInstance"/>: that epoch is a (start time, server name)
    /// pair whose change means every cumulative counter on the instance restarted, and it forgets EVERY
    /// delta baseline for the server. A job-history reseed is a numeric epoch on one table and says nothing
    /// about any counter, so borrowing that carrier would need <c>Stamp</c> widened to a numeric component
    /// and would clear baselines that are perfectly good. Widening <c>ServerEpoch</c> to carry a numeric
    /// epoch (and a persisted <c>job_history_identity</c> / <c>_previous</c> state pair, which would let the
    /// read layer's <c>discontinuities[]</c> name this by itself) is the named follow-up on #3885.</para>
    ///
    /// <para><b>No forget call.</b> job_history keeps no delta baselines, so there is nothing to clear, and the
    /// forget API (<c>ClearGroups</c> / <c>ClearServer</c>) belongs to the epoch comparator and the host remove
    /// paths alone (measurement-contract rule 3). The regression is recorded as a MEASUREMENT on the run
    /// (<see cref="IdentityRegressionsMeasurement"/> = 1), which the <c>collection_log</c> note and the host's
    /// cycle line both render with the two numbers the operator needs beside it.</para>
    /// </summary>
    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();
        var regressionReported = false;

        while (await reader.ReadAsync(cancellationToken))
        {
            var instanceId = Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture);

            if (!regressionReported
                && context.NumericWatermark.HasValue
                && instanceId <= context.NumericWatermark.Value)
            {
                regressionReported = true;
                context.Measure(IdentityRegressionsMeasurement, 1);
                /* The two numbers an operator needs, on the same note: the watermark the store remembered and the
                   first id the target now holds. Counts on the measurement seam are the definition's only channel
                   to the run record, and these two are honest as numbers (#3161). */
                context.Measure(IdentityWatermarkMeasurement, context.NumericWatermark.Value);
                context.Measure(IdentityTargetRowMeasurement, instanceId);
                /* The measurement IS the marker: the run's collection_log note renders identity_regressions=1 and
                   the host's cycle line carries it, so the store and the log both say the reseed happened on this
                   run. No ClearGroups/ClearServer: job_history keeps no delta baselines and the forget API is
                   reserved for the epoch comparator and the host remove paths (MeasurementContractCensusTests
                   rule 3) - a note is not a forget. */
            }

            rows.Add(new Row
            {
                InstanceId = instanceId,
                JobId = reader.IsDBNull(1) ? "" : reader.GetString(1),
                JobName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                JobEnabled = !reader.IsDBNull(3) && Convert.ToBoolean(reader.GetValue(3), CultureInfo.InvariantCulture),
                CategoryName = reader.IsDBNull(4) ? null : reader.GetString(4),
                StepId = reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture),
                StepName = reader.IsDBNull(6) ? null : reader.GetString(6),
                RunStatus = reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture),
                RunStatusDesc = reader.IsDBNull(8) ? null : reader.GetString(8),
                RunDateTime = reader.IsDBNull(9) ? null : reader.GetDateTime(9),
                RunDurationSeconds = reader.IsDBNull(10) ? 0 : Convert.ToInt64(reader.GetValue(10), CultureInfo.InvariantCulture),
                RetriesAttempted = reader.IsDBNull(11) ? 0 : Convert.ToInt32(reader.GetValue(11), CultureInfo.InvariantCulture),
                Message = reader.IsDBNull(12) ? null : reader.GetString(12),
            });
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.InstanceId)          /* instance_id BIGINT (numeric watermark) */
            .Value(row.JobId)               /* job_id VARCHAR */
            .Value(row.JobName)             /* job_name VARCHAR */
            .Value(row.JobEnabled)          /* job_enabled BOOLEAN */
            .Value(row.CategoryName)        /* category_name VARCHAR */
            .Value(row.StepId)              /* step_id INTEGER */
            .Value(row.StepName)            /* step_name VARCHAR */
            .Value(row.RunStatus)           /* run_status INTEGER */
            .Value(row.RunStatusDesc)       /* run_status_desc VARCHAR */
            .Value(row.RunDateTime)         /* run_datetime TIMESTAMP (watermark fallback) */
            .Value(row.RunDurationSeconds)  /* run_duration_seconds BIGINT */
            .Value(row.RetriesAttempted)    /* retries_attempted INTEGER */
            .Value(row.Message);            /* message VARCHAR */
    }
}
