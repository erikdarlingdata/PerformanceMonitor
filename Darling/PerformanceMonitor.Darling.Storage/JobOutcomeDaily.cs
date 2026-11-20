/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The per-(server, job, server-local day) summary of <c>collect.job_history</c>'s successful job-outcome rows (<c>step_id = 0 AND
/// run_status = 1</c>) that the Job History read's <c>job_stats</c> (a job's average successful duration and its last success) reads
/// whole days from (#9110). Without it that average decompresses every compressed <c>job_history</c> chunk of the window.
///
/// <para><b>The columns.</b> <c>run_day</c> is <c>run_datetime::date</c>, the day on the monitored server's own clock (the column holds
/// server-local time), so it does not depend on the server's UTC offset. <c>success_count</c> is the number of those rows whose
/// <c>run_duration_seconds</c> is not NULL and <c>success_duration_sum</c> is the sum of those durations, so
/// <c>sum / count</c> over any set of days equals <c>AVG(run_duration_seconds)</c> over the same rows, NULL durations left out just as
/// <c>AVG</c> leaves them out. A day whose successes all have a NULL duration therefore has a count of 0: read the average as
/// <c>sum::numeric / NULLIF(count, 0)</c>. <c>last_success_run_local</c> is the newest <c>run_datetime</c> of every row of the day,
/// including a row with a NULL duration.</para>
///
/// <para><b>The built table.</b> <c>job_outcome_daily_built</c> has one row per (server, day) that was built, including a day with no
/// success row (then no <c>job_outcome_daily</c> row exists and <c>source_through</c> is NULL). <c>source_through</c> is the newest
/// <c>collection_time</c> (naive UTC, stored as a <c>timestamptz</c> at UTC) of the rows the build read. A row of a built day with a
/// later <c>collection_time</c> reached the store after the build, so the rollup does not hold it: a reader takes the rollup for the
/// day and adds those rows from <c>job_history</c>, or reads the day raw. Compare with
/// <c>(b.source_through AT TIME ZONE 'UTC')</c> against <c>collection_time</c>, so the session time zone cannot move the comparison,
/// and treat a NULL <c>source_through</c> as "every row is late". A row is collected after its run starts, so a day's rows all have a
/// <c>collection_time</c> at or after the day's UTC start minus the largest UTC offset.</para>
///
/// <para>Plain tables, no hypertable. An older build ignores them. The service builds a day once it has ended on the server's clock and
/// once more a day later, newest day first (see the housekeeping tick).</para>
/// </summary>
public static class JobOutcomeDaily
{
    /// <summary>The per-(server, day, job) summary table.</summary>
    public const string Table = "collect.job_outcome_daily";

    /// <summary>The per-(server, day) record of which days are built, and how far their source reached.</summary>
    public const string BuiltTable = "collect.job_outcome_daily_built";

    /// <summary>The migration rung that creates both tables.</summary>
    public const int Version = 282;

    /// <summary>Creates both tables. Idempotent. No GRANT: the <c>collect</c> schema's blanket default privileges cover a new table.</summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS collect.job_outcome_daily
(
    server_id integer NOT NULL,
    job_id text NOT NULL,
    run_day date NOT NULL,
    success_count bigint NOT NULL,
    success_duration_sum bigint NOT NULL,
    last_success_run_local timestamp NOT NULL,
    CONSTRAINT pk_job_outcome_daily PRIMARY KEY (server_id, run_day, job_id)
);

CREATE TABLE IF NOT EXISTS collect.job_outcome_daily_built
(
    server_id integer NOT NULL,
    run_day date NOT NULL,
    built_at timestamptz NOT NULL,
    source_through timestamptz,
    CONSTRAINT pk_job_outcome_daily_built PRIMARY KEY (server_id, run_day)
);";
}
