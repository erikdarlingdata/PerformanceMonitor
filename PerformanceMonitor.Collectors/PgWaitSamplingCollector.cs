/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Wait events attributed to the query that waited — <c>pg_wait_sampling</c> (#2603).
///
/// <para><b>Why this exists.</b> <see cref="PgWaitStatsCollector"/> reads
/// <c>aurora_stat_system_waits()</c>, so wait analysis exists ONLY on Aurora. Every self-hosted, on-prem
/// and plain-RDS target has no wait data at all — which is backwards, because self-hosted is where the
/// extension story is richest. This is the same "which query waited on what" question the product answers
/// on SQL Server, available on any PostgreSQL 14+ that loads the module.</para>
///
/// <para><b>Every design choice below came from running it, not from reading about it.</b></para>
///
/// <para><b>1. <c>Activity</c> is idle, and it dominates permanently.</b> Measured on an idle-but-healthy
/// PostgreSQL 17, the top of the raw profile was <c>AutovacuumMain</c>, <c>LogicalLauncherMain</c>,
/// <c>WalWriterMain</c>, <c>CheckpointerMain</c> and <c>BgwriterHibernate</c> — every one a background
/// process waiting for work to arrive, and every one accumulating samples forever precisely BECAUSE nothing
/// is happening. At that same moment, filtering to attributed waits returned nothing at all. A collector
/// that ranked the raw profile would report autovacuum's idle loop as the server's top wait on every
/// healthy server it ever ran against. So <c>Activity</c> is excluded from the ranked set, and
/// <c>is_idle_class</c> exists so a read can say why a quiet server has no rows rather than implying the
/// instrument is broken.</para>
///
/// <para><b>2. <c>count</c> is SAMPLES, not milliseconds.</b> The module samples every
/// <c>pg_wait_sampling.profile_period</c> (10 ms by default), so a count is an observation tally. The
/// period is collected alongside it, because a count is uninterpretable without it and the two must not
/// drift apart — a reader that wants time multiplies them and owns that inference explicitly. Storing a
/// derived millisecond figure here would bake today's period into history that outlives it.</para>
///
/// <para><b>3. Cumulative since server start or the last <c>pg_wait_sampling_reset_profile()</c>.</b>
/// Stored raw, like <c>pg_statement_stats</c>, and deltas belong to the read. A counter that goes BACKWARDS
/// is a reset rather than a negative wait, and a reader that subtracts blindly reports a large negative
/// number at exactly the moment someone reset the profile to investigate something.</para>
///
/// <para><b>4. Cluster-wide, and there is deliberately no <c>database_name</c>.</b> Version 1.1 exposes
/// <c>(pid, event_type, event, queryid, count)</c> with no database column, and the profile spans every
/// backend on the instance. Attributing these rows to the connected database would be exactly the scope
/// error #2599 fixed in three other collectors — a column whose name promises something the catalog behind
/// it cannot deliver. <see cref="RunsPerDatabase"/> is therefore false and there is no such column to
/// mislead anyone.</para>
///
/// <para><b>5. Aggregated away from <c>pid</c>.</b> The raw profile is per-backend, so a busy server
/// accumulates a row per pid per wait, and pids are transient — the same query appears as many rows that
/// mean one thing. Grouping by <c>(event_type, event, queryid)</c> is the shape a read actually wants, and
/// <c>backend_count</c> preserves the one thing the pid dimension was carrying: how many backends were
/// waiting this way.</para>
/// </summary>
public sealed class PgWaitSamplingCollector : PostgresCollectorDefinitionBase<PgWaitSamplingCollector.Row>
{
    public static readonly PgWaitSamplingCollector Instance = new();

    private PgWaitSamplingCollector()
    {
    }

    /// <param name="EventType">PostgreSQL's wait-event class — <c>Lock</c>, <c>IO</c>, <c>LWLock</c>,
    /// <c>Client</c>, <c>Timeout</c>. <c>Activity</c> never reaches here; see the type header. A backend
    /// that was NOT waiting is labelled <c>CPU</c>/<c>Running</c> rather than stored as a blank.</param>
    /// <param name="QueryId">Joins <c>pg_statement_stats.queryid</c>. Zero means the wait belongs to no
    /// statement — a background process — and is kept rather than dropped so the attributed and
    /// unattributed halves of the profile can be told apart instead of silently blended.</param>
    /// <param name="SampleCount">A tally of observations, NOT a duration. See the type header.</param>
    /// <param name="ProfilePeriodMs">The sampling period the counts were gathered at. Travels with them
    /// because a count means nothing without it.</param>
    /// <param name="BackendCount">How many distinct backends contributed, since the per-pid rows are
    /// aggregated away.</param>
    public readonly record struct Row(
        string? EventType,
        string? Event,
        long QueryId,
        long SampleCount,
        int ProfilePeriodMs,
        int BackendCount);

    /* profile_period is read with the missing_ok form and defaulted rather than assumed: it is the
       module's own GUC, so it exists whenever the module is loaded, but a version that renamed it would
       otherwise take the whole collection down instead of degrading one column.

       Activity is excluded HERE rather than in the read, because a row that must never be ranked should not
       be stored and then remembered about. See the type header for the measurement behind that.

       queryid is NOT filtered. Unattributed waits are real waits, and dropping them would make the stored
       profile disagree with the server's own totals for no gain. */
    /* #2630: the SAME exclusion set the Aurora sibling applies, spliced from its single definition rather
       than restated. Excluding only 'Activity' here was measurably wrong the first time a target with real
       clients was profiled: ClientRead was 2,717,290 of 2,717,989 samples - 100.0% - and every real event
       rounded to zero. Client is the application idling on its socket and Timeout is a deliberate sleep;
       neither is the database doing anything, and both need a CLIENT to be idle before they dominate,
       which is why no container or CI run ever surfaced it.

       Quoted from a C# set rather than hardcoded so the two collectors cannot drift on what counts as a
       wait - they answer the same question from different sources, and #2625 tells operators to read this
       one INSTEAD of that one on stock PostgreSQL.

       coalesce FIRST, so a NULL event_type - a backend on CPU, this collector's distinctive signal -
       becomes 'CPU' and survives a filter that would otherwise be NULL and discard it. That is the same
       trap `IS DISTINCT FROM` was written for, one step further along. */
    private static readonly string IgnoredTypeList =
        string.Join(", ", PgWaitStatsCollector.IgnoredWaitTypes.OrderBy(t => t, StringComparer.Ordinal).Select(t => $"'{t}'"));

    private static string QueryText => @"
SELECT
    /* A NULL wait event means the backend was NOT waiting - it was on CPU. That is PostgreSQL's own
       convention (pg_stat_activity.wait_event_type is NULL for a running backend) and it is real signal
       worth keeping, but stored raw it renders as a BLANK row sitting near the top of a grid sorted by
       count. Measured on the live rig: 23 samples across 11 backends arrived exactly that way. Labelled
       here so the row says what it is; 'CPU' is not a wait_event_type PostgreSQL itself emits, so it
       cannot collide with a real one. */
    coalesce(p.event_type, 'CPU')::text                      AS event_type,
    coalesce(p.event, 'Running')::text                       AS event,
    p.queryid::bigint                                        AS query_id,
    sum(p.count)::bigint                                     AS sample_count,
    coalesce(
        nullif(regexp_replace(
            coalesce(current_setting('pg_wait_sampling.profile_period', true), '10'),
            '[^0-9]', '', 'g'), '')::int,
        10)                                                  AS profile_period_ms,
    count(DISTINCT p.pid)::int                               AS backend_count
FROM pg_wait_sampling_profile AS p
WHERE coalesce(p.event_type, 'CPU') NOT IN (" + IgnoredTypeList + @")
GROUP BY coalesce(p.event_type, 'CPU'), coalesce(p.event, 'Running'), p.queryid
ORDER BY sum(p.count) DESC, coalesce(p.event_type, 'CPU'), coalesce(p.event, 'Running')
LIMIT 500";

    public override string Name => "pg_wait_sampling";

    public override string TargetTable => "pg_wait_sampling";

    /// <summary>
    /// Any PostgreSQL target. The module's absence is a normal, non-fatal skip: reading
    /// <c>pg_wait_sampling_profile</c> where it was never loaded raises <c>42P01</c>, which the host
    /// classifies as <c>ObjectMissing</c> and records as <c>PERMISSIONS</c> — the same degradation
    /// <c>pg_buffer_usage</c> takes without <c>pg_buffercache</c>, and the reason
    /// <c>pg_extension_availability</c> exists to say which install would light it up.
    ///
    /// <para>No version gate. The extension supports PostgreSQL 13+ and the query uses nothing
    /// version-conditional, so a gate here would only be a second thing to keep in step with reality.</para>
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    /// <summary>
    /// Cluster-wide. The profile covers every backend on the instance and carries no database column, so
    /// running per database would collect the same rows once per database and invite exactly the
    /// false attribution #2599 removed elsewhere.
    /// </summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("event_type", CollectorColumnType.Varchar),
        new CollectorColumn("event", CollectorColumnType.Varchar),
        /* BigInt, not Integer: queryid is a 64-bit hash and the measured values here run to nineteen
           digits (4654506383535020975 on the live rig). */
        new CollectorColumn("query_id", CollectorColumnType.BigInt),
        new CollectorColumn("sample_count", CollectorColumnType.BigInt),
        new CollectorColumn("profile_period_ms", CollectorColumnType.Integer),
        new CollectorColumn("backend_count", CollectorColumnType.Integer),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                EventType: reader.IsDBNull(0) ? null : reader.GetString(0),
                Event: reader.IsDBNull(1) ? null : reader.GetString(1),
                QueryId: reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                SampleCount: reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                /* Defaulted to the module's own default rather than 0: a zero period would make any
                   reader's count-to-time inference divide the workload into nothing. */
                ProfilePeriodMs: reader.IsDBNull(4) ? 10 : reader.GetInt32(4),
                BackendCount: reader.IsDBNull(5) ? 0 : reader.GetInt32(5)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No deltas here. The profile is CUMULATIVE and is stored that way on purpose: a reset or a restart
           makes the counter go backwards, and a delta computed in the collector would publish a large
           negative wait at exactly the moment someone reset the profile to investigate something. The read
           owns that subtraction and can recognise the reset. */
        writer
            .Value(row.EventType)
            .Value(row.Event)
            .Value(row.QueryId)
            .Value(row.SampleCount)
            .Value(row.ProfilePeriodMs)
            .Value(row.BackendCount);
    }
}
