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
///
/// <para><b>6. Two arms, one table, one instrument per target (#3604).</b> A stock target WITHOUT the
/// extension used to get nothing from this collector but an hourly <c>EXTENSION_MISSING</c> skip — and
/// nothing from <see cref="PgWaitStatsCollector"/> either, which is Aurora-only. For the SQL Server DBA whose
/// whole performance worldview is <c>dm_os_wait_stats</c>, the flagship diagnostic dimension was simply
/// absent on exactly the targets most likely to be their first, and an empty wait chart reads as "the
/// product is broken". So this definition now has a SECOND arm: when
/// <see cref="CollectorTargetInfo.HasPgWaitSamplingExtension"/> is false it polls <c>pg_stat_activity</c>
/// itself — <see cref="SamplerSnapshotsPerCycle"/> one-second snapshots per cycle, run as ONE multi-statement
/// command so the whole window is one collection and one <c>collection_log</c> row — and accumulates the
/// tallies across cycles in its own per-server state (<see cref="TallyStateKey"/>) so the rows it writes are
/// CUMULATIVE like the extension's and the existing read differences them unchanged. Which arm ran is
/// recorded beside the tally (<see cref="InstrumentStateKey"/>, a <see cref="PgWaitInstrument"/> token) so
/// every read can disclose the grain it is serving. It is the same shape as <c>cpu_utilization</c>'s
/// Azure-vs-ring-buffer fork and <c>pg_statement_stats</c>' Aurora-vs-vanilla one: a target-aware definition,
/// not a second collector, because the catalog's one-collector-one-table rule is what the schema generator,
/// retention and the table census all rest on.</para>
///
/// <para><b>Why the sampler is a FLOOR and says so.</b> The extension samples in-engine every 10 ms and misses
/// nothing a backend did for longer than that. This arm samples from outside every second for
/// <see cref="SamplerSnapshotsPerCycle"/> seconds of every five-minute cycle — a 10% duty cycle. A wait shorter
/// than a second is seen with probability roughly its length over a second; nothing between samples, or
/// between windows, is seen at all. Shares of the profile are trustworthy for anything that is a steady
/// fraction of the server's time, which is the question a wait chart answers first; rare short events are
/// under-counted and a burst that fits between two windows is missed. The window is 30 s and not the whole
/// cycle for a reason outside this file: <c>SweepPressureClassifier</c> sums every collector's single-run
/// cost against a 60,000 ms body budget and calls the body <c>BODY_OVERRUN</c> past it, so a four-minute
/// sampling run would make every stock target read as saturated. Thirty seconds at one Hz is the densest
/// honest floor that leaves that surface truthful, and it is still thirty times the resolution
/// <c>pg_blocking</c> and <c>pg_lock_stats</c> — the two existing PostgreSQL samplers — get from one snapshot a
/// minute. The period is one second rather than five because each doubling of the period halves the odds of
/// seeing a wait shorter than it, while the cost — a shared-memory read of <c>pg_stat_activity</c>, well under
/// a millisecond at hundreds of backends, from a connection the pool holds open regardless — does not move.</para>
///
/// <para><b>Why the sampler reads are separate statements.</b> <c>pg_stat_activity</c> is snapshotted once per
/// transaction on first access and the same rows are returned for the rest of it; a single statement that
/// slept and re-read in a loop would return thirty copies of one instant. Each snapshot is therefore its own
/// statement, preceded by <c>pg_stat_clear_snapshot()</c> — its documented purpose — and a <c>pg_sleep</c>,
/// and the three are repeated in the command text so <see cref="ReadAsync"/> walks result sets rather than
/// rows. Under <c>pg_monitor</c> (the grant every PostgreSQL collector here already needs) all three are
/// callable and every backend's wait columns are visible; the arm adds no privilege.</para>
///
/// <para><b>What the sampler excludes.</b> Its own backend (<c>pg_backend_pid()</c>), every other connection
/// this service holds open (by <c>application_name</c> — the two names the connector presents), and the same
/// three wait TYPES the extension arm excludes, spliced from the same set so the two arms cannot disagree
/// about what counts as a wait. A running backend (<c>state = 'active'</c>, no wait) is <c>CPU</c>/
/// <c>Running</c>, as on the extension arm; an idle one is <c>Client</c>/<c>ClientRead</c> and drops out by
/// type. <c>query_id</c> is PostgreSQL 14+, and on 13 the column is not selected rather than errored on.</para>
///
/// <para><b>Aurora is gated OFF</b> — <see cref="AppliesTo"/> is <c>!IsAurora</c> now, where it used to be
/// <c>true</c>. Aurora cannot preload <c>pg_wait_sampling</c>, so the hourly <c>EXTENSION_MISSING</c> it recorded
/// there was a permanent gap wearing a fixable precondition's clothes; and the sampler arm running beside
/// <c>pg_wait_stats</c> would be two answers to one question on the one engine that has the real one.
/// <c>CollectorEngineCapability.CoveredInsteadBy</c> now points an Aurora caller of <c>get_pg_wait_sampling</c>
/// at <c>get_pg_wait_stats</c>, the mirror of the pointer that already ran the other way.</para>
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

    /// <summary>The state key under which the arm that ran records its <see cref="PgWaitInstrument"/> token
    /// (#3604), per server, every cycle — what the reads disclose as <c>instrument</c>.</summary>
    public const string InstrumentStateKey = "instrument";

    /// <summary>The state key holding the sampler arm's cumulative tally between cycles (#3604): one line per
    /// (event_type, event, query_id), tab-separated, count last. Text because <c>collector_state</c> is text;
    /// lines rather than JSON so the parser is a split and a bad line is one lost key rather than a lost
    /// tally. Capped at <see cref="SamplerTallyCap"/> keys.</summary>
    public const string TallyStateKey = "sampler_tally";

    /// <summary>One-second snapshots per cycle on the sampler arm — the window is this many seconds. See the
    /// type header for why thirty and not the whole cycle.</summary>
    public const int SamplerSnapshotsPerCycle = 30;

    /// <summary>The sampler's period, stored in every row it writes as <c>profile_period_ms</c> so the read's
    /// samples-times-period estimate is right for this arm too.</summary>
    public const int SamplerPeriodMs = 1000;

    /// <summary>Most keys the tally keeps — the same 500 the extension arm's <c>LIMIT</c> ships. Past it the
    /// least-sampled keys are dropped; a dropped key seen again starts over, which the read shows as that
    /// one series resetting rather than as anything wrong with the others.</summary>
    public const int SamplerTallyCap = 500;

    /// <summary>The <c>application_name</c> this service's monitoring connections present, excluded from the
    /// sampler so the tool does not count its own collectors as the server's workload. Pinned equal to the
    /// connector's literal by test; this assembly cannot reference the service project.</summary>
    public const string ServiceApplicationName = "PerformanceMonitorDarling";

    /// <summary>The remediation connections' <c>application_name</c>, excluded for the same reason.</summary>
    public const string ServiceRemediationApplicationName = "PerformanceMonitorDarling-Remediation";

    /// <summary>
    /// One <c>pg_stat_activity</c> snapshot, as the sampler arm reads it: four columns, one row per backend
    /// that is waiting on something the profile counts or is on CPU. Public so a test can run it alone.
    /// <para><c>query_id</c> exists from PostgreSQL 14; on 13 (or an unknown version reading as 0 — which the
    /// gates treat as "newest", so it selects the column) the caller passes <paramref name="hasQueryId"/>
    /// false and the column is a constant 0, the same "belongs to no statement" value the extension arm
    /// stores for a background process.</para>
    /// </summary>
    public static string SamplerSnapshotSql(bool hasQueryId) => @"
SELECT
    coalesce(a.wait_event_type, 'CPU')::text AS event_type,
    coalesce(a.wait_event, 'Running')::text  AS event,
    " + (hasQueryId ? "coalesce(a.query_id, 0)::bigint" : "0::bigint") + @"                AS query_id,
    a.pid::int                               AS pid
FROM pg_stat_activity AS a
WHERE a.pid <> pg_backend_pid()
  AND coalesce(a.application_name, '') NOT IN ('" + ServiceApplicationName + "', '" + ServiceRemediationApplicationName + @"')
  AND (a.wait_event_type IS NOT NULL OR a.state = 'active')
  AND coalesce(a.wait_event_type, 'CPU') NOT IN (" + IgnoredTypeList + ")";

    /// <summary>
    /// The sampler arm's whole cycle as ONE command: snapshot, then (sleep one period, clear the backend
    /// snapshot cache, snapshot) repeated <see cref="SamplerSnapshotsPerCycle"/> − 1 times. Statements in a
    /// batch execute in order, so the sleep-clear-read sequence is guaranteed without relying on evaluation
    /// order inside a statement. Public so a test can count its statements against the constant.
    /// </summary>
    public static string SamplerBatchSql(bool hasQueryId)
    {
        var snapshot = SamplerSnapshotSql(hasQueryId);
        var sb = new System.Text.StringBuilder();
        sb.Append(snapshot).Append(';');
        for (var i = 1; i < SamplerSnapshotsPerCycle; i++)
        {
            /* InvariantCulture, like every number this file puts in SQL or state: StringBuilder.Append(double)
               formats under the host's culture, and a half-second period on a comma-decimal host would emit
               pg_sleep(0,5) - a review catch on #3645 before any period but 1.0 ever shipped. */
            sb.Append("\nSELECT pg_sleep(")
              .Append((SamplerPeriodMs / 1000.0).ToString(System.Globalization.CultureInfo.InvariantCulture))
              .Append(");")
              .Append("\nSELECT pg_stat_clear_snapshot();")
              .Append(snapshot).Append(';');
        }

        return sb.ToString();
    }

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
    /// classifies as <c>ObjectMissing</c> and records as <c>EXTENSION_MISSING</c> (#3240) — the same
    /// degradation <c>pg_buffer_usage</c> takes without <c>pg_buffercache</c>, and the reason
    /// <c>pg_extension_availability</c> exists to say which install would light it up.
    ///
    /// <para>No version gate. The extension supports PostgreSQL 13+ and the query uses nothing
    /// version-conditional, so a gate here would only be a second thing to keep in step with reality.</para>
    ///
    /// <para><b>Since #3604: every PostgreSQL target that is NOT Aurora.</b> Aurora cannot preload the module,
    /// so the skip above was permanent there rather than a resting state, and the sampler arm this definition
    /// grew must not run beside <c>pg_wait_stats</c> on the one engine that has real counters. The
    /// engine-capability sweep fixes <c>IsAurora</c> per kind, so this reads as a permanent gap on the
    /// <c>aurora-postgres</c> kind with a pointer to the instrument that does answer there.</para>
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => !target.IsAurora;

    /// <summary>
    /// Cluster-wide. The profile covers every backend on the instance and carries no database column, so
    /// running per database would collect the same rows once per database and invite exactly the
    /// false attribution #2599 removed elsewhere.
    /// </summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    /// <summary>
    /// Preloaded, and the restart is the reason an operator sees nothing here for so long. This is
    /// the one dependency <c>PgExtensionAvailabilityCollector</c>'s roster deliberately omits, so a
    /// consumer reading that roster as the dependency set misses exactly this collector.
    ///
    /// <para><b>Still declared after #3604, with a narrower meaning.</b> The EXTENSION arm cannot run without
    /// it; the sampler arm is what runs instead when the connect probe finds it absent, so a missing module is
    /// no longer a skip — it is the coarser tier. The declaration stays because it is what the README's
    /// permissions paragraph is pinned to (the operator still needs to know what to install to reach the
    /// finer tier), and because a module DROPPED mid-connection on the extension arm still fails with
    /// <c>42P01</c>, which this declaration is what classifies as <c>EXTENSION_MISSING</c> rather than a
    /// permissions fault until the next connect re-decides the arm.</para>
    /// </summary>
    public override IReadOnlyList<PgExtensionDependency> RequiredPgExtensions { get; } = new[]
    {
        new PgExtensionDependency("pg_wait_sampling", PgExtensionInstallKind.SharedPreloadLibraries),
    };

    /// <summary>
    /// The arm (#3604): the extension's profile when the connect probe found <c>pg_wait_sampling</c> created in
    /// this database, the service sampler's batch when it did not. Decided off <see cref="CollectorContext.Target"/>
    /// rather than probed here so the choice is the connect-time one every other tier fact is.
    /// </summary>
    public override CollectorQuery BuildQuery(CollectorContext context) =>
        context.Target.HasPgWaitSamplingExtension
            ? new CollectorQuery(QueryText)
            : new CollectorQuery(SamplerBatchSql(HasQueryIdColumn(context.Target)));

    /// <summary>
    /// The sampler batch holds <see cref="SamplerSnapshotsPerCycle"/> − 1 seconds of deliberate sleep and its
    /// results are buffered by the server until the batch ends, so the client sees nothing for the whole
    /// window. The default 60 s would fit today's 30 s window but leave a cluster under load — where a snapshot
    /// itself can take longer — with no headroom; 120 s bounds the arm at four times its window. Applies to
    /// the extension arm too, where a longer ceiling on a 500-row read costs nothing.
    /// </summary>
    public override int? CommandTimeoutSecondsOverride => 120;

    /// <summary>
    /// Both arms record which instrument ran (<see cref="InstrumentStateKey"/>); the sampler arm also carries
    /// its cumulative tally between cycles (<see cref="TallyStateKey"/>). Declared so the host loads them
    /// before the cycle and persists them after — the #1962 mechanism for state a MAX() over the table cannot
    /// recover, which a tally the table only holds as its LAST written value is.
    /// </summary>
    public override IReadOnlyList<string> StateKeys { get; } = new[] { InstrumentStateKey, TallyStateKey };

    /// <summary><c>pg_stat_activity.query_id</c> arrived in PostgreSQL 14. 0 is "unknown", which every gate
    /// here reads as newest.</summary>
    private static bool HasQueryIdColumn(CollectorTargetInfo target) =>
        target.PostgresMajorVersion == 0 || target.PostgresMajorVersion >= 14;

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
        if (!context.Target.HasPgWaitSamplingExtension)
        {
            return await ReadSamplerAsync(reader, context, cancellationToken);
        }

        /* The arm that ran, recorded every cycle rather than once: a target that gains or loses the extension
           changes arm at its next connect, and the reads must follow on the next cycle, not the next restart. */
        context.PendingState[InstrumentStateKey] = PgWaitInstrument.ExtensionSampled;

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

    /// <summary>
    /// The sampler arm's read (#3604): walks the batch's result sets, tallies every four-column snapshot row
    /// by (event_type, event, query_id), folds the window into the tally carried in from the previous cycle,
    /// and emits the CUMULATIVE tally as rows in the extension arm's shape. Internal and reader-shaped so a
    /// test can drive it with a fixture batch and no server.
    ///
    /// <para><b>Result sets, not rows, are the unit.</b> A <c>pg_sleep</c> or <c>pg_stat_clear_snapshot()</c>
    /// result set is one column wide and is drained; a snapshot is four. Counting snapshots by shape rather
    /// than by position means a batch that a future edit reorders still tallies only what it should.</para>
    ///
    /// <para><b>backend_count is the WINDOW's distinct pids</b>, not a cumulative one: pids recycle, so a
    /// distinct count across cycles would grow without meaning. The extension arm's figure is cumulative
    /// because the module's is; the column means "how many backends were doing this" on both, over the
    /// span each instrument can honestly speak for.</para>
    /// </summary>
    internal static async ValueTask<List<Row>> ReadSamplerAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var window = new Dictionary<(string Type, string Event, long QueryId), (long Samples, HashSet<int> Pids)>();

        do
        {
            if (reader.FieldCount == 4)
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    var key = (
                        reader.IsDBNull(0) ? "CPU" : reader.GetString(0),
                        reader.IsDBNull(1) ? "Running" : reader.GetString(1),
                        reader.IsDBNull(2) ? 0L : reader.GetInt64(2));
                    var pid = reader.IsDBNull(3) ? 0 : reader.GetInt32(3);

                    if (!window.TryGetValue(key, out var seen))
                    {
                        seen = (0, new HashSet<int>());
                    }

                    seen.Pids.Add(pid);
                    window[key] = (seen.Samples + 1, seen.Pids);
                }
            }
            else
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                }
            }
        }
        while (await reader.NextResultAsync(cancellationToken));

        var tally = ParseTally(context.State.TryGetValue(TallyStateKey, out var carried) ? carried : null);
        foreach (var (key, seen) in window)
        {
            tally[key] = tally.TryGetValue(key, out var prior) ? prior + seen.Samples : seen.Samples;
        }

        /* Cap by dropping the least-sampled: the extension arm ships its top 500 by count, and a tally that
           grew with every distinct query_id ever seen waiting would make the state row unbounded. */
        if (tally.Count > SamplerTallyCap)
        {
            foreach (var victim in tally.OrderBy(kv => kv.Value).ThenBy(kv => kv.Key.Type, StringComparer.Ordinal)
                         .ThenBy(kv => kv.Key.Event, StringComparer.Ordinal).ThenBy(kv => kv.Key.QueryId)
                         .Take(tally.Count - SamplerTallyCap).Select(kv => kv.Key).ToList())
            {
                tally.Remove(victim);
            }
        }

        context.PendingState[TallyStateKey] = SerializeTally(tally);
        context.PendingState[InstrumentStateKey] = PgWaitInstrument.ServiceSampled;

        return tally
            .OrderByDescending(kv => kv.Value)
            .ThenBy(kv => kv.Key.Type, StringComparer.Ordinal)
            .ThenBy(kv => kv.Key.Event, StringComparer.Ordinal)
            .Select(kv => new Row(
                EventType: kv.Key.Type,
                Event: kv.Key.Event,
                QueryId: kv.Key.QueryId,
                SampleCount: kv.Value,
                ProfilePeriodMs: SamplerPeriodMs,
                BackendCount: window.TryGetValue(kv.Key, out var seen) ? seen.Pids.Count : 0))
            .ToList();
    }

    /// <summary>The tally's text form: one <c>type\tevent\tquery_id\tcount</c> line per key. Tabs because
    /// no wait event name carries one; a line that does not parse is skipped rather than failing the cycle.</summary>
    public static Dictionary<(string Type, string Event, long QueryId), long> ParseTally(string? text)
    {
        var tally = new Dictionary<(string, string, long), long>();
        if (string.IsNullOrEmpty(text))
        {
            return tally;
        }

        foreach (var line in text.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            var parts = line.Split('\t');
            if (parts.Length != 4
                || !long.TryParse(parts[2], System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var queryId)
                || !long.TryParse(parts[3], System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var count))
            {
                continue;
            }

            tally[(parts[0], parts[1], queryId)] = count;
        }

        return tally;
    }

    public static string SerializeTally(Dictionary<(string Type, string Event, long QueryId), long> tally)
    {
        var sb = new System.Text.StringBuilder();
        foreach (var (key, count) in tally.OrderByDescending(kv => kv.Value).ThenBy(kv => kv.Key.Type, StringComparer.Ordinal).ThenBy(kv => kv.Key.Event, StringComparer.Ordinal))
        {
            sb.Append(key.Type).Append('\t').Append(key.Event).Append('\t')
              .Append(key.QueryId.ToString(System.Globalization.CultureInfo.InvariantCulture)).Append('\t')
              .Append(count.ToString(System.Globalization.CultureInfo.InvariantCulture)).Append('\n');
        }

        return sb.ToString();
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
