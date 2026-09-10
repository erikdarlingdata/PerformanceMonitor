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
/// Cumulative wait statistics for an Amazon Aurora PostgreSQL target — the Postgres counterpart of
/// <see cref="WaitStatsCollector"/>, with the same shape: cumulative counters in, deltas computed on
/// read, noise filtered out.
/// <para><b>Core PostgreSQL has no cumulative wait accounting at all.</b> There is no
/// <c>sys.dm_os_wait_stats</c> equivalent — only the instantaneous <c>pg_stat_activity.wait_event</c>,
/// and a proposal to add accumulation has been rejected twice on overhead grounds. The usual
/// workaround is a sampling extension (<c>pg_wait_sampling</c>, <c>pgsentinel</c>), and Aurora permits
/// exactly thirteen preloadable libraries, none of which are those. So on Aurora this function is not
/// merely the convenient source — it is the only one.</para>
/// <para>Aurora provides it as a built-in, no extension required, which is why this collector gates on
/// <see cref="CollectorTargetInfo.IsAurora"/> rather than on a version: on stock PostgreSQL there is
/// nothing to read and the collector must not run at all.</para>
/// <para><b>A wait event with zero NEW waits since the last cycle is not shipped.</b> <c>wait_time</c> is
/// cumulative since instance start with no reset function anywhere in the Aurora API (see the query
/// remarks below), so once any wait type has ever fired, <c>WHERE w.wait_time &gt; 0</c> keeps matching
/// it forever — the same row gets re-shipped every single cycle whether or not the event fired again.
/// This is the identical shape <see cref="PgStatementStatsCollector"/> had before #2691 (which reached
/// 176.9 GB of a 180 GB store in 36 hours): a bounded catalog here (a fixed wait-event enumeration
/// rather than an unbounded statement set) kept this from runaway-exploding the same way, but it was
/// still the 2nd-largest table in the store at 1.25 GB on a 1-minute cadence with nothing but idle
/// repeats to show for it.</para>
/// <para>The delta on WAITS decides it, computed once in <see cref="ReadAsync"/> rather than
/// <c>WritePayload</c> for the same reason #2691 documents: the write loop starts the binary-import row
/// before <c>WritePayload</c> runs (see <c>DarlingCollectorRunner</c>), so by the time a collector could
/// refuse to write, the row already exists. A REAL interval (not a first sighting, counter reset, or gap
/// re-baseline — see <see cref="CollectorDeltaCalculator"/>) with zero new waits means the event
/// demonstrably did not fire since the last look, and Aurora only advances <c>wait_time</c> when a wait
/// completes — the same moment <c>waits</c> increments — so a confirmed zero-waits interval means
/// <c>wait_time</c> is unchanged too. interval == 0 covers first-sight/reset/gap-reset together, and all
/// three are genuinely new information, so all three still ship.</para>
/// <para>The wait-time delta series is still computed for every row this reads, including ones this
/// ultimately skips — not for symmetry, but because <see cref="CollectorDeltaCalculator"/> refreshes a
/// series' cached timestamp only when it is called. Skipping that call during an idle stretch would leave
/// the wait-time series' baseline stale, so the next GENUINELY active cycle could see a gap exceeding
/// <see cref="CollectorDeltaCalculator.DefaultMaxGapSeconds"/> against a baseline that is, in fact, still
/// current — reporting a false counter-reset on an event whose waits delta correctly showed the activity.</para>
/// </summary>
public sealed class PgWaitStatsCollector : PostgresCollectorDefinitionBase<PgWaitStatsCollector.Row>
{
    public static PgWaitStatsCollector Instance { get; } = new();

    private PgWaitStatsCollector()
    {
    }

    public readonly record struct Row(
        int TypeId,
        long EventId,
        string? TypeName,
        string? EventName,
        long Waits,
        long WaitTimeMicroseconds,
        /* Computed in ReadAsync, not WritePayload — see the class remarks on why the delta decides
           whether this row ships at all, which means it must exist before the row is (or is not)
           added to the list WritePayload will later be called once per. */
        long DeltaWaits,
        long DeltaWaitTime);

    /* Wait TYPES whose events are never a finding. Filtered by type rather than by event name so a
       new background worker in a future Aurora release is excluded automatically instead of arriving
       as a spike.

         Activity — the server process is idle. Every event here is a background sleep loop
           (WalWriterMain, CheckpointerMain, AutoVacuumMain, AuroraServerlessMonitoringMain, ...) with
           a fixed cycle, so wait_time grows at ~1 second per second of uptime forever. Measured on a
           prod cluster: AuroraRuntimeMain alone at 8,563,569 seconds over 8,562,216 seconds of
           uptime. Left in, it is 99%+ of the chart.
         Client — waiting on the application's socket. ClientRead measured at 565,758,023 seconds on
           one prod writer and 1,674,540,998 on another: that is idle connections, i.e. the app, not
           the database.
         Timeout — deliberate sleeps, including VacuumDelay (autovacuum's own cost-limit pause).

       Compared with the SQL Server side this is a type-level list, not the per-name
       IgnoredWaitDefaults set, because Postgres wait types partition much more cleanly by intent. */
    /* Case-insensitive on purpose: Aurora renamed events between majors without changing their ids
       (AutoVacuumMain on 16.11, AutovacuumMain on 17.7), so an ordinal set would filter on one major
       and quietly stop filtering on the other. Deltas key on the numeric event_id for the same reason. */
    /// <summary>
    /// The wait TYPES neither PostgreSQL wait collector reports, shared rather than restated (#2630).
    ///
    /// <para>This is one editorial judgment about what counts as a wait, and it has to hold for both
    /// sources or the same server gives two different answers depending on its flavor —
    /// <c>pg_wait_sampling</c> is the stock-PostgreSQL answer to the question this collector answers on
    /// Aurora, and #2625 now tells operators so in as many words. It excluded only <c>Activity</c>, and on
    /// a target with real clients <c>ClientRead</c> was <b>100%</b> of the profile.</para>
    /// </summary>
    public static readonly IReadOnlySet<string> IgnoredWaitTypes =
        new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "Activity", "Client", "Timeout" };

    /* Signatures below are VERIFIED against live Aurora 16.11 and 17.7, not taken from the AWS
       reference, which is wrong about one of them:

         aurora_stat_system_waits() -> (type_id, event_id, waits, wait_time)
         aurora_stat_wait_type()    -> (type_id, type_name)
         aurora_stat_wait_event()   -> (type_id, event_id, event_name)   <- THREE columns

       The AWS docs describe aurora_stat_wait_event() as returning four columns including type_name.
       It returns three, and type_id comes first. Getting that wrong does not error: the alias binds
       event_id to type_id, the join matches nothing, and every event_name comes back NULL. That is
       exactly how this was discovered.

       LEFT JOIN, never NATURAL JOIN (which the AWS example uses): the documented wait-type list omits
       type 2, Aurora Limitless adds type 12, and an inner join silently drops any event whose type is
       not in the lookup. A wait we cannot name is still a wait we must record.

       Explicit casts pin the reader's types. Without them the mapping depends on whether Aurora
       returns int4 or int8 for a given column, and Npgsql's type checking is strict — GetInt64 on an
       int4 column throws.

       No delta computed in SQL: wait_time is cumulative since instance start with NO reset function
       anywhere in the Aurora API, so the only way to get an interval is snapshot-and-subtract, which
       is what the shared delta machinery already does for SQL Server. A counter that went backwards
       means the instance restarted. */
    private const string QueryText = @"
SELECT
    w.type_id::int          AS type_id,
    w.event_id::bigint      AS event_id,
    t.type_name             AS type_name,
    e.event_name            AS event_name,
    w.waits::bigint         AS waits,
    w.wait_time::bigint     AS wait_time_us
FROM aurora_stat_system_waits() AS w(type_id, event_id, waits, wait_time)
LEFT JOIN aurora_stat_wait_type() AS t(type_id, type_name)
       ON t.type_id = w.type_id
LEFT JOIN aurora_stat_wait_event() AS e(type_id, event_id, event_name)
       /* BOTH ids. event_id is unique across types today because Aurora packs the type into its high byte
          (event_id >> 24 = type_id), so joining on event_id alone happens to be correct — but that is an
          undocumented encoding, the function hands us type_id right there, and adding it costs nothing.
          Without it, a future Aurora that reuses an event_id under a different type would silently produce a
          row-multiplying join and double every wait figure for the affected events. */
       ON  e.event_id = w.event_id
       AND e.type_id  = w.type_id
WHERE w.wait_time > 0";

    public override string Name => "pg_wait_stats";

    public override string TargetTable => "pg_wait_stats";

    /// <summary>
    /// Aurora only. The wait functions are Aurora built-ins; stock PostgreSQL has no cumulative wait
    /// source at all, so there is nothing for this collector to read there.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => target.IsAurora;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("wait_type_id", CollectorColumnType.Integer),
        new CollectorColumn("wait_event_id", CollectorColumnType.BigInt),
        new CollectorColumn("wait_type", CollectorColumnType.Varchar),
        new CollectorColumn("wait_event", CollectorColumnType.Varchar),
        new CollectorColumn("waits", CollectorColumnType.BigInt),
        new CollectorColumn("wait_time_us", CollectorColumnType.BigInt),
        new CollectorColumn("delta_waits", CollectorColumnType.BigInt),
        new CollectorColumn("delta_wait_time_us", CollectorColumnType.BigInt),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            var typeName = reader.IsDBNull(2) ? null : reader.GetString(2);

            /* Filter by type name, case-insensitively and on our own list rather than
               context.IgnoredWaitTypes, which holds SQL Server wait-type names. An undecodable type
               (NULL type_name) is deliberately KEPT: it means the lookup did not know the type, and
               dropping it would hide exactly the new-wait-type case worth seeing. */
            if (typeName is not null && IgnoredWaitTypes.Contains(typeName))
            {
                continue;
            }

            var eventId = reader.GetInt64(1);
            var waits = reader.GetInt64(4);
            var waitTimeMicroseconds = reader.GetInt64(5);

            /* Delta key is the numeric event_id, NOT the event name. Wait-event NAME CASING DIFFERS
               BETWEEN AURORA MAJORS — 16.11 emits AutoVacuumMain and BgWriterMain where 17.7 emits
               AutovacuumMain and BgwriterMain — so a name-keyed delta would break its own history the
               moment a cluster is upgraded, reading as one series ending and another beginning. The id
               is stable, and event_id >> 24 == type_id holds (verified on every event on two clusters),
               so the id also carries the type. */
            var key = eventId.ToString(System.Globalization.CultureInfo.InvariantCulture);

            var deltaWaits = context.Deltas.CalculateDeltaWithInterval(
                context.ServerId, "pg_wait_stats_waits", key, waits, out var waitsIntervalSeconds,
                collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
            /* Computed unconditionally, even on a row this pass goes on to skip — see the class remarks
               on why the wait-time series' baseline must stay fresh regardless. */
            var deltaWaitTime = context.Deltas.CalculateDelta(
                context.ServerId, "pg_wait_stats_time", key, waitTimeMicroseconds,
                collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);

            /* The skip: a REAL interval (this is not a first sighting, a counter reset, or a gap this
               pass just re-baselined) with zero new waits means the event demonstrably did not fire,
               and Aurora only advances wait_time when a wait completes — so nothing on this row changed
               and there is nothing new to write. See the class remarks for the full reasoning. */
            if (waitsIntervalSeconds > 0 && deltaWaits == 0)
            {
                continue;
            }

            rows.Add(new Row(
                TypeId: reader.GetInt32(0),
                EventId: eventId,
                TypeName: typeName,
                EventName: reader.IsDBNull(3) ? null : reader.GetString(3),
                Waits: waits,
                WaitTimeMicroseconds: waitTimeMicroseconds,
                DeltaWaits: deltaWaits,
                DeltaWaitTime: deltaWaitTime));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.TypeId)                  /* wait_type_id INTEGER */
            .Value(row.EventId)                 /* wait_event_id BIGINT */
            .Value(row.TypeName)                /* wait_type VARCHAR */
            .Value(row.EventName)               /* wait_event VARCHAR */
            .Value(row.Waits)                   /* waits BIGINT */
            .Value(row.WaitTimeMicroseconds)    /* wait_time_us BIGINT */
            .Value(row.DeltaWaits)              /* delta_waits BIGINT */
            .Value(row.DeltaWaitTime);           /* delta_wait_time_us BIGINT */
    }
}
