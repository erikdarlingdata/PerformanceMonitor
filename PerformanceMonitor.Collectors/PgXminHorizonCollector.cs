/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// What is holding back the xmin horizon — the reason autovacuum can run, report success, and still
/// reclaim nothing.
/// <para>This is the highest-leverage composite signal in PostgreSQL monitoring, because <b>four
/// unrelated causes present identically</b>: dead tuples accumulate, autovacuum runs and logs success,
/// and the table never shrinks. A long-running or idle-in-transaction session, an abandoned replication
/// slot, a standby feeding back its xmin with <c>hot_standby_feedback</c>, and an orphaned prepared
/// transaction all produce exactly that picture — and the remedy is completely different for each. Kill
/// a session, drop a slot, disable feedback, or commit/rollback a two-phase transaction: doing the wrong
/// one is at best useless.</para>
/// <para>So attribution IS the deliverable. A collector that reported only "the horizon is old" would
/// leave the reader exactly where they started, which is why this emits the oldest holder <i>per source</i>
/// and marks which source is winning, rather than a single aggregate number.</para>
/// <para>The consequence compounds beyond bloat: a pinned horizon also blocks freezing, so an unattended
/// xmin holder is an upstream cause of the wraparound risk <see cref="PgWraparoundStatsCollector"/>
/// measures. Core catalog surfaces only, so this runs on any PostgreSQL target.</para>
/// </summary>
public sealed class PgXminHorizonCollector : PostgresCollectorDefinitionBase<PgXminHorizonCollector.Row>
{
    public static PgXminHorizonCollector Instance { get; } = new();

    private PgXminHorizonCollector()
    {
    }

    public readonly record struct Row(
        string Source,
        long XminAge,
        string? Holder,
        string? Detail,
        bool IsWinner);

    /* One UNION ALL branch per cause, reduced to the OLDEST holder per source by DISTINCT ON. That
       bound matters: pg_stat_activity can carry hundreds of backends with an xmin, and storing all of
       them every cycle would be a lot of rows to say one thing. Five rows per collection at most.

       Two branches come from pg_replication_slots deliberately. A slot's xmin holds back ordinary row
       cleanup; its catalog_xmin holds back CATALOG cleanup specifically, which is what a logical
       decoding slot pins, and the two can differ by a lot. Reporting only one would misattribute the
       other.

       Every branch is independently empty-tolerant. No holder for a cause simply yields no row, and
       zero rows overall is the healthy state — nothing is pinning the horizon. pg_stat_replication in
       particular is expected to be empty on Aurora, where replicas read the same storage volume rather
       than streaming WAL, so its absence must not read as an error.

       age() rather than arithmetic on the raw xid: modular wrap makes naive subtraction wrong exactly
       near the boundary. */
    private const string QueryText = @"
WITH holders AS
(
    SELECT
        'session'::text                                       AS source,
        /* The GREATER of the two ages. An idle-in-transaction writer under READ COMMITTED has RELEASED its
           snapshot — backend_xmin is NULL — while still holding backend_xid, which pins the horizon just as
           hard. Reading only backend_xmin made this collector blind to idle-in-transaction, which is its
           single most-cited cause and the one the read surface leads with. */
        GREATEST(
            coalesce(age(a.backend_xmin), 0),
            coalesce(age(a.backend_xid), 0))::bigint            AS xmin_age,
        a.pid::text                                            AS holder,
        'state=' || coalesce(a.state, '(none)')
            || ' application=' || coalesce(a.application_name, '(none)')
            /* AT TIME ZONE 'UTC', not ::text. These are timestamptz, so ::text renders in the SESSION
               TimeZone — invisible on a UTC server, wrong everywhere else. The store's contract is naive UTC
               and this detail string was the one place on the branch still breaking it. */
            || ' xact_start=' || coalesce((a.xact_start AT TIME ZONE 'UTC')::text, '(none)')
            || ' query_start=' || coalesce((a.query_start AT TIME ZONE 'UTC')::text, '(none)') AS detail
    FROM pg_stat_activity AS a
    WHERE (a.backend_xmin IS NOT NULL OR a.backend_xid IS NOT NULL)
    /* Never attribute the horizon to the collector's own snapshot. Darling's read sits in
       pg_stat_activity with a backend_xmin like any other session, so without this it is a PERMANENT
       'session' holder: zero-rows-when-healthy becomes unreachable, and it silently pads the persistence
       denominator so a real transient holder reads as chronic. */
    AND   a.pid <> pg_backend_pid()

    UNION ALL

    SELECT
        'replication_slot'::text,
        age(s.xmin)::bigint,
        s.slot_name,
        'type=' || coalesce(s.slot_type, '(none)')
            || ' active=' || coalesce(s.active::text, '(none)')
            || ' database=' || coalesce(s.database, '(none)')
    FROM pg_replication_slots AS s
    WHERE s.xmin IS NOT NULL

    UNION ALL

    SELECT
        'replication_slot_catalog'::text,
        age(s.catalog_xmin)::bigint,
        s.slot_name,
        'type=' || coalesce(s.slot_type, '(none)')
            || ' active=' || coalesce(s.active::text, '(none)')
            || ' plugin=' || coalesce(s.plugin, '(none)')
    FROM pg_replication_slots AS s
    WHERE s.catalog_xmin IS NOT NULL

    UNION ALL

    SELECT
        'standby_feedback'::text,
        age(r.backend_xmin)::bigint,
        coalesce(r.application_name, r.client_addr::text, r.pid::text),
        'state=' || coalesce(r.state, '(none)')
            || ' sync_state=' || coalesce(r.sync_state, '(none)')
    FROM pg_stat_replication AS r
    WHERE r.backend_xmin IS NOT NULL

    UNION ALL

    SELECT
        'prepared_transaction'::text,
        age(p.transaction)::bigint,
        p.gid,
        'prepared=' || coalesce((p.prepared AT TIME ZONE 'UTC')::text, '(none)')
            || ' owner=' || coalesce(p.owner, '(none)')
            || ' database=' || coalesce(p.database, '(none)')
    FROM pg_prepared_xacts AS p
)
SELECT DISTINCT ON (source)
    source,
    xmin_age,
    holder,
    detail
FROM holders
WHERE xmin_age IS NOT NULL
ORDER BY source, xmin_age DESC";

    public override string Name => "pg_xmin_horizon";

    public override string TargetTable => "pg_xmin_horizon";

    /// <summary>Core catalog surfaces only — any PostgreSQL target.</summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("source", CollectorColumnType.Varchar),
        new CollectorColumn("xmin_age", CollectorColumnType.BigInt),
        new CollectorColumn("holder", CollectorColumnType.Varchar),
        new CollectorColumn("detail", CollectorColumnType.Varchar),
        /* Stamped at collection rather than derived on read, so a stored row always names the winner as
           of the moment it was measured. Deriving it later would depend on which rows a query happened
           to select, and a filtered read could crown a different winner than actually held the horizon. */
        new CollectorColumn("is_winner", CollectorColumnType.Boolean),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var holders = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            holders.Add(new Row(
                Source: reader.GetString(0),
                XminAge: reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                Holder: reader.IsDBNull(2) ? null : reader.GetString(2),
                Detail: reader.IsDBNull(3) ? null : reader.GetString(3),
                IsWinner: false));
        }

        if (holders.Count == 0)
        {
            /* Nothing pins the horizon. The healthy state, and the reason an empty result must never be
               treated as a collection failure. */
            return holders;
        }

        /* The winner is the single oldest holder across all sources — the one actually setting the
           horizon. Ties resolve to the first source encountered, which is immaterial: if two causes are
           at identical age, both need attention. */
        var oldest = holders.Max(h => h.XminAge);
        var winnerStamped = false;
        for (var i = 0; i < holders.Count; i++)
        {
            if (!winnerStamped && holders[i].XminAge == oldest)
            {
                holders[i] = holders[i] with { IsWinner = true };
                winnerStamped = true;
            }
        }

        return holders;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No deltas: like freeze age, an xmin age is a level. What matters is how old the horizon is
           right now and what is holding it, not how fast it aged since the last sample. */
        writer
            .Value(row.Source)
            .Value(row.XminAge)
            .Value(row.Holder)
            .Value(row.Detail)
            .Value(row.IsWinner);
    }
}
