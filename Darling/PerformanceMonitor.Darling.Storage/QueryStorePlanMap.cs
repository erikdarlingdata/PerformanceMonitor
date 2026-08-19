/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The <c>(server_id, database_name, plan_id) → digest</c> map that lets Query Store facts reference plan XML
/// they no longer carry (#2210). Query Store plan XML was stored INLINE on <c>query_store_stats</c> — measured
/// at 43 GB total, 3,743 MB of plan XML per day, and 871,196 XML-carrying rows against 175,328 distinct
/// <c>(database_name, plan_id)</c>, so **5.0x** of it was the same plans re-shipped. The plan fetch now lands
/// each plan once, in <c>plan_id</c> order, and this map is how a fact row finds its content.
///
/// <para>Facts deliberately do NOT gain a digest column, which is why this table exists rather than a
/// <see cref="PayloadDimensions"/> entry: a fact row is written when the runtime stats arrive, potentially
/// budgeted cycles BEFORE its plan's XML is fetched, so there is no digest to write at fact time. The map's
/// absence of a row IS the pending state, and a reader with no map row renders "plan not yet collected"
/// instead of resolving to nothing. Since #2312 the map is also the FETCH's source of truth — the
/// activity-driven fetch asks <see cref="TouchAndProbeSql"/> which of the cycle's referenced plans lack a
/// row and fetches exactly those, which is what retired the per-database watermark and its
/// daily-expiry catalog walk.</para>
///
/// <para><b>THIS TABLE'S last_seen IS LOAD-BEARING, AND IT IS THE ONLY PROTECTION QUERY STORE DIGESTS HAVE.</b>
/// The dimension GC does not enumerate references — an anti-join per dim row against two hypertables is not
/// affordable at this size — so it sweeps on <c>last_seen</c>, which must be refreshed on every cycle
/// that references a digest. Ending plan re-shipping ended the accidental refresh the walk provided, and a
/// plan fetched once would have its dim row collected while live facts still referenced it. Hence
/// <see cref="TouchAndProbeSql"/>, which asserts liveness for every plan the cycle's batch references —
/// designed for exactly this in #2210, unwired until #2312 Finding 3.</para>
///
/// <para>The GC's second belt does not cover this either: its cutoff is clamped to one day before the oldest
/// surviving DIGEST-CARRYING fact (<c>DarlingRetention.ComputeDimensionCutoff</c>), and Query Store facts carry
/// no digest, so the measured floor is blind to them. Do not add a Query Store entry to
/// <see cref="PayloadDimensions.All"/> to "fix" that — the entry means "this fact column holds a digest",
/// which is not true here, and the clamp would still be measuring a column that does not exist.
/// <see cref="TouchAndProbeSql"/> is the whole of the protection.</para>
/// </summary>
public static class QueryStorePlanMap
{
    /// <summary>The map table. Not a hypertable: one row per distinct plan per database, ~175k rows/day of
    /// churn on the measured fleet and near-static once a catalog is warm, so it is dimension-shaped rather
    /// than time-series and is pruned on <see cref="LastSeenColumn"/> rather than by drop_chunks.</summary>
    public const string TableName = "collect.query_store_plan_map";

    /// <summary>The liveness column, swept by the prune and stamped by <see cref="TouchAndProbeSql"/>.</summary>
    public const string LastSeenColumn = "last_seen";

    /* This const mirrors the IMMUTABLE migration rung that created the table and stays byte-frozen with it.
       The LIVE shape differs in one place: V77 relaxed digest to nullable for the #2312 content-less marker
       rows (a plan whose XML the engine cannot persist gets a map row with a NULL digest, so the probe reads
       it as known instead of refetching it forever). */
    public const string CreateTableSql = @"CREATE TABLE IF NOT EXISTS collect.query_store_plan_map (
    server_id integer NOT NULL,
    database_name text NOT NULL,
    plan_id bigint NOT NULL,
    digest bytea NOT NULL,
    plan_hash text,
    last_seen timestamp NOT NULL,
    PRIMARY KEY (server_id, database_name, plan_id)
);
CREATE INDEX IF NOT EXISTS idx_query_store_plan_map_last_seen ON collect.query_store_plan_map(last_seen);";

    /* Deliberately NO index on digest. Nothing reads this table by digest: readers resolve
       (server_id, database_name, plan_id) -> digest through the primary key, the liveness touch joins on that
       same key, and the dimension GC sweeps its OWN last_seen rather than asking who references a digest. An
       index nothing queries is pure write tax on a table every plan fetch upserts into. If a reverse lookup
       ("which plans share this content") is ever wanted, add it with the query that needs it. */

    /// <summary>
    /// Records what the plan fetch landed: one row per plan, carrying the digest of the content written to
    /// <c>query_plan_dim</c> — or a NULL digest for a plan whose XML the engine itself reports as absent
    /// (too large to persist, certain forced-plan-failure paths). The NULL-digest row is the #2312
    /// content-less MARKER: under store-as-watermark, "seen, and the content will never exist" has to be a
    /// stored fact, or the probe would re-select those plans as missing on every cycle forever — the old
    /// watermark's stall reborn in miniature. V77 relaxed the column for exactly this row shape.
    ///
    /// <para>Both COALESCEs in the conflict arm point the same direction — never replace knowledge with
    /// absence. A refetch that comes back NULL for a plan whose content the store already holds keeps the
    /// content (<c>digest</c>); a fetch path that did not carry a hash keeps the stored one
    /// (<c>plan_hash</c>). A refetch that carries REAL content or a REAL hash still advances both, which is
    /// how an in-place rewrite's corrected content gets pointed at.</para>
    ///
    /// <para><c>plan_hash</c> is the in-place-rewrite detector, stored rather than derived because
    /// <c>sys.query_store_plan.query_plan_hash</c> reads WITHOUT decompressing the plan:
    /// <see cref="TouchAndProbeSql"/> compares it against the batch's live hash on every cycle, so an
    /// active plan whose XML was rewritten in place (same <c>plan_id</c>, new content) is refetched within
    /// one cycle — where the retired re-verify cursor would have taken up to a day to reach it, had it ever
    /// been wired (#2312 Finding 4: it was not). Measured base rate: 0 of 38,420 plan_ids changed hash in a
    /// day of fleet data.</para>
    ///
    /// <para>Ordered by the conflict key. Same reason as <see cref="DarlingModuleMap.RefreshSql"/>: concurrent
    /// batch upserts that take row locks in different relative orders deadlock (#1801), and a plan fetch runs
    /// per database against a fleet of servers. Checked, not assumed — do not drop the ORDER BY on the belief
    /// that a single-row-per-plan insert cannot conflict with anything.</para>
    /// </summary>
    public const string UpsertSql = @"INSERT INTO collect.query_store_plan_map
    (server_id, database_name, plan_id, digest, plan_hash, last_seen)
SELECT server_id, database_name, plan_id, digest, plan_hash, stamped
FROM unnest($1::integer[], $2::text[], $3::bigint[], $4::bytea[], $5::text[], $6::timestamp[])
     AS batch(server_id, database_name, plan_id, digest, plan_hash, stamped)
ORDER BY server_id, database_name, plan_id
ON CONFLICT (server_id, database_name, plan_id) DO UPDATE SET
    digest = COALESCE(EXCLUDED.digest, query_store_plan_map.digest),
    plan_hash = COALESCE(EXCLUDED.plan_hash, query_store_plan_map.plan_hash),
    last_seen = EXCLUDED.last_seen
WHERE EXCLUDED.last_seen >= query_store_plan_map.last_seen";

    /// <summary>
    /// The liveness assertion AND the missing-set probe, one round trip (#2312): for the distinct
    /// <c>(database_name, plan_id, plan_hash)</c> a cycle's runtime batch references, refresh BOTH this map
    /// row's <c>last_seen</c> and the dimension row's — so neither can age out while facts still point at
    /// the plan — and return, per batch row, whether the store already resolves it and whether its stored
    /// hash still matches the engine's live one. The batch already carries all three columns, so nothing
    /// extra is collected to make this work.
    ///
    /// <para>Both timestamps are stamped by the SAME pass, which is what makes the map-prune-versus-dim-GC race
    /// structurally impossible rather than carefully avoided: a map row's <c>last_seen</c> can never be older
    /// than the newest fact batch that referenced it, so the prune cannot take a row that live facts are
    /// touching. This is also what makes the V75 plan-content horizon mean what it says for Query Store
    /// plans: content ages out N-days-since-last-REFERENCE, not since-last-refetch — before #2312 wired
    /// this, the perpetual daily catalog walk was accidentally standing in for it.</para>
    ///
    /// <para>The probe columns are the activity-driven fetch's entire input. <c>resolved</c> is "a map row
    /// exists" — INCLUDING the NULL-digest content-less markers, which is the point of storing them: a plan
    /// the engine cannot persist must read as known, or it rides every cycle's fetch list forever.
    /// <c>hash_stale</c> is the in-place-rewrite signal: a stored hash that differs from the batch's live
    /// one means the plan kept its id and changed its content, and the caller refetches it. A stored hash
    /// of NULL is never stale — it is adopted from the batch in the same statement (legacy rows from before
    /// the fetch carried hashes), so the fleet's hash coverage backfills organically with zero refetches.</para>
    ///
    /// <para>What this deliberately does NOT do is detect Query Store resets, because nothing needs to any
    /// more: a reset renumbers plans, the new ids come back unresolved, and the fetch list picks them up
    /// budget-bounded — recovery is the normal path rather than a special arm. (The retired watermark design
    /// needed the mass-absence judgement precisely because it had a watermark to zero; see #2312.)</para>
    ///
    /// <para>The preceding CTEs still run. Postgres executes data-modifying <c>WITH</c> statements exactly
    /// once and to completion whether or not the primary query reads their output, so making the final statement
    /// a SELECT does not turn the liveness stamping into a no-op. That is a load-bearing detail: if it were not
    /// true, this restructure would silently stop refreshing <c>last_seen</c> and reintroduce the GC hazard the
    /// touch exists to prevent.</para>
    ///
    /// <para>Guarded at one hour like the dimension upsert's own conflict arm, and for the same reason — the
    /// horizons are multi-day, so an update per row per hour is enough freshness and the write amplification
    /// stays bounded on a hot catalog. The margin arithmetic already accounts for this trailing hour. Hash
    /// adoption rides the same guard: it is a backfill, not a correctness deadline, and un-guarding it would
    /// re-write every legacy row on every cycle until the first touch landed.</para>
    ///
    /// <para>The CTE is ordered by the map's primary key for the #1801 reason on <see cref="UpsertSql"/> —
    /// this is the one statement in the design that touches many rows across two tables on every cycle of
    /// every server, so it is the most likely place for an unordered-batch deadlock to form. Being precise
    /// about how much that buys: an <c>ORDER BY</c> inside a CTE feeding <c>UPDATE ... FROM</c> is NOT a
    /// guaranteed lock-acquisition order in Postgres the way ordering an <c>INSERT ... ON CONFLICT</c>'s
    /// input is — the planner may reorder. It makes the common plan deterministic rather than making the
    /// deadlock impossible. If one is observed, the fix is to drive the update from an explicitly ordered
    /// <c>SELECT ... FOR UPDATE</c>, not to widen this comment.</para>
    /// </summary>
    public const string TouchAndProbeSql = @"WITH touched AS (
    SELECT m.server_id, m.database_name, m.plan_id, m.digest, batch.plan_hash AS live_hash
    FROM collect.query_store_plan_map AS m
    JOIN unnest($1::integer[], $2::text[], $3::bigint[], $4::text[])
         AS batch(server_id, database_name, plan_id, plan_hash)
      ON  batch.server_id = m.server_id
      AND batch.database_name = m.database_name
      AND batch.plan_id = m.plan_id
    WHERE m.last_seen < $5::timestamp - interval '1 hour'
    ORDER BY m.server_id, m.database_name, m.plan_id
),
map_touch AS (
    UPDATE collect.query_store_plan_map AS m
    SET last_seen = $5::timestamp,
        plan_hash = COALESCE(m.plan_hash, t.live_hash)
    FROM touched AS t
    WHERE m.server_id = t.server_id
      AND m.database_name = t.database_name
      AND m.plan_id = t.plan_id
    RETURNING t.digest
),
dim_touch AS (
    UPDATE collect.query_plan_dim AS d
    SET last_seen = $5::timestamp
    WHERE d.digest IN (SELECT digest FROM map_touch WHERE digest IS NOT NULL)
      AND d.last_seen < $5::timestamp - interval '1 hour'
    RETURNING d.digest
)
SELECT
    batch.server_id,
    batch.database_name,
    batch.plan_id,
    (m.plan_id IS NOT NULL) AS resolved,
    (m.plan_id IS NOT NULL AND m.plan_hash IS NOT NULL AND batch.plan_hash IS NOT NULL
        AND m.plan_hash <> batch.plan_hash) AS hash_stale
FROM unnest($1::integer[], $2::text[], $3::bigint[], $4::text[])
     AS batch(server_id, database_name, plan_id, plan_hash)
LEFT JOIN collect.query_store_plan_map AS m
       ON  m.server_id = batch.server_id
       AND m.database_name = batch.database_name
       AND m.plan_id = batch.plan_id
ORDER BY batch.server_id, batch.database_name, batch.plan_id";

    /// <summary>
    /// Strips the <see cref="DateTimeKind"/> off a timestamp before it is bound to any of this class's
    /// <c>::timestamp</c> parameters. Every call site that binds a <see cref="DateTime"/> here must go through
    /// this — <see cref="UpsertSql"/>'s stamp array and <see cref="TouchAndProbeSql"/>'s <c>$5</c>, plus the prune's
    /// cutoff.
    ///
    /// <para>This is the #1969 trap, and it is silent. Npgsql infers the parameter type from the value's Kind:
    /// a <c>Utc</c> or <c>Local</c> DateTime infers <c>timestamptz</c>, Postgres then converts it into the
    /// session time zone on the way into a naive <c>timestamp</c> column, and the row lands at the wrong hour
    /// with no error anywhere. For the liveness columns that is worse than a visible failure: a
    /// <c>last_seen</c> written hours early ages a map or dimension row out ahead of the facts that reference
    /// it, which is the silent-missing-plans outcome this whole design is built to prevent, arrived at through
    /// a timezone rather than through a GC bug.</para>
    ///
    /// <para>A helper rather than a convention because a convention is what fails at the one call site somebody
    /// adds later. The value is not shifted, only relabelled: callers are expected to pass UTC already, and
    /// <see cref="System.DateTime.SpecifyKind"/> changes the Kind without touching the ticks.</para>
    /// </summary>
    public static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    /// <summary>
    /// Days of margin the map prune adds past the fact-retention horizon. **Strictly less than the dimension
    /// GC's margin**, which is <c>ChunkIntervalDays + 1</c> — see <see cref="MarginOrderingHolds"/> for why
    /// that direction is the safe one and not merely a convention.
    /// </summary>
    public const int PruneMarginDays = 1;

    /// <summary>
    /// The invariant the two horizons must satisfy: the DIMENSION must outlive the MAP, because the two bad
    /// end-states are not symmetric.
    ///
    /// <para>A pruned map row whose dim row survives is a plan rendering "not collected" plus some dim bytes
    /// that go unreclaimed until the dim's own horizon passes — visibly degraded, self-correcting, no wrong
    /// answers. A pruned DIM row whose map row survives is a reader resolving a live fact to absent content,
    /// which is the silent-missing-plans failure this entire design exists to prevent. Ordering the margins
    /// makes the recoverable end-state the only reachable one.</para>
    ///
    /// <para>Pinned as a function rather than asserted in a comment so a future change to either margin — or to
    /// <c>ChunkIntervalDays</c>, which is where the dim's margin comes from — fails a test instead of quietly
    /// inverting the ordering.</para>
    /// </summary>
    public static bool MarginOrderingHolds(int chunkIntervalDays) =>
        PruneMarginDays < chunkIntervalDays + 1;

    /// <summary>
    /// Retires map rows whose facts have all aged out: an index range scan on <see cref="LastSeenColumn"/>
    /// against the fact horizon plus <see cref="PruneMarginDays"/>, time-sliced like every sibling purge.
    ///
    /// <para>Timestamp-driven, NOT an existence check against <c>query_store_stats</c>. An anti-join against a
    /// 43 GB hypertable per map row is exactly the cost this architecture avoids, and it is unnecessary here
    /// because <see cref="TouchAndProbeSql"/> keeps <c>last_seen</c> current for anything live — the same argument the
    /// dimension GC already rests on, applied to one more timestamped table.</para>
    ///
    /// <para>A plan whose query goes quiet needs no special handling: it stops being touched, its facts age out
    /// within retention, and the margin ordering retires the map row before the dim row it points at.</para>
    /// </summary>
    public static string PruneSql(int chunkIntervalDays) =>
        "DELETE FROM collect.query_store_plan_map WHERE " + LastSeenColumn + " < $1" +
        " AND " + LastSeenColumn + " >= (SELECT min(" + LastSeenColumn + ") FROM collect.query_store_plan_map WHERE " +
        LastSeenColumn + " < $1)" +
        " AND " + LastSeenColumn + " < (SELECT min(" + LastSeenColumn + ") FROM collect.query_store_plan_map WHERE " +
        LastSeenColumn + " < $1) + INTERVAL '" +
        chunkIntervalDays.ToString(CultureInfo.InvariantCulture) + " days'";
}
