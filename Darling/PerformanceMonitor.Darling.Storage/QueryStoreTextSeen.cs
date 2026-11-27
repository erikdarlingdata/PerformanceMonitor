/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The liveness stamp of <c>collect.query_store_text</c>, in its own narrow table (#9043): one row per
/// <c>(server_id, database_name, query_id)</c> holding only <c>last_seen</c>.
///
/// <para><b>Why the stamp is not on the text row.</b> A text row is wide (about 1.4 KB on a large store) and
/// carries a trigram GIN index for <c>find_queries_by_text</c>. The pages of a busy store are full, so most
/// updates of a text row cannot stay heap-only, and an update that is not heap-only writes a new copy of the
/// whole row and every trigram of an unchanged text into the GIN. Measured on PostgreSQL 18 with 3.45M texts
/// of about 1.4 KB at fillfactor 90 and a 2.1 GB server-keyed trigram index, one 1,500-row liveness touch at
/// about 9% heap-only: 308-455 ms and 33-55 MB of WAL on the text row, against 4-5 ms and 0.5 MB on this
/// table. Without the trigram index the text-row touch is 10-13 ms and 5 MB.</para>
///
/// <para><b>Who writes it.</b> A statement trigger on <c>collect.query_store_text</c>
/// (<see cref="TriggerSql"/>) inserts the seen row of every text row an <c>INSERT</c> lands, whatever build
/// runs the insert: this build's <see cref="QueryStoreTextStore.UpsertSql"/>, an older build run against the
/// same store after a rollback, or an older instance that shares the store. The liveness touch
/// (<see cref="QueryStoreTextStore.TouchSeenSql"/>) is an upsert, so a referenced text with no seen row gets
/// one, and the daily prune first gives every other text with no seen row one (<see cref="AdoptSlice"/>).
/// The foreign key cascades, so every delete of a text row removes its seen row.</para>
///
/// <para><b>How it is read.</b> A text's effective stamp is the later of the two stamps (the prune tests both
/// against its cutoff, and <c>find_queries_by_text</c> returns <c>GREATEST</c> of the two with the seen row
/// <c>LEFT JOIN</c>ed, which ignores a NULL): <c>query_store_text.last_seen</c> still moves on every upsert, on the
/// rollback refresh (<see cref="TextRowRefreshHours"/>), and on every touch of an older build. A text with
/// no seen row reads as its own <c>last_seen</c>, never as live forever and never as expired.</para>
///
/// <para><b>Lock order.</b> Every writer takes the text row before the seen row, each in key order: the
/// upsert inserts the text rows and its trigger then inserts their seen rows, the touch refreshes text rows
/// in its first statement and upserts seen rows in its second, and the prune deletes a text row and the
/// cascade then deletes its seen row.</para>
///
/// <para>A plain table, never a hypertable, with one primary key and no index on <c>last_seen</c>: the touch
/// changes no indexed column, so it stays heap-only wherever the page has room, and fillfactor 90 keeps that
/// room. The prune reads it with a sequential scan, which is about 1/28 of the text heap (209 MB against
/// 5,958 MB at 3.45M rows).</para>
/// </summary>
public static class QueryStoreTextSeen
{
    public const string TableName = "collect.query_store_text_seen";

    /// <summary>The liveness column, the same name as <see cref="QueryStoreTextStore.LastSeenColumn"/>.</summary>
    public const string LastSeenColumn = "last_seen";

    /// <summary>
    /// The table of the rung (V281), created with no key. <see cref="PrimaryKeySql"/> and <see cref="ForeignKeySql"/> add
    /// the two constraints after <see cref="BackfillSql"/>, because loading first is several times cheaper. Measured on
    /// PostgreSQL 18 against 6.9 million text rows (a 4.2 GB heap): with the primary key in place and
    /// <c>ON CONFLICT DO NOTHING</c> the load took 51-55 s and wrote 1.6 GB of WAL into a 393 MB index. Loaded bare, it took
    /// 3.7 s and 0.6 GB of WAL, the key then built in 2.3-2.4 s as a 243 MB index, and the foreign key validated in
    /// 4.8-4.9 s, about 11 s in all. A foreign key present during the load would also check every inserted row with a
    /// <c>FOR KEY SHARE</c> lock on its text row, which writes every page of the text heap.
    /// </summary>
    public const string CreateTableSql = @"
CREATE TABLE IF NOT EXISTS collect.query_store_text_seen (
    server_id integer NOT NULL,
    database_name text NOT NULL,
    query_id bigint NOT NULL,
    last_seen timestamp NOT NULL
) WITH (fillfactor = 90);";

    /// <summary>The primary key's name, the one PostgreSQL gives an inline key.</summary>
    public const string PrimaryKeyName = "query_store_text_seen_pkey";

    /// <summary>The primary key, added after <see cref="BackfillSql"/> so the index is built once from sorted keys.
    /// Idempotent.</summary>
    public const string PrimaryKeySql = @"
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'collect.query_store_text_seen'::regclass AND contype = 'p') THEN
        ALTER TABLE collect.query_store_text_seen
            ADD CONSTRAINT " + PrimaryKeyName + @" PRIMARY KEY (server_id, database_name, query_id);
    END IF;
END
$$;";

    /// <summary>The trigger function's name, schema-qualified.</summary>
    public const string TrackFunctionName = "collect.query_store_text_seen_track";

    /// <summary>The insert trigger on <c>collect.query_store_text</c>.</summary>
    public const string InsertTriggerName = "trg_query_store_text_seen_ins";

    /// <summary>
    /// The trigger of the rung (V281): after each <c>INSERT</c> statement on <c>collect.query_store_text</c>, one
    /// <c>INSERT ... SELECT</c> writes the seen rows of the text rows that statement inserted, stamped with their
    /// <c>last_seen</c>, in key order.
    ///
    /// <para><b>Shape.</b> <c>AFTER INSERT ... FOR EACH STATEMENT</c> with a transition table, so a batch of new texts costs one
    /// insert rather than one function call per row. An <c>ON CONFLICT DO UPDATE</c> puts only the rows it inserted in the
    /// transition table, so a text the upsert updated keeps the seen row it has. The liveness touch is an <c>UPDATE</c>, so it
    /// never fires this. <c>ON CONFLICT DO NOTHING</c> because the row can already exist only when something outside the
    /// product wrote it, and the newer stamp is then the one to keep.</para>
    ///
    /// <para><b>Idempotent.</b> <c>CREATE OR REPLACE FUNCTION</c>, then <c>DROP TRIGGER IF EXISTS</c> and <c>CREATE TRIGGER</c>,
    /// all schema-qualified, the shape of <see cref="QueryStoreSelfKeys.TriggerSql"/>. <c>CREATE TRIGGER</c> takes a SHARE ROW
    /// EXCLUSIVE lock on the text table, which the rung holds until it commits, so no text insert falls between the trigger
    /// and the backfill.</para>
    /// </summary>
    public const string TriggerSql = @"
CREATE OR REPLACE FUNCTION " + TrackFunctionName + @"() RETURNS trigger
LANGUAGE plpgsql AS $f$
BEGIN
    INSERT INTO collect.query_store_text_seen (server_id, database_name, query_id, last_seen)
    SELECT n.server_id, n.database_name, n.query_id, n.last_seen
    FROM landed AS n
    ORDER BY n.server_id, n.database_name, n.query_id
    ON CONFLICT (server_id, database_name, query_id) DO NOTHING;
    RETURN NULL;
END
$f$;

DROP TRIGGER IF EXISTS " + InsertTriggerName + @" ON collect.query_store_text;
CREATE TRIGGER " + InsertTriggerName + @"
    AFTER INSERT ON collect.query_store_text
    REFERENCING NEW TABLE AS landed
    FOR EACH STATEMENT
    EXECUTE FUNCTION " + TrackFunctionName + @"();";

    /// <summary>
    /// Writes the seen row of every text row that has none, stamped with the text row's own <c>last_seen</c>. The rung runs
    /// it once after <see cref="TriggerSql"/>, when the seen table is empty, as one sequential pass over the text table's
    /// main heap (it reads no TOAST). The anti-join makes a second run insert nothing, and it needs no key on the seen
    /// table, which the rung adds afterwards. The rung holds <see cref="TriggerSql"/>'s lock on the text table, so no text
    /// row arrives between this read and the key.
    /// </summary>
    public const string BackfillSql = @"
INSERT INTO collect.query_store_text_seen (server_id, database_name, query_id, last_seen)
SELECT t.server_id, t.database_name, t.query_id, t.last_seen
FROM collect.query_store_text AS t
WHERE NOT EXISTS (
    SELECT 1 FROM collect.query_store_text_seen AS s
    WHERE s.server_id = t.server_id AND s.database_name = t.database_name AND s.query_id = t.query_id);";

    /// <summary>
    /// The prune's first step: gives every text row with no seen row one, stamped with the text row's own
    /// <c>last_seen</c>, so the prune that follows judges it by that stamp. A text row is without a seen row only when it
    /// was written with <see cref="InsertTriggerName"/> off (a restore that disables triggers, a session in replica mode),
    /// and a prune that reads the seen table alone would keep such a row for good.
    ///
    /// <para>It runs in keyset slices of <c>(server_id, database_name, query_id)</c>, compared as a row in primary key
    /// order (<see cref="LivenessAdoptSlice"/>), so no statement reads either primary key from end to end and none reads
    /// the text heap except for a text that has no seen row.</para>
    /// </summary>
    public static readonly LivenessAdoptSlice AdoptSlice = new(
        QueryStoreTextStore.TableName, TableName, new[] { "server_id", "database_name", "query_id" }, LastSeenColumn);

    /// <summary>The foreign key's name.</summary>
    public const string ForeignKeyName = "query_store_text_seen_text_fkey";

    /// <summary>
    /// The foreign key to the text row, <c>ON DELETE CASCADE</c>, so the prune, a server's removal and any other delete of a
    /// text row remove its seen row in the same statement. Added after <see cref="BackfillSql"/> and
    /// <see cref="PrimaryKeySql"/>, so its validation is one join instead of a check per backfilled row. The touch changes
    /// no key column, so it never fires the check; only an insert of a seen row does, with a <c>FOR KEY SHARE</c> lock on
    /// the text row. Idempotent.
    /// </summary>
    public const string ForeignKeySql = @"
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'collect.query_store_text_seen'::regclass AND conname = '" + ForeignKeyName + @"') THEN
        ALTER TABLE collect.query_store_text_seen
            ADD CONSTRAINT " + ForeignKeyName + @" FOREIGN KEY (server_id, database_name, query_id)
            REFERENCES collect.query_store_text (server_id, database_name, query_id) ON DELETE CASCADE;
    END IF;
END
$$;";

    /// <summary>
    /// The smallest widest fact retention the configuration allows for
    /// <see cref="QueryStoreTextStore.ReferencingCollectors"/>, in days: the retention resolver clamps every collector at
    /// one day, and none of the three has a higher product floor. <c>QueryStoreTextRowRefreshTests</c> drives the resolver
    /// to its floor and requires this to equal it.
    /// </summary>
    public const int SmallestFactRetentionDays = 1;

    /// <summary>
    /// The youngest age, in hours, at which a build that prunes on <c>query_store_text.last_seen</c> alone can retire a
    /// text: its cutoff at <see cref="SmallestFactRetentionDays"/>, which is that retention plus
    /// <see cref="QueryStoreTextStore.PruneMarginDays"/>. That is every build before the seen table, run against this
    /// store after a rollback.
    /// </summary>
    public const int OlderBuildCutoffHours = (SmallestFactRetentionDays + QueryStoreTextStore.PruneMarginDays) * 24;

    /// <summary>
    /// What share of <see cref="OlderBuildCutoffHours"/> the text row's own stamp may trail by, as a divisor. Chosen, not
    /// measured: half leaves the other half for the probe cache, which shows the touch a text at most once per
    /// <see cref="QueryStoreLivenessTouchGuard.GuardHours"/>, and for the time the service is stopped across a downgrade.
    /// </summary>
    public const int TextRowRefreshShareDivisor = 2;

    /// <summary>
    /// How stale <c>query_store_text.last_seen</c> may get before the touch refreshes it, in hours.
    ///
    /// <para><b>Why the text row is refreshed at all.</b> An older build prunes on the text row's stamp and runs a prune at
    /// service start, before its first touch. After a rollback it must find every live text inside its cutoff, or it
    /// retires text that facts still reference. So the refresh interval sits inside the older build's cutoff at the
    /// smallest retention the configuration allows, continuously, and the price is one text-row write per text per
    /// interval instead of one per <see cref="QueryStoreLivenessTouchGuard.GuardHours"/>.</para>
    /// </summary>
    public const int TextRowRefreshHours = OlderBuildCutoffHours / TextRowRefreshShareDivisor;

    /// <summary>The SQL fragment <see cref="QueryStoreTextStore.RefreshTextRowSql"/> interpolates.</summary>
    public static readonly string TextRowRefreshInterval =
        "interval '" + TextRowRefreshHours.ToString(CultureInfo.InvariantCulture) + " hours'";

    /// <summary>
    /// The invariant the derivation has to satisfy: the divisor divides the cutoff exactly, the refresh is no more frequent
    /// than the liveness guard, and a live text's stamp stays inside the older build's cutoff. The stamp of a text that keeps
    /// being referenced trails by at most the refresh interval plus one probe-cache lifetime
    /// (<see cref="QueryStoreLivenessTouchGuard.GuardHours"/>), and what is left of the cutoff after that must be at least
    /// another guard width.
    /// </summary>
    public static bool TextRowRefreshStaysInsideOlderBuildCutoff() =>
        TextRowRefreshHours * TextRowRefreshShareDivisor == OlderBuildCutoffHours
        && TextRowRefreshHours >= QueryStoreLivenessTouchGuard.GuardHours
        && TextRowRefreshHours + (2 * QueryStoreLivenessTouchGuard.GuardHours) <= OlderBuildCutoffHours;

    /// <summary>
    /// Retires text whose two stamps are both behind the cutoff (<c>$1</c>), at most <paramref name="cap"/> rows a statement.
    /// It reads the seen table with one sequential scan, probes the text row of each seen row behind the cutoff by primary
    /// key, and deletes the text row by <c>ctid</c>. The cascade removes the seen row. Measured on 3.45 million texts with
    /// one seen row in 16 behind the cutoff: 258,750 texts retired in 3.8 s, the cascade included, where a pass over the
    /// text heap to find them reads 765,000 buffers (6 GB) before it deletes anything.
    ///
    /// <para>The text row's own stamp is tested twice. Inside the capped select, so a seen row that is stale while its text
    /// row is fresh (a store back from an older build, which stamped the text row only) takes no place under the cap. And on
    /// the <c>DELETE</c> itself, which is what a concurrent touch is re-checked against: every text behind the cutoff is also
    /// behind <see cref="TextRowRefreshHours"/>, so a touch that references it rewrites the text row first, and the delete
    /// then skips the new row version. The new version also has another <c>ctid</c>, and a hash join on <c>ctid</c>
    /// compares it again on that re-check, as a Tid Scan does on PostgreSQL 18 (measured on 18.6 under both plans: the row
    /// is skipped without the stamp test). A Tid Scan before PostgreSQL 17.7, and before the minor of each older branch
    /// released with it, does not test its <c>ctid</c> condition again, and there the stamp test is the only thing that
    /// keeps the text.</para>
    ///
    /// <para>The <c>LATERAL ... OFFSET 0</c> keeps the text side a primary-key probe per stale seen row. Joined plainly, the
    /// planner is free to hash the whole text heap, which is the scan this table exists to avoid. No <c>ORDER BY</c>, for
    /// the reason <c>DarlingRetention.UnorderedRowCappedDeleteSql</c> gives: the cutoff is fixed, so the matching set only
    /// shrinks.</para>
    /// </summary>
    public static string PruneSql(int cap) => @"
DELETE FROM collect.query_store_text AS t
USING (
    SELECT x.text_ctid
    FROM collect.query_store_text_seen AS s
    CROSS JOIN LATERAL (
        SELECT k.ctid AS text_ctid
        FROM collect.query_store_text AS k
        WHERE k.server_id = s.server_id AND k.database_name = s.database_name AND k.query_id = s.query_id
        AND   k.last_seen < $1
        OFFSET 0
    ) AS x
    WHERE s.last_seen < $1
    LIMIT " + cap.ToString(CultureInfo.InvariantCulture) + @"
) AS due
WHERE t.ctid = due.text_ctid
AND   t.last_seen < $1";
}
