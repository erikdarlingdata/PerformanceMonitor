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
/// The liveness stamp of the payload dimension tables (<c>collect.query_text_dim</c>, <c>collect.query_plan_dim</c>), in
/// one narrow table each (#9304): <c>collect.{dim}_seen</c>, one row per digest holding only <c>last_seen</c>. It is
/// <see cref="QueryStoreTextSeen"/>'s design (V281, #9043) applied to the dimensions.
///
/// <para><b>Why the stamp is not on the dimension row.</b> <c>last_seen</c> of a dimension row was indexed (the dimension
/// GC orders by it), so a refresh changed an indexed column and could never be a heap-only update. Each refresh wrote a
/// new copy of the wide row, and for <c>query_text_dim</c> it also added every trigram of an unchanged text to the GIN
/// index's pending list. Here the refresh writes a row of about 70 bytes and the GC orders by this table's own index. The
/// dimension row is written once, when its content first arrives, and again only for the rollback refresh below.</para>
///
/// <para><b>Who writes it.</b> A statement trigger on each dimension table (<see cref="TriggerSql"/>) inserts the stamp
/// row of every dimension row an <c>INSERT</c> lands, whatever build runs the insert: this build's
/// <see cref="PayloadDimensions.UpsertSql"/>, an older build run against the same store after a rollback, or an older
/// instance that shares the store. The refresh (<see cref="PayloadDimensions.TouchSql"/>) updates the stamp, and inserts
/// one for a referenced digest that has none. The foreign key cascades, so every delete of a dimension row removes its
/// stamp row. The dimension GC (<see cref="PayloadDimensions.CursoredDeleteSql"/>) first gives every dimension row with no
/// stamp one (<see cref="AdoptSlice"/>).</para>
///
/// <para><b>What an older build sees.</b> It keeps stamping and pruning on the dimension row's own <c>last_seen</c>, and
/// never reads this table. Its prune has no index to order by after the rung, so it scans the dimension. To keep that
/// prune from retiring live content, the dimension row's own <c>last_seen</c> is still refreshed, every
/// <see cref="WideRowRefreshHours"/> instead of every <see cref="PayloadDimensions.LastSeenRefreshGuardHours"/>, in the
/// same statement and under the same lock rule. That refresh can be heap-only now that the index is gone.</para>
///
/// <para>A plain table with one primary key and one index on <c>last_seen</c>. The index is what the GC orders by, so the
/// stamp update is not heap-only, but the row is narrow: the index entry and the heap row together are a few percent of a
/// dimension row and its trigrams.</para>
/// </summary>
public static class PayloadDimensionSeen
{
    /// <summary>The stamp table of a dimension table, schema-qualified (<c>collect.query_text_dim_seen</c>).</summary>
    public static string TableName(string dimTable) => "collect." + SeenName(dimTable);

    /// <summary>The unqualified stamp table name of a dimension table.</summary>
    public static string SeenName(string dimTable)
    {
        PayloadDimensions.PayloadColumnOf(dimTable);
        return dimTable + "_seen";
    }

    /// <summary>The primary key's name, the one PostgreSQL gives an inline key.</summary>
    public static string PrimaryKeyName(string dimTable) => SeenName(dimTable) + "_pkey";

    /// <summary>The foreign key's name.</summary>
    public static string ForeignKeyName(string dimTable) => SeenName(dimTable) + "_dim_fkey";

    /// <summary>The index the dimension GC orders by.</summary>
    public static string LastSeenIndexName(string dimTable) => "idx_" + SeenName(dimTable) + "_last_seen";

    /// <summary>The trigger function's name, schema-qualified.</summary>
    public static string TrackFunctionName(string dimTable) => "collect." + SeenName(dimTable) + "_track";

    /// <summary>The insert trigger's name on the dimension table.</summary>
    public static string InsertTriggerName(string dimTable) => "trg_" + SeenName(dimTable) + "_ins";

    /// <summary>The dimension table's own <c>last_seen</c> index before the rung, dropped by <see cref="DropWideIndexSql"/>.</summary>
    public static string WideIndexName(string dimTable) => "idx_" + dimTable + "_last_seen";

    /// <summary>
    /// The smallest horizon an older build's dimension prune can use, in hours: its cutoff is the retention (at least one day,
    /// the resolver's clamp) plus one day of margin (<c>DarlingRetention.ComputeDimensionCutoff</c> and the plan dimension's
    /// dedicated horizon both subtract at least that).
    /// </summary>
    public const int OlderBuildCutoffHours = 48;

    /// <summary>
    /// How stale the dimension row's own <c>last_seen</c> may get before the refresh rewrites it, in hours. Half of
    /// <see cref="OlderBuildCutoffHours"/>: a live row's own stamp then trails by at most this plus one
    /// <see cref="PayloadDimensions.LastSeenRefreshGuardHours"/> (the refresh only looks at a row when its stamp row is stale),
    /// which stays well inside the older build's cutoff.
    /// </summary>
    public const int WideRowRefreshHours = OlderBuildCutoffHours / 2;

    /// <summary>The SQL fragment the refresh interpolates.</summary>
    public static readonly string WideRowRefreshInterval =
        "interval '" + WideRowRefreshHours.ToString(CultureInfo.InvariantCulture) + " hours'";

    /// <summary>
    /// The invariant the derivation has to satisfy: the refresh is no more frequent than the stamp's own guard, and a live
    /// digest's dimension row stays inside the dimension GC's smallest cutoff (<see cref="OlderBuildCutoffHours"/>). The
    /// dimension row of a digest that keeps being referenced trails by at most the refresh interval plus one stamp guard
    /// (the refresh only looks at a digest when its stamp is stale), and what is left of the cutoff after that must be at
    /// least another guard width. That is also why every stamp the GC retires is behind <see cref="WideRowRefreshHours"/>, so
    /// a refresh that races the GC always rewrites the dimension row the GC then re-checks.
    /// </summary>
    public static bool WideRowRefreshStaysInsideDimensionCutoff() =>
        WideRowRefreshHours >= PayloadDimensions.LastSeenRefreshGuardHours
        && WideRowRefreshHours + (2 * PayloadDimensions.LastSeenRefreshGuardHours) <= OlderBuildCutoffHours;

    /// <summary>
    /// The table of the rung (V298), created with no key. <see cref="PrimaryKeySql"/>, <see cref="ForeignKeySql"/> and
    /// <see cref="LastSeenIndexSql"/> are added after <see cref="BackfillSql"/>, because loading into a bare table is several
    /// times cheaper than into one with its key and index in place (V281's measurement). Fillfactor 90 keeps room for the
    /// rows the refresh rewrites.
    /// </summary>
    public static string CreateTableSql(string dimTable) => $@"
CREATE TABLE IF NOT EXISTS {TableName(dimTable)} (
    digest bytea NOT NULL,
    last_seen timestamp NOT NULL
) WITH (fillfactor = 90);";

    /// <summary>The primary key, added after <see cref="BackfillSql"/>. Idempotent.</summary>
    public static string PrimaryKeySql(string dimTable) => $@"
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = '{TableName(dimTable)}'::regclass AND contype = 'p') THEN
        ALTER TABLE {TableName(dimTable)} ADD CONSTRAINT {PrimaryKeyName(dimTable)} PRIMARY KEY (digest);
    END IF;
END
$$;";

    /// <summary>
    /// The foreign key to the dimension row, <c>ON DELETE CASCADE</c>, so the GC, a server's removal and any other delete of a
    /// dimension row remove its stamp row in the same statement. Idempotent.
    /// </summary>
    public static string ForeignKeySql(string dimTable) => $@"
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = '{TableName(dimTable)}'::regclass AND conname = '{ForeignKeyName(dimTable)}') THEN
        ALTER TABLE {TableName(dimTable)}
            ADD CONSTRAINT {ForeignKeyName(dimTable)} FOREIGN KEY (digest)
            REFERENCES collect.{dimTable} (digest) ON DELETE CASCADE;
    END IF;
END
$$;";

    /// <summary>The index the GC orders by, built after the load. Idempotent.</summary>
    public static string LastSeenIndexSql(string dimTable) =>
        $"CREATE INDEX IF NOT EXISTS {LastSeenIndexName(dimTable)} ON {TableName(dimTable)} (last_seen);";

    /// <summary>
    /// The trigger of the rung: after each <c>INSERT</c> statement on the dimension table, one <c>INSERT ... SELECT</c> writes
    /// the stamp rows of the dimension rows that statement inserted, in key order. A statement trigger with a transition
    /// table, so a batch costs one insert, and an <c>ON CONFLICT DO UPDATE</c> or <c>DO NOTHING</c> puts only the rows it
    /// inserted there. <c>CREATE TRIGGER</c> takes a SHARE ROW EXCLUSIVE lock on the dimension table, held to the rung's
    /// commit, so no insert falls between the trigger and the backfill. Idempotent.
    /// </summary>
    public static string TriggerSql(string dimTable) => $@"
CREATE OR REPLACE FUNCTION {TrackFunctionName(dimTable)}() RETURNS trigger
LANGUAGE plpgsql AS $f$
BEGIN
    INSERT INTO {TableName(dimTable)} (digest, last_seen)
    SELECT n.digest, n.last_seen
    FROM landed AS n
    ORDER BY n.digest
    ON CONFLICT (digest) DO NOTHING;
    RETURN NULL;
END
$f$;

DROP TRIGGER IF EXISTS {InsertTriggerName(dimTable)} ON collect.{dimTable};
CREATE TRIGGER {InsertTriggerName(dimTable)}
    AFTER INSERT ON collect.{dimTable}
    REFERENCING NEW TABLE AS landed
    FOR EACH STATEMENT
    EXECUTE FUNCTION {TrackFunctionName(dimTable)}();";

    /// <summary>
    /// Writes the stamp row of every dimension row that has none, stamped with the dimension row's own <c>last_seen</c>. The
    /// rung runs it once after <see cref="TriggerSql"/>, when the stamp table is empty, as one sequential pass over the
    /// dimension's main heap (it reads no TOAST). The anti-join makes a second run insert nothing.
    /// </summary>
    public static string BackfillSql(string dimTable) => $@"
INSERT INTO {TableName(dimTable)} (digest, last_seen)
SELECT t.digest, t.last_seen
FROM collect.{dimTable} AS t
WHERE NOT EXISTS (SELECT 1 FROM {TableName(dimTable)} AS s WHERE s.digest = t.digest);";

    /// <summary>
    /// Drops the dimension row's own <c>last_seen</c> index. The rung runs it last: from the rung's commit the GC reads this
    /// table's index, so nothing orders by the dimension's. An older build that prunes on it scans the dimension instead.
    /// </summary>
    public static string DropWideIndexSql(string dimTable) => $"DROP INDEX IF EXISTS collect.{WideIndexName(dimTable)};";

    /// <summary>
    /// The GC's first step: gives every dimension row with no stamp row one, stamped with its own <c>last_seen</c>, so the GC
    /// that follows judges it by that stamp. A dimension row is without one only when it was written with the trigger off (a
    /// restore that disables triggers, a session in replica mode), and a GC that reads the stamp table alone would keep such a
    /// row for good. It runs in keyset slices of the digest (<see cref="LivenessAdoptSlice"/>), so no statement reads either
    /// primary key from end to end.
    /// </summary>
    public static LivenessAdoptSlice AdoptSlice(string dimTable) =>
        new("collect." + dimTable, TableName(dimTable), new[] { PayloadDimensions.DigestColumn }, PayloadDimensions.LastSeenColumn);

    /// <summary>The whole rung (V298), in its one transaction, for every dimension table. Order: the lock timeout, then per
    /// table the empty stamp table, the trigger, the backfill, the key, the foreign key and the index, and last the drop of
    /// the dimension's own <c>last_seen</c> index.</summary>
    public static string RungSql(int lockTimeoutSeconds)
    {
        var sb = new System.Text.StringBuilder();
        sb.Append("SET LOCAL lock_timeout = '").Append(lockTimeoutSeconds.ToString(CultureInfo.InvariantCulture)).Append("s';\n");
        foreach (var dimTable in PayloadDimensions.DimTables)
        {
            sb.Append(CreateTableSql(dimTable)).Append('\n')
              .Append(TriggerSql(dimTable)).Append('\n')
              .Append(BackfillSql(dimTable)).Append('\n')
              .Append(PrimaryKeySql(dimTable)).Append('\n')
              .Append(ForeignKeySql(dimTable)).Append('\n')
              .Append(LastSeenIndexSql(dimTable)).Append('\n');
        }

        foreach (var dimTable in PayloadDimensions.DimTables)
        {
            sb.Append(DropWideIndexSql(dimTable)).Append('\n');
        }

        return sb.ToString();
    }
}
