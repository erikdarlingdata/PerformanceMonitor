/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.IO;
using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The hash-keyed dimension tables that hold query_stats' / procedure_stats' large text payloads
/// ONCE instead of inline on every collected row (#1767).
///
/// <para>Measured on a production field instance: query_stats (176 GB) and procedure_stats (58 GB)
/// were 94% of a 250 GB store, and 99.9% of a COMPRESSED query_stats chunk was the text/plan TOAST —
/// columnar compression crushes the monitoring numerics (665x) but TOAST compression is byte-level
/// and has no cross-row awareness, so re-collecting the same cached plan every cycle stores it again
/// every cycle. A dedup PoC over a real 1-hour window (514,896 rows) measured 3,166 MB of inline
/// payload collapsing to ~23 MB — and the ratio IMPROVES as the window widens, because distinct
/// plans grow far slower than rows.</para>
///
/// <para><b>The key is a CONTENT digest, not one of SQL Server's hash columns.</b> That is a
/// deliberate departure from the issue's sketch, for three reasons the alternatives cannot satisfy:
/// <list type="number">
/// <item><c>query_plan_hash</c> is a plan-SHAPE hash. The same shape recompiled with different
/// sniffed parameter values carries the same hash and DIFFERENT XML, so a shape-keyed dim with
/// ON CONFLICT DO NOTHING would pin the first XML forever and serve stale
/// <c>ParameterCompiledValue</c>s — the single most diagnostic part of a plan for this product.</item>
/// <item><c>query_hash</c> is a query-SHAPE hash: literal variants collapse, pinning one arbitrary
/// literal set forever, where every reader today shows the LATEST text.</item>
/// <item><c>plan_handle</c> does not determine the XML at all. query_stats captures the STATEMENT
/// plan via <c>dm_exec_text_query_plan(plan_handle, statement_start_offset, statement_end_offset)</c>
/// and the offsets are not stored; this codebase already recorded that exact failure — "keying on
/// plan_handle alone cross-contaminated multi-statement plans (one plan_handle, many statements at
/// different offsets)" (QueryStatsCollector's delta key).</item>
/// </list>
/// A content digest is correct by construction and rests on no SQL Server cache-internals
/// assumption. It costs two <c>bytea</c> columns per query_stats row (~1% of the payload it
/// removes) and preserves the dedup ratio, because the repetition being collapsed comes from
/// re-collecting the SAME cached plan each cycle — byte-identical XML, hence one digest.</para>
///
/// <para><b>Migration is ZERO-REWRITE</b> (the ~234 GB payload rewrite rejected in #1759): the dims
/// and the new writer ship, the inline columns REMAIN, and new rows simply leave them NULL. Every
/// reader coalesces the inline column with the dim join for the transition, and raw retention ages
/// the inline copies out on its own. Dropping the columns is a later, separate decision.</para>
/// </summary>
public static class PayloadDimensions
{
    /// <summary>One diverted payload column: where it lives, and where its content goes instead.</summary>
    /// <param name="TargetTable">The fact table whose collector emits this payload column.</param>
    /// <param name="PayloadColumn">The inline column name in the collector definition (kept, written NULL on new rows).</param>
    /// <param name="DimTable">The dimension table that stores the content once, keyed by digest.</param>
    /// <param name="DigestColumn">The fact column carrying the digest that resolves back to <paramref name="DimTable"/>.</param>
    public sealed record PayloadDimension(
        string TargetTable,
        string PayloadColumn,
        string DimTable,
        string DigestColumn);

    /// <summary>The dimension table holding distinct query text, keyed by its content digest.</summary>
    public const string QueryTextDimTable = "query_text_dim";

    /// <summary>The dimension table holding distinct plan XML, keyed by its content digest.</summary>
    public const string QueryPlanDimTable = "query_plan_dim";

    /// <summary>The digest primary-key column both dimension tables share.</summary>
    public const string DigestColumn = "digest";

    /// <summary>
    /// How stale a dimension row's <see cref="LastSeenColumn"/> may be before a re-sighting refreshes it
    /// (both the <c>WHERE NOT EXISTS</c> pre-filter and the <c>ON CONFLICT ... WHERE</c> guard in
    /// <see cref="UpsertSql"/>, for every dimension table). Widened from 1 hour to 6 (#4503): measured on
    /// one production store, <c>query_plan_dim</c> took 894,656 non-HOT touches in 71 hours under the
    /// 1-hour guard — <c>last_seen</c> is indexed, so the update cannot go HOT — at ~42 KB of WAL per row
    /// (4.87 M full-page images against a large, scattered heap). The guard already caps a continuously-seen
    /// plan to 24 touches a day; widening it to 6 hours caps the same plan to 4. It must stay no wider than
    /// <see cref="DarlingRetention.ComputeDimensionCutoff"/>'s trailing margin, which reserves room for
    /// exactly this staleness — see that method's summary.
    /// </summary>
    public const int LastSeenRefreshGuardHours = 6;

    /// <summary>
    /// The GC watermark column both dimension tables share — the newest collection that referenced
    /// this content.
    ///
    /// <para>It exists because the obvious sweep (delete dim rows no live fact references) is an
    /// anti-join against two hypertables per dim row and is not affordable at this size. The write
    /// path stamps last_seen on every cycle that references a digest, so it is never older than the
    /// newest fact row pointing at it: once it falls past the fact tables' own retention horizon,
    /// every fact that could reference this content has already been purged. That turns the sweep
    /// into an indexed range scan on this column. DarlingRetention runs it, through the same
    /// time-sliced DELETE every other purge uses.</para>
    ///
    /// <para>Bounded rather than kept forever because the worst case compounds: ~23 MB/hour of
    /// distinct plans on the measured field instance is ~552 MB/day, ~16.6 GB/month, ~200 GB/year
    /// if nothing ever expires — a store that would eventually re-create the problem this change
    /// exists to solve.</para>
    /// </summary>
    public const string LastSeenColumn = "last_seen";

    /// <summary>
    /// Every diverted payload column, across both fact tables. <c>procedure_stats</c> deliberately
    /// has no text entry: it has no <c>query_text</c> column at all (verified against
    /// ProcedureStatsCollector.PayloadColumns) — its entire payload share is plan XML. Both fact
    /// tables share ONE plan dimension: identical plan XML collected from two different DMVs, or
    /// from two different servers running the same vendor application, is the same content and
    /// stores once fleet-wide.
    /// </summary>
    public static readonly IReadOnlyList<PayloadDimension> All = new[]
    {
        new PayloadDimension("query_stats", "query_text", QueryTextDimTable, "query_text_digest"),
        new PayloadDimension("query_stats", "query_plan_xml", QueryPlanDimTable, "query_plan_digest"),
        new PayloadDimension("procedure_stats", "query_plan_xml", QueryPlanDimTable, "query_plan_digest"),
    };

    /// <summary>The distinct dimension tables, in creation order.</summary>
    public static readonly IReadOnlyList<string> DimTables = new[] { QueryTextDimTable, QueryPlanDimTable };

    /// <summary>
    /// Per dim-feeding fact table, the SQL predicate "this row carries any payload digest" — the OR of
    /// that table's digest columns from <see cref="All"/>, in declaration order. The #1795 measured GC
    /// bound probes <c>min(collection_time)</c> under EXACTLY this predicate, and the V39 partial
    /// indexes are declared with EXACTLY this predicate, so the probe stays an index-edge read instead
    /// of an oldest-chunk walk. Derived from the map rather than restated so a new dimension column
    /// lands in the probe the day it lands in <see cref="All"/> — and a pin test holds the V39 DDL to
    /// these strings so the index predicate can never drift from the probe's.
    /// </summary>
    public static IReadOnlyDictionary<string, string> DigestPredicateByTable { get; } =
        All.GroupBy(d => d.TargetTable, StringComparer.Ordinal)
           .ToDictionary(
                g => g.Key,
                g => string.Join(" OR ", g.Select(d => d.DigestColumn + " IS NOT NULL").Distinct()),
                StringComparer.Ordinal);

    /// <summary>The diverted columns for one fact table (empty for every table that has none).</summary>
    public static IReadOnlyList<PayloadDimension> ForTable(string targetTable)
        => All.Where(d => string.Equals(d.TargetTable, targetTable, StringComparison.Ordinal)).ToArray();

    /// <summary>The payload column a dimension table stores (the content, not the key).</summary>
    public static string PayloadColumnOf(string dimTable)
        => dimTable switch
        {
            QueryTextDimTable => "query_text",
            QueryPlanDimTable => "query_plan_xml",
            _ => throw new ArgumentOutOfRangeException(nameof(dimTable), dimTable, "Not a payload dimension table"),
        };

    /// <summary>
    /// Maps a collector's PAYLOAD COLUMN INDEX to the dimension that column diverts into — the
    /// contract the binary-COPY write path runs on, where column ORDER is everything. Derived from
    /// the shared collector definition by NAME so a column reorder cannot silently misroute a
    /// payload, and it THROWS when a declared dimension's column is absent: renaming
    /// <c>query_text</c> in the shared definition must break the build loudly, because the silent
    /// failure mode is that diversion quietly stops and every row goes back to storing its payload
    /// inline — the exact 94%-of-the-store defect this file exists to remove, reintroduced with no
    /// error anywhere.
    /// </summary>
    public static IReadOnlyDictionary<int, PayloadDimension> DiversionPlanFor(ICollectorSchemaInfo schema)
    {
        if (schema is null)
        {
            throw new ArgumentNullException(nameof(schema));
        }

        var dimensions = ForTable(schema.TargetTable);
        if (dimensions.Count == 0)
        {
            return EmptyPlan;
        }

        var plan = new Dictionary<int, PayloadDimension>(dimensions.Count);
        foreach (var dimension in dimensions)
        {
            var index = -1;
            for (var i = 0; i < schema.PayloadColumns.Count; i++)
            {
                if (string.Equals(schema.PayloadColumns[i].Name, dimension.PayloadColumn, StringComparison.Ordinal))
                {
                    index = i;
                    break;
                }
            }

            if (index < 0)
            {
                throw new InvalidOperationException(
                    $"Payload dimension '{dimension.DimTable}' expects column '{dimension.PayloadColumn}' on " +
                    $"'{schema.TargetTable}', which the collector definition no longer declares. Update " +
                    $"{nameof(PayloadDimensions)}.{nameof(All)} — leaving it stale silently stops diverting and " +
                    "stores the full payload inline on every row again (#1767).");
            }

            plan[index] = dimension;
        }

        return plan;
    }

    private static readonly IReadOnlyDictionary<int, PayloadDimension> EmptyPlan =
        new Dictionary<int, PayloadDimension>();

    /// <summary>The byte length of a <see cref="Digest"/> (SHA-256).</summary>
    public const int DigestLengthBytes = 32;

    /// <summary>
    /// The content digest: SHA-256 over the UTF-8 bytes of the payload EXACTLY as it would have been
    /// stored inline — i.e. after <c>PgCollectorRowWriter.StripEmbeddedNuls</c>, so the digest keys
    /// the bytes the dim actually holds. Callers must strip before hashing; the write path funnels
    /// both through one place so they cannot drift.
    /// </summary>
    public static byte[] Digest(string payload)
    {
        if (payload is null)
        {
            throw new ArgumentNullException(nameof(payload));
        }

        return SHA256.HashData(Encoding.UTF8.GetBytes(payload));
    }

    /// <summary>
    /// The dimension table whose CONTENT is stored gzip-compressed (#2069): plan XML measured 14.0×
    /// under gzip against lz4-TOAST's 8.9× on live content — 101 GB of the production store was this
    /// one table. Query text stays uncompressed (21 MB total; 1.5× ratio; not worth reader churn).
    /// The digest is ALWAYS computed over the UNCOMPRESSED text (see <see cref="Digest"/>), so
    /// content identity is stable across the format change and a digest written by an old build
    /// matches the same plan written by a new one.
    /// </summary>
    public const string CompressedContentDimTable = QueryPlanDimTable;

    /// <summary>The compressed-content column added beside the legacy text column (#2069):
    /// new rows carry gzip bytes here and NULL text; pre-upgrade rows keep text; readers take
    /// gz-if-present-else-text and the GC ages the text era out within its window.</summary>
    public const string CompressedContentColumn = "query_plan_gz";

    /// <summary>Gzips dimension content for storage (#2069) — CompressionLevel.Optimal, measured
    /// 14.0× on live plan XML at ~10 MB/s single-threaded (write-path cost is minutes of one core
    /// per hour at peak intake).</summary>
    public static byte[] CompressContent(string payload)
    {
        if (payload is null)
        {
            throw new ArgumentNullException(nameof(payload));
        }

        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Optimal, leaveOpen: true))
        {
            var bytes = Encoding.UTF8.GetBytes(payload);
            gzip.Write(bytes, 0, bytes.Length);
        }

        return output.ToArray();
    }

    /// <summary>Inflates stored dimension content (#2069). Null in, null out — the reader's
    /// gz-else-text coalesce handles the pre-upgrade era.</summary>
    public static string? DecompressContent(byte[]? compressed)
    {
        if (compressed is null || compressed.Length == 0)
        {
            return null;
        }

        using var input = new MemoryStream(compressed);
        using var gzip = new GZipStream(input, CompressionMode.Decompress);
        using var reader = new StreamReader(gzip, Encoding.UTF8);
        return reader.ReadToEnd();
    }

    /// <summary>
    /// The one resolution rule every plan reader applies: inline/dimension TEXT when the row still
    /// carries it (pre-V54 content), else the gzip bytes decompressed (post-V54 content), else null.
    /// Text-first is deliberate — a row can never carry both (the writer stores exactly one form),
    /// so the order only decides which column a mixed RESULT prefers, and text is free while gzip
    /// costs a decompression.
    /// </summary>
    public static string? ResolveContent(string? inlineText, byte[]? compressed)
        => inlineText ?? DecompressContent(compressed);

    /// <summary>
    /// The CREATE TABLE for one dimension table. PLAIN PostgreSQL tables, deliberately NOT
    /// hypertables: they have no time axis to partition on, and a hypertable's unique constraint
    /// must include the partitioning column, which a content-keyed dim cannot satisfy. Being plain
    /// engine objects is also why they belong in a versioned migration, unlike the TimescaleDB
    /// objects that are runtime setup.
    /// </summary>
    public static string CreateDimTable(string dimTable)
    {
        var payloadColumn = PayloadColumnOf(dimTable);
        /* #2069: the compressed-content dim carries its bytea column from creation (fresh stores run
           V38's generated view, which references it) and its text column NULLABLE — new rows store
           gzip bytes only. Other dims keep the original NOT NULL text shape. */
        var contentColumns = string.Equals(dimTable, CompressedContentDimTable, StringComparison.Ordinal)
            ? $"    {payloadColumn} text NULL,\n" +
              $"    {CompressedContentColumn} bytea NULL,\n"
            : $"    {payloadColumn} text NOT NULL,\n";
        return
            $"CREATE TABLE IF NOT EXISTS {dimTable} (\n" +
            $"    {DigestColumn} bytea NOT NULL PRIMARY KEY,\n" +
            contentColumns +
            $"    {LastSeenColumn} timestamp NOT NULL\n" +
            ");\n" +
            $"CREATE INDEX IF NOT EXISTS idx_{dimTable}_last_seen ON {dimTable}({LastSeenColumn});";
    }

    /// <summary>
    /// The rendered SQL interval for <see cref="LastSeenRefreshGuardHours"/> — the ONE place
    /// <see cref="UpsertSql"/>'s four guard sites (pre-filter and conflict arm, times the compressed and
    /// text branches) get the width from, so the guard cannot drift between sites or between the two dim
    /// tables. Plural unconditionally — PostgreSQL accepts <c>interval '1 hours'</c>.
    /// </summary>
    internal static readonly string LastSeenRefreshGuardInterval =
        "INTERVAL '" + LastSeenRefreshGuardHours.ToString(CultureInfo.InvariantCulture) + " hours'";

    /// <summary>
    /// The batch insert of new content: ONE statement, two array parameters plus the collection timestamp, so it
    /// carries no multi-statement/positional-parameter hazard (an Npgsql batch mixing the two fails
    /// SILENTLY). <c>ON CONFLICT DO NOTHING</c> is what makes the write path idempotent and is the
    /// whole dedup mechanism: the second and every later sighting of the same content writes nothing here.
    ///
    /// <para><b>#9304: this statement never updates a dimension row.</b> The liveness stamp of a digest is on its narrow
    /// <see cref="PayloadDimensionSeen"/> row, and <see cref="TouchSql"/> refreshes it, so an unchanged text or plan is
    /// not rewritten (and a text's trigrams are not added to the GIN index again). The <c>WHERE NOT EXISTS</c> pre-filter
    /// reads the stamp table: a digest whose stamp is inside the <see cref="LastSeenRefreshGuardHours"/> window never
    /// reaches the insert, so the conflict arm runs at most once per guard window for a digest.</para>
    ///
    /// <para><b>The conflict arm LOCKS the conflicting row, on purpose.</b> It is <c>DO UPDATE ... WHERE</c>, whose
    /// <c>WHERE</c> is true only when the dimension row's own <c>last_seen</c> is more than
    /// <see cref="PayloadDimensionSeen.WideRowRefreshHours"/> behind, and every GC candidate is. Taking the row lock is what
    /// keeps a digest this batch is about to reference from being retired under it: a GC batch that reaches the row later
    /// waits for this transaction, re-reads the row's new <c>last_seen</c> and skips it. With <c>DO NOTHING</c>, which takes
    /// no lock, this order loses the content: the wide row is live (nothing happens), the GC locks it, <see cref="TouchSql"/>
    /// skips it (<c>SKIP LOCKED</c>) and stamps the stamp row, the writer commits its fact rows, and the GC's cascade
    /// deletes the stamp, leaving the fact rows with no content. The wide row is locked before the stamp row, the same order
    /// as the GC (wide row in the <c>DELETE</c>, stamp in the cascade), so this adds no deadlock. The #4249 cost of the old
    /// arm, a lock on every conflicting row, stays small: the pre-filter keeps a fresh digest from reaching the arm. A new
    /// dimension row's stamp row is written by the insert trigger (<see cref="PayloadDimensionSeen.TriggerSql"/>), at the
    /// same collection timestamp.</para>
    ///
    /// <para>Callers MUST pass digest-deduplicated arrays, and in digest ORDER: dedup makes a batch conflict-free with
    /// itself, the order makes it conflict-free with its siblings (two sessions that insert two shared digests in
    /// opposite order wait on each other, 40P01, #1801). <c>PayloadDimensionBatch.ToArrays</c> imposes both, and the
    /// <c>ORDER BY u.digest</c> keeps the order the filtered plan inserts in, whatever join the planner picks for the
    /// pre-filter (#4249).</para>
    ///
    /// <para>#2069: the compressed-content dim inserts gzip BYTES into <see cref="CompressedContentColumn"/> (text column
    /// left NULL on new rows); every other dim keeps the text shape. #2171: <paramref name="compressContent"/> false routes
    /// the plan dim through the TEXT branch instead (<c>plan_xml_compression = 'none'</c>).</para>
    /// </summary>
    public static string UpsertSql(string dimTable, bool compressContent = true)
    {
        var seen = PayloadDimensionSeen.SeenName(dimTable);
        var freshStamp =
            $"WHERE NOT EXISTS (\n" +
            $"    SELECT 1 FROM {seen} s\n" +
            $"    WHERE s.{DigestColumn} = u.digest\n" +
            $"    AND   s.{LastSeenColumn} >= $3 - {LastSeenRefreshGuardInterval})\n" +
            $"ORDER BY u.digest\n" +
            $"ON CONFLICT ({DigestColumn}) DO UPDATE SET {LastSeenColumn} = EXCLUDED.{LastSeenColumn}\n" +
            $"WHERE {dimTable}.{LastSeenColumn} < EXCLUDED.{LastSeenColumn} - {PayloadDimensionSeen.WideRowRefreshInterval}";

        if (compressContent && string.Equals(dimTable, CompressedContentDimTable, StringComparison.Ordinal))
        {
            return
                $"INSERT INTO {dimTable} ({DigestColumn}, {CompressedContentColumn}, {LastSeenColumn})\n" +
                $"SELECT u.digest, u.payload, $3\n" +
                $"FROM unnest($1::bytea[], $2::bytea[]) AS u(digest, payload)\n" +
                freshStamp;
        }

        var payloadColumn = PayloadColumnOf(dimTable);
        return
            $"INSERT INTO {dimTable} ({DigestColumn}, {payloadColumn}, {LastSeenColumn})\n" +
            $"SELECT u.digest, u.payload, $3\n" +
            $"FROM unnest($1::bytea[], $2::text[]) AS u(digest, payload)\n" +
            freshStamp;
    }

    /// <summary>
    /// The keep-alive for digests a batch referenced: ONE statement, one array of digests plus the collection timestamp. It
    /// refreshes the stamp row (<see cref="PayloadDimensionSeen"/>) of every digest whose stamp is more than
    /// <see cref="LastSeenRefreshGuardHours"/> stale, writes one for a dimension row that has none, and returns the digests
    /// of the array that have NO dimension row at all (the fact row written from them resolves to nothing, and the caller
    /// must stop presenting them). The writer runs it for the digests it inserted or found as well as the ones a batch
    /// referenced without content, so every digest a batch references is stamped.
    ///
    /// <para>The GC liveness invariant holds as it did on the dimension row: a digest a batch references is stamped, never
    /// left older than <see cref="LastSeenRefreshGuardHours"/> behind the newest fact row pointing at it, so the sweep (which
    /// trails by more than that guard) cannot delete content a live row still references. The same guard keeps the same
    /// cost cap: a continuously referenced digest takes at most <c>24 / LastSeenRefreshGuardHours</c> stamp updates a day.</para>
    ///
    /// <para><b>The dimension row's own <c>last_seen</c></b> is refreshed here too, when it is more than
    /// <see cref="PayloadDimensionSeen.WideRowRefreshHours"/> stale, for the sake of an older build that prunes on it (see
    /// <see cref="PayloadDimensionSeen"/>). No other path updates a dimension row.</para>
    ///
    /// <para>The rows to refresh are chosen and locked in a subquery (<c>FOR UPDATE SKIP LOCKED</c>), so the refresh never
    /// waits on a row lock, for the reason the old shape gave: there is no single lock order across this statement, the
    /// insert (<see cref="UpsertSql"/>), the Query Store plan map's <c>dim_touch</c> and the GC's cascade, and a waiting
    /// refresh would turn that into a deadlock that rolls back a whole batch. Skipping a locked row is safe because whoever
    /// holds a stale-but-live stamp is refreshing it, or a transaction that rolls back leaves it stale for the next cycle,
    /// far inside the GC's margin. The stamp insert takes a key share lock on the dimension row through the foreign key, so the
    /// adopt step tests the row with <c>FOR KEY SHARE SKIP LOCKED</c> and leaves one a GC batch holds, which would otherwise wait on
    /// it and deadlock (#9304); the GC adopts that row's stamp itself on its next pass. The stamp test sits behind <c>OFFSET 0</c>, so the planner cannot push the locking test below it and lock the wide row of every wanted digest: only a digest with no stamp row is locked (#9304). The GC cannot be the holder of a row a live digest owns: it only reaches stamps older
    /// than the dimension cutoff of <c>DarlingRetention.ComputeDimensionCutoff</c>, at least the guard plus one day behind. A skipped row
    /// correctly counts as present. A stamp row that does not exist yet is inserted with <c>DO NOTHING</c>, which takes no
    /// lock either.</para>
    ///
    /// <para>The absent-digest read is a plain read of the table as the statement began, and a digest another, uncommitted
    /// batch is still inserting is reported absent. That costs one extra render on the next cycle and nothing else.</para>
    /// </summary>
    public static string TouchSql(string dimTable)
    {
        PayloadColumnOf(dimTable);
        var seen = PayloadDimensionSeen.SeenName(dimTable);
        return
            $"WITH want AS (\n" +
            $"    SELECT d.{DigestColumn} FROM {dimTable} d\n" +
            $"    WHERE d.{DigestColumn} = ANY($1::bytea[])\n" +
            $"    AND   NOT EXISTS (\n" +
            $"        SELECT 1 FROM {seen} s\n" +
            $"        WHERE s.{DigestColumn} = d.{DigestColumn}\n" +
            $"        AND   s.{LastSeenColumn} >= $2 - {LastSeenRefreshGuardInterval})),\n" +
            $"stamped AS (\n" +
            $"    UPDATE {seen} SET {LastSeenColumn} = $2\n" +
            $"    WHERE {DigestColumn} IN (\n" +
            $"        SELECT s.{DigestColumn} FROM {seen} s\n" +
            $"        WHERE s.{DigestColumn} IN (SELECT {DigestColumn} FROM want)\n" +
            $"        AND   s.{LastSeenColumn} < $2 - {LastSeenRefreshGuardInterval}\n" +
            $"        ORDER BY s.{DigestColumn}\n" +
            $"        FOR UPDATE SKIP LOCKED)\n" +
            $"    RETURNING 1),\n" +
            $"adopted AS (\n" +
            $"    INSERT INTO {seen} ({DigestColumn}, {LastSeenColumn})\n" +
            $"    SELECT m.{DigestColumn}, $2\n" +
            $"    FROM (SELECT w.{DigestColumn} FROM want w\n" +
            $"          WHERE NOT EXISTS (SELECT 1 FROM {seen} s WHERE s.{DigestColumn} = w.{DigestColumn})\n" +
            $"          OFFSET 0) AS m\n" +
            $"    WHERE EXISTS (SELECT 1 FROM {dimTable} d WHERE d.{DigestColumn} = m.{DigestColumn} FOR KEY SHARE SKIP LOCKED)\n" +
            $"    ORDER BY m.{DigestColumn}\n" +
            $"    ON CONFLICT ({DigestColumn}) DO NOTHING\n" +
            $"    RETURNING 1),\n" +
            $"widened AS (\n" +
            $"    UPDATE {dimTable} SET {LastSeenColumn} = $2\n" +
            $"    WHERE {DigestColumn} IN (\n" +
            $"        SELECT d.{DigestColumn} FROM {dimTable} d\n" +
            $"        WHERE d.{DigestColumn} IN (SELECT {DigestColumn} FROM want)\n" +
            $"        AND   d.{LastSeenColumn} < $2 - {PayloadDimensionSeen.WideRowRefreshInterval}\n" +
            $"        ORDER BY d.{DigestColumn}\n" +
            $"        FOR UPDATE SKIP LOCKED)\n" +
            $"    RETURNING 1)\n" +
            $"SELECT w.digest FROM unnest($1::bytea[]) AS w(digest)\n" +
            $"WHERE NOT EXISTS (SELECT 1 FROM {dimTable} d WHERE d.{DigestColumn} = w.digest)\n" +
            $"ORDER BY w.digest";
    }

    /// <summary>
    /// The dimension GC's batch (#9304): deletes up to <paramref name="cap"/> dimension rows whose stamp
    /// (<see cref="PayloadDimensionSeen"/>) is behind the cutoff (<c>$1</c>), oldest first, and returns the stamp values of
    /// the rows it deleted, so the caller takes their maximum as the next batch's cursor (<paramref name="hasCursor"/> adds
    /// <c>AND last_seen &gt;= $2</c>, the keyset cursor of <c>DarlingRetention.CursoredRowCappedDeleteSql</c>). The stamp row
    /// goes with its dimension row through the foreign key's cascade, in the same statement, so a batch that times out
    /// rolls back both.
    ///
    /// <para>The order and the cap come from the stamp table's own index, so the batch reads the narrow table and probes the
    /// dimension row of each due stamp by primary key. A dimension row that is not there (never, while the foreign key
    /// holds) deletes nothing and returns nothing, so it takes no place under the cap.</para>
    ///
    /// <para>The dimension row's own <c>last_seen</c> is tested twice, as <c>QueryStoreTextSeen.PruneSql</c> tests the text
    /// row's. Inside the capped select, so a stamp that is stale while its dimension row is fresh (a store back from an older
    /// build, which stamped the dimension row only) takes no place under the cap. And on the <c>DELETE</c> itself, which is
    /// what a concurrent refresh is re-checked against: every stamp behind the cutoff is also behind
    /// <see cref="PayloadDimensionSeen.WideRowRefreshHours"/>, so a refresh that references the digest rewrites the dimension
    /// row first (<see cref="TouchSql"/>), and the delete, which waited for that row, reads the new version and skips it.
    /// Without the second test the re-check would only compare the digest and delete a row the refresh just kept alive.</para>
    ///
    /// <para>The <c>LATERAL ... OFFSET 0</c> keeps the dimension side a primary-key probe per stale stamp, in stamp order, so
    /// the <c>LIMIT</c> stops the stamp index scan after <paramref name="cap"/> due rows.</para>
    /// </summary>
    public static string CursoredDeleteSql(string dimTable, int cap, bool hasCursor)
    {
        var seen = PayloadDimensionSeen.TableName(dimTable);
        return
            $"WITH due AS (\n" +
            $"    SELECT s.{DigestColumn}, s.{LastSeenColumn} FROM {seen} s\n" +
            $"    CROSS JOIN LATERAL (\n" +
            $"        SELECT k.{DigestColumn} FROM collect.{dimTable} k\n" +
            $"        WHERE k.{DigestColumn} = s.{DigestColumn}\n" +
            $"        AND   k.{LastSeenColumn} < $1\n" +
            $"        OFFSET 0) AS x\n" +
            $"    WHERE s.{LastSeenColumn} < $1\n" +
            (hasCursor ? $"    AND   s.{LastSeenColumn} >= $2\n" : string.Empty) +
            $"    ORDER BY s.{LastSeenColumn} LIMIT {cap.ToString(CultureInfo.InvariantCulture)}),\n" +
            $"gone AS (\n" +
            $"    DELETE FROM collect.{dimTable} d USING due\n" +
            $"    WHERE d.{DigestColumn} = due.{DigestColumn}\n" +
            $"    AND   d.{LastSeenColumn} < $1\n" +
            $"    RETURNING due.{LastSeenColumn})\n" +
            $"SELECT {LastSeenColumn} FROM gone";
    }
}
