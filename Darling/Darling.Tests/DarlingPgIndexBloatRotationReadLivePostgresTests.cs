/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A measurement taken earlier in the window SURVIVES a later cycle that only labelled the same index
/// (#3153) — the read-side half of rotation, and the half that decides whether rotation is visible to an
/// operator at all.
///
/// <para><b>Why this needed its own test.</b> The collector now measures a rotating slice, so every index
/// gets a row on every cycle and most of those rows are labels. The read took
/// <c>DISTINCT ON (index identity) ... ORDER BY collection_time DESC</c> — the newest ROW — which is the
/// newest LABEL for any index the current cycle passed over. Measured on a two-cycle store: an index
/// measured at 72.5% density with 194 MB estimated reclaimable came back the next day with no density, no
/// estimate and a rotation-cursor reason, because the label was newer. Rotation would have accumulated
/// coverage into the store and the only surface that reports index bloat would have shown the last cycle's
/// measured set and nothing else — which is exactly what it showed BEFORE rotation, so the whole change
/// would have been invisible in the product while every collector-side pin stayed green.</para>
///
/// <para><b>The three cases, and why each is separately load-bearing.</b> An index measured in an earlier
/// cycle and labelled in a later one must come back MEASURED, with the measurement's own timestamp. An
/// index with TWO measurements and a label between them must come back with the NEWER one — the fix has to
/// prefer measurements without preferring stale ones. And an index the window never measured at all must
/// still come back carrying its reason, because dropping it would read as an index that does not exist.</para>
///
/// <para><b>The second case is here because the first version of this test could not see it.</b> With one
/// measurement per index, ranking measurements oldest-first and newest-first select the same row, so a
/// mutation that ordered them <c>ASC</c> — handing back a 30-hour-old density while a 6-hour-old one sat in
/// the same window — passed. Two measurements on one index is the smallest fixture that discriminates the
/// claim actually being made.</para>
///
/// <para>Rows are SEEDED rather than collected. The property under test belongs to the read, and seeding
/// is what lets two cycles a day apart exist inside one test — <c>PgIndexBloatBudgetLivePostgresTests</c>
/// is where the collector's own rotation is executed against a real catalog.</para>
///
/// <para><b>Since #3234 this suite exercises the HISTORICAL row shape, deliberately.</b> The collector no
/// longer calls <c>pgstatindex</c> on a schedule — it estimates from catalog statistics — but the store
/// holds 90 days of exact rows written before that change, and the seeding below names only the pre-V114
/// columns. So <c>est_tuple_bytes</c> is NULL on every row here, which is what makes
/// <c>measurement_kind</c> resolve to <c>measured</c> and <c>MeasuredAt</c> carry a time. That is the
/// property under test and it is not incidental: keying the estimate/measured discriminator on the
/// PRESENCE of a modelled tuple width rather than on a null density is what keeps these rows correctly
/// labelled, and an empty measured index — whose density is legitimately null — from being reported as an
/// estimate.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingPgIndexBloatRotationReadLivePostgresTests
{
    /// <summary>
    /// NEGATIVE, the convention every live class here follows: teardown deletes by server id, and a real
    /// store assigns ids from a sequence, so a positive sentinel is one collision away from removing an
    /// operator's own registry row.
    /// </summary>
    private const int ServerId = -993_153;

    private const string ServerName = "pg-index-bloat-rotation-probe";

    private const int ReadLimit = 25;

    [Fact]
    public async Task AnEarlierMeasurementSurvivesALaterLabel_AgainstLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live pg_index_bloat rotation read test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* THREE cycles inside the window read below. Truncated to seconds because the store column is
               `timestamp` and the assertions compare the values back out.

               Three rather than two, and that is not padding: with a single measurement per index, ranking
               measurements OLDEST-first and newest-first pick the same row, so a two-cycle fixture cannot
               tell "prefer a measurement" from "prefer the oldest measurement" — measured, and the second
               of those passed a two-cycle version of this test. measured_twice below is the case that
               separates them. */
            var older = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-30));
            var middle = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-18));
            var newer = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-6));

            /* rotated_past: MEASURED in the older cycle, then only labelled in the newer one. This is the
               row the read used to lose. */
            await SeedAsync(connection, ct, older, "rotated_past", "rotated_past_ix",
                1_000_000_000L, treeLevel: 3, density: 72.5, fragmentation: 30.0, skippedReason: null);
            await SeedAsync(connection, ct, newer, "rotated_past", "rotated_past_ix",
                1_000_000_000L, treeLevel: null, density: null, fragmentation: null,
                skippedReason: "above the rotation cursor: this pass has already advanced past this position");

            /* measured_twice: MEASURED in the oldest cycle, labelled in the middle one, MEASURED again in
               the newest. The fix must prefer a measurement over a label WITHOUT preferring an old
               measurement over a new one, and only a second measurement can tell those apart. The two
               densities are far apart so the assertion names which cycle the read chose. */
            await SeedAsync(connection, ct, older, "measured_twice", "measured_twice_ix",
                800_000_000L, treeLevel: 3, density: 40.0, fragmentation: 55.0, skippedReason: null);
            await SeedAsync(connection, ct, middle, "measured_twice", "measured_twice_ix",
                800_000_000L, treeLevel: null, density: null, fragmentation: null,
                skippedReason: "not measured this cycle (work budget): pgstatindex reads every page");
            await SeedAsync(connection, ct, newer, "measured_twice", "measured_twice_ix",
                800_000_000L, treeLevel: 3, density: 55.0, fragmentation: 40.0, skippedReason: null);

            /* never_measured: labelled in both. It must still come back, with its reason. */
            await SeedAsync(connection, ct, older, "never_measured", "never_measured_ix",
                90_000_000_000L, treeLevel: null, density: null, fragmentation: null,
                skippedReason: "larger than the measurement ceiling: recorded but NEVER measured");
            await SeedAsync(connection, ct, newer, "never_measured", "never_measured_ix",
                90_000_000_000L, treeLevel: null, density: null, fragmentation: null,
                skippedReason: "larger than the measurement ceiling: recorded but NEVER measured");

            await using var postgres = NpgsqlDataSource.Create(connectionString!);

            var rows = await DarlingPgIndexBloatReader.GetPgIndexBloatAsync(
                postgres, ServerId, DarlingMcpTestData.Naive(DateTime.UtcNow.AddHours(-48)),
                DarlingMcpTestData.Naive(DateTime.UtcNow.AddHours(1)), ReadLimit, ct);

            /* Accounting first: three indexes stored across three cycles, three rows read. Seven stored rows
               collapsing to anything but three would mean the distinct key stopped identifying an index. */
            Assert.Equal(
                new[] { "measured_twice", "never_measured", "rotated_past" },
                rows.Select(r => r.TableName).OrderBy(n => n, StringComparer.Ordinal).ToArray());

            // ── the row the read used to lose ────────────────────────────────────────────────────────
            var rotated = rows.Single(r => r.TableName == "rotated_past");

            Assert.Null(rotated.SkippedReason);
            Assert.Equal(72.5, rotated.AvgLeafDensity);
            Assert.NotNull(rotated.EstimatedReclaimableBytes);
            Assert.True(
                rotated.EstimatedReclaimableBytes > 0,
                "the surviving measurement has to carry its reclaimable estimate, which is the figure the "
                + "grid ranks by — a row that came back measured but with no estimate would sort as though "
                + "there were nothing to reclaim");

            /* And it reports the measurement's OWN time, which is the whole reason the timestamp is on the
               surfaces: this density is a day older than the last collection. */
            Assert.Equal(older, rotated.CaptureTime);
            Assert.Equal(older, rotated.MeasuredAt);

            // ── and not by preferring whatever measurement is oldest ─────────────────────────────────
            /* This index has TWO measurements and a label between them. Ranking measurements ahead of
               labels is necessary but not sufficient: ordering them oldest-first also satisfies every
               assertion above, and would hand back a density from 30 hours ago while a 6-hour-old one sat
               in the same window. 40.0 here means exactly that mistake. */
            var latest = rows.Single(r => r.TableName == "measured_twice");

            Assert.Null(latest.SkippedReason);
            Assert.Equal(55.0, latest.AvgLeafDensity);
            Assert.Equal(newer, latest.MeasuredAt);
            Assert.Equal(newer, latest.CaptureTime);

            // ── never measured in the window: still present, still reasoned, no invented age ─────────
            var never = rows.Single(r => r.TableName == "never_measured");

            Assert.NotNull(never.SkippedReason);
            Assert.Null(never.AvgLeafDensity);
            Assert.Null(never.EstimatedReclaimableBytes);

            /* MeasuredAt is NULL rather than the label's own timestamp. A "measured" column filled in from
               the label would read as the age of a measurement that does not exist, which is the same class
               of false claim #3153 was about. */
            Assert.Null(never.MeasuredAt);

            /* Unmeasured still sorts FIRST — an index with no measurement in the window cannot be ranked
               below measured ones by a reclaimable figure it does not have. */
            Assert.Equal("never_measured", rows[0].TableName);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static Task SeedAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc,
        string tableName, string indexName, long indexBytes, int? treeLevel,
        double? density, double? fragmentation, string? skippedReason) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_index_bloat
    (collection_id, collection_time, server_id, server_name, database_name, schema_name,
     table_name, index_name, index_bytes, tree_level, internal_pages, leaf_pages,
     empty_pages, deleted_pages, avg_leaf_density, leaf_fragmentation, skipped_reason)
VALUES ($1::bigint, $2::timestamp, $3::integer, $4::text, $5::text, $6::text,
        $7::text, $8::text, $9::bigint, $10::integer, $11::bigint, $12::bigint,
        $13::bigint, $14::bigint, $15::double precision, $16::double precision, $17::text)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), ServerId, ServerName,
            "appdb", "public", tableName, indexName, indexBytes, treeLevel, null, null, 0L, 0L,
            density, fragmentation, skippedReason);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(
            connection, ct, "DELETE FROM pg_index_bloat WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(
            connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
    }
}
