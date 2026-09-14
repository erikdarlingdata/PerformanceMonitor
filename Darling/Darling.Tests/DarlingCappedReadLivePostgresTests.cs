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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The two SQL changes #3424 and #3425 rest on, EXECUTED against a real store, over fixtures LARGER than
/// the limit they are read at.
///
/// <para><b>Why a live store and not a source scan.</b> Everything about these fixes that can be wrong
/// silently is in the SQL: whether the answered-only gate really drops the answerless rows without
/// disturbing which row represents each index, whether the ranking behind that gate really is reclaimable
/// bytes descending, whether the database filter really narrows the population, and whether the install
/// census really counts one row per extension instead of per (database, extension). A text assertion sees
/// none of it, and <c>PgCappedReadSurfaceTests</c>' positional pins can only say the clauses are in the
/// right places.</para>
///
/// <para><b>Every fixture exceeds its read limit, deliberately.</b> A truncation test whose fixture fits
/// inside the cap asserts nothing about truncation: every arm agrees there, so the test passes whichever
/// code shipped. The index fixture seeds 12 answerless rows against a read limit of 10, which is the
/// #3424 shape in miniature — at that limit the unfiltered read returns ten answerless rows and zero
/// answers, exactly as the fleet's largest census does at 1,000.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingCappedReadLivePostgresTests
{
    /// <summary>
    /// NEGATIVE, the convention these live classes follow: teardown deletes from <c>servers</c> by id and a
    /// real store assigns ids from a sequence, so a positive sentinel is one collision away from removing an
    /// operator's own registry row.
    /// </summary>
    private const int ServerId = -993_424;

    private const string ServerName = "pg-capped-read-probe";

    /// <summary>
    /// The read limit every arm below uses, and it is SMALLER than either fixture on purpose. Ten against
    /// twelve answerless index rows and against eighteen extension rows, so both reads truncate and the
    /// assertions are about what a truncation does rather than about what a complete read does.
    /// </summary>
    private const int ReadLimit = 10;

    private const long Mib = 1024L * 1024L;

    /// <summary>
    /// The collector's own prose, prefixed with a marker <c>PgIndexBloatCoverage.Markers</c> keys on —
    /// quoted rather than generated so this seeds what a real collector writes.
    /// </summary>
    private const string WidthsReason =
        "column widths are not visible for every key: pg_stats filters on has_column_privilege, so a "
        + "monitoring role without SELECT sees nothing, and a never-analyzed parent has no rows either. "
        + "Grant pg_read_all_data, or ANALYZE the parent.";

    /// <summary>
    /// #3424: at a limit smaller than the answerless population the unfiltered read returns NO answers, and
    /// <c>answered_only</c> returns them ranked by reclaimable bytes descending.
    ///
    /// <para><b>The unfiltered arm is the control and it has to stay red-worthy.</b> It asserts the defect
    /// still exists — zero answers in ten rows — because that is what makes the filtered arm's success
    /// meaningful. If a later change made the unfiltered read return answers, this test should be re-read
    /// rather than deleted: #3278's answerless-first order would have been quietly abandoned.</para>
    /// </summary>
    [Fact]
    public async Task AnsweredOnlyReachesTheAnswersThatNoLimitCouldPagePast()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #3424 answered_only test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var newest = now.AddHours(-2);
            var older = now.AddHours(-26);

            /* TWELVE answerless indexes against a read limit of TEN. The whole point: the leading block
               alone exceeds the page, so the unfiltered read cannot show one answer however the rest of the
               fixture is arranged. */
            for (var i = 0; i < 12; i++)
            {
                await SeedIndexAsync(
                    connection, ct, newest, "wide", "wide_ix_" + i.ToString("00", CultureInfo.InvariantCulture),
                    (100 + i) * Mib, estTupleBytes: 48L, reclaimable: null, skippedReason: WidthsReason);
            }

            /* FOUR answers, seeded in an order that is NOT their ranking, so the ORDER BY has to do the work
               rather than the insert sequence. The largest reclaimable figure belongs to the SMALLEST index
               here, which is also why the ranking is on bytes reclaimable and never on size. */
            await SeedIndexAsync(connection, ct, newest, "orders", "orders_b_ix", 900 * Mib,
                estTupleBytes: 48L, reclaimable: 20 * Mib, skippedReason: null);
            await SeedIndexAsync(connection, ct, newest, "orders", "orders_a_ix", 100 * Mib,
                estTupleBytes: 48L, reclaimable: 90 * Mib, skippedReason: null);
            await SeedIndexAsync(connection, ct, newest, "orders", "orders_c_ix", 500 * Mib,
                estTupleBytes: 48L, reclaimable: 50 * Mib, skippedReason: null);

            /* THE TIE-BREAK'S OWN CASE, and the reason the gate is outside the DISTINCT ON. This index was
               ANSWERED on the older cycle and LABELLED on the newer one. #3278's inner ordering keeps the
               measurement, so answered_only must keep this index — a gate applied inside that scope would
               filter the rows before the tie-break chose, and this index would vanish from a filter whose
               stated meaning is "the indexes that have an answer". */
            await SeedIndexAsync(connection, ct, older, "shipments", "shipments_pkey", 700 * Mib,
                estTupleBytes: 48L, reclaimable: 70 * Mib, skippedReason: null);
            await SeedIndexAsync(connection, ct, newest, "shipments", "shipments_pkey", 700 * Mib,
                estTupleBytes: 48L, reclaimable: null, skippedReason: WidthsReason);

            await LogRunAsync(connection, ct, newest, "SUCCESS");

            await using var postgres = NpgsqlDataSource.Create(connectionString!);

            var start = DarlingMcpTestData.Naive(now.AddHours(-48));
            var end = DarlingMcpTestData.Naive(now.AddHours(1));

            // ── THE CONTROL: the defect, still present, at this limit ─────────────────────────────────
            var unfiltered = await DarlingPgIndexBloatReader.GetPgIndexBloatAsync(
                postgres, ServerId, start, end, ReadLimit, cancellationToken: ct);

            Assert.Equal(ReadLimit, unfiltered.Count);
            Assert.DoesNotContain(unfiltered, r => r.SkippedReason is null);

            // ── THE FIX: the same limit, and every answer is present ──────────────────────────────────
            var answered = await DarlingPgIndexBloatReader.GetPgIndexBloatAsync(
                postgres, ServerId, start, end, ReadLimit, answeredOnly: true, cancellationToken: ct);

            Assert.Equal(4, answered.Count);
            Assert.DoesNotContain(answered, r => r.SkippedReason is not null);

            /* THE INDEX WHOSE NEWEST ROW IS A LABEL is among them, which is the gate's placement being
               correct rather than merely present. */
            Assert.Contains("shipments_pkey", answered.Select(r => r.IndexName));

            /* RANKED BY RECLAIMABLE BYTES DESCENDING, asserted as the whole sequence and not as a first
               element: a first-element check passes on a fixture whose largest happens to be inserted
               first, and this one deliberately is not. */
            Assert.Equal(
                new[] { "orders_a_ix", "shipments_pkey", "orders_c_ix", "orders_b_ix" },
                answered.Select(r => r.IndexName).ToArray());

            /* ACCOUNTING over the whole seeded set, not a lookup of the interesting row: 16 distinct
               indexes stored, 12 with no answer and 4 with one, and the two reads together have to add up
               to the population the census reports. A filter written to make the arm above pass has to
               survive this. */
            var everything = await DarlingPgIndexBloatReader.GetPgIndexBloatAsync(
                postgres, ServerId, start, end, 500, cancellationToken: ct);

            Assert.Equal(16, everything.Count);
            Assert.Equal(4, everything.Count(r => r.SkippedReason is null));
            Assert.Equal(
                answered.Select(r => r.IndexName).OrderBy(n => n, StringComparer.Ordinal).ToArray(),
                everything.Where(r => r.SkippedReason is null)
                    .Select(r => r.IndexName).OrderBy(n => n, StringComparer.Ordinal).ToArray());

            /* THE DEFAULT IS UNCHANGED, which is what #3278 is owed: the same unfiltered read at a limit
               wide enough to hold everything still puts every answerless row FIRST. */
            Assert.Equal(
                Enumerable.Repeat(true, 12).Concat(Enumerable.Repeat(false, 4)).ToArray(),
                everything.Select(r => r.SkippedReason is not null).ToArray());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteRowsAsync);
        }
    }

    /// <summary>
    /// #3425: a truncated extension read drops the <c>installed</c> rows specifically, the
    /// <c>database_name</c> filter completes one database, and the install census counts installations
    /// whatever the limit did.
    ///
    /// <para><b>The fixture reproduces the fleet's own ordering hazard in miniature.</b> Three databases x
    /// six extension names is eighteen rows against a read limit of ten, and in each database exactly one
    /// non-relevant extension is <c>installed</c> while the rest are <c>available</c> — the measured shape,
    /// where <c>plpgsql</c> is the only non-relevant installed extension among 102. So the truncated read
    /// holds no row for it at all, and an install census taken from those rows reports zero.</para>
    /// </summary>
    [Fact]
    public async Task TheInstallCensusSurvivesATruncationThatDropsEveryInstalledRow()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #3425 install census test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var newest = now.AddHours(-2);
            var older = now.AddHours(-26);
            var databases = new[] { "appdb", "hangfire", "postgres" };

            /* TWO CYCLES, both in the window, both writing every pair — which is what this collector does.
               A census without DISTINCT ON would report every installation count DOUBLED. */
            foreach (var cycle in new[] { older, newest })
            {
                foreach (var database in databases)
                {
                    /* The two monitoring-relevant rows, which sort FIRST and are therefore the rows a small
                       limit returns — the "sampling this looks like a complete per-database answer" trap. */
                    await SeedExtensionAsync(
                        connection, ct, cycle, database, "pg_stat_statements", "installed", relevant: true);
                    await SeedExtensionAsync(
                        connection, ct, cycle, database, "pgstattuple", "available", relevant: true);

                    /* Three non-relevant AVAILABLE rows per database, which is the block that displaces
                       everything behind it. */
                    foreach (var name in new[] { "amcheck", "bloom", "citext" })
                    {
                        await SeedExtensionAsync(
                            connection, ct, cycle, database, name, "available", relevant: false);
                    }

                    /* And the one non-relevant INSTALLED row, which the state ordering puts behind every
                       available row of every database. plpgsql's own position, in miniature. */
                    await SeedExtensionAsync(
                        connection, ct, cycle, database, "plpgsql", "installed", relevant: false);
                }
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);

            var start = DarlingMcpTestData.Naive(now.AddHours(-48));
            var end = DarlingMcpTestData.Naive(now.AddHours(1));

            // ── THE CONTROL: the defect, at this limit ────────────────────────────────────────────────
            var truncated = await DarlingPgExtensionAvailabilityReader.GetPgExtensionAvailabilityAsync(
                postgres, ServerId, start, end, ReadLimit, cancellationToken: ct);

            Assert.Equal(ReadLimit, truncated.Count);
            Assert.DoesNotContain("plpgsql", truncated.Select(r => r.ExtensionName));

            /* NOT a random slice: what survived is every relevant row plus the head of the available
               block, and the only `installed` rows in it are the relevant ones. An install census built
               from this page reports plpgsql in zero databases. */
            Assert.Equal(
                3,
                truncated.Count(r => string.Equals(r.State, "installed", StringComparison.Ordinal)));

            // ── THE FILTER: one database, complete ────────────────────────────────────────────────────
            var oneDatabase = await DarlingPgExtensionAvailabilityReader.GetPgExtensionAvailabilityAsync(
                postgres, ServerId, start, end, ReadLimit, "hangfire", ct);

            Assert.Equal(6, oneDatabase.Count);
            Assert.All(oneDatabase, row => Assert.Equal("hangfire", row.DatabaseName));
            Assert.Contains("plpgsql", oneDatabase.Select(r => r.ExtensionName));

            /* BLANK IS NOT A DATABASE. The web surface hands over a query-string value, so an empty one has
               to mean "every database" rather than selecting the databases named with the empty string. */
            var blankFilter = await DarlingPgExtensionAvailabilityReader.GetPgExtensionAvailabilityAsync(
                postgres, ServerId, start, end, 500, "   ", ct);

            Assert.Equal(18, blankFilter.Count);

            /* A NAME THAT MATCHES NOTHING returns nothing, rather than silently widening to everything —
               the other direction of the same mistake. */
            var missingFilter = await DarlingPgExtensionAvailabilityReader.GetPgExtensionAvailabilityAsync(
                postgres, ServerId, start, end, 500, "no-such-database", ct);

            Assert.Empty(missingFilter);

            // ── THE CENSUS: aggregated, and unaffected by any of that ─────────────────────────────────
            var census = await DarlingPgExtensionAvailabilityReader.GetInstallCensusAsync(
                postgres, ServerId, start, end, cancellationToken: ct);

            /* ONE ROW PER EXTENSION CREATED ANYWHERE, so two rows out of six extension names — the HAVING
               doing its job. Eighteen (database, extension) pairs across 36 stored rows, so DISTINCT ON is
               doing its job too. */
            Assert.Equal(2, census.Count);
            Assert.Equal(3, census[0].DatabasesTotal);
            Assert.Equal(6, census[0].ExtensionNameCount);
            Assert.Equal(18, census[0].RowsAvailable);

            var byName = census.ToDictionary(row => row.ExtensionName!, StringComparer.Ordinal);

            Assert.Equal(
                new[] { "pg_stat_statements", "plpgsql" },
                byName.Keys.OrderBy(n => n, StringComparer.Ordinal).ToArray());

            foreach (var name in byName.Keys)
            {
                Assert.Equal(3, byName[name].DatabasesInstalled);
                Assert.Equal(0, byName[name].DatabasesOutdated);
                Assert.Equal(3, byName[name].DatabasesReporting);
                Assert.Equal(3, byName[name].DatabasesTotal);
            }

            /* THE WHOLE POINT, stated as the comparison: plpgsql is invisible in the page and complete in
               the census, from the same call's data. */
            Assert.Equal(0, truncated.Count(r => string.Equals(r.ExtensionName, "plpgsql", StringComparison.Ordinal)));
            Assert.Equal(3, byName["plpgsql"].DatabasesInstalled);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteRowsAsync);
        }
    }

    /// <summary>
    /// An <c>outdated</c> extension is CREATED and must not be counted as up to date, nor dropped from the
    /// census as though it were absent.
    ///
    /// <para>Its own arm because it is the one state that means both things at once, and because a census
    /// built by counting <c>state = 'installed'</c> alone would report it as installed nowhere — which reads
    /// as "not installed" on the one state that says "installed, and behind".</para>
    /// </summary>
    [Fact]
    public async Task OutdatedCountsAsCreatedAndIsReportedApart()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #3425 outdated-state test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var newest = now.AddHours(-2);

            /* MIXED: outdated in one database, installed in the other. */
            await SeedExtensionAsync(connection, ct, newest, "appdb", "hypopg", "outdated", relevant: true);
            await SeedExtensionAsync(connection, ct, newest, "other", "hypopg", "installed", relevant: true);

            /* OUTDATED EVERYWHERE AND INSTALLED NOWHERE, which is the case that discriminates. A census
               whose created test read `state = 'installed'` alone keeps `hypopg` on the strength of its
               other database and drops THIS extension entirely - so without this pair the claim "outdated
               counts as created" is asserted by a fixture that does not depend on it. Measured: dropping
               'outdated' from the census HAVING left every assertion in this class passing until this row
               existed. */
            await SeedExtensionAsync(connection, ct, newest, "appdb", "pg_cron", "outdated", relevant: true);
            await SeedExtensionAsync(connection, ct, newest, "other", "pg_cron", "outdated", relevant: true);

            /* CREATED NOWHERE: absent in one database, available in the other. */
            await SeedExtensionAsync(connection, ct, newest, "appdb", "bloom", "absent", relevant: false);
            await SeedExtensionAsync(connection, ct, newest, "other", "bloom", "available", relevant: false);

            await using var postgres = NpgsqlDataSource.Create(connectionString!);

            var census = await DarlingPgExtensionAvailabilityReader.GetInstallCensusAsync(
                postgres, ServerId,
                DarlingMcpTestData.Naive(now.AddHours(-48)), DarlingMcpTestData.Naive(now.AddHours(1)),
                cancellationToken: ct);

            var byName = census.ToDictionary(row => row.ExtensionName!, StringComparer.Ordinal);

            Assert.Equal(
                new[] { "hypopg", "pg_cron" },
                byName.Keys.OrderBy(name => name, StringComparer.Ordinal).ToArray());

            Assert.Equal(1, byName["hypopg"].DatabasesInstalled);
            Assert.Equal(1, byName["hypopg"].DatabasesOutdated);
            Assert.Equal(2, byName["hypopg"].DatabasesReporting);
            Assert.Equal(2, byName["hypopg"].DatabasesTotal);
            Assert.Equal(6, byName["hypopg"].RowsAvailable);

            /* ZERO installed and TWO outdated: created in both databases, current in neither. Reported
               apart rather than summed, because "created" and "up to date" are different questions and a
               single figure answers whichever one the reader assumed. */
            Assert.Equal(0, byName["pg_cron"].DatabasesInstalled);
            Assert.Equal(2, byName["pg_cron"].DatabasesOutdated);

            /* `bloom` is absent in one database and available in the other, so it is created NOWHERE and
               the HAVING excludes it. A census that listed it would put a row reading "installed in 0 of 2"
               beside the ones that matter, on every uninstalled extension the build offers - 100 of them on
               the measured fleet. */
            Assert.DoesNotContain("bloom", census.Select(row => row.ExtensionName));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteRowsAsync);
        }
    }

    /// <summary>
    /// A server with NO stored rows still gets its denominators, because the scalars hang off a one-row
    /// relation the per-extension groups join to.
    ///
    /// <para>The row an inner join would drop, and it is the only one that separates "we looked and nothing
    /// is installed" from "we did not look". An empty list for both is the failure this whole pair of issues
    /// is about, one level down.</para>
    /// </summary>
    [Fact]
    public async Task AnEmptyCensusStillCarriesItsDenominators()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #3425 empty-census test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            await using var postgres = NpgsqlDataSource.Create(connectionString!);

            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);

            var census = await DarlingPgExtensionAvailabilityReader.GetInstallCensusAsync(
                postgres, ServerId,
                DarlingMcpTestData.Naive(now.AddHours(-48)), DarlingMcpTestData.Naive(now.AddHours(1)),
                cancellationToken: ct);

            var row = Assert.Single(census);

            Assert.Null(row.ExtensionName);
            Assert.Equal(0, row.DatabasesTotal);
            Assert.Equal(0, row.ExtensionNameCount);
            Assert.Equal(0, row.RowsAvailable);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteRowsAsync);
        }
    }

    /// <summary>
    /// The two MCP payloads, end to end: each one's <c>reach</c> block carries the arm AND the figures it
    /// was decided from, over the same fixtures as above.
    ///
    /// <para><b>Why this exists and a source pin did not do.</b> Mutation testing found that replacing the
    /// extension read's <c>rowsAhead</c> argument with a literal zero left every other assertion in this
    /// suite green — the source pins see that the classifier is CALLED and that its verdict reaches the
    /// payload, and <c>PgCappedReadTests</c> sees that the classifier is right about inputs it is handed
    /// directly. Neither can see the surface handing it the wrong inputs, which turns
    /// <see cref="PerformanceMonitor.Collectors.PgCappedReach.Unreachable"/> into
    /// <see cref="PerformanceMonitor.Collectors.PgCappedReach.RankedTail"/> and prints "you have the
    /// top-ranked rows" over a page that holds none of them. The figures are asserted numerically for the
    /// same reason: an arm alone is one of four values and two wrong arguments can still produce the right
    /// one.</para>
    /// </summary>
    [Fact]
    public async Task BothPayloadsCarryTheirReachFiguresAndNotJustTheArm()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #3424/#3425 payload test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var newest = now.AddHours(-2);

            /* Twelve answerless index rows and four answered ones — the same shape as the reader arm, so
               the figures below are checkable against a fixture a reader can count. */
            for (var i = 0; i < 12; i++)
            {
                await SeedIndexAsync(
                    connection, ct, newest, "wide", "wide_ix_" + i.ToString("00", CultureInfo.InvariantCulture),
                    (100 + i) * Mib, estTupleBytes: 48L, reclaimable: null, skippedReason: WidthsReason);
            }

            foreach (var (name, reclaimable) in new[]
            {
                ("orders_a_ix", 90L), ("orders_b_ix", 20L), ("orders_c_ix", 50L), ("orders_d_ix", 30L),
            })
            {
                await SeedIndexAsync(
                    connection, ct, newest, "orders", name, 500 * Mib,
                    estTupleBytes: 48L, reclaimable: reclaimable * Mib, skippedReason: null);
            }

            await LogRunAsync(connection, ct, newest, "SUCCESS");

            /* Three databases x six extension names = 18 pairs, two of them created in every database. */
            foreach (var database in new[] { "appdb", "hangfire", "postgres" })
            {
                await SeedExtensionAsync(
                    connection, ct, newest, database, "pg_stat_statements", "installed", relevant: true);
                await SeedExtensionAsync(
                    connection, ct, newest, database, "pgstattuple", "available", relevant: true);

                foreach (var name in new[] { "amcheck", "bloom", "citext" })
                {
                    await SeedExtensionAsync(
                        connection, ct, newest, database, name, "available", relevant: false);
                }

                await SeedExtensionAsync(
                    connection, ct, newest, database, "plpgsql", "installed", relevant: false);
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);

            // ── get_pg_index_bloat, unfiltered: 12 answerless ahead of 4 answers ──────────────────────
            using var unfiltered = JsonDocument.Parse(
                await DarlingMcpPgIndexTools.GetPgIndexBloat(postgres, ServerName, 168, ReadLimit));

            var unfilteredReach = unfiltered.RootElement.GetProperty("reach");

            Assert.Equal("Partial", unfilteredReach.GetProperty("arm").GetString());
            Assert.False(unfilteredReach.GetProperty("is_complete").GetBoolean());
            Assert.True(unfilteredReach.GetProperty("a_raised_limit_would_help").GetBoolean());
            Assert.Equal(12, unfilteredReach.GetProperty("answerless_rows_ahead").GetInt64());
            Assert.Equal(4, unfilteredReach.GetProperty("answered_rows_on_server").GetInt64());
            Assert.Equal(0, unfilteredReach.GetProperty("answered_rows_reachable_here").GetInt64());
            Assert.Equal(4, unfilteredReach.GetProperty("answered_rows_withheld").GetInt64());
            Assert.False(unfiltered.RootElement.GetProperty("answered_only").GetBoolean());

            // ── and filtered: nothing ahead, everything reachable ─────────────────────────────────────
            using var filtered = JsonDocument.Parse(
                await DarlingMcpPgIndexTools.GetPgIndexBloat(
                    postgres, ServerName, 168, ReadLimit, answered_only: true));

            var filteredReach = filtered.RootElement.GetProperty("reach");

            Assert.Equal("Complete", filteredReach.GetProperty("arm").GetString());
            Assert.Equal(0, filteredReach.GetProperty("answerless_rows_ahead").GetInt64());
            Assert.Equal(4, filteredReach.GetProperty("answered_rows_reachable_here").GetInt64());
            Assert.Equal(0, filteredReach.GetProperty("answered_rows_withheld").GetInt64());
            Assert.True(filtered.RootElement.GetProperty("answered_only").GetBoolean());

            /* A LIMIT SMALLER THAN THE ANSWERED POPULATION is a RankedTail and not a Complete: the top two
               of four by reclaimable bytes is the ranking working, and it is still a page. */
            using var narrow = JsonDocument.Parse(
                await DarlingMcpPgIndexTools.GetPgIndexBloat(
                    postgres, ServerName, 168, 2, answered_only: true));

            Assert.Equal("RankedTail", narrow.RootElement.GetProperty("reach").GetProperty("arm").GetString());
            Assert.Equal(
                2, narrow.RootElement.GetProperty("reach").GetProperty("answered_rows_withheld").GetInt64());

            // ── get_pg_extensions: 16 non-created rows ahead of 6 created ones ────────────────────────
            using var extensions = JsonDocument.Parse(
                await DarlingMcpPgServerStateTools.GetPgExtensions(postgres, ServerName, 168, ReadLimit));

            var root = extensions.RootElement;
            var extensionReach = root.GetProperty("reach");

            /* THE ARITHMETIC, not just the arm. 18 pairs of which 6 are created, so 12 rows are ahead of
               them — and a surface that handed the classifier a zero here would answer RankedTail and
               print "you have the top-ranked rows" over a page holding none of them. */
            Assert.Equal(12, extensionReach.GetProperty("rows_ahead_of_created").GetInt64());
            Assert.Equal(6, extensionReach.GetProperty("created_rows_on_server").GetInt64());
            Assert.Equal("Partial", extensionReach.GetProperty("arm").GetString());
            Assert.Equal(0, extensionReach.GetProperty("created_rows_reachable_here").GetInt64());
            Assert.Equal(6, extensionReach.GetProperty("created_rows_withheld").GetInt64());

            var census = root.GetProperty("census");

            Assert.Equal(3, census.GetProperty("databases_total").GetInt64());
            Assert.Equal(6, census.GetProperty("extension_name_count").GetInt64());
            Assert.Equal(18, census.GetProperty("rows_available").GetInt64());
            Assert.Equal(18, census.GetProperty("rows_expected_if_uniform").GetInt64());
            Assert.Equal(3, census.GetProperty("databases_in_page").GetInt32());

            /* NOT ONE database is complete in a ten-row page of an eighteen-row product, and the payload
               says so rather than leaving a reader to divide the row count by anything. */
            Assert.Equal(0, census.GetProperty("databases_complete_in_page").GetInt32());

            /* THE STATE TOTALS ARE WITHHELD on a truncated page, and the install census is not. */
            Assert.Equal(JsonValueKind.Null, root.GetProperty("installed").ValueKind);

            var installed = root.GetProperty("install_census").EnumerateArray()
                .ToDictionary(row => row.GetProperty("extension_name").GetString()!, StringComparer.Ordinal);

            Assert.Equal(
                new[] { "pg_stat_statements", "plpgsql" },
                installed.Keys.OrderBy(name => name, StringComparer.Ordinal).ToArray());
            Assert.Equal(3, installed["plpgsql"].GetProperty("databases_installed").GetInt64());
            Assert.Equal(3, installed["plpgsql"].GetProperty("databases_total").GetInt64());

            /* AND plpgsql IS ABSENT FROM THE ROWS in the same payload, which is the whole of #3425 stated
               as one comparison rather than two tests. */
            Assert.DoesNotContain(
                "plpgsql",
                root.GetProperty("extensions").EnumerateArray()
                    .Select(row => row.GetProperty("extension_name").GetString()));

            // ── the database filter completes one database, in the payload too ────────────────────────
            using var oneDatabase = JsonDocument.Parse(
                await DarlingMcpPgServerStateTools.GetPgExtensions(
                    postgres, ServerName, 168, 50, "hangfire"));

            Assert.Equal("hangfire", oneDatabase.RootElement.GetProperty("database_name").GetString());
            Assert.Equal(6, oneDatabase.RootElement.GetProperty("extension_count").GetInt32());
            Assert.Contains(
                "plpgsql",
                oneDatabase.RootElement.GetProperty("extensions").EnumerateArray()
                    .Select(row => row.GetProperty("extension_name").GetString()));

            /* ONE RESPONSE, ONE SCOPE - the assertion that found a real defect in review. The census must
               narrow with the rows: measured against a server-wide census beside one database's rows, a
               sixteen-database host put a denominator of 1,632 next to a complete 102-row answer and the
               reach classifier correctly answered Unreachable, telling a filtered caller they could not see
               what they were holding. Here the filtered call is Complete, over a census of one database. */
            var filteredCensus = oneDatabase.RootElement.GetProperty("census");

            Assert.Equal(1, filteredCensus.GetProperty("databases_total").GetInt64());
            Assert.Equal(6, filteredCensus.GetProperty("rows_available").GetInt64());
            Assert.Equal(1, filteredCensus.GetProperty("databases_in_page").GetInt32());
            Assert.Equal(1, filteredCensus.GetProperty("databases_complete_in_page").GetInt32());
            Assert.Equal(0, filteredCensus.GetProperty("databases_absent_from_page").GetInt64());

            var oneDatabaseReach = oneDatabase.RootElement.GetProperty("reach");

            Assert.Equal("Complete", oneDatabaseReach.GetProperty("arm").GetString());
            Assert.True(oneDatabaseReach.GetProperty("is_complete").GetBoolean());
            Assert.Equal(2, oneDatabaseReach.GetProperty("created_rows_on_server").GetInt64());
            Assert.Equal(0, oneDatabaseReach.GetProperty("created_rows_withheld").GetInt64());

            /* And the install census narrowed with it: plpgsql in 1 of 1, not 3 of 3. */
            var filteredInstalled = oneDatabase.RootElement.GetProperty("install_census").EnumerateArray()
                .ToDictionary(row => row.GetProperty("extension_name").GetString()!, StringComparer.Ordinal);

            Assert.Equal(1, filteredInstalled["plpgsql"].GetProperty("databases_installed").GetInt64());
            Assert.Equal(1, filteredInstalled["plpgsql"].GetProperty("databases_total").GetInt64());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteRowsAsync);
        }
    }

    /// <summary>
    /// <c>answered_only</c> over a server whose every index is unmodellable answers <c>no_answers</c>, and
    /// the refusal carries the suppression breakdown.
    ///
    /// <para>The fleet's commonest state — a monitoring login that cannot read <c>pg_stats</c> — and the one
    /// where an <c>empty</c> status would read as "nothing to reclaim" over a server holding hundreds of
    /// gigabytes of unmeasurable index.</para>
    /// </summary>
    [Fact]
    public async Task AnsweredOnlyOverAServerWithNoAnswersRefusesRatherThanReadingEmpty()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #3424 no_answers test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var newest = now.AddHours(-2);

            for (var i = 0; i < 3; i++)
            {
                await SeedIndexAsync(
                    connection, ct, newest, "wide", "wide_ix_" + i.ToString(CultureInfo.InvariantCulture),
                    200 * Mib, estTupleBytes: 48L, reclaimable: null, skippedReason: WidthsReason);
            }

            await LogRunAsync(connection, ct, newest, "SUCCESS");

            await using var postgres = NpgsqlDataSource.Create(connectionString!);

            using var refused = JsonDocument.Parse(
                await DarlingMcpPgIndexTools.GetPgIndexBloat(
                    postgres, ServerName, 168, ReadLimit, answered_only: true));

            Assert.Equal("no_answers", refused.RootElement.GetProperty("status").GetString());

            var message = refused.RootElement.GetProperty("message").GetString()!;

            Assert.Contains("NOT ONE of them", message, StringComparison.Ordinal);
            Assert.Contains("not a clean bill of health", message, StringComparison.Ordinal);

            /* THE BREAKDOWN TRAVELS WITH THE REFUSAL. A refusal naming no cause sends the reader back for
               a second call to learn what they cannot see, which is the round trip #3278's census exists
               to remove. */
            var hints = refused.RootElement.GetProperty("hints");

            Assert.Equal(3, hints.GetProperty("candidate_index_count").GetInt64());
            Assert.Equal(0, hints.GetProperty("trusted_index_count").GetInt64());
            Assert.Equal("NothingTrusted", hints.GetProperty("arm").GetString());
            Assert.Equal(
                "ColumnWidthsNotVisible",
                Assert.Single(hints.GetProperty("suppressed_by_reason").EnumerateArray())
                    .GetProperty("reason").GetString());

            /* THE CONTROL: without the filter the same server answers with ROWS, so `no_answers` is the
               filter's own refusal and not this server being empty. */
            using var unfiltered = JsonDocument.Parse(
                await DarlingMcpPgIndexTools.GetPgIndexBloat(postgres, ServerName, 168, ReadLimit));

            Assert.Equal(3, unfiltered.RootElement.GetProperty("index_count").GetInt32());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static Task SeedIndexAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc,
        string tableName, string indexName, long indexBytes, long? estTupleBytes, long? reclaimable,
        string? skippedReason) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_index_bloat
    (collection_id, collection_time, server_id, server_name, database_name, schema_name,
     table_name, index_name, index_bytes, skipped_reason, est_tuple_bytes, est_reclaimable_bytes)
VALUES ($1::bigint, $2::timestamp, $3::integer, $4::text, $5::text, $6::text,
        $7::text, $8::text, $9::bigint, $10::text, $11::bigint, $12::bigint)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), ServerId, ServerName,
            "appdb", "public", tableName, indexName, indexBytes, skippedReason, estTupleBytes, reclaimable);

    private static Task SeedExtensionAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc,
        string databaseName, string extensionName, string state, bool relevant) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_extension_availability
    (collection_id, collection_time, server_id, server_name, database_name, extension_name,
     state, installed_version, default_version, is_monitoring_relevant)
VALUES ($1::bigint, $2::timestamp, $3::integer, $4::text, $5::text, $6::text,
        $7::text, $8::text, $9::text, $10::boolean)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), ServerId, ServerName,
            databaseName, extensionName, state,
            state == "installed" ? "1.0" : state == "outdated" ? "0.9" : null,
            state == "absent" ? null : "1.0",
            relevant);

    private static Task LogRunAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, string status) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time, status)
VALUES ($1::bigint, $2::integer, $3::text, 'pg_index_bloat', $4::timestamp, $5::text)",
            CollectionIdGenerator.Next(), ServerId, ServerName,
            DarlingMcpTestData.Naive(collectionTimeUtc), status);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(
            connection, ct, "DELETE FROM pg_index_bloat WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(
            connection, ct, "DELETE FROM pg_extension_availability WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(
            connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(
            connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
    }
}
