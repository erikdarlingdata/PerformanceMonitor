/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Live-Postgres pin for #2991, the Darling twin of Lite's
/// <c>ParameterSensitivityClockFrameTests</c>: the PARAMETER_SENSITIVITY detector's
/// compiled-before-the-window predicate must select the same plan population no matter what the
/// monitored server's UTC offset is.
///
/// <para><c>query_stats.creation_time</c> is the monitored server's LOCAL wall clock —
/// <c>QueryStatsCollector</c> ships the <c>sys.dm_exec_query_stats</c> value verbatim, and says so about
/// this very column: "creation_time is in the monitored server's local time while collection times are
/// UTC". The window bound it is compared against is naive UTC off <c>DateTime.UtcNow</c>. Comparing them
/// untranslated is a different question on every server, and the answer is wrong in both directions.</para>
///
/// <para><b>What the predicate is for.</b> It means "this plan was compiled before the analysis window
/// opened", which is what makes a wide min/max worker-time spread evidence of PARAMETER SENSITIVITY
/// rather than an artefact of a plan too young to have seen varied parameters yet. Loosening it or
/// removing it destroys the signal, so the fix is to make it mean what it says.</para>
///
/// <para><b>The invariance, not a timestamp.</b> This asserts a FRAME RELATIONSHIP: the same five-plan
/// fixture, re-expressed in each server's local clock the way the collector would really have stored it,
/// must yield the identical offender set. Pinning one expected <c>creation_time</c> instead would pass
/// for the wrong reason the moment anything shifted. Membership is asserted alongside invariance on
/// purpose — a constant empty result is also invariant, so invariance alone is not a test.</para>
///
/// <para><b>Both signs and zero.</b> UTC-4 is the live production fleet (every monitored SQL Server
/// reports <c>utc_offset_minutes = -240</c>; the only target at <c>0</c> is a dev box), and it is the
/// FALSE-POSITIVE direction: a plan compiled at UTC instant T is stored as T-4h, so the untranslated
/// predicate admits T &lt;= W+4h and on the default four-hour window that is every plan compiled inside
/// the window — precisely the population the predicate exists to exclude. A POSITIVE offset is the
/// suppression direction: at UTC+10 it admits only plans compiled at least ten hours before the window,
/// so the plans that legitimately predate it are discarded and the finding class is sharply thinned. A
/// fixture at -240 alone would never see that half.</para>
///
/// <para><b>Watched RED before the fix</b> at 5 / 3 / 1 offenders for -240 / 0 / +600 against a true 3,
/// on this exact fixture. Zero was the only offset that was ever right, which is why every store anyone
/// develops against agreed with the bug.</para>
///
/// <para>Live rather than a string pin because the thing under test is the ENGINE's answer to signed
/// <c>make_interval</c> arithmetic against a naive <c>timestamp</c> column, and the resolution of the
/// collected offset through the real migrated schema. An <c>Assert.Contains</c> on query text cannot see
/// either, and a retyped copy of the query would only prove the transcription.</para>
///
/// <para><b>Since #4821</b> the SQL keeps only a rough filter on the newest snapshot's offset, an hour wider than the
/// window start, and the reader makes the exact test through the server's zone and then applies the cap. Two more
/// arms pin that split: the mirror of the January-plans case (a winter newest snapshot, July plans), where the hour
/// of margin is what keeps a plan compiled a minute before the window, and a cap boundary, where plans the rough
/// filter admits and the exact test rejects outrank the real offenders and must not take their slots.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class ParameterSensitivityClockFrameLiveTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -299101;
    private const string TestServerName = "SynthSrv";
    private const string Db = "SynthDb";

    /* The three offsets the fix has to hold at. -240 is the live fleet and the false-positive
       direction, +600 is the suppression direction, 0 is the dev box and the no-offset fallback. */
    private const int EasternOffsetMinutes = -240;
    private const int UtcOffsetMinutes = 0;
    private const int FarEastOffsetMinutes = 600;

    /* US Eastern in January (#4821): the offset in force at the plans, not the summer one the newest snapshot holds. */
    private const int WinterEasternOffsetMinutes = -300;

    /* Minutes of the plan's compile instant relative to the window START, in UTC, and whether it
       therefore belongs in the offender set. Two plans straddle the bound by a single minute in each
       direction: that is what makes a wrong frame change the ANSWER rather than just the arithmetic. */
    private static readonly (string Label, int MinutesFromWindowStart, bool Expected)[] Plans =
    [
        ("old_3d", -3 * 24 * 60, true),
        ("pre_5h", -5 * 60,      true),
        ("pre_1m", -1,           true),
        ("in_1m",  1,            false),
        ("in_3h",  3 * 60,       false),
    ];

    private static int ExpectedOffenders => Plans.Count(p => p.Expected);

    private static DateTime TruncateToSeconds(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    /// <summary>
    /// Opens a session with the store's search_path SET explicitly rather than inherited — Npgsql pools
    /// PHYSICAL sessions, so one opened before this store's first migration keeps the pre-ALTER default
    /// for its whole life and the bare table names below resolve to nothing on a first run.
    /// </summary>
    private static async Task<NpgsqlConnection> OpenWithSearchPathAsync(string connectionString, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var setPath = new NpgsqlCommand("SET search_path = " + PgSchemaGenerator.SearchPath, connection);
        await setPath.ExecuteNonQueryAsync(ct);
        return connection;
    }

    [Fact]
    public async Task TheCompiledBeforeTheWindowPredicate_SelectsTheSamePlans_AtEveryServerUtcOffset()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #2991 clock-frame test.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
        {
            await DeleteTestRowsAsync(connection, ct);
        }

        try
        {
            var windowEnd = TruncateToSeconds(DateTime.UtcNow);
            var windowStart = windowEnd.AddHours(-4);
            var context = new AnalysisContext
            {
                ServerId = TestServerId,
                ServerName = TestServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
            };

            var observed = new Dictionary<string, double>(StringComparer.Ordinal);
            var drilled = new Dictionary<string, double>(StringComparer.Ordinal);

            /* Each case re-expresses the SAME five compile instants in a different server-local clock,
               exactly as the collector would have stored them, and writes the matching collected
               offset. The last case writes NO offset at all. */
            foreach (var (name, offsetMinutes) in new (string, int?)[]
            {
                ("utc-4 (the live fleet)", EasternOffsetMinutes),
                ("utc (the dev box)",      UtcOffsetMinutes),
                ("utc+10 (suppression)",   FarEastOffsetMinutes),
                ("no collected offset",    null),
            })
            {
                await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
                {
                    await DeleteTestRowsAsync(connection, ct);
                    if (offsetMinutes.HasValue)
                    {
                        await SeedServerPropertiesAsync(connection, windowEnd, offsetMinutes.Value, ct);
                    }
                    await SeedPlansAsync(connection, windowStart, offsetMinutes ?? 0, ct);
                }

                await using var postgres = NpgsqlDataSource.Create(connectionString!);
                var fact = await CollectParameterSensitivityFactAsync(postgres, context);

                Assert.NotNull(fact);
                observed[name] = fact!.Metadata["offender_count"];

                /* The drill-down re-runs the detector's own signature to list the offenders, so it
                   carries a SECOND copy of the same predicate. A fix applied only to the detector
                   would leave the operator reading a list assembled in the wrong frame. */
                drilled[name] = await CountParameterSensitiveDrillDownAsync(postgres, context);
            }

            /* The invariance. The offset is a property of the SERVER's clock, not of its workload, so it
               must not be able to change which plans the detector counts. */
            Assert.True(
                observed.Values.Distinct().Count() == 1,
                "the compiled-before-the-window predicate selected a DIFFERENT plan population per server "
                + "UTC offset, so creation_time is still being compared across frames: "
                + string.Join(", ", observed.Select(kv => $"{kv.Key} => {kv.Value}")));

            /* And membership, because a constant empty answer would satisfy the invariance above while
               having destroyed the signal. The two plans that straddle the window bound by one minute
               are what make this assertion load-bearing. */
            foreach (var (name, count) in observed)
            {
                Assert.True(
                    count == ExpectedOffenders,
                    $"at {name} the detector counted {count} offenders against the {ExpectedOffenders} plans "
                    + "whose UTC compile instant really precedes the window. Admitting the in-window plans "
                    + "is the UTC-4 failure (5 here); dropping the ones that just predate the window is the "
                    + "UTC+10 failure (1 here).");
            }

            /* Same two assertions again for the drill-down's own copy of the predicate. */
            Assert.True(
                drilled.Values.Distinct().Count() == 1,
                "the drill-down's copy of the compiled-before-the-window predicate selected a DIFFERENT "
                + "plan population per server UTC offset: "
                + string.Join(", ", drilled.Select(kv => $"{kv.Key} => {kv.Value}")));

            foreach (var (name, count) in drilled)
            {
                Assert.True(
                    count == ExpectedOffenders,
                    $"at {name} the drill-down listed {count} offenders against the {ExpectedOffenders} "
                    + "plans whose UTC compile instant really precedes the window.");
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteTestRowsAsync);
        }
    }

    /// <summary>
    /// #4821: the newest snapshot is from the summer (-240) but the server reports its zone, and every plan was
    /// compiled in January, when US Eastern is 5 hours behind UTC. Converting with the ONE newest offset puts each
    /// plan an hour early, so the plan that really compiled a minute AFTER the window start reads as compiled
    /// before it; the zone puts every plan at its real instant.
    /// </summary>
    [Fact]
    public async Task TheCompiledBeforeTheWindowPredicate_ConvertsEachPlanWithTheOffsetInForceAtItsCompileTime()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #4821 daylight-saving test.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        try
        {
            var windowEnd = new DateTime(2026, 1, 15, 18, 0, 0, DateTimeKind.Unspecified);
            var windowStart = windowEnd.AddHours(-4);
            var context = new AnalysisContext
            {
                ServerId = TestServerId,
                ServerName = TestServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
            };

            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await DeleteTestRowsAsync(connection, ct);
                await SeedServerPropertiesAsync(connection, windowEnd, EasternOffsetMinutes, ct, "Eastern Standard Time");
                await SeedPlansAsync(connection, windowStart, WinterEasternOffsetMinutes, ct);
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var fact = await CollectParameterSensitivityFactAsync(postgres, context);

            Assert.NotNull(fact);
            Assert.Equal(ExpectedOffenders, fact!.Metadata["offender_count"]);
            Assert.Equal(ExpectedOffenders, await CountParameterSensitiveDrillDownAsync(postgres, context));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteTestRowsAsync);
        }
    }

    /// <summary>
    /// The mirror of the test above, for the SQL's one-hour margin (#4821): the newest snapshot is from the WINTER
    /// (-300, collected in January and months old, because <c>server_properties</c> is collected when the server
    /// connects) and every plan was compiled in July, when US Eastern is at -240. The newest offset reads a plan
    /// compiled a minute before the window opens 59 minutes AFTER it, so a first filter without the hour of margin
    /// would drop a plan the exact test keeps. With the margin the three plans compiled before the window stay and
    /// the two compiled after it still go.
    /// </summary>
    [Fact]
    public async Task TheCompiledBeforeTheWindowPredicate_KeepsAPlanCompiledJustBeforeTheWindow_WhenTheNewestSnapshotIsWinterAndThePlansAreSummer()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #4821 daylight-saving margin test.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        try
        {
            var windowEnd = new DateTime(2026, 7, 15, 18, 0, 0, DateTimeKind.Unspecified);
            var windowStart = windowEnd.AddHours(-4);
            var context = new AnalysisContext
            {
                ServerId = TestServerId,
                ServerName = TestServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
            };

            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await DeleteTestRowsAsync(connection, ct);
                await SeedServerPropertiesAsync(
                    connection, new DateTime(2026, 1, 15, 18, 0, 0, DateTimeKind.Unspecified), WinterEasternOffsetMinutes, ct,
                    "Eastern Standard Time");
                await SeedPlansAsync(connection, windowStart, EasternOffsetMinutes, ct);
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var fact = await CollectParameterSensitivityFactAsync(postgres, context);

            Assert.NotNull(fact);
            Assert.Equal(ExpectedOffenders, fact!.Metadata["offender_count"]);
            Assert.Equal(ExpectedOffenders, await CountParameterSensitiveDrillDownAsync(postgres, context));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteTestRowsAsync);
        }
    }

    /// <summary>
    /// The fact's cap of twenty used to be a <c>LIMIT</c> in SQL, after the compiled-before-the-window test, so it kept
    /// the twenty worst plans that test admitted. The exact test now runs in the reader (#4821), so the cap has to run
    /// after it too. Twenty plans that pass the SQL's rough first filter (January plans read against a summer newest
    /// offset), fail the exact test (they were compiled 30 minutes AFTER the window opened) and outrank every real
    /// offender would take every slot of a cap applied first and leave the fact empty. The real offenders must still
    /// fill it, and no decoy may count.
    /// </summary>
    [Fact]
    public async Task TheParameterSensitivityFact_StillFillsItsCapWithRealOffenders_WhenPlansTheExactTestRejectsOutrankThem()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #4821 cap-boundary test.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        try
        {
            var windowEnd = new DateTime(2026, 1, 15, 18, 0, 0, DateTimeKind.Unspecified);
            var windowStart = windowEnd.AddHours(-4);
            var context = new AnalysisContext
            {
                ServerId = TestServerId,
                ServerName = TestServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
            };
            var cap = PgFactCollector.ParameterSensitivityMaxOffenders;

            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await DeleteTestRowsAsync(connection, ct);
                await SeedServerPropertiesAsync(connection, windowEnd, EasternOffsetMinutes, ct, "Eastern Standard Time");
                await SeedCapBoundaryPlansAsync(connection, windowStart, WinterEasternOffsetMinutes, decoys: cap, realOffenders: cap + 2, ct);
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var fact = await CollectParameterSensitivityFactAsync(postgres, context);

            Assert.NotNull(fact);
            Assert.Equal(cap, fact!.Metadata["offender_count"]);
            Assert.Equal((double)RealMaxWorkerTime / RealMinWorkerTime, fact.Metadata["worst_ratio"]);
            Assert.Equal(RealMinWorkerTime, fact.Metadata["worst_min_worker_us"]);
            Assert.Equal(RealMaxWorkerTime, fact.Metadata["worst_max_worker_us"]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteTestRowsAsync);
        }
    }

    /// <summary>
    /// Drives the drill-down's own copy of the detection through the real enrich seam. Severity is set
    /// past the display gate, below which the expensive drill-downs are skipped wholesale and this
    /// collector never runs at all.
    /// </summary>
    private static async Task<double> CountParameterSensitiveDrillDownAsync(
        NpgsqlDataSource postgres, AnalysisContext context)
    {
        var finding = new AnalysisFinding
        {
            RootFactKey = "PARAMETER_SENSITIVITY",
            StoryPath = "PARAMETER_SENSITIVITY",
            /* #3859: the collector matches on PathKeys, not on a split of the rendered path. */
            PathKeys = ["PARAMETER_SENSITIVITY"],
            Severity = 1.0,
        };

        await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);

        if (finding.DrillDown is null
            || !finding.DrillDown.TryGetValue("parameter_sensitive_queries", out var raw))
        {
            return 0;
        }

        return System.Text.Json.JsonSerializer.SerializeToElement(raw).GetArrayLength();
    }

    private static async Task<Fact?> CollectParameterSensitivityFactAsync(NpgsqlDataSource postgres, AnalysisContext context)
    {
        var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);
        return facts.FirstOrDefault(f => f.Key == "PARAMETER_SENSITIVITY");
    }

    private static async Task SeedServerPropertiesAsync(
        NpgsqlConnection connection, DateTime collectionTime, int offsetMinutes, CancellationToken ct,
        string? timeZoneId = null)
    {
        await using var cmd = new NpgsqlCommand(@"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name, edition, product_version, product_level,
     engine_edition, utc_offset_minutes, time_zone_id)
VALUES ($1, $2, $3, $4, 'Enterprise Edition', '16.0.4085.2', 'RTM', 3, $5, $6)", connection);
        cmd.Parameters.AddWithValue(-9_299_000L);
        cmd.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        cmd.Parameters.AddWithValue(TestServerId);
        cmd.Parameters.AddWithValue(TestServerName);
        cmd.Parameters.AddWithValue(offsetMinutes);
        cmd.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Text, (object?)timeZoneId ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Seeds the five plans. <paramref name="offsetMinutes"/> is applied to <c>creation_time</c> and
    /// ONLY to creation_time: server-local is UTC plus the offset, which is what the DMV would have read
    /// on a server at that offset. collection_time stays UTC because the collector stamps it from the
    /// monitoring host's clock. Every row clears the detector's other floors with room to spare, so the
    /// only thing that can move the offender count is the compile-time predicate.
    /// </summary>
    private static async Task SeedPlansAsync(
        NpgsqlConnection connection, DateTime windowStart, int offsetMinutes, CancellationToken ct)
    {
        var id = -9_299_100L;

        for (var i = 0; i < Plans.Length; i++)
        {
            var plan = Plans[i];
            var compiledUtc = windowStart.AddMinutes(plan.MinutesFromWindowStart);

            await using var cmd = new NpgsqlCommand(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     creation_time, execution_count, min_worker_time, max_worker_time, min_grant_kb, max_grant_kb,
     min_spills, max_spills, query_text, delta_execution_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 5000, 20000, 20000000, 1024, 1048576, 0, 50, $9, 500)", connection);
            cmd.Parameters.AddWithValue(id--);
            cmd.Parameters.AddWithValue(DateTime.SpecifyKind(windowStart.AddMinutes(30 + i), DateTimeKind.Unspecified));
            cmd.Parameters.AddWithValue(TestServerId);
            cmd.Parameters.AddWithValue(TestServerName);
            cmd.Parameters.AddWithValue(Db);
            cmd.Parameters.AddWithValue("0xQH_" + plan.Label);
            cmd.Parameters.AddWithValue("0xPH_" + plan.Label);
            cmd.Parameters.AddWithValue(DateTime.SpecifyKind(compiledUtc.AddMinutes(offsetMinutes), DateTimeKind.Unspecified));
            cmd.Parameters.AddWithValue("SELECT * FROM dbo.Synth_" + plan.Label + " WHERE col = @p");
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    /* A real offender's worker times (ratio 1,000) and a decoy's (ratio 25,000, so a decoy sorts ahead of every real one). */
    private const long RealMinWorkerTime = 20_000;
    private const long RealMaxWorkerTime = 20_000_000;
    private const long DecoyMinWorkerTime = 10_000;
    private const long DecoyMaxWorkerTime = 250_000_000;

    /// <summary>
    /// Seeds the plans of the cap arm, stored at the server's local clock as it read then
    /// (<paramref name="offsetMinutes"/> is applied to <c>creation_time</c> only, as in <see cref="SeedPlansAsync"/>).
    /// Each of the <paramref name="realOffenders"/> was compiled five hours before the window opened. Each of the
    /// <paramref name="decoys"/> was compiled 30 minutes AFTER it opened and has a higher worker ratio, so it sorts
    /// first: against a newest snapshot from the other side of a daylight saving change it reads as compiled 30
    /// minutes BEFORE the window, which passes the SQL's rough first filter, and only the exact test rejects it.
    /// </summary>
    private static async Task SeedCapBoundaryPlansAsync(
        NpgsqlConnection connection, DateTime windowStart, int offsetMinutes, int decoys, int realOffenders, CancellationToken ct)
    {
        var id = -9_299_200L;

        for (var i = 0; i < decoys + realOffenders; i++)
        {
            var isDecoy = i < decoys;
            var label = (isDecoy ? "decoy_" : "real_") + i.ToString("D2", System.Globalization.CultureInfo.InvariantCulture);
            var compiledUtc = isDecoy ? windowStart.AddMinutes(30) : windowStart.AddHours(-5);

            await using var cmd = new NpgsqlCommand(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     creation_time, execution_count, min_worker_time, max_worker_time, min_grant_kb, max_grant_kb,
     min_spills, max_spills, query_text, delta_execution_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 5000, $10, $11, 1024, 1048576, 0, 50, $9, 500)", connection);
            cmd.Parameters.AddWithValue(id--);
            cmd.Parameters.AddWithValue(DateTime.SpecifyKind(windowStart.AddMinutes(30 + i), DateTimeKind.Unspecified));
            cmd.Parameters.AddWithValue(TestServerId);
            cmd.Parameters.AddWithValue(TestServerName);
            cmd.Parameters.AddWithValue(Db);
            cmd.Parameters.AddWithValue("0xQH_" + label);
            cmd.Parameters.AddWithValue("0xPH_" + label);
            cmd.Parameters.AddWithValue(DateTime.SpecifyKind(compiledUtc.AddMinutes(offsetMinutes), DateTimeKind.Unspecified));
            cmd.Parameters.AddWithValue("SELECT * FROM dbo.Synth_" + label + " WHERE col = @p");
            cmd.Parameters.AddWithValue(isDecoy ? DecoyMinWorkerTime : RealMinWorkerTime);
            cmd.Parameters.AddWithValue(isDecoy ? DecoyMaxWorkerTime : RealMaxWorkerTime);
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "query_stats", "server_properties" })
        {
            await using var cmd = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = $1", connection);
            cmd.Parameters.AddWithValue(TestServerId);
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }
}
