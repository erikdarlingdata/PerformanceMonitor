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
        NpgsqlConnection connection, DateTime collectionTime, int offsetMinutes, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(@"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name, edition, product_version, product_level,
     engine_edition, utc_offset_minutes)
VALUES ($1, $2, $3, $4, 'Enterprise Edition', '16.0.4085.2', 'RTM', 3, $5)", connection);
        cmd.Parameters.AddWithValue(-9_299_000L);
        cmd.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        cmd.Parameters.AddWithValue(TestServerId);
        cmd.Parameters.AddWithValue(TestServerName);
        cmd.Parameters.AddWithValue(offsetMinutes);
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
