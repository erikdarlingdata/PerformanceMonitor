/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
/// get_collection_log (#2484): the raw per-run log the viewer's Collection Log and Duration Trends tabs
/// read, which had no MCP tool and therefore no /api/read endpoint either -- so neither the web dashboard
/// nor an agent could see it, and the WPF viewer on a Windows desktop was the only way to it.
///
/// <para>The assertions that matter are the two KINDS of empty. "No runs in the last N hours" is true both
/// of a server that collected fine and was quiet, and of a server that has never collected at all, and
/// those want opposite responses -- widen the window, versus go find out why collection is not running.
/// A single sentence covering both is the exact failure #2485 catalogues elsewhere, so this read is not
/// allowed to add another instance of it.</para>
///
/// <para>Gated on DARLING_TEST_PG like every other live class.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingCollectionLogReadTests
{
    private const int ServerId = -949552;
    private const string ServerName = "collection-log-read";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task EmptyWindow_AndNeverCollected_AreDifferentAnswers_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live collection-log read test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);

        /*
            The cleanup runs on its OWN connection, not this body's. A teardown on the body's
            connection throws out of the finally and REPLACES the exception already in flight, so a
            failing test reports its cleanup error instead of its own -- and it is the body's failure
            that closed the connection in the first place. LiveStoreCleanup is the enforced route.
        */
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* ── 1. registered, never collected: a FAULT, and it has to say so ── */
            var never = await DarlingMcpDataTools.GetCollectionLog(dataSource, ServerName, 24, 200);
            var neverDoc = JsonDocument.Parse(never);
            Assert.Equal("unavailable", neverDoc.RootElement.GetProperty("status").GetString());
            var neverText = neverDoc.RootElement.GetProperty("message").GetString()!;
            Assert.Contains("EVER", neverText, StringComparison.Ordinal);

            /* The trap this test exists for: a caller told to widen the window will never fill it. */
            Assert.DoesNotContain("widen", neverText, StringComparison.OrdinalIgnoreCase);

            /*
                And still the FAULT when a FILTER is supplied, because a fault outranks a miss.

                #3287's filtered branch short-circuited ahead of the never-collected probe, so this call
                answered "the filters were applied, so unfiltered runs may well exist -- drop them to see
                what the window holds" about a server with nothing to see either way. That is the same
                defect the two branches above exist to prevent, reintroduced by the branch added to prevent
                it. Review caught it; nothing here covered it, which is how it got in. Both filters
                separately, because the branch triggers on either one.
            */
            foreach (var filtered in new[]
                     {
                         await DarlingMcpDataTools.GetCollectionLog(dataSource, ServerName, 24, 200, collector_name: "query_store"),
                         await DarlingMcpDataTools.GetCollectionLog(dataSource, ServerName, 24, 200, min_duration_ms: 1000),
                     })
            {
                var filteredRoot = JsonDocument.Parse(filtered).RootElement;
                Assert.Equal("unavailable", filteredRoot.GetProperty("status").GetString());

                var filteredText = filteredRoot.GetProperty("message").GetString()!;
                Assert.Contains("EVER", filteredText, StringComparison.Ordinal);

                /* Not the filtered wording, which would send this caller to unfilter a window that will
                   never fill -- the specific false instruction the ordering fixes. */
                Assert.DoesNotContain("Drop them", filteredText, StringComparison.Ordinal);
                Assert.DoesNotContain("widen", filteredText, StringComparison.OrdinalIgnoreCase);
            }

            /* ── 2. collected, but not inside the asked-for window: a TRUE NEGATIVE ── */
            await SeedAsync(connection, ct, "query_store", HoursAgo(48));

            var quiet = await DarlingMcpDataTools.GetCollectionLog(dataSource, ServerName, 1, 200);
            var quietDoc = JsonDocument.Parse(quiet);
            Assert.Equal("empty", quietDoc.RootElement.GetProperty("status").GetString());
            var quietText = quietDoc.RootElement.GetProperty("message").GetString()!;

            /* Same row count as case 1 -- zero -- and it must NOT reach for the same word. */
            Assert.DoesNotContain("EVER", quietText, StringComparison.Ordinal);
            Assert.Contains("widen", quietText, StringComparison.OrdinalIgnoreCase);

            /* ── 3. rows in the window: the data path, and the split that makes the log worth reading ── */
            await SeedAsync(connection, ct, "query_store", MinutesAgo(10));
            await SeedAsync(connection, ct, "deadlocks", MinutesAgo(5));

            var hit = await DarlingMcpDataTools.GetCollectionLog(dataSource, ServerName, 24, 200);
            var root = JsonDocument.Parse(hit).RootElement;
            Assert.Equal(ServerName, root.GetProperty("server").GetString());
            Assert.Equal(2, root.GetProperty("run_count").GetInt32());
            Assert.False(root.GetProperty("truncated").GetBoolean());

            var runs = root.GetProperty("runs");
            Assert.Equal(2, runs.GetArrayLength());

            /* Newest first, so the 5-minute-old deadlocks run leads the 10-minute-old query_store one. */
            Assert.Equal("deadlocks", runs[0].GetProperty("collector").GetString());

            /* The whole point of the raw log over the rollup: WHERE the time went. A collector slow
               because the monitored server is slow needs a different fix from one slow because the
               store is, and the total alone cannot separate them. */
            Assert.Equal(80, runs[0].GetProperty("sql_duration_ms").GetDouble());
            Assert.Equal(20, runs[0].GetProperty("store_duration_ms").GetDouble());

            /* ── 4. the cap announces itself rather than leaving it to be inferred ── */
            var capped = await DarlingMcpDataTools.GetCollectionLog(dataSource, ServerName, 24, 1);
            var cappedRoot = JsonDocument.Parse(capped).RootElement;
            Assert.Equal(1, cappedRoot.GetProperty("run_count").GetInt32());
            Assert.True(cappedRoot.GetProperty("truncated").GetBoolean());

            /*
                And it does NOT announce itself when the window simply holds exactly the cap. Comparing
                the row count to the limit cannot separate those two, so the read over-fetches by one and
                reports what it observed. Two rows, cap of two: full, but nothing beyond it.
            */
            var exact = await DarlingMcpDataTools.GetCollectionLog(dataSource, ServerName, 24, 2);
            var exactRoot = JsonDocument.Parse(exact).RootElement;
            Assert.Equal(2, exactRoot.GetProperty("run_count").GetInt32());
            Assert.False(exactRoot.GetProperty("truncated").GetBoolean());

            /* ── 5. an out-of-range cap is refused, not silently clamped ── */
            var tooBig = await DarlingMcpDataTools.GetCollectionLog(dataSource, ServerName, 24, 5000);
            Assert.Contains("exceeds maximum of", tooBig, StringComparison.Ordinal);
            Assert.Contains("1000", tooBig, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #3287's four additions, against a live store because every one of them is a property of the SQL: two
    /// filters applied BEFORE the cap, the ordering the duration filter implies, and the span the read
    /// actually reached.
    ///
    /// <para><b>The fixture is adversarial on purpose and would be worthless otherwise.</b> Every matching row
    /// is OLDER than every non-matching one, and there are more matches beyond the cap than a page holds. That
    /// is what makes an in-SQL filter and a post-cap filter give different answers: filtering the returned page
    /// would take the newest rows first, find only non-matching ones, and return the no-matches status. With a
    /// fixture whose matches happen to be recent, both implementations return the same rows and the test passes
    /// while proving nothing. The same shape is what makes <c>truncated</c> checkable — it has to mean "more
    /// rows MATCH than you were given", not "more rows were in the window".</para>
    ///
    /// <para>The three matching rows are also arranged so DURATION order and TIME order disagree, and so the
    /// slowest-first page ends on a row that is NOT its oldest. Both are deliberate: the first makes the
    /// ordering assertion able to fail, and the second is the only arrangement under which
    /// <c>rows.Min(collection_time)</c> and <c>rows[^1].collection_time</c> differ. A reach field taken from
    /// the last element is correct under time ordering and wrong under duration ordering, so a fixture that
    /// did not separate them would pin the bug.</para>
    /// </summary>
    [Fact]
    public async Task Filters_ApplyInSql_AheadOfTheCap_AndADurationFloorRanksByDuration_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live collection-log filter test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteFilterRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, FilterServerId, FilterServerName, ct);

            /*
                Every timestamp is captured ONCE, here, and reused by the seeds and the assertions.

                MinutesAgo() reads the clock, so calling it again at assertion time is a different instant:
                the seconds tick over between seeding and asserting and the two disagree by exactly one
                second. That is a flake, not a failure -- it passed locally and failed in CI on the same
                commit, which is the worst way to learn it. One capture, one value.
            */
            var t1 = MinutesAgo(1);
            var t2 = MinutesAgo(2);
            var t3 = MinutesAgo(3);
            var t10 = MinutesAgo(10);
            var t20 = MinutesAgo(20);
            var t30 = MinutesAgo(30);

            /* The three NON-matching rows are the newest in the window, so any implementation that caps
               before it filters sees only these. Cheap and recent, like the ordinary traffic that buries a
               slow run on a real fleet. */
            await SeedFilterAsync(connection, ct, "wait_stats", t1, 10);
            await SeedFilterAsync(connection, ct, "wait_stats", t2, 12);
            await SeedFilterAsync(connection, ct, "wait_stats", t3, 11);

            /* The three matching rows, all older, with duration deliberately NOT monotone in time and the
               heaviest one the OLDEST. */
            await SeedFilterAsync(connection, ct, "plan_correction", t30, 50_000);
            await SeedFilterAsync(connection, ct, "plan_correction", t10, 40_000);
            await SeedFilterAsync(connection, ct, "plan_correction", t20, 30_000);

            /* ── 1. collector_name filters in SQL, so the cap applies to the MATCHES ── */
            var byCollector = await DarlingMcpDataTools.GetCollectionLog(
                dataSource, FilterServerName, 24, 2, collector_name: "plan_correction");
            var collectorRoot = JsonDocument.Parse(byCollector).RootElement;

            /* Named explicitly because it is the mutation's own signature: filtering the RETURNED PAGE
               instead of the query takes the two newest rows, finds both are wait_stats, and returns the
               no-matches STATUS -- which has no run_count at all, so every assertion below would fail as a
               missing key rather than as the wrong number. Stated so the next person reads the diagnosis
               instead of a dictionary error. */
            Assert.True(
                collectorRoot.TryGetProperty("run_count", out _),
                "get_collection_log returned a status envelope, not a page. The filter was applied AFTER the "
                + "cap: the newest rows in this fixture are all non-matching, so a post-cap filter finds "
                + "nothing. It has to go into the SQL.");

            Assert.Equal(2, collectorRoot.GetProperty("run_count").GetInt32());
            Assert.True(collectorRoot.GetProperty("truncated").GetBoolean(),
                "Three rows match and two were returned, so truncated must describe the MATCHING set.");
            foreach (var run in collectorRoot.GetProperty("runs").EnumerateArray())
                Assert.Equal("plan_correction", run.GetProperty("collector").GetString());

            /* The filter that produced the page rides the page, so run_count above is readable. */
            Assert.Equal("plan_correction", collectorRoot.GetProperty("collector_name").GetString());

            /* No floor, so the order is unchanged: newest first among the matches. */
            Assert.Equal("collection_time_desc", collectorRoot.GetProperty("order").GetString());
            Assert.Equal(
                t10,
                collectorRoot.GetProperty("runs")[0].GetProperty("collection_time").GetDateTime());

            /* ── 2. min_duration_ms filters in SQL AND ranks by duration ── */
            var byDuration = await DarlingMcpDataTools.GetCollectionLog(
                dataSource, FilterServerName, 24, 2, min_duration_ms: 20_000);
            var durationRoot = JsonDocument.Parse(byDuration).RootElement;

            Assert.Equal(2, durationRoot.GetProperty("run_count").GetInt32());
            Assert.True(durationRoot.GetProperty("truncated").GetBoolean());
            Assert.Equal("duration_ms_desc", durationRoot.GetProperty("order").GetString());

            /*
                The assertion the whole issue turns on. Newest-first ordering over the same three matches
                would lead with the 40,000 ms run at -10m and never reach the 50,000 ms one, which is exactly
                how a 114,331 ms run stayed invisible behind a page whose maximum was 19,064 ms. Slowest-first
                puts the heaviest run first even though it is the oldest.
            */
            var ranked = durationRoot.GetProperty("runs");
            Assert.Equal(50_000, ranked[0].GetProperty("duration_ms").GetDouble());
            Assert.Equal(40_000, ranked[1].GetProperty("duration_ms").GetDouble());

            /* ── 3. the reach fields describe the page, not the request ── */
            var reach = await DarlingMcpDataTools.GetCollectionLog(
                dataSource, FilterServerName, 24, 3, min_duration_ms: 20_000);
            var reachRoot = JsonDocument.Parse(reach).RootElement;

            /* Still the span REQUESTED, under its shipped name. */
            Assert.Equal(24, reachRoot.GetProperty("hours_back").GetInt32());
            Assert.False(reachRoot.GetProperty("truncated").GetBoolean());

            /*
                And the span REACHED. Under slowest-first the page runs 50,000 (-30m), 40,000 (-10m),
                30,000 (-20m) -- so the last element is -20m and neither end of the array is an end of the
                window. rows[^1] would report -20m as the oldest and rows[0] would report -30m as the newest;
                both are wrong, and both are what a lane reusing the time-ordered idiom would write.
            */
            Assert.Equal(t30, reachRoot.GetProperty("oldest_returned_collection_time").GetDateTime());
            Assert.Equal(t10, reachRoot.GetProperty("newest_returned_collection_time").GetDateTime());

            /* The last row really is neither, so the two assertions above cannot be passing by coincidence. */
            var last = reachRoot.GetProperty("runs")[2].GetProperty("collection_time").GetDateTime();
            Assert.Equal(t20, last);

            /* ── 4. a filter that matches nothing says so, and does NOT call the window quiet ── */
            var noMatch = await DarlingMcpDataTools.GetCollectionLog(
                dataSource, FilterServerName, 24, 200, collector_name: "plan_corection");
            var noMatchRoot = JsonDocument.Parse(noMatch).RootElement;
            Assert.Equal("empty", noMatchRoot.GetProperty("status").GetString());
            var noMatchText = noMatchRoot.GetProperty("message").GetString()!;

            /* The filter is named back, so a typo is diagnosable from the answer. */
            Assert.Contains("plan_corection", noMatchText, StringComparison.Ordinal);

            /*
                And the two sentences this read already had must NOT be reachable here. Six rows sit in this
                window, so "genuinely quiet" would be false and "widen hours_back" would send the caller the
                wrong way -- a filtered read has not looked at the window and cannot describe it.
            */
            Assert.DoesNotContain("genuinely quiet", noMatchText, StringComparison.Ordinal);
            Assert.DoesNotContain("EVER", noMatchText, StringComparison.Ordinal);

            /* Positive control for those two negatives: the unfiltered read on this SAME server and window
               DOES reach the data path, so the assertions above are not passing against a server that has
               nothing. Without this, a broken fixture would satisfy every negative in this block. */
            var unfiltered = await DarlingMcpDataTools.GetCollectionLog(dataSource, FilterServerName, 24, 200);
            Assert.Equal(6, JsonDocument.Parse(unfiltered).RootElement.GetProperty("run_count").GetInt32());

            /* ── 5. a floor above every run is the same honest nothing, not a quiet window ── */
            var tooHigh = await DarlingMcpDataTools.GetCollectionLog(
                dataSource, FilterServerName, 24, 200, min_duration_ms: 999_999);
            var tooHighRoot = JsonDocument.Parse(tooHigh).RootElement;
            Assert.Equal("empty", tooHighRoot.GetProperty("status").GetString());
            Assert.DoesNotContain("genuinely quiet", tooHighRoot.GetProperty("message").GetString()!, StringComparison.Ordinal);

            /* ── 6. a floor of ZERO is a real request, not an absent one ── */
            var floorZero = await DarlingMcpDataTools.GetCollectionLog(
                dataSource, FilterServerName, 24, 200, min_duration_ms: 0);
            var zeroRoot = JsonDocument.Parse(floorZero).RootElement;
            Assert.Equal(6, zeroRoot.GetProperty("run_count").GetInt32());

            /* Every row admitted AND ranked by cost -- which is the only way to ask for that. */
            Assert.Equal("duration_ms_desc", zeroRoot.GetProperty("order").GetString());
            Assert.Equal(50_000, zeroRoot.GetProperty("runs")[0].GetProperty("duration_ms").GetDouble());

            /* ── 7. a negative floor is REFUSED, not read as "no floor" ── */
            var negative = await DarlingMcpDataTools.GetCollectionLog(
                dataSource, FilterServerName, 24, 200, min_duration_ms: -1);
            Assert.Contains("cannot be negative", negative, StringComparison.Ordinal);

            /* It is an error string, not a payload: silently accepting it would hand back a duration-ranked
               full page with nothing to say the filter did not apply. */
            Assert.DoesNotContain("\"runs\"", negative, StringComparison.Ordinal);

            /* ── 8. a NON-FINITE floor is refused too, and a negative test cannot see one ── */
            /*
                Every comparison against NaN is false and +Infinity < 0 is false, so both passed the
                original range check and came back as an empty, duration-RANKED page reporting
                "matched min_duration_ms NaN". All of them are reachable through /api/read: TryParse under
                NumberStyles.Float accepts the named literals AND accepts an oversized number like 1e400,
                which overflows to +Infinity rather than failing. Review found this after the lane had
                reasoned -- wrongly -- that no real surface could deliver one.
            */
            foreach (var unusable in new[] { double.NaN, double.PositiveInfinity, double.NegativeInfinity })
            {
                var refused = await DarlingMcpDataTools.GetCollectionLog(
                    dataSource, FilterServerName, 24, 200, min_duration_ms: unusable);

                Assert.Contains("must be a finite number", refused, StringComparison.Ordinal);
                Assert.DoesNotContain("\"runs\"", refused, StringComparison.Ordinal);

                /* And NOT the empty status: an unusable floor is a refusal, not a miss. */
                Assert.DoesNotContain("\"empty\"", refused, StringComparison.Ordinal);

                /* And not the no-matches MESSAGE either, which names the floor it applied and so reads as a
                   fact about the window. Asserted beside the status token because the two can drift: the
                   status is the branch, this sentence is what the caller actually reads. */
                Assert.DoesNotContain("matched min_duration_ms", refused, StringComparison.Ordinal);
            }

            /* Positive control for those negatives: a FINITE floor on the same read still returns a page,
               so the three assertions above are not passing against a read that refuses everything. */
            var finite = await DarlingMcpDataTools.GetCollectionLog(
                dataSource, FilterServerName, 24, 200, min_duration_ms: 20_000);
            Assert.Contains("\"runs\"", finite, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteFilterRowsAsync(cleanup, cleanupCt));
        }
    }

    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static DateTime HoursAgo(int hours) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-hours));

    private static async Task SeedAsync(
        NpgsqlConnection connection, CancellationToken ct, string collector, DateTime collectionTimeUtc) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            CollectionIdGenerator.Next(), ServerId, ServerName, collector,
            DarlingMcpTestData.Naive(collectionTimeUtc), 100, "SUCCESS", null, 10, 80, 20);

    /// <summary>The filter fixture's own sentinel, so its six rows cannot reach the empty-kinds test above
    /// (which asserts on a server that has never collected, a state any stray row destroys).</summary>
    private const int FilterServerId = -949553;

    /// <inheritdoc cref="FilterServerId"/>
    private const string FilterServerName = "collection-log-filters";

    private static async Task SeedFilterAsync(
        NpgsqlConnection connection, CancellationToken ct, string collector, DateTime collectionTimeUtc, double durationMs) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            CollectionIdGenerator.Next(), FilterServerId, FilterServerName, collector,
            DarlingMcpTestData.Naive(collectionTimeUtc), durationMs, "SUCCESS", null, 10, durationMs * 0.8, durationMs * 0.2);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }

    private static async Task DeleteFilterRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", FilterServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", FilterServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", FilterServerId);
    }
}
