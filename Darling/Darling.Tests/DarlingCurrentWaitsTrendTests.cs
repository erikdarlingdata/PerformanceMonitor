/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// get_current_waits_trend (#2484): the viewer's Current Waits tab, which had no endpoint on either
/// surface. get_waiting_tasks returns the snapshot rows and can never answer "was this worse an hour ago",
/// which is the question that decides whether anything is actually wrong.
///
/// <para>The assertion that carries the most weight is the empty one, and for a sharper reason than the
/// collection log's. Here the WRONG answer is the reassuring one: "nothing was waiting" reads as an
/// all-clear and a caller who believes it stops looking. If the collector has never sampled the server
/// there is nothing to be clear about, and the read has to say so.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingCurrentWaitsTrendTests
{
    private const int ServerId = -949553;
    private const string ServerName = "current-waits-trend";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AnAllClear_AndNeverSampled_AreDifferentAnswers_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live current-waits trend test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            /* #7010: the server was registered days ago, so a window of a few hours is inside what the store can cover (the window floor of a server with no collection log runs is its registration time). */
            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET created_date = $2 WHERE server_id = $1", ServerId, DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-3)));

            /* ── never sampled: NOT an all-clear, and it must refuse to read as one ── */
            var never = await DarlingMcpDataTools.GetCurrentWaitsTrend(dataSource, ServerName, 4, null);
            var neverDoc = JsonDocument.Parse(never);
            Assert.Equal("unavailable", neverDoc.RootElement.GetProperty("status").GetString());
            var neverText = neverDoc.RootElement.GetProperty("message").GetString()!;
            Assert.Contains("NOT an all-clear", neverText, StringComparison.Ordinal);
            Assert.Contains("EVER", neverText, StringComparison.Ordinal);

            /* ── sampled, but everything outside the window: a genuine all-clear ── */
            await SeedWaitAsync(connection, ct, HoursAgo(48), "LCK_M_X", 500, blockingSessionId: 99, database: "AppDb");
            await RecordRunAsync(connection, ct, "waiting_tasks");

            var clear = await DarlingMcpDataTools.GetCurrentWaitsTrend(dataSource, ServerName, 1, null);
            var clearDoc = JsonDocument.Parse(clear);
            Assert.Equal("empty", clearDoc.RootElement.GetProperty("status").GetString());
            var clearText = clearDoc.RootElement.GetProperty("message").GetString()!;
            Assert.Contains("genuine all-clear", clearText, StringComparison.Ordinal);

            /* Same zero rows as the case above, and it must NOT reach for the same word. */
            Assert.DoesNotContain("EVER", clearText, StringComparison.Ordinal);

            /* ── both series, in one payload ── */
            var t10 = MinutesAgo(10);
            for (var thread = 0; thread < 3; thread++)
            {
                await SeedWaitAsync(connection, ct, t10, "LCK_M_X", 500, blockingSessionId: 99, database: "AppDb");
            }
            await SeedWaitAsync(connection, ct, t10, "PAGEIOLATCH_SH", 250, blockingSessionId: 0, database: "AppDb");

            /* #7577: a parallel query's exchange threads wait on their OWN session (blocking_session_id = session_id). Four of them
               are not four blocked sessions, and a session blocked on several threads is one blocked session. */
            var t5 = MinutesAgo(5);
            for (var thread = 0; thread < 4; thread++)
            {
                await SeedWaitAsync(connection, ct, t5, "CXCONSUMER", 300, blockingSessionId: 55, database: "AppDb");
            }

            var hit = await DarlingMcpDataTools.GetCurrentWaitsTrend(dataSource, ServerName, 4, null);
            var root = JsonDocument.Parse(hit).RootElement;

            var waits = root.GetProperty("waiting_tasks");
            Assert.Equal(3, waits.GetArrayLength());

            /*
                Only the LCK row is blocked (blocking_session_id > 0). The PAGEIOLATCH row waits on IO and
                blocks on nothing, so it belongs to the wait series and NOT the blocked series -- which is
                the whole reason the two are read together rather than treated as one number.
            */
            var blocked = root.GetProperty("blocked_sessions");
            Assert.Equal(1, blocked.GetArrayLength());
            Assert.Equal("AppDb", blocked[0].GetProperty("database_name").GetString());
            Assert.Equal(1, blocked[0].GetProperty("blocked_count").GetInt64());

            /* ── the database filter narrows the blocked series and leaves the waits alone ── */
            await SeedWaitAsync(connection, ct, MinutesAgo(9), "LCK_M_S", 700, blockingSessionId: 77, database: "OtherDb");

            var filtered = await DarlingMcpDataTools.GetCurrentWaitsTrend(dataSource, ServerName, 4, "OtherDb");
            var filteredRoot = JsonDocument.Parse(filtered).RootElement;
            var filteredBlocked = filteredRoot.GetProperty("blocked_sessions");
            Assert.Equal(1, filteredBlocked.GetArrayLength());
            Assert.Equal("OtherDb", filteredBlocked[0].GetProperty("database_name").GetString());

            /* The wait series is server-wide and is deliberately NOT narrowed by the database filter. */
            Assert.Equal(4, filteredRoot.GetProperty("waiting_tasks").GetArrayLength());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #6453: a collector whose newest run completed clean and stored no rows is HEALTHY in get_collection_health, so the
    /// read must say <c>empty</c> (a true negative), not <c>unavailable</c>. A faulting collector keeps <c>unavailable</c>:
    /// the "check collection" pointer is right for it. An empty stall-probe window is <c>empty</c> too, never
    /// <c>not_collected</c>.
    /// </summary>
    [Fact]
    public async Task ACleanRunWithNoRows_IsEmpty_AndAFaultingRun_StaysUnavailable_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live current-waits trend test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            await SeedCollectionLogAsync(connection, ct, MinutesAgo(30), "SUCCESS", null);
            await SeedCollectionLogAsync(connection, ct, MinutesAgo(5), "SUCCESS", null);

            var healthy = JsonDocument.Parse(await DarlingMcpDataTools.GetCurrentWaitsTrend(dataSource, ServerName, 4, null));
            Assert.Equal("empty", healthy.RootElement.GetProperty("status").GetString());
            var healthyText = healthy.RootElement.GetProperty("message").GetString()!;
            Assert.Contains("true negative", healthyText, StringComparison.Ordinal);
            Assert.DoesNotContain("NOT an all-clear", healthyText, StringComparison.Ordinal);

            /* The newest run faulted: the collector is not healthy, and the read goes back to pointing at collection health. */
            await SeedCollectionLogAsync(connection, ct, MinutesAgo(1), "ERROR", "login failed");
            var faulted = JsonDocument.Parse(await DarlingMcpDataTools.GetCurrentWaitsTrend(dataSource, ServerName, 4, null));
            Assert.Equal("unavailable", faulted.RootElement.GetProperty("status").GetString());

            /* A server with no probes in the window: empty, and the window statement is not a not_collected claim. */
            var probes = JsonDocument.Parse(await DarlingMcpStallProbeTools.GetCollectorStallProbes(dataSource, ServerName, 7));
            Assert.Equal("empty", probes.RootElement.GetProperty("status").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #7877, #7904: two hours of one-minute collections read with a 30-hour look-back. The read used to return one row per collection
    /// per series (120 each, over the point budget); now each point is a bucket carrying its width and collection count, and its figure is
    /// still the figure of ONE collection (the average over the bucket), never the bucket's sum.
    /// </summary>
    [Fact]
    public async Task ALongWindow_IsBucketed_EachPointIsPerCollection_AndNamesItsWidthAndCount_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live current-waits trend test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET created_date = $2 WHERE server_id = $1", ServerId, DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-3)));

            const int collections = 120;
            for (var minute = 0; minute < collections; minute++)
            {
                await SeedWaitAsync(connection, ct, MinutesAgo(5 + minute), "LCK_M_X", 500, blockingSessionId: 99, database: "AppDb");
            }

            var root = JsonDocument.Parse(await DarlingMcpDataTools.GetCurrentWaitsTrend(dataSource, ServerName, 30, null)).RootElement;

            /* The width and its meaning ride the payload, and the answer stays inside the point budget. */
            var bucketMinutes = root.GetProperty("bucket_minutes").GetInt32();
            Assert.True(bucketMinutes > 1, "a 30-hour window with two series cannot be served one collection per point");
            Assert.Contains("ONE collection", root.GetProperty("note").GetString(), StringComparison.Ordinal);
            var waits = root.GetProperty("waiting_tasks").EnumerateArray().ToArray();
            var blocked = root.GetProperty("blocked_sessions").EnumerateArray().ToArray();
            Assert.NotEmpty(waits);
            Assert.NotEmpty(blocked);
            Assert.True(waits.Length + blocked.Length <= TrendBuckets.McpPointBudget, "the two series together stay within the point budget");

            /* Every point is the figure of one collection, whatever the width: 500 ms, never 500 times the collections it merged. */
            Assert.All(waits, w =>
            {
                Assert.Equal(500, w.GetProperty("total_wait_ms").GetInt64());
                Assert.Equal(500, w.GetProperty("peak_wait_ms").GetInt64());
            });
            Assert.All(blocked, b =>
            {
                Assert.Equal(1, b.GetProperty("blocked_count").GetDouble());
                Assert.Equal(1, b.GetProperty("peak_blocked_count").GetInt64());
            });
            Assert.True(waits.Max(w => w.GetProperty("collection_count").GetInt64()) > 1, "a bucket says how many collections it covers");
            Assert.Equal(collections, waits.Sum(w => w.GetProperty("collection_count").GetInt64()));

            /* A width the caller names is refused, not widened, when it would put more points on the wire than the read's cap. */
            var narrow = await DarlingMcpDataTools.GetCurrentWaitsTrend(dataSource, ServerName, 30, null, null, 1);
            Assert.Contains("bucket_minutes", narrow, StringComparison.Ordinal);
            Assert.DoesNotContain("waiting_tasks", narrow, StringComparison.Ordinal);

            /* A window short enough to need no bucketing serves one point per collection, as before. */
            var shortWindow = JsonDocument.Parse(await DarlingMcpDataTools.GetCurrentWaitsTrend(dataSource, ServerName, 1, null)).RootElement;
            Assert.Equal(1, shortWindow.GetProperty("bucket_minutes").GetInt32());
            Assert.Equal(1, shortWindow.GetProperty("waiting_tasks")[0].GetProperty("collection_count").GetInt64());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #7974: a bucket's mean is over EVERY collection that ran in it, a collection with nothing waiting or blocked counting 0. A wait type and a
    /// database seen in 1 of 60 one-minute collections of an hour read about 1/60, not the 1.0 and the whole wait of one seen in all 60; a steady
    /// series still reads its full figure. The second hour holds only the sporadic series (the other 59 collections ran and wrote no row at all),
    /// so its 60 comes from the collection log and cannot be inferred from any other series' rows. collection_count is the 60 runs.
    /// </summary>
    [Fact]
    public async Task ABucketMean_CoversEveryCollectionThatRan_SoASporadicSeriesReadsAFractionAndASteadyOneReadsItsFigure_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live current-waits trend test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET created_date = $2 WHERE server_id = $1", ServerId, DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-3)));

            var thisHour = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            thisHour = new DateTime(thisHour.Year, thisHour.Month, thisHour.Day, thisHour.Hour, 0, 0, DateTimeKind.Utc);
            var steadyHour = thisHour.AddHours(-3);
            var quietHour = thisHour.AddHours(-2);

            for (var minute = 0; minute < 60; minute++)
            {
                await SeedCollectionLogAsync(connection, ct, steadyHour.AddMinutes(minute), "SUCCESS", null);
                await SeedCollectionLogAsync(connection, ct, quietHour.AddMinutes(minute), "SUCCESS", null);
                await SeedWaitAsync(connection, ct, steadyHour.AddMinutes(minute), "LCK_M_X", 500, blockingSessionId: 99, database: "SteadyDb");
            }

            /* The sporadic series: one collection of the first hour and one of the second. */
            await SeedWaitAsync(connection, ct, steadyHour.AddMinutes(7), "PAGEIOLATCH_SH", 600, blockingSessionId: 98, database: "SporadicDb");
            await SeedWaitAsync(connection, ct, quietHour.AddMinutes(30), "PAGEIOLATCH_SH", 600, blockingSessionId: 98, database: "SporadicDb");

            var root = JsonDocument.Parse(await DarlingMcpDataTools.GetCurrentWaitsTrend(dataSource, ServerName, 6, null, null, 60)).RootElement;
            Assert.Equal(60, root.GetProperty("bucket_minutes").GetInt32());

            var steadyStamp = steadyHour.ToString("yyyy-MM-ddTHH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
            var quietStamp = quietHour.ToString("yyyy-MM-ddTHH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
            JsonElement Point(string series, string key, string name, string stamp) => root.GetProperty(series).EnumerateArray()
                .Single(p => p.GetProperty(key).GetString() == name && p.GetProperty("collection_time").GetString()!.StartsWith(stamp, StringComparison.Ordinal));

            var steadyBlocked = Point("blocked_sessions", "database_name", "SteadyDb", steadyStamp);
            Assert.Equal(1.0, steadyBlocked.GetProperty("blocked_count").GetDouble());
            Assert.Equal(60, steadyBlocked.GetProperty("collection_count").GetInt64());
            var steadyWait = Point("waiting_tasks", "wait_type", "LCK_M_X", steadyStamp);
            Assert.Equal(500, steadyWait.GetProperty("total_wait_ms").GetInt64());

            var sporadicBlocked = Point("blocked_sessions", "database_name", "SporadicDb", steadyStamp);
            Assert.Equal(1.0 / 60, sporadicBlocked.GetProperty("blocked_count").GetDouble(), 3);
            Assert.True(sporadicBlocked.GetProperty("blocked_count").GetDouble() > 0, "a database blocked in 1 of 60 collections must not round to 0");
            Assert.Equal(1, sporadicBlocked.GetProperty("peak_blocked_count").GetInt64());
            Assert.Equal(60, sporadicBlocked.GetProperty("collection_count").GetInt64());
            var sporadicWait = Point("waiting_tasks", "wait_type", "PAGEIOLATCH_SH", steadyStamp);
            Assert.Equal(10, sporadicWait.GetProperty("total_wait_ms").GetInt64());
            Assert.Equal(600, sporadicWait.GetProperty("peak_wait_ms").GetInt64());
            Assert.Equal(60, sporadicWait.GetProperty("collection_count").GetInt64());

            /* The hour in which the other 59 collections ran and wrote no row: still 60 runs, still 1/60. */
            var quietBlocked = Point("blocked_sessions", "database_name", "SporadicDb", quietStamp);
            Assert.Equal(1.0 / 60, quietBlocked.GetProperty("blocked_count").GetDouble(), 3);
            Assert.Equal(60, quietBlocked.GetProperty("collection_count").GetInt64());
            var quietWait = Point("waiting_tasks", "wait_type", "PAGEIOLATCH_SH", quietStamp);
            Assert.Equal(10, quietWait.GetProperty("total_wait_ms").GetInt64());
            Assert.Equal(60, quietWait.GetProperty("collection_count").GetInt64());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #7877 review: this read has no hours_back ceiling, and the shared ladder gives up at its widest rung (one day). A 30-day window holding
    /// 8 wait types and a database is 31 day-buckets x 9 series = 279 points: over the 200 target, inside the 1,500 cap, so the widest rung is
    /// served and the note says the target was missed. With 50 wait types it is 1,581 points, over the cap at every width, so both the automatic
    /// path and an explicit width are refused, and the refusal names the window, never a width that is itself refused.
    /// </summary>
    [Fact]
    public async Task AWindowNoWidthFits_IsRefusedOnBothPaths_AndOneThatOnlyMissesTheTargetServesTheWidestWithAnHonestNote_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live current-waits trend test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            for (var type = 0; type < 8; type++)
            {
                await SeedWaitAsync(connection, ct, MinutesAgo(5), $"WT_{type:000}", 500, blockingSessionId: 99, database: "AppDb");
            }

            /* 9 series x 31 day-buckets: over the 200 target, inside the cap. */
            var missed = JsonDocument.Parse(await DarlingMcpDataTools.GetCurrentWaitsTrend(dataSource, ServerName, 720, null)).RootElement;
            Assert.Equal(1440, missed.GetProperty("bucket_minutes").GetInt32());
            var points = missed.GetProperty("waiting_tasks").GetArrayLength() + missed.GetProperty("blocked_sessions").GetArrayLength();
            Assert.Equal(9, points);
            var note = missed.GetProperty("aggregate_note").GetString()!;
            Assert.Contains("widest bucket", note, StringComparison.Ordinal);
            Assert.Contains("could not bring this answer near 200 points", note, StringComparison.Ordinal);
            Assert.Contains("it serves 279", note, StringComparison.Ordinal);
            Assert.DoesNotContain("chosen to keep this answer near", note, StringComparison.Ordinal);

            /* The same window at the width the caller names is served, and says the width is theirs. */
            var named = JsonDocument.Parse(await DarlingMcpDataTools.GetCurrentWaitsTrend(dataSource, ServerName, 720, null, null, 1440)).RootElement;
            Assert.Equal("The width is the bucket_minutes you passed.", named.GetProperty("aggregate_note").GetString());

            for (var type = 8; type < 50; type++)
            {
                await SeedWaitAsync(connection, ct, MinutesAgo(5), $"WT_{type:000}", 500, blockingSessionId: 99, database: "AppDb");
            }

            /* 51 series x 31 buckets = 1,581 points: over the 1,500 cap at the widest width, so nothing serves it. */
            foreach (var requested in new int?[] { null, 1440, 60 })
            {
                var refused = await DarlingMcpDataTools.GetCurrentWaitsTrend(dataSource, ServerName, 720, null, null, requested);
                var root = JsonDocument.Parse(refused).RootElement;
                Assert.Equal("invalid", root.GetProperty("status").GetString());
                Assert.Equal("hours_back", root.GetProperty("hints").GetProperty("parameter").GetString());
                var message = root.GetProperty("message").GetString()!;
                Assert.Contains("even at the widest bucket", message, StringComparison.Ordinal);
                Assert.Contains("1581 points", message, StringComparison.Ordinal);
                Assert.Contains("Narrow hours_back, move as_of, or filter database_name", message, StringComparison.Ordinal);
                Assert.DoesNotContain("Use bucket_minutes", message, StringComparison.Ordinal);
                Assert.DoesNotContain("waiting_tasks", refused, StringComparison.Ordinal);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task SeedCollectionLogAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, string status, string? errorMessage) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            CollectionIdGenerator.Next(), ServerId, ServerName, "waiting_tasks",
            DarlingMcpTestData.Naive(collectionTimeUtc), 100, status, errorMessage, 0);

    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static DateTime HoursAgo(int hours) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-hours));

    private static async Task SeedWaitAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc,
        string waitType, long waitMs, int blockingSessionId, string database) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO waiting_tasks
    (collection_id, collection_time, server_id, server_name, session_id, wait_type,
     wait_duration_ms, blocking_session_id, resource_description, database_name)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc),
            ServerId, ServerName, 55, waitType, waitMs, blockingSessionId, null, database);

    /// <summary>
    /// #7010: one clean collector run for the table's collector half an hour ago. A running collector logs a run every cycle, and the window floor counts a
    /// server in a window when it holds a row there or the collection log records a run there: a sparse table's quiet window has the runs and no rows.
    /// </summary>
    private static Task RecordRunAsync(NpgsqlConnection connection, CancellationToken ct, string table) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
VALUES ($1,$2,$3,$4,$5,100,'SUCCESS',0)",
            CollectionIdGenerator.Next(), ServerId, ServerName, CollectorCatalog.All.First(c => c.TargetTable == table).Name,
            DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-2)));

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM waiting_tasks WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
