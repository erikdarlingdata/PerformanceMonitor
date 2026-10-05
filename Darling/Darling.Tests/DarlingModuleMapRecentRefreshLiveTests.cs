/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The module map's refresh watermark and the hourly incremental refresh that advances it (#4605). The watermark
/// is the largest <c>procedure_stats.collection_time</c> a refresh has read, written in the same statement as the
/// map upsert; the hourly refresh reads from ten minutes behind it so a row committed late is still picked up.
/// Every fact mints its own scratch store, seeds at a fixed past anchor, and passes the clock explicitly, so none
/// reads the wall clock and none touches another class's rows.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingModuleMapRecentRefreshLiveTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the module map's incremental refresh pins (each mints its own scratch database).";

    private const string Server = "modmap-recent";

    private static readonly DateTime Anchor = new(2026, 1, 10, 12, 0, 0, DateTimeKind.Unspecified);

    private static long s_collectionId = 7_000_000L;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task RunLiveAsync(Func<NpgsqlConnection, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await DarlingModuleMap.EnsureTableAsync(connection, null, ct));
        var bodySucceeded = false;
        try
        {
            await body(connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    private static async Task InsertProcAsync(NpgsqlConnection c, DateTime t, string sqlHandle, string objectName, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            @"INSERT INTO collect.procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle)
VALUES ($1,$2,1,$3,'TestDb','dbo',$4,$5)", c);
        cmd.Parameters.AddWithValue(Interlocked.Increment(ref s_collectionId));
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = t });
        cmd.Parameters.AddWithValue(Server);
        cmd.Parameters.AddWithValue(objectName);
        cmd.Parameters.AddWithValue(sqlHandle);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection c, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        var value = await cmd.ExecuteScalarAsync(ct);
        return value is DBNull ? null : value;
    }

    private static async Task ExecAsync(NpgsqlConnection c, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> ObjectNameForAsync(NpgsqlConnection c, string sqlHandle, CancellationToken ct) =>
        (string?)await ScalarAsync(c, $"SELECT object_name FROM collect.module_map WHERE server_name = '{Server}' AND sql_handle = '{sqlHandle}'", ct);

    /// <summary>The second argument of every refresh below: a clock a fixed hour past the anchor, far inside the
    /// two-day lookback of every seed.</summary>
    private static DateTime Now => Anchor.AddHours(1);

    [Fact]
    public async Task TheWatermark_AdvancesWithEachRefresh_AndNeverMovesBack()
    {
        await RunLiveAsync(async (connection, ct) =>
        {
            await InsertProcAsync(connection, Anchor, "0xRECENT_A", "alpha", ct);
            Assert.Equal(1, await DarlingModuleMap.RefreshRecentAsync(connection, null, Now, ct));
            Assert.Equal(Anchor, await DarlingModuleMap.ReadWatermarkAsync(connection, ct));

            /* A newer row moves it forward. */
            await InsertProcAsync(connection, Anchor.AddMinutes(30), "0xRECENT_A", "alpha", ct);
            await DarlingModuleMap.RefreshRecentAsync(connection, null, Now, ct);
            Assert.Equal(Anchor.AddMinutes(30), await DarlingModuleMap.ReadWatermarkAsync(connection, ct));

            /* The newest source rows age out and only an older one is left inside the read window. That run's
               own maximum is BEHIND the stored watermark; the stored one must hold. */
            await ExecAsync(connection, "DELETE FROM collect.procedure_stats", ct);
            await InsertProcAsync(connection, Anchor.AddMinutes(25), "0xRECENT_A", "alpha", ct);
            await DarlingModuleMap.RefreshRecentAsync(connection, null, Now, ct);
            Assert.Equal(Anchor.AddMinutes(30), await DarlingModuleMap.ReadWatermarkAsync(connection, ct));

            /* The daily refresh obeys the same rule: its two-day window holds only the older row. */
            await DarlingModuleMap.RefreshAsync(connection, null, ct);
            Assert.Equal(Anchor.AddMinutes(30), await DarlingModuleMap.ReadWatermarkAsync(connection, ct));
        });
    }

    [Fact]
    public async Task TheDailyRefresh_AdvancesTheWatermarkToo()
    {
        await RunLiveAsync(async (connection, ct) =>
        {
            /* The daily statement reads now() - 2 days, so this seed has to be recent in wall-clock terms. */
            var recent = DateTime.SpecifyKind(DateTime.UtcNow.AddHours(-3), DateTimeKind.Unspecified);
            recent = new DateTime(recent.Ticks - (recent.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);
            await InsertProcAsync(connection, recent, "0xRECENT_D", "daily", ct);
            Assert.Null(await DarlingModuleMap.ReadWatermarkAsync(connection, ct));
            Assert.Equal(1, await DarlingModuleMap.RefreshAsync(connection, null, ct));
            Assert.Equal(recent, await DarlingModuleMap.ReadWatermarkAsync(connection, ct));
        });
    }

    [Fact]
    public async Task TwoConsecutiveRefreshes_LeaveNoGap_ARowCommittedBehindTheWatermarkIsPickedUp()
    {
        await RunLiveAsync(async (connection, ct) =>
        {
            await InsertProcAsync(connection, Anchor, "0xRECENT_FIRST", "first", ct);
            await DarlingModuleMap.RefreshRecentAsync(connection, null, Now, ct);
            Assert.Equal(Anchor, await DarlingModuleMap.ReadWatermarkAsync(connection, ct));

            /* A collection stamped five minutes BEFORE the watermark that only commits now: a reader that began
               exactly at the watermark would never see it. */
            await InsertProcAsync(connection, Anchor.AddMinutes(-5), "0xRECENT_LATE", "late", ct);
            Assert.Null(await ObjectNameForAsync(connection, "0xRECENT_LATE", ct));
            await DarlingModuleMap.RefreshRecentAsync(connection, null, Now, ct);
            Assert.Equal("late", await ObjectNameForAsync(connection, "0xRECENT_LATE", ct));
            Assert.Equal("first", await ObjectNameForAsync(connection, "0xRECENT_FIRST", ct));
            Assert.Equal(Anchor, await DarlingModuleMap.ReadWatermarkAsync(connection, ct));
        });
    }

    [Fact]
    public async Task AModuleRenamedAfterTheFirstRefresh_IsRenamedInTheMapAfterTheSecond()
    {
        await RunLiveAsync(async (connection, ct) =>
        {
            await InsertProcAsync(connection, Anchor, "0xRECENT_R", "before_rename", ct);
            await DarlingModuleMap.RefreshRecentAsync(connection, null, Now, ct);
            Assert.Equal("before_rename", await ObjectNameForAsync(connection, "0xRECENT_R", ct));

            await InsertProcAsync(connection, Anchor.AddMinutes(20), "0xRECENT_R", "after_rename", ct);
            await DarlingModuleMap.RefreshRecentAsync(connection, null, Now, ct);
            Assert.Equal("after_rename", await ObjectNameForAsync(connection, "0xRECENT_R", ct));
            Assert.Equal(Anchor.AddMinutes(20), await ScalarAsync(connection, $"SELECT last_seen FROM collect.module_map WHERE server_name = '{Server}' AND sql_handle = '0xRECENT_R'", ct));
        });
    }

    [Fact]
    public async Task AnEmptySlice_LeavesTheWatermarkAndItsStampUnchanged()
    {
        await RunLiveAsync(async (connection, ct) =>
        {
            await InsertProcAsync(connection, Anchor, "0xRECENT_E", "empty", ct);
            await DarlingModuleMap.RefreshRecentAsync(connection, null, Now, ct);
            var stamp = await ScalarAsync(connection, "SELECT refreshed_at FROM collect.module_map_state WHERE id = 1", ct);
            Assert.NotNull(stamp);

            await ExecAsync(connection, "DELETE FROM collect.procedure_stats", ct);
            Assert.Equal(0, await DarlingModuleMap.RefreshRecentAsync(connection, null, Now, ct));
            Assert.Equal(Anchor, await DarlingModuleMap.ReadWatermarkAsync(connection, ct));
            Assert.Equal(stamp, await ScalarAsync(connection, "SELECT refreshed_at FROM collect.module_map_state WHERE id = 1", ct));
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM collect.module_map_state", ct));
        });
    }

    [Fact]
    public async Task TheWatermark_ReadsNull_WithNoStateRow_AndWhenTheTableIsGone()
    {
        await RunLiveAsync(async (connection, ct) =>
        {
            Assert.Null(await DarlingModuleMap.ReadWatermarkAsync(connection, ct));

            /* No watermark: the hourly refresh falls back to the two-day floor and writes the first row. */
            await InsertProcAsync(connection, Anchor.AddHours(-47), "0xRECENT_OLD", "old", ct);
            await InsertProcAsync(connection, Anchor.AddHours(-49), "0xRECENT_TOOOLD", "too_old", ct);
            await DarlingModuleMap.RefreshRecentAsync(connection, null, Now, ct);
            Assert.Equal("old", await ObjectNameForAsync(connection, "0xRECENT_OLD", ct));
            Assert.Null(await ObjectNameForAsync(connection, "0xRECENT_TOOOLD", ct));

            await ExecAsync(connection, "DROP TABLE collect.module_map_state", ct);
            Assert.Null(await DarlingModuleMap.ReadWatermarkAsync(connection, ct));
            /* The connection survives the failed read, and a refresh without the table warns instead of throwing. */
            Assert.Equal(0, await DarlingModuleMap.RefreshRecentAsync(connection, null, Now, ct));
        });
    }

    [Fact]
    public void TheLowerBound_IsTheWatermarkLessTheSlack_NeverOlderThanTheLookback_AndNaive()
    {
        var now = Anchor;
        var floor = now - DarlingModuleMap.MaxLookback;

        Assert.Equal(floor, DarlingModuleMap.SinceFor(null, now));
        Assert.Equal(now.AddMinutes(-70), DarlingModuleMap.SinceFor(now.AddMinutes(-60), now));
        Assert.Equal(floor, DarlingModuleMap.SinceFor(now.AddDays(-5), now));
        Assert.Equal(floor, DarlingModuleMap.SinceFor(floor.AddMinutes(5), now));
        Assert.Equal(TimeSpan.FromMinutes(10), DarlingModuleMap.WatermarkSlack);
        Assert.Equal(TimeSpan.FromDays(2), DarlingModuleMap.MaxLookback);

        foreach (var since in new[] { DarlingModuleMap.SinceFor(null, DateTime.UtcNow), DarlingModuleMap.SinceFor(DateTime.UtcNow, DateTime.UtcNow) })
        {
            Assert.Equal(DateTimeKind.Unspecified, since.Kind);
        }
    }

    [Fact]
    public void TheTenant_IsTheLastAwaitInTheStoreMaintenanceTick_WithItsOwnCatchAll()
    {
        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(worker);
        var tick = Body(code, "private async Task RunStoreMaintenanceTickAsync(CancellationToken stoppingToken)");
        Assert.False(string.IsNullOrEmpty(tick), "could not locate RunStoreMaintenanceTickAsync");

        const string Refresh = "await RefreshModuleMapRecentAsync(stoppingToken);";
        var refreshAt = tick.IndexOf(Refresh, StringComparison.Ordinal);
        Assert.True(refreshAt > tick.IndexOf("await BuildQueryStoreTopDailyAsync(stoppingToken);", StringComparison.Ordinal), "the refresh is awaited after the summary builder");
        Assert.Equal(1, tick.Split(Refresh).Length - 1);
        Assert.Equal(1, code.Split(Refresh).Length - 1);
        Assert.True(string.IsNullOrWhiteSpace(tick[(refreshAt + Refresh.Length)..]), "nothing follows the refresh in the tick");

        var tenant = Body(code, "private async Task RefreshModuleMapRecentAsync(CancellationToken stoppingToken)");
        Assert.False(string.IsNullOrEmpty(tenant), "could not locate RefreshModuleMapRecentAsync");
        Assert.Contains("catch (Exception ex) when (ex is not OperationCanceledException)", tenant, StringComparison.Ordinal);
        Assert.Contains("DarlingModuleMap.RefreshRecentAsync(", tenant, StringComparison.Ordinal);
        Assert.DoesNotContain("throw", tenant, StringComparison.Ordinal);
    }

    private static string Body(string source, string signature)
    {
        var start = source.IndexOf(signature, StringComparison.Ordinal);
        if (start < 0)
        {
            return string.Empty;
        }

        var open = source.IndexOf('{', start);
        var depth = 0;
        for (var i = open; i < source.Length; i++)
        {
            if (source[i] == '{')
            {
                depth++;
            }
            else if (source[i] == '}' && --depth == 0)
            {
                return source[(open + 1)..i];
            }
        }

        return string.Empty;
    }
}
