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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5526: the fleet's PostgreSQL deadlock totals are computed one server at a time (a <c>LATERAL</c> per
/// server) instead of one <c>LAG</c> window over every server's rows, and they must come out IDENTICAL.
/// This runs the PREVIOUS query text (kept below as a literal, the way the issue quotes it) and the shipped
/// <see cref="DarlingFleetReader.FleetPgDeadlockSql"/> and <see cref="ViewerDataService.FleetTotalsSql"/>
/// against the same seed and compares them field by field.
///
/// <para><b>The seed is shaped to separate every plausible wrong rewrite.</b> One server's series has a
/// counter reset inside the window (110 → 113 → 5, a −108 that must clamp to zero and not subtract), a
/// stats reset (the post-reset rows carry a new <c>stats_reset</c>), a NULL-named shared-relation series, a
/// database that first appears mid-window, and a row BEFORE the window that a difference must not reach
/// back to (the window-bounded <c>LAG</c> gives the first in-window row a NULL difference), and a retried
/// write of one sample (two rows, same timestamp and value). A second
/// server has one row in the window and the same database name as the first (a read that lost the server
/// partition would difference across them; a rewrite that dropped one-row servers would lose its
/// <c>intervals</c> = 0 row). A third has a flat series and a rising one. A fourth has rows only outside the
/// window and must not appear at all.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class FleetPgDeadlockPerServerLiveTests
{
    /* NEGATIVE, per this family's convention (see FleetCardPostgresCpuLivePostgresTests). */
    private const int ResetServerId = -993_5261;
    private const int LonelyServerId = -993_5262;
    private const int FlatServerId = -993_5263;
    private const int OutsideServerId = -993_5264;

    private static readonly int[] SentinelIds = [ResetServerId, LonelyServerId, FlatServerId, OutsideServerId];

    /// <summary>The query as it stood before #5526: one window over every server's rows. Do not "fix" this
    /// copy; it is the reference the shipped text is compared against.</summary>
    private const string PreviousFleetPgDeadlockSql = @"
WITH sampled AS
(
    SELECT
        server_id,
        collection_time,
        deadlocks - LAG(deadlocks) OVER (PARTITION BY server_id, database_name ORDER BY collection_time) AS raw_delta
    FROM pg_database_stats
    WHERE collection_time >= $1
    AND   collection_time <= $2
)
SELECT
    server_id,
    CAST(coalesce(SUM(GREATEST(raw_delta, 0)), 0) AS bigint) AS cnt,
    MAX(collection_time) FILTER (WHERE raw_delta > 0) AS last_seen,
    CAST(count(raw_delta) AS bigint) AS intervals
FROM sampled
GROUP BY server_id";

    /// <summary>The previous fleet-totals PostgreSQL arm, on its own.</summary>
    private const string PreviousViewerPgTotalSql = @"
SELECT CAST(COALESCE(SUM(GREATEST(sampled.raw_delta, 0)), 0) AS bigint)
FROM
(
    SELECT deadlocks - LAG(deadlocks) OVER (PARTITION BY server_id, database_name ORDER BY collection_time) AS raw_delta
    FROM pg_database_stats
    WHERE collection_time >= $1
    AND   collection_time <= $2
) AS sampled";

    private sealed record Row(int ServerId, long Count, DateTime? LastSeen, long Intervals);

    [Fact]
    public async Task TheShippedPerServerReads_GiveTheSameTotalsAsThePreviousFleetWideWindow()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fleet deadlock-total comparison.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteSentinelRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;
            var start = now.AddHours(-3);
            DateTime M(int minutesAgo) => now.AddMinutes(-minutesAgo);

            /* Server 1: counter reset + stats reset on "orders"; a NULL-named series; "billing" that starts
               mid-window; a row before the window the first in-window row must not difference against. */
            await InsertAsync(connection, ResetServerId, "orders", M(240), 100, null, ct);
            await InsertAsync(connection, ResetServerId, "orders", M(150), 110, null, ct);
            await InsertAsync(connection, ResetServerId, "orders", M(120), 110, null, ct);
            await InsertAsync(connection, ResetServerId, "orders", M(90), 113, null, ct);
            await InsertAsync(connection, ResetServerId, "orders", M(90), 113, null, ct);   // a retried write of the same sample
            await InsertAsync(connection, ResetServerId, "orders", M(60), 5, M(61), ct);    // reset: -108 clamps to 0
            await InsertAsync(connection, ResetServerId, "orders", M(30), 6, M(61), ct);    // +1 survives the reset
            await InsertAsync(connection, ResetServerId, "orders", M(10), 6, M(61), ct);
            await InsertAsync(connection, ResetServerId, null, M(150), 9, null, ct);
            await InsertAsync(connection, ResetServerId, null, M(100), 9, null, ct);
            await InsertAsync(connection, ResetServerId, null, M(50), 12, null, ct);
            await InsertAsync(connection, ResetServerId, "billing", M(40), 0, null, ct);
            await InsertAsync(connection, ResetServerId, "billing", M(20), 2, null, ct);

            /* Server 2: ONE row in the window, the same database name as server 1, plus older rows. */
            await InsertAsync(connection, LonelyServerId, "orders", M(300), 7, null, ct);
            await InsertAsync(connection, LonelyServerId, "orders", M(45), 50, null, ct);

            /* Server 3: a flat series and a rising one, in the same two minutes as each other. */
            await InsertAsync(connection, FlatServerId, "app", M(80), 12, null, ct);
            await InsertAsync(connection, FlatServerId, "app", M(70), 12, null, ct);
            await InsertAsync(connection, FlatServerId, "app", M(60), 12, null, ct);
            await InsertAsync(connection, FlatServerId, "reports", M(80), 1000, null, ct);
            await InsertAsync(connection, FlatServerId, "reports", M(70), 1002, null, ct);
            await InsertAsync(connection, FlatServerId, "reports", M(60), 1005, null, ct);

            /* Server 4: rows only outside the window. */
            await InsertAsync(connection, OutsideServerId, "orders", M(400), 1, null, ct);
            await InsertAsync(connection, OutsideServerId, "orders", M(390), 9, null, ct);

            var previous = await ReadRowsAsync(connection, PreviousFleetPgDeadlockSql, start, now, ct);
            var shipped = await ReadRowsAsync(connection, DarlingFleetReader.FleetPgDeadlockSql, start, now, ct);

            /* The whole result, every server the store has, must match - not only the sentinels. */
            Assert.Equal(previous.OrderBy(r => r.ServerId).ToList(), shipped.OrderBy(r => r.ServerId).ToList());

            /* And the sentinels carry the numbers worked out by hand, so a pair of queries that agreed on a
               wrong answer would still fail. Server 1: orders 0+3+0+0+1+0 = 4 over 6 differences (the
               duplicate-timestamp row differences against its equal-valued twin, 0, whichever of the two
               comes first; last increase 30 minutes ago); NULL series 0+3 = 3 over 2 (50 ago); billing 2
               over 1 (20 ago); so 9 over 9, last_seen the newest increase, billing's, 20 minutes ago.
               Server 2: one row, no difference, last_seen NULL. Server 3: app 0+0, reports 2+3 = 5 over 4,
               last_seen the last rise, 60 minutes ago. Server 4: absent. */
            var byId = shipped.ToDictionary(r => r.ServerId);
            Assert.Equal(9, byId[ResetServerId].Count);
            Assert.Equal(9, byId[ResetServerId].Intervals);
            Assert.Equal(DateTime.SpecifyKind(M(20), DateTimeKind.Unspecified), byId[ResetServerId].LastSeen!.Value, TimeSpan.FromMilliseconds(1));
            Assert.Equal(0, byId[LonelyServerId].Count);
            Assert.Equal(0, byId[LonelyServerId].Intervals);
            Assert.Null(byId[LonelyServerId].LastSeen);
            Assert.Equal(5, byId[FlatServerId].Count);
            Assert.Equal(4, byId[FlatServerId].Intervals);
            Assert.Equal(DateTime.SpecifyKind(M(60), DateTimeKind.Unspecified), byId[FlatServerId].LastSeen!.Value, TimeSpan.FromMilliseconds(1));
            Assert.False(byId.ContainsKey(OutsideServerId));

            /* The viewer's fleet total: graphs counted by the unchanged arm, plus the PostgreSQL counter
               differences - which must be the previous arm's number. */
            var previousPgTotal = await ScalarAsync(connection, PreviousViewerPgTotalSql, start, now, ct);
            var graphCount = await ScalarAsync(connection,
                "SELECT COUNT(*) FROM v_deadlocks WHERE deadlock_time >= $1 AND deadlock_time <= $2 AND collection_time >= $3",
                start, now, ct, EventWindowFloor.For(start));
            var viewerTotal = await ScalarAsync(connection,
                "SELECT total_deadlocks FROM (" + ViewerDataService.FleetTotalsSql + ") AS t",
                start, now, ct, EventWindowFloor.For(start));
            Assert.Equal(previousPgTotal + graphCount, viewerTotal);
            Assert.True(previousPgTotal >= 14, "the seed's own PostgreSQL differences (9 + 0 + 5) must be in the fleet total");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelRowsAsync);
        }
    }

    private static async Task<List<Row>> ReadRowsAsync(
        NpgsqlConnection connection, string sql, DateTime start, DateTime end, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
        var rows = new List<Row>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new Row(
                reader.GetInt32(0),
                Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture),
                reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                Convert.ToInt64(reader.GetValue(3), CultureInfo.InvariantCulture)));
        }

        return rows;
    }

    private static async Task<long> ScalarAsync(
        NpgsqlConnection connection, string sql, DateTime start, DateTime end, CancellationToken ct,
        DateTime? floor = null)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
        if (floor is not null)
        {
            command.Parameters.AddWithValue(DateTime.SpecifyKind(floor.Value, DateTimeKind.Unspecified));
        }

        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task InsertAsync(
        NpgsqlConnection connection, int serverId, string? databaseName, DateTime collectionTime, long deadlocks,
        DateTime? statsReset, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name, deadlocks, stats_reset)
VALUES ($1, $2, $3, $4, $5, $6, $7)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue("zz-fleet-pg-deadlock-" + serverId.ToString(CultureInfo.InvariantCulture));
        command.Parameters.AddWithValue(databaseName is null ? DBNull.Value : databaseName);
        command.Parameters.AddWithValue(deadlocks);
        command.Parameters.AddWithValue(statsReset is null ? DBNull.Value : DateTime.SpecifyKind(statsReset.Value, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteSentinelRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = string.Join(", ", SentinelIds.Select(i => i.ToString(CultureInfo.InvariantCulture)));
        await using var cleanup = new NpgsqlCommand($"DELETE FROM pg_database_stats WHERE server_id IN ({ids});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
