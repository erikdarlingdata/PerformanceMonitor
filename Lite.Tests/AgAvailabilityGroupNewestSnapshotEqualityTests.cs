/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4228 Lite parity. G1's PR body dismissed Lite because "a Lite install monitors one server" — wrong: Lite
/// can and does monitor several (<c>LocalDataService.AvailabilityGroups.cs</c> reads per server with
/// <c>WHERE server_id = $1</c>), and the per-server <c>MAX(collection_time)</c> with no bound can scan that
/// one server's whole retained history regardless of fleet size. Timed against a DuckDB table seeded at the
/// real <c>ScheduleManager</c> defaults for both AG collectors (1-minute frequency, 30-day retention, several
/// servers' rows interleaved by collection tick): the old correlated-MAX query measured 47-93 ms, well over
/// the ~20 ms parity bar. <c>GetLatestAgReplicaStatesAsync</c> / <c>GetLatestAgDatabaseReplicaStatesAsync</c>
/// now use the same two-step newest-instant split <c>DarlingAgStatesReader</c> uses (ordered-descent step
/// one, then step two bound by that exact instant) — the query itself dropped to 2-4 ms in that same seed.
///
/// <para>This class pins the NEW two-step code against the OLD (pre-#4228) correlated-subquery SQL text,
/// row for row, across the two edge cases the brief calls out: a TIE (several replicas/databases sharing the
/// newest <c>collection_time</c> — the normal shape of any AG snapshot) and a NULL (<c>ag_name</c> or
/// <c>replica_server_name</c> reading NULL under WSFC quorum loss, which both old and new code must DROP from
/// the projected rows without it affecting which instant is "newest" — <c>collection_time</c> itself is
/// <c>NOT NULL</c> in the schema, so it is never the null column).</para>
/// </summary>
public sealed class AgAvailabilityGroupNewestSnapshotEqualityTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public AgAvailabilityGroupNewestSnapshotEqualityTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedReplicaAsync(int serverId, DateTime collectionTime, string? agName, string? replicaServerName)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO ag_replica_states
    (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc,
     operational_state_desc, connected_state_desc, recovery_health_desc, synchronization_health_desc,
     availability_mode_desc, failover_mode_desc, endpoint_url, is_local)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SRV" + serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)agName ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)replicaServerName ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = "PRIMARY" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "ONLINE" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "CONNECTED" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "HEALTHY" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "HEALTHY" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SYNCHRONOUS_COMMIT" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "MANUAL" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "tcp://x" });
        cmd.Parameters.Add(new DuckDBParameter { Value = true });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedDatabaseAsync(int serverId, DateTime collectionTime, string? agName, string? databaseName, string? replicaServerName)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO ag_database_replica_states
    (collection_id, collection_time, server_id, server_name, ag_name, database_name, replica_server_name,
     secondary_lag_seconds, redo_queue_size, is_suspended, suspend_reason_desc)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SRV" + serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)agName ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)databaseName ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)replicaServerName ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = 5L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0L });
        cmd.Parameters.Add(new DuckDBParameter { Value = false });
        cmd.Parameters.Add(new DuckDBParameter { Value = DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>The exact pre-#4228 statement text for the replica grain — unbounded correlated MAX.</summary>
    private const string OldReplicaSql = @"
SELECT ag_name, replica_server_name, role_desc, connected_state_desc, collection_time
FROM ag_replica_states
WHERE server_id = $1
AND   collection_time = (SELECT MAX(collection_time) FROM ag_replica_states WHERE server_id = $1)
ORDER BY ag_name, replica_server_name";

    /// <summary>The exact pre-#4228 statement text for the database grain — unbounded correlated MAX.</summary>
    private const string OldDatabaseSql = @"
SELECT ag_name, database_name, replica_server_name, secondary_lag_seconds, redo_queue_size,
       is_suspended, suspend_reason_desc, collection_time
FROM ag_database_replica_states
WHERE server_id = $1
AND   collection_time = (SELECT MAX(collection_time) FROM ag_database_replica_states WHERE server_id = $1)
ORDER BY ag_name, database_name, replica_server_name";

    private async Task<(DateTime? Newest, List<AgReplicaReading> Rows)> RunOldReplicaSqlAsync(int serverId)
    {
        var rows = new List<AgReplicaReading>();
        DateTime? newest = null;

        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = OldReplicaSql;
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });

        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            if (!reader.IsDBNull(4))
            {
                var stamped = DateTime.SpecifyKind(reader.GetDateTime(4), DateTimeKind.Utc);
                if (newest is null || stamped > newest)
                {
                    newest = stamped;
                }
            }

            if (reader.IsDBNull(0) || reader.IsDBNull(1))
            {
                continue;
            }

            rows.Add(new AgReplicaReading(
                AgName: reader.GetString(0),
                ReplicaServerName: reader.GetString(1),
                RoleDesc: reader.IsDBNull(2) ? null : reader.GetString(2),
                ConnectedStateDesc: reader.IsDBNull(3) ? null : reader.GetString(3)));
        }

        return (newest, rows);
    }

    private async Task<(DateTime? Newest, List<AgDatabaseReading> Rows)> RunOldDatabaseSqlAsync(int serverId)
    {
        var rows = new List<AgDatabaseReading>();
        DateTime? newest = null;

        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = OldDatabaseSql;
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });

        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            if (!reader.IsDBNull(7))
            {
                var stamped = DateTime.SpecifyKind(reader.GetDateTime(7), DateTimeKind.Utc);
                if (newest is null || stamped > newest)
                {
                    newest = stamped;
                }
            }

            if (reader.IsDBNull(0) || reader.IsDBNull(1) || reader.IsDBNull(2))
            {
                continue;
            }

            rows.Add(new AgDatabaseReading(
                AgName: reader.GetString(0),
                DatabaseName: reader.GetString(1),
                ReplicaServerName: reader.GetString(2),
                SecondaryLagSeconds: reader.IsDBNull(3) ? null : Convert.ToInt64(reader.GetValue(3)),
                RedoQueueSizeKb: reader.IsDBNull(4) ? null : Convert.ToInt64(reader.GetValue(4)),
                IsSuspended: reader.IsDBNull(5) ? null : reader.GetBoolean(5),
                SuspendReasonDesc: reader.IsDBNull(6) ? null : reader.GetString(6)));
        }

        return (newest, rows);
    }

    [Fact]
    public async Task ReplicaRead_MatchesOldUnboundedSql_WithATieAndANullKeyedRowAtTheNewestInstant()
    {
        var stale = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Unspecified);
        var newest = new DateTime(2026, 9, 25, 12, 0, 0, DateTimeKind.Unspecified);

        // an older snapshot that must NOT be selected
        await SeedReplicaAsync(9001, stale, "AG1", "NODE-OLD");

        // the newest instant: a tie of two keyable rows, plus a NULL-keyed row (WSFC quorum loss) that both
        // old and new code must drop from the projection without it affecting which instant is "newest"
        await SeedReplicaAsync(9001, newest, "AG1", "NODE-A");
        await SeedReplicaAsync(9001, newest, "AG1", "NODE-B");
        await SeedReplicaAsync(9001, newest, null, "NODE-C");

        // a different server's rows must not leak in
        await SeedReplicaAsync(9002, newest, "AG1", "OTHER-NODE");

        var expected = await RunOldReplicaSqlAsync(9001);
        var service = new LocalDataService(_duckDb);
        var actual = await service.GetLatestAgReplicaStatesAsync(9001);

        Assert.Equal(2, expected.Rows.Count);
        Assert.Equal(expected.Newest, actual.CollectionTimeUtc);
        Assert.Equal(expected.Rows, actual.Replicas);
    }

    [Fact]
    public async Task DatabaseRead_MatchesOldUnboundedSql_WithATieAndANullKeyedRowAtTheNewestInstant()
    {
        var stale = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Unspecified);
        var newest = new DateTime(2026, 9, 25, 12, 0, 0, DateTimeKind.Unspecified);

        await SeedDatabaseAsync(9001, stale, "AG1", "DB-OLD", "NODE-OLD");

        await SeedDatabaseAsync(9001, newest, "AG1", "DB1", "NODE-A");
        await SeedDatabaseAsync(9001, newest, "AG1", "DB2", "NODE-A");
        await SeedDatabaseAsync(9001, newest, "AG1", null, "NODE-A");

        await SeedDatabaseAsync(9002, newest, "AG1", "DB1", "OTHER-NODE");

        var expected = await RunOldDatabaseSqlAsync(9001);
        var service = new LocalDataService(_duckDb);
        var actual = await service.GetLatestAgDatabaseReplicaStatesAsync(9001);

        Assert.Equal(2, expected.Rows.Count);
        Assert.Equal(expected.Newest, actual.CollectionTimeUtc);
        Assert.Equal(expected.Rows, actual.Databases);
    }
}
