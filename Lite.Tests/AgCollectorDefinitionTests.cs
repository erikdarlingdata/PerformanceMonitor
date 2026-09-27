/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the cross-SKU parity contract of the two shared Availability Group collector definitions
/// (#991): verbatim query text, target gate, payload column order, and the ReadAsync mapping —
/// including its null tolerance, which is the part most likely to regress. An intentional change to
/// any of these must consciously update these tests; that is the point.
///
/// <para>Neither collector computes a delta (both DMVs report current state, never a counter), so
/// unlike the wait/latch definition tests there is no delta group/key/gap contract to pin — the
/// absence is itself pinned by <see cref="WritePayload_EmitsPayloadOrder_AndTakesNoDeltas"/>.</para>
/// </summary>
public sealed class AgCollectorDefinitionTests
{
    private static readonly ICollectorSchemaInfo[] AgCollectors =
    {
        AgReplicaStatesCollector.Instance,
        AgDatabaseReplicaStatesCollector.Instance,
    };

    private const string ExpectedReplicaQuery = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    ag_name = ag.name,
    replica_server_name = ar.replica_server_name,
    role_desc = ars.role_desc,
    operational_state_desc = ars.operational_state_desc,
    connected_state_desc = ars.connected_state_desc,
    recovery_health_desc = ars.recovery_health_desc,
    synchronization_health_desc = ars.synchronization_health_desc,
    availability_mode_desc = ar.availability_mode_desc,
    failover_mode_desc = ar.failover_mode_desc,
    endpoint_url = ar.endpoint_url,
    is_local = ars.is_local,
    group_id = CONVERT(nvarchar(36), ag.group_id)
FROM sys.availability_replicas AS ar
JOIN sys.availability_groups AS ag
  ON ar.group_id = ag.group_id
JOIN sys.dm_hadr_availability_replica_states AS ars
  ON ar.replica_id = ars.replica_id
WHERE COALESCE(ag.is_distributed, 0) = 0
ORDER BY
    ag.name,
    ar.replica_server_name
OPTION(RECOMPILE);";

    private const string ExpectedDatabaseQuery = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    ag_name = ag.name,
    database_name = d.name,
    replica_server_name = ar.replica_server_name,
    is_local = hdrs.is_local,
    synchronization_state_desc = hdrs.synchronization_state_desc,
    last_hardened_lsn = CONVERT(nvarchar(40), hdrs.last_hardened_lsn),
    last_commit_lsn = CONVERT(nvarchar(40), hdrs.last_commit_lsn),
    log_send_queue_size = hdrs.log_send_queue_size,
    redo_queue_size = hdrs.redo_queue_size,
    log_send_rate = hdrs.log_send_rate,
    redo_rate = hdrs.redo_rate,
    is_suspended = hdrs.is_suspended,
    suspend_reason_desc = hdrs.suspend_reason_desc,
    availability_mode_desc = ar.availability_mode_desc,
    secondary_lag_seconds = hdrs.secondary_lag_seconds,
    last_commit_time = hdrs.last_commit_time,
    last_hardened_time = hdrs.last_hardened_time,
    last_redone_time = hdrs.last_redone_time,
    last_received_time = hdrs.last_received_time,
    est_redo_completion_time_min = CONVERT(float, (hdrs.redo_queue_size * 1.0 / NULLIF(hdrs.redo_rate, 0)) / 60.0),
    est_send_drain_time_min = CONVERT(float, (hdrs.log_send_queue_size * 1.0 / NULLIF(hdrs.log_send_rate, 0)) / 60.0),
    group_id = CONVERT(nvarchar(36), ag.group_id)
FROM sys.dm_hadr_database_replica_states AS hdrs
JOIN sys.availability_replicas AS ar
  ON  hdrs.replica_id = ar.replica_id
  AND hdrs.group_id = ar.group_id
JOIN sys.availability_groups AS ag
  ON hdrs.group_id = ag.group_id
JOIN sys.databases AS d
  ON hdrs.database_id = d.database_id
WHERE COALESCE(ag.is_distributed, 0) = 0
ORDER BY
    ag.name,
    ar.replica_server_name,
    d.name
OPTION(RECOMPILE);";

    [Fact]
    public void ReplicaQuery_IsTheVerbatimParityContract()
    {
        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());

        Assert.Equal(ExpectedReplicaQuery, AgReplicaStatesCollector.Instance.BuildQuery(context).Text);
        Assert.Empty(AgReplicaStatesCollector.Instance.BuildQuery(context).Parameters);
        Assert.Null(AgReplicaStatesCollector.Instance.WatermarkColumn);
        Assert.Equal("ag_replica_states", AgReplicaStatesCollector.Instance.Name);
        Assert.Equal("ag_replica_states", AgReplicaStatesCollector.Instance.TargetTable);
        Assert.Equal("collection_time", AgReplicaStatesCollector.Instance.PrefixTimeColumnName);
    }

    [Fact]
    public void DatabaseQuery_IsTheVerbatimParityContract()
    {
        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());

        Assert.Equal(ExpectedDatabaseQuery, AgDatabaseReplicaStatesCollector.Instance.BuildQuery(context).Text);
        Assert.Empty(AgDatabaseReplicaStatesCollector.Instance.BuildQuery(context).Parameters);
        Assert.Null(AgDatabaseReplicaStatesCollector.Instance.WatermarkColumn);
        Assert.Equal("ag_database_replica_states", AgDatabaseReplicaStatesCollector.Instance.Name);
        Assert.Equal("ag_database_replica_states", AgDatabaseReplicaStatesCollector.Instance.TargetTable);
        Assert.Equal("collection_time", AgDatabaseReplicaStatesCollector.Instance.PrefixTimeColumnName);
    }

    [Fact]
    public void Queries_KeepTheDistributedAgFilter()
    {
        /* The DAG container rows must stay out of both tables — member AGs still appear. Pinned
           separately from the verbatim text so the intent survives a reformat of the query. */
        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());

        Assert.Contains("COALESCE(ag.is_distributed, 0) = 0", AgReplicaStatesCollector.Instance.BuildQuery(context).Text, StringComparison.Ordinal);
        Assert.Contains("COALESCE(ag.is_distributed, 0) = 0", AgDatabaseReplicaStatesCollector.Instance.BuildQuery(context).Text, StringComparison.Ordinal);
    }

    [Fact]
    public void AppliesTo_EverywhereExceptAzureSqlDb()
    {
        /* On-prem, Managed Instance and RDS all expose the AG surface; Azure SQL DB does not. An
           AG-less on-prem server is NOT gated off — it collects and returns zero rows. */
        foreach (var collector in AgCollectors)
        {
            Assert.False(collector.AppliesTo(new CollectorTargetInfo { IsAzureSqlDb = true }));
            Assert.True(collector.AppliesTo(new CollectorTargetInfo { IsAzureManagedInstance = true }));
            Assert.True(collector.AppliesTo(new CollectorTargetInfo { IsAwsRds = true }));
            Assert.True(collector.AppliesTo(new CollectorTargetInfo()));

            /* msdb access is irrelevant here — these read server-scoped HADR objects, not msdb. */
            Assert.True(collector.AppliesTo(new CollectorTargetInfo { HasMsdbAccess = false }));
        }
    }

    [Fact]
    public void NeitherCollector_RunsPerDatabase()
    {
        /* Server-scope single sweep: the database grain comes from the DMV's own rows, NOT from the
           host enumerating databases and connecting to each. */
        Assert.False(AgReplicaStatesCollector.Instance.RunsPerDatabase(new CollectorTargetInfo()));
        Assert.False(AgReplicaStatesCollector.Instance.RunsPerDatabase(new CollectorTargetInfo { IsAzureManagedInstance = true }));
        Assert.False(AgDatabaseReplicaStatesCollector.Instance.RunsPerDatabase(new CollectorTargetInfo()));
        Assert.False(AgDatabaseReplicaStatesCollector.Instance.RunsPerDatabase(new CollectorTargetInfo { IsAzureManagedInstance = true }));
    }

    [Fact]
    public void ReplicaPayloadColumns_AreInAppendOrder()
    {
        var names = AgReplicaStatesCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Equal(
            new[]
            {
                "ag_name",
                "replica_server_name",
                "role_desc",
                "operational_state_desc",
                "connected_state_desc",
                "recovery_health_desc",
                "synchronization_health_desc",
                "availability_mode_desc",
                "failover_mode_desc",
                "endpoint_url",
                /* #1696: appended LAST, so an upgraded store's ALTER lands it in the same physical position
                   a fresh generated table puts it - which is what keeps the two provenances comparable. */
                "is_local",
                /* #4475: appended AFTER is_local, for the same reason - it lands in the same physical
                   position a fresh generated table puts it. */
                "group_id",
            },
            names);

        /* Every state string is a Varchar; is_local is the one Boolean, and typing it as text would make the
           migration's ALTER disagree with the generated fresh shape. */
        Assert.All(AgReplicaStatesCollector.Instance.PayloadColumns.Where(c => c.Name != "is_local"),
            c => Assert.Equal(CollectorColumnType.Varchar, c.Type));
        Assert.Equal(
            CollectorColumnType.Boolean,
            AgReplicaStatesCollector.Instance.PayloadColumns.Single(c => c.Name == "is_local").Type);
    }

    [Fact]
    public void DatabasePayloadColumns_AreInAppendOrder_WithTheDeclaredTypes()
    {
        var columns = AgDatabaseReplicaStatesCollector.Instance.PayloadColumns;

        Assert.Equal(
            new[]
            {
                "ag_name",
                "database_name",
                "replica_server_name",
                "is_local",
                "synchronization_state_desc",
                "last_hardened_lsn",
                "last_commit_lsn",
                "log_send_queue_size",
                "redo_queue_size",
                "log_send_rate",
                "redo_rate",
                "is_suspended",
                "suspend_reason_desc",
                "availability_mode_desc",
                "secondary_lag_seconds",
                "last_commit_time",
                "last_hardened_time",
                "last_redone_time",
                "last_received_time",
                "est_redo_completion_time_min",
                "est_send_drain_time_min",
                /* #4475: appended LAST, same rationale as the replica table's group_id. */
                "group_id",
            },
            columns.Select(c => c.Name).ToArray());

        Assert.Equal(CollectorColumnType.Varchar, columns.Single(c => c.Name == "group_id").Type);

        /* The LSNs are numeric(25,0) in the DMV — far wider than BIGINT — so they are converted
           server-side and stored as text. Storing them numerically would silently overflow. */
        Assert.Equal(CollectorColumnType.Varchar, columns.Single(c => c.Name == "last_hardened_lsn").Type);
        Assert.Equal(CollectorColumnType.Varchar, columns.Single(c => c.Name == "last_commit_lsn").Type);

        Assert.Equal(CollectorColumnType.Boolean, columns.Single(c => c.Name == "is_local").Type);
        Assert.Equal(CollectorColumnType.Boolean, columns.Single(c => c.Name == "is_suspended").Type);

        foreach (var numeric in new[] { "log_send_queue_size", "redo_queue_size", "log_send_rate", "redo_rate", "secondary_lag_seconds" })
        {
            Assert.Equal(CollectorColumnType.BigInt, columns.Single(c => c.Name == numeric).Type);
        }

        /* The four DMV timestamps the reference query skips. */
        foreach (var time in new[] { "last_commit_time", "last_hardened_time", "last_redone_time", "last_received_time" })
        {
            Assert.Equal(CollectorColumnType.Timestamp, columns.Single(c => c.Name == time).Type);
        }

        /* The drain-time estimates are CONVERT(float, ...) server-side — Double, not Decimal: without the
           explicit CONVERT the expression's numeric type would come back as decimal and GetDouble throws. */
        foreach (var estimate in new[] { "est_redo_completion_time_min", "est_send_drain_time_min" })
        {
            Assert.Equal(CollectorColumnType.Double, columns.Single(c => c.Name == estimate).Type);
        }
    }

    [Fact]
    public void DrainTimeEstimates_GuardIntegerDivisionAndZeroRate()
    {
        /* Two guards, both load-bearing, both easy to drop in a reformat: `* 1.0` stops BIGINT / BIGINT
           integer division (which would floor a 0.4-minute drain to 0), and NULLIF(rate, 0) stops the
           divide-by-zero that an idle or suspended replica — rate 0 — would otherwise raise, failing the
           whole collection cycle rather than one column. */
        var text = AgDatabaseReplicaStatesCollector.Instance.BuildQuery(CollectorTestContext.Make(new RecordingCollectorDeltaCalculator())).Text;

        Assert.Contains("(hdrs.redo_queue_size * 1.0 / NULLIF(hdrs.redo_rate, 0)) / 60.0", text, StringComparison.Ordinal);
        Assert.Contains("(hdrs.log_send_queue_size * 1.0 / NULLIF(hdrs.log_send_rate, 0)) / 60.0", text, StringComparison.Ordinal);
        Assert.Contains("est_redo_completion_time_min = CONVERT(float,", text, StringComparison.Ordinal);
        Assert.Contains("est_send_drain_time_min = CONVERT(float,", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReplicaReadAsync_MapsColumns()
    {
        using var reader = new FakeCollectorDataReader(
            new object[] { "AG1", "NODE1", "PRIMARY", "ONLINE", "CONNECTED", "ONLINE", "HEALTHY", "SYNCHRONOUS_COMMIT", "AUTOMATIC", "TCP://NODE1.corp:5022", true, "11111111-1111-1111-1111-111111111111" },
            new object[] { "AG1", "NODE2", "SECONDARY", "ONLINE", "CONNECTED", "ONLINE", "HEALTHY", "SYNCHRONOUS_COMMIT", "AUTOMATIC", "TCP://NODE2.corp:5022", false, "11111111-1111-1111-1111-111111111111" });

        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());

        var rows = await AgReplicaStatesCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(2, rows.Count);
        Assert.Equal(
            new AgReplicaStatesCollector.Row("AG1", "NODE1", "PRIMARY", "ONLINE", "CONNECTED", "ONLINE", "HEALTHY", "SYNCHRONOUS_COMMIT", "AUTOMATIC", "TCP://NODE1.corp:5022", true, "11111111-1111-1111-1111-111111111111"),
            rows[0]);
        Assert.Equal("NODE2", rows[1].ReplicaServerName);
        Assert.Equal("SECONDARY", rows[1].RoleDesc);
        /* #1696: is_local marks which row is the connected replica's own, so the alert path can pick ONE
           node's view of an AG that every node can see. */
        Assert.True(rows[0].IsLocal);
        Assert.False(rows[1].IsLocal);
        /* #4475: group_id maps like any other trailing text column. */
        Assert.Equal("11111111-1111-1111-1111-111111111111", rows[0].GroupId);
    }

    [Fact]
    public async Task ReplicaReadAsync_ToleratesNulls()
    {
        /* endpoint_url is documented NULL when the instance cannot reach the WSFC cluster, and in that
           state sys.availability_replicas serves only locally cached metadata — so every column can be
           null. A quorum-loss read must produce a row, not an InvalidCastException. */
        using var reader = new FakeCollectorDataReader(
            new object[] { "AG1", "NODE1", "PRIMARY", "ONLINE", "CONNECTED", "ONLINE", "HEALTHY", "SYNCHRONOUS_COMMIT", "AUTOMATIC", DBNull.Value, DBNull.Value, DBNull.Value },
            new object[] { DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value });

        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());

        var rows = await AgReplicaStatesCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(2, rows.Count);
        Assert.Null(rows[0].EndpointUrl);
        /* #1696: is_local must read NULL under quorum loss, never false — the de-dup treats unknown as
           "cannot rank this vantage" and keeps judging, whereas a false would silently demote a real one. */
        Assert.Null(rows[0].IsLocal);
        Assert.Null(rows[1].IsLocal);
        Assert.Equal("AG1", rows[0].AgName);
        Assert.Equal(default(AgReplicaStatesCollector.Row), rows[1]);
        /* #4475: group_id is nullable, and NULL here (a pre-v65 row would carry none). */
        Assert.Null(rows[0].GroupId);
    }

    [Fact]
    public async Task DatabaseReadAsync_MapsColumns()
    {
        using var reader = new FakeCollectorDataReader(
            new object[]
            {
                "AG1", "Orders", "NODE2", false, "SYNCHRONIZING",
                "37000000045600001", "37000000045600002",
                4096L, 2048L, 512L, 256L,
                false, DBNull.Value, "SYNCHRONOUS_COMMIT", 12L,
                new DateTime(2026, 7, 26, 12, 0, 0), new DateTime(2026, 7, 26, 12, 0, 1),
                new DateTime(2026, 7, 26, 11, 59, 55), new DateTime(2026, 7, 26, 12, 0, 2),
                /* The two drain estimates are computed SERVER-side, so the fake supplies them directly and
                   they need not agree with the queue/rate columns above. Deliberately different values so a
                   swapped mapping between the two fails rather than passing on a coincidence. */
                8.0d, 0.5d,
                /* #4475: group_id, trailing. */
                "22222222-2222-2222-2222-222222222222",
            });

        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());

        var rows = await AgDatabaseReplicaStatesCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal("AG1", row.AgName);
        Assert.Equal("Orders", row.DatabaseName);
        Assert.Equal("NODE2", row.ReplicaServerName);
        Assert.False(row.IsLocal);
        Assert.Equal("SYNCHRONIZING", row.SynchronizationStateDesc);
        Assert.Equal("37000000045600001", row.LastHardenedLsn);
        Assert.Equal("37000000045600002", row.LastCommitLsn);
        Assert.Equal(4096L, row.LogSendQueueSize);
        Assert.Equal(2048L, row.RedoQueueSize);
        Assert.Equal(512L, row.LogSendRate);
        Assert.Equal(256L, row.RedoRate);
        Assert.False(row.IsSuspended);
        Assert.Null(row.SuspendReasonDesc);
        Assert.Equal("SYNCHRONOUS_COMMIT", row.AvailabilityModeDesc);
        Assert.Equal(12L, row.SecondaryLagSeconds);
        Assert.Equal(new DateTime(2026, 7, 26, 12, 0, 0), row.LastCommitTime);
        Assert.Equal(new DateTime(2026, 7, 26, 12, 0, 1), row.LastHardenedTime);
        Assert.Equal(new DateTime(2026, 7, 26, 11, 59, 55), row.LastRedoneTime);
        Assert.Equal(new DateTime(2026, 7, 26, 12, 0, 2), row.LastReceivedTime);

        Assert.Equal(8.0d, row.EstRedoCompletionTimeMin);
        Assert.Equal(0.5d, row.EstSendDrainTimeMin);
        /* #4475: group_id maps like any other trailing text column. */
        Assert.Equal("22222222-2222-2222-2222-222222222222", row.GroupId);
    }

    [Fact]
    public async Task DatabaseReadAsync_ToleratesNulls()
    {
        /* Every payload column of sys.dm_hadr_database_replica_states is documented nullable —
           last_hardened_lsn is explicitly NULL on an async-commit primary, and the queues/rates/lag
           are null on a replica that is not reporting. The two drain estimates are ALWAYS null on this
           shape: NULLIF(rate, 0) makes a null or zero rate produce a null estimate rather than a
           divide-by-zero, which is the whole point of the guard. */
        using var reader = new FakeCollectorDataReader(
            new object[]
            {
                "AG1", "Orders", "NODE1", true, "SYNCHRONIZED",
                DBNull.Value, DBNull.Value,
                DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
                DBNull.Value, DBNull.Value, "ASYNCHRONOUS_COMMIT", DBNull.Value,
                DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
                DBNull.Value, DBNull.Value,
                /* #4475: group_id, trailing, nullable. */
                DBNull.Value,
            });

        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());

        var rows = await AgDatabaseReplicaStatesCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.True(row.IsLocal);
        Assert.Null(row.LastHardenedLsn);
        Assert.Null(row.LastCommitLsn);
        Assert.Null(row.LogSendQueueSize);
        Assert.Null(row.RedoQueueSize);
        Assert.Null(row.LogSendRate);
        Assert.Null(row.RedoRate);
        Assert.Null(row.IsSuspended);
        Assert.Null(row.SuspendReasonDesc);
        Assert.Null(row.SecondaryLagSeconds);
        Assert.Null(row.LastCommitTime);
        Assert.Null(row.LastHardenedTime);
        Assert.Null(row.LastRedoneTime);
        Assert.Null(row.LastReceivedTime);

        /* NULL, not 0 — an un-drainable queue must never read as "drains instantly". */
        Assert.Null(row.EstRedoCompletionTimeMin);
        Assert.Null(row.EstSendDrainTimeMin);
        /* #4475: group_id is nullable, and NULL here (a pre-v65 row would carry none). */
        Assert.Null(row.GroupId);
    }

    [Fact]
    public async Task ReadAsync_OnAnAgLessServer_YieldsZeroRowsNotAnError()
    {
        /* The normal case on the overwhelming majority of monitored servers: both catalog views and
           the DMV exist whether or not Always On is configured, so the sweep is an empty result set,
           and the host logs SUCCESS with 0 rows rather than an error. */
        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());

        using var replicaReader = new FakeCollectorDataReader();
        Assert.Empty(await AgReplicaStatesCollector.Instance.ReadAsync(replicaReader, context, CancellationToken.None));

        using var databaseReader = new FakeCollectorDataReader();
        Assert.Empty(await AgDatabaseReplicaStatesCollector.Instance.ReadAsync(databaseReader, context, CancellationToken.None));
    }

    [Fact]
    public void WritePayload_EmitsPayloadOrder_AndTakesNoDeltas()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var context = CollectorTestContext.Make(deltas);

        var replicaWriter = new RecordingCollectorRowWriter();
        AgReplicaStatesCollector.Instance.WritePayload(
            new AgReplicaStatesCollector.Row("AG1", "NODE1", "PRIMARY", "ONLINE", "CONNECTED", "ONLINE", "HEALTHY", "SYNCHRONOUS_COMMIT", "AUTOMATIC", null, true, null),
            replicaWriter,
            context);

        Assert.Equal(
            new object?[] { "AG1", "NODE1", "PRIMARY", "ONLINE", "CONNECTED", "ONLINE", "HEALTHY", "SYNCHRONOUS_COMMIT", "AUTOMATIC", null, true, null },
            replicaWriter.Values);

        /* Modeled on a real measured sample from the Docker AG fixture: a SUSPEND_FROM_USER replica 62 s
           into suspension. Note the lag is 62, NOT 0 — the docs claim lag reads 0 while suspended, and the
           live instance does the opposite. A send-drain estimate of null is the matching reality: the send
           queue goes NULL while suspended, so there is nothing to divide. */
        var databaseWriter = new RecordingCollectorRowWriter();
        AgDatabaseReplicaStatesCollector.Instance.WritePayload(
            new AgDatabaseReplicaStatesCollector.Row(
                "AG1", "Orders", "NODE2", false, "SYNCHRONIZING", "1", "2", 4096L, 2048L, 512L, 256L, true, "SUSPEND_FROM_USER", "SYNCHRONOUS_COMMIT", 62L,
                new DateTime(2026, 7, 26, 12, 0, 0), new DateTime(2026, 7, 26, 12, 0, 1),
                new DateTime(2026, 7, 26, 11, 59, 55), new DateTime(2026, 7, 26, 12, 0, 2),
                8.0d, null, null),
            databaseWriter,
            context);

        Assert.Equal(
            new object?[]
            {
                "AG1", "Orders", "NODE2", false, "SYNCHRONIZING", "1", "2", 4096L, 2048L, 512L, 256L, true, "SUSPEND_FROM_USER", "SYNCHRONOUS_COMMIT", 62L,
                new DateTime(2026, 7, 26, 12, 0, 0), new DateTime(2026, 7, 26, 12, 0, 1),
                new DateTime(2026, 7, 26, 11, 59, 55), new DateTime(2026, 7, 26, 12, 0, 2),
                8.0d, null, null,
            },
            databaseWriter.Values);

        /* The queues, rates and lag are instantaneous gauges, NOT counters: neither collector may
           touch the delta calculator. A delta here would silently produce nonsense. */
        Assert.Empty(deltas.Calls);
    }

    [Fact]
    public void BothCollectors_AreRegisteredInTheCatalog_WithDefaultSchedules()
    {
        foreach (var name in new[] { "ag_replica_states", "ag_database_replica_states" })
        {
            Assert.Contains(CollectorCatalog.All, c => c.Name == name);
            Assert.True(CollectorScheduleDefaults.All.TryGetValue(name, out var entry));
            Assert.Equal(1, entry!.FrequencyMinutes);
            Assert.Equal(30, entry.RetentionDays);

            /* Default ON — unlike long_query_completions, an AG read costs nothing on a server
               without AGs, so there is no reason to make it opt-in. */
            Assert.True(entry.DefaultEnabled);
        }
    }

    [Fact]
    public void CatalogAppliesTo_ByName_AgreesWithTheDefinitions()
    {
        /* Lite consults the gate BY NAME for its pre-dispatch SKIPPED log; a typo there would
           silently return "not gated". Pin that the by-name lookup resolves to the real override. */
        var azure = new CollectorTargetInfo { IsAzureSqlDb = true };

        Assert.False(CollectorCatalog.AppliesTo("ag_replica_states", azure));
        Assert.False(CollectorCatalog.AppliesTo("ag_database_replica_states", azure));
        Assert.True(CollectorCatalog.AppliesTo("ag_replica_states", new CollectorTargetInfo()));
        Assert.True(CollectorCatalog.AppliesTo("ag_database_replica_states", new CollectorTargetInfo()));
    }
}
