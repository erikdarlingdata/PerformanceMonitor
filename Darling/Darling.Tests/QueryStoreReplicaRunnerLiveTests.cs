/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6254 / #6279: drives the host's REAL enumerated Query Store loop with a fake target that applies the cutoff the way the SQL
/// does (<c>last_execution_time &gt; @cutoff_time</c>) and tells the primary connection from the replica's. A late row that lands
/// between W - L and W must be stored, a row already shipped must not be written again, the replica's rows land under the
/// primary's server id, a setting that is empty opens no replica connection, and a replica that fails costs nothing but its reason.
///
/// <para><b>#1776 own-store</b> - every test here creates and drops its own scratch database through
/// <see cref="ScratchPostgres"/>, so none is in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class QueryStoreReplicaRunnerLiveTests
{
    private const int LiveServerId = -625400;
    private const string Db = "AgDb";
    private const string PrimaryConnection = "Server=primary";
    private const string ReplicaConnection = "Server=replica";
    private static readonly Guid GroupDatabaseId = Guid.Parse("7a88cd1f-6fcf-444c-a320-feaed6e9a6a1");

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static ServerRuntime MakeServer(string? replica) => new()
    {
        Config = new MonitoredServer { Name = "t", Host = "h", QueryStoreReplica = replica },
        ConnectionString = PrimaryConnection,
        Target = new CollectorTargetInfo { IsAzureSqlDb = false, Engine = CollectorTargetEngine.SqlServer },
        StorageName = "h",
        ServerId = LiveServerId,
    };

    /// <summary>What the fake servers hold and what happened to them.</summary>
    private sealed class Target
    {
        public List<object[]> Rows { get; } = new();
        public int FlushSeconds { get; set; } = 60;
        public bool ReplicaOpenFails { get; set; }
        public bool ReplicaReadFails { get; set; }
        public List<string> ConnectionsOpened { get; } = new();
        public int ReplicaReads;
        public List<DateTime> Cutoffs { get; } = new();
        public int PrimaryReads;
        public DateTime CommitTime { get; set; } = DateTime.UtcNow;
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<string> Lines { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel >= LogLevel.Warning)
            {
                lock (Lines)
                {
                    Lines.Add(formatter(state, exception));
                }
            }
        }

        public string Text { get { lock (Lines) { return string.Join(" | ", Lines); } } }
    }

    private sealed class Rig : IAsyncDisposable
    {
        public ScratchPostgres Scratch = null!;
        public NpgsqlDataSource Source = null!;
        public DarlingCollectorRunner Runner = null!;
        public CapturingLogger Log = new();

        public static async Task<Rig> OpenAsync(Target target, CancellationToken ct)
        {
            var rig = new Rig();
            rig.Scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
            await using (var migrate = new NpgsqlConnection(rig.Scratch.ConnectionString))
            {
                await migrate.OpenAsync(ct);
                await PgMigrations.MigrateAsync(migrate, ct);
            }

            rig.Source = new NpgsqlDataSourceBuilder(rig.Scratch.ConnectionString).Build();
            rig.Runner = new DarlingCollectorRunner(rig.Source, new CollectorDeltaCalculator(), rig.Log);
            var provider = new FakeTargetProvider(target);
            rig.Runner.TargetProviderOverrideForTests = _ => provider;
            rig.Runner.QueryStoreReplicaConnectionStringOverrideForTests = _ => ReplicaConnection;
            return rig;
        }

        public async ValueTask DisposeAsync()
        {
            await Source.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    /// <summary>One runtime-stats row for interval <paramref name="id"/>, last executed at <paramref name="lastUtc"/>.</summary>
    private static object[] Row(long id, DateTime lastUtc, string? replicaRole = null, long executions = 1)
    {
        var stamp = new DateTimeOffset(lastUtc, TimeSpan.Zero);
        var row = new object[56];
        Array.Fill(row, DBNull.Value);
        row[0] = id;
        row[1] = id;
        row[2] = "Regular";
        row[3] = stamp;
        row[4] = stamp;
        row[7] = "0x" + id.ToString("X8", System.Globalization.CultureInfo.InvariantCulture);
        row[8] = executions;
        row[51] = "0x" + id.ToString("X8", System.Globalization.CultureInfo.InvariantCulture);
        row[52] = (object?)replicaRole ?? DBNull.Value;
        row[53] = id;
        return row;
    }

    private static DateTime WholeSeconds(DateTime value) => new(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);

    private static async Task<Dictionary<long, long>> StoredAsync(Rig rig, CancellationToken token)
    {
        await using var connection = new NpgsqlConnection(rig.Scratch.ConnectionString);
        await connection.OpenAsync(token);
        await using var command = new NpgsqlCommand(
            "SELECT runtime_stats_interval_id, count(*) FROM collect.query_store_stats WHERE server_id = @s AND database_name = @d GROUP BY 1",
            connection);
        command.Parameters.AddWithValue("s", LiveServerId);
        command.Parameters.AddWithValue("d", Db);
        var result = new Dictionary<long, long>();
        await using var reader = await command.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
        {
            result[reader.GetInt64(0)] = reader.GetInt64(1);
        }

        return result;
    }

    /// <summary>
    /// T5: with a replica set, run 1 shows row A at W; run 2 shows A again and a late row B older than W. B reaches the store
    /// (the cutoff is W - L, not W), A is not written twice (the ship guard), the rows are the primary's server id, and the
    /// run says where it read.
    /// </summary>
    [Fact]
    public async Task AReplicaRead_StoresALateRowBelowTheWatermark_NeverWritesAShippedRowAgain_UnderThePrimarysServerId()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6254 runner pins.");
        var token = TestContext.Current.CancellationToken;
        var target = new Target();
        await using var rig = await Rig.OpenAsync(target, token);

        var w = WholeSeconds(DateTime.UtcNow.AddMinutes(-20));
        target.Rows.Add(Row(1, w));

        var first = await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer("replica-host"), token);
        Assert.Equal(1, first.Rows);
        Assert.Equal("replica 1, primary 0", first.ReadSource);

        /* B first executed three minutes before A's time: below the watermark, inside the 6-minute look-back (60 s flush + 5). */
        target.Rows.Add(Row(2, w.AddMinutes(-3)));
        var second = await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer("replica-host"), token);

        var stored = await StoredAsync(rig, token);
        Assert.Equal(new Dictionary<long, long> { [1] = 1, [2] = 1 }, stored);
        Assert.Equal(1, second.Rows);
        Assert.Equal("replica 1, primary 0", second.ReadSource);
        Assert.Equal(0, target.PrimaryReads);
        Assert.Equal(2, target.ReplicaReads);

        /* Nothing new: nothing ships, and the guard does not let the look-back re-read write A or B again. */
        var third = await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer("replica-host"), token);
        Assert.Equal(0, third.Rows);
        Assert.Equal(new Dictionary<long, long> { [1] = 1, [2] = 1 }, await StoredAsync(rig, token));
    }

    /// <summary>
    /// #9352: a replica read whose stored watermark is older than the catch-up is clamped, and when an earlier cycle that clamped the
    /// same watermark landed after reading up to some time, the new clamp is a new gap. Its hole starts the look-back before what that
    /// cycle read, not the look-back before the raw watermark, which overlapped the hole the earlier clamp recorded.
    /// </summary>
    [Fact]
    public async Task AReplicaClampThatIsANewGapAfterALandedCycle_OpensItsHoleAtWhatTheCycleRead_MinusTheLookBack()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6254 runner pins.");
        var token = TestContext.Current.CancellationToken;
        var target = new Target();
        await using var rig = await Rig.OpenAsync(target, token);

        target.Rows.Add(Row(1, WholeSeconds(DateTime.UtcNow.AddMinutes(-20))));
        var first = await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer("replica-host"), token);
        Assert.Equal(1, first.Rows);

        /* The stored watermark W is 3 hours old. An earlier cycle that clamped it landed after reading up to 90 minutes ago, so this
           cycle's floor (collection time - 1 hour) lies past that read and the clamp is a new gap. */
        var watermark = WholeSeconds(DateTime.UtcNow.AddHours(-3));
        var readThrough = WholeSeconds(DateTime.UtcNow.AddMinutes(-90));
        await using (var connection = new NpgsqlConnection(rig.Scratch.ConnectionString))
        {
            await connection.OpenAsync(token);
            await using var update = new NpgsqlCommand(
                "UPDATE collect.query_store_stats SET last_execution_time = @w WHERE server_id = @s AND database_name = @d", connection);
            update.Parameters.AddWithValue("w", DateTime.SpecifyKind(watermark, DateTimeKind.Unspecified));
            update.Parameters.AddWithValue("s", LiveServerId);
            update.Parameters.AddWithValue("d", Db);
            Assert.True(await update.ExecuteNonQueryAsync(token) > 0);
        }

        /* A second runner over the same store, as after a restart: the first one cached the watermark it saw, and the update above
           goes around that cache. */
        var restarted = new DarlingCollectorRunner(rig.Source, new CollectorDeltaCalculator(), rig.Log);
        var provider = new FakeTargetProvider(target);
        restarted.TargetProviderOverrideForTests = _ => provider;
        restarted.QueryStoreReplicaConnectionStringOverrideForTests = _ => ReplicaConnection;
        restarted.QueryStoreClampReportsForTests.Land(LiveServerId, Db, watermark, readThrough);
        target.Rows.Clear();
        await restarted.RunAsync(QueryStoreCollector.Instance, MakeServer("replica-host"), token);

        string? encoded;
        await using (var connection = new NpgsqlConnection(rig.Scratch.ConnectionString))
        {
            await connection.OpenAsync(token);
            await using var read = new NpgsqlCommand(
                "SELECT state_value FROM collector_state WHERE server_id = @s AND collector_name = @c AND state_key = @k", connection);
            read.Parameters.AddWithValue("s", LiveServerId);
            read.Parameters.AddWithValue("c", QueryStoreBackfillState.StateCollectorName);
            read.Parameters.AddWithValue("k", QueryStoreBackfillState.HoleKeyPrefix + Db);
            encoded = (string?)await read.ExecuteScalarAsync(token);
        }

        Assert.True(QueryStoreBackfillState.TryDecodeHoles(encoded, out var holes));
        var hole = Assert.Single(holes);

        /* The look-back is 6 minutes here (a 60 second flush plus 5 minutes). */
        Assert.Equal(readThrough.AddMinutes(-6), hole.FromUtc);
        Assert.True(hole.ToUtc > readThrough);
    }

    private DarlingCollectorRunner RestartedRunner(Rig rig, Target target)
    {
        var restarted = new DarlingCollectorRunner(rig.Source, new CollectorDeltaCalculator(), rig.Log);
        var provider = new FakeTargetProvider(target);
        restarted.TargetProviderOverrideForTests = _ => provider;
        restarted.QueryStoreReplicaConnectionStringOverrideForTests = _ => ReplicaConnection;
        return restarted;
    }

    private static async Task SetStoredWatermarkAsync(Rig rig, DateTime watermark, CancellationToken token)
    {
        await using var connection = new NpgsqlConnection(rig.Scratch.ConnectionString);
        await connection.OpenAsync(token);
        await using var update = new NpgsqlCommand(
            "UPDATE collect.query_store_stats SET last_execution_time = @w WHERE server_id = @s AND database_name = @d", connection);
        update.Parameters.AddWithValue("w", DateTime.SpecifyKind(watermark, DateTimeKind.Unspecified));
        update.Parameters.AddWithValue("s", LiveServerId);
        update.Parameters.AddWithValue("d", Db);
        Assert.True(await update.ExecuteNonQueryAsync(token) > 0);
    }

    private static async Task<string?> ReadBackfillStateAsync(Rig rig, string key, CancellationToken token)
    {
        await using var connection = new NpgsqlConnection(rig.Scratch.ConnectionString);
        await connection.OpenAsync(token);
        await using var read = new NpgsqlCommand(
            "SELECT state_value FROM collector_state WHERE server_id = @s AND collector_name = @c AND state_key = @k", connection);
        read.Parameters.AddWithValue("s", LiveServerId);
        read.Parameters.AddWithValue("c", QueryStoreBackfillState.StateCollectorName);
        read.Parameters.AddWithValue("k", key);
        return (string?)await read.ExecuteScalarAsync(token);
    }

    private static async Task WriteBackfillStateAsync(Rig rig, string key, string? value, CancellationToken token)
    {
        await using var connection = new NpgsqlConnection(rig.Scratch.ConnectionString);
        await connection.OpenAsync(token);
        await using var write = new NpgsqlCommand(
            value is null
                ? "DELETE FROM collector_state WHERE server_id = @s AND collector_name = @c AND state_key = @k"
                : "INSERT INTO collector_state (server_id, collector_name, state_key, state_value, updated_at) VALUES (@s, @c, @k, @v, now() AT TIME ZONE 'UTC') "
                  + "ON CONFLICT (server_id, collector_name, state_key) DO UPDATE SET state_value = EXCLUDED.state_value",
            connection);
        write.Parameters.AddWithValue("s", LiveServerId);
        write.Parameters.AddWithValue("c", QueryStoreBackfillState.StateCollectorName);
        write.Parameters.AddWithValue("k", key);
        if (value is not null)
        {
            write.Parameters.AddWithValue("v", value);
        }

        await write.ExecuteNonQueryAsync(token);
    }

    /// <summary>
    /// #9359: an idle database whose clamp an earlier process already reported and read. After a restart its first clamp opens no
    /// hole, because the stored entry says the range up to the last cycle was read. Once the database stops clamping, the entry is
    /// retired.
    /// </summary>
    [Fact]
    public async Task AnIdleDatabaseAfterARestart_OpensNoHole_AndItsStoredEntryIsRetiredWhenItStopsClamping()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6254 runner pins.");
        var token = TestContext.Current.CancellationToken;
        var target = new Target();
        await using var rig = await Rig.OpenAsync(target, token);
        var landedKey = QueryStoreBackfillState.LandedKeyPrefix + Db;
        var holeKey = QueryStoreBackfillState.HoleKeyPrefix + Db;

        target.Rows.Add(Row(1, WholeSeconds(DateTime.UtcNow.AddMinutes(-20))));
        Assert.Equal(1, (await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer("replica-host"), token)).Rows);

        /* The stored watermark goes stale and the source has nothing new: the first clamp records its hole and its entry. */
        await SetStoredWatermarkAsync(rig, WholeSeconds(DateTime.UtcNow.AddHours(-3)), token);
        target.Rows.Clear();
        var beforeRestart = RestartedRunner(rig, target);
        await beforeRestart.RunAsync(QueryStoreCollector.Instance, MakeServer("replica-host"), token);
        Assert.NotNull(await ReadBackfillStateAsync(rig, holeKey, token));
        Assert.NotNull(await ReadBackfillStateAsync(rig, landedKey, token));

        /* The backfill digs and retires that hole. A restarted process then clamps the same watermark. */
        await WriteBackfillStateAsync(rig, holeKey, null, token);
        var afterRestart = RestartedRunner(rig, target);
        await afterRestart.RunAsync(QueryStoreCollector.Instance, MakeServer("replica-host"), token);
        Assert.Null(await ReadBackfillStateAsync(rig, holeKey, token));
        Assert.NotNull(await ReadBackfillStateAsync(rig, landedKey, token));

        /* A row arrives: that cycle still read the stale watermark and keeps the entry, and the next one clamps nothing and retires it. */
        target.Rows.Add(Row(2, WholeSeconds(DateTime.UtcNow.AddMinutes(-5))));
        await afterRestart.RunAsync(QueryStoreCollector.Instance, MakeServer("replica-host"), token);
        Assert.NotNull(await ReadBackfillStateAsync(rig, landedKey, token));
        target.Rows.Clear();
        await afterRestart.RunAsync(QueryStoreCollector.Instance, MakeServer("replica-host"), token);
        Assert.Null(await ReadBackfillStateAsync(rig, landedKey, token));
    }

    /// <summary>
    /// #9359: a delete of the stored entry whose write fails is returned again by the next cycle that does not clamp, and once a
    /// delete lands nothing more is returned. The failing write goes to a store nobody listens on, so it fails for certain.
    /// </summary>
    [Fact]
    public async Task AFailedDeleteOfTheStoredEntry_IsReturnedAgainNextCycle_AndOnceItLandsNothingMoreIs()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6254 runner pins.");
        var token = TestContext.Current.CancellationToken;
        var target = new Target();
        await using var rig = await Rig.OpenAsync(target, token);
        var landedKey = QueryStoreBackfillState.LandedKeyPrefix + Db;
        var raw = WholeSeconds(DateTime.UtcNow.AddHours(-3));

        await using var unreachable = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=darling;Database=darlingtest;Timeout=2;Command Timeout=2");
        var failing = new DarlingCollectorRunner(unreachable, new CollectorDeltaCalculator(), rig.Log);
        failing.QueryStoreClampReportsForTests.Land(LiveServerId, Db, raw, DateTime.UtcNow);
        var failed = failing.QueryStoreClampReportsForTests.Land(LiveServerId, Db, null, DateTime.UtcNow);
        Assert.NotNull(failed);
        await failing.PersistQueryStoreLandedAsync(LiveServerId, [failed!.Value], token);
        Assert.NotNull(failing.QueryStoreClampReportsForTests.Land(LiveServerId, Db, null, DateTime.UtcNow));

        /* The same sequence on the live store: the entry is written, retired by the first cycle that stops clamping, and then
           asked for no more. */
        var runner = new DarlingCollectorRunner(rig.Source, new CollectorDeltaCalculator(), rig.Log);
        var reports = runner.QueryStoreClampReportsForTests;
        await runner.PersistQueryStoreLandedAsync(LiveServerId, [reports.Land(LiveServerId, Db, raw, DateTime.UtcNow)!.Value], token);
        Assert.NotNull(await ReadBackfillStateAsync(rig, landedKey, token));

        var delete = reports.Land(LiveServerId, Db, null, DateTime.UtcNow);
        Assert.NotNull(delete);
        await runner.PersistQueryStoreLandedAsync(LiveServerId, [delete!.Value], token);
        Assert.Null(await ReadBackfillStateAsync(rig, landedKey, token));
        Assert.Null(reports.Land(LiveServerId, Db, null, DateTime.UtcNow));
    }

    /// <summary>
    /// #9359: every clamping database of a server stores its entry in one write, and the first cycle that stops clamping retires the
    /// entries of several databases in one write too, upserts and deletes together.
    /// </summary>
    [Fact]
    public async Task SeveralDatabasesLandedInOneCycle_AreStoredAndRetiredInOneWrite()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6254 runner pins.");
        var token = TestContext.Current.CancellationToken;
        var target = new Target();
        await using var rig = await Rig.OpenAsync(target, token);
        var runner = new DarlingCollectorRunner(rig.Source, new CollectorDeltaCalculator(), rig.Log);
        var reports = runner.QueryStoreClampReportsForTests;
        var raw = WholeSeconds(DateTime.UtcNow.AddHours(-3));
        var key = (string name) => QueryStoreBackfillState.LandedKeyPrefix + name;

        var landed = new List<QueryStoreLandedChange>
        {
            reports.Land(LiveServerId, "alpha", raw, DateTime.UtcNow)!.Value,
            reports.Land(LiveServerId, "beta", raw, DateTime.UtcNow)!.Value,
        };
        await runner.PersistQueryStoreLandedAsync(LiveServerId, landed, token);
        Assert.NotNull(await ReadBackfillStateAsync(rig, key("alpha"), token));
        Assert.NotNull(await ReadBackfillStateAsync(rig, key("beta"), token));

        /* Alpha stops clamping while a third database starts: one call carries the delete and the upsert. */
        var mixed = new List<QueryStoreLandedChange>
        {
            reports.Land(LiveServerId, "alpha", null, DateTime.UtcNow)!.Value,
            reports.Land(LiveServerId, "gamma", raw, DateTime.UtcNow)!.Value,
        };
        await runner.PersistQueryStoreLandedAsync(LiveServerId, mixed, token);
        Assert.Null(await ReadBackfillStateAsync(rig, key("alpha"), token));
        Assert.NotNull(await ReadBackfillStateAsync(rig, key("beta"), token));
        Assert.NotNull(await ReadBackfillStateAsync(rig, key("gamma"), token));
        Assert.Null(reports.Land(LiveServerId, "alpha", null, DateTime.UtcNow));
    }

    /// <summary>
    /// #9359: a real gap after a restart still opens its hole, at what the stored entry says the last cycle read.
    /// </summary>
    [Fact]
    public async Task ARealGapAfterARestart_OpensItsHoleAtTheStoredReadThrough_MinusTheLookBack()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6254 runner pins.");
        var token = TestContext.Current.CancellationToken;
        var target = new Target();
        await using var rig = await Rig.OpenAsync(target, token);

        target.Rows.Add(Row(1, WholeSeconds(DateTime.UtcNow.AddMinutes(-20))));
        Assert.Equal(1, (await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer("replica-host"), token)).Rows);

        var watermark = WholeSeconds(DateTime.UtcNow.AddHours(-3));
        var readThrough = WholeSeconds(DateTime.UtcNow.AddMinutes(-90));
        await SetStoredWatermarkAsync(rig, watermark, token);
        await WriteBackfillStateAsync(
            rig, QueryStoreBackfillState.LandedKeyPrefix + Db, QueryStoreBackfillState.EncodeLanded(watermark, readThrough), token);
        target.Rows.Clear();
        await RestartedRunner(rig, target).RunAsync(QueryStoreCollector.Instance, MakeServer("replica-host"), token);

        Assert.True(QueryStoreBackfillState.TryDecodeHoles(
            await ReadBackfillStateAsync(rig, QueryStoreBackfillState.HoleKeyPrefix + Db, token), out var holes));
        var hole = Assert.Single(holes);

        /* The look-back is 6 minutes here (a 60 second flush plus 5 minutes). */
        Assert.Equal(readThrough.AddMinutes(-6), hole.FromUtc);
    }

    /// <summary>
    /// T6: the setting is empty and no secondary captures: no replica connection opens, nothing reads the replica, the
    /// run reports no read source, and a row older than the watermark is NOT re-read (today's cutoff exactly).
    /// </summary>
    [Fact]
    public async Task WithTheSettingEmptyAndNoCapturingSecondary_NoReplicaConnectionOpens_AndTheCutoffIsTodays()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6254 runner pins.");
        var token = TestContext.Current.CancellationToken;
        var target = new Target();
        await using var rig = await Rig.OpenAsync(target, token);

        var w = WholeSeconds(DateTime.UtcNow.AddMinutes(-20));
        target.Rows.Add(Row(1, w));
        var first = await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer(null), token);
        target.Rows.Add(Row(2, w.AddMinutes(-3)));
        var second = await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer(null), token);

        Assert.Equal(1, first.Rows);
        Assert.Null(first.ReadSource);
        Assert.Null(second.ReadSource);
        Assert.DoesNotContain(ReplicaConnection, target.ConnectionsOpened);
        Assert.Equal(0, target.ReplicaReads);
        Assert.Equal(0, second.Rows);
        Assert.Equal(new Dictionary<long, long> { [1] = 1 }, await StoredAsync(rig, token));
    }

    /// <summary>
    /// #6279: the setting is empty, but rows arrive with a non-primary replica role (a capturing secondary's). The next read of
    /// that database uses the look-back: a late row below the watermark is stored, a shipped one is not written twice, and still
    /// no replica connection opens.
    /// </summary>
    [Fact]
    public async Task ACapturingSecondaryOnTheSettingEmptyPath_GetsTheLookBack_WithoutAReplicaConnection()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6254 runner pins.");
        var token = TestContext.Current.CancellationToken;
        var target = new Target();
        await using var rig = await Rig.OpenAsync(target, token);

        var w = WholeSeconds(DateTime.UtcNow.AddMinutes(-40));
        target.Rows.Add(Row(1, w, replicaRole: "Secondary"));
        await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer(null), token);

        /* Late by 15 minutes: inside the capturing look-back (900 s flush + 10 minutes = 25 minutes), far below W. */
        target.Rows.Add(Row(2, w.AddMinutes(-15), replicaRole: "Secondary"));
        var second = await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer(null), token);

        /* The guard was empty on the first look-back read (run 1 had none), so A is written once more and B, the late row, lands: the
           one rewrite at activation the design allows. From here the guard knows both. */
        Assert.Equal(new Dictionary<long, long> { [1] = 2, [2] = 1 }, await StoredAsync(rig, token));
        Assert.Equal(2, second.Rows);
        Assert.Null(second.ReadSource);

        var third = await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer(null), token);
        Assert.Equal(0, third.Rows);
        Assert.Equal(new Dictionary<long, long> { [1] = 2, [2] = 1 }, await StoredAsync(rig, token));
        Assert.DoesNotContain(ReplicaConnection, target.ConnectionsOpened);
    }

    /// <summary>A replica read that fails reads the primary in the same run: the rows are stored once, and the run names the reason.</summary>
    [Fact]
    public async Task AReplicaReadThatFails_IsRetriedOnThePrimaryInTheSameRun_AndSaysWhy()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6254 runner pins.");
        var token = TestContext.Current.CancellationToken;
        var target = new Target { ReplicaReadFails = true };
        await using var rig = await Rig.OpenAsync(target, token);

        target.Rows.Add(Row(1, WholeSeconds(DateTime.UtcNow.AddMinutes(-20))));
        var result = await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer("replica-host"), token);

        Assert.Equal(1, result.Rows);
        Assert.Equal(1, target.PrimaryReads);
        Assert.Equal("replica 0, primary 1 (replica_unavailable: " + Db + ")", result.ReadSource);
        Assert.Equal(new Dictionary<long, long> { [1] = 1 }, await StoredAsync(rig, token));
    }

    /// <summary>A replica that cannot be opened is every database on the primary, with the same reason, and the run still succeeds.</summary>
    [Fact]
    public async Task AReplicaThatCannotBeOpened_SendsEveryDatabaseToThePrimary()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6254 runner pins.");
        var token = TestContext.Current.CancellationToken;
        var target = new Target { ReplicaOpenFails = true };
        await using var rig = await Rig.OpenAsync(target, token);

        target.Rows.Add(Row(1, WholeSeconds(DateTime.UtcNow.AddMinutes(-20))));
        var result = await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer("replica-host"), token);

        Assert.Equal(1, result.Rows);
        Assert.Equal(0, target.ReplicaReads);
        Assert.Equal("replica 0, primary 1 (replica_unavailable: " + Db + ")", result.ReadSource);
    }

    /// <summary>The run's read source reaches <c>collection_log.read_source</c> through the one writer; NULL when none is given.</summary>
    [Fact]
    public async Task TheReadSource_IsWrittenToTheCollectionLog_AndIsNullWhenAbsent()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6254 runner pins.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(new Target(), token);
        var server = MakeServer("replica-host");

        await DarlingObservability.LogCollectionAsync(
            rig.Source, server, "query_store", "SUCCESS", 1, 1, 1, null, null, null, null, null, null, null, token,
            readSource: "replica 3, primary 1 (redo_behind: Sales)");
        await DarlingObservability.LogCollectionAsync(
            rig.Source, server, "wait_stats", "SUCCESS", 1, 1, 1, null, null, null, null, null, null, null, token);

        await using var connection = new NpgsqlConnection(rig.Scratch.ConnectionString);
        await connection.OpenAsync(token);
        await using var command = new NpgsqlCommand(
            "SELECT collector_name, read_source FROM collection_log WHERE server_id = @s ORDER BY collector_name", connection);
        command.Parameters.AddWithValue("s", LiveServerId);
        var seen = new Dictionary<string, string?>();
        await using var reader = await command.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
        {
            seen[reader.GetString(0)] = reader.IsDBNull(1) ? null : reader.GetString(1);
        }

        Assert.Equal("replica 3, primary 1 (redo_behind: Sales)", seen["query_store"]);
        Assert.Null(seen["wait_stats"]);
    }

    /// <summary>
    /// The runner's own wiring of the per-database watermark commit (#4661, #5684), driven through <c>RunAsync</c>: a database's rows
    /// are flushed to <c>collect.query_store_stats</c> first and ONLY THEN does the cache advance to the batch's newest row. The
    /// flush is held at a table lock; while it waits, the cache must still say what the store says (nothing). A runner that commits
    /// before the flush says the new row while the store has none, and a runner that never commits leaves the cache behind the store.
    /// </summary>
    [Fact]
    public async Task TheRunnerCommitsTheDatabaseWatermark_OnlyAfterTheFlush_AndDoesCommit()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6254 runner pins.");
        var token = TestContext.Current.CancellationToken;
        var target = new Target();
        await using var rig = await Rig.OpenAsync(target, token);
        var server = MakeServer(null);
        var w = WholeSeconds(DateTime.UtcNow.AddMinutes(-20));
        target.Rows.Add(Row(1, w));

        async Task<DateTime?> CachedAsync()
        {
            var now = DateTime.UtcNow;
            return await rig.Runner.ResolveQueryStoreDatabaseWatermarkAsync(
                server, "query_store_stats", "last_execution_time", "database_name", Db, WatermarkPolicy.ReadFloor(now)!.Value, now, token);
        }

        Task<CollectorRunResult> run;
        await using (var holder = new NpgsqlConnection(rig.Scratch.ConnectionString))
        {
            await holder.OpenAsync(token);
            await using var transaction = await holder.BeginTransactionAsync(token);
            await using (var lockCommand = new NpgsqlCommand("LOCK TABLE collect.query_store_stats IN SHARE ROW EXCLUSIVE MODE", holder, transaction))
            {
                await lockCommand.ExecuteNonQueryAsync(token);
            }

            run = rig.Runner.RunAsync(QueryStoreCollector.Instance, server, token);

            /* The flush is waiting on the lock: the rows are read and staged, nothing is stored. */
            var blocked = false;
            for (var attempt = 0; attempt < 200 && !blocked; attempt++)
            {
                await using var probe = new NpgsqlConnection(rig.Scratch.ConnectionString);
                await probe.OpenAsync(token);
                await using var waiting = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock' AND pid <> pg_backend_pid()", probe);
                blocked = Convert.ToInt64(await waiting.ExecuteScalarAsync(token), System.Globalization.CultureInfo.InvariantCulture) > 0;
                if (!blocked)
                {
                    Assert.False(run.IsCompleted, "the run finished without waiting on the table lock");
                    await Task.Delay(50, token);
                }
            }

            Assert.True(blocked, "the flush never reached the table lock");
            Assert.Null(await CachedAsync());
            await transaction.RollbackAsync(token);
        }

        var result = await run;

        Assert.Equal(1, result.Rows);
        Assert.Equal(new Dictionary<long, long> { [1] = 1 }, await StoredAsync(rig, token));
        Assert.Equal(w, await CachedAsync());
    }

    // ---- the fake target ----------------------------------------------------------------------------------------------------

    private sealed class FakeTargetProvider(Target target) : ITargetProvider
    {
        public CollectorTargetEngine Engine => CollectorTargetEngine.SqlServer;

        public DbConnection CreateConnection(string connectionString) => new FakeConnection(connectionString, target);

        public DbCommand CreateCommand(CollectorQuery query, DbConnection connection, int commandTimeoutSeconds) =>
            new FakeCommand((FakeConnection)connection, target, query);

        public CollectorTargetFault Classify(Exception exception, bool yieldsOnLockTimeout) => CollectorTargetFault.Unclassified;

        public string WithDatabase(string connectionString, string databaseName) => connectionString;

        public (string ConnectionString, CollectorQuery Query) BuildDatabaseListPlan(
            string connectionString, IReadOnlyList<string>? excludedDatabases, IReadOnlyList<string>? databaseScope) =>
            throw new NotSupportedException();
    }

    private sealed class FakeConnection(string connectionString, Target target) : DbConnection
    {
        private string _cs = connectionString;
        public bool IsReplica => _cs == ReplicaConnection;

        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ConnectionString { get => _cs; set => _cs = value ?? string.Empty; }
        public override string Database => string.Empty;
        public override string DataSource => string.Empty;
        public override string ServerVersion => string.Empty;
        public override ConnectionState State => ConnectionState.Open;
        public override void ChangeDatabase(string databaseName) { }
        public override void Close() { }
        public override void Open() { }

        public override Task OpenAsync(CancellationToken cancellationToken)
        {
            lock (target.ConnectionsOpened)
            {
                target.ConnectionsOpened.Add(_cs);
            }

            return IsReplica && target.ReplicaOpenFails
                ? Task.FromException(new InvalidOperationException("the replica is down"))
                : Task.CompletedTask;
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
        protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
    }

    private sealed class FakeCommand(FakeConnection owner, Target target, CollectorQuery query) : DbCommand
    {
        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string CommandText { get; set; } = string.Empty;
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }
        protected override DbConnection? DbConnection { get; set; }
        protected override DbParameterCollection DbParameterCollection => throw new NotSupportedException();
        protected override DbTransaction? DbTransaction { get; set; }
        public override void Cancel() { }
        public override int ExecuteNonQuery() => throw new NotSupportedException();
        public override object? ExecuteScalar() =>
            query.Text == QueryStoreCollector.ProductVersionProbeText ? 16 : throw new NotSupportedException();
        public override void Prepare() { }
        protected override DbParameter CreateDbParameter() => throw new NotSupportedException();
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new NotSupportedException();

        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            var text = query.Text;
            if (text == QueryStoreCollector.ProductVersionProbeText)
            {
                return Task.FromResult<DbDataReader>(new Reader([[16]]));
            }

            if (text.Contains("sys.dm_hadr_database_replica_states", StringComparison.Ordinal))
            {
                var redo = new DataTable();
                redo.Columns.Add("database_name", typeof(string));
                redo.Columns.Add("group_database_id", typeof(Guid));
                redo.Columns.Add("last_commit_time", typeof(DateTime));
                redo.Columns.Add("is_primary_replica", typeof(bool));
                redo.Rows.Add(Db, GroupDatabaseId, target.CommitTime, !owner.IsReplica);
                return Task.FromResult<DbDataReader>(redo.CreateDataReader());
            }

            var cutoff = query.Parameters?.FirstOrDefault(static p => p.Name == "@cutoff_time")?.Value as DateTime?;
            if (cutoff is null && text.Contains("sys.databases", StringComparison.Ordinal))
            {
                return Task.FromResult<DbDataReader>(Enumeration(owner.IsReplica));
            }

            if (cutoff is null)
            {
                /* The plan-XML and statement-text fetches: nothing to fetch for these rows. */
                return Task.FromResult<DbDataReader>(new Reader(Array.Empty<object[]>()));
            }

            if (owner.IsReplica)
            {
                Interlocked.Increment(ref target.ReplicaReads);
                if (target.ReplicaReadFails)
                {
                    return Task.FromException<DbDataReader>(new InvalidOperationException("the replica read failed"));
                }
            }
            else
            {
                Interlocked.Increment(ref target.PrimaryReads);
            }

            lock (target.Cutoffs)
            {
                target.Cutoffs.Add(cutoff.Value);
            }

            /* The real predicate: last_execution_time > @cutoff_time. */
            var floor = DateTime.SpecifyKind(cutoff.Value, DateTimeKind.Utc);
            return Task.FromResult<DbDataReader>(
                new Reader(target.Rows.Where(r => ((DateTimeOffset)r[4]).UtcDateTime > floor).ToArray()));
        }

        /// <summary>The database list: the primary's three result sets (names, probe failures, flush facts), or the replica's two.</summary>
        private DbDataReader Enumeration(bool replica)
        {
            var failures = new DataTable();
            failures.Columns.Add("item_name", typeof(string));
            failures.Columns.Add("error_text", typeof(string));
            if (replica)
            {
                var list = new DataTable();
                list.Columns.Add("name", typeof(string));
                list.Columns.Add("actual_state", typeof(int));
                list.Columns.Add("readonly_reason", typeof(int));
                list.Columns.Add("flush_interval_seconds", typeof(int));
                list.Rows.Add(Db, 0, 8, target.FlushSeconds);
                return new DataTableReader([list, failures]);
            }

            var names = new DataTable();
            names.Columns.Add("name", typeof(string));
            var facts = new DataTable();
            facts.Columns.Add("name", typeof(string));
            facts.Columns.Add("flush_interval_seconds", typeof(int));
            names.Rows.Add(Db);
            facts.Rows.Add(Db, target.FlushSeconds);
            return new DataTableReader([names, failures, facts]);
        }
    }

    private sealed class Reader(object[][] rows) : DbDataReader
    {
        private int _row = -1;
        private object[] Cur => rows[_row];

        public override bool Read() => ++_row < rows.Length;
        public override bool NextResult() => false;
        public override string GetString(int o) => (string)Cur[o];
        public override long GetInt64(int o) => Convert.ToInt64(Cur[o], System.Globalization.CultureInfo.InvariantCulture);
        public override DateTime GetDateTime(int o) => (DateTime)Cur[o];
        public override bool GetBoolean(int o) => (bool)Cur[o];
        public override bool IsDBNull(int o) => Cur[o] is DBNull;
        public override object GetValue(int o) => Cur[o];
        public override int FieldCount => rows.Length == 0 ? 0 : rows[0].Length;
        public override bool HasRows => rows.Length > 0;
        public override bool IsClosed => false;
        public override int Depth => 0;
        public override int RecordsAffected => -1;
        public override object this[int o] => Cur[o];
        public override object this[string n] => throw new NotSupportedException();
        public override byte GetByte(int o) => throw new NotSupportedException();
        public override long GetBytes(int o, long d, byte[]? b, int bo, int l) => throw new NotSupportedException();
        public override char GetChar(int o) => throw new NotSupportedException();
        public override long GetChars(int o, long d, char[]? b, int bo, int l) => throw new NotSupportedException();
        public override string GetDataTypeName(int o) => throw new NotSupportedException();
        public override decimal GetDecimal(int o) => throw new NotSupportedException();
        public override double GetDouble(int o) => throw new NotSupportedException();
        public override IEnumerator GetEnumerator() => throw new NotSupportedException();
        public override Type GetFieldType(int o) => throw new NotSupportedException();
        public override float GetFloat(int o) => throw new NotSupportedException();
        public override Guid GetGuid(int o) => throw new NotSupportedException();
        public override short GetInt16(int o) => throw new NotSupportedException();
        public override int GetInt32(int o) => throw new NotSupportedException();
        public override string GetName(int o) => throw new NotSupportedException();
        public override int GetOrdinal(string n) => throw new NotSupportedException();
        public override int GetValues(object[] v) => throw new NotSupportedException();
    }
}
