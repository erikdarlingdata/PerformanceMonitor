/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8884: the store side of the Instance Restarted and Memory Dumps alerts. The judges that read the identity pair and the memory dump stamp from
/// <c>collector_state</c> (the cases the alert rests on: the first observation, a restart, a failover, a rename, a rise, a drop), the collector's
/// observation of the dump stamp (change-only, a NULL stamp observes nothing), and the DuckDB execution of the shared read through Lite's own adapter query.
/// </summary>
public sealed class InstanceEventsTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private static readonly DateTime Persisted = new(2026, 11, 16, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime OldStart = new(2026, 11, 1, 6, 0, 0);
    private static readonly DateTime NewStart = new(2026, 11, 16, 11, 58, 0);

    private static BaselineDiscontinuities.StateRow Row(string collector, string key, string value) =>
        new(collector, key, value, Persisted);

    private static BaselineDiscontinuities.StateRow Identity(string collector, string key, DateTime start, string? name) =>
        Row(collector, key, ServerEpoch.Serialize(new ServerEpoch.Stamp(start, name)));

    private static List<BaselineDiscontinuities.StateRow> Pair(DateTime before, string? beforeName, DateTime now, string? nowName, string collector = "wait_stats") =>
        new()
        {
            Identity(collector, ServerEpoch.IdentityPreviousStateKey, before, beforeName),
            Identity(collector, ServerEpoch.IdentityStateKey, now, nowName),
        };

    [Fact]
    public void Restart_FirstObservation_HasNoPreviousPair_AndIsNotARestart()
    {
        var rows = new List<BaselineDiscontinuities.StateRow> { Identity("wait_stats", ServerEpoch.IdentityStateKey, NewStart, "SRV1") };

        Assert.Null(InstanceEvents.JudgeRestart(rows));
    }

    [Fact]
    public void Restart_LaterStartTime_SameName_IsARestart_AndNamesBothStartTimes()
    {
        var found = InstanceEvents.JudgeRestart(Pair(OldStart, "SRV1", NewStart, "srv1"));

        Assert.NotNull(found);
        Assert.Equal(OldStart, found!.Value.PreviousStartTime);
        Assert.Equal(NewStart, found.Value.NewStartTime);
        Assert.Equal(Persisted, found.Value.ObservedAtUtc);
    }

    [Fact]
    public void Restart_NameUnknownOnOneSide_IsStillARestart()
    {
        Assert.NotNull(InstanceEvents.JudgeRestart(Pair(OldStart, null, NewStart, "SRV1")));
    }

    [Fact]
    public void Restart_Failover_RenameAndAnEarlierStartTime_AreNotRestarts()
    {
        Assert.Null(InstanceEvents.JudgeRestart(Pair(OldStart, "NODE1", NewStart, "NODE2")));
        Assert.Null(InstanceEvents.JudgeRestart(Pair(OldStart, "SRV1", OldStart, "SRV2")));
        Assert.Null(InstanceEvents.JudgeRestart(Pair(NewStart, "SRV1", OldStart, "SRV1")));
    }

    [Fact]
    public void Dumps_ARise_IsFound_WithCountsFromTheStamp()
    {
        var before = new InstanceEvents.MemoryDumpStamp(Persisted.AddDays(-3), 2, 0);
        var now = new InstanceEvents.MemoryDumpStamp(Persisted.AddMinutes(-2), 4, 3);
        var rows = new List<BaselineDiscontinuities.StateRow>
        {
            Row("cpu_utilization", InstanceEvents.MemoryDumpsPreviousStateKey, InstanceEvents.SerializeDumps(before)),
            Row("cpu_utilization", InstanceEvents.MemoryDumpsStateKey, InstanceEvents.SerializeDumps(now)),
        };

        var info = InstanceEventInfoMapper.ToInstanceEventInfo(rows).MemoryDumps;

        Assert.NotNull(info);
        Assert.Equal(now.NewestUtc, info!.NewestDumpUtc);
        Assert.Equal(4, info.TotalCount);
        Assert.Equal(3, info.RecentCount);
        Assert.Equal(2, info.NewCount);
    }

    /// <summary>Runs the collector's own observation for each stamp in turn, landing its staged state between passes the way the host does, and returns
    /// the rows the store would then hold for the cpu_utilization collector. Returns each pass's answer through <paramref name="rose"/>.</summary>
    private static List<BaselineDiscontinuities.StateRow> RunTheCollector(List<bool> rose, params InstanceEvents.MemoryDumpStamp?[] passes)
    {
        var state = new Dictionary<string, string>();
        foreach (var pass in passes)
        {
            var context = Context(state);
            rose.Add(InstanceEvents.ObserveMemoryDumps(context, pass));
            foreach (var staged in context.PendingState)
            {
                state[staged.Key] = staged.Value;
            }
        }

        return state.Select(kv => Row("cpu_utilization", kv.Key, kv.Value)).ToList();
    }

    [Fact]
    public void Dumps_FirstObservation_ADrop_AndANoDumpStamp_AreNotARise()
    {
        var dump = new InstanceEvents.MemoryDumpStamp(Persisted.AddDays(-3), 5, 0);
        var first = new List<BaselineDiscontinuities.StateRow> { Row("cpu_utilization", InstanceEvents.MemoryDumpsStateKey, InstanceEvents.SerializeDumps(dump)) };
        Assert.Null(InstanceEvents.JudgeMemoryDumps(first));

        /* The collector's real sequence: a first observation, a rise that never fired (the alert was off, or the host was down), then a drop with no newer
           dump. The pair the collector leaves must not read as a rise: it describes the latest change, the drop. */
        var before = new InstanceEvents.MemoryDumpStamp(Persisted.AddDays(-30), 2, 0);
        var risen = new InstanceEvents.MemoryDumpStamp(Persisted.AddMinutes(-20), 5, 0);
        var rose = new List<bool>();
        var drop = RunTheCollector(rose, before, risen, risen with { Count = 2 });
        Assert.Equal(new[] { false, true, false }, rose);
        Assert.Null(InstanceEvents.JudgeMemoryDumps(drop));
        Assert.Null(InstanceEventInfoMapper.ToInstanceEventInfo(drop).MemoryDumps);

        /* And the pair after the rise alone is still the rise (the control: the same helper, one pass fewer). */
        var riseOnly = RunTheCollector(new List<bool>(), before, risen);
        Assert.NotNull(InstanceEvents.JudgeMemoryDumps(riseOnly));

        var none = new List<BaselineDiscontinuities.StateRow>
        {
            Row("cpu_utilization", InstanceEvents.MemoryDumpsPreviousStateKey, InstanceEvents.SerializeDumps(dump)),
            Row("cpu_utilization", InstanceEvents.MemoryDumpsStateKey, InstanceEvents.SerializeDumps(new InstanceEvents.MemoryDumpStamp(null, 0, 0))),
        };
        Assert.Null(InstanceEvents.JudgeMemoryDumps(none));
    }

    [Fact]
    public void Stamp_RoundTrips_AndAMalformedValueIsNoStamp()
    {
        var stamp = new InstanceEvents.MemoryDumpStamp(Persisted, 7, 3);
        Assert.True(InstanceEvents.TryParseDumps(InstanceEvents.SerializeDumps(stamp), out var back));
        Assert.Equal(stamp, back);
        Assert.True(InstanceEvents.TryParseDumps(InstanceEvents.SerializeDumps(new InstanceEvents.MemoryDumpStamp(null, 0, 0)), out var empty));
        Assert.Null(empty.NewestUtc);
        Assert.False(InstanceEvents.TryParseDumps("garbage", out _));
        Assert.False(InstanceEvents.TryParseDumps(null, out _));
    }

    private static CollectorContext Context(Dictionary<string, string> state) => new()
    {
        ServerId = 7,
        ServerName = "test-server",
        CollectionTime = Persisted,
        Deltas = new CollectorDeltaCalculator(),
        Target = new CollectorTargetInfo(),
        State = state,
    };

    [Fact]
    public void Observe_FirstObservation_OnlyRecordsTheStamp()
    {
        var context = Context(new Dictionary<string, string>());

        Assert.False(InstanceEvents.ObserveMemoryDumps(context, new InstanceEvents.MemoryDumpStamp(Persisted, 3, 1)));

        Assert.True(context.PendingState.ContainsKey(InstanceEvents.MemoryDumpsStateKey));
        Assert.False(context.PendingState.ContainsKey(InstanceEvents.MemoryDumpsPreviousStateKey));
        Assert.Empty(context.Measurements);
    }

    [Fact]
    public void Observe_ARise_StagesThePreviousStamp_AndMeasuresTheNewDumps()
    {
        var prior = new InstanceEvents.MemoryDumpStamp(Persisted.AddDays(-1), 2, 0);
        var context = Context(new Dictionary<string, string> { [InstanceEvents.MemoryDumpsStateKey] = InstanceEvents.SerializeDumps(prior) });

        Assert.True(InstanceEvents.ObserveMemoryDumps(context, new InstanceEvents.MemoryDumpStamp(Persisted, 4, 2)));

        Assert.Equal(InstanceEvents.SerializeDumps(prior), context.PendingState[InstanceEvents.MemoryDumpsPreviousStateKey]);
        var measurement = Assert.Single(context.Measurements);
        Assert.Equal(InstanceEvents.MemoryDumpsNewMeasurement, measurement.Label);
        Assert.Equal(2, measurement.Value);
    }

    [Fact]
    public void Observe_SteadyState_WritesNothing_AndANullStampObservesNothing()
    {
        var stamp = new InstanceEvents.MemoryDumpStamp(Persisted.AddDays(-1), 2, 0);
        var state = new Dictionary<string, string> { [InstanceEvents.MemoryDumpsStateKey] = InstanceEvents.SerializeDumps(stamp) };

        var steady = Context(state);
        Assert.False(InstanceEvents.ObserveMemoryDumps(steady, stamp with { RecentCount = 0 }));
        Assert.Empty(steady.PendingState);

        var failed = Context(state);
        Assert.False(InstanceEvents.ObserveMemoryDumps(failed, null));
        Assert.Empty(failed.PendingState);
        Assert.Empty(failed.Measurements);
    }

    [Fact]
    public void Observe_ADrop_IsRecorded_ButIsNotARise()
    {
        var prior = new InstanceEvents.MemoryDumpStamp(Persisted.AddDays(-1), 5, 0);
        var context = Context(new Dictionary<string, string> { [InstanceEvents.MemoryDumpsStateKey] = InstanceEvents.SerializeDumps(prior) });

        Assert.False(InstanceEvents.ObserveMemoryDumps(context, prior with { Count = 2 }));

        Assert.True(context.PendingState.ContainsKey(InstanceEvents.MemoryDumpsStateKey));
        /* The replaced stamp goes to the previous key too, so the pair describes the drop and is never read as a rise. */
        Assert.Equal(InstanceEvents.SerializeDumps(prior), context.PendingState[InstanceEvents.MemoryDumpsPreviousStateKey]);
    }

    /* ---------------- the DuckDB execution ---------------- */

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private const int ServerId = 424242;

    public InstanceEventsTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private async Task SeedStateAsync(string collector, string key, string value, DateTime updatedAtUtc)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _seedConn ??= _duckDb.CreateConnection();
        if (_seedConn.State != System.Data.ConnectionState.Open)
        {
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT OR REPLACE INTO collector_state (server_id, collector_name, state_key, state_value, updated_at)
VALUES ($1, $2, $3, $4, $5)";
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = key });
        cmd.Parameters.Add(new DuckDBParameter { Value = value });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(updatedAtUtc, DateTimeKind.Unspecified) });
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task TheAdapterRead_FindsTheSeededRestartAndDumpRise_AndNothingForAnotherServer()
    {
        var service = new LocalDataService(_duckDb);
        Assert.Null((await service.GetInstanceEventsAsync(ServerId)).Restart);

        var observed = DateTime.UtcNow.AddMinutes(-3);
        observed = new DateTime(observed.Ticks - observed.Ticks % TimeSpan.TicksPerSecond, DateTimeKind.Utc);
        await SeedStateAsync("wait_stats", ServerEpoch.IdentityPreviousStateKey, ServerEpoch.Serialize(new(OldStart, "SRV1")), observed);
        await SeedStateAsync("wait_stats", ServerEpoch.IdentityStateKey, ServerEpoch.Serialize(new(NewStart, "SRV1")), observed);
        await SeedStateAsync("cpu_utilization", InstanceEvents.MemoryDumpsPreviousStateKey, InstanceEvents.SerializeDumps(new(observed.AddDays(-2), 1, 0)), observed);
        await SeedStateAsync("cpu_utilization", InstanceEvents.MemoryDumpsStateKey, InstanceEvents.SerializeDumps(new(observed.AddMinutes(-1), 4, 3)), observed);

        var info = await service.GetInstanceEventsAsync(ServerId);

        Assert.NotNull(info.Restart);
        Assert.Equal(OldStart, info.Restart!.PreviousStartTime);
        Assert.Equal(NewStart, info.Restart.NewStartTime);
        Assert.Equal(observed, info.Restart.ObservedAtUtc);
        Assert.NotNull(info.MemoryDumps);
        Assert.Equal(4, info.MemoryDumps!.TotalCount);
        Assert.Equal(3, info.MemoryDumps.RecentCount);
        Assert.Equal(3, info.MemoryDumps.NewCount);

        var other = await service.GetInstanceEventsAsync(ServerId + 1);
        Assert.Null(other.Restart);
        Assert.Null(other.MemoryDumps);
    }
}
