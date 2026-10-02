/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Real-DuckDB round trip for Lite's forced-plan-failures read (#2157), replaying the #3579 production
/// series: a forced plan whose <c>force_failure_count</c> went 0 → 0 → 1 → 0 across four collections. The
/// read must return the plan exactly while the newest sighting is the rise, must carry that sighting's
/// <c>collection_time</c> as the observation stamp (the fact the engine keys its once-per-observation guard
/// on), and must return nothing once the next collection lands with the counter reset — the recovery.
///
/// <para>Two things a text pin cannot prove and this does: that DuckDB's <c>TIMESTAMP</c> comes back
/// through <c>GetDateTime(7)</c> at the ordinal the reader binds, and that the value round-trips to the
/// tick — the engine compares one plan's stamps for equality, so a stamp that came back shifted or
/// truncated would either never match (cooldown-repeat returns) or match a different collection.</para>
/// </summary>
public sealed class ForcePlanFailuresReadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 3579;
    private const string Db = "ForcedDb";

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public ForcePlanFailuresReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    /* Whole seconds by construction: DuckDB TIMESTAMP is microsecond-resolution, so raw DateTime ticks
       would not survive the round trip and the tick-equality below would be testing the wrong thing.
       Kind Unspecified, the store's naive-UTC convention. Inside the read's two-hour window. */
    private static readonly DateTime Base = MinuteFloor(DateTime.UtcNow.AddMinutes(-70));

    private static DateTime MinuteFloor(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    /// <summary>One collection of one forced plan, two interval rows (the forcing columns are plan-level and
    /// repeat across a plan's rows — the read's MAX collapse has to have something to collapse).</summary>
    private async Task SeedCollectionAsync(DateTime collectionTime, long failures, bool forced, string forcingType, string? reason)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        for (var interval = 0; interval < 2; interval++)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_text, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
     query_plan_hash, is_forced_plan, force_failure_count, plan_forcing_type, last_force_failure_reason,
     runtime_stats_interval_id, interval_start_time_utc)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)";
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
            cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = "ForcedSrv" });
            cmd.Parameters.Add(new DuckDBParameter { Value = Db });
            cmd.Parameters.Add(new DuckDBParameter { Value = 11L });
            cmd.Parameters.Add(new DuckDBParameter { Value = 22L });
            cmd.Parameters.Add(new DuckDBParameter { Value = "Regular" });
            cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime.AddMinutes(-10 - interval) });
            cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
            cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT 11" });
            cmd.Parameters.Add(new DuckDBParameter { Value = "0xHASH11" });
            cmd.Parameters.Add(new DuckDBParameter { Value = 100L + interval });
            cmd.Parameters.Add(new DuckDBParameter { Value = 500L });
            cmd.Parameters.Add(new DuckDBParameter { Value = 900L });
            cmd.Parameters.Add(new DuckDBParameter { Value = 40L });
            cmd.Parameters.Add(new DuckDBParameter { Value = 0L });
            cmd.Parameters.Add(new DuckDBParameter { Value = 0L });
            cmd.Parameters.Add(new DuckDBParameter { Value = "0xPLAN22" });
            cmd.Parameters.Add(new DuckDBParameter { Value = forced });
            cmd.Parameters.Add(new DuckDBParameter { Value = failures });
            cmd.Parameters.Add(new DuckDBParameter { Value = forcingType });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)reason ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = 1000L + interval });
            cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime.AddMinutes(-10 - interval) });
            await cmd.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task TheRise_IsReturnedOnce_StampedWithTheNewestCollection_AndVanishesWhenTheCounterResets()
    {
        var service = new LocalDataService(_duckDb);

        /* 03:47 and 04:02 of the series: not forced, counter flat at zero. Nothing to report. */
        await SeedCollectionAsync(Base, failures: 0, forced: false, "NONE", null);
        await SeedCollectionAsync(Base.AddMinutes(15), failures: 0, forced: false, "NONE", null);
        Assert.Empty(await service.GetForcePlanFailuresAsync(ServerId));

        /* 04:18: Automatic Plan Correction forced the plan and the force failed once. */
        var riseCollection = Base.AddMinutes(31);
        await SeedCollectionAsync(riseCollection, failures: 1, forced: true, "AUTO", "GENERAL_FAILURE");

        var row = Assert.Single(await service.GetForcePlanFailuresAsync(ServerId));
        Assert.Equal(Db, row.DatabaseName);
        Assert.Equal(11L, row.QueryId);
        Assert.Equal(22L, row.PlanId);
        Assert.Equal("AUTO", row.ForcingType);
        Assert.Equal("GENERAL_FAILURE", row.FailureReason);
        Assert.Equal(1L, row.FailureDelta);
        Assert.Equal(1L, row.TotalFailures);
        /* #3579: the observation's identity is the newer sighting's collection_time, to the tick, Kind Utc. */
        Assert.Equal((DateTime?)riseCollection, row.ObservedAtUtc);
        Assert.Equal(DateTimeKind.Utc, row.ObservedAtUtc!.Value.Kind);

        /* Re-reading between collections is the same observation: same row, same stamp. This is the read the
           engine used to fire on six times; the stamp is what lets it recognise the repeat. */
        var reread = Assert.Single(await service.GetForcePlanFailuresAsync(ServerId));
        Assert.Equal(row.ObservedAtUtc, reread.ObservedAtUtc);

        /* 04:50: APC released the forcing; the counter reset to zero with it. A LOWER counter is silence. */
        await SeedCollectionAsync(Base.AddMinutes(63), failures: 0, forced: false, "NONE", null);
        Assert.Empty(await service.GetForcePlanFailuresAsync(ServerId));
    }
}
