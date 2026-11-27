/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using PerformanceMonitorLite.Tests.Helpers;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #6254 and #6279 through Lite's real definition runner (<c>RunCollectorAsync</c>, the enumerated arm), with the primary
/// and the replica supplied as reader seams that apply the cutoff the way the SQL does (<c>last_execution_time &gt;
/// @cutoff_time</c>): a replica read starts at W - L and the ship guard keeps the re-read rows from being stored twice,
/// a late row below the watermark is stored, an empty setting opens nothing, and a database a capturing secondary feeds
/// gets the same look-back with no replica involved.
/// </summary>
public sealed class QueryStoreReplicaRunnerTests : IDisposable
{
    private const string Sales = "Sales";
    private const string Orders = "Orders";
    private static readonly Guid GroupDatabaseId = Guid.Parse("7a88cd1f-6fcf-444c-a320-feb92dfbd0d2");

    private readonly List<DuckDbInitializer> _initializers = [];
    private readonly string _tempDir;
    private readonly string _configDir;

    public QueryStoreReplicaRunnerTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        _configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(_configDir);
    }

    public void Dispose()
    {
        foreach (var initializer in _initializers)
        {
            initializer.Dispose();
        }

        try
        {
            if (Directory.Exists(_tempDir))
            {
                Directory.Delete(_tempDir, recursive: true);
            }
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    [Fact]
    public async Task AReplicaRead_StartsAtWatermarkMinusLookBack_StoresTheLateRow_AndNotTheReReadOne()
    {
        var f = await BuildAsync(replica: "replica-host");
        var now = Truncate(DateTime.UtcNow);
        var a = Row(queryId: 1, lastExecution: now.AddMinutes(-20));
        var b = Row(queryId: 2, lastExecution: now.AddMinutes(-28));
        f.ReplicaRows = [a];

        await RunAsync(f);
        Assert.Equal(1, await CountAsync(f));

        /* Run 2: the replica now shows A again and B, a row that became visible late with an older execution time (below
           the watermark A set). A cutoff of W would never ask for B; W - L (the flush interval plus 5 minutes) does. */
        f.ReplicaRows = [a, b];
        await RunAsync(f);

        Assert.Equal(2, await CountAsync(f));
        Assert.Equal(1, await CountAsync(f, queryId: 1));
        Assert.Equal(1, await CountAsync(f, queryId: 2));
        var cutoffs = f.ReplicaItemCutoffs;
        Assert.Equal(2, cutoffs.Count);
        Assert.Equal(a.LastExecution - TimeSpan.FromMinutes(20), cutoffs[1]);
        Assert.Empty(f.PrimaryItemCutoffs);
        Assert.Equal(2, f.Service.QueryStoreReplicaConnectionsOpened);
        Assert.StartsWith("replica 1", await ReadSourceAsync(f), StringComparison.Ordinal);
    }

    [Fact]
    public async Task ASettingThatIsEmpty_OpensNoReplicaConnection_ReadsThePrimaryFromTheWatermark_AndWritesNoReadSource()
    {
        var f = await BuildAsync(replica: null);
        var now = Truncate(DateTime.UtcNow);
        var a = Row(queryId: 1, lastExecution: now.AddMinutes(-20));
        f.PrimaryRows = [a];

        await RunAsync(f);
        await RunAsync(f);

        Assert.Equal(0, f.Service.QueryStoreReplicaConnectionsOpened);
        Assert.Equal(0, f.ReplicaCalls);
        Assert.Equal(1, await CountAsync(f));
        Assert.Equal(a.LastExecution, f.PrimaryItemCutoffs[1]);
        Assert.Null(await ReadSourceAsync(f));
    }

    [Fact]
    public async Task ACapturingSecondary_OnAServerWithNoReplicaSet_GetsTheLookBack_AndOpensNoReplicaConnection()
    {
        var f = await BuildAsync(replica: null);
        var now = Truncate(DateTime.UtcNow);
        var a = Row(queryId: 1, lastExecution: now.AddMinutes(-20), role: "Secondary");
        var late = Row(queryId: 2, lastExecution: now.AddMinutes(-40), role: "Secondary");
        f.PrimaryRows = [a];
        await RunAsync(f);

        /* The first read showed a row of another replica group: the database is now known to have a capturing secondary,
           so the next read starts at W - (flush interval + 10 minutes) and finds the late row. */
        f.PrimaryRows = [a, late];
        await RunAsync(f);

        Assert.Equal(0, f.Service.QueryStoreReplicaConnectionsOpened);
        Assert.Equal(a.LastExecution - TimeSpan.FromMinutes(25), f.PrimaryItemCutoffs[1]);
        Assert.Equal(1, await CountAsync(f, queryId: 2));

        /* The first look-back read writes the window once more (the guard learned nothing before it); from then on the guard
           keeps every re-read row out, so a third run with the same rows adds nothing. */
        var storedAfterSecondRun = await CountAsync(f);
        await RunAsync(f);
        Assert.Equal(storedAfterSecondRun, await CountAsync(f));
        Assert.Equal(1, await CountAsync(f, queryId: 2));
        Assert.Null(await ReadSourceAsync(f));
    }

    [Fact]
    public async Task AReplicaThatCannotBeEnumerated_SendsEveryDatabaseToThePrimary_AndSaysSo()
    {
        var f = await BuildAsync(replica: "replica-host");
        var now = Truncate(DateTime.UtcNow);
        f.PrimaryRows = [Row(queryId: 1, lastExecution: now.AddMinutes(-20))];
        f.ReplicaFails = true;

        await RunAsync(f);

        Assert.Equal(1, await CountAsync(f));
        Assert.Empty(f.ReplicaItemCutoffs);
        var source = await ReadSourceAsync(f);
        Assert.Contains("primary 1", source, StringComparison.Ordinal);
        Assert.Contains("replica_unavailable", source, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AzureSqlDatabase_IgnoresTheSetting_NoReplicaConnection_NoReadSource()
    {
        var f = await BuildAsync(replica: "replica-host", engineEdition: 5);
        f.Service.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { Sales });
        var azureReads = 0;
        f.Service.AzureDatabaseReaderOverrideForTests = (_, _) =>
        {
            Interlocked.Increment(ref azureReads);
            return new DataTable().CreateDataReader();
        };

        await RunAsync(f);

        /* The run really went through the Azure per-database arm and was logged, so the null below is not an absent run. */
        Assert.True(azureReads > 0, "the Azure per-database arm never read");
        Assert.Equal(1, await LogRowsAsync(f));
        Assert.Equal(0, f.Service.QueryStoreReplicaConnectionsOpened);
        Assert.Equal(0, f.ReplicaCalls);
        Assert.Null(await ReadSourceAsync(f));
    }

    [Fact]
    public async Task AReplicaReadThatFailsForOneDatabase_SendsThatDatabaseToThePrimary_WhileTheOthersStillReadTheReplica()
    {
        var f = await BuildAsync(replica: "replica-host");
        f.Databases.Add(Orders);
        var now = Truncate(DateTime.UtcNow);
        f.ReplicaRowsByDb[Sales] = [Row(queryId: 1, lastExecution: now.AddMinutes(-20))];
        f.ReplicaRowsByDb[Orders] = [Row(queryId: 10, lastExecution: now.AddMinutes(-20))];
        f.PrimaryRowsByDb[Orders] = [Row(queryId: 20, lastExecution: now.AddMinutes(-18))];
        f.ReplicaItemReadFails.Add(Orders);

        await RunAsync(f);

        /* Sales read the replica and nothing else; Orders tried the replica, failed, and was read from the primary in the
           same run (the primary's row is stored, the replica's is not). */
        Assert.Equal(1, await CountAsync(f, queryId: 1));
        Assert.Single(f.ReplicaItemCutoffs);
        Assert.Empty(f.PrimaryItemCutoffs);
        Assert.Single(Fixture.CutoffsOf(f.ReplicaCutoffsByDb, Orders));
        Assert.Single(Fixture.CutoffsOf(f.PrimaryCutoffsByDb, Orders));
        Assert.Equal(1, await CountAsync(f, queryId: 20, database: Orders));
        Assert.Equal(0, await CountAsync(f, queryId: 10, database: Orders));

        /* The run records which database fell back, and why. */
        var source = await ReadSourceAsync(f);
        Assert.StartsWith("replica 1, primary 1", source, StringComparison.Ordinal);
        Assert.Contains("replica_unavailable", source, StringComparison.Ordinal);
        Assert.Contains(Orders, source, StringComparison.Ordinal);

        /* The fallback is for the run: the next run tries the replica for Orders again. */
        await RunAsync(f);
        Assert.Equal(2, Fixture.CutoffsOf(f.ReplicaCutoffsByDb, Orders).Count);
        Assert.Equal(2, f.ReplicaItemCutoffs.Count);
    }

    [Fact]
    public async Task AReplicaReadOfAWatermarkOlderThanTheCatchUp_StartsAtTheClamp_AndLeavesTheLookBackWindowAsAHole_NotReadTwice()
    {
        var f = await BuildAsync(replica: "replica-host");
        var now = Truncate(DateTime.UtcNow);
        f.ReplicaRows = [Row(queryId: 1, lastExecution: now.AddMinutes(-30))];
        await RunAsync(f);

        /* The stored watermark W is 2 hours old: older than the 1-hour catch-up, so the clamp fires on the RAW value. */
        var watermark = now.AddHours(-2);
        await SetWatermarkAsync(f, watermark);

        var late = Row(queryId: 2, lastExecution: watermark.AddMinutes(-10));        /* inside (W - L, W): the look-back would have re-read it */
        var between = Row(queryId: 3, lastExecution: watermark.AddMinutes(30));      /* between W and the clamp: the clamp skips it, as before */
        var fresh = Row(queryId: 4, lastExecution: now.AddMinutes(-45));            /* after the clamp: the run's own work */
        f.ReplicaRows = [Row(queryId: 1, lastExecution: now.AddMinutes(-30)), late, between, fresh];

        var before = DateTime.UtcNow;
        await RunAsync(f);
        var after = DateTime.UtcNow;

        /* Clamped: the cutoff is the clamp (collection time - 1 hour), never W - L, so the hole's rows are not read here. */
        var cutoff = f.ReplicaItemCutoffs[1]!.Value;
        Assert.InRange(cutoff, before.AddHours(-1).AddSeconds(-2), after.AddHours(-1));
        Assert.Equal(1, await CountAsync(f, queryId: 4));
        Assert.Equal(0, await CountAsync(f, queryId: 2));
        Assert.Equal(0, await CountAsync(f, queryId: 3));

        /* The hole is (W - L, clamp), recorded for the backfill, so the rows the look-back would have re-read are not lost. */
        var hole = await ReadHoleAsync(f, Sales);
        Assert.NotNull(hole);
        Assert.Equal(watermark.AddMinutes(-20), hole!.Value.From);
        Assert.Equal(cutoff, hole.Value.To);
    }

    [Fact]
    public async Task AReplicaClampThatIsANewGapAfterALandedCycle_OpensItsHoleAtWhatTheCycleRead_MinusTheLookBack()
    {
        var f = await BuildAsync(replica: "replica-host");
        var now = Truncate(DateTime.UtcNow);
        f.ReplicaRows = [Row(queryId: 1, lastExecution: now.AddMinutes(-30))];
        await RunAsync(f);

        /* The stored watermark W is 3 hours old, and an earlier cycle that clamped it landed after reading up to 90 minutes ago. The
           new floor (collection time - 1 hour) lies past that, so this clamp is a new gap (#9352). */
        var watermark = now.AddHours(-3);
        var readThrough = now.AddMinutes(-90);
        await SetWatermarkAsync(f, watermark);
        f.Service.QueryStoreClampReportsForTests.Land(f.ServerId, Sales, watermark, readThrough);
        f.ReplicaRows = [Row(queryId: 1, lastExecution: now.AddMinutes(-30))];
        await RunAsync(f);

        /* The hole starts the look-back (20 minutes here) before what the landed cycle read. Starting it at W - L overlapped the hole
           the earlier clamp recorded, and merging the two sent the dig back over a range it had filled. */
        var cutoff = f.ReplicaItemCutoffs[1]!.Value;
        var hole = await ReadHoleAsync(f, Sales);
        Assert.NotNull(hole);
        Assert.Equal(readThrough.AddMinutes(-20), hole!.Value.From);
        Assert.Equal(cutoff, hole.Value.To);
    }

    [Fact]
    public async Task AReplicaReadCutByTheRowCap_RecordsTheLookBackWindowAsAHole_AndTheNextReadStartsAtTheWatermark()
    {
        var f = await BuildAsync(replica: "replica-host");
        var now = Truncate(DateTime.UtcNow);
        var start = now.AddMinutes(-55);
        f.ReplicaRows = Enumerable.Range(1, QueryStoreCollector.MaxRowsPerDatabase)
            .Select(i => Row(queryId: i, lastExecution: start.AddSeconds(i % 300)))
            .ToList();
        var watermark = start.AddSeconds(299);

        await RunAsync(f);

        /* The read hit the row cap, so the rows beyond it are for the next read, and the window the look-back would have
           re-read before this one's watermark rides a hole instead. */
        Assert.Equal(QueryStoreCollector.MaxRowsPerDatabase, await CountAsync(f));
        var hole = await ReadHoleAsync(f, Sales);
        Assert.NotNull(hole);
        Assert.Equal(watermark.AddMinutes(-20), hole!.Value.From);
        Assert.Equal(watermark, hole.Value.To);

        /* A late row inside that window: the next read starts AT the watermark (a look-back would repeat the same full
           window), so the live path does not read it and the hole is the only place it is found. */
        var inside = Row(queryId: QueryStoreCollector.MaxRowsPerDatabase + 1, lastExecution: watermark.AddMinutes(-5));
        f.ReplicaRows.Add(inside);
        await RunAsync(f);

        Assert.Equal(2, f.ReplicaItemCutoffs.Count);
        Assert.Equal(watermark, f.ReplicaItemCutoffs[1]);
        Assert.Equal(0, await CountAsync(f, queryId: inside.QueryId));
    }

    private sealed class CapturingLogger(List<(LogLevel Level, string Message)> sink) : ILogger<RemoteCollectorService>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => sink.Add((logLevel, formatter(state, exception)));
    }

    private sealed record QsRow(long QueryId, DateTime LastExecution, string Role);

    private static QsRow Row(long queryId, DateTime lastExecution, string role = "Primary") => new(queryId, lastExecution, role);

    /* ---- #9088: a stale watermark warns once per real gap, not once per cycle ---- */

    private static int ClampWarnings(Fixture f) =>
        f.Logged.Count(l => l.Level == LogLevel.Warning && l.Message.Contains("catch-up clamped", StringComparison.Ordinal));

    /// <summary>A database with one stored row, then its stored watermark moved to <paramref name="age"/> before now and
    /// the source left idle: the shape of a Query Store with nothing new.</summary>
    private async Task<Fixture> IdleDatabaseWithStaleWatermarkAsync(TimeSpan age, int engineEdition = 3)
    {
        var f = await BuildAsync(replica: null, engineEdition: engineEdition);
        if (engineEdition == 5)
        {
            f.Service.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { Sales });
            f.Service.AzureDatabaseReaderOverrideForTests = (database, plan) => Answer(f, plan, replica: false, database);
        }

        f.PrimaryRows = [Row(queryId: 1, lastExecution: DateTime.UtcNow.AddMinutes(-30))];
        await RunAsync(f);
        Assert.Equal(1, await CountAsync(f));
        f.PrimaryRows = [];
        await SetWatermarkAsync(f, Truncate(DateTime.UtcNow - age));
        f.Logged.Clear();
        return f;
    }

    [Theory]
    [InlineData(3)]
    [InlineData(5)]
    public async Task AnIdleDatabaseWithAStaleWatermark_WarnsAndRecordsTheHoleOnce_NotOnEveryCycle(int engineEdition)
    {
        var f = await IdleDatabaseWithStaleWatermarkAsync(WatermarkPolicy.MaxCatchup + TimeSpan.FromMinutes(30), engineEdition);

        await RunAsync(f);
        var firstHole = await ReadHoleAsync(f, Sales);
        Assert.Equal(1, ClampWarnings(f));
        Assert.NotNull(firstHole);

        await RunAsync(f);
        await RunAsync(f);

        /* Three cycles over the same stale watermark: one warning, and the hole is the one the first cycle recorded (a repeat
           record would merge it wider, because the clamp floor moves with the clock). */
        Assert.Equal(1, ClampWarnings(f));
        Assert.Equal(firstHole, await ReadHoleAsync(f, Sales));
    }

    [Fact]
    public async Task AWatermarkThatMovesAndGoesStaleAgain_WarnsAgain()
    {
        var f = await IdleDatabaseWithStaleWatermarkAsync(WatermarkPolicy.MaxCatchup + TimeSpan.FromMinutes(30));
        await RunAsync(f);
        await RunAsync(f);
        Assert.Equal(1, ClampWarnings(f));

        /* New rows arrive, so the watermark moves to a fresh value and nothing clamps. */
        f.PrimaryRows = [Row(queryId: 2, lastExecution: DateTime.UtcNow.AddMinutes(-20))];
        await RunAsync(f);
        Assert.Equal(1, ClampWarnings(f));
        Assert.Equal(1, await CountAsync(f, queryId: 2));

        /* It then goes stale again, at a different value: a new gap, reported once. */
        f.PrimaryRows = [];
        await SetWatermarkAsync(f, Truncate(DateTime.UtcNow - WatermarkPolicy.MaxCatchup - TimeSpan.FromHours(1)));
        await RunAsync(f);
        await RunAsync(f);
        Assert.Equal(2, ClampWarnings(f));
    }

    [Fact]
    public async Task ACycleThatFailsAfterTheClamp_DoesNotUseUpTheReport_SoTheNextCycleReportsAndWidensTheHole()
    {
        var f = await IdleDatabaseWithStaleWatermarkAsync(WatermarkPolicy.MaxCatchup + TimeSpan.FromMinutes(30));

        f.PrimaryItemReadFails.Add(Sales);
        await RunAsync(f);
        var afterFailure = await ReadHoleAsync(f, Sales);
        Assert.Equal(1, ClampWarnings(f));

        /* The read faulted, so the range was not covered: the next cycle reports again and the hole grows to its new floor. */
        f.PrimaryItemReadFails.Clear();
        await RunAsync(f);
        var afterRecovery = await ReadHoleAsync(f, Sales);
        Assert.Equal(2, ClampWarnings(f));
        Assert.True(afterRecovery!.Value.To > afterFailure!.Value.To);

        /* That cycle read cleanly, so the next one stays quiet. */
        await RunAsync(f);
        Assert.Equal(2, ClampWarnings(f));
        Assert.Equal(afterRecovery, await ReadHoleAsync(f, Sales));
    }

    /* ---- #9359: the last landed clamped read survives a restart ---- */

    /// <summary>A second service over the same store, as after a restart: it shares the DuckDB file and the readers but none of the
    /// first one's memory.</summary>
    private void Restart(Fixture f, int engineEdition = 3)
    {
        var service = new RemoteCollectorService(f.DuckDb, f.ServerManager, new ScheduleManager(_configDir), new CapturingLogger(f.Logged));
        service.ServerReaderOverrideForTests = plan => Answer(f, plan, replica: false);
        service.ReplicaReaderOverrideForTests = plan => Answer(f, plan, replica: true);
        if (engineEdition == 5)
        {
            service.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { Sales });
            service.AzureDatabaseReaderOverrideForTests = (database, plan) => Answer(f, plan, replica: false, database);
        }

        f.Service = service;
    }

    private static async Task<string?> ReadStateAsync(Fixture f, string key)
    {
        using var connection = f.DuckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT state_value FROM collector_state WHERE server_id = $1 AND collector_name = $2 AND state_key = $3";
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = f.ServerId });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = QueryStoreBackfillState.StateCollectorName });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = key });
        return await command.ExecuteScalarAsync() as string;
    }

    private static async Task WriteStateAsync(Fixture f, string key, string? value)
    {
        using var connection = f.DuckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = value is null
            ? "DELETE FROM collector_state WHERE server_id = $1 AND collector_name = $2 AND state_key = $3"
            : "INSERT OR REPLACE INTO collector_state (server_id, collector_name, state_key, state_value, updated_at) VALUES ($1, $2, $3, $4, $5)";
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = f.ServerId });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = QueryStoreBackfillState.StateCollectorName });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = key });
        if (value is not null)
        {
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = value });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = DateTime.UtcNow });
        }

        await command.ExecuteNonQueryAsync();
    }

    [Theory]
    [InlineData(3)]
    [InlineData(5)]
    public async Task AnIdleDatabaseAfterARestart_OpensNoHole_AndWarnsNothing(int engineEdition)
    {
        var f = await IdleDatabaseWithStaleWatermarkAsync(WatermarkPolicy.MaxCatchup + TimeSpan.FromMinutes(30), engineEdition);
        await RunAsync(f);
        Assert.Equal(1, ClampWarnings(f));
        Assert.NotNull(await ReadStateAsync(f, QueryStoreBackfillState.LandedKeyPrefix + Sales));

        /* The backfill worker digs that hole and retires it. The range after it was read live and holds nothing. */
        await WriteStateAsync(f, QueryStoreBackfillState.HoleKeyPrefix + Sales, null);
        f.Logged.Clear();
        Restart(f, engineEdition);

        await RunAsync(f);

        /* The restart forgot nothing: the same raw watermark is already reported and read, so there is no warning and no new hole
           for the backfill to dig over a span that holds nothing. */
        Assert.Equal(0, ClampWarnings(f));
        Assert.Null(await ReadHoleAsync(f, Sales));
    }

    [Fact]
    public async Task ARealGapAfterARestart_OpensItsHoleAtWhatTheLastCycleRead()
    {
        var f = await BuildAsync(replica: null);
        var now = Truncate(DateTime.UtcNow);
        f.PrimaryRows = [Row(queryId: 1, lastExecution: now.AddMinutes(-30))];
        await RunAsync(f);

        /* The stored watermark is 3 hours old, and the cycle that clamped it before the restart read up to 90 minutes ago. */
        var watermark = now.AddHours(-3);
        var readThrough = now.AddMinutes(-90);
        await SetWatermarkAsync(f, watermark);
        await WriteStateAsync(f, QueryStoreBackfillState.LandedKeyPrefix + Sales, QueryStoreBackfillState.EncodeLanded(watermark, readThrough));
        f.PrimaryRows = [];
        Restart(f);

        await RunAsync(f);

        /* The new floor (collection time minus an hour) lies past what that cycle read, so the span between is a real gap, and the
           hole starts where the read stopped. Without the stored entry it started at the raw watermark. */
        var hole = await ReadHoleAsync(f, Sales);
        Assert.NotNull(hole);
        Assert.Equal(readThrough, hole!.Value.From);
    }

    [Fact]
    public async Task ADatabaseThatStopsClamping_RetiresItsStoredEntryOnce()
    {
        var f = await IdleDatabaseWithStaleWatermarkAsync(WatermarkPolicy.MaxCatchup + TimeSpan.FromMinutes(30));
        await RunAsync(f);
        var landedKey = QueryStoreBackfillState.LandedKeyPrefix + Sales;
        Assert.NotNull(await ReadStateAsync(f, landedKey));

        /* New rows arrive. That cycle still read the stale watermark, so it clamps and keeps the entry. The next one sees the
           fresh watermark, clamps nothing, and the stored entry is gone. */
        f.PrimaryRows = [Row(queryId: 2, lastExecution: DateTime.UtcNow.AddMinutes(-20))];
        await RunAsync(f);
        Assert.NotNull(await ReadStateAsync(f, landedKey));
        await RunAsync(f);
        Assert.Null(await ReadStateAsync(f, landedKey));
    }

    [Fact]
    public async Task EveryClampingDatabaseOfTheServer_StoresItsEntry_AndEveryOneThatStopsHasItRetired()
    {
        var f = await BuildAsync(replica: null);
        f.Databases.Add(Orders);
        f.PrimaryRowsByDb[Sales] = [Row(queryId: 1, lastExecution: DateTime.UtcNow.AddMinutes(-30))];
        f.PrimaryRowsByDb[Orders] = [Row(queryId: 10, lastExecution: DateTime.UtcNow.AddMinutes(-30))];
        await RunAsync(f);
        f.PrimaryRowsByDb[Sales] = [];
        f.PrimaryRowsByDb[Orders] = [];
        await SetWatermarkAsync(f, Truncate(DateTime.UtcNow - WatermarkPolicy.MaxCatchup - TimeSpan.FromMinutes(30)));

        await RunAsync(f);

        Assert.NotNull(await ReadStateAsync(f, QueryStoreBackfillState.LandedKeyPrefix + Sales));
        Assert.NotNull(await ReadStateAsync(f, QueryStoreBackfillState.LandedKeyPrefix + Orders));

        /* New rows arrive for both. The first cycle still reads the stale watermark and keeps both entries; the next clamps
           nothing and retires both in its one write. */
        f.PrimaryRowsByDb[Sales] = [Row(queryId: 2, lastExecution: DateTime.UtcNow.AddMinutes(-20))];
        f.PrimaryRowsByDb[Orders] = [Row(queryId: 20, lastExecution: DateTime.UtcNow.AddMinutes(-20))];
        await RunAsync(f);
        await RunAsync(f);

        Assert.Null(await ReadStateAsync(f, QueryStoreBackfillState.LandedKeyPrefix + Sales));
        Assert.Null(await ReadStateAsync(f, QueryStoreBackfillState.LandedKeyPrefix + Orders));
    }

    [Fact]
    public async Task AFailedDeleteOfTheStoredEntry_IsReturnedAgainNextCycle_AndOnceItLandsNothingMoreIs()
    {
        var f = await IdleDatabaseWithStaleWatermarkAsync(WatermarkPolicy.MaxCatchup + TimeSpan.FromMinutes(30));
        await RunAsync(f);
        var landedKey = QueryStoreBackfillState.LandedKeyPrefix + Sales;
        Assert.NotNull(await ReadStateAsync(f, landedKey));
        var reports = f.Service.QueryStoreClampReportsForTests;

        /* The first cycle that stops clamping asks for a delete, and the write fails (a cancelled token fails it before it opens). */
        var failed = reports.Land(f.ServerId, Sales, null, DateTime.UtcNow);
        Assert.NotNull(failed);
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        await f.Service.PersistQueryStoreLandedAsync(f.ServerId, [failed!.Value], cancelled.Token);
        Assert.NotNull(await ReadStateAsync(f, landedKey));

        /* The next cycle asks again, the write lands, and the cycle after that has nothing to delete. */
        var again = reports.Land(f.ServerId, Sales, null, DateTime.UtcNow);
        Assert.NotNull(again);
        await f.Service.PersistQueryStoreLandedAsync(f.ServerId, [again!.Value], CancellationToken.None);
        Assert.Null(await ReadStateAsync(f, landedKey));
        Assert.Null(reports.Land(f.ServerId, Sales, null, DateTime.UtcNow));
    }

    private static DateTime Truncate(DateTime value) =>
        new(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);

    private sealed class Fixture
    {
        public required RemoteCollectorService Service { get; set; }
        public required ServerManager ServerManager { get; init; }
        public required ServerConnection Server { get; init; }
        public required int ServerId { get; init; }
        public required DuckDbInitializer DuckDb { get; init; }
        /* The databases the server has (Sales alone unless a test adds more). */
        public List<string> Databases { get; } = [Sales];
        public Dictionary<string, List<QsRow>> PrimaryRowsByDb { get; } = new(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, List<QsRow>> ReplicaRowsByDb { get; } = new(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, List<DateTime?>> PrimaryCutoffsByDb { get; } = new(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, List<DateTime?>> ReplicaCutoffsByDb { get; } = new(StringComparer.OrdinalIgnoreCase);

        /* Databases whose item read on the replica throws (the replica answers everything else). */
        public HashSet<string> ReplicaItemReadFails { get; } = new(StringComparer.OrdinalIgnoreCase);
        public bool ReplicaFails { get; set; }

        /* #9088: databases whose primary item read throws, and every line the service logged. */
        public HashSet<string> PrimaryItemReadFails { get; } = new(StringComparer.OrdinalIgnoreCase);
        public required List<(LogLevel Level, string Message)> Logged { get; init; }
        public int ReplicaCalls;

        public List<QsRow> PrimaryRows
        {
            get => PrimaryRowsByDb.GetValueOrDefault(Sales) ?? [];
            set => PrimaryRowsByDb[Sales] = value;
        }

        public List<QsRow> ReplicaRows
        {
            get => ReplicaRowsByDb.GetValueOrDefault(Sales) ?? [];
            set => ReplicaRowsByDb[Sales] = value;
        }

        public List<DateTime?> PrimaryItemCutoffs => CutoffsOf(PrimaryCutoffsByDb, Sales);
        public List<DateTime?> ReplicaItemCutoffs => CutoffsOf(ReplicaCutoffsByDb, Sales);

        public static List<DateTime?> CutoffsOf(Dictionary<string, List<DateTime?>> byDb, string database) =>
            byDb.TryGetValue(database, out var list) ? list : [];
    }

    private async Task<Fixture> BuildAsync(string? replica, int engineEdition = 3)
    {
        var duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _initializers.Add(duckDb);
        await duckDb.InitializeFromTemplateAsync();

        var serverManager = new ServerManager(_configDir);
        var server = new ServerConnection { ServerName = "sql-test", DisplayName = "sql-test", QueryStoreReplica = replica };
        serverManager.AddServer(server);
        var status = serverManager.GetConnectionStatus(server.Id);
        status.SqlEngineEdition = engineEdition;
        status.SqlMajorVersion = 16;

        var logged = new List<(LogLevel Level, string Message)>();
        var service = new RemoteCollectorService(duckDb, serverManager, new ScheduleManager(_configDir), new CapturingLogger(logged));
        var fixture = new Fixture
        {
            Service = service,
            ServerManager = serverManager,
            Server = server,
            ServerId = RemoteCollectorService.GetServerId(server),
            DuckDb = duckDb,
            Logged = logged,
        };

        service.ServerReaderOverrideForTests = plan => Answer(fixture, plan, replica: false);
        service.ReplicaReaderOverrideForTests = plan => Answer(fixture, plan, replica: true);
        return fixture;
    }

    private static Task RunAsync(Fixture f) =>
        f.Service.RunCollectorAsync(f.Server, QueryStoreCollector.Instance.Name, CancellationToken.None);

    /// <summary>One side's reader for one statement. Item reads apply the cutoff as the SQL does, and note it.</summary>
    private static DbDataReader Answer(Fixture f, CollectorQuery plan, bool replica, string? azureDatabase = null)
    {
        if (replica)
        {
            Interlocked.Increment(ref f.ReplicaCalls);
            if (f.ReplicaFails)
            {
                throw new InvalidOperationException("the replica is down");
            }
        }

        if (plan.Parameters?.Any(static p => p.Name == "@cutoff_time") == true)
        {
            /* The item statement names its database: EXECUTE [db].sys.sp_executesql ... */
            string database;
            if (azureDatabase is not null)
            {
                database = azureDatabase;
            }
            else
            {
                var open = plan.Text.IndexOf("EXECUTE [", StringComparison.Ordinal) + "EXECUTE [".Length;
                database = plan.Text[open..plan.Text.IndexOf("].sys.sp_executesql", open, StringComparison.Ordinal)];
            }

            var cutoff = plan.Parameters.First(static p => p.Name == "@cutoff_time").Value as DateTime?;
            var cutoffs = replica ? f.ReplicaCutoffsByDb : f.PrimaryCutoffsByDb;
            if (!cutoffs.TryGetValue(database, out var seen))
            {
                cutoffs[database] = seen = [];
            }

            seen.Add(cutoff);
            if (replica && f.ReplicaItemReadFails.Contains(database))
            {
                throw new InvalidOperationException("the replica cannot be read for " + database);
            }

            if (!replica && f.PrimaryItemReadFails.Contains(database))
            {
                throw new InvalidOperationException("the primary cannot be read for " + database);
            }

            var rows = ((replica ? f.ReplicaRowsByDb : f.PrimaryRowsByDb).GetValueOrDefault(database) ?? [])
                .Where(r => cutoff is null || r.LastExecution > cutoff).ToList();
            return ItemTable(rows).CreateDataReader();
        }

        if (plan.Text == QueryStoreCollector.ProductVersionProbeText)
        {
            var version = new DataTable();
            version.Columns.Add("v", typeof(int));
            version.Rows.Add(16);
            return version.CreateDataReader();
        }

        if (plan.Text == QueryStoreCollector.BuildRedoStateQuery().Text)
        {
            var redo = new DataTable();
            redo.Columns.Add("name", typeof(string));
            redo.Columns.Add("group_database_id", typeof(Guid));
            redo.Columns.Add("last_commit_time", typeof(DateTime));
            redo.Columns.Add("is_primary_replica", typeof(bool));
            foreach (var database in f.Databases)
            {
                redo.Rows.Add(database, database == Sales ? GroupDatabaseId : new Guid(System.Security.Cryptography.MD5.HashData(System.Text.Encoding.UTF8.GetBytes(database))), DateTime.UtcNow, !replica);
            }

            return redo.CreateDataReader();
        }

        /* An enumeration: the databases, then the (empty) probe failures, then, on the primary, the flush intervals. */
        var databases = new DataTable();
        var failures = new DataTable();
        failures.Columns.Add("item_name", typeof(string));
        failures.Columns.Add("error_text", typeof(string));
        if (replica)
        {
            databases.Columns.Add("name", typeof(string));
            databases.Columns.Add("actual_state", typeof(int));
            databases.Columns.Add("readonly_reason", typeof(int));
            databases.Columns.Add("flush", typeof(int));
            foreach (var database in f.Databases)
            {
                databases.Rows.Add(database, 0, 8, 900);
            }

            return new DataTableReader([databases, failures]);
        }

        databases.Columns.Add("database_name", typeof(string));
        var facts = new DataTable();
        facts.Columns.Add("name", typeof(string));
        facts.Columns.Add("flush", typeof(int));
        foreach (var database in f.Databases)
        {
            databases.Rows.Add(database);
            facts.Rows.Add(database, 900);
        }

        return new DataTableReader([databases, failures, facts]);
    }

    /// <summary>The 56-column runtime-stats read the collector's row reader expects, one row per entry.</summary>
    private static DataTable ItemTable(IEnumerable<QsRow> rows)
    {
        var table = new DataTable();
        for (var i = 0; i < 56; i++)
        {
            var type = i switch
            {
                2 or 5 or 6 or 7 or 44 or 45 or 48 or 50 or 51 or 52 => typeof(string),
                3 or 4 => typeof(DateTimeOffset),
                46 => typeof(bool),
                49 => typeof(int),
                54 or 55 => typeof(DateTime),
                _ => typeof(long),
            };
            table.Columns.Add("c" + i.ToString(CultureInfo.InvariantCulture), type);
        }

        foreach (var row in rows)
        {
            var values = new object?[56];
            for (var i = 0; i < 56; i++)
            {
                values[i] = table.Columns[i].DataType == typeof(long) ? 0L : DBNull.Value;
            }

            values[0] = row.QueryId;
            values[1] = row.QueryId;
            values[2] = "Regular";
            values[3] = new DateTimeOffset(row.LastExecution.AddMinutes(-1), TimeSpan.Zero);
            values[4] = new DateTimeOffset(row.LastExecution, TimeSpan.Zero);
            values[6] = "SELECT " + row.QueryId.ToString(CultureInfo.InvariantCulture);
            values[8] = 5L;
            values[52] = row.Role;
            values[53] = row.QueryId;
            table.Rows.Add(values.Select(v => v ?? DBNull.Value).ToArray());
        }

        return table;
    }

    private static async Task<long> CountAsync(Fixture f, long? queryId = null, string database = Sales)
    {
        using var connection = f.DuckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM query_store_stats WHERE server_id = $1 AND database_name = $2"
            + (queryId is null ? "" : " AND query_id = $3");
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = f.ServerId });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = database });
        if (queryId is not null)
        {
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = queryId.Value });
        }

        return Convert.ToInt64(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
    }

    private static async Task<string?> ReadSourceAsync(Fixture f)
    {
        using var connection = f.DuckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT read_source FROM collection_log WHERE server_id = $1 AND collector_name = 'query_store' ORDER BY collection_time DESC LIMIT 1";
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = f.ServerId });
        var value = await command.ExecuteScalarAsync();
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    /// <summary>Moves a database's stored watermark (its newest last_execution_time) to the given instant.</summary>
    private static async Task SetWatermarkAsync(Fixture f, DateTime watermark)
    {
        using var connection = f.DuckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE query_store_stats SET last_execution_time = $1 WHERE server_id = $2";
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = watermark });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = f.ServerId });
        await command.ExecuteNonQueryAsync();
    }

    /// <summary>The backfill hole recorded for a database, as the backfill worker reads it.</summary>
    private static async Task<(DateTime From, DateTime To)?> ReadHoleAsync(Fixture f, string database)
    {
        using var connection = f.DuckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT state_value FROM collector_state WHERE server_id = $1 AND collector_name = $2 AND state_key = $3";
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = f.ServerId });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = QueryStoreBackfillState.StateCollectorName });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = QueryStoreBackfillState.HoleKeyPrefix + database });
        var value = await command.ExecuteScalarAsync();
        return value is string encoded && QueryStoreBackfillState.TryDecodeHole(encoded, out var from, out var to) ? (from, to) : null;
    }

    private static async Task<long> LogRowsAsync(Fixture f)
    {
        using var connection = f.DuckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM collection_log WHERE server_id = $1 AND collector_name = 'query_store'";
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = f.ServerId });
        return Convert.ToInt64(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
    }
}
