/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// #5633 item 4: reads over stored history leave out a database the server no longer has, judged by the server's newest
/// <c>database_states</c> snapshot (the source #5486's backfill check reads). Covers the FinOps recommendations (idle,
/// dev/test, compression, which now names its database), the FinOps index inputs, get_top_procedures_by_cpu, and the Query
/// Store clutter reads (configuration, plan churn). Each read is also run with NO snapshot and with a STALE one, where
/// nothing is filtered, and with the dropped database named, where it is read as asked.
/// </summary>
public sealed class DroppedDatabaseScopeLiveTests
{
    private const int TimeoutSeconds = 30;
    private const string BaseSkip = "Set DARLING_TEST_PG to a Postgres connection string to run the live dropped-database scope test.";
    private static int GoldenServerId => FinOpsRecommendationsGoldenLiveTests.ServerIdA;

    private static async Task SnapshotAsync(NpgsqlConnection connection, int serverId, string serverName, TimeSpan age, CancellationToken ct, params string[] databases)
    {
        var at = DateTime.SpecifyKind(DateTime.UtcNow - age, DateTimeKind.Unspecified);
        var id = 1;
        foreach (var database in databases)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ($1,$2,$3,$4,$5,$6,'ONLINE',FALSE)",
                CollectionIdGenerator.Next(), at, serverId, serverName, database, id++);
        }
    }

    private static FinOpsRecommendation? Find(IEnumerable<FinOpsRecommendation> rows, string findingContains) =>
        rows.FirstOrDefault(r => r.Finding.Contains(findingContains, StringComparison.Ordinal));

    private static async Task<ScratchPostgres> SeedGoldenAsync(string baseConnectionString, CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await FinOpsRecommendationsGoldenLiveTests.SeedAsync(connection, ct);
        return scratch;
    }

    [Fact]
    public async Task FinOps_DroppedDatabases_LeaveTheRecommendationsAndTheIndexInputs_AndCompressionNamesItsDatabase()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), BaseSkip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedGoldenAsync(baseConnectionString!, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var serverName = FinOpsRecommendationsGoldenLiveTests.ServerNameA;

        /* No database_states row at all (an older store, an Azure SQL Database): nothing is filtered, and the 2 GB index in SalesA is
           named with its database. */
        var baseline = await DarlingFinOpsRecommendationsReader.GetRecommendationsAsync(dataSource, GoldenServerId, 1000m, TimeoutSeconds, cancellationToken: ct);
        Assert.NotNull(Find(baseline, "idle database"));
        Assert.Contains("SalesA.", Find(baseline, "uncompressed object")!.Detail, StringComparison.Ordinal);
        Assert.Contains("OrdersA", Find(baseline, "low IO latency")!.Detail, StringComparison.Ordinal);
        var baselineInputs = await DarlingFinOpsIndexAnalysisReader.GetIndexCleanupInputsAsync(dataSource, GoldenServerId, TimeoutSeconds, ct);
        Assert.Contains(baselineInputs, i => i.DatabaseName == "SalesA");

        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        /* A snapshot older than the horizon is stale: unknown, so nothing is filtered. */
        await SnapshotAsync(connection, GoldenServerId, serverName, CurrentDatabaseScope.SnapshotHorizon + TimeSpan.FromHours(1), ct, "OrdersA");
        var stale = await DarlingFinOpsRecommendationsReader.GetRecommendationsAsync(dataSource, GoldenServerId, 1000m, TimeoutSeconds, cancellationToken: ct);
        Assert.Equal(baseline.Select(r => (r.Finding, r.Detail)).ToArray(), stale.Select(r => (r.Finding, r.Detail)).ToArray());

        /* The newest snapshot names OrdersA only: ArchiveA and SalesA were dropped. */
        await SnapshotAsync(connection, GoldenServerId, serverName, TimeSpan.FromMinutes(5), ct, "OrdersA");
        var rows = await DarlingFinOpsRecommendationsReader.GetRecommendationsAsync(dataSource, GoldenServerId, 1000m, TimeoutSeconds, cancellationToken: ct);
        Assert.Null(Find(rows, "idle database"));
        Assert.Null(Find(rows, "uncompressed object"));
        Assert.DoesNotContain(rows, r => r.Detail.Contains("ArchiveA", StringComparison.Ordinal) || r.Detail.Contains("SalesA", StringComparison.Ordinal));
        Assert.Contains("OrdersA", Find(rows, "low IO latency")!.Detail, StringComparison.Ordinal);

        var inputs = await DarlingFinOpsIndexAnalysisReader.GetIndexCleanupInputsAsync(dataSource, GoldenServerId, TimeoutSeconds, ct);
        Assert.DoesNotContain(inputs, i => i.DatabaseName == "SalesA");
        var analysis = await DarlingFinOpsIndexAnalysisReader.GetIndexAnalysisWithSnapshotTimesAsync(dataSource, GoldenServerId, TimeoutSeconds, ct);
        Assert.DoesNotContain(analysis.Result.DatabaseRollups, r => r.DatabaseName == "SalesA");
    }

    [Fact]
    public async Task TopProcedures_AndTheClutterReads_LeaveADroppedDatabaseOut_ButReadItWhenNamed()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), BaseSkip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        const string serverName = "dropped-scope-e2e";
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);
        var now = DarlingMcpTestData.Naive(DateTime.UtcNow);
        /* GoneDb is the heavier database: before the fix it filled a top-1 page by itself. */
        foreach (var (database, weight) in new[] { ("GoneDb", 900L), ("LiveDb", 100L) })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
                                               delta_worker_time, delta_elapsed_time, delta_execution_count, delta_spills, sample_interval_seconds)
                  VALUES ($1,$2,$3,$4,$5,'dbo','usp_Work',$6,$7,$8,$9,0,60)",
                CollectionIdGenerator.Next(), now.AddHours(-2), serverId, serverName, database, "0xH" + database, weight * 1000L, weight * 2000L, weight);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes, query_capture_mode)
                  VALUES ($1,$2,$3,$4,$5,'READ_ONLY','READ_WRITE',2,10,8192,'AUTO',21,200,60,'AUTO')",
                CollectionIdGenerator.Next(), now.AddHours(-1), serverId, serverName, database);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_hash, query_plan_hash, query_text, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time)
                  VALUES ($1,$2,$3,$4,$5,1,1,'0xQ1','0xP1','SELECT 1',10,1000,800,$2)",
                CollectionIdGenerator.Next(), now.AddHours(-1), serverId, serverName, database);
        }

        static List<string> Databases(string json) =>
            JsonDocument.Parse(json).RootElement.GetProperty("procedures").EnumerateArray()
                .Select(p => p.GetProperty("database_name").GetString()!).ToList();

        /* No snapshot: both databases, heaviest first. */
        Assert.Equal(new[] { "GoneDb", "LiveDb" }, Databases(await DarlingMcpDataTools.GetTopProceduresByCpu(dataSource, serverName, hours_back: 168, top: 20)));
        var configBefore = await DarlingQueryStoreClutterReader.GetConfigAsync(dataSource, new[] { serverId }, now.AddDays(-2), now.AddMinutes(5), cancellationToken: ct);
        Assert.Equal(2, configBefore.Count);

        /* An earlier snapshot named GoneDb and LiveDb; the newest names LiveDb only, so GoneDb was dropped in between. */
        await SnapshotAsync(connection, serverId, serverName, TimeSpan.FromHours(1), ct, "GoneDb", "LiveDb");
        await SnapshotAsync(connection, serverId, serverName, TimeSpan.FromMinutes(5), ct, "LiveDb");

        /* The page is LiveDb's even at top 1, where GoneDb used to take the slot. */
        Assert.Equal(new[] { "LiveDb" }, Databases(await DarlingMcpDataTools.GetTopProceduresByCpu(dataSource, serverName, hours_back: 168, top: 1)));

        /* Review L2 leftover: NewDb was created after the newest snapshot, so NO snapshot names it. It is not a drop and it ranks (it is
           the heaviest database, so it takes the top-1 slot) instead of staying hidden until the next snapshot. */
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
                                           delta_worker_time, delta_elapsed_time, delta_execution_count, delta_spills, sample_interval_seconds)
              VALUES ($1,$2,$3,$4,'NewDb','dbo','usp_Work','0xHNewDb',950000,1900000,950,0,60)",
            CollectionIdGenerator.Next(), now.AddMinutes(-1), serverId, serverName);
        Assert.Equal(new[] { "NewDb", "LiveDb" }, Databases(await DarlingMcpDataTools.GetTopProceduresByCpu(dataSource, serverName, hours_back: 168, top: 20)));
        Assert.Equal(new[] { "NewDb" }, Databases(await DarlingMcpDataTools.GetTopProceduresByCpu(dataSource, serverName, hours_back: 168, top: 1)));
        Assert.Equal(new[] { "GoneDb" }, (await CurrentDatabaseScope.ReadDroppedAsync(dataSource, serverId, windowStartUtc: now.AddHours(-168), cancellationToken: ct))!.ToArray());
        /* Named, a dropped database is a question about its history and is read as asked. */
        Assert.Equal(new[] { "GoneDb" }, Databases(await DarlingMcpDataTools.GetTopProceduresByCpu(dataSource, serverName, hours_back: 168, top: 20, database_name: "GoneDb")));

        var config = await DarlingQueryStoreClutterReader.GetConfigAsync(dataSource, new[] { serverId }, now.AddDays(-2), now.AddMinutes(5), cancellationToken: ct);
        Assert.Equal(new[] { "LiveDb" }, config.Select(c => c.DatabaseName).ToArray());
        var churn = await DarlingQueryStoreClutterReader.GetPlanChurnAsync(dataSource, new[] { serverId }, now.AddDays(-2), now.AddMinutes(5), cancellationToken: ct);
        Assert.Equal(new[] { "LiveDb" }, churn.Select(c => c.DatabaseName).ToArray());
        var namedConfig = await DarlingQueryStoreClutterReader.GetConfigAsync(
            dataSource, new[] { serverId }, now.AddDays(-2), now.AddMinutes(5), DatabaseFilter.One("GoneDb"), cancellationToken: ct);
        Assert.Equal(new[] { "GoneDb" }, namedConfig.Select(c => c.DatabaseName).ToArray());
    }

    /// <summary>
    /// Review M2: a read with an as_of uses the newest snapshot AT OR BEFORE it (horizon from it), so a database dropped after the
    /// window still ranks in a window it existed in. Review M2 (second half): a row stamped after the newest snapshot is a database
    /// created since it, and is kept (the snapshot is up to 30 minutes old).
    /// </summary>
    [Fact]
    public async Task AsOfReads_UseTheSnapshotAtOrBeforeIt_AndARowNewerThanTheSnapshotIsKept()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), BaseSkip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        const string serverName = "dropped-scope-asof";
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);
        var now = DarlingMcpTestData.Naive(DateTime.UtcNow);
        var windowTime = now.AddHours(-72);
        foreach (var (database, weight) in new[] { ("GoneDb", 900L), ("LiveDb", 100L) })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
                                               delta_worker_time, delta_elapsed_time, delta_execution_count, delta_spills, sample_interval_seconds)
                  VALUES ($1,$2,$3,$4,$5,'dbo','usp_Work',$6,$7,$8,$9,0,60)",
                CollectionIdGenerator.Next(), windowTime, serverId, serverName, database, "0xH" + database, weight * 1000L, weight * 2000L, weight);
        }

        /* Three days ago both existed; the newest snapshot (5 minutes ago) names LiveDb only, so GoneDb was dropped since. */
        await SnapshotAsync(connection, serverId, serverName, TimeSpan.FromHours(73), ct, "GoneDb", "LiveDb");
        await SnapshotAsync(connection, serverId, serverName, TimeSpan.FromMinutes(5), ct, "LiveDb");

        static List<string> Databases(string json) =>
            JsonDocument.Parse(json).RootElement.GetProperty("procedures").EnumerateArray()
                .Select(p => p.GetProperty("database_name").GetString()!).ToList();

        var asOf = windowTime.AddHours(2).ToString("yyyy-MM-ddTHH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
        Assert.Equal(new[] { "GoneDb", "LiveDb" },
            Databases(await DarlingMcpDataTools.GetTopProceduresByCpu(dataSource, serverName, hours_back: 12, top: 20, as_of: asOf)));

        /* The set itself: at or before the anchor, and unknown when that snapshot is older than the horizon from the anchor. */
        var then = await CurrentDatabaseScope.ReadAsync(dataSource, serverId, asOfUtc: windowTime.AddHours(2), cancellationToken: ct);
        Assert.Equal(new[] { "GoneDb", "LiveDb" }, then!.OrderBy(n => n, StringComparer.Ordinal).ToArray());
        Assert.Null(await CurrentDatabaseScope.ReadAsync(dataSource, serverId, asOfUtc: windowTime.AddHours(2) + CurrentDatabaseScope.SnapshotHorizon, cancellationToken: ct));
        Assert.Equal(new[] { "LiveDb" }, (await CurrentDatabaseScope.ReadAsync(dataSource, serverId, cancellationToken: ct))!.ToArray());

        /* A database created after the newest snapshot: its newest capture is newer than the snapshot, so it stays; GoneDb's is older. */
        foreach (var (database, capturedAt) in new[] { ("GoneDb", now.AddHours(-1)), ("LiveDb", now.AddHours(-1)), ("NewDb", now.AddMinutes(-1)) })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes, query_capture_mode)
                  VALUES ($1,$2,$3,$4,$5,'READ_ONLY','READ_WRITE',2,10,8192,'AUTO',21,200,60,'AUTO')",
                CollectionIdGenerator.Next(), capturedAt, serverId, serverName, database);
        }

        var config = await DarlingQueryStoreClutterReader.GetConfigAsync(dataSource, new[] { serverId }, now.AddDays(-2), now.AddMinutes(5), cancellationToken: ct);
        Assert.Equal(new[] { "LiveDb", "NewDb" }, config.Select(c => c.DatabaseName).OrderBy(n => n, StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public void TheScopeRules_UnknownFiltersNothing_AndANamedDatabaseIsKept()
    {
        var current = new HashSet<string>(StringComparer.Ordinal) { "A" };
        Assert.False(CurrentDatabaseScope.IsDropped(null, "Z"));
        Assert.False(CurrentDatabaseScope.IsDropped(current, "A"));
        Assert.True(CurrentDatabaseScope.IsDropped(current, "Z"));
        Assert.True(CurrentDatabaseScope.IsDropped(current, "a"));
        var dropped = new HashSet<string>(StringComparer.Ordinal) { "Z" };
        Assert.True(CurrentDatabaseScope.Narrow(DatabaseFilter.All, null).IsAll);
        Assert.Equal(DatabaseFilter.All, CurrentDatabaseScope.Narrow(DatabaseFilter.All, new HashSet<string>(StringComparer.Ordinal)));
        var narrowed = CurrentDatabaseScope.Narrow(DatabaseFilter.All, dropped);
        Assert.True(narrowed.IsAll);
        Assert.Equal(new[] { "Z" }, narrowed.ExcludedNames.ToArray());
        Assert.Equal(new[] { "Z" }, CurrentDatabaseScope.Narrow(DatabaseFilter.One("Z"), dropped).Names.ToArray());
        Assert.Empty(CurrentDatabaseScope.Narrow(DatabaseFilter.One("Z"), dropped).ExcludedNames);
    }

    /// <summary>Review L2 leftover: dropped = listed by an earlier snapshot and absent from the newest; a collector that stopped is unknown.</summary>
    [Fact]
    public void DroppedFrom_NamesOnlyWhatAnEarlierSnapshotListedAndTheNewestDoesNot()
    {
        var anchor = new DateTime(2026, 10, 12, 12, 0, 0, DateTimeKind.Unspecified);
        var seen = new Dictionary<string, DateTime>(StringComparer.Ordinal)
        {
            ["Live"] = anchor.AddMinutes(-5),
            ["Gone"] = anchor.AddMinutes(-35),
        };
        Assert.Equal(new[] { "Gone" }, CurrentDatabaseScope.DroppedFrom(seen, anchor)!.ToArray());
        Assert.Empty(CurrentDatabaseScope.DroppedFrom(new Dictionary<string, DateTime> { ["Live"] = anchor.AddMinutes(-5) }, anchor)!);
        Assert.Null(CurrentDatabaseScope.DroppedFrom(new Dictionary<string, DateTime>(), anchor));
        Assert.Null(CurrentDatabaseScope.DroppedFrom(seen, anchor + CurrentDatabaseScope.SnapshotHorizon));
        Assert.Equal(anchor - CurrentDatabaseScope.SnapshotHorizon, CurrentDatabaseScope.LookBackFloor(anchor, null));
        Assert.Equal(anchor.AddHours(-24), CurrentDatabaseScope.LookBackFloor(anchor, anchor.AddHours(-24)));
        Assert.Equal(anchor - CurrentDatabaseScope.DropLookBack, CurrentDatabaseScope.LookBackFloor(anchor, anchor.AddDays(-30)));

        var filter = DatabaseFilter.Excluding(new[] { "Gone", " ", "Gone" });
        Assert.True(filter.IsAll);
        Assert.Equal(new[] { "Gone" }, filter.ExcludedNames.ToArray());
        Assert.NotEqual(DatabaseFilter.All, filter);
        Assert.Equal(DatabaseFilter.All, DatabaseFilter.Excluding(new[] { "" }));
        Assert.Equal(" AND ($6::text[] IS NULL OR database_name IS NULL OR database_name <> ALL($6))", DatabaseFilter.ExcludeClause("database_name", 6));
    }

    /// <summary>
    /// Review M3: get_query_store_clutter with as_of reads the snapshot at or before the anchor, so a database dropped after the window is
    /// still reported for a window it existed in; without as_of the same database is left out.
    /// </summary>
    [Fact]
    public async Task QueryStoreClutter_WithAsOf_KeepsADatabaseDroppedAfterTheWindow()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), BaseSkip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        const string serverName = "dropped-scope-clutter-asof";
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);
        var now = DarlingMcpTestData.Naive(DateTime.UtcNow);
        var windowTime = now.AddHours(-72);
        foreach (var database in new[] { "GoneDb", "LiveDb" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes, query_capture_mode)
                  VALUES ($1,$2,$3,$4,$5,'READ_ONLY','READ_WRITE',2,10,8192,'AUTO',21,200,60,'AUTO')",
                CollectionIdGenerator.Next(), windowTime, serverId, serverName, database);
        }

        await SnapshotAsync(connection, serverId, serverName, TimeSpan.FromHours(73), ct, "GoneDb", "LiveDb");
        await SnapshotAsync(connection, serverId, serverName, TimeSpan.FromMinutes(5), ct, "LiveDb");

        var asOf = windowTime.AddHours(2).ToString("yyyy-MM-ddTHH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
        var historical = await DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(dataSource, serverName, 6, 20, false, asOf, null, ct);
        Assert.Contains("GoneDb", historical, StringComparison.Ordinal);
        Assert.Contains("LiveDb", historical, StringComparison.Ordinal);

        var rows = await DarlingQueryStoreClutterReader.GetConfigAsync(dataSource, new[] { serverId }, windowTime.AddHours(-1), windowTime.AddHours(3), cancellationToken: ct);
        Assert.Equal(new[] { "LiveDb" }, rows.Select(r => r.DatabaseName).ToArray());
        var asOfRows = await DarlingQueryStoreClutterReader.GetConfigAsync(dataSource, new[] { serverId }, windowTime.AddHours(-1), windowTime.AddHours(3), asOfUtc: windowTime.AddHours(2), cancellationToken: ct);
        Assert.Equal(new[] { "GoneDb", "LiveDb" }, asOfRows.Select(r => r.DatabaseName).OrderBy(n => n, StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public void CompressionNamesTheDatabase_AndStaysSchemaQualifiedWhenTheInputHasNone()
    {
        static IndexCleanupIndexInput Index(string db) => new()
        {
            DatabaseName = db, SchemaName = "dbo", TableName = "r", DataCompressionDesc = "NONE", ReservedMb = 1843m,
        };

        Assert.Contains("Sales.dbo.r (1.8GB)", FinOpsRecommendationFigures.Compression(new[] { Index("Sales") })!.Detail, StringComparison.Ordinal);
        Assert.Contains("Large uncompressed tables/indexes: dbo.r (1.8GB)", FinOpsRecommendationFigures.Compression(new[] { Index("") })!.Detail, StringComparison.Ordinal);
    }
}
