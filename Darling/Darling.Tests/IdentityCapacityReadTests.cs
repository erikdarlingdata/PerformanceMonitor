/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Numerics;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6875 part 2: the time-to-exhaustion arithmetic and the bands, pure. The rate is identity values issued per day toward the end the
/// increment moves to, from a column's oldest and newest stored value. Nothing is reported when there is nothing to divide.
/// </summary>
public sealed class IdentityExhaustionTests
{
    private static readonly DateTime T0 = new(2026, 10, 1, 0, 0, 0, DateTimeKind.Unspecified);

    private static IdentityExhaustion? Estimate(string type, int precision, BigInteger seed, BigInteger increment, BigInteger? oldValue, BigInteger? newValue, double days = 10) =>
        IdentityCapacityMath.EstimateExhaustion(type, precision, seed, increment, oldValue, T0, newValue, T0.AddDays(days));

    [Fact]
    public void AnIntColumnFillingTowardTheMaximum_GivesTheRate_AndTheDaysForWhatIsLeft()
    {
        var result = Estimate("int", 10, 1, 1, 1_000_000_000, 1_100_000_000);

        Assert.NotNull(result);
        Assert.Equal(10_000_000d, result.Value.StepsPerDay, 3);
        /* Used 1,100,000,000 of 2,147,483,647: 1,047,483,647 left, at 10,000,000 a day. */
        Assert.Equal(104.7483647d, result.Value.DaysLeft, 3);
    }

    [Fact]
    public void ANegativeIncrement_CountsTowardTheMinimum()
    {
        var result = Estimate("smallint", 5, -1, -1, -10_000, -12_000, days: 2);

        Assert.NotNull(result);
        Assert.Equal(1_000d, result.Value.StepsPerDay, 3);
        /* 12,000 of 32,768 issued: 20,768 left at 1,000 a day. */
        Assert.Equal(20.768d, result.Value.DaysLeft, 3);
    }

    [Fact]
    public void AnIncrementOfTen_CountsValuesIssued_NotTheDistanceMoved()
    {
        var result = Estimate("int", 10, 0, 10, 1_000, 2_000, days: 1);

        Assert.NotNull(result);
        Assert.Equal(100d, result.Value.StepsPerDay, 3);
        /* Used 201 of 214,748,365, so 214,748,164 left at 100 a day. */
        Assert.Equal(2_147_481.64d, result.Value.DaysLeft, 2);
    }

    [Fact]
    public void ADecimalColumnNearTheTopOfThirtyEightDigits_StaysExact()
    {
        var tenTo37 = BigInteger.Pow(10, 37);
        var result = Estimate("decimal", 38, 1, 1, tenTo37, 2 * tenTo37, days: 1);

        Assert.NotNull(result);
        Assert.Equal(1e37, result.Value.StepsPerDay, 1e31);
        Assert.Equal(8d, result.Value.DaysLeft, 6);
    }

    [Fact]
    public void AFullColumn_HasZeroDaysLeft()
    {
        var result = Estimate("int", 10, 1, 1, 2_147_483_000, int.MaxValue);

        Assert.NotNull(result);
        Assert.Equal(0d, result.Value.DaysLeft);
        Assert.True(result.Value.StepsPerDay > 0);
    }

    [Theory]
    [InlineData(null, 1_000_000_000L, "no older sample")]
    [InlineData(1_000_000_000L, null, "no newer value")]
    [InlineData(1_000_000_000L, 1_000_000_000L, "the value did not move")]
    [InlineData(1_900_000_000L, 1_500_000_000L, "reseeded: the value moved backwards")]
    public void NothingToDivide_GivesNull(long? oldValue, long? newValue, string why)
    {
        Assert.True(Estimate("int", 10, 1, 1, oldValue, newValue) is null, why);
    }

    [Fact]
    public void ANegativeIncrementWhoseValueRose_MovedAwayFromTheEnd_GivesNull()
    {
        Assert.Null(Estimate("int", 10, -1, -1, -1_000_000, -500_000));
    }

    [Fact]
    public void OneSample_OrNoTimeBetweenSamples_GivesNull()
    {
        Assert.Null(IdentityCapacityMath.EstimateExhaustion("int", 10, 1, 1, 1_000, T0, 2_000, T0));
        Assert.Null(IdentityCapacityMath.EstimateExhaustion("int", 10, 1, 1, 1_000, T0.AddDays(1), 2_000, T0));
    }

    [Fact]
    public void ATypeWithNoCapacity_GivesNull()
    {
        Assert.Null(Estimate("varchar", 0, 1, 1, 1_000, 2_000));
    }

    [Theory]
    [InlineData(0d, "ok")]
    [InlineData(69.9999d, "ok")]
    [InlineData(70d, "warning")]
    [InlineData(89.9999d, "warning")]
    [InlineData(90d, "critical")]
    [InlineData(100d, "critical")]
    public void TheBand_StartsAtSeventyAndAtNinety(double percent, string band)
    {
        Assert.Equal(band, IdentityCapacityMath.Band(percent));
    }

    [Fact]
    public void AValueTheCollectorCouldNotStore_KeepsItsStoredPercent_AndIsNeverReadAsNothingUsed()
    {
        /* A decimal(38,0) value past a CLR decimal is stored NULL with an exact percent beside it. */
        var fact = new IdentityColumnFact("Db", "dbo", "Wide", "Id", "decimal", 38, null, 1, 1, 95.5, T0, null, T0);

        Assert.Equal(95.5, fact.PercentUsed);
        Assert.Equal("critical", fact.Band);
        Assert.Null(fact.ValuesRemaining);
        Assert.Null(fact.Exhaustion);
    }
}

/// <summary>
/// #6875 part 2: <c>get_identity_capacity</c> and its reader over a real store. Bands, a clean database read as clean, a database with no
/// scan read as not scanned, the rate from a column's own two samples, and a server with no capture yet.
/// </summary>
[Collection("live-postgres")]
public sealed class IdentityCapacityReadLiveTests
{
    private const string DataServer = "darling-identity-data";
    private const string CleanServer = "darling-identity-clean";
    private const string EmptyServer = "darling-identity-empty";
    private static readonly int DataId = ServerIdHelper.GetDeterministicHashCode(DataServer);
    private static readonly int CleanId = ServerIdHelper.GetDeterministicHashCode(CleanServer);
    private static readonly int EmptyId = ServerIdHelper.GetDeterministicHashCode(EmptyServer);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task WithStoreAsync(Func<NpgsqlConnection, CancellationToken, Task> seed, Func<NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the identity capacity read live tests (they mint their own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, DataId, DataServer, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, CleanId, CleanServer, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, EmptyId, EmptyServer, ct);

        var bodySucceeded = false;
        try
        {
            await seed(connection, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            await body(postgres, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static Task Marker(NpgsqlConnection c, CancellationToken ct, int serverId, string server, string database, DateTime at) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO collect.identity_capacity (collection_id, collection_time, server_id, server_name, database_name) VALUES ($1,$2,$3,$4,$5)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), serverId, server, database);

    private static Task Column(
        NpgsqlConnection c, CancellationToken ct, string database, string table, string type, int precision, string last, string seed, string increment,
        double percent, DateTime at) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO collect.identity_capacity
(collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name, column_name, data_type, type_precision, last_value, seed_value, increment_value, percent_used)
VALUES ($1,$2,$3,$4,$5,'dbo',$6,'Id',$7,$8,$9::numeric,$10::numeric,$11::numeric,$12)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), DataId, DataServer, database, table, type, precision, last, seed, increment, percent);

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    [Fact]
    public async Task Bands_Rate_CleanMarker_AndTheUnscannedDatabase_ReadBackThroughTheReaderAndTheTool()
    {
        var now = DateTime.UtcNow;
        var oldAt = now.AddDays(-4);
        var newAt = now.AddHours(-1);
        await WithStoreAsync(async (c, ct) =>
        {
            /* Older scan. Sales: Audit (2.0e9 less 10e6), Reseeded (2.0e9, before a reseed), Gone (only here), Orders (below 70 percent). */
            await Marker(c, ct, DataId, DataServer, "Sales", oldAt);
            await Column(c, ct, "Sales", "Audit", "int", 10, "1990000000", "1", "1", 92.6, oldAt);
            await Column(c, ct, "Sales", "Reseeded", "int", 10, "2000000000", "1", "1", 93.1, oldAt);
            await Column(c, ct, "Sales", "Gone", "int", 10, "1700000000", "1", "1", 79.2, oldAt);
            await Column(c, ct, "Sales", "Orders", "int", 10, "1400000000", "1", "1", 65.2, oldAt);
            await Marker(c, ct, DataId, DataServer, "Clean", oldAt);
            await Marker(c, ct, DataId, DataServer, "Down", oldAt);
            await Column(c, ct, "Down", "Countdown", "smallint", 5, "-30000", "-1", "-1", 91.6, oldAt);
            /* Stale was scanned 40 days ago: outside the 7-day window, so not scanned in it. */
            await Marker(c, ct, DataId, DataServer, "Stale", now.AddDays(-40));

            /* Newest scan. Warm crossed 10 percent only now: one sample, no rate. */
            await Marker(c, ct, DataId, DataServer, "Sales", newAt);
            await Column(c, ct, "Sales", "Audit", "int", 10, "2000000000", "1", "1", 93.1, newAt);
            await Column(c, ct, "Sales", "Reseeded", "int", 10, "1950000000", "1", "1", 90.8, newAt);
            await Column(c, ct, "Sales", "Warm", "int", 10, "1600000000", "1", "1", 74.5, newAt);
            await Column(c, ct, "Sales", "Orders", "int", 10, "1500000000", "1", "1", 69.8, newAt);
            await Marker(c, ct, DataId, DataServer, "Clean", newAt);
            await Marker(c, ct, DataId, DataServer, "Down", newAt);
            await Column(c, ct, "Down", "Countdown", "smallint", 5, "-31000", "-1", "-1", 94.6, newAt);

            /* A server whose every scanned database is clean. */
            await Marker(c, ct, CleanId, CleanServer, "Quiet", newAt);
        }, async (postgres, ct) =>
        {
            var read = await DarlingIdentityCapacityReader.ReadAsync(postgres, DataId, DatabaseFilter.All, now.AddHours(-168), now, 30, ct);

            Assert.Equal(new[] { "Clean", "Down", "Sales" }, read.Databases.Select(d => d.DatabaseName).ToArray());
            Assert.Equal(
                new[] { "Countdown", "Audit", "Reseeded", "Warm", "Orders" },
                read.Columns.Select(f => f.TableName).ToArray());
            Assert.DoesNotContain(read.Columns, f => f.TableName == "Gone");

            var audit = read.Columns.Single(f => f.TableName == "Audit");
            Assert.Equal("critical", audit.Band);
            Assert.Equal(93.1, audit.PercentUsed!.Value, 1);
            Assert.Equal(new BigInteger(147_483_647), audit.ValuesRemaining);
            /* 10,000,000 values in the 3 days 23 hours between the samples. */
            var days = (newAt - oldAt).TotalDays;
            Assert.Equal(10_000_000 / days, audit.Exhaustion!.Value.StepsPerDay, 0);
            Assert.Equal(147_483_647 / (10_000_000 / days), audit.Exhaustion.Value.DaysLeft, 1);

            var down = read.Columns.Single(f => f.TableName == "Countdown");
            Assert.Equal("critical", down.Band);
            Assert.Equal(new BigInteger(32_768 - 31_000), down.ValuesRemaining);
            Assert.Equal(1_000 / days, down.Exhaustion!.Value.StepsPerDay, 0);

            Assert.Equal("critical", read.Columns.Single(f => f.TableName == "Reseeded").Band);
            Assert.Null(read.Columns.Single(f => f.TableName == "Reseeded").Exhaustion);
            Assert.Equal("warning", read.Columns.Single(f => f.TableName == "Warm").Band);
            Assert.Null(read.Columns.Single(f => f.TableName == "Warm").Exhaustion);
            Assert.Equal("ok", read.Columns.Single(f => f.TableName == "Orders").Band);

            var root = Parse(await DarlingMcpIdentityCapacityTools.GetIdentityCapacity(postgres, DataServer, 168, null, null, 200, ct));
            var summary = root.GetProperty("summary");
            Assert.Equal(3, summary.GetProperty("databases_scanned").GetInt32());
            Assert.Equal(5, summary.GetProperty("columns_above_10_percent").GetInt32());
            Assert.Equal(3, summary.GetProperty("critical").GetInt32());
            Assert.Equal(1, summary.GetProperty("warning").GetInt32());
            Assert.Equal("critical", summary.GetProperty("worst_band").GetString());
            var databases = root.GetProperty("databases").EnumerateArray().ToDictionary(d => d.GetProperty("database").GetString()!);
            Assert.Equal(0, databases["Clean"].GetProperty("columns_above_10_percent").GetInt32());
            Assert.True(databases["Clean"].GetProperty("scanned").GetBoolean());
            Assert.False(databases.ContainsKey("Stale"));
            var first = root.GetProperty("columns")[0];
            Assert.Equal("Countdown", first.GetProperty("table").GetString());
            Assert.Equal("1768", first.GetProperty("values_remaining").GetString());
            Assert.Equal(JsonValueKind.Number, first.GetProperty("days_to_exhaustion").ValueKind);
            Assert.Equal(JsonValueKind.Null, root.GetProperty("columns").EnumerateArray().Single(x => x.GetProperty("table").GetString() == "Warm").GetProperty("days_to_exhaustion").ValueKind);

            /* A database that was never scanned reads as not scanned, with its name in the answer. */
            var unscanned = Parse(await DarlingMcpIdentityCapacityTools.GetIdentityCapacity(postgres, DataServer, 168, null, "Unscanned", 200, ct));
            Assert.Equal("unavailable", unscanned.GetProperty("status").GetString());
            Assert.Contains("'Unscanned'", unscanned.GetProperty("message").GetString());

            /* A server whose databases are all clean. */
            var clean = Parse(await DarlingMcpIdentityCapacityTools.GetIdentityCapacity(postgres, CleanServer, 168, null, null, 200, ct));
            Assert.Equal("empty", clean.GetProperty("status").GetString());
            Assert.Equal(1, clean.GetProperty("summary").GetProperty("databases_scanned").GetInt32());

            /* A server with no capture yet. */
            var empty = Parse(await DarlingMcpIdentityCapacityTools.GetIdentityCapacity(postgres, EmptyServer, 168, null, null, 200, ct));
            Assert.Equal("unavailable", empty.GetProperty("status").GetString());
            Assert.Contains("identity_capacity", empty.GetProperty("message").GetString());
        });
    }

    private static Task ConfigRow(NpgsqlConnection c, CancellationToken ct, string database, string state, DateTime at) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO database_config (config_id, capture_time, server_id, server_name, database_name, state_desc) VALUES ($1,$2,$3,$4,$5,$6)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), DataId, DataServer, database, state);

    [Fact]
    public async Task ADatabaseWithNoScanInTheWindow_IsListedAsNotYetScanned_WithItsLastScanWhenOneExists_NeverLeftOutOrClean()
    {
        var now = DateTime.UtcNow;
        var oldConfig = now.AddDays(-3);
        var newConfig = now.AddHours(-2);
        var scanAt = now.AddHours(-1);
        var earlierScan = now.AddDays(-20);
        await WithStoreAsync(async (c, ct) =>
        {
            /* An older config capture still lists a database that was dropped since: only the newest capture counts. */
            await ConfigRow(c, ct, "Dropped", "ONLINE", oldConfig);
            foreach (var (name, state) in new[]
            {
                ("Sales", "ONLINE"), ("Fresh", "ONLINE"), ("Behind", "ONLINE"), ("Offline", "OFFLINE"),
                ("ReportServer", "ONLINE"), ("master", "ONLINE"), ("PerformanceMonitor", "ONLINE"),
            })
                await ConfigRow(c, ct, name, state, newConfig);

            /* Sales was scanned in the window (and is clean). Behind was scanned 20 days ago, outside the 7-day window. Fresh never was. */
            await Marker(c, ct, DataId, DataServer, "Sales", scanAt);
            await Marker(c, ct, DataId, DataServer, "Behind", earlierScan);
            await Marker(c, ct, DataId, DataServer, "Behind", earlierScan.AddDays(-1));
        }, async (postgres, ct) =>
        {
            var read = await DarlingIdentityCapacityReader.ReadAsync(postgres, DataId, DatabaseFilter.All, now.AddHours(-168), now, 30, ct);

            Assert.Equal(new[] { "Sales" }, read.Databases.Select(d => d.DatabaseName).ToArray());
            Assert.Equal(new[] { "Behind", "Fresh" }, read.Unscanned.Select(d => d.DatabaseName).ToArray());
            Assert.Equal(DarlingMcpTestData.Naive(earlierScan), read.Unscanned.Single(d => d.DatabaseName == "Behind").LastScannedAt!.Value, TimeSpan.FromMilliseconds(5));
            Assert.Null(read.Unscanned.Single(d => d.DatabaseName == "Fresh").LastScannedAt);

            /* The database filter narrows the list the same way it narrows the scans. */
            var one = await DarlingIdentityCapacityReader.ReadAsync(postgres, DataId, DatabaseFilter.One("Fresh"), now.AddHours(-168), now, 30, ct);
            Assert.Equal(new[] { "Fresh" }, one.Unscanned.Select(d => d.DatabaseName).ToArray());
            Assert.Empty(one.Databases);

            var root = Parse(await DarlingMcpIdentityCapacityTools.GetIdentityCapacity(postgres, DataServer, 168, null, null, 200, ct));
            Assert.False(root.TryGetProperty("status", out _), "a server with a database not yet scanned is not an empty (clean) answer");
            Assert.Equal(1, root.GetProperty("summary").GetProperty("databases_scanned").GetInt32());
            Assert.Equal(2, root.GetProperty("summary").GetProperty("databases_not_yet_scanned").GetInt32());
            var databases = root.GetProperty("databases").EnumerateArray().ToDictionary(d => d.GetProperty("database").GetString()!);
            Assert.True(databases["Sales"].GetProperty("scanned").GetBoolean());
            Assert.False(databases["Fresh"].GetProperty("scanned").GetBoolean());
            Assert.Equal(JsonValueKind.Null, databases["Fresh"].GetProperty("last_scanned_at").ValueKind);
            Assert.Equal(JsonValueKind.String, databases["Behind"].GetProperty("last_scanned_at").ValueKind);
            Assert.False(databases.ContainsKey("Dropped") || databases.ContainsKey("Offline") || databases.ContainsKey("ReportServer")
                || databases.ContainsKey("master") || databases.ContainsKey("PerformanceMonitor"));

            /* No scan in the window at all, but databases waiting: unavailable, naming them. */
            var none = Parse(await DarlingMcpIdentityCapacityTools.GetIdentityCapacity(postgres, DataServer, 168, null, "Fresh", 200, ct));
            Assert.Equal("unavailable", none.GetProperty("status").GetString());
            Assert.Contains("1 database(s) not yet scanned (Fresh)", none.GetProperty("message").GetString());
        });
    }
}
