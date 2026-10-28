/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6875 part 1: the identity capacity arithmetic. The type's range is the end a positive increment moves to; a negative increment moves
/// to the minimum; the percent is of the values from the seed to that end.
/// </summary>
public sealed class IdentityCapacityMathTests
{
    private static IdentityCapacity? Calc(string? type, int precision, BigInteger seed, BigInteger increment, BigInteger? last) =>
        IdentityCapacityMath.Calculate(type, precision, seed, increment, last);

    [Theory]
    [InlineData("tinyint", 0, 1L, 1L, 255L, 255L, 255L, 100.0)]
    [InlineData("tinyint", 0, 1L, 1L, 128L, 128L, 255L, 50.196)]
    [InlineData("tinyint", 0, 255L, -1L, 0L, 256L, 256L, 100.0)]
    [InlineData("tinyint", 0, 255L, -1L, 128L, 128L, 256L, 50.0)]
    [InlineData("smallint", 0, 1L, 1L, 32767L, 32767L, 32767L, 100.0)]
    [InlineData("smallint", 0, 1L, 1L, 16384L, 16384L, 32767L, 50.0015)]
    [InlineData("smallint", 0, -1L, -1L, -32768L, 32768L, 32768L, 100.0)]
    [InlineData("smallint", 0, -1L, -1L, -1L, 1L, 32768L, 0.003)]
    [InlineData("int", 0, 1L, 1L, 2147483647L, 2147483647L, 2147483647L, 100.0)]
    [InlineData("int", 0, 1L, 1L, 1073741824L, 1073741824L, 2147483647L, 50.0)]
    [InlineData("int", 0, -2147483648L, 1L, 0L, 2147483649L, 4294967296L, 50.0)]
    [InlineData("int", 0, 1L, 10L, 2147483641L, 214748365L, 214748365L, 100.0)]
    [InlineData("bigint", 0, 1L, 1L, 4611686018427387904L, 4611686018427387904L, 9223372036854775807L, 50.0)]
    [InlineData("decimal", 5, 1L, 1L, 50000L, 50000L, 99999L, 50.0005)]
    [InlineData("numeric", 5, 1L, 1L, 99999L, 99999L, 99999L, 100.0)]
    [InlineData("decimal", 5, -1L, -1L, -99999L, 99999L, 99999L, 100.0)]
    public void ThePercent_IsOfTheValuesBetweenTheSeedAndTheEndTheIncrementMovesTo(
        string type, int precision, long seed, long increment, long last, long expectedUsed, long expectedTotal, double expectedPercent)
    {
        var capacity = Calc(type, precision, seed, increment, last);

        Assert.NotNull(capacity);
        Assert.Equal(new BigInteger(expectedUsed), capacity.Value.Used);
        Assert.Equal(new BigInteger(expectedTotal), capacity.Value.Total);
        Assert.Equal(new BigInteger(expectedTotal - expectedUsed), capacity.Value.Remaining);
        Assert.Equal(expectedPercent, capacity.Value.PercentUsed, 4);
    }

    [Fact]
    public void ABigintSeededAtItsMinimum_HasTheWholeRange_AndHalfOfItIsUsedAtZero()
    {
        var capacity = Calc("bigint", 0, long.MinValue, 1, 0)!.Value;

        Assert.Equal((BigInteger)ulong.MaxValue + 1, capacity.Total);
        Assert.Equal(50.0, capacity.PercentUsed, 4);
    }

    [Fact]
    public void ADecimalOfThirtyEightDigits_IsExactPastWhatADoubleHolds()
    {
        var max = BigInteger.Pow(10, 38) - 1;
        var capacity = Calc("decimal", 38, 1, 1, BigInteger.Pow(10, 37))!.Value;

        Assert.Equal(max, capacity.Total);
        Assert.Equal(BigInteger.Pow(10, 37), capacity.Used);
        Assert.Equal(10.0, capacity.PercentUsed, 4);
    }

    [Fact]
    public void ANullLastValue_MeansNothingInsertedYet_NotAnError()
    {
        var capacity = Calc("int", 0, 1, 1, null)!.Value;

        Assert.Equal(BigInteger.Zero, capacity.Used);
        Assert.Equal(new BigInteger(int.MaxValue), capacity.Remaining);
        Assert.Equal(0.0, capacity.PercentUsed);

        /* Whatever the seed: a column seeded at the very end still reads empty until a row goes in. */
        Assert.Equal(0.0, Calc("tinyint", 0, 255, 1, null)!.Value.PercentUsed);
    }

    [Fact]
    public void ALastValueBeforeTheSeed_CountsAsNothingUsed_AndOnePastTheEndAsFull()
    {
        /* DBCC CHECKIDENT RESEED backwards leaves last_value on the wrong side of the seed. */
        Assert.Equal(0.0, Calc("int", 0, 1000, 1, 5)!.Value.PercentUsed);
        Assert.Equal(100.0, Calc("tinyint", 0, 1, 1, 1000)!.Value.PercentUsed);
    }

    [Fact]
    public void ATypeWithNoCapacity_AZeroIncrement_AndASeedOutsideTheRange_ReturnNull()
    {
        Assert.Null(Calc("varchar", 0, 1, 1, 1));
        Assert.Null(Calc("decimal", 0, 1, 1, 1));
        Assert.Null(Calc("decimal", 39, 1, 1, 1));
        Assert.Null(Calc("int", 0, 1, 0, 1));
        Assert.Null(Calc("tinyint", 0, 300, 1, 301));
        Assert.Null(Calc(null, 0, 1, 1, 1));
    }

    [Fact]
    public void TheRanges_AreTheEnginesOwn()
    {
        Assert.True(IdentityCapacityMath.TryGetRange("tinyint", 0, out var min, out var max));
        Assert.Equal((BigInteger.Zero, new BigInteger(255)), (min, max));
        Assert.True(IdentityCapacityMath.TryGetRange("SMALLINT", 0, out min, out max));
        Assert.Equal((new BigInteger(short.MinValue), new BigInteger(short.MaxValue)), (min, max));
        Assert.True(IdentityCapacityMath.TryGetRange("decimal", 3, out min, out max));
        Assert.Equal((new BigInteger(-999), new BigInteger(999)), (min, max));
        Assert.False(IdentityCapacityMath.TryGetRange("float", 0, out _, out _));
    }
}

/// <summary>
/// #6875 part 1: the identity capacity collector. Its SQL guards, its database list, the rows it keeps (top 20 above 10 percent plus one
/// marker per scanned database), a database it did not scan, a budget stop, and its V222 table.
/// </summary>
public sealed class IdentityCapacityCollectorTests
{
    private static CollectorContext Context(bool azureDb = false) => new()
    {
        CollectionTime = DateTime.UtcNow, ServerId = 1, ServerName = "s",
        Target = new CollectorTargetInfo { SqlMajorVersion = 16, IsAzureSqlDb = azureDb },
        Deltas = new CollectorDeltaCalculator(),
    };

    private static DataTable NewTable()
    {
        var table = new DataTable();
        table.Columns.Add("schema_name", typeof(string));
        table.Columns.Add("table_name", typeof(string));
        table.Columns.Add("column_name", typeof(string));
        table.Columns.Add("data_type", typeof(string));
        table.Columns.Add("type_precision", typeof(int));
        table.Columns.Add("last_value", typeof(string));
        table.Columns.Add("seed_value", typeof(string));
        table.Columns.Add("increment_value", typeof(string));
        table.Columns.Add("skip_reason", typeof(string));
        return table;
    }

    private static void AddMarker(DataTable table) =>
        table.Rows.Add(DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value);

    private static void AddSkip(DataTable table, string reason) =>
        table.Rows.Add(DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, reason);

    private static void AddInt(DataTable table, string name, long last) =>
        table.Rows.Add("dbo", name, "id", "int", 10, last.ToString(System.Globalization.CultureInfo.InvariantCulture), "1", "1", DBNull.Value);

    private static async Task<List<IdentityCapacityCollector.Row>> ReadAsync(string database, DataTable table)
    {
        var rows = new List<IdentityCapacityCollector.Row>();
        await IdentityCapacityCollector.Instance.ReadItemAsync(database, table.CreateDataReader(), rows, Context(), TestContext.Current.CancellationToken);
        return rows;
    }

    [Fact]
    public void ThePayload_ReadsCatalogViewsOnly_UnderADirtyReadAndALockTimeout_AndStopsWithoutTheRightToSeeTheCatalog()
    {
        var body = IdentityCapacityCollector.BuildPayloadBody();

        Assert.Contains("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED", body, StringComparison.Ordinal);
        Assert.Contains("SET LOCK_TIMEOUT 5000", body, StringComparison.Ordinal);
        Assert.Contains("FROM sys.identity_columns AS ic", body, StringComparison.Ordinal);
        Assert.Contains("o.is_ms_shipped = 0", body, StringComparison.Ordinal);
        Assert.Contains("ic.system_type_id IN (48, 52, 56, 127, 106, 108)", body, StringComparison.Ordinal);
        Assert.Contains("TOP (20)", body, StringComparison.Ordinal);
        Assert.DoesNotContain("COUNT(", body, StringComparison.OrdinalIgnoreCase);

        /* The gate runs before any select, so a login without the right gets no result set and no marker. */
        var gate = body.IndexOf("HAS_PERMS_BY_NAME(DB_NAME(), N'DATABASE', N'VIEW DEFINITION')", StringComparison.Ordinal);
        Assert.True(gate > 0);
        Assert.True(gate < body.IndexOf("SELECT /* PerformanceMonitorLite */", StringComparison.Ordinal));

        /* The on-premises per-database text runs the body through the database's own sp_executesql, quote-doubled. */
        var perItem = IdentityCapacityCollector.Instance.BuildPerItemQuery("Sales]Db", Context());
        Assert.Contains("[Sales]]Db].sys.sp_executesql", perItem.Text, StringComparison.Ordinal);
        Assert.Contains("N''DATABASE''", perItem.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void TheEnumeration_SkipsSnapshotsAndTheDatabasesTheServersScopeLeavesOut_AndAzureDoesNotEnumerate()
    {
        var context = new CollectorContext
        {
            CollectionTime = DateTime.UtcNow, ServerId = 1, ServerName = "s", Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            Deltas = new CollectorDeltaCalculator(),
            DatabaseScope = new[] { "KeepA" },
            ExcludedDatabases = new[] { "Skip" },
        };

        var query = IdentityCapacityCollector.Instance.BuildEnumerationQuery(context)!;

        Assert.Contains("d.state_desc = N'ONLINE'", query.Text, StringComparison.Ordinal);
        Assert.Contains("d.database_id > 4", query.Text, StringComparison.Ordinal);

        /* A snapshot, a secondary and a database the login cannot enter stay in the list: the per-database text records each with its reason. */
        Assert.DoesNotContain("source_database_id", query.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("is_primary_replica", query.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("HAS_DBACCESS", query.Text, StringComparison.Ordinal);
        Assert.Contains("@scope_db_0", query.Text, StringComparison.Ordinal);
        Assert.Contains("@excl_db_0", query.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("/*EXCLUSION_FILTER*/", query.Text, StringComparison.Ordinal);
        Assert.Contains(query.Parameters, p => Equals(p.Value, "KeepA"));
        Assert.Contains(query.Parameters, p => Equals(p.Value, "Skip"));

        Assert.Null(IdentityCapacityCollector.Instance.BuildEnumerationQuery(Context(azureDb: true)));
        Assert.True(IdentityCapacityCollector.Instance.RunsPerDatabase(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.False(IdentityCapacityCollector.Instance.RunsPerDatabase(new CollectorTargetInfo { SqlMajorVersion = 16 }));
        Assert.Contains("sys.identity_columns", IdentityCapacityCollector.Instance.BuildQuery(Context(azureDb: true)).Text, StringComparison.Ordinal);
        Assert.Throws<NotSupportedException>(() => IdentityCapacityCollector.Instance.BuildQuery(Context()));
    }

    [Fact]
    public void ADatabaseTheCollectorSkipsOnPurpose_IsAnsweredWithItsReasonBeforeItIsEntered()
    {
        var perItem = IdentityCapacityCollector.Instance.BuildPerItemQuery("O'Brien", Context()).Text;

        /* The catalog check names the item (quote-doubled), the three reasons, and answers before the database is entered. */
        Assert.Contains("WHERE d.name = N'O''Brien'", perItem, StringComparison.Ordinal);
        Assert.Contains("THEN 'snapshot'", perItem, StringComparison.Ordinal);
        Assert.Contains("THEN 'secondary'", perItem, StringComparison.Ordinal);
        Assert.Contains("THEN 'no_access'", perItem, StringComparison.Ordinal);
        Assert.Contains("HAS_DBACCESS(d.name) <> 1", perItem, StringComparison.Ordinal);
        Assert.Contains("d.source_database_id IS NOT NULL", perItem, StringComparison.Ordinal);
        Assert.Contains("ISNULL(drs.is_primary_replica, 0) <> 1", perItem, StringComparison.Ordinal);
        var answer = perItem.IndexOf("skip_reason = @skip_reason", StringComparison.Ordinal);
        var returns = perItem.IndexOf("RETURN;", StringComparison.Ordinal);
        var enter = perItem.IndexOf("EXECUTE [O'Brien].sys.sp_executesql", StringComparison.Ordinal);
        Assert.True(answer > 0 && answer < returns && returns < enter, "the skip answer and RETURN come before the database is entered");

        /* The payload carries the ninth column on both result sets of its UNION. */
        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(IdentityCapacityCollector.BuildPayloadBody(), @"varchar\(20\), NULL").Count);
    }

    [Theory]
    [InlineData("secondary", "not scanned: readable secondary")]
    [InlineData("snapshot", "not scanned: database snapshot")]
    [InlineData("no_access", "not scanned: the monitoring login has no access to this database (on Amazon RDS it needs a user in each database)")]
    public async Task ASkippedDatabase_StoresOneRowWithItsReason_NoMarker_AndNoColumns(string reason, string text)
    {
        var table = NewTable();
        AddSkip(table, reason);

        var rows = await ReadAsync("SkippedDb", table);

        var row = Assert.Single(rows);
        Assert.True(row.IsSkip);
        Assert.False(row.IsMarker, "a skipped database is not a scan marker");
        Assert.Equal(reason, row.SkipReason);
        Assert.Equal("SkippedDb", row.DbName);
        Assert.Null(row.ColumnName);
        Assert.Equal(text, IdentityCapacityCollector.SkipReasonText(reason));
    }

    [Fact]
    public void AnUnknownSkipReason_HasNoSentenceOfItsOwn()
    {
        Assert.Null(IdentityCapacityCollector.SkipReasonText("made_up"));
        Assert.Null(IdentityCapacityCollector.SkipReasonText(null));
    }

    [Fact]
    public async Task ACleanDatabase_StoresTheMarkerRowAlone()
    {
        var table = NewTable();
        AddMarker(table);

        var rows = await ReadAsync("CleanDb", table);

        var marker = Assert.Single(rows);
        Assert.True(marker.IsMarker);
        Assert.Null(marker.SkipReason);
        Assert.Equal("CleanDb", marker.DbName);
        Assert.Null(marker.ColumnName);
        Assert.Null(marker.PercentUsed);
    }

    [Fact]
    public async Task ADatabaseTheQueryDidNotScan_StoresNothing_NotEvenAMarker()
    {
        /* No result set at all (the right to see the catalog was missing) is an empty reader. */
        var rows = await ReadAsync("HiddenDb", NewTable());

        Assert.Empty(rows);
    }

    [Fact]
    public async Task OnlyColumnsAboveTenPercent_AreKept_FullestFirst_BehindTheMarker()
    {
        var table = NewTable();
        AddMarker(table);
        AddInt(table, "Low", 100_000_000);             // 4.66 percent: dropped
        AddInt(table, "AtTen", 214_748_365);           // 10.0000 percent exactly: dropped (above 10 only)
        AddInt(table, "Mid", 1_073_741_824);           // 50 percent
        AddInt(table, "High", 2_147_000_000);          // 99.98 percent
        table.Rows.Add("dbo", "Empty", "id", "int", 10, DBNull.Value, "1", "1");   // no row inserted yet

        var rows = await ReadAsync("Sales", table);

        Assert.Equal(new[] { null, "High", "Mid" }, rows.Select(r => r.TableName).ToArray());
        Assert.True(rows[0].IsMarker);
        Assert.Equal(50.0, rows[2].PercentUsed!.Value, 4);
        Assert.InRange(rows[1].PercentUsed!.Value, 99.9, 100.0);
        Assert.All(rows, r => Assert.Equal("Sales", r.DbName));
        Assert.Equal(new BigInteger(1_073_741_824), rows[2].LastValue);
        Assert.Equal(BigInteger.One, rows[2].SeedValue);
        Assert.Equal(BigInteger.One, rows[2].IncrementValue);
        Assert.Equal(10, rows[2].TypePrecision);
    }

    [Fact]
    public async Task MoreThanTwentyHighColumns_KeepTheTwentyFullest_PlusTheMarker()
    {
        var table = NewTable();
        AddMarker(table);
        for (var i = 0; i < 30; i++)
        {
            /* 30 columns from 50 percent up in one-percent steps, shuffled by name so the order is the percent's. */
            AddInt(table, "T" + (29 - i).ToString("00", System.Globalization.CultureInfo.InvariantCulture), (long)(2147483647L * (0.50 + (i * 0.01))));
        }

        var rows = await ReadAsync("Wide", table);

        Assert.Equal(1 + IdentityCapacityMath.MaxColumnsPerDatabase, rows.Count);
        Assert.True(rows[0].IsMarker);
        var percents = rows.Skip(1).Select(r => r.PercentUsed!.Value).ToList();
        Assert.Equal(percents.OrderByDescending(p => p).ToList(), percents);
        /* The twenty fullest of thirty: the lowest kept is the 11th step up (60 percent), not the 50 the list started at. */
        Assert.InRange(percents[^1], 59.9, 60.1);
    }

    [Fact]
    public async Task ANegativeSeedAndANegativeIncrement_CountTowardTheEndTheyMoveTo()
    {
        var table = NewTable();
        AddMarker(table);
        /* smallint counting down from -1: at -32000 it has used 32000 of 32768. */
        table.Rows.Add("dbo", "Down", "id", "smallint", 5, "-32000", "-1", "-1");
        /* int seeded at its minimum counting up: at 0 it has used half of 4,294,967,296. */
        table.Rows.Add("dbo", "Up", "id", "int", 10, "0", "-2147483648", "1");

        var rows = await ReadAsync("Neg", table);

        Assert.Equal(new[] { "Down", "Up" }, rows.Skip(1).Select(r => r.TableName).ToArray());
        Assert.InRange(rows[1].PercentUsed!.Value, 97.6, 97.7);
        Assert.Equal(50.0, rows[2].PercentUsed!.Value, 4);
    }

    [Fact]
    public async Task ADecimalColumnPastWhatACLRDecimalHolds_ReadsExact_AndStoresItsPercent()
    {
        var table = NewTable();
        AddMarker(table);
        var big = (BigInteger.Pow(10, 37) * 2).ToString(System.Globalization.CultureInfo.InvariantCulture);
        table.Rows.Add("dbo", "Wide", "id", "decimal", 38, big, "1", "1");

        var rows = await ReadAsync("Dec", table);

        var wide = rows[1];
        Assert.Equal(BigInteger.Pow(10, 37) * 2, wide.LastValue);
        Assert.Equal(20.0, wide.PercentUsed!.Value, 4);
    }

    [Fact]
    public async Task ABudgetStop_LeavesTheUnscannedDatabaseWithNoMarker_AndTheScannedOnesWithTheirs()
    {
        var items = new List<string> { "First", "Slow", "Last" };
        var written = new List<IdentityCapacityCollector.Row>();
        var failed = new List<string>();

        var result = await EnumeratedCollectorDriver.RunAsync<IdentityCapacityCollector.Row>(
            items,
            perItemWatermark: null,
            readItem: async (item, ct) =>
            {
                if (item == "Slow")
                {
                    /* The database that runs past its wall clock: the driver cancels the token and the read gives up. */
                    await Task.Delay(TimeSpan.FromSeconds(30), ct);
                }

                var table = NewTable();
                AddMarker(table);
                return await ReadAsync(item, table);
            },
            writeBatch: (batch, _) =>
            {
                written.AddRange(batch);
                return Task.CompletedTask;
            },
            onItemComplete: (_, _, _, _) => { },
            onItemError: (item, _) => failed.Add(item),
            TestContext.Current.CancellationToken,
            perItemBudget: TimeSpan.FromMilliseconds(200));

        Assert.Equal(new[] { "First", "Last" }, written.Select(r => r.DbName).ToArray());
        Assert.All(written, r => Assert.True(r.IsMarker));
        Assert.DoesNotContain(written, r => r.DbName == "Slow");
        Assert.Equal(new[] { "Slow" }, failed.ToArray());
        Assert.Equal(1, result.Failed);
        Assert.Equal(3, result.Attempted);
    }

    /// <summary>A clock that moves only when a read says so, so the pass budget is exercised without waiting.</summary>
    private sealed class HandClock : TimeProvider
    {
        private long _ticks;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override long GetTimestamp() => _ticks;
        public void Advance(TimeSpan by) => _ticks += by.Ticks;
    }

    /// <summary>One pass through the real driver: each database costs a minute of the hand clock and stores the marker the collector reads.</summary>
    private static async Task<(EnumeratedRunResult Result, List<string> Scanned)> RunPassAsync(
        IReadOnlyList<string> allDatabases, string? resumeAfter, TimeSpan budget)
    {
        var clock = new HandClock();
        var scanned = new List<string>();
        var result = await EnumeratedCollectorDriver.RunAsync<IdentityCapacityCollector.Row>(
            PassBudget.ResumeOrder(allDatabases, resumeAfter),
            perItemWatermark: null,
            readItem: async (item, _) =>
            {
                clock.Advance(TimeSpan.FromMinutes(1));
                var table = NewTable();
                AddMarker(table);
                return await ReadAsync(item, table);
            },
            writeBatch: (batch, _) =>
            {
                scanned.AddRange(batch.Where(r => r.IsMarker).Select(r => r.DbName));
                return Task.CompletedTask;
            },
            onItemComplete: (_, _, _, _) => { },
            onItemError: (_, _) => { },
            TestContext.Current.CancellationToken,
            passBudget: new PassBudget(budget, clock));
        return (result, scanned);
    }

    [Fact]
    public async Task APassThatSpendsItsBudget_StopsAfterDatabaseK_TheNextStartsAtKPlusOne_AndWraps()
    {
        var databases = new[] { "A", "B", "C", "D", "E" };

        var (pass1, scanned1) = await RunPassAsync(databases, resumeAfter: null, TimeSpan.FromMinutes(2));
        Assert.Equal(new[] { "A", "B" }, scanned1.ToArray());
        Assert.Equal("B", pass1.StoppedAfter);
        Assert.Equal(2, pass1.Attempted);
        Assert.Equal(5, pass1.Total);

        var (pass2, scanned2) = await RunPassAsync(databases, resumeAfter: pass1.StoppedAfter, TimeSpan.FromMinutes(4));
        Assert.Equal(new[] { "C", "D", "E", "A" }, scanned2.ToArray());
        Assert.Equal("A", pass2.StoppedAfter);

        /* B is the one pass 2 did not reach: it holds no marker from that pass, only the one pass 1 stored. */
        Assert.DoesNotContain("B", scanned2);
        Assert.Contains("B", scanned1);

        var (pass3, scanned3) = await RunPassAsync(databases, resumeAfter: pass2.StoppedAfter, TimeSpan.FromMinutes(30));
        Assert.Equal(new[] { "B", "C", "D", "E", "A" }, scanned3.ToArray());
        Assert.Null(pass3.StoppedAfter);
    }

    [Fact]
    public async Task ABudgetSmallerThanOneDatabase_StillReadsOne_SoTheRotationMovesOn()
    {
        var databases = new[] { "A", "B", "C" };
        var (pass, scanned) = await RunPassAsync(databases, resumeAfter: "A", TimeSpan.FromTicks(1));
        Assert.Equal(new[] { "B" }, scanned.ToArray());
        Assert.Equal("B", pass.StoppedAfter);
    }

    [Theory]
    [InlineData(null, "A,B,C")]
    [InlineData("A", "B,C,A")]
    [InlineData("B", "C,A,B")]
    [InlineData("C", "A,B,C")]
    [InlineData("BB", "C,A,B")]
    [InlineData("0", "A,B,C")]
    public void TheResumeOrder_StartsAfterTheLastDatabaseStarted_ByValue_AndWraps(string? resumeAfter, string expected)
    {
        Assert.Equal(expected, string.Join(',', PassBudget.ResumeOrder(new[] { "C", "A", "B" }, resumeAfter)));
    }

    [Fact]
    public async Task TheRunner_KeepsWhereAStoppedPassEnded_PerServerAndCollector_AndClearsItWhenAPassCoversEveryDatabase()
    {
        await using var postgres = NpgsqlDataSource.Create("Host=127.0.0.1;Database=never_opened");
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var identity = IdentityCapacityCollector.Instance;
        var databases = new[] { "C", "A", "B" };

        var (first, firstOrder) = runner.BeginPass(identity, serverId: 7, databases);
        Assert.NotNull(first);
        Assert.Equal("A,B,C", string.Join(',', firstOrder));
        Assert.NotNull(runner.EndPass(first, identity, 7, stoppedAfter: "A", reached: 1, total: 3));

        /* The next pass on that server starts at B. Another server, and a collector with no pass budget, are untouched. */
        var (second, secondOrder) = runner.BeginPass(identity, 7, databases);
        Assert.Equal("B,C,A", string.Join(',', secondOrder));
        Assert.Equal("A,B,C", string.Join(',', runner.BeginPass(identity, 8, databases).Items));
        var (none, untouched) = runner.BeginPass(AgentStatusCollector.Instance, 7, databases);
        Assert.Null(none);
        Assert.Equal("C,A,B", string.Join(',', untouched));

        Assert.Null(runner.EndPass(second, identity, 7, stoppedAfter: null, reached: 3, total: 3));
        Assert.Equal("A,B,C", string.Join(',', runner.BeginPass(identity, 7, databases).Items));
    }

    [Fact]
    public void TheCollector_IsRegistered_EverySixHours_NinetyDays_AsAPlainDarlingOnlyTableOnEveryTarget()
    {
        var instance = IdentityCapacityCollector.Instance;
        Assert.Contains(instance, CollectorCatalog.All);
        Assert.Equal(360, CollectorScheduleDefaults.All["identity_capacity"].FrequencyMinutes);
        Assert.Equal(90, CollectorScheduleDefaults.All["identity_capacity"].RetentionDays);
        Assert.True(instance.StaysPlainTable);
        Assert.True(instance.IsDarlingOnly);
        Assert.Equal(TimeSpan.FromSeconds(120), instance.PerItemWallClockBudget);
        Assert.True(instance.CommandTimeoutSecondsOverride is > 0 and <= 300);
        Assert.DoesNotContain(TimescaleSupport.HypertableTables, t => t.TargetTable == "identity_capacity");
        Assert.All(
            new[]
            {
                new CollectorTargetInfo { SqlMajorVersion = 13 },
                new CollectorTargetInfo { SqlMajorVersion = 17 },
                new CollectorTargetInfo { IsAzureSqlDb = true },
                new CollectorTargetInfo { IsAzureManagedInstance = true },
                new CollectorTargetInfo { IsAwsRds = true },
            },
            t => Assert.True(instance.AppliesTo(t)));
    }

    [Fact]
    public void TheV222Table_IsTheGeneratorsOutputForTheCatalogEntry_AndAddsTheMarkerIndex()
    {
        var rung = PgMigrations.Scripts.Single(m => m.Version == 222).Sql.Replace("collect.", "", StringComparison.Ordinal);
        var generated = PgSchemaGenerator.CreateTable(IdentityCapacityCollector.Instance) + "\n\n" + PgSchemaGenerator.CreateIndex(IdentityCapacityCollector.Instance);

        static string Norm(string s) => string.Join(' ', s.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));

        Assert.StartsWith(Norm(generated).TrimEnd(';'), Norm(rung), StringComparison.Ordinal);
        Assert.Contains("WHERE column_name IS NULL", rung, StringComparison.Ordinal);
    }
}

/// <summary>
/// #6875 part 1: rows written through the product's own COPY writer land in <c>collect.identity_capacity</c> with every column intact,
/// the marker row all NULL but the database, and a <c>decimal(38,0)</c> value exact.
/// </summary>
[Collection("live-postgres")]
public sealed class IdentityCapacityStoreLiveTests
{
    private const int ServerId = 6875;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheMarkerAndTheColumns_GoThroughTheProductsWriter_AndReadBackExact()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the identity capacity live test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var definition = IdentityCapacityCollector.Instance;
            var runTime = new DateTime(2026, 10, 28, 6, 0, 0, DateTimeKind.Unspecified);
            var rows = new[]
            {
                new IdentityCapacityCollector.Row { DbName = "Clean" },
                new IdentityCapacityCollector.Row { DbName = "Rds", SkipReason = IdentityCapacityCollector.SkipNoAccess },
                new IdentityCapacityCollector.Row { DbName = "Sales" },
                new IdentityCapacityCollector.Row
                {
                    DbName = "Sales", SchemaName = "dbo", TableName = "Orders", ColumnName = "OrderId", DataType = "int", TypePrecision = 10,
                    LastValue = 1_500_000_000, SeedValue = 1, IncrementValue = 1, PercentUsed = 69.8491,
                },
                new IdentityCapacityCollector.Row
                {
                    DbName = "Sales", SchemaName = "dbo", TableName = "Wide", ColumnName = "Id", DataType = "decimal", TypePrecision = 28,
                    LastValue = BigInteger.Parse("5000000000000000000000000000", System.Globalization.CultureInfo.InvariantCulture),
                    SeedValue = -1, IncrementValue = -1, PercentUsed = 50.0,
                },
            };

            var writer = new PgCollectorRowWriter();
            await using (var importer = await connection.BeginBinaryImportAsync(PgCollectorRowWriter.CopyCommandFor(definition), ct))
            {
                writer.Importer = importer;
                foreach (var row in rows)
                {
                    await importer.StartRowAsync(ct);
                    writer.Value(CollectionIdGenerator.Next());
                    writer.Value(runTime).Value(ServerId).Value("identity-test");
                    writer.BeginPayload();
                    definition.WritePayload(row, writer, new CollectorContext { ServerId = ServerId, ServerName = "identity-test", CollectionTime = runTime, Deltas = null! });
                    writer.EndPayload(definition.PayloadColumns.Count);
                }

                await importer.CompleteAsync(ct);
            }

            Assert.Equal(5L, await ScalarAsync(connection, "SELECT count(*) FROM collect.identity_capacity WHERE server_id = " + ServerId, ct));
            Assert.Equal(3L, await ScalarAsync(connection, "SELECT count(*) FROM collect.identity_capacity WHERE server_id = " + ServerId + " AND column_name IS NULL", ct));
            Assert.Equal("Rds", (string)(await ScalarAsync(connection, "SELECT database_name FROM collect.identity_capacity WHERE skip_reason = 'no_access'", ct))!);
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM collect.identity_capacity WHERE skip_reason IS NOT NULL", ct));

            /* The marker is all NULL but the database. */
            Assert.Equal(0L, await ScalarAsync(connection,
                "SELECT count(*) FROM collect.identity_capacity WHERE column_name IS NULL AND skip_reason IS NULL AND (schema_name IS NOT NULL OR table_name IS NOT NULL OR data_type IS NOT NULL OR type_precision IS NOT NULL "
                + "OR last_value IS NOT NULL OR seed_value IS NOT NULL OR increment_value IS NOT NULL OR percent_used IS NOT NULL)", ct));

            Assert.Equal("Sales.dbo.Orders.OrderId:int:10:1500000000:1:1:69.8491", (string)(await ScalarAsync(connection,
                "SELECT database_name || '.' || schema_name || '.' || table_name || '.' || column_name || ':' || data_type || ':' || type_precision || ':' || last_value || ':' || seed_value || ':' || increment_value || ':' || percent_used "
                + "FROM collect.identity_capacity WHERE table_name = 'Orders'", ct))!);
            Assert.Equal("5000000000000000000000000000:-1:-1", (string)(await ScalarAsync(connection,
                "SELECT last_value || ':' || seed_value || ':' || increment_value FROM collect.identity_capacity WHERE table_name = 'Wide'", ct))!);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        return await command.ExecuteScalarAsync(ct);
    }
}
