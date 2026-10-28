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
using System.Numerics;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6875 part 3b: the Viewer's Identity Capacity words as functions of the read. They are the words the web Configuration tab shows,
/// so the assertions are on the drawn values.
/// </summary>
public sealed class ViewerIdentityCapacityTextTests
{
    private static readonly DateTime Captured = new(2026, 10, 28, 12, 0, 0, DateTimeKind.Utc);

    private static IdentityColumnFact Int(string database, string table, long last, double stored, long? firstValue = null) =>
        new(database, "dbo", table, "Id", "int", 10, last, 1, 1, stored, Captured,
            firstValue ?? last, firstValue is null ? Captured : Captured.AddDays(-4));

    private static ViewerIdentityCapacityRead Read(IReadOnlyList<IdentityColumnFact> columns, params (string Database, DateTime At)[] scanned) =>
        ReadWithUnscanned(columns, [], scanned);

    private static ViewerIdentityCapacityRead ReadWithUnscanned(
        IReadOnlyList<IdentityColumnFact> columns, IReadOnlyList<IdentityNotYetScannedDatabase> unscanned, params (string Database, DateTime At)[] scanned)
    {
        var read = new IdentityCapacityRead(columns, scanned.Select(s => new IdentityScannedDatabase(s.Database, s.At)).ToList(), unscanned);
        return new ViewerIdentityCapacityRead(
            columns.Count > 0 || unscanned.Count > 0 ? ViewerIdentityCapacityRead.Data : ViewerIdentityCapacityRead.Empty, null, read);
    }

    [Fact]
    public void ACriticalColumn_DrawsItsPercent_ValuesLeft_Rate_Days_AndBand()
    {
        /* 1,990,000,000 four days before the scan and 2,000,000,000 at it: 2,500,000 values a day against 147,483,647 left. */
        var rows = ViewerIdentityCapacityText.ColumnRows(Read([Int("Sales", "Audit", 2_000_000_000, 93.1, firstValue: 1_990_000_000)], ("Sales", Captured)));

        Assert.Equal(new ViewerIdentityColumnRow("Sales", "dbo.Audit", "Id", "int", "93.1", "147,483,647", "2,500,000.0", "59.0", "critical"), rows.Single());
    }

    [Fact]
    public void AWarningColumn_WithOneSample_HasNoRateAndNoDays()
    {
        var rows = ViewerIdentityCapacityText.ColumnRows(Read([Int("Sales", "Warm", 1_600_000_000, 74.5)], ("Sales", Captured)));

        Assert.Equal(new ViewerIdentityColumnRow("Sales", "dbo.Warm", "Id", "int", "74.5", "547,483,647", "-", "-", "warning"), rows.Single());
    }

    [Fact]
    public void ADecimal38Value_IsDrawnWithEveryDigit()
    {
        var last = (BigInteger.Pow(10, 37) * 9) + 123_456_789;
        var fact = new IdentityColumnFact("Big", "dbo", "Ledger", "Id", "decimal", 38, last, 1, 1, 90.0, Captured, last, Captured);

        var row = ViewerIdentityCapacityText.ColumnRows(Read([fact], ("Big", Captured))).Single();

        Assert.Equal("decimal(38)", row.Type);
        Assert.Equal("9,999,999,999,999,999,999,999,999,999,876,543,210", row.ValuesRemaining);
        Assert.Equal("90.0", row.PercentUsed);
        Assert.Equal("critical", row.Band);
    }

    [Fact]
    public void ADecimalValueTheCollectorDidNotStore_StillDrawsTheStoredPercent_AndNoValues()
    {
        var fact = new IdentityColumnFact("Big", "dbo", "Ledger", "Id", "decimal", 38, null, 1, 1, 96.5, Captured, null, null);

        var row = ViewerIdentityCapacityText.ColumnRows(Read([fact], ("Big", Captured))).Single();

        Assert.Equal(("96.5", "-", "-", "-", "critical"), (row.PercentUsed, row.ValuesRemaining, row.RatePerDay, row.DaysToExhaustion, row.Band));
    }

    [Fact]
    public void TheStatusLine_CountsCriticalAndWarning_AndTheDatabasesScanned()
    {
        var read = Read(
            [Int("Sales", "Audit", 2_000_000_000, 93.1), Int("Sales", "Warm", 1_600_000_000, 74.5), Int("Sales", "Orders", 1_000_000_000, 46.6)],
            ("Sales", Captured), ("Clean", Captured));

        Assert.Equal("Critical: 1   Warning: 1   Columns above 10 percent: 3   Databases scanned: 2   Not yet scanned: 0   Skipped: 0", ViewerIdentityCapacityText.StatusLine(read));
    }

    [Fact]
    public void ADatabaseScannedWithNoColumn_IsScannedClean_AndOneWithColumnsSaysHowMany()
    {
        var read = Read([Int("Sales", "Audit", 2_000_000_000, 93.1), Int("Sales", "Warm", 1_600_000_000, 74.5)], ("Clean", Captured), ("Sales", Captured));

        var rows = ViewerIdentityCapacityText.ScanRows(read);

        Assert.Equal(["Clean", "Sales"], rows.Select(r => r.Database).ToArray());
        Assert.Equal(["Scanned, clean", "Scanned, 2 columns above 10 percent"], rows.Select(r => r.State).ToArray());
        Assert.Equal("Scanned, 1 column above 10 percent", ViewerIdentityCapacityText.ScanState(1));
    }

    [Fact]
    public void ADatabaseTheRotationHasNotReached_IsNotYetScanned_NeverCleanAndNeverLeftOut()
    {
        var earlier = new DateTime(2026, 9, 1, 8, 30, 0, DateTimeKind.Utc);
        var read = ReadWithUnscanned(
            [], [new IdentityNotYetScannedDatabase("Fresh", null), new IdentityNotYetScannedDatabase("Stale", earlier)], ("Clean", Captured));

        var rows = ViewerIdentityCapacityText.ScanRows(read);

        Assert.Equal(["Clean", "Fresh", "Stale"], rows.Select(r => r.Database).ToArray());
        Assert.Equal(["Scanned, clean", "Not yet scanned", "Not yet scanned"], rows.Select(r => r.State).ToArray());
        Assert.Equal("-", rows[1].LastScanned);
        Assert.Equal(earlier.ToLocalTime().ToString("g", CultureInfo.CurrentCulture), rows[2].LastScanned);
        Assert.Equal("Critical: 0   Warning: 0   Columns above 10 percent: 0   Databases scanned: 1   Not yet scanned: 2   Skipped: 0", ViewerIdentityCapacityText.StatusLine(read));
        Assert.Equal(ViewerIdentityCapacityRead.Data, read.Status);
    }

    [Fact]
    public void ADatabaseSkippedOnPurpose_SaysWhy_ApartFromNotYetScanned_AndIsNeverClean()
    {
        var skipped = new IdentitySkippedDatabase[]
        {
            new("Mirror", IdentityCapacityCollector.SkipSecondary, null),
            new("Snap", IdentityCapacityCollector.SkipSnapshot, null),
            new("Locked", IdentityCapacityCollector.SkipNoAccess, null),
        };
        var read = new ViewerIdentityCapacityRead(
            ViewerIdentityCapacityRead.Data, null,
            new IdentityCapacityRead([], [new IdentityScannedDatabase("Clean", Captured)], [new IdentityNotYetScannedDatabase("Fresh", null)], skipped));

        var rows = ViewerIdentityCapacityText.ScanRows(read);

        Assert.Equal(["Clean", "Fresh", "Mirror", "Snap", "Locked"], rows.Select(r => r.Database).ToArray());
        Assert.Equal(
            [
                "Scanned, clean", "Not yet scanned", "Not scanned: readable secondary", "Not scanned: database snapshot",
                "Not scanned: the monitoring login has no access to this database (on Amazon RDS it needs a user in each database)",
            ],
            rows.Select(r => r.State).ToArray());
        Assert.Contains("Skipped: 3", ViewerIdentityCapacityText.StatusLine(read), StringComparison.Ordinal);
        Assert.Contains("Not yet scanned: 1", ViewerIdentityCapacityText.StatusLine(read), StringComparison.Ordinal);
    }

    [Fact]
    public void WhenEveryScannedDatabaseIsClean_TheColumnListSaysSo_AndTheScanListStillDraws()
    {
        var read = Read([], ("Clean", Captured));

        Assert.Empty(ViewerIdentityCapacityText.ColumnRows(read));
        Assert.Equal("No identity column is above 10 percent used.", ViewerIdentityCapacityText.ColumnsEmptyText(read));
        Assert.Equal("Scanned, clean", ViewerIdentityCapacityText.ScanRows(read).Single().State);
    }

    [Theory]
    [InlineData(ViewerIdentityCapacityRead.Unavailable, "No identity capacity scan in the 168-hour window.")]
    [InlineData(ViewerIdentityCapacityRead.Precondition, "The identity_capacity collector was refused: SELECT permission was denied.")]
    [InlineData(ViewerIdentityCapacityRead.NotCollected, "This engine does not have this data.")]
    public void ARead_WithoutAScan_ShowsTheReadsOwnSentence_InPlaceOfTheLists(string status, string message)
    {
        var read = new ViewerIdentityCapacityRead(status, message, new IdentityCapacityRead([], []));

        Assert.Equal(message, ViewerIdentityCapacityText.StatusLine(read));
        Assert.Equal(message, ViewerIdentityCapacityText.ColumnsEmptyText(read));
        Assert.Equal(message, ViewerIdentityCapacityText.ScanEmptyText(read));
        Assert.Empty(ViewerIdentityCapacityText.ColumnRows(read));
        Assert.Empty(ViewerIdentityCapacityText.ScanRows(read));
    }
}

/// <summary>
/// #6875 part 3b: the web read (<c>get_identity_capacity</c> through the web dispatch) and the Viewer's data method over one seeded
/// store give the same bands, figures and scan states, with and without a scan.
/// </summary>
[Collection("live-postgres")]
public sealed class IdentityCapacitySurfacesLiveTests
{
    private const string ServerName = "darling-identity-surfaces";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static async Task<T> WithStoreAsync<T>(Func<NpgsqlConnection, CancellationToken, Task> seed, Func<NpgsqlDataSource, string, CancellationToken, Task<T>> body)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live identity capacity surface tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        var bodySucceeded = false;
        try
        {
            await seed(connection, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var result = await body(postgres, scratch.ConnectionString, ct);
            bodySucceeded = true;
            return result;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static Task Marker(NpgsqlConnection c, CancellationToken ct, string database, DateTime at) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO collect.identity_capacity (collection_id, collection_time, server_id, server_name, database_name) VALUES ($1,$2,$3,$4,$5)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, database);

    private static Task Column(NpgsqlConnection c, CancellationToken ct, string database, string table, string last, double percent, DateTime at) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO collect.identity_capacity
(collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name, column_name, data_type, type_precision, last_value, seed_value, increment_value, percent_used)
VALUES ($1,$2,$3,$4,$5,'dbo',$6,'Id','int',10,$7::numeric,1,1,$8)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, database, table, last, percent);

    private static async Task<JsonElement> WebReadAsync(NpgsqlDataSource postgres, params string[] databases)
    {
        var context = new DefaultHttpContext();
        var query = new List<KeyValuePair<string, string?>> { new("server_name", ServerName) };
        query.AddRange(databases.Select(d => new KeyValuePair<string, string?>("database_name", d)));
        context.Request.QueryString = QueryString.Create(query);
        return JsonDocument.Parse(await DarlingWebEndpoints.BuildReadDispatch()["get_identity_capacity"](context, postgres, null!)).RootElement;
    }

    private static DarlingServer Server() =>
        new(ServerId, ServerName, ServerName, true, 16, engineKind: MonitoredEngineKind.SqlServer, engineEdition: CollectorEngineCapability.UnknownEngineEdition);

    [Fact]
    public async Task ASeededStore_GivesTheSameBandsAndFigures_OnTheWebReadAndInTheViewer()
    {
        var now = DateTime.UtcNow;
        var oldAt = now.AddDays(-4);
        var newAt = now.AddHours(-1);
        await WithStoreAsync(async (c, ct) =>
        {
            await Marker(c, ct, "Sales", oldAt);
            await Column(c, ct, "Sales", "Audit", "1990000000", 92.6, oldAt);
            await Marker(c, ct, "Clean", oldAt);
            await Marker(c, ct, "Sales", newAt);
            await Column(c, ct, "Sales", "Audit", "2000000000", 93.1, newAt);
            await Column(c, ct, "Sales", "Warm", "1600000000", 74.5, newAt);
            await Marker(c, ct, "Clean", newAt);
            /* Fresh is online in the newest database config capture and has no marker: the rotation has not reached it. */
            foreach (var database in new[] { "Sales", "Clean", "Fresh" })
            {
                await DarlingMcpTestData.ExecAsync(c, ct,
                    "INSERT INTO database_config (config_id, capture_time, server_id, server_name, database_name, state_desc) VALUES ($1,$2,$3,$4,$5,'ONLINE')",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddHours(-2)), ServerId, ServerName, database);
            }
        }, async (postgres, cs, ct) =>
        {
            var web = await WebReadAsync(postgres);
            await using var viewer = new ViewerDataService(cs);
            var read = await viewer.ReadIdentityCapacityAsync(Server(), now, cancellationToken: ct);

            Assert.Equal(ViewerIdentityCapacityRead.Data, read.Status);
            var webColumns = web.GetProperty("columns").EnumerateArray().ToArray();
            Assert.Equal(["Audit", "Warm"], webColumns.Select(w => w.GetProperty("table").GetString()).ToArray());
            Assert.Equal(["critical", "warning"], webColumns.Select(w => w.GetProperty("band").GetString()).ToArray());
            Assert.Equal("147483647", webColumns[0].GetProperty("values_remaining").GetString());
            Assert.Equal("547483647", webColumns[1].GetProperty("values_remaining").GetString());
            Assert.Equal(1, web.GetProperty("summary").GetProperty("critical").GetInt32());
            Assert.Equal(1, web.GetProperty("summary").GetProperty("warning").GetInt32());

            var rows = ViewerIdentityCapacityText.ColumnRows(read);
            Assert.Equal(["dbo.Audit", "dbo.Warm"], rows.Select(r => r.Table).ToArray());
            Assert.Equal(["critical", "warning"], rows.Select(r => r.Band).ToArray());
            Assert.Equal(["147,483,647", "547,483,647"], rows.Select(r => r.ValuesRemaining).ToArray());
            /* The rate and the days left are the same two numbers, whichever surface draws them. */
            Assert.Equal(webColumns[0].GetProperty("rate_per_day").GetDouble(), read.Read!.Columns[0].Exhaustion!.Value.StepsPerDay, 1);
            Assert.Equal(webColumns[0].GetProperty("days_to_exhaustion").GetDouble(), read.Read.Columns[0].Exhaustion!.Value.DaysLeft, 1);
            Assert.Equal("-", rows[1].RatePerDay);

            var scan = web.GetProperty("databases").EnumerateArray().ToDictionary(d => d.GetProperty("database").GetString()!, d => d.GetProperty("columns_above_10_percent").GetInt32());
            Assert.Equal(new Dictionary<string, int> { ["Clean"] = 0, ["Sales"] = 2, ["Fresh"] = 0 }, scan);
            var fresh = web.GetProperty("databases").EnumerateArray().Single(d => d.GetProperty("database").GetString() == "Fresh");
            Assert.False(fresh.GetProperty("scanned").GetBoolean());
            Assert.Equal(1, web.GetProperty("summary").GetProperty("databases_not_yet_scanned").GetInt32());
            Assert.Equal(
                ["Scanned, clean", "Scanned, 2 columns above 10 percent", "Not yet scanned"],
                ViewerIdentityCapacityText.ScanRows(read).Select(r => r.State).ToArray());
            return 0;
        });
    }

    [Fact]
    public async Task TheWebDatabaseFilter_NarrowsTheColumnsAndTheScanList()
    {
        var now = DateTime.UtcNow;
        await WithStoreAsync(async (c, ct) =>
        {
            await Marker(c, ct, "Sales", now.AddHours(-1));
            await Column(c, ct, "Sales", "Audit", "2000000000", 93.1, now.AddHours(-1));
            await Marker(c, ct, "Clean", now.AddHours(-1));
        }, async (postgres, cs, ct) =>
        {
            var web = await WebReadAsync(postgres, "Clean");

            Assert.Equal("empty", web.GetProperty("status").GetString());
            Assert.Equal(["Clean"], web.GetProperty("databases").EnumerateArray().Select(d => d.GetProperty("database").GetString()).ToArray());
            return 0;
        });
    }

    [Fact]
    public async Task NoScan_OnAPlainServer_IsUnavailable_OnBothSurfaces()
    {
        await WithStoreAsync((_, _) => Task.CompletedTask, async (postgres, cs, ct) =>
        {
            var web = await WebReadAsync(postgres);
            await using var viewer = new ViewerDataService(cs);
            var read = await viewer.ReadIdentityCapacityAsync(Server(), DateTime.UtcNow, cancellationToken: ct);

            Assert.Equal("unavailable", web.GetProperty("status").GetString());
            Assert.Equal(ViewerIdentityCapacityRead.Unavailable, read.Status);
            Assert.StartsWith("No identity capacity scan in the 168-hour window ending ", read.Message, StringComparison.Ordinal);
            Assert.Equal(read.Message, ViewerIdentityCapacityText.StatusLine(read));
            return 0;
        });
    }

    [Fact]
    public async Task NoScan_AfterARefusedRun_IsAPrecondition_WithTheMonitoredServersSentence_OnBothSurfaces()
    {
        const string Sentence = "The SELECT permission was denied on the object 'identity_columns'.";
        await WithStoreAsync(
            (c, ct) => DarlingMcpTestData.ExecAsync(c, ct,
                "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message) VALUES ($1,$2,$3,'identity_capacity',$4,5,'PERMISSIONS',$5)",
                CollectionIdGenerator.Next(), ServerId, ServerName, DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-5)), Sentence),
            async (postgres, cs, ct) =>
            {
                var web = await WebReadAsync(postgres);
                await using var viewer = new ViewerDataService(cs);
                var read = await viewer.ReadIdentityCapacityAsync(Server(), DateTime.UtcNow, cancellationToken: ct);

                Assert.Equal("precondition", web.GetProperty("status").GetString());
                Assert.Equal(ViewerIdentityCapacityRead.Precondition, read.Status);
                Assert.Contains(Sentence, web.GetProperty("message").GetString(), StringComparison.Ordinal);
                Assert.Contains(Sentence, read.Message, StringComparison.Ordinal);
                return 0;
            });
    }
}
