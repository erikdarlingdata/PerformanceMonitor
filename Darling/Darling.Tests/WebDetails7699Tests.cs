/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// #7699: the small details behind the web pages that the services and desktop grids share. The result words of the long query
/// completions, the Host OS cut, the persistent version store's empty sentence and the Latches wait category.
/// </summary>
public sealed class WebDetails7699Tests
{
    /// <summary>Item 1: the XE result map is 0 OK, 1 Error, 2 Abort, 3 Skipped. Rows stored before the collector read the map's text hold the number.</summary>
    [Theory]
    [InlineData("0", "OK")]
    [InlineData("1", "Error")]
    [InlineData("2", "Abort")]
    [InlineData("3", "Skipped")]
    [InlineData(" 2 ", "Abort")]
    [InlineData("Abort", "Abort")]
    [InlineData("OK", "OK")]
    [InlineData("7", "7")]
    public void LongQueryResult_PrintsTheMapWord_ForANumberStoredBeforeTheCollectorReadTheText(string stored, string expected)
    {
        Assert.Equal(expected, LongQueryResultText.Word(stored));
        Assert.Null(LongQueryResultText.Word(null));
    }

    /// <summary>Item 3: a SQL Server 2016 row keeps the end of @@VERSION. It is cut to the product name; every other value comes back as stored.</summary>
    [Theory]
    [InlineData("Windows Server 2016 Datacenter 10.0 <X64> (Build 14393: ) (Hypervisor)", "Windows Server 2016 Datacenter")]
    [InlineData("Windows Server 2012 R2 Standard 6.3 <X64> (Build 9600: )", "Windows Server 2012 R2 Standard")]
    [InlineData("Windows NT 6.3 <X64> (Build 9600: )", "Windows NT 6.3")]
    [InlineData("Windows Server 2022 Datacenter", "Windows Server 2022 Datacenter")]
    [InlineData("Ubuntu 20.04.6 LTS", "Ubuntu 20.04.6 LTS")]
    [InlineData("  Windows Server 2022\r\n", "Windows Server 2022")]
    [InlineData(null, "")]
    public void HostOsText_CutsTheVersionTail_AndKeepsEveryOtherName(string? stored, string expected) =>
        Assert.Equal(expected, HostOsText.Clean(stored));

    /// <summary>Item 7: before SQL Server 2019 the version store does not exist; otherwise the collector just has not completed a cycle.</summary>
    [Theory]
    [InlineData(13, true)]
    [InlineData(14, true)]
    [InlineData(15, false)]
    [InlineData(16, false)]
    [InlineData(0, false)]
    public void PvsEmptySentence_NamesTheVersion_OnlyWhenTheVersionHasNoVersionStore(int major, bool noVersionStore)
    {
        var message = PvsStatsCollector.NoRowsMessage(new CollectorTargetInfo { SqlMajorVersion = major });

        if (noVersionStore)
        {
            Assert.Contains("has no persistent version store", message, StringComparison.Ordinal);
            Assert.DoesNotContain("sys.dm_tran_persistent_version_store_stats", message, StringComparison.Ordinal);
        }
        else
        {
            Assert.StartsWith("No PVS data collected for this server", message, StringComparison.Ordinal);
            Assert.DoesNotContain("persistent version store", message, StringComparison.Ordinal);
        }
    }

    /// <summary>Item 7: Azure SQL Database reports a low major version yet has the version store, so its empty answer is the second sentence.</summary>
    [Fact]
    public void PvsEmptySentence_AnAzureSqlDatabaseIsNeverTakenForAnOldVersion()
    {
        var message = PvsStatsCollector.NoRowsMessage(new CollectorTargetInfo { IsAzureSqlDb = true, SqlMajorVersion = 12 });

        Assert.StartsWith("No PVS data collected for this server", message, StringComparison.Ordinal);
    }

    /// <summary>Item 10: LATCH_ and PAGELATCH_ waits are the Latches category and no longer part of Other; page I/O latches stay Storage.</summary>
    [Fact]
    public async Task WaitCategories_ListLatchesApart_AgainstLivePostgres()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live wait category test.");

        var ct = TestContext.Current.CancellationToken;
        const string serverName = "web-details-7699-latches";
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);

        var at = DateTime.SpecifyKind(DateTime.UtcNow.AddHours(-2), DateTimeKind.Unspecified);
        foreach (var (type, ms) in new[] { ("LATCH_EX", 400L), ("PAGELATCH_UP", 300L), ("PAGEIOLATCH_SH", 200L), ("BROKER_TASK_STOP", 100L) })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_wait_time_ms, delta_waiting_tasks)
                  VALUES ($1,$2,$3,$4,$5,$6,1)",
                CollectionIdGenerator.Next(), at, serverId, serverName, type, ms);
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rows = await DarlingFinOpsOptimizationReader.GetWaitCategorySummaryAsync(dataSource, serverId, DateTime.UtcNow.AddHours(-24), 30, ct);

        Assert.NotEmpty(rows);
        var byCategory = rows.ToDictionary(r => r.Category, StringComparer.Ordinal);
        Assert.Equal(700L, byCategory["Latches"].TotalWaitTimeMs);
        Assert.Equal("LATCH_EX", byCategory["Latches"].TopWaitType);
        Assert.Equal(200L, byCategory["Storage"].TotalWaitTimeMs);
        Assert.Equal(100L, byCategory["Other"].TotalWaitTimeMs);
        Assert.Equal("BROKER_TASK_STOP", byCategory["Other"].TopWaitType);
    }
}
