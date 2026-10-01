/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// A logical server's <c>master</c> has no service objective to resize: Server Inventory and the 7-day trend say N/A, and
/// the constant that finds it is the collector's stored edition.
/// </summary>
public sealed class AzureSqlDatabaseMasterPinsTests
{
    private static IDataReader FleetRow(int engineEdition, string edition)
    {
        var table = new DataTable();
        table.Columns.Add("server_id", typeof(int));
        table.Columns.Add("avg_cpu_pct", typeof(decimal));
        table.Columns.Add("total_storage_gb", typeof(decimal));
        table.Columns.Add("idle_db_count", typeof(int));
        table.Columns.Add("max_cpu_pct", typeof(decimal));
        table.Columns.Add("p95_cpu_pct", typeof(decimal));
        table.Columns.Add("max_workers_count", typeof(int));
        table.Columns.Add("current_workers_count", typeof(int));
        table.Columns.Add("max_grant_waiters", typeof(long));
        table.Columns.Add("grant_timeouts", typeof(long));
        table.Columns.Add("forced_grants", typeof(long));
        table.Columns.Add("grant_utilization_pct", typeof(decimal));
        table.Columns.Add("engine_edition", typeof(int));
        table.Columns.Add("edition", typeof(string));
        table.Rows.Add(1, 6m, 20m, 1, 8m, 7m, DBNull.Value, DBNull.Value, 0L, 0L, 0L, 0m, engineEdition, edition);
        var reader = table.CreateDataReader();
        Assert.True(reader.Read());
        return reader;
    }

    [Fact]
    public void FleetProvisioningStatusFor_LogicalServerMaster_GetsNotApplicable()
    {
        using var master = (System.Data.Common.DbDataReader)FleetRow(5, "Azure SQL Database (System)");
        using var userDatabase = (System.Data.Common.DbDataReader)FleetRow(5, "Azure SQL Database (General Purpose)");

        Assert.Equal(ProvisioningVerdict.NotApplicable, LocalDataService.FleetProvisioningStatusFor(master));
        Assert.Equal(ProvisioningVerdict.OverProvisioned, LocalDataService.FleetProvisioningStatusFor(userDatabase));
    }

    [Fact]
    public void ProvisioningTrendRow_ShowsNotApplicableAsTheCardDoes()
    {
        Assert.Equal(ProvisioningVerdict.NotApplicableLabel, new ProvisioningTrendRow { Status = ProvisioningVerdict.NotApplicable }.StatusDisplay);
        Assert.Equal("OVER PROVISIONED", new ProvisioningTrendRow { Status = ProvisioningVerdict.OverProvisioned }.StatusDisplay);
    }

    [Fact]
    public void LogicalServerMasterEdition_MatchesTheCollectorsFormat()
    {
        var sql = Regex.Replace(ReadRepoFile("PerformanceMonitor.Collectors/ServerPropertiesCollector.cs"), @"/\*.*?\*/", "", RegexOptions.Singleline);

        Assert.Matches(new Regex(@"THEN\s+N'Azure SQL Database'\s*\+\s*ISNULL\(N' \('"), sql);
        Assert.Matches(new Regex(@"ELSE\s+CONVERT\(nvarchar\(128\),\s*DATABASEPROPERTYEX\(DB_NAME\(\),\s*N'Edition'\)\)\s+END\s*\+\s*N'\)'"), sql);
        Assert.DoesNotContain("WHEN N'System'", sql, StringComparison.Ordinal);
        Assert.Equal("Azure SQL Database" + " (" + "System" + ")", ServerHardwareScope.AzureSqlDatabaseSystemEdition);
    }

    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile);
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
