// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6889, measured live. (1) On Amazon RDS every login can enter <c>rdsadmin</c>, so a list that did not screen it collected
/// Amazon's own database each cycle. (2) A Managed Instance reports ProductVersion 12.0.x with EngineEdition 8, so a SQL-side
/// version check that never looks at the edition skips it: plan corrections were never listed there, and the page-to-object
/// lookup for blocked processes was skipped. The SQL cannot run here, so these read the composed text of each query.
/// </summary>
public sealed class VendorDatabaseScreenAndManagedInstanceGateTests
{
    private static readonly CollectorDeltaCalculator s_deltas = new();

    private static CollectorContext NewContext() =>
        new()
        {
            ServerId = 1,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 10, 28, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo(),
        };

    private const string VendorScreen = "d.name NOT IN (N'rdsadmin', N'gcloud_cloudsqladmin')";

    [Fact]
    public void DatabaseScopedConfigList_LeavesOutTheVendorControlledDatabases_AndKeepsTempdb()
    {
        var text = DatabaseScopedConfigCollector.Instance.BuildEnumerationQuery(NewContext())!.Text;

        Assert.Contains(VendorScreen, text, StringComparison.Ordinal);
        Assert.Contains("d.database_id = 2", text, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStoreHealthList_LeavesOutTheVendorControlledDatabases_AndKeepsTempdb()
    {
        var text = QueryStoreHealthCollector.Instance.BuildEnumerationQuery(NewContext())!.Text;

        Assert.Contains(VendorScreen, text, StringComparison.Ordinal);
        Assert.Contains("d.database_id = 2", text, StringComparison.Ordinal);
    }

    [Fact]
    public void PlanCorrectionList_LeavesOutTheVendorControlledDatabases_AndKeepsTempdb()
    {
        var text = PlanCorrectionCollector.Instance.BuildEnumerationQuery(NewContext())!.Text;

        Assert.Contains(VendorScreen, text, StringComparison.Ordinal);
        Assert.Contains("d.database_id = 2", text, StringComparison.Ordinal);
    }

    [Fact]
    public void IndexObjectStatsList_LeavesOutTheVendorControlledDatabases_AndKeepsMasterAndMsdb()
    {
        var text = IndexObjectStatsCollector.Instance.BuildEnumerationQuery(NewContext())!.Text;

        Assert.Contains(VendorScreen, text, StringComparison.Ordinal);
        /* Master and msdb stay in this list on purpose: no name screen names them. */
        Assert.DoesNotContain("N'master'", text, StringComparison.Ordinal);
        Assert.DoesNotContain("N'msdb'", text, StringComparison.Ordinal);
    }

    [Fact]
    public void TheQueryStoreList_SharesTheSameVendorNames()
    {
        var text = QueryStoreCollector.Instance.BuildEnumerationQuery(NewContext())!.Text;

        Assert.Contains(DatabaseScopeFilter.VendorControlledDatabaseNames, text, StringComparison.Ordinal);
    }

    [Fact]
    public void PlanCorrectionList_ListsAManagedInstance_ByItsEngineEdition()
    {
        var text = PlanCorrectionCollector.Instance.BuildEnumerationQuery(NewContext())!.Text;

        /* The version predicate and the edition test sit in one group, so 12.0.x passes on edition 8 and a pre-2017 box still lists nothing. */
        Assert.Matches(
            new Regex(@"WHERE\s*\(\s*@product_major >= 14[^\r\n]*\r?\n\s*OR CONVERT\(integer, SERVERPROPERTY\('EngineEdition'\)\) = 8[^\r\n]*\r?\n\)", RegexOptions.CultureInvariant),
            text);
    }

    [Fact]
    public void BlockedProcessPageLookup_RunsOnAManagedInstance_WithoutWideningAzure()
    {
        var text = BlockedProcessReportCollector.Instance.BuildQuery(NewContext()).Text;

        Assert.Matches(
            new Regex(@"IF @product_version >= 15\s+OR @azure = 1\s+OR CONVERT\(integer, SERVERPROPERTY\('EngineEdition'\)\) = 8\s+BEGIN", RegexOptions.CultureInvariant),
            text);
        /* @azure is still Azure SQL Database only: the rest of the batch (the XE source, the database screens) reads it. */
        Assert.Matches(new Regex(@"@azure bit =\s+CASE\s+WHEN CONVERT\(integer, SERVERPROPERTY\('EngineEdition'\)\) = 5\s+THEN 1", RegexOptions.CultureInvariant), text);
    }
}
