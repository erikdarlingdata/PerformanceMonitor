// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6889, measured live. On Amazon RDS every login can enter <c>rdsadmin</c>, so a list that did not screen it collected
/// Amazon's own database each cycle. These four read the composed text of each per-database list and check it carries the
/// shared vendor-name screen (the SQL cannot run here). They check only that the screen is present, not how the rest of the
/// query is worded.
///
/// <para>The Managed Instance gates (plan corrections listed on EngineEdition 8, the blocked process page lookup run there) have
/// no test here on purpose: a test of the gate's SQL wording fails on a reorder and passes on a dead block (#5833). They were
/// measured live on a Managed Instance in Round 4 of the pull request: <c>FORCE_LAST_GOOD_PLAN</c> was ON, the old list returned
/// 0 rows, the new list named the user databases, and <c>sys.dm_db_page_info</c> answered.</para>
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
    public void DatabaseScopedConfigList_LeavesOutTheVendorControlledDatabases()
    {
        var text = DatabaseScopedConfigCollector.Instance.BuildEnumerationQuery(NewContext())!.Text;

        Assert.Contains(VendorScreen, text, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStoreHealthList_LeavesOutTheVendorControlledDatabases()
    {
        var text = QueryStoreHealthCollector.Instance.BuildEnumerationQuery(NewContext())!.Text;

        Assert.Contains(VendorScreen, text, StringComparison.Ordinal);
    }

    [Fact]
    public void PlanCorrectionList_LeavesOutTheVendorControlledDatabases()
    {
        var text = PlanCorrectionCollector.Instance.BuildEnumerationQuery(NewContext())!.Text;

        Assert.Contains(VendorScreen, text, StringComparison.Ordinal);
    }

    [Fact]
    public void IndexObjectStatsList_LeavesOutTheVendorControlledDatabases()
    {
        var text = IndexObjectStatsCollector.Instance.BuildEnumerationQuery(NewContext())!.Text;

        Assert.Contains(VendorScreen, text, StringComparison.Ordinal);
    }
}
