/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9697: the service's "Connected" line names a SQL Server target's engine type and its edition separately, and
/// the edition is the word <c>--test-connection</c> prints for the same server.
/// </summary>
public sealed class ConnectedLineFactsTests
{
    [Fact]
    public void BoxEnterprise_NamesTheEngineTypeAndTheEdition_AsTestConnectionDoes()
    {
        var target = new CollectorTargetInfo { Engine = CollectorTargetEngine.SqlServer, SqlMajorVersion = 16 };

        var facts = DarlingServerConnector.DescribeConnectedFacts(target, engineEdition: 3, editionName: "Enterprise Edition: Core-based Licensing (64-bit)");

        Assert.Equal("major 16, engine Box, edition Enterprise", facts);
        Assert.Contains(DarlingServerConnector.DescribeEditionForProbe(3, null), facts, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("Web Edition (64-bit)", 2, "Web")]
    [InlineData("Express Edition (64-bit)", 4, "Express")]
    [InlineData("Standard Edition (64-bit)", 2, "Standard")]
    public void BoxEdition_ComesFromTheEditionName_WhenEngineEditionSharesAValue(string editionName, int engineEdition, string expected)
    {
        var target = new CollectorTargetInfo { Engine = CollectorTargetEngine.SqlServer, SqlMajorVersion = 15 };

        Assert.Equal(
            $"major 15, engine Box, edition {expected}",
            DarlingServerConnector.DescribeConnectedFacts(target, engineEdition, editionName));
    }

    [Fact]
    public void AzureSqlDatabase_NamesItsEngineTypeAndPlatformEdition()
    {
        var target = new CollectorTargetInfo { Engine = CollectorTargetEngine.SqlServer, SqlMajorVersion = 12, IsAzureSqlDb = true };

        Assert.Equal(
            "major 12, engine AzureSqlDb, edition Azure SQL Database",
            DarlingServerConnector.DescribeConnectedFacts(target, engineEdition: 5, editionName: null));
    }

    [Fact]
    public void ManagedInstance_NamesItsEngineTypeAndPlatformEdition()
    {
        var target = new CollectorTargetInfo { Engine = CollectorTargetEngine.SqlServer, SqlMajorVersion = 16, IsAzureManagedInstance = true };

        Assert.Equal(
            "major 16, engine ManagedInstance, edition Azure SQL Managed Instance",
            DarlingServerConnector.DescribeConnectedFacts(target, engineEdition: 8, editionName: null));
    }

    /// <summary>A PostgreSQL target has no SQL Server engine type and no edition, so the line names neither and gives its own major version.</summary>
    [Theory]
    [InlineData(false, "PostgreSQL")]
    [InlineData(true, "Aurora PostgreSQL")]
    public void PostgresTarget_NamesItsEngineAndMajor_AndNoEdition(bool isAurora, string engine)
    {
        var target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17, IsAurora = isAurora };

        var facts = DarlingServerConnector.DescribeConnectedFacts(target, engineEdition: 0, editionName: null);

        Assert.Equal($"major 17, engine {engine}", facts);
        Assert.DoesNotContain("edition", facts, StringComparison.Ordinal);
        Assert.DoesNotContain("Box", facts, StringComparison.Ordinal);
    }
}
