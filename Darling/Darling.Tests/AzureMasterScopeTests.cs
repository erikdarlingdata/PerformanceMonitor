/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

public sealed class AzureMasterScopeTests
{
    private const string Host = "srv.database.windows.net";

    private static AlertTargetIdentity T(string id, string? db, string host = Host, bool enabled = true, bool ro = false) =>
        new(id, host, db, enabled, ro);

    private static IReadOnlyList<string> Run(bool azure, string? db, params AlertTargetIdentity[] targets) =>
        AzureMasterScope.SeparatelyMonitoredDatabases(azure, "self", Host, db, targets);

    [Fact]
    public void NotAzure_IsEmpty() =>
        Assert.Empty(Run(false, "master", T("a", "GP")));

    [Fact]
    public void UserDatabaseTarget_IsEmpty() =>
        Assert.Empty(Run(true, "GP", T("a", "HS")));

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("Master")]
    public void MasterTarget_ReturnsTheOtherTargetsDatabases(string? db) =>
        Assert.Equal(["GP", "HS"], Run(true, db, T("a", "GP"), T("b", "HS")));

    [Fact]
    public void SkipsSelfDisabledReadOnlyOtherHostsAndMasterTargets() =>
        Assert.Equal(["GP"], Run(true, "master",
            T("self", "Own"), T("a", "GP"), T("b", "Off", enabled: false), T("c", "Ro", ro: true),
            T("d", "Elsewhere", host: "other.database.windows.net"), T("e", "master"), T("f", ""), T("g", null)));

    [Fact]
    public void HostMatchIsTrimmedAndCaseInsensitive() =>
        Assert.Equal(["GP"], Run(true, "master", T("a", "GP", host: "  SRV.Database.Windows.NET ")));

    [Fact]
    public void ReturnsEachDatabaseOnce() =>
        Assert.Equal(["GP"], Run(true, "master", T("a", "GP"), T("b", "gp")));

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("master")]
    [InlineData("MASTER")]
    [InlineData("GP")]
    public void MasterRuleAgreesWithTheCollectorsRule(string? db)
    {
        bool collectorsSaysMaster = AzureSweepScope.OwnDatabaseOrEmpty(db).Count == 0;
        bool scopeSaysMaster = Run(true, db, T("a", "GP")).Any();
        Assert.Equal(collectorsSaysMaster, scopeSaysMaster);
    }
}
