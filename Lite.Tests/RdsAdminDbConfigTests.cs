/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9587: <c>rdsadmin</c> is the platform's own database on Amazon RDS for SQL Server, which the customer cannot change.
/// The <c>DB_CONFIG</c> fact and its drill-down leave it out beside master, msdb, model and tempdb, so an RDS target whose
/// user databases are all fine raises no <c>DB_CONFIG</c> finding and no RCSI remediation names it. The twin of Darling's
/// <c>RdsAdminDbConfigLiveTests</c>.
/// </summary>
public sealed class RdsAdminDbConfigTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = -958_701;

    private readonly DuckDbInitializer _duckDb;

    public RdsAdminDbConfigTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private async Task SeedAsync(DateTime end, params (string Database, bool RcsiOn)[] databases)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var c = _duckDb.CreateConnection();
        await c.OpenAsync(TestContext.Current.CancellationToken);
        foreach (var (database, rcsiOn) in databases)
            await LatestValueSeed.InsertDatabaseConfigAsync(c, ServerId, end.AddHours(-1), database, autoShrink: false, rcsiOn: rcsiOn);
    }

    private static DateTime Now()
    {
        var end = DateTime.UtcNow;
        return new DateTime(end.Ticks - end.Ticks % TimeSpan.TicksPerSecond, DateTimeKind.Unspecified);
    }

    private async Task<Fact?> DbConfigFactAsync(DateTime end) =>
        (await new DuckDbFactCollector(_duckDb).CollectFactsAsync(LatestValueSeed.Context(ServerId, end)))
            .SingleOrDefault(f => f.Key == "DB_CONFIG");

    [Fact]
    public async Task OnlyRdsadminWithRcsiOff_CountsNoRcsiOffDatabase_AndTheFactScoresNothing()
    {
        var end = Now();
        await SeedAsync(end, ("Sales", true), ("rdsadmin", false));

        var fact = Assert.IsType<Fact>(await DbConfigFactAsync(end));
        Assert.Equal(0, fact.Metadata["rcsi_off_count"]);
        Assert.Equal(1, fact.Metadata["database_count"]);

        new FactScorer().ScoreAll([fact]);
        Assert.Equal(0.0, fact.BaseSeverity);
    }

    [Fact]
    public async Task RdsadminAlone_RaisesNoDbConfigFact()
    {
        var end = Now();
        await SeedAsync(end, ("rdsadmin", false));

        Assert.Null(await DbConfigFactAsync(end));
    }

    [Fact]
    public async Task AUserDatabaseWithRcsiOff_StillCounts_BesideRdsadmin()
    {
        var end = Now();
        await SeedAsync(end, ("Sales", false), ("Orders", true), ("rdsadmin", false));

        var fact = Assert.IsType<Fact>(await DbConfigFactAsync(end));
        Assert.Equal(1, fact.Metadata["rcsi_off_count"]);
        Assert.Equal(2, fact.Metadata["database_count"]);

        new FactScorer().ScoreAll([fact]);
        Assert.True(fact.BaseSeverity > 0, "a user database with RCSI off must still score");
    }

    [Fact]
    public async Task TheDrillDown_ListsNoRdsadminRow_AndNoRemediationTargetsIt()
    {
        var end = Now();
        await SeedAsync(end, ("Sales", false), ("Orders", true), ("rdsadmin", false));

        var finding = new AnalysisFinding
        {
            ServerId = ServerId, RootFactKey = "DB_CONFIG", StoryPath = "DB_CONFIG", PathKeys = ["DB_CONFIG"], Severity = 0.3,
        };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], LatestValueSeed.Context(ServerId, end));

        var names = JsonSerializer.SerializeToElement(finding.DrillDown!["config_issues"]).EnumerateArray()
            .Select(r => r.GetProperty("database").GetString()).ToArray();
        Assert.Equal(["Sales"], names);
        Assert.DoesNotContain(FactRemediation.CollectRcsiTargets(finding), t => t.Database == "rdsadmin");
    }

    [Fact]
    public async Task OnlyRdsadminWithRcsiOff_GivesNoDrillDownRows()
    {
        var end = Now();
        await SeedAsync(end, ("Sales", true), ("rdsadmin", false));

        var finding = new AnalysisFinding
        {
            ServerId = ServerId, RootFactKey = "DB_CONFIG", StoryPath = "DB_CONFIG", PathKeys = ["DB_CONFIG"], Severity = 0.3,
        };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], LatestValueSeed.Context(ServerId, end));

        Assert.True(finding.DrillDown is null || !finding.DrillDown.ContainsKey("config_issues"));
    }
}
