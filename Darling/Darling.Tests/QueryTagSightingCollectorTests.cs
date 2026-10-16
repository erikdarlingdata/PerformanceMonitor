/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common.QueryTags;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5980: each collector describes the query keys and caller fields a row saw. Every test plants a row and calls the
/// collector's own hook, the same call the store builders will make; nothing here touches a store.
/// </summary>
public sealed class QueryTagSightingCollectorTests
{
    [Fact]
    public void ActiveQueryRow_YieldsOneHashSighting_WithApplicationLoginHostAndDatabase()
    {
        var row = new QuerySnapshotsCollector.Row
        {
            QueryHash = "0xAABB",
            DatabaseName = "Sales",
            ProgramName = "Report Server",
            LoginName = "svc_reports",
            HostName = "RPT01",
            QueryText = "SELECT 1;",
        };

        var sighting = Assert.Single(QuerySnapshotsCollector.Instance.DescribeQueryTagSightings(row)!);

        Assert.Equal(QueryTagKeyKind.Hash, sighting.KeyKind);
        Assert.Equal("0xAABB", sighting.Key);
        Assert.Equal("Sales", sighting.Database);
        Assert.Equal("Report Server", sighting.Application);
        Assert.Equal("svc_reports", sighting.Login);
        Assert.Equal("RPT01", sighting.Host);
        Assert.Equal("SELECT 1;", sighting.TextSample);
    }

    [Fact]
    public void LongQueryCompletion_YieldsOneHashSighting_CarryingItsObjectName()
    {
        var row = new LongQueryCompletionsCollector.Row
        {
            QueryHash = "0xCC01",
            DatabaseName = "Sales",
            ClientAppName = "OrderApp",
            ServerPrincipalName = "CORP\\jane",
            ObjectName = "usp_PlaceOrder",
            StatementText = "EXEC usp_PlaceOrder;",
        };

        var sighting = Assert.Single(LongQueryCompletionsCollector.Instance.DescribeQueryTagSightings(row)!);

        Assert.Equal(QueryTagKeyKind.Hash, sighting.KeyKind);
        Assert.Equal("0xCC01", sighting.Key);
        Assert.Equal("OrderApp", sighting.Application);
        Assert.Equal("CORP\\jane", sighting.Login);
        Assert.NotNull(sighting.ObjectRef);
        Assert.Equal("usp_PlaceOrder", sighting.ObjectRef!.Name);
        Assert.Equal("Sales", sighting.ObjectRef.Database);
    }

    [Fact]
    public void QueryStoreRow_YieldsAHashSighting_CarryingModuleNameAsTheObject()
    {
        var row = new QueryStoreCollector.Row
        {
            QueryHash = "0xDD02",
            DatabaseName = "Sales",
            ModuleName = "dbo.usp_PlaceOrder",
            QueryText = "SELECT 2;",
        };

        var sighting = Assert.Single(QueryStoreCollector.Instance.DescribeQueryTagSightings(row)!);

        Assert.Equal(QueryTagKeyKind.Hash, sighting.KeyKind);
        Assert.Equal("0xDD02", sighting.Key);
        Assert.Equal("Sales.dbo.usp_PlaceOrder", sighting.ObjectRef!.Qualified);

        /* Query Store's own placeholders for "no module" are not objects. */
        row.ModuleName = "Adhoc";
        Assert.Null(Assert.Single(QueryStoreCollector.Instance.DescribeQueryTagSightings(row)!).ObjectRef);
    }

    [Fact]
    public void ProcedureStatsRow_YieldsOneObjectSighting_AndNoHashSighting()
    {
        var row = default(ProcedureStatsCollector.Row) with
        {
            DatabaseName = "Sales",
            SchemaName = "dbo",
            ObjectName = "usp_PlaceOrder",
        };

        var sighting = Assert.Single(ProcedureStatsCollector.Instance.DescribeQueryTagSightings(row)!);

        Assert.Equal(QueryTagKeyKind.Object, sighting.KeyKind);
        Assert.Equal("Sales.dbo.usp_PlaceOrder", sighting.Key);
        Assert.Equal("Sales", sighting.Database);
        Assert.True(sighting.ObjectRef!.SameObjectAs(new QueryTagObject("Sales", "dbo", "usp_PlaceOrder")));
    }

    [Fact]
    public void BlockedProcessReport_YieldsABlockedAndABlockingCallerSighting_WithTheRightFieldsOnEach()
    {
        var row = new BlockedProcessReportCollector.Row
        {
            DatabaseName = "Sales",
            BlockedClientApp = "OrderApp",
            BlockedLoginName = "orders_rw",
            BlockedHostName = "WEB01",
            BlockedSqlText = "UPDATE a;",
            BlockingClientApp = "NightlyBatch",
            BlockingLoginName = "batch_svc",
            BlockingHostName = "ETL02",
            BlockingSqlText = "UPDATE b;",
        };

        var sightings = BlockedProcessReportCollector.Instance.DescribeQueryTagSightings(row)!;

        Assert.Equal(2, sightings.Count);
        Assert.All(sightings, s => Assert.Equal(QueryTagKeyKind.Caller, s.KeyKind));
        var blocked = sightings[0];
        var blocking = sightings[1];
        Assert.Equal(("OrderApp", "orders_rw", "WEB01", "Sales", "UPDATE a;"),
            (blocked.Application, blocked.Login, blocked.Host, blocked.Database, blocked.TextSample));
        Assert.Equal(("NightlyBatch", "batch_svc", "ETL02", "Sales", "UPDATE b;"),
            (blocking.Application, blocking.Login, blocking.Host, blocking.Database, blocking.TextSample));
        Assert.NotEqual(blocked.Key, blocking.Key);
    }

    [Fact]
    public void DmvBlockingSnapshot_YieldsABlockedAndABlockingCallerSighting()
    {
        var row = default(DmvBlockingSnapshotCollector.Row) with
        {
            DatabaseName = "Sales",
            BlockedClientApp = "OrderApp",
            BlockedLoginName = "orders_rw",
            BlockedHostName = "WEB01",
            BlockingClientApp = "NightlyBatch",
            BlockingLoginName = "batch_svc",
            BlockingHostName = "ETL02",
        };

        var sightings = DmvBlockingSnapshotCollector.Instance.DescribeQueryTagSightings(row)!;

        Assert.Equal(2, sightings.Count);
        Assert.Equal("OrderApp", sightings[0].Application);
        Assert.Equal("NightlyBatch", sightings[1].Application);
        Assert.Equal("ETL02", sightings[1].Host);
    }

    [Fact]
    public void DeadlockGraphWithThreeProcesses_YieldsThreeCallerSightings()
    {
        string Process(string id, string app, string login, string host, string db) =>
            $"<process id=\"{id}\" spid=\"5{id.Length}\" clientapp=\"{app}\" loginname=\"{login}\" hostname=\"{host}\" currentdbname=\"{db}\"><inputbuf>SELECT {id};</inputbuf></process>";

        var row = new DeadlocksCollector.Row
        {
            GraphXml = "<deadlock><victim-list><victimProcess id=\"p1\"/></victim-list><process-list>"
                + Process("p1", "AppOne", "login1", "HOST1", "DbOne")
                + Process("p2", "AppTwo", "login2", "HOST2", "DbTwo")
                + Process("p3", "AppThree", "login3", "HOST3", "DbThree")
                + "</process-list><resource-list/></deadlock>",
        };

        var sightings = DeadlocksCollector.Instance.DescribeQueryTagSightings(row)!;

        Assert.Equal(3, sightings.Count);
        Assert.All(sightings, s => Assert.Equal(QueryTagKeyKind.Caller, s.KeyKind));
        Assert.Equal(new[] { "AppOne", "AppTwo", "AppThree" }, sightings.Select(s => s.Application));
        Assert.Equal(new[] { "login1", "login2", "login3" }, sightings.Select(s => s.Login));
        Assert.Equal(new[] { "HOST1", "HOST2", "HOST3" }, sightings.Select(s => s.Host));
        Assert.Equal(new[] { "DbOne", "DbTwo", "DbThree" }, sightings.Select(s => s.Database));
    }

    [Fact]
    public void TextOverTheCap_IsCutToTheCap()
    {
        var row = new QueryStatsCollector.Row
        {
            QueryHash = "0xEE03",
            QueryText = new string('x', QueryTagLimits.TextSampleCap + 500),
        };

        var sighting = Assert.Single(QueryStatsCollector.Instance.DescribeQueryTagSightings(row)!);

        Assert.Equal(4000, QueryTagLimits.TextSampleCap);
        Assert.Equal(QueryTagLimits.TextSampleCap, sighting.TextSample!.Length);
    }

    [Fact]
    public void TextCutThroughASurrogatePair_StopsBeforeTheHalfPair()
    {
        /* The cap lands between the two halves of an emoji: keeping the high half alone is not valid text. */
        string text = new string('x', QueryTagLimits.TextSampleCap - 1) + "\U0001F600" + "tail";
        var row = new QueryStatsCollector.Row { QueryHash = "0xEE04", QueryText = text };

        var sighting = Assert.Single(QueryStatsCollector.Instance.DescribeQueryTagSightings(row)!);

        Assert.Equal(QueryTagLimits.TextSampleCap - 1, sighting.TextSample!.Length);
        Assert.False(char.IsHighSurrogate(sighting.TextSample[^1]));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void RowWithANullOrEmptyQueryHash_YieldsNoHashSighting(string? hash)
    {
        Assert.Null(QuerySnapshotsCollector.Instance.DescribeQueryTagSightings(
            new QuerySnapshotsCollector.Row { QueryHash = hash, ProgramName = "App", QueryText = "SELECT 1;" }));
        Assert.Null(QueryStatsCollector.Instance.DescribeQueryTagSightings(
            new QueryStatsCollector.Row { QueryHash = hash ?? "", QueryText = "SELECT 1;" }));
        Assert.Null(QueryStoreCollector.Instance.DescribeQueryTagSightings(
            new QueryStoreCollector.Row { QueryHash = hash, ModuleName = "dbo.p" }));
        Assert.Null(LongQueryCompletionsCollector.Instance.DescribeQueryTagSightings(
            new LongQueryCompletionsCollector.Row { QueryHash = hash, ObjectName = "p" }));
    }

    [Fact]
    public void ACollectorWithoutAnImplementation_AnswersNull()
    {
        Assert.Null(WaitStatsCollector.Instance.DescribeQueryTagSightings(
            new WaitStatsCollector.Row("CXPACKET", 1, 2, 3)));
    }
}
