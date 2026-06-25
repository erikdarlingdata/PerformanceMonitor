using System;
using System.Linq;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Guards the shared incident groupers (#1140 / #1141): blocking rows that are one chain collapse to a
/// single incident with the true count + wait range (gotqn's "shown 3x" report), and deadlocks group by
/// involved-object set. Both apps' live builders call these, so this is the canonical grouping coverage.
/// </summary>
public class IncidentGroupingTests
{
    private const string Server = "SQL2022";

    private static BlockingIncidentGrouper.BlockedEvent Blocked(
        string? db, string? obj, string? blocked, string? blocking, long waitMs) =>
        new(db, obj, blocked, blocking, waitMs);

    [Fact]
    public void Blocking_SameChainAcrossSamples_CollapsesToOneIncidentWithCountAndWaitRange()
    {
        // gotqn: one card showed the same chain 3x differing only by wait time (90.8 / 85.8 / 80.8 s).
        var events = new[]
        {
            Blocked("SalesDB", null, "UPDATE SurveyInstances SET x = 1", "SELECT * FROM A CROSS JOIN B", 90800),
            Blocked("SalesDB", null, "UPDATE SurveyInstances SET x = 1", "SELECT * FROM A CROSS JOIN B", 85800),
            Blocked("SalesDB", null, "UPDATE SurveyInstances SET x = 1", "SELECT * FROM A CROSS JOIN B", 80800),
        };

        var groups = BlockingIncidentGrouper.Group(Server, events);

        Assert.Single(groups);
        Assert.Equal(3, groups[0].OccurrenceCount);
        Assert.Equal(80800, groups[0].MinWaitMs);
        Assert.Equal(90800, groups[0].MaxWaitMs);
        Assert.Equal("80.8s-90.8s", groups[0].Incident.WaitRange);
        Assert.Equal(3, groups[0].Incident.OccurrenceCount);
        Assert.False(string.IsNullOrEmpty(groups[0].Incident.DedupKey));
    }

    [Fact]
    public void Blocking_LiteralVaryingQueries_StillGroupTogether()
    {
        var events = new[]
        {
            Blocked("DB", null, "UPDATE T SET c = 1 WHERE id = 5", "SELECT * FROM T WHERE id = 5", 1000),
            Blocked("DB", null, "UPDATE T SET c = 1 WHERE id = 99", "SELECT * FROM T WHERE id = 99", 2000),
        };

        var groups = BlockingIncidentGrouper.Group(Server, events);
        Assert.Single(groups);
        Assert.Equal(2, groups[0].OccurrenceCount);
    }

    [Fact]
    public void Blocking_DistinctChains_ProduceDistinctIncidents()
    {
        var events = new[]
        {
            Blocked("DB", null, "UPDATE A SET c = 1", "SELECT * FROM A", 1000),
            Blocked("DB", null, "UPDATE B SET c = 1", "SELECT * FROM B", 1000),
        };

        var groups = BlockingIncidentGrouper.Group(Server, events);
        Assert.Equal(2, groups.Count);
        Assert.NotEqual(groups[0].Incident.DedupKey, groups[1].Incident.DedupKey);
    }

    [Fact]
    public void Blocking_ContentiousObject_DrivesIdentityAndInvolvedObjects()
    {
        // When the object is resolved, two different query pairs on the SAME object are one incident.
        var events = new[]
        {
            Blocked("DB", "DB.dbo.Orders", "UPDATE Orders SET a = 1", "SELECT * FROM Orders WHERE x = 1", 1000),
            Blocked("DB", "DB.dbo.Orders", "DELETE Orders WHERE z = 2", "SELECT TOP 1 * FROM Orders", 3000),
        };

        var groups = BlockingIncidentGrouper.Group(Server, events);
        Assert.Single(groups);
        Assert.Equal(2, groups[0].OccurrenceCount);
        Assert.Equal(new[] { "DB.dbo.Orders" }, groups[0].Incident.InvolvedObjects);
    }

    [Fact]
    public void Blocking_EmptyInput_ReturnsEmpty()
    {
        Assert.Empty(BlockingIncidentGrouper.Group(Server, Array.Empty<BlockingIncidentGrouper.BlockedEvent>()));
    }

    [Fact]
    public void Deadlock_SameObjectSet_CollapsesAndCounts()
    {
        var events = new[]
        {
            new DeadlockIncidentGrouper.DeadlockEvent(new[] { "SalesDB.dbo.Orders", "SalesDB.dbo.LineItems" }),
            new DeadlockIncidentGrouper.DeadlockEvent(new[] { "SalesDB.dbo.LineItems", "SalesDB.dbo.Orders" }), // order swapped
        };

        var groups = DeadlockIncidentGrouper.Group(Server, events);
        Assert.Single(groups);
        Assert.Equal(2, groups[0].OccurrenceCount);
        Assert.Equal(2, groups[0].Incident.OccurrenceCount);
        Assert.Equal(new[] { "SalesDB.dbo.LineItems", "SalesDB.dbo.Orders" }, groups[0].Incident.InvolvedObjects);
    }

    [Fact]
    public void Deadlock_DistinctObjectSets_ProduceDistinctIncidents()
    {
        var events = new[]
        {
            new DeadlockIncidentGrouper.DeadlockEvent(new[] { "DB.dbo.A" }),
            new DeadlockIncidentGrouper.DeadlockEvent(new[] { "DB.dbo.B" }),
        };

        var groups = DeadlockIncidentGrouper.Group(Server, events);
        Assert.Equal(2, groups.Count);
        Assert.NotEqual(groups[0].Incident.DedupKey, groups[1].Incident.DedupKey);
    }

    [Fact]
    public void Deadlock_NoParseableObjects_AreSkipped()
    {
        var events = new[] { new DeadlockIncidentGrouper.DeadlockEvent(Array.Empty<string>()) };
        Assert.Empty(DeadlockIncidentGrouper.Group(Server, events));
    }

    [Fact]
    public void DeadlockObjectExtractor_PullsObjectNamesFromResourceList()
    {
        const string xml = @"<deadlock>
  <resource-list>
    <keylock dbid='5' objectname='SalesDB.dbo.Orders'><owner-list/><waiter-list/></keylock>
    <keylock dbid='5' objectname='SalesDB.dbo.LineItems'><owner-list/><waiter-list/></keylock>
    <pagelock dbid='5' objectname='SalesDB.dbo.Orders'><owner-list/></pagelock>
  </resource-list>
</deadlock>";

        var objects = DeadlockObjectExtractor.FromGraphXml(xml);
        Assert.Equal(new[] { "SalesDB.dbo.LineItems", "SalesDB.dbo.Orders" }, objects); // distinct + sorted
    }

    [Fact]
    public void DeadlockObjectExtractor_MalformedOrEmpty_ReturnsEmpty()
    {
        Assert.Empty(DeadlockObjectExtractor.FromGraphXml(null));
        Assert.Empty(DeadlockObjectExtractor.FromGraphXml("not xml <<<"));
        Assert.Empty(DeadlockObjectExtractor.FromGraphXml("<deadlock><process-list/></deadlock>"));
    }
}
