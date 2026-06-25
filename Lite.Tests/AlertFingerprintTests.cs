using System;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Guards <see cref="AlertFingerprint"/> (#1140): the stable dedup key must be deterministic,
/// order- and case-insensitive over the involved objects, scoped by server + incident type, and
/// must NOT move when volatile per-sample fields (occurrence count, wait range) change. The shared
/// helper covers both apps, so this one suite is the canonical fingerprint coverage.
/// </summary>
public class AlertFingerprintTests
{
    private const string Server = "SQL2022";

    [Fact]
    public void ForObjects_IsDeterministic()
    {
        var a = AlertFingerprint.ForObjects(Server, AlertFingerprint.Deadlock, new[] { "SalesDB.dbo.Orders" });
        var b = AlertFingerprint.ForObjects(Server, AlertFingerprint.Deadlock, new[] { "SalesDB.dbo.Orders" });
        Assert.NotNull(a);
        Assert.NotNull(b);
        Assert.Equal(a!.DedupKey, b!.DedupKey);
    }

    [Fact]
    public void ForObjects_IsOrderIndependent()
    {
        var a = AlertFingerprint.ForObjects(Server, AlertFingerprint.Deadlock, new[] { "db.dbo.A", "db.dbo.B" });
        var b = AlertFingerprint.ForObjects(Server, AlertFingerprint.Deadlock, new[] { "db.dbo.B", "db.dbo.A" });
        Assert.Equal(a!.DedupKey, b!.DedupKey);
    }

    [Fact]
    public void ForObjects_CollapsesDuplicatesCasingAndWhitespace()
    {
        var a = AlertFingerprint.ForObjects(Server, AlertFingerprint.Deadlock, new[] { "db.dbo.A" });
        var b = AlertFingerprint.ForObjects(Server, AlertFingerprint.Deadlock, new[] { "  DB.DBO.A  ", "db.dbo.a", "db.dbo.A" });
        Assert.Equal(a!.DedupKey, b!.DedupKey);
    }

    [Fact]
    public void ForObjects_PreservesOriginalCasingForDisplay()
    {
        var a = AlertFingerprint.ForObjects(Server, AlertFingerprint.Deadlock, new[] { "SalesDB.dbo.Orders" });
        Assert.Equal(new[] { "SalesDB.dbo.Orders" }, a!.InvolvedObjects);
    }

    [Fact]
    public void ForObjects_DistinctIncidentType_DiffersKey()
    {
        var deadlock = AlertFingerprint.ForObjects(Server, AlertFingerprint.Deadlock, new[] { "db.dbo.A" });
        var blocking = AlertFingerprint.ForObjects(Server, AlertFingerprint.Blocking, new[] { "db.dbo.A" });
        Assert.NotEqual(deadlock!.DedupKey, blocking!.DedupKey);
    }

    [Fact]
    public void ForObjects_DistinctServer_DiffersKey()
    {
        var a = AlertFingerprint.ForObjects("SQL2019", AlertFingerprint.Deadlock, new[] { "db.dbo.A" });
        var b = AlertFingerprint.ForObjects("SQL2022", AlertFingerprint.Deadlock, new[] { "db.dbo.A" });
        Assert.NotEqual(a!.DedupKey, b!.DedupKey);
    }

    [Fact]
    public void ForObjects_VolatileFields_DoNotAffectKey()
    {
        var a = AlertFingerprint.ForObjects(Server, AlertFingerprint.Blocking, new[] { "db.dbo.A" }, occurrenceCount: 3, waitRange: "80.8-90.8 s");
        var b = AlertFingerprint.ForObjects(Server, AlertFingerprint.Blocking, new[] { "db.dbo.A" }, occurrenceCount: 8, waitRange: "10-20 s");
        Assert.Equal(a!.DedupKey, b!.DedupKey);
        Assert.Equal(3, a.OccurrenceCount);
        Assert.Equal("80.8-90.8 s", a.WaitRange);
    }

    [Fact]
    public void ForObjects_NoUsableObjects_ReturnsNull()
    {
        Assert.Null(AlertFingerprint.ForObjects(Server, AlertFingerprint.Deadlock, new[] { "", "   " }));
        Assert.Null(AlertFingerprint.ForObjects(Server, AlertFingerprint.Deadlock, Array.Empty<string>()));
    }

    [Fact]
    public void ForKey_IsDeterministicAndScoped()
    {
        var a = AlertFingerprint.ForKey(Server, AlertFingerprint.Query, "0xABCD");
        var b = AlertFingerprint.ForKey(Server, AlertFingerprint.Query, "0xabcd"); // case-normalized to the same key
        Assert.Equal(a!.DedupKey, b!.DedupKey);

        var differentType = AlertFingerprint.ForKey(Server, AlertFingerprint.Job, "0xABCD");
        Assert.NotEqual(a.DedupKey, differentType!.DedupKey);
    }

    [Fact]
    public void ForKey_BlankKey_ReturnsNull()
    {
        Assert.Null(AlertFingerprint.ForKey(Server, AlertFingerprint.Disk, "   "));
    }

    [Fact]
    public void ForKey_DisplayObjects_DoNotAffectKey()
    {
        var a = AlertFingerprint.ForKey(Server, AlertFingerprint.Job, "Nightly ETL", new[] { "shown A" });
        var b = AlertFingerprint.ForKey(Server, AlertFingerprint.Job, "Nightly ETL", new[] { "shown B" });
        Assert.Equal(a!.DedupKey, b!.DedupKey);
        Assert.Equal(new[] { "shown A" }, a.InvolvedObjects);
    }

    [Fact]
    public void Hash_IsLowercaseHex64()
    {
        var h = AlertFingerprint.Hash("anything");
        Assert.Equal(64, h.Length);
        Assert.Matches("^[0-9a-f]{64}$", h);
    }
}
