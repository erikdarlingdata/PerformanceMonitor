using System.Collections.Generic;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #1154: the per-fingerprint cooldown seed matches rows by the serialized #1140 dedup key.
/// These tests pin the match against the REAL serializer output (so a future naming-policy
/// change breaks here, not silently in production), and the null/blank guards that keep the
/// Dashboard scan from NRE-ing on its many null-ContextJson rows.
/// </summary>
public class ContextJsonContainsDedupKeyTests
{
    private static string SerializeWith(params string[] dedupKeys)
    {
        var ctx = new AlertContext
        {
            Incidents = new List<AlertIncident>()
        };
        foreach (var k in dedupKeys)
            ctx.Incidents.Add(new AlertIncident(k, new[] { "db.dbo.T" }));
        return AlertContextSerializer.Serialize(ctx);
    }

    [Fact]
    public void Finds_EachDedupKey_InRealSerializedContext()
    {
        var json = SerializeWith("aaaa1111", "bbbb2222");

        Assert.True(AlertContextSerializer.ContextJsonContainsDedupKey(json, "aaaa1111"));
        Assert.True(AlertContextSerializer.ContextJsonContainsDedupKey(json, "bbbb2222"));
    }

    [Fact]
    public void Misses_AbsentDedupKey()
    {
        var json = SerializeWith("aaaa1111");
        Assert.False(AlertContextSerializer.ContextJsonContainsDedupKey(json, "cccc3333"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void ReturnsFalse_OnNullOrBlankContextJson(string? contextJson)
    {
        // The Dashboard scan visits many rows whose ContextJson is null (tray/muted/server rows);
        // an un-guarded match would NRE and unwind every fingerprinted send.
        Assert.False(AlertContextSerializer.ContextJsonContainsDedupKey(contextJson, "aaaa1111"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void ReturnsFalse_OnNullOrBlankDedupKey(string? dedupKey)
    {
        var json = SerializeWith("aaaa1111");
        Assert.False(AlertContextSerializer.ContextJsonContainsDedupKey(json, dedupKey));
    }

    [Fact]
    public void IsAnchored_DoesNotMatchSameValueUnderADifferentProperty()
    {
        // The same string under a non-DedupKey property must NOT match — the anchor is "DedupKey":"…".
        const string notADedupKey = "{\"QueryHash\":\"deadbeef\",\"Other\":\"deadbeef\"}";
        Assert.False(AlertContextSerializer.ContextJsonContainsDedupKey(notADedupKey, "deadbeef"));
    }
}
