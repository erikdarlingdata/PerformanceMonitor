using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5748: a get_collection_health partial row leaves out every key whose value is null, false or 0, so the worst case
/// (every collector needing a look) stays under the default MCP response budget as collectors are added. The identity
/// keys collector, status, partial_detail, total_runs and errors always stay. A missing key therefore means null, false
/// or 0, and full_detail=true returns every field. Lite's twin of Darling's CollectionHealthPartialRowDefaultsTests.
/// </summary>
public class CollectionHealthPartialRowDefaultsTests
{
    private static readonly string[] IdentityKeys = { "collector", "status", "partial_detail", "total_runs", "errors" };

    private static Dictionary<string, object?> Partial(CollectorHealthRow row, CollectorRunTimeReading? runTime = null) =>
        McpHealthTools.PartialCollectionHealthRow(row, runTime);

    [Fact]
    public void APartialRowWithEveryValueAtItsDefaultCarriesOnlyTheIdentityKeys()
    {
        var keys = Partial(new CollectorHealthRow { CollectorName = "wait_stats" }).Keys;

        Assert.Equal(IdentityKeys.OrderBy(k => k, StringComparer.Ordinal), keys.OrderBy(k => k, StringComparer.Ordinal));

        /* The identity keys stay even at 0, and the serialized row has no null, false or 0 key beyond them. */
        var json = JsonSerializer.Serialize(Partial(new CollectorHealthRow { CollectorName = "wait_stats" }), McpHelpers.JsonOptions);
        Assert.Contains("\"total_runs\":0", json, StringComparison.Ordinal);
        Assert.Contains("\"errors\":0", json, StringComparison.Ordinal);
        Assert.DoesNotContain("null", json, StringComparison.Ordinal);
        Assert.DoesNotContain("false", json, StringComparison.Ordinal);
    }

    [Fact]
    public void ARowWithAValueSetCarriesThatKey()
    {
        var at = new DateTime(2026, 10, 13, 8, 0, 0, DateTimeKind.Utc);
        var cases = new (string Key, Action<CollectorHealthRow> Set)[]
        {
            ("session_missing", r => r.SessionMissingCount = 2),
            ("abandoned", r => r.AbandonedCount = 1),
            ("rows_stored", r => r.RowsStored = 439),
            ("last_success", r => r.LastSuccessTime = at),
            ("last_error", r => r.LastError = "Login failed for user 'darling_monitor'."),
            ("last_error_at", r => r.LastErrorTime = at),
            ("last_denied_at", r => r.LastDeniedTime = at),
        };

        foreach (var (key, set) in cases)
        {
            var row = new CollectorHealthRow { CollectorName = "wait_stats" };
            set(row);
            var partial = Partial(row);
            Assert.True(partial.ContainsKey(key), $"{key} has a value, so the partial row must carry it.");
            foreach (var identity in IdentityKeys)
                Assert.True(partial.ContainsKey(identity), $"{identity} is an identity key and must stay.");
        }
    }

    [Fact]
    public void ALongErrorCarriesTheTruncatedFlagAndAShortOneDoesNot()
    {
        var shortError = Partial(new CollectorHealthRow { CollectorName = "wait_stats", LastError = "boom" });
        Assert.False(shortError.ContainsKey("last_error_truncated"));

        var longError = Partial(new CollectorHealthRow { CollectorName = "wait_stats", LastError = new string('x', 600) });
        Assert.Equal(true, longError["last_error_truncated"]);
    }

    [Fact]
    public void ADenialNotClearedByALaterSuccessCarriesDeniedSinceLastSuccess()
    {
        var at = new DateTime(2026, 10, 13, 8, 0, 0, DateTimeKind.Utc);
        var denied = new CollectorHealthRow { CollectorName = "wait_stats", PermissionDeniedCount = 1, LastDeniedTime = at };
        Assert.True(denied.DeniedSinceLastSuccess);
        Assert.Equal(true, Partial(denied)["denied_since_last_success"]);

        var cleared = new CollectorHealthRow { CollectorName = "wait_stats", PermissionDeniedCount = 1, LastDeniedTime = at, LastSuccessTime = at.AddMinutes(5) };
        Assert.False(cleared.DeniedSinceLastSuccess);
        Assert.False(Partial(cleared).ContainsKey("denied_since_last_success"));
    }

    [Fact]
    public void TheRunTimeKeysRideOnlyWithARunTimeAndTheNoteOnlyWhenADayWasSkipped()
    {
        var row = new CollectorHealthRow { CollectorName = "server_properties" };
        var next = new DateTime(2026, 10, 14, 3, 0, 0, DateTimeKind.Utc);

        Assert.False(Partial(row).ContainsKey("run_at"));

        var onTime = Partial(row, new CollectorRunTimeReading("03:00", next));
        Assert.Equal("03:00", onTime["run_at"]);
        Assert.True(onTime.ContainsKey("next_run_utc"));
        Assert.False(onTime.ContainsKey("run_time_note"));

        var skipped = Partial(row, new CollectorRunTimeReading("03:00", next, "a day was skipped"));
        Assert.Equal("a day was skipped", skipped["run_time_note"]);
    }
}
