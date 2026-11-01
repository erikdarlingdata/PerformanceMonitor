using System;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5715 part B: <see cref="DuckDbAlertHistoryStore.GetLastFiredTierAsync"/>, the read that seeds the
/// Warning-to-Critical escalation tier across a restart. Rows are inserted directly so the alert_time of
/// each one (and so every tie and window edge) is exact; times are whole microseconds because DuckDB
/// TIMESTAMP stores microseconds and a .NET tick is not stored.
/// </summary>
public sealed class EscalationSeedHistoryStoreTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string Fire = "High CPU";
    private const string Resolution = "CPU Resolved";
    private const string Warning = "{\"Severity\":\"Warning\"}";
    private const string Critical = "{\"Severity\":\"Critical\"}";

    private readonly DuckDbInitializer _duckDb;
    private readonly DuckDbAlertHistoryStore _store;
    private DuckDBConnection? _conn;

    // A whole-microsecond instant well inside any window, so no case depends on the wall clock.
    private static readonly DateTime Now = new DateTime(2026, 10, 13, 12, 0, 0, DateTimeKind.Utc).AddTicks(10 * 123);

    public EscalationSeedHistoryStoreTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _store = new DuckDbAlertHistoryStore(_duckDb);
    }

    public void Dispose() => _conn?.Dispose();

    private async Task InsertAsync(
        DateTime at, string metric, string? context, int serverId = 7, bool sent = true,
        string type = "email", string? error = null, bool muted = false)
    {
        if (_conn is null)
        {
            _conn = _duckDb.CreateConnection();
            await _conn.OpenAsync();
        }

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, send_error, muted, context_json)
VALUES ($1, $2, 'Srv', $3, 0, 0, $4, $5, $6, $7, $8)";
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = metric });
        cmd.Parameters.Add(new DuckDBParameter { Value = sent });
        cmd.Parameters.Add(new DuckDBParameter { Value = type });
        cmd.Parameters.Add(new DuckDBParameter { Value = error ?? (object)DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = muted });
        cmd.Parameters.Add(new DuckDBParameter { Value = context ?? (object)DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private Task<AlertSeverityLevel?> ReadAsync(
        string serverId = "7", TimeSpan? window = null, bool countMuted = true, DateTime? until = null) =>
        _store.GetLastFiredTierAsync(
            serverId, Fire, Resolution, Now - (window ?? TimeSpan.FromMinutes(15)), countMuted, untilUtc: until);

    [Fact]
    public void TheStoreOverridesTheInterfaceDefault()
    {
        var map = typeof(DuckDbAlertHistoryStore).GetInterfaceMap(typeof(IAlertHistoryStore));
        var method = typeof(IAlertHistoryStore).GetMethod(nameof(IAlertHistoryStore.GetLastFiredTierAsync))!;
        Assert.Equal(typeof(DuckDbAlertHistoryStore), map.TargetMethods[Array.IndexOf(map.InterfaceMethods, method)].DeclaringType);
    }

    [Fact]
    public async Task GetOpenFire_HasNoLowerBound_ReadsTheValue_AndAnyResolutionTitleClosesIt()
    {
        // #7068: the same cases as the Darling store's live test, so the two stores answer alike.
        await InsertAsync(Now.AddDays(-40), Fire, Warning);
        using (var set = _conn!.CreateCommand())
        {
            set.CommandText = "UPDATE config_alert_log SET current_value = 1.5 WHERE server_id = 7";
            await set.ExecuteNonQueryAsync();
        }

        var titles = new[] { "Capture Restored", Resolution };
        var open = await _store.GetOpenFireAsync("7", Fire, titles, null, Now);
        Assert.NotNull(open);
        Assert.Equal(1.5, open!.CurrentValue);
        Assert.Equal(Warning, open.ContextJson);
        Assert.Equal(Now.AddDays(-40), open.FireAtUtc);

        Assert.Null(await _store.GetOpenFireAsync("7", Fire, titles, null, Now.AddDays(-40)));
        Assert.Null(await _store.GetOpenFireAsync("7", Fire, titles, Now.AddDays(-1), Now));

        await InsertAsync(Now.AddDays(-39), "Capture Restored", null, sent: false, type: "none");
        Assert.Null(await _store.GetOpenFireAsync("7", Fire, titles, null, Now));
        Assert.NotNull(await _store.GetOpenFireAsync("7", Fire, new[] { Resolution }, null, Now));

        await InsertAsync(Now.AddDays(-2), Fire, Critical);
        Assert.Equal(Critical, (await _store.GetOpenFireAsync("7", Fire, titles, null, Now))!.ContextJson);
    }

    [Fact]
    public async Task NewestFireWins()
    {
        await InsertAsync(Now.AddMinutes(-5), Fire, Warning);
        await InsertAsync(Now.AddMinutes(-2), Fire, Critical);
        await InsertAsync(Now.AddMinutes(-9), Fire, Warning);

        Assert.Equal(AlertSeverityLevel.Critical, await ReadAsync());
    }

    [Fact]
    public async Task WarningOnly_SeedsWarning()
    {
        await InsertAsync(Now.AddMinutes(-3), Fire, Warning);

        Assert.Equal(AlertSeverityLevel.Warning, await ReadAsync());
    }

    [Fact]
    public async Task NewerResolution_GivesNull_ButAnOlderResolutionDoesNot()
    {
        await InsertAsync(Now.AddMinutes(-6), Fire, Warning);
        await InsertAsync(Now.AddMinutes(-4), Resolution, null, sent: false, type: "none");
        Assert.Null(await ReadAsync());

        // A fire AFTER the resolution is a new incident, and seeds.
        await InsertAsync(Now.AddMinutes(-1), Fire, Warning);
        Assert.Equal(AlertSeverityLevel.Warning, await ReadAsync());
    }

    [Fact]
    public async Task OutsideTheWindow_GivesNull_AndTheEdgeIsInclusive()
    {
        await InsertAsync(Now.AddMinutes(-16), Fire, Critical);
        Assert.Null(await ReadAsync(window: TimeSpan.FromMinutes(15)));

        await InsertAsync(Now.AddMinutes(-15), Fire, Warning);
        Assert.Equal(AlertSeverityLevel.Warning, await ReadAsync(window: TimeSpan.FromMinutes(15)));
    }

    [Fact]
    public async Task TheUpperBound_IsExclusive_SoARowAtOrAfterTheEngineStartIsNotRead()
    {
        // #5715 round 1: a row at or after the engine start is this process's own, already in memory.
        var start = Now.AddMinutes(-2);
        await InsertAsync(Now.AddMinutes(-6), Fire, Warning);
        await InsertAsync(start, Fire, Critical);
        await InsertAsync(Now.AddMinutes(-1), Fire, Critical);

        Assert.Equal(AlertSeverityLevel.Warning, await ReadAsync(until: start));
        Assert.Equal(AlertSeverityLevel.Critical, await ReadAsync());

        // Only rows at or after the bound exist: nothing is read.
        Assert.Null(await ReadAsync(until: Now.AddMinutes(-6)));
    }

    [Fact]
    public async Task TheUpperBound_IsMicrosecondPrecise_AndAMicrosecondBeforeIsStillRead()
    {
        var start = Now.AddMinutes(-2);
        await InsertAsync(start.AddTicks(-10), Fire, Warning);
        await InsertAsync(start, Fire, Critical);

        // The engine's clock has a tick (100 ns) the column does not keep: the bound truncates to the microsecond.
        Assert.Equal(AlertSeverityLevel.Warning, await ReadAsync(until: start.AddTicks(7)));
        Assert.Equal(AlertSeverityLevel.Warning, await ReadAsync(until: start));
        Assert.Equal(AlertSeverityLevel.Critical, await ReadAsync(until: start.AddTicks(10)));
    }

    [Fact]
    public async Task AResolutionAfterTheUpperBound_StillClosesAFireBeforeIt()
    {
        var start = Now.AddMinutes(-2);
        await InsertAsync(Now.AddMinutes(-6), Fire, Warning);
        await InsertAsync(Now.AddMinutes(-1), Resolution, null, sent: false, type: "none");

        Assert.Null(await ReadAsync(until: start));
    }

    [Fact]
    public async Task MutedFire_Counts_UnlessTheFamilyDoesNotRecordIt()
    {
        await InsertAsync(Now.AddMinutes(-3), Fire, Warning, sent: false, type: "muted", muted: true);

        Assert.Equal(AlertSeverityLevel.Warning, await ReadAsync());
        Assert.Null(await ReadAsync(countMuted: false));
    }

    [Fact]
    public async Task FireAndResolutionAtTheSameMicrosecond_GoesToTheResolution()
    {
        var tie = Now.AddMinutes(-4);
        await InsertAsync(tie, Fire, Critical);
        await InsertAsync(tie, Resolution, null, sent: false, type: "none");

        Assert.Null(await ReadAsync());

        // One microsecond later the fire is the newer row and seeds again.
        await InsertAsync(tie.AddTicks(10), Fire, Critical);
        Assert.Equal(AlertSeverityLevel.Critical, await ReadAsync());
    }

    [Fact]
    public async Task FireWhoseEveryChannelFailed_IsNotCounted_AndTheOlderDeliveredFireIsRead()
    {
        await InsertAsync(Now.AddMinutes(-6), Fire, Warning);
        await InsertAsync(Now.AddMinutes(-2), Fire, Critical, sent: false, type: "failed", error: "smtp down");

        Assert.Equal(AlertSeverityLevel.Warning, await ReadAsync());
    }

    [Fact]
    public async Task ThrottledAndUnconfiguredFires_AreCounted()
    {
        await InsertAsync(Now.AddMinutes(-2), Fire, Critical, sent: false, type: "throttled");

        Assert.Equal(AlertSeverityLevel.Critical, await ReadAsync());
    }

    [Fact]
    public async Task NewestFireWithNoTier_IsTheFallbackTheHostGives_OrNothing()
    {
        await InsertAsync(Now.AddMinutes(-6), Fire, Critical);
        await InsertAsync(Now.AddMinutes(-2), Fire, null);

        // No fallback: the newest row says nothing, and the older Critical is not read past it.
        Assert.Null(await ReadAsync());

        // Volume Free Space writes a tier only for a Critical fire, so its tierless row is a Warning.
        Assert.Equal(
            AlertSeverityLevel.Warning,
            await _store.GetLastFiredTierAsync("7", Fire, Resolution, Now - TimeSpan.FromMinutes(15), true, AlertSeverityLevel.Warning));
    }

    [Fact]
    public async Task ServerIdentity_IsScopedLikeTheCooldownSeed()
    {
        await InsertAsync(Now.AddMinutes(-3), Fire, Critical, serverId: 7);
        await InsertAsync(Now.AddMinutes(-3), Fire, Warning, serverId: 0);

        Assert.Equal(AlertSeverityLevel.Critical, await ReadAsync("7"));
        Assert.Null(await ReadAsync("8"));                  // another server's history is not this server's
        Assert.Null(await ReadAsync("not-an-integer"));     // unparseable keys collapse into bucket 0: declined (#3456)
        Assert.Null(await ReadAsync("0"));                  // and so does the literal 0

        // The email seed answers the same two keys the same way.
        Assert.Null(await _store.GetLastEmailSentUtcAsync("not-an-integer", Fire));
        Assert.Null(await _store.GetLastEmailSentUtcAsync("0", Fire));
    }
}
