using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7068: Lite's AG Sync Fell Behind is not announced again after a restart while the lag stands, per availability group
/// database, and the DuckDB read that seeds it returns every open fire so two databases behind each stay quiet.
/// </summary>
public sealed class StandingAgSyncRestartSeedTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string Ag = "AG1";
    private const string Replica = "NODE2";
    private static readonly DateTime Start = new(2026, 11, 1, 12, 0, 0, DateTimeKind.Utc);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _conn;

    public StandingAgSyncRestartSeedTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _conn?.Dispose();

    private sealed class FakeHistory : IAlertHistoryStore
    {
        public AlertHistoryStanding Standing { get; set; } = AlertHistoryStanding.Empty;
        public bool Throws { get; set; }
        public int Reads { get; private set; }

        public Task RecordAlertAsync(AlertHistoryRecord record) => Task.CompletedTask;
        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) => Task.FromResult<DateTime?>(null);

        public Task<AlertHistoryStanding> GetStandingAsync(
            string serverId, string metricName, IReadOnlyList<string> resolutionMetricNames, DateTime? sinceUtc, DateTime? untilUtc, bool countMuted = true)
        {
            Reads++;
            if (Throws)
            {
                throw new InvalidOperationException("history is down");
            }

            return Task.FromResult(Standing);
        }
    }

    private static AgDatabaseReading Reading(string database, bool behind) =>
        new(Ag, database, Replica, SecondaryLagSeconds: behind ? 900 : 0, RedoQueueSizeKb: 0, IsSuspended: false, SuspendReasonDesc: null);

    private static AlertHistoryFire Fire(string database, DateTime at) => new(
        at, 0, AlertContextSerializer.Serialize(AgAlertContexts.ForDatabase(database, Ag, Replica)));

    private static AlertHistoryClear Clear(string database, DateTime at) => new(
        at, null, $"Database {database} in AG {Ag} on replica {Replica} has caught up with the primary.");

    private static int SyncAlerts(AgAlertEvaluator e, params AgDatabaseReading[] databases) =>
        e.EvaluateDatabases(7, databases, 300, 0, TimeSpan.Zero).Count(a => !a.IsResolution && a.MetricName == AgAlertPolicy.SyncFellBehindMetric);

    private static async Task<int> SweepAsync(AgAlertEvaluator e, params AgDatabaseReading[] databases)
    {
        await e.SeedStandingAsync(7, databases, 300, 0);
        return SyncAlerts(e, databases);
    }

    private static AgAlertEvaluator Build(FakeHistory history) => new(() => Start, history);

    [Fact]
    public async Task TwoStandingDatabases_AfterARestart_FireNothing_AndTheHistoryIsReadOnce()
    {
        var history = new FakeHistory { Standing = new AlertHistoryStanding(new[] { Fire("SalesB", Start.AddHours(-5)), Fire("SalesA", Start.AddHours(-9)) }, Array.Empty<AlertHistoryClear>()) };
        var e = Build(history);

        Assert.Equal(0, await SweepAsync(e, Reading("SalesA", true), Reading("SalesB", true)));
        Assert.Equal(0, await SweepAsync(e, Reading("SalesA", true), Reading("SalesB", true)));
        Assert.Equal(1, history.Reads);
    }

    [Fact]
    public async Task WithNoHistory_TheStandingLagAnnouncesOnce()
    {
        var e = Build(new FakeHistory());

        Assert.Equal(1, await SweepAsync(e, Reading("SalesA", true)));
        Assert.Equal(0, await SweepAsync(e, Reading("SalesA", true)));
    }

    [Fact]
    public async Task ADatabaseClearedBeforeTheRestart_FiresAgain_TheOtherStaysQuiet()
    {
        var history = new FakeHistory
        {
            Standing = new AlertHistoryStanding(
                new[] { Fire("SalesB", Start.AddHours(-5)), Fire("SalesA", Start.AddHours(-9)) },
                new[] { Clear("SalesA", Start.AddHours(-7)) })
        };

        Assert.Equal(1, await SweepAsync(Build(history), Reading("SalesA", true), Reading("SalesB", true)));
    }

    [Fact]
    public async Task DatabaseSales_IsAnnounced_WhenOnlySales2HasAnOpenRow()
    {
        var history = new FakeHistory { Standing = new AlertHistoryStanding(new[] { Fire("Sales2", Start.AddHours(-5)) }, Array.Empty<AlertHistoryClear>()) };

        Assert.Equal(1, await SweepAsync(Build(history), Reading("Sales", true), Reading("Sales2", true)));
    }

    [Fact]
    public async Task AClearOfSales2_DoesNotCloseSales()
    {
        var history = new FakeHistory
        {
            Standing = new AlertHistoryStanding(
                new[] { Fire("Sales2", Start.AddHours(-5)), Fire("Sales", Start.AddHours(-9)) },
                new[] { Clear("Sales2", Start.AddHours(-3)) })
        };

        /* Sales stays quiet from its own open row; Sales2 was cleared and is announced. */
        Assert.Equal(1, await SweepAsync(Build(history), Reading("Sales", true), Reading("Sales2", true)));
    }

    [Fact]
    public async Task AClearOfALongerReplicaName_DoesNotCloseTheShorterOne()
    {
        var history = new FakeHistory
        {
            Standing = new AlertHistoryStanding(
                new[] { Fire("Sales", Start.AddHours(-9)) },
                new[] { new AlertHistoryClear(Start.AddHours(-3), null, $"Database Sales in AG {Ag} on replica {Replica}2 has caught up with the primary.") })
        };

        Assert.Equal(0, await SweepAsync(Build(history), Reading("Sales", true)));
    }

    [Fact]
    public async Task ADatabaseSeenCaughtUpFirst_ThenBehind_Fires()
    {
        var history = new FakeHistory { Standing = new AlertHistoryStanding(new[] { Fire("SalesA", Start.AddHours(-9)) }, Array.Empty<AlertHistoryClear>()) };
        var e = Build(history);

        Assert.Equal(0, await SweepAsync(e, Reading("SalesA", false)));
        Assert.Equal(1, await SweepAsync(e, Reading("SalesA", true)));
    }

    [Fact]
    public async Task ADifferentDatabase_Fires_AndAFailedReadFallsBackToFiring_ReadOnce()
    {
        var history = new FakeHistory { Standing = new AlertHistoryStanding(new[] { Fire("SalesA", Start.AddHours(-9)) }, Array.Empty<AlertHistoryClear>()) };
        Assert.Equal(1, await SweepAsync(Build(history), Reading("SalesA", true), Reading("SalesC", true)));

        var failing = new FakeHistory { Throws = true };
        var e = Build(failing);
        Assert.Equal(1, await SweepAsync(e, Reading("SalesA", true)));
        await SweepAsync(e, Reading("SalesA", true));
        Assert.Equal(1, failing.Reads);
    }

    private async Task InsertAsync(DateTime at, string metric, string? context, string? detail, int serverId = 7, string serverName = "Srv")
    {
        _conn ??= _duckDb.CreateConnection();
        if (_conn.State != System.Data.ConnectionState.Open)
        {
            await _conn.OpenAsync();
        }

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, send_error, muted, detail_text, context_json)
VALUES ($1, $2, $3, $4, 0, 0, true, 'email', NULL, false, $5, $6)";
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverName });
        cmd.Parameters.Add(new DuckDBParameter { Value = metric });
        cmd.Parameters.Add(new DuckDBParameter { Value = detail ?? (object)DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = context ?? (object)DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task TheDuckDbStore_ReturnsEveryOpenFire_AndAClearNamingOneDatabaseClosesOnlyIt()
    {
        var store = new DuckDbAlertHistoryStore(_duckDb);
        var ctxA = AlertContextSerializer.Serialize(AgAlertContexts.ForDatabase("SalesA", Ag, Replica));
        var ctxB = AlertContextSerializer.Serialize(AgAlertContexts.ForDatabase("SalesB", Ag, Replica));
        await InsertAsync(Start.AddHours(-9), "AG Sync Fell Behind", ctxA, "a");
        await InsertAsync(Start.AddHours(-5), "AG Sync Fell Behind", ctxB, "b");
        var titles = new[] { "AG Sync Recovered" };

        var standing = await store.GetStandingAsync("7", "AG Sync Fell Behind", titles, null, Start);
        Assert.Equal(2, standing.Fires.Count);
        bool OwnsA(AlertHistoryFire f) => AgAlertContexts.OwnsSyncFire(f.ContextJson, Ag, "SalesA", Replica);
        bool OwnsB(AlertHistoryFire f) => AgAlertContexts.OwnsSyncFire(f.ContextJson, Ag, "SalesB", Replica);
        Assert.NotNull(standing.OpenFor(OwnsA));
        Assert.NotNull(standing.OpenFor(OwnsB));
        Assert.Equal(Start.AddHours(-5), (await store.GetOpenFireAsync("7", "AG Sync Fell Behind", titles, null, Start))!.FireAtUtc);

        await InsertAsync(Start.AddHours(-7), "AG Sync Recovered", null, $"Database SalesA in AG {Ag} on replica {Replica} has caught up with the primary.");
        standing = await store.GetStandingAsync("7", "AG Sync Fell Behind", titles, null, Start);
        Assert.Null(standing.OpenFor(OwnsA, c => AgAlertContexts.OwnsSyncClear(c.DetailText, Ag, "SalesA", Replica)));
        Assert.NotNull(standing.OpenFor(OwnsB, c => AgAlertContexts.OwnsSyncClear(c.DetailText, Ag, "SalesB", Replica)));
    }

    [Fact]
    public async Task TheDuckDbStore_ReadsASingleKeyFleetMetricFromTheZeroBucket_AndNoOther()
    {
        var store = new DuckDbAlertHistoryStore(_duckDb);
        await InsertAsync(Start.AddDays(-2), "Collection Falling Behind", null, "fleet", serverId: 0, serverName: "store");
        await InsertAsync(Start.AddDays(-2), "Store Job Over Cadence", null, "job 1", serverId: 0, serverName: "store");

        Assert.NotNull(await store.GetOpenFireAsync("fleetgate", "Collection Falling Behind", new[] { "Collection Falling Behind Cleared" }, null, Start));
        Assert.Null(await store.GetOpenFireAsync("store:job:1", "Store Job Over Cadence", new[] { "Store Job Cadence Recovered" }, null, Start));

        await InsertAsync(Start.AddDays(-1), "Collection Falling Behind Cleared", null, "clear", serverId: 0, serverName: "store");
        Assert.Null(await store.GetOpenFireAsync("fleetgate", "Collection Falling Behind", new[] { "Collection Falling Behind Cleared" }, null, Start));
    }
}
