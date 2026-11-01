/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5715 part B, live against PostgreSQL: <see cref="PgAlertHistoryStore.GetLastFiredTierAsync"/>, the read that seeds the
/// Warning-to-Critical escalation tier across a restart. Rows are inserted directly so every alert_time is exact;
/// the instants are whole microseconds because PostgreSQL <c>timestamp</c> keeps microseconds and a .NET tick is not
/// stored. The same cases as Lite's <c>EscalationSeedHistoryStoreTests</c>, so the two stores answer alike.
/// </summary>
[Collection("live-postgres")]
public sealed class EscalationSeedLiveStoreTests
{
    private const int Server = -571510;
    private const int OtherServer = -571511;
    private const string Fire = "High CPU";
    private const string Resolution = "CPU Resolved";
    private const string Warning = "{\"Severity\":\"Warning\"}";
    private const string Critical = "{\"Severity\":\"Critical\"}";

    private static readonly DateTime Now = new DateTime(2026, 10, 13, 12, 0, 0, DateTimeKind.Unspecified).AddTicks(10 * 123);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static async Task InsertAsync(
        NpgsqlConnection connection, DateTime at, string metric, string? context, int serverId = Server, bool sent = true,
        string type = "email", string? error = null, bool muted = false)
    {
        using var cmd = new NpgsqlCommand(@"
INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, send_error, muted, context_json)
VALUES ($1, $2, 'Srv', $3, 0, 0, $4, $5, $6, $7, $8)", connection);
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = at });
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue(metric);
        cmd.Parameters.AddWithValue(sent);
        cmd.Parameters.AddWithValue(type);
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = (object?)error ?? DBNull.Value });
        cmd.Parameters.AddWithValue(muted);
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = (object?)context ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM config_alert_log WHERE server_id IN ({Server}, {OtherServer}, 0);", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task RunLiveAsync(Func<PgAlertHistoryStore, NpgsqlConnection, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live escalation-seed store test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(Ct);
        await PgMigrations.MigrateAsync(connection, Ct);
        await DeleteTestRowsAsync(connection, Ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            await body(new PgAlertHistoryStore(postgres, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds), connection);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    private static Task<AlertSeverityLevel?> ReadAsync(
        PgAlertHistoryStore store, string serverId = "-571510", TimeSpan? window = null, bool countMuted = true,
        AlertSeverityLevel? fallback = null, DateTime? until = null) =>
        store.GetLastFiredTierAsync(
            serverId, Fire, Resolution, Now - (window ?? TimeSpan.FromMinutes(15)), countMuted, fallback, until);

    [Fact]
    public Task NewestFireWins() => RunLiveAsync(async (store, c) =>
    {
        await InsertAsync(c, Now.AddMinutes(-5), Fire, Warning);
        await InsertAsync(c, Now.AddMinutes(-2), Fire, Critical);
        await InsertAsync(c, Now.AddMinutes(-9), Fire, Warning);

        Assert.Equal(AlertSeverityLevel.Critical, await ReadAsync(store));
    });

    [Fact]
    public Task GetOpenFire_HasNoLowerBound_ReadsTheValue_AndAnyResolutionTitleClosesIt() => RunLiveAsync(async (store, c) =>
    {
        // #7068: an incident that has stood for weeks is still open; the value and context come back; either title closes it.
        await InsertAsync(c, Now.AddDays(-40), Fire, Warning);
        using (var set = new NpgsqlCommand("UPDATE config_alert_log SET current_value = 1.5 WHERE server_id = $1;", c))
        {
            set.Parameters.AddWithValue(Server);
            await set.ExecuteNonQueryAsync(Ct);
        }

        var titles = new[] { "Capture Restored", Resolution };
        var open = await store.GetOpenFireAsync("-571510", Fire, titles, null, Now);
        Assert.NotNull(open);
        Assert.Equal(1.5, open!.CurrentValue);
        Assert.Equal(Warning, open.ContextJson);
        Assert.Equal(Now.AddDays(-40), open.FireAtUtc);

        // The exclusive upper bound and the window still apply.
        Assert.Null(await store.GetOpenFireAsync("-571510", Fire, titles, null, Now.AddDays(-40)));
        Assert.Null(await store.GetOpenFireAsync("-571510", Fire, titles, Now.AddDays(-1), Now));

        // A resolution under either title, as new as the fire, closes it.
        await InsertAsync(c, Now.AddDays(-39), "Capture Restored", null, sent: false, type: "none");
        Assert.Null(await store.GetOpenFireAsync("-571510", Fire, titles, null, Now));
        Assert.NotNull(await store.GetOpenFireAsync("-571510", Fire, new[] { Resolution }, null, Now));

        // A fire after the resolution is open again.
        await InsertAsync(c, Now.AddDays(-2), Fire, Critical);
        Assert.Equal(Critical, (await store.GetOpenFireAsync("-571510", Fire, titles, null, Now))!.ContextJson);
    });

    [Fact]
    public Task NewerResolution_GivesNull_ButAFireAfterItSeeds() => RunLiveAsync(async (store, c) =>
    {
        await InsertAsync(c, Now.AddMinutes(-6), Fire, Warning);
        await InsertAsync(c, Now.AddMinutes(-4), Resolution, null, sent: false, type: "none");
        Assert.Null(await ReadAsync(store));

        await InsertAsync(c, Now.AddMinutes(-1), Fire, Warning);
        Assert.Equal(AlertSeverityLevel.Warning, await ReadAsync(store));
    });

    [Fact]
    public Task OutsideTheWindow_GivesNull_AndTheEdgeIsInclusive() => RunLiveAsync(async (store, c) =>
    {
        await InsertAsync(c, Now.AddMinutes(-16), Fire, Critical);
        Assert.Null(await ReadAsync(store));

        await InsertAsync(c, Now.AddMinutes(-15), Fire, Warning);
        Assert.Equal(AlertSeverityLevel.Warning, await ReadAsync(store));
    });

    [Fact]
    public Task TheUpperBound_IsExclusive_SoARowAtOrAfterTheEngineStartIsNotRead() => RunLiveAsync(async (store, c) =>
    {
        // #5715 round 1: a row at or after the engine start is this process's own, already in memory.
        var start = Now.AddMinutes(-2);
        await InsertAsync(c, Now.AddMinutes(-6), Fire, Warning);
        await InsertAsync(c, start, Fire, Critical);
        await InsertAsync(c, Now.AddMinutes(-1), Fire, Critical);

        Assert.Equal(AlertSeverityLevel.Warning, await ReadAsync(store, until: start));
        Assert.Equal(AlertSeverityLevel.Critical, await ReadAsync(store));
        Assert.Null(await ReadAsync(store, until: Now.AddMinutes(-6)));
    });

    [Fact]
    public Task TheUpperBound_IsMicrosecondPrecise_AndAMicrosecondBeforeIsStillRead() => RunLiveAsync(async (store, c) =>
    {
        var start = Now.AddMinutes(-2);
        await InsertAsync(c, start.AddTicks(-10), Fire, Warning);
        await InsertAsync(c, start, Fire, Critical);

        // The engine's clock has a tick (100 ns) the column does not keep: the bound truncates to the microsecond.
        Assert.Equal(AlertSeverityLevel.Warning, await ReadAsync(store, until: start.AddTicks(7)));
        Assert.Equal(AlertSeverityLevel.Warning, await ReadAsync(store, until: start));
        Assert.Equal(AlertSeverityLevel.Critical, await ReadAsync(store, until: start.AddTicks(10)));
    });

    [Fact]
    public Task AResolutionAfterTheUpperBound_StillClosesAFireBeforeIt() => RunLiveAsync(async (store, c) =>
    {
        var start = Now.AddMinutes(-2);
        await InsertAsync(c, Now.AddMinutes(-6), Fire, Warning);
        await InsertAsync(c, Now.AddMinutes(-1), Resolution, null, sent: false, type: "none");

        Assert.Null(await ReadAsync(store, until: start));
    });

    [Fact]
    public Task MutedFire_Counts_UnlessTheFamilyDoesNotRecordIt() => RunLiveAsync(async (store, c) =>
    {
        await InsertAsync(c, Now.AddMinutes(-3), Fire, Warning, sent: false, type: "muted", muted: true);

        Assert.Equal(AlertSeverityLevel.Warning, await ReadAsync(store));
        Assert.Null(await ReadAsync(store, countMuted: false));
    });

    [Fact]
    public Task FireAndResolutionAtTheSameMicrosecond_GoesToTheResolution() => RunLiveAsync(async (store, c) =>
    {
        var tie = Now.AddMinutes(-4);
        await InsertAsync(c, tie, Fire, Critical);
        await InsertAsync(c, tie, Resolution, null, sent: false, type: "none");
        Assert.Null(await ReadAsync(store));

        await InsertAsync(c, tie.AddTicks(10), Fire, Critical);
        Assert.Equal(AlertSeverityLevel.Critical, await ReadAsync(store));
    });

    [Fact]
    public Task FireWhoseEveryChannelFailed_IsNotCounted_AndThrottledFiresAre() => RunLiveAsync(async (store, c) =>
    {
        await InsertAsync(c, Now.AddMinutes(-6), Fire, Warning);
        await InsertAsync(c, Now.AddMinutes(-2), Fire, Critical, sent: false, type: "failed", error: "smtp down");
        Assert.Equal(AlertSeverityLevel.Warning, await ReadAsync(store));

        await InsertAsync(c, Now.AddMinutes(-1), Fire, Critical, sent: false, type: "throttled");
        Assert.Equal(AlertSeverityLevel.Critical, await ReadAsync(store));
    });

    [Fact]
    public Task NewestFireWithNoTier_IsTheFallbackTheHostGives_OrNothing() => RunLiveAsync(async (store, c) =>
    {
        await InsertAsync(c, Now.AddMinutes(-6), Fire, Critical);
        await InsertAsync(c, Now.AddMinutes(-2), Fire, null);

        Assert.Null(await ReadAsync(store));
        Assert.Equal(AlertSeverityLevel.Warning, await ReadAsync(store, fallback: AlertSeverityLevel.Warning));
    });

    [Fact]
    public Task ServerIdentity_IsScopedLikeTheCooldownSeed() => RunLiveAsync(async (store, c) =>
    {
        await InsertAsync(c, Now.AddMinutes(-3), Fire, Critical, serverId: Server);
        await InsertAsync(c, Now.AddMinutes(-3), Fire, Warning, serverId: 0);

        Assert.Equal(AlertSeverityLevel.Critical, await ReadAsync(store, "-571510"));
        Assert.Null(await ReadAsync(store, "-571511"));
        Assert.Null(await ReadAsync(store, "not-an-integer"));
        Assert.Null(await ReadAsync(store, "0"));

        Assert.Null(await store.GetLastEmailSentUtcAsync("not-an-integer", Fire));
        Assert.Null(await store.GetLastEmailSentUtcAsync("0", Fire));
    });
}
