/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Client Timeouts settings (#5640, V179: client_timeouts_enabled, _warning_per_minute, _critical_per_minute) on a real
/// PostgreSQL store, through both writers: the MCP <c>update_alert_settings</c> (a valid pair saves, a lone bar that would cross
/// the stored other bar is refused with nothing written, the engine's clamp holds on the echo) and the viewer's
/// <c>config_alert_settings</c> select and upsert (the three columns at ordinals 73 to 75 round trip and no other column moves).
/// The row is the store's singleton, so the original three values are put back. Gated on DARLING_TEST_PG.
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so
   cross-test row churn cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class ClientTimeoutsSettingsLiveTests
{
    private const string BarsSql = "SELECT client_timeouts_enabled, client_timeouts_warning_per_minute, client_timeouts_critical_per_minute FROM config_alert_settings WHERE id = 1";

    [Fact]
    public async Task UpdateAlertSettings_SavesAValidPair_RefusesACrossingLoneBar_AndTheEchoIsClamped()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            /* A valid pair, with the switch, in one body. */
            var saved = await DarlingMcpAlertTools.UpdateAlertSettings(
                postgres, "{\"client_timeouts\":{\"enabled\":true,\"warning_per_minute\":5,\"critical_per_minute\":20}}");
            Assert.Equal("updated", DarlingMcpTestData.StatusOf(saved));
            Assert.Equal((true, 5, 20), await ReadBarsAsync(connection, ct));
            using (var doc = JsonDocument.Parse(saved))
            {
                var echo = doc.RootElement.GetProperty("settings").GetProperty("client_timeouts");
                Assert.True(echo.GetProperty("enabled").GetBoolean());
                Assert.Equal(5, echo.GetProperty("warning_per_minute").GetInt32());
                Assert.Equal(20, echo.GetProperty("critical_per_minute").GetInt32());
                Assert.Contains("client_timeouts_critical_per_minute", doc.RootElement.GetProperty("updated_fields").EnumerateArray().Select(e => e.GetString()));
            }

            /* A lone Critical bar below the stored Warning, and a lone Warning bar above the stored Critical, are refused with nothing written. */
            foreach (var (body, fragment) in new[]
            {
                ("{\"client_timeouts\":{\"critical_per_minute\":3}}", "Warning 5, Critical 3"),
                ("{\"client_timeouts\":{\"warning_per_minute\":25}}", "Warning 25, Critical 20"),
            })
            {
                var refused = await DarlingMcpAlertTools.UpdateAlertSettings(postgres, body);
                Assert.Equal("invalid", DarlingMcpTestData.StatusOf(refused));
                Assert.Contains(fragment, refused, StringComparison.Ordinal);
                Assert.Equal((true, 5, 20), await ReadBarsAsync(connection, ct));
            }

            /* Both bars in one body that cross are refused too; a pair that moves both across the old one is fine. */
            Assert.Equal("invalid", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.UpdateAlertSettings(
                postgres, "{\"client_timeouts\":{\"warning_per_minute\":30,\"critical_per_minute\":10}}")));
            Assert.Equal((true, 5, 20), await ReadBarsAsync(connection, ct));
            Assert.Equal("updated", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.UpdateAlertSettings(
                postgres, "{\"client_timeouts\":{\"warning_per_minute\":40,\"critical_per_minute\":50}}")));
            Assert.Equal((true, 40, 50), await ReadBarsAsync(connection, ct));

            /* A bar below 1 is out of range. */
            Assert.Equal("invalid", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.UpdateAlertSettings(
                postgres, "{\"client_timeouts\":{\"warning_per_minute\":0}}")));
            Assert.Equal((true, 40, 50), await ReadBarsAsync(connection, ct));

            /* The clamp: a pair the store holds crossed (a direct write, past the tool) is echoed the way the engine reads it. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "UPDATE config_alert_settings SET client_timeouts_warning_per_minute = 30, client_timeouts_critical_per_minute = 12 WHERE id = 1");
            var clamped = await DarlingMcpAlertTools.UpdateAlertSettings(postgres, "{\"client_timeouts\":{\"enabled\":false}}");
            Assert.Equal("updated", DarlingMcpTestData.StatusOf(clamped));
            using (var doc = JsonDocument.Parse(clamped))
            {
                var echo = doc.RootElement.GetProperty("settings").GetProperty("client_timeouts");
                Assert.False(echo.GetProperty("enabled").GetBoolean());
                Assert.Equal(30, echo.GetProperty("warning_per_minute").GetInt32());
                Assert.Equal(30, echo.GetProperty("critical_per_minute").GetInt32());
            }
        });
    }

    [Fact]
    public async Task TheViewersSelectAndUpsert_RoundTripTheThreeColumns_AndMoveNoOtherColumn()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            await using var viewer = new ViewerDataService(Environment.GetEnvironmentVariable("DARLING_TEST_PG")!);

            var before = await viewer.GetAlertSettingsAsync(ct);
            Assert.NotNull(before);

            /* The row as the store seeds it. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "UPDATE config_alert_settings SET client_timeouts_enabled = FALSE, client_timeouts_warning_per_minute = 10, client_timeouts_critical_per_minute = 60 WHERE id = 1");
            before = await viewer.GetAlertSettingsAsync(ct);
            Assert.False(before!.ClientTimeoutsEnabled);
            Assert.Equal(10, before.ClientTimeoutsWarningPerMinute);
            Assert.Equal(60, before.ClientTimeoutsCriticalPerMinute);

            /* Write off/10/60 back unchanged: the row is the same, column for column. */
            await viewer.UpsertAlertSettingsAsync(before, ct);
            var same = await viewer.GetAlertSettingsAsync(ct);
            Assert.True(before.ValueEquals(same!), "an unchanged save moved a column");
            Assert.Equal((false, 10, 60), await ReadBarsAsync(connection, ct));

            /* A changed pair, with the switch on. */
            var changed = await viewer.GetAlertSettingsAsync(ct);
            changed!.ClientTimeoutsEnabled = true;
            changed.ClientTimeoutsWarningPerMinute = 7;
            changed.ClientTimeoutsCriticalPerMinute = 33;
            await viewer.UpsertAlertSettingsAsync(changed, ct);
            Assert.Equal((true, 7, 33), await ReadBarsAsync(connection, ct));

            var after = await viewer.GetAlertSettingsAsync(ct);
            Assert.True(after!.ClientTimeoutsEnabled);
            Assert.Equal(7, after.ClientTimeoutsWarningPerMinute);
            Assert.Equal(33, after.ClientTimeoutsCriticalPerMinute);

            /* Nothing else moved: put the three back on the read row and the two rows are equal. */
            after.ClientTimeoutsEnabled = before.ClientTimeoutsEnabled;
            after.ClientTimeoutsWarningPerMinute = before.ClientTimeoutsWarningPerMinute;
            after.ClientTimeoutsCriticalPerMinute = before.ClientTimeoutsCriticalPerMinute;
            Assert.True(before.ValueEquals(after), "saving the Client Timeouts columns moved another column");

            /* The save goes through the engine's clamp: a crossed pair is stored lifted, a Warning below 1 is stored as 1. */
            var crossed = await viewer.GetAlertSettingsAsync(ct);
            crossed!.ClientTimeoutsWarningPerMinute = 0;
            crossed.ClientTimeoutsCriticalPerMinute = 0;
            await viewer.UpsertAlertSettingsAsync(crossed, ct);
            Assert.Equal((true, 1, 1), await ReadBarsAsync(connection, ct));
        });
    }

    private static async Task<(bool Enabled, int Warning, int Critical)> ReadBarsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(BarsSql, connection);
        using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "config_alert_settings has no singleton row");
        return (reader.GetBoolean(0), reader.GetInt32(1), reader.GetInt32(2));
    }

    private static async Task RunAsync(Func<NpgsqlDataSource, NpgsqlConnection, CancellationToken, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Client Timeouts settings test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* The singletons the writes touch (a no-op when they exist on a shared store). */
        await DarlingMcpTestData.ExecAsync(connection, ct, "INSERT INTO config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING");
        await DarlingMcpTestData.ExecAsync(connection, ct, "INSERT INTO config_alert_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING");
        await DarlingMcpTestData.ExecAsync(connection, ct, "INSERT INTO config_notification (id) VALUES (1) ON CONFLICT (id) DO NOTHING");

        var (originalEnabled, originalWarning, originalCritical) = await ReadBarsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            await body(postgres, connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt,
                    "UPDATE config_alert_settings SET client_timeouts_enabled = $1, client_timeouts_warning_per_minute = $2, client_timeouts_critical_per_minute = $3 WHERE id = 1",
                    originalEnabled, originalWarning, originalCritical));
        }
    }
}
