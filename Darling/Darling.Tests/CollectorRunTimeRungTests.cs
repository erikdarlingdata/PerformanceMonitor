/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Darling rung that gives a collector an optional run time (#4938):
/// <c>config.config_collector_schedules.run_at_minute</c>, a nullable smallint of minutes after midnight on the
/// monitored server's clock, with a CHECK from -1 to 1439. NULL falls through per column, as <c>databases</c>
/// does; -1 means "no fixed time on this server" and stops a fleet-wide time. This file holds the rung (ladder,
/// SQL, viewer probe, the service's read) and the resolution of the value, including the warning for a run time
/// that cannot apply. Every fact finds the rung by NAME, so a renumber is one edit to the registration, the
/// constant and the viewer's <c>return</c>.
/// </summary>
public sealed class CollectorRunTimeRungTests
{
    public const string RungName = "collector-run-time";

    private const string Column = "run_at_minute";

    /// <summary>The probe's newest sentinel, so the last argument; the ordinal is a fact of the probe's shape.</summary>
    private const int ProbeOrdinal = 135;

    private const int ServerId = 41;

    public static int RungVersion => Rung.Version;

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Name == RungName);

    [Fact]
    public void TheRungIsTheTopOfADenseLadder_AtVersion160()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(160, StorageVersion.SchemaVersion);
        Assert.Equal(160, Rung.Version);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        Assert.Contains(Rung.Version - 1, versions);
    }

    [Fact]
    public void TheRungAddsOneNullableSmallintColumnWithItsCheck_IdempotentlyAndSchemaQualified()
    {
        var sql = Rung.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Contains(
            $"ALTER TABLE config.config_collector_schedules ADD COLUMN IF NOT EXISTS {Column} smallint CHECK ({Column} BETWEEN -1 AND 1439);",
            sql, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT NULL", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("DEFAULT", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("GRANT", sql, StringComparison.Ordinal);
        foreach (var shape in new[] { "UPDATE ", "DELETE ", "INSERT ", "CREATE INDEX", "ADD CONSTRAINT" })
        {
            Assert.DoesNotContain(shape, sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheProbeCarriesTheColumnAsItsLastArm_AndMapsFullyMigratedToTheTopRung()
    {
        var probe = ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        var arm = $"table_schema = 'config' AND table_name = 'config_collector_schedules' AND column_name = '{Column}'";
        Assert.Contains(arm, probe, StringComparison.Ordinal);
        Assert.True(probe.LastIndexOf("EXISTS", StringComparison.Ordinal) < probe.IndexOf(arm, StringComparison.Ordinal),
            "the new arm is the probe's last EXISTS, so it reads at the next ordinal");

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain($"reader.GetBoolean({ProbeOrdinal + 1})", viewer, StringComparison.Ordinal);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.Equal(ProbeOrdinal, arity - 1);
        Assert.Equal("hasCollectorRunAt", method.GetParameters()[ProbeOrdinal].Name);

        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(160, (int)method.Invoke(null, all)!);

        var behind = (object[])all.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(159, (int)method.Invoke(null, behind)!);

        var thisArm = viewer.IndexOf("if (hasCollectorRunAt)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasInstallIdTableOid)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "no sentinel arm: a fully-migrated store would map one rung short");
        Assert.True(thisArm < previousArm, "this arm sits below the previous rung's, so a current store maps one rung short");
        Assert.Contains("return 160;", viewer[thisArm..previousArm], StringComparison.Ordinal);
    }

    [Fact]
    public void TheServiceReadSelectsTheColumnLast_AndReadsItAsASmallint()
    {
        var service = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "StoreConfigProvider.cs");

        Assert.Contains(
            $"SELECT server_id, collector_name, frequency_minutes, retention_days, enabled, databases, {Column} FROM config_collector_schedules",
            service, StringComparison.Ordinal);
        Assert.Contains("reader.IsDBNull(6) ? null : reader.GetInt16(6)", service, StringComparison.Ordinal);
    }

    /* ---- the layering (pure) --------------------------------------------------------------------------- */

    private static ScheduleOverride Server(string collector, int? runAt, int? frequency = null) =>
        new(ServerId, collector, frequency, null, true, null, runAt);

    private static ScheduleOverride Fleet(string collector, int? runAt, int? frequency = null) =>
        new(null, collector, frequency, null, true, null, runAt);

    /// <summary>The server value wins over the fleet value; -1 on either row is "none" and on the server row stops
    /// the fleet value; NULL falls through per column; a value outside the CHECK's range is treated as not set; a
    /// run time on an hourly or per-minute effective interval resolves to none; a daily, a two-day and an on-load
    /// collector keep theirs (the on-load collectors re-run every 1440 minutes).</summary>
    [Fact]
    public void ResolveSchedule_LayersTheRunTimePerColumn_AndOnlyAWholeDayIntervalKeepsIt()
    {
        const string Daily = "index_object_stats";
        const string OnLoad = "server_properties";
        var table = new (string Name, string Collector, ScheduleOverride[] Rows, int? Expected)[]
        {
            ("no rows", Daily, Array.Empty<ScheduleOverride>(), null),
            ("fleet value", Daily, new[] { Fleet(Daily, 120) }, 120),
            ("server value", Daily, new[] { Server(Daily, 130) }, 130),
            ("server wins over fleet", Daily, new[] { Server(Daily, 130), Fleet(Daily, 120) }, 130),
            ("server -1 stops the fleet value", Daily, new[] { Server(Daily, -1), Fleet(Daily, 120) }, null),
            ("fleet -1 is none", Daily, new[] { Fleet(Daily, -1) }, null),
            ("server NULL falls through", Daily, new[] { Server(Daily, null), Fleet(Daily, 120) }, 120),
            ("both NULL", Daily, new[] { Server(Daily, null), Fleet(Daily, null) }, null),
            ("midnight is a time", Daily, new[] { Fleet(Daily, 0) }, 0),
            ("last minute of the day", Daily, new[] { Fleet(Daily, 1439) }, 1439),
            ("server value out of range falls through", Daily, new[] { Server(Daily, 1440), Fleet(Daily, 120) }, 120),
            ("below -1 is not set", Daily, new[] { Server(Daily, -2) }, null),
            ("hourly default has none", "pg_buffer_usage", new[] { Fleet("pg_buffer_usage", 120) }, null),
            ("per-minute default has none", "wait_stats", new[] { Fleet("wait_stats", 120), Server("wait_stats", 90) }, null),
            ("server frequency override makes it hourly", Daily, new[] { Server(Daily, null, 60), Fleet(Daily, 120) }, null),
            ("two-day interval keeps it", Daily, new[] { Server(Daily, 90, 2880) }, 90),
            ("on-load collector keeps it", OnLoad, new[] { Fleet(OnLoad, 150) }, 150),
            ("explicit on-load frequency keeps it", OnLoad, new[] { Server(OnLoad, 150, 0) }, 150),
            ("a row for another collector is not read", Daily, new[] { Fleet(OnLoad, 150) }, null),
        };

        foreach (var (name, collector, rows, expected) in table)
        {
            var resolved = StoreConfigProvider.ResolveSchedule(collector, ServerId, rows);
            Assert.True(expected == resolved.RunAtMinute, $"{name}: expected {expected?.ToString() ?? "none"} but resolved {resolved.RunAtMinute?.ToString() ?? "none"}");
        }
    }

    /// <summary>A dropped run time leaves the cadence alone: the frequency is what the override says, as it was
    /// before the column existed.</summary>
    [Fact]
    public void ADroppedRunTime_DoesNotChangeTheFrequency()
    {
        var resolved = StoreConfigProvider.ResolveSchedule("wait_stats", ServerId, new[] { Server("wait_stats", 90, 15) });

        Assert.Null(resolved.RunAtMinute);
        Assert.Equal(15, resolved.FrequencyMinutes);
    }

    /* ---- the warning (once per load, judged per server) -------------------------------------------------- */

    private static MonitoredServer Target(int id, string name, string engine = "sqlserver") =>
        new() { Name = name, Host = name + "-host", Engine = engine, StoredServerId = id };

    [Fact]
    public void AFleetRunTime_IsJudgedPerServer_SoOnlyAServerWithItsOwnHourlyFrequencyIsNamed()
    {
        var servers = new[] { Target(41, "alpha"), Target(42, "bravo") };
        var overrides = new[]
        {
            Fleet("index_object_stats", 120),
            new ScheduleOverride(42, "index_object_stats", 60, null, true),
        };

        var dropped = Assert.Single(StoreConfigProvider.FindDroppedRunTimes(servers, overrides));

        Assert.Equal("index_object_stats", dropped.CollectorName);
        Assert.Equal(42, dropped.ServerId);
        Assert.Equal("bravo", dropped.ServerName);
        Assert.Equal(120, dropped.RunAtMinute);
        Assert.Equal(60, dropped.IntervalMinutes);
        Assert.False(dropped.FromServerRow);
    }

    [Fact]
    public void AServerRowRunTimeOnAnHourlyCollector_IsNamedWithItsRow_AndAnEngineThatNeverRunsItIsSkipped()
    {
        var servers = new[] { Target(41, "alpha"), Target(43, "pgone", "postgres") };

        var serverRow = Assert.Single(StoreConfigProvider.FindDroppedRunTimes(servers, new[] { Server("wait_stats", 120) }));
        Assert.Equal(41, serverRow.ServerId);
        Assert.True(serverRow.FromServerRow);

        /* A fleet row for a PostgreSQL-only collector names the PostgreSQL server, not the SQL Server one that
           never runs it. */
        var fleetRow = Assert.Single(StoreConfigProvider.FindDroppedRunTimes(servers, new[] { Fleet("pg_buffer_usage", 120) }));
        Assert.Equal(43, fleetRow.ServerId);
    }

    [Fact]
    public void NothingIsDropped_ForANoneValue_ADailyCollector_OrAnOnLoadCollector()
    {
        var servers = new[] { Target(41, "alpha") };
        var overrides = new[]
        {
            Server("wait_stats", -1),
            Fleet("index_object_stats", 120),
            Fleet("server_properties", 150),
            Fleet("wait_stats", null),
        };

        Assert.Empty(StoreConfigProvider.FindDroppedRunTimes(servers, overrides));
    }

    /// <summary>One warning per dropped run time, naming the collector, the server and the time; none when nothing is
    /// dropped. It logs at the load, not in <c>ResolveSchedule</c>, which runs every sweep and stays silent.</summary>
    [Fact]
    public void TheWarning_NamesTheCollectorAndTheServer_AndResolveScheduleItselfLogsNothing()
    {
        var servers = new[] { Target(41, "alpha") };
        var overrides = new[] { Server("wait_stats", 120) };
        var logger = new CapturingTestLogger();

        StoreConfigProvider.LogDroppedRunTimes(logger, servers, overrides);

        var line = Assert.Single(logger.Lines);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Contains("wait_stats", line, StringComparison.Ordinal);
        Assert.Contains("alpha", line, StringComparison.Ordinal);
        Assert.Contains("02:00", line, StringComparison.Ordinal);

        for (var sweep = 0; sweep < 3; sweep++)
        {
            _ = StoreConfigProvider.ResolveSchedule("wait_stats", 41, overrides);
        }

        Assert.Single(logger.Lines);

        var quiet = new CapturingTestLogger();
        StoreConfigProvider.LogDroppedRunTimes(quiet, servers, new[] { Server("wait_stats", -1) });
        Assert.Empty(quiet.Lines);
        StoreConfigProvider.LogDroppedRunTimes(null, servers, overrides);
    }
}

/// <summary>The live half of <see cref="CollectorRunTimeRungTests"/>: the column, its CHECK, an upgrade from V159,
/// the service's read and its warning once per load, and what the viewer's schedule save does with the column
/// present.</summary>
/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the shared
   store's tables, so it cannot race the live collection and serializing it would be pure slowdown. */
public sealed class CollectorRunTimeRungLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the run-time column's live pins (each mints its own scratch database).";

    private static async Task<NpgsqlConnection> MigratedAsync(ScratchPostgres scratch, System.Threading.CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    [Fact]
    public async Task TheColumnIsNullableSmallint_NullByDefault_AndTheCheckRefusesMinusTwoAndOneThousandFourHundredForty()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            Assert.Equal("smallint", await ScalarAsync(connection,
                "SELECT data_type FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'config_collector_schedules' AND column_name = 'run_at_minute'", ct));
            Assert.Equal("YES", await ScalarAsync(connection,
                "SELECT is_nullable FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'config_collector_schedules' AND column_name = 'run_at_minute'", ct));
            Assert.True(await ScalarAsync(connection,
                "SELECT column_default IS NULL FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'config_collector_schedules' AND column_name = 'run_at_minute'", ct) is true);

            await using (var insert = new NpgsqlCommand(
                "INSERT INTO config.config_collector_schedules (server_id, collector_name) VALUES (NULL, 'rt_default')", connection))
            {
                await insert.ExecuteNonQueryAsync(ct);
            }

            Assert.True(await ScalarAsync(connection,
                "SELECT run_at_minute IS NULL FROM config.config_collector_schedules WHERE collector_name = 'rt_default'", ct) is true);

            foreach (var refused in new[] { -2, 1440 })
            {
                await using var bad = new NpgsqlCommand(
                    $"INSERT INTO config.config_collector_schedules (server_id, collector_name, run_at_minute) VALUES (NULL, 'rt_bad_{refused}', {refused})", connection);
                var ex = await Assert.ThrowsAsync<PostgresException>(async () => await bad.ExecuteNonQueryAsync(ct));
                Assert.Equal("23514", ex.SqlState);
            }

            foreach (var taken in new[] { -1, 0, 1439 })
            {
                await using var good = new NpgsqlCommand(
                    $"INSERT INTO config.config_collector_schedules (server_id, collector_name, run_at_minute) VALUES (NULL, 'rt_ok_{taken + 1}', {taken})", connection);
                Assert.Equal(1, await good.ExecuteNonQueryAsync(ct));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task AStoreAtV159_UpgradesToTheRung_KeepingItsRowsNull_AndARerunIsIdempotent()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            /* Put the store back at V159: no column, its stamp gone, a row an operator already wrote. */
            foreach (var sql in new[]
            {
                "ALTER TABLE config.config_collector_schedules DROP COLUMN run_at_minute",
                "DELETE FROM darling_schema_version WHERE version >= 160",
                "INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled) VALUES (NULL, 'index_object_stats', 1440, 30, TRUE)",
            })
            {
                await using var step = new NpgsqlCommand(sql, connection);
                await step.ExecuteNonQueryAsync(ct);
            }

            Assert.Equal(159, Convert.ToInt32(await ScalarAsync(connection, "SELECT MAX(version) FROM darling_schema_version", ct)));

            await PgMigrations.MigrateAsync(connection, ct);

            Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await ScalarAsync(connection, "SELECT MAX(version) FROM darling_schema_version", ct)));
            Assert.True(await ScalarAsync(connection,
                "SELECT run_at_minute IS NULL FROM config.config_collector_schedules WHERE collector_name = 'index_object_stats'", ct) is true);
            Assert.Equal(1440, Convert.ToInt32(await ScalarAsync(connection,
                "SELECT frequency_minutes FROM config.config_collector_schedules WHERE collector_name = 'index_object_stats'", ct)));

            await using var bad = new NpgsqlCommand(
                "UPDATE config.config_collector_schedules SET run_at_minute = 1440 WHERE collector_name = 'index_object_stats'", connection);
            var ex = await Assert.ThrowsAsync<PostgresException>(async () => await bad.ExecuteNonQueryAsync(ct));
            Assert.Equal("23514", ex.SqlState);

            /* Startup runs the ladder again on a store that already has the column. */
            await PgMigrations.MigrateAsync(connection, ct);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>The viewer's schedule save, with the column present: it still works, and because it deletes the
    /// scope's rows and inserts them again without naming the column, a run time stored in that scope is cleared.
    /// This pins today's behavior so the change that makes the editor carry the time has to flip it on purpose.</summary>
    [Fact]
    public async Task TheViewerScheduleSave_StillWorksWithTheColumnPresent_AndClearsAStoredRunTime()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            foreach (var sql in new[]
            {
                "INSERT INTO config.config_collector_schedules (server_id, collector_name, run_at_minute) VALUES (NULL, 'index_object_stats', 120)",
                "INSERT INTO config.config_collector_schedules (server_id, collector_name, run_at_minute) VALUES (7, 'index_object_stats', 130)",
            })
            {
                await using var seed = new NpgsqlCommand(sql, connection);
                await seed.ExecuteNonQueryAsync(ct);
            }

            await using var viewer = new ViewerDataService(scratch.ConnectionString);

            await viewer.ReplaceFleetSchedulesAsync(new[] { new CollectorScheduleRow(null, "index_object_stats", 1440, 30, true, null) }, ct);
            Assert.True(await ScalarAsync(connection,
                "SELECT run_at_minute IS NULL FROM config.config_collector_schedules WHERE server_id IS NULL AND collector_name = 'index_object_stats'", ct) is true);
            Assert.Equal(130, Convert.ToInt32(await ScalarAsync(connection,
                "SELECT run_at_minute FROM config.config_collector_schedules WHERE server_id = 7 AND collector_name = 'index_object_stats'", ct)));

            await viewer.ReplaceServerSchedulesAsync(7, new[] { new CollectorScheduleRow(7, "index_object_stats", null, null, true, null) }, ct);
            Assert.True(await ScalarAsync(connection,
                "SELECT run_at_minute IS NULL FROM config.config_collector_schedules WHERE server_id = 7 AND collector_name = 'index_object_stats'", ct) is true);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>The service's read fills the run time, and the load warns once for a run time that cannot apply,
    /// each time the schedules are loaded.</summary>
    [Fact]
    public async Task LoadViewAsync_ReadsTheRunTime_AndWarnsOncePerLoad_ForAServerWhoseIntervalIsHourly()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var logger = new CapturingTestLogger();
            var provider = new StoreConfigProvider(dataSource, logger);

            var seeded = new MonitoredServer
            {
                Name = "runtime-alpha",
                Host = "runtime-alpha-host",
                Auth = "sql",
                Username = "runtime-user",
                EncryptedPassword = "not-a-real-blob",
                EncryptMode = "Strict",
                TrustServerCertificate = true,
            };
            var config = new DarlingConfig();
            config.Servers.Add(seeded);
            await provider.SeedIfEmptyAsync(config, ct);

            var serverId = Convert.ToInt32(await ScalarAsync(connection, "SELECT server_id FROM config_monitored_servers WHERE name = 'runtime-alpha'", ct));
            foreach (var sql in new[]
            {
                "INSERT INTO config.config_collector_schedules (server_id, collector_name, run_at_minute) VALUES (NULL, 'index_object_stats', 120)",
                $"INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes) VALUES ({serverId}, 'index_object_stats', 60)",
                "INSERT INTO config.config_collector_schedules (server_id, collector_name, run_at_minute) VALUES (NULL, 'server_properties', 150)",
            })
            {
                await using var seed = new NpgsqlCommand(sql, connection);
                await seed.ExecuteNonQueryAsync(ct);
            }

            var first = await provider.LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(first);
            Assert.Equal(120, first!.ScheduleOverrides.Single(o => o.ServerId is null && o.CollectorName == "index_object_stats").RunAtMinute);
            Assert.Null(first.ScheduleOverrides.Single(o => o.ServerId == serverId).RunAtMinute);
            Assert.Equal(150, StoreConfigProvider.ResolveSchedule("server_properties", serverId, first.ScheduleOverrides).RunAtMinute);
            Assert.Null(StoreConfigProvider.ResolveSchedule("index_object_stats", serverId, first.ScheduleOverrides).RunAtMinute);

            Assert.Single(logger.Lines, l => l.Contains("run time", StringComparison.OrdinalIgnoreCase));

            /* Each load warns again; a sweep between loads, which only resolves, does not. */
            _ = StoreConfigProvider.ResolveSchedule("index_object_stats", serverId, first.ScheduleOverrides);
            Assert.Single(logger.Lines, l => l.Contains("run time", StringComparison.OrdinalIgnoreCase));
            await provider.LoadViewAsync(new DarlingConfig(), ct);
            Assert.Equal(2, logger.Lines.Count(l => l.Contains("run time", StringComparison.OrdinalIgnoreCase)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}
