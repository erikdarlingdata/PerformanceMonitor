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
using System.Text.RegularExpressions;
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
/// Pins the Darling rung that gives a collector an optional run time (#4938): <c>config.config_collector_run_times</c>,
/// a table of its own, because the viewer's schedule Save deletes and re-inserts the schedules table's rows with a fixed
/// column list and a column there would be cleared by every Save from a viewer that does not know it. A row holds minutes
/// after midnight on the monitored server's clock, from 0 to 1439, fleet-wide when <c>server_id</c> is NULL; -1 is allowed
/// on a server row only and means "no fixed time on this server", which stops a fleet-wide time. No row means no run
/// time. This file holds the rung (ladder, SQL, viewer probe, the service's read and merge) and the resolution of the
/// value, including the warning for a run time that cannot apply. Every fact finds the rung by NAME, so a renumber is one
/// edit to the registration, the constant and the viewer's <c>return</c>.
/// </summary>
public sealed class CollectorRunTimeRungTests
{
    public const string RungName = "collector-run-time";

    private const string Table = "config_collector_run_times";

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

    /// <summary>The rung creates the one table, its two partial unique indexes and the reload trigger, schema-qualified,
    /// and every statement is guarded so a second run changes nothing. It names no column of the schedules table and
    /// touches no row.</summary>
    [Fact]
    public void TheRungCreatesTheRunTimesTable_IdempotentlyAndSchemaQualified_AndTouchesNothingElse()
    {
        var sql = Rung.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Contains($"CREATE TABLE IF NOT EXISTS config.{Table} (", sql, StringComparison.Ordinal);
        Assert.Contains("run_at_minute smallint NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("CHECK (run_at_minute BETWEEN 0 AND 1439 OR (run_at_minute = -1 AND server_id IS NOT NULL))", sql, StringComparison.Ordinal);
        Assert.Contains(
            $"CREATE UNIQUE INDEX IF NOT EXISTS ux_{Table}_fleet\n    ON config.{Table} (collector_name) WHERE server_id IS NULL;",
            sql, StringComparison.Ordinal);
        Assert.Contains(
            $"CREATE UNIQUE INDEX IF NOT EXISTS ux_{Table}_server\n    ON config.{Table} (server_id, collector_name) WHERE server_id IS NOT NULL;",
            sql, StringComparison.Ordinal);
        Assert.Contains($"DROP TRIGGER IF EXISTS trg_bump_collector_run_times ON config.{Table};", sql, StringComparison.Ordinal);
        Assert.Contains("AFTER INSERT OR UPDATE OR DELETE ON config." + Table, sql, StringComparison.Ordinal);
        Assert.Contains("FOR EACH STATEMENT EXECUTE FUNCTION config.config_bump_version();", sql, StringComparison.Ordinal);

        /* The schedules table is not touched at all: no ALTER, no column of it, no row of anything. */
        Assert.DoesNotContain("config_collector_schedules", sql, StringComparison.Ordinal);
        Assert.False(
            Regex.IsMatch(sql, @"^\s*(ALTER TABLE|GRANT|UPDATE|DELETE FROM|INSERT INTO|TRUNCATE|DROP TABLE|DROP COLUMN)\b", RegexOptions.Multiline),
            "the rung may only create the table, its indexes and its trigger");

        /* Every CREATE of a table or index carries IF NOT EXISTS and the one DROP carries IF EXISTS. */
        foreach (Match create in Regex.Matches(sql, @"^CREATE (TABLE|UNIQUE INDEX|INDEX)\b.*$", RegexOptions.Multiline))
        {
            Assert.Contains("IF NOT EXISTS", create.Value, StringComparison.Ordinal);
        }

        foreach (Match drop in Regex.Matches(sql, @"^DROP\b.*$", RegexOptions.Multiline))
        {
            Assert.Contains("IF EXISTS", drop.Value, StringComparison.Ordinal);
        }
    }

    /// <summary>The viewer's schedule statements, the ones its Save runs, never name the run time: that is what keeps a
    /// released viewer's Save from touching it, and what the next viewer's Save must keep doing until it carries the time
    /// on purpose.</summary>
    [Fact]
    public void TheViewersScheduleStatements_NeverNameTheRunTime()
    {
        foreach (var sql in new[]
        {
            ViewerDataService.CollectorSchedulesSelectSql,
            ViewerDataService.CollectorScheduleFleetUpsertSql,
            ViewerDataService.CollectorScheduleServerUpsertSql,
            ViewerDataService.CollectorScheduleDeleteFleetScopeSql,
            ViewerDataService.CollectorScheduleDeleteServerScopeSql,
            ViewerDataService.CollectorScheduleDeleteAllServerScopesSql,
        })
        {
            Assert.DoesNotContain("run_at_minute", sql, StringComparison.Ordinal);
            Assert.DoesNotContain(Table, sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheProbeCarriesTheTableAsItsLastArm_AndMapsFullyMigratedToTheTopRung()
    {
        var probe = ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        var arm = $"to_regclass('config.{Table}') IS NOT NULL";
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

        /* The V71 finding: the table is named in the probe line and nowhere in the arm's prose. The comment block sits
           ABOVE the `if`, and this probe line has no information_schema in it, so the prose must not repeat the name. */
        var armProseStart = viewer.LastIndexOf("/* V160 (#4938)", thisArm, StringComparison.Ordinal);
        Assert.True(armProseStart >= 0, "the arm has no comment block saying why it exists");
        Assert.DoesNotContain(Table, viewer[armProseStart..thisArm], StringComparison.Ordinal);
    }

    [Fact]
    public void TheServiceReadsTheSchedulesWithoutARunTimeColumn_AndTheRunTimesFromTheirOwnTable()
    {
        var service = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "StoreConfigProvider.cs");

        Assert.Contains(
            "\"SELECT server_id, collector_name, frequency_minutes, retention_days, enabled, databases FROM config_collector_schedules\"",
            service, StringComparison.Ordinal);
        Assert.DoesNotContain("run_at_minute FROM config_collector_schedules", service, StringComparison.Ordinal);

        Assert.Equal(
            $"SELECT server_id, collector_name, run_at_minute FROM config.{Table} ORDER BY server_id NULLS FIRST, collector_name",
            StoreConfigProvider.RunTimesSelectSql);
        Assert.Contains("reader.GetInt16(2)", service, StringComparison.Ordinal);

        /* A store at V159 has no table: that read, and only that one, answers 42P01 with no run times. */
        Assert.Contains("catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)", service, StringComparison.Ordinal);
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

    /// <summary>A stored value the table's CHECK would refuse (a store whose CHECK was dropped by hand) resolves to none,
    /// and the load says so: one warning for each, naming the collector, the scope and the value. A value the CHECK allows
    /// (0, 1439, and -1 on a server) is never reported, and a sweep that only resolves logs nothing.</summary>
    [Fact]
    public void TheWarning_NamesAValueTheTableWouldRefuse_WithTheCollectorTheScopeAndTheValue()
    {
        var servers = new[] { Target(41, "alpha") };
        var refused = new[]
        {
            Fleet("index_object_stats", 1440),
            Server("server_properties", -2),
            Fleet("server_properties", 0),
            Server("index_object_stats", 1439),
        };
        var logger = new CapturingTestLogger();

        StoreConfigProvider.LogDroppedRunTimes(logger, servers, refused);

        Assert.Equal(2, logger.CountAtLevel(LogLevel.Warning));
        var fleet = Assert.Single(logger.Lines, l => l.Contains("index_object_stats", StringComparison.Ordinal));
        Assert.Contains("fleet-wide", fleet, StringComparison.Ordinal);
        Assert.Contains("1440", fleet, StringComparison.Ordinal);
        var server = Assert.Single(logger.Lines, l => l.Contains("server_properties", StringComparison.Ordinal));
        Assert.Contains("server id 41", server, StringComparison.Ordinal);
        Assert.Contains("-2", server, StringComparison.Ordinal);

        /* A sweep that only resolves logs nothing more. */
        for (var sweep = 0; sweep < 3; sweep++)
        {
            _ = StoreConfigProvider.ResolveSchedule("server_properties", ServerId, refused);
        }

        Assert.Equal(2, logger.Lines.Count);

        var quiet = new CapturingTestLogger();
        StoreConfigProvider.LogDroppedRunTimes(quiet, servers, new[]
        {
            Fleet("index_object_stats", 0),
            Server("index_object_stats", -1),
            Fleet("server_properties", 1439),
        });
        Assert.Empty(quiet.Lines);
    }

    /* ---- the merge of the run-time rows onto the schedule rows (pure) ------------------------------------- */

    private static RunTimeOverride RunTime(int? serverId, string collector, int minute) => new(serverId, collector, minute);

    [Fact]
    public void MergeRunTimes_CarriesARunTimeOnItsScheduleRow_AndKeepsEveryOtherColumnOfTheRow()
    {
        var scope = new[] { "alpha_db" };
        var fleetRow = new ScheduleOverride(null, "index_object_stats", 1440, 30, false, scope);
        var serverRow = new ScheduleOverride(ServerId, "index_object_stats", 2880, null, true);

        var merged = StoreConfigProvider.MergeRunTimes(
            new[] { fleetRow, serverRow },
            new[] { RunTime(null, "index_object_stats", 120), RunTime(ServerId, "index_object_stats", -1) });

        Assert.Equal(2, merged.Count);
        Assert.Equal(fleetRow with { RunAtMinute = 120 }, Assert.Single(merged, o => o.ServerId is null));
        Assert.Equal(serverRow with { RunAtMinute = -1 }, Assert.Single(merged, o => o.ServerId == ServerId));
    }

    /// <summary>A run time with no schedule row makes an entry that sets nothing else. In particular its enabled state is
    /// not set, so it can neither switch on a collector that is off by default nor switch off one the fleet row enabled,
    /// and every other column resolves exactly as it did without the entry.</summary>
    [Fact]
    public void MergeRunTimes_GivesARunTimeWithNoScheduleRowAnEntryThatSetsNothingElse()
    {
        var merged = StoreConfigProvider.MergeRunTimes(
            Array.Empty<ScheduleOverride>(),
            new[] { RunTime(null, "index_object_stats", 120), RunTime(ServerId, "server_properties", 150) });

        Assert.Equal(2, merged.Count);
        foreach (var entry in merged)
        {
            Assert.Null(entry.FrequencyMinutes);
            Assert.Null(entry.RetentionDays);
            Assert.Null(entry.Enabled);
            Assert.Null(entry.Databases);
        }

        Assert.Equal(120, StoreConfigProvider.ResolveSchedule("index_object_stats", ServerId, merged).RunAtMinute);
        Assert.Equal(150, StoreConfigProvider.ResolveSchedule("server_properties", ServerId, merged).RunAtMinute);
        Assert.Null(StoreConfigProvider.ResolveSchedule("server_properties", ServerId + 1, merged).RunAtMinute);
    }

    [Fact]
    public void ARunTimeEntry_NeverChangesEnabledFrequencyRetentionOrScope_AtAnyLevel()
    {
        var rows = new[]
        {
            new ScheduleOverride(null, "index_object_stats", 1440, 7, false, new[] { "alpha_db" }),
            new ScheduleOverride(null, "pg_index_usage_stats", null, 9, true),
        };
        var runTimes = new[]
        {
            RunTime(ServerId, "index_object_stats", 130),
            RunTime(null, "long_query_completions", 100),
            RunTime(ServerId, "long_query_completions", 110),
            RunTime(null, "pg_index_usage_stats", 140),
            RunTime(null, "wait_stats", 150),
        };
        var merged = StoreConfigProvider.MergeRunTimes(rows, runTimes);

        foreach (var collector in new[] { "index_object_stats", "pg_index_usage_stats", "long_query_completions", "wait_stats", "server_properties" })
        {
            var before = StoreConfigProvider.ResolveSchedule(collector, ServerId, rows);
            var after = StoreConfigProvider.ResolveSchedule(collector, ServerId, merged);
            Assert.Equal(before.Enabled, after.Enabled);
            Assert.Equal(before.FrequencyMinutes, after.FrequencyMinutes);
            Assert.Equal(before.RetentionDays, after.RetentionDays);
            Assert.Equal(StoreConfigProvider.ResolveFleetRetentionDays(collector, rows), StoreConfigProvider.ResolveFleetRetentionDays(collector, merged));
            Assert.Equal(StoreConfigProvider.ResolveDatabaseScope(collector, ServerId, rows), StoreConfigProvider.ResolveDatabaseScope(collector, ServerId, merged));
        }

        /* The fleet row disabled index_object_stats and the server's run time must not re-enable it for that server, and
           a collector off by default stays off. */
        Assert.False(StoreConfigProvider.ResolveSchedule("index_object_stats", ServerId, merged).Enabled);
        Assert.False(StoreConfigProvider.ResolveSchedule("long_query_completions", ServerId, merged).Enabled);
        Assert.Equal(130, StoreConfigProvider.ResolveSchedule("index_object_stats", ServerId, merged).RunAtMinute);
    }

    [Fact]
    public void MergeRunTimes_HoldsOneEntryPerScopeAndCollector_MatchingTheNameWithoutRegardToCase()
    {
        var merged = StoreConfigProvider.MergeRunTimes(
            new[] { new ScheduleOverride(null, "Index_Object_Stats", 1440, 30, true) },
            new[] { RunTime(null, "index_object_stats", 120), RunTime(ServerId, "index_object_stats", 130), RunTime(ServerId + 1, "index_object_stats", 140) });

        Assert.Equal(3, merged.Count);
        Assert.Equal(1, merged.Count(o => o.ServerId is null));
        Assert.Equal(120, merged.Single(o => o.ServerId is null).RunAtMinute);
        Assert.Equal(30, merged.Single(o => o.ServerId is null).RetentionDays);

        /* The fleet row's retention is still the first, and only, fleet entry the retention horizon reads. */
        Assert.Equal(30, StoreConfigProvider.ResolveFleetRetentionDays("index_object_stats", merged));
    }

    [Fact]
    public void MergeRunTimes_WithNoRunTimes_ReturnsTheRowsAsTheyAre()
    {
        var rows = new[] { new ScheduleOverride(null, "index_object_stats", 1440, 30, true) };

        Assert.Same(rows, StoreConfigProvider.MergeRunTimes(rows, Array.Empty<RunTimeOverride>()));
    }
}

/// <summary>The live half of <see cref="CollectorRunTimeRungTests"/>: the table, its CHECK, indexes and reload trigger, an
/// upgrade from V159, the rung's SQL run twice on one store, what the viewer's schedule Save leaves alone, and the
/// service's read (merged onto the schedule rows, warned once per load, and tolerant of a store without the table).</summary>
/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the shared
   store's tables, so it cannot race the live collection and serializing it would be pure slowdown. */
public sealed class CollectorRunTimeRungLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the run-time table's live pins (each mints its own scratch database).";

    private const string Insert = "INSERT INTO config.config_collector_run_times (server_id, collector_name, run_at_minute) VALUES ";

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

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<int> VersionAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct) =>
        Convert.ToInt32(await ScalarAsync(connection, "SELECT MAX(version) FROM darling_schema_version", ct));

    /// <summary>Every run-time row as one string, so a test can say "unchanged" in one assertion.</summary>
    private static async Task<string?> RowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct) =>
        (string?)await ScalarAsync(connection,
            "SELECT COALESCE(string_agg(COALESCE(server_id::text, 'fleet') || '/' || collector_name || '=' || run_at_minute, ',' ORDER BY server_id NULLS FIRST, collector_name), '') FROM config.config_collector_run_times", ct);

    private static async Task<StoreConfigProvider> SeededProviderAsync(NpgsqlDataSource dataSource, CapturingTestLogger logger, System.Threading.CancellationToken ct)
    {
        var provider = new StoreConfigProvider(dataSource, logger);
        var config = new DarlingConfig();
        config.Servers.Add(new MonitoredServer
        {
            Name = "runtime-alpha",
            Host = "runtime-alpha-host",
            Auth = "sql",
            Username = "runtime-user",
            EncryptedPassword = "not-a-real-blob",
            EncryptMode = "Strict",
            TrustServerCertificate = true,
        });
        await provider.SeedIfEmptyAsync(config, ct);
        return provider;
    }

    [Fact]
    public async Task TheTable_HasItsColumnsItsCheckItsTwoUniqueIndexesAndTheReloadTrigger()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            Assert.Equal("server_id:integer:YES,collector_name:text:NO,run_at_minute:smallint:NO", await ScalarAsync(connection,
                "SELECT string_agg(column_name || ':' || data_type || ':' || is_nullable, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'config_collector_run_times'", ct));
            Assert.Equal(0L, await ScalarAsync(connection,
                "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'config_collector_schedules' AND column_name = 'run_at_minute'", ct));

            /* The CHECK: 0 to 1439 anywhere, -1 on a server row only. */
            foreach (var refused in new[] { "NULL, 'rt_bad', -1", "NULL, 'rt_bad', -2", "NULL, 'rt_bad', 1440", "7, 'rt_bad', -2", "7, 'rt_bad', 1440" })
            {
                var ex = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, Insert + $"({refused})", ct));
                Assert.Equal("23514", ex.SqlState);
            }

            var notNull = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, Insert + "(NULL, 'rt_null', NULL)", ct));
            Assert.Equal("23502", notNull.SqlState);

            foreach (var taken in new[] { "NULL, 'rt_a', 0", "NULL, 'rt_b', 1439", "7, 'rt_c', -1", "7, 'rt_d', 0" })
            {
                await ExecAsync(connection, Insert + $"({taken})", ct);
            }

            /* One fleet row and one server row per collector; the same collector on another server, or fleet-wide and on
               a server, is fine. */
            foreach (var duplicate in new[] { "NULL, 'rt_a', 5", "7, 'rt_d', 9" })
            {
                var ex = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, Insert + $"({duplicate})", ct));
                Assert.Equal("23505", ex.SqlState);
            }

            await ExecAsync(connection, Insert + "(8, 'rt_d', 9), (7, 'rt_a', 9)", ct);
            Assert.Contains("(collector_name) WHERE (server_id IS NULL)", (string?)await ScalarAsync(connection,
                "SELECT indexdef FROM pg_indexes WHERE schemaname = 'config' AND indexname = 'ux_config_collector_run_times_fleet'", ct), StringComparison.Ordinal);
            Assert.Contains("(server_id, collector_name) WHERE (server_id IS NOT NULL)", (string?)await ScalarAsync(connection,
                "SELECT indexdef FROM pg_indexes WHERE schemaname = 'config' AND indexname = 'ux_config_collector_run_times_server'", ct), StringComparison.Ordinal);

            /* A write bumps the reload beacon, as a schedule write does, so a running service reloads the run times. */
            Assert.Equal(1L, await ScalarAsync(connection,
                "SELECT count(*) FROM pg_trigger WHERE tgname = 'trg_bump_collector_run_times' AND NOT tgisinternal", ct));
            await ExecAsync(connection, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            foreach (var write in new[]
            {
                Insert + "(NULL, 'rt_e', 10)",
                "UPDATE config.config_collector_run_times SET run_at_minute = 11 WHERE collector_name = 'rt_e'",
                "DELETE FROM config.config_collector_run_times WHERE collector_name = 'rt_e'",
            })
            {
                var before = Convert.ToInt64(await ScalarAsync(connection, "SELECT config_version FROM config.config_service", ct));
                await ExecAsync(connection, write, ct);
                Assert.True(Convert.ToInt64(await ScalarAsync(connection, "SELECT config_version FROM config.config_service", ct)) > before, write);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task AStoreAtV159_UpgradesToTheRung_WithNoRunTimes_AndItsScheduleRowsAndColumnsUntouched()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            const string ScheduleColumns =
                "SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'config_collector_schedules'";
            var columnsBefore = await ScalarAsync(connection, ScheduleColumns, ct);

            /* Put the store back at V159: no table, its stamp gone, a schedule row an operator already wrote. */
            foreach (var sql in new[]
            {
                "DROP TABLE config.config_collector_run_times",
                "DELETE FROM darling_schema_version WHERE version >= 160",
                "INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled) VALUES (NULL, 'index_object_stats', 1440, 30, TRUE)",
            })
            {
                await ExecAsync(connection, sql, ct);
            }

            Assert.Equal(159, await VersionAsync(connection, ct));
            Assert.True(await ScalarAsync(connection, "SELECT to_regclass('config.config_collector_run_times') IS NULL", ct) is true);

            await PgMigrations.MigrateAsync(connection, ct);

            Assert.Equal(StorageVersion.SchemaVersion, await VersionAsync(connection, ct));
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM config.config_collector_run_times", ct));
            Assert.Equal("1440|30", await ScalarAsync(connection,
                "SELECT frequency_minutes || '|' || retention_days FROM config.config_collector_schedules WHERE collector_name = 'index_object_stats'", ct));
            Assert.Equal(columnsBefore, await ScalarAsync(connection, ScheduleColumns, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>The runner skips a rung a store is already stamped at, so migrating twice never runs the rung's SQL
    /// twice. This runs the rung's own SQL twice on one store that holds rows and asserts the second run is a no-op:
    /// no error, the same columns, indexes, constraint and trigger, and the same rows.</summary>
    [Fact]
    public async Task TheRungSql_RunTwiceOnOneStore_ChangesNothing_AndKeepsItsRows()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            var rungSql = PgMigrations.Scripts.Single(m => m.Name == CollectorRunTimeRungTests.RungName).Sql;
            await ExecAsync(connection, Insert + "(NULL, 'index_object_stats', 120), (7, 'index_object_stats', -1)", ct);

            const string Shape =
                "SELECT (SELECT string_agg(column_name || ':' || data_type || ':' || is_nullable, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'config_collector_run_times')"
                + " || '|' || (SELECT string_agg(indexname || ':' || indexdef, ';' ORDER BY indexname) FROM pg_indexes WHERE schemaname = 'config' AND tablename = 'config_collector_run_times')"
                + " || '|' || (SELECT string_agg(conname || ':' || pg_get_constraintdef(oid), ';' ORDER BY conname) FROM pg_constraint WHERE conrelid = 'config.config_collector_run_times'::regclass)"
                + " || '|' || (SELECT count(*) FROM pg_trigger WHERE tgrelid = 'config.config_collector_run_times'::regclass AND NOT tgisinternal)";
            var shapeBefore = await ScalarAsync(connection, Shape, ct);
            var rowsBefore = await RowsAsync(connection, ct);
            Assert.Equal("fleet/index_object_stats=120,7/index_object_stats=-1", rowsBefore);

            await ExecAsync(connection, rungSql, ct);
            await ExecAsync(connection, rungSql, ct);

            Assert.Equal(shapeBefore, await ScalarAsync(connection, Shape, ct));
            Assert.Equal(rowsBefore, await RowsAsync(connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>The install id's row survives the upgrade from V159 untouched (#4938, #4961). V159 is the install id table's
    /// final shape, and this rung creates a table of its own and restates nothing of it: the id, its bindings and its
    /// creation time read the same after the rung as before it, and the table keeps exactly its seven columns.</summary>
    [Fact]
    public async Task AStoreAtV159_KeepsItsInstallIdRowAndTableShapeUnchanged_AcrossTheUpgradeToTheRung()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            const string ReadRow =
                "SELECT install_id || '|' || system_identifier || '|' || database_oid || '|' || table_oid || '|' || server_major || '|' || created_at::text FROM config.config_install_id";
            const string ReadShape =
                "SELECT string_agg(column_name || ':' || data_type, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'config_install_id'";
            const string Row = "0a1b2c3d|7000000000000000001|16384|16400|17|2026-01-02 03:04:05.678901";
            const string Shape = "id:smallint,install_id:text,system_identifier:bigint,database_oid:bigint,created_at:timestamp without time zone,table_oid:bigint,server_major:integer";

            Assert.Equal(160, await VersionAsync(connection, ct));
            Assert.Equal(Shape, await ScalarAsync(connection, ReadShape, ct));

            /* Put the store back at V159: the run time's table and stamp gone, an install id row the service already made. */
            foreach (var sql in new[]
            {
                "DROP TABLE config.config_collector_run_times",
                "DELETE FROM darling_schema_version WHERE version >= 160",
                "INSERT INTO config.config_install_id (install_id, system_identifier, database_oid, table_oid, server_major, created_at) VALUES ('0a1b2c3d', 7000000000000000001, 16384, 16400, 17, '2026-01-02 03:04:05.678901')",
            })
            {
                await ExecAsync(connection, sql, ct);
            }

            Assert.Equal(159, await VersionAsync(connection, ct));
            Assert.Equal(Row, await ScalarAsync(connection, ReadRow, ct));
            Assert.Equal(Shape, await ScalarAsync(connection, ReadShape, ct));

            await PgMigrations.MigrateAsync(connection, ct);

            Assert.Equal(160, await VersionAsync(connection, ct));
            Assert.True(await ScalarAsync(connection, "SELECT to_regclass('config.config_collector_run_times') IS NOT NULL", ct) is true);
            Assert.Equal(Row, await ScalarAsync(connection, ReadRow, ct));
            Assert.Equal(Shape, await ScalarAsync(connection, ReadShape, ct));
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT COUNT(*) FROM config.config_install_id", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>The viewer's schedule Save, run live (its real SQL: delete the scope's rows, insert them again), leaves every
    /// run-time row alone, for a fleet scope and for a server scope. A viewer released before the table existed does the
    /// same statements, so the run times survive its Save too. The Save itself still works.</summary>
    [Fact]
    public async Task TheViewerScheduleSave_LeavesEveryRunTimeRowAlone_ForAFleetScopeAndAServerScope()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            await ExecAsync(connection,
                Insert + "(NULL, 'index_object_stats', 120), (NULL, 'server_properties', 150), (7, 'index_object_stats', 130), (7, 'server_properties', -1), (8, 'index_object_stats', 200)", ct);
            await ExecAsync(connection,
                "INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes) VALUES (NULL, 'index_object_stats', 720), (7, 'index_object_stats', 2880)", ct);
            var expected = await RowsAsync(connection, ct);
            Assert.Equal("fleet/index_object_stats=120,fleet/server_properties=150,7/index_object_stats=130,7/server_properties=-1,8/index_object_stats=200", expected);

            await using var viewer = new ViewerDataService(scratch.ConnectionString);

            await viewer.ReplaceFleetSchedulesAsync(new[] { new CollectorScheduleRow(null, "index_object_stats", 1440, 30, true, null) }, ct);
            Assert.Equal(1440, Convert.ToInt32(await ScalarAsync(connection,
                "SELECT frequency_minutes FROM config.config_collector_schedules WHERE server_id IS NULL AND collector_name = 'index_object_stats'", ct)));
            Assert.Equal(expected, await RowsAsync(connection, ct));

            await viewer.ReplaceServerSchedulesAsync(7, new[] { new CollectorScheduleRow(7, "index_object_stats", 1440, null, true, null) }, ct);
            Assert.Equal(1440, Convert.ToInt32(await ScalarAsync(connection,
                "SELECT frequency_minutes FROM config.config_collector_schedules WHERE server_id = 7 AND collector_name = 'index_object_stats'", ct)));
            Assert.Equal(expected, await RowsAsync(connection, ct));

            /* A Save that leaves a scope empty, and the reset of every server's rows, delete schedule rows only. */
            await viewer.ReplaceServerSchedulesAsync(7, Array.Empty<CollectorScheduleRow>(), ct);
            await viewer.ResetAllServerSchedulesAsync(ct);
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM config.config_collector_schedules WHERE server_id IS NOT NULL", ct));
            Assert.Equal(expected, await RowsAsync(connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>The service's read merges the run times onto the schedule rows, and the load warns once for a run time that
    /// cannot apply, each time the schedules are loaded.</summary>
    [Fact]
    public async Task LoadViewAsync_ReadsTheRunTimesFromTheirTable_AndWarnsOncePerLoad_ForAServerWhoseIntervalIsHourly()
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
            var provider = await SeededProviderAsync(dataSource, logger, ct);

            var serverId = Convert.ToInt32(await ScalarAsync(connection, "SELECT server_id FROM config_monitored_servers WHERE name = 'runtime-alpha'", ct));
            foreach (var sql in new[]
            {
                Insert + "(NULL, 'index_object_stats', 120)",
                $"INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes) VALUES ({serverId}, 'index_object_stats', 60)",
                Insert + "(NULL, 'server_properties', 150)",
                "INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days) VALUES (NULL, 'pg_index_usage_stats', 1440, 30)",
                Insert + "(NULL, 'pg_index_usage_stats', 200)",
            })
            {
                await ExecAsync(connection, sql, ct);
            }

            var first = await provider.LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(first);
            Assert.Equal(120, first!.ScheduleOverrides.Single(o => o.ServerId is null && o.CollectorName == "index_object_stats").RunAtMinute);
            Assert.Null(first.ScheduleOverrides.Single(o => o.ServerId == serverId).RunAtMinute);

            /* A run time for a collector that has a schedule row rides on that row. */
            var carried = first.ScheduleOverrides.Single(o => o.ServerId is null && o.CollectorName == "pg_index_usage_stats");
            Assert.Equal(30, carried.RetentionDays);
            Assert.Equal(200, carried.RunAtMinute);

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

    /// <summary>A store still at V159 has no run-time table. The load reads it as no run times, with no warning and no error
    /// (one debug line at most), keeps every schedule row, and leaves the connection usable for the next load.</summary>
    [Fact]
    public async Task LoadViewAsync_OnAStoreWithoutTheRunTimeTable_ReadsNoRunTimes_WithNoWarningAndNoError()
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
            var provider = await SeededProviderAsync(dataSource, logger, ct);

            await ExecAsync(connection, "DROP TABLE config.config_collector_run_times", ct);
            await ExecAsync(connection,
                "INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days) VALUES (NULL, 'index_object_stats', 1440, 30)", ct);

            var view = await provider.LoadViewAsync(new DarlingConfig(), ct);

            Assert.NotNull(view);
            var row = Assert.Single(view!.ScheduleOverrides);
            Assert.Equal(30, row.RetentionDays);
            Assert.Null(row.RunAtMinute);
            Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
            Assert.Equal(0, logger.CountAtLevel(LogLevel.Error));
            Assert.True(logger.Lines.Count(l => l.Contains("run-time table", StringComparison.OrdinalIgnoreCase)) <= 1, logger.Joined);

            Assert.NotNull(await provider.LoadViewAsync(new DarlingConfig(), ct));
            Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>A store whose CHECK was dropped by hand can hold a value the CHECK would refuse. The load resolves it to none
    /// and warns once, naming the collector, the scope and the value.</summary>
    [Fact]
    public async Task LoadViewAsync_WarnsOnce_ForAStoredRunTimeTheCheckWouldRefuse_AndResolvesItToNone()
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
            var provider = await SeededProviderAsync(dataSource, logger, ct);

            await ExecAsync(connection, "ALTER TABLE config.config_collector_run_times DROP CONSTRAINT ck_config_collector_run_times_range", ct);
            await ExecAsync(connection, Insert + "(NULL, 'index_object_stats', 1440)", ct);

            var view = await provider.LoadViewAsync(new DarlingConfig(), ct);

            Assert.NotNull(view);
            Assert.Null(StoreConfigProvider.ResolveSchedule("index_object_stats", 1, view!.ScheduleOverrides).RunAtMinute);
            Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
            var line = Assert.Single(logger.Lines, l => l.StartsWith("Warning", StringComparison.Ordinal));
            Assert.Contains("index_object_stats", line, StringComparison.Ordinal);
            Assert.Contains("fleet-wide", line, StringComparison.Ordinal);
            Assert.Contains("1440", line, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}
