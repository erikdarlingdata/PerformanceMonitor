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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every live fact mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #6873: the "Backup Overdue", "Log Backup Overdue" and "CHECKDB Overdue" self-alerts, the fleet card's overdue field and the three settings
/// switches. The alerts are judged by <see cref="DatabaseProtectionBands.OverdueFacts"/> over the newest fresh capture. These tests drive the
/// evaluator with a controllable clock and a recording deliverer, and the live ones use a seeded <c>collect.database_protection</c> on a throwaway
/// PostgreSQL that each test mints for itself.
/// </summary>
public sealed class DatabaseProtectionAlertTests
{
    private const int ServerId = 515151;
    private const string Name = "PROTECTION-SRV";
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly TimeSpan Fresh = TimeSpan.FromMinutes(5);

    /// <summary>A database whose three facts were all recorded <paramref name="ago"/> before the capture. The defaults leave every fact in date.</summary>
    private static DatabaseProtectionFact Db(
        string name, DateTime capture, TimeSpan? fullAgo = null, TimeSpan? logAgo = null, TimeSpan? checkDbAgo = null,
        string platform = DatabaseProtectionCollector.PlatformNative, string state = "ONLINE", string recovery = "FULL",
        string fullState = DatabaseProtectionCollector.BackupRecorded, string logState = DatabaseProtectionCollector.BackupRecorded,
        string checkDbState = DatabaseProtectionCollector.CheckDbChecked) =>
        new(name, state, recovery, capture.AddDays(-400), null, platform,
            checkDbAgo is { } c ? capture - c : null, checkDbState,
            fullAgo is { } f ? capture - f : null, fullState,
            logAgo is { } l ? capture - l : null, logState);

    private static DatabaseProtectionFact Healthy(string name, DateTime capture) =>
        Db(name, capture, TimeSpan.FromDays(1), TimeSpan.FromMinutes(10), TimeSpan.FromDays(3));

    private static DatabaseProtectionRead Read(DateTime now, params DatabaseProtectionFact[] facts) => new(facts, now - Fresh);

    private static DatabaseProtectionRead ReadAt(DateTime now, Func<DateTime, DatabaseProtectionFact[]> facts)
    {
        var capture = now - Fresh;
        return new DatabaseProtectionRead(facts(capture), capture);
    }

    private static DarlingSelfAlertTests.Harness AllOn() =>
        new() { BackupOverdueEnabled = true, LogBackupOverdueEnabled = true, CheckDbOverdueEnabled = true };

    [Fact]
    public async Task TheAlerts_AreOffByDefault_AndFireNothingForAnOverdueDatabase_UntilTheirOwnSwitchIsOn()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var read = ReadAt(h.Now, c => new[] { Db("Sales", c, TimeSpan.FromDays(30), TimeSpan.FromHours(5), TimeSpan.FromDays(60)) });

        await h.Build().ApplyDatabaseProtectionAsync(ServerId, Name, read, Ct);
        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);

        /* Only the full backup switch on: only that alert fires. */
        h.BackupOverdueEnabled = true;
        await h.Build().ApplyDatabaseProtectionAsync(ServerId, Name, read, Ct);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Backup Overdue", fired.MetricName);
    }

    [Fact]
    public async Task AnOverdueFullBackup_FiresOnceNamingEveryOverdueDatabase_StaysQuiet_ClearsWithOneRow_AndFiresAgainWhenItReturns()
    {
        var h = AllOn();
        var e = h.Build();
        var late = ReadAt(h.Now, c => new[]
        {
            Db("Sales", c, TimeSpan.FromDays(9), TimeSpan.FromMinutes(5), TimeSpan.FromDays(2)),
            Db("Orders", c, fullAgo: null, logAgo: TimeSpan.FromMinutes(5), checkDbAgo: TimeSpan.FromDays(2), fullState: DatabaseProtectionCollector.BackupNoneInWindow),
            Healthy("Audit", c),
        });

        await e.ApplyDatabaseProtectionAsync(ServerId, Name, late, Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Backup Overdue", fired.MetricName);
        Assert.Equal(AlertSeverityLevel.Warning, fired.Severity);
        Assert.Contains("Sales (9 days old)", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("Orders (no backup on record)", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("Audit", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("older than 7 days", fired.DetailText, StringComparison.Ordinal);
        Assert.Equal("2 overdue", fired.CurrentValue);

        /* It stands a minute later: one send for the occurrence. */
        h.Now = h.Now.AddMinutes(1);
        await e.ApplyDatabaseProtectionAsync(ServerId, Name, late, Ct);
        Assert.Single(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);

        /* A capture with every full backup in date clears it: one audit row, no send, and a second pass writes nothing more. */
        var good = ReadAt(h.Now, c => new[] { Healthy("Sales", c), Healthy("Orders", c), Healthy("Audit", c) });
        await e.ApplyDatabaseProtectionAsync(ServerId, Name, good, Ct);
        var cleared = Assert.Single(h.History.Records);
        Assert.Equal("Backup Overdue Cleared", cleared.MetricName);
        Assert.False(cleared.AlertSent);
        Assert.Single(h.Deliverer.Outcomes);
        await e.ApplyDatabaseProtectionAsync(ServerId, Name, good, Ct);
        Assert.Single(h.History.Records);

        /* It goes overdue again: a new occurrence, a second send. */
        await e.ApplyDatabaseProtectionAsync(ServerId, Name, late, Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task AStandingOverdue_SendsAgainAfterTheRefireInterval_AndNotBefore()
    {
        var h = AllOn();
        h.ConnectionRefireMinutes = 30;
        var e = h.Build();
        var late = ReadAt(h.Now, c => new[] { Db("Sales", c, TimeSpan.FromDays(30), TimeSpan.FromMinutes(5), TimeSpan.FromDays(2)) });

        await e.ApplyDatabaseProtectionAsync(ServerId, Name, late, Ct);
        h.Now = h.Now.AddMinutes(29);
        await e.ApplyDatabaseProtectionAsync(ServerId, Name, late, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        h.Now = h.Now.AddMinutes(2);
        await e.ApplyDatabaseProtectionAsync(ServerId, Name, late, Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.Contains("Still the case", h.Deliverer.Outcomes.Last().DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task EachAlert_FiresOnItsOwnFact_AndTheThreeStandAndClearIndependently()
    {
        var h = AllOn();
        var e = h.Build();
        var read = ReadAt(h.Now, c => new[]
        {
            Db("FullLate", c, TimeSpan.FromDays(8), TimeSpan.FromMinutes(5), TimeSpan.FromDays(2)),
            Db("LogLate", c, TimeSpan.FromDays(1), TimeSpan.FromMinutes(61), TimeSpan.FromDays(2)),
            Db("CheckLate", c, TimeSpan.FromDays(1), TimeSpan.FromMinutes(5), TimeSpan.FromDays(15)),
            Db("CheckNever", c, TimeSpan.FromDays(1), TimeSpan.FromMinutes(5), null, checkDbState: DatabaseProtectionCollector.CheckDbNever),
        });

        await e.ApplyDatabaseProtectionAsync(ServerId, Name, read, Ct);

        var byMetric = h.Deliverer.Outcomes.ToDictionary(o => o.MetricName);
        Assert.Equal(3, byMetric.Count);
        Assert.Contains("FullLate (8 days old)", byMetric["Backup Overdue"].DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("LogLate", byMetric["Backup Overdue"].DetailText, StringComparison.Ordinal);
        Assert.Contains("LogLate (1 hour 1 minute old)", byMetric["Log Backup Overdue"].DetailText, StringComparison.Ordinal);
        Assert.Contains("older than 60 minutes", byMetric["Log Backup Overdue"].DetailText, StringComparison.Ordinal);
        Assert.Contains("CheckLate (15 days old)", byMetric["CHECKDB Overdue"].DetailText, StringComparison.Ordinal);
        Assert.Contains("CheckNever (never checked)", byMetric["CHECKDB Overdue"].DetailText, StringComparison.Ordinal);

        /* Only the log backup recovers: one Cleared row, for that alert alone. */
        var logFixed = ReadAt(h.Now, c => new[]
        {
            Db("FullLate", c, TimeSpan.FromDays(8), TimeSpan.FromMinutes(5), TimeSpan.FromDays(2)),
            Db("LogLate", c, TimeSpan.FromDays(1), TimeSpan.FromMinutes(5), TimeSpan.FromDays(2)),
            Db("CheckLate", c, TimeSpan.FromDays(1), TimeSpan.FromMinutes(5), TimeSpan.FromDays(15)),
            Db("CheckNever", c, TimeSpan.FromDays(1), TimeSpan.FromMinutes(5), null, checkDbState: DatabaseProtectionCollector.CheckDbNever),
        });
        await e.ApplyDatabaseProtectionAsync(ServerId, Name, logFixed, Ct);
        Assert.Equal("Log Backup Overdue Cleared", Assert.Single(h.History.Records).MetricName);
        Assert.Equal(3, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task APlatformManagedServer_NeverFires_AndNeitherDoesADatabaseThatIsNotJudged()
    {
        var h = AllOn();
        var e = h.Build();
        var read = ReadAt(h.Now, c => new[]
        {
            /* Azure SQL Database and Managed Instance rows: ancient ages and a CHECKDB that never ran are shown, never judged. */
            Db("AzureDb", c, TimeSpan.FromDays(400), TimeSpan.FromDays(400), null, DatabaseProtectionCollector.PlatformManaged,
                checkDbState: DatabaseProtectionCollector.CheckDbNever),
            /* Amazon RDS: ancient backups are shown, never judged (#6902: its CHECKDB is, so this one was checked yesterday). */
            Db("NativeOnly", c, TimeSpan.FromDays(400), TimeSpan.FromDays(400), TimeSpan.FromDays(1), DatabaseProtectionCollector.PlatformNativeOnly),
            /* Offline and a SIMPLE-recovery log are not judged either. */
            Db("Offline", c, TimeSpan.FromDays(400), TimeSpan.FromDays(400), TimeSpan.FromDays(400), state: "OFFLINE"),
            Db("Simple", c, TimeSpan.FromDays(1), null, TimeSpan.FromDays(1), recovery: "SIMPLE", logState: DatabaseProtectionCollector.BackupNotExpected),
        });

        await e.ApplyDatabaseProtectionAsync(ServerId, Name, read, Ct);

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);
    }

    [Fact]
    public async Task AnAmazonRdsTarget_FiresCheckDbOverdue_ButNeverTheBackupAlerts()
    {
        var h = AllOn();
        var e = h.Build();
        var read = ReadAt(h.Now, c => new[]
        {
            Db("RdsLate", c, TimeSpan.FromDays(400), TimeSpan.FromDays(400), TimeSpan.FromDays(20), DatabaseProtectionCollector.PlatformNativeOnly),
            Db("RdsNever", c, TimeSpan.FromDays(400), TimeSpan.FromDays(400), null, DatabaseProtectionCollector.PlatformNativeOnly,
                checkDbState: DatabaseProtectionCollector.CheckDbNever),
        });

        await e.ApplyDatabaseProtectionAsync(ServerId, Name, read, Ct);

        var only = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("CHECKDB Overdue", only.MetricName);
        Assert.Contains("RdsLate (20 days old)", only.DetailText, StringComparison.Ordinal);
        Assert.Contains("RdsNever (never checked)", only.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ANewDatabase_IsNotOverdueYet_ButAnOldOneWithNoBackupIs()
    {
        var h = AllOn();
        var capture = h.Now - Fresh;
        var young = Db("Young", capture, fullAgo: null, logAgo: null, checkDbAgo: null,
            fullState: DatabaseProtectionCollector.BackupNoneInWindow, logState: DatabaseProtectionCollector.BackupNoneInWindow,
            checkDbState: DatabaseProtectionCollector.CheckDbNever) with { CreateDate = capture.AddMinutes(-20) };
        var old = Db("Old", capture, fullAgo: null, logAgo: TimeSpan.FromMinutes(5), checkDbAgo: TimeSpan.FromDays(1),
            fullState: DatabaseProtectionCollector.BackupNoneInWindow);

        await h.Build().ApplyDatabaseProtectionAsync(ServerId, Name, Read(h.Now, young, old), Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Backup Overdue", fired.MetricName);
        Assert.Contains("Old (no backup on record)", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("Young", fired.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ALaggingFeed_NeitherFiresNorClearsAStandingAlert_AndTheMasterSwitchOffJudgesNothing()
    {
        var h = AllOn();
        var e = h.Build();
        var late = ReadAt(h.Now, c => new[] { Db("Sales", c, TimeSpan.FromDays(30), TimeSpan.FromMinutes(5), TimeSpan.FromDays(2)) });
        await e.ApplyDatabaseProtectionAsync(ServerId, Name, late, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* No fresh capture: the standing state is untouched, so the recovery below still writes its row. */
        await e.ApplyDatabaseProtectionAsync(ServerId, Name, null, Ct);
        Assert.Empty(h.History.Records);
        await e.ApplyDatabaseProtectionAsync(ServerId, Name, ReadAt(h.Now, c => new[] { Healthy("Sales", c) }), Ct);
        Assert.Equal("Backup Overdue Cleared", Assert.Single(h.History.Records).MetricName);

        var off = AllOn();
        off.Settings.AlertsEnabled = false;
        await off.Build().ApplyDatabaseProtectionAsync(ServerId, Name, late, Ct);
        Assert.Empty(off.Deliverer.Outcomes);
    }

    private static DarlingFleetReader.FleetServerRow Server() => new(1, "srv", "srv", 3, "sqlserver", false);

    private static FleetServerCard Card(IReadOnlyList<FleetDatabaseProtectionOverdue>? overdue) =>
        DarlingFleetReader.BuildCard(
            Server(), default, default, default, default, default, default, default, default,
            new DateTime(2026, 9, 10, 12, 0, 0, DateTimeKind.Utc).AddSeconds(-30), default, null,
            new DateTime(2026, 9, 10, 12, 0, 0, DateTimeKind.Utc), TimeSpan.FromHours(1), DeadlockRateThresholds.Default,
            databaseProtectionOverdue: overdue);

    [Fact]
    public void TheFleetCard_CarriesTheOverdueFacts_AndItsBandIsTheSameWithOrWithoutThem()
    {
        var facts = new[] { new FleetDatabaseProtectionOverdue("full_backup", "Sales", "overdue", 777600, "Full backup: Sales (9 days old)") };

        var with = Card(facts);
        var without = Card(null);
        var empty = Card(Array.Empty<FleetDatabaseProtectionOverdue>());

        Assert.Equal("Sales", Assert.Single(with.DatabaseProtectionOverdue!).Database);
        Assert.Null(without.DatabaseProtectionOverdue);
        Assert.Null(empty.DatabaseProtectionOverdue);
        Assert.Equal(without.OverallMetricSeverity, with.OverallMetricSeverity);
        Assert.Equal(without.CollectorSeverity, with.CollectorSeverity);
        Assert.Equal(without.Band, with.Band);
    }

    /* ---------------- live: a seeded store ---------------- */

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>One capture of the given databases, every row stamped with ONE collection_time (a pass stamps one for every database).</summary>
    private static Task InsertCaptureAsync(NpgsqlConnection connection, CancellationToken ct, DateTime at, params DatabaseProtectionFact[] facts) =>
        InsertCaptureForAsync(connection, ct, ServerId, Name, at, facts);

    private static async Task InsertCaptureForAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime at, params DatabaseProtectionFact[] facts)
    {
        foreach (var f in facts)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, @"INSERT INTO collect.database_protection
(collection_id, collection_time, server_id, server_name, database_name, state_desc, recovery_model_desc, create_date, replica_role, platform,
 last_good_checkdb_time, checkdb_state, last_full_backup_time, full_backup_state, last_log_backup_time, log_backup_state)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), serverId, serverName, f.DatabaseName, f.StateDesc, f.RecoveryModelDesc,
                f.CreateDate is { } created ? DarlingMcpTestData.Naive(created) : null, f.ReplicaRole, f.Platform,
                f.LastGoodCheckDbTime is { } k ? DarlingMcpTestData.Naive(k) : null, f.CheckDbState,
                f.LastFullBackupTime is { } b ? DarlingMcpTestData.Naive(b) : null, f.FullBackupState,
                f.LastLogBackupTime is { } l ? DarlingMcpTestData.Naive(l) : null, f.LogBackupState);
        }
    }

    private static async Task<T> WithStoreAsync<T>(Func<NpgsqlConnection, CancellationToken, Task> seed, Func<NpgsqlDataSource, NpgsqlConnection, CancellationToken, Task<T>> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live database protection alert tests.");
        var ct = TestContext.Current.CancellationToken;
        /* #1776 own-store: a scratch database of its own. */
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, Name, ct);
        await seed(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            var result = await body(postgres, connection, ct);
            bodySucceeded = true;
            return result;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static int Count(DarlingSelfAlertTests.Harness h, string metric) => h.Deliverer.Outcomes.Count(o => o.MetricName == metric);

    [Fact]
    public async Task ASeededStore_Fires_WhenTheNewestCaptureHasAnOverdueDatabase_AndClearsWhenAFreshGoodCaptureLands()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            var at = DateTime.UtcNow.AddMinutes(-5);
            await InsertCaptureAsync(connection, ct, at,
                Db("Sales", at, TimeSpan.FromDays(30), TimeSpan.FromMinutes(5), TimeSpan.FromDays(2)),
                Healthy("Audit", at));
        }, async (postgres, connection, ct) =>
        {
            var h = new DarlingSelfAlertTests.Harness { BackupOverdueEnabled = true, Now = DateTime.UtcNow };
            var e = h.Build();
            await e.EvaluateStoreAlertsAsync(postgres, ServerId, Name, connected: true, ct);
            Assert.Equal(1, Count(h, "Backup Overdue"));
            var fired = h.Deliverer.Outcomes.Single(o => o.MetricName == "Backup Overdue");
            Assert.Contains("Sales (30 days old)", fired.DetailText, StringComparison.Ordinal);
            Assert.DoesNotContain("Audit", fired.DetailText, StringComparison.Ordinal);

            /* A newer capture, all in date, clears it. Every row of a capture carries the same collection_time. */
            var later = DateTime.UtcNow.AddMinutes(-1);
            await InsertCaptureAsync(connection, ct, later, Healthy("Sales", later), Healthy("Audit", later));
            h.Now = DateTime.UtcNow;
            await e.EvaluateStoreAlertsAsync(postgres, ServerId, Name, connected: true, ct);
            Assert.Equal(1, Count(h, "Backup Overdue"));
            Assert.Contains(h.History.Records, r => r.MetricName == "Backup Overdue Cleared");
            return 0;
        });
    }

    [Fact]
    public async Task ASeededStore_DoesNotFire_ForAStaleCapture_OrWhenTheSwitchIsOff_AndNotForAPlatformManagedServer()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            /* Four hours old is past the three-hour freshness window. */
            var old = DateTime.UtcNow.AddHours(-4);
            await InsertCaptureAsync(connection, ct, old, Db("Sales", old, TimeSpan.FromDays(30), TimeSpan.FromMinutes(5), TimeSpan.FromDays(2)));
        }, async (postgres, connection, ct) =>
        {
            var stale = new DarlingSelfAlertTests.Harness { BackupOverdueEnabled = true, Now = DateTime.UtcNow };
            await stale.Build().EvaluateStoreAlertsAsync(postgres, ServerId, Name, connected: true, ct);
            Assert.Equal(0, Count(stale, "Backup Overdue"));

            /* A fresh overdue capture with the switches off: nothing. */
            var at = DateTime.UtcNow.AddMinutes(-2);
            await InsertCaptureAsync(connection, ct, at, Db("Sales", at, TimeSpan.FromDays(30), TimeSpan.FromMinutes(5), TimeSpan.FromDays(2)));
            var off = new DarlingSelfAlertTests.Harness { Now = DateTime.UtcNow };
            await off.Build().EvaluateStoreAlertsAsync(postgres, ServerId, Name, connected: true, ct);
            Assert.Empty(off.Deliverer.Outcomes);

            /* The same store with the switch on fires. */
            var on = new DarlingSelfAlertTests.Harness { BackupOverdueEnabled = true, Now = DateTime.UtcNow };
            await on.Build().EvaluateStoreAlertsAsync(postgres, ServerId, Name, connected: true, ct);
            Assert.Equal(1, Count(on, "Backup Overdue"));

            /* A newer capture of a platform-managed database (ancient times) leaves nothing to fire. */
            var managed = DateTime.UtcNow.AddMinutes(-1);
            await InsertCaptureAsync(connection, ct, managed,
                Db("AzureDb", managed, TimeSpan.FromDays(400), TimeSpan.FromDays(400), null, DatabaseProtectionCollector.PlatformManaged,
                    checkDbState: DatabaseProtectionCollector.CheckDbNever));
            var after = AllOn();
            after.Now = DateTime.UtcNow;
            await after.Build().EvaluateStoreAlertsAsync(postgres, ServerId, Name, connected: true, ct);
            Assert.Empty(after.Deliverer.Outcomes);
            return 0;
        });
    }

    [Fact]
    public async Task TheFleetRead_ListsOverdueFactsOnlyForAServerWithAFreshCaptureThatHasThem()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            var at = DateTime.UtcNow.AddMinutes(-5);
            await InsertCaptureAsync(connection, ct, at,
                Db("Sales", at, TimeSpan.FromDays(30), TimeSpan.FromMinutes(90), TimeSpan.FromDays(2)),
                Healthy("Audit", at));
        }, async (postgres, connection, ct) =>
        {
            var map = await DarlingFleetReader.ReadDatabaseProtectionOverdueAsync(postgres, DateTime.UtcNow, ct);
            var facts = map[ServerId];
            Assert.Equal(new[] { "full_backup", "log_backup" }, facts.Select(f => f.Kind));
            Assert.All(facts, f => Assert.Equal("Sales", f.Database));
            Assert.Contains("Full backup: Sales (30 days old)", facts[0].Text, StringComparison.Ordinal);
            Assert.Equal(30L * 24 * 3600, facts[0].AgeSeconds);

            /* A newer capture with everything in date leaves the server out. */
            var later = DateTime.UtcNow.AddMinutes(-1);
            await InsertCaptureAsync(connection, ct, later, Healthy("Sales", later), Healthy("Audit", later));
            Assert.False((await DarlingFleetReader.ReadDatabaseProtectionOverdueAsync(postgres, DateTime.UtcNow, ct)).ContainsKey(ServerId));

            /* The same capture read four hours later is past the freshness window: no facts and no claim. */
            Assert.Empty(await DarlingFleetReader.ReadDatabaseProtectionOverdueAsync(postgres, DateTime.UtcNow.AddHours(4), ct));

            /* A platform-managed capture is never listed. */
            var managed = DateTime.UtcNow;
            await InsertCaptureAsync(connection, ct, managed,
                Db("AzureDb", managed, TimeSpan.FromDays(400), TimeSpan.FromDays(400), null, DatabaseProtectionCollector.PlatformManaged,
                    checkDbState: DatabaseProtectionCollector.CheckDbNever));
            Assert.Empty(await DarlingFleetReader.ReadDatabaseProtectionOverdueAsync(postgres, DateTime.UtcNow.AddSeconds(1), ct));

            /* A PostgreSQL target and a disabled server are not probed (the lateral's WHERE), so an overdue capture on either lists nothing. */
            var overdueAt = DateTime.UtcNow;
            await DarlingMcpTestData.ExecAsync(connection, ct, @"INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, engine_kind)
VALUES (515152, 'PROTECTION-PG', 'PROTECTION-PG', TRUE, 0, $1), (515153, 'PROTECTION-OFF', 'PROTECTION-OFF', FALSE, 0, NULL)", MonitoredEngineKind.Postgres);
            await InsertCaptureForAsync(connection, ct, 515152, "PROTECTION-PG", overdueAt, Db("Sales", overdueAt, TimeSpan.FromDays(30), TimeSpan.FromMinutes(90), TimeSpan.FromDays(2)));
            await InsertCaptureForAsync(connection, ct, 515153, "PROTECTION-OFF", overdueAt, Db("Sales", overdueAt, TimeSpan.FromDays(30), TimeSpan.FromMinutes(90), TimeSpan.FromDays(2)));
            await InsertCaptureAsync(connection, ct, overdueAt, Db("Sales", overdueAt, TimeSpan.FromDays(30), TimeSpan.FromMinutes(90), TimeSpan.FromDays(2)));
            var scoped = await DarlingFleetReader.ReadDatabaseProtectionOverdueAsync(postgres, DateTime.UtcNow, ct);
            Assert.True(scoped.ContainsKey(ServerId));
            Assert.False(scoped.ContainsKey(515152));
            Assert.False(scoped.ContainsKey(515153));
            return 0;
        });
    }

    [Fact]
    public async Task TheViewerChip_ShowsTheOverdueFacts_AndNothingWhenNoneAreOverdue()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            var at = DateTime.UtcNow.AddMinutes(-5);
            await InsertCaptureAsync(connection, ct, at, Db("Sales", at, TimeSpan.FromDays(30), TimeSpan.FromMinutes(5), null, checkDbState: DatabaseProtectionCollector.CheckDbNever));
        }, async (postgres, connection, ct) =>
        {
            await using var viewer = new ViewerDataService(postgres.ConnectionString);
            var (value, detail) = await viewer.GetDatabaseProtectionChipAsync(ServerId, DateTime.UtcNow, ct);
            Assert.Equal("2 overdue", value);
            Assert.Equal("Full backup: Sales (30 days old), CHECKDB: Sales (never checked)", detail);

            /* The Overview card itself carries the chip: the summary read fills both strings the XAML binds, so a chip read that is never
               copied onto the card (a lost initializer line) fails here rather than showing nothing. */
            var card = await viewer.GetServerSummaryAsync(ServerId, "Protection card", null, ct);
            Assert.Equal(value, card.DatabaseProtectionChipValue);
            Assert.Equal(detail, card.DatabaseProtectionChipDetail);

            var later = DateTime.UtcNow.AddMinutes(-1);
            await InsertCaptureAsync(connection, ct, later, Healthy("Sales", later));
            Assert.Equal((null, null), await viewer.GetDatabaseProtectionChipAsync(ServerId, DateTime.UtcNow, ct));
            var clearCard = await viewer.GetServerSummaryAsync(ServerId, "Protection card", null, ct);
            Assert.Null(clearCard.DatabaseProtectionChipValue);
            Assert.False(clearCard.HasDatabaseProtectionOverdue);
            return 0;
        });
    }

    [Fact]
    public async Task TheSettings_ShipOff_AndRoundTripThroughTheSeed_TheStoreRead_TheTool_AndTheViewerSave()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V223 settings round trip (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            /* The seed from a default config leaves all three off. */
            var provider = new StoreConfigProvider(dataSource);
            await provider.SeedIfEmptyAsync(new DarlingConfig(), ct);
            var view = await provider.LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(view);
            Assert.False(view.Alerts.BackupOverdueEnabled);
            Assert.False(view.Alerts.LogBackupOverdueEnabled);
            Assert.False(view.Alerts.CheckDbOverdueEnabled);
            var held = new DarlingConfig();
            StoreConfigProvider.ApplyToConfig(held, view);
            var settings = new DarlingAlertSettings(held);
            Assert.False(settings.BackupOverdueEnabled);
            Assert.False(settings.LogBackupOverdueEnabled);
            Assert.False(settings.CheckDbOverdueEnabled);

            using (var doc = JsonDocument.Parse(await DarlingMcpAlertTools.GetAlertSettings(dataSource)))
            {
                var root = doc.RootElement.TryGetProperty("settings", out var wrapped) ? wrapped : doc.RootElement;
                Assert.False(root.GetProperty("backup_overdue").GetProperty("enabled").GetBoolean());
                Assert.False(root.GetProperty("log_backup_overdue").GetProperty("enabled").GetBoolean());
                Assert.False(root.GetProperty("checkdb_overdue").GetProperty("enabled").GetBoolean());
            }

            /* update_alert_settings turns two on; the store read and the held settings see them, the third stays off. */
            var updated = await DarlingMcpAlertTools.UpdateAlertSettings(
                dataSource, "{\"backup_overdue\":{\"enabled\":true},\"checkdb_overdue\":{\"enabled\":true}}");
            Assert.False(McpHelpers.IsErrorEnvelope(updated), updated);
            view = await provider.LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(view);
            Assert.True(view.Alerts.BackupOverdueEnabled);
            Assert.False(view.Alerts.LogBackupOverdueEnabled);
            Assert.True(view.Alerts.CheckDbOverdueEnabled);

            /* The Viewer reads the same row, and its Settings save writes a change the service reads back, moving no other column. */
            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var row = await viewer.GetAlertSettingsAsync(ct);
            Assert.NotNull(row);
            Assert.True(row.BackupOverdueEnabled);
            Assert.False(row.LogBackupOverdueEnabled);
            Assert.True(row.CheckDbOverdueEnabled);
            var before = await viewer.GetAlertSettingsAsync(ct);
            row.BackupOverdueEnabled = false;
            row.LogBackupOverdueEnabled = true;
            await viewer.UpsertAlertSettingsAsync(row, ct);
            view = await provider.LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(view);
            Assert.False(view.Alerts.BackupOverdueEnabled);
            Assert.True(view.Alerts.LogBackupOverdueEnabled);
            Assert.True(view.Alerts.CheckDbOverdueEnabled);
            var after = await viewer.GetAlertSettingsAsync(ct);
            after!.BackupOverdueEnabled = before!.BackupOverdueEnabled;
            after.LogBackupOverdueEnabled = before.LogBackupOverdueEnabled;
            Assert.True(before.ValueEquals(after), "saving the database protection switches moved another column");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
