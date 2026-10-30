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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>#6873 part 2: the pure banding of the stored facts into words, ages and overdue flags.</summary>
public sealed class DatabaseProtectionBandsTests
{
    private static readonly DateTime Now = new(2026, 10, 28, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DatabaseProtectionThresholds Limits = DatabaseProtectionThresholds.Default;

    private static DatabaseProtectionFact Healthy() => new(
        "d", "ONLINE", "FULL", Now.AddDays(-400), null, "native",
        Now.AddDays(-3), "checked", Now.AddDays(-1), "recorded", Now.AddMinutes(-10), "recorded");

    public static TheoryData<string, DatabaseProtectionFact, string, string, string> Cases() => new()
    {
        { "healthy", Healthy(), "ok", "ok", "ok" },

        // Age against the threshold: strictly above is overdue, exactly at it is not.
        { "full backup 7 days is on the line", Healthy() with { LastFullBackupTime = Now.AddDays(-7) }, "ok", "ok", "ok" },
        { "full backup 7 days and a minute", Healthy() with { LastFullBackupTime = Now.AddDays(-7).AddMinutes(-1) }, "ok", "overdue", "ok" },
        { "log backup 60 minutes is on the line", Healthy() with { LastLogBackupTime = Now.AddMinutes(-60) }, "ok", "ok", "ok" },
        { "log backup 61 minutes", Healthy() with { LastLogBackupTime = Now.AddMinutes(-61) }, "ok", "ok", "overdue" },
        { "CHECKDB 14 days is on the line", Healthy() with { LastGoodCheckDbTime = Now.AddDays(-14) }, "ok", "ok", "ok" },
        { "CHECKDB 15 days", Healthy() with { LastGoodCheckDbTime = Now.AddDays(-15) }, "overdue", "ok", "ok" },
        { "a time after the capture is age zero, not negative", Healthy() with { LastFullBackupTime = Now.AddMinutes(5) }, "ok", "ok", "ok" },

        // Never, and none in the 60 day window, are overdue on an old database.
        { "old database, CHECKDB never", Healthy() with { LastGoodCheckDbTime = null, CheckDbState = "never" }, "never", "ok", "ok" },
        { "old database, no backups in the window",
            Healthy() with { LastFullBackupTime = null, FullBackupState = "none_in_window", LastLogBackupTime = null, LogBackupState = "none_in_window" },
            "ok", "none_in_window", "none_in_window" },

        // New-database grace: created less than the threshold ago is not late for "never".
        { "created 13 days ago, CHECKDB never", Healthy() with { CreateDate = Now.AddDays(-13), LastGoodCheckDbTime = null, CheckDbState = "never" }, "new_database", "ok", "ok" },
        { "created 15 days ago, CHECKDB never", Healthy() with { CreateDate = Now.AddDays(-15), LastGoodCheckDbTime = null, CheckDbState = "never" }, "never", "ok", "ok" },
        { "created 2 days ago, no full backup", Healthy() with { CreateDate = Now.AddDays(-2), LastFullBackupTime = null, FullBackupState = "none_in_window" }, "ok", "new_database", "ok" },
        { "created 8 days ago, no full backup", Healthy() with { CreateDate = Now.AddDays(-8), LastFullBackupTime = null, FullBackupState = "none_in_window" }, "ok", "none_in_window", "ok" },
        { "created 30 minutes ago, no log backup", Healthy() with { CreateDate = Now.AddMinutes(-30), LastLogBackupTime = null, LogBackupState = "none_in_window" }, "ok", "ok", "new_database" },
        { "created 90 minutes ago, no log backup", Healthy() with { CreateDate = Now.AddMinutes(-90), LastLogBackupTime = null, LogBackupState = "none_in_window" }, "ok", "ok", "none_in_window" },
        { "no create date means no grace", Healthy() with { CreateDate = null, LastGoodCheckDbTime = null, CheckDbState = "never" }, "never", "ok", "ok" },
        { "grace does not hide a recorded old backup", Healthy() with { CreateDate = Now.AddDays(-2), LastFullBackupTime = Now.AddDays(-9) }, "ok", "overdue", "ok" },

        // Recovery model: SIMPLE expects no log backup.
        { "SIMPLE recovery, no log backup expected",
            Healthy() with { RecoveryModelDesc = "SIMPLE", LastLogBackupTime = null, LogBackupState = "not_expected" }, "ok", "ok", "not_expected" },

        // Not judged at all.
        { "offline", Healthy() with { StateDesc = "OFFLINE", LastFullBackupTime = null, FullBackupState = "none_in_window" }, "excluded", "excluded", "excluded" },
        { "restoring", Healthy() with { StateDesc = "RESTORING", LastFullBackupTime = Now.AddDays(-90) }, "excluded", "excluded", "excluded" },
        { "secondary replica", Healthy() with { ReplicaRole = "SECONDARY", LastLogBackupTime = Now.AddDays(-2) }, "excluded", "excluded", "excluded" },
        { "primary replica is judged", Healthy() with { ReplicaRole = "PRIMARY", LastLogBackupTime = Now.AddDays(-2) }, "ok", "ok", "overdue" },

        // The platform owns it: the age shows, never overdue.
        { "platform managed, old backups",
            Healthy() with { Platform = "platform_managed", LastFullBackupTime = Now.AddDays(-30), LastLogBackupTime = Now.AddDays(-2), LastGoodCheckDbTime = Now.AddDays(-90) },
            "platform_managed", "platform_managed", "platform_managed" },
        { "platform managed, CHECKDB never and no backups",
            Healthy() with { Platform = "platform_managed", LastGoodCheckDbTime = null, CheckDbState = "never", LastFullBackupTime = null, FullBackupState = "none_in_window", CreateDate = Now.AddDays(-200) },
            "platform_managed", "platform_managed", "platform_managed" },
        // #6902: native_only (Amazon RDS) leaves the two backup facts unjudged and judges CHECKDB as on SQL Server.
        { "Amazon RDS, old backups, CHECKDB 1 day old", Healthy() with { Platform = "native_only", LastFullBackupTime = Now.AddDays(-30) }, "ok", "native_only", "native_only" },
        { "Amazon RDS, CHECKDB 20 days old", Healthy() with { Platform = "native_only", LastGoodCheckDbTime = Now.AddDays(-20) }, "overdue", "native_only", "native_only" },
        { "Amazon RDS, CHECKDB never on an old database",
            Healthy() with { Platform = "native_only", LastGoodCheckDbTime = null, CheckDbState = "never", CreateDate = Now.AddDays(-200) }, "never", "native_only", "native_only" },
        { "Amazon RDS, CHECKDB never on a database created 5 days ago",
            Healthy() with { Platform = "native_only", LastGoodCheckDbTime = null, CheckDbState = "never", CreateDate = Now.AddDays(-5) }, "new_database", "native_only", "native_only" },
        { "Amazon RDS, no backups in the window stay native_only",
            Healthy() with { Platform = "native_only", LastFullBackupTime = null, FullBackupState = "none_in_window", LastLogBackupTime = null, LogBackupState = "none_in_window", CreateDate = Now.AddDays(-200) },
            "ok", "native_only", "native_only" },
        { "Amazon RDS, CHECKDB unknown passes through", Healthy() with { Platform = "native_only", LastGoodCheckDbTime = null, CheckDbState = "unknown" }, "unknown", "native_only", "native_only" },

        // The collector's own words pass through.
        { "CHECKDB unknown (2017 before CU9)", Healthy() with { LastGoodCheckDbTime = null, CheckDbState = "unknown" }, "unknown", "ok", "ok" },
        { "backup source unavailable", Healthy() with { LastFullBackupTime = null, FullBackupState = "unavailable", LastLogBackupTime = null, LogBackupState = "unavailable" }, "ok", "unavailable", "unavailable" },
        { "unavailable stays unavailable on a managed platform", Healthy() with { Platform = "platform_managed", LastFullBackupTime = null, FullBackupState = "unavailable" }, "platform_managed", "unavailable", "platform_managed" },
    };

    [Fact]
    public void APlatformManagedDatabaseWithNoBackupInTheWindow_ReadsPlatformManagedWithNoAge_AndAnUnreadableSourceReadsUnavailable()
    {
        var none = Healthy() with
        {
            Platform = "platform_managed", LastFullBackupTime = null, FullBackupState = "none_in_window",
            LastLogBackupTime = null, LogBackupState = "none_in_window",
        };
        var noneBand = DatabaseProtectionBands.Evaluate(none, Now, Limits);
        foreach (var f in new[] { noneBand.FullBackup, noneBand.LogBackup })
        {
            Assert.Equal("platform_managed", f.State);
            Assert.Null(f.Age);
            Assert.False(f.Overdue);
        }

        var refused = none with { FullBackupState = "unavailable", LogBackupState = "unavailable" };
        var refusedBand = DatabaseProtectionBands.Evaluate(refused, Now, Limits);
        Assert.Equal("unavailable", refusedBand.FullBackup.State);
        Assert.Equal("unavailable", refusedBand.LogBackup.State);
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void EachFact_GetsItsWord(string name, DatabaseProtectionFact fact, string checkDb, string full, string log)
    {
        var band = DatabaseProtectionBands.Evaluate(fact, Now, Limits);

        Assert.True(checkDb == band.CheckDb.State, name + ": CHECKDB " + band.CheckDb.State);
        Assert.True(full == band.FullBackup.State, name + ": full " + band.FullBackup.State);
        Assert.True(log == band.LogBackup.State, name + ": log " + band.LogBackup.State);
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void OverdueIsTrueOnlyForTheOverdueWords(string name, DatabaseProtectionFact fact, string checkDb, string full, string log)
    {
        _ = (checkDb, full, log);
        var band = DatabaseProtectionBands.Evaluate(fact, Now, Limits);

        foreach (var f in new[] { band.CheckDb, band.FullBackup, band.LogBackup })
        {
            var expected = f.State is "overdue" or "never" or "none_in_window";
            Assert.True(expected == f.Overdue, name + ": " + f.State + " overdue=" + f.Overdue);
        }
    }

    [Fact]
    public void TheAge_IsMeasuredAtTheCapture_AndShownEvenWhenNotJudged()
    {
        var managed = DatabaseProtectionBands.Evaluate(
            Healthy() with { Platform = "platform_managed", LastFullBackupTime = Now.AddDays(-30) }, Now, Limits);
        var excluded = DatabaseProtectionBands.Evaluate(
            Healthy() with { StateDesc = "OFFLINE", LastFullBackupTime = Now.AddDays(-30) }, Now, Limits);

        Assert.Equal(TimeSpan.FromDays(30), managed.FullBackup.Age);
        Assert.Equal(TimeSpan.FromDays(30), excluded.FullBackup.Age);
        Assert.Equal("offline", excluded.ExcludedReason);
        Assert.Equal("secondary", DatabaseProtectionBands.Evaluate(Healthy() with { ReplicaRole = "SECONDARY" }, Now, Limits).ExcludedReason);
        Assert.Null(DatabaseProtectionBands.Evaluate(Healthy(), Now, Limits).ExcludedReason);
    }

    [Fact]
    public void TheSummary_NamesWhichDatabasesAreOverdueOnWhat_AndLeavesTheExcludedOut()
    {
        var facts = new[]
        {
            Healthy() with { DatabaseName = "fine" },
            Healthy() with { DatabaseName = "stale_full", LastFullBackupTime = Now.AddDays(-9) },
            Healthy() with { DatabaseName = "stale_both", LastFullBackupTime = Now.AddDays(-9), LastLogBackupTime = Now.AddHours(-5), LastGoodCheckDbTime = Now.AddDays(-40) },
            Healthy() with { DatabaseName = "off", StateDesc = "OFFLINE", LastFullBackupTime = Now.AddDays(-90) },
        };
        var banded = facts.Select(f => (Fact: f, Band: DatabaseProtectionBands.Evaluate(f, Now, Limits))).ToList();

        var summary = DatabaseProtectionBands.Summarize(banded);

        Assert.Equal(4, summary.Databases);
        Assert.Equal(3, summary.Assessed);
        Assert.Equal(1, summary.Excluded);
        Assert.Equal(2, summary.OverdueDatabases);
        Assert.Equal(["stale_full", "stale_both"], summary.OverdueFullBackup);
        Assert.Equal(["stale_both"], summary.OverdueLogBackup);
        Assert.Equal(["stale_both"], summary.OverdueCheckDb);
        Assert.Contains("2 of 3 judged databases overdue", summary.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSummary_SaysSoWhenNothingIsOverdue()
    {
        var banded = new[] { Healthy() }.Select(f => (Fact: f, Band: DatabaseProtectionBands.Evaluate(f, Now, Limits))).ToList();

        var summary = DatabaseProtectionBands.Summarize(banded);

        Assert.Equal(0, summary.OverdueDatabases);
        Assert.Equal("No database is overdue (1 judged).", summary.Text);
    }

    [Theory]
    [InlineData(42, "42 minutes")]
    [InlineData(1, "1 minute")]
    [InlineData(130, "2 hours 10 minutes")]
    [InlineData(3 * 1440 + 4 * 60 + 5, "3 days 4 hours")]
    public void TheAgeText_ShowsTheTwoLargestUnits(int minutes, string expected)
    {
        Assert.Equal(expected, DarlingMcpDatabaseProtectionTools.Describe(TimeSpan.FromMinutes(minutes)));
    }
}

/// <summary>
/// #6873 part 2: <c>get_database_protection</c> over a real store. A seeded capture gives each database its words, ages and overdue flags,
/// an older capture does not bring a dropped database back, a platform-managed target is never overdue, and a server with no capture
/// reads <c>unavailable</c>.
/// </summary>
[Collection("live-postgres")]
public sealed class DatabaseProtectionReadLiveTests
{
    private const string ServerName = "darling-database-protection";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<T> WithStoreAsync<T>(Func<NpgsqlConnection, CancellationToken, Task> seed, Func<NpgsqlDataSource, CancellationToken, Task<T>> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live database protection tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await seed(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        return await body(postgres, ct);
    }

    private sealed record Seed(
        string Name, string State = "ONLINE", string Recovery = "FULL", TimeSpan? CreatedAgo = null, string? Role = null,
        string Platform = "native", TimeSpan? CheckDbAgo = null, string CheckDbState = "checked", TimeSpan? FullAgo = null,
        string FullState = "recorded", TimeSpan? LogAgo = null, string LogState = "recorded");

    private static Task InsertAsync(NpgsqlConnection connection, CancellationToken ct, DateTime at, Seed s)
    {
        DateTime? Ago(TimeSpan? span) => span is { } v ? DarlingMcpTestData.Naive(at - v) : null;
        return DarlingMcpTestData.ExecAsync(connection, ct, @"INSERT INTO collect.database_protection
(collection_id, collection_time, server_id, server_name, database_name, state_desc, recovery_model_desc, create_date, replica_role, platform,
 last_good_checkdb_time, checkdb_state, last_full_backup_time, full_backup_state, last_log_backup_time, log_backup_state)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, s.Name, s.State, s.Recovery,
            Ago(s.CreatedAgo ?? TimeSpan.FromDays(400)), s.Role, s.Platform, Ago(s.CheckDbAgo), s.CheckDbState, Ago(s.FullAgo), s.FullState,
            Ago(s.LogAgo), s.LogState);
    }

    private static Task<string> Call(NpgsqlDataSource postgres, CancellationToken ct, bool overdueOnly = false) =>
        DarlingMcpDatabaseProtectionTools.GetDatabaseProtection(postgres, ServerName, 24, null, overdueOnly, ct);

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    private static Dictionary<string, JsonElement> ByName(JsonElement root) =>
        root.GetProperty("databases").EnumerateArray().ToDictionary(d => d.GetProperty("database").GetString()!);

    private static string[] Names(JsonElement array) => array.EnumerateArray().Select(v => v.GetString()!).ToArray();

    [Fact]
    public async Task TheNewestCapture_GivesEachDatabaseItsWords_AndTheSummaryNamesTheOverdueOnes()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            var older = DateTime.UtcNow.AddHours(-2);
            var at = DateTime.UtcNow.AddMinutes(-5);
            var day = TimeSpan.FromDays(1);
            var fine = new Seed("fine", CheckDbAgo: 3 * day, FullAgo: day, LogAgo: TimeSpan.FromMinutes(10));
            /* A dropped database is only in the older capture and must not come back. */
            await InsertAsync(connection, ct, older, fine);
            await InsertAsync(connection, ct, older, new Seed("dropped", CheckDbAgo: 3 * day, FullAgo: day, LogAgo: TimeSpan.FromMinutes(10)));
            await InsertAsync(connection, ct, at, fine);
            await InsertAsync(connection, ct, at, new Seed("old_full", CheckDbAgo: 3 * day, FullAgo: 9 * day, LogAgo: TimeSpan.FromMinutes(10)));
            await InsertAsync(connection, ct, at, new Seed("never_db", CheckDbState: "never", FullState: "none_in_window", LogState: "none_in_window"));
            await InsertAsync(connection, ct, at, new Seed("new_db", CreatedAgo: TimeSpan.FromMinutes(40), CheckDbState: "never", FullState: "none_in_window", LogState: "none_in_window"));
            await InsertAsync(connection, ct, at, new Seed("simple_db", Recovery: "SIMPLE", CheckDbAgo: 3 * day, FullAgo: day, LogState: "not_expected"));
            await InsertAsync(connection, ct, at, new Seed("off_db", State: "OFFLINE", FullState: "none_in_window", CheckDbState: "never", LogState: "none_in_window"));
            await InsertAsync(connection, ct, at, new Seed("sec_db", Role: "SECONDARY", FullAgo: 30 * day, CheckDbAgo: 90 * day, LogAgo: 2 * day));
        }, async (postgres, ct) =>
        {
            var root = Parse(await Call(postgres, ct));
            var dbs = ByName(root);

            Assert.False(dbs.ContainsKey("dropped"));
            Assert.Equal(7, dbs.Count);
            Assert.Equal("ok", dbs["fine"].GetProperty("full_backup").GetProperty("state").GetString());
            Assert.Equal(1440, dbs["fine"].GetProperty("full_backup").GetProperty("age_minutes").GetInt64());

            var oldFull = dbs["old_full"].GetProperty("full_backup");
            Assert.Equal("overdue", oldFull.GetProperty("state").GetString());
            Assert.True(oldFull.GetProperty("overdue").GetBoolean());
            Assert.Equal("9 days", oldFull.GetProperty("age").GetString());
            Assert.Equal("ok", dbs["old_full"].GetProperty("checkdb").GetProperty("state").GetString());

            Assert.Equal("never", dbs["never_db"].GetProperty("checkdb").GetProperty("state").GetString());
            Assert.Equal("none_in_window", dbs["never_db"].GetProperty("full_backup").GetProperty("state").GetString());
            Assert.Equal("none_in_window", dbs["never_db"].GetProperty("log_backup").GetProperty("state").GetString());

            foreach (var fact in new[] { "checkdb", "full_backup", "log_backup" })
            {
                Assert.Equal("new_database", dbs["new_db"].GetProperty(fact).GetProperty("state").GetString());
                Assert.False(dbs["new_db"].GetProperty(fact).GetProperty("overdue").GetBoolean());
            }

            Assert.Equal("not_expected", dbs["simple_db"].GetProperty("log_backup").GetProperty("state").GetString());
            Assert.Equal("excluded", dbs["off_db"].GetProperty("full_backup").GetProperty("state").GetString());
            Assert.Equal("offline", dbs["off_db"].GetProperty("excluded_reason").GetString());
            Assert.Equal("excluded", dbs["sec_db"].GetProperty("checkdb").GetProperty("state").GetString());
            Assert.Equal("secondary", dbs["sec_db"].GetProperty("excluded_reason").GetString());

            var summary = root.GetProperty("summary");
            Assert.Equal(7, summary.GetProperty("databases").GetInt32());
            Assert.Equal(5, summary.GetProperty("assessed").GetInt32());
            Assert.Equal(2, summary.GetProperty("excluded").GetInt32());
            Assert.Equal(2, summary.GetProperty("overdue_databases").GetInt32());
            Assert.Equal(["never_db", "old_full"], Names(summary.GetProperty("overdue_full_backup")));
            Assert.Equal(["never_db"], Names(summary.GetProperty("overdue_log_backup")));
            Assert.Equal(["never_db"], Names(summary.GetProperty("overdue_checkdb")));
            return 0;
        });
    }

    [Fact]
    public async Task OverdueOnly_ListsJustTheDatabasesOverdueOnSomething()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            var at = DateTime.UtcNow.AddMinutes(-5);
            var day = TimeSpan.FromDays(1);
            await InsertAsync(connection, ct, at, new Seed("fine", CheckDbAgo: 3 * day, FullAgo: day, LogAgo: TimeSpan.FromMinutes(10)));
            await InsertAsync(connection, ct, at, new Seed("late_log", CheckDbAgo: 3 * day, FullAgo: day, LogAgo: TimeSpan.FromHours(3)));
        }, async (postgres, ct) =>
        {
            var root = Parse(await Call(postgres, ct, overdueOnly: true));

            Assert.Equal(["late_log"], ByName(root).Keys.ToArray());
            Assert.Equal(1, root.GetProperty("databases_listed").GetInt32());
            Assert.Equal(2, root.GetProperty("summary").GetProperty("databases").GetInt32());
            return 0;
        });
    }

    [Fact]
    public async Task AManagedPlatform_ShowsTheAge_AndOnlyRdsCheckDbIsJudged()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            var at = DateTime.UtcNow.AddMinutes(-5);
            var day = TimeSpan.FromDays(1);
            await InsertAsync(connection, ct, at, new Seed("managed_db", Platform: "platform_managed", CheckDbState: "never", FullAgo: 20 * day, LogAgo: 3 * day));
            await InsertAsync(connection, ct, at, new Seed("rds_db", Platform: "native_only", CheckDbAgo: 60 * day, FullAgo: 20 * day, LogAgo: 3 * day));
            await InsertAsync(connection, ct, at, new Seed("rds_fresh_db", Platform: "native_only", CheckDbAgo: 1 * day, FullAgo: 20 * day, LogAgo: 3 * day));
            await InsertAsync(connection, ct, at, new Seed("hyperscale_db", Platform: "platform_managed", CheckDbState: "never", FullState: "unavailable", LogState: "unavailable"));
        }, async (postgres, ct) =>
        {
            var root = Parse(await Call(postgres, ct));
            var dbs = ByName(root);

            Assert.Equal("platform_managed", dbs["managed_db"].GetProperty("full_backup").GetProperty("state").GetString());
            Assert.Equal(20 * 1440, dbs["managed_db"].GetProperty("full_backup").GetProperty("age_minutes").GetInt64());
            Assert.Equal("platform_managed", dbs["managed_db"].GetProperty("checkdb").GetProperty("state").GetString());
            // #6902: RDS backups are not judged, its CHECKDB is: 60 days old is overdue, 1 day old is ok.
            Assert.Equal("overdue", dbs["rds_db"].GetProperty("checkdb").GetProperty("state").GetString());
            Assert.Equal("native_only", dbs["rds_db"].GetProperty("full_backup").GetProperty("state").GetString());
            Assert.Equal("native_only", dbs["rds_db"].GetProperty("log_backup").GetProperty("state").GetString());
            Assert.Equal("ok", dbs["rds_fresh_db"].GetProperty("checkdb").GetProperty("state").GetString());
            Assert.Equal("unavailable", dbs["hyperscale_db"].GetProperty("full_backup").GetProperty("state").GetString());
            Assert.Equal(JsonValueKind.Null, dbs["hyperscale_db"].GetProperty("full_backup").GetProperty("age_minutes").ValueKind);
            Assert.Equal(1, root.GetProperty("summary").GetProperty("overdue_databases").GetInt32());
            return 0;
        });
    }

    [Fact]
    public async Task TheCollectorsOwnWords_PassThroughAsTheirOwn()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            await InsertAsync(connection, ct, DateTime.UtcNow.AddMinutes(-5),
                new Seed("old_build", CheckDbState: "unknown", FullAgo: TimeSpan.FromDays(1), LogState: "unavailable"));
        }, async (postgres, ct) =>
        {
            var db = ByName(Parse(await Call(postgres, ct)))["old_build"];

            Assert.Equal("unknown", db.GetProperty("checkdb").GetProperty("state").GetString());
            Assert.Equal("unavailable", db.GetProperty("log_backup").GetProperty("state").GetString());
            Assert.False(db.GetProperty("checkdb").GetProperty("overdue").GetBoolean());
            return 0;
        });
    }

    [Theory]
    [InlineData(null)]
    [InlineData(5)]  // Azure SQL Database runs this collector too, so a miss there is a missing capture, not an engine gap
    public async Task NoCapture_ReadsUnavailable(int? edition)
    {
        await WithStoreAsync(
            (connection, ct) => edition is null
                ? Task.CompletedTask
                : DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET sql_engine_edition = $1 WHERE server_id = $2", edition.Value, ServerId),
            async (postgres, ct) =>
            {
                var root = Parse(await Call(postgres, ct));

                Assert.Equal("unavailable", root.GetProperty("status").GetString());
                Assert.Contains("database_protection", root.GetProperty("message").GetString(), StringComparison.Ordinal);
                return 0;
            });
    }
}
