using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The database-state reads in <see cref="LocalDataService"/> after the 512 MB archive-and-reset has moved every
/// hot row to Parquet, and the cases where the store cannot judge at all.
///
/// <para>Two rules are pinned here. First, the editor, "reset to current" and every statement of the alert sweep
/// read the same source, so a server whose rows live only in the archive behaves like one whose rows are hot.
/// Second, the sweep gives no verdict (null) when the source holds fewer than two snapshots, or when its newest
/// snapshot is older than <see cref="ArchiveService.HotDataDays"/>. No verdict changes no state and prunes
/// nothing, except that a fresh first snapshot may still seed missing baselines. These pins run the REAL
/// <see cref="ArchiveService.ArchiveAllAndResetAsync"/>.</para>
/// </summary>
/* ArchiveAllAndResetAsync touches the process-wide reset state and ArchiveService's static archive lock, so this
   class joins the serialized collection the other reset tests use. */
[Collection("CollectionResetGate")]
public sealed class DatabaseStateNoVerdictTests : IDisposable
{
    private const int ServerId = 4917;
    private static readonly DateTime T1 = DateTime.SpecifyKind(DateTime.UtcNow.AddHours(-1), DateTimeKind.Unspecified);
    private static readonly DateTime T2 = T1.AddMinutes(1);
    private static readonly DateTime T3 = T1.AddMinutes(2);

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = 1;

    public DatabaseStateNoVerdictTests()
    {
        CollectionResetGate.ResetForTests();
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
        _duckDb = new DuckDbInitializer(_dbPath);
    }

    public void Dispose()
    {
        _duckDb.Dispose();
        CollectionResetGate.ResetForTests();
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    private static string Ts(DateTime t) => $"TIMESTAMP '{t:yyyy-MM-dd HH:mm:ss}'";

    private string Snap(DateTime when, string database, string state) =>
        $"INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ({_nextId++}, {Ts(when)}, {ServerId}, 'S', '{database}', 5, '{state}', false)";

    private static string Baseline(string database, string expected, string? alerted = null) =>
        $"INSERT INTO config_database_state_expected (server_id, database_name, expected_state, is_user_override, updated_at, last_alerted_state, last_alerted_at) VALUES ({ServerId}, '{database}', '{expected}', false, {Ts(T1)}, {(alerted is null ? "NULL" : $"'{alerted}'")}, {(alerted is null ? "NULL" : Ts(T1))})";

    private static string Override(string database, string expected) =>
        $"INSERT INTO config_database_state_expected (server_id, database_name, expected_state, is_user_override, updated_at) VALUES ({ServerId}, '{database}', '{expected}', true, {Ts(T1)})";

    private async Task ExecAsync(params string[] statements)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        foreach (var sql in statements)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    private async Task SeedAsync(params string[] statements)
    {
        await _duckDb.InitializeAsync();
        await ExecAsync(statements);
    }

    private Task ResetAsync() => new ArchiveService(_duckDb, _archiveDir, NullLogger<ArchiveService>.Instance).ArchiveAllAndResetAsync();

    private async Task<string?> TextAsync(string sql)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        var value = await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        return value is null or DBNull ? null : Convert.ToString(value);
    }

    private async Task<long> CountAsync(string sql) => Convert.ToInt64(await TextAsync(sql));

    private Task<string?> ExpectedAsync(string database) =>
        TextAsync($"SELECT expected_state FROM config_database_state_expected WHERE server_id = {ServerId} AND database_name = '{database}'");

    private Task<string?> AlertedAsync(string database) =>
        TextAsync($"SELECT last_alerted_state FROM config_database_state_expected WHERE server_id = {ServerId} AND database_name = '{database}'");

    private Task<long> BaselineRowsAsync(string database) =>
        CountAsync($"SELECT COUNT(*) FROM config_database_state_expected WHERE server_id = {ServerId} AND database_name = '{database}'");

    private Task<long> HotCountAsync() => CountAsync($"SELECT COUNT(*) FROM database_states WHERE server_id = {ServerId}");

    private Task<long> ViewCountAsync() => CountAsync($"SELECT COUNT(*) FROM v_database_states WHERE server_id = {ServerId}");

    private Task<List<DatabaseStateInfo>?> SweepAsync() => new LocalDataService(_duckDb).GetDatabaseStateDeviationsAsync(ServerId);

    /// <summary>
    /// Two snapshots, the newest <paramref name="age"/> old, both moved to the archive by the real reset. The hot
    /// table is empty for the server and the baselines are in place: one database that deviates and was announced,
    /// one that is as expected, one that is no longer in the snapshots.
    /// </summary>
    private async Task<List<DatabaseStateInfo>?> SweepTwoArchivedSnapshotsAsync(TimeSpan age)
    {
        var newest = DateTime.SpecifyKind(DateTime.UtcNow - age, DateTimeKind.Unspecified);
        var older = newest.AddMinutes(-1);
        await SeedAsync(
            Snap(older, "DbA", "OFFLINE"), Snap(older, "DbB", "ONLINE"),
            Snap(newest, "DbA", "OFFLINE"), Snap(newest, "DbB", "ONLINE"),
            Baseline("DbA", "ONLINE", alerted: "OFFLINE"), Baseline("DbB", "ONLINE"), Baseline("DbGone", "ONLINE"));
        await ResetAsync();
        Assert.Equal(0, await HotCountAsync());

        return await SweepAsync();
    }

    /* ---- The editor and "reset to current" read the archive when the hot table is empty ---- */

    [Fact]
    public async Task AfterArchiveReset_TheEditorListsTheDatabasesOfTheNewestArchivedSnapshot()
    {
        await SeedAsync(
            Snap(T1, "DbA", "ONLINE"), Snap(T1, "DbGone", "ONLINE"),
            Snap(T2, "DbA", "OFFLINE"), Snap(T2, "DbB", "ONLINE"),
            Baseline("DbA", "ONLINE"), Override("DbB", DatabaseStateTokens.Ignore));
        await ResetAsync();
        Assert.Equal(0, await HotCountAsync());

        var rows = await new LocalDataService(_duckDb).GetDatabaseStateExpectationsAsync(ServerId);

        Assert.Equal(new[] { "DbA", "DbB" }, rows.Select(r => r.DatabaseName).ToArray());
        Assert.Equal("OFFLINE", rows[0].CurrentState);
        Assert.Equal("ONLINE", rows[0].ExpectedState);
        Assert.False(rows[0].IsUserOverride);
        Assert.Equal("ONLINE", rows[1].CurrentState);
        Assert.Equal(DatabaseStateTokens.Ignore, rows[1].ExpectedState);
        Assert.True(rows[1].IsUserOverride);
    }

    [Fact]
    public async Task AfterArchiveReset_ReBaseliningUsesTheNewestArchivedSnapshot()
    {
        await SeedAsync(
            Snap(T1, "DbA", "OFFLINE"), Snap(T2, "DbA", "OFFLINE"),
            Override("DbA", "ONLINE"));
        await ResetAsync();
        Assert.Equal(0, await HotCountAsync());

        await new LocalDataService(_duckDb).ResetDatabaseStateExpectedToCurrentAsync(ServerId, "DbA");

        Assert.Equal("OFFLINE", await ExpectedAsync("DbA"));
        Assert.Equal(0, await CountAsync($"SELECT COUNT(*) FROM config_database_state_expected WHERE server_id = {ServerId} AND database_name = 'DbA' AND is_user_override = true"));
    }

    /* ---- No verdict: fewer than two snapshots ---- */

    [Fact]
    public async Task NoSnapshotsAnywhere_GiveNoVerdict_AndChangeNothing()
    {
        await SeedAsync(Baseline("DbA", "ONLINE", alerted: "OFFLINE"), Override("DbB", "OFFLINE"));
        Assert.Equal(0, await HotCountAsync());
        Assert.Equal(0, await ViewCountAsync());

        var deviations = await SweepAsync();

        Assert.Null(deviations);
        Assert.Equal("ONLINE", await ExpectedAsync("DbA"));
        Assert.Equal("OFFLINE", await AlertedAsync("DbA"));
        Assert.Equal("OFFLINE", await ExpectedAsync("DbB"));
    }

    [Fact]
    public async Task OneSnapshotAndNoArchive_GiveNoVerdict_OnlySeed_AndChangeNothingElse()
    {
        await SeedAsync(
            Snap(T3, "DbNew", "ONLINE"), Snap(T3, "DbHeal", "ONLINE"), Snap(T3, "DbForget", "ONLINE"),
            Baseline("DbHeal", "RESTORING"), Baseline("DbForget", "ONLINE", alerted: "OFFLINE"), Baseline("DbGone", "ONLINE"));
        Assert.Equal(3, await HotCountAsync());
        Assert.Equal(3, await ViewCountAsync());

        var deviations = await SweepAsync();

        Assert.Null(deviations);
        Assert.Equal("ONLINE", await ExpectedAsync("DbNew"));
        Assert.Equal("RESTORING", await ExpectedAsync("DbHeal"));
        Assert.Equal("OFFLINE", await AlertedAsync("DbForget"));
        Assert.Equal(1, await BaselineRowsAsync("DbGone"));
    }

    /* ---- No verdict: the newest snapshot is older than the archival age ---- */

    [Fact]
    public async Task ArchivedRowsOlderThanTheArchivalAge_GiveNoVerdict_AndChangeNothing()
    {
        var deviations = await SweepTwoArchivedSnapshotsAsync(TimeSpan.FromDays(ArchiveService.HotDataDays + 1));

        Assert.Null(deviations);
        Assert.Equal(2, await CountAsync($"SELECT COUNT(*) FROM config_database_state_expected WHERE server_id = {ServerId} AND is_user_override = false AND expected_state = 'ONLINE' AND database_name IN ('DbA', 'DbB')"));
        Assert.Equal("OFFLINE", await AlertedAsync("DbA"));
        Assert.Equal(1, await BaselineRowsAsync("DbGone"));
    }

    [Fact]
    public async Task ArchivedRowsOneDayOld_StillAlert()
    {
        var deviations = await SweepTwoArchivedSnapshotsAsync(TimeSpan.FromDays(1));

        Assert.NotNull(deviations);
        var deviation = Assert.Single(deviations);
        Assert.Equal("DbA", deviation.DatabaseName);
        Assert.Equal("OFFLINE", deviation.StateDesc);
        Assert.Equal("ONLINE", deviation.ExpectedState);
    }

    /* ---- Each maintenance statement of the sweep reads the archive when the hot table is empty ---- */

    [Fact]
    public async Task ArchiveOnly_TheSeedReadsTheArchive()
    {
        await SeedAsync(Snap(T1, "DbNew", "ONLINE"), Snap(T2, "DbNew", "ONLINE"));
        await ResetAsync();
        Assert.Equal(0, await HotCountAsync());

        Assert.NotNull(await SweepAsync());

        Assert.Equal("ONLINE", await ExpectedAsync("DbNew"));
    }

    [Fact]
    public async Task ArchiveOnly_TheHealReadsTheArchive()
    {
        await SeedAsync(Snap(T1, "DbHeal", "ONLINE"), Snap(T2, "DbHeal", "ONLINE"), Baseline("DbHeal", "RESTORING"));
        await ResetAsync();
        Assert.Equal(0, await HotCountAsync());

        Assert.NotNull(await SweepAsync());

        Assert.Equal("ONLINE", await ExpectedAsync("DbHeal"));
    }

    [Fact]
    public async Task ArchiveOnly_TheForgetOfARecoveredDatabaseReadsTheArchive()
    {
        await SeedAsync(Snap(T1, "DbForget", "ONLINE"), Snap(T2, "DbForget", "ONLINE"), Baseline("DbForget", "ONLINE", alerted: "OFFLINE"));
        await ResetAsync();
        Assert.Equal(0, await HotCountAsync());

        Assert.NotNull(await SweepAsync());

        Assert.Null(await AlertedAsync("DbForget"));
    }

    [Fact]
    public async Task ArchiveOnly_ThePruneOfADroppedDatabaseReadsTheArchive()
    {
        await SeedAsync(Snap(T1, "DbA", "ONLINE"), Snap(T2, "DbA", "ONLINE"), Baseline("DbA", "ONLINE"), Baseline("DbGone", "ONLINE"));
        await ResetAsync();
        Assert.Equal(0, await HotCountAsync());

        Assert.NotNull(await SweepAsync());

        Assert.Equal(1, await BaselineRowsAsync("DbA"));
        Assert.Equal(0, await BaselineRowsAsync("DbGone"));
    }
}
