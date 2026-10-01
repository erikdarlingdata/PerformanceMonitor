using System;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The alert sweep (<see cref="LocalDataService.GetDatabaseStateDeviationsAsync"/>) must give the same answer
/// whether the two newest database_states snapshots are in the hot table or in the Parquet archive.
///
/// <para>The 512 MB archive-and-reset moves every hot row to Parquet and leaves the hot table empty for the
/// server until two collection cycles land. The sweep used to read only the hot table: the prune then deleted
/// every auto-baseline (the config table survives the reset, then the first sweep wiped it), and the deviation
/// read returned nothing, which the alert path reads as "every database recovered". These pins run the REAL
/// <see cref="ArchiveService.ArchiveAllAndResetAsync"/> and then one real sweep.</para>
/// </summary>
/* ArchiveAllAndResetAsync touches CollectionResetGate and ArchiveService's static archive lock, both
   process-wide, so this class joins the serialized collection the other reset tests use. */
[Collection("CollectionResetGate")]
public sealed class DatabaseStateSweepAfterArchiveTests : IDisposable
{
    private const int ServerId = 4887;
    private static readonly DateTime T1 = DateTime.SpecifyKind(DateTime.UtcNow.AddHours(-1), DateTimeKind.Unspecified);
    private static readonly DateTime T2 = T1.AddMinutes(1);
    private static readonly DateTime T3 = T1.AddMinutes(2);

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = 1;

    public DatabaseStateSweepAfterArchiveTests()
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

    private async Task<long> CountAsync(string sql)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    private Task<long> AutoBaselinesAsync() => CountAsync(
        $"SELECT COUNT(*) FROM config_database_state_expected WHERE server_id = {ServerId} AND is_user_override = false AND expected_state = 'ONLINE' AND database_name IN ('DbA', 'DbB')");

    /// <summary>
    /// The main pin. Two snapshots are archived by the real reset and the hot table is empty for the server.
    /// One sweep must keep both auto-baselines and still report the database that deviates in both snapshots,
    /// with its announced-state memory intact.
    /// </summary>
    [Fact]
    public async Task AfterArchiveReset_TheSweepKeepsBaselines_AndStillReportsTheDeviation()
    {
        await SeedAsync(
            Snap(T1, "DbA", "OFFLINE"), Snap(T1, "DbB", "ONLINE"),
            Snap(T2, "DbA", "OFFLINE"), Snap(T2, "DbB", "ONLINE"),
            Baseline("DbA", "ONLINE", alerted: "OFFLINE"), Baseline("DbB", "ONLINE"));
        await ResetAsync();

        Assert.Equal(0, await CountAsync($"SELECT COUNT(*) FROM database_states WHERE server_id = {ServerId}"));
        Assert.Equal(4, await CountAsync($"SELECT COUNT(*) FROM v_database_states WHERE server_id = {ServerId}"));
        Assert.Equal(2, await AutoBaselinesAsync());

        var deviations = await new LocalDataService(_duckDb).GetDatabaseStateDeviationsAsync(ServerId);

        Assert.NotNull(deviations);
        var deviation = Assert.Single(deviations);
        Assert.Equal("DbA", deviation.DatabaseName);
        Assert.Equal("OFFLINE", deviation.StateDesc);
        Assert.Equal("ONLINE", deviation.ExpectedState);
        Assert.Equal("OFFLINE", deviation.LastAlertedState);
        Assert.Equal(2, await AutoBaselinesAsync());
    }

    /// <summary>
    /// One new hot snapshot after the reset, older ones archived: the same answer as the main pin. A dropped
    /// database's auto-baseline is still tidied against the real newest snapshot.
    /// </summary>
    [Fact]
    public async Task OneHotSnapshotPlusArchivedOlderOnes_ReportsTheSameDeviation()
    {
        await SeedAsync(
            Snap(T1, "DbA", "OFFLINE"), Snap(T1, "DbB", "ONLINE"),
            Snap(T2, "DbA", "OFFLINE"), Snap(T2, "DbB", "ONLINE"),
            Baseline("DbA", "ONLINE", alerted: "OFFLINE"), Baseline("DbB", "ONLINE"), Baseline("DbGone", "ONLINE"));
        await ResetAsync();
        await ExecAsync(Snap(T3, "DbA", "OFFLINE"), Snap(T3, "DbB", "ONLINE"));

        var deviations = await new LocalDataService(_duckDb).GetDatabaseStateDeviationsAsync(ServerId);

        Assert.NotNull(deviations);
        var deviation = Assert.Single(deviations);
        Assert.Equal("DbA", deviation.DatabaseName);
        Assert.Equal("OFFLINE", deviation.StateDesc);
        Assert.Equal("ONLINE", deviation.ExpectedState);
        Assert.Equal("OFFLINE", deviation.LastAlertedState);
        Assert.Equal(2, await AutoBaselinesAsync());
        Assert.Equal(0, await CountAsync($"SELECT COUNT(*) FROM config_database_state_expected WHERE server_id = {ServerId} AND database_name = 'DbGone'"));
    }

    /// <summary>
    /// The hot table is empty and the archive holds only one snapshot, so nothing can be judged yet. The sweep
    /// reports nothing and must not prune: an empty hot table is missing data, not a list of dropped databases.
    /// </summary>
    [Fact]
    public async Task EmptyHotTableAndOneArchivedSnapshot_PrunesNothing()
    {
        await SeedAsync(
            Snap(T1, "DbA", "OFFLINE"), Snap(T1, "DbB", "ONLINE"),
            Baseline("DbA", "ONLINE"), Baseline("DbB", "ONLINE"));
        await ResetAsync();
        Assert.Equal(2, await AutoBaselinesAsync());

        var deviations = await new LocalDataService(_duckDb).GetDatabaseStateDeviationsAsync(ServerId);

        Assert.Null(deviations);
        Assert.Equal(2, await AutoBaselinesAsync());
    }
}
