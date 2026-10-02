using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The two alert-state writes update a surviving key in place rather than deleting it and inserting it
/// again in one transaction, and they still store exactly what the delete-and-insert stored.
///
/// <para>In place is read from DuckDB's <c>rowid</c>: an update that writes no indexed column keeps the row's
/// rowid, while a delete followed by an insert appends a new row with a new one. Rowids are comparable only
/// while nothing checkpoints, since a checkpoint can vacuum deleted rows and renumber the rest. So each test
/// owns its database file, holds a connection open across the saves so the database never closes (the
/// connection string turns automatic checkpoints off), and checks that the WAL only grew between the two
/// reads, as it would not if a checkpoint had emptied it.</para>
/// </summary>
public sealed class AlertStateUpsertTests : IDisposable
{
    private static readonly DateTime T0 = new(2026, 9, 1, 10, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime T1 = new(2026, 9, 1, 10, 5, 0, DateTimeKind.Utc);
    private static readonly DateTime T2 = new(2026, 9, 1, 10, 10, 0, DateTimeKind.Utc);

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckDbInitializer _duckDb;

    public AlertStateUpsertTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _duckDb = new DuckDbInitializer(_dbPath);
    }

    public void Dispose()
    {
        _duckDb.Dispose();
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

    [Fact]
    public async Task PersistenceGateSave_InsertsANewKey_AndUpdatesAnExistingKeyInPlace()
    {
        await _duckDb.InitializeAsync();
        var store = new DuckDbAlertHistoryStore(_duckDb);
        using var held = _duckDb.CreateConnection();
        await held.OpenAsync();

        /* Other keys first, so the WAL holds far more than one save and a checkpoint between the reads
           could not hide behind its growth. */
        for (var i = 0; i < 20; i++)
        {
            await store.SaveAlertPersistenceAsync(2, "Neighbour " + i, 1, 0, false, null);
        }
        await store.SaveAlertPersistenceAsync(1, "Memory", 1, 0, false, null);
        await store.SaveAlertPersistenceAsync(1, "High CPU", 2, 0, false, T0);

        var first = await ReadPersistenceAsync(held, 1, "High CPU");
        Assert.Equal((2, 0, false, (DateTime?)T0), (first.Breaches, first.Clears, first.Firing, first.Sample));
        var walAfterFirst = WalLength();

        await Task.Delay(20);
        await store.SaveAlertPersistenceAsync(1, "High CPU", 5, 3, true, T1);

        Assert.True(WalLength() > walAfterFirst, "A checkpoint ran between the saves, so their rowids are not comparable.");
        var second = await ReadPersistenceAsync(held, 1, "High CPU");
        Assert.Equal((5, 3, true, (DateTime?)T1), (second.Breaches, second.Clears, second.Firing, second.Sample));
        Assert.True(second.UpdatedAt > first.UpdatedAt, "updated_at was not rewritten.");
        Assert.Equal(first.RowId, second.RowId);

        /* A null sample replaces a stored one, as the whole-row insert did. */
        await store.SaveAlertPersistenceAsync(1, "High CPU", 0, 4, false, null);
        var third = await ReadPersistenceAsync(held, 1, "High CPU");
        Assert.Equal((0, 4, false, (DateTime?)null), (third.Breaches, third.Clears, third.Firing, third.Sample));
        Assert.Equal(first.RowId, third.RowId);

        Assert.Equal(1L, await ScalarAsync(held,
            "SELECT COUNT(*) FROM config_alert_persistence_state WHERE server_id = 1 AND metric_name = 'High CPU'"));
        var neighbour = await ReadPersistenceAsync(held, 1, "Memory");
        Assert.Equal((1, 0, false, (DateTime?)null), (neighbour.Breaches, neighbour.Clears, neighbour.Firing, neighbour.Sample));
        Assert.Equal(21L, await ScalarAsync(held, "SELECT COUNT(*) FROM config_alert_persistence_state WHERE metric_name <> 'High CPU'"));
    }

    [Fact]
    public async Task IncidentOccurrencesSave_ReplacesTheSet_AndKeepsASurvivingKeyInPlace()
    {
        await _duckDb.InitializeAsync();
        var store = new DuckDbAlertHistoryStore(_duckDb);
        using var held = _duckDb.CreateConnection();
        await held.OpenAsync();

        /* Another metric and another server: no save below may touch them. */
        for (var i = 0; i < 20; i++)
        {
            await store.SaveIncidentOccurrencesAsync(3, "Neighbour " + i, new[] { ("x", 1L, 1, T0, T1) });
        }
        await store.SaveIncidentOccurrencesAsync(1, "Blocking", new[] { ("a", 1L, 1, T0, T1) });
        await store.SaveIncidentOccurrencesAsync(2, "Deadlocks", new[] { ("a", 1L, 1, T0, T1) });
        await store.SaveIncidentOccurrencesAsync(1, "Deadlocks", new[] { ("a", 4L, 2, T0, T1), ("b", 7L, 3, T0, T1) });

        var first = await ReadOccurrencesAsync(held, 1, "Deadlocks");
        Assert.Equal(new[] { "a", "b" }, first.Keys);
        var walAfterFirst = WalLength();

        /* A different set: "a" ends, "b" continues with new numbers, "c" starts. */
        await store.SaveIncidentOccurrencesAsync(1, "Deadlocks", new[] { ("b", 9L, 4, T0, T2), ("c", 1L, 1, T2, T2) });

        Assert.True(WalLength() > walAfterFirst, "A checkpoint ran between the saves, so their rowids are not comparable.");
        var second = await ReadOccurrencesAsync(held, 1, "Deadlocks");
        Assert.Equal(new[] { "b", "c" }, second.Keys);
        Assert.Equal((9L, 4, T0, T2), second["b"].Values);
        Assert.Equal((1L, 1, T2, T2), second["c"].Values);
        Assert.Equal(first["b"].RowId, second["b"].RowId);

        /* An empty set clears the metric. */
        await store.SaveIncidentOccurrencesAsync(
            1, "Deadlocks", Array.Empty<(string, long, int, DateTime, DateTime)>());
        Assert.Empty(await ReadOccurrencesAsync(held, 1, "Deadlocks"));

        Assert.Equal((1L, 1, T0, T1), (await ReadOccurrencesAsync(held, 1, "Blocking"))["a"].Values);
        Assert.Equal((1L, 1, T0, T1), (await ReadOccurrencesAsync(held, 2, "Deadlocks"))["a"].Values);
        Assert.Equal(20L, await ScalarAsync(held, "SELECT COUNT(*) FROM config_incident_occurrences WHERE server_id = 3"));
    }

    private long WalLength()
    {
        var wal = new FileInfo(_dbPath + ".wal");
        return wal.Exists ? wal.Length : 0;
    }

    private static async Task<long> ScalarAsync(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync());
    }

    private static async Task<(int Breaches, int Clears, bool Firing, DateTime? Sample, DateTime UpdatedAt, long RowId)>
        ReadPersistenceAsync(DuckDBConnection connection, int serverId, string metricName)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT consecutive_breaches, consecutive_clears, firing, last_observed_sample_at, updated_at, rowid
FROM config_alert_persistence_state
WHERE server_id = $1
AND   metric_name = $2";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = metricName });
        using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync(), $"No persistence row for ({serverId}, {metricName}).");
        return (
            Convert.ToInt32(reader.GetValue(0)),
            Convert.ToInt32(reader.GetValue(1)),
            reader.GetBoolean(2),
            reader.IsDBNull(3) ? null : Convert.ToDateTime(reader.GetValue(3)),
            Convert.ToDateTime(reader.GetValue(4)),
            Convert.ToInt64(reader.GetValue(5)));
    }

    private static async Task<SortedDictionary<string, ((long Total, int Window, DateTime Started, DateTime Last) Values, long RowId)>>
        ReadOccurrencesAsync(DuckDBConnection connection, int serverId, string metricName)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT dedup_key, total_occurrences, observed_window_count, incident_started_at, last_observed_at, rowid
FROM config_incident_occurrences
WHERE server_id = $1
AND   metric_name = $2";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = metricName });
        var rows = new SortedDictionary<string, ((long, int, DateTime, DateTime), long)>(StringComparer.Ordinal);
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(reader.GetString(0), (
                (Convert.ToInt64(reader.GetValue(1)), Convert.ToInt32(reader.GetValue(2)),
                 Convert.ToDateTime(reader.GetValue(3)), Convert.ToDateTime(reader.GetValue(4))),
                Convert.ToInt64(reader.GetValue(5))));
        }
        return rows;
    }
}
