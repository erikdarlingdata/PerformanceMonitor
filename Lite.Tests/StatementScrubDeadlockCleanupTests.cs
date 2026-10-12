using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4348 (statement filter, collection of blocking and deadlocks): a deadlock graph
/// that the filter withheld WHOLE is stored as the marker text, the same for every such event. Two different
/// deadlocks at the same time would then look like exact copies of each other, so neither the startup cleanup that
/// deletes exact copies nor the read that shows each stored event once may treat a marker graph as a copy. A real
/// copy still goes.
/// </summary>
public class StatementScrubDeadlockCleanupTests : IDisposable
{
    private const int Server = -445301;
    private static readonly DateTime EventTime = new(2026, 3, 10, 9, 30, 0);
    private const string Plain = "<deadlock><victim-list><victimProcess id=\"process1\"/></victim-list></deadlock>";
    private const string Marker = SensitiveStatements.PlaceholderText;

    private readonly string _tempDir;
    private readonly string _dbPath;

    public StatementScrubDeadlockCleanupTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
    }

    public void Dispose()
    {
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
    public async Task TwoWholeMarkerDeadlocksAtTheSameTime_AreBothKept_ByTheCleanup_AndBothShownByTheRead_WhileARealCopyStillGoes()
    {
        using (var initializer = new DuckDbInitializer(_dbPath))
        {
            await initializer.InitializeAsync();
        }

        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync();

        /* Two different deadlocks at one time, both withheld whole; and one real copy pair as the control. */
        await InsertAsync(connection, 1, EventTime.AddMinutes(1), Marker);
        await InsertAsync(connection, 2, EventTime.AddMinutes(2), Marker);
        await InsertAsync(connection, 3, EventTime.AddMinutes(3), Plain);
        await InsertAsync(connection, 4, EventTime.AddMinutes(4), Plain);

        /* The read collapses copies but keeps both marker events. */
        var shown = await IdsAsync(connection,
            "SELECT deadlock_id FROM " + StoredEventCopies.Deadlocks("server_id = " + Server) + " AS d ORDER BY 1");
        Assert.Equal(new List<long> { 1, 2, 3 }, shown);

        var removed = await DeadlockDuplicateCleanup.RemoveAsync(connection, logger: null);

        Assert.Equal(1, removed);
        Assert.Equal(new List<long> { 1, 2, 3 }, await IdsAsync(connection, "SELECT deadlock_id FROM deadlocks ORDER BY 1"));
    }

    private static async Task InsertAsync(DuckDBConnection connection, long id, DateTime collectionTime, string graph)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml) VALUES ($1,$2,$3,$4,$5,$6)";
        cmd.Parameters.Add(new DuckDBParameter { Value = id });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = Server });
        cmd.Parameters.Add(new DuckDBParameter { Value = "S" + Server });
        cmd.Parameters.Add(new DuckDBParameter { Value = EventTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = graph });
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<List<long>> IdsAsync(DuckDBConnection connection, string sql)
    {
        var ids = new List<long>();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            ids.Add(Convert.ToInt64(reader.GetValue(0)));
        return ids;
    }
}
