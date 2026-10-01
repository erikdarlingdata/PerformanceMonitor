using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The startup cleanup that removes EXACT duplicate rows already stored in the hot <c>deadlocks</c> table (one
/// Azure deadlock stored twice, once from a database's own session and once from the server's telemetry). It is
/// a DELETE path, so the pins are written against data loss first: only a row whose server, <c>deadlock_time</c>
/// and full graph text all match an earlier row goes; the earliest <c>collection_time</c> stays; a NULL or empty
/// graph, a graph that differs by one character, a NULL time and the same graph on another server are never
/// touched; a second start removes nothing.
///
/// <para>The rows are seeded AFTER the first initialisation and the file is initialised again, because the
/// cleanup runs on every start. Rows already moved into archived Parquet behind the <c>v_deadlocks</c> view
/// cannot be deleted from here, so only the hot table is cleaned.</para>
/// </summary>
public class DeadlockDuplicateCleanupTests : IDisposable
{
    private const int ServerA = -445201;
    private const int ServerB = -445202;

    private static readonly DateTime Day1 = new(2026, 3, 10, 0, 0, 0);
    private static readonly DateTime Day2 = Day1.AddDays(1);
    private static readonly DateTime EventOne = Day1.AddHours(9).AddMinutes(30);
    private static readonly DateTime EventTwo = Day1.AddHours(11);
    private static readonly DateTime EventThree = Day1.AddHours(23).AddMinutes(57);
    private static readonly DateTime EventNull = Day1.AddHours(13);

    private const string GraphOne = "<deadlock><victim-list><victimProcess id=\"process1\"/></victim-list></deadlock>";
    private const string GraphOneOffByOne = "<deadlock><victim-list><victimProcess id=\"process2\"/></victim-list></deadlock>";
    private const string GraphTwo = "<deadlock><victim-list><victimProcess id=\"process7\"/></victim-list></deadlock>";
    private const string GraphThree = "<deadlock><victim-list><victimProcess id=\"process9\"/></victim-list></deadlock>";

    private static readonly long[] RemovedIds = { 102, 103, 302 };

    private static readonly long[] KeptIds = { 101, 201, 301, 401, 402, 501, 502, 503, 504, 601, 602 };

    private readonly string _tempDir;
    private readonly string _dbPath;

    public DeadlockDuplicateCleanupTests()
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
    public async Task TheNextStartLeavesOneRowPerExactDuplicate_KeepsTheEarliest_AndTheStartAfterRemovesNothing()
    {
        await new DuckDbInitializer(_dbPath).InitializeAsync();

        using (var seed = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await seed.OpenAsync();

            /* D1: three copies, the earliest collected first. Inserted out of order so the keeper cannot be
               "the lowest id" or "the first inserted" by accident. */
            await InsertAsync(seed, 103, ServerA, Day1.AddHours(9).AddMinutes(41), EventOne, GraphOne);
            await InsertAsync(seed, 101, ServerA, Day1.AddHours(9).AddMinutes(31), EventOne, GraphOne);
            await InsertAsync(seed, 102, ServerA, Day1.AddHours(9).AddMinutes(36), EventOne, GraphOne);

            await InsertAsync(seed, 201, ServerA, Day1.AddHours(11).AddMinutes(1), EventTwo, GraphTwo);

            /* A copy that straddles midnight. */
            await InsertAsync(seed, 301, ServerA, Day1.AddHours(23).AddMinutes(58), EventThree, GraphThree);
            await InsertAsync(seed, 302, ServerA, Day2.AddMinutes(3), EventThree, GraphThree);

            /* Never touched. */
            await InsertAsync(seed, 401, ServerB, Day1.AddHours(9).AddMinutes(32), EventOne, GraphOne);
            await InsertAsync(seed, 402, ServerA, Day1.AddHours(9).AddMinutes(33), EventOne, GraphOneOffByOne);
            await InsertAsync(seed, 501, ServerA, Day1.AddHours(13).AddMinutes(1), EventNull, null);
            await InsertAsync(seed, 502, ServerA, Day1.AddHours(13).AddMinutes(2), EventNull, null);
            await InsertAsync(seed, 503, ServerA, Day1.AddHours(13).AddMinutes(3), EventNull, string.Empty);
            await InsertAsync(seed, 504, ServerA, Day1.AddHours(13).AddMinutes(4), EventNull, string.Empty);
            await InsertAsync(seed, 601, ServerA, Day1.AddHours(14).AddMinutes(1), null, GraphTwo);
            await InsertAsync(seed, 602, ServerA, Day1.AddHours(14).AddMinutes(2), null, GraphTwo);

            Assert.Equal(KeptIds.Length + RemovedIds.Length, await CountAsync(seed));
        }

        await new DuckDbInitializer(_dbPath).InitializeAsync();

        using var verify = new DuckDBConnection($"Data Source={_dbPath}");
        await verify.OpenAsync();

        Assert.Equal(KeptIds.OrderBy(i => i).ToList(), await ReadIdsAsync(verify));

        using (var keeper = verify.CreateCommand())
        {
            keeper.CommandText = "SELECT collection_time FROM deadlocks WHERE server_id = " + ServerA +
                " AND deadlock_time = TIMESTAMP '2026-03-10 09:30:00' AND deadlock_graph_xml = '" + GraphOne + "'";
            Assert.Equal(Day1.AddHours(9).AddMinutes(31), (DateTime)(await keeper.ExecuteScalarAsync())!);
        }

        /* The cleanup itself, called directly, now finds nothing; and so does one more start. */
        Assert.Equal(0, await DeadlockDuplicateCleanup.RemoveAsync(verify, logger: null));
        verify.Close();

        await new DuckDbInitializer(_dbPath).InitializeAsync();
        using var again = new DuckDBConnection($"Data Source={_dbPath}");
        await again.OpenAsync();
        Assert.Equal(KeptIds.OrderBy(i => i).ToList(), await ReadIdsAsync(again));
    }

    [Fact]
    public async Task TheCleanupCalledDirectlyReportsTheRowsItRemoved()
    {
        await new DuckDbInitializer(_dbPath).InitializeAsync();

        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync();
        await InsertAsync(connection, 1, ServerA, Day1.AddHours(9), EventOne, GraphOne);
        await InsertAsync(connection, 2, ServerA, Day1.AddHours(10), EventOne, GraphOne);

        Assert.Equal(1, await DeadlockDuplicateCleanup.RemoveAsync(connection, logger: null));
        Assert.Equal(new List<long> { 1 }, await ReadIdsAsync(connection));
    }

    private static async Task InsertAsync(
        DuckDBConnection connection, long id, int serverId, DateTime collectionTime, DateTime? deadlockTime, string? graph)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml) VALUES ($1,$2,$3,$4,$5,$6)";
        cmd.Parameters.Add(new DuckDBParameter { Value = id });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "S" + serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)deadlockTime ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)graph ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<long> CountAsync(DuckDBConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM deadlocks";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    private static async Task<List<long>> ReadIdsAsync(DuckDBConnection connection)
    {
        var ids = new List<long>();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT deadlock_id FROM deadlocks ORDER BY deadlock_id";
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            ids.Add(reader.GetInt64(0));
        }

        return ids;
    }
}
