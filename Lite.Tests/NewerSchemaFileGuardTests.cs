using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4727: a data file stamped with a schema version newer than the app's is refused, with a message naming both
/// versions, and nothing is written to it. A read-only open would not be enough: the collectors would still fail.
/// </summary>
public class NewerSchemaFileGuardTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;

    public NewerSchemaFileGuardTests()
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
    public async Task AFileStampedOneVersionAboveTheApp_IsRefused_AndNothingIsWrittenToIt()
    {
        var newer = DuckDbInitializer.CurrentSchemaVersion + 1;

        using (var seed = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await seed.OpenAsync();
            await ExecAsync(seed, "CREATE TABLE schema_version (version INTEGER NOT NULL)");
            await ExecAsync(seed, $"INSERT INTO schema_version VALUES ({newer})");
        }

        var bytesBefore = SHA256.HashData(File.ReadAllBytes(_dbPath));
        var writtenBefore = File.GetLastWriteTimeUtc(_dbPath);

        using var initializer = new DuckDbInitializer(_dbPath);
        var refusal = await Assert.ThrowsAsync<SchemaVersionTooNewException>(() => initializer.InitializeAsync());

        Assert.Equal(newer, refusal.FileVersion);
        Assert.Equal(DuckDbInitializer.CurrentSchemaVersion, refusal.AppVersion);
        Assert.Contains($"schema version {newer}", refusal.Message, StringComparison.Ordinal);
        Assert.Contains($"schema version {DuckDbInitializer.CurrentSchemaVersion}", refusal.Message, StringComparison.Ordinal);

        Assert.Equal(bytesBefore, SHA256.HashData(File.ReadAllBytes(_dbPath)));
        Assert.Equal(writtenBefore, File.GetLastWriteTimeUtc(_dbPath));
        Assert.False(File.Exists(_dbPath + ".wal"), "a refused open must not leave a write-ahead log behind");

        /* And no table was created: the stamp table is still the only thing in the file. */
        using var verify = new DuckDBConnection($"Data Source={_dbPath};ACCESS_MODE=READ_ONLY");
        await verify.OpenAsync();
        using var count = verify.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main'";
        Assert.Equal(1L, Convert.ToInt64(await count.ExecuteScalarAsync()));
    }

    [Fact]
    public async Task AFileStampedAtTheAppsOwnVersion_StillOpens()
    {
        using var first = new DuckDbInitializer(_dbPath);
        await first.InitializeAsync();

        using var second = new DuckDbInitializer(_dbPath);
        await second.InitializeAsync();

        using var verify = new DuckDBConnection($"Data Source={_dbPath}");
        await verify.OpenAsync();
        using var version = verify.CreateCommand();
        version.CommandText = "SELECT MAX(version) FROM schema_version";
        Assert.Equal((long)DuckDbInitializer.CurrentSchemaVersion, Convert.ToInt64(await version.ExecuteScalarAsync()));
    }

    private static async Task ExecAsync(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }
}
