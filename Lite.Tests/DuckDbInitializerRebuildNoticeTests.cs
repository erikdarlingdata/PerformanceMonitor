using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7812: when the storage-version rebuild (<c>MigrateViaParquetAsync</c>) leaves the new database without the old data,
/// the initializer records where the old file went so the window can say so. A rebuild whose import worked records nothing.
/// </summary>
public sealed class DuckDbInitializerRebuildNoticeTests : IDisposable
{
    private readonly string _dir;
    private readonly string _archive;

    public DuckDbInitializerRebuildNoticeTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        _archive = Path.Combine(_dir, "archive");
        Directory.CreateDirectory(_archive);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_dir))
                Directory.Delete(_dir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    private string CreateRealDatabase()
    {
        var path = Path.Combine(_dir, "real.duckdb");
        using var conn = new DuckDBConnection($"Data Source={path}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE history (id INTEGER); INSERT INTO history VALUES (1), (2), (3);";
        cmd.ExecuteNonQuery();
        return path;
    }

    [Fact]
    public async Task UnreadableFile_RecordsTheBackupPath_AndNoExportFolder()
    {
        var path = Path.Combine(_dir, "test.duckdb");
        File.WriteAllText(path, "not a duckdb file");
        using var initializer = new DuckDbInitializer(path);

        await initializer.MigrateViaParquetAsync(_archive);

        var outcome = Assert.IsType<DatabaseRebuildOutcome>(initializer.RebuildOutcome);
        var backup = Assert.Single(Directory.GetFiles(_dir, "test.duckdb.backup_*"));
        Assert.Equal(backup, outcome.BackupPath);
        Assert.Null(outcome.ExportFolder);
    }

    [Fact]
    public async Task RebuildWhoseImportWorks_RecordsNothing()
    {
        var path = CreateRealDatabase();
        using var initializer = new DuckDbInitializer(path);

        await initializer.MigrateViaParquetAsync(_archive);

        Assert.NotEmpty(Directory.GetFiles(_dir, "real.duckdb.backup_*"));
        Assert.Null(initializer.RebuildOutcome);
    }

    [Fact]
    public async Task ImportFailing_RecordsTheBackupPath_AndTheExportFolder()
    {
        var path = CreateRealDatabase();
        /* After the old file moves aside, a file DuckDB cannot open takes its place, so the import into the new database fails. */
        using var initializer = new DuckDbInitializer(path)
        {
            MoveFileForTests = (source, destination) =>
            {
                File.Move(source, destination);
                if (string.Equals(source, path, StringComparison.Ordinal))
                    File.WriteAllText(path, "not a duckdb file");
            }
        };

        await initializer.MigrateViaParquetAsync(_archive);

        var outcome = Assert.IsType<DatabaseRebuildOutcome>(initializer.RebuildOutcome);
        var backup = Assert.Single(Directory.GetFiles(_dir, "real.duckdb.backup_*"));
        Assert.Equal(backup, outcome.BackupPath);
        var exportDir = Assert.Single(Directory.GetDirectories(_archive, "upgrade_*"));
        Assert.Equal(exportDir, outcome.ExportFolder);
    }

    [Fact]
    public void NoticeText_NamesTheBackupPath_AndTheExportFolderOnlyWhenThereIsOne()
    {
        var withoutFolder = new DatabaseRebuildOutcome(@"C:\data\lite.duckdb.backup_1", null).Describe();
        Assert.Contains(@"C:\data\lite.duckdb.backup_1", withoutFolder);
        Assert.Contains("started with an empty one", withoutFolder);
        Assert.DoesNotContain("Parquet", withoutFolder);

        var withFolder = new DatabaseRebuildOutcome(@"C:\data\lite.duckdb.backup_1", @"C:\data\archive\upgrade_1").Describe();
        Assert.Contains(@"C:\data\lite.duckdb.backup_1", withFolder);
        Assert.Contains(@"C:\data\archive\upgrade_1", withFolder);
    }
}
