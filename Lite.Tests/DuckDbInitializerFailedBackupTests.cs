using System;
using System.IO;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7769: the storage-version rebuild (<c>MigrateViaParquetAsync</c>) used to delete the database file and its
/// .wal when the backup rename failed, even when the export had failed too, which left no copy of the history.
/// Each test stands in a file DuckDB cannot open (so the export fails) and makes the rename fail.
/// </summary>
public sealed class DuckDbInitializerFailedBackupTests : IDisposable
{
    private const string DbText = "not a duckdb file, the only copy of the history";
    private const string WalText = "uncheckpointed writes";

    private readonly string _dir;
    private readonly string _dbPath;
    private readonly string _walPath;
    private readonly string _archive;

    public DuckDbInitializerFailedBackupTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_dir);
        _archive = Path.Combine(_dir, "archive");
        Directory.CreateDirectory(_archive);
        _dbPath = Path.Combine(_dir, "test.duckdb");
        _walPath = _dbPath + ".wal";
        File.WriteAllText(_dbPath, DbText);
        File.WriteAllText(_walPath, WalText);
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

    [Fact]
    public async Task ExportAndRenameBothFailing_KeepsTheFileAndItsWal_AndNamesTheFile()
    {
        using var initializer = new DuckDbInitializer(_dbPath)
        {
            MoveFileForTests = (_, _) => throw new IOException("rename refused")
        };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => initializer.MigrateViaParquetAsync(_archive));

        Assert.Contains(_dbPath, ex.Message);
        Assert.True(File.Exists(_dbPath));
        Assert.Equal(DbText, File.ReadAllText(_dbPath));
        Assert.True(File.Exists(_walPath));
        Assert.Equal(WalText, File.ReadAllText(_walPath));
    }

    [Fact]
    public async Task WalRenameFailing_KeepsTheWal_AndTheMainFile()
    {
        using var initializer = new DuckDbInitializer(_dbPath)
        {
            MoveFileForTests = (source, destination) =>
            {
                if (source.EndsWith(".wal", StringComparison.Ordinal))
                    throw new IOException("wal rename refused");
                File.Move(source, destination);
            }
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() => initializer.MigrateViaParquetAsync(_archive));

        Assert.True(File.Exists(_walPath));
        Assert.Equal(WalText, File.ReadAllText(_walPath));
        Assert.True(File.Exists(_dbPath));
        Assert.Equal(DbText, File.ReadAllText(_dbPath));
    }

    [Fact]
    public async Task MainRenameFailingAfterTheWalMoved_PutsTheWalBack()
    {
        using var initializer = new DuckDbInitializer(_dbPath)
        {
            MoveFileForTests = (source, destination) =>
            {
                if (string.Equals(source, _dbPath, StringComparison.Ordinal))
                    throw new IOException("main rename refused");
                File.Move(source, destination);
            }
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() => initializer.MigrateViaParquetAsync(_archive));

        Assert.True(File.Exists(_dbPath));
        Assert.True(File.Exists(_walPath));
        Assert.Equal(WalText, File.ReadAllText(_walPath));
    }

    [Fact]
    public async Task RenameSucceeding_MovesTheFileAndItsWalToTheBackup()
    {
        using var initializer = new DuckDbInitializer(_dbPath);

        await initializer.MigrateViaParquetAsync(_archive);

        Assert.False(File.Exists(_dbPath));
        Assert.False(File.Exists(_walPath));
        var backups = Directory.GetFiles(_dir, "test.duckdb.backup_*");
        Assert.Equal(2, backups.Length);
        Assert.Contains(backups, b => b.EndsWith(".wal", StringComparison.Ordinal) && File.ReadAllText(b) == WalText);
        Assert.Contains(backups, b => !b.EndsWith(".wal", StringComparison.Ordinal) && File.ReadAllText(b) == DbText);
    }
}
