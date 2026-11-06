/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Import Data opens the previous install's <c>monitor.duckdb</c> to flush its hot tables. A file another process holds
/// is a lock, and closing the previous Lite and retrying fixes it, so the import asks. Any other open failure, a store
/// written by a newer DuckDB storage version or a corrupt file, cannot be fixed by retrying, so the import must stop
/// and name the real cause instead of asking the user to close an application that is not running.
/// </summary>
public sealed class DataImportOpenFailureTests : IDisposable
{
    private readonly string _tempRoot;

    public DataImportOpenFailureTests()
    {
        _tempRoot = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempRoot);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempRoot))
            {
                Directory.Delete(_tempRoot, recursive: true);
            }
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    [Fact]
    public async Task ACorruptOldStore_FailsTheImportNamingTheCause_AndNeverAsksToRetry()
    {
        var oldInstall = Path.Combine(_tempRoot, "old-install");
        Directory.CreateDirectory(oldInstall);
        /* Bytes that are not a DuckDB file where the previous install's database should be. */
        File.WriteAllBytes(Path.Combine(oldInstall, "monitor.duckdb"), Enumerable.Repeat((byte)0xAB, 8192).ToArray());
        using var current = new DuckDbInitializer(Path.Combine(_tempRoot, "current.duckdb"));
        var import = new DataImportService(current, Path.Combine(_tempRoot, "archive"));
        var retryPrompts = 0;

        var result = await import.RunImportAsync(oldInstall, _ =>
        {
            retryPrompts++;
            return Task.FromResult(false);
        });

        Assert.False(result.Success);
        Assert.Equal(0, retryPrompts);
        Assert.NotNull(result.ErrorMessage);
        Assert.Contains("corrupt", result.ErrorMessage, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("lock", result.ErrorMessage, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("cancel", result.ErrorMessage, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("IO Error: Could not set lock on file \"C:/old/monitor.duckdb\": Conflicting lock is held in C:/old/Lite.exe (PID 4242)", DataImportOpenFailure.Locked)]
    [InlineData("The process cannot access the file because it is being used by another process.", DataImportOpenFailure.Locked)]
    [InlineData("IO Error: Trying to read a database file with version number 65, but we can only read version 64.", DataImportOpenFailure.StorageVersion)]
    [InlineData("Serialization Error: Failed to deserialize: unknown field id 7", DataImportOpenFailure.StorageVersion)]
    [InlineData("IO Error: The file \"C:/old/monitor.duckdb\" exists, but it is not a valid DuckDB database file!", DataImportOpenFailure.Other)]
    [InlineData("IO Error: Checksum mismatch in block 12", DataImportOpenFailure.Other)]
    public void Classify_TellsALockApartFromTheFailuresNoRetryCanFix(string message, DataImportOpenFailure expected)
    {
        Assert.Equal(expected, DataImportOpenFailureClassifier.Classify(new InvalidOperationException(message)));
    }

    [Fact]
    public void Classify_ReadsAnInnerException()
    {
        var wrapped = new InvalidOperationException("open failed", new IOException("Could not set lock on file: Conflicting lock is held"));

        Assert.Equal(DataImportOpenFailure.Locked, DataImportOpenFailureClassifier.Classify(wrapped));
    }

    /// <summary>
    /// Lite runs on Windows, where DuckDB reports a sharing violation as "Cannot open file ...:" followed by the OS
    /// message in the user's language, with "File is already open in [exe] (PID n)" appended when it can name the holder.
    /// The Win32 sharing-violation (32) and lock-violation (33) codes also arrive as an <see cref="IOException"/>
    /// HResult or an inner <see cref="Win32Exception"/>. Each is a lock, whatever language the OS text is in.
    /// </summary>
    public static TheoryData<string, Exception> WindowsLockShapes() => new()
    {
        { "already open in, behind a localized OS message", new InvalidOperationException("IO Error: Cannot open file \"C:\\old\\monitor.duckdb\": Der Prozess kann nicht auf die Datei zugreifen, da sie von einem anderen Prozess verwendet wird.\r\n\nFile is already open in \nC:\\old\\Lite.exe (PID 4242)") },
        { "IOException with the sharing-violation HResult", new IOException("Cannot open file \"C:\\old\\monitor.duckdb\": Zugriff verweigert", unchecked((int)0x80070020)) },
        { "IOException with the lock-violation HResult", new IOException("Cannot open file \"C:\\old\\monitor.duckdb\": Zugriff verweigert", unchecked((int)0x80070021)) },
        { "inner Win32Exception 32", new InvalidOperationException("open failed", new Win32Exception(32, "Zugriff verweigert")) },
        { "inner Win32Exception 33", new InvalidOperationException("open failed", new Win32Exception(33, "Zugriff verweigert")) },
    };

    [Theory]
    [MemberData(nameof(WindowsLockShapes))]
    public void Classify_ReadsTheWindowsLockShapes(string label, Exception ex)
    {
        var kind = DataImportOpenFailureClassifier.Classify(ex);

        Assert.True(kind == DataImportOpenFailure.Locked, $"{label}: classified as {kind}");
    }

    /// <summary>
    /// When only the old hot database cannot be read, the parquet files already in the old install's archive folder are
    /// still worth importing: the import copies them, refreshes the views, and reports the failure next to the count.
    /// </summary>
    [Fact]
    public async Task AnOldStoreThatCannotBeRead_StillImportsItsArchiveFolder_AndReportsTheFailureWithTheCount()
    {
        var table = ArchiveService.ArchivableTables[0].Table;
        var oldInstall = Path.Combine(_tempRoot, "old-install");
        var oldArchive = Path.Combine(oldInstall, "archive");
        Directory.CreateDirectory(oldArchive);
        File.WriteAllBytes(Path.Combine(oldInstall, "monitor.duckdb"), Enumerable.Repeat((byte)0xAB, 8192).ToArray());
        var currentDir = Path.Combine(_tempRoot, "current");
        var currentArchive = Path.Combine(currentDir, "archive");
        Directory.CreateDirectory(currentArchive);
        using var current = new DuckDbInitializer(Path.Combine(currentDir, "monitor.duckdb"));
        await current.InitializeFromTemplateAsync();
        /* One parquet file in the old install's archive folder, exported from the same table so the refreshed views read it. */
        var oldParquet = Path.Combine(oldArchive, $"20260101_0000_{table}.parquet").Replace("\\", "/");
        using (current.AcquireReadLock())
        using (var connection = current.CreateConnection())
        {
            await connection.OpenAsync();
            using var export = connection.CreateCommand();
            export.CommandText = $"COPY (SELECT * FROM {table} LIMIT 0) TO '{DuckDbInitializer.EscapeSqlPath(oldParquet)}' (FORMAT PARQUET)";
            await export.ExecuteNonQueryAsync();
        }
        var import = new DataImportService(current, currentArchive);
        var retryPrompts = 0;

        var result = await import.RunImportAsync(oldInstall, _ =>
        {
            retryPrompts++;
            return Task.FromResult(false);
        });

        Assert.Equal(0, retryPrompts);
        Assert.Equal(1, result.FilesImported);
        Assert.True(File.Exists(Path.Combine(currentArchive, $"imported_20260101_0000_{table}.parquet")), "the old install's archive file was not copied");
        Assert.False(result.Success);
        Assert.NotNull(result.ErrorMessage);
        Assert.Contains("corrupt", result.ErrorMessage, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("1 parquet file", result.ErrorMessage, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(DataImportOpenFailure.StorageVersion, "storage version")]
    [InlineData(DataImportOpenFailure.Other, "corrupt")]
    public void UserMessage_NamesTheCause_CarriesDuckDbsText_AndDoesNotBlameALock(DataImportOpenFailure failure, string cause)
    {
        var text = DataImportOpenFailureClassifier.UserMessage(failure, new InvalidOperationException("duckdb said so"));

        Assert.Contains(cause, text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("duckdb said so", text, StringComparison.Ordinal);
        Assert.DoesNotContain("lock", text, StringComparison.OrdinalIgnoreCase);
    }
}
