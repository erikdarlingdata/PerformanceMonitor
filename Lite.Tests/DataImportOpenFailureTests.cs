/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
