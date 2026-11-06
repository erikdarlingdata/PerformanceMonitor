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
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// <see cref="DataImportService.FlushOldDatabase"/> built its <c>COPY … TO '{path}'</c> by interpolating the
/// export path raw, while every sibling COPY in the app (ArchiveService, ParquetCompaction) routes the same
/// value through <see cref="DuckDbInitializer.EscapeSqlPath"/>. The path comes from the import dialog's folder
/// picker, and an apostrophe is perfectly legal in a Windows path (<c>C:\Users\O'Brien\…</c>), so the quote
/// closed the SQL literal, the COPY failed to parse, and the per-table catch swallowed it as a warning: the
/// import reported success having silently flushed nothing.
///
/// <para>The test is end-to-end against a real DuckDB under a directory with an apostrophe in its name, not an
/// assertion about the generated string — a string check would pass on any escaping that merely looked right,
/// and the failure being pinned is a silent zero, not an exception.</para>
/// </summary>
public sealed class DataImportPathEscapingTests : IDisposable
{
    private readonly string _tempRoot;

    public DataImportPathEscapingTests()
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

    [Theory]
    [InlineData("plain-folder")]
    [InlineData("O'Brien's data")]   // the bug: an apostrophe closed the SQL string literal
    public void FlushOldDatabase_ExportsToParquet_EvenWhenThePathContainsAQuote(string folderName)
    {
        var folder = Path.Combine(_tempRoot, folderName);
        Directory.CreateDirectory(folder);

        /* One real archivable table with one row — FlushOldDatabase skips tables that are absent or empty. */
        var table = ArchiveService.ArchivableTables[0].Table;
        using (var connection = new DuckDBConnection($"Data Source={Path.Combine(folder, "monitor.duckdb")}"))
        {
            connection.Open();
            using var create = connection.CreateCommand();
            create.CommandText = $"CREATE TABLE {table} (id BIGINT); INSERT INTO {table} VALUES (1)";
            create.ExecuteNonQuery();
        }

        var (failure, tablesFlushed, _) = DataImportService.FlushOldDatabase(folder);

        Assert.Equal(DataImportOpenFailure.None, failure);
        Assert.Equal(1, tablesFlushed);

        /* The per-table catch turns a COPY failure into a logged warning and a zero count, so prove the file
           is really on disk rather than trusting the counter alone. */
        var written = Directory.GetFiles(Path.Combine(folder, "archive"), "*.parquet");
        Assert.Single(written);
        Assert.EndsWith($"_{table}.parquet", written[0], StringComparison.Ordinal);
        Assert.True(new FileInfo(written[0]).Length > 0, "the parquet export is empty");
    }

    [Fact]
    public void EscapeSqlPath_DoublesEveryQuote_AndLeavesOrdinaryPathsAlone()
    {
        Assert.Equal(@"C:/data/archive", DuckDbInitializer.EscapeSqlPath(@"C:/data/archive"));
        Assert.Equal(@"C:/Users/O''Brien/archive", DuckDbInitializer.EscapeSqlPath(@"C:/Users/O'Brien/archive"));
        Assert.Equal("''''", DuckDbInitializer.EscapeSqlPath("''"));
    }

    /// <summary>
    /// Every <c>COPY … TO</c> / <c>read_parquet</c> path in the app goes through the one escaper. Pinned by
    /// reading the sources, because the defect was a single call site that had drifted from its siblings —
    /// there is nothing to unit-test about a line that simply forgot to call something.
    /// </summary>
    [Theory]
    [InlineData("DataImportService.cs")]
    [InlineData("ArchiveService.cs")]
    [InlineData("ParquetCompaction.cs")]
    public void NoSqlPathIsInterpolatedRaw(string fileName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", fileName);
        Assert.True(File.Exists(path), $"{fileName} was not copied beside the test binary — check the csproj None/Link item.");

        var offenders = File.ReadAllLines(path)
            .Select((line, index) => (line, number: index + 1))
            .Where(l => l.line.Contains("TO '{", StringComparison.Ordinal)
                        || l.line.Contains("read_parquet('{", StringComparison.Ordinal))
            .Where(l => !l.line.Contains("EscapeSqlPath", StringComparison.Ordinal))
            .Select(l => $"{fileName}:{l.number}: {l.line.Trim()}")
            .ToList();

        Assert.True(offenders.Count == 0, "a SQL path literal is interpolated without EscapeSqlPath:\n" + string.Join("\n", offenders));
    }
}
