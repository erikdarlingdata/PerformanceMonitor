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
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Files copied from a previous install are prefixed <c>imported_</c>. Retention parsed the date from the
/// start of the name, so the prefix hid it and no imported file ever expired; the imported copy of
/// <c>query_snapshots</c>, the largest table, stayed on disk for good.
/// </summary>
public sealed class RetentionImportedFileTests : IDisposable
{
    private readonly string _archiveDir;

    public RetentionImportedFileTests()
    {
        _archiveDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8], "archive");
        Directory.CreateDirectory(_archiveDir);
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(Path.GetDirectoryName(_archiveDir)!, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    private void Touch(string fileName) => File.WriteAllText(Path.Combine(_archiveDir, fileName), "parquet");

    [Fact]
    public void ImportedFiles_ExpireOnTheSameDateAsLocalOnes()
    {
        var old = DateTime.UtcNow.AddMonths(-(RetentionService.ArchiveRetentionMonths + 2));
        var recent = DateTime.UtcNow;

        Touch($"imported_{old:yyyyMM}_query_snapshots.parquet");
        Touch($"imported_{old:yyyyMMdd}_1200_query_snapshots.parquet");
        Touch($"imported_{old:yyyyMM}_wait_stats_pt001.parquet");
        Touch($"{old:yyyyMM}_wait_stats.parquet");
        Touch($"imported_{recent:yyyyMM}_wait_stats.parquet");
        Touch($"{recent:yyyyMM}_wait_stats.parquet");

        new RetentionService(_archiveDir).CleanupOldArchives();

        var remaining = Directory.GetFiles(_archiveDir).Select(Path.GetFileName).Order(StringComparer.Ordinal).ToArray();
        Assert.Equal([$"{recent:yyyyMM}_wait_stats.parquet", $"imported_{recent:yyyyMM}_wait_stats.parquet"], remaining);
    }
}
