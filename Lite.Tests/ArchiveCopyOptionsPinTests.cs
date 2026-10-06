/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Darling.Tests;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5381: DuckDB decodes about a row group's whole column chunk for any read that touches the column, so an
/// archive file written at DuckDB's default 122,880 rows (one row group per daily file) made every windowed read
/// of query_text decode the whole day. Every COPY that writes an archive file takes its options from one place,
/// <see cref="ParquetCompaction.ArchiveCopyOptions"/>, and these tests keep a new COPY from skipping it.
/// </summary>
public sealed class ArchiveCopyOptionsSourceScanTests
{
    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 12 && directory is not null; i++)
        {
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        throw new InvalidOperationException("PerformanceMonitor.sln not found above " + AppContext.BaseDirectory);
    }

    private static IEnumerable<(string Path, string Text)> LiteSources()
    {
        var lite = Path.Combine(RepoRoot(), "Lite");
        foreach (var file in Directory.EnumerateFiles(lite, "*.cs", SearchOption.AllDirectories))
        {
            var relative = Path.GetRelativePath(lite, file).Replace('\\', '/');
            if (relative.StartsWith("bin/", StringComparison.Ordinal) || relative.StartsWith("obj/", StringComparison.Ordinal))
            {
                continue;
            }

            yield return (relative, File.ReadAllText(file));
        }
    }

    /* A COPY's destination in an interpolated SQL string: TO '{path}', with or without an option list after it.
       The path expression may hold parentheses but never a closing brace, so [^}]* spans it. A bare
       COPY ... TO '{path}' has no option list at all (DuckDB infers parquet from the extension) and would write
       122,880-row groups, so the destination alone counts, not only one followed by a parenthesis (#5381). */
    private static readonly Regex CopyDestination = new(@"\bTO\s+'\{[^}]*\}'", RegexOptions.CultureInvariant);

    private static readonly Regex CopyWithSharedOptions = new(
        @"\bTO\s+'\{[^}]*\}'\s*\(\{(?:ParquetCompaction\.)?(?:ArchiveCopyOptions|BuildArchiveCopyOptions\([^}]*\))\}\)",
        RegexOptions.CultureInvariant);

    private static (int All, int Shared) CountCopies(string text) =>
        (CopyDestination.Matches(text).Count, CopyWithSharedOptions.Matches(text).Count);

    [Fact]
    public void ABareCopyWithNoOptionList_IsCountedAsAnOffender()
    {
        /* A planted COPY with no option list: it must count as a COPY that is not on the shared options. */
        var (bareAll, bareShared) = CountCopies("cmd.CommandText = $\"COPY (SELECT * FROM {table}) TO '{path}'\";");
        Assert.Equal(1, bareAll);
        Assert.Equal(0, bareShared);

        var (listAll, listShared) = CountCopies("cmd.CommandText = $\"COPY (SELECT 1) TO '{path}' (FORMAT PARQUET)\";");
        Assert.Equal(1, listAll);
        Assert.Equal(0, listShared);

        var (okAll, okShared) = CountCopies(
            "cmd.CommandText = $\"COPY (SELECT 1) TO '{path}' ({ParquetCompaction.ArchiveCopyOptions})\";");
        Assert.Equal(1, okAll);
        Assert.Equal(1, okShared);
    }

    [Fact]
    public void EveryLiteArchiveCopy_TakesItsOptionsFromTheSharedConstant()
    {
        var offenders = new List<string>();
        var compliant = 0;

        foreach (var (path, text) in LiteSources())
        {
            var (all, shared) = CountCopies(text);
            compliant += shared;
            if (all != shared)
            {
                offenders.Add($"{path}: {all - shared} COPY(ies) without ParquetCompaction.ArchiveCopyOptions");
            }
        }

        Assert.True(compliant >= 6,
            $"expected the writer, compaction, the two other ArchiveService COPYs, DataImportService and Query Store slice repair; found {compliant}");
        Assert.True(offenders.Count == 0, string.Join(Environment.NewLine, offenders));
    }

    [Fact]
    public void NoLiteSourceSpellsOutAParquetFormatOutsideTheSharedConstant()
    {
        /* The one exception is DuckDbInitializer's EXPORT DATABASE backup: it writes a restorable database
           export, not an archive file the views read, and EXPORT DATABASE takes a different option list. */
        var offenders = new List<string>();
        foreach (var (path, text) in LiteSources())
        {
            if (path == "Services/ParquetCompaction.cs")
            {
                continue;
            }

            /* Read the string-literal bodies the walker finds, not lines filtered by a comment prefix: a
               "FORMAT PARQUET" inside a comment is not a literal, and a block comment's continuation line
               carries no prefix to filter on. Line numbers come from the body's offset in the file. */
            foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(text))
            {
                var firstLine = text.AsSpan(0, start).Count('\n') + 1;
                var lines = body.Split('\n');
                for (var i = 0; i < lines.Length; i++)
                {
                    if (lines[i].Contains("FORMAT PARQUET", StringComparison.OrdinalIgnoreCase)
                        && !lines[i].Contains("EXPORT DATABASE", StringComparison.Ordinal))
                    {
                        offenders.Add($"{path}:{firstLine + i}");
                    }
                }
            }
        }

        Assert.True(offenders.Count == 0, "FORMAT PARQUET spelled out, use ParquetCompaction.ArchiveCopyOptions: " + string.Join(", ", offenders));
    }

    [Fact]
    public void TheSharedConstant_NamesARowGroupSizeBelowDuckDbsDefault()
    {
        Assert.Contains($"ROW_GROUP_SIZE {ParquetCompaction.ArchiveRowGroupSize}", ParquetCompaction.ArchiveCopyOptions, StringComparison.Ordinal);
        Assert.Contains("COMPRESSION ZSTD", ParquetCompaction.ArchiveCopyOptions, StringComparison.Ordinal);
        Assert.Equal(ParquetCompaction.ArchiveRowGroupSize, ParquetCompaction.DefaultRowGroupSize);
        Assert.InRange(ParquetCompaction.ArchiveRowGroupSize, 2048, 8192);
    }
}

[Collection("CollectionResetGate")]
public sealed class ArchiveWriterRowGroupTests : IDisposable
{
    private readonly List<DuckDbInitializer> _initializers = [];
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;

    public ArchiveWriterRowGroupTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
    }

    public void Dispose()
    {
        foreach (var initializer in _initializers)
        {
            initializer.Dispose();
        }

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

    private async Task<long> ScalarAsync(string sql)
    {
        using var connection = new DuckDBConnection("Data Source=:memory:");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    /* The row count is well above one row group at ArchiveRowGroupSize and far below DuckDB's default 122,880,
       so a file written without the bound is exactly one row group. */
    private const int SeededRows = 9000;

    [Fact]
    public async Task ArchivalWriter_WritesMoreThanOneRowGroup_ForATableAboveTheBound()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        _initializers.Add(initializer);
        await initializer.InitializeAsync();

        using (var connection = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var seed = connection.CreateCommand();
            seed.CommandText = $@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
SELECT i, 1, 'S1', 'wait_stats', TIMESTAMP '2026-01-01 00:00:00' + INTERVAL (i) SECOND, 'SUCCESS' FROM range(1, {SeededRows + 1}) t(i)";
            await seed.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        await new ArchiveService(initializer, _archiveDir).ArchiveOldDataAsync(hotDataDays: 7);

        var file = Directory.GetFiles(_archiveDir, "*_collection_log.parquet").Single().Replace('\\', '/');
        Assert.Equal(SeededRows, await ScalarAsync($"SELECT count(*) FROM read_parquet('{file}')"));

        var groups = await ScalarAsync($"SELECT count(DISTINCT row_group_id) FROM parquet_metadata('{file}')");
        var largest = await ScalarAsync($"SELECT max(row_group_num_rows) FROM parquet_metadata('{file}')");
        Assert.True(groups > 1, $"the archive file is {groups} row group(s) of up to {largest} rows; a windowed read decodes whole groups");
        Assert.True(largest < SeededRows, $"largest row group {largest} holds every row");

        /* DuckDB 1.5.5's parallel writer can put whole row groups in a different order from the source (the rows
           inside a group stay in order), and nothing in Lite reads by file position. What the views depend on is
           that each group covers its own narrow span of collection_time, so the footer min/max can prune: no two
           groups may overlap in time. */
        /* A group with no footer min or max gives the join below no pair, so "no overlap" would pass without
           checking anything. Every group must carry both stats before the overlap is read (#5381). */
        var withoutStats = await ScalarAsync($@"
SELECT count(*) FROM parquet_metadata('{file}')
WHERE path_in_schema = 'collection_time' AND (stats_min IS NULL OR stats_max IS NULL)");
        var statGroups = await ScalarAsync($@"
SELECT count(*) FROM parquet_metadata('{file}') WHERE path_in_schema = 'collection_time'");
        Assert.True(statGroups == groups, $"{statGroups} collection_time column chunk(s) for {groups} row group(s)");
        Assert.True(withoutStats == 0, $"{withoutStats} row group(s) have no footer min/max for collection_time, so the overlap check below would pass vacuously");

        var overlapping = await ScalarAsync($@"
WITH g AS (SELECT row_group_id, min(stats_min::TIMESTAMP) AS mn, max(stats_max::TIMESTAMP) AS mx
           FROM parquet_metadata('{file}') WHERE path_in_schema = 'collection_time' GROUP BY row_group_id)
SELECT count(*) FROM g a JOIN g b ON a.row_group_id < b.row_group_id AND a.mn <= b.mx AND b.mn <= a.mx");
        Assert.True(overlapping == 0, $"{overlapping} pair(s) of row groups overlap in collection_time, so a window read cannot skip either");
    }

    [Fact]
    public async Task Compaction_WritesTheSharedRowGroupSize()
    {
        var source = Path.Combine(_tempDir, "20260101_0000_collection_log.parquet").Replace('\\', '/');
        var output = Path.Combine(_tempDir, "merged.parquet").Replace('\\', '/');
        using (var connection = new DuckDBConnection("Data Source=:memory:"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var write = connection.CreateCommand();
            write.CommandText = $"COPY (SELECT i AS log_id, TIMESTAMP '2026-01-01 00:00:00' + INTERVAL (i) SECOND AS collection_time FROM range(1, {SeededRows + 1}) t(i)) TO '{source}' (FORMAT PARQUET)";
            await write.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        ParquetCompaction.MergeBatchToFile("collection_log", [source], output, _tempDir.Replace('\\', '/'));

        var groups = await ScalarAsync($"SELECT count(DISTINCT row_group_id) FROM parquet_metadata('{output}')");
        Assert.True(groups > 1, $"compaction wrote {groups} row group(s)");
        Assert.Equal(0, await ScalarAsync($"SELECT count(*) FROM (SELECT DISTINCT row_group_id, row_group_num_rows FROM parquet_metadata('{output}')) WHERE row_group_num_rows > {ParquetCompaction.ArchiveRowGroupSize * 4}"));
    }
}
