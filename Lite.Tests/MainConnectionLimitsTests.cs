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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5381 (owner ruling 2026-10-06): Lite's main DuckDB connection runs at <c>memory_limit</c> 2 GB and at most
/// 8 threads. Measured: at 1 GB with every core (32 threads) five wide reads fail with out-of-memory; at 2 GB
/// and 8 threads every measured read passes, worst peak 1,100 MB. These pins hold the connection string, the
/// two places that put the limit back (the trim cycle and the COPY raise), and the source against a 1 GB
/// literal creeping back in.
/// </summary>
/* Same collection as DuckDbSentinelConnectionTests: the trim test lowers the process-wide TrimThresholdBytes. */
[Collection("CollectionResetGate")]
public class MainConnectionLimitsTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;

    public MainConnectionLimitsTests()
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
        catch { /* best-effort temp cleanup */ }
    }

    /// <summary>How DuckDB itself reports a given memory_limit text, so the pins never guess its formatting.</summary>
    private static async Task<string> ReportedMemoryLimitAsync(string setting)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        await connection.OpenAsync();
        using (var set = connection.CreateCommand())
        {
            set.CommandText = $"SET memory_limit = '{setting}'";
            await set.ExecuteNonQueryAsync();
        }
        return await CurrentLimitAsync(connection);
    }

    private static async Task<string> CurrentLimitAsync(DuckDBConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT current_setting('memory_limit')";
        return Convert.ToString(await cmd.ExecuteScalarAsync()) ?? "";
    }

    [Fact]
    public async Task MainConnection_OpenedLikeLite_IsTwoGigabytesAndAtMostEightThreads()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        using var connection = initializer.CreateConnection();
        await connection.OpenAsync();

        Assert.Equal(await ReportedMemoryLimitAsync("2GB"), await CurrentLimitAsync(connection));

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT current_setting('threads')";
        var threads = Convert.ToInt64(await cmd.ExecuteScalarAsync());
        Assert.Equal(Math.Max(1, Math.Min(8, Environment.ProcessorCount)), threads);
        Assert.InRange(threads, 1, 8);
    }

    [Fact]
    public async Task MemoryLimit_AfterTrimCycle_IsBackToTwoGigabytes()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        var originalThreshold = DuckDbInitializer.TrimThresholdBytes;
        DuckDbInitializer.TrimThresholdBytes = 1;
        try
        {
            initializer.RunMemoryTrimCycle();

            using var connection = initializer.CreateConnection();
            await connection.OpenAsync();
            Assert.Equal(await ReportedMemoryLimitAsync("2GB"), await CurrentLimitAsync(connection));
        }
        finally
        {
            DuckDbInitializer.TrimThresholdBytes = originalThreshold;
        }
    }

    [Fact]
    public async Task MemoryLimit_AfterRaisedLimitCopy_IsBackToTwoGigabytes()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        using var connection = initializer.CreateConnection();
        await connection.OpenAsync();

        string during = "";
        await ArchiveService.WithRaisedCopyMemoryLimit(connection, async () =>
        {
            during = await CurrentLimitAsync(connection);
        });

        Assert.Equal(await ReportedMemoryLimitAsync("4GB"), during);
        Assert.Equal(await ReportedMemoryLimitAsync("2GB"), await CurrentLimitAsync(connection));
    }

    /// <summary>
    /// No 1 GB literal for the main connection's limit anywhere in Lite's code. The separate DuckDB instances
    /// (compaction's in-memory 4 GB merge connection, the importer's plain connection) carry no 1 GB limit
    /// either, so nothing needs an allow-list. <c>checkpoint_threshold=1GB</c> is a different setting.
    /// </summary>
    [Fact]
    public void LiteSource_HasNoOneGigabyteMemoryLimitLiteral()
    {
        var litePath = Path.Combine(RepoRoot(), "Lite");
        var bad = new Regex(@"memory_limit\s*=\s*'?\s*1\s*GB|""1\s?GB""|'1\s?GB'|""1024\s?MB""", RegexOptions.IgnoreCase);
        var sep = Path.DirectorySeparatorChar;
        var hits = Directory.EnumerateFiles(litePath, "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains($"{sep}obj{sep}") && !f.Contains($"{sep}bin{sep}"))
            .SelectMany(f => File.ReadLines(f).Select((line, i) => (f, n: i + 1, line)))
            .Where(x =>
            {
                var t = x.line.TrimStart();
                var isComment = t.StartsWith("//", StringComparison.Ordinal) || t.StartsWith("*", StringComparison.Ordinal)
                    || t.StartsWith("/*", StringComparison.Ordinal);
                return !isComment && bad.IsMatch(x.line);
            })
            .Select(x => $"{Path.GetRelativePath(RepoRoot(), x.f)}:{x.n}")
            .ToList();

        Assert.True(hits.Count == 0, "Main connection memory_limit must come from DuckDbInitializer.MainConnectionMemoryLimit (2GB), not a 1GB literal: " + string.Join(", ", hits));
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
