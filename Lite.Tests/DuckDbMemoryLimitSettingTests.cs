/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5457 (owner ruling 2026-10-08): the main DuckDB connection's <c>memory_limit</c> is a Lite setting
/// (<c>duckdb_memory_limit_gb</c> in settings.json, Settings &gt; Dashboard Defaults). Default 2 GB, a whole
/// number of GB from 1 to 80% of physical memory, applied at startup. These pins hold the default, the range and
/// the fallback, and that every reader of the limit (the connection string, the COPY raise and its restore, the
/// trim cycle's restore) follows the setting, and that an out-of-memory message names it.
/// </summary>
/* Same collection as MainConnectionLimitsTests: these tests set the process-wide ConfiguredMemoryLimitGb. */
[Collection("CollectionResetGate")]
public class DuckDbMemoryLimitSettingTests : IDisposable
{
    private const long Gb = 1024L * 1024 * 1024;

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly int _originalGb = DuckDbInitializer.ConfiguredMemoryLimitGb;

    public DuckDbMemoryLimitSettingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
    }

    public void Dispose()
    {
        DuckDbInitializer.ConfiguredMemoryLimitGb = _originalGb;
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch { /* best-effort temp cleanup */ }
    }

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
    public void Default_IsTwoGigabytes_DevsCurrentValue()
    {
        Assert.Equal(2, DuckDbMemoryLimitSetting.DefaultGb);
        Assert.Equal(1, DuckDbMemoryLimitSetting.MinGb);
        Assert.Equal(2, DuckDbMemoryLimitSetting.Resolve(null, 16 * Gb, out _));
    }

    [Theory]
    [InlineData(16L, 12)]
    [InlineData(8L, 6)]
    [InlineData(4L, 3)]
    [InlineData(2L, 1)]
    [InlineData(1L, 1)]
    public void MaxGb_IsEightyPercentOfPhysicalRoundedDownAndNeverBelowOne(long physicalGb, int expected)
    {
        Assert.Equal(expected, DuckDbMemoryLimitSetting.MaxGb(physicalGb * Gb));
    }

    [Fact]
    public void MaxGb_RoundsDownNotUp()
    {
        /* 80% of 7 GB is 5.6 GB. */
        Assert.Equal(5, DuckDbMemoryLimitSetting.MaxGb(7 * Gb));
    }

    [Theory]
    [InlineData(0, false)]
    [InlineData(1, true)]
    [InlineData(2, true)]
    [InlineData(12, true)]
    [InlineData(13, false)]
    [InlineData(-3, false)]
    public void IsInRange_OnSixteenGigabyteMachine(int gb, bool expected)
    {
        Assert.Equal(expected, DuckDbMemoryLimitSetting.IsInRange(gb, 16 * Gb));
    }

    [Fact]
    public void RangeText_StatesTheRange()
    {
        var text = DuckDbMemoryLimitSetting.RangeText(16 * Gb);
        Assert.Contains("1 to 12 GB", text);
        Assert.Contains("80%", text);
    }

    [Fact]
    public void Resolve_ValidStoredValue_IsUsedWithoutANote()
    {
        var gb = DuckDbMemoryLimitSetting.Resolve("{\"duckdb_memory_limit_gb\": 6}", 16 * Gb, out var note);
        Assert.Equal(6, gb);
        Assert.Null(note);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("{}")]
    [InlineData("{\"other\": 1}")]
    [InlineData("[1,2]")]
    [InlineData("{ not json")]
    [InlineData("{\"duckdb_memory_limit_gb\": \"six\"}")]
    [InlineData("{\"duckdb_memory_limit_gb\": 2.5}")]
    [InlineData("{\"duckdb_memory_limit_gb\": 0}")]
    [InlineData("{\"duckdb_memory_limit_gb\": -4}")]
    [InlineData("{\"duckdb_memory_limit_gb\": 13}")]
    [InlineData("{\"duckdb_memory_limit_gb\": 99999999999}")]
    [InlineData("{\"duckdb_memory_limit_gb\": null}")]
    public void Resolve_MissingUnparsableOrOutOfRange_FallsBackToDefaultWithOneNote(string? settingsText)
    {
        var gb = DuckDbMemoryLimitSetting.Resolve(settingsText, 16 * Gb, out var note);
        Assert.Equal(DuckDbMemoryLimitSetting.DefaultGb, gb);
        Assert.NotNull(note);
        Assert.Contains("default 2 GB", note);
    }

    [Fact]
    public void Resolve_OutOfRangeNote_StatesTheRange()
    {
        DuckDbMemoryLimitSetting.Resolve("{\"duckdb_memory_limit_gb\": 40}", 16 * Gb, out var note);
        Assert.Contains("1 to 12 GB", note);
    }

    [Fact]
    public void LoadAtStartup_InstallsTheStoredValue_AndAnUnusableOneFallsBackToDefault()
    {
        var physical = DuckDbMemoryLimitSetting.PhysicalMemoryBytes();
        var max = DuckDbMemoryLimitSetting.MaxGb(physical);

        DuckDbMemoryLimitSetting.LoadAtStartup("{\"duckdb_memory_limit_gb\": 1}");
        Assert.Equal(1, DuckDbInitializer.ConfiguredMemoryLimitGb);

        DuckDbMemoryLimitSetting.LoadAtStartup($"{{\"duckdb_memory_limit_gb\": {max + 1}}}");
        Assert.Equal(DuckDbMemoryLimitSetting.DefaultGb, DuckDbInitializer.ConfiguredMemoryLimitGb);

        DuckDbMemoryLimitSetting.LoadAtStartup(null);
        Assert.Equal(DuckDbMemoryLimitSetting.DefaultGb, DuckDbInitializer.ConfiguredMemoryLimitGb);
    }

    [Fact]
    public async Task ConnectionString_CarriesTheSetting()
    {
        DuckDbInitializer.ConfiguredMemoryLimitGb = 3;
        using var initializer = new DuckDbInitializer(_dbPath);
        Assert.Contains("memory_limit=3GB;", initializer.ConnectionString);
        Assert.Equal("3GB", DuckDbInitializer.MainConnectionMemoryLimit);

        await initializer.InitializeAsync();
        using var connection = initializer.CreateConnection();
        await connection.OpenAsync();
        Assert.Equal(await ReportedMemoryLimitAsync("3GB"), await CurrentLimitAsync(connection));
    }

    [Theory]
    [InlineData(1, "4GB")]
    [InlineData(2, "4GB")]
    [InlineData(4, "4GB")]
    [InlineData(6, "6GB")]
    public void CopyLimit_IsTheLargerOfFourGigabytesAndTheSetting(int settingGb, string expectedCopy)
    {
        DuckDbInitializer.ConfiguredMemoryLimitGb = settingGb;
        Assert.Equal(expectedCopy, ArchiveService.MainConnectionCopyMemoryLimit);
    }

    [Theory]
    [InlineData(1, "4GB")]
    [InlineData(6, "6GB")]
    public async Task CopyRaise_BelowAndAboveFourGigabytes_RaisesThenRestoresToTheSetting(int settingGb, string expectedDuring)
    {
        DuckDbInitializer.ConfiguredMemoryLimitGb = settingGb;
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();
        using var connection = initializer.CreateConnection();
        await connection.OpenAsync();

        string during = "";
        await ArchiveService.WithRaisedCopyMemoryLimit(connection, async () =>
        {
            during = await CurrentLimitAsync(connection);
        });

        Assert.Equal(await ReportedMemoryLimitAsync(expectedDuring), during);
        Assert.Equal(await ReportedMemoryLimitAsync($"{settingGb}GB"), await CurrentLimitAsync(connection));
    }

    [Fact]
    public async Task TrimCycle_RestoresTheSetting()
    {
        DuckDbInitializer.ConfiguredMemoryLimitGb = 3;
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        var originalThreshold = DuckDbInitializer.TrimThresholdBytes;
        DuckDbInitializer.TrimThresholdBytes = 1;
        try
        {
            initializer.RunMemoryTrimCycle();

            using var connection = initializer.CreateConnection();
            await connection.OpenAsync();
            Assert.Equal(await ReportedMemoryLimitAsync("3GB"), await CurrentLimitAsync(connection));
        }
        finally
        {
            DuckDbInitializer.TrimThresholdBytes = originalThreshold;
        }
    }

    [Theory]
    [InlineData("Out of Memory Error: failed to pin block of size 256.0 KiB (1.8 GiB / 1.8 GiB used)")]
    [InlineData("Out of Memory Error: could not free up enough memory for the new allocation")]
    public void OutOfMemoryMessage_NamesTheSettingAndItsCurrentValue(string duckDbMessage)
    {
        DuckDbInitializer.ConfiguredMemoryLimitGb = 3;
        var text = DuckDbMemoryLimitSetting.Describe(new InvalidOperationException(duckDbMessage));

        Assert.StartsWith(duckDbMessage, text);
        Assert.Contains("DuckDB memory limit", text);
        Assert.Contains("Settings", text);
        Assert.Contains("3 GB", text);
    }

    [Fact]
    public void OutOfMemoryMessage_FoundInAnInnerException()
    {
        DuckDbInitializer.ConfiguredMemoryLimitGb = 5;
        var ex = new InvalidOperationException("The read failed", new InvalidOperationException("Out of Memory Error: failed to pin block"));
        Assert.Contains("5 GB", DuckDbMemoryLimitSetting.Describe(ex));
    }

    [Fact]
    public void OtherErrors_AreLeftAlone()
    {
        var ex = new InvalidOperationException("Catalog Error: Table with name x does not exist");
        Assert.Equal(ex.Message, DuckDbMemoryLimitSetting.Describe(ex));
        Assert.False(DuckDbMemoryLimitSetting.IsOutOfMemory(null));
    }

    [Fact]
    public void SaveReport_AnOutOfRangeMemoryLimit_IsAnObjection()
    {
        Assert.Equal(SettingsSaveOutcome.WrittenWithObjections,
            SettingsSaveReport.Classify(true, false, true, true, true, memoryLimitValid: false));
        Assert.Equal(SettingsSaveOutcome.Saved,
            SettingsSaveReport.Classify(true, false, true, true, true));
        Assert.Equal(SettingsSaveOutcome.NothingWritten,
            SettingsSaveReport.Classify(false, false, true, true, true, memoryLimitValid: false));
    }

    /// <summary>
    /// The Settings window rejects an out-of-range entry with a message that states the range, says the value is
    /// not saved, and warns that a change needs a restart. A WPF window cannot be built here, so the source is
    /// the pin.
    /// </summary>
    [Fact]
    public void SettingsWindow_ValidatesThroughTheSharedRangeAndSaysItNeedsARestart()
    {
        var src = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Windows", "SettingsWindow.xaml.cs"));
        Assert.Contains("DuckDbMemoryLimitSetting.IsInRange(gb, physical)", src);
        Assert.Contains("DuckDbMemoryLimitSetting.RangeText(physical)", src);
        Assert.Contains("takes effect after Lite restarts", src);

        var xaml = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Windows", "SettingsWindow.xaml"));
        Assert.Contains("DuckDbMemoryLimitBox", xaml);
        Assert.Contains("Takes effect after Lite restarts", xaml);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
