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
using System.Text.Json;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9772: a collection_schedule.json the loader cannot parse is not lost. The rejected file is kept beside it as
/// <c>collection_schedule.json.rejected-&lt;UTC stamp&gt;</c> before anything writes collection_schedule.json, a broken file with
/// no usable backup runs on the default schedules without being replaced by them, and one warning says what was rejected and
/// what Lite runs on. A v1 file is a successful read: it is upgraded and saved as before. Every test runs over a temp folder.
/// </summary>
public sealed class CollectionScheduleRejectedCopyTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pmlite-9772s-" + Guid.NewGuid().ToString("N"));
    private readonly CapturingLogger _log = new();

    public CollectionScheduleRejectedCopyTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private string ScheduleFile => Path.Combine(_dir, "collection_schedule.json");
    private string BackupFile => ScheduleFile + ".bak";

    private sealed class CapturingLogger : ILogger<ScheduleManager>
    {
        public List<(LogLevel Level, string Message)> Lines { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => Lines.Add((logLevel, formatter(state, exception)));
    }

    private ScheduleManager NewManager() => new(_dir, _log);

    private string[] RejectedCopies() =>
        Directory.GetFiles(_dir, "collection_schedule.json.rejected-*").OrderBy(f => f, StringComparer.Ordinal).ToArray();

    private static string V2(string collector, string frequency) =>
        $$"""{ "version": 2, "default_schedule": [ { "name": "{{collector}}", "enabled": true, "frequency_minutes": {{frequency}}, "retention_days": 30 } ], "server_overrides": {} }""";

    private int Frequency(ScheduleManager manager, string collector) =>
        manager.GetDefaultSchedule().First(s => s.Name == collector).FrequencyMinutes;

    /// <summary>A user's hand edit: a cadence that is not a number, so the whole file fails at $.default_schedule[0].frequency_minutes.</summary>
    private static readonly string s_userEdit = V2("deadlocks", "\"often\"");

    [Fact]
    public void AGoodFile_LoadsItsSchedule_KeepsNoCopy_AndWarnsOfNothing()
    {
        File.WriteAllText(ScheduleFile, V2("deadlocks", "7"));

        var manager = NewManager();

        Assert.Equal(7, Frequency(manager, "deadlocks"));
        Assert.Empty(RejectedCopies());
        Assert.DoesNotContain(_log.Lines, l => l.Level >= LogLevel.Warning);
    }

    [Fact]
    public void AMissingFile_StillGetsItsFirstSave()
    {
        NewManager();

        Assert.True(File.Exists(ScheduleFile));
        Assert.Empty(RejectedCopies());
        Assert.DoesNotContain(_log.Lines, l => l.Level >= LogLevel.Warning);
    }

    [Fact]
    public void ABrokenFile_WithAGoodBackup_IsKept_RunsOnTheBackup_AndTheNextSaveDoesNotLoseIt()
    {
        File.WriteAllText(BackupFile, V2("deadlocks", "7"));
        File.WriteAllText(ScheduleFile, s_userEdit);
        var original = File.ReadAllBytes(ScheduleFile);

        var manager = NewManager();

        Assert.Equal(7, Frequency(manager, "deadlocks"));
        var copy = Assert.Single(RejectedCopies());
        Assert.Equal(original, File.ReadAllBytes(copy));
        Assert.Equal(original, File.ReadAllBytes(ScheduleFile));

        manager.UpdateSchedule("deadlocks", enabled: false);

        Assert.NotEqual(original, File.ReadAllBytes(ScheduleFile));
        Assert.DoesNotContain("often", File.ReadAllText(ScheduleFile), StringComparison.Ordinal);
        Assert.Equal(7, Frequency(NewManager(), "deadlocks"));
        Assert.Equal(original, File.ReadAllBytes(copy));
        Assert.Single(RejectedCopies());
    }

    [Fact]
    public void ABrokenFile_WithAGoodV1Backup_RunsOnTheBackup_AndLeavesTheFileAlone()
    {
        File.WriteAllText(BackupFile, """{ "collectors": [ { "name": "deadlocks", "enabled": true, "frequency_minutes": 9, "retention_days": 30 } ] }""");
        File.WriteAllText(ScheduleFile, s_userEdit);
        var original = File.ReadAllBytes(ScheduleFile);

        var manager = NewManager();

        Assert.Equal(9, Frequency(manager, "deadlocks"));
        Assert.Equal(original, File.ReadAllBytes(ScheduleFile));
        Assert.Equal(original, File.ReadAllBytes(Assert.Single(RejectedCopies())));
        var warn = Assert.Single(_log.Lines, l => l.Level >= LogLevel.Warning);
        Assert.Contains("v1 format", warn.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ABackupWithAnOverCapDeltaCadence_IsClampedInMemory_AndTheFileIsNotWritten()
    {
        var overCap = CollectorDeltaCalculator.MaxDeltaFrequencyMinutes + 100;
        File.WriteAllText(BackupFile, V2("wait_stats", overCap.ToString()));
        File.WriteAllText(ScheduleFile, s_userEdit);
        var original = File.ReadAllBytes(ScheduleFile);

        var manager = NewManager();

        Assert.Equal(CollectorDeltaCalculator.MaxDeltaFrequencyMinutes, Frequency(manager, "wait_stats"));
        Assert.Equal(original, File.ReadAllBytes(ScheduleFile));
        Assert.Equal(original, File.ReadAllBytes(Assert.Single(RejectedCopies())));
    }

    [Fact]
    public void ABrokenFile_SaysOnceWhatWasRejected_WhatLiteRunsOn_AndWhereTheCopyIs()
    {
        File.WriteAllText(BackupFile, V2("deadlocks", "7"));
        File.WriteAllText(ScheduleFile, s_userEdit);

        NewManager();

        var warn = Assert.Single(_log.Lines, l => l.Level >= LogLevel.Warning);
        Assert.Equal(LogLevel.Warning, warn.Level);
        Assert.Contains(ScheduleFile, warn.Message, StringComparison.Ordinal);
        Assert.Contains("$.default_schedule[0].frequency_minutes", warn.Message, StringComparison.Ordinal);
        Assert.Contains("JsonException", warn.Message, StringComparison.Ordinal);
        Assert.Contains("in collection_schedule.json.bak", warn.Message, StringComparison.Ordinal);
        Assert.Contains(Path.GetFileName(Assert.Single(RejectedCopies())), warn.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ABrokenFile_WithNoBackup_RunsOnTheDefaults_AndIsNotReplacedByThem()
    {
        File.WriteAllText(ScheduleFile, s_userEdit);
        var original = File.ReadAllBytes(ScheduleFile);

        var manager = NewManager();

        Assert.NotEmpty(manager.GetDefaultSchedule());
        Assert.Equal(original, File.ReadAllBytes(ScheduleFile));
        Assert.Equal(original, File.ReadAllBytes(Assert.Single(RejectedCopies())));
        var warn = Assert.Single(_log.Lines, l => l.Level >= LogLevel.Warning);
        Assert.Contains("there is no collection_schedule.json.bak", warn.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ABrokenFile_WithABrokenBackup_RunsOnTheDefaults_AndLeavesTheFileAlone()
    {
        File.WriteAllText(BackupFile, "{ not json");
        File.WriteAllText(ScheduleFile, s_userEdit);
        var original = File.ReadAllBytes(ScheduleFile);

        var manager = NewManager();

        Assert.NotEmpty(manager.GetDefaultSchedule());
        Assert.Equal(original, File.ReadAllBytes(ScheduleFile));
        Assert.Equal(original, File.ReadAllBytes(Assert.Single(RejectedCopies())));
        var warn = Assert.Single(_log.Lines, l => l.Level >= LogLevel.Warning);
        Assert.Contains("collection_schedule.json.bak could not be read either", warn.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AScheduleChangedAfterABrokenFile_DoesNotCostTheUserTheirFile()
    {
        File.WriteAllText(ScheduleFile, s_userEdit);
        var original = File.ReadAllBytes(ScheduleFile);
        var manager = NewManager();

        manager.UpdateSchedule("deadlocks", frequencyMinutes: 11);

        Assert.Equal(11, Frequency(NewManager(), "deadlocks"));
        Assert.Equal(original, File.ReadAllBytes(Assert.Single(RejectedCopies())));
    }

    [Fact]
    public void StartingAgainOnTheSameUnfixedFile_KeepsOneCopy()
    {
        File.WriteAllText(ScheduleFile, s_userEdit);

        NewManager();
        NewManager();
        NewManager();

        Assert.Single(RejectedCopies());
    }

    [Fact]
    public void AV1File_IsStillUpgraded_Saved_AndBackedUp_WithNoRejectedCopy()
    {
        var v1 = """{ "collectors": [ { "name": "deadlocks", "enabled": true, "frequency_minutes": 9, "retention_days": 30 } ] }""";
        File.WriteAllText(ScheduleFile, v1);

        var manager = NewManager();

        Assert.Equal(9, Frequency(manager, "deadlocks"));
        Assert.Equal(v1, File.ReadAllText(ScheduleFile + ".v1.bak"));
        using var doc = JsonDocument.Parse(File.ReadAllText(ScheduleFile));
        Assert.Equal(2, doc.RootElement.GetProperty("version").GetInt32());
        Assert.Empty(RejectedCopies());
        Assert.DoesNotContain(_log.Lines, l => l.Level >= LogLevel.Warning);
        Assert.Equal(9, Frequency(NewManager(), "deadlocks"));
    }
}
