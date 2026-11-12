/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Helpers;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7228: the three history windows save a plan through <c>FileSaveHelper.SavePlanFile</c>, which returns true only
/// when the file was written (so QueryStatsHistoryWindow shows "Saved" on true alone) and names the file safely.
/// </summary>
public class HistoryWindowPlanSaveTests
{
    [Fact]
    public void TryWriteText_AFailedWrite_ReturnsFalseWithTheReason_AndNeverThrows()
    {
        var folder = Path.Combine(Path.GetTempPath(), "pm7228-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(folder);
        try
        {
            // A directory is not a file: the write fails the way a full disk or a locked file does.
            var ok = FileSaveHelper.TryWriteText(folder, "<ShowPlanXML/>", out var error);

            Assert.False(ok);
            Assert.False(string.IsNullOrWhiteSpace(error));
        }
        finally
        {
            Directory.Delete(folder);
        }
    }

    [Fact]
    public void TryWriteText_AGoodWrite_ReturnsTrue_AndWritesUtf8()
    {
        var path = Path.Combine(Path.GetTempPath(), "pm7228-" + Guid.NewGuid().ToString("N") + ".sqlplan");
        try
        {
            Assert.True(FileSaveHelper.TryWriteText(path, "<ShowPlanXML/>", out var error));
            Assert.Null(error);
            Assert.Equal("<ShowPlanXML/>", File.ReadAllText(path));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void SanitizeFileName_ReplacesEveryCharacterAFileNameCannotHold()
    {
        // A collected object name can carry any of these; the pre-filled name must still be a legal file name.
        var name = "ProcPlan_" + string.Join("", Path.GetInvalidFileNameChars()) + "_end";
        var clean = FileSaveHelper.SanitizeFileName(name);

        Assert.Equal(name.Length, clean.Length);
        Assert.StartsWith("ProcPlan_", clean, StringComparison.Ordinal);
        Assert.EndsWith("_end", clean, StringComparison.Ordinal);
        Assert.Equal(-1, clean.IndexOfAny(Path.GetInvalidFileNameChars()));
        Assert.Equal("proc_plan_dbo.Orders", FileSaveHelper.SanitizeFileName("proc_plan_dbo.Orders"));
        Assert.Equal("a_b_c", FileSaveHelper.SanitizeFileName("a/b:c"));
    }

    [Theory]
    [InlineData(0, "Refreshed just now")]
    [InlineData(4, "Refreshed just now")]
    [InlineData(5, "Refreshed 5s ago")]
    [InlineData(59, "Refreshed 59s ago")]
    [InlineData(60, "Refreshed 1m ago")]
    [InlineData(3599, "Refreshed 59m ago")]
    public void StaleDataIndicator_RefreshedText_MatchesTheOldTabText(int seconds, string expected)
    {
        Assert.Equal(expected, StaleDataIndicator.RefreshedText(TimeSpan.FromSeconds(seconds)));
    }
}
