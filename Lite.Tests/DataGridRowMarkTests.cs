/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Ui;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2645: session-scoped row marks, so an operator can record "it is done" / "to do" / "do not do"
/// against rows they are working through — asked for on the Index Analysis grid, where you decide index
/// by index and need to remember which ones you have dealt with.
///
/// <para>
/// The marks are held against the row OBJECT. That is a deliberate limit: <c>UpdateData</c> replaces the
/// row objects on every refresh, so on a live grid a mark lasts until the next one, and on a
/// run-on-demand grid — which is where this was asked for — it lasts as long as the result set. Keying on
/// row CONTENT instead would survive refreshes and needs a key function per grid, fifty-two of them, where
/// a wrong key silently moves somebody's "do not do" onto a different index. For a note whose entire value
/// is being right about which row it is on, that trade is the wrong way round.
/// </para>
/// </summary>
public class DataGridRowMarkTests
{
    private sealed class Row
    {
        public string Name { get; init; } = "";
    }

    [Fact]
    public void AMarkIsRememberedAgainstTheRowItWasSetOn()
    {
        var a = new Row { Name = "ix_a" };
        var b = new Row { Name = "ix_b" };

        DataGridRowMarks.Set(a, DataGridRowMark.Done);

        Assert.Equal(DataGridRowMark.Done, DataGridRowMarks.Get(a));
        Assert.Equal(DataGridRowMark.None, DataGridRowMarks.Get(b));
    }

    [Fact]
    public void SettingAMarkAgainReplacesIt_RatherThanStacking()
    {
        var row = new Row();

        DataGridRowMarks.Set(row, DataGridRowMark.ToDo);
        DataGridRowMarks.Set(row, DataGridRowMark.DoNot);

        Assert.Equal(DataGridRowMark.DoNot, DataGridRowMarks.Get(row));
    }

    [Fact]
    public void NoneClearsTheMark()
    {
        var row = new Row();

        DataGridRowMarks.Set(row, DataGridRowMark.Done);
        DataGridRowMarks.Set(row, DataGridRowMark.None);

        Assert.Equal(DataGridRowMark.None, DataGridRowMarks.Get(row));
    }

    /// <summary>
    /// Identity, not equality. Two rows that happen to carry the same values are different rows, and a
    /// refresh that produces an equal-looking object must NOT inherit the old one's mark — that is the
    /// content-keyed behaviour this deliberately does not implement, and inheriting it by accident would
    /// be the worst of both.
    /// </summary>
    [Fact]
    public void MarksAreByIdentity_NotByValue()
    {
        var original = new Row { Name = "ix_a" };
        var afterRefresh = new Row { Name = "ix_a" };

        DataGridRowMarks.Set(original, DataGridRowMark.Done);

        Assert.Equal(DataGridRowMark.None, DataGridRowMarks.Get(afterRefresh));
    }

    [Fact]
    public void ANullRowIsNeitherMarkedNorThrows()
    {
        DataGridRowMarks.Set(null, DataGridRowMark.Done);

        Assert.Equal(DataGridRowMark.None, DataGridRowMarks.Get(null));
    }

    /// <summary>
    /// Every grid that shows the mark items must also paint them.
    ///
    /// <para>The menu is one shared resource, so adding the items put them on all twenty FinOps grids at
    /// once — while the paint hangs off each grid's own <c>LoadingRow</c>. A grid with the menu and no
    /// hook offers an action that appears to do nothing, which is worse than not offering it. I shipped
    /// exactly that on the first cut: menu on twenty, hook on two.</para>
    /// </summary>
    [Fact]
    public void EveryGridOfferingTheMarkMenu_AlsoPaintsIt()
    {
        var xaml = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Controls", "FinOpsTab.xaml"));

        Assert.Contains("Click=\"MarkRow_Click\"", xaml, StringComparison.Ordinal);

        var unwired = Regex.Matches(xaml, @"<DataGrid x:Name=""(?<name>\w+)""(?<body>.*?)>", RegexOptions.Singleline)
            .Where(m => !m.Groups["body"].Value.Contains("MarkedGrid_LoadingRow", StringComparison.Ordinal))
            .Select(m => m.Groups["name"].Value)
            .ToArray();

        Assert.True(unwired.Length == 0,
            "These grids carry the shared context menu (and so the mark items) but have no LoadingRow hook, " +
            "so marking them would appear to do nothing: " + string.Join(", ", unwired));
    }

    /// <summary>
    /// All four items share one handler and differ only by <c>Tag</c>, so a fifth mark is a XAML line
    /// rather than a fifth handler — and a typo'd Tag falls to <c>None</c>, which clears rather than
    /// mismarks.
    /// </summary>
    [Fact]
    public void TheFourMarkItemsShareOneHandler()
    {
        var xaml = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Controls", "FinOpsTab.xaml"));

        foreach (var tag in new[] { "Done", "ToDo", "DoNot", "None" })
        {
            Assert.Contains($"Tag=\"{tag}\" Click=\"MarkRow_Click\"", xaml, StringComparison.Ordinal);
        }
    }

    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? throw new InvalidOperationException("PerformanceMonitor.sln not found above the test output directory.");
    }
}
